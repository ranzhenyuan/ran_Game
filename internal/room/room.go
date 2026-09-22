// Package room 实现 RoomActor 与房间管理器（架构文档 §8）。
//
// 并发模型：
//   - 房间全部状态（成员表、KV、玩法状态）只在 RoomActor 自己的 goroutine 访问，免锁；
//   - 跨 Actor 入口（Manager.Join/Leave）只做一件事：把控制消息投进 RoomActor 邮箱；
//   - 与 Session 的共享仅发生在下行推送（Session 内部带锁，§7.5 共享点①）。
package room

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/pkg/framework"
)

// 控制消息（投递到 RoomActor 邮箱的内部 Payload 类型）。
type (
	joinMsg struct {
		sess  *session.Session
		reply chan error
	}
	leaveMsg struct{ uid string }
	closeMsg struct{}
)

// SessionProvider 按 UID 取会话（*session.Manager 天然满足）。
type SessionProvider interface {
	Get(uid string) (*session.Session, bool)
}

// LogicFactory 房间逻辑工厂：每个房间独立实例，禁止跨房共享可变状态。
type LogicFactory func() framework.RoomLogic

// ModuleDef 模块注册项：逻辑工厂 + 房间默认配置。
type ModuleDef struct {
	Factory LogicFactory
	Config  framework.RoomConfig
}

// Manager 房间管理器：模块注册、创建/定位房间、进/退房入口与存活计数。
type Manager struct {
	engine   *actor.Engine
	sessions SessionProvider
	storage  framework.Storage
	logger   *slog.Logger

	defaultTick   time.Duration
	defaultPolicy framework.MailboxPolicy

	modules atomic.Value // map[string]ModuleDef
	active  atomic.Int64
}

// Option 管理器选项。
type Option func(*Manager)

// WithDefaultTick 默认 Tick 间隔（模块未设置时使用）。
func WithDefaultTick(d time.Duration) Option {
	return func(m *Manager) { m.defaultTick = d }
}

// WithDefaultMailbox 房间 Actor 默认邮箱策略。
func WithDefaultMailbox(p framework.MailboxPolicy) Option {
	return func(m *Manager) { m.defaultPolicy = p }
}

// NewManager 创建房间管理器。
func NewManager(eng *actor.Engine, sp SessionProvider, st framework.Storage, logger *slog.Logger, opts ...Option) *Manager {
	m := &Manager{
		engine:   eng,
		sessions: sp,
		storage:  st,
		logger:   logger,
	}
	m.modules.Store(map[string]ModuleDef{})
	for _, o := range opts {
		o(m)
	}
	if m.defaultTick <= 0 {
		m.defaultTick = 100 * time.Millisecond
	}
	return m
}

// ModuleDef 返回已注册模块定义（匹配规则/默认配置查询用）。
func (m *Manager) ModuleDef(module string) (ModuleDef, bool) {
	def, ok := m.modules.Load().(map[string]ModuleDef)[module]
	return def, ok
}

// RegisterModule 注册玩法模块（启动期调用，运行期只读）。
func (m *Manager) RegisterModule(def ModuleDef) {
	old := m.modules.Load().(map[string]ModuleDef)
	next := make(map[string]ModuleDef, len(old)+1)
	for k, v := range old {
		next[k] = v
	}
	next[def.Config.Module] = def
	m.modules.Store(next)
}

// CreateRoom 按模块名创建房间并注册 roomID→RoomActor；返回房间 ID。
func (m *Manager) CreateRoom(module string) (string, error) {
	defs := m.modules.Load().(map[string]ModuleDef)
	def, ok := defs[module]
	if !ok {
		return "", errors.New("room: module not registered: " + module)
	}
	cfg := def.Config
	if cfg.Module == "" {
		cfg.Module = module
	}
	if cfg.MaxPlayers <= 0 {
		cfg.MaxPlayers = 8
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = m.defaultTick
	}
	if cfg.Mailbox.Capacity <= 0 {
		cfg.Mailbox = m.defaultPolicy
	}

	roomID := genRoomID()
	ra := &roomActor{
		roomID:  roomID,
		cfg:     cfg,
		logic:   def.Factory(),
		mgr:     m,
		members: make(map[string]*member),
		kv:      make(map[string]any),
	}
	id, err := m.engine.Spawn(ra, cfg.Mailbox, 0)
	if err != nil {
		return "", err
	}
	m.engine.Registry().BindRoom(roomID, id)
	m.active.Add(1)
	return roomID, nil
}

// Join 请求玩家进入房间（异步：入箱后由 RoomActor 串行处理，错误经 reply 返回）。
// 会话不存在（宽限期已过）返回错误。
func (m *Manager) Join(roomID, uid string) error {
	target, ok := m.engine.ResolveRoom(roomID)
	if !ok {
		return framework.NewErr(framework.ErrNotInRoom, "room not found")
	}
	sess, ok := m.sessions.Get(uid)
	if !ok {
		return framework.NewErr(framework.ErrUnauthorized, "session not found")
	}
	reply := make(chan error, 1)
	if err := m.engine.Tell(target, &framework.Envelope{
		Payload: joinMsg{sess: sess, reply: reply},
	}); err != nil {
		return err
	}
	return <-reply
}

// Leave 请求玩家离开房间（尽力投递，不等待；会话释放/退房路径使用）。
func (m *Manager) Leave(roomID, uid string) {
	target, ok := m.engine.ResolveRoom(roomID)
	if !ok {
		return
	}
	_ = m.engine.Tell(target, &framework.Envelope{Payload: leaveMsg{uid: uid}})
}

// Exists 房间是否存活（active_rooms 指标/Drain 判定用）。
func (m *Manager) Exists(roomID string) bool {
	_, ok := m.engine.ResolveRoom(roomID)
	return ok
}

// Rooms 当前活跃房间数。
func (m *Manager) Rooms() int { return int(m.active.Load()) }

// roomActor 房间 Actor。除 OnStop 外所有方法都在本 Actor goroutine 执行。
type roomActor struct {
	roomID string
	cfg    framework.RoomConfig
	logic  framework.RoomLogic
	mgr    *Manager

	ctx  framework.ActorCtx
	rctx *roomCtx

	members map[string]*member
	order   []string // 稳定的成员遍历顺序（进房先后）
	kv      map[string]any
	closing bool

	lastTick  time.Time
	tickTimer framework.Handle
	maxTimer  framework.Handle
}

func (a *roomActor) Init(ctx framework.ActorCtx) {
	a.ctx = ctx
	a.rctx = &roomCtx{a: a}
	a.logic.OnCreate(a.rctx, a.cfg)

	a.lastTick = time.Now()
	a.tickTimer = ctx.Every(a.cfg.TickInterval, func() {
		now := time.Now()
		dt := now.Sub(a.lastTick)
		a.lastTick = now
		a.logic.Tick(a.rctx, dt)
	})
	if a.cfg.MaxDuration > 0 {
		a.maxTimer = ctx.After(a.cfg.MaxDuration, func() {
			if tl, ok := a.logic.(framework.TimeoutLogic); ok {
				tl.OnTimeout(a.rctx)
			}
			a.rctx.Close()
		})
	}
}

func (a *roomActor) OnMessage(_ framework.ActorCtx, env *framework.Envelope) {
	switch msg := env.Payload.(type) {
	case joinMsg:
		a.handleJoin(msg)
	case leaveMsg:
		a.handleLeave(msg.uid)
	case closeMsg:
		a.doClose()
	default:
		// 上行对战消息：定位成员后交玩法逻辑 switch msgID 处理。
		p, ok := a.members[env.SenderUID]
		if !ok {
			return // 非本房成员（退房在途/重复帧），忽略
		}
		a.logic.OnMessage(a.rctx, p, env)
	}
}

func (a *roomActor) OnStop(_ framework.ActorCtx) {
	a.mgr.active.Add(-1)
}

func (a *roomActor) handleJoin(msg joinMsg) {
	err := func() error {
		if a.closing {
			return framework.NewErr(framework.ErrNotInRoom, "room closing")
		}
		uid := msg.sess.UID()
		if _, exists := a.members[uid]; exists {
			return nil // 幂等：重复进房
		}
		if len(a.members) >= a.cfg.MaxPlayers {
			return framework.NewErr(framework.ErrRoomFull, "")
		}
		m := &member{sess: msg.sess}
		a.members[uid] = m
		a.order = append(a.order, uid)
		msg.sess.SetRoomID(a.roomID)
		a.logic.OnJoin(a.rctx, m)
		return nil
	}()
	msg.reply <- err
}

func (a *roomActor) handleLeave(uid string) {
	if a.closing {
		return
	}
	m, ok := a.members[uid]
	if !ok {
		return
	}
	delete(a.members, uid)
	a.removeOrder(uid)
	m.sess.SetRoomID("")
	a.logic.OnLeave(a.rctx, m)
	if len(a.members) == 0 {
		a.logic.OnEmpty(a.rctx)
	}
}

func (a *roomActor) removeOrder(uid string) {
	for i, u := range a.order {
		if u == uid {
			a.order = append(a.order[:i], a.order[i+1:]...)
			return
		}
	}
}

// doClose 唯一的销毁入口：OnDestroy → 解绑成员会话 → 停 Actor（Registry 自动解绑房间）。
func (a *roomActor) doClose() {
	if a.closing {
		return
	}
	a.closing = true
	if a.tickTimer != nil {
		a.tickTimer.Cancel()
	}
	if a.maxTimer != nil {
		a.maxTimer.Cancel()
	}
	a.logic.OnDestroy(a.rctx)
	for uid, m := range a.members {
		m.sess.SetRoomID("")
		delete(a.members, uid)
	}
	a.order = nil
	a.ctx.Stop()
}

// member 实现 framework.Player。
type member struct {
	sess *session.Session
}

func (m *member) UID() string  { return m.sess.UID() }
func (m *member) Online() bool { return m.sess.IsOnline() }
func (m *member) Push(msgID framework.MsgID, msg any) error {
	return m.sess.PushReliable(msgID, msg)
}
func (m *member) PushSnapshot(msgID framework.MsgID, msg any) error {
	return m.sess.PushSnapshot(msgID, msg)
}

// roomCtx 实现 framework.RoomCtx（全部方法在 RoomActor goroutine 调用）。
type roomCtx struct {
	a *roomActor
}

func (r *roomCtx) RoomID() string { return r.a.roomID }

func (r *roomCtx) Members() []framework.Player {
	out := make([]framework.Player, 0, len(r.a.order))
	for _, uid := range r.a.order {
		if m, ok := r.a.members[uid]; ok {
			out = append(out, m)
		}
	}
	return out
}

func (r *roomCtx) Member(uid string) (framework.Player, bool) {
	m, ok := r.a.members[uid]
	if !ok {
		return nil, false
	}
	return m, true
}

func (r *roomCtx) KV(key string) any { return r.a.kv[key] }
func (r *roomCtx) SetKV(key string, v any) {
	r.a.kv[key] = v
}

// fanOut 编码一次、逐成员投递（广播高效路径，§3.2 扇出）。
func (r *roomCtx) fanOut(msgID framework.MsgID, msg any, snapshot bool, except map[string]bool) {
	body, err := protocol.MustGet(protocol.TypeJSON).Marshal(msg)
	if err != nil {
		return
	}
	id := uint32(msgID)
	for _, uid := range r.a.order {
		if except[uid] {
			continue
		}
		if m, ok := r.a.members[uid]; ok {
			_ = m.sess.PushEncoded(id, body, snapshot)
		}
	}
}

func (r *roomCtx) Broadcast(msgID framework.MsgID, msg any, exceptUID ...string) {
	r.fanOut(msgID, msg, false, exceptSet(exceptUID))
}

func (r *roomCtx) BroadcastSnapshot(msgID framework.MsgID, msg any, exceptUID ...string) {
	r.fanOut(msgID, msg, true, exceptSet(exceptUID))
}

func (r *roomCtx) Kick(uid string, code framework.Code, reason string) {
	m, ok := r.a.members[uid]
	if !ok {
		return
	}
	notice := struct {
		Code   framework.Code `json:"code"`
		Reason string         `json:"reason"`
	}{Code: code, Reason: reason}
	_ = m.sess.PushReliable(framework.MsgKick, notice)
	r.a.handleLeave(uid)
}

func (r *roomCtx) Close() {
	r.a.OnMessage(nil, &framework.Envelope{Payload: closeMsg{}})
}

func (r *roomCtx) After(d time.Duration, fn func()) framework.Handle {
	return r.a.ctx.After(d, fn)
}

func (r *roomCtx) Every(d time.Duration, fn func()) framework.Handle {
	return r.a.ctx.Every(d, fn)
}

func (r *roomCtx) Storage() framework.Storage { return r.a.mgr.storage }

func exceptSet(except []string) map[string]bool {
	if len(except) == 0 {
		return nil
	}
	m := make(map[string]bool, len(except))
	for _, u := range except {
		m[u] = true
	}
	return m
}

func genRoomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
