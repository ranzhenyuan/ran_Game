package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

var (
	// ErrTokenInvalid reconnectToken 无效或已被一次性消费（§11.4）。
	ErrTokenInvalid = errors.New("session: invalid or consumed reconnect token")
	// ErrNotFound 会话不存在。
	ErrNotFound = errors.New("session: not found")
)

// ReleaseFunc 会话销毁回调（宽限期超时或被顶号）。
// 阶段 5 用于停止 PlayerActor；阶段 6 同时写 recover:room:{uid}（§5.1）。
type ReleaseFunc func(uid, roomID string)

// Config Manager 配置。
type Config struct {
	ReplayCapacity int           // 可靠环形缓冲容量，默认 256
	GracePeriod    time.Duration // 宽限期，默认 60s
	SweepInterval  time.Duration // 扫描间隔，默认 5s
	HeartbeatMS    int           // 登录响应下发的心跳间隔
	OnRelease      ReleaseFunc
	Metrics        *obs.Registry
	Now            func() time.Time // 测试可注入固定时钟
}

func (c *Config) withDefaults() {
	if c.ReplayCapacity <= 0 {
		c.ReplayCapacity = 256
	}
	if c.GracePeriod <= 0 {
		c.GracePeriod = 60 * time.Second
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = 5 * time.Second
	}
	if c.HeartbeatMS <= 0 {
		c.HeartbeatMS = 15000
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Manager 会话注册表与生命周期管理。
type Manager struct {
	cfg Config

	mu      sync.RWMutex
	byUID   map[string]*Session
	byConn  map[uint64]*Session
	byToken map[string]*Session
}

// NewManager 创建会话管理器（不启动后台扫描；需调用 Start）。
func NewManager(cfg Config) *Manager {
	cfg.withDefaults()
	return &Manager{
		cfg:     cfg,
		byUID:   make(map[string]*Session),
		byConn:  make(map[uint64]*Session),
		byToken: make(map[string]*Session),
	}
}

// SetOnRelease 设置会话销毁回调（装配期在 NewManager 之后补接线）。
func (m *Manager) SetOnRelease(fn ReleaseFunc) {
	m.cfg.OnRelease = fn
}

// Start 启动宽限期扫描；ctx 取消后停止。
func (m *Manager) Start(ctx context.Context) {
	t := time.NewTicker(m.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Sweep(m.cfg.Now())
		}
	}
}

// Sweep 释放宽限期超时的离线会话（导出以便测试确定性触发）。
func (m *Manager) Sweep(now time.Time) int {
	m.mu.RLock()
	candidates := make([]*Session, 0, len(m.byUID))
	for _, s := range m.byUID {
		if s.stateVal() == stateOffline && s.offlineDuration(now) >= m.cfg.GracePeriod {
			candidates = append(candidates, s)
		}
	}
	m.mu.RUnlock()

	for _, s := range candidates {
		m.destroy(s, "grace period expired")
	}
	return len(candidates)
}

// Create 登录创建会话。同 uid 已存在会话时执行顶号：
// 向旧连接尽力推 2004 踢人通知 → 销毁旧会话（触发 OnRelease）→ 建立新会话。
func (m *Manager) Create(uid string, conn transport.Conn, codec byte) (*Session, string) {
	token := genToken()
	sess := newSession(uid, conn, codec, m.cfg.ReplayCapacity)
	sess.setToken(token)

	m.mu.Lock()
	old := m.byUID[uid]
	m.byUID[uid] = sess
	m.byToken[token] = sess
	m.byConn[conn.ID()] = sess
	m.mu.Unlock()

	if m.cfg.Metrics != nil {
		m.cfg.Metrics.Gauge("online_sessions").Inc()
	}

	if old != nil {
		m.kickAndDestroy(old, "replaced by new login")
	}
	return sess, token
}

// MarkOffline 连接断开时由读泵调用：解绑 Conn 进入宽限期，
// 会话与 PlayerActor 保留、双通道缓冲继续累积（§5.1）。
func (m *Manager) MarkOffline(sess *Session) {
	old := sess.unbind()
	m.mu.Lock()
	if m.byConn[old.ID()] == sess {
		delete(m.byConn, old.ID())
	}
	m.mu.Unlock()
	if old != nil {
		_ = old.Close("session offline")
	}
}

// MarkOfflineConn 读泵退出时的安全版本：仅当会话仍绑定该读泵对应的连接时才解绑。
// 返回 false 表示会话已重连到新连接，本次退出无需处理。
func (m *Manager) MarkOfflineConn(sess *Session, connID uint64) bool {
	old, ok := sess.unbindIfConn(connID)
	if !ok {
		return false
	}
	m.mu.Lock()
	if m.byConn[connID] == sess {
		delete(m.byConn, connID)
	}
	m.mu.Unlock()
	if old != nil {
		_ = old.Close("session offline")
	}
	return true
}

// Reconnect 用一次性 reconnectToken 重连：校验 → 轮换 token → 重绑新连接。
// 返回会话；调用方随后通过 sess.Replay 取重放数据推给新连接。
func (m *Manager) Reconnect(token string, conn transport.Conn, codec byte) (*Session, error) {
	m.mu.RLock()
	sess := m.byToken[token]
	m.mu.RUnlock()
	if sess == nil {
		return nil, ErrTokenInvalid
	}

	newToken := genToken()
	// rotateToken 原子校验旧 token：并发重连同一 token 只有一方能成功。
	if !sess.rotateToken(token, newToken) {
		return nil, ErrTokenInvalid
	}
	oldConn := sess.bind(conn, codec)

	m.mu.Lock()
	delete(m.byToken, token)
	m.byToken[newToken] = sess
	if oldConn != nil && m.byConn[oldConn.ID()] == sess {
		delete(m.byConn, oldConn.ID())
	}
	m.byConn[conn.ID()] = sess
	m.mu.Unlock()

	if oldConn != nil && oldConn.ID() != conn.ID() {
		_ = oldConn.Close("reconnected from another conn")
	}
	return sess, nil
}

// Get 按 UID 查找会话。
func (m *Manager) Get(uid string) (*Session, bool) {
	m.mu.RLock()
	s, ok := m.byUID[uid]
	m.mu.RUnlock()
	return s, ok
}

// GetByConnID 按连接 ID 查找会话。
func (m *Manager) GetByConnID(id uint64) (*Session, bool) {
	m.mu.RLock()
	s, ok := m.byConn[id]
	m.mu.RUnlock()
	return s, ok
}

// OnlineCount 当前会话总数（含宽限离线态）。
func (m *Manager) OnlineCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byUID)
}

// destroy 彻底销毁会话（宽限期超时路径）。
func (m *Manager) destroy(sess *Session, reason string) {
	if !m.removeFromMaps(sess) {
		return // 已被销毁/替换
	}
	conn := sess.currentConn()
	sess.markReleased()
	if conn != nil {
		_ = conn.Close(reason)
	}
	if m.cfg.Metrics != nil {
		m.cfg.Metrics.Gauge("online_sessions").Add(-1)
		m.cfg.Metrics.Counter("session_released_total").Inc()
	}
	if m.cfg.OnRelease != nil {
		m.cfg.OnRelease(sess.UID(), sess.RoomID())
	}
}

// kickAndDestroy 顶号路径：先发 2004 再销毁。
// 注意：此时 byUID 槽已指向新会话，不能走依赖 uid 槽的 removeFromMaps，
// 只清理旧会话自身的 token/conn 索引，再执行释放回调。
func (m *Manager) kickAndDestroy(sess *Session, reason string) {
	// 尽力下发踢人通知（走可靠消息，带服务端 seq；旧连接若已离线则忽略）。
	notice := struct {
		Code   uint16 `json:"code"`
		Reason string `json:"reason"`
	}{Code: uint16(framework.ErrKicked), Reason: reason}
	_ = sess.PushReliable(framework.MsgKick, notice)

	m.mu.Lock()
	if t := sess.currentToken(); t != "" && m.byToken[t] == sess {
		delete(m.byToken, t)
	}
	if c := sess.currentConn(); c != nil && m.byConn[c.ID()] == sess {
		delete(m.byConn, c.ID())
	}
	m.mu.Unlock()

	conn := sess.currentConn()
	sess.markReleased()
	if conn != nil {
		_ = conn.Close(reason)
	}
	if m.cfg.Metrics != nil {
		m.cfg.Metrics.Gauge("online_sessions").Add(-1)
		m.cfg.Metrics.Counter("session_released_total").Inc()
	}
	if m.cfg.OnRelease != nil {
		m.cfg.OnRelease(sess.UID(), sess.RoomID())
	}
}

// removeFromMaps 在锁内清理指向 sess 的全部索引；返回 false 表示已不在册。
func (m *Manager) removeFromMaps(sess *Session) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cur := m.byUID[sess.UID()]; cur != sess {
		return false
	}
	delete(m.byUID, sess.UID())
	if t := sess.currentToken(); t != "" {
		delete(m.byToken, t)
	}
	if c := sess.currentConn(); c != nil {
		if m.byConn[c.ID()] == sess {
			delete(m.byConn, c.ID())
		}
	}
	return true
}

// NewKickFrame 构造踢人通知帧（连接器在其他需要主动踢的场景复用）。
func NewKickFrame(codec byte, reason string) *transport.Frame {
	notice := struct {
		Code   uint16 `json:"code"`
		Reason string `json:"reason"`
	}{Code: uint16(framework.ErrKicked), Reason: reason}
	f, err := protocol.NewErrorFrameFor(codec, framework.MsgKick, 0, framework.ErrKicked, reason, notice)
	if err != nil {
		return nil
	}
	return f
}

func genToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 随机源不可用属于系统级故障；退化为时间戳+panic 前的兜底不应发生。
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
