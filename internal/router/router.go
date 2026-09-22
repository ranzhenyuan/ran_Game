// Package router 实现 msgID 路由注册表、个人/房间两级定位与中间件管道（架构文档 §6）。
//
// 执行位置分两段（高并发设计）：
//   - 读泵 goroutine：查路由、解码、Auth、RateLimit——拒绝在入箱前廉价完成；
//   - Actor goroutine：Recover/Metric 包裹 Handler 闭包（Invoke）串行执行，
//     Handler 返回的 resp/err 由路由器按原 msgID/seq 自动回包（§6.1）。
package router

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// Source 上行消息来源（阶段 4 由 session.Session 实现）。
type Source interface {
	UID() string
	// Send 在当前连接上直接下发一帧（实现须 goroutine 安全）。
	Send(f *transport.Frame) error
}

// RoomSource 房间消息来源：能提供当前所在房间（阶段 4 的会话实现）。
type RoomSource interface {
	RoomID() string
}

// Authenticator 已登录身份来源；未实现该接口的 Source 视为未登录。
type Authenticator interface {
	Authenticated() bool
}

// Dispatcher 路由器需要的引擎能力（*actor.Engine 天然满足，接口化便于测试）。
type Dispatcher interface {
	Tell(target framework.ID, env *framework.Envelope) error
	ResolvePlayer(uid string) (framework.ID, bool)
	ResolveRoom(roomID string) (framework.ID, bool)
}

// Middleware 中间件：包裹 Handler，可在前后注入逻辑。
type Middleware func(next framework.HandlerFunc) framework.HandlerFunc

type targetKind int

const (
	targetPlayer targetKind = iota
	targetRoom
)

type routeEntry struct {
	factory  func() any
	target   targetKind
	snapshot bool                  // 房间路由：true=快照类（下行 flag bit6，§5.3）
	handler  framework.HandlerFunc // 房间路由为 nil，由 RoomActor.OnMessage 分发
}

// Router 消息路由器。启动期注册路由，运行期只读路由表（§7.3 红线 3）。
type Router struct {
	disp Dispatcher

	mu     sync.RWMutex
	routes map[framework.MsgID]routeEntry

	// 读泵段中间件（按顺序 Auth → RateLimit）。
	preMws []Middleware
	// Actor 段中间件（Recover → Trace → Metric）。
	inMws []Middleware

	buckets    *rateBuckets
	ratePerSec float64
	rateBurst  float64
	authFree   map[framework.MsgID]bool
	metrics    *obs.Registry

	nowFunc func() time.Time
}

// Option 路由器选项。
type Option func(*Router)

// WithRateLimit 开启每 UID 令牌桶（§3.3/§6.3）。rate<=0 表示不限流。
func WithRateLimit(rate, burst float64) Option {
	return func(r *Router) { r.ratePerSec, r.rateBurst = rate, burst }
}

// WithAuthFree 自定义免鉴权 msgID 集合（默认含 Ping/Login/Reconnect）。
func WithAuthFree(ids ...framework.MsgID) Option {
	return func(r *Router) {
		r.authFree = make(map[framework.MsgID]bool, len(ids))
		for _, id := range ids {
			r.authFree[id] = true
		}
	}
}

// WithMiddleware 追加自定义中间件（在默认段之后、Handler 之前执行）。
func WithMiddleware(mw ...Middleware) Option {
	return func(r *Router) { r.inMws = append(r.inMws, mw...) }
}

// New 创建路由器。
func New(disp Dispatcher, opts ...Option) *Router {
	r := &Router{
		disp:    disp,
		routes:  make(map[framework.MsgID]routeEntry),
		buckets: newRateBuckets(),
		authFree: map[framework.MsgID]bool{
			framework.MsgPing:      true,
			framework.MsgLogin:     true,
			framework.MsgReconnect: true,
		},
		nowFunc: time.Now,
	}
	for _, o := range opts {
		o(r)
	}
	r.preMws = []Middleware{authMiddleware(r), rateLimitMiddleware(r)}
	r.inMws = append([]Middleware{recoverMiddleware(), traceMiddleware(), metricMiddleware(r)}, r.inMws...)
	return r
}

// Register 注册个人消息：Handler 在该 UID 的 PlayerActor 内执行，
// 返回值由路由器按原 seq 自动回包（§6.1 两套 API 分工）。
func (r *Router) Register(id framework.MsgID, reqFactory func() any, h framework.HandlerFunc) {
	r.mu.Lock()
	r.routes[id] = routeEntry{factory: reqFactory, target: targetPlayer, handler: h}
	r.mu.Unlock()
}

// RegisterRoom 注册房间消息：仅声明投递目标，逻辑在 RoomLogic.OnMessage 内 switch。
// snapshot=true 表示该消息是快照类（latest-wins，不进可靠重放缓冲，§5.3）。
func (r *Router) RegisterRoom(id framework.MsgID, reqFactory func() any, snapshot bool) {
	r.mu.Lock()
	r.routes[id] = routeEntry{factory: reqFactory, target: targetRoom, snapshot: snapshot}
	r.mu.Unlock()
}

// Lookup 仅供测试/观测。
func (r *Router) Lookup(id framework.MsgID) (bool, targetKind) {
	r.mu.RLock()
	e, ok := r.routes[id]
	r.mu.RUnlock()
	return ok, e.target
}

// Dispatch 读泵入口：解码 → 前置中间件 → 定位 Actor → 入箱。
// 所有同步拒绝（未知消息/解码失败/鉴权/限流/背压）都直接回错误帧。
func (r *Router) Dispatch(src Source, f *transport.Frame) {
	ctx := &msgCtx{
		uid:       src.UID(),
		msgID:     framework.MsgID(f.MsgID),
		seq:       f.Seq,
		trace:     genTraceID(),
		codecType: f.CodecType(),
		src:       src,
	}

	defer func() {
		if rec := recover(); rec != nil {
			r.sendError(src, f, framework.NewErr(framework.ErrInternal, ""))
		}
	}()

	r.mu.RLock()
	entry, ok := r.routes[ctx.msgID]
	r.mu.RUnlock()
	if !ok {
		r.sendError(src, f, framework.NewErr(framework.ErrUnknownMsg, ""))
		return
	}

	codec, err := protocol.Get(f.CodecType())
	if err != nil {
		r.sendError(src, f, framework.NewErr(framework.ErrUnknownMsg, "codec not supported"))
		return
	}

	req := entry.factory()
	if len(f.Body) > 0 {
		if err := codec.Unmarshal(f.Body, req); err != nil {
			r.sendError(src, f, framework.NewErr(framework.ErrDecode, err.Error()))
			return
		}
	}

	// 解析目标 Actor（房间消息在鉴权通过后才需要 roomID）。
	var targetID framework.ID
	resolve := framework.HandlerFunc(func(_ framework.MsgCtx, _ any) (any, error) {
		switch entry.target {
		case targetPlayer:
			id, ok := r.disp.ResolvePlayer(src.UID())
			if !ok {
				return nil, framework.NewErr(framework.ErrUnauthorized, "")
			}
			targetID = id
		case targetRoom:
			rs, ok := src.(RoomSource)
			if !ok {
				return nil, framework.NewErr(framework.ErrNotInRoom, "")
			}
			id, ok := r.disp.ResolveRoom(rs.RoomID())
			if !ok {
				return nil, framework.NewErr(framework.ErrNotInRoom, "")
			}
			targetID = id
		}
		return nil, nil
	})

	// 读泵段：Auth → RateLimit → resolve。任一步返回错误即同步拒绝。
	preChain := chain(resolve, r.preMws)
	if _, err := preChain(ctx, req); err != nil {
		r.sendError(src, f, asErrCode(err))
		return
	}

	env := &framework.Envelope{
		MsgID:     ctx.msgID,
		Seq:       f.Seq,
		SenderUID: src.UID(),
		TraceID:   ctx.trace,
	}

	if entry.target == targetRoom {
		// 房间消息：请求体作为普通消息投给 RoomActor，由 OnMessage switch 分发。
		env.Payload = req
		if err := r.disp.Tell(targetID, env); err != nil {
			r.sendError(src, f, r.tellErrToCode(err))
		}
		return
	}

	// 个人消息：把整条处理链封装成 Invoke，在 PlayerActor goroutine 内串行执行。
	inChain := chain(entry.handler, r.inMws)
	env.Payload = actor.Invoke(func(framework.ActorCtx) {
		resp, err := inChain(ctx, req)
		if err != nil {
			r.sendError(src, f, asErrCode(err))
			return
		}
		if resp != nil {
			frame, err := protocol.NewOKFrameFor(f.CodecType(), ctx.msgID, f.Seq, resp)
			if err == nil {
				_ = src.Send(frame)
			}
		}
	})

	if err := r.disp.Tell(targetID, env); err != nil {
		r.sendError(src, f, r.tellErrToCode(err))
	}
}

func (r *Router) sendError(src Source, f *transport.Frame, ec *framework.ErrCode) {
	frame, err := protocol.NewErrorFrameFor(f.CodecType(), framework.MsgID(f.MsgID), f.Seq, ec.Code, ec.Msg, nil)
	if err != nil {
		return
	}
	_ = src.Send(frame)
}

func asErrCode(err error) *framework.ErrCode {
	var ec *framework.ErrCode
	if errors.As(err, &ec) {
		return ec
	}
	return framework.NewErr(framework.ErrInternal, err.Error())
}

func (r *Router) tellErrToCode(err error) *framework.ErrCode {
	switch {
	case errors.Is(err, actor.ErrDropped):
		return framework.NewErr(framework.ErrOverloaded, "") // 4002：客户端指数退避
	case errors.Is(err, actor.ErrKicked):
		return framework.NewErr(framework.ErrKicked, "") // 2004
	case errors.Is(err, actor.ErrActorNotFound):
		return framework.NewErr(framework.ErrUnauthorized, "") // 2001
	case errors.Is(err, actor.ErrShuttingDown):
		return framework.NewErr(framework.ErrMaintenance, "") // 4005
	default:
		return framework.NewErr(framework.ErrInternal, err.Error())
	}
}

// chain 按中间件声明顺序包裹（mws[0] 在最外层）。
func chain(h framework.HandlerFunc, mws []Middleware) framework.HandlerFunc {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

func genTraceID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "trace-unknown"
	}
	return hex.EncodeToString(b[:])
}
