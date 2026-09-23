package app

import (
	"log/slog"
	"time"

	"github.com/rangame/server/internal/gateway"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/router"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// LogicHandler Logic 侧帧接收器（§12.7 上行入口）。
//
// 注册到 NodeTransport 的 AddPeer 回调，接收 Gateway 转发的帧：
//   - 登录/重连：建/绑会话（RemoteConn 作为下行端点），回传响应
//   - 心跳：直回 pong
//   - 业务帧：router.Dispatch 到 Actor 系统
type LogicHandler struct {
	sessions  *session.Manager
	router    *router.Router
	transport gateway.NodeTransport
	routes    gateway.RouteTable // 一级路由表（由 Logic 写入/清除）
	nodeID    string
	logger    *slog.Logger

	startCb func(*session.Session) // 登录成功回调（Spawn PlayerActor）
}

// NewLogicHandler 创建 Logic 侧接收器。
func NewLogicHandler(
	sessions *session.Manager,
	r *router.Router,
	tr gateway.NodeTransport,
	routes gateway.RouteTable,
	nodeID string,
	logger *slog.Logger,
) *LogicHandler {
	lh := &LogicHandler{
		sessions:  sessions,
		router:    r,
		transport: tr,
		routes:    routes,
		nodeID:    nodeID,
		logger:    logger,
	}
	// 注册自身为 Gateway 转发的目标（peer ID 即本 Logic nodeID，Mem 骨架按此索引）。
	_ = tr.AddPeer(nodeID, lh.handle)
	// TCP 链路：Gateway 副本动态扩缩，入站帧 inner.GatewayID 是发送方身份，
	// 无法逐个预注册；挂默认入口统一接收所有 Gateway 转发。
	if dhs, ok := tr.(gateway.DefaultHandlerSetter); ok {
		dhs.SetDefaultHandler(lh.handle)
	}
	return lh
}

// SetOnSessionStart 注入登录成功回调（与单机 Connector.OnSessionStart 同构）。
func (h *LogicHandler) SetOnSessionStart(cb func(*session.Session)) { h.startCb = cb }

// handle 处理来自 Gateway 的帧（NodeTransport 回调入口）。
func (h *LogicHandler) handle(inner gateway.InnerHeader, f *transport.Frame) {
	if h.logger != nil {
		h.logger.Debug("logic handle", "uid", inner.UID, "msgID", f.MsgID, "gwID", inner.GatewayID)
	}
	switch framework.MsgID(f.MsgID) {
	case framework.MsgLogin:
		h.handleLogin(inner, f)
	case framework.MsgReconnect:
		h.handleReconnect(inner, f)
	case framework.MsgPing:
		h.handlePing(inner, f)
	case framework.MsgPullSnap:
		h.handlePullSnap(inner, f)
	default:
		h.dispatch(inner, f)
	}
}

// sendFunc 为指定 uid 构造下行回传函数（RemoteConn 用）。
// 目标 Gateway 由 inner.GatewayID 指定（即帧来源 Gateway）。
func (h *LogicHandler) sendFunc(inner gateway.InnerHeader) func(*transport.Frame) error {
	return func(f *transport.Frame) error {
		return h.transport.Forward(inner.GatewayID, gateway.InnerHeader{
			UID:        inner.UID,
			MsgType:    gateway.MsgUnicast,
			ForwardSeq: f.Seq,
		}, f)
	}
}

// handleLogin 登录：解码 uid → 建会话（RemoteConn）→ 回传响应（token 带 nodeID 前缀）。
func (h *LogicHandler) handleLogin(inner gateway.InnerHeader, f *transport.Frame) {
	codec, err := codecOf(f)
	if err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrDecode, "codec not supported"))
		return
	}
	var req session.LoginReq
	if err := codec.Unmarshal(f.Body, &req); err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrDecode, err.Error()))
		return
	}
	if req.UID == "" {
		h.sendError(inner, f, framework.NewErr(framework.ErrDecode, "uid required"))
		return
	}

	// RemoteConn：下行通过 NodeTransport 回传给来源 Gateway。
	rc := session.NewRemoteConn(req.UID, inner.GatewayID, h.sendFunc(inner))
	sess, token := h.sessions.Create(req.UID, rc, codec.Type())

	// 写一级路由 uid→本节点（必须在 Create 之后：顶踢时 Create 会先触发旧会话
	// OnRelease→Unbind，再由本 Bind 写入新节点，顺序保证不被覆盖）。
	if err := h.routes.Bind(req.UID, h.nodeID); err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrInternal, "route bind failed"))
		return
	}

	// token 加 nodeID 前缀，Gateway 重连时据此路由回本节点。
	prefixedToken := h.nodeID + ":" + token
	resp := session.LoginResp{
		ReconnectToken: prefixedToken,
		HeartbeatMS:    h.sessions.HeartbeatMS(),
		SessionID:      req.UID,
	}
	frame, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgLogin, f.Seq, resp)
	if err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrInternal, err.Error()))
		return
	}
	_ = h.sendFunc(inner)(frame)

	// 触发会话启动回调（Spawn PlayerActor）——与单机 Connector.OnSessionStart 同构。
	h.onSessionStart(sess)
}

// handleReconnect 重连：剥 nodeID 前缀 → 校验 token → 重绑 RemoteConn → 回响应+重放。
func (h *LogicHandler) handleReconnect(inner gateway.InnerHeader, f *transport.Frame) {
	codec, err := codecOf(f)
	if err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrDecode, "codec not supported"))
		return
	}
	var req session.ReconnectReq
	if err := codec.Unmarshal(f.Body, &req); err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrDecode, err.Error()))
		return
	}

	// 剥掉 nodeID 前缀，还原 session.Manager 存储的原始 token。
	rawToken := stripNodeIDPrefix(req.ReconnectToken, h.nodeID)
	rc := session.NewRemoteConn("", inner.GatewayID, h.sendFunc(inner))
	sess, err := h.sessions.Reconnect(rawToken, rc, codec.Type())
	if err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrTokenInvalid, ""))
		return
	}

	// 重连成功后刷路由（跨节点重连时迁移 uid 到新节点，Lua 自动处理反向集）。
	if err := h.routes.Bind(sess.UID(), h.nodeID); err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrInternal, "route bind failed"))
		return
	}

	reliable, snapshots, overflow := sess.Replay(req.LastReliableSeq, nil)
	resp := session.ReconnectResp{
		NewReconnectToken: h.nodeID + ":" + sess.Token(),
		ResyncRequired:    overflow,
	}
	frame, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgReconnect, f.Seq, resp)
	if err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrInternal, err.Error()))
		return
	}
	send := h.sendFunc(inner)
	_ = send(frame)

	// 重放：可靠差额（溢出则不推）→ 快照。
	if !overflow {
		for _, rf := range reliable {
			if send(rf) != nil {
				break
			}
		}
	}
	for _, sf := range snapshots {
		if send(sf) != nil {
			break
		}
	}
}

// handlePing 心跳直回（不进 Actor）。
func (h *LogicHandler) handlePing(inner gateway.InnerHeader, f *transport.Frame) {
	codec, err := codecOf(f)
	if err != nil {
		return
	}
	var p session.Ping
	_ = codec.Unmarshal(f.Body, &p)
	frame, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgPong, f.Seq, session.Pong{
		ClientTS: p.ClientTS,
		ServerTS: nowMS(),
	})
	if err == nil {
		_ = h.sendFunc(inner)(frame)
	}
}

// handlePullSnap 拉取快照通道最新帧。
func (h *LogicHandler) handlePullSnap(inner gateway.InnerHeader, f *transport.Frame) {
	sess, ok := h.sessions.Get(inner.UID)
	if !ok {
		h.sendError(inner, f, framework.NewErr(framework.ErrUnauthorized, ""))
		return
	}
	codec, err := codecOf(f)
	if err != nil {
		h.sendError(inner, f, framework.NewErr(framework.ErrDecode, "codec not supported"))
		return
	}
	var req session.PullSnapshotReq
	if len(f.Body) > 0 {
		_ = codec.Unmarshal(f.Body, &req)
	}
	send := h.sendFunc(inner)
	okFrame, _ := protocol.NewOKFrameFor(codec.Type(), framework.MsgPullSnap, f.Seq, struct{}{})
	if okFrame != nil {
		_ = send(okFrame)
	}
	for _, sf := range sess.Snapshots(req.MsgIDs) {
		if send(sf) != nil {
			return
		}
	}
}

// dispatch 业务帧：按 uid 查会话 → router.Dispatch。
func (h *LogicHandler) dispatch(inner gateway.InnerHeader, f *transport.Frame) {
	sess, ok := h.sessions.Get(inner.UID)
	if !ok {
		h.sendError(inner, f, framework.NewErr(framework.ErrUnauthorized, "session not found"))
		return
	}
	sess.Touch()
	h.router.Dispatch(sess, f)
}

// sendError 回传错误帧到来源 Gateway。
func (h *LogicHandler) sendError(inner gateway.InnerHeader, f *transport.Frame, ec *framework.ErrCode) {
	frame, err := protocol.NewErrorFrameFor(f.CodecType(), framework.MsgID(f.MsgID), f.Seq, ec.Code, ec.Msg, nil)
	if err != nil {
		return
	}
	_ = h.sendFunc(inner)(frame)
}

// onSessionStart 登录成功回调（Spawn PlayerActor 并绑定注册表）。
func (h *LogicHandler) onSessionStart(sess *session.Session) {
	if h.startCb != nil {
		h.startCb(sess)
	}
}
func stripNodeIDPrefix(token, nodeID string) string {
	prefix := nodeID + ":"
	if len(token) > len(prefix) && token[:len(prefix)] == prefix {
		return token[len(prefix):]
	}
	return token
}

// nowMS 当前毫秒时间戳。
func nowMS() int64 { return time.Now().UnixMilli() }
