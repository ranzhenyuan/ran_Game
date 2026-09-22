// Package app 演进态 Gateway 连接器（架构文档 §12.4 / §12.7）。
//
// GatewayConnector 替换单机 Connector 的职责：
//   - 上行：客户端帧头解析 → 查 RouteTable → NodeTransport.Forward → Logic
//   - 下行：NodeTransport handler → 按 uid 找本地 Conn → 写聚合 flush
//
// 登录时由 Gateway 选 Logic 节点（§13.6 粘性不迁移），Logic 侧建会话后回传响应，
// Gateway 写入路由表 uid → logicNode。重连 token 带 nodeID 前缀以便路由。
package app

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/rangame/server/internal/gateway"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// GatewayConnector 网关侧连接处理器（§12.7 上行/下行）。
type GatewayConnector struct {
	gw     *gateway.Gateway
	logger *slog.Logger

	// uid → 本地连接（下行投递用）。
	conns sync.Map // uid -> *gwConn

	// 登录/重连的响应通道：key=connID，收到 Logic 响应后唤醒阻塞的 ServeConn。
	pending sync.Map // uint64(connID) -> chan *transport.Frame
}

// NewGatewayConnector 创建 Gateway 连接器。
func NewGatewayConnector(gw *gateway.Gateway, logger *slog.Logger) *GatewayConnector {
	gc := &GatewayConnector{gw: gw, logger: logger}
	tr := gw.Transport()
	// 注册下行回调：Logic 发回的帧按 uid 路由到本地连接。
	_ = tr.AddPeer(gw.ID(), gc.handleDownlink)
	// TCP 链路：入站帧按发送方 nodeID 索引，Logic 分片动态扩缩无法预注册，
	// 统一挂默认入口（具名 handler 未命中时兜底）。
	if dhs, ok := tr.(gateway.DefaultHandlerSetter); ok {
		dhs.SetDefaultHandler(gc.handleDownlink)
	}
	return gc
}

// gwConn 单条客户端连接的 Gateway 侧状态。
type gwConn struct {
	conn      transport.Conn
	uid       string
	logicNode string
}

// ServeConn 读泵：握手（登录/重连）→ 业务帧转发。
func (c *GatewayConnector) ServeConn(ctx context.Context, conn transport.Conn) {
	gc := &gwConn{conn: conn}
	defer c.cleanupConn(gc)

	for {
		f, err := conn.Read(ctx)
		if err != nil {
			return
		}

		switch framework.MsgID(f.MsgID) {
		case framework.MsgLogin:
			if err := c.handleLogin(ctx, gc, f); err != nil {
				c.writeError(conn, f, err)
				return
			}
		case framework.MsgReconnect:
			if err := c.handleReconnect(ctx, gc, f); err != nil {
				c.writeError(conn, f, err)
				return
			}
		case framework.MsgPing:
			// 心跳在 Gateway 直回，不进内部链路（§5.2）。
			c.replyPong(conn, f)
		default:
			// 业务帧：查路由表转发到 Logic。
			if err := c.forwardBusiness(gc, f); err != nil {
				c.writeError(conn, f, err)
			}
		}
	}
}

// handleLogin 登录转发：选 Logic 节点 → 转发 → 等响应 → 写客户端 → Bind 路由。
func (c *GatewayConnector) handleLogin(_ context.Context, gc *gwConn, f *transport.Frame) error {
	// 先解析 uid 并注册到本地连接表，确保下行响应能找到本连接
	// （响应在 request 阻塞期间到达，须提前绑定）。
	uid := parseUIDFromLoginReq(f)
	if uid == "" {
		return framework.NewErr(framework.ErrDecode, "uid required")
	}
	gc.uid = uid
	c.conns.Store(uid, gc)

	nodeID, ok := c.gw.PickLogicNode()
	if !ok {
		return framework.NewErr(framework.ErrMaintenance, "no logic node available")
	}

	resp, err := c.request(nodeID, gc, gateway.InnerHeader{
		MsgType: gateway.MsgUnicast,
	}, f)
	if err != nil {
		return err
	}
	if err := write(gc.conn, resp); err != nil {
		return err
	}

	gc.logicNode = nodeID
	_ = c.gw.Routes().Bind(uid, nodeID)
	return nil
}

// handleReconnect 重连转发：从 token 前缀解析 nodeID → 转发 → 等响应 → 写客户端。
func (c *GatewayConnector) handleReconnect(_ context.Context, gc *gwConn, f *transport.Frame) error {
	nodeID, ok := parseNodeIDFromToken(f)
	if !ok {
		// token 无法解析节点：广播到所有 Logic 节点（演进态骨架兜底）。
		return c.broadcastReconnect(gc, f)
	}

	resp, err := c.request(nodeID, gc, gateway.InnerHeader{
		MsgType: gateway.MsgUnicast,
	}, f)
	if err != nil {
		return err
	}
	return write(gc.conn, resp)
}

// broadcastReconnect 广播重连到所有 Active Logic 节点（token 无 nodeID 前缀时兜底）。
func (c *GatewayConnector) broadcastReconnect(gc *gwConn, f *transport.Frame) error {
	nodes, err := c.gw.Registry().List("logic", true)
	if err != nil || len(nodes) == 0 {
		return framework.NewErr(framework.ErrMaintenance, "no logic node available")
	}
	for _, n := range nodes {
		resp, rerr := c.request(n.ID, gc, gateway.InnerHeader{MsgType: gateway.MsgUnicast}, f)
		if rerr == nil {
			return write(gc.conn, resp)
		}
	}
	return framework.NewErr(framework.ErrTokenInvalid, "")
}

// forwardBusiness 业务帧：查路由表 → 转发到 Logic（不等响应，业务响应走下行通道）。
func (c *GatewayConnector) forwardBusiness(gc *gwConn, f *transport.Frame) error {
	if gc.uid == "" {
		return framework.NewErr(framework.ErrUnauthorized, "login required")
	}
	nodeID, err := c.gw.Routes().Lookup(gc.uid)
	if err != nil {
		return framework.NewErr(framework.ErrUnauthorized, "session not routed")
	}
	return c.gw.Transport().Forward(nodeID, gateway.InnerHeader{
		UID:        gc.uid,
		MsgType:    gateway.MsgUnicast,
		ForwardSeq: f.Seq,
	}, f)
}

// request 转发一帧并阻塞等待 Logic 的响应（登录/重连用）。
func (c *GatewayConnector) request(nodeID string, gc *gwConn, inner gateway.InnerHeader, f *transport.Frame) (*transport.Frame, error) {
	respCh := make(chan *transport.Frame, 1)
	c.pending.Store(gc.conn.ID(), respCh)
	defer c.pending.Delete(gc.conn.ID())

	inner.UID = gc.uid
	if err := c.gw.Transport().Forward(nodeID, inner, f); err != nil {
		return nil, framework.NewErr(framework.ErrInternal, err.Error())
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-time.After(10 * time.Second):
		return nil, framework.NewErr(framework.ErrInternal, "logic response timeout")
	}
}

// handleDownlink 下行回调：Logic 发回的帧按 uid 投递到本地连接。
// 若是登录/重连的响应帧（seq 匹配 pending 请求），走 respCh；否则直接写连接。
func (c *GatewayConnector) handleDownlink(inner gateway.InnerHeader, f *transport.Frame) {
	if c.logger != nil {
		c.logger.Debug("downlink", "uid", inner.UID, "msgID", f.MsgID)
	}
	if inner.UID == "" {
		return
	}
	v, ok := c.conns.Load(inner.UID)
	if !ok {
		if c.logger != nil {
			c.logger.Debug("downlink: no conn for uid", "uid", inner.UID)
		}
		return // 客户端已断开
	}
	gc := v.(*gwConn)

	// 登录/重连响应：唤醒阻塞的 request
	if isHandshakeResp(f) {
		if ch, ok := c.pending.LoadAndDelete(gc.conn.ID()); ok {
			ch.(chan *transport.Frame) <- f
			return
		}
		if c.logger != nil {
			c.logger.Debug("downlink: no pending channel", "connID", gc.conn.ID())
		}
	}
	_ = write(gc.conn, f)
}

// isHandshakeResp 判断是否为登录/重连的响应帧（用于唤醒 pending request）。
// 演进态骨架：通过 msgID 判断（登录/重连响应复用请求 msgID）。
func isHandshakeResp(f *transport.Frame) bool {
	switch framework.MsgID(f.MsgID) {
	case framework.MsgLogin, framework.MsgReconnect:
		return true
	}
	return false
}

func (c *GatewayConnector) cleanupConn(gc *gwConn) {
	if gc.uid != "" {
		c.conns.Delete(gc.uid)
		// 不 Unbind 路由表：会话宽限期内可能重连到其他 Gateway，路由由 Logic 维护
	}
	_ = gc.conn.Close("gateway conn closed")
}

func (c *GatewayConnector) replyPong(conn transport.Conn, f *transport.Frame) {
	codec, err := codecOf(f)
	if err != nil {
		return
	}
	var p struct{ ClientTS int64 }
	_ = codec.Unmarshal(f.Body, &p)
	frame, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgPong, f.Seq, struct {
		ClientTS int64
		ServerTS int64
	}{p.ClientTS, time.Now().UnixMilli()})
	if err == nil {
		_ = write(conn, frame)
	}
}

func (c *GatewayConnector) writeError(conn transport.Conn, f *transport.Frame, err error) {
	var ec *framework.ErrCode
	if !errors.As(err, &ec) {
		ec = framework.NewErr(framework.ErrInternal, err.Error())
	}
	frame, ferr := protocol.NewErrorFrameFor(f.CodecType(), framework.MsgID(f.MsgID), f.Seq, ec.Code, ec.Msg, nil)
	if ferr == nil {
		_ = write(conn, frame)
	}
}

// parseUIDFromLoginReq 从登录请求体解析 uid（演进态骨架：Gateway 需 uid 建路由）。
func parseUIDFromLoginReq(f *transport.Frame) string {
	codec, err := codecOf(f)
	if err != nil {
		return ""
	}
	var req struct {
		UID string `json:"uid"`
	}
	_ = codec.Unmarshal(f.Body, &req)
	return req.UID
}

// parseNodeIDFromToken 从重连 token 解析 nodeID 前缀（格式 "nodeID:token"）。
func parseNodeIDFromToken(f *transport.Frame) (string, bool) {
	codec, err := codecOf(f)
	if err != nil {
		return "", false
	}
	var req struct {
		ReconnectToken string `json:"reconnect_token"`
	}
	_ = codec.Unmarshal(f.Body, &req)
	idx := strings.IndexByte(req.ReconnectToken, ':')
	if idx <= 0 {
		return "", false
	}
	return req.ReconnectToken[:idx], true
}
