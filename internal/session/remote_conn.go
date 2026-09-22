package session

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/rangame/server/internal/transport"
)

// remoteConnSeq 全局自增的 RemoteConn ID（与 transport.Conn.ID 同命名空间）。
var remoteConnSeq atomic.Uint64

// RemoteConn 是 Logic 侧 Session 的下行端点（架构文档 §12.2 / §12.7）。
//
// 单机形态下 Session.conn 是包装 TCP/WS 的 ConnSession；
// 拆分后 Logic 侧的 Session.conn 改为 RemoteConn，下行帧通过 node-to-node
// Transport 回传给 Gateway，再由 Gateway 写聚合 flush 到客户端。
//
// RemoteConn 实现 transport.Conn 接口，可直接传入 Manager.Create/Reconnect，
// Session 代码零改动（Send/ID/Close 语义不变）。
type RemoteConn struct {
	id        uint64
	uid       string
	gatewayID string
	send      func(*transport.Frame) error // 下行回传：调 NodeTransport.Forward
	meta      sync.Map

	closeOnce sync.Once
	closed    atomic.Bool
}

// NewRemoteConn 创建 Logic 侧下行连接。
// send 回调由 LogicHandler 注入，负责把帧转发回源 Gateway。
func NewRemoteConn(uid, gatewayID string, send func(*transport.Frame) error) *RemoteConn {
	return &RemoteConn{
		id:        remoteConnSeq.Add(1),
		uid:       uid,
		gatewayID: gatewayID,
		send:      send,
	}
}

// ID 返回连接级唯一 ID。
func (c *RemoteConn) ID() uint64 { return c.id }

// Push 下行投递：解码已编码帧 → 回调 send 转发到 Gateway。
// ConnSession.Send 已先 EncodeFrame，这里 Decode 回来是为了复用 NodeTransport
// 的 InnerHeader+Frame 接口（演进态骨架；真实部署可直接传 raw 减少编解码）。
func (c *RemoteConn) Push(data []byte) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	f, err := transport.DecodeFrame(bytes.NewReader(data), 0)
	if err != nil {
		return err
	}
	return c.send(f)
}

// Read 不适用：Logic 侧帧由 NodeTransport handler 主动推送，不走 Read 循环。
func (c *RemoteConn) Read(_ context.Context) (*transport.Frame, error) {
	return nil, errors.New("session: RemoteConn.Read not supported")
}

// Close 标记关闭（不主动通知 Gateway；由会话释放流程处理下行清理）。
func (c *RemoteConn) Close(_ string) error {
	c.closeOnce.Do(func() { c.closed.Store(true) })
	return nil
}

// RemoteAddr 返回源 Gateway ID（Logic 侧无真实对端网络地址）。
func (c *RemoteConn) RemoteAddr() string { return "gw:" + c.gatewayID }

// Meta 连接级元数据。
func (c *RemoteConn) Meta() *sync.Map { return &c.meta }
