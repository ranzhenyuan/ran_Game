package session

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/transport"
)

// Conn 是 PlayerSession 的下行投递端点抽象（架构文档 §12.2）。
//
// 单机形态下由 ConnSession（包装 transport.Conn）实现；
// 分布式拆分后由 node-to-node Transport 存根实现，Session 代码零改动。
type Conn interface {
	// Send 编码并投递一帧；连接未绑定时返回 ErrOffline。
	Send(f *transport.Frame) error
	// ID 底层连接 ID（0 表示未绑定）。
	ID() uint64
	// Close 关闭底层连接。
	Close(reason string) error
}

// ConnSession 连接级会话（Gateway 侧，§12.2）。
//
// 持有 transport.Conn + reconnectToken + 心跳时间戳。
// ConnSession 是稳定身份：跨重连存活，仅内部 transport.Conn 被替换。
// 与 PlayerSession（Session）一一对应。
type ConnSession struct {
	mu   sync.Mutex
	conn transport.Conn // 断线时为 nil（宽限期）

	token      string
	lastActive atomic.Int64 // unix nano
}

// NewConnSession 从 transport.Conn 创建 ConnSession（登录路径）。
func NewConnSession(conn transport.Conn) *ConnSession {
	cs := &ConnSession{conn: conn}
	cs.lastActive.Store(time.Now().UnixNano())
	return cs
}

// Send 编码帧并投递到当前连接；未绑定返回 ErrOffline。
func (c *ConnSession) Send(f *transport.Frame) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return ErrOffline
	}
	raw, err := transport.EncodeFrame(f)
	if err != nil {
		return err
	}
	return conn.Push(raw)
}

// ID 底层连接 ID；未绑定返回 0。
func (c *ConnSession) ID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return 0
	}
	return c.conn.ID()
}

// Close 解绑并关闭底层连接（重复调用安全）。
func (c *ConnSession) Close(reason string) error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		return conn.Close(reason)
	}
	return nil
}

// Conn 返回当前绑定的 transport.Conn（未绑定返回 nil）。
func (c *ConnSession) Conn() transport.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// Bind 重绑 transport.Conn（重连路径）；返回旧连接（可能为 nil）。
func (c *ConnSession) Bind(conn transport.Conn) transport.Conn {
	c.mu.Lock()
	old := c.conn
	c.conn = conn
	c.mu.Unlock()
	return old
}

// Unbind 解绑当前 transport.Conn（连接断开、进宽限期）；返回旧连接。
func (c *ConnSession) Unbind() transport.Conn {
	c.mu.Lock()
	old := c.conn
	c.conn = nil
	c.mu.Unlock()
	return old
}

// UnbindIfConn 仅当当前绑定连接 ID 等于 connID 时解绑（原子，避免重连竞态）。
// 返回（解绑下的旧连接，是否确实解绑）。
func (c *ConnSession) UnbindIfConn(connID uint64) (transport.Conn, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.conn.ID() != connID {
		return nil, false
	}
	old := c.conn
	c.conn = nil
	return old, true
}

// ---- token（一次性消费，§11.4）----

func (c *ConnSession) Token() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token
}

func (c *ConnSession) SetToken(t string) {
	c.mu.Lock()
	c.token = t
	c.mu.Unlock()
}

// RotateToken 校验旧 token 并原子更换；失败返回 false。
func (c *ConnSession) RotateToken(old, new string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != old {
		return false
	}
	c.token = new
	return true
}

// ---- 心跳（§5.2）----

// Touch 更新最后活动时间（读泵收到任意帧时调用）。
func (c *ConnSession) Touch() {
	c.lastActive.Store(time.Now().UnixNano())
}

func (c *ConnSession) LastActive() time.Time {
	return time.Unix(0, c.lastActive.Load())
}

// Alive 距上次活动是否未超过 keepAlive。
func (c *ConnSession) Alive(keepAlive time.Duration) bool {
	return time.Since(c.LastActive()) <= keepAlive
}
