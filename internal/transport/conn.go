package transport

import (
	"context"
	"sync"
)

// Conn 屏蔽 TCP / WebSocket / 内部 node-to-node 差异（架构文档 §3.1 / §12.3）。
// 实现内置每连接 1 个写聚合协程；读循环由上层读泵驱动调用 Read。
type Conn interface {
	// ID 连接级唯一 ID（uint64，单调递增）。
	ID() uint64
	// Read 返回下一个完整帧（len 拆包已在实现内部完成）。
	Read(ctx context.Context) (*Frame, error)
	// Push 非阻塞投递已编码字节到写 channel；满时返回 ErrSlowConsumer（§3.3）。
	Push(data []byte) error
	// Close 关闭底层连接；重复调用安全。
	Close(reason string) error
	// RemoteAddr 对端地址。
	RemoteAddr() string
	// Meta 连接级元数据（握手 token、UA 等）。
	Meta() *sync.Map
}
