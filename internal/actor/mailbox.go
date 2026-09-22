// Package actor 实现 Actor 引擎：每 Actor 一个 goroutine + 有界邮箱 +
// 批量 drain + Drop/Kick/Block 满箱策略 + panic 监督（架构文档 §7）。
package actor

import (
	"errors"

	"github.com/rangame/server/pkg/framework"
)

var (
	// ErrActorNotFound 目标 Actor 不存在（未创建或已停止）。
	ErrActorNotFound = errors.New("actor: target not found")
	// ErrDropped 邮箱已满且策略为 drop：本条消息被丢弃。
	ErrDropped = errors.New("actor: mailbox full, message dropped")
	// ErrKicked 邮箱已满且策略为 kick：Actor 已被停止。
	ErrKicked = errors.New("actor: mailbox full, actor kicked")
	// ErrShuttingDown 引擎正在关闭，不再接受 Block 投递。
	ErrShuttingDown = errors.New("actor: engine shutting down")
)

const (
	defaultCapacity   = 1024
	defaultDrainBatch = 64
	panicWindow       = 60 // 秒：滑动窗口
	maxPanicInWindow  = 3  // 窗口内连续 panic 上限 → 停 Actor（§7.4）
)

// normalizePolicy 填充默认值并校验策略名。
func normalizePolicy(p framework.MailboxPolicy) framework.MailboxPolicy {
	if p.Capacity <= 0 {
		p.Capacity = defaultCapacity
	}
	if p.DrainBatch <= 0 {
		p.DrainBatch = defaultDrainBatch
	}
	switch p.OnFull {
	case framework.FullDrop, framework.FullKick, framework.FullBlock:
	default:
		p.OnFull = framework.FullKick
	}
	return p
}

// mailbox 有界邮箱：一个带缓冲 channel，容量即背压水位（§7.2）。
type mailbox struct {
	ch     chan *framework.Envelope
	policy framework.MailboxPolicy
}

func newMailbox(p framework.MailboxPolicy) *mailbox {
	p = normalizePolicy(p)
	return &mailbox{
		ch:     make(chan *framework.Envelope, p.Capacity),
		policy: p,
	}
}

func (m *mailbox) Len() int { return len(m.ch) }

// Invoke 是一种特殊 Payload：在 Actor goroutine 内被直接调用而非转交 OnMessage。
// 路由器提交的 Handler 闭包、定时器回调都通过它回到 Actor 串行执行（§6.1/§9）。
type Invoke func(ctx framework.ActorCtx)
