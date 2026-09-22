// Package timer 提供可注入时钟与最小堆定时器调度（架构文档 §9 / §15.4）。
//
// 设计要点：
//   - 全局单个调度 goroutine + container/heap 精确唤醒；
//   - 定时器回调不直接执行业务，默认在调度 goroutine 执行 fn；
//     阶段 3 接入 Actor 后将注入 Executor，把 fn 以 Tell 投递到属主 Actor 邮箱，
//     保证定时器逻辑与 Actor 其他消息串行（§9）。
package timer

import (
	"sync"
	"time"
)

// Clock 时钟抽象（§15.4：测试可替换为 ManualClock，无需 sleep）。
type Clock interface {
	Now() time.Time
	// After 返回 d 后触发的通道。
	After(d time.Duration) <-chan time.Time
}

// SystemClock 系统时钟。
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// ManualClock 手动时钟：Advance 前推时间并触发到期定时器，用于确定性单测。
type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	pending []manualTimer
}

type manualTimer struct {
	at time.Time
	ch chan time.Time
}

// NewManualClock 从 t 时刻创建手动时钟。
func NewManualClock(t time.Time) *ManualClock {
	return &ManualClock{now: t}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After 注册一个 d 后到期的定时器（与 time.After 语义一致，通道缓冲为 1）。
func (c *ManualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	c.pending = append(c.pending, manualTimer{at: c.now.Add(d), ch: ch})
	return ch
}

// Advance 把时钟前推 d，同步触发所有到期定时器。
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
	remaining := c.pending[:0]
	for _, t := range c.pending {
		if !t.at.After(c.now) {
			select {
			case t.ch <- c.now:
			default: // 缓冲已满，丢弃重复触发（与 time.Timer 单发语义一致）
			}
			continue
		}
		remaining = append(remaining, t)
	}
	c.pending = remaining
}
