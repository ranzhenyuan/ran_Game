package storage

import (
	"sync"
	"time"
)

// breakerState 熔断三态（§10.3）。
type breakerState byte

const (
	breakerClosed   breakerState = iota // 正常放行
	breakerOpen                         // 拒绝该 table 的异步写
	breakerHalfOpen                     // 冷却到期：只放行一个探测请求
)

// tableBreaker 按 table 粒度的熔断器。
// 后端故障通常是实例级的，table 粒度用于让不同表独立降级
// （如排行榜表故障不影响对局记录表），并防止内存无界增长。
type tableBreaker struct {
	cooldown time.Duration
	now      func() time.Time

	mu      sync.Mutex
	states  map[string]breakerState
	openAt  map[string]time.Time
	tripped chan string // 状态变更信号（测试观测用，可为 nil）
}

func newTableBreaker(cooldown time.Duration, now func() time.Time) *tableBreaker {
	if now == nil {
		now = time.Now
	}
	return &tableBreaker{
		cooldown: cooldown,
		now:      now,
		states:   make(map[string]breakerState),
		openAt:   make(map[string]time.Time),
	}
}

// Allow 是否允许向该 table 异步写入。
// open 且冷却未到 → false；open 且冷却已到 → 切 half-open，本次放行作为探测。
func (b *tableBreaker) Allow(table string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.states[table] {
	case breakerClosed:
		return true // map 缺省零值也是 closed，同走此分支
	case breakerHalfOpen:
		return false // 探测请求已放行，等待结果
	default: // open
		if b.now().Sub(b.openAt[table]) >= b.cooldown {
			b.states[table] = breakerHalfOpen
			return true
		}
		return false
	}
}

// OnFailure flush 失败反馈：仅在半开探测态重新打开；closed 态不动作
// （此时仍有 WAL 兜底，写入应继续被接受，熔断只在 WAL 也失败时由 Trip 打开）。
func (b *tableBreaker) OnFailure(table string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states[table] == breakerHalfOpen {
		b.states[table] = breakerOpen
		b.openAt[table] = b.now()
	}
}

// Trip 强制熔断（走投无路：队列溢出且 WAL 不可用/写失败）。
func (b *tableBreaker) Trip(table string) {
	b.mu.Lock()
	b.states[table] = breakerOpen
	b.openAt[table] = b.now()
	b.mu.Unlock()
	if b.tripped != nil {
		select {
		case b.tripped <- table:
		default:
		}
	}
}

// OnSuccess flush/探测成功 → 关闭熔断。
func (b *tableBreaker) OnSuccess(table string) {
	b.mu.Lock()
	delete(b.states, table)
	delete(b.openAt, table)
	b.mu.Unlock()
}

// State 查询当前状态（观测用）。
func (b *tableBreaker) State(table string) breakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.states[table]
}
