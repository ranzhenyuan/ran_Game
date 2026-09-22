package transport

import (
	"sync"
	"time"
)

// TokenBucket 令牌桶限流器（架构文档 §3.3 / §6.3：per-conn 限流）。
// 惰性补充：不依赖后台 goroutine，每次 Allow 按距上次的经过时间补令牌。
type TokenBucket struct {
	mu       sync.Mutex
	rate     float64 // 每秒补充令牌数
	capacity float64
	tokens   float64
	last     time.Time
}

// NewTokenBucket 构造令牌桶。rate<=0 表示不限流（Allow 恒真）。
func NewTokenBucket(rate, capacity float64, now time.Time) *TokenBucket {
	return &TokenBucket{
		rate:     rate,
		capacity: capacity,
		tokens:   capacity,
		last:     now,
	}
}

// Allow 消耗 1 个令牌。
func (b *TokenBucket) Allow(now time.Time) bool {
	return b.AllowN(now, 1)
}

// AllowN 消耗 n 个令牌。
func (b *TokenBucket) AllowN(now time.Time, n float64) bool {
	if b.rate <= 0 {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}

	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}
