package router

import (
	"sync"
	"time"

	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// WithMetrics 注入指标注册表：msg_in_total / msg_err_total / msg_rate_limited_total。
func WithMetrics(m *obs.Registry) Option {
	return func(r *Router) { r.metrics = m }
}

// rateBuckets 每 UID 一个令牌桶（读泵并发访问，mutex 保护 map）。
type rateBuckets struct {
	mu      sync.Mutex
	buckets map[string]*transport.TokenBucket
}

func newRateBuckets() *rateBuckets {
	return &rateBuckets{buckets: make(map[string]*transport.TokenBucket)}
}

func (b *rateBuckets) allow(uid string, now time.Time, rate, burst float64) bool {
	b.mu.Lock()
	tb, ok := b.buckets[uid]
	if !ok {
		tb = transport.NewTokenBucket(rate, burst, now)
		b.buckets[uid] = tb
	}
	b.mu.Unlock()
	return tb.Allow(now)
}

// DeleteBucket 会话销毁时清理（阶段 4 调用，避免 map 无限增长）。
func (b *rateBuckets) DeleteBucket(uid string) {
	b.mu.Lock()
	delete(b.buckets, uid)
	b.mu.Unlock()
}

// authMiddleware 鉴权：免鉴权 msgID（Ping/Login/Reconnect）直通；
// 其余要求 Source 实现 Authenticator 且已登录（§11.4 基线）。
func authMiddleware(r *Router) Middleware {
	return func(next framework.HandlerFunc) framework.HandlerFunc {
		return func(ctx framework.MsgCtx, req any) (any, error) {
			if !r.authFree[ctx.MsgID()] {
				// 通过 msgCtx 持有的 src 做能力断言。
				if src := sourceOf(ctx); src != nil {
					a, ok := src.(Authenticator)
					if !ok || !a.Authenticated() {
						return nil, framework.NewErr(framework.ErrUnauthorized, "")
					}
				}
			}
			return next(ctx, req)
		}
	}
}

// sourceOf 从 MsgCtx 取回 Source（仅路由器内部使用）。
func sourceOf(ctx framework.MsgCtx) Source {
	if mc, ok := ctx.(*msgCtx); ok {
		return mc.src
	}
	return nil
}

// rateLimitMiddleware 每 UID 令牌桶限流，拒绝码 2003（§3.3）。
func rateLimitMiddleware(r *Router) Middleware {
	return func(next framework.HandlerFunc) framework.HandlerFunc {
		return func(ctx framework.MsgCtx, req any) (any, error) {
			if r.ratePerSec > 0 && !r.buckets.allow(ctx.UID(), r.nowFunc(), r.ratePerSec, r.rateBurst) {
				if r.metrics != nil {
					r.metrics.Counter("msg_rate_limited_total").Inc()
				}
				return nil, framework.NewErr(framework.ErrRateLimited, "")
			}
			return next(ctx, req)
		}
	}
}

// recoverMiddleware Actor 段兜底：Handler panic → 4001，不拖垮 Actor goroutine。
// 引擎层另有 panic 滑窗监督（§7.4），本中间件保证单次 panic 不升级为 Actor 死亡。
func recoverMiddleware() Middleware {
	return func(next framework.HandlerFunc) framework.HandlerFunc {
		return func(ctx framework.MsgCtx, req any) (resp any, err error) {
			defer func() {
				if rec := recover(); rec != nil {
					err = framework.NewErr(framework.ErrInternal, "handler panic")
				}
			}()
			return next(ctx, req)
		}
	}
}

// traceMiddleware traceID 已在 Dispatch 入口生成并放入 Envelope；
// 此处保留位置以便后续把 traceID 注入日志上下文（§6.3 链路追踪）。
func traceMiddleware() Middleware {
	return func(next framework.HandlerFunc) framework.HandlerFunc {
		return func(ctx framework.MsgCtx, req any) (any, error) {
			return next(ctx, req)
		}
	}
}

// metricMiddleware 消息处理计数（延迟直方图在接入 Prometheus 阶段补充）。
func metricMiddleware(r *Router) Middleware {
	return func(next framework.HandlerFunc) framework.HandlerFunc {
		return func(ctx framework.MsgCtx, req any) (any, error) {
			if r.metrics != nil {
				r.metrics.Counter("msg_in_total").Inc()
			}
			resp, err := next(ctx, req)
			if err != nil && r.metrics != nil {
				r.metrics.Counter("msg_err_total").Inc()
			}
			return resp, err
		}
	}
}
