// trace.go traceID 生成与透传（§11.1）。
//
//	traceID 在接入层中间件生成，随 Envelope 透传，跨 Actor 不丢失；
//	日志通过 logger.With("traceID", traceID) 自动携带；
//	下行帧不含 traceID（客户端不可见），仅服务端日志/指标用。
package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

// TraceKeyType 上下文 key 类型（避免与其他包冲突）。
type TraceKeyType struct{}

// TraceKey 上下文 key 用于存取 traceID。
var TraceKey = TraceKeyType{}

// GenerateTraceID 生成 16 字节十六进制 traceID（32 字符）。
//
//	演进态骨架：crypto/rand 生成随机数，无全局协调；
//	真实分布式部署可接入 OpenTelemetry traceparent 格式。
func GenerateTraceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// 极端情况下退化为时间戳 + 计数器
		return fallbackTraceID()
	}
	return hex.EncodeToString(b)
}

// fallbackTraceID 退化方案：时间戳 + 原子计数器。
var traceCounter uint64

func fallbackTraceID() string {
	ts := time.Now().UnixNano()
	c := atomic.AddUint64(&traceCounter, 1)
	return fmt.Sprintf("%016x%016x", ts, c)
}

// ContextWithTrace 把 traceID 注入 context。
func ContextWithTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceKey, traceID)
}

// TraceFromContext 从 context 取 traceID；不存在返回空串。
func TraceFromContext(ctx context.Context) string {
	v, _ := ctx.Value(TraceKey).(string)
	return v
}

// ShortTrace 返回 traceID 的前 8 字符（短形式，便于日志展示）。
func ShortTrace(traceID string) string {
	if len(traceID) <= 8 {
		return traceID
	}
	return traceID[:8]
}
