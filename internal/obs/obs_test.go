// obs_test.go 可观测性测试（架构文档 §11.2 指标 + traceID + Prometheus 导出）。

package obs

import (
	"context"
	"strings"
	"testing"
)

// TestHistogram_ObserveAndP99 直方图观测与 P99 近似。
func TestHistogram_ObserveAndP99(t *testing.T) {
	h := NewHistogram([]float64{1, 5, 10, 25, 50, 100})
	// 100 个 1ms 的样本
	for i := 0; i < 100; i++ {
		h.Observe(1)
	}
	// 5 个 60ms 的样本（P99 应在 100ms 桶）
	for i := 0; i < 5; i++ {
		h.Observe(60)
	}
	if h.Total() != 105 {
		t.Fatalf("total: want 105, got %d", h.Total())
	}
	if p99 := h.P99(); p99 != 100 {
		t.Fatalf("P99: want 100, got %v", p99)
	}
}

// TestPrometheusExporter_TextFormat 验证导出文本格式。
func TestPrometheusExporter_TextFormat(t *testing.T) {
	reg := NewRegistry()
	exporter := NewPrometheusExporter(reg)
	std := RegisterStandard(reg, exporter)

	// 写入一些指标值
	std.OnlineConnections.Set(42)
	std.MsgQPSUp.Add(100)
	std.HandlerDurationMS.Observe(5)

	out := exporter.String()
	// 必须包含 HELP/TYPE/值
	if !strings.Contains(out, "# HELP online_connections 在线连接数") {
		t.Fatalf("missing HELP for online_connections:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE online_connections gauge") {
		t.Fatalf("missing TYPE for online_connections:\n%s", out)
	}
	if !strings.Contains(out, "online_connections 42") {
		t.Fatalf("missing value for online_connections:\n%s", out)
	}
	if !strings.Contains(out, "msg_qps_up 100") {
		t.Fatalf("missing value for msg_qps_up:\n%s", out)
	}
	// histogram 应有 _bucket{le=...} / _sum / _count
	if !strings.Contains(out, "handler_duration_ms_bucket{le=\"5\"}") {
		t.Fatalf("missing bucket for handler_duration_ms:\n%s", out)
	}
	if !strings.Contains(out, "handler_duration_ms_sum 5") {
		t.Fatalf("missing sum for handler_duration_ms:\n%s", out)
	}
	if !strings.Contains(out, "handler_duration_ms_count 1") {
		t.Fatalf("missing count for handler_duration_ms:\n%s", out)
	}
}

// TestTraceID_GenerateAndContext traceID 生成 + context 透传。
func TestTraceID_GenerateAndContext(t *testing.T) {
	id1 := GenerateTraceID()
	id2 := GenerateTraceID()
	if len(id1) != 32 || len(id2) != 32 {
		t.Fatalf("traceID length: want 32, got %d / %d", len(id1), len(id2))
	}
	if id1 == id2 {
		t.Fatalf("two traceIDs are identical: %s", id1)
	}
	if ShortTrace(id1) != id1[:8] {
		t.Fatalf("ShortTrace: got %q want %q", ShortTrace(id1), id1[:8])
	}

	ctx := context.Background()
	ctxWithTrace := ContextWithTrace(ctx, id1)
	got := TraceFromContext(ctxWithTrace)
	if got != id1 {
		t.Fatalf("TraceFromContext: got %q want %q", got, id1)
	}
	// 空 context 返回空串
	if TraceFromContext(ctx) != "" {
		t.Fatalf("empty ctx should return empty traceID")
	}
}
