// Package obs 提供框架横切的可观测能力：结构化日志、进程内指标、pprof。
// 对应架构文档 §11.1 / §11.2；Prometheus 导出器在后续阶段接入（指标采集面先行）。
package obs

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger 按 level 名称构造结构化 JSON logger。
// 必带业务字段（traceID/uid/roomID/msgID/actorID）由调用方在 handler 中 With 注入。
func NewLogger(level string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}

	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv})
	return slog.New(h)
}
