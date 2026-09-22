// prometheus.go Prometheus 文本格式导出器（§11.2）。
//
// 输出格式遵循 Prometheus Exposition Format v0.0.4：
//
//	# HELP online_connections 在线连接数
//	# TYPE online_connections gauge
//	online_connections 42
//
//	# HELP handler_duration_ms 处理耗时
//	# TYPE handler_duration_ms histogram
//	handler_duration_ms_bucket{le="1"} 100
//	handler_duration_ms_bucket{le="+Inf"} 250
//	handler_duration_ms_sum 1500
//	handler_duration_ms_count 250
//
// 设计点：
//   - 不依赖 prometheus/client_golang，零外部依赖保证可测试性
//   - 指标名/标签口径与架构文档 §11.2 一致，后续接入 prometheus-adapter 直接对接
//   - 通过 HTTP Handler 注册到 admin server 的 /metrics 端点（§11.1）

package obs

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// MetricType Prometheus 指标类型。
type MetricType string

const (
	TypeCounter   MetricType = "counter"
	TypeGauge     MetricType = "gauge"
	TypeHistogram MetricType = "histogram"
)

// MetricMeta 指标元信息（HELP/TYPE 注释用）。
type MetricMeta struct {
	Help string
	Type MetricType
}

// PrometheusExporter Prometheus 文本格式导出器。
type PrometheusExporter struct {
	reg  *Registry
	meta map[string]MetricMeta
}

// NewPrometheusExporter 创建导出器。
func NewPrometheusExporter(reg *Registry) *PrometheusExporter {
	return &PrometheusExporter{
		reg:  reg,
		meta: make(map[string]MetricMeta),
	}
}

// Register 注册指标元信息；多次调用覆盖。
func (e *PrometheusExporter) Register(name, help string, t MetricType) {
	e.meta[name] = MetricMeta{Help: help, Type: t}
}

// PrometheusText 写入 Prometheus 文本格式到 w。
// 不命名为 WriteTo 以避免实现 io.WriterTo 接口触发 vet 的签名检查。
func (e *PrometheusExporter) PrometheusText(w io.Writer) (int, error) {
	e.reg.mu.RLock()
	defer e.reg.mu.RUnlock()

	// 按指标名排序输出
	names := make([]string, 0, len(e.reg.counters)+len(e.reg.gauges)+len(e.reg.histograms))
	for n := range e.reg.counters {
		names = append(names, n)
	}
	for n := range e.reg.gauges {
		names = append(names, n)
	}
	for n := range e.reg.histograms {
		names = append(names, n)
	}
	sort.Strings(names)

	total := 0
	for _, name := range names {
		n, err := fmt.Fprintf(w, "# HELP %s %s\n", name, e.metaHelp(name))
		total += n
		if err != nil {
			return total, err
		}
		n, err = fmt.Fprintf(w, "# TYPE %s %s\n", name, e.metaType(name))
		total += n
		if err != nil {
			return total, err
		}
		if c, ok := e.reg.counters[name]; ok {
			n, err = fmt.Fprintf(w, "%s %d\n", name, c.Value())
			total += n
			if err != nil {
				return total, err
			}
		} else if g, ok := e.reg.gauges[name]; ok {
			n, err = fmt.Fprintf(w, "%s %d\n", name, g.Value())
			total += n
			if err != nil {
				return total, err
			}
		} else if h, ok := e.reg.histograms[name]; ok {
			n, err = e.writeHistogram(w, name, h)
			total += n
			if err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// writeHistogram 输出 _bucket{le=...} _sum _count。
func (e *PrometheusExporter) writeHistogram(w io.Writer, name string, h *Histogram) (int, error) {
	total := 0
	buckets := h.Buckets()
	counts := h.Count()
	for i, b := range buckets {
		n, err := fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, b, counts[i])
		total += n
		if err != nil {
			return total, err
		}
	}
	// +Inf 桶
	n, err := fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, counts[len(counts)-1])
	total += n
	if err != nil {
		return total, err
	}
	n, err = fmt.Fprintf(w, "%s_sum %g\n", name, h.Sum())
	total += n
	if err != nil {
		return total, err
	}
	n, err = fmt.Fprintf(w, "%s_count %d\n", name, h.Total())
	total += n
	return total, err
}

func (e *PrometheusExporter) metaHelp(name string) string {
	if m, ok := e.meta[name]; ok && m.Help != "" {
		return m.Help
	}
	return name
}

func (e *PrometheusExporter) metaType(name string) string {
	if m, ok := e.meta[name]; ok && m.Type != "" {
		return string(m.Type)
	}
	// 按注册表类型推断
	if _, ok := e.reg.counters[name]; ok {
		return string(TypeCounter)
	}
	if _, ok := e.reg.gauges[name]; ok {
		return string(TypeGauge)
	}
	if _, ok := e.reg.histograms[name]; ok {
		return string(TypeHistogram)
	}
	return string(TypeGauge)
}

// String 返回完整的 Prometheus 文本格式（便于测试断言）。
func (e *PrometheusExporter) String() string {
	var sb strings.Builder
	_, _ = e.PrometheusText(&sb)
	return sb.String()
}
