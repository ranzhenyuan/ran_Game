package obs

import (
	"sort"
	"sync"
	"sync/atomic"
)

// 本文件是 §11.2 Prometheus 指标的进程内最小实现：
// 仅用标准库保证零外部依赖与可测试性；后续阶段加 promhttp 导出器时，
// 指标名/标签口径不变（online_connections / msg_qps / mailbox_len ...）。

// Counter 单调递增计数器。
type Counter struct {
	v atomic.Int64
}

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n int64)  { c.v.Add(n) }
func (c *Counter) Value() int64 { return c.v.Load() }

// Gauge 可增可减的瞬时值。
type Gauge struct {
	v atomic.Int64
}

func (g *Gauge) Set(n int64)  { g.v.Store(n) }
func (g *Gauge) Add(n int64)  { g.v.Add(n) }
func (g *Gauge) Inc()         { g.v.Add(1) }
func (g *Gauge) Dec()         { g.v.Add(-1) }
func (g *Gauge) Value() int64 { return g.v.Load() }

// Registry 指标注册表（指标在启动期注册，运行期只做原子读写，无需热注册加锁也可，
// 这里保留 RWMutex 以便 Snapshot 与注册并发安全）。
type Registry struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
}

func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
}

func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok {
		c = &Counter{}
		r.counters[name] = c
	}
	return c
}

func (r *Registry) Gauge(name string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.gauges[name]
	if !ok {
		g = &Gauge{}
		r.gauges[name] = g
	}
	return g
}

// Histogram 获取或创建直方图；buckets 为空时用默认桶。
func (r *Registry) Histogram(name string, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.histograms[name]
	if !ok {
		h = NewHistogram(buckets)
		r.histograms[name] = h
	}
	return h
}

// Snapshot 返回当前全部 Counter/Gauge 快照（供测试断言与未来的文本/Prometheus 导出）。
// Histogram 不计入 Snapshot，需通过 PrometheusText 单独导出。
func (r *Registry) Snapshot() map[string]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]int64, len(r.counters)+len(r.gauges))
	for name, c := range r.counters {
		out[name] = c.Value()
	}
	for name, g := range r.gauges {
		out[name] = g.Value()
	}
	return out
}

// SnapshotSorted 返回按名称排序的键值对，便于稳定输出/断言。
func (r *Registry) SnapshotSorted() []struct {
	Name  string
	Value int64
} {
	m := r.Snapshot()
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)

	out := make([]struct {
		Name  string
		Value int64
	}, 0, len(names))
	for _, n := range names {
		out = append(out, struct {
			Name  string
			Value int64
		}{n, m[n]})
	}
	return out
}
