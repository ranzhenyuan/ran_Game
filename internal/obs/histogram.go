// histogram.go 直方图指标（§11.2 mailbox_len / handler_duration_ms）。
//
// 采用固定桶边界（默认按 P99 关注范围设计），无外部依赖；
// Prometheus 导出时按桶累计输出 _bucket{le="..."}。

package obs

import (
	"math"
	"sync"
	"sync/atomic"
)

// 默认桶边界（毫秒/个数），覆盖 P50-P99 关注范围。
var defaultBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// Histogram 固定桶直方图。
//
//	线程安全：桶计数用 atomic，总和/平方和用互斥。
//	桶边界在构造时固定，运行期不可改。
type Histogram struct {
	buckets []float64 // 升序排列的桶上界
	counts  []int64   // 每个桶的计数（含上界）
	sumMu   sync.Mutex
	sum     float64
	count   int64
}

// NewHistogram 创建直方图；buckets 为空时用 defaultBuckets。
func NewHistogram(buckets []float64) *Histogram {
	if len(buckets) == 0 {
		buckets = defaultBuckets
	}
	return &Histogram{
		buckets: buckets,
		counts:  make([]int64, len(buckets)+1), // +1 为 +Inf 桶
	}
}

// Observe 记录一个观测值。
func (h *Histogram) Observe(v float64) {
	// 找到第一个 >= v 的桶
	idx := len(h.buckets) // 默认 +Inf 桶
	for i, b := range h.buckets {
		if v <= b {
			idx = i
			break
		}
	}
	// 累加到 idx 及之后所有桶（Prometheus 累计语义）
	for i := idx; i < len(h.counts); i++ {
		atomic.AddInt64(&h.counts[i], 1)
	}
	h.sumMu.Lock()
	h.sum += v
	h.count++
	h.sumMu.Unlock()
}

// Buckets 返回桶边界（不含 +Inf）。
func (h *Histogram) Buckets() []float64 { return h.buckets }

// Count 返回各桶累计计数（含 +Inf 桶，长度 = len(buckets)+1）。
func (h *Histogram) Count() []int64 {
	out := make([]int64, len(h.counts))
	for i := range h.counts {
		out[i] = atomic.LoadInt64(&h.counts[i])
	}
	return out
}

// Sum 返回观测值总和。
func (h *Histogram) Sum() float64 {
	h.sumMu.Lock()
	defer h.sumMu.Unlock()
	return h.sum
}

// Total 返回观测总次数。
func (h *Histogram) Total() int64 {
	h.sumMu.Lock()
	defer h.sumMu.Unlock()
	return h.count
}

// Mean 平均值（无锁快照，可能有微小偏差，监控口径可接受）。
func (h *Histogram) Mean() float64 {
	h.sumMu.Lock()
	defer h.sumMu.Unlock()
	if h.count == 0 {
		return 0
	}
	return h.sum / float64(h.count)
}

// P99 近似 P99：找到累计计数 >= total*0.99 的最小桶。
func (h *Histogram) P99() float64 {
	total := h.Total()
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(float64(total) * 0.99))
	counts := h.Count()
	for i, c := range counts {
		if c >= target {
			if i < len(h.buckets) {
				return h.buckets[i]
			}
			return h.buckets[len(h.buckets)-1] // +Inf 桶返回最大边界
		}
	}
	return h.buckets[len(h.buckets)-1]
}
