package storage

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/pkg/framework"
)

// Options Store 门面配置（零值可用，缺省在 NewStore 内填充）。
type Options struct {
	QueueSize       int           // 有界写回队列容量，默认 4096
	FlushInterval   time.Duration // 批量 flush 间隔，默认 10ms
	MaxBatch        int           // 单次 flush 上限，默认 256
	MaxRetry        int           // 单批失败重试次数，默认 2
	SyncTimeout     time.Duration // 同步路径（Get/SetSync/IncrBy）超时，默认 2s
	BreakerCooldown time.Duration // 熔断冷却→半开探测，默认 5s
	WAL             *WAL          // 本地 WAL（nil=禁用，溢出直接死信+熔断）
	Metrics         *obs.Registry
	Logger          *slog.Logger
	Now             func() time.Time
}

// Store framework.Storage 的远程实现门面：
//
//	业务 Set/Del ──► 有界队列 ──► 后台批量 flush ──► Backend(Redis/MySQL)
//	                     溢出            重试失败
//	                      ▼               ▼
//	                    WAL 追加 ◄────────┘
//	                      │ WAL 失败
//	                      ▼
//	            死信日志 + 按 table 熔断（4003）─冷却─► 半开 Ping 探测 ─► 恢复
//
// Actor 调用路径零 IO 等待（§10.2）；Get/SetSync/IncrBy 为显式同步路径，直达 Backend。
type Store struct {
	backend Backend
	opts    Options
	logger  *slog.Logger

	ch      chan walRecord
	breaker *tableBreaker

	closing atomic.Bool
	wg      sync.WaitGroup
	closeCh chan struct{}

	metrics *obs.Registry
}

// NewStore 创建门面并启动 flush worker；构造时尝试重放历史 WAL。
func NewStore(backend Backend, opts Options) (*Store, error) {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 4096
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 10 * time.Millisecond
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 256
	}
	if opts.MaxRetry < 0 {
		opts.MaxRetry = 2
	}
	if opts.SyncTimeout <= 0 {
		opts.SyncTimeout = 2 * time.Second
	}
	if opts.BreakerCooldown <= 0 {
		opts.BreakerCooldown = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	s := &Store{
		backend: backend,
		opts:    opts,
		logger:  opts.Logger,
		ch:      make(chan walRecord, opts.QueueSize),
		breaker: newTableBreaker(opts.BreakerCooldown, opts.Now),
		closeCh: make(chan struct{}),
		metrics: opts.Metrics,
	}
	s.wg.Add(1)
	go s.flushLoop()
	return s, nil
}

// Backend 暴露底层后端（测试/管理用）。
func (s *Store) Backend() Backend { return s.backend }

// ---- framework.Storage 实现 ----

// Get 同步直达（读不经过写回队列）。
func (s *Store) Get(ctx context.Context, table, key string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.SyncTimeout)
	defer cancel()
	raw, err := s.backend.Get(ctx, table, key)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// Set 异步入写回队列；熔断中返回 4003，Actor 不阻塞等 IO（§10.2）。
func (s *Store) Set(_ context.Context, table, key string, val any) error {
	raw, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return s.enqueue(walRecord{TS: s.opts.Now().UnixNano(), Table: table, Key: key, Value: raw})
}

// SetSync 强一致同步路径：直达后端、绕过队列与熔断（业务显式选择，§10.2）。
func (s *Store) SetSync(ctx context.Context, table, key string, val any) error {
	raw, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, s.opts.SyncTimeout)
	defer cancel()
	return s.backend.Put(ctx, table, key, raw)
}

// Del 异步入写回队列（删除同样可能在 flush 时失败，走同一套兜底）。
func (s *Store) Del(_ context.Context, table, key string) error {
	return s.enqueue(walRecord{TS: s.opts.Now().UnixNano(), Table: table, Key: key, Deleted: true})
}

// IncrBy 同步直达（货币类原子操作不可异步，§10.2）。
func (s *Store) IncrBy(ctx context.Context, table, key string, delta int64) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.SyncTimeout)
	defer cancel()
	return s.backend.IncrBy(ctx, table, key, delta)
}

// Ranking 排行榜子接口（同步语义；写失败由调用方/死信路径处理）。
func (s *Store) Ranking() framework.Ranking { return s.backend.Ranking() }

// Raw 逃生口：绕过写回与熔断，业务自负（§10.2）。
func (s *Store) Raw() any { return s.backend.Raw() }

// enqueue 入队；队列满走 WAL，WAL 失败死信 + 熔断。
func (s *Store) enqueue(rec walRecord) error {
	if s.closing.Load() {
		return framework.NewErr(framework.ErrMaintenance, "storage shutting down")
	}
	if !s.breaker.Allow(rec.Table) {
		return framework.NewErr(framework.ErrStorageDegraded, "breaker open for table "+rec.Table)
	}
	select {
	case s.ch <- rec:
		s.gaugeQueue()
		return nil
	default:
		return s.spill(rec)
	}
}

// spill 队列溢出的二级降级：WAL 追加；WAL 不可用则死信 + 熔断。
func (s *Store) spill(rec walRecord) error {
	s.counter("storage_writeback_spill_total").Inc()
	if s.opts.WAL != nil {
		if err := s.opts.WAL.Append(rec); err != nil {
			s.logger.Error("wal append failed", "table", rec.Table, "key", rec.Key, "err", err)
		} else {
			s.logger.Warn("writeback queue full, spilled to WAL",
				"table", rec.Table, "key", rec.Key, "queue", s.opts.QueueSize)
			return nil
		}
	}
	s.deadLetter(rec, errors.New("writeback queue full and WAL unavailable"))
	s.breaker.Trip(rec.Table)
	s.counter("storage_breaker_trip_total").Inc()
	return framework.NewErr(framework.ErrStorageDegraded, "writeback overflow for table "+rec.Table)
}

func (s *Store) deadLetter(rec walRecord, cause error) {
	s.counter("storage_deadletter_total").Inc()
	s.logger.Error("storage dead letter",
		"table", rec.Table, "key", rec.Key, "deleted", rec.Deleted,
		"bytes", len(rec.Value), "cause", cause.Error())
}

// ---- flush worker ----

func (s *Store) flushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.flushBatch()
			// 后端可能在本轮恢复：每个 tick 顺带尝试重放 WAL（内部有失败保护）。
			s.tryReplayWAL()
		}
	}
}

// flushBatch 非阻塞取一批（取到即处理，不等攒满），写入后端。
func (s *Store) flushBatch() {
	rec := s.drainOne()
	if rec == nil {
		return
	}
	batch := make([]walRecord, 0, s.opts.MaxBatch)
	batch = append(batch, *rec)
	for len(batch) < s.opts.MaxBatch {
		if r := s.drainOne(); r != nil {
			batch = append(batch, *r)
		} else {
			break
		}
	}
	s.gaugeQueue()
	s.writeBatch(batch)
}

func (s *Store) drainOne() *walRecord {
	select {
	case r := <-s.ch:
		return &r
	default:
		return nil
	}
}

// writeBatch 整批重试写入；最终失败落 WAL，WAL 也失败则死信 + 熔断对应 table。
func (s *Store) writeBatch(batch []walRecord) {
	ctx := context.Background()
	var lastErr error
	for attempt := 0; attempt <= s.opts.MaxRetry; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*attempt) * 10 * time.Millisecond) // 10ms,40ms
		}
		var failed []walRecord
		for _, rec := range batch {
			if err := s.apply(ctx, rec); err != nil {
				lastErr = err
				failed = append(failed, rec)
			}
		}
		batch = failed
		if len(batch) == 0 {
			return
		}
	}

	// 重试耗尽：half-open 探测中的 table 重新打开（closed 态不受影响，仍靠 WAL 兜底）。
	reopened := make(map[string]bool)
	for _, rec := range batch {
		if !reopened[rec.Table] {
			s.breaker.OnFailure(rec.Table)
			reopened[rec.Table] = true
		}
	}

	// 逐条落 WAL（隔离单条坏记录），WAL 失败者死信 + 强制熔断。
	tripped := make(map[string]bool)
	for _, rec := range batch {
		if s.opts.WAL != nil {
			if err := s.opts.WAL.Append(rec); err == nil {
				continue
			}
		}
		s.deadLetter(rec, lastErr)
		if !tripped[rec.Table] {
			s.breaker.Trip(rec.Table)
			s.counter("storage_breaker_trip_total").Inc()
			tripped[rec.Table] = true
		}
	}
}

// apply 单条写后端；成功同时清除该 table 的熔断（half-open 探测成功路径）。
// 失败不在此打开熔断——普通故障仍有 WAL 兜底，见 writeBatch。
func (s *Store) apply(ctx context.Context, rec walRecord) error {
	var err error
	if rec.Deleted {
		err = s.backend.Delete(ctx, rec.Table, rec.Key)
	} else {
		err = s.backend.Put(ctx, rec.Table, rec.Key, rec.Value)
	}
	if err != nil {
		return err
	}
	s.breaker.OnSuccess(rec.Table)
	return nil
}

// tryReplayWAL 后端恢复探测：尝试把 WAL 全部补写并清空。
// 半开/关闭态都安全——失败只是保留文件等下轮，不改变熔断器的拒绝决策
// （WAL 路径本身就是"接受写入但不触达后端"的缓冲）。
func (s *Store) tryReplayWAL() {
	if s.opts.WAL == nil {
		return
	}
	n, err := s.opts.WAL.Replay(func(rec walRecord) error {
		return s.apply(context.Background(), rec)
	})
	if err != nil {
		s.logger.Warn("wal replay incomplete", "records", n, "err", err)
		return
	}
	if n > 0 {
		s.logger.Info("wal replay succeeded", "records", n)
	}
}

// Close 停机：标记 closing → 停 worker → 排空队列 → 重放 WAL → 关后端（§11.3）。
// ctx 超时后未写完的数据保留在队列/ WAL 中（下次启动继续重放）。
func (s *Store) Close() error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	close(s.closeCh)
	s.wg.Wait()

	// 排空残余队列（带 ctx 上界由调用方在 Shutdown 控制；此处限时 syncTimeout*32 上限避免无限等）。
	drainCtx, cancel := context.WithTimeout(context.Background(), s.opts.SyncTimeout*32)
	defer cancel()
	for {
		rec := s.drainOne()
		if rec == nil {
			break
		}
		if err := s.apply(drainCtx, *rec); err != nil {
			if s.opts.WAL != nil {
				_ = s.opts.WAL.Append(*rec)
			} else {
				s.deadLetter(*rec, err)
			}
		}
		if drainCtx.Err() != nil {
			break
		}
	}
	s.tryReplayWAL()
	if s.opts.WAL != nil {
		_ = s.opts.WAL.Close()
	}
	return s.backend.Close()
}

func (s *Store) counter(name string) *obs.Counter {
	if s.metrics == nil {
		return nopCounter
	}
	return s.metrics.Counter(name)
}

func (s *Store) gaugeQueue() {
	if s.metrics != nil {
		s.metrics.Gauge("storage_writeback_queue_depth").Set(int64(len(s.ch)))
	}
}

// nopCounter 未注入 Metrics 时的空计数器。
var nopCounter = &obs.Counter{}
