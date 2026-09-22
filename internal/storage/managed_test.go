package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rangame/server/pkg/framework"
)

func jsonUnmarshal(raw []byte, out any) error { return json.Unmarshal(raw, out) }

// ---- 可故障注入的假后端 ----

type fakeBackend struct {
	mu      sync.Mutex
	failing bool
	pingErr error
	data    map[string]map[string][]byte
	counts  map[string]map[string]int64
	puts    int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		data:   make(map[string]map[string][]byte),
		counts: make(map[string]map[string]int64),
	}
}

func (b *fakeBackend) setFailing(v bool) { b.mu.Lock(); b.failing = v; b.mu.Unlock() }

func (b *fakeBackend) Get(_ context.Context, table, key string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failing {
		return nil, errors.New("backend down")
	}
	if row, ok := b.data[table]; ok {
		if v, ok := row[key]; ok {
			return v, nil
		}
	}
	return nil, framework.ErrStorageNotFound
}

func (b *fakeBackend) Put(_ context.Context, table, key string, raw []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failing {
		return errors.New("backend down")
	}
	row, ok := b.data[table]
	if !ok {
		row = make(map[string][]byte)
		b.data[table] = row
	}
	row[key] = append([]byte(nil), raw...)
	b.puts++
	return nil
}

func (b *fakeBackend) Delete(_ context.Context, table, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failing {
		return errors.New("backend down")
	}
	delete(b.data[table], key)
	return nil
}

func (b *fakeBackend) IncrBy(_ context.Context, table, key string, delta int64) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failing {
		return 0, errors.New("backend down")
	}
	row, ok := b.counts[table]
	if !ok {
		row = make(map[string]int64)
		b.counts[table] = row
	}
	row[key] += delta
	return row[key], nil
}

func (b *fakeBackend) Ranking() framework.Ranking { return &fakeRanking{b: b} }
func (b *fakeBackend) Ping(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failing {
		return errors.New("backend down")
	}
	return b.pingErr
}
func (b *fakeBackend) Raw() any     { return b }
func (b *fakeBackend) Close() error { return nil }

func (b *fakeBackend) waitPuts(n int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		got := b.puts
		b.mu.Unlock()
		if got >= n {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// 测试快速构造参数。
func testOpts(t *testing.T, queue int, walDir string) Options {
	t.Helper()
	return Options{
		QueueSize:       queue,
		FlushInterval:   5 * time.Millisecond,
		MaxBatch:        64,
		MaxRetry:        1,
		SyncTimeout:     time.Second,
		BreakerCooldown: 80 * time.Millisecond,
		WAL:             openWALOrNil(t, walDir),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func openWALOrNil(t *testing.T, dir string) *WAL {
	t.Helper()
	if dir == "" {
		return nil
	}
	w, err := OpenWAL(filepath.Join(t.TempDir(), dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() }) // 幂等：Store.Close 已关时为空操作
	return w
}

type doc struct {
	Name string `json:"name"`
	V    int    `json:"v"`
}

// 1) 正常路径：Set 异步入队 → 批量 flush → Get 可读回；SetSync 直达。
func TestWriteBackNormalFlush(t *testing.T) {
	b := newFakeBackend()
	opts := testOpts(t, 16, "wal")
	s, err := NewStore(b, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := s.Set(ctx, "result", "r1", doc{Name: "a", V: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, "result", "r2", doc{Name: "b", V: 2}); err != nil {
		t.Fatal(err)
	}
	if !b.waitPuts(2) {
		t.Fatal("flush timeout")
	}
	var got doc
	if err := s.Get(ctx, "result", "r2", &got); err != nil || got.Name != "b" || got.V != 2 {
		t.Fatalf("get after flush: %+v %v", got, err)
	}
	// SetSync 直达。
	if err := s.SetSync(ctx, "result", "r3", doc{Name: "c", V: 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.Get(ctx, "result", "r3", &got); err != nil || got.Name != "c" {
		t.Fatalf("setsync get: %+v %v", got, err)
	}
	// IncrBy 同步原子计数。
	if v, err := s.IncrBy(ctx, "counter", "p1:win", 2); err != nil || v != 2 {
		t.Fatalf("incr = %d %v", v, err)
	}
	// Del 异步生效。
	if err := s.Del(ctx, "result", "r1"); err != nil {
		t.Fatal(err)
	}
	waitTrue(t, func() bool {
		return errors.Is(s.Get(ctx, "result", "r1", &doc{}), framework.ErrStorageNotFound)
	}, "delete flushed")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// 2) 后端故障 → 队列溢出落 WAL → 后端恢复 → 队列+WAL 全部补写。
func TestOverflowSpillToWALThenReplay(t *testing.T) {
	b := newFakeBackend()
	b.setFailing(true)
	opts := testOpts(t, 2, "wal")
	s, err := NewStore(b, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 队列容量 2：前 2 条入队（随后 flush 失败转入 WAL），后 3 条溢出直接进 WAL。
	for i := 0; i < 5; i++ {
		if err := s.Set(ctx, "result", keyN(i), doc{V: i}); err != nil {
			t.Fatalf("set %d with WAL spillback should be accepted: %v", i, err)
		}
	}

	// 恢复后端：flush 重试 + 每 tick 的 WAL 重放应最终补齐全部 5 条。
	b.setFailing(false)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for i := 0; i < 5; i++ {
			var d doc
			if err := s.Get(ctx, "result", keyN(i), &d); err != nil || d.V != i {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		var d doc
		if err := s.Get(ctx, "result", keyN(i), &d); err != nil {
			t.Fatalf("key %d not recovered: %v", i, err)
		}
		if d.V != i {
			t.Fatalf("key %d wrong value %d", i, d.V)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// 3) WAL 也不可用：溢出即死信 + 熔断（4003）→ 冷却后半开探测 → 恢复。
func TestBreakerOpenThenHalfOpenRecover(t *testing.T) {
	b := newFakeBackend()
	b.setFailing(true)
	opts := testOpts(t, 1, "") // 无 WAL
	s, err := NewStore(b, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 第 1 条占满队列；第 2 条溢出 → 无 WAL → 死信 + 熔断，返回 4003。
	if err := s.Set(ctx, "t", "k1", doc{V: 1}); err != nil {
		t.Fatal(err)
	}
	err = s.Set(ctx, "t", "k2", doc{V: 2})
	if ec, ok := err.(*framework.ErrCode); !ok || ec.Code != framework.ErrStorageDegraded {
		t.Fatalf("overflow without WAL should return 4003, got %v", err)
	}
	// 熔断打开：后续写入立即拒绝（不再试探队列）。
	if err := s.Set(ctx, "t", "k3", doc{V: 3}); err == nil {
		t.Fatal("breaker open must reject writes")
	}

	// 后端恢复；等冷却到期 → 半开放行一个探测 → flush 成功 → 熔断关闭。
	b.setFailing(false)
	waitTrue(t, func() bool {
		return s.Set(ctx, "t", "k4", doc{V: 4}) == nil
	}, "breaker recovered after half-open probe")

	// 恢复后写入持续可用并落盘。
	if err := s.SetSync(ctx, "t", "k5", doc{V: 5}); err != nil {
		t.Fatal(err)
	}
	var d doc
	if err := s.Get(ctx, "t", "k5", &d); err != nil || d.V != 5 {
		t.Fatalf("post-recovery get: %+v %v", d, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// 4) SetSync 绕过熔断：熔断中仍直达后端（成功则成功、失败是后端错误而非 4003）。
func TestSetSyncBypassesBreaker(t *testing.T) {
	b := newFakeBackend()
	b.setFailing(true)
	opts := testOpts(t, 1, "")
	s, _ := NewStore(b, opts)
	ctx := context.Background()

	_ = s.Set(ctx, "t", "k1", doc{V: 1})
	_ = s.Set(ctx, "t", "overflow", doc{V: 9}) // 触发熔断

	// 后端仍挂：SetSync 得到后端原始错误，不是 4003。
	if err := s.SetSync(ctx, "t", "money", doc{V: 100}); err == nil {
		t.Fatal("setsync should hit failing backend")
	}
	// 后端恢复：即使熔断仍 open，SetSync 也能直达成功。
	b.setFailing(false)
	if err := s.SetSync(ctx, "t", "money", doc{V: 100}); err != nil {
		t.Fatalf("setsync bypasses breaker: %v", err)
	}
	_ = s.Close()
}

// 5) Close 排空：未等 flush tick 的在队数据也要落盘。
func TestCloseDrainsQueue(t *testing.T) {
	b := newFakeBackend()
	opts := Options{
		QueueSize: 64, FlushInterval: time.Hour, // 故意不触发周期 flush
		MaxBatch: 64, MaxRetry: 0, SyncTimeout: time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s, _ := NewStore(b, opts)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := s.Set(ctx, "t", keyN(i), doc{V: i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		var d doc
		raw, err := b.Get(ctx, "t", keyN(i))
		if err != nil {
			t.Fatalf("key %d missing after close: %v", i, err)
		}
		_ = jsonUnmarshal(raw, &d)
		if d.V != i {
			t.Fatalf("key %d = %d", i, d.V)
		}
	}
}

// ---- WAL 直测 ----

func TestWALAppendReplayTruncate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	recs := []walRecord{
		{TS: 1, Table: "t", Key: "a", Value: []byte(`{"v":1}`)},
		{TS: 2, Table: "t", Key: "b", Deleted: true},
	}
	for _, r := range recs {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	var got []walRecord
	n, err := w.Replay(func(r walRecord) error { got = append(got, r); return nil })
	if err != nil || n != 2 {
		t.Fatalf("replay n=%d err=%v", n, err)
	}
	if got[0].Key != "a" || string(got[0].Value) != `{"v":1}` || !got[1].Deleted {
		t.Fatalf("replay records wrong: %+v", got)
	}
	// 成功后已截断：再次 replay 为 0。
	n2, err := w.Replay(func(walRecord) error { t.Fatal("should be empty"); return nil })
	if err != nil || n2 != 0 {
		t.Fatalf("wal should be truncated: n=%d err=%v", n2, err)
	}
	_ = w.Close()
}

// ---- 熔断器直测 ----

func TestBreakerStateMachine(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1000, 0)}
	br := newTableBreaker(50*time.Millisecond, clock.now)
	if !br.Allow("t") {
		t.Fatal("closed should allow")
	}
	br.Trip("t")
	if br.Allow("t") {
		t.Fatal("open should reject")
	}
	// 冷却未到：仍拒绝。
	clock.advance(40 * time.Millisecond)
	if br.Allow("t") {
		t.Fatal("should stay open within cooldown")
	}
	// 冷却到：half-open 只放行一次探测。
	clock.advance(20 * time.Millisecond)
	if !br.Allow("t") {
		t.Fatal("half-open should allow one probe")
	}
	if br.Allow("t") {
		t.Fatal("half-open must reject concurrent second probe")
	}
	// 探测失败 → 重新 open 并重置冷却。
	br.OnFailure("t")
	if br.Allow("t") {
		t.Fatal("re-opened after failed probe")
	}
	clock.advance(60 * time.Millisecond)
	if !br.Allow("t") {
		t.Fatal("probe allowed again after cooldown")
	}
	br.OnSuccess("t")
	if !br.Allow("t") {
		t.Fatal("closed after successful probe")
	}
}

// ---- 假排行榜（保证 Backend 接口完整）----

type fakeRanking struct{ b *fakeBackend }

func (r *fakeRanking) ZAdd(_ context.Context, _, member string, score int64) error {
	return nil
}
func (r *fakeRanking) ZIncrBy(_ context.Context, _, member string, delta int64) (int64, error) {
	return delta, nil
}
func (r *fakeRanking) ZRevRange(_ context.Context, _ string, start, stop int64) ([]framework.RankItem, error) {
	return nil, nil
}
func (r *fakeRanking) ZRank(_ context.Context, _, _ string) (int64, error) { return 0, nil }

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func keyN(i int) string { return "k" + strconv.Itoa(i) }

func waitTrue(t *testing.T, fn func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("wait %s timeout", what)
}
