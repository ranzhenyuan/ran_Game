package redisstore_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/rangame/server/internal/storage"
	"github.com/rangame/server/internal/storage/redisstore"
	"github.com/rangame/server/pkg/framework"
)

func startRedis(t *testing.T) (*miniredis.Miniredis, *redisstore.Backend) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	b, err := redisstore.New(context.Background(), redisstore.Config{
		Addr: mr.Addr(), KeyPrefix: "test", MaxRetries: 0, DialTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return mr, b
}

type doc struct {
	Name string `json:"name"`
	V    int    `json:"v"`
}

func TestRedisKVAndCounter(t *testing.T) {
	_, b := startRedis(t)
	ctx := context.Background()

	if err := b.Put(ctx, "player", "p1", []byte(`{"name":"ran","v":1}`)); err != nil {
		t.Fatal(err)
	}
	raw, err := b.Get(ctx, "player", "p1")
	if err != nil || string(raw) != `{"name":"ran","v":1}` {
		t.Fatalf("get: %s %v", raw, err)
	}
	if _, err := b.Get(ctx, "player", "missing"); err != framework.ErrStorageNotFound {
		t.Fatalf("missing err = %v", err)
	}
	if err := b.Delete(ctx, "player", "p1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "player", "p1"); err != framework.ErrStorageNotFound {
		t.Fatal("delete should remove key")
	}

	v, err := b.IncrBy(ctx, "counter", "p1:win", 3)
	if err != nil || v != 3 {
		t.Fatalf("incr1 = %d %v", v, err)
	}
	v, err = b.IncrBy(ctx, "counter", "p1:win", 2)
	if err != nil || v != 5 {
		t.Fatalf("incr2 = %d %v", v, err)
	}
}

func TestRedisRanking(t *testing.T) {
	_, b := startRedis(t)
	ctx := context.Background()
	rk := b.Ranking()

	if err := rk.ZAdd(ctx, "score", "a", 10); err != nil {
		t.Fatal(err)
	}
	if err := rk.ZAdd(ctx, "score", "b", 30); err != nil {
		t.Fatal(err)
	}
	if v, err := rk.ZIncrBy(ctx, "score", "c", 20); err != nil || v != 20 {
		t.Fatalf("zincr = %d %v", v, err)
	}
	if v, err := rk.ZIncrBy(ctx, "score", "c", 5); err != nil || v != 25 {
		t.Fatalf("zincr2 = %d %v", v, err)
	}

	top, err := rk.ZRevRange(ctx, "score", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 3 || top[0].Member != "b" || top[1].Member != "c" || top[2].Member != "a" {
		t.Fatalf("desc order wrong: %+v", top)
	}
	// 正序排名：a=0 c=1 b=2。
	if n, err := rk.ZRank(ctx, "score", "c"); err != nil || n != 1 {
		t.Fatalf("zrank c = %d %v", n, err)
	}
	if _, err := rk.ZRank(ctx, "score", "nobody"); err != framework.ErrStorageNotFound {
		t.Fatalf("zrank missing = %v", err)
	}
}

// Redis 故障注入：停服 → 操作失败；Restart 后 Ping 恢复且数据保留。
func TestRedisOutageAndRecover(t *testing.T) {
	mr, b := startRedis(t)
	ctx := context.Background()

	if err := b.Put(ctx, "t", "keep", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	mr.Close()
	if err := b.Put(ctx, "t", "x", []byte(`{}`)); err == nil {
		t.Fatal("put must fail when redis down")
	}
	if err := b.Ping(ctx); err == nil {
		t.Fatal("ping must fail when redis down")
	}

	mr.Restart() // miniredis Restart 保留内存数据
	if err := b.Ping(ctx); err != nil {
		t.Fatalf("ping after restart: %v", err)
	}
	raw, err := b.Get(ctx, "t", "keep")
	if err != nil || string(raw) != `{"v":1}` {
		t.Fatalf("data after restart: %s %v", raw, err)
	}
}

// 与写回门面集成：异步 Set 最终落到 Redis，Get 走同步路径读回。
func TestRedisWithWriteBackStore(t *testing.T) {
	_, b := startRedis(t)
	s, err := storage.NewStore(b, storage.Options{
		QueueSize: 16, FlushInterval: 5 * time.Millisecond, MaxBatch: 16,
		SyncTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.Set(ctx, "snake_result", "room-1", doc{Name: "final", V: 42}); err != nil {
		t.Fatal(err)
	}
	var got doc
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.Get(ctx, "snake_result", "room-1", &got); err == nil {
			break
		}
		time.Sleep(3 * time.Millisecond)
	}
	if got.Name != "final" || got.V != 42 {
		t.Fatalf("async set not flushed to redis: %+v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
