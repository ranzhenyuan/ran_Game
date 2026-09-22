package app_test

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rangame/server/internal/app"
	"github.com/rangame/server/internal/config"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func storageCfg(t *testing.T, driver, addr string) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Storage.Driver = driver
	cfg.Storage.RedisAddr = addr
	cfg.Storage.WALPath = filepath.Join(t.TempDir(), "wal")
	cfg.Storage.SyncTimeout = config.Duration(500 * time.Millisecond)
	cfg.Storage.FlushInterval = config.Duration(5 * time.Millisecond)
	cfg.PProf.Enabled = false
	return &cfg
}

// Redis 不可用时 Build 必须快速失败（装配期 Ping，不带病启动）。
func TestBuildFailsWhenRedisDown(t *testing.T) {
	cfg := storageCfg(t, "redis", "127.0.0.1:1") // 1 端口无服务
	_, err := app.Build(cfg, quietLogger(), nil)
	if err == nil {
		t.Fatal("build must fail when redis unreachable")
	}
}

// Redis 在线：Build 成功，Set 经异步写回落到 Redis，Shutdown 排空不丢。
func TestBuildWithRedisWriteBack(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	cfg := storageCfg(t, "redis", mr.Addr())
	srv, err := app.Build(cfg, quietLogger(), nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Shutdown（不 Run）也必须安全：仅关 acceptor + flush 存储。
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	if err := srv.Store().Set(context.Background(), "snake_result", "room-1",
		map[string]any{"winner": "p2", "rounds": 8}); err != nil {
		t.Fatal(err)
	}

	client := srv.Store().Raw().(*redis.Client)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := client.Exists(context.Background(), "rangame:kv:snake_result:room-1").Result()
		if err != nil {
			t.Fatalf("redis exists: %v", err)
		}
		if n == 1 {
			raw, err := client.Get(context.Background(), "rangame:kv:snake_result:room-1").Result()
			if err != nil {
				t.Fatal(err)
			}
			if raw != `{"rounds":8,"winner":"p2"}` {
				t.Fatalf("unexpected flushed json: %s", raw)
			}
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatal("async set was not flushed to redis")
}
