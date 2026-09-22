package app

import (
	"context"
	"fmt"

	"log/slog"

	"github.com/rangame/server/internal/config"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/internal/storage"
	"github.com/rangame/server/internal/storage/redisstore"
	"github.com/rangame/server/internal/storage/sqlstore"
	"github.com/rangame/server/pkg/framework"
)

// buildStorage 按 cfg.Storage.Driver 装配持久化层：
//   - memory：进程内实现（单机默认，零外部依赖，无写回/WAL）；
//   - redis：会话/排行榜天然适配，经异步写回门面（WAL+熔断保护）；
//   - mysql：对局记录/资产等强一致数据，同样经写回门面。
//
// 远程后端在装配期 Ping 校验连通性，连不上直接 Build 失败（快速失败优于带病启动）。
func buildStorage(cfg *config.Config, logger *slog.Logger, metrics *obs.Registry) (framework.Storage, error) {
	if logger == nil {
		logger = slog.Default()
	}
	switch cfg.Storage.Driver {
	case "memory", "":
		return storage.NewMemoryStorage(), nil
	}

	sc := cfg.Storage
	ctx, cancel := context.WithTimeout(context.Background(), sc.SyncTimeout.Std())
	defer cancel()

	var backend storage.Backend
	switch sc.Driver {
	case "redis":
		b, err := redisstore.New(ctx, redisstore.Config{Addr: sc.RedisAddr, DB: sc.RedisDB})
		if err != nil {
			return nil, fmt.Errorf("build storage: %w", err)
		}
		backend = b
	case "mysql":
		b, err := sqlstore.Open(ctx, "mysql", sc.MySQLDSN)
		if err != nil {
			return nil, fmt.Errorf("build storage: %w", err)
		}
		backend = b
	default:
		return nil, fmt.Errorf("build storage: unknown driver %q", sc.Driver)
	}

	wal, err := storage.OpenWAL(sc.WALPath)
	if err != nil {
		_ = backend.Close()
		return nil, fmt.Errorf("build storage: %w", err)
	}
	st, err := storage.NewStore(backend, storage.Options{
		QueueSize:       sc.WriteBackQueue,
		FlushInterval:   sc.FlushInterval.Std(),
		MaxBatch:        sc.FlushBatch,
		MaxRetry:        sc.FlushRetry,
		SyncTimeout:     sc.SyncTimeout.Std(),
		BreakerCooldown: sc.BreakerCooldown.Std(),
		WAL:             wal,
		Metrics:         metrics,
		Logger:          logger,
	})
	if err != nil {
		_ = backend.Close()
		return nil, fmt.Errorf("build storage: %w", err)
	}
	return st, nil
}
