// Command server 游戏服务器入口（架构文档 §11 + §12.6 演进态）。
//
// 按 cfg.Server.Role 装配：
//
//	standalone（默认）：单进程完整装配
//	gateway：演进态骨架，复用 Build + cluster.Gateway 路由表
//	logic：演进态骨架，复用 Build + DrainController + 注册到 NodeRegistry
//
// 装配链：配置/观测 → [Cluster Registry/Gateway/Drain] → Actor 引擎 → 存储 →
// Session/Room/Match → Router（框架消息 + 玩法模块消息）→ TCP/WS Acceptor → 信号优雅停机。
//
//	go run ./cmd/server -config configs/server.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rangame/server/games/card"
	"github.com/rangame/server/games/quiz"
	"github.com/rangame/server/games/snake"
	"github.com/rangame/server/internal/app"
	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/config"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/pkg/framework"
)

func main() {
	configPath := flag.String("config", "configs/server.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic(err)
	}
	logger := obs.NewLogger(cfg.Log.Level)
	metrics := obs.NewRegistry()

	if cfg.PProf.Enabled {
		shutdown, err := obs.StartPProf(cfg.PProf.Addr)
		if err != nil {
			logger.Warn("pprof disabled", "err", err)
		} else {
			defer func() { _ = shutdown(context.Background()) }()
		}
	}

	// 玩法模块装配（games 仅依赖 pkg/framework，由 cmd 注入）。
	// 列表索引决定消息 ID 段（0→0x1000, 1→0x1100, 2→0x1200），上线后不得调整顺序。
	modules := []framework.GameModule{snake.Module{}, card.Module{}, quiz.Module{}}

	// 按 server.role 分派装配（§12.6）。
	var srv *app.Server
	var logicExtra *app.LogicExtra
	var registry cluster.NodeRegistry
	switch cfg.Server.Role {
	case "standalone", "":
		srv, err = app.Build(cfg, logger, modules, app.WithMetrics(metrics))
	case "gateway", "logic":
		registry, err = buildRegistry(cfg, logger)
		if err != nil {
			logger.Error("registry build failed", "err", err)
			os.Exit(1)
		}
		defer registry.Close()
		cc := app.ClusterConfig{
			NodeID:                cfg.Cluster.NodeID,
			Modules:               cfg.Cluster.Modules,
			DrainDeadline:         cfg.Cluster.DrainDeadline.Std(),
			Registry:              registry,
			InternalAddr:          cfg.Cluster.InternalAddr,
			InternalAdvertiseAddr: cfg.Cluster.InternalAdvertiseAddr,
		}
		if cfg.Server.Role == "gateway" {
			role := cluster.RoleGateway
			cc.Role = role
			var gwExtra *app.GatewayExtra
			srv, gwExtra, err = app.BuildGateway(cfg, logger, modules, cc, metrics)
			if gwExtra != nil {
				defer gwExtra.Gateway.Close()
			}
		} else {
			cc.Role = cluster.RoleLogic
			srv, logicExtra, err = app.BuildLogic(cfg, logger, modules, cc, metrics)
			if logicExtra != nil && logicExtra.Gateway != nil {
				defer logicExtra.Gateway.Close()
			}
		}
	default:
		err = fmt.Errorf("unknown server.role: %q", cfg.Server.Role)
	}
	if err != nil {
		logger.Error("server build failed", "err", err)
		os.Exit(1)
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.Run(rootCtx)

	// §11 优雅停机：SIGTERM/SIGINT → 停接入 → 排水(若 logic)→ Actor 排空(10s) → 存储 flush。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("signal received, shutting down", "signal", sig.String())

	// Logic 角色：触发排水式缩容（§13.3 preStop 等价）。
	if logicExtra != nil {
		drainCtx, dcancel := context.WithTimeout(context.Background(), cfg.Cluster.DrainDeadline.Std())
		if err := app.StartClusterDrain(drainCtx, logicExtra, cfg.Cluster.DrainDeadline.Std()); err != nil {
			logger.Warn("drain did not complete in time, forcing exit", "err", err)
		}
		dcancel()
	}

	cancel()
	shutdownCtx, sc := context.WithTimeout(context.Background(), 10*time.Second)
	defer sc()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "err", err)
	}
	logger.Info("server stopped")
}

// buildRegistry 按 cluster 配置创建 NodeRegistry。
//
//	redis_addr 为空 → 进程内 MemRegistry（演进态骨架，单进程集成测试用）；
//	非空 → RedisRegistry（真实分布式部署，需 Redis 实例）。
func buildRegistry(cfg *config.Config, logger *slog.Logger) (cluster.NodeRegistry, error) {
	if cfg.Cluster.RedisAddr == "" {
		logger.Info("using in-memory node registry (evolutionary stub)")
		return cluster.NewMemRegistry(), nil
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.Cluster.RedisAddr,
		DB:   cfg.Cluster.RedisDB,
	})
	return cluster.NewRedisRegistry(rdb, cluster.Config{
		NodeTTL: cfg.Cluster.NodeTTL.Std(),
		Channel: "cluster:nodes:change",
	}), nil
}
