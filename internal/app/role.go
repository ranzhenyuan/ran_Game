// role.go 演进态装配入口（架构文档 §12.6 / §13）。
//
//	按 cfg.Server.Role 分派（Build 内部调用）：
//	  standalone：原 Build 路径（单机形态，无 cluster 组件）
//	  gateway：BuildGateway，复用 Build 装配 + 注入 cluster.Gateway（路由表+Transport）
//	  logic：BuildLogic，复用 Build 装配 + 注入 DrainController + 注册到 NodeRegistry
//
//	演进态骨架：gateway/logic 仍在同进程内装配，复用 standalone 组件，
//	仅多挂一层 cluster.Gateway / DrainController 以验证走查 12-16 设计可行。
//	真实拆分部署时：拆 ConnSession/PlayerSession，本骨架不实现。
package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/rangame/server/internal/admin"
	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/config"
	"github.com/rangame/server/internal/gateway"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/pkg/framework"
)

// ClusterConfig 集群装配配置。
type ClusterConfig struct {
	// NodeID 本节点 ID（StatefulSet：logic-{module}-{ordinal}；Deployment：gateway 随机）
	NodeID string
	// Role 节点角色
	Role cluster.Role
	// Modules 本节点服务的 module 列表（Logic 必填；Gateway 留空）
	Modules []string
	// DrainDeadline 排水截止时间（默认 20min，§13.3）
	DrainDeadline time.Duration
	// Registry 节点注册表（演进态骨架：MemRegistry；真实部署：RedisRegistry）
	Registry cluster.NodeRegistry
}

// GatewayExtra Gateway 角色额外装配。
type GatewayExtra struct {
	Gateway *gateway.Gateway
}

// LogicExtra Logic 角色额外装配。
type LogicExtra struct {
	Drain cluster.DrainController
}

// BuildGateway 按 gateway 角色装配（演进态骨架）。
//
// 复用 Build 装配 standalone 全部组件，额外注入：
//   - cluster.Gateway 装配对象（路由表 + 内部 Transport）
//   - 注册自身到 NodeRegistry（RoleGateway, Active）
//   - admin server 注入 NodeRegistry（/admin/nodes 可用）
//
// 演进态骨架：Connector 不替换（单进程下 Logic 即 Gateway，路由表 Bind 在 BuildLogic 侧完成）。
// 真实拆分部署时：Connector 的 onSessionStart 改为 Bind uid → 选定的 Logic 节点。
func BuildGateway(
	cfg *config.Config,
	logger *slog.Logger,
	modules []framework.GameModule,
	cc ClusterConfig,
	metrics *obs.Registry,
) (*Server, *GatewayExtra, error) {
	// 提前创建 PrometheusExporter 与 admin server，注入 NodeRegistry
	exporter := obs.NewPrometheusExporter(metrics)
	adm := admin.NewServer(admin.Config{
		Addr:         cfg.Admin.Addr,
		TrustedCIDRs: cfg.Admin.TrustedCIDRs,
	}, logger, exporter, cc.Registry, nil)

	srv, err := Build(cfg, logger, modules, WithMetrics(metrics), WithExporter(exporter), WithAdmin(adm))
	if err != nil {
		return nil, nil, err
	}

	gw := gateway.NewGateway(cc.NodeID, cc.Registry)
	if cc.Registry != nil {
		_ = cc.Registry.Register(cluster.NodeInfo{
			ID:    cc.NodeID,
			Role:  cluster.RoleGateway,
			Addr:  cfg.TCP.Addr,
			State: cluster.StateActive,
		})
	}
	return srv, &GatewayExtra{Gateway: gw}, nil
}

// BuildLogic 按 logic 角色装配（演进态骨架）。
//
// 复用 Build 装配 standalone 全部组件，额外注入：
//   - DrainController 状态机（§13.3）
//   - 注册到 NodeRegistry（RoleLogic, Active, Modules）
//   - 启动心跳协程（§13.2 每 5s 刷新 TTL 与 activeRooms）
//   - admin server 注入 NodeRegistry + drainFn（/admin/drain 可用）
func BuildLogic(
	cfg *config.Config,
	logger *slog.Logger,
	modules []framework.GameModule,
	cc ClusterConfig,
	metrics *obs.Registry,
) (*Server, *LogicExtra, error) {
	exporter := obs.NewPrometheusExporter(metrics)

	// 先用 nil 创建 admin，drainFn 在 drain 构造后再注入
	adm := admin.NewServer(admin.Config{
		Addr:         cfg.Admin.Addr,
		TrustedCIDRs: cfg.Admin.TrustedCIDRs,
	}, logger, exporter, cc.Registry, nil)

	srv, err := Build(cfg, logger, modules, WithMetrics(metrics), WithExporter(exporter), WithAdmin(adm))
	if err != nil {
		return nil, nil, err
	}

	// DrainController：active_rooms 来自 room.Manager，在线会话来自 session.Manager
	roomsSrc := func() int { return srv.rooms.Rooms() }
	sessionsSrc := func() int { return srv.sessions.OnlineCount() }
	forceSettle := func() {
		// §13.3 强结算路径：触发所有房间的 TimeoutLogic（演进态骨架：仅日志占位）
		// 真实实现应遍历活跃房间调 OnTimeout；本骨架不实现以保持 standalone 兼容
		logger.Info("force settle triggered", "node", cc.NodeID)
	}
	drain := cluster.NewDrainController(roomsSrc, sessionsSrc, forceSettle, cc.Registry, cc.NodeID)

	// 给 admin 注入 drainFn（覆盖构造期传入的 nil）
	adm.SetDrainFn(func(ctx context.Context, deadline time.Duration) error {
		return StartClusterDrain(ctx, &LogicExtra{Drain: drain}, deadline)
	})

	if cc.Registry != nil {
		_ = cc.Registry.Register(cluster.NodeInfo{
			ID:      cc.NodeID,
			Role:    cluster.RoleLogic,
			Addr:    cfg.TCP.Addr,
			Modules: cc.Modules,
			State:   cluster.StateActive,
		})
		go logicHeartbeatLoop(cc.Registry, cc.NodeID, roomsSrc, logger)
	}

	return srv, &LogicExtra{Drain: drain}, nil
}

// logicHeartbeatLoop Logic 节点心跳循环（§13.2：每 5s 刷新 TTL 与 activeRooms）。
// 演进态骨架：进程内 MemRegistry 无 TTL 概念，仅刷新 LastBeat/ActiveRooms。
func logicHeartbeatLoop(
	registry cluster.NodeRegistry,
	nodeID string,
	roomsSrc func() int,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := registry.Heartbeat(nodeID, roomsSrc()); err != nil {
			logger.Warn("heartbeat failed", "node", nodeID, "err", err)
			return
		}
	}
}

// StartClusterDrain 触发 Logic 节点排水（preStop 调用，§13.3）。
//
//	deadline 超时后强制结算；调用方阻塞至 Drained 或 ctx 取消。
//	真实部署：K8s preStop 钩子调用本函数；HPA 缩容由控制器触发。
func StartClusterDrain(
	ctx context.Context,
	logic *LogicExtra,
	deadline time.Duration,
) error {
	if logic == nil || logic.Drain == nil {
		return nil
	}
	if err := logic.Drain.Drain(deadline); err != nil {
		return err
	}
	return logic.Drain.WaitDrained(ctx)
}
