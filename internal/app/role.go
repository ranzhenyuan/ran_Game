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
	"fmt"
	"log/slog"
	"time"

	"github.com/rangame/server/internal/admin"
	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/config"
	"github.com/rangame/server/internal/gateway"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/internal/storage"
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
	// InternalAddr 内部链路监听地址（§12.3，如 ":7002"）；
	// 空=进程内 MemNodeTransport（同进程骨架），非空=TCPNodeTransport（跨 Pod）。
	InternalAddr string
	// InternalAdvertiseAddr 注册到 Registry 的对端可达地址（如 "${POD_IP}:7002"）；
	// 空=用 InternalAddr。
	InternalAdvertiseAddr string
	// Gateway 共享的 Gateway 装配（路由表 + NodeTransport）。
	// 演进态骨架同进程部署时 Gateway 与 Logic 共享同一实例，使 Forward 直达；
	// 真实拆分部署时各自创建独立 Gateway，走 TCPNodeTransport 跨进程。
	Gateway *gateway.Gateway
}

// registryPeerProvider 把 NodeRegistry 适配为 TCPNodeTransport 的地址解析器。
// nodeID → Addr 全部来自注册表（Gateway/Logic 注册时写内部通告地址）。
type registryPeerProvider struct {
	r cluster.NodeRegistry
}

func (p registryPeerProvider) Get(nodeID string) (gateway.NodeInfoLite, bool) {
	n, err := p.r.Get(nodeID)
	if err != nil {
		return gateway.NodeInfoLite{}, false
	}
	return gateway.NodeInfoLite{Addr: n.Addr}, true
}

// buildClusterGateway 构造 Gateway 装配。
// InternalAddr 非空：TCPNodeTransport 监听内部端口（真实跨 Pod）；
// 为空：MemNodeTransport（同进程骨架/测试）。
func buildClusterGateway(nodeID string, registry cluster.NodeRegistry, cc ClusterConfig) (*gateway.Gateway, error) {
	if cc.InternalAddr == "" {
		return gateway.NewGateway(nodeID, registry), nil
	}
	tr := gateway.NewTCPNodeTransport(nodeID, cc.InternalAddr, registryPeerProvider{r: registry})
	if _, err := tr.Listen(); err != nil {
		return nil, fmt.Errorf("listen internal transport %q: %w", cc.InternalAddr, err)
	}
	return gateway.NewGatewayWithTransport(nodeID, registry, tr), nil
}

// internalAdvertise 返回注册到 Registry 的内部通告地址。
func internalAdvertise(cc ClusterConfig) string {
	if cc.InternalAdvertiseAddr != "" {
		return cc.InternalAdvertiseAddr
	}
	return cc.InternalAddr
}

// GatewayExtra Gateway 角色额外装配。
type GatewayExtra struct {
	Gateway *gateway.Gateway
	// Connector 网关侧连接处理器（上行转发 + 下行回写）。
	Connector *GatewayConnector
}

// LogicExtra Logic 角色额外装配。
type LogicExtra struct {
	Drain   cluster.DrainController
	Handler *LogicHandler // Logic 侧帧接收器（接收 Gateway 转发帧并分发）
	// Gateway 本节点的 Gateway 装配（持有 NodeTransport）；停机时 Close 释放内部监听。
	Gateway *gateway.Gateway
}

// BuildGateway 按 gateway 角色装配（演进态骨架）。
//
// 复用 Build 装配 standalone 全部组件，额外注入：
//   - gateway.Gateway 装配对象（路由表 + 内部 Transport）
//   - GatewayConnector 替换单机 Connector：上行转发到 Logic，下行回写客户端
//   - 注册自身到 NodeRegistry（RoleGateway, Active）
//   - admin server 注入 NodeRegistry（/admin/nodes 可用）
//
// 演进态骨架：cc.Gateway 可由调用方传入以与 Logic 共享同进程 Transport；
// 为空时 BuildGateway 自建。
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
	}, logger, exporter, cc.Registry, nil, nil)

	srv, err := Build(cfg, logger, modules, WithMetrics(metrics), WithExporter(exporter), WithAdmin(adm))
	if err != nil {
		return nil, nil, err
	}
	// admin 提前创建时 profileQ 为 nil：storage 装配后补注入（§10.4.3 离线查询）
	adm.SetProfileQueryService(storage.NewProfileQueryService(srv.profileStore))

	// Gateway 装配（路由表 + NodeTransport）：
	// 调用方传入共享实例则复用（同进程骨架）；否则按 internal_addr 建 TCP/Mem。
	gw := cc.Gateway
	if gw == nil {
		var err error
		gw, err = buildClusterGateway(cc.NodeID, cc.Registry, cc)
		if err != nil {
			return nil, nil, err
		}
	}

	// GatewayConnector：替换单机 Connector 的上行/下行职责。
	gc := NewGatewayConnector(gw, logger)
	srv.gwConnector = gc
	srv.serveConn = gc.ServeConn

	if cc.Registry != nil {
		_ = cc.Registry.Register(cluster.NodeInfo{
			ID:    cc.NodeID,
			Role:  cluster.RoleGateway,
			Addr:  internalAdvertise(cc), // 内部链路通告地址（其他节点拨号目标）
			State: cluster.StateActive,
		})
	}
	return srv, &GatewayExtra{Gateway: gw, Connector: gc}, nil
}

// BuildLogic 按 logic 角色装配（演进态骨架）。
//
// 复用 Build 装配 standalone 全部组件，额外注入：
//   - DrainController 状态机（§13.3）
//   - LogicHandler：接收 Gateway 转发的帧并分发到会话/路由
//   - 注册到 NodeRegistry（RoleLogic, Active, Modules）
//   - 启动心跳协程（§13.2 每 5s 刷新 TTL 与 activeRooms）
//   - admin server 注入 NodeRegistry + drainFn（/admin/drain 可用）
//
// cc.Gateway 非空时复用其 NodeTransport（同进程骨架）；为空则自建。
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
	}, logger, exporter, cc.Registry, nil, nil)

	srv, err := Build(cfg, logger, modules, WithMetrics(metrics), WithExporter(exporter), WithAdmin(adm))
	if err != nil {
		return nil, nil, err
	}
	// admin 提前创建时 profileQ 为 nil：storage 装配后补注入（§10.4.3 离线查询）
	adm.SetProfileQueryService(storage.NewProfileQueryService(srv.profileStore))

	// Gateway 装配（同进程骨架共享直达；跨 Pod 各自 TCPNodeTransport）。
	gw := cc.Gateway
	if gw == nil {
		var err error
		gw, err = buildClusterGateway(cc.NodeID, cc.Registry, cc)
		if err != nil {
			return nil, nil, err
		}
	}

	// LogicHandler：注册到 NodeTransport，接收 Gateway 转发帧并分发。
	lh := NewLogicHandler(srv.sessions, srv.router, gw.Transport(), cc.NodeID, logger)
	lh.SetOnSessionStart(srv.onSessionStart)

	// DrainController：active_rooms 来自 room.Manager，在线会话来自 session.Manager
	roomsSrc := func() int { return srv.rooms.Rooms() }
	sessionsSrc := func() int { return srv.sessions.OnlineCount() }
	forceSettle := func() {
		logger.Info("force settle triggered", "node", cc.NodeID)
	}
	drain := cluster.NewDrainController(roomsSrc, sessionsSrc, forceSettle, cc.Registry, cc.NodeID)

	adm.SetDrainFn(func(ctx context.Context, deadline time.Duration) error {
		return StartClusterDrain(ctx, &LogicExtra{Drain: drain}, deadline)
	})

	if cc.Registry != nil {
		_ = cc.Registry.Register(cluster.NodeInfo{
			ID:      cc.NodeID,
			Role:    cluster.RoleLogic,
			Addr:    internalAdvertise(cc), // 内部链路通告地址（Gateway 懒拨号目标）
			Modules: cc.Modules,
			State:   cluster.StateActive,
		})
		go logicHeartbeatLoop(cc.Registry, cc.NodeID, roomsSrc, logger)
	}

	return srv, &LogicExtra{Drain: drain, Handler: lh, Gateway: gw}, nil
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
