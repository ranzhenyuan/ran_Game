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
	// Routes 注入的一级路由表（多 Gateway 副本用 Redis 共享实现）。
	// 空=buildClusterGateway 自建 MemRouteTable（同进程骨架）。
	Routes gateway.RouteTable
	// KeepaliveInterval 内部链路应用层 idle 心跳周期（§12.3）；<=0 用默认 30s。
	KeepaliveInterval time.Duration
	// ReconnCheck 内部链路后台探活/重连扫描周期（§12.3）；<=0 用默认 2s。
	ReconnCheck time.Duration
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
// cc.Routes 非空时注入该路由表（Redis 共享），否则用 MemRouteTable。
func buildClusterGateway(nodeID string, registry cluster.NodeRegistry, cc ClusterConfig) (*gateway.Gateway, error) {
	var tr gateway.NodeTransport
	if cc.InternalAddr == "" {
		tr = gateway.NewMemNodeTransport(nodeID)
	} else {
		tcp := gateway.NewTCPNodeTransport(nodeID, cc.InternalAddr, registryPeerProvider{r: registry},
			gateway.WithKeepaliveInterval(cc.KeepaliveInterval),
			gateway.WithReconnCheck(cc.ReconnCheck))
		if _, err := tcp.Listen(); err != nil {
			return nil, fmt.Errorf("listen internal transport %q: %w", cc.InternalAddr, err)
		}
		tr = tcp
	}
	if cc.Routes != nil {
		return gateway.NewGatewayWithDeps(nodeID, registry, cc.Routes, tr), nil
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

// transportWarmer 具备连接预热能力的 Transport（TCPNodeTransport 满足；Mem 不满足时联动自动跳过）。
type transportWarmer interface {
	Warmup(nodeID string) error
}

// wireTransportWarmup 把 NodeTransport 的长连接生命周期与注册表联动（连接预热 + 注册表联动）。
//
//	初始扫描：对端角色当前 Active 节点逐一 Warmup，消除首帧拨号延迟；
//	事件驱动：Watch 到对端节点 Active → Warmup 预拨号；Down/Draining → RemovePeer 清连接停重连。
//
// 仅对支持 Warmup 的 Transport（TCP）生效；registry.Close 时 Watch channel 关闭，协程自行退出。
func wireTransportWarmup(tr gateway.NodeTransport, reg cluster.NodeRegistry, localID string, peerRole cluster.Role, logger *slog.Logger) {
	warmer, ok := tr.(transportWarmer)
	if !ok {
		return // MemNodeTransport 进程内直达，无需预热
	}
	isPeer := func(id string) bool {
		if id == localID {
			return false
		}
		n, err := reg.Get(id)
		return err == nil && n.Role == peerRole
	}

	// 先同步订阅，再扫描：杜绝“扫描后、订阅前”节点上线的事件丢失窗口。
	// 订阅后上线的节点由事件捕获；订阅前已上线的由扫描捕获（重叠部分 Warmup 幂等无害）。
	evCh := reg.Watch()

	if nodes, err := reg.List(peerRole, true); err == nil {
		for _, n := range nodes {
			if n.ID == localID {
				continue
			}
			if err := warmer.Warmup(n.ID); err != nil {
				logger.Warn("warmup peer failed", "peer", n.ID, "err", err)
			}
		}
	}

	go func() {
		for ev := range evCh {
			switch ev.New {
			case cluster.StateActive:
				if !isPeer(ev.NodeID) {
					continue
				}
				if err := warmer.Warmup(ev.NodeID); err != nil {
					logger.Warn("warmup peer on active event failed", "peer", ev.NodeID, "err", err)
				}
			case cluster.StateDown, cluster.StateDraining:
				tr.RemovePeer(ev.NodeID) // 幂等：非对端/不存在无副作用
			}
		}
	}()
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
		// 连接预热 + 注册表联动：对端 Logic 上线即预拨号，下线即清理。
		wireTransportWarmup(gw.Transport(), cc.Registry, cc.NodeID, cluster.RoleLogic, logger)
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
	// 路由表由 Logic 写入/清除（登录 Bind、会话释放 Unbind），Gateway 只读。
	lh := NewLogicHandler(srv.sessions, srv.router, gw.Transport(), gw.Routes(), cc.NodeID, logger)
	lh.SetOnSessionStart(srv.onSessionStart)

	// 会话释放链：先条件 Unbind 路由（防顶踢误删新映射），再走原单机释放逻辑。
	origRelease := srv.onSessionRelease
	nodeID := cc.NodeID
	srv.sessions.SetOnRelease(func(uid, roomID string) {
		gw.Routes().Unbind(uid, nodeID) // 路由表与会话同生命周期；失败仅影响路由清理，不阻断释放
		origRelease(uid, roomID)
	})

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
		go logicHeartbeatLoop(cc.Registry, cc.NodeID, roomsSrc, logger, metrics, 5*time.Second)
		// 连接预热 + 注册表联动：对端 Gateway 上线即预拨号（下行推送链路），下线即清理。
		wireTransportWarmup(gw.Transport(), cc.Registry, cc.NodeID, cluster.RoleGateway, logger)
	}

	return srv, &LogicExtra{Drain: drain, Handler: lh, Gateway: gw}, nil
}

// logicHeartbeatLoop Logic 节点心跳循环（§13.2：每 5s 刷新 TTL 与 activeRooms）。
// 演进态骨架：进程内 MemRegistry 无 TTL 概念，仅刷新 LastBeat/ActiveRooms。
//
// 容错：单次心跳失败（Redis 抖动/短暂断连）只记日志 + 计数，不退出循环。
// 原实现失败即 return 会导致心跳协程永久死亡 → 节点 TTL 过期被标 Down 级联故障；
// 现在由 go-redis 连接池自愈重连，下一拍自动恢复续期。
func logicHeartbeatLoop(
	registry cluster.NodeRegistry,
	nodeID string,
	roomsSrc func() int,
	logger *slog.Logger,
	metrics *obs.Registry,
	interval time.Duration,
) {
	var failCnt *obs.Counter
	if metrics != nil {
		failCnt = metrics.Counter("registry_heartbeat_fail_total")
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := registry.Heartbeat(nodeID, roomsSrc()); err != nil {
			if failCnt != nil {
				failCnt.Inc()
			}
			// 仅记录，不退出：等待连接池重连后下一拍恢复
			logger.Warn("heartbeat failed (will retry next tick)", "node", nodeID, "err", err)
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
