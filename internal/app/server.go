package app

import (
	"context"
	"log/slog"
	"sync"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/admin"
	"github.com/rangame/server/internal/config"
	"github.com/rangame/server/internal/match"
	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/room"
	"github.com/rangame/server/internal/router"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// Server 单机形态服务器：装配全部组件并管理生命周期（§11 停机）。
type Server struct {
	cfg     *config.Config
	logger  *slog.Logger
	metrics *obs.Registry

	engine    *actor.Engine
	storage   framework.Storage
	sessions  *session.Manager
	rooms     *room.Manager
	matcher   *match.Maker
	router    *router.Router
	connector *Connector

	tcpAcc *transport.TCPAcceptor
	wsAcc  *transport.WSAcceptor

	adminSrv   *admin.Server
	exporter   *obs.PrometheusExporter
	stdMetrics *obs.StandardMetrics

	playerMu sync.Mutex
	players  map[string]framework.ID // uid → PlayerActor（生命周期跟随 Session 宽限期）
}

// Option 装配选项。
type Option func(*Server)

// WithStorage 注入存储实现（测试用；默认进程内内存存储）。
func WithStorage(st framework.Storage) Option {
	return func(s *Server) { s.storage = st }
}

// WithMetrics 注入指标注册表。
func WithMetrics(reg *obs.Registry) Option {
	return func(s *Server) { s.metrics = reg }
}

// WithAdmin 注入 admin server（BuildGateway/BuildLogic 在外部构造后注入）。
func WithAdmin(s *admin.Server) Option {
	return func(srv *Server) { srv.adminSrv = s }
}

// WithExporter 注入 Prometheus 导出器。
func WithExporter(e *obs.PrometheusExporter) Option {
	return func(s *Server) { s.exporter = e }
}

// Build 按配置装配服务器（不监听端口；调用 Run 后开始接受连接）。
func Build(cfg *config.Config, logger *slog.Logger, modules []framework.GameModule, opts ...Option) (*Server, error) {
	s := &Server{cfg: cfg, logger: logger, players: make(map[string]framework.ID)}
	for _, o := range opts {
		o(s)
	}
	if logger == nil {
		logger = slog.Default()
		s.logger = logger
	}
	// metrics 默认创建（除非 WithMetrics 注入了）
	if s.metrics == nil {
		s.metrics = obs.NewRegistry()
	}
	if s.storage == nil {
		st, err := buildStorage(cfg, s.logger, s.metrics)
		if err != nil {
			return nil, err
		}
		s.storage = st
	}

	s.engine = actor.New(actor.Config{Metrics: s.metrics})

	s.sessions = session.NewManager(session.Config{
		ReplayCapacity: cfg.Session.ReplayBuffer,
		GracePeriod:    cfg.Session.GracePeriod.Std(),
		SweepInterval:  cfg.Session.SweepInterval.Std(),
		HeartbeatMS:    cfg.Session.HeartbeatMS,
		Metrics:        s.metrics,
	})
	// 会话释放（宽限期到期/顶号）：取消匹配 → 退房 → 停 PlayerActor。
	s.sessions.SetOnRelease(s.onSessionRelease)

	s.rooms = room.NewManager(s.engine, s.sessions, s.storage, logger,
		room.WithDefaultMailbox(framework.MailboxPolicy{Capacity: 4096, OnFull: framework.FullDrop}),
	)

	s.router = router.New(s.engine, router.WithMetrics(s.metrics))
	s.registerFrameworkRoutes()

	// 注册玩法模块（房间定义 + 房间消息路由）。
	for _, mod := range modules {
		rc, factory := mod.RoomDef()
		if rc.Module == "" {
			rc.Module = mod.Name()
		}
		s.rooms.RegisterModule(room.ModuleDef{Factory: factory, Config: rc})
		mod.RegisterRoutes(s.router)
	}

	s.matcher = match.New(s.rooms, s.notifyMatchResult)

	s.connector = NewConnector(s.sessions, s.router, logger,
		cfg.Session.MaxLoginBody, cfg.Session.HeartbeatMS)
	s.connector.OnSessionStart = s.onSessionStart

	tcpOpts := transport.Options{
		ReadTimeout:        cfg.TCP.ReadTimeout.Std(),
		WriteChannelSize:   cfg.TCP.WriteChannelSize,
		WriteFlushInterval: cfg.TCP.WriteFlushInterval.Std(),
		MaxFrameSize:       cfg.TCP.MaxFrameSize,
		NoDelay:            cfg.TCP.TCPNoDelay,
	}
	tcpAcc, err := transport.NewTCPAcceptor(cfg.TCP.Addr, tcpOpts)
	if err != nil {
		return nil, err
	}
	s.tcpAcc = tcpAcc

	if cfg.WS.Enabled {
		wsAcc, werr := transport.NewWSAcceptor(cfg.WS.Addr, cfg.WS.Path, transport.WSOptions{
			AllowedOrigins: cfg.WS.AllowedOrigins,
			Options: transport.Options{
				ReadTimeout:      cfg.WS.ReadTimeout.Std(),
				WriteChannelSize: cfg.WS.WriteChannelSize,
				MaxFrameSize:     cfg.WS.MaxFrameSize,
			},
		})
		if werr != nil {
			_ = tcpAcc.Close()
			return nil, werr
		}
		s.wsAcc = wsAcc
	}

	// §11.2 标准指标登记 + Prometheus 导出器 + admin server
	if s.exporter == nil {
		s.exporter = obs.NewPrometheusExporter(s.metrics)
	}
	s.stdMetrics = obs.RegisterStandard(s.metrics, s.exporter)
	if s.adminSrv == nil {
		s.adminSrv = admin.NewServer(admin.Config{
			Addr:         cfg.Admin.Addr,
			TrustedCIDRs: cfg.Admin.TrustedCIDRs,
		}, logger, s.exporter, nil, nil)
	}

	return s, nil
}

// TCPAddr / WSAddr 返回实际监听地址（":0" 随机端口测试用）。
func (s *Server) TCPAddr() string { return s.tcpAcc.Addr() }
func (s *Server) WSAddr() string {
	if s.wsAcc == nil {
		return ""
	}
	return s.wsAcc.Addr()
}

// Sessions/Rooms/Matcher/Storage 暴露给测试与观测。
func (s *Server) Sessions() *session.Manager { return s.sessions }
func (s *Server) Rooms() *room.Manager       { return s.rooms }
func (s *Server) Matcher() *match.Maker      { return s.matcher }
func (s *Server) Store() framework.Storage   { return s.storage }

// Run 启动扫描与接入循环；ctx 取消后停止接受新连接（停机用 Shutdown 完成排空）。
func (s *Server) Run(ctx context.Context) {
	go s.sessions.Start(ctx)
	go s.serveAcceptor(ctx, s.tcpAcc)
	if s.wsAcc != nil {
		go s.serveAcceptor(ctx, s.wsAcc)
	}
	if s.adminSrv != nil {
		if err := s.adminSrv.Start(); err != nil {
			s.logger.Warn("admin server failed to start", "err", err)
		} else {
			// 标记就绪（演进态骨架：acceptor 起来即就绪）
			s.adminSrv.SetReady(true)
		}
	}
	s.logger.Info("server listening",
		"tcp", s.tcpAcc.Addr(),
		"ws", wsAddr(s.wsAcc),
		"role", s.cfg.Server.Role)
}

func (s *Server) serveAcceptor(ctx context.Context, acc interface {
	Accept(context.Context) (transport.Conn, error)
}) {
	for {
		conn, err := acc.Accept(ctx)
		if err != nil {
			return
		}
		go s.connector.ServeConn(ctx, conn)
	}
}

// Shutdown 优雅停机（§11.3）：停接入 → admin 标 not-ready → Actor 排空 → flush 存储 → admin 关。
func (s *Server) Shutdown(ctx context.Context) error {
	if s.adminSrv != nil {
		s.adminSrv.SetReady(false)
	}
	_ = s.tcpAcc.Close()
	if s.wsAcc != nil {
		_ = s.wsAcc.Close()
	}
	s.engine.Shutdown(ctx)
	if s.adminSrv != nil {
		_ = s.adminSrv.Shutdown(ctx)
	}
	return s.storage.Close()
}

// onSessionStart 登录/重连成功：确保 PlayerActor 存在并绑定 Registry（重连幂等）。
func (s *Server) onSessionStart(sess *session.Session) {
	uid := sess.UID()
	s.playerMu.Lock()
	defer s.playerMu.Unlock()
	if _, ok := s.players[uid]; ok {
		return // 重连：Actor 宽限期内一直存活
	}
	id, err := s.engine.Spawn(&playerActor{uid: uid},
		framework.MailboxPolicy{Capacity: 1024, OnFull: framework.FullKick, DrainBatch: 64}, 0)
	if err != nil {
		s.logger.Error("spawn player actor failed", "uid", uid, "err", err)
		return
	}
	s.engine.Registry().BindUID(uid, id)
	s.players[uid] = id
}

// onSessionRelease 会话最终释放：清理匹配队列、退房并停止 PlayerActor。
func (s *Server) onSessionRelease(uid, roomID string) {
	s.matcher.Cancel(uid)
	if roomID != "" {
		s.rooms.Leave(roomID, uid)
	}
	s.playerMu.Lock()
	id := s.players[uid]
	delete(s.players, uid)
	s.playerMu.Unlock()
	if id != 0 {
		s.engine.Stop(id)
	}
}

// registerFrameworkRoutes 注册个人消息段：匹配/取消匹配（房间消息由模块注册）。
func (s *Server) registerFrameworkRoutes() {
	s.router.Register(framework.MsgMatch, func() any { return &match.Request{} },
		func(ctx framework.MsgCtx, req any) (any, error) {
			r := req.(*match.Request)
			// 人数取模块房间配置的 MatchRule；缺省 1 人一桌（便于单人玩法/测试）。
			rule := s.matchRuleFor(r.Module, r.Code)
			if err := s.matcher.Enqueue(ctx.UID(), rule, ctx.Seq()); err != nil {
				return nil, mapMatchErr(err)
			}
			return nil, nil // 成桌结果由 notifyMatchResult 主动下发
		})

	s.router.Register(framework.MsgMatchCancel, func() any { return &match.CancelRequest{} },
		func(framework.MsgCtx, any) (any, error) {
			return struct{}{}, nil
		})
}

func (s *Server) matchRuleFor(module, code string) framework.MatchRule {
	// room.Manager 已注册模块配置；通过一次“试探性”读取避免再暴露新接口：
	// 人数与规则直接用模块在 RoomDef 中声明的 MatchRule（为空时 1 人即刻成桌）。
	rule := framework.MatchRule{Module: module, Code: code, Players: 1}
	if def, ok := s.rooms.ModuleDef(module); ok {
		if def.Config.MatchRule.Players > 0 {
			rule = def.Config.MatchRule
			if rule.Module == "" {
				rule.Module = module
			}
			rule.Code = code
		}
	}
	return rule
}

func mapMatchErr(err error) error {
	if err == match.ErrAlreadyQueued {
		return framework.NewErr(framework.ErrInQueue, "")
	}
	if err == match.ErrBadRule {
		return framework.NewErr(framework.ErrDecode, "unknown match module")
	}
	return err
}

// notifyMatchResult 成桌/失败回调：在匹配器调用栈内按原 seq 主动下发 MsgMatch 帧。
func (s *Server) notifyMatchResult(uid string, seq uint32, roomID string, merr error) {
	sess, ok := s.sessions.Get(uid)
	if !ok {
		return // 会话已释放（取消在途竞态），丢弃
	}
	var f *transport.Frame
	if merr != nil {
		ec := toErrCode(merr)
		f, _ = protocol.NewErrorFrameFor(sess.CodecType(), framework.MsgMatch, seq, ec.Code, ec.Msg, nil)
	} else {
		f, _ = protocol.NewOKFrameFor(sess.CodecType(), framework.MsgMatch, seq, match.Response{RoomID: roomID})
	}
	if f != nil {
		_ = sess.Send(f)
	}
}

// playerActor 玩家个人 Actor：承载个人消息 Handler 闭包串行执行（§7）。
// 生命周期跟随 Session 宽限期（OnRelease 时停止），而非连接。
type playerActor struct {
	uid string
}

func (a *playerActor) Init(framework.ActorCtx)                           {}
func (a *playerActor) OnMessage(framework.ActorCtx, *framework.Envelope) {}
func (a *playerActor) OnStop(framework.ActorCtx)                         {}

func wsAddr(a *transport.WSAcceptor) string {
	if a == nil {
		return "disabled"
	}
	return a.Addr()
}
