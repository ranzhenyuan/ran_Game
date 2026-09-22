// standard_metrics.go §11.2 Prometheus 指标清单的集中登记。
//
//	调用 RegisterStandard(reg) 在启动期一次性登记全部指标；
//	各模块运行期通过 reg.Gauge("online_connections") 等获取句柄读写。
//	指标名/类型/help 文本与架构文档 §11.2 完全一致，prometheus-adapter 可直接对接。

package obs

// StandardMetrics §11.2 指标清单（按架构文档表格顺序）。
//
//	注意：active_rooms/match_queue_depth/node_state 在演进态骨架由 cluster 包导出，
//	standalone 形态登记但不写入，HPA 不读取（无 NodeRegistry 也不会发）。
type StandardMetrics struct {
	OnlineConnections *Gauge     // online_connections
	OnlineSessions    *Gauge     // online_sessions
	MsgQPSUp          *Counter   // msg_qps{dir=up}
	MsgQPSDown        *Counter   // msg_qps{dir=down}
	HandlerDurationMS *Histogram // handler_duration_ms
	MailboxLen        *Histogram // mailbox_len
	MailboxDropTotal  *Counter   // mailbox_drop_total
	MailboxKickTotal  *Counter   // mailbox_kick_total
	Rooms             *Gauge     // rooms
	Actors            *Gauge     // actors
	ActiveRooms       *Gauge     // active_rooms{node,module}
	MatchQueueDepth   *Gauge     // match_queue_depth{module}
	NodeState         *Gauge     // node_state{node,role}
	PanicTotal        *Counter   // panic_total
}

// RegisterStandard 在启动期登记全部 §11.2 指标并返回 StandardMetrics 句柄。
//
// 同时向 PrometheusExporter 注册 HELP/TYPE 元信息。
func RegisterStandard(reg *Registry, exporter *PrometheusExporter) *StandardMetrics {
	// Gauge / Counter
	gOnlineConn := reg.Gauge("online_connections")
	gOnlineSess := reg.Gauge("online_sessions")
	cMsgUp := reg.Counter("msg_qps_up")
	cMsgDown := reg.Counter("msg_qps_down")
	hHandler := reg.Histogram("handler_duration_ms", nil)
	hMailbox := reg.Histogram("mailbox_len", nil)
	cDrop := reg.Counter("mailbox_drop_total")
	cKick := reg.Counter("mailbox_kick_total")
	gRooms := reg.Gauge("rooms")
	gActors := reg.Gauge("actors")
	gActiveRooms := reg.Gauge("active_rooms")
	gMatchQueue := reg.Gauge("match_queue_depth")
	gNodeState := reg.Gauge("node_state")
	cPanic := reg.Counter("panic_total")

	// 元信息
	if exporter != nil {
		exporter.Register("online_connections", "在线连接数", TypeGauge)
		exporter.Register("online_sessions", "有效会话数", TypeGauge)
		exporter.Register("msg_qps_up", "上行消息 QPS", TypeCounter)
		exporter.Register("msg_qps_down", "下行消息 QPS", TypeCounter)
		exporter.Register("handler_duration_ms", "处理耗时（P99 关注）", TypeHistogram)
		exporter.Register("mailbox_len", "邮箱水位（慢 Actor 定位）", TypeHistogram)
		exporter.Register("mailbox_drop_total", "背压触发 Drop 次数", TypeCounter)
		exporter.Register("mailbox_kick_total", "背压触发 Kick 次数", TypeCounter)
		exporter.Register("rooms", "进行中房间数", TypeGauge)
		exporter.Register("actors", "活跃 Actor 数", TypeGauge)
		exporter.Register("active_rooms", "进行中房间数（Logic 排水与 HPA 核心信号）", TypeGauge)
		exporter.Register("match_queue_depth", "匹配队列深度（扩容领先指标）", TypeGauge)
		exporter.Register("node_state", "节点状态：1=Active / 0.5=Draining / 0=Drained", TypeGauge)
		exporter.Register("panic_total", "panic 次数", TypeCounter)
	}

	return &StandardMetrics{
		OnlineConnections: gOnlineConn,
		OnlineSessions:    gOnlineSess,
		MsgQPSUp:          cMsgUp,
		MsgQPSDown:        cMsgDown,
		HandlerDurationMS: hHandler,
		MailboxLen:        hMailbox,
		MailboxDropTotal:  cDrop,
		MailboxKickTotal:  cKick,
		Rooms:             gRooms,
		Actors:            gActors,
		ActiveRooms:       gActiveRooms,
		MatchQueueDepth:   gMatchQueue,
		NodeState:         gNodeState,
		PanicTotal:        cPanic,
	}
}
