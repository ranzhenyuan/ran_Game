package framework

import "time"

// MatchRule 匹配规则（模块 + 玩法内规则码 + 人数 + 段位段）。
// 定义在稳定面：RoomConfig 引用它，而 pkg/framework 不允许依赖 internal/match。
type MatchRule struct {
	Module   string
	Code     string // 玩法内规则码
	Players  int
	ScoreMin int64 // 段位段（阶段 5 简易匹配不校验，仅透传）
	ScoreMax int64
}

// RoomConfig 房间创建参数（由模块在注册 RoomLogic 时提供，§8.1）。
type RoomConfig struct {
	Module       string
	MaxPlayers   int
	MaxDuration  time.Duration // §8.2 排水/强结算上界；<=0 不限制
	TickInterval time.Duration // Tick 定时器间隔；<=0 由房间管理器填默认值
	Mailbox      MailboxPolicy
	MatchRule    MatchRule
}

// Player 房间内的玩家视图，仅暴露给 RoomLogic（RoomActor goroutine 内使用）。
type Player interface {
	UID() string
	// Online 会话是否绑定活跃连接（宽限离线态返回 false，玩法据此托管，§5.1）。
	Online() bool
	// Push 可靠通道单推（分配服务端 seq、进重放缓冲）。
	Push(msgID MsgID, msg any) error
	// PushSnapshot 快照通道单推（per-msgID latest-wins）。
	PushSnapshot(msgID MsgID, msg any) error
	// Profile 读取玩家档案快照（只读，§10.4.1）；返回 *PlayerProfile 不可修改。
	// 快照由 PlayerActor OnSessionStart 加载，通过 atomic.Pointer 安全暴露给 RoomActor。
	Profile() *PlayerProfile
}

// RoomCtx 房间上下文。所有方法仅允许在 RoomActor 自己的 goroutine 内调用（免锁，§7.3）。
type RoomCtx interface {
	RoomID() string
	Members() []Player
	Member(uid string) (Player, bool)

	// KV 房间级临时 KV（随房间销毁而消失）。
	KV(key string) any
	SetKV(key string, v any)

	// Broadcast 可靠通道广播（除 exceptUID 外全员）。
	Broadcast(msgID MsgID, msg any, exceptUID ...string)
	// BroadcastSnapshot 快照通道广播（flag bit6，latest-wins）。
	BroadcastSnapshot(msgID MsgID, msg any, exceptUID ...string)

	// Kick 将玩家移出房间并下发 2004 通知（不断连接）。
	Kick(uid string, code Code, reason string)
	// Close 触发 OnDestroy 并回收房间（排空邮箱后注销）。
	Close()

	After(d time.Duration, fn func()) Handle
	Every(d time.Duration, fn func()) Handle

	Storage() Storage
	// ProfileStore 玩家档案存取接口（§10.4）；RoomLogic 在 OnDestroy 结算时
	// 通过此接口 Patch/SaveSync 回写玩家档案（如 extra 累计字段、成就解锁）。
	ProfileStore() ProfileStore
}

// RoomLogic 游戏开发者唯一必须实现的业务接口（§8.1）。
// 全部回调都在同一个 RoomActor goroutine 内串行执行，实现内禁止开协程改状态。
type RoomLogic interface {
	OnCreate(r RoomCtx, cfg RoomConfig)
	OnJoin(r RoomCtx, p Player)
	OnMessage(r RoomCtx, p Player, env *Envelope)
	Tick(r RoomCtx, dt time.Duration)
	OnLeave(r RoomCtx, p Player)
	OnEmpty(r RoomCtx)
	OnDestroy(r RoomCtx)
}

// TimeoutLogic 可选实现：房间 MaxDuration 到期强结算（§8.2）。
type TimeoutLogic interface {
	OnTimeout(r RoomCtx)
}
