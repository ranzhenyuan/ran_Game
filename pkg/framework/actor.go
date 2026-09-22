package framework

import "time"

// ID Actor 标识。本期为进程内 uint64；演进态扩为 {nodeID, localID}（§12.6）。
type ID uint64

// Handle 可取消句柄（定时器等）。
type Handle interface {
	Cancel()
}

// Envelope 跨 Actor 传递的消息信封（§7.1）。
// Payload 所有权随消息转移，禁止在多个 Actor 间共享同一可变对象（§7.3 红线 2）。
type Envelope struct {
	MsgID   MsgID
	Seq     uint32
	Payload any
	Sender  ID
	// SenderUID 发送方玩家 UID（由路由器在入箱前填入）；
	// 房间消息据此定位 RoomActor 内的成员，系统/内部消息为空。
	SenderUID string
	TraceID   string
}

// Actor 业务 Actor 实现接口；所有方法仅在该 Actor 自己的 goroutine 被调用，
// 因此实现内无需加锁（§7.3 红线 1）。
type Actor interface {
	Init(ctx ActorCtx)
	OnMessage(ctx ActorCtx, env *Envelope)
	OnStop(ctx ActorCtx)
}

// ActorCtx Actor 运行上下文，由引擎在 Actor goroutine 内提供。
type ActorCtx interface {
	Self() ID
	// Tell 向目标 Actor 异步投递一条消息（非阻塞，满箱按对方策略处理）。
	Tell(target ID, env *Envelope) error
	// Spawn 以当前 Actor 为父创建子 Actor（子停止时父收到 WatchChild 通知）。
	Spawn(child Actor, opts ...SpawnOption) (ID, error)
	// Stop 请求停止自身（优雅：排空已入箱消息后退出，§11.3）。
	Stop()
	// After/Every 定时器回调保证在本 Actor goroutine 内执行（§9）。
	After(d time.Duration, fn func()) Handle
	Every(d time.Duration, fn func()) Handle
}

// MailboxPolicy 邮箱容量与满箱策略（§7.2）。
type MailboxPolicy struct {
	// Capacity 有界邮箱容量，<=0 用引擎默认值（1024）。
	Capacity int
	// OnFull 满箱策略：drop | kick | block。
	OnFull string
	// DrainBatch 单次唤醒最多批量处理的消息数，<=0 用默认值（64）。
	DrainBatch int
}

const (
	FullDrop  = "drop"  // 丢弃新消息并计数（快照/日志类）
	FullKick  = "kick"  // 停掉 Actor（默认，个人 Actor 即踢会话）
	FullBlock = "block" // 阻塞投递（仅停机排水路径与强一致场景）
)

// SpawnOption Spawn 可选项。
type SpawnOption func(*SpawnConfig)

// SpawnConfig Spawn 选项集合。
type SpawnConfig struct {
	Policy *MailboxPolicy
}

// WithPolicy 指定子 Actor 邮箱策略。
func WithPolicy(p MailboxPolicy) SpawnOption {
	return func(c *SpawnConfig) { c.Policy = &p }
}
