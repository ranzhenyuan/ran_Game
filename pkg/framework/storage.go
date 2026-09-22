package framework

import (
	"context"
	"errors"
)

// ErrStorageNotFound 存储中不存在指定 key（阶段 6 MySQL/Redis 实现同样复用）。
var ErrStorageNotFound = errors.New("framework: storage key not found")

// Storage 业务侧存储抽象（架构文档 §10）。游戏代码只依赖本接口；
// 阶段 5 提供进程内内存实现，阶段 6 替换为 Redis/MySQL + 异步写回/熔断，业务零改动。
type Storage interface {
	// Get 读取并反序列化到 out；不存在返回 ErrStorageNotFound。
	Get(ctx context.Context, table, key string, out any) error
	// Set 异步入写回队列（立即对本节点后续 Get 可见，§10.1）。
	Set(ctx context.Context, table, key string, val any) error
	// SetSync 强一致同步路径（结算等关键路径）。
	SetSync(ctx context.Context, table, key string, val any) error
	// Del 删除。
	Del(ctx context.Context, table, key string) error
	// IncrBy 原子计数，返回递增后的值。
	IncrBy(ctx context.Context, table, key string, delta int64) (int64, error)
	// Ranking 排行榜能力（ZSET 语义）。
	Ranking() Ranking
	// Raw 逃生口：返回底层客户端（如 *redis.Client / *sql.DB）供 Pipeline/事务。
	// ⚠️ 经 Raw 的调用绕过异步写回与熔断保护，超时重试业务自负（§10.2）。
	// 无底层客户端的实现（内存存储）返回 nil。
	Raw() any
	// Close 关闭并 flush 写回队列。
	Close() error
}

// RankItem 排行榜条目。
type RankItem struct {
	Member string
	Score  int64
}

// Ranking ZSET 语义排行榜（Redis 逃生语义同构）。
type Ranking interface {
	ZAdd(ctx context.Context, table, member string, score int64) error
	ZIncrBy(ctx context.Context, table, member string, delta int64) (int64, error)
	// ZRevRange 按分数倒序取 [start, stop]（含两端，-1 表示到末尾）。
	ZRevRange(ctx context.Context, table string, start, stop int64) ([]RankItem, error)
	// ZRank 返回正序排名（0 起）；不存在返回 ErrStorageNotFound。
	ZRank(ctx context.Context, table, member string) (int64, error)
}
