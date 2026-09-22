package storage

import (
	"context"

	"github.com/rangame/server/pkg/framework"
)

// Backend 远程持久化后端的最小写面（Redis/MySQL 适配实现）。
// 注意：Backend 的方法全部同步直达；异步/批量/熔断语义由上层 Store 门面负责（§10.2）。
type Backend interface {
	// Get 取原始 JSON 字节；不存在返回 framework.ErrStorageNotFound。
	Get(ctx context.Context, table, key string) ([]byte, error)
	// Put 写入原始 JSON 字节（upsert 语义）。
	Put(ctx context.Context, table, key string, raw []byte) error
	// Delete 删除。
	Delete(ctx context.Context, table, key string) error
	// IncrBy 原子计数，返回递增后值。
	IncrBy(ctx context.Context, table, key string, delta int64) (int64, error)
	// Ranking 排行榜（Redis ZSet / SQL 有序表）。
	Ranking() framework.Ranking
	// Ping 健康探测（熔断器半开恢复用）。
	Ping(ctx context.Context) error
	// Raw 逃生口：返回底层客户端（*redis.Client / *sql.DB），经此调用绕过所有保护（§10.2）。
	Raw() any
	// Close 关闭连接池。
	Close() error
}
