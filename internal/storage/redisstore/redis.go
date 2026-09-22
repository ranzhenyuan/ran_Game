// Package redisstore 是 storage.Backend 的 Redis 适配（架构文档 §10.2：
// 会话/token、排行榜天然匹配 Redis 的 TTL 与 ZSet）。
//
// key 布局：{prefix}:kv:{table}:{key}；计数器 {prefix}:ctr:{table}:{key}；
// ZSet 直接以 {prefix}:zset:{table} 为键。
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rangame/server/internal/storage"
	"github.com/rangame/server/pkg/framework"
)

// Config Redis 后端配置。
type Config struct {
	Addr     string
	Password string
	DB       int
	// KeyPrefix 业务键前缀（多实例共集群时隔离），默认 "rangame"。
	KeyPrefix string
	// DialTimeout 建连/Ping 超时，默认 2s。
	DialTimeout time.Duration
	// MaxRetries 命令失败重试次数，默认沿用 go-redis（3）；测试可调 0 快速失败。
	MaxRetries int
}

// New 创建并 Ping 校验连通性；失败返回错误（装配期快速失败）。
func New(ctx context.Context, cfg Config) (*Backend, error) {
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "rangame"
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 2 * time.Second
	}
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.DialTimeout,
		WriteTimeout: cfg.DialTimeout,
		MaxRetries:   cfg.MaxRetries,
	})
	pingCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisstore: ping %q: %w", cfg.Addr, err)
	}
	return &Backend{client: client, prefix: cfg.KeyPrefix}, nil
}

// Backend Redis 存储后端。
type Backend struct {
	client *redis.Client
	prefix string
}

func (b *Backend) kvKey(table, key string) string {
	return strings.Join([]string{b.prefix, "kv", table, key}, ":")
}
func (b *Backend) ctrKey(table, key string) string {
	return strings.Join([]string{b.prefix, "ctr", table, key}, ":")
}
func (b *Backend) zsetKey(table string) string {
	return strings.Join([]string{b.prefix, "zset", table}, ":")
}

func (b *Backend) Get(ctx context.Context, table, key string) ([]byte, error) {
	raw, err := b.client.Get(ctx, b.kvKey(table, key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, framework.ErrStorageNotFound
	}
	return raw, err
}

func (b *Backend) Put(ctx context.Context, table, key string, raw []byte) error {
	return b.client.Set(ctx, b.kvKey(table, key), raw, 0).Err()
}

func (b *Backend) Delete(ctx context.Context, table, key string) error {
	return b.client.Del(ctx, b.kvKey(table, key)).Err()
}

func (b *Backend) IncrBy(ctx context.Context, table, key string, delta int64) (int64, error) {
	return b.client.IncrBy(ctx, b.ctrKey(table, key), delta).Result()
}

func (b *Backend) Ranking() framework.Ranking { return &ranking{b: b} }

func (b *Backend) Ping(ctx context.Context) error { return b.client.Ping(ctx).Err() }
func (b *Backend) Raw() any                       { return b.client }
func (b *Backend) Close() error                   { return b.client.Close() }

// ranking ZSet 适配。
type ranking struct{ b *Backend }

func (r *ranking) ZAdd(ctx context.Context, table, member string, score int64) error {
	return r.b.client.ZAdd(ctx, r.b.zsetKey(table), redis.Z{Score: float64(score), Member: member}).Err()
}

func (r *ranking) ZIncrBy(ctx context.Context, table, member string, delta int64) (int64, error) {
	v, err := r.b.client.ZIncrBy(ctx, r.b.zsetKey(table), float64(delta), member).Result()
	return int64(v), err
}

func (r *ranking) ZRevRange(ctx context.Context, table string, start, stop int64) ([]framework.RankItem, error) {
	zs, err := r.b.client.ZRevRangeWithScores(ctx, r.b.zsetKey(table), start, stop).Result()
	if err != nil {
		return nil, err
	}
	items := make([]framework.RankItem, 0, len(zs))
	for _, z := range zs {
		member, _ := z.Member.(string)
		items = append(items, framework.RankItem{Member: member, Score: int64(z.Score)})
	}
	return items, nil
}

func (r *ranking) ZRank(ctx context.Context, table, member string) (int64, error) {
	// Redis ZSet 天然按"分数升序、同分 member 字典序升序"排名，与内存实现一致。
	n, err := r.b.client.ZRank(ctx, r.b.zsetKey(table), member).Result()
	if errors.Is(err, redis.Nil) {
		return 0, framework.ErrStorageNotFound
	}
	return n, err
}

// 编译期断言：redisstore 实现 Backend 与 Ranking。
var (
	_ storage.Backend   = (*Backend)(nil)
	_ framework.Ranking = (*ranking)(nil)
)
