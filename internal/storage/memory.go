// Package storage 提供 framework.Storage 的具体实现。
//
// 阶段 5：MemoryStorage 进程内实现（单机形态/测试）；
// 阶段 6：在此包新增 Redis/MySQL 实现与异步写回、熔断（§10），业务代码零改动。
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"

	"github.com/rangame/server/pkg/framework"
)

// MemoryStorage 进程内 KV + 排行榜实现。零外部依赖，并发安全。
//
// 值以 JSON 序列化语义存取（Get 反序列化到调用方结构），
// 与阶段 6 的 MySQL/Redis 序列化行为保持一致，便于提前暴露字段标签问题。
type MemoryStorage struct {
	mu    sync.RWMutex
	kv    map[string]map[string]json.RawMessage // table -> key -> raw
	zset  map[string]map[string]int64           // table -> member -> score
	incr  map[string]map[string]int64           // table -> key -> counter
	close sync.Once
}

// NewMemoryStorage 创建内存存储。
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		kv:   make(map[string]map[string]json.RawMessage),
		zset: make(map[string]map[string]int64),
		incr: make(map[string]map[string]int64),
	}
}

func (s *MemoryStorage) Get(_ context.Context, table, key string, out any) error {
	s.mu.RLock()
	row, ok := s.kv[table]
	var raw json.RawMessage
	if ok {
		raw = row[key]
	}
	s.mu.RUnlock()
	if !ok || raw == nil {
		return framework.ErrStorageNotFound
	}
	return json.Unmarshal(raw, out)
}

func (s *MemoryStorage) Set(ctx context.Context, table, key string, val any) error {
	return s.set(table, key, val)
}

func (s *MemoryStorage) SetSync(ctx context.Context, table, key string, val any) error {
	return s.set(table, key, val)
}

func (s *MemoryStorage) set(table, key string, val any) error {
	raw, err := json.Marshal(val)
	if err != nil {
		return err
	}
	s.mu.Lock()
	row, ok := s.kv[table]
	if !ok {
		row = make(map[string]json.RawMessage)
		s.kv[table] = row
	}
	row[key] = raw
	s.mu.Unlock()
	return nil
}

func (s *MemoryStorage) Del(_ context.Context, table, key string) error {
	s.mu.Lock()
	if row, ok := s.kv[table]; ok {
		delete(row, key)
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStorage) IncrBy(_ context.Context, table, key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.incr[table]
	if !ok {
		row = make(map[string]int64)
		s.incr[table] = row
	}
	row[key] += delta
	return row[key], nil
}

func (s *MemoryStorage) Ranking() framework.Ranking { return (*memRanking)(s) }

func (s *MemoryStorage) Raw() any { return s }

func (s *MemoryStorage) Close() error {
	s.close.Do(func() {})
	return nil
}

// memRanking 复用 MemoryStorage 的锁与 zset 表。
type memRanking MemoryStorage

func (r *memRanking) zsetTable(table string, create bool) map[string]int64 {
	row, ok := r.zset[table]
	if !ok && create {
		row = make(map[string]int64)
		r.zset[table] = row
	}
	return row
}

func (r *memRanking) ZAdd(_ context.Context, table, member string, score int64) error {
	r.mu.Lock()
	r.zsetTable(table, true)[member] = score
	r.mu.Unlock()
	return nil
}

func (r *memRanking) ZIncrBy(_ context.Context, table, member string, delta int64) (int64, error) {
	r.mu.Lock()
	row := r.zsetTable(table, true)
	row[member] += delta
	v := row[member]
	r.mu.Unlock()
	return v, nil
}

func (r *memRanking) ZRevRange(_ context.Context, table string, start, stop int64) ([]framework.RankItem, error) {
	r.mu.RLock()
	row := r.zset[table]
	items := make([]framework.RankItem, 0, len(row))
	for m, sc := range row {
		items = append(items, framework.RankItem{Member: m, Score: sc})
	}
	r.mu.RUnlock()

	sort.Slice(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score > items[j].Score
		}
		return items[i].Member < items[j].Member // 同分按 member 字典序，保证确定性
	})

	n := int64(len(items))
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return []framework.RankItem{}, nil
	}
	return items[start : stop+1], nil
}

func (r *memRanking) ZRank(_ context.Context, table, member string) (int64, error) {
	r.mu.RLock()
	row, ok := r.zset[table]
	_, exists := row[member]
	items := make([]framework.RankItem, 0, len(row))
	if ok {
		for m, sc := range row {
			items = append(items, framework.RankItem{Member: m, Score: sc})
		}
	}
	r.mu.RUnlock()
	if !exists {
		return 0, framework.ErrStorageNotFound
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score < items[j].Score
		}
		return items[i].Member < items[j].Member
	})
	for i, it := range items {
		if it.Member == member {
			return int64(i), nil
		}
	}
	return 0, framework.ErrStorageNotFound
}

// 编译期保证接口实现。
var (
	_ framework.Storage = (*MemoryStorage)(nil)
	_ framework.Ranking = (*memRanking)(nil)
)

// IsNotFound 便捷判断。
func IsNotFound(err error) bool { return errors.Is(err, framework.ErrStorageNotFound) }
