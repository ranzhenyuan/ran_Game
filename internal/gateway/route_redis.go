package gateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rangame/server/internal/cluster"
)

// Redis 路由表 key 前缀（与 NodeRegistry 的 cluster:node:* 同命名空间）。
const (
	routeKeyPrefix  = "cluster:route:"      // cluster:route:{uid}  → STRING nodeID
	routeNodePrefix = "cluster:route:node:" // cluster:route:node:{nodeID} → SET uid
)

// 本地缓存 TTL：Gateway 侧 Lookup 打 Redis 前先查本地，命中即免一次 RTT。
// 3s 不一致窗口安全：最坏转发到已释放会话的旧节点，Logic 侧 sessions.Get 会校验并回错。
const routeCacheTTL = 3 * time.Second

// luaBind 原子写入 uid→nodeID 并维护反向集合：
//   - 若旧归属与新 nodeID 不同，先从旧节点的反向集合移除 uid（节点迁移/顶踢）；
//   - SET 路由 + TTL，SADD 反向集 + 同 TTL。
//
// 用 Lua 保证读改写原子，避免并发 Bind（顶踢/重连跨节点）产生脏映射。
const luaBind = `
local key = KEYS[1]
local setKey = KEYS[2]
local uid = ARGV[1]
local nodeID = ARGV[2]
local ttl = tonumber(ARGV[3])
local nodeSetPrefix = ARGV[4]
local old = redis.call('GET', key)
if old and old ~= nodeID then
  redis.call('SREM', nodeSetPrefix .. old, uid)
end
redis.call('SET', key, nodeID, 'EX', ttl)
redis.call('SADD', setKey, uid)
redis.call('EXPIRE', setKey, ttl)
return 1
`

// luaUnbind 条件删除：仅当 uid 当前归属 nodeID 时才删路由与反向集成员。
// 防顶踢/快速重连后旧会话释放误删新节点刚写入的映射。
const luaUnbind = `
local key = KEYS[1]
local setKey = KEYS[2]
local uid = ARGV[1]
local nodeID = ARGV[2]
local cur = redis.call('GET', key)
if cur == nodeID then
  redis.call('DEL', key)
  redis.call('SREM', setKey, uid)
  return 1
end
return 0
`

// RedisRouteTable 分布式一级路由表（§12.4 多 Gateway 副本实现）。
//
// 写入主体是 Logic（会话建立/销毁的唯一知情方）；Gateway 侧只读 + 本地短 TTL 缓存。
// 节点 Down 时通过 registry Watch 事件 Invalidate 该节点全部路由。
type RedisRouteTable struct {
	client   *redis.Client
	registry cluster.NodeRegistry
	ttl      time.Duration

	cacheMu sync.RWMutex
	cache   map[string]cacheEntry // uid -> {nodeID, expireAt}

	ch     <-chan cluster.ChangeEvent
	stopCh chan struct{}
}

type cacheEntry struct {
	nodeID   string
	expireAt time.Time
}

// NewRedisRouteTable 创建 Redis 路由表。ttl<=0 时用默认 24h（仅防崩溃僵尸映射）。
func NewRedisRouteTable(client *redis.Client, registry cluster.NodeRegistry, ttl time.Duration) *RedisRouteTable {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	t := &RedisRouteTable{
		client:   client,
		registry: registry,
		ttl:      ttl,
		cache:    make(map[string]cacheEntry),
		stopCh:   make(chan struct{}),
	}
	if registry != nil {
		t.ch = registry.Watch()
		go t.watchLoop()
	}
	return t
}

// Lookup 先查本地缓存，miss 再打 Redis 并回填。
func (t *RedisRouteTable) Lookup(uid string) (string, error) {
	now := time.Now()
	t.cacheMu.RLock()
	if e, ok := t.cache[uid]; ok && now.Before(e.expireAt) {
		t.cacheMu.RUnlock()
		return e.nodeID, nil
	}
	t.cacheMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	nodeID, err := t.client.Get(ctx, routeKeyPrefix+uid).Result()
	if err == redis.Nil {
		return "", ErrNotRouted
	}
	if err != nil {
		return "", fmt.Errorf("route lookup: %w", err)
	}
	t.cacheMu.Lock()
	t.cache[uid] = cacheEntry{nodeID: nodeID, expireAt: now.Add(routeCacheTTL)}
	t.cacheMu.Unlock()
	return nodeID, nil
}

// Bind 由 Logic 在登录/重连成功后调用：原子写路由 + 维护反向集。
func (t *RedisRouteTable) Bind(uid, nodeID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := t.client.Eval(ctx, luaBind,
		[]string{routeKeyPrefix + uid, routeNodePrefix + nodeID},
		uid, nodeID, int64(t.ttl.Seconds()), routeNodePrefix,
	).Result()
	if err != nil {
		return fmt.Errorf("route bind: %w", err)
	}
	return nil
}

// Unbind 由 Logic 在会话释放时调用：条件删除（防顶踢误删）。
func (t *RedisRouteTable) Unbind(uid, nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = t.client.Eval(ctx, luaUnbind,
		[]string{routeKeyPrefix + uid, routeNodePrefix + nodeID},
		uid, nodeID,
	).Result()
	t.evictCache(uid)
}

// Invalidate 节点故障/下线：清掉归属该节点的全部路由与反向集（不 SCAN 全库）。
func (t *RedisRouteTable) Invalidate(nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	setKey := routeNodePrefix + nodeID
	uids, err := t.client.SMembers(ctx, setKey).Result()
	if err != nil && err != redis.Nil {
		return
	}
	pipe := t.client.Pipeline()
	for _, uid := range uids {
		pipe.Del(ctx, routeKeyPrefix+uid)
	}
	pipe.Del(ctx, setKey)
	_, _ = pipe.Exec(ctx)

	// 清本地缓存中归属该节点的条目。
	t.cacheMu.Lock()
	for uid, e := range t.cache {
		if e.nodeID == nodeID {
			delete(t.cache, uid)
		}
	}
	t.cacheMu.Unlock()
}

func (t *RedisRouteTable) evictCache(uid string) {
	t.cacheMu.Lock()
	delete(t.cache, uid)
	t.cacheMu.Unlock()
}

func (t *RedisRouteTable) watchLoop() {
	if t.ch == nil {
		return
	}
	for {
		select {
		case <-t.stopCh:
			return
		case ev, ok := <-t.ch:
			if !ok {
				return
			}
			if ev.New == cluster.StateDown {
				t.Invalidate(ev.NodeID)
			}
		}
	}
}

// Close 停止 Watch 协程（不关闭共享的 redis client，生命周期由调用方管理）。
func (t *RedisRouteTable) Close() {
	select {
	case <-t.stopCh:
	default:
		close(t.stopCh)
	}
}
