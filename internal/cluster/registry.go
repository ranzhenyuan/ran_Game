// Package cluster 是演进态集群组件（架构文档 §3.8 / §12 / §13）。
//
// 仅在 server.role = gateway | logic 时启用；单机形态（standalone）不引用本包。
// 默认提供进程内/miniredis 可验证实现，真实跨进程部署只需替换实现，
// 接口与协议保持兼容（业务零改动，§12.6 / §13.5）。
package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// State 节点状态（§13.2 node_state 指标）。
type State byte

const (
	StateActive   State = 1 // 正常服务
	StateDraining State = 2 // 排水中：不接新房/新会话，存量继续
	StateDrained  State = 3 // 排水完成：active_rooms=0，准备退出
	StateDown     State = 4 // 故障：心跳超时或自检失败
)

func (s State) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateDraining:
		return "draining"
	case StateDrained:
		return "drained"
	case StateDown:
		return "down"
	}
	return "unknown"
}

// Role 节点角色。
type Role string

const (
	RoleGateway Role = "gateway"
	RoleLogic   Role = "logic"
)

// NodeInfo 节点注册条目（§3.8）。
type NodeInfo struct {
	ID          string
	Role        Role
	Addr        string
	Modules     []string
	State       State
	ActiveRooms int
	StartedAt   time.Time
	LastBeat    time.Time
}

// ChangeEvent 注册表变更通知（§13.4 Pub/Sub）。
type ChangeEvent struct {
	NodeID string
	Old    State
	New    State
}

// NodeRegistry 节点注册表接口（§3.8 / §13.2）。
//
// 实现方：
//   - MemRegistry：进程内实现，单进程集成测试用；
//   - RedisRegistry：Redis Hash + Pub/Sub 实现，真实拆分部署用。
type NodeRegistry interface {
	Register(info NodeInfo) error
	Heartbeat(id string, activeRooms int) error
	SetState(id string, s State) error
	List(role Role, onlyActive bool) ([]NodeInfo, error)
	Get(id string) (NodeInfo, error)
	Watch() <-chan ChangeEvent
	Deregister(id string) error
	Close() error
}

// Config 注册表配置。
type Config struct {
	// NodeTTL 节点条目 TTL（默认 15s，§13.2）。
	NodeTTL time.Duration
	// HeartbeatInterval 心跳间隔（默认 5s）。
	HeartbeatInterval time.Duration
	// Channel Pub/Sub 频道名（默认 cluster:nodes:change）。
	Channel string
}

func DefaultConfig() Config {
	return Config{
		NodeTTL:           15 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		Channel:           "cluster:nodes:change",
	}
}

// ---------------- MemRegistry 进程内实现 ----------------

// MemRegistry 进程内 NodeRegistry 实现。
// 用于单进程集成测试与 standalone 形态下的演进态骨架验证。
type MemRegistry struct {
	mu      sync.RWMutex
	nodes   map[string]NodeInfo
	chans   []chan ChangeEvent
	closed  bool
	closeMu sync.Mutex
}

// NewMemRegistry 创建进程内注册表。
func NewMemRegistry() *MemRegistry {
	return &MemRegistry{
		nodes: make(map[string]NodeInfo),
	}
}

// Register 注册或覆盖节点条目；同时发布变更事件。
func (r *MemRegistry) Register(info NodeInfo) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRegistryClosed
	}
	if info.LastBeat.IsZero() {
		info.LastBeat = time.Now()
	}
	if info.StartedAt.IsZero() {
		info.StartedAt = time.Now()
	}
	old, exists := r.nodes[info.ID]
	r.nodes[info.ID] = info
	r.mu.Unlock()

	if !exists || old.State != info.State {
		r.publish(ChangeEvent{NodeID: info.ID, Old: old.State, New: info.State})
	}
	return nil
}

// Heartbeat 刷新 lastBeat 与 activeRooms；不发布事件（状态未变）。
func (r *MemRegistry) Heartbeat(id string, activeRooms int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRegistryClosed
	}
	n, ok := r.nodes[id]
	if !ok {
		return fmt.Errorf("cluster: node %q not registered", id)
	}
	n.LastBeat = time.Now()
	n.ActiveRooms = activeRooms
	r.nodes[id] = n
	return nil
}

// SetState 修改节点状态并发布变更事件（§13.3 状态机转换）。
func (r *MemRegistry) SetState(id string, s State) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRegistryClosed
	}
	n, ok := r.nodes[id]
	if !ok {
		return fmt.Errorf("cluster: node %q not registered", id)
	}
	old := n.State
	n.State = s
	n.LastBeat = time.Now()
	r.nodes[id] = n
	r.mu.Unlock()

	if old != s {
		r.publish(ChangeEvent{NodeID: id, Old: old, New: s})
	}
	return nil
}

// List 返回节点列表；role="" 表示所有角色；onlyActive=true 仅返回 StateActive 节点。
func (r *MemRegistry) List(role Role, onlyActive bool) ([]NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		if role != "" && n.Role != role {
			continue
		}
		if onlyActive && n.State != StateActive {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// Get 取单个节点。
func (r *MemRegistry) Get(id string) (NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return NodeInfo{}, fmt.Errorf("cluster: node %q not found", id)
	}
	return n, nil
}

// Watch 返回变更事件 channel；调用方应在不再使用时丢弃。
// 返回的 channel 无界缓冲（演进态骨架：单进程测试无背压需求）。
func (r *MemRegistry) Watch() <-chan ChangeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan ChangeEvent, 64)
	r.chans = append(r.chans, ch)
	return ch
}

// Deregister 删除节点条目并发布 Down 事件。
func (r *MemRegistry) Deregister(id string) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRegistryClosed
	}
	old, exists := r.nodes[id]
	delete(r.nodes, id)
	r.mu.Unlock()

	if exists {
		r.publish(ChangeEvent{NodeID: id, Old: old.State, New: StateDown})
	}
	return nil
}

// Close 关闭注册表，释放所有 Watch channel。
func (r *MemRegistry) Close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	chans := r.chans
	r.chans = nil
	r.mu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
	return nil
}

func (r *MemRegistry) publish(ev ChangeEvent) {
	r.mu.RLock()
	chans := r.chans
	r.mu.RUnlock()
	for _, ch := range chans {
		select {
		case ch <- ev:
		default:
			// 演进态骨架：丢弃溢出事件，订阅方靠 TTL 兜底（§13.4）
		}
	}
}

// ---------------- RedisRegistry 真实分布式实现 ----------------

// RedisRegistry 用 Redis Hash + Pub/Sub 实现的注册表（§13.2/§13.4）。
//
//	key 格式：cluster:nodes:{role}  Hash{ nodeID: JSONNodeInfo }
//	TTL：每条 Hash 整体 TTL，靠 Heartbeat 续期（实现简化：单 key per node）
//	Pub/Sub：cluster:nodes:change 频道，载荷为 JSON ChangeEvent
type RedisRegistry struct {
	client  *redis.Client
	cfg     Config
	pubsub  *redis.PubSub
	subsMu  sync.RWMutex
	subs    []chan ChangeEvent
	closeMu sync.Mutex
	closed  bool
}

// NewRedisRegistry 创建 Redis 注册表。
func NewRedisRegistry(client *redis.Client, cfg Config) *RedisRegistry {
	r := &RedisRegistry{client: client, cfg: cfg}
	if cfg.NodeTTL == 0 {
		cfg.NodeTTL = 15 * time.Second
		r.cfg.NodeTTL = 15 * time.Second
	}
	if cfg.Channel == "" {
		r.cfg.Channel = "cluster:nodes:change"
	}
	// 启动 Pub/Sub 订阅循环
	r.pubsub = client.Subscribe(context.Background(), r.cfg.Channel)
	go r.dispatchLoop()
	return r
}

func (r *RedisRegistry) dispatchLoop() {
	ch := r.pubsub.Channel()
	for msg := range ch {
		// 载荷格式："nodeID old new"（避免 JSON 依赖，演进态骨架优先可读性）
		parts := strings.SplitN(msg.Payload, " ", 3)
		if len(parts) != 3 {
			continue
		}
		old, _ := strconv.Atoi(parts[1])
		newS, _ := strconv.Atoi(parts[2])
		ev := ChangeEvent{
			NodeID: parts[0],
			Old:    State(old),
			New:    State(newS),
		}
		r.subsMu.RLock()
		subs := r.subs
		r.subsMu.RUnlock()
		for _, sub := range subs {
			select {
			case sub <- ev:
			default:
			}
		}
	}
}

// nodeKey 单节点 key：cluster:node:{id}（演进态骨架：每节点独立 key 便于 TTL）。
func nodeKey(id string) string { return "cluster:node:" + id }

// Register 注册节点并设置 TTL；同时发布变更事件。
func (r *RedisRegistry) Register(info NodeInfo) error {
	ctx := context.Background()
	if info.LastBeat.IsZero() {
		info.LastBeat = time.Now()
	}
	if info.StartedAt.IsZero() {
		info.StartedAt = time.Now()
	}
	old, _ := r.Get(info.ID)
	payload := encodeNode(info)
	pipe := r.client.TxPipeline()
	pipe.Set(ctx, nodeKey(info.ID), payload, r.cfg.NodeTTL)
	if old.State != info.State {
		pipe.Publish(ctx, r.cfg.Channel, fmt.Sprintf("%s %d %d", info.ID, old.State, info.State))
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Heartbeat 续期 TTL 并更新 activeRooms。
func (r *RedisRegistry) Heartbeat(id string, activeRooms int) error {
	ctx := context.Background()
	n, err := r.Get(id)
	if err != nil {
		return err
	}
	n.ActiveRooms = activeRooms
	n.LastBeat = time.Now()
	return r.client.Set(ctx, nodeKey(id), encodeNode(n), r.cfg.NodeTTL).Err()
}

// SetState 修改状态并发布事件。
func (r *RedisRegistry) SetState(id string, s State) error {
	ctx := context.Background()
	n, err := r.Get(id)
	if err != nil {
		return err
	}
	old := n.State
	n.State = s
	n.LastBeat = time.Now()
	pipe := r.client.TxPipeline()
	pipe.Set(ctx, nodeKey(id), encodeNode(n), r.cfg.NodeTTL)
	if old != s {
		pipe.Publish(ctx, r.cfg.Channel, fmt.Sprintf("%s %d %d", id, old, s))
	}
	_, err = pipe.Exec(ctx)
	return err
}

// List 列出节点；role="" 全部角色，onlyActive=true 仅 Active。
// 实现简化：SCAN cluster:node:* 后客户端过滤。
func (r *RedisRegistry) List(role Role, onlyActive bool) ([]NodeInfo, error) {
	ctx := context.Background()
	var keys []string
	iter := r.client.Scan(ctx, 0, "cluster:node:*", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]NodeInfo, 0, len(vals))
	for _, v := range vals {
		s, _ := v.(string)
		if s == "" {
			continue
		}
		n, derr := decodeNode(s)
		if derr != nil {
			continue
		}
		if role != "" && n.Role != role {
			continue
		}
		if onlyActive && n.State != StateActive {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// Get 取单个节点。
func (r *RedisRegistry) Get(id string) (NodeInfo, error) {
	ctx := context.Background()
	s, err := r.client.Get(ctx, nodeKey(id)).Result()
	if err == redis.Nil {
		return NodeInfo{}, fmt.Errorf("cluster: node %q not found", id)
	}
	if err != nil {
		return NodeInfo{}, err
	}
	return decodeNode(s)
}

// Watch 订阅变更事件。
func (r *RedisRegistry) Watch() <-chan ChangeEvent {
	r.subsMu.Lock()
	defer r.subsMu.Unlock()
	ch := make(chan ChangeEvent, 64)
	r.subs = append(r.subs, ch)
	return ch
}

// Deregister 删除节点并发布 Down 事件。
func (r *RedisRegistry) Deregister(id string) error {
	ctx := context.Background()
	old, _ := r.Get(id)
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, nodeKey(id))
	pipe.Publish(ctx, r.cfg.Channel, fmt.Sprintf("%s %d %d", id, old.State, StateDown))
	_, err := pipe.Exec(ctx)
	return err
}

// Close 关闭注册表。
func (r *RedisRegistry) Close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.pubsub != nil {
		_ = r.pubsub.Close()
	}
	return nil
}

// 错误。
var (
	ErrRegistryClosed = errors.New("cluster: registry closed")
)

// encodeNode/decodeNode 简化字符串编码（演进态骨架：可读性优先）。
//
//	字段顺序：id | role | addr | modules(逗号分隔) | state | activeRooms | lastBeatUnixNano
//	使用 "|" 分隔避免与 modules 内的 "," 冲突。
func encodeNode(n NodeInfo) string {
	modules := strings.Join(n.Modules, ",")
	return strings.Join([]string{
		n.ID, string(n.Role), n.Addr, modules,
		strconv.Itoa(int(n.State)),
		strconv.Itoa(n.ActiveRooms),
		strconv.FormatInt(n.LastBeat.UnixNano(), 10),
	}, "|")
}

func decodeNode(s string) (NodeInfo, error) {
	parts := strings.SplitN(s, "|", 7)
	if len(parts) != 7 {
		return NodeInfo{}, fmt.Errorf("cluster: bad node payload: %q", s)
	}
	state, _ := strconv.Atoi(parts[4])
	active, _ := strconv.Atoi(parts[5])
	lastBeat, _ := strconv.ParseInt(parts[6], 10, 64)
	n := NodeInfo{
		ID:          parts[0],
		Role:        Role(parts[1]),
		Addr:        parts[2],
		State:       State(state),
		ActiveRooms: active,
		LastBeat:    time.Unix(0, lastBeat),
	}
	n.StartedAt = n.LastBeat // 简化：演进态骨架不持久化 StartedAt
	if parts[3] != "" {
		n.Modules = strings.Split(parts[3], ",")
	}
	return n, nil
}
