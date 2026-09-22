// cluster_test.go 演进态骨架测试（架构文档 §13 走查 13/14/15 佐证）。
//
// 覆盖：
//   走查 13（扩容）：NodeRegistry 注册/发现/Watch 变更事件
//   走查 14（排水缩容）：DrainController Active→Draining→Drained 状态机
//   走查 15（突发防护）：RedisMatcher 队列深度作为扩容领先信号（§13.8）
//   走查 12（拆分转发）相关测试在 internal/gateway 包，避免循环依赖。

package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rangame/server/pkg/framework"
)

// ---------------- 走查 13：扩容 / 节点注册 ----------------

// TestMemRegistry_RegisterWatch 验证注册表注册/List/Watch。
func TestMemRegistry_RegisterWatch(t *testing.T) {
	reg := NewMemRegistry()
	defer reg.Close()

	ch := reg.Watch()

	_ = reg.Register(NodeInfo{
		ID:      "logic-0",
		Role:    RoleLogic,
		Modules: []string{"snake"},
		State:   StateActive,
	})

	// List Active logic 节点
	logics, _ := reg.List(RoleLogic, true)
	if len(logics) != 1 || logics[0].ID != "logic-0" {
		t.Fatalf("expected 1 active logic, got %v", logics)
	}

	// 验证 Watch 事件：注册即发布 Active 事件
	select {
	case ev := <-ch:
		if ev.NodeID != "logic-0" || ev.New != StateActive {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("watch event not received")
	}

	// 状态变更
	_ = reg.SetState("logic-0", StateDraining)
	select {
	case ev := <-ch:
		if ev.Old != StateActive || ev.New != StateDraining {
			t.Fatalf("unexpected transition: %+v", ev)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("drain event not received")
	}
}

// TestRedisRegistry_miniredis RedisRegistry 基本流程（miniredis）。
func TestRedisRegistry_miniredis(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	reg := NewRedisRegistry(client, Config{
		NodeTTL: 5 * time.Second,
		Channel: "cluster:nodes:change",
	})
	defer reg.Close()

	_ = reg.Register(NodeInfo{
		ID:      "logic-0",
		Role:    RoleLogic,
		Addr:    "127.0.0.1:7000",
		Modules: []string{"snake"},
		State:   StateActive,
	})

	got, err := reg.Get("logic-0")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Role != RoleLogic || got.State != StateActive || len(got.Modules) != 1 || got.Modules[0] != "snake" {
		t.Fatalf("unexpected node: %+v", got)
	}

	logics, _ := reg.List(RoleLogic, true)
	if len(logics) != 1 {
		t.Fatalf("expected 1 logic node, got %d", len(logics))
	}
}

// ---------------- 走查 14：排水缩容 ----------------

// TestDrainController_StateMachine 验证 Active→Draining→Drained 状态机。
func TestDrainController_StateMachine(t *testing.T) {
	var rooms atomic.Int32
	var sessions atomic.Int32
	rooms.Store(2)
	sessions.Store(1)

	roomsSrc := func() int { return int(rooms.Load()) }
	sessionsSrc := func() int { return int(sessions.Load()) }
	forceSettle := func() {}

	d := NewDrainController(roomsSrc, sessionsSrc, forceSettle, nil, "logic-0")
	if d.State() != StateActive {
		t.Fatalf("expected Active, got %s", d.State())
	}

	// 进入 Draining
	if err := d.Drain(30 * time.Minute); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if d.State() != StateDraining {
		t.Fatalf("expected Draining, got %s", d.State())
	}

	// 房间未清空：WaitDrained 应阻塞
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := d.WaitDrained(ctx); err == nil {
		t.Fatalf("expected WaitDrained to block (rooms>0)")
	}

	// 房间归零，但会话还在：仍阻塞
	rooms.Store(0)
	drainer := d.(*drainController)
	drainer.notifyRoomChange()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	if err := d.WaitDrained(ctx2); err == nil {
		t.Fatalf("expected WaitDrained to block (sessions>0)")
	}

	// 会话归零：Drained
	sessions.Store(0)
	drainer.notifyRoomChange()
	ctx3, cancel3 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel3()
	if err := d.WaitDrained(ctx3); err != nil {
		t.Fatalf("expected Drained, err=%v", err)
	}
	if d.State() != StateDrained {
		t.Fatalf("expected Drained, got %s", d.State())
	}
}

// ---------------- 走查 15：突发防护 ----------------

// TestRedisMatcher_QueueDepth 验证队列深度作为扩容领先信号（§13.8）。
// 演进态骨架：仅验证 ZSET 入队/未成桌时 QueueDepth 递增。
func TestRedisMatcher_QueueDepth(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	var settled atomic.Int32
	matcher := NewRedisMatcher(client, RedisMatcherConfig{
		KeyPrefix: "match:queue:",
		QueueTTL:  5 * time.Minute,
	}, func(uids []string, rule framework.MatchRule, nodeID string) (string, error) {
		settled.Add(1)
		return "room-0", nil
	}, func(module string) (string, error) {
		return "logic-0", nil
	}, func(uid string, seq uint32, roomID string, err error) {
		// 测试不验证回调
	})

	rule := framework.MatchRule{Module: "snake", Code: "ranked", Players: 4}
	// 入队 3 人（< 4）不应成桌
	for i, uid := range []string{"a", "b", "c"} {
		if err := matcher.Enqueue(uid, rule, uint32(i+1)); err != nil {
			t.Fatalf("enqueue %s: %v", uid, err)
		}
	}
	if qd := matcher.QueueDepth("snake", "ranked"); qd != 3 {
		t.Fatalf("queue depth: want 3, got %d", qd)
	}
	if settled.Load() != 0 {
		t.Fatalf("expected no settle, got %d", settled.Load())
	}

	// 第 4 人入队触发成桌
	if err := matcher.Enqueue("d", rule, 4); err != nil {
		t.Fatalf("enqueue d: %v", err)
	}
	if settled.Load() != 1 {
		t.Fatalf("expected 1 settle, got %d", settled.Load())
	}
	// 成桌后队列应为空
	if qd := matcher.QueueDepth("snake", "ranked"); qd != 0 {
		t.Fatalf("queue depth after match: want 0, got %d", qd)
	}
}

// TestRedisMatcher_AlreadyQueued 验证重复入队返回错误。
func TestRedisMatcher_AlreadyQueued(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	matcher := NewRedisMatcher(client, RedisMatcherConfig{
		KeyPrefix: "match:queue:",
		QueueTTL:  5 * time.Minute,
	}, func([]string, framework.MatchRule, string) (string, error) {
		return "room-0", nil
	}, func(string) (string, error) {
		return "logic-0", nil
	}, func(string, uint32, string, error) {})

	rule := framework.MatchRule{Module: "snake", Code: "ranked", Players: 4}
	if err := matcher.Enqueue("alice", rule, 1); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := matcher.Enqueue("alice", rule, 2); err != ErrRedisAlreadyQueued {
		t.Fatalf("expected ErrRedisAlreadyQueued, got %v", err)
	}
}
