// route_redis_test.go Redis 分布式路由表测试（§12.4 多 Gateway 副本）。
package gateway_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/gateway"
)

func newMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	t.Cleanup(s.Close)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return s, client
}

// 两个独立路由表实例共享同一 Redis：A Bind、B Lookup 命中（多副本核心语义）。
func TestRedisRouteTable_CrossInstance(t *testing.T) {
	_, client := newMiniredis(t)
	rtA := gateway.NewRedisRouteTable(client, nil, time.Hour)
	rtB := gateway.NewRedisRouteTable(client, nil, time.Hour)
	defer rtA.Close()
	defer rtB.Close()

	if err := rtA.Bind("alice", "logic-1"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	got, err := rtB.Lookup("alice")
	if err != nil || got != "logic-1" {
		t.Fatalf("cross lookup: got %q err %v", got, err)
	}
}

// 顶踢迁移：旧节点释放时条件 Unbind 不删除新节点写入的映射。
func TestRedisRouteTable_UnbindConditional(t *testing.T) {
	_, client := newMiniredis(t)
	rt := gateway.NewRedisRouteTable(client, nil, time.Hour)
	defer rt.Close()

	_ = rt.Bind("bob", "logic-0")
	_ = rt.Bind("bob", "logic-1") // 顶踢/重连切到新节点

	rt.Unbind("bob", "logic-0") // 旧节点释放：条件不满足，不应删除
	got, err := rt.Lookup("bob")
	if err != nil || got != "logic-1" {
		t.Fatalf("after stale unbind: got %q err %v", got, err)
	}

	rt.Unbind("bob", "logic-1") // 正确节点释放：应删除
	if _, err := rt.Lookup("bob"); err != gateway.ErrNotRouted {
		t.Fatalf("expected not routed after unbind, got err %v", err)
	}
}

// Bind 迁移时反向集合：旧节点集合移除 uid，新节点集合加入 uid。
func TestRedisRouteTable_ReverseSetMigration(t *testing.T) {
	_, client := newMiniredis(t)
	rt := gateway.NewRedisRouteTable(client, nil, time.Hour)
	defer rt.Close()

	_ = rt.Bind("carol", "logic-0")
	_ = rt.Bind("carol", "logic-1")

	ctx := context.Background()
	oldMembers, _ := client.SMembers(ctx, "cluster:route:node:logic-0").Result()
	if len(oldMembers) != 0 {
		t.Fatalf("old reverse set should be empty, got %v", oldMembers)
	}
	newMembers, _ := client.SMembers(ctx, "cluster:route:node:logic-1").Result()
	if len(newMembers) != 1 || newMembers[0] != "carol" {
		t.Fatalf("new reverse set mismatch: %v", newMembers)
	}
}

// Invalidate：节点全部路由 + 反向集清除。
func TestRedisRouteTable_Invalidate(t *testing.T) {
	s, client := newMiniredis(t)
	rt := gateway.NewRedisRouteTable(client, nil, time.Hour)
	defer rt.Close()

	_ = rt.Bind("u1", "logic-0")
	_ = rt.Bind("u2", "logic-0")
	_ = rt.Bind("u3", "logic-1")

	rt.Invalidate("logic-0")
	for _, uid := range []string{"u1", "u2"} {
		if _, err := rt.Lookup(uid); err != gateway.ErrNotRouted {
			t.Fatalf("uid %s should be invalidated", uid)
		}
	}
	if got, _ := rt.Lookup("u3"); got != "logic-1" {
		t.Fatalf("u3 should remain on logic-1, got %q", got)
	}
	if s.Exists("cluster:route:node:logic-0") {
		t.Fatalf("reverse set should be deleted")
	}
}

// TTL：条目过期后 Lookup 未命中（miniredis FastForward，不 sleep）。
func TestRedisRouteTable_TTLExpiry(t *testing.T) {
	s, client := newMiniredis(t)
	rt := gateway.NewRedisRouteTable(client, nil, time.Hour)
	defer rt.Close()

	_ = rt.Bind("dave", "logic-0")
	s.FastForward(2 * time.Hour)
	if _, err := rt.Lookup("dave"); err != gateway.ErrNotRouted {
		t.Fatalf("expected not routed after ttl, got err %v", err)
	}
}

// Watch：节点 Down 事件触发 Invalidate。
func TestRedisRouteTable_WatchDownInvalidates(t *testing.T) {
	_, client := newMiniredis(t)
	reg := cluster.NewMemRegistry()
	t.Cleanup(func() { _ = reg.Close() })
	rt := gateway.NewRedisRouteTable(client, reg, time.Hour)
	defer rt.Close()

	_ = reg.Register(cluster.NodeInfo{ID: "logic-0", Role: cluster.RoleLogic, State: cluster.StateActive})
	_ = rt.Bind("erin", "logic-0")
	// 预热本地缓存，便于验证 Invalidate 同时清掉了本地缓存与 Redis。
	if _, err := rt.Lookup("erin"); err != nil {
		t.Fatalf("seed lookup: %v", err)
	}

	// 自有订阅先确认 Down 事件已发布，再等待路由表 watchLoop 消费处理。
	evCh := reg.Watch()
	_ = reg.SetState("logic-0", cluster.StateDown)
	for {
		ev, ok := <-evCh
		if !ok {
			t.Fatalf("watch channel closed before down event")
		}
		if ev.New == cluster.StateDown {
			break
		}
	}
	// 事件已送达：轮询等待 watchLoop 完成 Invalidate（SMembers→批量 Del→清缓存）。
	// 轮询间小睡让出调度；这是等待异步协程（非用 sleep 模拟 TTL/时间），截止 2s。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := rt.Lookup("erin"); err == gateway.ErrNotRouted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected not routed after node down, but route still present")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
