package actor

import (
	"testing"

	"github.com/rangame/server/pkg/framework"
)

func TestRegistryUIDBindings(t *testing.T) {
	r := NewRegistry()

	r.BindUID("u1", 10)
	r.BindUID("u2", 20)

	if id, ok := r.LookupUID("u1"); !ok || id != 10 {
		t.Fatalf("u1 lookup: %d %v", id, ok)
	}
	if id, ok := r.LookupUID("u2"); !ok || id != 20 {
		t.Fatalf("u2 lookup: %d %v", id, ok)
	}
	if _, ok := r.LookupUID("missing"); ok {
		t.Fatal("missing uid should not be found")
	}

	// 同 uid 重绑到新 Actor：旧映射被替换。
	r.BindUID("u1", 30)
	if id, _ := r.LookupUID("u1"); id != 30 {
		t.Fatalf("rebind failed: %d", id)
	}

	// Unbind 新 ID 应清掉 uid。
	r.Unbind(30)
	if _, ok := r.LookupUID("u1"); ok {
		t.Fatal("u1 should be unbound")
	}
}

func TestRegistryRoomBindings(t *testing.T) {
	r := NewRegistry()

	r.BindRoom("room-A", framework.ID(100))
	if id, ok := r.LookupRoom("room-A"); !ok || id != 100 {
		t.Fatalf("room lookup: %d %v", id, ok)
	}

	// 同一 Actor 先绑 uid 再绑 room，Unbind 一次清两处。
	r.BindUID("p1", 200)
	r.BindRoom("room-B", 200)
	r.Unbind(200)
	if _, ok := r.LookupUID("p1"); ok {
		t.Fatal("uid binding should be removed")
	}
	if _, ok := r.LookupRoom("room-B"); ok {
		t.Fatal("room binding should be removed")
	}
}
