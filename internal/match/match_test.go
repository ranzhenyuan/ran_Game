package match_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/match"
	"github.com/rangame/server/internal/room"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/storage"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

var connSeq uint64

type fakeConn struct{ id uint64 }

func newFakeConn() *fakeConn           { return &fakeConn{id: atomic.AddUint64(&connSeq, 1)} }
func (c *fakeConn) ID() uint64         { return c.id }
func (c *fakeConn) RemoteAddr() string { return "fake" }
func (c *fakeConn) Meta() *sync.Map    { return nil }
func (c *fakeConn) Read(ctx context.Context) (*transport.Frame, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *fakeConn) Push([]byte) error  { return nil }
func (c *fakeConn) Close(string) error { return nil }

type noopLogic struct{}

func (noopLogic) OnCreate(framework.RoomCtx, framework.RoomConfig)                   {}
func (noopLogic) OnJoin(framework.RoomCtx, framework.Player)                         {}
func (noopLogic) OnMessage(framework.RoomCtx, framework.Player, *framework.Envelope) {}
func (noopLogic) Tick(framework.RoomCtx, time.Duration)                              {}
func (noopLogic) OnLeave(framework.RoomCtx, framework.Player)                        {}
func (noopLogic) OnEmpty(r framework.RoomCtx)                                        { r.Close() }
func (noopLogic) OnDestroy(framework.RoomCtx)                                        {}

func setup(t *testing.T) (*actor.Engine, *session.Manager, *room.Manager) {
	t.Helper()
	eng := actor.New(actor.Config{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		eng.Shutdown(ctx)
	})
	sm := session.NewManager(session.Config{GracePeriod: time.Minute, SweepInterval: time.Hour})
	rm := room.NewManager(eng, sm, storage.NewMemoryStorage(), nil,
		room.WithDefaultMailbox(framework.MailboxPolicy{Capacity: 256, OnFull: framework.FullDrop}),
	)
	rm.RegisterModule(room.ModuleDef{
		Factory: func() framework.RoomLogic { return noopLogic{} },
		Config: framework.RoomConfig{
			Module: "test", MaxPlayers: 2, TickInterval: 50 * time.Millisecond,
			Mailbox: framework.MailboxPolicy{Capacity: 256, OnFull: framework.FullDrop},
		},
	})
	return eng, sm, rm
}

func TestSimpleMatchFillTable(t *testing.T) {
	_, sm, rm := setup(t)
	sm.Create("p1", newFakeConn(), 0)
	sm.Create("p2", newFakeConn(), 0)

	var mu sync.Mutex
	results := map[string]string{}
	mk := match.New(rm, func(uid string, _ uint32, roomID string, err error) {
		if err != nil {
			t.Errorf("unexpected match error for %s: %v", uid, err)
			return
		}
		mu.Lock()
		results[uid] = roomID
		mu.Unlock()
	})

	rule := match.Rule{Module: "test", Code: "ranked", Players: 2}
	if err := mk.Enqueue("p1", rule, 11); err != nil {
		t.Fatal(err)
	}
	if mk.QueueDepth("test", "ranked") != 1 {
		t.Fatal("depth should be 1 before fill")
	}
	// 第一人成桌前重复入队被拒。
	if err := mk.Enqueue("p1", rule, 11); !errors.Is(err, match.ErrAlreadyQueued) {
		t.Fatalf("dup enqueue err = %v", err)
	}
	if err := mk.Enqueue("p2", rule, 22); err != nil {
		t.Fatal(err)
	}

	if len(results) != 2 || results["p1"] == "" || results["p1"] != results["p2"] {
		t.Fatalf("both players should share roomID: %+v", results)
	}
	if !rm.Exists(results["p1"]) || rm.Rooms() != 1 {
		t.Fatal("matched room should exist")
	}
	if mk.QueueDepth("test", "ranked") != 0 {
		t.Fatal("queue should be drained")
	}
	// 成桌后再 Cancel 无效（不在队）。
	if mk.Cancel("p1") {
		t.Fatal("cancel after matched should return false")
	}
}

func TestSimpleMatchCancel(t *testing.T) {
	_, sm, rm := setup(t)
	sm.Create("p1", newFakeConn(), 0)
	mk := match.New(rm, func(string, uint32, string, error) {
		t.Fatal("canceled match must not notify")
	})
	rule := match.Rule{Module: "test", Code: "casual", Players: 2}
	if err := mk.Enqueue("p1", rule, 1); err != nil {
		t.Fatal(err)
	}
	if !mk.Cancel("p1") {
		t.Fatal("cancel should find queued uid")
	}
	if mk.QueueDepth("test", "casual") != 0 {
		t.Fatal("queue should be empty after cancel")
	}
}

func TestSimpleMatchBadRule(t *testing.T) {
	_, _, rm := setup(t)
	mk := match.New(rm, nil)
	if err := mk.Enqueue("p1", match.Rule{Module: "", Players: 2}, 1); !errors.Is(err, match.ErrBadRule) {
		t.Fatalf("empty module err = %v", err)
	}
	if err := mk.Enqueue("p1", match.Rule{Module: "test", Players: 0}, 1); !errors.Is(err, match.ErrBadRule) {
		t.Fatalf("zero players err = %v", err)
	}
	// 未注册模块 → 成桌（假设有 2 人）时建房失败，经回调报错。
	sm2 := session.NewManager(session.Config{GracePeriod: time.Minute, SweepInterval: time.Hour})
	sm2.Create("a", newFakeConn(), 0)
	sm2.Create("b", newFakeConn(), 0)
	var gotErr atomic.Bool
	mk2 := match.New(rm, func(_ string, _ uint32, _ string, err error) {
		if err != nil {
			gotErr.Store(true)
		}
	})
	rule := match.Rule{Module: "unregistered", Players: 2}
	_ = mk2.Enqueue("a", rule, 1)
	_ = mk2.Enqueue("b", rule, 2)
	if !gotErr.Load() {
		t.Fatal("unknown module should fail the table via callback")
	}
}
