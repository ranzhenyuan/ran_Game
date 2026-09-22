package room_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/room"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/storage"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// ---- 假连接（收集已编码帧）----

var connSeq uint64

type fakeConn struct {
	id     uint64
	mu     sync.Mutex
	pushed [][]byte
}

func newFakeConn() *fakeConn           { return &fakeConn{id: atomic.AddUint64(&connSeq, 1)} }
func (c *fakeConn) ID() uint64         { return c.id }
func (c *fakeConn) RemoteAddr() string { return "fake" }
func (c *fakeConn) Meta() *sync.Map    { return nil }
func (c *fakeConn) Read(ctx context.Context) (*transport.Frame, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *fakeConn) Push(raw []byte) error {
	c.mu.Lock()
	dup := make([]byte, len(raw))
	copy(dup, raw)
	c.pushed = append(c.pushed, dup)
	c.mu.Unlock()
	return nil
}
func (c *fakeConn) Close(string) error { return nil }

func (c *fakeConn) frames() []*transport.Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*transport.Frame, 0, len(c.pushed))
	for _, raw := range c.pushed {
		f, err := transport.DecodeFrame(bytes.NewReader(raw), len(raw))
		if err != nil {
			panic(err)
		}
		out = append(out, f)
	}
	return out
}

// ---- 录制型玩法 ----

type recLogic struct {
	t            *testing.T
	joined       atomic.Int32
	left         atomic.Int32
	ticks        atomic.Int32
	emptied      atomic.Int32
	destroy      atomic.Int32
	closeOnEmpty bool
}

type pushMsg struct {
	V int `json:"v"`
}

const msgTest framework.MsgID = 0x1000
const msgSnap framework.MsgID = 0x1001

func (l *recLogic) OnCreate(r framework.RoomCtx, _ framework.RoomConfig) {}
func (l *recLogic) OnJoin(r framework.RoomCtx, p framework.Player) {
	l.joined.Add(1)
	r.Broadcast(framework.MsgMemberChange, map[string]string{"uid": p.UID(), "event": "join"})
}
func (l *recLogic) OnMessage(r framework.RoomCtx, p framework.Player, env *framework.Envelope) {}
func (l *recLogic) Tick(r framework.RoomCtx, dt time.Duration) {
	if l.ticks.Add(1) == 1 {
		r.BroadcastSnapshot(msgSnap, pushMsg{V: 1})
	}
}
func (l *recLogic) OnLeave(r framework.RoomCtx, p framework.Player) { l.left.Add(1) }
func (l *recLogic) OnEmpty(r framework.RoomCtx) {
	l.emptied.Add(1)
	if l.closeOnEmpty {
		r.Close()
	}
}
func (l *recLogic) OnDestroy(r framework.RoomCtx) { l.destroy.Add(1) }

func setup(t *testing.T, logic *recLogic, maxPlayers int) (*actor.Engine, *session.Manager, *room.Manager) {
	t.Helper()
	eng := actor.New(actor.Config{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		eng.Shutdown(ctx)
	})
	sm := session.NewManager(session.Config{GracePeriod: time.Minute, SweepInterval: time.Hour})
	mgr := room.NewManager(eng, sm, storage.NewMemoryStorage(), nil,
		room.WithDefaultTick(20*time.Millisecond),
		room.WithDefaultMailbox(framework.MailboxPolicy{Capacity: 256, OnFull: framework.FullDrop}),
	)
	mgr.RegisterModule(room.ModuleDef{
		Factory: func() framework.RoomLogic { return logic },
		Config: framework.RoomConfig{
			Module: "test", MaxPlayers: maxPlayers, TickInterval: 20 * time.Millisecond,
			Mailbox: framework.MailboxPolicy{Capacity: 256, OnFull: framework.FullDrop},
		},
	})
	return eng, sm, mgr
}

func createSess(t *testing.T, sm *session.Manager, uid string) (*session.Session, *fakeConn) {
	t.Helper()
	c := newFakeConn()
	sess, _ := sm.Create(uid, c, 0)
	return sess, c
}

func TestRoomJoinBroadcastAndSnapshot(t *testing.T) {
	logic := &recLogic{t: t}
	eng, sm, mgr := setup(t, logic, 4)

	roomID, err := mgr.CreateRoom("test")
	if err != nil {
		t.Fatal(err)
	}
	if !mgr.Exists(roomID) || mgr.Rooms() != 1 {
		t.Fatal("room should exist")
	}

	s1, c1 := createSess(t, sm, "p1")
	s2, c2 := createSess(t, sm, "p2")
	if err := mgr.Join(roomID, "p1"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Join(roomID, "p2"); err != nil {
		t.Fatal(err)
	}
	if s1.RoomID() != roomID || s2.RoomID() != roomID {
		t.Fatal("session roomID should be bound")
	}
	// 重复进房幂等。
	if err := mgr.Join(roomID, "p2"); err != nil {
		t.Fatalf("idempotent join: %v", err)
	}

	// p1 进房广播自己也收到 1 条；p2 进房广播两人各收 1 条（p1 共 2，p2 共 1）。
	waitLen(t, c1, 2)
	waitLen(t, c2, 1)
	for _, f := range c1.frames() {
		if f.MsgID != uint32(framework.MsgMemberChange) {
			t.Fatalf("unexpected msgID %d", f.MsgID)
		}
	}

	// 等首个 Tick 的快照帧（bit6、seq 不递增）。
	waitSnap(t, c1, msgSnap)
	snaps := filterFrames(c1.frames(), uint32(msgSnap))
	if len(snaps) != 1 || snaps[0].Flag&transport.FlagSnapshot == 0 {
		t.Fatalf("snapshot wrong: %+v", snaps)
	}
	if logic.ticks.Load() == 0 {
		t.Fatal("tick not fired")
	}
	_ = eng
}

func TestRoomFull(t *testing.T) {
	logic := &recLogic{t: t}
	_, sm, mgr := setup(t, logic, 1)
	roomID, _ := mgr.CreateRoom("test")
	_, _ = createSess(t, sm, "p1")
	if err := mgr.Join(roomID, "p1"); err != nil {
		t.Fatal(err)
	}
	_, _ = createSess(t, sm, "p2")
	err := mgr.Join(roomID, "p2")
	var ec *framework.ErrCode
	if !asErr(err, &ec) || ec.Code != framework.ErrRoomFull {
		t.Fatalf("want 3002, got %v", err)
	}
}

func TestRoomLeaveEmptyRecycle(t *testing.T) {
	logic := &recLogic{t: t, closeOnEmpty: true}
	_, sm, mgr := setup(t, logic, 4)
	roomID, _ := mgr.CreateRoom("test")
	s1, _ := createSess(t, sm, "p1")
	if err := mgr.Join(roomID, "p1"); err != nil {
		t.Fatal(err)
	}

	mgr.Leave(roomID, "p1")
	waitTrue(t, func() bool { return !mgr.Exists(roomID) }, "room recycled")
	if s1.RoomID() != "" {
		t.Fatal("session roomID should be cleared after room close")
	}
	if logic.destroy.Load() != 1 || logic.emptied.Load() != 1 || logic.left.Load() != 1 {
		t.Fatalf("lifecycle wrong: leave=%d empty=%d destroy=%d",
			logic.left.Load(), logic.emptied.Load(), logic.destroy.Load())
	}
	if mgr.Rooms() != 0 {
		t.Fatalf("active rooms = %d", mgr.Rooms())
	}
}

func TestRoomKick(t *testing.T) {
	// 用首个 Tick 内 Kick 的玩法，覆盖 Kick 通知 + OnLeave + 空房关闭全路径。
	logic := &recLogic{t: t}
	kickLogic := &kickOnTickLogic{inner: logic, uid: "p3"}
	_, sm, mgr := setup(t, logic, 4)
	mgr.RegisterModule(room.ModuleDef{
		Factory: func() framework.RoomLogic { return kickLogic },
		Config: framework.RoomConfig{Module: "kick", MaxPlayers: 4, TickInterval: 20 * time.Millisecond,
			Mailbox: framework.MailboxPolicy{Capacity: 256, OnFull: framework.FullDrop}},
	})
	room2, _ := mgr.CreateRoom("kick")
	_, c2 := createSess(t, sm, "p3")
	if err := mgr.Join(room2, "p3"); err != nil {
		t.Fatal(err)
	}
	kickLogic.roomID = room2

	// 等到 p3 收到 2004 踢人帧，且空房被回收。
	waitKick(t, c2)
	waitTrue(t, func() bool { return !mgr.Exists(room2) }, "kicked room recycled")
}

// kickOnTickLogic 首个 Tick 踢掉指定玩家。
type kickOnTickLogic struct {
	inner  *recLogic
	uid    string
	roomID string
}

func (l *kickOnTickLogic) OnCreate(framework.RoomCtx, framework.RoomConfig)                   {}
func (l *kickOnTickLogic) OnJoin(r framework.RoomCtx, p framework.Player)                     { l.inner.OnJoin(r, p) }
func (l *kickOnTickLogic) OnMessage(framework.RoomCtx, framework.Player, *framework.Envelope) {}
func (l *kickOnTickLogic) Tick(r framework.RoomCtx, dt time.Duration) {
	if l.inner.ticks.Add(1) == 1 {
		r.Kick(l.uid, framework.ErrKicked, "test kick")
	}
}
func (l *kickOnTickLogic) OnLeave(r framework.RoomCtx, p framework.Player) { l.inner.OnLeave(r, p) }
func (l *kickOnTickLogic) OnEmpty(r framework.RoomCtx) {
	l.inner.emptied.Add(1)
	r.Close()
}
func (l *kickOnTickLogic) OnDestroy(r framework.RoomCtx) { l.inner.destroy.Add(1) }

// ---- 辅助 ----

func waitLen(t *testing.T, c *fakeConn, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := len(c.pushed)
		c.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("wait %d frames timeout", n)
}

func waitSnap(t *testing.T, c *fakeConn, id framework.MsgID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, f := range c.frames() {
			if f.MsgID == uint32(id) && f.Flag&transport.FlagSnapshot != 0 {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("snapshot frame timeout")
}

func waitKick(t *testing.T, c *fakeConn) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, f := range c.frames() {
			if f.MsgID == uint32(framework.MsgKick) {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("kick frame timeout")
}

func waitTrue(t *testing.T, fn func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("wait %s timeout", what)
}

func filterFrames(frames []*transport.Frame, msgID uint32) []*transport.Frame {
	var out []*transport.Frame
	for _, f := range frames {
		if f.MsgID == msgID {
			out = append(out, f)
		}
	}
	return out
}

func asErr(err error, target **framework.ErrCode) bool {
	if ec, ok := err.(*framework.ErrCode); ok {
		*target = ec
		return true
	}
	return false
}
