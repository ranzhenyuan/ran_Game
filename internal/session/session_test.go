package session

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

var connSeq uint64

// fakeConn 内存连接：Push 收集已编码帧，Read 永久阻塞直到 Close。
type fakeConn struct {
	id       uint64
	mu       sync.Mutex
	pushed   [][]byte
	closed   bool
	closeMsg string
}

func newFakeConn() *fakeConn {
	return &fakeConn{id: atomic.AddUint64(&connSeq, 1)}
}

func (c *fakeConn) ID() uint64         { return c.id }
func (c *fakeConn) RemoteAddr() string { return "fake" }
func (c *fakeConn) Meta() *sync.Map    { return nil }

func (c *fakeConn) Read(ctx context.Context) (*transport.Frame, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *fakeConn) Push(raw []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("conn closed")
	}
	dup := make([]byte, len(raw))
	copy(dup, raw)
	c.pushed = append(c.pushed, dup)
	return nil
}

func (c *fakeConn) Close(_ string) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

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

type relMsg struct {
	V int `json:"v"`
}

func TestReliablePushAndReplay(t *testing.T) {
	conn := newFakeConn()
	s := newSession("u1", conn, protocol.TypeJSON, 256)

	for i := 1; i <= 3; i++ {
		if err := s.PushReliable(framework.MsgID(1000+i), relMsg{V: i}); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if s.LastReliableSeq() != 3 {
		t.Fatalf("last seq = %d, want 3", s.LastReliableSeq())
	}
	got := conn.frames()
	if len(got) != 3 {
		t.Fatalf("online push should deliver 3 frames, got %d", len(got))
	}
	for i, f := range got {
		if f.Seq != uint32(i+1) {
			t.Fatalf("frame %d seq = %d", i, f.Seq)
		}
		if f.Flag&transport.FlagSnapshot != 0 {
			t.Fatal("reliable frame must not carry snapshot bit")
		}
	}

	// 客户端已收 seq=1：重放返回 seq 2、3。
	rep, snaps, overflow := s.Replay(1, nil)
	if overflow || len(rep) != 2 || rep[0].Seq != 2 || len(snaps) != 0 {
		t.Fatalf("replay(1) got rep=%d snaps=%d overflow=%v", len(rep), len(snaps), overflow)
	}
}

func TestSnapshotLatestWinsDoesNotConsumeSeq(t *testing.T) {
	conn := newFakeConn()
	s := newSession("u1", conn, protocol.TypeJSON, 256)

	if err := s.PushReliable(framework.MsgID(1001), relMsg{V: 1}); err != nil {
		t.Fatal(err)
	}
	const snapID = framework.MsgID(2001)
	if err := s.PushSnapshot(snapID, relMsg{V: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.PushSnapshot(snapID, relMsg{V: 2}); err != nil {
		t.Fatal(err)
	}
	// 快照不占用可靠 seq。
	if s.LastReliableSeq() != 1 {
		t.Fatalf("snapshot must not consume seq, last=%d", s.LastReliableSeq())
	}
	frames := conn.frames()
	if len(frames) != 3 || frames[1].Flag&transport.FlagSnapshot == 0 {
		t.Fatalf("snapshot frames wrong: %+v", frames)
	}

	snaps := s.Snapshots(nil)
	if len(snaps) != 1 || snaps[0].Flag&transport.FlagSnapshot == 0 {
		t.Fatalf("snapshot cache wrong: %+v", snaps)
	}
	var v relMsg
	if err := protocol.MustGet(protocol.TypeJSON).Unmarshal(snaps[0].Body, &v); err != nil || v.V != 2 {
		t.Fatalf("latest snapshot should be v=2, got %+v err=%v", v, err)
	}
}

func TestOfflineBufferAndReconnectReplay(t *testing.T) {
	c1 := newFakeConn()
	s := newSession("u1", c1, protocol.TypeJSON, 4)

	// 在线推 seq1。
	if err := s.PushReliable(framework.MsgID(1001), relMsg{V: 1}); err != nil {
		t.Fatal(err)
	}
	// 断线进宽限期：直接发送（RPC 应答）失败，但可靠推送继续缓冲。
	s.unbind()
	if !s.IsOffline() {
		t.Fatal("should be offline after unbind")
	}
	if err := s.Send(&transport.Frame{}); !errors.Is(err, ErrOffline) {
		t.Fatalf("Send offline err = %v, want ErrOffline", err)
	}
	for i := 2; i <= 5; i++ {
		if err := s.PushReliable(framework.MsgID(1001), relMsg{V: i}); err != nil {
			t.Fatalf("offline push %d should buffer without error: %v", i, err)
		}
	}
	if s.LastReliableSeq() != 5 {
		t.Fatalf("offline seq = %d", s.LastReliableSeq())
	}

	// 重连绑定新连接。
	c2 := newFakeConn()
	if old := s.bind(c2, protocol.TypeJSON); old != nil {
		t.Fatal("unbound session should return nil old conn on bind")
	}
	if !s.IsOnline() {
		t.Fatal("should be online after bind")
	}
	// 客户端已收 seq1；ring 容量 4 保留 seq2..5，无溢出。
	rep, snaps, overflow := s.Replay(1, nil)
	if overflow || len(rep) != 4 || rep[0].Seq != 2 || rep[3].Seq != 5 || len(snaps) != 0 {
		t.Fatalf("replay after offline: rep=%d overflow=%v", len(rep), overflow)
	}
	for _, f := range rep {
		if err := s.PushRaw(f); err != nil {
			t.Fatalf("replay push: %v", err)
		}
	}
	if got := c2.frames(); len(got) != 4 {
		t.Fatalf("new conn should receive 4 replay frames, got %d", len(got))
	}
}

func TestReplayOverflow(t *testing.T) {
	conn := newFakeConn()
	s := newSession("u1", conn, protocol.TypeJSON, 4)
	for i := 1; i <= 6; i++ {
		if err := s.PushReliable(framework.MsgID(1001), relMsg{V: i}); err != nil {
			t.Fatal(err)
		}
	}
	// 6 帧中 ring 只留 seq3..6：客户端 lastSeq=1（缺的 seq2 已淘汰）触发 Resync。
	if _, _, overflow := s.Replay(1, nil); !overflow {
		t.Fatal("expected overflow")
	}
	// lastSeq=2 与现存最旧帧 seq3 恰好衔接：不溢出，返回 4 帧。
	rep, _, overflow := s.Replay(2, nil)
	if overflow || len(rep) != 4 || rep[0].Seq != 3 {
		t.Fatalf("replay(2) mismatch: overflow=%v rep=%d", overflow, len(rep))
	}
}

func TestUnbindIfConnStaleConnectionNoop(t *testing.T) {
	c1 := newFakeConn()
	s := newSession("u1", c1, protocol.TypeJSON, 4)

	// 模拟重连：新连接已接管会话。
	c2 := newFakeConn()
	s.bind(c2, protocol.TypeJSON)

	// 旧读泵随后退出：不得把会话误标离线。
	if _, ok := s.unbindIfConn(c1.ID()); ok {
		t.Fatal("stale conn unbind must be noop")
	}
	if !s.IsOnline() {
		t.Fatal("session must stay online after stale unbind")
	}
	// 当前连接的读泵退出：正常解绑。
	if _, ok := s.unbindIfConn(c2.ID()); !ok || !s.IsOffline() {
		t.Fatal("current conn unbind should succeed and go offline")
	}
}

func TestTokenRotateOneTime(t *testing.T) {
	s := newSession("u1", newFakeConn(), protocol.TypeJSON, 4)
	s.setToken("tok-a")
	if !s.rotateToken("tok-a", "tok-b") {
		t.Fatal("first rotate should succeed")
	}
	if s.rotateToken("tok-a", "tok-c") {
		t.Fatal("consumed token must not rotate again")
	}
	if s.Token() != "tok-b" {
		t.Fatalf("token = %q", s.Token())
	}
}

func TestTouchAndAlive(t *testing.T) {
	s := newSession("u1", newFakeConn(), protocol.TypeJSON, 4)
	time.Sleep(2 * time.Millisecond)
	if s.Alive(time.Millisecond) {
		t.Fatal("session should be considered stale")
	}
	s.Touch()
	if !s.Alive(time.Minute) {
		t.Fatal("session should be alive after Touch")
	}
}
