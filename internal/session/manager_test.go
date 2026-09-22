package session

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/pkg/framework"
)

func newTestManager(grace time.Duration) (*Manager, *atomic.Int32) {
	var releases atomic.Int32
	m := NewManager(Config{
		ReplayCapacity: 256,
		GracePeriod:    grace,
		SweepInterval:  time.Hour,
		OnRelease:      func(uid, roomID string) { releases.Add(1) },
	})
	return m, &releases
}

func TestManagerCreateAndIndexes(t *testing.T) {
	m, releases := newTestManager(time.Minute)
	c1 := newFakeConn()
	s1, tok1 := m.Create("p1", c1, protocol.TypeJSON)

	if got, ok := m.Get("p1"); !ok || got != s1 {
		t.Fatal("Get mismatch")
	}
	if got, ok := m.GetByConnID(c1.ID()); !ok || got != s1 {
		t.Fatal("byConn index mismatch")
	}
	if m.OnlineCount() != 1 || tok1 == "" {
		t.Fatal("initial manager state wrong")
	}

	// 顶号：同 uid 再次登录，旧会话销毁、旧连接关闭、OnRelease 触发。
	c2 := newFakeConn()
	s2, tok2 := m.Create("p1", c2, protocol.TypeJSON)
	if s2 == s1 || tok2 == tok1 {
		t.Fatal("new login must create new session/token")
	}
	if releases.Load() != 1 {
		t.Fatalf("old session should be released once, got %d", releases.Load())
	}
	if got, ok := m.Get("p1"); !ok || got != s2 {
		t.Fatal("Get should point to new session")
	}
	if _, ok := m.GetByConnID(c1.ID()); ok {
		t.Fatal("old conn index should be removed")
	}
	c1.mu.Lock()
	closed := c1.closed
	c1.mu.Unlock()
	if !closed {
		t.Fatal("old conn should be closed on kick")
	}
	// 旧会话应先收到 2004 踢人帧。
	if frames := c1.frames(); len(frames) != 1 || frames[0].MsgID != uint32(framework.MsgKick) {
		t.Fatalf("old conn should get kick frame, got %+v", frames)
	}
}

func TestManagerGraceSweep(t *testing.T) {
	m, releases := newTestManager(30 * time.Millisecond)
	c1 := newFakeConn()
	s1, _ := m.Create("p1", c1, protocol.TypeJSON)

	m.MarkOffline(s1)
	if !s1.IsOffline() {
		t.Fatal("session should be offline")
	}
	c1.mu.Lock()
	closed := c1.closed
	c1.mu.Unlock()
	if !closed {
		t.Fatal("offline should close old conn")
	}

	// 宽限期内扫描不释放。
	if n := m.Sweep(time.Now()); n != 0 {
		t.Fatalf("sweep within grace released %d", n)
	}
	time.Sleep(35 * time.Millisecond)
	if n := m.Sweep(time.Now()); n != 1 || releases.Load() != 1 {
		t.Fatalf("sweep after grace: n=%d releases=%d", n, releases.Load())
	}
	if _, ok := m.Get("p1"); ok || m.OnlineCount() != 0 {
		t.Fatal("session should be removed from indexes")
	}
	if !errors.Is(s1.Send(nil), ErrReleased) {
		t.Fatal("released session Send should return ErrReleased")
	}
}

func TestManagerReconnectOneTimeToken(t *testing.T) {
	m, releases := newTestManager(time.Minute)
	c1 := newFakeConn()
	s1, tok := m.Create("p1", c1, protocol.TypeJSON)
	_ = s1.PushReliable(framework.MsgID(1001), relMsg{V: 1})

	m.MarkOfflineConn(s1, c1.ID())

	// 宽限期内离线推送继续累积。
	_ = s1.PushReliable(framework.MsgID(1001), relMsg{V: 2})
	_ = s1.PushSnapshot(framework.MsgID(2001), relMsg{V: 9})

	c2 := newFakeConn()
	s2, err := m.Reconnect(tok, c2, protocol.TypeJSON)
	if err != nil || s2 != s1 {
		t.Fatalf("reconnect: %v", err)
	}
	newTok := s2.Token()
	if newTok == tok {
		t.Fatal("token must rotate after reconnect")
	}
	if !s2.IsOnline() {
		t.Fatal("session should be online after reconnect")
	}
	if _, ok := m.GetByConnID(c2.ID()); !ok {
		t.Fatal("byConn should index new conn")
	}

	// 旧 token 二次使用必须失败。
	if _, err := m.Reconnect(tok, newFakeConn(), protocol.TypeJSON); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("reused token err = %v, want ErrTokenInvalid", err)
	}

	// 重放数据完整：seq 差额 1 帧 + 快照 1 帧。
	rep, snaps, overflow := s2.Replay(1, nil)
	if overflow || len(rep) != 1 || rep[0].Seq != 2 || len(snaps) != 1 {
		t.Fatalf("reconnect replay: rep=%d snaps=%d overflow=%v", len(rep), len(snaps), overflow)
	}
	if releases.Load() != 0 {
		t.Fatal("reconnect must not release session")
	}
}

func TestManagerReconnectUnknownToken(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	if _, err := m.Reconnect("nope", newFakeConn(), protocol.TypeJSON); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("err = %v", err)
	}
}

func TestManagerMarkOfflineConnStale(t *testing.T) {
	m, _ := newTestManager(time.Minute)
	c1 := newFakeConn()
	s1, tok := m.Create("p1", c1, protocol.TypeJSON)
	m.MarkOfflineConn(s1, c1.ID())

	c2 := newFakeConn()
	if _, err := m.Reconnect(tok, c2, protocol.TypeJSON); err != nil {
		t.Fatal(err)
	}
	// 旧读泵延迟退出：必须 no-op，会话保持在线。
	if m.MarkOfflineConn(s1, c1.ID()) {
		t.Fatal("stale conn MarkOfflineConn should return false")
	}
	if !s1.IsOnline() {
		t.Fatal("session wrongly marked offline by stale pump")
	}
}
