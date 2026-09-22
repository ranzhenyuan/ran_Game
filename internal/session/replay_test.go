package session

import (
	"testing"

	"github.com/rangame/server/internal/transport"
)

func TestRingSinceInOrder(t *testing.T) {
	r := newRingBuffer(4)
	for i := uint32(1); i <= 4; i++ {
		r.push(&transport.Frame{Seq: i})
	}

	got, overflow := r.since(2)
	if overflow {
		t.Fatal("fresh ring must not overflow")
	}
	if len(got) != 2 || got[0].Seq != 3 || got[1].Seq != 4 {
		t.Fatalf("since(2) mismatch: %+v", got)
	}

	// lastSeq=0 → 全量。
	got, _ = r.since(0)
	if len(got) != 4 {
		t.Fatalf("since(0) should return all, got %d", len(got))
	}
}

func TestRingOverwriteAndOverflow(t *testing.T) {
	r := newRingBuffer(4)
	for i := uint32(1); i <= 6; i++ {
		r.push(&transport.Frame{Seq: i})
	}
	if r.len() != 4 || r.oldestSeq() != 3 {
		t.Fatalf("ring state wrong: len=%d oldest=%d", r.len(), r.oldestSeq())
	}

	// 客户端只收到 seq=2：现存 3..6 恰好衔接，不溢出。
	got, overflow := r.since(2)
	if overflow || len(got) != 4 || got[0].Seq != 3 {
		t.Fatalf("since(2) mismatch: overflow=%v got=%+v", overflow, got)
	}
	// 客户端只收到 seq=1：需要的 seq2 已被淘汰 → overflow。
	if _, overflow := r.since(1); !overflow {
		t.Fatal("expected overflow when lastSeq+1 older than oldest buffered")
	}
}

func TestSnapshotCacheLatestWins(t *testing.T) {
	c := newSnapshotCache()
	c.put(&transport.Frame{MsgID: 100, Seq: 1, Body: []byte("v1")})
	c.put(&transport.Frame{MsgID: 100, Seq: 2, Body: []byte("v2")})
	c.put(&transport.Frame{MsgID: 200, Seq: 1, Body: []byte("a")})

	all := c.all(nil)
	if len(all) != 2 {
		t.Fatalf("expected 2 msgIDs cached, got %d", len(all))
	}
	if got := c.all([]uint32{100}); len(got) != 1 || string(got[0].Body) != "v2" || got[0].Seq != 2 {
		t.Fatalf("latest-wins failed: %+v", got)
	}
	if got := c.all([]uint32{999}); len(got) != 0 {
		t.Fatal("unknown msgID should yield no snapshot")
	}
}
