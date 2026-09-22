// gateway_test.go 走查 12（拆分转发）测试。
//
// 覆盖：
//   - MemRouteTable Bind/Lookup/Invalidate（节点 Down 触发路由失效）
//   - MemNodeTransport 进程内转发
//   - InnerHeader 编解码往返

package gateway

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/transport"
)

// TestMemRouteTable_BindInvalidate 验证一级路由表 Bind/Lookup/Invalidate。
func TestMemRouteTable_BindInvalidate(t *testing.T) {
	reg := cluster.NewMemRegistry()
	defer reg.Close()
	rt := NewMemRouteTable(reg)
	defer rt.Close()

	// 先注册 node-A 并等 Watch 协程就绪
	_ = reg.Register(cluster.NodeInfo{ID: "node-A", Role: cluster.RoleLogic, State: cluster.StateActive})
	// Bind uid → node-A
	if err := rt.Bind("alice", "node-A"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	got, err := rt.Lookup("alice")
	if err != nil || got != "node-A" {
		t.Fatalf("lookup: got=%q err=%v", got, err)
	}

	// 模拟 node-A Down：Watch 应触发 Invalidate（§13.4）
	_ = reg.SetState("node-A", cluster.StateDown)

	// 等待 Watch 协程处理
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := rt.Lookup("alice"); err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := rt.Lookup("alice"); err == nil {
		t.Fatalf("expected ErrNotRouted after node Down")
	}
}

// TestMemNodeTransport_Forward 进程内 NodeTransport 转发。
func TestMemNodeTransport_Forward(t *testing.T) {
	tr := NewMemNodeTransport("gw-0")

	var got atomic.Int32
	_ = tr.AddPeer("logic-0", func(inner InnerHeader, f *transport.Frame) {
		if inner.UID == "alice" && f.MsgID == 0x10 {
			got.Add(1)
		}
	})

	inner := InnerHeader{UID: "alice", MsgType: MsgUnicast}
	frame := &transport.Frame{Ver: 1, MsgID: 0x10, Flag: 1, Seq: 1}
	if err := tr.Forward("logic-0", inner, frame); err != nil {
		t.Fatalf("forward: %v", err)
	}
	if got.Load() != 1 {
		t.Fatalf("handler not invoked: %d", got.Load())
	}

	// 未注册节点
	if err := tr.Forward("logic-X", inner, frame); err == nil {
		t.Fatalf("expected error for unknown peer")
	}
}

// TestInnerHeader_RoundTrip innerHeader+帧编解码往返。
func TestInnerHeader_RoundTrip(t *testing.T) {
	original := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: 0x1000,
		Flag:  1, // PB codec
		Seq:   42,
		Body:  []byte{0x08, 0x01}, // protobuf MoveReq dir=1
	}
	inner := InnerHeader{
		UID:        "alice",
		SessionID:  "sess-42",
		GatewayID:  "gw-0",
		MsgType:    MsgUnicast,
		ForwardSeq: 42,
	}

	encoded := EncodeInner(inner, original)
	gotInner, gotFrame, err := DecodeInner(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotInner.UID != inner.UID || gotInner.SessionID != inner.SessionID ||
		gotInner.GatewayID != inner.GatewayID || gotInner.MsgType != inner.MsgType ||
		gotInner.ForwardSeq != inner.ForwardSeq {
		t.Fatalf("inner mismatch: got=%+v want=%+v", gotInner, inner)
	}
	if gotFrame.MsgID != original.MsgID || gotFrame.Seq != original.Seq ||
		gotFrame.Flag != original.Flag || len(gotFrame.Body) != len(original.Body) {
		t.Fatalf("frame mismatch: got=%+v want=%+v", gotFrame, original)
	}
}
