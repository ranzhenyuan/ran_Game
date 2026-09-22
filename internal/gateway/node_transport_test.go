// node_transport_test.go TCP node-to-node Transport 测试。
//
// 覆盖：
//   - 单帧转发（gateway → logic 入站回调收到正确 inner+frame）
//   - 多帧顺序与写聚合
//   - 出站连接断开后自动重拨
//   - Close 清理

package gateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/internal/transport"
)

// startPair 启动一对 TCPNodeTransport（a 主动连 b），返回双方及 b 的入站回调计数。
func startPair(t *testing.T) (a, b *TCPNodeTransport, bRecv *atomic.Int32) {
	t.Helper()
	b = NewTCPNodeTransport("logic-0", "127.0.0.1:0", nil)
	addr, err := b.Listen()
	if err != nil {
		t.Fatalf("b listen: %v", err)
	}

	var recv atomic.Int32
	_ = b.AddPeer("gw-0", func(inner InnerHeader, f *transport.Frame) {
		recv.Add(1)
	})

	a = NewTCPNodeTransport("gw-0", "127.0.0.1:0", nil)
	a.AddPeerAddr("logic-0", addr)
	return a, b, &recv
}

func TestTCPNodeTransport_SingleForward(t *testing.T) {
	a, b, recv := startPair(t)
	defer a.Close()
	defer b.Close()

	inner := InnerHeader{UID: "alice", SessionID: "s1", MsgType: MsgUnicast, ForwardSeq: 7}
	f := &transport.Frame{Ver: transport.ProtocolVer, MsgID: 0x1001, Flag: 1, Seq: 7, Body: []byte{1, 2, 3}}

	if err := a.Forward("logic-0", inner, f); err != nil {
		t.Fatalf("forward: %v", err)
	}

	// 等入站回调
	deadline := time.Now().Add(time.Second)
	for recv.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if recv.Load() != 1 {
		t.Fatalf("b did not receive frame, count=%d", recv.Load())
	}
}

func TestTCPNodeTransport_MultiFrameOrder(t *testing.T) {
	a, b, _ := startPair(t)
	defer a.Close()
	defer b.Close()

	const n = 50
	var order []uint32
	var mu sync.Mutex
	b.handlers = map[string]func(InnerHeader, *transport.Frame){
		"gw-0": func(_ InnerHeader, f *transport.Frame) {
			mu.Lock()
			order = append(order, f.Seq)
			mu.Unlock()
		},
	}

	for i := uint32(1); i <= n; i++ {
		_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: i},
			&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: i})
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		got := len(order)
		mu.Unlock()
		if got >= n || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != n {
		t.Fatalf("received %d frames, want %d", len(order), n)
	}
	for i, v := range order {
		if v != uint32(i+1) {
			t.Fatalf("frame order broken at %d: got %d", i, v)
		}
	}
}

func TestTCPNodeTransport_ReconnectAfterClose(t *testing.T) {
	a, b, recv := startPair(t)
	defer a.Close()

	// 第一次转发成功
	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: 1},
		&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: 1})
	deadline := time.Now().Add(time.Second)
	for recv.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// 关闭 b 的监听（模拟对端重启），a 的出站连接会写失败
	b.Close()

	// 重启 b 到同端口
	addr := a.peerAddrs["logic-0"]
	b2 := NewTCPNodeTransport("logic-0", addr, nil)
	if _, err := b2.Listen(); err != nil {
		t.Fatalf("b2 listen: %v", err)
	}
	defer b2.Close()
	var recv2 atomic.Int32
	_ = b2.AddPeer("gw-0", func(_ InnerHeader, _ *transport.Frame) { recv2.Add(1) })

	// 等待 a 的 writeLoop 检测到旧连接已死（TCP 半关闭下需要一次 flush 才发现）
	time.Sleep(50 * time.Millisecond)

	// 第一次 Forward：旧连接写失败被标记 dead，帧丢失但连接被清理
	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: 2},
		&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: 2})
	// 第二次 Forward：检测到 dead，重新拨号到 b2
	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: 3},
		&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: 3})
	deadline = time.Now().Add(time.Second)
	for recv2.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if recv2.Load() < 1 {
		t.Fatalf("reconnect forward failed, count=%d", recv2.Load())
	}
}

func TestTCPNodeTransport_GatewayIDStamped(t *testing.T) {
	a, b, _ := startPair(t)
	defer a.Close()
	defer b.Close()

	var got string
	var mu sync.Mutex
	b.handlers = map[string]func(InnerHeader, *transport.Frame){
		"gw-0": func(inner InnerHeader, _ *transport.Frame) {
			mu.Lock()
			got = inner.GatewayID
			mu.Unlock()
		},
	}

	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast},
		&transport.Frame{Ver: 1, MsgID: 0x1000})

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		v := got
		mu.Unlock()
		if v != "" || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got != "gw-0" {
		t.Fatalf("GatewayID not stamped: got %q", got)
	}
}
