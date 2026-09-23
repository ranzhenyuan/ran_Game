// node_transport_test.go TCP node-to-node Transport 测试。
//
// 覆盖：
//   - 单帧转发（gateway → logic 入站回调收到正确 inner+frame）
//   - 多帧顺序与写聚合
//   - 出站连接断开后自动重拨
//   - Close 清理

package gateway

import (
	"encoding/binary"
	"net"
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

// WithKeepaliveInterval / WithReconnCheck 选项应正确注入，<=0 回退默认值。
func TestTCPNodeTransport_OptionsApplied(t *testing.T) {
	a := NewTCPNodeTransport("gw-0", "127.0.0.1:0", nil,
		WithKeepaliveInterval(123*time.Millisecond),
		WithReconnCheck(456*time.Millisecond))
	defer a.Close()
	if a.keepaliveInterval != 123*time.Millisecond {
		t.Fatalf("keepaliveInterval not applied: %v", a.keepaliveInterval)
	}
	if a.reconnCheck != 456*time.Millisecond {
		t.Fatalf("reconnCheck not applied: %v", a.reconnCheck)
	}

	b := NewTCPNodeTransport("gw-1", "127.0.0.1:0", nil,
		WithKeepaliveInterval(0), WithReconnCheck(-1))
	defer b.Close()
	if b.keepaliveInterval != defaultKeepaliveInterval {
		t.Fatalf("keepaliveInterval should fall back to default, got %v", b.keepaliveInterval)
	}
	if b.reconnCheck != defaultReconnCheck {
		t.Fatalf("reconnCheck should fall back to default, got %v", b.reconnCheck)
	}
}

// writeRawEnvelope 向裸 TCP 连接写一个 inner envelope（4B 长度前缀 + EncodeInner 输出）。
func writeRawEnvelope(t *testing.T, c net.Conn, inner InnerHeader) {
	t.Helper()
	raw := EncodeInner(inner, &transport.Frame{Ver: transport.ProtocolVer, MsgID: 0x1000})
	buf := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(raw)))
	copy(buf[4:], raw)
	if _, err := c.Write(buf); err != nil {
		t.Fatalf("write envelope: %v", err)
	}
}

// idle 超过 keepaliveInterval 无业务写出时，出站连接应自动发送链路心跳。
func TestTCPNodeTransport_KeepaliveSentOnIdle(t *testing.T) {
	a, b, _ := startPair(t)
	defer a.Close()
	defer b.Close()
	a.keepaliveInterval = 10 * time.Millisecond
	a.reconnCheck = time.Hour // 关闭重连，避免干扰观测

	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: 1},
		&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: 1})

	a.mu.RLock()
	c := a.outConns["logic-0"]
	a.mu.RUnlock()
	if c == nil {
		t.Fatal("outbound connection not established")
	}
	// 真实等待异步心跳：每 10ms 一发，轮询断言（非 sleep 模拟时间）。
	deadline := time.Now().Add(time.Second)
	for c.keepaliveSent.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := c.keepaliveSent.Load(); n == 0 {
		t.Fatalf("expected keepalive frames on idle, got %d", n)
	}
}

// 接收方必须丢弃链路心跳，不投递给业务 handler。
func TestTCPNodeTransport_KeepaliveFilteredAtReceiver(t *testing.T) {
	b := NewTCPNodeTransport("logic-0", "127.0.0.1:0", nil)
	addr, err := b.Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer b.Close()
	var recv atomic.Int32
	b.SetDefaultHandler(func(_ InnerHeader, _ *transport.Frame) { recv.Add(1) })

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()

	writeRawEnvelope(t, raw, InnerHeader{MsgType: MsgKeepalive})               // 应被过滤
	writeRawEnvelope(t, raw, InnerHeader{GatewayID: "x", MsgType: MsgUnicast}) // 应计数 1

	deadline := time.Now().Add(time.Second)
	for recv.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := recv.Load(); got != 1 {
		t.Fatalf("expected exactly 1 business frame (keepalive filtered), got %d", got)
	}
}

// 对端断开后，后台 reconcile 应在退避到期后自动重拨恢复连接，无需业务 Forward 触发。
func TestTCPNodeTransport_AutoReconnectAfterPeerDown(t *testing.T) {
	a, b, _ := startPair(t)
	defer a.Close()
	a.keepaliveInterval = 10 * time.Millisecond // 心跳快速写失败 → 及时标 dead
	a.reconnCheck = 20 * time.Millisecond       // 快速扫描重连

	// 建立连接并纳入 activePeers
	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: 1},
		&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: 1})
	a.mu.RLock()
	first := a.outConns["logic-0"]
	a.mu.RUnlock()
	if first == nil {
		t.Fatal("outbound connection not established")
	}

	// 对端重启到同端口：先 down，再起来
	addr := a.peerAddrs["logic-0"]
	b.Close()
	b2 := NewTCPNodeTransport("logic-0", addr, nil)
	if _, err := b2.Listen(); err != nil {
		t.Fatalf("b2 listen: %v", err)
	}
	defer b2.Close()
	var recv2 atomic.Int32
	b2.SetDefaultHandler(func(_ InnerHeader, _ *transport.Frame) { recv2.Add(1) })

	// 轮询等待后台自动重连出一条活连接（含 dialPeer 失败后的 reconnBackoff 退避）。
	deadline := time.Now().Add(3 * time.Second)
	var live *nodeConn
	for time.Now().Before(deadline) {
		a.mu.RLock()
		c := a.outConns["logic-0"]
		a.mu.RUnlock()
		if c != nil && !c.dead.Load() && c != first {
			live = c
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if live == nil {
		t.Fatal("expected auto-reconnect to establish a live connection")
	}

	// 业务帧经自动重建的连接送达新对端
	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: 9},
		&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: 9})
	d2 := time.Now().Add(time.Second)
	for recv2.Load() == 0 && time.Now().Before(d2) {
		time.Sleep(5 * time.Millisecond)
	}
	if recv2.Load() < 1 {
		t.Fatalf("forward after auto-reconnect failed, count=%d", recv2.Load())
	}
}

// Warmup 预拨号应在无业务 Forward 时即建立出站长连接（消除首帧延迟），且幂等。
func TestTCPNodeTransport_WarmupEstablishesConn(t *testing.T) {
	a, b, recv := startPair(t)
	defer a.Close()
	defer b.Close()

	if err := a.Warmup("logic-0"); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	a.mu.RLock()
	c := a.outConns["logic-0"]
	a.mu.RUnlock()
	if c == nil || c.dead.Load() {
		t.Fatal("warmup did not establish a live outbound connection")
	}
	first := c

	// 幂等：再次 Warmup 复用同一连接，不新建。
	if err := a.Warmup("logic-0"); err != nil {
		t.Fatalf("warmup second: %v", err)
	}
	a.mu.RLock()
	again := a.outConns["logic-0"]
	a.mu.RUnlock()
	if again != first {
		t.Fatal("warmup should be idempotent and reuse existing connection")
	}

	// 预热后业务 Forward 立即可达。
	_ = a.Forward("logic-0", InnerHeader{UID: "u", MsgType: MsgUnicast, ForwardSeq: 1},
		&transport.Frame{Ver: 1, MsgID: 0x1000, Seq: 1})
	d := time.Now().Add(time.Second)
	for recv.Load() == 0 && time.Now().Before(d) {
		time.Sleep(5 * time.Millisecond)
	}
	if recv.Load() != 1 {
		t.Fatalf("forward after warmup failed, count=%d", recv.Load())
	}
}
