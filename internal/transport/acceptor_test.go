package transport_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rangame/server/internal/transport"
)

// startEchoServer 启动 echo 形态 Acceptor，返回实际监听地址与清理函数。
func startEchoServer(t *testing.T, readTimeout time.Duration) (string, func()) {
	t.Helper()

	opts := transport.DefaultOptions()
	opts.ReadTimeout = readTimeout
	opts.WriteFlushInterval = time.Millisecond

	acc, err := transport.NewTCPAcceptor("127.0.0.1:0", opts)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			c, err := acc.Accept(ctx)
			if err != nil {
				return
			}
			go func(c transport.Conn) {
				defer c.Close("echo end")
				for {
					f, err := c.Read(ctx)
					if err != nil {
						return
					}
					out, err := transport.EncodeFrame(f)
					if err != nil {
						return
					}
					if err := c.Push(out); err != nil {
						return
					}
				}
			}(c)
		}
	}()

	cleanup := func() {
		cancel()
		_ = acc.Close()
	}
	return acc.Addr(), cleanup
}

func TestEchoOverTCP(t *testing.T) {
	addr, cleanup := startEchoServer(t, 5*time.Second)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	want := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: 0x1001,
		Flag:  0,
		Seq:   99,
		Body:  []byte("hello echo"),
	}
	raw, err := transport.EncodeFrame(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := transport.DecodeFrame(conn, transport.DefaultMaxFrame)
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if got.MsgID != want.MsgID || got.Seq != want.Seq || string(got.Body) != string(want.Body) {
		t.Fatalf("echo mismatch: %+v", got)
	}
}

func TestReadTimeoutClosesConn(t *testing.T) {
	// 读超时 300ms：客户端不发任何数据，服务端应在超时后关闭连接。
	addr, cleanup := startEchoServer(t, 300*time.Millisecond)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 8)
	_, err = conn.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF after server read timeout, got %v", err)
	}
}

func TestWriteAggregationBatchesMultipleFrames(t *testing.T) {
	addr, cleanup := startEchoServer(t, 5*time.Second)
	defer cleanup()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// 连发 3 帧；服务端写聚合可能合并为 1 次 TCP Write，客户端仍应解出 3 帧。
	for i := uint32(1); i <= 3; i++ {
		raw, _ := transport.EncodeFrame(&transport.Frame{
			Ver: transport.ProtocolVer, MsgID: i, Seq: i, Body: []byte("x"),
		})
		if _, err := conn.Write(raw); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	seen := map[uint32]bool{}
	deadline := time.Now().Add(3 * time.Second)
	for len(seen) < 3 {
		_ = conn.SetReadDeadline(deadline)
		f, err := transport.DecodeFrame(conn, transport.DefaultMaxFrame)
		if err != nil {
			t.Fatalf("decode: %v (seen=%v)", err, seen)
		}
		seen[f.Seq] = true
	}
}
