package transport_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rangame/server/internal/transport"
)

func startWSAcceptor(t *testing.T, origins []string) *transport.WSAcceptor {
	t.Helper()
	a, err := transport.NewWSAcceptor("127.0.0.1:0", "/ws", transport.WSOptions{
		AllowedOrigins:       origins,
		ProtocolPingInterval: 200 * time.Millisecond,
		Options: transport.Options{
			ReadTimeout: 2 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func dialWS(t *testing.T, addr string, origin string) *websocket.Conn {
	t.Helper()
	d := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	h := http.Header{}
	if origin != "" {
		h.Set("Origin", origin)
	}
	c, _, err := d.Dial("ws://"+addr+"/ws", h)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// 服务端 echo：读帧 → 原样编码回写。
func serveEcho(t *testing.T, a *transport.WSAcceptor) {
	t.Helper()
	go func() {
		for {
			conn, err := a.Accept(context.Background())
			if err != nil {
				return
			}
			go func(conn transport.Conn) {
				defer conn.Close("echo done")
				for {
					f, err := conn.Read(context.Background())
					if err != nil {
						return
					}
					raw, err := transport.EncodeFrame(f)
					if err != nil {
						return
					}
					if conn.Push(raw) != nil {
						return
					}
				}
			}(conn)
		}
	}()
}

func readFrame(t *testing.T, c *websocket.Conn) *transport.Frame {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read ws: %v", err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("want binary, got %d", mt)
	}
	f, err := transport.DecodeFrame(bytes.NewReader(data), len(data))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return f
}

func TestWSEchoAndProtocolPing(t *testing.T) {
	a := startWSAcceptor(t, nil)
	serveEcho(t, a)

	c := dialWS(t, a.Addr(), "http://"+a.Addr()) // 同源 Origin 应放行

	orig := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: 0x1000,
		Flag:  0,
		Seq:   42,
		Body:  []byte(`{"hello":"ws"}`),
	}
	raw, err := transport.EncodeFrame(orig)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteMessage(websocket.BinaryMessage, raw); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := readFrame(t, c)
	if got.MsgID != 0x1000 || got.Seq != 42 || string(got.Body) != `{"hello":"ws"}` {
		t.Fatalf("echo mismatch: %+v", got)
	}

	// 协议层 ping/pong：gorilla 的 ReadMessage 不返回控制帧，用处理器信号通道观察。
	serverPingCh := make(chan struct{}, 4)
	pongCh := make(chan struct{}, 4)
	c.SetPingHandler(func(appData string) error {
		select {
		case serverPingCh <- struct{}{}:
		default:
		}
		// 保留默认行为：回 pong。
		deadline := time.Now().Add(time.Second)
		return c.WriteControl(websocket.PongMessage, []byte(appData), deadline)
	})
	c.SetPongHandler(func(string) error {
		select {
		case pongCh <- struct{}{}:
		default:
		}
		return nil
	})
	// 控制帧只在 ReadMessage 循环内被处理，起后台排空协程。
	go func() {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// ① 客户端 ping → 服务端默认 ping handler 自动回 pong。
	if err := c.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("ping: %v", err)
	}
	select {
	case <-pongCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no pong from server")
	}
	// ② 服务端按 200ms 间隔主动 ping，读超时被持续刷新（连接存活）。
	select {
	case <-serverPingCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no protocol ping from server")
	}
}

func TestWSRejectTextFrame(t *testing.T) {
	a := startWSAcceptor(t, nil)
	connCh := make(chan transport.Conn, 1)
	go func() {
		conn, err := a.Accept(context.Background())
		if err == nil {
			connCh <- conn
		}
	}()

	c := dialWS(t, a.Addr(), "")
	if err := c.WriteMessage(websocket.TextMessage, []byte("not-binary")); err != nil {
		t.Fatal(err)
	}
	conn := <-connCh
	// 服务端 Read 必须返回 ErrTextFrameNotSupported 并随后关闭连接。
	if _, err := conn.Read(context.Background()); err != transport.ErrTextFrameNotSupported {
		t.Fatalf("err = %v, want ErrTextFrameNotSupported", err)
	}
}

func TestWSOriginPolicy(t *testing.T) {
	// 跨域 Origin 在默认同源策略下应被 403 拒绝。
	a := startWSAcceptor(t, nil)
	d := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	_, resp, err := d.Dial("ws://"+a.Addr()+"/ws", http.Header{"Origin": []string{"http://evil.example"}})
	if err == nil {
		t.Fatal("cross-origin dial should fail")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %+v err=%v", resp, err)
	}

	// 白名单显式放行。
	a2 := startWSAcceptor(t, []string{"http://trusted.example"})
	serveEcho(t, a2)
	c := dialWS(t, a2.Addr(), "http://trusted.example")
	_ = c
}
