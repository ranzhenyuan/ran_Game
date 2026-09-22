// gateway_logic_test.go 走查 12（Gateway/Logic 拆分）端到端测试。
//
// 验证链路：客户端 → GatewayConnector（路由表+转发）→ NodeTransport →
// LogicHandler（建会话+分发）→ 响应回传 → GatewayConnector 写回客户端。

package app_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/app"
	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/gateway"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/router"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// pipeConn 基于 net.Pipe 的 transport.Conn 实现（测试用）。
type pipeConn struct {
	id   uint64
	conn net.Conn
	meta sync.Map
}

func newPipeConn(id uint64, c net.Conn) *pipeConn {
	return &pipeConn{id: id, conn: c}
}

func (c *pipeConn) ID() uint64 { return c.id }
func (c *pipeConn) Read(_ context.Context) (*transport.Frame, error) {
	return transport.DecodeFrame(c.conn, 0)
}
func (c *pipeConn) Push(data []byte) error {
	_, err := c.conn.Write(data)
	return err
}
func (c *pipeConn) Close(_ string) error { return c.conn.Close() }
func (c *pipeConn) RemoteAddr() string   { return c.conn.RemoteAddr().String() }
func (c *pipeConn) Meta() *sync.Map      { return &c.meta }

// sendFrame 向 net.Conn 写一帧。
func sendFrame(t *testing.T, c net.Conn, msgID framework.MsgID, seq uint32, body any) {
	t.Helper()
	var raw []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		raw = b
	}
	f := &transport.Frame{Ver: transport.ProtocolVer, MsgID: uint32(msgID), Seq: seq, Body: raw}
	enc, err := transport.EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(enc); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// recvFrame 从 net.Conn 读一帧。
func recvFrame(t *testing.T, c net.Conn) *transport.Frame {
	t.Helper()
	f, err := transport.DecodeFrame(c, 0)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return f
}

// setupTCPSplitStack 装配真实跨节点链路：两个独立 TCPNodeTransport 经 loopback 通信，
// 模拟 K8s 中 Gateway Pod 与 Logic Pod 各自监听 7002、按 Registry 地址懒拨号。
func setupTCPSplitStack(t *testing.T) (net.Conn, func()) {
	t.Helper()

	reg := cluster.NewMemRegistry()
	t.Cleanup(func() { _ = reg.Close() })

	// 两个节点的内部 Transport，独立监听随机 loopback 端口。
	gwTr := gateway.NewTCPNodeTransport("gw-0", "127.0.0.1:0", nil)
	logicTr := gateway.NewTCPNodeTransport("logic-0", "127.0.0.1:0", nil)
	gwAddr, err := gwTr.Listen()
	if err != nil {
		t.Fatalf("gw transport listen: %v", err)
	}
	logicAddr, err := logicTr.Listen()
	if err != nil {
		t.Fatalf("logic transport listen: %v", err)
	}
	// 双向地址簿：等价生产环境 Registry 通告地址解析。
	gwTr.AddPeerAddr("logic-0", logicAddr)
	logicTr.AddPeerAddr("gw-0", gwAddr)

	gwAssem := gateway.NewGatewayWithTransport("gw-0", reg, gwTr)
	logicAssem := gateway.NewGatewayWithTransport("logic-0", reg, logicTr)
	t.Cleanup(gwAssem.Close)
	t.Cleanup(logicAssem.Close)

	// Logic 侧：会话管理器 + 路由器 + LogicHandler。
	eng := actor.New(actor.Config{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		eng.Shutdown(ctx)
	})
	r := router.New(eng)
	r.Register(msgEcho, func() any { return &echoReq{} },
		func(ctx framework.MsgCtx, req any) (any, error) {
			return echoResp{Greeting: "hi " + req.(*echoReq).Name}, nil
		})

	mgr := session.NewManager(session.Config{
		ReplayCapacity: 256,
		GracePeriod:    30 * time.Second,
		SweepInterval:  time.Hour,
		HeartbeatMS:    15000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.Start(ctx)

	logicH := app.NewLogicHandler(mgr, r, logicAssem.Transport(), "logic-0", slog.Default())
	logicH.SetOnSessionStart(func(sess *session.Session) {
		if _, ok := eng.ResolvePlayer(sess.UID()); ok {
			return
		}
		id, err := eng.Spawn(noopActor{}, framework.MailboxPolicy{}, 0)
		if err != nil {
			t.Errorf("spawn player: %v", err)
			return
		}
		eng.Registry().BindUID(sess.UID(), id)
	})

	// Registry 注册 Logic（Addr 为内部通告地址，生产由 PodIP:7002 注入）。
	_ = reg.Register(cluster.NodeInfo{
		ID:    "logic-0",
		Role:  cluster.RoleLogic,
		Addr:  logicAddr,
		State: cluster.StateActive,
	})

	gc := app.NewGatewayConnector(gwAssem, slog.Default())

	srvConn, cliConn := net.Pipe()
	go gc.ServeConn(ctx, newPipeConn(1, srvConn))

	cleanup := func() {
		cancel()
		_ = cliConn.Close()
	}
	return cliConn, cleanup
}

// TestSplitTCP_LoginAndEcho 验证真实 TCP 节点链路（4B 长度前缀 + InnerHeader 编解码
// + 写聚合）：登录转发 → 建会话 → 业务帧分发 → 下行回写，全部跨 TCP 连接完成。
func TestSplitTCP_LoginAndEcho(t *testing.T) {
	cli, cleanup := setupTCPSplitStack(t)
	defer cleanup()

	// 1. 登录：Gateway → TCP → Logic，响应原路返回。
	sendFrame(t, cli, framework.MsgLogin, 1, session.LoginReq{UID: "tcp-alice"})
	loginResp := recvFrame(t, cli)
	loginEnv, err := protocol.DecodeEnvelope(loginResp)
	if err != nil {
		t.Fatal(err)
	}
	if loginEnv.Code != framework.OK {
		t.Fatalf("login failed: code=%d msg=%s", loginEnv.Code, loginEnv.Msg)
	}
	var loginBody session.LoginResp
	_ = json.Unmarshal(loginEnv.Body, &loginBody)
	if loginBody.SessionID != "tcp-alice" {
		t.Fatalf("session id mismatch: %s", loginBody.SessionID)
	}

	// 2. 业务帧经 TCP 内部链路到 Logic Actor，再下行回写。
	sendFrame(t, cli, msgEcho, 2, echoReq{Name: "tcp-world"})
	echoFrame := recvFrame(t, cli)
	echoEnv, err := protocol.DecodeEnvelope(echoFrame)
	if err != nil {
		t.Fatal(err)
	}
	if echoEnv.Code != framework.OK {
		t.Fatalf("echo failed: code=%d", echoEnv.Code)
	}
	var body echoResp
	_ = json.Unmarshal(echoEnv.Body, &body)
	if body.Greeting != "hi tcp-world" {
		t.Fatalf("echo body mismatch: %+v", body)
	}
}

// setupSplitStack 装配 Gateway+Logic 拆分栈（同进程共享 MemNodeTransport）。
func setupSplitStack(t *testing.T) (*app.GatewayConnector, net.Conn, func()) {
	t.Helper()

	// 共享 Gateway：路由表 + MemNodeTransport（同进程直达）。
	reg := cluster.NewMemRegistry()
	t.Cleanup(func() { _ = reg.Close() })
	gw := gateway.NewGateway("gw-0", reg)

	// Logic 侧：会话管理器 + 路由器 + LogicHandler。
	eng := actor.New(actor.Config{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		eng.Shutdown(ctx)
	})
	r := router.New(eng)
	r.Register(msgEcho, func() any { return &echoReq{} },
		func(ctx framework.MsgCtx, req any) (any, error) {
			return echoResp{Greeting: "hi " + req.(*echoReq).Name}, nil
		})

	mgr := session.NewManager(session.Config{
		ReplayCapacity: 256,
		GracePeriod:    30 * time.Second,
		SweepInterval:  time.Hour,
		HeartbeatMS:    15000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.Start(ctx)

	dbg := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logicH := app.NewLogicHandler(mgr, r, gw.Transport(), "logic-0", dbg)
	logicH.SetOnSessionStart(func(sess *session.Session) {
		if _, ok := eng.ResolvePlayer(sess.UID()); ok {
			return
		}
		id, err := eng.Spawn(noopActor{}, framework.MailboxPolicy{}, 0)
		if err != nil {
			t.Errorf("spawn player: %v", err)
			return
		}
		eng.Registry().BindUID(sess.UID(), id)
	})

	// 注册 Logic 节点到注册表，供 Gateway.PickLogicNode 使用。
	_ = reg.Register(cluster.NodeInfo{
		ID:    "logic-0",
		Role:  cluster.RoleLogic,
		State: cluster.StateActive,
	})

	// Gateway 侧连接器。
	gc := app.NewGatewayConnector(gw, dbg)

	// 用 net.Pipe 模拟客户端连接。
	srvConn, cliConn := net.Pipe()
	go gc.ServeConn(ctx, newPipeConn(1, srvConn))

	cleanup := func() {
		cancel()
		_ = cliConn.Close()
	}
	return gc, cliConn, cleanup
}

// TestSplit_LoginAndEcho 验证拆分态登录 + 业务消息端到端。
func TestSplit_LoginAndEcho(t *testing.T) {
	_, cli, cleanup := setupSplitStack(t)
	defer cleanup()

	// 1. 登录
	sendFrame(t, cli, framework.MsgLogin, 1, session.LoginReq{UID: "alice"})
	loginResp := recvFrame(t, cli)
	loginEnv, err := protocol.DecodeEnvelope(loginResp)
	if err != nil {
		t.Fatal(err)
	}
	if loginEnv.Code != framework.OK {
		t.Fatalf("login failed: code=%d msg=%s", loginEnv.Code, loginEnv.Msg)
	}
	var loginBody session.LoginResp
	_ = json.Unmarshal(loginEnv.Body, &loginBody)
	if loginBody.SessionID != "alice" {
		t.Fatalf("session id mismatch: %s", loginBody.SessionID)
	}
	if loginBody.ReconnectToken == "" {
		t.Fatal("reconnect token empty")
	}

	// 2. 业务消息（echo）
	sendFrame(t, cli, msgEcho, 2, echoReq{Name: "world"})
	echoFrame := recvFrame(t, cli)
	echoEnv, err := protocol.DecodeEnvelope(echoFrame)
	if err != nil {
		t.Fatal(err)
	}
	if echoEnv.Code != framework.OK {
		t.Fatalf("echo failed: code=%d", echoEnv.Code)
	}
	var body echoResp
	_ = json.Unmarshal(echoEnv.Body, &body)
	if body.Greeting != "hi world" {
		t.Fatalf("echo body mismatch: %+v", body)
	}
}

// TestSplit_PingPong 验证心跳在 Gateway 直回（不进内部链路）。
func TestSplit_PingPong(t *testing.T) {
	_, cli, cleanup := setupSplitStack(t)
	defer cleanup()

	// 先登录建立路由
	sendFrame(t, cli, framework.MsgLogin, 1, session.LoginReq{UID: "bob"})
	_ = recvFrame(t, cli)

	// 心跳应直接由 Gateway 应答
	sendFrame(t, cli, framework.MsgPing, 2, session.Ping{ClientTS: 12345})
	pong := recvFrame(t, cli)
	if framework.MsgID(pong.MsgID) != framework.MsgPong {
		t.Fatalf("expected pong, got msgID=%d", pong.MsgID)
	}
}

// TestSplit_Reconnect 验证拆分态重连：token 带 nodeID 前缀（Gateway 据此路由）。
func TestSplit_Reconnect(t *testing.T) {
	_, cli, cleanup := setupSplitStack(t)
	defer cleanup()

	// 登录
	sendFrame(t, cli, framework.MsgLogin, 1, session.LoginReq{UID: "carol"})
	loginResp := recvFrame(t, cli)
	loginEnv, err := protocol.DecodeEnvelope(loginResp)
	if err != nil {
		t.Fatal(err)
	}
	var loginBody session.LoginResp
	_ = json.Unmarshal(loginEnv.Body, &loginBody)
	token := loginBody.ReconnectToken

	// 重连 token 必须带 "logic-0:" 前缀，Gateway 据此转发到对应 Logic 节点。
	prefix := "logic-0:"
	if len(token) <= len(prefix) || token[:len(prefix)] != prefix {
		t.Fatalf("reconnect token should have nodeID prefix %q, got %q", prefix, token)
	}
}
