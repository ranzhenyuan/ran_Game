// gateway_logic_test.go 走查 12（Gateway/Logic 拆分）端到端测试。
//
// 验证链路：客户端 → GatewayConnector（路由表+转发）→ NodeTransport →
// LogicHandler（建会话+分发）→ 响应回传 → GatewayConnector 写回客户端。

package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/app"
	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/gateway"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/router"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
	"github.com/redis/go-redis/v9"
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

	// 共享路由表：模拟生产 Redis 共享表（Gateway 与 Logic 跨进程必须读写同一份）。
	routes := gateway.NewMemRouteTable(reg)
	t.Cleanup(routes.Close)
	gwAssem := gateway.NewGatewayWithDeps("gw-0", reg, routes, gwTr)
	logicAssem := gateway.NewGatewayWithDeps("logic-0", reg, routes, logicTr)
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

	logicH := app.NewLogicHandler(mgr, r, logicAssem.Transport(), logicAssem.Routes(), "logic-0", slog.Default())
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
	logicH := app.NewLogicHandler(mgr, r, gw.Transport(), gw.Routes(), "logic-0", dbg)
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

// newAppMiniredis 起一个 miniredis 并返回共享 client（测试用，不污染全局）。
func newAppMiniredis(t *testing.T) *redis.Client {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Skipf("miniredis unavailable: %v", err)
	}
	t.Cleanup(s.Close)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// setupRedisTwoGateway 装配两个独立 Gateway 副本 + 一个 Logic，三者各自持有一份
// 基于同一 miniredis 的 RedisRouteTable（模拟生产跨进程共享 Redis），内部链路走 TCP。
//
// 返回：gwA 客户端连接、gwB 客户端连接、gwB 的路由表（用于断言跨副本可见性）、清理函数。
func setupRedisTwoGateway(t *testing.T) (net.Conn, net.Conn, gateway.RouteTable, func()) {
	t.Helper()

	client := newAppMiniredis(t)
	reg := cluster.NewMemRegistry()
	t.Cleanup(func() { _ = reg.Close() })

	// 三个进程各持一份路由表（底层共享同一 Redis）：这正是多副本的核心语义。
	gwARoutes := gateway.NewRedisRouteTable(client, reg, time.Hour)
	gwBRoutes := gateway.NewRedisRouteTable(client, reg, time.Hour)
	logicRoutes := gateway.NewRedisRouteTable(client, reg, time.Hour)
	t.Cleanup(gwARoutes.Close)
	t.Cleanup(gwBRoutes.Close)
	t.Cleanup(logicRoutes.Close)

	// 三个独立 TCP 内部 Transport，模拟三 Pod 各自监听 7002。
	gwATr := gateway.NewTCPNodeTransport("gw-a", "127.0.0.1:0", nil)
	gwBTr := gateway.NewTCPNodeTransport("gw-b", "127.0.0.1:0", nil)
	logicTr := gateway.NewTCPNodeTransport("logic-0", "127.0.0.1:0", nil)
	gwAAddr, err := gwATr.Listen()
	if err != nil {
		t.Fatalf("gw-a listen: %v", err)
	}
	gwBAddr, err := gwBTr.Listen()
	if err != nil {
		t.Fatalf("gw-b listen: %v", err)
	}
	logicAddr, err := logicTr.Listen()
	if err != nil {
		t.Fatalf("logic listen: %v", err)
	}
	// 地址簿：网关→logic，logic→两个网关（下行按来源 GatewayID 回投）。
	gwATr.AddPeerAddr("logic-0", logicAddr)
	gwBTr.AddPeerAddr("logic-0", logicAddr)
	logicTr.AddPeerAddr("gw-a", gwAAddr)
	logicTr.AddPeerAddr("gw-b", gwBAddr)

	gwA := gateway.NewGatewayWithDeps("gw-a", reg, gwARoutes, gwATr)
	gwB := gateway.NewGatewayWithDeps("gw-b", reg, gwBRoutes, gwBTr)
	t.Cleanup(gwA.Close)
	t.Cleanup(gwB.Close)

	// Logic 侧：会话管理器 + 路由器 + LogicHandler（用 logicRoutes 写一级路由）。
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

	logicH := app.NewLogicHandler(mgr, r, logicTr, logicRoutes, "logic-0", slog.Default())
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

	_ = reg.Register(cluster.NodeInfo{
		ID:    "logic-0",
		Role:  cluster.RoleLogic,
		Addr:  logicAddr,
		State: cluster.StateActive,
	})

	gcA := app.NewGatewayConnector(gwA, slog.Default())
	gcB := app.NewGatewayConnector(gwB, slog.Default())

	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	go gcA.ServeConn(ctx, newPipeConn(1, srvA))
	go gcB.ServeConn(ctx, newPipeConn(2, srvB))

	cleanup := func() {
		cancel()
		_ = cliA.Close()
		_ = cliB.Close()
	}
	return cliA, cliB, gwBRoutes, cleanup
}

// TestRedisRoute_TwoGatewaySplit 验证 §12.4 核心：gwA 登录写入的一级路由，
// gwB（独立副本、独立路由表实例）能经共享 Redis 读到并据此路由业务帧。
func TestRedisRoute_TwoGatewaySplit(t *testing.T) {
	cliA, cliB, gwBRoutes, cleanup := setupRedisTwoGateway(t)
	defer cleanup()

	// 1. 客户端经 gwA 登录：Logic 建会话后 Bind uid→logic-0 到共享 Redis。
	sendFrame(t, cliA, framework.MsgLogin, 1, session.LoginReq{UID: "rg-alice"})
	loginEnv, err := protocol.DecodeEnvelope(recvFrame(t, cliA))
	if err != nil {
		t.Fatal(err)
	}
	if loginEnv.Code != framework.OK {
		t.Fatalf("login via gwA failed: code=%d", loginEnv.Code)
	}

	// 2. 跨副本可见性：gwB 从未处理该登录，但其路由表应能从 Redis 解析到 logic-0。
	if got, err := gwBRoutes.Lookup("rg-alice"); err != nil || got != "logic-0" {
		t.Fatalf("gwB cross-gateway lookup: got %q err %v", got, err)
	}

	// 3. 业务帧经 gwA 正常回显（证明 Redis 路由表在入站网关转发链路上可用）。
	sendFrame(t, cliA, msgEcho, 2, echoReq{Name: "gwA-world"})
	echoEnv, err := protocol.DecodeEnvelope(recvFrame(t, cliA))
	if err != nil {
		t.Fatal(err)
	}
	if echoEnv.Code != framework.OK {
		t.Fatalf("echo via gwA failed: code=%d", echoEnv.Code)
	}

	// 4. 同 uid 经 gwB 二次登录（顶踢 gwA 的会话）：gwB 入站链路走通，
	//    Logic 顶踢旧会话后重新 Bind uid→logic-0 到共享 Redis。
	sendFrame(t, cliB, framework.MsgLogin, 1, session.LoginReq{UID: "rg-alice"})
	loginBEnv, err := protocol.DecodeEnvelope(recvFrame(t, cliB))
	if err != nil {
		t.Fatal(err)
	}
	if loginBEnv.Code != framework.OK {
		t.Fatalf("login via gwB failed: code=%d msg=%s", loginBEnv.Code, loginBEnv.Msg)
	}

	// 5. gwB 上的业务帧：forwardBusiness 走 gwBRoutes.Lookup（共享 Redis）→ logic-0，
	//    响应经 logic→gwB 下行回投到 cliB。这一步证明 gwB 用共享路由表完成了业务路由。
	sendFrame(t, cliB, msgEcho, 2, echoReq{Name: "gwB-world"})
	echoBEnv, err := protocol.DecodeEnvelope(recvFrame(t, cliB))
	if err != nil {
		t.Fatal(err)
	}
	if echoBEnv.Code != framework.OK {
		t.Fatalf("echo via gwB failed: code=%d", echoBEnv.Code)
	}
	var body echoResp
	_ = json.Unmarshal(echoBEnv.Body, &body)
	if body.Greeting != "hi gwB-world" {
		t.Fatalf("echo body mismatch: %+v", body)
	}
}

// doEcho 经 cli 发一帧 echo 并断言回显内容（故障演练中轮询复用）。
// 设置短读截止时间：链路中断时快速返回错误而非永久阻塞。
func doEcho(t *testing.T, cli net.Conn, name string) error {
	t.Helper()
	sendFrame(t, cli, msgEcho, 2, echoReq{Name: name})
	_ = cli.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	f, err := transport.DecodeFrame(cli, 0)
	_ = cli.SetReadDeadline(time.Time{}) // 复位
	if err != nil {
		return err
	}
	env, err := protocol.DecodeEnvelope(f)
	if err != nil {
		return err
	}
	if env.Code != framework.OK {
		return fmt.Errorf("echo code=%d", env.Code)
	}
	var body echoResp
	_ = json.Unmarshal(env.Body, &body)
	if body.Greeting != "hi "+name {
		return fmt.Errorf("echo body mismatch: %+v", body)
	}
	return nil
}

// TestSplitTCP_FaultDrill_LogicRestart 端到端故障演练：
// 登录+回显正常 → Logic 进程崩溃（关闭内部 Transport）→ 在同地址重启新 Logic →
// Gateway 侧长连接自愈重连，业务回显恢复，无需客户端重连。
func TestSplitTCP_FaultDrill_LogicRestart(t *testing.T) {
	reg := cluster.NewMemRegistry()
	t.Cleanup(func() { _ = reg.Close() })

	// 短保活/重连间隔，加速故障检测与自愈（仅测试用）。
	gwTr := gateway.NewTCPNodeTransport("gw-0", "127.0.0.1:0", nil,
		gateway.WithKeepaliveInterval(10*time.Millisecond),
		gateway.WithReconnCheck(20*time.Millisecond))
	logicTr1 := gateway.NewTCPNodeTransport("logic-0", "127.0.0.1:0", nil,
		gateway.WithKeepaliveInterval(10*time.Millisecond),
		gateway.WithReconnCheck(20*time.Millisecond))
	gwAddr, err := gwTr.Listen()
	if err != nil {
		t.Fatalf("gw listen: %v", err)
	}
	logicAddr, err := logicTr1.Listen()
	if err != nil {
		t.Fatalf("logic listen: %v", err)
	}
	gwTr.AddPeerAddr("logic-0", logicAddr)
	logicTr1.AddPeerAddr("gw-0", gwAddr)
	t.Cleanup(func() { _ = gwTr.Close() })

	routes := gateway.NewMemRouteTable(reg)
	t.Cleanup(routes.Close)
	gwAssem := gateway.NewGatewayWithDeps("gw-0", reg, routes, gwTr)
	t.Cleanup(gwAssem.Close)

	eng := actor.New(actor.Config{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		eng.Shutdown(ctx)
	})
	r := router.New(eng)
	r.Register(msgEcho, func() any { return &echoReq{} },
		func(_ framework.MsgCtx, req any) (any, error) {
			return echoResp{Greeting: "hi " + req.(*echoReq).Name}, nil
		})
	mgr := session.NewManager(session.Config{ReplayCapacity: 256, GracePeriod: 30 * time.Second, SweepInterval: time.Hour, HeartbeatMS: 15000})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Start(ctx)

	// 首个 Logic 实例。
	logicAssem1 := gateway.NewGatewayWithDeps("logic-0", reg, routes, logicTr1)
	logicH1 := app.NewLogicHandler(mgr, r, logicAssem1.Transport(), logicAssem1.Routes(), "logic-0", slog.Default())
	logicH1.SetOnSessionStart(func(sess *session.Session) {
		if _, ok := eng.ResolvePlayer(sess.UID()); ok {
			return
		}
		id, err := eng.Spawn(noopActor{}, framework.MailboxPolicy{}, 0)
		if err != nil {
			t.Errorf("spawn: %v", err)
			return
		}
		eng.Registry().BindUID(sess.UID(), id)
	})
	_ = reg.Register(cluster.NodeInfo{ID: "logic-0", Role: cluster.RoleLogic, Addr: logicAddr, State: cluster.StateActive})

	gc := app.NewGatewayConnector(gwAssem, slog.Default())
	srvConn, cliConn := net.Pipe()
	go gc.ServeConn(ctx, newPipeConn(1, srvConn))
	defer cliConn.Close()

	// 1. 登录 + 回显正常。
	sendFrame(t, cliConn, framework.MsgLogin, 1, session.LoginReq{UID: "fd-user"})
	if env, err := protocol.DecodeEnvelope(recvFrame(t, cliConn)); err != nil || env.Code != framework.OK {
		t.Fatalf("login failed: err=%v code=%v", err, env.Code)
	}
	if err := doEcho(t, cliConn, "before-crash"); err != nil {
		t.Fatalf("echo before crash: %v", err)
	}

	// 2. Logic 崩溃：关闭其内部 Transport（模拟进程退出），Gateway 连接变 dead。
	logicTr1.Close()

	// 3. 在同一内部地址重启新 Logic 实例（含新会话/路由处理）。
	logicTr2 := gateway.NewTCPNodeTransport("logic-0", logicAddr, nil,
		gateway.WithKeepaliveInterval(10*time.Millisecond),
		gateway.WithReconnCheck(20*time.Millisecond))
	if _, err := logicTr2.Listen(); err != nil {
		t.Fatalf("logic2 listen on same addr: %v", err)
	}
	logicTr2.AddPeerAddr("gw-0", gwAddr)
	t.Cleanup(func() { _ = logicTr2.Close() })
	logicAssem2 := gateway.NewGatewayWithDeps("logic-0", reg, routes, logicTr2)
	logicH2 := app.NewLogicHandler(mgr, r, logicAssem2.Transport(), logicAssem2.Routes(), "logic-0", slog.Default())
	logicH2.SetOnSessionStart(func(sess *session.Session) {
		if _, ok := eng.ResolvePlayer(sess.UID()); ok {
			return
		}
		id, err := eng.Spawn(noopActor{}, framework.MailboxPolicy{}, 0)
		if err != nil {
			t.Errorf("spawn2: %v", err)
			return
		}
		eng.Registry().BindUID(sess.UID(), id)
	})
	_ = reg.Register(cluster.NodeInfo{ID: "logic-0", Role: cluster.RoleLogic, Addr: logicAddr, State: cluster.StateActive})

	// 4. 轮询回显恢复：Gateway 长连接自愈重连后业务帧重新可达。
	var lastErr error
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if err := doEcho(t, cliConn, "after-restart"); err == nil {
			return // 自愈成功
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("echo did not recover after logic restart: %v", lastErr)
}
