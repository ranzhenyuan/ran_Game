package app_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/app"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/router"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

const (
	msgEcho framework.MsgID = 0x1000
	msgSnap framework.MsgID = 0x2001
)

type noopActor struct{}

func (noopActor) Init(framework.ActorCtx)                           {}
func (noopActor) OnMessage(framework.ActorCtx, *framework.Envelope) {}
func (noopActor) OnStop(framework.ActorCtx)                         {}

type echoReq struct {
	Name string `json:"name"`
}
type echoResp struct {
	Greeting string `json:"greeting"`
}

type wsClient struct {
	t       *testing.T
	c       *websocket.Conn
	pending []byte // 单个 WS 消息可能聚合多帧（写聚合），按 4B 长度前缀切分
}

func dialClient(t *testing.T, addr string) *wsClient {
	t.Helper()
	d := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	c, _, err := d.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &wsClient{t: t, c: c}
}

func (w *wsClient) send(msgID framework.MsgID, seq uint32, body any) {
	w.t.Helper()
	var raw []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			w.t.Fatal(err)
		}
		raw = b
	}
	f := &transport.Frame{Ver: transport.ProtocolVer, MsgID: uint32(msgID), Seq: seq, Body: raw}
	enc, err := transport.EncodeFrame(f)
	if err != nil {
		w.t.Fatal(err)
	}
	if err := w.c.WriteMessage(websocket.BinaryMessage, enc); err != nil {
		w.t.Fatalf("write: %v", err)
	}
}

func (w *wsClient) read() *transport.Frame {
	w.t.Helper()

	// 等待缓冲内至少包含一个完整帧（len(4B)+payload）；帧不会跨 WS 消息截断，
	// 但一个消息可能携带多帧（服务端 2ms 写聚合）。
	for {
		if len(w.pending) >= 4 {
			plen := int(binary.BigEndian.Uint32(w.pending[:4]))
			total := 4 + plen
			if len(w.pending) >= total {
				f, err := transport.DecodeFrame(bytes.NewReader(w.pending[:total]), total)
				if err != nil {
					w.t.Fatal(err)
				}
				w.pending = w.pending[total:]
				return f
			}
		}
		_ = w.c.SetReadDeadline(time.Now().Add(3 * time.Second))
		mt, data, err := w.c.ReadMessage()
		if err != nil {
			w.t.Fatalf("read: %v", err)
		}
		if mt != websocket.BinaryMessage {
			w.t.Fatalf("want binary, got %d", mt)
		}
		w.pending = append(w.pending, data...)
	}
}

func (w *wsClient) closeNow() { _ = w.c.Close() }

func envBody(t *testing.T, f *transport.Frame, out any) protocol.ErrorEnvelope {
	t.Helper()
	env, err := protocol.DecodeEnvelope(f)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil && len(env.Body) > 0 {
		if err := json.Unmarshal(env.Body, out); err != nil {
			t.Fatal(err)
		}
	}
	return *env
}

// startStack 装配 WS Acceptor + Manager + Router + Connector。
func startStack(t *testing.T) (*session.Manager, string) {
	t.Helper()
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

	conn := app.NewConnector(mgr, r, slog.Default(), 4096, 15000)
	conn.OnSessionStart = func(sess *session.Session) {
		if _, ok := eng.ResolvePlayer(sess.UID()); ok {
			return
		}
		id, err := eng.Spawn(noopActor{}, framework.MailboxPolicy{}, 0)
		if err != nil {
			t.Errorf("spawn player: %v", err)
			return
		}
		eng.Registry().BindUID(sess.UID(), id)
	}

	a, err := transport.NewWSAcceptor("127.0.0.1:0", "/ws", transport.WSOptions{
		Options: transport.Options{ReadTimeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go mgr.Start(ctx)
	go func() {
		for {
			c, err := a.Accept(ctx)
			if err != nil {
				return
			}
			go conn.ServeConn(ctx, c)
		}
	}()
	return mgr, a.Addr()
}

func waitOffline(t *testing.T, sess *session.Session) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sess.IsOffline() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("session did not enter grace offline state")
}

// 端到端：登录 → 在线可靠推送 → 断线宽限缓冲 → 重连重放 + 快照 → 心跳/业务消息 → 旧 token 失效。
func TestConnectorLoginReconnectFlow(t *testing.T) {
	mgr, addr := startStack(t)

	// ① 登录。
	c1 := dialClient(t, addr)
	c1.send(framework.MsgLogin, 1, session.LoginReq{UID: "p1"})
	loginFrame := c1.read()
	var login session.LoginResp
	if env := envBody(t, loginFrame, &login); env.Code != framework.OK || login.ReconnectToken == "" {
		t.Fatalf("login failed: %+v", env)
	}
	if loginFrame.MsgID != uint32(framework.MsgLogin) {
		t.Fatalf("login frame msgID = %d", loginFrame.MsgID)
	}

	sess, ok := mgr.Get("p1")
	if !ok {
		t.Fatal("session not registered")
	}

	// ② 在线期间服务端可靠推送 seq=1。
	if err := sess.PushReliable(msgEcho, echoResp{Greeting: "push1"}); err != nil {
		t.Fatal(err)
	}
	f := c1.read()
	if f.Seq != 1 || f.Flag&transport.FlagSnapshot != 0 {
		t.Fatalf("online reliable frame wrong: %+v", f)
	}

	// ③ 断线：读泵退出 → 宽限离线；离线期间继续累积可靠帧与快照。
	c1.closeNow()
	waitOffline(t, sess)

	if err := sess.PushReliable(msgEcho, echoResp{Greeting: "push2"}); err != nil {
		t.Fatalf("offline reliable push: %v", err)
	}
	if err := sess.PushSnapshot(msgSnap, echoResp{Greeting: "snap"}); err != nil {
		t.Fatalf("offline snapshot push: %v", err)
	}

	// ④ 宽限期内重连：收到新 token、重放帧（seq=2）、快照帧。
	c2 := dialClient(t, addr)
	c2.send(framework.MsgReconnect, 2, session.ReconnectReq{
		ReconnectToken:  login.ReconnectToken,
		LastReliableSeq: 1,
	})

	rcFrame := c2.read()
	var rc session.ReconnectResp
	if env := envBody(t, rcFrame, &rc); env.Code != framework.OK || rc.ResyncRequired {
		t.Fatalf("reconnect resp: %+v", env)
	}
	if rc.NewReconnectToken == "" || rc.NewReconnectToken == login.ReconnectToken {
		t.Fatal("reconnect must rotate token")
	}

	replayed := c2.read()
	if replayed.Seq != 2 || replayed.Flag&transport.FlagSnapshot != 0 {
		t.Fatalf("replayed reliable frame wrong: %+v", replayed)
	}
	var gotPush echoResp
	if err := json.Unmarshal(replayed.Body, &gotPush); err != nil || gotPush.Greeting != "push2" {
		t.Fatalf("replayed body: %v %+v", err, gotPush)
	}

	snapFrame := c2.read()
	if snapFrame.MsgID != uint32(msgSnap) || snapFrame.Flag&transport.FlagSnapshot == 0 {
		t.Fatalf("snapshot frame wrong: %+v", snapFrame)
	}

	// 会话已在线，旧 token 已失效。
	if !sess.IsOnline() {
		t.Fatal("session should be online")
	}
	if sess.Token() != rc.NewReconnectToken {
		t.Fatal("manager session token should match rotated token")
	}

	// ⑤ 应用层心跳读泵直回（不经过 Actor 邮箱）。
	c2.send(framework.MsgPing, 3, session.Ping{ClientTS: 123})
	pongFrame := c2.read()
	var pong session.Pong
	if env := envBody(t, pongFrame, &pong); env.Code != framework.OK || pong.ClientTS != 123 || pong.ServerTS == 0 {
		t.Fatalf("pong wrong: %+v %+v", env, pong)
	}
	if pongFrame.MsgID != uint32(framework.MsgPong) {
		t.Fatalf("pong msgID = %d", pongFrame.MsgID)
	}

	// ⑥ 业务消息经 Router → PlayerActor → 自动 OK 应答。
	c2.send(msgEcho, 99, echoReq{Name: "ran"})
	echoFrame := c2.read()
	var gotEcho echoResp
	if env := envBody(t, echoFrame, &gotEcho); env.Code != framework.OK || gotEcho.Greeting != "hi ran" {
		t.Fatalf("echo wrong: %+v %+v", env, gotEcho)
	}
	if echoFrame.Seq != 99 {
		t.Fatalf("echo seq = %d", echoFrame.Seq)
	}

	// ⑦ PullSnapshot：先 OK 应答，再逐帧推最新快照。
	c2.send(framework.MsgPullSnap, 4, session.PullSnapshotReq{MsgIDs: []uint32{uint32(msgSnap)}})
	pullOK := c2.read()
	if pullOK.MsgID != uint32(framework.MsgPullSnap) {
		t.Fatalf("pull snap resp msgID = %d", pullOK.MsgID)
	}
	if env, _ := protocol.DecodeEnvelope(pullOK); env.Code != framework.OK {
		t.Fatalf("pull snap code = %d", env.Code)
	}
	pulled := c2.read()
	if pulled.MsgID != uint32(msgSnap) || pulled.Flag&transport.FlagSnapshot == 0 {
		t.Fatalf("pulled snapshot wrong: %+v", pulled)
	}

	// ⑧ 旧 reconnectToken 一次性消费，再用必须失败（2002），连接被关闭。
	c3 := dialClient(t, addr)
	c3.send(framework.MsgReconnect, 5, session.ReconnectReq{
		ReconnectToken:  login.ReconnectToken,
		LastReliableSeq: 2,
	})
	bad := c3.read()
	if env, _ := protocol.DecodeEnvelope(bad); env.Code != framework.ErrTokenInvalid {
		t.Fatalf("reused token code = %d, want %d", env.Code, framework.ErrTokenInvalid)
	}
}

// 未登录直接发业务消息 → 3001 鉴权拒绝，且连接继续可用（可重试登录）。
func TestConnectorUnauthBeforeLogin(t *testing.T) {
	_, addr := startStack(t)
	c := dialClient(t, addr)

	c.send(msgEcho, 1, echoReq{Name: "x"})
	f := c.read()
	env, _ := protocol.DecodeEnvelope(f)
	if env.Code != framework.ErrUnauthorized {
		t.Fatalf("code = %d, want ErrUnauthorized", env.Code)
	}

	// 拒绝后仍可正常登录。
	c.send(framework.MsgLogin, 2, session.LoginReq{UID: "p2"})
	login := c.read()
	if env, _ := protocol.DecodeEnvelope(login); env.Code != framework.OK {
		t.Fatalf("login after reject: %d", env.Code)
	}
}
