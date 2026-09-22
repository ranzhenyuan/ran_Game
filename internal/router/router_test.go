package router

import (
	"sync"
	"testing"
	"time"

	"github.com/rangame/server/internal/actor"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// ---- 测试夹具 ----

type fakeSource struct {
	uid    string
	authed bool
	roomID string

	mu     sync.Mutex
	sent   []*transport.Frame
	frames chan *transport.Frame
}

func newFakeSource(uid string, authed bool) *fakeSource {
	return &fakeSource{
		uid:    uid,
		authed: authed,
		frames: make(chan *transport.Frame, 32),
	}
}

func (s *fakeSource) UID() string { return s.uid }

func (s *fakeSource) Send(f *transport.Frame) error {
	s.mu.Lock()
	s.sent = append(s.sent, f)
	s.mu.Unlock()
	select {
	case s.frames <- f:
	default:
	}
	return nil
}

func (s *fakeSource) Authenticated() bool { return s.authed }

func (s *fakeSource) RoomID() string { return s.roomID }

func (s *fakeSource) waitFrame(t *testing.T) *transport.Frame {
	t.Helper()
	select {
	case f := <-s.frames:
		return f
	case <-time.After(2 * time.Second):
		t.Fatal("no response frame before timeout")
		return nil
	}
}

func (s *fakeSource) sentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

// noopActor 个人/房间 Actor：Invoke 由引擎直接执行，普通消息由 roomCaptureActor 处理。
type noopActor struct{}

func (noopActor) Init(framework.ActorCtx)                           {}
func (noopActor) OnMessage(framework.ActorCtx, *framework.Envelope) {}
func (noopActor) OnStop(framework.ActorCtx)                         {}

type roomCaptureActor struct {
	noopActor
	envs chan *framework.Envelope
}

func (a *roomCaptureActor) OnMessage(_ framework.ActorCtx, env *framework.Envelope) {
	a.envs <- env
}

func testFrame(msgID framework.MsgID, seq uint32, body string) *transport.Frame {
	return &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(msgID),
		Flag:  0, // JSON
		Seq:   seq,
		Body:  []byte(body),
	}
}

func envCode(t *testing.T, f *transport.Frame) (framework.Code, *protocol.ErrorEnvelope) {
	t.Helper()
	env, err := protocol.DecodeEnvelope(f)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return env.Code, env
}

const (
	msgEcho framework.MsgID = 0x1000
	msgRoom framework.MsgID = 0x1001
)

type echoReq struct {
	Name string `json:"name"`
}
type echoResp struct {
	Greeting string `json:"greeting"`
}
type roomReq struct {
	X int `json:"x"`
}

func newTestEngine(t *testing.T) (*actor.Engine, framework.ID) {
	t.Helper()
	e := actor.New(actor.Config{})
	id, err := e.Spawn(noopActor{}, framework.MailboxPolicy{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	e.Registry().BindUID("u1", id)
	return e, id
}

// ---- 用例 ----

func TestDispatchOKAutoReply(t *testing.T) {
	e, playerID := newTestEngine(t)
	r := New(e)
	r.Register(msgEcho, func() any { return &echoReq{} },
		func(ctx framework.MsgCtx, req any) (any, error) {
			r := req.(*echoReq)
			if ctx.UID() != "u1" {
				t.Fatalf("uid mismatch: %s", ctx.UID())
			}
			return &echoResp{Greeting: "hi " + r.Name}, nil
		})

	src := newFakeSource("u1", true)
	r.Dispatch(src, testFrame(msgEcho, 55, `{"name":"ran"}`))

	f := src.waitFrame(t)
	if f.Seq != 55 || f.MsgID != uint32(msgEcho) {
		t.Fatalf("frame header mismatch: %+v", f)
	}
	code, env := envCode(t, f)
	if code != framework.OK {
		t.Fatalf("expected code 0, got %d (%s)", code, env.Msg)
	}
	var body echoResp
	if err := (protocol.JSONCodec{}).Unmarshal(env.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body.Greeting != "hi ran" {
		t.Fatalf("body mismatch: %+v", body)
	}

	// 确认消息确实在 PlayerActor 内执行（路由解析到同一 Actor）。
	if id, ok := e.ResolvePlayer("u1"); !ok || id != playerID {
		t.Fatal("resolve player mismatch")
	}
}

func TestUnknownMessage(t *testing.T) {
	e, _ := newTestEngine(t)
	r := New(e)

	src := newFakeSource("u1", true)
	r.Dispatch(src, testFrame(0x7777, 1, `{}`))

	f := src.waitFrame(t)
	if code, _ := envCode(t, f); code != framework.ErrUnknownMsg {
		t.Fatalf("expected 1001, got %d", code)
	}
}

func TestDecodeFailure(t *testing.T) {
	e, _ := newTestEngine(t)
	r := New(e)
	r.Register(msgEcho, func() any { return &echoReq{} }, nil)

	src := newFakeSource("u1", true)
	r.Dispatch(src, testFrame(msgEcho, 1, `{not-json`))

	f := src.waitFrame(t)
	if code, _ := envCode(t, f); code != framework.ErrDecode {
		t.Fatalf("expected 1002, got %d", code)
	}
}

func TestAuthReject(t *testing.T) {
	e, _ := newTestEngine(t)
	r := New(e)
	r.Register(msgEcho, func() any { return &echoReq{} },
		func(framework.MsgCtx, any) (any, error) { return &echoResp{}, nil })

	// 未登录来源：2001。
	src := newFakeSource("u1", false)
	r.Dispatch(src, testFrame(msgEcho, 1, `{"name":"x"}`))
	f := src.waitFrame(t)
	if code, _ := envCode(t, f); code != framework.ErrUnauthorized {
		t.Fatalf("expected 2001, got %d", code)
	}
}

func TestRateLimit(t *testing.T) {
	e, _ := newTestEngine(t)
	r := New(e, WithRateLimit(1, 1)) // 1/s，突发 1
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.nowFunc = func() time.Time { return fixed }

	r.Register(msgEcho, func() any { return &echoReq{} },
		func(framework.MsgCtx, any) (any, error) { return &echoResp{}, nil })

	src := newFakeSource("u1", true)
	r.Dispatch(src, testFrame(msgEcho, 1, `{"name":"a"}`))
	if code, _ := envCode(t, src.waitFrame(t)); code != framework.OK {
		t.Fatal("first request within burst should pass")
	}

	// 同一时刻第二条：超出令牌桶 → 2003。
	r.Dispatch(src, testFrame(msgEcho, 2, `{"name":"b"}`))
	if code, _ := envCode(t, src.waitFrame(t)); code != framework.ErrRateLimited {
		t.Fatalf("expected 2003, got %d", code)
	}
}

func TestHandlerPanicReturns4001AndActorSurvives(t *testing.T) {
	e, _ := newTestEngine(t)
	r := New(e)

	panicking := true
	r.Register(msgEcho, func() any { return &echoReq{} },
		func(framework.MsgCtx, any) (any, error) {
			if panicking {
				panic("boom")
			}
			return &echoResp{Greeting: "alive"}, nil
		})

	src := newFakeSource("u1", true)
	r.Dispatch(src, testFrame(msgEcho, 1, `{"name":"x"}`))
	if code, _ := envCode(t, src.waitFrame(t)); code != framework.ErrInternal {
		t.Fatalf("expected 4001 on panic")
	}

	// Actor 未死亡：后续消息仍能正常处理（Recover 中间件 + 引擎监督双保险）。
	panicking = false
	r.Dispatch(src, testFrame(msgEcho, 2, `{"name":"x"}`))
	if code, env := envCode(t, src.waitFrame(t)); code != framework.OK {
		t.Fatalf("actor should survive, got code %d (%s)", code, env.Msg)
	}
}

func TestBackpressureReturns4002(t *testing.T) {
	e := actor.New(actor.Config{})
	// 容量 1 + drop 策略的 PlayerActor。
	id, _ := e.Spawn(noopActor{}, framework.MailboxPolicy{Capacity: 1, OnFull: framework.FullDrop}, 0)
	e.Registry().BindUID("u1", id)

	r := New(e)
	gate := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	r.Register(msgEcho, func() any { return &echoReq{} },
		func(framework.MsgCtx, any) (any, error) {
			once.Do(func() { close(started) })
			<-gate // 卡住 Actor，制造积压
			return &echoResp{}, nil
		})

	src := newFakeSource("u1", true)
	r.Dispatch(src, testFrame(msgEcho, 1, `{}`))
	<-started

	// 第二条占满槽位，第三条被 Drop → 同步回 4002。
	r.Dispatch(src, testFrame(msgEcho, 2, `{}`))
	r.Dispatch(src, testFrame(msgEcho, 3, `{}`))

	f := src.waitFrame(t)
	if code, _ := envCode(t, f); code != framework.ErrOverloaded {
		t.Fatalf("expected 4002, got %d", code)
	}
	close(gate)
}

func TestRoomDispatch(t *testing.T) {
	e := actor.New(actor.Config{})
	roomActor := &roomCaptureActor{envs: make(chan *framework.Envelope, 4)}
	roomID, _ := e.Spawn(roomActor, framework.MailboxPolicy{}, 0)
	e.Registry().BindRoom("room-1", roomID)

	// 房间消息不需要 PlayerActor，但前置鉴权要有已登录 Source。
	r := New(e)
	r.RegisterRoom(msgRoom, func() any { return &roomReq{} }, false)

	src := newFakeSource("u1", true)
	src.roomID = "room-1"
	r.Dispatch(src, testFrame(msgRoom, 9, `{"x":42}`))

	select {
	case env := <-roomActor.envs:
		if env.MsgID != msgRoom || env.Seq != 9 {
			t.Fatalf("env header mismatch: %+v", env)
		}
		req, ok := env.Payload.(*roomReq)
		if !ok || req.X != 42 {
			t.Fatalf("payload mismatch: %+v", env.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("room actor did not receive message")
	}

	// 房间消息无自动应答。
	time.Sleep(50 * time.Millisecond)
	if src.sentCount() != 0 {
		t.Fatalf("room route must not auto-reply, got %d frames", src.sentCount())
	}
}

func TestRoomDispatchWithoutRoom(t *testing.T) {
	e, _ := newTestEngine(t)
	r := New(e)
	r.RegisterRoom(msgRoom, func() any { return &roomReq{} }, false)

	// 已登录但 Source 不提供房间信息（不在房间）→ 3001。
	src := newFakeSource("u1", true)
	r.Dispatch(src, testFrame(msgRoom, 1, `{"x":1}`))
	if code, _ := envCode(t, src.waitFrame(t)); code != framework.ErrNotInRoom {
		t.Fatalf("expected 3001")
	}
}
