package actor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// recorderActor 记录收到的消息，可用 gate 阻塞处理以制造邮箱积压。
type recorderActor struct {
	mu        sync.Mutex
	msgIDs    []framework.MsgID
	got       chan *framework.Envelope
	gate      chan struct{} // 非 nil 时，每条消息处理前阻塞等待 gate 关闭
	started   chan struct{} // 非 nil 时，首条消息进入处理即关闭一次
	startOnce sync.Once
	stopped   atomic.Bool
}

func (a *recorderActor) Init(framework.ActorCtx) {}

func (a *recorderActor) OnMessage(_ framework.ActorCtx, env *framework.Envelope) {
	if a.started != nil {
		a.startOnce.Do(func() { close(a.started) })
	}
	if a.gate != nil {
		<-a.gate
	}
	a.mu.Lock()
	a.msgIDs = append(a.msgIDs, env.MsgID)
	a.mu.Unlock()
	if a.got != nil {
		select {
		case a.got <- env:
		default:
		}
	}
}

func (a *recorderActor) OnStop(framework.ActorCtx) { a.stopped.Store(true) }

func (a *recorderActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.msgIDs)
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestTellOrderedAndBatchDrain(t *testing.T) {
	e := New(Config{})
	rec := &recorderActor{}
	id, err := e.Spawn(rec, framework.MailboxPolicy{}, 0)
	if err != nil {
		t.Fatal(err)
	}

	const n = 200
	for i := 0; i < n; i++ {
		if err := e.Tell(id, &framework.Envelope{MsgID: framework.MsgID(1000 + i)}); err != nil {
			t.Fatalf("tell %d: %v", i, err)
		}
	}

	waitUntil(t, 2*time.Second, func() bool { return rec.count() == n })

	// 顺序校验：单 Actor goroutine 串行，消息必须按入箱顺序处理。
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for i := 0; i < n; i++ {
		if rec.msgIDs[i] != framework.MsgID(1000+i) {
			t.Fatalf("order broken at %d: got %d", i, rec.msgIDs[i])
		}
	}
}

func TestDropPolicy(t *testing.T) {
	e := New(Config{})
	gate := make(chan struct{})
	started := make(chan struct{})
	rec := &recorderActor{gate: gate, started: started}
	id, _ := e.Spawn(rec, framework.MailboxPolicy{Capacity: 1, OnFull: framework.FullDrop}, 0)

	// 第一条被取出处理并卡在 gate（邮箱此时已空）。
	if err := e.Tell(id, &framework.Envelope{MsgID: 1}); err != nil {
		t.Fatal(err)
	}
	<-started

	// 第二条占满唯一槽位；第三条满箱 → Drop。
	if err := e.Tell(id, &framework.Envelope{MsgID: 2}); err != nil {
		t.Fatalf("second tell should enqueue: %v", err)
	}
	if err := e.Tell(id, &framework.Envelope{MsgID: 3}); err != ErrDropped {
		t.Fatalf("expected ErrDropped, got %v", err)
	}

	close(gate)
	waitUntil(t, time.Second, func() bool { return rec.count() == 2 })
}

func TestKickPolicy(t *testing.T) {
	e := New(Config{})
	gate := make(chan struct{})
	started := make(chan struct{})
	rec := &recorderActor{gate: gate, started: started}
	id, _ := e.Spawn(rec, framework.MailboxPolicy{Capacity: 1, OnFull: framework.FullKick}, 0)

	e.Tell(id, &framework.Envelope{MsgID: 1})
	<-started
	e.Tell(id, &framework.Envelope{MsgID: 2}) // 占满槽位

	// 满箱 kick：Actor 进入 drain，本条返回 ErrKicked。
	if err := e.Tell(id, &framework.Envelope{MsgID: 3}); err != ErrKicked {
		t.Fatalf("expected ErrKicked, got %v", err)
	}
	close(gate)

	waitUntil(t, time.Second, func() bool { return rec.stopped.Load() })

	// Actor 已注销：后续 Tell 返回 NotFound。
	waitUntil(t, time.Second, func() bool {
		return e.Tell(id, &framework.Envelope{}) == ErrActorNotFound
	})
}

// panicActor 前 panicLeft 次消息处理时 panic。
type panicActor struct {
	panicLeft int32
	okCount   atomic.Int32
	stopped   atomic.Bool
}

func (a *panicActor) Init(framework.ActorCtx) {}

func (a *panicActor) OnMessage(_ framework.ActorCtx, _ *framework.Envelope) {
	if atomic.LoadInt32(&a.panicLeft) > 0 {
		atomic.AddInt32(&a.panicLeft, -1)
		panic("poison message")
	}
	a.okCount.Add(1)
}

func (a *panicActor) OnStop(framework.ActorCtx) { a.stopped.Store(true) }

func TestPanicIsolationAndSupervision(t *testing.T) {
	e := New(Config{})

	// 父 Actor 用于接收监督通知。
	parentRec := &recorderActor{}
	parentID, _ := e.Spawn(parentRec, framework.MailboxPolicy{}, 0)
	childDown := e.WatchChild(parentID)

	victim := &panicActor{panicLeft: 3}
	victimID, _ := e.Spawn(victim, framework.MailboxPolicy{}, parentID)

	// 邻居 Actor：受害者死亡不应影响它。
	neighbor := &recorderActor{}
	neighborID, _ := e.Spawn(neighbor, framework.MailboxPolicy{}, 0)

	for i := 0; i < 3; i++ {
		e.Tell(victimID, &framework.Envelope{MsgID: framework.MsgID(i + 1)})
	}
	waitUntil(t, 2*time.Second, func() bool { return victim.stopped.Load() })

	// 父收到子停止通知。
	select {
	case id := <-childDown:
		if id != victimID {
			t.Fatalf("supervision notified wrong id: %d", id)
		}
	case <-time.After(time.Second):
		t.Fatal("parent did not receive child-down notification")
	}

	// 邻居照常收消息。
	e.Tell(neighborID, &framework.Envelope{MsgID: 99})
	waitUntil(t, time.Second, func() bool { return neighbor.count() == 1 })
}

func TestPanicUnderThresholdSurvives(t *testing.T) {
	e := New(Config{})
	a := &panicActor{panicLeft: 2} // 窗口内仅 2 次，未达 3 次上限
	id, _ := e.Spawn(a, framework.MailboxPolicy{}, 0)

	for i := 0; i < 4; i++ {
		e.Tell(id, &framework.Envelope{MsgID: framework.MsgID(i + 1)})
	}
	waitUntil(t, 2*time.Second, func() bool { return a.okCount.Load() == 2 })
	if a.stopped.Load() {
		t.Fatal("actor with 2 panics in window should survive")
	}
}

func TestGracefulStopDrainsMailbox(t *testing.T) {
	e := New(Config{})
	rec := &recorderActor{}
	id, _ := e.Spawn(rec, framework.MailboxPolicy{Capacity: 128}, 0)

	// 用 Invoke 阻塞 Actor，先把 100 条消息塞进邮箱，再 Stop。
	started := make(chan struct{})
	release := make(chan struct{})
	e.Tell(id, &framework.Envelope{Payload: Invoke(func(framework.ActorCtx) {
		close(started)
		<-release
	})})
	<-started

	for i := 0; i < 100; i++ {
		e.Tell(id, &framework.Envelope{MsgID: framework.MsgID(i + 1)})
	}
	e.Stop(id)
	close(release)

	waitUntil(t, 2*time.Second, func() bool { return rec.count() == 100 && rec.stopped.Load() })
}

func TestTimerFiresInsideActor(t *testing.T) {
	e := New(Config{})
	var fired atomic.Int32

	a := &timerActor{onInit: func(ctx framework.ActorCtx) {
		ctx.After(20*time.Millisecond, func() { fired.Store(1) })
	}}
	id, _ := e.Spawn(a, framework.MailboxPolicy{}, 0)

	waitUntil(t, 2*time.Second, func() bool { return fired.Load() == 1 })
	e.Stop(id)
}

type timerActor struct {
	onInit func(ctx framework.ActorCtx)
}

func (a *timerActor) Init(ctx framework.ActorCtx)                       { a.onInit(ctx) }
func (a *timerActor) OnMessage(framework.ActorCtx, *framework.Envelope) {}
func (a *timerActor) OnStop(framework.ActorCtx)                         {}

func TestShutdownWaitsAllActors(t *testing.T) {
	e := New(Config{})
	r1, r2 := &recorderActor{}, &recorderActor{}
	id1, _ := e.Spawn(r1, framework.MailboxPolicy{}, 0)
	id2, _ := e.Spawn(r2, framework.MailboxPolicy{}, 0)

	e.Tell(id1, &framework.Envelope{MsgID: 1})
	e.Tell(id2, &framework.Envelope{MsgID: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	e.Shutdown(ctx)

	if !r1.stopped.Load() || !r2.stopped.Load() {
		t.Fatal("shutdown should stop all actors gracefully")
	}
}
