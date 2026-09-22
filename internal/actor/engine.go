package actor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/obs"
	"github.com/rangame/server/internal/timer"
	"github.com/rangame/server/pkg/framework"
)

// Config 引擎配置，均可选。
type Config struct {
	// Scheduler 注入后，Actor 的 After/Every 走最小堆调度器（§9）；
	// 不注入则用 time.AfterFunc（功能等价，定时器数量极大时建议注入）。
	Scheduler *timer.Scheduler
	// Metrics 注入后输出 actor_count / 丢弃 / kick / panic 指标（§11.2）。
	Metrics *obs.Registry
}

// Engine Actor 引擎：Spawn / Tell / Stop / Shutdown。
type Engine struct {
	nextID atomic.Uint64

	procs sync.Map // framework.ID → *process
	reg   *Registry

	sched   *timer.Scheduler
	metrics *obs.Registry

	wg       sync.WaitGroup
	stopAll  chan struct{}
	stopOnce sync.Once

	watchMu  sync.Mutex
	watchers map[framework.ID]chan framework.ID // 父 Actor → 子停止通知
}

// New 创建引擎。
func New(cfg Config) *Engine {
	return &Engine{
		reg:      NewRegistry(),
		sched:    cfg.Scheduler,
		metrics:  cfg.Metrics,
		stopAll:  make(chan struct{}),
		watchers: make(map[framework.ID]chan framework.ID),
	}
}

// Registry 返回共享注册表（§7.5 共享点②）。
func (e *Engine) Registry() *Registry { return e.reg }

// ResolvePlayer / ResolveRoom 供路由器等上层按业务键定位 Actor。
func (e *Engine) ResolvePlayer(uid string) (framework.ID, bool) {
	return e.reg.LookupUID(uid)
}

func (e *Engine) ResolveRoom(roomID string) (framework.ID, bool) {
	return e.reg.LookupRoom(roomID)
}

// Spawn 创建并启动一个 Actor。parent 为 0 表示根 Actor；
// 子 Actor 停止时父可通过 WatchChild 收到通知（§7.4 监督）。
func (e *Engine) Spawn(a framework.Actor, policy framework.MailboxPolicy, parent framework.ID) (framework.ID, error) {
	if a == nil {
		return 0, ErrActorNotFound
	}
	id := framework.ID(e.nextID.Add(1))
	p := &process{
		id:      id,
		actor:   a,
		mb:      newMailbox(policy),
		parent:  parent,
		stopSig: make(chan struct{}),
	}
	p.ctx = &actorCtx{e: e, p: p}

	e.procs.Store(id, p)
	e.wg.Add(1)
	e.gaugeAdd("actor_count", 1)

	go e.run(p)
	return id, nil
}

// Tell 非阻塞投递。满箱按目标邮箱策略处理（§7.2）。
func (e *Engine) Tell(target framework.ID, env *framework.Envelope) error {
	v, ok := e.procs.Load(target)
	if !ok {
		return ErrActorNotFound
	}
	p := v.(*process)
	if p.finished.Load() {
		return ErrActorNotFound
	}

	// 快速路径：邮箱有空位立即入箱。
	select {
	case p.mb.ch <- env:
		return nil
	default:
	}

	switch p.mb.policy.OnFull {
	case framework.FullDrop:
		e.counter("actor_msg_dropped_total").Inc()
		return ErrDropped

	case framework.FullKick:
		e.counter("actor_msg_kicked_total").Inc()
		p.beginDrain() // 循环排空存量消息后停止该 Actor
		return ErrKicked

	default: // block：仅停机/强一致场景；引擎整体关闭时解除阻塞。
		select {
		case p.mb.ch <- env:
			return nil
		case <-e.stopAll:
			return ErrShuttingDown
		}
	}
}

// Stop 请求单个 Actor 优雅停止（排空已入箱消息后退出，§11.3 消息级停机）。
func (e *Engine) Stop(id framework.ID) {
	if v, ok := e.procs.Load(id); ok {
		v.(*process).beginDrain()
	}
}

// Shutdown 引擎级优雅停机：所有 Actor 停止接单并排空邮箱；
// ctx 超时后仍未退出的（通常是 Block 死等）放弃等待直接返回。
func (e *Engine) Shutdown(ctx context.Context) {
	e.stopOnce.Do(func() { close(e.stopAll) })

	e.procs.Range(func(_, v any) bool {
		v.(*process).beginDrain()
		return true
	})

	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// run 是每个 Actor 唯一的 goroutine：串行处理所有消息（§7.1）。
func (e *Engine) run(p *process) {
	defer e.finish(p)

	// Init 同样受 panic 监督保护。
	e.safeCall(p, func() { p.actor.Init(p.ctx) })
	if p.isStopping() {
		return
	}

	for {
		select {
		case env := <-p.mb.ch:
			e.handleOne(p, env)
			if p.isStopping() {
				e.drainRemaining(p)
				return
			}
			e.drainBatch(p) // 单次唤醒批量处理，降低 channel 收发与调度开销
			if p.isStopping() {
				return
			}
		case <-p.stopSig:
			e.drainRemaining(p)
			return
		}
	}
}

// drainBatch 非阻塞地继续取一批消息（§7.2 批量 drain）。
func (e *Engine) drainBatch(p *process) {
	for i := 0; i < p.mb.policy.DrainBatch; i++ {
		select {
		case env := <-p.mb.ch:
			e.handleOne(p, env)
			if p.isStopping() {
				return
			}
		default:
			return
		}
	}
}

// drainRemaining 停机路径：把已入箱消息全部处理完再退出（上限=邮箱容量，天然有界）。
func (e *Engine) drainRemaining(p *process) {
	for {
		select {
		case env := <-p.mb.ch:
			e.handleOne(p, env)
		default:
			return
		}
	}
}

func (e *Engine) handleOne(p *process, env *framework.Envelope) {
	e.safeCall(p, func() {
		if inv, ok := env.Payload.(Invoke); ok {
			inv(p.ctx) // 闭包类任务（Handler / 定时器）在 Actor goroutine 内串行执行
			return
		}
		p.actor.OnMessage(p.ctx, env)
	})
}

// safeCall 捕获单次消息处理的 panic：窗口内累计达阈值才停 Actor（§7.4），
// 未达阈值时 Actor 存活、继续处理后续消息——实现故障隔离。
func (e *Engine) safeCall(p *process, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			e.counter("actor_panic_total").Inc()
			if p.notePanic(time.Now()) {
				p.beginDrain()
			}
		}
	}()
	fn()
}

func (e *Engine) finish(p *process) {
	if !p.finished.CompareAndSwap(false, true) {
		return
	}
	p.beginDrain()

	// OnStop 本身也不允许 panic 影响引擎。
	e.safeCall(p, func() { p.actor.OnStop(p.ctx) })

	e.procs.Delete(p.id)
	e.reg.Unbind(p.id)
	e.wg.Done()
	e.gaugeAdd("actor_count", -1)
	e.notifyParent(p.parent, p.id)
}

func (e *Engine) counter(name string) *obs.Counter {
	if e.metrics == nil {
		return nopCounter
	}
	return e.metrics.Counter(name)
}

func (e *Engine) gaugeAdd(name string, delta int64) {
	if e.metrics != nil {
		e.metrics.Gauge(name).Add(delta)
	}
}

// nopCounter 未注入 Metrics 时的空计数器（obs.Counter 零值即可用）。
var nopCounter = &obs.Counter{}

// stdTimer 包装 time.Timer 为 framework.Handle。
type stdTimer struct{ t *time.Timer }

func (h stdTimer) Cancel() { h.t.Stop() }

// actorCtx 实现 framework.ActorCtx。
type actorCtx struct {
	e *Engine
	p *process
}

func (c *actorCtx) Self() framework.ID { return c.p.id }

func (c *actorCtx) Tell(target framework.ID, env *framework.Envelope) error {
	if env.Sender == 0 {
		env.Sender = c.p.id
	}
	return c.e.Tell(target, env)
}

func (c *actorCtx) Spawn(child framework.Actor, opts ...framework.SpawnOption) (framework.ID, error) {
	cfg := &framework.SpawnConfig{}
	for _, o := range opts {
		o(cfg)
	}
	policy := framework.MailboxPolicy{}
	if cfg.Policy != nil {
		policy = *cfg.Policy
	}
	return c.e.Spawn(child, policy, c.p.id)
}

func (c *actorCtx) Stop() { c.p.beginDrain() }

func (c *actorCtx) After(d time.Duration, fn func()) framework.Handle {
	return c.schedule(false, d, fn)
}

func (c *actorCtx) Every(d time.Duration, fn func()) framework.Handle {
	return c.schedule(true, d, fn)
}

// schedule 定时器只负责唤醒；回调被包装成 Invoke 投递回本 Actor 邮箱，
// 因此 fn 一定在 Actor goroutine 内串行执行（§9）。
func (c *actorCtx) schedule(periodic bool, d time.Duration, fn func()) framework.Handle {
	post := func() { _ = c.e.Tell(c.p.id, &framework.Envelope{Payload: Invoke(func(framework.ActorCtx) { fn() })}) }
	if c.e.sched != nil {
		if periodic {
			return c.e.sched.Every(d, post)
		}
		return c.e.sched.After(d, post)
	}
	if periodic {
		t := time.NewTicker(d)
		go func() {
			for {
				select {
				case <-t.C:
					post()
				case <-c.p.stopSig:
					t.Stop()
					return
				case <-c.e.stopAll:
					t.Stop()
					return
				}
			}
		}()
		return stdTimerTicker{t}
	}
	t := time.AfterFunc(d, post)
	return stdTimer{t}
}

type stdTimerTicker struct{ t *time.Ticker }

func (h stdTimerTicker) Cancel() { h.t.Stop() }
