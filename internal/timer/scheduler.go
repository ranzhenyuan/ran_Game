package timer

import (
	"container/heap"
	"sync"
	"time"
)

// Handle 定时器句柄，可取消。
type Handle interface {
	Cancel()
}

// task 最小堆元素（§9：插入 O(logN)，小游戏规模足够）。
type task struct {
	id        int64
	fireAt    time.Time
	interval  time.Duration // 0 = 一次性；>0 = 周期任务
	fn        func()
	index     int // container/heap 维护；弹出后置 -1
	cancelled bool
}

type taskHeap []*task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool { return h[i].fireAt.Before(h[j].fireAt) }

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x any) {
	t := x.(*task)
	t.index = len(*h)
	*h = append(*h, t)
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.index = -1
	*h = old[:n-1]
	return t
}

// Executor 回调执行策略：默认在调度 goroutine 直接执行；
// 阶段 3 注入 Actor Tell 版本（fn 投递到属主 Actor 邮箱）。
type Executor func(fn func())

// Scheduler 最小堆定时器调度器，单 goroutine 驱动。
type Scheduler struct {
	clock Clock

	mu     sync.Mutex
	tasks  taskHeap
	nextID int64
	closed bool

	wake chan struct{}
	stop chan struct{}
	exec Executor
}

// NewScheduler 创建并启动调度器。
func NewScheduler(clock Clock, opts ...func(*Scheduler)) *Scheduler {
	s := &Scheduler{
		clock: clock,
		wake:  make(chan struct{}, 1),
		stop:  make(chan struct{}),
		exec:  func(fn func()) { fn() },
	}
	for _, o := range opts {
		o(s)
	}
	go s.loop()
	return s
}

// WithExecutor 注入回调执行策略。
func WithExecutor(ex Executor) func(*Scheduler) {
	return func(s *Scheduler) { s.exec = ex }
}

// After 一次性定时器。
func (s *Scheduler) After(d time.Duration, fn func()) Handle {
	return s.add(d, 0, fn)
}

// Every 周期定时器（首次触发在 d 后）。
func (s *Scheduler) Every(d time.Duration, fn func()) Handle {
	return s.add(d, d, fn)
}

func (s *Scheduler) add(first, interval time.Duration, fn func()) Handle {
	t := &task{
		fireAt:   s.clock.Now().Add(first),
		interval: interval,
		fn:       fn,
		index:    -1,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		t.cancelled = true
		return &handle{s: s, t: t}
	}

	s.nextID++
	t.id = s.nextID

	wasEmpty := s.tasks.Len() == 0
	heap.Push(&s.tasks, t)
	if wasEmpty || s.tasks[0] == t {
		s.signal()
	}
	return &handle{s: s, t: t}
}

// Close 停止调度循环（已到期但未执行的回调不再执行）。
func (s *Scheduler) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	close(s.stop)
}

func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) loop() {
	const idleWait = time.Hour

	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		wait := idleWait
		if s.tasks.Len() > 0 {
			wait = s.tasks[0].fireAt.Sub(s.clock.Now())
			if wait < 0 {
				wait = 0
			}
		}
		s.mu.Unlock()

		select {
		case <-s.stop:
			return
		case <-s.wake:
			continue
		case <-s.clock.After(wait):
		}

		now := s.clock.Now()
		var due []*task
		s.mu.Lock()
		for s.tasks.Len() > 0 && !s.tasks[0].fireAt.After(now) {
			t := heap.Pop(&s.tasks).(*task)
			if !t.cancelled {
				due = append(due, t)
			}
		}
		s.mu.Unlock()

		for _, t := range due {
			s.exec(t.fn)
			if t.interval > 0 && !t.cancelled {
				s.rearm(t, now.Add(t.interval))
			}
		}
	}
}

func (s *Scheduler) rearm(t *task, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || t.cancelled {
		return
	}
	t.fireAt = at
	t.index = -1
	heap.Push(&s.tasks, t)
	s.signal()
}

type handle struct {
	s *Scheduler
	t *task
}

func (h *handle) Cancel() {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	if h.t.cancelled {
		return
	}
	h.t.cancelled = true
	if h.t.index >= 0 && h.t.index < h.s.tasks.Len() {
		heap.Remove(&h.s.tasks, h.t.index)
	}
}
