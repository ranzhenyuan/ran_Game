package app

import (
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/obs"
)

// fakeRegistry 仅实现心跳循环用到的 Heartbeat；其余接口方法返回零值即可。
// failN：前 failN 次心跳返回错误，之后成功，模拟 Redis 抖动后自愈。
type fakeRegistry struct {
	failN int64 // 剩余失败次数（atomic）
	calls int64 // 总调用次数（atomic）
	okCnt int64 // 成功次数（atomic）
}

func (f *fakeRegistry) Register(cluster.NodeInfo) error                  { return nil }
func (f *fakeRegistry) SetState(string, cluster.State) error             { return nil }
func (f *fakeRegistry) List(cluster.Role, bool) ([]cluster.NodeInfo, error) { return nil, nil }
func (f *fakeRegistry) Get(string) (cluster.NodeInfo, error)             { return cluster.NodeInfo{}, nil }
func (f *fakeRegistry) Watch() <-chan cluster.ChangeEvent                { return nil }
func (f *fakeRegistry) Deregister(string) error                          { return nil }
func (f *fakeRegistry) Close() error                                     { return nil }

func (f *fakeRegistry) Heartbeat(_ string, _ int) error {
	atomic.AddInt64(&f.calls, 1)
	if atomic.LoadInt64(&f.failN) > 0 {
		atomic.AddInt64(&f.failN, -1)
		return errors.New("redis down")
	}
	atomic.AddInt64(&f.okCnt, 1)
	return nil
}

// 心跳循环在连续失败后不退出，恢复后能继续成功续期，并累计失败指标。
func TestLogicHeartbeatLoop_FailureDoesNotExit(t *testing.T) {
	reg := &fakeRegistry{failN: 3}
	metrics := obs.NewRegistry()

	// 5ms 间隔快速跑多拍；循环本身随测试结束由进程回收。
	go logicHeartbeatLoop(reg, "logic-0", func() int { return 0 }, slog.Default(), metrics, 5*time.Millisecond)

	// 等待：至少 3 次失败 + 若干次成功（循环未因失败而退出）。
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&reg.okCnt) < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := atomic.LoadInt64(&reg.okCnt); got < 2 {
		t.Fatalf("heartbeat loop exited on failure; okCnt=%d calls=%d", got, atomic.LoadInt64(&reg.calls))
	}
	if calls := atomic.LoadInt64(&reg.calls); calls < 5 {
		t.Fatalf("heartbeat loop stopped early; calls=%d", calls)
	}
	if failCnt := metrics.Counter("registry_heartbeat_fail_total").Value(); failCnt < 3 {
		t.Fatalf("expected >=3 heartbeat failures counted, got %d", failCnt)
	}
}
