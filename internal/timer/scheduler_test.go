package timer

import (
	"testing"
	"time"
)

func TestManualClockDeterministic(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewManualClock(t0)

	ch1 := c.After(5 * time.Second)
	ch2 := c.After(10 * time.Second)

	c.Advance(4 * time.Second)
	select {
	case <-ch1:
		t.Fatal("5s timer should not fire at 4s")
	default:
	}

	c.Advance(1 * time.Second) // now=5s
	select {
	case <-ch1:
	default:
		t.Fatal("5s timer should fire at 5s")
	}
	select {
	case <-ch2:
		t.Fatal("10s timer should not fire at 5s")
	default:
	}

	c.Advance(5 * time.Second) // now=10s
	select {
	case <-ch2:
	default:
		t.Fatal("10s timer should fire at 10s")
	}
}

func TestSchedulerAfter(t *testing.T) {
	s := NewScheduler(SystemClock{})
	defer s.Close()

	fired := make(chan struct{}, 1)
	s.After(20*time.Millisecond, func() { fired <- struct{}{} })

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("After callback did not fire")
	}
}

func TestSchedulerEveryAndCancel(t *testing.T) {
	s := NewScheduler(SystemClock{})
	defer s.Close()

	var count int
	done := make(chan struct{}, 1)
	var h Handle
	h = s.Every(15*time.Millisecond, func() {
		count++
		if count >= 3 {
			h.Cancel()
			done <- struct{}{}
		}
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("periodic timer should fire 3 times, got %d", count)
	}
	if count != 3 {
		t.Fatalf("count=%d, want 3", count)
	}

	// 再等一个周期确认已取消，不会继续触发。
	time.Sleep(40 * time.Millisecond)
	if count != 3 {
		t.Fatalf("cancelled timer still fired: count=%d", count)
	}
}

func TestSchedulerCancelledOneShot(t *testing.T) {
	s := NewScheduler(SystemClock{})
	defer s.Close()

	fired := make(chan struct{}, 1)
	h := s.After(30*time.Millisecond, func() { fired <- struct{}{} })
	h.Cancel()

	time.Sleep(60 * time.Millisecond)
	select {
	case <-fired:
		t.Fatal("cancelled one-shot should not fire")
	default:
	}
}
