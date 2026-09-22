package actor

import (
	"time"

	"github.com/rangame/server/pkg/framework"
)

// notePanic 记录一次 panic 时间，返回 60s 滑窗内是否已达 3 次上限（§7.4）。
// 未达上限的 panic 只计数、不停 Actor——单条毒消息不应杀死整个会话。
func (p *process) notePanic(now time.Time) bool {
	p.panicMu.Lock()
	defer p.panicMu.Unlock()

	cutoff := now.Add(-panicWindow * time.Second)
	kept := p.panics[:0]
	for _, t := range p.panics {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	p.panics = append(kept, now)
	return len(p.panics) >= maxPanicInWindow
}

// WatchChild 父 Actor 订阅子 Actor 停止事件。
// 返回的通道在子 Actor finish（正常停止/panic 超限/kick）时收到其 ID；
// 未被任何父订阅的停止事件直接丢弃。通道有缓冲，满时丢弃（避免影响引擎）。
func (e *Engine) WatchChild(parent framework.ID) <-chan framework.ID {
	e.watchMu.Lock()
	defer e.watchMu.Unlock()

	ch, ok := e.watchers[parent]
	if !ok {
		ch = make(chan framework.ID, 64)
		e.watchers[parent] = ch
	}
	return ch
}

func (e *Engine) notifyParent(parent, child framework.ID) {
	if parent == 0 {
		return
	}
	e.watchMu.Lock()
	ch := e.watchers[parent]
	e.watchMu.Unlock()

	if ch == nil {
		return
	}
	select {
	case ch <- child:
	default: // 父 Actor 不消费则丢弃，监督通知不反向阻塞引擎
	}
}
