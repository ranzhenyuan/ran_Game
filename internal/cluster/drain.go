// drain.go 节点状态机与排水缩容（架构文档 §13.3）。
//
// 排水流程：
//   preStop → Drain() 置 Draining → MatchMaker/分配器停止向该节点派新房/新会话
//          → 存量对局继续（自然结束或触发 RoomConfig.MaxDuration 强结算）
//          → active_rooms=0 → 残余个人会话 4005 → 进入 §11.3 消息级停机
//          → 排水截止时间到期 → ForceDrain（TimeoutLogic 强结算 + 残局 4005）

package cluster

import (
	"context"
	"sync"
	"time"
)

// DrainController 排水控制器接口（§3.8 / §13.3）。
//
// 实现方在 Logic 进程内持有：active_rooms 计数 + 个人会话计数 + 状态机。
// Gateway 排水更简单（无房间状态），可复用本接口，ActiveRooms 恒为 0。
type DrainController interface {
	// Drain 进入 Draining 状态；deadline 超时后强制结算。
	// 幂等：已 Draining 时重复调用只更新 deadline。
	Drain(deadline time.Duration) error
	State() State
	ActiveRooms() int
	// WaitDrained 阻塞至 active_rooms=0 且个人会话清空，或 ctx 取消。
	WaitDrained(ctx context.Context) error
	// ForceDrain 立即触发强结算路径（§13.3 超时分支）。
	ForceDrain() error
}

// activeRoomsSource 由 Logic 装配层注入：返回当前节点活跃房间数。
// 通常实现为 `func() int { return rooms.ActiveCount() }`。
type activeRoomsSource func() int

// sessionCountSource 返回残余个人会话数（不依赖 active_rooms 即可判断 Drained）。
type sessionCountSource func() int

// forceSettleFn 强结算回调（§13.3 TimeoutLogic 强结算路径）。
// 由装配层注入：触发所有进行中房间的 OnTimeout/TimeoutLogic → 4005 + Resync。
type forceSettleFn func()

// drainController 默认实现。
type drainController struct {
	mu          sync.Mutex
	cond        *sync.Cond
	state       State
	deadline    time.Time
	roomsSrc    activeRoomsSource
	sessionsSrc sessionCountSource
	forceSettle forceSettleFn
	registry    NodeRegistry
	nodeID      string

	// forceTimer 超时触发 ForceDrain 的定时器。
	forceTimer *time.Timer
}

// NewDrainController 创建排水控制器。
//
//	roomsSrc: 返回当前活跃房间数（用于判断 Drained）
//	sessionsSrc: 返回残余个人会话数（roomsSrc=0 后再清个人会话）
//	forceSettle: 强结算回调（可空：仅做状态置 Drained + 退出）
//	registry: 节点注册表，状态变更会发布 Pub/Sub；可空（单进程骨架）
//	nodeID: 本节点 ID
func NewDrainController(
	roomsSrc activeRoomsSource,
	sessionsSrc sessionCountSource,
	forceSettle forceSettleFn,
	registry NodeRegistry,
	nodeID string,
) DrainController {
	d := &drainController{
		state:       StateActive,
		roomsSrc:    roomsSrc,
		sessionsSrc: sessionsSrc,
		forceSettle: forceSettle,
		registry:    registry,
		nodeID:      nodeID,
	}
	d.cond = sync.NewCond(&d.mu)
	return d
}

// Drain 进入 Draining 状态。
func (d *drainController) Drain(deadline time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.deadline = time.Now().Add(deadline)
	if d.state == StateActive {
		d.state = StateDraining
		if d.registry != nil {
			_ = d.registry.SetState(d.nodeID, StateDraining)
		}
		// 启动强制结算定时器
		if d.forceTimer != nil {
			d.forceTimer.Stop()
		}
		d.forceTimer = time.AfterFunc(deadline, func() {
			_ = d.ForceDrain()
		})
	}
	return nil
}

// State 返回当前状态。
func (d *drainController) State() State {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state
}

// ActiveRooms 返回当前活跃房间数。
func (d *drainController) ActiveRooms() int {
	if d.roomsSrc == nil {
		return 0
	}
	return d.roomsSrc()
}

// WaitDrained 阻塞至 active_rooms=0 且 session_count=0，或 ctx 取消。
//
// 实现要点（§13.3）：
//   - active_rooms=0 后才清个人会话（对局先自然结束，再 4005 个人会话）
//   - ctx 取消时返回 ctx.Err()，但状态机不回退
func (d *drainController) WaitDrained(ctx context.Context) error {
	// 用 goroutine 监听 ctx + cond 唤醒
	done := make(chan struct{})
	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		for d.state == StateDraining {
			if d.roomsSrc != nil && d.roomsSrc() == 0 {
				if d.sessionsSrc == nil || d.sessionsSrc() == 0 {
					break
				}
			}
			d.cond.Wait()
		}
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// 唤醒 cond 让 goroutine 退出
		d.cond.Broadcast()
		return ctx.Err()
	}
}

// ForceDrain 立即强结算：触发 forceSettle 回调并置 Drained。
func (d *drainController) ForceDrain() error {
	d.mu.Lock()
	if d.state != StateDraining {
		d.mu.Unlock()
		return nil // 未在 Draining 不触发
	}
	d.state = StateDrained
	if d.forceTimer != nil {
		d.forceTimer.Stop()
		d.forceTimer = nil
	}
	forceSettle := d.forceSettle
	registry := d.registry
	nodeID := d.nodeID
	d.mu.Unlock()

	if forceSettle != nil {
		forceSettle()
	}
	if registry != nil {
		_ = registry.SetState(nodeID, StateDrained)
	}
	d.cond.Broadcast()
	return nil
}

// notifyRoomChange 房间数变化时由装配层调用，唤醒 WaitDrained。
func (d *drainController) notifyRoomChange() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == StateDraining && d.roomsSrc != nil && d.roomsSrc() == 0 {
		// active_rooms 归零，准备 Drained（但需等个人会话也清空）
		if d.sessionsSrc == nil || d.sessionsSrc() == 0 {
			d.state = StateDrained
			if d.forceTimer != nil {
				d.forceTimer.Stop()
				d.forceTimer = nil
			}
			if d.registry != nil {
				_ = d.registry.SetState(d.nodeID, StateDrained)
			}
		}
	}
	d.cond.Broadcast()
}
