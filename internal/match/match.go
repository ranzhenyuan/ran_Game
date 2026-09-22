// Package match 实现简易匹配器（架构文档 §8.3）。
//
// 阶段 5 策略：按 module+code 组队，先到先得，人数达到 rule.Players 即刻建房成桌。
// 设计要点（避免阻塞 PlayerActor 邮箱）：
//   - Enqueue 不阻塞等待成桌，请求挂入队列后立即返回；
//   - 成桌/失败由 ResultFunc 回调通知（装配层负责按原 seq 主动下发 MsgMatch 响应帧）；
//   - 同一 UID 同时只允许一个在队请求；Cancel/会话释放负责清理。
package match

import (
	"errors"
	"sync"

	"github.com/rangame/server/internal/room"
	"github.com/rangame/server/pkg/framework"
)

// 错误。
var (
	ErrAlreadyQueued = errors.New("match: uid already queued")
	ErrBadRule       = errors.New("match: invalid match rule")
)

// Rule 匹配规则（= framework.MatchRule，保留包内别名方便业务 import）。
type Rule = framework.MatchRule

// ResultFunc 成桌/失败通知。在触发成桌的那个 Enqueue 调用栈内同步回调；
// seq 为玩家原始请求帧 seq，roomID 为空且 err 非 nil 表示匹配失败。
type ResultFunc func(uid string, seq uint32, roomID string, err error)

// Maker 简易同步匹配器。
type Maker struct {
	rooms    *room.Manager
	onResult ResultFunc

	mu     sync.Mutex
	queues map[string][]entry // key: module|code
	byUID  map[string]string  // uid -> queue key（去重 + Cancel 定位）
}

type entry struct {
	uid  string
	seq  uint32
	rule Rule
}

// New 创建匹配器。
func New(rooms *room.Manager, onResult ResultFunc) *Maker {
	return &Maker{
		rooms:    rooms,
		onResult: onResult,
		queues:   make(map[string][]entry),
		byUID:    make(map[string]string),
	}
}

// Enqueue 入队。立即返回 nil 表示请求已受理（结果走 onResult）；
// 返回非 nil 错误表示同步拒绝（已在队列/规则非法），由 Handler 直接回错误帧。
func (m *Maker) Enqueue(uid string, rule Rule, seq uint32) error {
	if rule.Module == "" || rule.Players <= 0 {
		return ErrBadRule
	}
	key := queueKey(rule)

	m.mu.Lock()
	if _, busy := m.byUID[uid]; busy {
		m.mu.Unlock()
		return ErrAlreadyQueued
	}
	q := append(m.queues[key], entry{uid: uid, seq: seq, rule: rule})
	if len(q) < rule.Players {
		m.queues[key] = q
		m.byUID[uid] = key
		m.mu.Unlock()
		return nil
	}
	// 成桌：摘下本队，锁内只摘数据，建房/进房在锁外执行（可能阻塞等待 RoomActor）。
	matched := q[:rule.Players]
	remain := q[rule.Players:]
	m.queues[key] = remain
	for _, e := range matched {
		delete(m.byUID, e.uid)
	}
	m.mu.Unlock()

	m.settle(matched)
	return nil
}

// settle 锁外：建房 → 全员进房 → 通知结果。任一进房失败则拆桌
// （已进房者退房，空房由玩法 OnEmpty→Close 回收），全员通知失败。
func (m *Maker) settle(matched []entry) {
	rule := matched[0].rule
	roomID, err := m.rooms.CreateRoom(rule.Module)
	if err != nil {
		m.notifyAll(matched, "", err)
		return
	}
	for _, e := range matched {
		if err := m.rooms.Join(roomID, e.uid); err != nil {
			for _, joined := range matched {
				if joined.uid == e.uid {
					break
				}
				m.rooms.Leave(roomID, joined.uid)
			}
			m.notifyAll(matched, "", err)
			return
		}
	}
	m.notifyAll(matched, roomID, nil)
}

func (m *Maker) notifyAll(matched []entry, roomID string, err error) {
	for _, e := range matched {
		m.onResult(e.uid, e.seq, roomID, err)
	}
}

// Cancel 主动取消；不在队列中（已在对局/从未排队）返回 false，不产生下发。
func (m *Maker) Cancel(uid string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok := m.byUID[uid]
	if !ok {
		return false
	}
	delete(m.byUID, uid)
	q := m.queues[key]
	for i, e := range q {
		if e.uid == uid {
			m.queues[key] = append(q[:i], q[i+1:]...)
			break
		}
	}
	return true
}

// QueueDepth 某规则当前排队人数（扩容领先指标，§13.8）。
func (m *Maker) QueueDepth(module, code string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queues[queueKeyStr(module, code)])
}

func queueKey(rule Rule) string              { return queueKeyStr(rule.Module, rule.Code) }
func queueKeyStr(module, code string) string { return module + "|" + code }
