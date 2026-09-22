// Package session 管理登录会话：Session 与 Conn 解绑/重绑、应用层心跳、
// 下行双通道（可靠环形缓冲 + 快照 latest-wins）与断线重连重放（架构文档 §5）。
//
// 并发模型（§7.5 共享点①）：RoomActor 广播扇出与 PlayerActor 自身推送会并发
// 调用同一会话的下行方法，因此 seq 分配与两套缓冲由会话内锁保护；
// 业务状态（PlayerActor 内）仍然免锁。
package session

import "github.com/rangame/server/internal/transport"

// ringBuffer 可靠通道「已下发未确认」环形缓冲，容量可配（默认 256）。
// 存满后最旧帧被淘汰；帧上携带服务端分配的单调 seq。
type ringBuffer struct {
	items []*transport.Frame
	cap   int
	start int // 最旧元素下标
	size  int
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{items: make([]*transport.Frame, capacity), cap: capacity}
}

// push 追加一帧；缓冲满时覆盖最旧帧。
func (r *ringBuffer) push(f *transport.Frame) {
	if r.size < r.cap {
		r.items[(r.start+r.size)%r.cap] = f
		r.size++
		return
	}
	// 已满：覆盖最旧槽位并前移起点。
	r.items[r.start] = f
	r.start = (r.start + 1) % r.cap
}

// oldestSeq 返回缓冲中最旧帧的 seq；缓冲为空返回 0。
func (r *ringBuffer) oldestSeq() uint32 {
	if r.size == 0 {
		return 0
	}
	return r.items[r.start].Seq
}

// since 返回所有 seq > lastSeq 的帧（按 seq 升序）。
// overflow=true 表示客户端需要的下一帧（lastSeq+1）已被淘汰——缓冲已滚动，
// 差额无法补齐，调用方必须通知客户端走全量同步（Resync，§5.3）。
// 注意 oldest == lastSeq+1 属于恰好衔接，不算溢出。
func (r *ringBuffer) since(lastSeq uint32) (frames []*transport.Frame, overflow bool) {
	if r.size == 0 {
		return nil, false
	}
	if r.oldestSeq() > lastSeq+1 {
		return nil, true
	}
	for i := 0; i < r.size; i++ {
		f := r.items[(r.start+i)%r.cap]
		if f.Seq > lastSeq {
			frames = append(frames, f)
		}
	}
	return frames, false
}

func (r *ringBuffer) len() int { return r.size }

// snapshotCache 快照通道：每个 msgID 只保留最新一帧（latest-wins，§5.3）。
// 高频状态同步（如 20fps 房间快照）不占用可靠环形缓冲。
type snapshotCache struct {
	latest map[uint32]*transport.Frame
}

func newSnapshotCache() *snapshotCache {
	return &snapshotCache{latest: make(map[uint32]*transport.Frame)}
}

func (c *snapshotCache) put(f *transport.Frame) {
	c.latest[f.MsgID] = f
}

// all 返回全部快照帧；msgIDs 非空时仅返回指定 msgID（对应 PullSnapshotReq）。
func (c *snapshotCache) all(msgIDs []uint32) []*transport.Frame {
	if len(msgIDs) == 0 {
		out := make([]*transport.Frame, 0, len(c.latest))
		for _, f := range c.latest {
			out = append(out, f)
		}
		return out
	}
	out := make([]*transport.Frame, 0, len(msgIDs))
	for _, id := range msgIDs {
		if f, ok := c.latest[id]; ok {
			out = append(out, f)
		}
	}
	return out
}
