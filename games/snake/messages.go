// Package snake 是框架内置玩法示例：贪吃蛇（架构文档 §14/§16.4 走查）。
//
// 仅依赖 pkg/framework 稳定面；由 cmd 层作为 framework.GameModule 装配。
// 链路：MsgMatch 成桌 → OnJoin 满人开局 → MoveReq(ToRoom) → Tick 推进 →
// 快照通道广播全量局面 → 结算可靠广播 + Storage 异步落库 → 空房 OnEmpty 回收。
package snake

import "github.com/rangame/server/pkg/framework"

// msgID 段：snake 为第 0 个业务模块（0x1000 起，每模块 256 个）。
const (
	// MsgMove 上行转向（可靠通道，ToRoom）。
	MsgMove framework.MsgID = 0x1000
	// MsgState 下行局面快照（快照通道，latest-wins）。
	MsgState framework.MsgID = 0x1001
	// MsgResult 下行结算（可靠通道）。
	MsgResult framework.MsgID = 0x1002
)

// 方向。
const (
	DirUp    = 1
	DirDown  = 2
	DirLeft  = 3
	DirRight = 4
)

// MoveReq 转向请求。
type MoveReq struct {
	Dir int `json:"dir" protobuf:"varint,1,opt,name=dir,proto3"`
}

// Point 格点。
type Point struct {
	X int `json:"x" protobuf:"varint,1,opt,name=x,proto3"`
	Y int `json:"y" protobuf:"varint,2,opt,name=y,proto3"`
}

// SnakeState 单条蛇的局面。
type SnakeState struct {
	UID   string  `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Body  []Point `json:"body" protobuf:"bytes,2,rep,name=body,proto3"` // [0] 为蛇头
	Alive bool    `json:"alive" protobuf:"varint,3,opt,name=alive,proto3"`
	Score int     `json:"score" protobuf:"varint,4,opt,name=score,proto3"`
}

// StateNtf 全量局面快照（MsgState）。
type StateNtf struct {
	Tick   int64        `json:"tick" protobuf:"varint,1,opt,name=tick,proto3"`
	Snakes []SnakeState `json:"snakes" protobuf:"bytes,2,rep,name=snakes,proto3"`
	Food   Point        `json:"food" protobuf:"bytes,3,opt,name=food,proto3"`
}

// ResultNtf 结算（MsgResult），同时落库 snake_result 表。
type ResultNtf struct {
	RoomID string `json:"room_id" protobuf:"bytes,1,opt,name=room_id,proto3"`
	Winner string `json:"winner" protobuf:"bytes,2,opt,name=winner,proto3"` // 空=平局
	Rounds int64  `json:"rounds" protobuf:"varint,3,opt,name=rounds,proto3"`
}

// MemberNtf 成员变更提示（复用通用 MsgMemberChange）。
type MemberNtf struct {
	UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Event string `json:"event" protobuf:"bytes,2,opt,name=event,proto3"` // join | leave
}
