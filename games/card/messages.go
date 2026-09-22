// Package card 是 §14 扩展验证示例：回合制卡牌比大小。
//
// 与 snake 的差异面（验证扩展 API 覆盖度）：
//   - 事件驱动：Tick 为空实现，回合由"双方出牌即结算"驱动；
//   - OnLeave 对局中退出判负、TimeoutLogic 强结算（§8.2）；
//   - OnDestroy 走 ProfileStore.Patch 回写本局得分（单写者路由，§10.4.4）。
//
// 仅依赖 pkg/framework；由 cmd 层作为 framework.GameModule 装配。
package card

import "github.com/rangame/server/pkg/framework"

// msgID 段：card 为第 1 个业务模块（ModuleBase(1) = 0x1100 起，每模块 256 个）。
const (
	// MsgPlay 上行出牌（可靠通道，ToRoom）。
	MsgPlay framework.MsgID = 0x1100
	// MsgRound 下行回合结果（可靠通道）。
	MsgRound framework.MsgID = 0x1101
	// MsgResult 下行结算（可靠通道）。
	MsgResult framework.MsgID = 0x1102
	// MsgDeal 下行开局发牌（可靠通道，单推）。
	MsgDeal framework.MsgID = 0x1103
)

// 对局参数。
const (
	HandCards = 5 // 每人手牌数 = 回合数
)

// PlayReq 上行出牌：打出自己手牌中下标为 Idx 的牌（已出过的下标无效）。
type PlayReq struct {
	Idx int `json:"idx" protobuf:"varint,1,opt,name=idx,proto3"`
}

// DealNtf 开局发牌（MsgDeal，开局时对每个成员单推）。
type DealNtf struct {
	Hand        []Card `json:"hand" protobuf:"bytes,1,rep,name=hand,proto3"`
	TotalRounds int    `json:"total_rounds" protobuf:"varint,2,opt,name=total_rounds,proto3"`
}

// Card 单张牌（点数 1-13）。
type Card struct {
	Point int `json:"point" protobuf:"varint,1,opt,name=point,proto3"`
}

// PlayNtf 单方出牌明细。
type PlayNtf struct {
	UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Card  Card   `json:"card" protobuf:"bytes,2,opt,name=card,proto3"`
	Win   bool   `json:"win" protobuf:"varint,3,opt,name=win,proto3"` // 本回合胜者（平局双方皆 false）
	Score int    `json:"score" protobuf:"varint,4,opt,name=score,proto3"`
}

// RoundNtf 回合结果（MsgRound，可靠广播）。
type RoundNtf struct {
	Round  int       `json:"round" protobuf:"varint,1,opt,name=round,proto3"` // 1 起
	Plays  []PlayNtf `json:"plays" protobuf:"bytes,2,rep,name=plays,proto3"`
	IsLast bool      `json:"is_last" protobuf:"varint,3,opt,name=is_last,proto3"`
}

// ScoreNtf 单方累计得分。
type ScoreNtf struct {
	UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Score int    `json:"score" protobuf:"varint,2,opt,name=score,proto3"`
}

// ResultNtf 结算（MsgResult），同时落库 card_result 表。
type ResultNtf struct {
	RoomID  string     `json:"room_id" protobuf:"bytes,1,opt,name=room_id,proto3"`
	Winner  string     `json:"winner" protobuf:"bytes,2,opt,name=winner,proto3"` // 空=平局
	Rounds  int        `json:"rounds" protobuf:"varint,3,opt,name=rounds,proto3"`
	Scores  []ScoreNtf `json:"scores" protobuf:"bytes,4,rep,name=scores,proto3"`
	Timeout bool       `json:"timeout" protobuf:"varint,5,opt,name=timeout,proto3"` // MaxDuration 到期强结算
}
