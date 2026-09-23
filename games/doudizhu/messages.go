// Package doudizhu 是框架内置玩法示例：斗地主（3 人回合制）。
//
// 同步模型（双通道，架构文档 §5.3/§16.4）：
//   - 公共确定性事件（发牌数/叫分/出牌/过牌/轮次切换）走可靠通道广播，保证有序不丢；
//   - 全量权威快照走快照通道（latest-wins），断线重连一次拉齐；
//   - 私有手牌只单推本人（Member.Push），绝不广播，从机制上杜绝透视。
//
// 仅依赖 pkg/framework；由 cmd 层作为 framework.GameModule 装配。
package doudizhu

import "github.com/rangame/server/pkg/framework"

// msgID 段：doudizhu 为第 3 个业务模块（ModuleBase(3) = 0x1300 起，每模块 256 个）。
// 注意：必须追加在 snake/card/quiz 之后，上线后模块列表顺序不可调整。
const (
	// MsgCall 上行叫分（可靠通道，ToRoom）。
	MsgCall framework.MsgID = 0x1300
	// MsgPlay 上行出牌（可靠通道，ToRoom）；Cards 为空表示「不出/过」。
	MsgPlay framework.MsgID = 0x1301

	// MsgDeal 下行发牌（可靠通道，单推本人手牌）。
	MsgDeal framework.MsgID = 0x1310
	// MsgStage 下行阶段切换（叫分/出牌/结算），可靠广播。
	MsgStage framework.MsgID = 0x1311
	// MsgCallNtf 下行叫分事件，可靠广播。
	MsgCallNtf framework.MsgID = 0x1312
	// MsgPlayed 下行出牌/过牌事件，可靠广播。
	MsgPlayed framework.MsgID = 0x1313
	// MsgTurn 下行轮到谁 + 出牌截止时间，可靠广播。
	MsgTurn framework.MsgID = 0x1314
	// MsgSnapshot 下行全量权威快照（快照通道，重连兜底）。
	MsgSnapshot framework.MsgID = 0x1315
	// MsgResult 下行结算，可靠广播。
	MsgResult framework.MsgID = 0x1316
)

// 阶段。
const (
	StageWaiting  = 0 // 等待开局（满员前）
	StageBidding  = 1 // 叫分阶段
	StagePlaying  = 2 // 出牌阶段
	StageFinished = 3 // 结算
)

// 牌型（服务端识别，用于比较能否压过上家）。
const (
	TypeInvalid    = ""
	TypeSingle     = "single"      // 单张
	TypePair       = "pair"        // 对子
	TypeTriple     = "triple"      // 三张
	TypeTriple1    = "triple_one"  // 三带一
	TypeTriple2    = "triple_two"  // 三带二
	TypeStraight   = "straight"    // 顺子（5+ 张连续单牌，不含 2/王）
	TypePlane      = "plane"       // 飞机（2+ 组连续三张）
	TypeBomb       = "bomb"        // 炸弹（四张）
	TypeRocket     = "rocket"      // 王炸（双王）
)

// Card 单张牌。
//   Suit: 0-3（♠♥♣♦），Joker: 0=小王 1=大王；Rank: 3-15（3..2），16=小王 17=大王。
//   比较以 Rank 为准，Suit 仅用于客户端花色展示与去重。
type Card struct {
	Suit  int `json:"suit" protobuf:"varint,1,opt,name=suit,proto3"`
	Rank  int `json:"rank" protobuf:"varint,2,opt,name=rank,proto3"`
	Joker int `json:"joker" protobuf:"varint,3,opt,name=joker,proto3"` // -1=普通牌
}

// ID 返回牌的唯一标识（用于手牌去重与客户端匹配）。
func (c Card) ID() int {
	if c.Joker >= 0 {
		return 100 + c.Joker
	}
	return c.Suit*100 + c.Rank
}

// Less 排序用：按 Rank 升序。
func (c Card) Less(o Card) bool { return c.Rank < o.Rank }

// HandNtf 发牌（MsgDeal，单推本人）。Landlord 底牌开局为空，叫分结束确定地主后随阶段下发。
type HandNtf struct {
	Hand      []Card `json:"hand" protobuf:"bytes,1,rep,name=hand,proto3"`
	Landlord3 []Card `json:"landlord3,omitempty" protobuf:"bytes,2,rep,name=landlord3,proto3"` // 成为地主时下发 3 张底牌
}

// StageNtf 阶段切换（MsgStage）。
type StageNtf struct {
	Stage int `json:"stage" protobuf:"varint,1,opt,name=stage,proto3"`
}

// CallNtf 叫分事件（MsgCallNtf）。
type CallNtf struct {
	UID    string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Score  int    `json:"score" protobuf:"varint,2,opt,name=score,proto3"` // 0=不叫
	Passed bool   `json:"passed" protobuf:"varint,3,opt,name=passed,proto3"`
}

// PlayedNtf 出牌/过牌事件（MsgPlayed）。
type PlayedNtf struct {
	UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Cards []Card `json:"cards,omitempty" protobuf:"bytes,2,rep,name=cards,proto3"` // 空=过牌
	Type  string `json:"type" protobuf:"bytes,3,opt,name=type,proto3"`
	Pass  bool   `json:"pass" protobuf:"varint,4,opt,name=pass,proto3"`
}

// TurnNtf 轮到谁（MsgTurn）。
type TurnNtf struct {
	CurUID     string `json:"cur_uid" protobuf:"bytes,1,opt,name=cur_uid,proto3"`
	Landlord   string `json:"landlord" protobuf:"bytes,2,opt,name=landlord,proto3"` // 叫分结束后填充
	BaseScore  int    `json:"base_score" protobuf:"varint,3,opt,name=base_score,proto3"`
	DeadlineMs int64  `json:"deadline_ms" protobuf:"varint,4,opt,name=deadline_ms,proto3"`
}

// PlayerSnap 快照中单个玩家的公开信息（不含手牌，仅张数）。
type PlayerSnap struct {
	UID     string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	HandCnt int    `json:"hand_cnt" protobuf:"varint,2,opt,name=hand_cnt,proto3"`
}

// SnapshotNtf 全量权威快照（MsgSnapshot，重连单推本人视角）。
// 关键：不含他人真实手牌，仅本人 MyHand。
type SnapshotNtf struct {
	Stage      int          `json:"stage" protobuf:"varint,1,opt,name=stage,proto3"`
	Landlord   string       `json:"landlord" protobuf:"bytes,2,opt,name=landlord,proto3"`
	CurUID     string       `json:"cur_uid" protobuf:"bytes,3,opt,name=cur_uid,proto3"`
	BaseScore  int          `json:"base_score" protobuf:"varint,4,opt,name=base_score,proto3"`
	Players    []PlayerSnap `json:"players" protobuf:"bytes,5,rep,name=players,proto3"`
	LastPlayed PlayedNtf    `json:"last_played" protobuf:"bytes,6,opt,name=last_played,proto3"`
	MyHand     []Card       `json:"my_hand" protobuf:"bytes,7,rep,name=my_hand,proto3"` // 仅本人
}

// CallReq 上行叫分（MsgCall）。Score: 0=不叫，1-3 叫分。
type CallReq struct {
	Score int `json:"score" protobuf:"varint,1,opt,name=score,proto3"`
}

// PlayReq 上行出牌（MsgPlay）。IDs 为客户端 Card.ID() 列表；空=过牌。
type PlayReq struct {
	IDs []int `json:"ids" protobuf:"varint,1,rep,packed,name=ids,proto3"`
}

// ResultNtf 结算（MsgResult），同时落库 doudizhu_result 表。
type ResultNtf struct {
	RoomID   string         `json:"room_id" protobuf:"bytes,1,opt,name=room_id,proto3"`
	Landlord string         `json:"landlord" protobuf:"bytes,2,opt,name=landlord,proto3"`
	Farmers  []string       `json:"farmers" protobuf:"bytes,3,rep,name=farmers,proto3"`
	LandlordWin bool       `json:"landlord_win" protobuf:"varint,4,opt,name=landlord_win,proto3"`
	BaseScore int           `json:"base_score" protobuf:"varint,5,opt,name=base_score,proto3"`
	Timeout   bool          `json:"timeout" protobuf:"varint,6,opt,name=timeout,proto3"`
}
