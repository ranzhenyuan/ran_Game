// Package quiz 是 §14 扩展验证示例：答题玩法。
//
// 与 snake/card 的差异面（验证扩展 API 覆盖度）：
//   - RoomCtx.After 定时器驱动：每题限时，超时自动跳题（跳题前 Cancel）；
//   - 题库内置导出（Bank），客户端/测试可确定性作答；
//   - OnDestroy 走 ProfileStore.Patch 回写本局得分（单写者路由，§10.4.4）。
//
// 仅依赖 pkg/framework；由 cmd 层作为 framework.GameModule 装配。
package quiz

import "github.com/rangame/server/pkg/framework"

// msgID 段：quiz 为第 2 个业务模块（ModuleBase(2) = 0x1200 起，每模块 256 个）。
const (
	// MsgAnswer 上行答题（可靠通道，ToRoom）。
	MsgAnswer framework.MsgID = 0x1200
	// MsgQuestion 下行出题（可靠通道广播）。
	MsgQuestion framework.MsgID = 0x1201
	// MsgResult 下行结算（可靠通道）。
	MsgResult framework.MsgID = 0x1202
)

// 对局参数。
const (
	Questions   = 5    // 每局题数（取 Bank 前 N 题）
	TimeLimitMS = 2000 // 每题限时（ms），超时自动跳题
)

// Question 单道题。
type Question struct {
	Text    string   `json:"text" protobuf:"bytes,1,opt,name=text,proto3"`
	Options []string `json:"options" protobuf:"bytes,2,rep,name=options,proto3"`
	Answer  int      `json:"-" protobuf:"varint,3,opt,name=answer,proto3"` // 正确选项下标；不下发（json 忽略）
}

// Bank 内置题库（确定性：每局固定取前 Questions 题）。
// 导出以便客户端联调与 e2e 测试确定性作答。
var Bank = []Question{
	{Text: "1+1=?", Options: []string{"2", "3", "11"}, Answer: 0},
	{Text: "Go 中声明函数的关键字?", Options: []string{"def", "func", "fn"}, Answer: 1},
	{Text: "HTTP 404 含义?", Options: []string{"OK", "Server Error", "Not Found"}, Answer: 2},
	{Text: "2 的 3 次方?", Options: []string{"6", "8", "9"}, Answer: 1},
	{Text: "TCP 属于哪一层协议?", Options: []string{"传输层", "应用层", "网络层"}, Answer: 0},
	{Text: "以下哪个不是 Go 的并发原语?", Options: []string{"goroutine", "thread", "channel"}, Answer: 1},
	{Text: "JSON 中表示 null 的字面量?", Options: []string{"nil", "null", "none"}, Answer: 1},
	{Text: "WebSocket 默认端口(over TLS)?", Options: []string{"80", "443", "8080"}, Answer: 1},
}

// AnswerReq 上行答题：当前题号 Idx + 选项下标 Option。
type AnswerReq struct {
	Idx    int `json:"idx" protobuf:"varint,1,opt,name=idx,proto3"`
	Option int `json:"option" protobuf:"varint,2,opt,name=option,proto3"`
}

// QuestionNtf 下行出题（MsgQuestion，可靠广播；Answer 不下发）。
type QuestionNtf struct {
	Idx         int      `json:"idx" protobuf:"varint,1,opt,name=idx,proto3"` // 题号，0 起
	Text        string   `json:"text" protobuf:"bytes,2,opt,name=text,proto3"`
	Options     []string `json:"options" protobuf:"bytes,3,rep,name=options,proto3"`
	TimeLimitMS int      `json:"time_limit_ms" protobuf:"varint,4,opt,name=time_limit_ms,proto3"`
	Total       int      `json:"total" protobuf:"varint,5,opt,name=total,proto3"`
}

// ScoreNtf 单方累计得分。
type ScoreNtf struct {
	UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Score int    `json:"score" protobuf:"varint,2,opt,name=score,proto3"`
}

// ResultNtf 结算（MsgResult），同时落库 quiz_result 表。
type ResultNtf struct {
	RoomID string     `json:"room_id" protobuf:"bytes,1,opt,name=room_id,proto3"`
	Winner string     `json:"winner" protobuf:"bytes,2,opt,name=winner,proto3"` // 空=平局
	Scores []ScoreNtf `json:"scores" protobuf:"bytes,3,rep,name=scores,proto3"`
}
