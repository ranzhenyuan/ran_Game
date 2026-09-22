package quiz

import (
	"time"

	"github.com/rangame/server/pkg/framework"
)

// Module 答题模块装配。
type Module struct{}

// Name 模块名（匹配请求 module 字段、RoomConfig.Module）。
func (Module) Name() string { return "quiz" }

// RoomDef 2 人一桌；局时长上界 = 题数 × (时限 + 余量)（§8.2 MaxDuration 兜底）。
func (Module) RoomDef() (framework.RoomConfig, func() framework.RoomLogic) {
	cfg := framework.RoomConfig{
		Module:       "quiz",
		MaxPlayers:   2,
		MaxDuration:  time.Duration(Questions)*time.Duration(TimeLimitMS)*time.Millisecond + 30*time.Second,
		TickInterval: time.Second,
		Mailbox: framework.MailboxPolicy{
			Capacity: 4096,
			OnFull:   framework.FullDrop,
		},
		MatchRule: framework.MatchRule{Module: "quiz", Code: "ranked", Players: 2},
	}
	return cfg, func() framework.RoomLogic { return NewLogic() }
}

// RegisterRoutes 声明房间消息：Answer 走可靠通道；Question/Result 仅下行无需注册。
func (Module) RegisterRoutes(r framework.RoomRegistrar) {
	r.RegisterRoom(MsgAnswer, func() any { return &AnswerReq{} }, false)
}
