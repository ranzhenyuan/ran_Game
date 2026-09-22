package snake

import (
	"time"

	"github.com/rangame/server/pkg/framework"
)

// Module 贪吃蛇模块装配。
type Module struct{}

// Name 模块名（匹配请求 module 字段、RoomConfig.Module）。
func (Module) Name() string { return "snake" }

// RoomDef 2 人一桌；邮箱满则丢（局面快照驱动，丢个别 Move 不影响一致性）；
// 单局最长 5 分钟（MaxDuration 到期强结算，§8.2）。
func (Module) RoomDef() (framework.RoomConfig, func() framework.RoomLogic) {
	cfg := framework.RoomConfig{
		Module:       "snake",
		MaxPlayers:   2,
		MaxDuration:  5 * time.Minute,
		TickInterval: 100 * time.Millisecond,
		Mailbox: framework.MailboxPolicy{
			Capacity: 4096,
			OnFull:   framework.FullDrop,
		},
		MatchRule: framework.MatchRule{Module: "snake", Code: "ranked", Players: 2},
	}
	return cfg, func() framework.RoomLogic { return NewLogic() }
}

// RegisterRoutes 声明房间消息：Move 走可靠通道，State 仅下行无需注册。
func (Module) RegisterRoutes(r framework.RoomRegistrar) {
	r.RegisterRoom(MsgMove, func() any { return &MoveReq{} }, false)
}
