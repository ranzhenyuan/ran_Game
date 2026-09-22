package card

import (
	"time"

	"github.com/rangame/server/pkg/framework"
)

// Module 卡牌模块装配。
type Module struct{}

// Name 模块名（匹配请求 module 字段、RoomConfig.Module）。
func (Module) Name() string { return "card" }

// RoomDef 2 人一桌；回合制事件驱动，Tick 只做离线托管（低频即可）；
// MaxDuration 到期强结算（§8.2）。
func (Module) RoomDef() (framework.RoomConfig, func() framework.RoomLogic) {
	cfg := framework.RoomConfig{
		Module:       "card",
		MaxPlayers:   2,
		MaxDuration:  2 * time.Minute,
		TickInterval: 500 * time.Millisecond,
		Mailbox: framework.MailboxPolicy{
			Capacity: 4096,
			OnFull:   framework.FullDrop,
		},
		MatchRule: framework.MatchRule{Module: "card", Code: "ranked", Players: 2},
	}
	return cfg, func() framework.RoomLogic { return NewLogic() }
}

// RegisterRoutes 声明房间消息：Play 走可靠通道；Round/Result/Deal 仅下行无需注册。
func (Module) RegisterRoutes(r framework.RoomRegistrar) {
	r.RegisterRoom(MsgPlay, func() any { return &PlayReq{} }, false)
}
