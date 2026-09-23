package doudizhu

import (
	"time"

	"github.com/rangame/server/pkg/framework"
)

// Module 斗地主模块装配。
type Module struct{}

// Name 模块名（匹配请求 module 字段、RoomConfig.Module）。
func (Module) Name() string { return "doudizhu" }

// RoomDef 3 人一桌；回合制事件驱动，Tick 仅做离线兜底（低频即可）；
// MaxDuration 到期强结算（§8.2）。
func (Module) RoomDef() (framework.RoomConfig, func() framework.RoomLogic) {
	cfg := framework.RoomConfig{
		Module:       "doudizhu",
		MaxPlayers:   3,
		MaxDuration:  10 * time.Minute,
		TickInterval: time.Second,
		Mailbox: framework.MailboxPolicy{
			Capacity: 4096,
			OnFull:   framework.FullDrop,
		},
		MatchRule: framework.MatchRule{Module: "doudizhu", Code: "ranked", Players: 3},
	}
	return cfg, func() framework.RoomLogic { return NewLogic() }
}

// RegisterRoutes 声明房间消息：叫分/出牌走可靠通道；其余仅下行无需注册。
func (Module) RegisterRoutes(r framework.RoomRegistrar) {
	r.RegisterRoom(MsgCall, func() any { return &CallReq{} }, false)
	r.RegisterRoom(MsgPlay, func() any { return &PlayReq{} }, false)
}
