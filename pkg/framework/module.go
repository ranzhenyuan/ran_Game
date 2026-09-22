package framework

// GameModule 玩法模块装配接口（§14 扩展指南）。
// games/* 只依赖 pkg/framework：实现本接口后由 cmd 层传入框架装配，
// internal 包永不 import games，保证依赖方向 games → framework。
type GameModule interface {
	Name() string
	// RoomDef 返回房间配置与逻辑工厂（每房一个独立逻辑实例）。
	RoomDef() (RoomConfig, func() RoomLogic)
	// RegisterRoutes 注册本模块的房间消息（ToRoom）。
	RegisterRoutes(r RoomRegistrar)
}

// RoomRegistrar 房间消息注册面（由内部路由器结构化满足）。
type RoomRegistrar interface {
	// RegisterRoom 声明房间消息的请求体工厂与通道属性：
	// snapshot=true 走快照通道（bit6，latest-wins），否则走可靠通道。
	RegisterRoom(msgID MsgID, reqFactory func() any, snapshot bool)
}
