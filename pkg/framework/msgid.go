package framework

// MsgID 消息路由 ID（帧头 uint32）。
// 分段规划（架构文档模块蓝图 §4.1）：
//
//	0x0000–0x00FF 框架系统消息
//	0x0100–0x01FF 房间/匹配通用消息
//	0x0200–0x0FFF 框架保留
//	0x1000+     业务模块，每 module 256 个
type MsgID uint32

// 系统段 0x0000–0x00FF
const (
	MsgPing        MsgID = 0x0001
	MsgPong        MsgID = 0x0002
	MsgLogin       MsgID = 0x0010
	MsgReconnect   MsgID = 0x0011
	MsgKick        MsgID = 0x0020
	MsgMaintenance MsgID = 0x0021 // 4005
	MsgLogicDown   MsgID = 0x0022
)

// 房间/匹配段 0x0100–0x01FF
const (
	MsgMatch        MsgID = 0x0100
	MsgMatchCancel  MsgID = 0x0101
	MsgJoinRoom     MsgID = 0x0110
	MsgLeaveRoom    MsgID = 0x0111
	MsgMemberChange MsgID = 0x0112
	MsgRecoverRoom  MsgID = 0x0120
	MsgResyncReq    MsgID = 0x0130
	MsgResyncNtf    MsgID = 0x0131
	MsgPullSnap     MsgID = 0x0140
)

// ModuleBase 返回某业务模块的 msgID 段起点。
// moduleIndex 从 0 开始（0 -> 0x1000，1 -> 0x1100 ...）。
func ModuleBase(moduleIndex int) MsgID {
	return MsgID(0x1000 + moduleIndex*0x0100)
}
