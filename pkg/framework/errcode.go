// Package framework 是游戏业务唯一允许依赖的框架稳定 API 面。
// 本文件定义统一错误码（架构文档 §4.3）。
package framework

import "fmt"

// Code 错误码。分段：
//
//	0     成功
//	1xxx  协议/参数错误
//	2xxx  会话/鉴权错误
//	3xxx  房间/业务错误
//	4xxx  服务端错误
type Code uint16

const (
	OK Code = 0

	// 1xxx 协议/参数
	ErrUnknownMsg   Code = 1001
	ErrDecode       Code = 1002
	ErrFrameTooLong Code = 1003

	// 2xxx 会话/鉴权
	ErrUnauthorized Code = 2001
	ErrTokenInvalid Code = 2002
	ErrRateLimited  Code = 2003
	ErrKicked       Code = 2004

	// 3xxx 房间/业务
	ErrNotInRoom Code = 3001
	ErrRoomFull  Code = 3002
	ErrGameStart Code = 3003
	ErrInQueue   Code = 3004 // 已在匹配队列中

	// 4xxx 服务端
	ErrInternal        Code = 4001
	ErrOverloaded      Code = 4002 // 背压触发，客户端指数退避
	ErrStorageDegraded Code = 4003 // 持久化降级：熔断中，异步写被拒（§10.3）
	ErrMaintenance     Code = 4005 // 停机/Logic 排水/滚动发版
)

// defaultMessages 框架级默认文案；业务错误码由模块自行扩展。
var defaultMessages = map[Code]string{
	OK:                 "ok",
	ErrUnknownMsg:      "unknown message id",
	ErrDecode:          "decode failed",
	ErrFrameTooLong:    "frame too large",
	ErrUnauthorized:    "unauthorized",
	ErrTokenInvalid:    "reconnect token invalid",
	ErrRateLimited:     "rate limited",
	ErrKicked:          "kicked",
	ErrNotInRoom:       "not in room",
	ErrRoomFull:        "room is full",
	ErrGameStart:       "game already started",
	ErrInQueue:         "already in match queue",
	ErrInternal:        "internal error",
	ErrOverloaded:      "server overloaded",
	ErrStorageDegraded: "storage degraded, try later",
	ErrMaintenance:     "server under maintenance",
}

// ErrCode 携带错误码的错误类型，Handler 返回它时由框架映射为错误信封（§6.1）。
type ErrCode struct {
	Code Code
	Msg  string
}

func (e *ErrCode) Error() string {
	return fmt.Sprintf("errcode %d: %s", e.Code, e.Msg)
}

// NewErr 构造错误；msg 为空时使用框架默认文案。
func NewErr(code Code, msg string) *ErrCode {
	if msg == "" {
		msg = defaultMessages[code]
	}
	return &ErrCode{Code: code, Msg: msg}
}

// Message 返回错误码对应的默认文案。
func (c Code) Message() string {
	return defaultMessages[c]
}
