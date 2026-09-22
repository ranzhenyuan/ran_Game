package framework

// MsgCtx 个人消息处理上下文（§6.1）。由路由器构造，在 PlayerActor goroutine 内使用。
//
// Handler 的返回值 (resp, err) 由框架自动按原 seq 回包；
// 需要主动推送多条消息时可调用 Reply（如战斗中的异步通知）。
type MsgCtx interface {
	UID() string
	MsgID() MsgID
	Seq() uint32
	TraceID() string
	// Reply 主动推送：err 非 nil 时推错误信封，否则推 resp 业务消息。
	// 这不是对当前请求的自动应答（自动应答由 Handler 返回值完成）。
	Reply(resp any, err error)
}

// HandlerFunc 个人消息处理器签名（§6.1）。
//   - resp != nil：框架自动以原 msgID/seq 回成功信封（code=0）；
//   - err != nil：*framework.ErrCode 映射对应错误码，其他 error 映射 4001。
type HandlerFunc func(ctx MsgCtx, req any) (resp any, err error)
