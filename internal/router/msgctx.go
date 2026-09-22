package router

import (
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/pkg/framework"
)

// msgCtx framework.MsgCtx 的路由器实现。
type msgCtx struct {
	uid       string
	msgID     framework.MsgID
	seq       uint32
	trace     string
	codecType byte
	src       Source
}

func (c *msgCtx) UID() string            { return c.uid }
func (c *msgCtx) MsgID() framework.MsgID { return c.msgID }
func (c *msgCtx) Seq() uint32            { return c.seq }
func (c *msgCtx) TraceID() string        { return c.trace }

// Reply 主动推送（非当前请求的自动应答）。
//   - err 非 nil：按当前 msgID/seq 推错误信封；
//   - resp 非 nil：信封 code=0 包装推送（阶段 4 起可用 session.Push 推带独立 msgID 的消息）。
func (c *msgCtx) Reply(resp any, err error) {
	if err != nil {
		frame, frameErr := protocol.NewErrorFrameFor(c.codecType, c.msgID, c.seq,
			asErrCode(err).Code, asErrCode(err).Msg, nil)
		if frameErr == nil {
			_ = c.src.Send(frame)
		}
		return
	}
	if resp != nil {
		frame, frameErr := protocol.NewOKFrameFor(c.codecType, c.msgID, c.seq, resp)
		if frameErr == nil {
			_ = c.src.Send(frame)
		}
	}
}

var _ framework.MsgCtx = (*msgCtx)(nil)
