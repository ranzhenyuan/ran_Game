package protocol

import (
	"encoding/json"

	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// ErrorEnvelope 是 §4.3 定义的统一错误/响应信封的 JSON 编码形态。
// Protobuf 形态（proto/framework/envelope.proto）在接入 pb Codec 时映射同构字段。
//
// 帧与信封的关系：帧头 msgID 沿用被响应的原请求 msgID，seq 为原请求 seq；
// Body 为业务 resp 的二次编码（按会话 Codec），无业务数据时为空。
type ErrorEnvelope struct {
	Seq  uint32          `json:"seq" protobuf:"varint,1,opt,name=seq,proto3"`
	Code framework.Code  `json:"code" protobuf:"varint,2,opt,name=code,proto3"`
	Msg  string          `json:"msg,omitempty" protobuf:"bytes,3,opt,name=msg,proto3"`
	Body json.RawMessage `json:"body,omitempty" protobuf:"bytes,4,opt,name=body,proto3"`
}

// NewErrorFrame 构造错误响应帧（JSON Codec）。等价 NewErrorFrameFor(TypeJSON, ...)。
func NewErrorFrame(msgID framework.MsgID, seq uint32, code framework.Code, msg string, body any) (*transport.Frame, error) {
	return NewErrorFrameFor(TypeJSON, msgID, seq, code, msg, body)
}

// NewErrorFrameFor 按指定 Codec 类型构造错误响应帧（§4.3）。
//
//	msgID：原请求 msgID（客户端据此关联请求）
//	code：错误码；body 可选业务数据，nil 表示无
func NewErrorFrameFor(typ byte, msgID framework.MsgID, seq uint32, code framework.Code, msg string, body any) (*transport.Frame, error) {
	c, err := Get(typ)
	if err != nil {
		return nil, err
	}
	if msg == "" {
		msg = code.Message()
	}
	env := ErrorEnvelope{Seq: seq, Code: code, Msg: msg}
	if body != nil {
		raw, err := c.Marshal(body)
		if err != nil {
			return nil, err
		}
		env.Body = raw
	}

	raw, err := c.Marshal(&env)
	if err != nil {
		return nil, err
	}

	return &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(msgID),
		Flag:  typ,
		Seq:   seq,
		Body:  raw,
	}, nil
}

// NewOKFrame 构造成功响应帧：resp 编码进信封 body，信封 code=0。
func NewOKFrame(msgID framework.MsgID, seq uint32, resp any) (*transport.Frame, error) {
	return NewErrorFrame(msgID, seq, framework.OK, "", resp)
}

// NewOKFrameFor 按指定 Codec 类型构造成功响应帧。
func NewOKFrameFor(typ byte, msgID framework.MsgID, seq uint32, resp any) (*transport.Frame, error) {
	return NewErrorFrameFor(typ, msgID, seq, framework.OK, "", resp)
}

// DecodeEnvelope 按 Codec 类型解码信封（阶段 1 仅 JSON；pb 接入后扩展）。
func DecodeEnvelope(f *transport.Frame) (*ErrorEnvelope, error) {
	c, err := Get(f.CodecType())
	if err != nil {
		return nil, err
	}
	var env ErrorEnvelope
	if err := c.Unmarshal(f.Body, &env); err != nil {
		return nil, err
	}
	return &env, nil
}
