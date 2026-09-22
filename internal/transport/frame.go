// Package transport 是接入层：Conn 统一抽象、TCP/WebSocket Acceptor、
// 帧拆包、每连接读泵 + 写聚合（架构文档 §3 / §4.1）。
//
// 本文件实现 §4.1 帧格式：
//
//	len(4B) | ver(1B) | msgID(4B) | flag(1B) | seq(4B) | [routeLen(2B) | route] | body
//
// len 为其后续全部字节数；len 拆包与帧头解析在 Transport 内部完成（§3.1 统一口径）。
package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ProtocolVer 当前协议版本（帧头 ver 字段）。
const ProtocolVer uint8 = 1

// flag 位定义（低 4 位为 codec 类型）。
const (
	FlagCodecMask   byte = 0x0F
	FlagCompress    byte = 0x10 // bit4：压缩（预留）
	FlagAckRequired byte = 0x20 // bit5：需要 ack
	FlagSnapshot    byte = 0x40 // bit6：快照类消息，latest-wins，不进重放缓冲（§5.3）
	FlagRoute       byte = 0x80 // bit7：帧体携带 routeLen|route（仅调试模式）
	DefaultMaxFrame int  = 64 * 1024
)

const (
	lenFieldSize   = 4
	headerFixedLen = 1 + 4 + 1 + 4 // ver + msgID + flag + seq（不含 len 自身）
	routeLenSize   = 2
)

var (
	ErrFrameTooLarge = errors.New("transport: frame exceeds max size")
	ErrBadFrame      = errors.New("transport: malformed frame")
)

// Frame 解析后的应用层帧。
type Frame struct {
	Ver   uint8
	MsgID uint32
	Flag  byte
	Seq   uint32
	Route string // 调试模式字符串路由；生产为空
	Body  []byte
}

// CodecType 取 flag 低 4 位的序列化类型（0=JSON, 1=Protobuf）。
func (f *Frame) CodecType() byte {
	return f.Flag & FlagCodecMask
}

// IsSnapshot 是否快照通道消息。
func (f *Frame) IsSnapshot() bool {
	return f.Flag&FlagSnapshot != 0
}

// EncodeFrame 把帧序列化为完整字节流（含 4B len 前缀）。
func EncodeFrame(f *Frame) ([]byte, error) {
	if len(f.Route) > 65535 {
		return nil, fmt.Errorf("%w: route too long", ErrBadFrame)
	}

	payloadLen := headerFixedLen + len(f.Body)
	if f.Route != "" {
		f.Flag |= FlagRoute
		payloadLen += routeLenSize + len(f.Route)
	} else {
		f.Flag &^= FlagRoute
	}
	if uint64(payloadLen) > 0xFFFFFFFF {
		return nil, fmt.Errorf("%w: payload overflow", ErrFrameTooLarge)
	}

	out := make([]byte, lenFieldSize+payloadLen)
	binary.BigEndian.PutUint32(out[0:4], uint32(payloadLen))

	pos := lenFieldSize
	out[pos] = f.Ver
	pos++
	binary.BigEndian.PutUint32(out[pos:pos+4], f.MsgID)
	pos += 4
	out[pos] = f.Flag
	pos++
	binary.BigEndian.PutUint32(out[pos:pos+4], f.Seq)
	pos += 4

	if f.Route != "" {
		binary.BigEndian.PutUint16(out[pos:pos+2], uint16(len(f.Route)))
		pos += 2
		pos += copy(out[pos:], f.Route)
	}
	copy(out[pos:], f.Body)

	return out, nil
}

// DecodeFrame 从 r 精确读取一帧（io.ReadFull 天然处理 TCP 粘包/半包：
// 数据不足时阻塞等待，bufio.Reader 等缓冲 Reader 可在多帧间连续调用）。
//
// maxFrameSize <= 0 时使用 DefaultMaxFrame。
func DecodeFrame(r io.Reader, maxFrameSize int) (*Frame, error) {
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrame
	}

	var lenBuf [lenFieldSize]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err // 连接关闭/超时由上层判断
	}

	payloadLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if payloadLen < headerFixedLen {
		return nil, fmt.Errorf("%w: payload length %d < header", ErrBadFrame, payloadLen)
	}
	if payloadLen > maxFrameSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, payloadLen, maxFrameSize)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	pos := 0
	f := &Frame{Ver: payload[pos]}
	pos++
	f.MsgID = binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	f.Flag = payload[pos]
	pos++
	f.Seq = binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4

	if payloadLen-pos > 0 {
		// route 是否存在由 flag bit7 显式标记（调试模式），避免猜测 body。
		if f.Flag&FlagRoute != 0 {
			rest := payloadLen - pos
			if rest < routeLenSize {
				return nil, fmt.Errorf("%w: truncated route length", ErrBadFrame)
			}
			routeLen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
			pos += routeLenSize
			if routeLen > rest-routeLenSize {
				return nil, fmt.Errorf("%w: truncated route", ErrBadFrame)
			}
			f.Route = string(payload[pos : pos+routeLen])
			pos += routeLen
		}
		f.Body = payload[pos:]
	} else if f.Flag&FlagRoute != 0 {
		return nil, fmt.Errorf("%w: route flag set but no payload", ErrBadFrame)
	}

	return f, nil
}
