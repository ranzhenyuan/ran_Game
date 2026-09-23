// Package bot 是自研 Go 压测客户端（架构文档 §15.1）。
//
// 支持两种序列化（JSON/PB）按帧 flag 选择，复用服务端 transport/protocol 包做帧编解码，
// 保证与线上协议完全一致。四个压测场景见 scenarios.go。
package bot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// Bot 单个压测连接。
type Bot struct {
	id       int
	connAddr string
	conn     *websocket.Conn
	codec    byte // 0=JSON, 1=PB
	uid      string
	token    string
	seq      uint32
	logger   *slog.Logger

	recvBuf []byte // WS 消息体流式解码残余
	mu      sync.Mutex

	// 指标
	pingsSent    atomic.Int64
	pongsRecv    atomic.Int64
	msgsSent     atomic.Int64
	msgsRecv     atomic.Int64
	errors       atomic.Int64
	reconnects   atomic.Int64
	latencySum   atomic.Int64 // ns
	latencyCount atomic.Int64
	latencyMax   atomic.Int64 // ns
}

// Config 单个 Bot 的配置。
type Config struct {
	ID     int
	UID    string
	Codec  byte // 0=JSON, 1=PB
	Logger *slog.Logger
}

// New 创建 Bot（不连接，ConnectTo 时才拨号）。
func New(cfg Config) *Bot {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Bot{
		id:     cfg.ID,
		codec:  cfg.Codec,
		uid:    cfg.UID,
		logger: cfg.Logger,
	}
}

// ConnectTo 拨号到指定 WS 地址并登录。
func (b *Bot) ConnectTo(ctx context.Context, addr string) error {
	b.connAddr = addr
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	header := map[string][]string{"Origin": {"*"}}
	conn, _, err := dialer.DialContext(ctx, addr, header)
	if err != nil {
		return fmt.Errorf("bot %d dial: %w", b.id, err)
	}
	b.conn = conn
	conn.SetReadLimit(256 * 1024)
	return b.login()
}

// Reconnect 断线后用 reconnectToken 重连。
func (b *Bot) Reconnect(ctx context.Context) error {
	b.reconnects.Add(1)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	header := map[string][]string{"Origin": {"*"}}
	conn, _, err := dialer.DialContext(ctx, b.connAddr, header)
	if err != nil {
		return fmt.Errorf("bot %d reconnect dial: %w", b.id, err)
	}
	b.conn = conn
	conn.SetReadLimit(256 * 1024)
	b.recvBuf = nil

	c, _ := protocol.Get(b.codec)
	req := session.ReconnectReq{
		ReconnectToken: b.token,
	}
	raw, _ := c.Marshal(&req)
	frame := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(framework.MsgReconnect),
		Flag:  b.codec,
		Seq:   b.nextSeq(),
		Body:  raw,
	}
	if err := b.writeFrame(frame); err != nil {
		return err
	}
	resp, err := b.recvFrame()
	if err != nil {
		return err
	}
	env, err := protocol.DecodeEnvelope(resp)
	if err != nil {
		return err
	}
	if env.Code != framework.OK {
		return fmt.Errorf("reconnect failed: code=%d", env.Code)
	}
	var rr session.ReconnectResp
	_ = c.Unmarshal(env.Body, &rr)
	b.token = rr.NewReconnectToken
	return nil
}

func (b *Bot) login() error {
	c, err := protocol.Get(b.codec)
	if err != nil {
		return err
	}
	req := session.LoginReq{
		UID:         b.uid,
		ProtocolVer: 1,
		Codec:       uint32(b.codec),
		Platform:    "bot",
	}
	raw, err := c.Marshal(&req)
	if err != nil {
		return err
	}
	frame := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(framework.MsgLogin),
		Flag:  b.codec,
		Seq:   b.nextSeq(),
		Body:  raw,
	}
	if err := b.writeFrame(frame); err != nil {
		return fmt.Errorf("bot %d login write: %w", b.id, err)
	}
	resp, err := b.recvFrame()
	if err != nil {
		return fmt.Errorf("bot %d login recv: %w", b.id, err)
	}
	env, err := protocol.DecodeEnvelope(resp)
	if err != nil {
		return fmt.Errorf("bot %d decode envelope: %w", b.id, err)
	}
	if env.Code != framework.OK {
		return fmt.Errorf("bot %d login failed: code=%d msg=%s", b.id, env.Code, env.Msg)
	}
	var lr session.LoginResp
	if err := c.Unmarshal(env.Body, &lr); err != nil {
		return fmt.Errorf("bot %d decode login resp: %w", b.id, err)
	}
	b.token = lr.ReconnectToken
	return nil
}

// Close 关闭连接。
func (b *Bot) Close() error {
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

// Ping 发送心跳。
func (b *Bot) Ping() error {
	c, _ := protocol.Get(b.codec)
	p := session.Ping{ClientTS: time.Now().UnixMilli()}
	raw, err := c.Marshal(&p)
	if err != nil {
		return err
	}
	frame := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(framework.MsgPing),
		Flag:  b.codec,
		Seq:   b.nextSeq(),
		Body:  raw,
	}
	b.pingsSent.Add(1)
	return b.writeFrame(frame)
}

// SendMatch 发送匹配请求。
func (b *Bot) SendMatch(module, code string) error {
	c, _ := protocol.Get(b.codec)
	req := struct {
		Module string `json:"module" protobuf:"bytes,1,opt,name=module,proto3"`
		Code   string `json:"code,omitempty" protobuf:"bytes,2,opt,name=code,proto3"`
	}{Module: module, Code: code}
	raw, err := c.Marshal(&req)
	if err != nil {
		return err
	}
	frame := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(framework.MsgMatch),
		Flag:  b.codec,
		Seq:   b.nextSeq(),
		Body:  raw,
	}
	b.msgsSent.Add(1)
	return b.writeFrame(frame)
}

// SendMove 发送 snake 转向（MsgMove=0x1000，可靠通道）。
func (b *Bot) SendMove(dir int) error {
	c, _ := protocol.Get(b.codec)
	req := struct {
		Dir int `json:"dir" protobuf:"varint,1,opt,name=dir,proto3"`
	}{Dir: dir}
	raw, err := c.Marshal(&req)
	if err != nil {
		return err
	}
	frame := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(0x1000), // snake.MsgMove（bot 不 import games 包，保持解耦）
		Flag:  b.codec,
		Seq:   b.nextSeq(),
		Body:  raw,
	}
	b.msgsSent.Add(1)
	return b.writeFrame(frame)
}

// SendCall 发送斗地主叫分（MsgCall=0x1300，可靠通道）。score: 0=不叫，1-3 叫分。
func (b *Bot) SendCall(score int) error {
	c, _ := protocol.Get(b.codec)
	req := struct {
		Score int `json:"score" protobuf:"varint,1,opt,name=score,proto3"`
	}{Score: score}
	raw, err := c.Marshal(&req)
	if err != nil {
		return err
	}
	frame := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(0x1300), // doudizhu.MsgCall（bot 不 import games 包，保持解耦）
		Flag:  b.codec,
		Seq:   b.nextSeq(),
		Body:  raw,
	}
	b.msgsSent.Add(1)
	return b.writeFrame(frame)
}

// SendPlay 发送斗地主出牌（MsgPlay=0x1301，可靠通道）。ids 为牌 ID 列表，空=过牌。
func (b *Bot) SendPlay(ids []int) error {
	c, _ := protocol.Get(b.codec)
	req := struct {
		IDs []int `json:"ids" protobuf:"varint,1,rep,packed,name=ids,proto3"`
	}{IDs: ids}
	raw, err := c.Marshal(&req)
	if err != nil {
		return err
	}
	frame := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: uint32(0x1301), // doudizhu.MsgPlay
		Flag:  b.codec,
		Seq:   b.nextSeq(),
		Body:  raw,
	}
	b.msgsSent.Add(1)
	return b.writeFrame(frame)
}

// RecvLoop 持续读取帧并交给 handler。
func (b *Bot) RecvLoop(ctx context.Context, handler func(f *transport.Frame)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		b.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		f, err := b.recvFrame()
		if err != nil {
			b.errors.Add(1)
			return err
		}
		b.msgsRecv.Add(1)
		// Pong 延迟统计
		if f.MsgID == uint32(framework.MsgPong) {
			c, _ := protocol.Get(b.codec)
			var p session.Pong
			if err := c.Unmarshal(f.Body, &p); err == nil && p.ClientTS > 0 {
				lat := time.Now().UnixMilli() - int64(p.ClientTS)
				if lat > 0 {
					ns := lat * int64(time.Millisecond)
					b.latencySum.Add(ns)
					b.latencyCount.Add(1)
					if ns > b.latencyMax.Load() {
						b.latencyMax.Store(ns)
					}
				}
			}
			b.pongsRecv.Add(1)
		}
		if handler != nil {
			handler(f)
		}
	}
}

// Token 返回当前 reconnect token。
func (b *Bot) Token() string { return b.token }

// ---- 内部方法 ----

func (b *Bot) nextSeq() uint32 {
	return atomic.AddUint32(&b.seq, 1)
}

func (b *Bot) writeFrame(f *transport.Frame) error {
	raw, err := transport.EncodeFrame(f)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conn.WriteMessage(websocket.BinaryMessage, raw)
}

func (b *Bot) recvFrame() (*transport.Frame, error) {
	for {
		// 先尝试从残余缓冲解帧
		if len(b.recvBuf) > 0 {
			r := bytes.NewReader(b.recvBuf)
			f, err := transport.DecodeFrame(r, 0)
			if err == nil {
				consumed := len(b.recvBuf) - r.Len()
				b.recvBuf = b.recvBuf[consumed:]
				return f, nil
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// 数据不足，继续读 WS
			} else {
				return nil, fmt.Errorf("decode: %w", err)
			}
		}
		// 读一条 WS 消息
		_, data, err := b.conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		b.recvBuf = append(b.recvBuf, data...)
	}
}

// ---- 指标 ----

type Metrics struct {
	ID           int
	PingsSent    int64
	PongsRecv    int64
	MsgsSent     int64
	MsgsRecv     int64
	Errors       int64
	Reconnects   int64
	LatencyAvgMS float64
	LatencyMaxMS float64
}

func (b *Bot) Metrics() Metrics {
	count := b.latencyCount.Load()
	var avg float64
	if count > 0 {
		avg = float64(b.latencySum.Load()) / float64(count) / float64(time.Millisecond)
	}
	return Metrics{
		ID:           b.id,
		PingsSent:    b.pingsSent.Load(),
		PongsRecv:    b.pongsRecv.Load(),
		MsgsSent:     b.msgsSent.Load(),
		MsgsRecv:     b.msgsRecv.Load(),
		Errors:       b.errors.Load(),
		Reconnects:   b.reconnects.Load(),
		LatencyAvgMS: avg,
		LatencyMaxMS: float64(b.latencyMax.Load()) / float64(time.Millisecond),
	}
}
