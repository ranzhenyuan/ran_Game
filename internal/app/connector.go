// Package app 负责框架组件装配与连接生命周期编排。
// Connector 把接入层连接接到会话管理与路由器：登录/重连握手 → 心跳直回 → 消息分发，
// 断连时安全地把会话置入宽限期（架构文档 §5/§6 衔接层）。
package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/router"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// Connector 每条连接一个 ServeConn 协程（读泵）。
type Connector struct {
	sessions     *session.Manager
	router       *router.Router
	logger       *slog.Logger
	maxLoginBody int
	heartbeatMS  int

	// OnSessionStart 登录/重连成功回调（阶段 5：Spawn PlayerActor 并绑定注册表）。
	OnSessionStart func(sess *session.Session)
}

// NewConnector 创建连接器。
func NewConnector(sessions *session.Manager, r *router.Router, logger *slog.Logger, maxLoginBody, heartbeatMS int) *Connector {
	if maxLoginBody <= 0 {
		maxLoginBody = 4096
	}
	return &Connector{
		sessions:     sessions,
		router:       r,
		logger:       logger,
		maxLoginBody: maxLoginBody,
		heartbeatMS:  heartbeatMS,
	}
}

// ServeConn 读泵全流程：握手 → 消息循环；退出时仅在连接仍属本读泵时标记离线。
func (c *Connector) ServeConn(ctx context.Context, conn transport.Conn) {
	sess, err := c.handshake(ctx, conn)
	if err != nil || sess == nil {
		_ = conn.Close("handshake failed or closed")
		return
	}
	if c.OnSessionStart != nil {
		c.OnSessionStart(sess)
	}

	// 退出时安全解绑：若会话已重连到新连接，本调用为 no-op。
	defer c.sessions.MarkOfflineConn(sess, conn.ID())

	c.pump(ctx, conn, sess)
}

// handshake 只接受 login/reconnect（及 ping），成功返回会话。
// 允许在超时前重试（解码失败/错误 token 回错误帧后继续读下一帧）。
func (c *Connector) handshake(ctx context.Context, conn transport.Conn) (*session.Session, error) {
	for {
		f, err := conn.Read(ctx)
		if err != nil {
			return nil, err
		}

		switch framework.MsgID(f.MsgID) {
		case framework.MsgPing:
			c.replyPong(conn, f)
			continue

		case framework.MsgLogin:
			sess, err := c.handleLogin(conn, f)
			if err != nil {
				c.writeError(conn, f, toErrCode(err))
				continue
			}
			return sess, nil

		case framework.MsgReconnect:
			sess, err := c.handleReconnect(conn, f)
			if err != nil {
				c.writeError(conn, f, toErrCode(err))
				continue
			}
			return sess, nil

		default:
			// 未握手前只允许三类消息（§6.3 Auth）。
			c.writeError(conn, f, framework.NewErr(framework.ErrUnauthorized, "login required"))
		}
	}
}

func (c *Connector) handleLogin(conn transport.Conn, f *transport.Frame) (*session.Session, error) {
	if len(f.Body) > c.maxLoginBody {
		return nil, framework.NewErr(framework.ErrFrameTooLong, "")
	}
	codec, err := codecOf(f)
	if err != nil {
		return nil, err
	}
	var req session.LoginReq
	if err := codec.Unmarshal(f.Body, &req); err != nil {
		return nil, framework.NewErr(framework.ErrDecode, err.Error())
	}
	if req.UID == "" {
		return nil, framework.NewErr(framework.ErrDecode, "uid required")
	}

	sess, token := c.sessions.Create(req.UID, conn, codec.Type())

	resp := session.LoginResp{
		ReconnectToken: token,
		HeartbeatMS:    c.heartbeatMS,
		SessionID:      req.UID,
	}
	frame, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgLogin, f.Seq, resp)
	if err != nil {
		return nil, framework.NewErr(framework.ErrInternal, err.Error())
	}
	if err := write(conn, frame); err != nil {
		return nil, framework.NewErr(framework.ErrInternal, err.Error())
	}
	return sess, nil
}

func (c *Connector) handleReconnect(conn transport.Conn, f *transport.Frame) (*session.Session, error) {
	if len(f.Body) > c.maxLoginBody {
		return nil, framework.NewErr(framework.ErrFrameTooLong, "")
	}
	codec, err := codecOf(f)
	if err != nil {
		return nil, err
	}
	var req session.ReconnectReq
	if err := codec.Unmarshal(f.Body, &req); err != nil {
		return nil, framework.NewErr(framework.ErrDecode, err.Error())
	}

	sess, err := c.sessions.Reconnect(req.ReconnectToken, conn, codec.Type())
	if err != nil {
		// token 无效/已消费 → 2002，客户端须改走完整登录。
		return nil, framework.NewErr(framework.ErrTokenInvalid, "")
	}

	// 取重放数据：可靠差额 + 全部最新快照。
	reliable, snapshots, overflow := sess.Replay(req.LastReliableSeq, nil)

	frame, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgReconnect, f.Seq, session.ReconnectResp{
		NewReconnectToken: sess.Token(),
		ResyncRequired:    overflow,
	})
	if err != nil {
		return nil, framework.NewErr(framework.ErrInternal, err.Error())
	}
	if err := write(conn, frame); err != nil {
		return nil, framework.NewErr(framework.ErrInternal, err.Error())
	}

	// 握手成功后按原始帧顺序推送：可靠差额（溢出则不推，等客户端 Resync）→ 快照。
	if !overflow {
		for _, rf := range reliable {
			if write(conn, rf) != nil {
				break
			}
		}
	}
	for _, sf := range snapshots {
		if write(conn, sf) != nil {
			break
		}
	}
	return sess, nil
}

// pump 登录后的消息循环：系统消息就地处理，业务消息交路由器（在 Actor 内执行）。
func (c *Connector) pump(ctx context.Context, conn transport.Conn, sess *session.Session) {
	for {
		f, err := conn.Read(ctx)
		if err != nil {
			if !errors.Is(err, transport.ErrSlowConsumer) && c.logger != nil {
				c.logger.Debug("conn read end", "uid", sess.UID(), "conn", conn.ID(), "err", err.Error())
			}
			return
		}
		sess.Touch()

		switch framework.MsgID(f.MsgID) {
		case framework.MsgPing:
			// 心跳在读泵直接应答，不进 Actor 邮箱（§5.2，防业务繁忙饿死心跳）。
			c.replyPong(conn, f)

		case framework.MsgPullSnap:
			c.handlePullSnapshots(conn, sess, f)

		case framework.MsgLogin, framework.MsgReconnect:
			c.writeError(conn, f, framework.NewErr(framework.ErrUnknownMsg, "already authenticated"))

		default:
			c.router.Dispatch(sess, f)
		}
	}
}

func (c *Connector) handlePullSnapshots(conn transport.Conn, sess *session.Session, f *transport.Frame) {
	codec, err := codecOf(f)
	if err != nil {
		c.writeError(conn, f, framework.NewErr(framework.ErrDecode, "codec not supported"))
		return
	}
	var req session.PullSnapshotReq
	if len(f.Body) > 0 {
		if err := codec.Unmarshal(f.Body, &req); err != nil {
			c.writeError(conn, f, framework.NewErr(framework.ErrDecode, err.Error()))
			return
		}
	}

	// 先回 PullSnap 的 OK 响应，再按原始帧逐帧推送最新快照（与重连同构，避免二次编码）。
	ok, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgPullSnap, f.Seq, struct{}{})
	if err == nil {
		_ = write(conn, ok)
	}
	for _, sf := range sess.Snapshots(req.MsgIDs) {
		if write(conn, sf) != nil {
			return
		}
	}
}

func (c *Connector) replyPong(conn transport.Conn, f *transport.Frame) {
	codec, err := codecOf(f)
	if err != nil {
		return
	}
	var p session.Ping
	_ = codec.Unmarshal(f.Body, &p)
	frame, err := protocol.NewOKFrameFor(codec.Type(), framework.MsgPong, f.Seq, session.Pong{
		ClientTS: p.ClientTS,
		ServerTS: c.now().UnixMilli(),
	})
	if err == nil {
		_ = write(conn, frame)
	}
}

func (c *Connector) writeError(conn transport.Conn, f *transport.Frame, ec *framework.ErrCode) {
	frame, err := protocol.NewErrorFrameFor(f.CodecType(), framework.MsgID(f.MsgID), f.Seq, ec.Code, ec.Msg, nil)
	if err != nil {
		return
	}
	_ = write(conn, frame)
}

// write 编码帧并投递到连接写 channel。
func write(conn transport.Conn, f *transport.Frame) error {
	raw, err := transport.EncodeFrame(f)
	if err != nil {
		return err
	}
	return conn.Push(raw)
}

func codecOf(f *transport.Frame) (protocol.Codec, error) {
	c, err := protocol.Get(f.CodecType())
	if err != nil {
		return nil, framework.NewErr(framework.ErrUnknownMsg, "codec not supported")
	}
	return c, nil
}

func (c *Connector) now() time.Time { return time.Now() }

// toErrCode 把任意 error 收敛为 *framework.ErrCode（非框架错误映射 4001）。
func toErrCode(err error) *framework.ErrCode {
	var ec *framework.ErrCode
	if errors.As(err, &ec) {
		return ec
	}
	return framework.NewErr(framework.ErrInternal, err.Error())
}
