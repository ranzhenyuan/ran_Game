package session

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// 会话状态。
type state byte

const (
	stateOnline   state = iota + 1 // 已绑定 Conn
	stateOffline                   // 宽限期内，Conn 已解绑
	stateReleased                  // 已销毁（宽限期超时或被顶号）
)

var (
	// ErrOffline 会话处于宽限离线态：消息照常进缓冲，但无法即时投递。
	ErrOffline = errors.New("session: offline (in grace period)")
	// ErrReleased 会话已销毁。
	ErrReleased = errors.New("session: released")
)

// Session 一个玩家的登录会话。生命周期跟随登录而非连接（§5.1 核心不变式）。
//
// 该类型同时实现 router.Source / router.RoomSource / router.Authenticator，
// 可直接作为 router.Dispatch 的消息来源（阶段 4 接线）。
type Session struct {
	uid string

	// connMu 保护连接绑定信息（§7.5 共享点①）。
	connMu sync.Mutex
	conn   transport.Conn
	codec  byte
	roomID string
	st     state
	token  string

	// 可靠通道：seq 分配 + 环形缓冲。
	seqMu   sync.Mutex
	nextSeq uint32
	ring    *ringBuffer

	// 快照通道：per-msgID latest-wins。
	snapMu sync.Mutex
	snaps  *snapshotCache

	lastActive atomic.Int64 // unix nano
	offlineAt  atomic.Int64 // unix nano，0 表示在线
}

func newSession(uid string, conn transport.Conn, codec byte, replayCap int) *Session {
	s := &Session{
		uid:   uid,
		conn:  conn,
		codec: codec,
		st:    stateOnline,
		ring:  newRingBuffer(replayCap),
		snaps: newSnapshotCache(),
	}
	s.lastActive.Store(time.Now().UnixNano())
	return s
}

func (s *Session) UID() string { return s.uid }

// ---- router 接口实现 ----

// Send 立即下发一帧（RPC 应答/重放帧走此路径），不进任何缓冲。
// 帧上的 seq/flag 原样保留：RPC 应答携带请求 seq，可靠推送携带服务端 seq。
func (s *Session) Send(f *transport.Frame) error {
	s.connMu.Lock()
	conn, st := s.conn, s.st
	s.connMu.Unlock()

	if st == stateReleased {
		return ErrReleased
	}
	if conn == nil || st == stateOffline {
		return ErrOffline
	}
	raw, err := transport.EncodeFrame(f)
	if err != nil {
		return err
	}
	return conn.Push(raw)
}

// Authenticated router.Authenticator：登录后（含宽限离线态）均为已鉴权。
func (s *Session) Authenticated() bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.st != stateReleased
}

// RoomID router.RoomSource：当前所在房间（未在房间为空串）。
func (s *Session) RoomID() string {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.roomID
}

// SetRoomID 由房间模块在进/退房时调用（RoomActor goroutine）。
func (s *Session) SetRoomID(id string) {
	s.connMu.Lock()
	s.roomID = id
	s.connMu.Unlock()
}

// ---- 连接绑定 ----

func (s *Session) currentConn() transport.Conn {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn
}

// bind 绑定新连接（登录或重连），返回旧连接（可能为 nil，由调用方关闭）。
func (s *Session) bind(conn transport.Conn, codec byte) transport.Conn {
	s.connMu.Lock()
	old := s.conn
	s.conn = conn
	s.codec = codec
	s.st = stateOnline
	s.connMu.Unlock()
	s.offlineAt.Store(0)
	s.Touch()
	return old
}

// unbind 连接断开：解绑进入宽限期，PlayerActor 不销毁、缓冲继续累积。
func (s *Session) unbind() transport.Conn {
	s.connMu.Lock()
	old := s.conn
	s.conn = nil
	if s.st == stateOnline {
		s.st = stateOffline
		s.offlineAt.Store(time.Now().UnixNano())
	}
	s.connMu.Unlock()
	return old
}

// unbindIfConn 仅当当前绑定连接 ID 等于 connID 时解绑。
// 读泵退出时使用：断线重连已把会话绑定到新连接，旧读泵不得误标离线。
// 返回（解绑下的旧连接，是否确实解绑）。
func (s *Session) unbindIfConn(connID uint64) (transport.Conn, bool) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil || s.conn.ID() != connID {
		return nil, false
	}
	old := s.conn
	s.conn = nil
	if s.st == stateOnline {
		s.st = stateOffline
		s.offlineAt.Store(time.Now().UnixNano())
	}
	return old, true
}

func (s *Session) markReleased() {
	s.connMu.Lock()
	s.st = stateReleased
	s.conn = nil
	s.connMu.Unlock()
}

func (s *Session) isState(st state) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.st == st
}

// IsOnline 是否绑定了活跃连接。
func (s *Session) IsOnline() bool { return s.isState(stateOnline) }

// IsOffline 是否处于宽限离线态。
func (s *Session) IsOffline() bool { return s.isState(stateOffline) }

func (s *Session) stateVal() state {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.st
}

func (s *Session) connID() uint64 {
	if c := s.currentConn(); c != nil {
		return c.ID()
	}
	return 0
}

func (s *Session) codecType() byte {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.codec
}

// CodecType 会话当前协商的序列化类型（0=JSON，1=PB）。
func (s *Session) CodecType() byte { return s.codecType() }

// ---- token（一次性消费，§11.4）----

func (s *Session) currentToken() string {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.token
}

// Token 当前有效的 reconnectToken（重连成功后已轮换为新值）。
func (s *Session) Token() string { return s.currentToken() }

func (s *Session) setToken(t string) {
	s.connMu.Lock()
	s.token = t
	s.connMu.Unlock()
}

// rotateToken 校验旧 token 并原子更换为新 token；校验失败返回 false。
func (s *Session) rotateToken(old, new string) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.st == stateReleased || s.token != old {
		return false
	}
	s.token = new
	return true
}

// ---- 心跳（§5.2）----

func (s *Session) Touch() {
	s.lastActive.Store(time.Now().UnixNano())
}

func (s *Session) LastActive() time.Time {
	return time.Unix(0, s.lastActive.Load())
}

// Alive 距上次活动是否未超过 keepAlive。
func (s *Session) Alive(keepAlive time.Duration) bool {
	return time.Since(s.LastActive()) <= keepAlive
}

func (s *Session) offlineDuration(now time.Time) time.Duration {
	v := s.offlineAt.Load()
	if v == 0 {
		return 0
	}
	return now.Sub(time.Unix(0, v))
}

// ---- 下行双通道（§5.3）----

// PushReliable 可靠推送：按会话 Codec 编码 → 分配服务端 seq → 入环形缓冲 → 即时下发。
// 宽限离线态下照常缓冲（返回 nil），重连后重放。
func (s *Session) PushReliable(msgID framework.MsgID, msg any) error {
	body, err := protocol.MustGet(s.codecType()).Marshal(msg)
	if err != nil {
		return err
	}
	return s.pushEncoded(uint32(msgID), body, false)
}

// PushSnapshot 快照推送：flag bit6 标记，仅按 msgID 缓存最新一帧。
func (s *Session) PushSnapshot(msgID framework.MsgID, msg any) error {
	body, err := protocol.MustGet(s.codecType()).Marshal(msg)
	if err != nil {
		return err
	}
	return s.pushEncoded(uint32(msgID), body, true)
}

// PushEncoded 广播高效路径：RoomActor 只编码一次 body，逐成员调用本方法
// （seq/帧头仍按会话独立分配）。snapshot=true 走快照通道。
func (s *Session) PushEncoded(msgID uint32, body []byte, snapshot bool) error {
	return s.pushEncoded(msgID, body, snapshot)
}

func (s *Session) pushEncoded(msgID uint32, body []byte, snapshot bool) error {
	f := &transport.Frame{
		Ver:   transport.ProtocolVer,
		MsgID: msgID,
		Body:  body,
	}

	if snapshot {
		f.Flag = s.codecType() | transport.FlagSnapshot
		s.snapMu.Lock()
		s.snaps.put(f)
		s.snapMu.Unlock()
	} else {
		f.Flag = s.codecType()
		s.seqMu.Lock()
		s.nextSeq++
		f.Seq = s.nextSeq
		s.ring.push(f)
		s.seqMu.Unlock()
	}

	// 即时投递失败（离线/慢消费者）不影响缓冲语义。
	if err := s.Send(f); err != nil && !errors.Is(err, ErrOffline) {
		return err
	}
	return nil
}

// PushRaw 原样下发外部已构造完整的帧（不缓冲、不分配 seq）。
func (s *Session) PushRaw(f *transport.Frame) error {
	return s.Send(f)
}

// LastReliableSeq 当前可靠通道已分配的最大 seq。
func (s *Session) LastReliableSeq() uint32 {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	return s.nextSeq
}

// Replay 返回重连重放数据：
//   - reliable：可靠通道 seq > lastSeq 的差额帧（升序）；
//   - overflow：缓冲已滚动，差额无法补齐，客户端须走 Resync；
//   - snapshots：快照通道最新帧（PullSnapshot 语义，传 nil 取全部）。
func (s *Session) Replay(lastSeq uint32, snapshotMsgIDs []uint32) (reliable []*transport.Frame, snapshots []*transport.Frame, overflow bool) {
	s.seqMu.Lock()
	reliable, overflow = s.ring.since(lastSeq)
	s.seqMu.Unlock()

	s.snapMu.Lock()
	snapshots = s.snaps.all(snapshotMsgIDs)
	s.snapMu.Unlock()
	return
}

// Snapshots 仅取快照通道最新帧（msgIDs 为空取全部），用于 PullSnapshotReq。
func (s *Session) Snapshots(msgIDs []uint32) []*transport.Frame {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	return s.snaps.all(msgIDs)
}
