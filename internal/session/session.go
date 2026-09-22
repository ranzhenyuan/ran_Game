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

// Session 玩家业务会话（Logic 侧，§12.2 中 PlayerSession）。
//
// 生命周期跟随登录而非连接（§5.1 核心不变式）。连接级状态（transport.Conn、token、
// 心跳）下沉到 ConnSession；Session 仅持有业务态：uid、codec、roomID、profile、
// 下行双通道（可靠环形缓冲 + 快照 latest-wins）、状态机。
//
// 该类型同时实现 router.Source / router.RoomSource / router.Authenticator，
// 可直接作为 router.Dispatch 的消息来源。
type Session struct {
	uid string

	// conn 连接级会话（稳定身份，跨重连存活）；由 Manager 在登录时注入。
	// Session 不再直接持有 transport.Conn，投递经 conn.Send 完成。
	conn *ConnSession
	// codec 当前协商序列化类型（0=JSON，1=PB），bind 时更新，用于下行编码。
	codec byte

	// connMu 保护业务态共享字段（§7.5 共享点①）。
	connMu sync.Mutex
	roomID string
	st     state

	// 可靠通道：seq 分配 + 环形缓冲。
	seqMu   sync.Mutex
	nextSeq uint32
	ring    *ringBuffer

	// 快照通道：per-msgID latest-wins。
	snapMu sync.Mutex
	snaps  *snapshotCache

	offlineAt atomic.Int64 // unix nano，0 表示在线

	// profile 玩家档案快照（§10.4.1）；由 PlayerActor OnSessionStart 加载，
	// 通过 atomic.Pointer 安全暴露给 RoomActor goroutine（只读）。
	profile atomic.Pointer[framework.PlayerProfile]
}

// newSession 登录创建会话；conn 被包装为 ConnSession（稳定身份）。
func newSession(uid string, conn transport.Conn, codec byte, replayCap int) *Session {
	s := &Session{
		uid:   uid,
		conn:  NewConnSession(conn),
		codec: codec,
		st:    stateOnline,
		ring:  newRingBuffer(replayCap),
		snaps: newSnapshotCache(),
	}
	s.conn.Touch()
	return s
}

func (s *Session) UID() string { return s.uid }

// Profile 返回玩家档案快照（只读，§10.4.1）；未加载时返回 nil。
func (s *Session) Profile() *framework.PlayerProfile {
	return s.profile.Load()
}

// SetProfile 设置玩家档案快照（PlayerActor OnSessionStart 调用，§10.4.4）。
func (s *Session) SetProfile(p *framework.PlayerProfile) {
	s.profile.Store(p)
}

// ---- router 接口实现 ----

// Send 立即下发一帧（RPC 应答/重放帧走此路径），不进任何缓冲。
// 帧上的 seq/flag 原样保留：RPC 应答携带请求 seq，可靠推送携带服务端 seq。
func (s *Session) Send(f *transport.Frame) error {
	s.connMu.Lock()
	st := s.st
	s.connMu.Unlock()

	if st == stateReleased {
		return ErrReleased
	}
	if st == stateOffline {
		return ErrOffline
	}
	return s.conn.Send(f)
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

// ---- 连接绑定（委托 ConnSession）----

func (s *Session) currentConn() transport.Conn {
	return s.conn.Conn()
}

// bind 绑定新连接（登录或重连），返回旧连接（可能为 nil，由调用方关闭）。
// ConnSession 稳定不变，仅其内部 transport.Conn 被替换。
func (s *Session) bind(conn transport.Conn, codec byte) transport.Conn {
	old := s.conn.Bind(conn)
	s.connMu.Lock()
	s.codec = codec
	s.st = stateOnline
	s.connMu.Unlock()
	s.offlineAt.Store(0)
	s.conn.Touch()
	return old
}

// unbind 连接断开：解绑进入宽限期，PlayerActor 不销毁、缓冲继续累积。
func (s *Session) unbind() transport.Conn {
	old := s.conn.Unbind()
	s.connMu.Lock()
	if s.st == stateOnline {
		s.st = stateOffline
		s.offlineAt.Store(time.Now().UnixNano())
	}
	s.connMu.Unlock()
	return old
}

// unbindIfConn 仅当当前绑定连接 ID 等于 connID 时解绑。
// 读泵退出时使用：断线重连已把会话绑定到新连接，旧读泵不得误标离线。
// 检查与解绑在 ConnSession 内原子完成，避免与重连 Bind 竞态。
// 返回（解绑下的旧连接，是否确实解绑）。
func (s *Session) unbindIfConn(connID uint64) (transport.Conn, bool) {
	old, ok := s.conn.UnbindIfConn(connID)
	if !ok {
		return nil, false
	}
	s.connMu.Lock()
	if s.st == stateOnline {
		s.st = stateOffline
		s.offlineAt.Store(time.Now().UnixNano())
	}
	s.connMu.Unlock()
	return old, true
}

func (s *Session) markReleased() {
	s.connMu.Lock()
	s.st = stateReleased
	s.connMu.Unlock()
	// 不关闭 conn：由 Manager 在 destroy 时统一关闭。
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
	return s.conn.ID()
}

// CodecType 会话当前协商的序列化类型（0=JSON，1=PB）。
func (s *Session) CodecType() byte {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.codec
}

func (s *Session) codecType() byte {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.codec
}

// ---- token（委托 ConnSession）----

func (s *Session) Token() string                    { return s.conn.Token() }
func (s *Session) setToken(t string)                { s.conn.SetToken(t) }
func (s *Session) rotateToken(old, new string) bool { return s.conn.RotateToken(old, new) }

// ---- 心跳（委托 ConnSession）----

func (s *Session) Touch()                             { s.conn.Touch() }
func (s *Session) LastActive() time.Time              { return s.conn.LastActive() }
func (s *Session) Alive(keepAlive time.Duration) bool { return s.conn.Alive(keepAlive) }

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
