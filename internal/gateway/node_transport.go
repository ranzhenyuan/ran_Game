// node_transport.go 真实 TCP node-to-node Transport（架构文档 §12.3）。
//
// 与 MemNodeTransport（进程内 stub）并列，实现同一 NodeTransport 接口。
// 复用 gateway.InnerHeader 编解码，通过 4 字节长度前缀解决 TCP 粘包，
// 写聚合机制与 transport.tcpConn 一致（首帧触发攒批窗口）。
//
// 链路字节流：
//
//	[4B payloadLen][EncodeInner 输出 = innerHeader 文本前缀 + 原始帧字节流]
//
// 每个 envelope 可在 TCP 层与其他 envelope 聚合发送，接收方按长度切分。
package gateway

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/transport"
)

// PeerAddrProvider 提供 nodeID → 网络地址映射（NodeRegistry 天然满足）。
type PeerAddrProvider interface {
	Get(nodeID string) (NodeInfoLite, bool)
}

// NodeInfoLite 节点信息精简体（只取地址，避免 gateway 依赖 cluster.NodeInfo 全字段）。
type NodeInfoLite struct {
	Addr string
}

// DefaultHandlerSetter 由支持"默认入站 handler"的 Transport 实现。
//
// TCP 入站帧的 inner.GatewayID 是发送方 nodeID（Forward 时加盖本端 localID），
// 而对端节点（Gateway 副本/Logic 分片）动态扩缩，接收方无法预知全部 nodeID
// 逐个 AddPeer，故需要一个不依赖发送方身份的默认入口。
type DefaultHandlerSetter interface {
	SetDefaultHandler(handler func(InnerHeader, *transport.Frame))
}

// 内部链路长连接参数（阶段二：保活心跳 + 后台探活重连）。
const (
	defaultKeepaliveInterval = 30 * time.Second // 应用层 idle 心跳周期：超过该间隔无业务写则发心跳
	defaultReconnCheck       = 2 * time.Second  // 后台探活/重连扫描周期
	reconnBackoff            = 1 * time.Second  // 重连失败退避，避免对端未起时热重试
	tcpKeepalivePeriod       = 30 * time.Second // 内核 TCP keepalive 探测周期
)

// TCPNodeTransport 基于 TCP 的 node-to-node Transport。
type TCPNodeTransport struct {
	localID    string
	listenAddr string

	peers     PeerAddrProvider // 可空：也可通过 AddPeer 手动注册地址
	peerAddrs map[string]string

	// 长连接参数（可由测试注入小值加速用例；0 取默认）。
	keepaliveInterval time.Duration
	reconnCheck       time.Duration

	mu          sync.RWMutex
	outConns    map[string]*nodeConn // nodeID → 出站连接
	handlers    map[string]func(InnerHeader, *transport.Frame)
	defHandler  func(InnerHeader, *transport.Frame) // 默认入站入口（对端身份动态）
	inConns     map[net.Conn]struct{}               // 已接受的入站连接（Close 时清理）
	activePeers map[string]struct{}                 // 曾成功建立出站连接的 nodeID，后台重连扫描集
	retryAt     map[string]time.Time                // nodeID → 下次允许重连的时间（退避）

	listener  net.Listener
	closeCh   chan struct{}
	closeOnce sync.Once
}

// TCPTransportOption 配置 NewTCPNodeTransport 的可选参数。
type TCPTransportOption func(*TCPNodeTransport)

// WithKeepaliveInterval 设置应用层 idle 心跳周期（<=0 取默认 30s）。
func WithKeepaliveInterval(d time.Duration) TCPTransportOption {
	return func(t *TCPNodeTransport) {
		if d > 0 {
			t.keepaliveInterval = d
		}
	}
}

// WithReconnCheck 设置后台探活/重连扫描周期（<=0 取默认 2s）。
func WithReconnCheck(d time.Duration) TCPTransportOption {
	return func(t *TCPNodeTransport) {
		if d > 0 {
			t.reconnCheck = d
		}
	}
}

// NewTCPNodeTransport 创建 TCP node-to-node Transport。
// listenAddr 为服务端监听地址（":0" 随机端口）；peers 可空（用 AddPeer 手动注册）。
func NewTCPNodeTransport(localID, listenAddr string, peers PeerAddrProvider, opts ...TCPTransportOption) *TCPNodeTransport {
	t := &TCPNodeTransport{
		localID:           localID,
		listenAddr:        listenAddr,
		peers:             peers,
		peerAddrs:         make(map[string]string),
		outConns:          make(map[string]*nodeConn),
		handlers:          make(map[string]func(InnerHeader, *transport.Frame)),
		inConns:           make(map[net.Conn]struct{}),
		activePeers:       make(map[string]struct{}),
		retryAt:           make(map[string]time.Time),
		keepaliveInterval: defaultKeepaliveInterval,
		reconnCheck:       defaultReconnCheck,
		closeCh:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}
	go t.reconnectLoop() // 后台探活：连接死亡/缺失自动重拨（带退避）
	return t
}

// AddPeer 注册对端节点地址与入站回调。
// 同一 nodeID 重复调用更新地址与回调。
func (t *TCPNodeTransport) AddPeer(nodeID string, handler func(InnerHeader, *transport.Frame)) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[nodeID] = handler
	return nil
}

// SetDefaultHandler 设置默认入站回调：具名 handler 未命中时兜底。
func (t *TCPNodeTransport) SetDefaultHandler(handler func(InnerHeader, *transport.Frame)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.defHandler = handler
}

// AddPeerAddr 手动注册 nodeID → 地址映射（不依赖 PeerAddrProvider 时使用）。
func (t *TCPNodeTransport) AddPeerAddr(nodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peerAddrs[nodeID] = addr
}

// RemovePeer 移除对端（关闭出站连接、注销回调）。
func (t *TCPNodeTransport) RemovePeer(nodeID string) {
	t.mu.Lock()
	c, ok := t.outConns[nodeID]
	delete(t.outConns, nodeID)
	delete(t.handlers, nodeID)
	delete(t.peerAddrs, nodeID)
	delete(t.activePeers, nodeID) // 停止后台重连扫描
	delete(t.retryAt, nodeID)
	t.mu.Unlock()
	if ok {
		c.Close()
	}
}

// Warmup 预拨号建立到 nodeID 的出站长连接（连接预热）。
// 节点上线时调用可消除首帧拨号延迟；幂等，已有活连接直接复用。
// 成功后该 nodeID 纳入 activePeers，由后台 reconnectLoop 持续保活重连。
func (t *TCPNodeTransport) Warmup(nodeID string) error {
	_, err := t.dialPeer(nodeID)
	if err != nil {
		return fmt.Errorf("gateway: warmup peer %q: %w", nodeID, err)
	}
	return nil
}

// OutboundAlive 返回到 nodeID 的出站连接当前是否存活（测试/可观测用，不加锁外可见）。
func (t *TCPNodeTransport) OutboundAlive(nodeID string) bool {
	t.mu.RLock()
	c := t.outConns[nodeID]
	t.mu.RUnlock()
	return c != nil && !c.dead.Load()
}

// Forward 把帧转发给目标节点（懒拨号 + 连接复用 + 失败重试一次）。
func (t *TCPNodeTransport) Forward(nodeID string, inner InnerHeader, f *transport.Frame) error {
	inner.GatewayID = t.localID // 标记来源网关
	raw := EncodeInner(inner, f)

	for attempt := 0; attempt < 2; attempt++ {
		c, err := t.getOrDial(nodeID)
		if err != nil {
			return fmt.Errorf("gateway: dial peer %q: %w", nodeID, err)
		}
		if err := c.Write(raw); err == nil {
			return nil
		}
		// 写失败：清掉坏连接，下次（或重试）重拨
		t.mu.Lock()
		if cur, ok := t.outConns[nodeID]; ok && cur == c {
			delete(t.outConns, nodeID)
		}
		t.mu.Unlock()
		c.Close()
	}
	return fmt.Errorf("gateway: forward to %q: connection failed", nodeID)
}

// HandleForward 由读循环在收到对端帧时调用（接口要求；TCP 实现内部分发到 handler）。
func (t *TCPNodeTransport) HandleForward(inner InnerHeader, f *transport.Frame) {
	t.mu.RLock()
	h := t.handlers[inner.GatewayID]
	if h == nil {
		h = t.defHandler // 对端 nodeID 未预注册（动态扩缩）走默认入口
	}
	t.mu.RUnlock()
	if h != nil {
		h(inner, f)
	}
}

// Listen 启动服务端监听；非阻塞，返回监听地址。
func (t *TCPNodeTransport) Listen() (string, error) {
	ln, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return "", err
	}
	t.listener = ln
	go t.acceptLoop()
	return ln.Addr().String(), nil
}

// Close 关闭监听与所有连接。
func (t *TCPNodeTransport) Close() error {
	var firstErr error
	t.closeOnce.Do(func() {
		close(t.closeCh)
		if t.listener != nil {
			firstErr = t.listener.Close()
		}
		t.mu.Lock()
		for _, c := range t.outConns {
			c.Close()
		}
		t.outConns = make(map[string]*nodeConn)
		for c := range t.inConns {
			_ = c.Close()
			delete(t.inConns, c)
		}
		t.mu.Unlock()
	})
	return firstErr
}

// Addr 返回实际监听地址（Listen 后可用）。
func (t *TCPNodeTransport) Addr() string {
	if t.listener == nil {
		return ""
	}
	return t.listener.Addr().String()
}

// ---------------- 内部 ----------------

func (t *TCPNodeTransport) getOrDial(nodeID string) (*nodeConn, error) {
	t.mu.RLock()
	c, ok := t.outConns[nodeID]
	t.mu.RUnlock()
	if ok {
		return c, nil
	}
	return t.dialPeer(nodeID)
}

// dialPeer 拨号建立到 nodeID 的出站长连接（含并发去重）。
// 成功：存入 outConns、记入 activePeers（纳入后台重连扫描）、清退避。
// 失败：记录 retryAt，后台 reconcile 到期后再试。
func (t *TCPNodeTransport) dialPeer(nodeID string) (*nodeConn, error) {
	addr, ok := t.resolveAddr(nodeID)
	if !ok {
		return nil, fmt.Errorf("peer %q address unknown", nodeID)
	}
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.mu.Lock()
		t.retryAt[nodeID] = time.Now().Add(reconnBackoff)
		t.mu.Unlock()
		return nil, err
	}
	nc := newNodeConn(nodeID, raw, t.keepaliveInterval)

	t.mu.Lock()
	// 并发拨号去重：已有则用已有、关掉本次新建
	if existing, ok := t.outConns[nodeID]; ok {
		t.mu.Unlock()
		nc.Close()
		return existing, nil
	}
	t.outConns[nodeID] = nc
	t.activePeers[nodeID] = struct{}{}
	delete(t.retryAt, nodeID)
	t.mu.Unlock()
	return nc, nil
}

func (t *TCPNodeTransport) resolveAddr(nodeID string) (string, bool) {
	t.mu.RLock()
	addr, ok := t.peerAddrs[nodeID]
	t.mu.RUnlock()
	if ok {
		return addr, true
	}
	if t.peers != nil {
		if info, ok := t.peers.Get(nodeID); ok {
			return info.Addr, true
		}
	}
	return "", false
}

// reconnectLoop 后台探活：周期扫描 activePeers，连接死亡/缺失则自动重拨（带退避）。
// 使断连恢复不依赖下一次业务 Forward，降低首帧延迟与丢帧率。
func (t *TCPNodeTransport) reconnectLoop() {
	interval := t.reconnCheck
	if interval <= 0 {
		interval = defaultReconnCheck
	}
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-t.closeCh:
			return
		case <-tk.C:
			t.reconcilePeers()
		}
	}
}

// reconcilePeers 重连一轮：对退避到期且连接缺失/已死的 active peer 重新拨号。
func (t *TCPNodeTransport) reconcilePeers() {
	t.mu.Lock()
	now := time.Now()
	var todo []string
	for id := range t.activePeers {
		if next, ok := t.retryAt[id]; ok && now.Before(next) {
			continue // 退避期内，跳过
		}
		c := t.outConns[id]
		if c == nil || c.dead.Load() {
			if c != nil {
				delete(t.outConns, id)
			}
			todo = append(todo, id)
		}
	}
	t.mu.Unlock()

	for _, id := range todo {
		// 并发 Forward 可能已重建活连接，确认后再重拨，避免无谓拨号。
		t.mu.RLock()
		c := t.outConns[id]
		t.mu.RUnlock()
		if c != nil && !c.dead.Load() {
			continue
		}
		if c != nil {
			c.Close() // 释放旧 fd（dead 连接的 Close 由 detectClose 触发，此处幂等）
		}
		_, _ = t.dialPeer(id) // 失败时 dialPeer 内部记录 retryAt 退避
	}
}

func (t *TCPNodeTransport) acceptLoop() {
	for {
		raw, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.closeCh:
				return
			default:
			}
			continue
		}
		go t.serveConn(raw)
	}
}

// serveConn 处理单条入站连接：读循环 → 解码 inner envelope → 分发 handler。
func (t *TCPNodeTransport) serveConn(raw net.Conn) {
	setTCPKeepalive(raw)
	// 注册到 inConns 以便 Close 时统一清理
	t.mu.Lock()
	t.inConns[raw] = struct{}{}
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		delete(t.inConns, raw)
		t.mu.Unlock()
		raw.Close()
	}()

	br := bufio.NewReader(raw)
	for {
		select {
		case <-t.closeCh:
			return
		default:
		}

		inner, f, err := readEnvelope(br)
		if err != nil {
			return
		}
		// 链路保活心跳：仅维持连接活性，不进业务 handler。
		if inner.MsgType == MsgKeepalive {
			continue
		}
		t.HandleForward(inner, f)
	}
}

// readEnvelope 从流读取一个 inner envelope（4B len + EncodeInner 输出）。
func readEnvelope(r *bufio.Reader) (InnerHeader, *transport.Frame, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return InnerHeader{}, nil, err
	}
	payloadLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if payloadLen <= 0 {
		return InnerHeader{}, nil, fmt.Errorf("gateway: invalid envelope length %d", payloadLen)
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return InnerHeader{}, nil, err
	}
	return DecodeInner(payload)
}

// ---------------- nodeConn：出站连接 + 写聚合 ----------------

var nodeConnSeq atomic.Uint64

// nodeConn 单条出站连接，带写聚合（与 transport.tcpConn 同机制）。
type nodeConn struct {
	id            uint64
	nodeID        string
	raw           net.Conn
	writeCh       chan []byte
	done          chan struct{}
	once          sync.Once
	dead          atomic.Bool   // 写失败/对端关闭后置位，Write 直接返回错误
	lastWrite     atomic.Int64  // 最近一次真实写出的 unix nano（心跳 idle 节流用）
	keepaliveSent atomic.Uint32 // 已发送心跳数（测试可观测）
}

func newNodeConn(nodeID string, raw net.Conn, keepaliveInterval time.Duration) *nodeConn {
	setTCPKeepalive(raw)
	if tc, ok := raw.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c := &nodeConn{
		id:        nodeConnSeq.Add(1),
		nodeID:    nodeID,
		raw:       raw,
		writeCh:   make(chan []byte, 256),
		done:      make(chan struct{}),
		lastWrite: atomic.Int64{},
	}
	c.lastWrite.Store(time.Now().UnixNano()) // 初始视为刚写过，首个周期内不发心跳
	go c.writeLoop()
	go c.detectClose()                    // 读协程：对端关闭时立即置 dead，避免写缓冲掩盖断连
	go c.keepaliveLoop(keepaliveInterval) // idle 心跳：保活中间设备 + 及时发现死链
	return c
}

// setTCPKeepalive 开启内核 TCP keepalive，探测半开死链（单向连接读不到对端数据时的唯一主动探活手段）。
func setTCPKeepalive(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(tcpKeepalivePeriod)
	}
}

// detectClose 持续读连接（丢弃数据），对端关闭/出错时标记 dead。
// TCP 半关闭下，写可能进内核缓冲成功，靠读 EOF 才能及时发现断连。
func (c *nodeConn) detectClose() {
	buf := make([]byte, 256)
	for {
		if _, err := c.raw.Read(buf); err != nil {
			c.dead.Store(true)
			c.Close()
			return
		}
	}
}

// keepaliveLoop idle 心跳：每 interval 检查，若该周期内无业务写出则发一帧心跳。
// 作用：① 防止 LB/NAT 因 idle 回收连接；② 写失败使 writeLoop 及时标 dead，触发后台重连。
func (c *nodeConn) keepaliveLoop(interval time.Duration) {
	if interval <= 0 {
		interval = defaultKeepaliveInterval
	}
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-tk.C:
			if time.Since(time.Unix(0, c.lastWrite.Load())) < interval {
				continue // 近期有业务流量，无需心跳
			}
			c.sendKeepalive()
		}
	}
}

// sendKeepalive 构造并投递一帧链路心跳（非阻塞，队列满则跳过）。
func (c *nodeConn) sendKeepalive() {
	raw := EncodeInner(InnerHeader{MsgType: MsgKeepalive}, &transport.Frame{})
	buf := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(raw)))
	copy(buf[4:], raw)
	select {
	case c.writeCh <- buf:
		c.keepaliveSent.Add(1)
	default: // 写聚合队列拥塞，跳过本次心跳
	}
}

// Write 投递一个 envelope（非阻塞，满则返回 ErrSlowConsumer）。
func (c *nodeConn) Write(data []byte) error {
	if c.dead.Load() {
		return net.ErrClosed
	}
	// 4 字节长度前缀 + envelope 负载
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)
	select {
	case c.writeCh <- buf:
		return nil
	case <-c.done:
		return net.ErrClosed
	default:
		return transport.ErrSlowConsumer
	}
}

func (c *nodeConn) Close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.raw.Close()
	})
}

// writeLoop 写聚合：首帧触发 2ms 攒批窗口，到期或达 4KB 上限一次 Write。
func (c *nodeConn) writeLoop() {
	buf := make([]byte, 0, 4*1024)
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		if _, err := c.raw.Write(buf); err != nil {
			c.dead.Store(true)
			c.Close()
			buf = buf[:0]
			return
		}
		c.lastWrite.Store(time.Now().UnixNano()) // 记录真实写出时刻，供心跳 idle 节流
		buf = buf[:0]
	}

outer:
	for {
		select {
		case <-c.done:
			return
		case first, ok := <-c.writeCh:
			if !ok {
				return
			}
			buf = append(buf, first...)
			timer.Reset(2 * time.Millisecond)

			for {
				select {
				case more, ok := <-c.writeCh:
					if !ok {
						flush()
						return
					}
					buf = append(buf, more...)
					if len(buf) >= 4*1024 {
						flush()
						continue outer
					}
				case <-timer.C:
					flush()
					continue outer
				case <-c.done:
					return
				}
			}
		}
	}
}
