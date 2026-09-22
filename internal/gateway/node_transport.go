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

// TCPNodeTransport 基于 TCP 的 node-to-node Transport。
type TCPNodeTransport struct {
	localID    string
	listenAddr string

	peers     PeerAddrProvider // 可空：也可通过 AddPeer 手动注册地址
	peerAddrs map[string]string

	mu         sync.RWMutex
	outConns   map[string]*nodeConn // nodeID → 出站连接
	handlers   map[string]func(InnerHeader, *transport.Frame)
	defHandler func(InnerHeader, *transport.Frame) // 默认入站入口（对端身份动态）
	inConns    map[net.Conn]struct{}               // 已接受的入站连接（Close 时清理）

	listener  net.Listener
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewTCPNodeTransport 创建 TCP node-to-node Transport。
// listenAddr 为服务端监听地址（":0" 随机端口）；peers 可空（用 AddPeer 手动注册）。
func NewTCPNodeTransport(localID, listenAddr string, peers PeerAddrProvider) *TCPNodeTransport {
	return &TCPNodeTransport{
		localID:    localID,
		listenAddr: listenAddr,
		peers:      peers,
		peerAddrs:  make(map[string]string),
		outConns:   make(map[string]*nodeConn),
		handlers:   make(map[string]func(InnerHeader, *transport.Frame)),
		inConns:    make(map[net.Conn]struct{}),
		closeCh:    make(chan struct{}),
	}
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
	t.mu.Unlock()
	if ok {
		c.Close()
	}
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

	// 解析对端地址
	addr, ok := t.resolveAddr(nodeID)
	if !ok {
		return nil, fmt.Errorf("peer %q address unknown", nodeID)
	}

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	nc := newNodeConn(nodeID, raw)

	t.mu.Lock()
	// 并发拨号去重
	if existing, ok := t.outConns[nodeID]; ok {
		t.mu.Unlock()
		nc.Close()
		return existing, nil
	}
	t.outConns[nodeID] = nc
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
	id      uint64
	nodeID  string
	raw     net.Conn
	writeCh chan []byte
	done    chan struct{}
	once    sync.Once
	dead    atomic.Bool // 写失败后置位，Write 直接返回错误
}

func newNodeConn(nodeID string, raw net.Conn) *nodeConn {
	if tc, ok := raw.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c := &nodeConn{
		id:      nodeConnSeq.Add(1),
		nodeID:  nodeID,
		raw:     raw,
		writeCh: make(chan []byte, 256),
		done:    make(chan struct{}),
	}
	go c.writeLoop()
	go c.detectClose() // 读协程：对端关闭时立即置 dead，避免写缓冲掩盖断连
	return c
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
		}
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
