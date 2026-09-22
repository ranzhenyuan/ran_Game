// Package gateway 是演进态网关装配（架构文档 §12）。
//
// 仅在 server.role = gateway 时启用。核心原则：Gateway 是"哑管道"——
// 只解析帧头（len/msgID/seq/uid），永不反序列化玩法消息。
//
// 演进态骨架（§49 不做真实多进程部署）：
//   - 一级路由表 RouteTable：uid → logicNodeID（内存实现，单进程集成测试用）
//   - 内部 Transport 接口：node-to-node 帧转发（进程内直连 stub，不真跨进程）
//   - Gateway 装配函数：替换 internal/app.Connector 的下游为内部链路而非本进程 Actor
package gateway

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rangame/server/internal/cluster"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// ---------------- 一级路由表 ----------------

// RouteTable 一级路由表：uid → logicNodeID（§12.4）。
//
//	粘性不迁移（§13.6）：存量映射在会话存活期内永不 rehash；
//	新会话分配：候选 Active 节点，权重 = 最少 active_rooms（由 NodePicker 提供）。
type RouteTable interface {
	// Lookup 查询 uid 的归属节点；未找到返回 ("", ErrNotRouted)。
	Lookup(uid string) (nodeID string, err error)
	// Bind 写入映射（登录/重连时由 Logic 调用，§12.4）。
	Bind(uid, nodeID string) error
	// Unbind 清除映射（会话释放时）。
	Unbind(uid string)
	// Invalidate 缓存失效（§13.4 节点变更通知触发）。
	Invalidate(nodeID string)
	// Close 释放资源（如 Watch 协程）。
	Close()
}

// ErrNotRouted 未在路由表中找到节点。
var ErrNotRouted = errors.New("gateway: uid not routed")

// MemRouteTable 进程内路由表实现（演进态骨架）。
//
// 真实部署替换为 Redis Hash + 本地缓存 + Pub/Sub 失效通知。
// 演进态骨架用纯内存映射，Watch NodeRegistry 的变更事件做节点失效处理。
type MemRouteTable struct {
	mu       sync.RWMutex
	routes   map[string]string    // uid -> nodeID
	registry cluster.NodeRegistry // 可空：用于 Watch 失效
	ch       <-chan cluster.ChangeEvent
	stopCh   chan struct{}
}

// NewMemRouteTable 创建路由表；registry 非空时启动 Watch 协程。
//
// 同步订阅：在构造期就调用 Watch()，避免后续 Register 的事件被 goroutine 调度延迟错过。
func NewMemRouteTable(registry cluster.NodeRegistry) *MemRouteTable {
	t := &MemRouteTable{
		routes:   make(map[string]string),
		registry: registry,
		stopCh:   make(chan struct{}),
	}
	if registry != nil {
		t.ch = registry.Watch()
		go t.watchLoop()
	}
	return t
}

func (t *MemRouteTable) watchLoop() {
	if t.ch == nil {
		return
	}
	for {
		select {
		case <-t.stopCh:
			return
		case ev, ok := <-t.ch:
			if !ok {
				return
			}
			// 节点 Down/Draining：清掉指向该节点的路由（§12.4 LogicDown 流程）
			if ev.New == cluster.StateDown {
				t.Invalidate(ev.NodeID)
			}
		}
	}
}

// Lookup 查询 uid 归属节点。
func (t *MemRouteTable) Lookup(uid string) (string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	nodeID, ok := t.routes[uid]
	if !ok {
		return "", ErrNotRouted
	}
	return nodeID, nil
}

// Bind 写入 uid → nodeID 映射（覆盖式）。
func (t *MemRouteTable) Bind(uid, nodeID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.routes[uid] = nodeID
	return nil
}

// Unbind 清除映射。
func (t *MemRouteTable) Unbind(uid string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.routes, uid)
}

// Invalidate 清掉指向某节点的所有路由（节点故障/缩容）。
func (t *MemRouteTable) Invalidate(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for uid, nid := range t.routes {
		if nid == nodeID {
			delete(t.routes, uid)
		}
	}
}

// Close 关闭 Watch 协程。
func (t *MemRouteTable) Close() {
	select {
	case <-t.stopCh:
	default:
		close(t.stopCh)
	}
}

// ---------------- 内部 Transport ----------------

// MsgType 内部链路消息类型（§12.3 innerHeader）。
type MsgType byte

const (
	MsgUnicast       MsgType = 1 // 单推
	MsgRoomBroadcast MsgType = 2 // 房间广播（逐成员路由模式）
)

// InnerHeader 内部链路头（§12.3：仅 Gateway↔Logic 之间，客户端不可见）。
type InnerHeader struct {
	UID        string
	SessionID  string
	GatewayID  string
	MsgType    MsgType
	ForwardSeq uint32 // 原始帧 seq（debug 用）
}

// EncodeInner 在原始帧前追加 innerHeader（演进态骨架：紧凑文本前缀）。
// 实际部署可用 protobuf 或自定义二进制；此处保证可读性与复用 transport.Frame。
func EncodeInner(inner InnerHeader, f *transport.Frame) []byte {
	// 简化：innerHeader 编码为 "uid|sess|gw|type|seq\n" 前缀 + 原始帧字节流
	prefix := fmt.Sprintf("%s|%s|%s|%d|%d\n",
		inner.UID, inner.SessionID, inner.GatewayID,
		inner.MsgType, inner.ForwardSeq)
	raw, _ := transport.EncodeFrame(f)
	out := make([]byte, len(prefix)+len(raw))
	copy(out, prefix)
	copy(out[len(prefix):], raw)
	return out
}

// DecodeInner 从字节流解析 innerHeader + 原始帧。
func DecodeInner(data []byte) (InnerHeader, *transport.Frame, error) {
	// 找首行分隔符
	idx := -1
	for i, c := range data {
		if c == '\n' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return InnerHeader{}, nil, errors.New("gateway: inner header truncated")
	}
	var inner InnerHeader
	parts := strings.SplitN(string(data[:idx]), "|", 5)
	if len(parts) != 5 {
		return InnerHeader{}, nil, fmt.Errorf("gateway: parse inner: want 5 fields, got %d", len(parts))
	}
	mt, err := strconv.Atoi(parts[3])
	if err != nil {
		return InnerHeader{}, nil, fmt.Errorf("gateway: parse inner: bad msgtype %q: %w", parts[3], err)
	}
	seq, err := strconv.Atoi(parts[4])
	if err != nil {
		return InnerHeader{}, nil, fmt.Errorf("gateway: parse inner: bad seq %q: %w", parts[4], err)
	}
	inner.UID = parts[0]
	inner.SessionID = parts[1]
	inner.GatewayID = parts[2]
	inner.MsgType = MsgType(mt)
	inner.ForwardSeq = uint32(seq)

	// 剩余字节流式解码原始帧（可能跨多帧聚合，演进态骨架单帧即可）
	r := newBytesReader(data[idx+1:])
	f, err := transport.DecodeFrame(r, 0)
	if err != nil {
		return inner, nil, fmt.Errorf("gateway: decode frame: %w", err)
	}
	return inner, f, nil
}

// NodeTransport node-to-node 内部链路 Transport 接口（§12.3）。
//
//	演进态骨架：进程内 stub（Forward 直接调对方 HandleForward）；
//	真实部署：每节点一个长连接，写聚合 flush 与客户端 Conn 一致。
type NodeTransport interface {
	// Forward 把帧转发给目标节点。
	Forward(nodeID string, inner InnerHeader, f *transport.Frame) error
	// HandleForward 处理来自对端节点的转发帧（Logic 侧入口）。
	HandleForward(inner InnerHeader, f *transport.Frame)
	// AddPeer 添加对端节点连接（演进态骨架：注册回调）。
	AddPeer(nodeID string, handler func(InnerHeader, *transport.Frame)) error
	// RemovePeer 删除对端。
	RemovePeer(nodeID string)
}

// MemNodeTransport 进程内 NodeTransport 实现。
type MemNodeTransport struct {
	mu      sync.RWMutex
	peers   map[string]func(InnerHeader, *transport.Frame)
	localID string
}

// NewMemNodeTransport 创建进程内 Transport。
// localID 用于校验自环（同节点不转发）。
func NewMemNodeTransport(localID string) *MemNodeTransport {
	return &MemNodeTransport{
		peers:   make(map[string]func(InnerHeader, *transport.Frame)),
		localID: localID,
	}
}

// Forward 在进程内直接调对端 handler（同进程演进态骨架）。
func (t *MemNodeTransport) Forward(nodeID string, inner InnerHeader, f *transport.Frame) error {
	inner.GatewayID = t.localID // 标记来源网关（与 TCPNodeTransport 语义一致）
	t.mu.RLock()
	h, ok := t.peers[nodeID]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("gateway: peer %q not connected", nodeID)
	}
	h(inner, f)
	return nil
}

// HandleForward 由 NodeTransport 实现内部调用（演进态骨架 Forward 已直接调 handler，本方法无独立逻辑）。
func (t *MemNodeTransport) HandleForward(inner InnerHeader, f *transport.Frame) {
	// 进程内 stub：Forward 已同步调用对端 handler，本方法仅为接口完整性。
	// 真实部署需从长连接读 innerHeader+frame 后调用本地 handler。
}

// AddPeer 注册对端节点与回调。
func (t *MemNodeTransport) AddPeer(nodeID string, handler func(InnerHeader, *transport.Frame)) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[nodeID] = handler
	return nil
}

// RemovePeer 删除对端。
func (t *MemNodeTransport) RemovePeer(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, nodeID)
}

// ---------------- Gateway 装配 ----------------

// Gateway 网关进程装配。
//
//	演进态骨架：复用 internal/app.Connector 的会话/心跳/限流/写聚合；
//	上行：Connector 解析帧头 → 查 RouteTable → NodeTransport.Forward → Logic；
//	下行：NodeTransport.HandleForward → 按 uid 找本地 Conn → 写聚合。
type Gateway struct {
	id        string
	routes    RouteTable
	transport NodeTransport
	registry  cluster.NodeRegistry
}

// NewGateway 创建 Gateway 装配。
func NewGateway(gatewayID string, registry cluster.NodeRegistry) *Gateway {
	return &Gateway{
		id:        gatewayID,
		routes:    NewMemRouteTable(registry),
		transport: NewMemNodeTransport(gatewayID),
		registry:  registry,
	}
}

// NewGatewayWithTransport 创建 Gateway 并注入自定义 NodeTransport（如 TCPNodeTransport）。
func NewGatewayWithTransport(gatewayID string, registry cluster.NodeRegistry, tr NodeTransport) *Gateway {
	return &Gateway{
		id:        gatewayID,
		routes:    NewMemRouteTable(registry),
		transport: tr,
		registry:  registry,
	}
}

// Routes 暴露路由表（Connector 用）。
func (g *Gateway) Routes() RouteTable { return g.routes }

// Transport 暴露内部 Transport（Connector / Logic 用）。
func (g *Gateway) Transport() NodeTransport { return g.transport }

// ID 返回 Gateway ID。
func (g *Gateway) ID() string { return g.id }

// Registry 暴露节点注册表（用于选 Logic 节点）。
func (g *Gateway) Registry() cluster.NodeRegistry { return g.registry }

// PickLogicNode 从注册表选一个 Active 的 Logic 节点（演进态骨架：轮询第一个）。
// 真实部署按 §13.6 最少活跃房间加权。
func (g *Gateway) PickLogicNode() (string, bool) {
	if g.registry == nil {
		return "", false
	}
	nodes, err := g.registry.List(cluster.RoleLogic, true)
	if err != nil || len(nodes) == 0 {
		return "", false
	}
	return nodes[0].ID, true
}

// Close 关闭 Watch 协程；若底层 NodeTransport 持有资源（TCPNodeTransport
// 的 listener/连接），一并关闭。MemNodeTransport 无资源，断言不命中。
func (g *Gateway) Close() {
	g.routes.Close()
	if cl, ok := g.transport.(interface{ Close() error }); ok {
		_ = cl.Close()
	}
}

// ---------------- 工具 ----------------

// bytesReader 简单包装 []byte 实现 io.Reader（避免 import bytes 在本文件）。
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(b []byte) *bytesReader { return &bytesReader{data: b} }

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, errors.New("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// 占位：确保 framework 包被引用（未来 OnLogicDown 等回调会用到）。
var _ = framework.MsgLogicDown
var _ = time.Second
