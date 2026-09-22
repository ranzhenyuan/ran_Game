package actor

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// Registry uid→PlayerActor、roomID→RoomActor 的共享注册表（§7.5 共享点②）。
// 读泵 goroutine 读、RoomManager/引擎 goroutine 写，用 RWMutex 保护。
type Registry struct {
	mu       sync.RWMutex
	byUID    map[string]framework.ID
	byRoom   map[string]framework.ID
	idToUID  map[framework.ID]string
	idToRoom map[framework.ID]string
}

func NewRegistry() *Registry {
	return &Registry{
		byUID:    make(map[string]framework.ID),
		byRoom:   make(map[string]framework.ID),
		idToUID:  make(map[framework.ID]string),
		idToRoom: make(map[framework.ID]string),
	}
}

func (r *Registry) BindUID(uid string, id framework.ID) {
	r.mu.Lock()
	if old, ok := r.idToUID[id]; ok {
		delete(r.byUID, old)
	}
	if oldID, ok := r.byUID[uid]; ok {
		delete(r.idToUID, oldID)
	}
	r.byUID[uid] = id
	r.idToUID[id] = uid
	r.mu.Unlock()
}

func (r *Registry) LookupUID(uid string) (framework.ID, bool) {
	r.mu.RLock()
	id, ok := r.byUID[uid]
	r.mu.RUnlock()
	return id, ok
}

func (r *Registry) BindRoom(roomID string, id framework.ID) {
	r.mu.Lock()
	if old, ok := r.idToRoom[id]; ok {
		delete(r.byRoom, old)
	}
	if oldID, ok := r.byRoom[roomID]; ok {
		delete(r.idToRoom, oldID)
	}
	r.byRoom[roomID] = id
	r.idToRoom[id] = roomID
	r.mu.Unlock()
}

func (r *Registry) LookupRoom(roomID string) (framework.ID, bool) {
	r.mu.RLock()
	id, ok := r.byRoom[roomID]
	r.mu.RUnlock()
	return id, ok
}

// Unbind 清理某 Actor 的全部绑定。
func (r *Registry) Unbind(id framework.ID) {
	r.mu.Lock()
	if uid, ok := r.idToUID[id]; ok {
		delete(r.byUID, uid)
		delete(r.idToUID, id)
	}
	if roomID, ok := r.idToRoom[id]; ok {
		delete(r.byRoom, roomID)
		delete(r.idToRoom, id)
	}
	r.mu.Unlock()
}

// 兼容蓝图 §3.5 命名的解析方法（Engine 转发）。
func (r *Registry) ResolvePlayer(uid string) (framework.ID, bool)  { return r.LookupUID(uid) }
func (r *Registry) ResolveRoom(roomID string) (framework.ID, bool) { return r.LookupRoom(roomID) }

// process 一个运行中的 Actor 实例：邮箱 + goroutine 运行态。
type process struct {
	id     framework.ID
	actor  framework.Actor
	mb     *mailbox
	parent framework.ID

	ctx      *actorCtx
	stopSig  chan struct{}
	stopOnce sync.Once

	stopping atomic.Bool
	finished atomic.Bool

	panicMu sync.Mutex
	panics  []time.Time
}

// beginDrain 标记进入优雅停机：投递转入 block 路径，循环排空已入箱消息后退出。
func (p *process) beginDrain() {
	p.stopOnce.Do(func() { close(p.stopSig) })
	p.stopping.Store(true)
}

func (p *process) isStopping() bool { return p.stopping.Load() }
