// 房间侧档案 Patch 的单写者路由（架构文档 §10.4.4 / §7.3 红线 1）。
//
// 档案的唯一权威副本在 PlayerActor（登录 Load，退出强存）。若 RoomActor 在
// OnDestroy 直接走存储层读改写（profileStore.Patch），与 PlayerActor 的周期
// checkpoint / 退出 SaveSync 形成跨 Actor 整档覆盖竞态，会回滚结算写入的 extra。
// 因此房间侧 Patch 先尝试路由到目标 PlayerActor（消息传递），Actor 不在
// （离线/已释放）时退化为存储层读改写——此时无人再整档覆盖，安全。
package app

import (
	"context"

	"github.com/rangame/server/pkg/framework"
)

// profilePatchMsg 投递到 PlayerActor 邮箱的档案 Patch 请求（Payload 类型）。
type profilePatchMsg struct {
	uid    string
	fields map[string]any
	reply  chan error
}

// patchRouter 面向 RoomLogic 的 ProfileStore：Patch 路由到 PlayerActor 单写者，
// 其余方法透传存储层（Load 只读安全；Save/SaveSync 由 PlayerActor 生命周期负责）。
type patchRouter struct {
	ps      framework.ProfileStore // 存储层实现（兜底路径）
	resolve func(uid string) (framework.ID, bool)
	tell    func(framework.ID, *framework.Envelope) error
}

var _ framework.ProfileStore = (*patchRouter)(nil)

// newPatchRouter 包装 profileStore，注入 PlayerActor 定位与投递。
func (s *Server) newPatchRouter() framework.ProfileStore {
	return &patchRouter{
		ps:   s.profileStore,
		tell: s.engine.Tell,
		resolve: func(uid string) (framework.ID, bool) {
			s.playerMu.Lock()
			defer s.playerMu.Unlock()
			id, ok := s.players[uid]
			return id, ok
		},
	}
}

// Patch 结算回写入口：优先路由到 PlayerActor（其 goroutine 内应用并立即强存）。
func (r *patchRouter) Patch(ctx context.Context, uid string, fields map[string]any) error {
	if id, ok := r.resolve(uid); ok {
		msg := profilePatchMsg{uid: uid, fields: fields, reply: make(chan error, 1)}
		// Actor 已停（release 在途）Tell 返回 ErrActorNotFound → 走兜底
		if err := r.tell(id, &framework.Envelope{Payload: msg}); err == nil {
			select {
			case err := <-msg.reply:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return r.ps.Patch(ctx, uid, fields)
}

func (r *patchRouter) Load(ctx context.Context, uid string) (*framework.PlayerProfile, error) {
	return r.ps.Load(ctx, uid)
}

func (r *patchRouter) Save(ctx context.Context, uid string, p *framework.PlayerProfile) error {
	return r.ps.Save(ctx, uid, p)
}

func (r *patchRouter) SaveSync(ctx context.Context, uid string, p *framework.PlayerProfile) error {
	return r.ps.SaveSync(ctx, uid, p)
}
