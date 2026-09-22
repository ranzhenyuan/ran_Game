package storage

import (
	"context"
	"sync"

	"github.com/rangame/server/pkg/framework"
)

// profileQueryService framework.ProfileQueryService 实现（§10.4.3）。
// 玩家不在线时直接查 Storage；高频查询可加进程内 LRU 缓存。
type profileQueryService struct {
	store framework.ProfileStore
}

// NewProfileQueryService 创建离线查询服务。
func NewProfileQueryService(s framework.ProfileStore) framework.ProfileQueryService {
	return &profileQueryService{store: s}
}

func (q *profileQueryService) Query(ctx context.Context, uid string) (*framework.PlayerProfile, error) {
	return q.store.Load(ctx, uid)
}

func (q *profileQueryService) BatchQuery(ctx context.Context, uids []string) (map[string]*framework.PlayerProfile, error) {
	out := make(map[string]*framework.PlayerProfile, len(uids))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, uid := range uids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			p, err := q.store.Load(ctx, id)
			if err != nil {
				return
			}
			mu.Lock()
			out[id] = p
			mu.Unlock()
		}(uid)
	}
	wg.Wait()
	return out, nil
}
