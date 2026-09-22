package storage

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// profileTable 玩家档案统一表名（§10.4.2 运维对账约定）。
const profileTable = "player_profile"

// profileStore framework.ProfileStore 实现：复用 framework.Storage 的 Get/Set/SetSync，
// table 固定 "player_profile"，key 为 uid。Patch 为读-改-写封装。
// 兼容 MemoryStorage 和远程 Store 门面（均实现 framework.Storage）。
type profileStore struct {
	store framework.Storage
}

// NewProfileStore 在任意 framework.Storage 之上创建 ProfileStore。
func NewProfileStore(s framework.Storage) framework.ProfileStore {
	return &profileStore{store: s}
}

// Load 加载玩家档案；不存在返回零值档案（首次登录，§10.4.1）。
func (ps *profileStore) Load(ctx context.Context, uid string) (*framework.PlayerProfile, error) {
	var p framework.PlayerProfile
	if err := ps.store.Get(ctx, profileTable, uid, &p); err != nil {
		if errors.Is(err, framework.ErrStorageNotFound) {
			return &framework.PlayerProfile{UID: uid, UpdatedAt: time.Now().UnixMilli()}, nil
		}
		return nil, err
	}
	if p.UID == "" {
		p.UID = uid
	}
	return &p, nil
}

// Save 异步写入（走写回队列，§10.2）。
func (ps *profileStore) Save(ctx context.Context, uid string, p *framework.PlayerProfile) error {
	if p == nil {
		return errors.New("profile: nil profile")
	}
	p.UID = uid
	p.UpdatedAt = time.Now().UnixMilli()
	return ps.store.Set(ctx, profileTable, uid, p)
}

// SaveSync 强一致写入（OnRelease/结算路径，§10.4.2）。
func (ps *profileStore) SaveSync(ctx context.Context, uid string, p *framework.PlayerProfile) error {
	if p == nil {
		return errors.New("profile: nil profile")
	}
	p.UID = uid
	p.UpdatedAt = time.Now().UnixMilli()
	return ps.store.SetSync(ctx, profileTable, uid, p)
}

// Patch 部分字段更新（读-改-写，§10.4.1）。
//
//	fields 支持 "extra.xxx" 嵌套路径（写入 Extra["xxx"]）。
//	货币类（coin/gem）禁止走 Patch，必须用 Storage.IncrBy。
func (ps *profileStore) Patch(ctx context.Context, uid string, fields map[string]any) error {
	p, err := ps.Load(ctx, uid)
	if err != nil {
		return err
	}
	for k, v := range fields {
		applyPatchField(p, k, v)
	}
	return ps.store.SetSync(ctx, profileTable, uid, p)
}

// ApplyProfilePatch 将 fields 批量写入 profile（含 "extra.xxx" 嵌套路径）。
// 导出给 PlayerActor 在自己的 goroutine 内应用房间侧结算 Patch（§10.4.4 单写者）。
func ApplyProfilePatch(p *framework.PlayerProfile, fields map[string]any) {
	for k, v := range fields {
		applyPatchField(p, k, v)
	}
}

// applyPatchField 将单个字段写入 profile；支持 "extra.xxx" 嵌套路径。
func applyPatchField(p *framework.PlayerProfile, key string, val any) {
	if strings.HasPrefix(key, "extra.") {
		field := strings.TrimPrefix(key, "extra.")
		if p.Extra == nil {
			p.Extra = make(map[string]any)
		}
		p.Extra[field] = val
		return
	}
	switch key {
	case "level":
		if i, ok := toInt32(val); ok {
			p.Level = i
		}
	case "exp":
		if i, ok := toInt64(val); ok {
			p.Exp = i
		}
	case "settings":
		if m, ok := val.(map[string]any); ok {
			p.Settings = m
		}
	case "achievements":
		if s, ok := val.([]string); ok {
			p.Achievements = s
		}
	default:
		// 未知字段落入 Extra
		if p.Extra == nil {
			p.Extra = make(map[string]any)
		}
		p.Extra[key] = val
	}
}

func toInt32(v any) (int32, bool) {
	switch n := v.(type) {
	case int:
		return int32(n), true
	case int32:
		return n, true
	case int64:
		return int32(n), true
	}
	return 0, false
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}
