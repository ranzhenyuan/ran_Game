package storage

import (
	"context"
	"testing"
	"time"

	"github.com/rangame/server/pkg/framework"
)

// 验证首次登录 Load 不存在的档案时返回零值档案（§10.4.1）。
func TestProfileStore_LoadMissingReturnsZero(t *testing.T) {
	ps := NewProfileStore(NewMemoryStorage())
	p, err := ps.Load(context.Background(), "newplayer")
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if p == nil {
		t.Fatal("Load returned nil profile")
	}
	if p.UID != "newplayer" {
		t.Errorf("UID = %q, want newplayer", p.UID)
	}
	if p.UpdatedAt == 0 {
		t.Error("UpdatedAt should be set for new profile")
	}
}

// SaveSync 强存后 Load 应能取回完整档案（往返测试）。
func TestProfileStore_SaveSyncLoadRoundtrip(t *testing.T) {
	ps := NewProfileStore(NewMemoryStorage())
	ctx := context.Background()
	orig := &framework.PlayerProfile{
		UID:          "u1",
		Level:        10,
		Exp:          5000,
		Coin:         1000,
		Gem:          50,
		Achievements: []string{"first_blood", "streak_5"},
		Settings:     map[string]any{"bgm": true, "lang": "zh"},
		Extra:        map[string]any{"snake_total_len": 42},
	}
	if err := ps.SaveSync(ctx, "u1", orig); err != nil {
		t.Fatalf("SaveSync: %v", err)
	}

	loaded, err := ps.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Level != 10 || loaded.Exp != 5000 || loaded.Coin != 1000 || loaded.Gem != 50 {
		t.Errorf("scalar mismatch: %+v", loaded)
	}
	if len(loaded.Achievements) != 2 || loaded.Achievements[0] != "first_blood" {
		t.Errorf("achievements = %v", loaded.Achievements)
	}
	if loaded.Settings["lang"] != "zh" {
		t.Errorf("settings = %v", loaded.Settings)
	}
	if loaded.Extra["snake_total_len"].(float64) != 42 { // JSON 反序列化为 float64
		t.Errorf("extra.snake_total_len = %v", loaded.Extra["snake_total_len"])
	}
}

// Save 异步路径在 MemoryStorage 等价于 SetSync，往返应一致。
func TestProfileStore_SaveLoadRoundtrip(t *testing.T) {
	ps := NewProfileStore(NewMemoryStorage())
	ctx := context.Background()
	orig := &framework.PlayerProfile{UID: "u2", Level: 3}
	if err := ps.Save(ctx, "u2", orig); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := ps.Load(ctx, "u2")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Level != 3 {
		t.Errorf("Level = %d, want 3", loaded.Level)
	}
}

// Patch 部分字段更新（§10.4.1）；不覆盖其他字段。
func TestProfileStore_PatchPartial(t *testing.T) {
	ps := NewProfileStore(NewMemoryStorage())
	ctx := context.Background()
	orig := &framework.PlayerProfile{
		UID:   "u3",
		Level: 5,
		Coin:  100,
		Extra: map[string]any{"keep": "v"},
	}
	if err := ps.SaveSync(ctx, "u3", orig); err != nil {
		t.Fatalf("SaveSync: %v", err)
	}

	err := ps.Patch(ctx, "u3", map[string]any{
		"level":           8,             // scalar 字段
		"extra.snake_len": 99,            // 嵌套路径写入 Extra
		"extra.new_field": "added",       // 新增 Extra 字段
		"achievements":    []string{"a"}, // 列表替换
	})
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	loaded, err := ps.Load(ctx, "u3")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Level != 8 {
		t.Errorf("Level = %d, want 8", loaded.Level)
	}
	if loaded.Coin != 100 {
		t.Errorf("Coin changed unexpectedly = %d, want 100", loaded.Coin)
	}
	if loaded.Extra["keep"] != "v" {
		t.Errorf("extra.keep lost = %v", loaded.Extra["keep"])
	}
	if loaded.Extra["snake_len"].(float64) != 99 {
		t.Errorf("extra.snake_len = %v", loaded.Extra["snake_len"])
	}
	if loaded.Extra["new_field"] != "added" {
		t.Errorf("extra.new_field = %v", loaded.Extra["new_field"])
	}
	if len(loaded.Achievements) != 1 || loaded.Achievements[0] != "a" {
		t.Errorf("achievements = %v", loaded.Achievements)
	}
}

// Save 和 SaveSync 都应更新 UpdatedAt（单调性校验基础，§10.4.1）。
func TestProfileStore_UpdatedAtMonotonic(t *testing.T) {
	ps := NewProfileStore(NewMemoryStorage())
	ctx := context.Background()

	p1 := &framework.PlayerProfile{UID: "u4", Level: 1}
	if err := ps.SaveSync(ctx, "u4", p1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	p2 := &framework.PlayerProfile{UID: "u4", Level: 2}
	if err := ps.SaveSync(ctx, "u4", p2); err != nil {
		t.Fatal(err)
	}

	loaded, err := ps.Load(ctx, "u4")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.UpdatedAt <= p1.UpdatedAt {
		t.Errorf("UpdatedAt not monotonic: first=%d second=%d", p1.UpdatedAt, loaded.UpdatedAt)
	}
}

// Save 后 UID 字段被强制对齐（防止外部传错 uid）。
func TestProfileStore_SaveAlignsUID(t *testing.T) {
	ps := NewProfileStore(NewMemoryStorage())
	ctx := context.Background()
	// 故意让 p.UID 与传入 uid 不一致
	p := &framework.PlayerProfile{UID: "wrong", Level: 1}
	if err := ps.Save(ctx, "correct", p); err != nil {
		t.Fatal(err)
	}
	loaded, err := ps.Load(ctx, "correct")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.UID != "correct" {
		t.Errorf("UID = %q, want correct", loaded.UID)
	}
}

// nil profile 应报错而非写空值。
func TestProfileStore_SaveNilProfile(t *testing.T) {
	ps := NewProfileStore(NewMemoryStorage())
	ctx := context.Background()
	if err := ps.Save(ctx, "u5", nil); err == nil {
		t.Error("Save nil should error")
	}
	if err := ps.SaveSync(ctx, "u5", nil); err == nil {
		t.Error("SaveSync nil should error")
	}
}

// ProfileQueryService.BatchQuery 并发查询多个 uid（§10.4.3）。
func TestProfileQueryService_BatchQuery(t *testing.T) {
	store := NewMemoryStorage()
	ps := NewProfileStore(store)
	q := NewProfileQueryService(ps)
	ctx := context.Background()

	// 预置两个玩家档案
	_ = ps.SaveSync(ctx, "a", &framework.PlayerProfile{UID: "a", Level: 1})
	_ = ps.SaveSync(ctx, "b", &framework.PlayerProfile{UID: "b", Level: 2})

	// batch 查询含存在与不存在
	results, err := q.BatchQuery(ctx, []string{"a", "b", "missing"})
	if err != nil {
		t.Fatalf("BatchQuery: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("results len = %d, want 3", len(results))
	}
	if results["a"].Level != 1 {
		t.Errorf("a.Level = %d, want 1", results["a"].Level)
	}
	if results["b"].Level != 2 {
		t.Errorf("b.Level = %d, want 2", results["b"].Level)
	}
	// missing 应返回零值档案（首次登录语义）
	if results["missing"] == nil || results["missing"].UID != "missing" {
		t.Errorf("missing profile = %+v, want zero with UID=missing", results["missing"])
	}
}

// ProfileQueryService.Query 单查。
func TestProfileQueryService_Query(t *testing.T) {
	store := NewMemoryStorage()
	ps := NewProfileStore(store)
	q := NewProfileQueryService(ps)
	ctx := context.Background()

	_ = ps.SaveSync(ctx, "u6", &framework.PlayerProfile{UID: "u6", Level: 7})
	p, err := q.Query(ctx, "u6")
	if err != nil {
		t.Fatal(err)
	}
	if p.Level != 7 {
		t.Errorf("Level = %d, want 7", p.Level)
	}
}
