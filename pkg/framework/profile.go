// Package framework 玩家档案/游戏存档接口（架构文档 §10.4）。
//
// 玩家档案 = 跨会话持久化的玩家级数据（等级/货币/成就/设置/extra），
// 与对局历史（§10.2 "对局记录"）正交。档案随 uid 而非随房间存在。
//
// 落库语义：周期 checkpoint（PlayerActor 自驱动 Every）+ 退出强存（OnRelease SaveSync）。
// 货币类禁止整档案覆盖，必须走 Storage.IncrBy（§10.4.1）。
package framework

import "context"

// PlayerProfile 通用档案结构（混合字段策略，§10.4.1）。
// 通用字段所有玩法共享；Extra 留给玩法自定义（如蛇蛇累计长度、卡组清单）。
type PlayerProfile struct {
	UID          string         `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Level        int32          `json:"level" protobuf:"varint,2,opt,name=level,proto3"`
	Exp          int64          `json:"exp" protobuf:"varint,3,opt,name=exp,proto3"`
	Coin         int64          `json:"coin" protobuf:"varint,4,opt,name=coin,proto3"` // 货币类强一致，走 IncrBy 而非直接赋值
	Gem          int64          `json:"gem" protobuf:"varint,5,opt,name=gem,proto3"`
	Settings     map[string]any `json:"settings,omitempty" protobuf:"bytes,6,opt,name=settings,proto3"`
	Achievements []string       `json:"achievements,omitempty" protobuf:"bytes,7,rep,name=achievements,proto3"`
	Extra        map[string]any `json:"extra,omitempty" protobuf:"bytes,8,opt,name=extra,proto3"`  // 玩法自定义扩展面
	UpdatedAt    int64          `json:"updated_at" protobuf:"varint,9,opt,name=updated_at,proto3"` // ms 时间戳，单调性校验
}

// ProfileStore 玩家档案存取接口（业务内调用，PlayerActor goroutine 内访问，§10.4.1）。
//
// table 命名约定 "player_profile"（统一表名，便于运维识别与对账）。
// 实现复用 framework.Storage 的 Get/Set/SetSync；Patch 为读-改-写封装。
type ProfileStore interface {
	// Load 登录或重连时加载档案；不存在返回零值档案（首次登录）。
	Load(ctx context.Context, uid string) (*PlayerProfile, error)
	// Save 增量写入（走异步队列，§10.2）。
	Save(ctx context.Context, uid string, p *PlayerProfile) error
	// SaveSync 强一致写入（走 Sync 变体）；OnRelease/结算等关键路径使用。
	SaveSync(ctx context.Context, uid string, p *PlayerProfile) error
	// Patch 部分字段更新（避免读-改-写竞态）；fields 为 "key→value" 映射。
	// 支持 "extra.xxx" 形式的嵌套路径（写入 Extra["xxx"]）。
	Patch(ctx context.Context, uid string, fields map[string]any) error
}

// ProfileQueryService 离线查询服务接口（玩家不在线时使用，§10.4.3）。
// 实现方式：① admin HTTP API（/admin/players/{uid}）；② 独立 gRPC 服务。
// 安全基线：仅内网/可信调用方访问（§11.4 ipGuard + mTLS）。
type ProfileQueryService interface {
	Query(ctx context.Context, uid string) (*PlayerProfile, error)
	BatchQuery(ctx context.Context, uids []string) (map[string]*PlayerProfile, error)
}
