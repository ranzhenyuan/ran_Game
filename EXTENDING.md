# 新玩法模块接入指南

本文档说明如何在不改动 `internal/` 任何代码的前提下，新增一个玩法模块。对应架构文档 §14 扩展指南。

## 依赖方向约束

```
games/*  →  pkg/framework
internal/*  ←  不允许 import games/*
```

玩法模块只能依赖 `pkg/framework` 稳定 API 面；`internal` 包通过 `framework.GameModule` 接口反向调用玩法，依赖倒置。

## 四步接入

### 1. 创建模块目录

```
games/<yourgame>/
  module.go         GameModule 装配
  messages.go       消息结构体 + protobuf tag
  logic.go          RoomLogic 实现
  proto/<yourgame>.proto  规范文档（可选但推荐）
```

### 2. 定义消息 ID 与消息结构

消息 ID 从 `framework.ModuleBase(moduleIndex)` 起分配，每模块 256 个。`moduleIndex` 由 `cmd/server/main.go` 中模块在 `modules` 列表的索引决定（0 → 0x1000，1 → 0x1100）。

```go
// games/<yourgame>/messages.go
package <yourgame>

import "github.com/rangame/server/pkg/framework"

const (
    MsgAction  framework.MsgID = 0x1000 // 上行动作（可靠通道）
    MsgState   framework.MsgID = 0x1001 // 下行快照（快照通道）
    MsgResult  framework.MsgID = 0x1002 // 下行结算（可靠通道）
)

// ActionReq 上行动作请求。
type ActionReq struct {
    X int `json:"x" protobuf:"varint,1,opt,name=x,proto3"`
    Y int `json:"y" protobuf:"varint,2,opt,name=y,proto3"`
}

// StateNtf 下行局面快照。
type StateNtf struct {
    Tick int64 `json:"tick" protobuf:"varint,1,opt,name=tick,proto3"`
    // ...
}
```

**约束**：每模块消息 ID 必须落在自己的 256 段内，跨模块冲突由 `ModuleBase` 保证不发生。

### 3. 实现 RoomLogic

```go
// games/<yourgame>/logic.go
package <yourgame>

import (
    "time"
    "github.com/rangame/server/pkg/framework"
)

type Logic struct {
    // 房间内状态字段
}

func NewLogic() framework.RoomLogic { return &Logic{} }

func (l *Logic) OnCreate(r framework.RoomCtx, cfg framework.RoomConfig) {
    // 初始化房间状态
    r.Every(100*time.Millisecond, func() { l.tick(r) })
}

func (l *Logic) OnJoin(r framework.RoomCtx, p framework.Player) {}
func (l *Logic) OnMessage(r framework.RoomCtx, p framework.Player, env *framework.Envelope) {
    // 按 env.MsgID 分派到具体处理
}
func (l *Logic) Tick(r framework.RoomCtx, dt time.Duration) {}
func (l *Logic) OnLeave(r framework.RoomCtx, p framework.Player) {}
func (l *Logic) OnEmpty(r framework.RoomCtx) { r.Close() }
func (l *Logic) OnDestroy(r framework.RoomCtx) {}
```

**关键约束**（架构文档 §7.3）：
- 所有回调在同一 RoomActor goroutine 内串行执行，**禁止开协程改状态**
- `RoomCtx` 方法仅在自己 goroutine 内调用，免锁
- 房间级临时 KV 随房间销毁而消失，不要存持久化数据

### 4. 实现 GameModule 装配

```go
// games/<yourgame>/module.go
package <yourgame>

import (
    "time"
    "github.com/rangame/server/pkg/framework"
)

type Module struct{}

func (Module) Name() string { return "<yourgame>" }

func (Module) RoomDef() (framework.RoomConfig, func() framework.RoomLogic) {
    cfg := framework.RoomConfig{
        Module:       "<yourgame>",
        MaxPlayers:   4,
        MaxDuration:  10 * time.Minute,  // 强结算上界（§8.2）
        TickInterval: 100 * time.Millisecond,
        Mailbox: framework.MailboxPolicy{
            Capacity: 4096,
            OnFull:   framework.FullDrop,  // 溢出策略：Drop/Kick/Block
        },
        MatchRule: framework.MatchRule{
            Module:  "<yourgame>",
            Code:    "ranked",
            Players: 4,
        },
    }
    return cfg, func() framework.RoomLogic { return NewLogic() }
}

func (Module) RegisterRoutes(r framework.RoomRegistrar) {
    // snapshot=false 走可靠通道（重放缓冲，分配 seq）
    // snapshot=true 走快照通道（latest-wins，不重放）
    r.RegisterRoom(MsgAction, func() any { return &ActionReq{} }, false)
}
```

### 5. 在 cmd 装配

```go
// cmd/server/main.go
modules := []framework.GameModule{
    snake.Module{},
    <yourgame>.Module{},  // 新增
}
```

`modules` 列表索引决定该模块的 `moduleIndex`，进而决定消息 ID 段起始。**索引一旦上线不要随意调整顺序**，否则消息 ID 段错位。

## 下行消息 API

在 `RoomLogic` 回调内通过 `RoomCtx` 或 `Player` 推送消息：

| 方法 | 通道 | 说明 |
|------|------|------|
| `r.Broadcast(msgID, msg, except...)` | 可靠 | 全员广播，进 ring buffer 重放 |
| `r.BroadcastSnapshot(msgID, msg, except...)` | 快照 | 全员广播，latest-wins 不重放 |
| `p.Push(msgID, msg)` | 可靠 | 单推，分配服务端 seq |
| `p.PushSnapshot(msgID, msg)` | 快照 | 单推，latest-wins |

**何时用快照通道**：高频全量局面（如贪吃蛇的 `StateNtf`），丢失中间帧不影响一致性，只关心最新值。低频关键结果（如 `ResultNtf`）必须走可靠通道。

## 邮箱溢出策略

`RoomConfig.Mailbox.OnFull` 三选一：

| 策略 | 行为 | 适用场景 |
|------|------|----------|
| `FullDrop` | 计数并丢弃 | 局面快照驱动，丢个别 Move 不影响一致性 |
| `FullKick` | 停止 Actor | 默认；强一致玩法 |
| `FullBlock` | 阻塞投递者 | 仅停机/强一致路径 |

## 定时器

```go
r.After(d, func())   // 一次性
r.Every(d, func())   // 周期性
```

返回 `framework.Handle`，可 `Stop()` 取消。回调在 RoomActor goroutine 内执行。

## 存储访问

```go
r.Storage().Set(ctx, key, value)
r.Storage().IncrBy(ctx, key, delta)
r.Storage().Ranking(ctx, key, member, score)
```

`Storage` 接口由 `internal/storage` 装配（memory/redis/sqlite/mysql）。异步队列 + WAL + 熔断，详见 [DEPLOY.md](DEPLOY.md) 存储段。

## 参考示例

完整示例见 [games/snake/](games/snake/)：
- [module.go](games/snake/module.go) — GameModule 装配
- [messages.go](games/snake/messages.go) — 消息定义 + protobuf tag
- [logic.go](games/snake/logic.go) — RoomLogic 实现（Tick 推进 + 快照广播 + 结算落库）

## .proto 规范文档

推荐在 `games/<yourgame>/proto/` 下放一份 `.proto` 作为规范文档，与 Go struct 的 `protobuf` tag 保持一致。当前项目用手写 ProtoCodec（`protowire` + 反射），不依赖 protoc。详见 [PROTOCOL.md](PROTOCOL.md)。
