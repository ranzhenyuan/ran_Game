# 模块拆分与接口蓝图

> 配套文档：[architecture.md](./architecture.md)（v0.4）　|　版本：v0.1　|　Go 1.22+
>
> 本文把架构设计落地为三份实现蓝图：**① 模块职责清单 → ② 接口清单（Go）→ ③ Proto 文件**。
> 假设 go.mod module 路径为 `github.com/rangame/server`（实现时按实际仓库替换，全文 import 前缀同步改）。

---

## 目录

1. [总体目录树与依赖方向](#1-总体目录树与依赖方向)
2. [模块职责清单](#2-模块职责清单)
3. [接口清单（Go）](#3-接口清单go)
4. [Proto 文件](#4-proto-文件)
5. [实现顺序建议](#5-实现顺序建议)

---

## 1. 总体目录树与依赖方向

```
rangGame/
├── cmd/
│   └── server/main.go                 # 进程装配入口（单机 / gateway / logic 三种角色）
├── internal/
│   ├── config/                        # YAML 配置加载与校验
│   ├── app/                           # App 装配、模块生命周期、信号与优雅停机编排
│   ├── transport/                     # 接入层：Conn 抽象、TCP/WS Acceptor、帧解析、读写泵
│   │   ├── conn.go
│   │   ├── acceptor_tcp.go
│   │   ├── acceptor_ws.go
│   │   ├── frame.go                   # §4.1 帧编解码（Transport 内部）
│   │   └── ratelimit.go
│   ├── protocol/                      # Codec 插件、错误码信封
│   │   ├── codec.go
│   │   ├── codec_json.go
│   │   ├── codec_proto.go
│   │   └── envelope.go
│   ├── session/                       # 会话、双通道下行缓冲、重连重放
│   │   ├── session.go
│   │   ├── replay.go                  # 可靠环形缓冲 + 快照 latest-wins
│   │   └── manager.go
│   ├── router/                        # msgID 注册表、两级定位、中间件
│   │   ├── router.go
│   │   ├── middleware.go
│   │   └── msgctx.go
│   ├── actor/                         # Actor 引擎：邮箱、调度、注册表、监督
│   │   ├── actor.go
│   │   ├── mailbox.go
│   │   ├── registry.go                # §7.5 共享点：RWMutex
│   │   ├── engine.go
│   │   └── supervisor.go
│   ├── room/                          # RoomActor 骨架、RoomManager
│   │   ├── room.go
│   │   ├── context.go                 # RoomCtx/Player
│   │   └── manager.go
│   ├── match/                         # MatchMaker 接口 + 进程内实现（分布式实现演进）
│   │   ├── matcher.go
│   │   └── simple.go
│   ├── timer/                         # 最小堆调度、Clock 抽象、Module Cron
│   │   ├── clock.go
│   │   ├── scheduler.go
│   │   └── cron.go
│   ├── storage/                       # Storage/Ranking 接口、写回队列、WAL、熔断
│   │   ├── storage.go
│   │   ├── ranking.go
│   │   ├── writeback.go
│   │   ├── wal.go
│   │   ├── breaker.go
│   │   ├── redis/
│   │   └── mysql/
│   ├── cluster/                       # 【演进态】节点注册/发现/排水状态机/分布式匹配
│   │   ├── registry.go                # §13.2 Redis 注册表 + Pub/Sub
│   │   ├── drain.go                   # §13.3 状态机
│   │   └── match_redis.go             # §13.5 Lua 原子组队
│   ├── gateway/                       # 【演进态】Gateway 装配：一级路由 + 内部 Transport
│   │   ├── gateway.go                 # §12.4 Gateway + MemRouteTable（watch 注册表失效）
│   │   └── node_transport.go          # §12.3 NodeTransport 接口 + Mem/TCP 两实现（懒拨号/写聚合）
│   └── obs/                           # logger / metrics / pprof / trace
├── pkg/
│   └── framework/                     # 面向游戏业务的稳定 API（仅此处允许被 games/ 引用）
│       ├── framework.go               # App/Module/Registry（业务装配面）
│       ├── actor.go                   # Actor/ActorCtx/Envelope
│       ├── room.go                    # RoomLogic/TimeoutLogic/RoomCtx/Player
│       ├── router.go                  # Route 选项：ToPlayer/ToRoom
│       ├── storage.go                 # Storage/Ranking 透传接口
│       ├── errcode.go                 # §4.3 错误码
│       └── msgid.go                   # 系统 msgID 常量
├── proto/
│   └── framework/                     # §4 系统 proto（业务 proto 放 games/*/proto）
├── games/
│   └── snake/                         # 示例游戏
└── bench/bot/                         # §15.1 压测 bot
```

**依赖方向（单向，禁止反向/环依赖）**：

```
cmd ──▶ app ──▶ transport / session / router / actor / room / match / timer / storage / obs
                       ▲                  ▲
                       └──── pkg/framework ───── games/*（业务只依赖 pkg/framework）
internal/* 之间只允许上层依赖下层：app > {router,session,room,match} > actor > {protocol,timer,storage}
cluster / gateway 为演进态包，仅 server.role=gateway|logic 时由 app.BuildGateway/BuildLogic 装配；
app 内 GatewayConnector / LogicHandler 是拆分两侧的收发实现，session.RemoteConn 是 Logic 侧下行端点
```

> 关键纪律：**games/ 只能 import `pkg/framework`**；internal/ 的具体类型（Mailbox、Engine 等）不向业务暴露，保证内核可替换。

---

## 2. 模块职责清单

| 模块 | 职责 | 关键产物 | 并发模型要点 | 对应文档 |
|------|------|----------|--------------|----------|
| `cmd/server` | 按角色（standalone/gateway/logic）装配并启动 | main.go | 仅装配，不含逻辑 | §13 |
| `internal/config` | YAML→强类型配置；默认值与必填校验 | Config 结构体 | 启动后只读（§7.3 红线 3） | 全文 |
| `internal/app` | 组件装配顺序、Module 加载、信号处理、两级停机编排；拆分态两侧收发器 | Build/BuildGateway/BuildLogic、GatewayConnector、LogicHandler | 编排器；调用 drain→消息级停机 | §11.3/§12/§13.3 |
| `internal/transport` | TCP/WS 监听、TLS、WS Origin、帧拆包、每连接读泵+写聚合、限流、慢消费者策略 | Conn、Acceptor、FrameCodec | **每连接 2 goroutine**；写 channel 为 §7.5 共享点 | §3/§11.4 |
| `internal/protocol` | Codec 注册（JSON/PB）、按 flag 选 codec；错误码信封构造 | Codec、Envelope | 无状态，可并发调用 | §4 |
| `internal/session` | 登录态绑定、心跳判定、Conn 解绑/重绑、下行 seq、可靠环形缓冲+快照缓存、reconnectToken；拆分态 Logic 侧下行端点 RemoteConn | Session、Manager、RemoteConn | Session 内细粒度 mutex（§7.5 共享点①） | §5/§12.7 |
| `internal/router` | msgID 注册表、reqFactory、个人/房间两级定位、中间件链（Auth/RateLimit/Trace/Metric/Recover） | Router、Middleware | 读多写少；路由表只读 | §6 |
| `internal/actor` | Actor 生成、有界邮箱、批量 drain、Drop/Kick/Block 策略、水位指标、panic recover、父子监督、Actor 注册表 | Engine、Mailbox、ActorRegistry、Supervisor | **每 Actor 1 goroutine 串行**；注册表 RWMutex（§7.5 共享点②） | §7 |
| `internal/room` | RoomActor 骨架、成员表、广播扇出、KV、房间 TTL+TimeoutLogic、空房回收、roomID→node | RoomActor、RoomManager | 全部状态仅 RoomActor goroutine 访问 | §8 |
| `internal/match` | MatchMaker 接口；进程内段位/人数队列（单机默认） | MatchMaker、SimpleMatcher | 独立 Matcher Actor 串行 | §8.2/§13.5 |
| `internal/timer` | 最小堆定时器、回调 Tell 回 Actor 邮箱、Actor 级定时器表随销毁取消、可注入 Clock、Module Cron runner | Scheduler、Clock、Cron | 全局 1 个调度 goroutine；回调不直接执行 | §9/§15.4 |
| `internal/storage` | KV/Ranking 接口、异步写回队列、失败重试、本地 WAL 兜底、按 table 熔断与半开探测、redis/mysql 适配 | Storage、Ranking、WriteBack、Breaker | 后台 flush worker 池；Actor 调用零阻塞 | §10 |
| `internal/cluster` | **演进态**：节点注册表（每节点独立 key `cluster:node:{id}` + TTL + Pub/Sub）、Active/Draining/Drained 状态机、preStop drain API、Redis Lua 分布式匹配 | NodeRegistry、DrainController、RedisMatcher | 控制面；心跳 5s | §13 |
| `internal/gateway` | **演进态（已装配）**：Gateway 装配、一级路由表（Mem 已落地/Redis 待演进）与注册表事件失效、内部 node-to-node Transport（进程内 Mem + 真实 TCP 懒拨号/写聚合/默认入站 handler） | Gateway、MemRouteTable、MemNodeTransport、TCPNodeTransport | Gateway 无状态水平扩；7002 仅集群内 | §12/§13 |
| `internal/obs` | slog 结构化日志、Prometheus 指标、pprof 端点、traceID 生成与透传 | Logger、Metrics、Tracer | 横切，无业务状态 | §11.1/§11.2 |
| `pkg/framework` | 业务唯一可依赖面：Module/Actor/RoomLogic 接口、Route 选项、错误码、系统 msgID、App 构造器 | 稳定 API 包 | 接口定义，无实现 | §14 |
| `games/snake` | 示例：proto + SnakeGame(实现 RoomLogic/TimeoutLogic) + SnakeModule(注册) | 业务代码 | 仅在 Actor goroutine 内执行 | §14 |
| `bench/bot` | 压测客户端：N 连接、真实消息序列、慢消费者注入、断线重连 | bot | 多 goroutine 模拟 | §15.1 |

---

## 3. 接口清单（Go）

> 包名以 `fw "github.com/rangame/server/pkg/framework"` 为业务入口；internal 接口给出实现侧完整签名。

### 3.1 transport（`internal/transport`）

```go
package transport

// Conn 屏蔽 TCP / WebSocket / 内部 node-to-node 差异（§3.1 / §12.3）
type Conn interface {
    ID() uint64
    Read(ctx context.Context) ([]byte, error) // 已完成帧拆包，返回一帧 body+head
    Push(data []byte) error                    // 非阻塞；投写 channel，满即慢消费者策略
    Close(reason string) error
    RemoteAddr() string
    Meta() *sync.Map
}

type Acceptor interface {
    Addr() string
    Accept(ctx context.Context) (Conn, error)
    Close() error
}

// FrameCodec 帧编解码：len|ver|msgID|flag|seq|[routeLen|route]body（§4.1）
type FrameCodec interface {
    Encode(f *Frame, w io.Writer) error
    Decode(r io.Reader) (*Frame, error) // 处理 TCP 粘包/半包
}

type Frame struct {
    Ver    uint8
    MsgID  uint32
    Flag   uint8  // bit0-3 codec / bit4 compress / bit5 ack / bit6 snapshot
    Seq    uint32
    Route  string // 调试模式
    Body   []byte
}
```

### 3.2 protocol（`internal/protocol`）

```go
package protocol

type Codec interface {
    Type() byte // Flag bit0-3
    Marshal(msg any) ([]byte, error)
    Unmarshal(data []byte, msg any) error
}

func Register(c Codec)                       // 启动期注册：JSON=0, Protobuf=1
func Get(typ byte) (Codec, error)

// 错误信封构造（§4.3）
func NewErrorEnvelope(seq uint32, id fw.MsgID, code errcode.Code, msg string, body any) *transport.Frame
```

### 3.3 session（`internal/session`）

```go
package session

type Session interface {
    UID() string
    Conn() transport.Conn            // 当前绑定连接（可能为 nil：宽限期掉线）
    Bind(c transport.Conn)           // 重绑（§5.1）
    Unbind() transport.Conn
    RoomID() string
    SetRoomID(id string)

    // 下行（§5.3 双通道）
    PushReliable(msgID fw.MsgID, msg any) error   // 进环形缓冲，带 seq
    PushSnapshot(msgID fw.MsgID, msg any) error   // latest-wins，不占缓冲
    PushRaw(frame *transport.Frame) error         // 已编码直投（广播路径）

    LastAckSeq() uint32
    Replay(fromSeq uint32) int                    // 重连重放差额，返回条数；溢出返回 -1
    PullSnapshots() []*transport.Frame            // 重连拉最新快照

    Touch() time.Time
    Alive(keepAlive time.Duration) bool
}

type Manager interface {
    Create(uid string, c transport.Conn) Session
    Get(uid string) (Session, bool)
    GetByConnID(id uint64) (Session, bool)
    Release(uid string)                 // 宽限期超时；写 recover:room:{uid}（§5.1）
    Reconnect(token string, c transport.Conn) (Session, error) // token 一次性消费（§11.4）
}
```

### 3.4 router（`internal/router`）

```go
package router

type HandlerFunc func(ctx *fw.MsgCtx, req any) (resp any, err error)

type Middleware func(next HandlerFunc) HandlerFunc

type Router interface {
    // 个人消息：Handler 在 PlayerActor 内执行，resp 自动按原 seq 回包（§6.1）
    Register(id fw.MsgID, reqFactory func() any, h HandlerFunc)
    // 房间消息：仅声明投递目标，逻辑在 RoomLogic.OnMessage 内 switch（§6.1）
    RegisterRoom(id fw.MsgID, reqFactory func() any, reliable bool) // reliable=false→快照类
    Use(mw ...Middleware)
    Dispatch(sess session.Session, f *transport.Frame) error        // 读泵入口
}
```

### 3.5 actor（`internal/actor`）

```go
package actor

type ID uint64 // 演进态：{nodeID, localID}（§12.6），本期 uint64

type Envelope struct {
    MsgID   fw.MsgID
    Seq     uint32
    Payload any    // 解码后的请求体；所有权转移，禁止跨 Actor 共享（§7.3 红线 2）
    Sender  ID
    TraceID string
}

type Actor interface {
    Init(ctx fw.ActorCtx)
    OnMessage(ctx fw.ActorCtx, env *Envelope)
    OnStop(ctx fw.ActorCtx)
}

type Mailbox interface {
    Tell(env *Envelope) error   // 非阻塞；满 → 按策略 Drop/Kick；Block 仅停机路径
    Ctx() <-chan *Envelope
    Len() int
    Close()
}

type Policy struct {
    Capacity   int    // 默认 1024
    OnFull     string // drop | kick | block
    DrainBatch int    // 默认 64
}

type Engine interface {
    Spawn(a fw.Actor, policy Policy, parent ID) (ID, error)
    Tell(target ID, env *Envelope) error
    Stop(id ID)
    Get(id ID) (fw.ActorCtx, bool)
    Len() int // 指标
}

// ActorRegistry = §7.5 共享点②：uid→PlayerActor、roomID→RoomActor，RWMutex
type Registry interface {
    BindUID(uid string, id ID)
    LookupUID(uid string) (ID, bool)
    BindRoom(roomID string, id ID)
    LookupRoom(roomID string) (ID, bool)
    Unbind(id ID)
}

type Supervisor interface {
    ReportPanic(id ID, r any)            // 连续 3 次/min → 停 Actor + 踢会话
    WatchChild(id ID)
    OnChildDown(id ID) <-chan ID
}
```

### 3.6 room（`internal/room`）— 业务侧接口在 pkg/framework，此处给实现面

```go
package room

type Player interface {
    UID() string
    Session() session.Session
    Online() bool
}

type Ctx interface { // RoomCtx，仅在 RoomActor goroutine 内使用（免锁）
    RoomID() string
    Members() []Player
    Member(uid string) (Player, bool)
    KV(key string) any
    SetKV(key string, v any)

    Broadcast(msgID fw.MsgID, msg any, exceptUID ...string) // 可靠通道
    BroadcastSnapshot(msgID fw.MsgID, msg any, exceptUID ...string)
    Kick(uid string, code errcode.Code, reason string)
    Close()                                  // → OnEmpty/OnDestroy

    After(d time.Duration, fn func()) timer.Handle
    Every(d time.Duration, fn func()) timer.Handle
    Storage() fw.Storage
}

type Manager interface {
    Create(cfg fw.RoomConfig, logic fw.RoomLogic) (string, error)
    Get(roomID string) (Ctx, bool)
    Join(roomID, uid string) error
    Leave(roomID, uid string)
    Rooms() int // active_rooms 指标 + Drain 判定
}
```

### 3.7 match / timer / storage

```go
package match

type Rule struct {
    Module   string
    Code     string // 玩法内规则码
    Players  int
    ScoreMin int64 // 段位段
    ScoreMax int64
}

type MatchMaker interface {
    Enqueue(ctx context.Context, uid string, rule Rule) (roomID string, err error)
    Cancel(uid string) error
    QueueDepth(module, code string) int // 扩容领先指标
}
```

```go
package timer

type Clock interface { // §15.4 测试可注入
    Now() time.Time
    NewTimer(d time.Duration) (<-chan time.Time, timer.Handle)
    Advance(d time.Duration) // 手动时钟专用
}

type Handle interface { Cancel() }

type Scheduler interface {
    After(owner actor.ID, d time.Duration, fn func()) Handle
    Every(owner actor.ID, d time.Duration, fn func()) Handle
    Cron(owner actor.ID, expr string, fn func()) (Handle, error) // Module Actor 承载
}
```

```go
package storage

type Storage interface {
    Get(ctx context.Context, table, key string, out any) error
    Set(ctx context.Context, table, key string, val any) error          // 异步入写回队列
    SetSync(ctx context.Context, table, key string, val any) error       // 强一致同步路径
    Del(ctx context.Context, table, key string) error
    IncrBy(ctx context.Context, table, key string, delta int64) (int64, error)
    Ranking() Ranking
    Raw() any // 逃生口：绕过写回/熔断保护，业务自负（§10.2）
    Close() error
}

type RankItem struct {
    Member string
    Score  int64
}

type Ranking interface {
    ZAdd(ctx context.Context, table, member string, score int64) error
    ZIncrBy(ctx context.Context, table, member string, delta int64) (int64, error)
    ZRevRange(ctx context.Context, table string, start, stop int64) ([]RankItem, error)
    ZRank(ctx context.Context, table, member string) (int64, error)
}
```

### 3.8 cluster（演进态，`internal/cluster`）

```go
package cluster

type State byte // node_state 指标：1 / 0.5 / 0
const (
    Active State = iota + 1
    Draining
    Drained
    Down
)

type NodeInfo struct {
    ID          string
    Role        string // gateway | logic
    Addr        string
    Modules     []string
    State       State
    ActiveRooms int
    StartedAt   time.Time
    LastBeat    time.Time
}

type NodeRegistry interface { // §13.2
    Register(info NodeInfo) error
    Heartbeat(id string, activeRooms int) error
    SetState(id string, s State) error
    List(role string, onlyActive bool) ([]NodeInfo, error)
    Watch() <-chan ChangeEvent          // Pub/Sub cluster:nodes:change
    Deregister(id string) error
}

type DrainController interface { // §13.3
    Drain(nodeID string, deadline time.Duration) error // preStop 调用
    State() State
    ActiveRooms() int
    WaitDrained(ctx context.Context) error             // active_rooms=0
}
```

### 3.9 pkg/framework（业务稳定面，`pkg/framework`）

```go
package framework

// —— 应用与模块 ——
type Module interface {
    Name() string
    OnLoad(reg *Registry)
}

type App interface {
    RegisterModule(m Module)
    Run() error
}

func New(opts ...Option) App

type Registry struct{ /* internal 句柄 */ }
func (r *Registry) Route(id MsgID, reqFactory func() any, opts ...RouteOption)
func (r *Registry) Cron(expr string, fn func()) error
func (r *Registry) MatchRule(rule match.Rule)
func (r *Registry) RoomLogic(factory func() RoomLogic, cfg RoomConfig)

// 路由选项（§6.1 两套 API 分工）
func ToRoom() RouteOption   // 投递 RoomActor；默认可靠
func ToPlayer() RouteOption
func Snapshot() RouteOption // 快照通道（§5.3）
func MustDeliver() RouteOption // 强一致：满时 Kick 而非 Drop（§15.2）

// —— Actor / Room（业务实现）——
type MsgID uint32

type Envelope struct {
    MsgID   MsgID
    Seq     uint32
    Payload any
    Sender  ID
    TraceID string
}

type Actor interface {
    Init(ctx ActorCtx)
    OnMessage(ctx ActorCtx, env *Envelope)
    OnStop(ctx ActorCtx)
}

type ActorCtx interface {
    Self() ID
    Tell(target ID, env *Envelope) error
    Session() Session         // PlayerActor 专属
    Storage() Storage
    Spawn(child Actor) (ID, error)
    After(d time.Duration, fn func()) TimerHandle
    Every(d time.Duration, fn func()) TimerHandle
}

// RoomLogic 是游戏开发者唯一必须实现的业务接口（§8.1）
type RoomLogic interface {
    OnCreate(r RoomCtx, cfg RoomConfig)
    OnJoin(r RoomCtx, p Player)
    OnMessage(r RoomCtx, p Player, env *Envelope)
    Tick(r RoomCtx, dt time.Duration)
    OnLeave(r RoomCtx, p Player)
    OnEmpty(r RoomCtx)
    OnDestroy(r RoomCtx)
}

// TimeoutLogic 可选实现：房间 MaxDuration 到期强结算（§8.2）
type TimeoutLogic interface {
    OnTimeout(r RoomCtx)
}

type RoomCtx interface {
    RoomID() string
    Members() []Player
    Member(uid string) (Player, bool)
    KV(key string) any
    SetKV(key string, v any)
    Broadcast(msgID MsgID, msg any, exceptUID ...string)
    BroadcastSnapshot(msgID MsgID, msg any, exceptUID ...string)
    Kick(uid string, code errcode.Code, reason string)
    Close()
    After(d time.Duration, fn func()) TimerHandle
    Every(d time.Duration, fn func()) TimerHandle
    Storage() Storage
}

type Player interface {
    UID() string
    Online() bool
    Push(msgID MsgID, msg any) error
}

type Session interface {
    UID() string
    RoomID() string
    Push(msgID MsgID, msg any) error       // 可靠
    PushSnapshot(msgID MsgID, msg any) error
}

type Storage interface { /* 透传 internal/storage.Storage，见 §3.7 */ }

type ID = actor.ID
type TimerHandle = timer.Handle

type RoomConfig struct {
    Module      string
    MaxPlayers  int
    MaxDuration time.Duration // §8.2 排水上界
    Mailbox     MailboxPolicy
    MatchRule   match.Rule
}

type MailboxPolicy struct {
    Capacity   int
    OnFull     string // drop | kick | block
    DrainBatch int
}
```

---

## 4. Proto 文件

### 4.1 文件结构与 msgID 分段

```
proto/framework/
├── envelope.proto   # 错误/响应信封（§4.3）
├── system.proto     # ping/pong、登录、重连、踢人、维护通知
├── room.proto       # 房间/匹配通用消息、RecoverableRoom、Resync
└── snapshot.proto   # 快照通道拉取（§5.3）

games/snake/proto/snake.proto    # 业务消息（§14 示例）
```

**msgID 分段规划**（`pkg/framework/msgid.go` 同步生成常量）：

| 段 | 用途 |
|----|------|
| `0x0000–0x00FF` | 框架系统消息（心跳/登录/信封/快照控制） |
| `0x0100–0x01FF` | 房间/匹配通用消息 |
| `0x0200–0x0FFF` | 框架保留 |
| `0x1000–0x10FF` | module: snake |
| `0x1100–0x11FF` | module: card（以此类推，每 module 256 个） |

### 4.2 `proto/framework/envelope.proto`

```protobuf
syntax = "proto3";
package fw;
option go_package = "github.com/rangame/server/proto/framework;fwpb";

// §4.3 统一错误/响应信封：err 或 RPC 式响应均用此结构，附原请求 seq
message ErrorEnvelope {
  uint32 seq  = 1; // 原请求 seq，客户端做请求级关联
  uint32 code = 2; // 0=成功；1xxx 协议 / 2xxx 会话 / 3xxx 房间 / 4xxx 服务端
  string msg  = 3;
  bytes body  = 4; // 可选业务数据（按会话 Codec 二次编码）
}

message Empty {}
```

### 4.3 `proto/framework/system.proto`

```protobuf
syntax = "proto3";
package fw;
option go_package = "github.com/rangame/server/proto/framework;fwpb";

// §5.2 心跳由读泵直接应答，不进 Actor 邮箱
message Ping { int64 client_ts = 1; }
message Pong { int64 client_ts = 1; int64 server_ts = 2; }

// §5 登录 / 重连
message LoginReq {
  string uid          = 1;
  string token        = 2; // 业务鉴权 token（自有账号/三方）
  uint32 protocol_ver = 3; // §4.1 ver 协商
  uint32 codec        = 4; // 0=JSON 1=PB
  string platform     = 5; // h5 / wx / app ...
}
message LoginResp {
  string reconnect_token = 1; // §11.4 一次性消费
  uint32 heartbeat_ms    = 2; // 服务端下发心跳间隔（默认 15000）
  string session_id      = 3;
}

message ReconnectReq {
  string reconnect_token = 1;
  uint32 last_reliable_seq = 2; // §5.3 可靠通道最后收到的 seq
}
message ReconnectResp {
  string new_reconnect_token = 1; // 旧 token 即作废
  repeated FrameLog replay   = 2; // 可靠通道差额（小包可直接随握手返回；大包改为后续推送）
}

// 重放的单条帧（可靠通道）
message FrameLog {
  uint32 seq   = 1;
  uint32 msgid = 2;
  bytes  body  = 3;
}

// §11.4 / §12.5 / §13.3 服务端主动通知
message KickNotice {
  uint32 code   = 1; // 2004 被踢
  string reason = 2;
}
message MaintenanceNotice { // 4005：停机/Logic 排水/滚动发版
  uint32 code         = 1;
  string reason       = 2;
  uint32 retry_after_ms = 3; // 客户端静默重连提示（指数退避由 SDK 执行）
}
message LogicDownNotice { string trace = 1; } // §12.4
```

### 4.4 `proto/framework/room.proto`

```protobuf
syntax = "proto3";
package fw;
option go_package = "github.com/rangame/server/proto/framework;fwpb";

// §8.2 / §13.5 匹配
message MatchReq {
  string rule_code = 1; // module 由登录态/路由决定
  int64 score      = 2;
}
message MatchResp {
  string room_id = 1;
  uint32 position_in_queue = 2; // §13.8 排队削峰：排队位置
  uint32 est_wait_ms       = 3;
}
message MatchCancelReq { string rule_code = 1; }

// §8 进/退房（框架通用，玩法状态走各自消息）
message JoinRoomReq  { string room_id = 1; }
message LeaveRoomReq { string room_id = 1; }
message RoomMemberChanged {
  string room_id = 1;
  repeated string uids = 2;
  bool joined = 3; // true=进房 false=离开
}

// §5.1 宽限期外的对局找回
message RecoverableRoom {
  string room_id = 1;
  string module  = 2;
  uint32 ttl_ms  = 3; // recover:room:{uid} 剩余 TTL
}

// 玩法自定义全量同步（可靠重放溢出时由服务端触发）
message ResyncReq  { string room_id = 1; }
message ResyncNtf  { string room_id = 1; bytes snapshot = 2; } // 内容由玩法定义
```

### 4.5 `proto/framework/snapshot.proto`

```protobuf
syntax = "proto3";
package fw;
option go_package = "github.com/rangame/server/proto/framework;fwpb";

// §5.3 重连后按 msgID 拉取快照通道最新值（latest-wins）
message PullSnapshotReq { repeated uint32 msgids = 1; }
message SnapshotItem {
  uint32 msgid = 1;
  bytes  body  = 2;
}
message PullSnapshotResp { repeated SnapshotItem items = 1; }
```

### 4.6 `games/snake/proto/snake.proto`（业务示例，§14）

```protobuf
syntax = "proto3";
package snake;
option go_package = "github.com/rangame/server/games/snake/proto;snakepb";

message Vec { float x = 1; float y = 2; }

message MoveReq     { Vec dir = 1; }            // 0x1000 房间消息
message JoinTip     { string uid = 1; string name = 2; } // 0x1001
message Food        { Vec pos = 1; uint32 id = 2; }
message SnakeState  {
  string uid = 1;
  Vec head = 2;
  repeated Vec body = 3;
  uint32 score = 4;
  bool alive = 5;
}
message RoomState { // 0x1010，注册为 Snapshot()（flag bit6，20fps）
  repeated SnakeState snakes = 1;
  repeated Food foods = 2;
  uint32 frame = 3;
}
message GameOverNtf { // 0x1011，可靠消息（结算）
  string winner_uid = 1;
  map<string, uint32> score = 2;
}
```

### 4.7 msgID 常量（`pkg/framework/msgid.go`）

```go
package framework

// 系统段 0x0000–0x00FF
const (
    MsgPing       MsgID = 0x0001
    MsgPong       MsgID = 0x0002
    MsgLogin      MsgID = 0x0010
    MsgReconnect  MsgID = 0x0011
    MsgKick       MsgID = 0x0020
    MsgMaintenance MsgID = 0x0021 // 4005
    MsgLogicDown  MsgID = 0x0022
)

// 房间/匹配段 0x0100–0x01FF
const (
    MsgMatch        MsgID = 0x0100
    MsgMatchCancel  MsgID = 0x0101
    MsgJoinRoom     MsgID = 0x0110
    MsgLeaveRoom    MsgID = 0x0111
    MsgMemberChange MsgID = 0x0112
    MsgRecoverRoom  MsgID = 0x0120
    MsgResyncReq    MsgID = 0x0130
    MsgResyncNtf    MsgID = 0x0131
    MsgPullSnap     MsgID = 0x0140
)
// 业务模块自管 0x1000 起，按 module 每段 256
```

---

## 5. 实现顺序建议

按依赖自底向上，每一阶段都可独立验证（呼应 §15.4 的可测试性）：

| 阶段 | 模块 | 验收 |
|------|------|------|
| 1 | config、obs、protocol（JSON Codec+信封） | 单测：帧/信封编解码 |
| 2 | transport（TCP 先行）、timer（Clock 抽象） | echo server：telnet 收发、超时 |
| 3 | actor（Mailbox/Engine/Supervisor）、router | 单测：Drop/Kick、panic 隔离、批量 drain |
| 4 | session、WS Acceptor | 单测：心跳判死、解绑重绑、双通道重放 |
| 5 | storage（接口+内存 fake）、room、match（simple） | 单机跑通贪吃蛇（§16.4 走查 1–11） |
| 6 | storage redis/mysql + 写回/WAL/熔断 | 故障注入：依赖挂掉降级路径 |
| 7 | protobuf Codec、bench/bot | §15.1 四场景压测达 §15.2 目标 |
| 8（演进，已落地） | cluster、gateway、RedisMatcher + app 装配（GatewayConnector/LogicHandler/RemoteConn） | 角色分派可运行；真实 TCP 双节点端到端通过；K8s 清单可部署；Redis 共享路由表/LogicDown 通知为后续阶段 |

**实现纪律**：单机形态（阶段 1–7）不 import `internal/cluster`、`internal/gateway`——业务代码零演进代码；演进能力只在 `server.role=gateway|logic` 时经 `app.BuildGateway/BuildLogic` 装配启用，games/* 仍只依赖 pkg/framework。
