# 通用小游戏后端框架 — 架构设计文档编写计划

## Context（背景）

用户要在空项目 `d:\TRAE_learning\pro_06\ranGame` 中设计一个通用小游戏后端框架，要求高并发、易扩展。经确认，本期**仅交付架构设计文档**（不写框架代码），技术方向已锁定：

- **接入协议**：WebSocket + TCP 双协议，统一抽象，可切换
- **架构规模**：单机多核 Actor 模型（每玩家/房间一个 goroutine + 邮箱队列），预留分布式扩展接口但本期不做分布式
- **序列化**：可插拔 Codec 接口，内置 JSON 与 Protobuf，消息头带序列化类型标记
- **交付物**：一份中文架构设计文档（Markdown）

## 交付物

唯一文件：`docs/architecture.md`（中文，约 14 章）

## 文档章节大纲

1. **概述** — 目标（通用/高并发/易扩展）、非目标（本期不做分布式）、术语表
2. **总体架构** — 分层图（接入→会话→编解码→路由→Actor 引擎→游戏模块）+ 消息端到端流转图
3. **接入层** — Connection 统一抽象、WS/TCP Acceptor、TLS、每连接 1 读泵 + 1 写聚合协程
4. **协议与序列化** — 帧格式 `len|msgID|flag(序列化类型/压缩)|seq|body`；Codec 接口；JSON/Protobuf 注册与协商规则
5. **会话管理** — Session 与 Connection 解绑、心跳（应用层 ping/pong）、断线重连（token 找回 Session、重绑 Actor）、按 seq 重放未确认下行（可选开关）
6. **消息路由** — msgID 注册表（`router.Register(msgID, handler)`，编译期安全、零反射）、中间件管道（限流/日志/recover/traceID 注入）、uid→PlayerActor 定位
7. **Actor 引擎**（核心章节）— Actor 接口与生命周期钩子（`Init/OnMessage/OnClose/OnStop`）、邮箱（有界 channel 1024 可配）、背压策略（满时 Drop 记指标 / Kick 断连可配、每轮 drain 批量 K 条、水位上报）、panic 隔离与 supervisor
8. **房间与匹配** — RoomActor 通用骨架（Join/Leave/Broadcast）、RoomManager、匹配队列接口、空房回收
9. **定时器与调度** — 最小堆 vs 时间轮选型对比、定时器回调投递到所属 Actor 邮箱保证串行
10. **数据持久化** — Repository 接口、Redis（会话/排行榜）+ MySQL（账号/对局）适配建议、异步写回队列
11. **可观测性与运维** — 日志规范、Prometheus 指标（连接数、QPS、mailbox_len 直方图、drop/kick 计数）、pprof、traceID 贯穿全链路、优雅停机编排（SIGTERM→停 Acceptor→排空 Actor 邮箱（全局超时）→storage flush→退出）
12. **扩展指南** — 新增一个游戏的四步流程 + 接口伪代码（见下）
13. **非功能设计** — 压测方案（自定义 Go bot 客户端模拟 N 连接 + 随机慢消费者；场景：纯心跳/单房广播风暴/全员随机消息）、容量估算、指标与目标（4C8G 单机 5w 长连接心跳、1w msg QPS、路由+调度 P99 < 50ms）
14. **附录** — 与 nano/leaf/Pitaya 设计对照、风险清单（逐条附对策）、预留分布式接口的本期边界标注

## 关键设计决策（文档中必须写明）

- **消息全链路**：上行 Conn 读泵→解帧→Codec 解码→Session 校验→Router 中间件→`playerActor.Tell(envelope)` 非阻塞投递→Actor 执行→`roomActor.Tell` / `session.Push()`；下行 Actor 内编码（sync.Pool 复用 buffer）→Conn 写 channel→写协程聚合 flush
- **并发安全红线**：Actor 状态仅本 goroutine 可变访问（免锁）；跨 Actor 只能消息传递，禁传可变指针；只读全局配置启动后只读/atomic。文档需写明违例后果
- **高并发手段清单**：每连接 1 读泵 + 1 写协程、sync.Pool 复用、令牌桶限流（per-conn + 全局）、预留 Transport 接口便于后续替换 gnet/netpoll、批量写、TCP_NODELAY 可配
- **扩展接口伪代码**（第 12 章落点）：

```go
type Actor interface {
    Init(ctx *ActorCtx)
    OnMessage(ctx *ActorCtx, env *Envelope)
    OnStop(ctx *ActorCtx)
}
type RoomLogic interface {        // 游戏只需实现此接口
    OnJoin(r *RoomCtx, p Player)
    OnMessage(r *RoomCtx, p Player, env *Envelope)
    Tick(r *RoomCtx, dt time.Duration)
    OnLeave(r *RoomCtx, p Player)
}
type Module interface {           // 游戏装配入口
    Name() string
    OnLoad(reg *Registry)         // reg.Route(msgID, fn); reg.SpawnRoom(logic); reg.Cron(...)
}
```

新增游戏四步：定义 proto 消息 → 实现 RoomLogic → 写 Module 注册路由与房管 → main 中 `module.Register(&SnakeModule{})`

- **规划目录结构**（文档第 2/14 章附图标注为规划，本期不建）：`cmd/server`、`internal/{server,transport,session,protocol,router,actor,timer,room,storage,obs}`、`pkg/framework`、`games/snake`

## 图表要求

文档内用 Mermaid 表达：总体分层架构图（第 2 章）、上行/下行消息时序图（第 2 章）、Actor 邮箱与背压状态图（第 7 章）、优雅停机流程图（第 11 章）。

## 验证方式（文档自检）

1. **附录放"贪吃蛇全链路走查表"**：握手→登录→匹配→上行→广播→断线→恢复→停机，逐环节标注对应章节，检查闭环无断点
2. 用第 12 章接口签名手写一遍贪吃蛇调用序列，验证接口自洽（伪代码编译级推演）
3. 风险清单逐条附对策；每个"预留分布式接口"标注本期边界，防止过度设计

## 明确不做

- 不创建任何 Go 代码文件、不建 internal/games 目录
- 不做分布式实现细节设计（仅标注扩展点）
- 不生成 README
