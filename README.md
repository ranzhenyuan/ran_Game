# ranGame Server

通用小游戏后端框架（Go）。Actor 模型 + 长连接接入层 + 房间/匹配抽象 + 可演进集群。架构设计文档见 [docs/architecture.md](docs/architecture.md)。

## 特性

- **单 goroutine 串行 Actor**：每个 PlayerActor/RoomActor 状态由自身 goroutine 独占，跨 Actor 仅通过消息投递
- **有界邮箱 + 三档背压**：Drop / Kick / Block，溢出可观测
- **双通道下行**：可靠帧进 ring buffer 重放，快照帧 latest-wins 不重放
- **协议无关 Codec**：JSON / Protobuf 协商，下行沿用会话上行类型
- **存储降级链路**：异步队列 + 批量 flush + WAL 兜底 + 熔断器
- **演进态集群骨架**：gateway/logic 角色 + NodeRegistry + DrainController + RedisMatcher（单进程内可验证）
- **可观测性内置**：Prometheus 文本导出 + admin API（healthz/readyz/drain/nodes/metrics）+ traceID 透传
- **零外部依赖压测客户端**：bench/bot 四场景（心跳/广播/随机负载/断线重连）

## 快速开始

```powershell
# 运行单机服务器（默认 configs/server.yaml）
go run ./cmd/server -config configs/server.yaml

# 另开终端跑压测客户端
go run ./cmd/benchbot -scenario heartbeat -bots 100 -duration 30s -addr ws://127.0.0.1:7001
```

默认监听：TCP `:7000`、WS `:7001`、admin `127.0.0.1:7100`、pprof `127.0.0.1:6060`。

## 架构总览

```
cmd/server          单机/gateway/logic 入口（按 server.role 分派）
cmd/benchbot        压测客户端 CLI
configs/server.yaml 配置
docs/               架构文档与模块拆解

pkg/framework/      稳定 API 面（GameModule/Storage/MatchRule/MsgID/ErrCode）
games/snake/        内置示例玩法（仅依赖 pkg/framework）

internal/
  actor/            Actor 引擎（单 goroutine 串行 + 有界邮箱 + 监督）
  admin/            运维面 HTTP API
  app/              装配入口（Build / BuildGateway / BuildLogic）
  cluster/          集群演进态（NodeRegistry / DrainController / RedisMatcher）
  config/           YAML 配置
  gateway/          一级路由表 + 内部 Transport
  match/            匹配器（进程内 Maker + Matcher 接口）
  obs/              可观测性（metrics/prometheus/trace/logger/pprof）
  protocol/         帧编解码 + Codec 注册表（JSON/Protobuf）
  room/             RoomActor + RoomManager
  router/           两级路由 + 中间件链
  session/          会话状态机 + ring buffer 重放 + 三重索引
  storage/          Backend 接口 + managed 门面 + WAL + 熔断 + redis/sql 适配
  timer/             定时器调度
  transport/        Conn 抽象 + TCP/WS Acceptor + 帧拆包

proto/              .proto 规范文档（手写 ProtoCodec，不依赖 protoc）
bench/bot/          压测客户端库
```

## 模块导航

| 模块 | 入口文件 | 说明 |
|------|----------|------|
| Actor 引擎 | [internal/actor/engine.go](internal/actor/engine.go) | 单 goroutine 串行 + 有界邮箱 + 监督故障恢复 |
| 会话 | [internal/session/session.go](internal/session/session.go) | 状态机 + ring buffer 重放 + 顶号/宽限/重连 |
| 路由 | [internal/router/router.go](internal/router/router.go) | 两级路由（个人/房间）+ Auth/RateLimit/Recover/Trace/Metric |
| 房间 | [internal/room/room.go](internal/room/room.go) | RoomActor 串行 inbox + 编码一次广播 |
| 匹配 | [internal/match/match.go](internal/match/match.go) | 进程内 Maker + Matcher 接口（可换 RedisMatcher）|
| 存储 | [internal/storage/managed.go](internal/storage/managed.go) | 异步队列 + 批量 flush + WAL + 熔断 |
| 集群 | [internal/cluster/registry.go](internal/cluster/registry.go) | NodeRegistry + DrainController + RedisMatcher |
| 网关 | [internal/gateway/gateway.go](internal/gateway/gateway.go) | 一级路由表 + 内部 Transport |
| 运维 | [internal/admin/admin.go](internal/admin/admin.go) | healthz/readyz/drain/nodes/metrics |
| 可观测 | [internal/obs/](internal/obs/) | metrics + prometheus + trace + logger + pprof |

## 运行与测试

```powershell
# 编译检查
go build ./...

# 全量测试（20 包）
go test ./... -count=1

# 静态检查
go vet ./...
gofmt -l .
go mod tidy
```

## 配置

配置文件 [configs/server.yaml](configs/server.yaml) 顶层段：

| 段 | 说明 |
|----|------|
| `server` | 角色：`standalone`（默认）/ `gateway` / `logic` |
| `tcp` / `ws` | 接入层监听地址与超时 |
| `session` | 宽限期 / ring buffer / 心跳 / 登录帧上限 |
| `storage` | driver（memory/redis/sqlite/mysql）+ 队列/WAL/熔断参数 |
| `cluster` | node_id / modules / drain_deadline / redis_addr / node_ttl |
| `admin` | 运维面 HTTP 地址 + trusted_cidrs |
| `log` / `pprof` | 日志级别 + pprof 端点 |

## 演进态角色

```yaml
# 单机（默认）
server:
  role: standalone

# 集群 gateway
server:
  role: gateway
cluster:
  node_id: "gw-0"
  redis_addr: "127.0.0.1:6379"

# 集群 logic
server:
  role: logic
cluster:
  node_id: "logic-snake-0"
  modules: [snake]
  redis_addr: "127.0.0.1:6379"
```

`redis_addr` 留空时用进程内 MemRegistry（单进程集成测试用）。

## 文档

- [docs/architecture.md](docs/architecture.md) — 完整架构设计（16 章）
- [docs/module-breakdown.md](docs/module-breakdown.md) — 模块拆解与实现计划
- [PROTOCOL.md](PROTOCOL.md) — 接入层协议规格
- [EXTENDING.md](EXTENDING.md) — 新玩法模块接入指南
- [DEPLOY.md](DEPLOY.md) — 部署手册

## 约束

- 依赖方向：`games → pkg/framework`，`internal` 不允许 import `games`
- 阶段 1-9 不实现真实多进程集群（架构文档 §49 约束），演进态骨架在单进程内验证设计可行
- 不主动创建文档；本次文档为用户明确请求
