## 1. 基础信息
- 语言：Go 1.24+；模块名`github.com/rangame/server` （目录名 ranGame，go.mod 模块名 server）
- 构建：`go build ./...` 、`go vet ./...` 、`go test ./... -count=1`
- 依赖管理：默认不开 GOTOOLCHAIN 远程拉取；本地构建用`GOTOOLCHAIN=local`
- 不引入新依赖 ，确需新增先问人（当前 go.mod 仅 yaml、go-redis、miniredis、gorilla/websocket、protobuf、mysql、sqlite）
## 2. 目录与依赖方向（红线）
- 实际分层为：`internal/*` （框架内核）→`pkg/framework` （业务稳定面）→`games/*` （业务）
- `games/*` 只允许 import`pkg/framework` ，禁止直接引用 internal 的具体类型（Mailbox、Engine 等不向业务暴露）
- `internal/*` 单向依赖：`app > {router, session, room, match} > actor > {protocol, timer, storage}` ；禁止反向/环依赖
- 演进态包（`internal/cluster` 、`internal/gateway` ）仅在`server.role=gateway|logic` 时由`app.BuildGateway/BuildLogic` 装配；单机形态不引用
- 不存在`service/domain/repo/ad` 分层；不要套用通用业务后端目录
## 3. 并发安全红线（框架最高原则）
- Actor 状态只允许被本 Actor 的 goroutine 访问 （免锁）；这是性能根基，违反即数据竞争
- 跨 Actor 只能传消息（Tell/Envelope），禁止传可变指针/引用 ；Envelope 中 payload 视作所有权转移
- 全局只读数据（配置表）启动后只读，或用`atomic`
- 仅允许的跨 goroutine 共享点（已内置同步）：Session 下行缓冲、Actor 注册表、Conn 写 channel；除此之外任何共享可变状态都违反红线
- CI 基线：`go test -race`
## 4. 存储（internal/storage）
- 货币类（Coin/Gem）必须用`Storage.IncrBy` （原子计数），禁止整档案`Save` 覆盖
- `Player.Profile()` 只读快照；修改走`ProfileStore.Patch` ，房间侧 Patch 经 PlayerActor 邮箱（单写者）
- Redis 用已有 client，不新加连接池配置（go-redis 默认 dialer 自带 KeepAlive）
- 外部副作用（发奖励/扣次数/写记录）必须可重放、幂等
## 5. 演进态约定
- Gateway 是「哑管道」：只解析帧头，永不反序列化玩法消息（登录帧取 uid 是唯一例外）
- Logic 用 StatefulSet 稳定 nodeID；Gateway 用 Deployment
- 节点间 7002 端口不挂 Service，按注册表通告的 PodIP 懒拨号
- 房间必须有 MaxDuration 上界，为缩容排水提供时间上界
- 拆分对业务代码零改动：Module/RoomLogic/Actor 接口不变
## 6. 编码规范
- 错误必须 wrap：`fmt.Errorf("xxx: %w", err)`
- 关键逻辑、业务约束、绕坑点用简明中文注释说明
- 不写`// TODO: optimize` 这类无信息量注释 ；TODO 必须写明触发条件与责任人
- ws 连接只接受 Binary 帧，拒绝 Text 帧（已有 ErrTextFrameNotSupported）
- 测试用真实 redis 或 miniredis， 不允许用 sleep 模拟时间 （用 timer.Clock 注入）