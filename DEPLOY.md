# 部署手册

本文档覆盖单机 standalone 与演进态集群两种形态的部署。对应架构文档 §11 / §12 / §13。

## 单机形态（standalone）

默认形态，单进程承载接入层 + 全部业务逻辑。适合开发/测试/小规模生产。

### 启动

```powershell
go build -o bin/server ./cmd/server
./bin/server -config configs/server.yaml
```

或开发期直接 `go run ./cmd/server`。

### 依赖

- **无外部依赖**：默认 `storage.driver=memory`，进程内 KV + ZSET 排行榜
- 可选 Redis/MySQL（见下"存储后端切换"）

### 监听端口（默认）

| 端口 | 用途 |
|------|------|
| TCP `:7000` | TCP 接入（原生客户端） |
| WS `:7001` | WebSocket 接入（浏览器/H5） |
| admin `127.0.0.1:7100` | 运维面 HTTP（healthz/readyz/drain/nodes/metrics） |
| pprof `127.0.0.1:6060` | Go pprof 端点 |

> 演进态 gateway/logic 角色另有内部链路端口 **7002**（`cluster.internal_addr`，仅拆分部署启用，不对客户端暴露）。

### 存储后端切换

```yaml
# Redis
storage:
  driver: redis
  redis_addr: "127.0.0.1:6379"
  redis_db: 0

# MySQL
storage:
  driver: mysql
  mysql_dsn: "game:pass@tcp(127.0.0.1:3306)/game?parseTime=true"
```

后端故障时自动降级：异步队列溢出 → WAL 落盘 → 熔断器 open → 拒绝写入（4003）→ 半开探测 → 恢复。

### 压测

```powershell
# 启动服务器后另开终端
go run ./cmd/benchbot -scenario heartbeat -bots 500 -duration 60s -addr ws://127.0.0.1:7001
go run ./cmd/benchbot -scenario all -bots 100 -duration 30s -addr ws://127.0.0.1:7001 -codec pb
```

四场景见 `bench/bot/scenarios.go`：`heartbeat` / `broadcast` / `random` / `reconnect`。

## 演进态集群（gateway + logic）

Gateway/Logic 拆分已落地：真实跨节点链路（TCPNodeTransport）已接线并经双节点端到端测试，`deploy/k8s/` 提供可直接 apply 的完整清单。一级路由表已支持 Redis 共享实现（`RedisRouteTable`）：配置 `cluster.redis_addr` 后，多 Gateway 副本经同一 Redis 共享 uid→logicNode 映射，已通过双副本端到端验证（gw-a 登录、gw-b 查得并转发）。

### 角色分派

```yaml
# gateway 节点
server:
  role: gateway
cluster:
  node_id: "gw-0"                        # K8s Deployment 建议 "gw-${POD_NAME}"
  redis_addr: "127.0.0.1:6379"           # 留空=进程内 MemRegistry（测试用）
  internal_addr: ":7002"                 # 内部链路监听；留空=进程内 MemTransport（同进程骨架）
  internal_advertise_addr: "10.0.0.11:7002"  # 注册到注册表的对端拨号地址；K8s 用 "${POD_IP}:7002"

# logic 节点
server:
  role: logic
cluster:
  node_id: "logic-snake-0"
  modules: [snake]                       # 本节点服务的玩法
  redis_addr: "127.0.0.1:6379"
  internal_addr: ":7002"
  internal_advertise_addr: "10.0.0.21:7002"
```

| 配置项 | 说明 |
|--------|------|
| `internal_addr` | 内部链路监听地址（约定 7002）。空 = 进程内 MemNodeTransport，Gateway/Logic 同进程直达（骨架/集成测试） |
| `internal_advertise_addr` | 注册到 NodeRegistry 的地址，其他节点按此懒拨号。必须填对端可达地址，**不能**留 `:7002`（通配地址无法拨号）；K8s 由 `POD_IP` 环境变量注入 |

Logic 侧 `ws.enabled` 应置 `false`：Logic 不接客户端，7000/7001 仅为复用单机装配的占位监听。

### Redis 依赖

演进态集群**必须** Redis（NodeRegistry + RedisMatcher）：
- `NodeRegistry`：节点注册与发现，TTL 15s，Pub/Sub 通知变化
- `RedisMatcher`：ZSET 匹配队列 + Lua 脚本原子凑齐

`redis_addr` 留空时退化为进程内 `MemRegistry` + 进程内 Maker，用于单进程集成测试。

### 部署拓扑

```
客户端
  │  TCP :30700 / WS :30701（NodePort）
  ▼
┌──────────────────────────────────────┐
│   Gateway (Deployment ×2，无状态)     │
│  - 接入层（TCP 7000 / WS 7001）       │
│  - 一级路由表（uid → logicNodeID）    │
│  - GatewayConnector：转发/下行回写    │
└───────────────┬──────────────────────┘
                │  内部链路 TCP 7002（PodIP 懒拨号，不挂 Service）
┌───────────────┴──────────────────────┐
│  Logic snake (StatefulSet ×3)        │
│  - LogicHandler + session.RemoteConn │
│  - PlayerActor / RoomActor / Router  │
│  - 仅 7002 / 7100 对集群内暴露        │
└───────────────┬──────────────────────┘
                │
┌───────────────┴──────────────────────┐
│  Redis                               │
│  - NodeRegistry（cluster:node:{id}） │
│  - RedisMatcher                      │
│  - DrainController                   │
└──────────────────────────────────────┘
```

### K8s 部署

完整可 apply 清单在 [`deploy/k8s/`](deploy/k8s/)，按序号执行：

| 文件 | 内容 |
|------|------|
| `00-namespace.yaml` | namespace `rangame` |
| `01-redis.yaml` | Registry/匹配/存储共用 Redis（生产替换为托管实例） |
| `02-configmap.yaml` | `gateway.yaml` / `logic.yaml` 两份配置（已含 internal_addr、POD_IP 注入模板） |
| `03-gateway.yaml` | Deployment×2 + 客户端 Service（NodePort 30700/30701） |
| `04-logic-snake.yaml` | StatefulSet×3 + headless Service（仅 7002/7100） |
| `05-hpa.yaml` | 按 active_rooms 弹性 |
| `06-networkpolicy.yaml` | gateway/logic 两条入站策略 |

```powershell
kubectl apply -f deploy/k8s/00-namespace.yaml
kubectl apply -f deploy/k8s/01-redis.yaml
kubectl apply -f deploy/k8s/02-configmap.yaml
kubectl apply -f deploy/k8s/03-gateway.yaml
kubectl apply -f deploy/k8s/04-logic-snake.yaml
kubectl apply -f deploy/k8s/05-hpa.yaml
kubectl apply -f deploy/k8s/06-networkpolicy.yaml
```

关键装配点（清单已内置，自写清单时勿漏）：

- 两个工作负载均注入 `POD_NAME`（`metadata.name`）与 `POD_IP`（`status.podIP`），配置中 `node_id: "${POD_NAME}"`、`internal_advertise_addr: "${POD_IP}:7002"` 由进程展开环境变量；
- Gateway 容器端口 7000/7001/7002/7100；Logic 只声明 7002/7100（7000/7001 是占位监听，不暴露）；
- Logic `terminationGracePeriodSeconds: 1250`（20min 排水 + 50s 余量），preStop 调本机 `POST :7100/admin/drain`；
- ConfigMap 中 logic 配置 `ws.enabled: false`。

本地 Docker 双容器冒烟用 [`deploy/local/`](deploy/local/)（gateway/logic 各一份 compose 配置，通告地址用容器名 `gateway:7002` / `logic:7002`）。

### 排水缩容（§13.3）

Logic 节点缩容时：
1. K8s preStop → `POST /admin/drain` → DrainController 进入 `Draining` 状态
2. 新建房间不再路由到本节点
3. 等待 `active_rooms=0` + `sessions=0`（或 `drain_deadline` 到期）
4. `terminationGracePeriodSeconds` 必须 ≥ `drain_deadline`
5. 超时未排空 → `ForceDrain` 强制踢出剩余会话

HPA 缩容时配合 `cluster.match_queue_depth` + `active_rooms` 指标做先行信号。

## 运维面 API

admin 端点（必须内网，`trusted_cidrs` 限制来源）：

| 端点 | 方法 | 说明 |
|------|------|------|
| `/healthz` | GET | 存活探针（进程存活即 200） |
| `/readyz` | GET | 就绪探针（Actor 引擎 + 存储就绪） |
| `/metrics` | GET | Prometheus 文本格式指标 |
| `/admin/nodes` | GET | 集群节点列表（仅 gateway/logic） |
| `/admin/drain` | POST | 触发本节点排水（仅 logic） |

**安全基线**：
- `/admin/*` 端点用 `ipGuard` 拒绝外网来源（`X-Forwarded-For` 存在即拒，`RemoteAddr` 非 loopback 即拒）
- 生产建议加 mTLS 或 Service Mesh 策略
- `admin.addr` 默认 `127.0.0.1:7100`；K8s 部署改 `0.0.0.0:7100` + NetworkPolicy 限制

## Prometheus 指标

`/metrics` 导出 §11.2 全部 14 个指标：

| 指标 | 类型 | 说明 |
|------|------|------|
| `online_connections` | gauge | 活跃 TCP/WS 连接数 |
| `online_sessions` | gauge | 已登录会话数 |
| `msg_qps_up` | counter | 上行消息总量 |
| `msg_qps_down` | counter | 下行消息总量 |
| `handler_duration_ms` | histogram | Handler 处理耗时 |
| `mailbox_len` | gauge | Actor 邮箱当前长度 |
| `mailbox_drop` | counter | Drop 策略丢弃数 |
| `mailbox_kick` | counter | Kick 策略触发的 Actor 停止数 |
| `rooms` | gauge | 活跃房间数 |
| `actors` | gauge | 活跃 Actor 总数 |
| `active_rooms` | gauge | 本节点活跃房间数（HPA 信号） |
| `match_queue_depth` | gauge | 匹配队列深度（HPA 信号） |
| `node_state` | gauge | 节点状态：0=active 1=draining 2=drained |
| `panic_total` | counter | Actor panic 恢复次数 |

HPA 示例（按 active_rooms 扩缩 logic）：

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: logic-snake
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: logic-snake
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Pods
    pods:
      metric:
        name: active_rooms
      target:
        type: AverageValue
        averageValue: "20"
```

## 优雅停机

SIGTERM/SIGINT 触发顺序（§11）：

1. 停止接入层（不再接受新连接）
2. **logic 角色**：触发排水（`DrainController.Drain`）→ 等待 `drain_deadline` 或排空
3. Actor 邮箱排空（10s 超时）
4. 存储队列 flush + WAL 重放
5. 退出

`terminationGracePeriodSeconds` 必须 ≥ `drain_deadline` + 10s 余量。

## 参考配置

完整配置项见 [configs/server.yaml](configs/server.yaml)，每项均有注释说明。
