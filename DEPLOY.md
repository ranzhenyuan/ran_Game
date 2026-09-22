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

**架构文档 §49 约束**：当前阶段不实现真实多进程集群跨节点长连接。演进态骨架在单进程内验证设计可行性；真实部署需补 node-to-node Transport。

### 角色分派

```yaml
# gateway 节点
server:
  role: gateway
cluster:
  node_id: "gw-0"
  redis_addr: "127.0.0.1:6379"   # 留空=进程内 MemRegistry（测试用）

# logic 节点
server:
  role: logic
cluster:
  node_id: "logic-snake-0"
  modules: [snake]               # 本节点服务的玩法
  redis_addr: "127.0.0.1:6379"
```

### Redis 依赖

演进态集群**必须** Redis（NodeRegistry + RedisMatcher）：
- `NodeRegistry`：节点注册与发现，TTL 15s，Pub/Sub 通知变化
- `RedisMatcher`：ZSET 匹配队列 + Lua 脚本原子凑齐

`redis_addr` 留空时退化为进程内 `MemRegistry` + 进程内 Maker，用于单进程集成测试。

### 部署拓扑

```
                ┌─────────────────────────────────┐
                │       Gateway (Deployment)      │
                │  - 接入层（TCP/WS）              │
                │  - 一级路由表（uid → nodeID）    │
                │  - 转发到 Logic 节点            │
                └────────────┬────────────────────┘
                             │  node-to-node Transport
                ┌────────────┴────────────────────┐
                │                                  │
       ┌────────┴────────┐               ┌──────────┴───────┐
       │ Logic snake-0  │      ...      │ Logic snake-N    │
       │ (StatefulSet)  │               │ (StatefulSet)    │
       │ PlayerActor     │               │ PlayerActor     │
       │ RoomActor       │               │ RoomActor        │
       └────────┬────────┘               └──────────┬───────┘
                │                                   │
                └─────────────┬─────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │   Redis           │
                    │  - NodeRegistry   │
                    │  - RedisMatcher   │
                    │  - DrainController│
                    └───────────────────┘
```

### K8s 部署

#### Logic（StatefulSet）

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: logic-snake
spec:
  serviceName: logic-snake
  replicas: 3
  selector:
    matchLabels: { app: logic-snake }
  template:
    metadata:
      labels: { app: logic-snake }
    spec:
      terminationGracePeriodSeconds: 1250   # 必须 ≥ cluster.drain_deadline（20m=1200s）
      containers:
      - name: server
        image: rangame/server:latest
        args: ["-config", "/etc/server/server.yaml"]
        ports:
        - { containerPort: 7000, name: tcp }
        - { containerPort: 7001, name: ws }
        - { containerPort: 7100, name: admin }
        env:
        - name: POD_ORDINAL
          valueFrom: { fieldRef: { fieldPath: metadata.name } }
        volumeMounts:
        - { name: config, mountPath: /etc/server }
        - { name: wal, mountPath: /data/wal }
        lifecycle:
          preStop:
            exec:
              command: ["curl", "-X", "POST", "http://127.0.0.1:7100/admin/drain"]
  volumeClaimTemplates:
  - metadata: { name: wal }
    spec:
      accessModes: [ReadWriteOnce]
      resources: { requests: { storage: 1Gi } }
  volumes:
  - name: config
    configMap: { name: server-config }
```

`node_id` 建议用环境变量注入 `logic-snake-${POD_ORDINAL}`，保证 StatefulSet 稳定。

#### Gateway（Deployment）

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gateway
spec:
  replicas: 2
  selector:
    matchLabels: { app: gateway }
  template:
    metadata:
      labels: { app: gateway }
    spec:
      containers:
      - name: server
        image: rangame/server:latest
        args: ["-config", "/etc/server/server.yaml"]
        ports:
        - { containerPort: 7000, name: tcp }
        - { containerPort: 7001, name: ws }
        - { containerPort: 7100, name: admin }
        volumeMounts:
        - { name: config, mountPath: /etc/server }
      volumes:
      - name: config
        configMap: { name: server-config }
```

Gateway 无状态，可自由水平扩缩。

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
