# 接入层协议规格

本文档定义客户端与服务端之间的二进制帧协议。对应架构文档 §4.1 / §4.2 / §5 / §6。

## 帧格式

```
len(4B) | ver(1B) | msgID(4B) | flag(1B) | seq(4B) | [routeLen(2B) | route] | body
```

| 字段 | 长度 | 说明 |
|------|------|------|
| `len` | 4B | **后续全部字节数**（不含 len 自身）；大端 uint32 |
| `ver` | 1B | 协议版本，当前固定 `0x01` |
| `msgID` | 4B | 消息路由 ID；大端 uint32 |
| `flag` | 1B | 标志位，见下表 |
| `seq` | 4B | 序列号；大端 uint32（可靠通道单调递增） |
| `routeLen` | 2B | 可选；flag bit7=1 时存在；大端 uint16 |
| `route` | routeLen B | 可选；调试模式字符串路由，生产为空 |
| `body` | 变长 | Codec 编码后的消息体 |

默认最大帧 64KB（`DefaultMaxFrame`），超限返回 `ErrFrameTooLarge (1003)`。

## flag 位定义

| bit | 名称 | 含义 |
|-----|------|------|
| 0-3 | CodecMask | 序列化类型：`0`=JSON，`1`=Protobuf |
| 4 | Compress | 预留（压缩标志，未实现） |
| 5 | AckRequired | 需要 ACK（预留） |
| 6 | Snapshot | 快照类消息：latest-wins，**不进重放缓冲** |
| 7 | Route | 帧体携带 routeLen\|route（仅调试模式） |

## Codec 协商

- **上行**：客户端在每帧 flag 低 4 位声明本帧的 codec，服务端按帧适配
- **下行**：服务端沿用会话登录时记录的 codec 类型（`LoginReq.codec` 字段）
- 支持 JSON（`TypeJSON=0`）与 Protobuf（`TypeProtobuf=1`）

## WebSocket 约束

- **只接受 Binary 帧**，Text 帧拒绝握手
- Origin 校验：支持完整 Origin、裸 Host、白名单、通配 `*`
- WS 消息体是**长度前缀帧流**：一个 WS Binary message 可能聚合多帧，客户端必须按字节流解码（否则静默丢帧）

## 消息 ID 分配

```
0x0000–0x00FF  框架系统消息
0x0100–0x01FF  房间/匹配通用消息
0x0200–0x0FFF  框架保留
0x1000+        业务模块，每 module 256 个（moduleIndex 0 → 0x1000，1 → 0x1100 ...）
```

### 系统消息（0x0000–0x00FF）

| MsgID | 名称 | 方向 | 说明 |
|-------|------|------|------|
| `0x0001` | MsgPing | 上行 | 应用层心跳，读泵直回不进 Actor |
| `0x0002` | MsgPong | 下行 | 心跳应答 |
| `0x0010` | MsgLogin | 上行 | 登录请求 |
| `0x0011` | MsgReconnect | 上行 | 断线重连 |
| `0x0020` | MsgKick | 下行 | 顶号通知（4004） |
| `0x0021` | MsgMaintenance | 下行 | 维护/停机通知（4005） |
| `0x0022` | MsgLogicDown | 下行 | Logic 节点故障通知 |

### 房间/匹配消息（0x0100–0x01FF）

| MsgID | 名称 | 方向 | 说明 |
|-------|------|------|------|
| `0x0100` | MsgMatch | 上行 | 匹配请求（ToRoom 语义） |
| `0x0101` | MsgMatchCancel | 上行 | 取消匹配 |
| `0x0110` | MsgJoinRoom | 下行 | 加房通知 |
| `0x0111` | MsgLeaveRoom | 下行 | 退房通知 |
| `0x0112` | MsgMemberChange | 下行 | 成员变更（join/leave） |
| `0x0120` | MsgRecoverRoom | 下行 | 可恢复房间列表 |
| `0x0130` | MsgResyncReq | 上行 | 请求全量重同步 |
| `0x0131` | MsgResyncNtf | 下行 | 重同步通知 |
| `0x0140` | MsgPullSnap | 上行 | 拉取快照通道最新值 |

### 业务模块（0x1000+）

各 module 从 `ModuleBase(moduleIndex)` 起分配 256 个 MsgID。示例 snake（moduleIndex=0）：

| MsgID | 名称 | 方向 | 说明 |
|-------|------|------|------|
| `0x1000` | MsgMove | 上行 | 转向（可靠通道，ToRoom） |
| `0x1001` | MsgState | 下行 | 局面快照（快照通道，latest-wins） |
| `0x1002` | MsgResult | 下行 | 结算（可靠通道） |

## 错误码

错误信封 `{seq, code, msg, body}` 通过 `ErrorEnvelope` 结构返回。

| Code | 名称 | 含义 |
|------|------|------|
| `0` | OK | 成功 |
| `1001` | ErrUnknownMsg | 未知消息 ID |
| `1002` | ErrDecode | 解码失败 |
| `1003` | ErrFrameTooLong | 帧过大 |
| `2001` | ErrUnauthorized | 未鉴权 |
| `2002` | ErrTokenInvalid | 重连 token 无效 |
| `2003` | ErrRateLimited | 限流 |
| `2004` | ErrKicked | 被顶号 |
| `3001` | ErrNotInRoom | 不在房间 |
| `3002` | ErrRoomFull | 房间满 |
| `3003` | ErrGameStart | 游戏已开始 |
| `3004` | ErrInQueue | 已在匹配队列 |
| `4001` | ErrInternal | 内部错误 |
| `4002` | ErrOverloaded | 背压触发，客户端指数退避 |
| `4003` | ErrStorageDegraded | 持久化降级，稍后重试 |
| `4005` | ErrMaintenance | 停机/排水/滚动发版 |

## 关键流程

### 登录

```
Client → Server:  MsgLogin(0x0010) { uid, token, protocol_ver, codec, platform }
Server → Client:  ErrorEnvelope{code=0} wrapping LoginResp { reconnect_token, heartbeat_ms, session_id }
```

- `codec` 字段决定后续下行帧的序列化方式
- `reconnect_token` 一次性消费（重连成功后立即失效，颁发新 token）

### 断线重连

```
Client → Server:  MsgReconnect(0x0011) { reconnect_token, last_reliable_seq }
Server → Client:  ErrorEnvelope{code=0} wrapping ReconnectResp { new_reconnect_token, resync_required }
Server → Client:  [replay 差额帧，按 seq 顺序]
Server → Client:  [最新快照帧（若 resync_required=true）]
```

- ring buffer 溢出判定：`oldest > lastSeq+1`（下一帧已被覆盖）→ `resync_required=true`
- 重放帧按原始 seq 顺序推送，客户端按 seq 去重

### 心跳

```
Client → Server:  MsgPing(0x0001) { client_ts }
Server → Client:  MsgPong(0x0002) { client_ts, server_ts }
```

- 读泵直接应答，不进 Actor 邮箱
- 默认 15s 间隔；超时未收到 Pong 判定连接失效

### 匹配与加房

```
Client → Server:  MsgMatch(0x0100) { module, code }   [seq=N]
Server → Client:  ErrorEnvelope{seq=N, code=0} wrapping Response { room_id }
Server → Client:  MsgJoinRoom(0x0110) + MsgMemberChange(0x0112)
```

- 匹配结果复用原请求 seq 推送
- 成桌后服务端主动推送加房通知

### 快照通道

- `flag bit6=1` 标记为快照帧
- 不进 ring buffer 重放，latest-wins 语义
- 重连时若 ring buffer 溢出，服务端推送最新快照 + 置 `resync_required=true`

## 参考实现

- 帧编解码：[internal/transport/frame.go](internal/transport/frame.go)
- Codec 注册表：[internal/protocol/codec.go](internal/protocol/codec.go) / [codec_proto.go](internal/protocol/codec_proto.go)
- 消息定义：[internal/session/messages.go](internal/session/messages.go) / [internal/match/messages.go](internal/match/messages.go) / [games/snake/messages.go](games/snake/messages.go)
- .proto 规范：[proto/framework/](proto/framework/) / [games/snake/proto/](games/snake/proto/)
