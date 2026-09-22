package session

// 系统握手消息的 Go 结构（与 module-breakdown §4.3 system.proto 同构）。
// 阶段 7 接入 protoc 生成代码后字段名保持一致；阶段 4 走 JSON Codec。

// LoginReq 登录请求。uid+token 由业务鉴权体系校验；阶段 4 信任 uid（无鉴权后端）。
type LoginReq struct {
	UID         string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
	Token       string `json:"token,omitempty" protobuf:"bytes,2,opt,name=token,proto3"`
	ProtocolVer uint32 `json:"protocol_ver,omitempty" protobuf:"varint,3,opt,name=protocol_ver,proto3"`
	Codec       uint32 `json:"codec,omitempty" protobuf:"varint,4,opt,name=codec,proto3"` // 0=JSON 1=PB
	Platform    string `json:"platform,omitempty" protobuf:"bytes,5,opt,name=platform,proto3"`
}

// LoginResp 登录成功响应（信封 code=0 包装）。
type LoginResp struct {
	ReconnectToken string `json:"reconnect_token" protobuf:"bytes,1,opt,name=reconnect_token,proto3"`
	HeartbeatMS    int    `json:"heartbeat_ms" protobuf:"varint,2,opt,name=heartbeat_ms,proto3"`
	SessionID      string `json:"session_id" protobuf:"bytes,3,opt,name=session_id,proto3"`
}

// ReconnectReq 断线重连请求。token 一次性消费（§11.4）。
type ReconnectReq struct {
	ReconnectToken  string `json:"reconnect_token" protobuf:"bytes,1,opt,name=reconnect_token,proto3"`
	LastReliableSeq uint32 `json:"last_reliable_seq" protobuf:"varint,2,opt,name=last_reliable_seq,proto3"`
}

// ReconnectResp 重连握手响应。
//
// 重放差额帧与最新快照不在本响应体内编码，而是握手成功后由服务端按原始帧
// 原样顺序推送（客户端按普通帧消费、按 seq 去重）——避免 JSON base64 双重编码，
// 且与 Codec 类型无关。对应 proto 注释中的「大包改为后续推送」方式。
type ReconnectResp struct {
	NewReconnectToken string `json:"new_reconnect_token" protobuf:"bytes,1,opt,name=new_reconnect_token,proto3"`
	ResyncRequired    bool   `json:"resync_required" protobuf:"varint,2,opt,name=resync_required,proto3"`
}

// Ping/Pong 应用层心跳（§5.2），读泵直接应答，不进 Actor 邮箱。
type Ping struct {
	ClientTS int64 `json:"client_ts,omitempty" protobuf:"varint,1,opt,name=client_ts,proto3"`
}

// Pong 应用层心跳应答。
type Pong struct {
	ClientTS int64 `json:"client_ts,omitempty" protobuf:"varint,1,opt,name=client_ts,proto3"`
	ServerTS int64 `json:"server_ts,omitempty" protobuf:"varint,2,opt,name=server_ts,proto3"`
}

// PullSnapshotReq 重连后拉取快照通道最新值（§5.3）。msgIDs 为空表示拉全部。
type PullSnapshotReq struct {
	MsgIDs []uint32 `json:"msgids,omitempty"`
}
