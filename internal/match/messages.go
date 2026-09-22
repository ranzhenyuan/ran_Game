package match

// Request 匹配请求（MsgMatch，0x0100）。
// 段位段 ScoreMin/ScoreMax 可选；阶段 5 简易匹配按 module+code 凑桌。
type Request struct {
	Module string `json:"module" protobuf:"bytes,1,opt,name=module,proto3"`
	Code   string `json:"code,omitempty" protobuf:"bytes,2,opt,name=code,proto3"`
}

// CancelRequest 取消匹配（MsgMatchCancel，0x0101），无业务字段。
type CancelRequest struct{}

// Response 匹配成功通知（由服务端在成桌时主动下发，复用原请求 seq）。
type Response struct {
	RoomID string `json:"room_id" protobuf:"bytes,1,opt,name=room_id,proto3"`
}
