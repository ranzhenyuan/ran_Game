package protocol

import "encoding/json"

// JSONCodec 标准库 JSON 实现，type=0。调试友好，性能弱于 Protobuf。
type JSONCodec struct{}

func (JSONCodec) Type() byte { return TypeJSON }

func (JSONCodec) Marshal(msg any) ([]byte, error) {
	return json.Marshal(msg)
}

func (JSONCodec) Unmarshal(data []byte, msg any) error {
	return json.Unmarshal(data, msg)
}

func init() {
	Register(JSONCodec{})
}
