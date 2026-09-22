// Package protocol 实现序列化 Codec 插件机制（架构文档 §4.2）。
// 服务端按上行帧 flag 低 4 位选择 Codec；下行沿用会话记录的类型。
package protocol

import (
	"errors"
	"sync"
)

// Codec 序列化插件。Type 与帧 flag 低 4 位对应。
type Codec interface {
	Type() byte
	Marshal(msg any) ([]byte, error)
	Unmarshal(data []byte, msg any) error
}

const (
	TypeJSON     byte = 0
	TypeProtobuf byte = 1
)

var (
	codecsMu sync.RWMutex
	codecs   = map[byte]Codec{}

	errCodecNotFound = errors.New("protocol: codec type not registered")
)

// Register 注册 Codec（启动期调用）。重复注册同类型直接覆盖，便于测试替换。
func Register(c Codec) {
	codecsMu.Lock()
	defer codecsMu.Unlock()
	codecs[c.Type()] = c
}

// Get 按类型取 Codec。
func Get(typ byte) (Codec, error) {
	codecsMu.RLock()
	defer codecsMu.RUnlock()
	c, ok := codecs[typ]
	if !ok {
		return nil, errCodecNotFound
	}
	return c, nil
}

// MustGet 取 Codec，不存在时 panic（仅用于启动期装配）。
func MustGet(typ byte) Codec {
	c, err := Get(typ)
	if err != nil {
		panic(err)
	}
	return c
}
