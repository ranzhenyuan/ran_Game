package protocol

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// ProtoCodec 基于 protowire 的反射式 protobuf 编解码器（Type=1）。
//
// 设计决策：不依赖 protoc 生成的 .pb.go 代码，而是用 Go 反射 + struct tag
// 直接编码/解码 protobuf wire format。输出与标准 protobuf 二进制完全兼容，
// 任何标准 protobuf 客户端均可互操作。
//
// struct tag 格式（标准 protobuf tag）：
//
//	protobuf:"varint,1,opt,name=dir,proto3"
//	protobuf:"bytes,2,opt,name=uid,proto3"
//	protobuf:"bytes,3,rep,name=snakes,proto3"   // rep=repeated
//	protobuf:"fixed64,4,opt,name=ts,proto3"
//	protobuf:"fixed32,5,opt,name=f,proto3"
//
// 线类型映射：
//
//	varint   → int*/uint*/bool/enum
//	fixed64  → float64
//	fixed32  → float32
//	bytes    → string/[]byte/嵌套 message
type ProtoCodec struct{}

func (ProtoCodec) Type() byte { return TypeProtobuf }

// ---- Marshal ----

func (ProtoCodec) Marshal(msg any) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	v := reflect.ValueOf(msg)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("proto: marshal expects struct, got %s", v.Kind())
	}
	return marshalStruct(nil, v)
}

func marshalStruct(buf []byte, v reflect.Value) ([]byte, error) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("protobuf")
		if tag == "" {
			continue // 跳过无 protobuf tag 的字段（如 MessageState 等内部字段）
		}
		fv := v.Field(i)
		if !fv.CanInterface() {
			continue
		}
		ft, fn, repeated, err := parseProtoTag(tag)
		if err != nil {
			return nil, fmt.Errorf("proto: field %s: %w", field.Name, err)
		}
		if repeated {
			buf, err = marshalRepeated(buf, fn, ft, fv)
		} else {
			buf, err = marshalField(buf, fn, ft, fv)
		}
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// marshalField 编码单值字段；proto3 语义下零值不编码。
func marshalField(buf []byte, fn int, ft string, fv reflect.Value) ([]byte, error) {
	if isZeroValue(fv) {
		return buf, nil // proto3：零值不编码
	}
	return marshalSingleValue(buf, fn, ft, fv)
}

func marshalSingleValue(buf []byte, fn int, ft string, fv reflect.Value) ([]byte, error) {
	switch ft {
	case "varint":
		var n uint64
		switch fv.Kind() {
		case reflect.Bool:
			if fv.Bool() {
				n = 1
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n = uint64(fv.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			n = fv.Uint()
		default:
			return nil, fmt.Errorf("proto: varint field kind %s", fv.Kind())
		}
		buf = protowire.AppendTag(buf, protowire.Number(fn), protowire.VarintType)
		buf = protowire.AppendVarint(buf, n)
	case "fixed64":
		if fv.Kind() != reflect.Float64 {
			return nil, fmt.Errorf("proto: fixed64 field kind %s", fv.Kind())
		}
		buf = protowire.AppendTag(buf, protowire.Number(fn), protowire.Fixed64Type)
		buf = protowire.AppendFixed64(buf, math.Float64bits(fv.Float()))
	case "fixed32":
		if fv.Kind() != reflect.Float32 {
			return nil, fmt.Errorf("proto: fixed32 field kind %s", fv.Kind())
		}
		buf = protowire.AppendTag(buf, protowire.Number(fn), protowire.Fixed32Type)
		buf = protowire.AppendFixed32(buf, math.Float32bits(float32(fv.Float())))
	case "bytes":
		var data []byte
		switch fv.Kind() {
		case reflect.String:
			data = []byte(fv.String())
		case reflect.Slice:
			if fv.Type().Elem().Kind() == reflect.Uint8 {
				// []byte
				data = fv.Bytes()
			} else {
				return nil, fmt.Errorf("proto: bytes slice elem %s", fv.Type().Elem().Kind())
			}
		case reflect.Struct:
			inner, err := marshalStruct(nil, fv)
			if err != nil {
				return nil, err
			}
			data = inner
		case reflect.Ptr:
			if fv.IsNil() {
				return buf, nil
			}
			inner, err := marshalStruct(nil, fv.Elem())
			if err != nil {
				return nil, err
			}
			data = inner
		default:
			return nil, fmt.Errorf("proto: bytes field kind %s", fv.Kind())
		}
		buf = protowire.AppendTag(buf, protowire.Number(fn), protowire.BytesType)
		buf = protowire.AppendString(buf, string(data))
	default:
		return nil, fmt.Errorf("proto: unknown wire type %q", ft)
	}
	return buf, nil
}

// marshalRepeated 编码 repeated 字段（proto3 非打包模式：每元素独立 tag+value）。
func marshalRepeated(buf []byte, fn int, ft string, fv reflect.Value) ([]byte, error) {
	n := fv.Len()
	for i := 0; i < n; i++ {
		var err error
		buf, err = marshalSingleValue(buf, fn, ft, fv.Index(i))
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// ---- Unmarshal ----

func (ProtoCodec) Unmarshal(data []byte, msg any) error {
	v := reflect.ValueOf(msg)
	if v.Kind() != reflect.Ptr {
		return errors.New("proto: unmarshal requires pointer")
	}
	if v.IsNil() {
		return errors.New("proto: unmarshal target is nil")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("proto: unmarshal expects struct, got %s", v.Kind())
	}
	return unmarshalStruct(data, v)
}

func unmarshalStruct(data []byte, v reflect.Value) error {
	t := v.Type()
	// 构建 field number → field index 映射
	fields := make(map[protowire.Number]int)
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("protobuf")
		if tag == "" {
			continue
		}
		_, fn, _, err := parseProtoTag(tag)
		if err != nil {
			continue
		}
		fields[protowire.Number(fn)] = i
	}

	for len(data) > 0 {
		num, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]

		fieldIdx, ok := fields[num]
		if !ok {
			// 未知字段：跳过（proto3 前向兼容）
			n := protowire.ConsumeFieldValue(num, wireType, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}

		fv := v.Field(fieldIdx)
		ft := t.Field(fieldIdx).Tag.Get("protobuf")
		_, _, repeated, _ := parseProtoTag(ft)

		n, err := unmarshalField(data, wireType, fv, repeated)
		if err != nil {
			return fmt.Errorf("proto: field %s: %w", t.Field(fieldIdx).Name, err)
		}
		data = data[n:]
	}
	return nil
}

func unmarshalField(data []byte, wireType protowire.Type, fv reflect.Value, repeated bool) (int, error) {
	if repeated {
		return unmarshalRepeated(data, wireType, fv)
	}
	return unmarshalSingle(data, wireType, fv)
}

func unmarshalSingle(data []byte, wireType protowire.Type, fv reflect.Value) (int, error) {
	switch wireType {
	case protowire.VarintType:
		val, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		switch fv.Kind() {
		case reflect.Bool:
			fv.SetBool(val != 0)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fv.SetInt(int64(val))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fv.SetUint(val)
		default:
			return 0, fmt.Errorf("proto: varint into %s", fv.Kind())
		}
		return n, nil
	case protowire.Fixed64Type:
		val, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		if fv.Kind() == reflect.Float64 {
			fv.SetFloat(math.Float64frombits(val))
			return n, nil
		}
		return 0, fmt.Errorf("proto: fixed64 into %s", fv.Kind())
	case protowire.Fixed32Type:
		val, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		if fv.Kind() == reflect.Float32 {
			fv.SetFloat(float64(math.Float32frombits(val)))
			return n, nil
		}
		return 0, fmt.Errorf("proto: fixed32 into %s", fv.Kind())
	case protowire.BytesType:
		val, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return 0, protowire.ParseError(n)
		}
		switch fv.Kind() {
		case reflect.String:
			fv.SetString(string(val))
		case reflect.Slice:
			if fv.Type().Elem().Kind() == reflect.Uint8 {
				// []byte
				b := make([]byte, len(val))
				copy(b, val)
				fv.SetBytes(b)
			} else {
				return 0, fmt.Errorf("proto: bytes into slice of %s", fv.Type().Elem().Kind())
			}
		case reflect.Struct:
			err := unmarshalStruct(val, fv)
			if err != nil {
				return 0, err
			}
		case reflect.Ptr:
			if fv.IsNil() {
				fv.Set(reflect.New(fv.Type().Elem()))
			}
			if err := unmarshalStruct(val, fv.Elem()); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("proto: bytes into %s", fv.Kind())
		}
		return n, nil
	default:
		return 0, fmt.Errorf("proto: unsupported wire type %d", wireType)
	}
}

func unmarshalRepeated(data []byte, wireType protowire.Type, fv reflect.Value) (int, error) {
	if fv.Kind() != reflect.Slice {
		return 0, fmt.Errorf("proto: repeated field is %s, not slice", fv.Kind())
	}
	// 当前 slice 长度，append 一个元素
	elem := reflect.New(fv.Type().Elem()).Elem()
	n, err := unmarshalSingle(data, wireType, elem)
	if err != nil {
		return 0, err
	}
	fv.Set(reflect.Append(fv, elem))
	return n, nil
}

// ---- 辅助 ----

// parseProtoTag 解析 protobuf struct tag，返回 (wireType, fieldNumber, repeated, err)。
// 格式："varint,1,opt,name=dir,proto3" 或 "bytes,3,rep,name=snakes,proto3"
func parseProtoTag(tag string) (wireType string, fieldNum int, repeated bool, err error) {
	parts := strings.Split(tag, ",")
	if len(parts) < 2 {
		return "", 0, false, errors.New("proto: tag needs at least wireType,fieldNum")
	}
	wireType = parts[0]
	fieldNum, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, false, fmt.Errorf("proto: field number: %w", err)
	}
	for _, opt := range parts[2:] {
		if opt == "rep" || opt == "repeated" {
			repeated = true
		}
	}
	return
}

func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.String:
		return v.String() == ""
	case reflect.Slice, reflect.Array:
		return v.Len() == 0
	case reflect.Ptr:
		return v.IsNil()
	case reflect.Struct:
		// 非零结构体始终编码（proto3 message 字段的语义）
		return false
	default:
		return false
	}
}

func init() {
	Register(ProtoCodec{})
}
