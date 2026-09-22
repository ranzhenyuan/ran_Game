package transport

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	src := &Frame{
		Ver:   ProtocolVer,
		MsgID: 0x0100,
		Flag:  0, // JSON codec 类型位为 0
		Seq:   42,
		Body:  []byte(`{"hello":"world"}`),
	}
	raw, err := EncodeFrame(src)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := DecodeFrame(bytes.NewReader(raw), DefaultMaxFrame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.MsgID != src.MsgID || got.Seq != src.Seq || got.Flag != src.Flag ||
		string(got.Body) != string(src.Body) || got.Route != "" {
		t.Fatalf("frame mismatch: %+v vs %+v", got, src)
	}
}

func TestFrameRoute(t *testing.T) {
	src := &Frame{
		Ver:   ProtocolVer,
		MsgID: 0x1000,
		Flag:  FlagSnapshot,
		Seq:   7,
		Route: "snake.move",
		Body:  []byte("body"),
	}
	raw, err := EncodeFrame(src)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeFrame(bytes.NewReader(raw), DefaultMaxFrame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Route != "snake.move" || !got.IsSnapshot() || string(got.Body) != "body" {
		t.Fatalf("route/snapshot/body mismatch: %+v", got)
	}
}

func TestDecodeTwoFramesFromOneBuffer(t *testing.T) {
	f1 := &Frame{Ver: ProtocolVer, MsgID: 1, Seq: 1, Body: []byte("aaa")}
	f2 := &Frame{Ver: ProtocolVer, MsgID: 2, Seq: 2, Body: []byte("bbbb")}
	r1, _ := EncodeFrame(f1)
	r2, _ := EncodeFrame(f2)

	// 模拟 TCP 粘包：两帧一次到达，通过 bufio 风格的 bytes.Reader 连续解两帧。
	r := bytes.NewReader(append(r1, r2...))
	g1, err := DecodeFrame(r, DefaultMaxFrame)
	if err != nil {
		t.Fatalf("decode f1: %v", err)
	}
	g2, err := DecodeFrame(r, DefaultMaxFrame)
	if err != nil {
		t.Fatalf("decode f2: %v", err)
	}
	if g1.Seq != 1 || g2.Seq != 2 || string(g2.Body) != "bbbb" {
		t.Fatalf("concat frames mismatch: %+v %+v", g1, g2)
	}
}

func TestDecodeHalfFrame(t *testing.T) {
	f := &Frame{Ver: ProtocolVer, MsgID: 1, Body: make([]byte, 100)}
	raw, _ := EncodeFrame(f)

	// 半包：只到一半字节，ReadFull 应返回 ErrUnexpectedEOF。
	_, err := DecodeFrame(bytes.NewReader(raw[:len(raw)/2]), DefaultMaxFrame)
	if err == nil {
		t.Fatal("expected error on half frame, got nil")
	}
}

func TestFrameTooLarge(t *testing.T) {
	f := &Frame{Ver: ProtocolVer, MsgID: 1, Body: make([]byte, 2048)}
	raw, _ := EncodeFrame(f)

	_, err := DecodeFrame(bytes.NewReader(raw), 1024)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}
