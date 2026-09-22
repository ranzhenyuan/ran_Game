package protocol

import (
	"bytes"
	"testing"

	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

type pingBody struct {
	Name string `json:"name"`
}

func TestJSONCodecRegistry(t *testing.T) {
	c, err := Get(TypeJSON)
	if err != nil {
		t.Fatalf("json codec should be registered: %v", err)
	}
	raw, err := c.Marshal(&pingBody{Name: "abc"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"abc"`)) {
		t.Fatalf("unexpected json: %s", raw)
	}

	var out pingBody
	if err := c.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Name != "abc" {
		t.Fatalf("unmarshal mismatch: %+v", out)
	}

	if _, err := Get(99); err == nil {
		t.Fatal("unknown codec type should error")
	}
}

func TestErrorEnvelopeRoundTrip(t *testing.T) {
	frame, err := NewErrorFrame(framework.MsgLogin, 100, framework.ErrUnauthorized, "", nil)
	if err != nil {
		t.Fatalf("new error frame: %v", err)
	}
	if frame.MsgID != uint32(framework.MsgLogin) || frame.Seq != 100 {
		t.Fatalf("frame header mismatch: %+v", frame)
	}

	// 帧编码 → 解码 → 信封解码（模拟线上全链路）。
	raw, err := transport.EncodeFrame(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	gotFrame, err := transport.DecodeFrame(bytes.NewReader(raw), transport.DefaultMaxFrame)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	env, err := DecodeEnvelope(gotFrame)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Code != framework.ErrUnauthorized || env.Seq != 100 ||
		env.Msg != framework.ErrUnauthorized.Message() {
		t.Fatalf("envelope mismatch: %+v", env)
	}
}

func TestOKEnvelopeWithBody(t *testing.T) {
	frame, err := NewOKFrame(framework.MsgPing, 1, &pingBody{Name: "x"})
	if err != nil {
		t.Fatalf("new ok frame: %v", err)
	}
	env, err := DecodeEnvelope(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Code != framework.OK {
		t.Fatalf("expected code 0, got %d", env.Code)
	}
	var body pingBody
	if err := (JSONCodec{}).Unmarshal(env.Body, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body.Name != "x" {
		t.Fatalf("body mismatch: %+v", body)
	}
}
