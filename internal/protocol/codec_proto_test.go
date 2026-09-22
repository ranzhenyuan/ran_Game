package protocol_test

import (
	"testing"

	"github.com/rangame/server/games/snake"
	"github.com/rangame/server/internal/match"
	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/session"
	"github.com/rangame/server/pkg/framework"
)

// 1) 简单消息往返：MoveReq
func TestProtoCodecMoveReq(t *testing.T) {
	c := protocol.ProtoCodec{}
	orig := snake.MoveReq{Dir: 3}
	data, err := c.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("proto output should not be empty for non-zero value")
	}
	var got snake.MoveReq
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Dir != orig.Dir {
		t.Fatalf("dir: got %d want %d", got.Dir, orig.Dir)
	}
}

// 2) 零值不编码（proto3 语义）
func TestProtoCodecZeroOmission(t *testing.T) {
	c := protocol.ProtoCodec{}
	orig := snake.MoveReq{Dir: 0}
	data, err := c.Marshal(&orig)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("zero value should produce empty output, got %d bytes", len(data))
	}
}

// 3) 嵌套消息 + repeated：SnakeState → StateNtf
func TestProtoCodecNestedRepeated(t *testing.T) {
	c := protocol.ProtoCodec{}
	orig := snake.StateNtf{
		Tick: 42,
		Snakes: []snake.SnakeState{
			{
				UID:   "p1",
				Body:  []snake.Point{{X: 3, Y: 7}, {X: 4, Y: 7}},
				Alive: true,
				Score: 5,
			},
			{
				UID:   "p2",
				Body:  []snake.Point{{X: 11, Y: 7}},
				Alive: false,
				Score: 0,
			},
		},
		Food: snake.Point{X: 5, Y: 5},
	}
	data, err := c.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got snake.StateNtf
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Tick != orig.Tick {
		t.Fatalf("tick: %d != %d", got.Tick, orig.Tick)
	}
	if len(got.Snakes) != 2 {
		t.Fatalf("snakes len: %d != 2", len(got.Snakes))
	}
	s1 := got.Snakes[0]
	if s1.UID != "p1" || !s1.Alive || s1.Score != 5 || len(s1.Body) != 2 {
		t.Fatalf("snake1 mismatch: %+v", s1)
	}
	if s1.Body[0].X != 3 || s1.Body[0].Y != 7 || s1.Body[1].X != 4 {
		t.Fatalf("body points: %+v", s1.Body)
	}
	s2 := got.Snakes[1]
	if s2.UID != "p2" || s2.Alive || s2.Score != 0 || len(s2.Body) != 1 {
		t.Fatalf("snake2 mismatch: %+v", s2)
	}
	if got.Food.X != 5 || got.Food.Y != 5 {
		t.Fatalf("food: %+v", got.Food)
	}
}

// 4) ErrorEnvelope 往返（含 Body 嵌套编码）
func TestProtoCodecEnvelope(t *testing.T) {
	c := protocol.ProtoCodec{}
	inner := match.Response{RoomID: "abc123"}
	innerBytes, err := c.Marshal(&inner)
	if err != nil {
		t.Fatal(err)
	}
	orig := protocol.ErrorEnvelope{
		Seq:  42,
		Code: framework.OK,
		Body: innerBytes,
	}
	data, err := c.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got protocol.ErrorEnvelope
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Seq != orig.Seq || got.Code != orig.Code {
		t.Fatalf("envelope: %+v != %+v", got, orig)
	}
	var innerGot match.Response
	if err := c.Unmarshal(got.Body, &innerGot); err != nil {
		t.Fatalf("inner unmarshal: %v", err)
	}
	if innerGot.RoomID != "abc123" {
		t.Fatalf("inner room: %s", innerGot.RoomID)
	}
}

// 5) 登录请求往返（uint32 codec 字段）
func TestProtoCodecLoginReq(t *testing.T) {
	c := protocol.ProtoCodec{}
	orig := session.LoginReq{
		UID:         "player1",
		Token:       "tok-abc",
		ProtocolVer: 1,
		Codec:       1,
		Platform:    "h5",
	}
	data, err := c.Marshal(&orig)
	if err != nil {
		t.Fatal(err)
	}
	var got session.LoginReq
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.UID != orig.UID || got.Token != orig.Token || got.Codec != 1 || got.Platform != "h5" {
		t.Fatalf("login: %+v != %+v", got, orig)
	}
}

// 6) protobuf 比 JSON 更紧凑
func TestProtoCodecSmallerThanJSON(t *testing.T) {
	jc := protocol.JSONCodec{}
	pc := protocol.ProtoCodec{}
	msg := snake.StateNtf{
		Tick: 42,
		Snakes: []snake.SnakeState{
			{UID: "p1", Body: []snake.Point{{X: 3, Y: 7}}, Alive: true, Score: 5},
		},
		Food: snake.Point{X: 5, Y: 5},
	}
	jdata, _ := jc.Marshal(&msg)
	pdata, _ := pc.Marshal(&msg)
	if len(pdata) >= len(jdata) {
		t.Fatalf("proto %d bytes should be < json %d bytes", len(pdata), len(jdata))
	}
}

// 7) 未知字段前向兼容
func TestProtoCodecUnknownFieldSkip(t *testing.T) {
	c := protocol.ProtoCodec{}
	orig := snake.MoveReq{Dir: 3}
	data, err := c.Marshal(&orig)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, 0x98, 0x06, 0x4D) // tag #99 varint=77
	var got snake.MoveReq
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal with unknown field: %v", err)
	}
	if got.Dir != 3 {
		t.Fatalf("dir: %d", got.Dir)
	}
}
