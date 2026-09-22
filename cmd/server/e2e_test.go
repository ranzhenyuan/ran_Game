package main_test

// 阶段 5 验收：贪吃蛇全链路走查（架构文档 §16.4 步骤 1-11）。
// 真实 WS 客户端 → 登录/匹配/进房/对战/快照广播/心跳/断线宽限/重连重放/结算落库/空房回收/停机。

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rangame/server/games/snake"
	"github.com/rangame/server/internal/app"
	"github.com/rangame/server/internal/config"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

func mustBuildServer(t *testing.T) *app.Server {
	t.Helper()
	cfg := config.Default()
	cfg.TCP.Addr = "127.0.0.1:0"
	cfg.WS.Enabled = true
	cfg.WS.Addr = "127.0.0.1:0"
	cfg.WS.Path = "/ws"
	cfg.WS.AllowedOrigins = []string{"*"}
	cfg.Session.GracePeriod = config.Duration(60 * time.Second)
	cfg.Session.SweepInterval = config.Duration(time.Hour)
	cfg.Log.Level = "error"

	srv, err := app.Build(&cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		[]framework.GameModule{snake.Module{}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	srv.Run(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

func dial(t *testing.T, srv *app.Server) *websocket.Conn {
	t.Helper()
	d := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	c, _, err := d.Dial("ws://"+srv.WSAddr()+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func writeFrame(t *testing.T, c *websocket.Conn, msgID uint32, seq uint32, body any) {
	t.Helper()
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	f := &transport.Frame{Ver: transport.ProtocolVer, MsgID: msgID, Seq: seq, Body: raw}
	b, err := transport.EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// recvBuf 每个客户端的残余字节缓冲：一个 WS Binary 消息可能聚合了多个协议帧（写聚合），
// 必须按 len 前缀作为字节流连续解码（SDK 同此契约，§3.2）。
var (
	recvBuf  = make(map[*websocket.Conn][]byte)
	recvSeen = make(map[*websocket.Conn][]*transport.Frame)
)

func readFrame(t *testing.T, c *websocket.Conn, timeout time.Duration) *transport.Frame {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	for {
		buf := recvBuf[c]
		if len(buf) > 0 {
			r := bytes.NewReader(buf)
			f, err := transport.DecodeFrame(r, 1<<20)
			if err == nil {
				recvBuf[c] = buf[len(buf)-r.Len():]
				return f
			}
		}
		mt, data, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if mt != websocket.BinaryMessage {
			t.Fatalf("want binary, got %d", mt)
		}
		recvBuf[c] = append(recvBuf[c], data...)
	}
}

type basicEnv struct {
	Code   int    `json:"code"`
	RoomID string `json:"room_id"`
}

// splitEnvelope 区分两类下行：
//   - 自动应答/错误帧：信封包装 {"code":...,"body":{...}}；
//   - 房间主动推送（广播/快照）：业务结构裸体。
func splitEnvelope(f *transport.Frame) (code int, payload []byte) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(f.Body, &probe); err == nil {
		if rawCode, ok := probe["code"]; ok {
			_ = json.Unmarshal(rawCode, &code)
			return code, probe["body"]
		}
	}
	return 0, f.Body
}

func login(t *testing.T, c *websocket.Conn, uid string) string {
	t.Helper()
	writeFrame(t, c, uint32(framework.MsgLogin), 1, map[string]string{"uid": uid})
	f := readFrame(t, c, 3*time.Second)
	if f.MsgID != uint32(framework.MsgLogin) {
		t.Fatalf("login resp msgID=%d", f.MsgID)
	}
	code, payload := splitEnvelope(f)
	var resp struct {
		ReconnectToken string `json:"reconnect_token"`
	}
	if err := json.Unmarshal(payload, &resp); err != nil || code != 0 || resp.ReconnectToken == "" {
		t.Fatalf("login resp: code=%d payload=%s", code, payload)
	}
	return resp.ReconnectToken
}

// waitFor 持续读帧直到 pred 命中；包含先前聚合读入但未匹配的历史帧。
// 命中帧之外的帧经 onOther 回调（可能重复回调，调用方需幂等）。
func waitFor(t *testing.T, c *websocket.Conn, d time.Duration,
	pred func(*transport.Frame) bool, onOther func(*transport.Frame)) *transport.Frame {
	t.Helper()
	seen := recvSeen[c]
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, f := range seen {
			if pred(f) {
				return f
			}
		}
		if onOther != nil {
			for _, f := range seen {
				if !pred(f) {
					onOther(f)
				}
			}
		}
		remaining := time.Until(deadline) + 50*time.Millisecond
		seen = append(seen, readFrame(t, c, remaining))
		recvSeen[c] = seen
	}
	t.Fatalf("frame not matched within %v", d)
	return nil
}

func TestSnakeEndToEndWalkthrough(t *testing.T) {
	srv := mustBuildServer(t)

	// —— 步骤 1-2：建连 + 登录 ——
	c1 := dial(t, srv)
	c2 := dial(t, srv)
	token1 := login(t, c1, "p1")
	token2 := login(t, c2, "p2")
	if token1 == token2 {
		t.Fatal("reconnect tokens must differ")
	}

	// —— 步骤 3：匹配成桌（异步响应）——
	writeFrame(t, c1, uint32(framework.MsgMatch), 2, map[string]string{"module": "snake", "code": "ranked"})
	writeFrame(t, c2, uint32(framework.MsgMatch), 2, map[string]string{"module": "snake", "code": "ranked"})

	var room1, room2 string
	waitFor(t, c1, 3*time.Second,
		func(f *transport.Frame) bool {
			code, payload := splitEnvelope(f)
			var e basicEnv
			if f.MsgID == uint32(framework.MsgMatch) && json.Unmarshal(payload, &e) == nil && code == 0 && e.RoomID != "" {
				room1 = e.RoomID
				return true
			}
			return false
		}, nil)
	waitFor(t, c2, 3*time.Second,
		func(f *transport.Frame) bool {
			code, payload := splitEnvelope(f)
			var e basicEnv
			if f.MsgID == uint32(framework.MsgMatch) && json.Unmarshal(payload, &e) == nil && code == 0 && e.RoomID != "" {
				room2 = e.RoomID
				return true
			}
			return false
		}, nil)
	if room1 != room2 || room1 == "" {
		t.Fatalf("matched rooms differ: %q vs %q", room1, room2)
	}

	// —— 步骤 4：进房广播（成员变更，可靠帧带服务端 seq）——
	var lastReliable uint32
	waitFor(t, c1, 2*time.Second,
		func(f *transport.Frame) bool { return f.MsgID == uint32(framework.MsgMemberChange) },
		func(f *transport.Frame) {
			if f.Flag&transport.FlagSnapshot == 0 && f.Seq > lastReliable {
				lastReliable = f.Seq
			}
		})

	// —— 步骤 5：上行对战（不自动应答）。只让 p1 转向；
	// p1 稍后掉线，其蛇按最后方向托管直行，将在上方撞墙，p2 获胜。——
	writeFrame(t, c1, uint32(snake.MsgMove), 3, map[string]int{"dir": snake.DirUp})

	// —— 步骤 6：Tick → 快照通道广播全量局面 ——
	state := waitFor(t, c2, 3*time.Second,
		func(f *transport.Frame) bool { return f.MsgID == uint32(snake.MsgState) }, nil)
	if state.Flag&transport.FlagSnapshot == 0 {
		t.Fatal("state frame must carry snapshot bit6")
	}
	var st snake.StateNtf
	if err := json.Unmarshal(state.Body, &st); err != nil || len(st.Snakes) != 2 || st.Tick == 0 {
		t.Fatalf("state body wrong: %v body=%s", err, state.Body)
	}

	// —— 步骤 7：心跳读泵直答 ——
	writeFrame(t, c2, uint32(framework.MsgPing), 90, map[string]int64{"client_ts": 123})
	pong := readFrame(t, c2, 2*time.Second)
	if pong.MsgID != uint32(framework.MsgPong) || pong.Seq != 90 {
		t.Fatalf("pong wrong: id=%d seq=%d", pong.MsgID, pong.Seq)
	}

	// —— 步骤 8-9：掉线宽限 + 重连（一次性 token、可靠差额、最新快照）——
	_ = c1.Close()
	time.Sleep(350 * time.Millisecond) // 跨过 ≥3 个 Tick，离线期间产生快照/可能的结算帧

	c1b := dial(t, srv)
	writeFrame(t, c1b, uint32(framework.MsgReconnect), 4, map[string]any{
		"reconnect_token":   token1,
		"last_reliable_seq": lastReliable,
	})
	rc := readFrame(t, c1b, 3*time.Second)
	if rc.MsgID != uint32(framework.MsgReconnect) {
		t.Fatalf("reconnect resp id=%d body=%s", rc.MsgID, rc.Body)
	}
	var rr struct {
		NewToken string `json:"new_reconnect_token"`
		Resync   bool   `json:"resync_required"`
	}
	rcCode, rcPayload := splitEnvelope(rc)
	_ = json.Unmarshal(rcPayload, &rr)
	if rcCode != 0 || rr.NewToken == "" || rr.Resync {
		t.Fatalf("reconnect failed: code=%d %+v", rcCode, rr)
	}

	// 握手后原始帧推送中必须能拿到最新局面快照（bit6）。
	var gotSnapAfterReconnect bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !gotSnapAfterReconnect {
		f := readFrame(t, c1b, time.Until(deadline)+50*time.Millisecond)
		if f.MsgID == uint32(snake.MsgState) && f.Flag&transport.FlagSnapshot != 0 {
			gotSnapAfterReconnect = true
		}
	}
	if !gotSnapAfterReconnect {
		t.Fatal("reconnect did not replay a state snapshot")
	}

	// 旧 token 二次消费必须被拒（2002）。
	c1c := dial(t, srv)
	writeFrame(t, c1c, uint32(framework.MsgReconnect), 5, map[string]any{
		"reconnect_token":   token1,
		"last_reliable_seq": lastReliable,
	})
	bad := readFrame(t, c1c, 2*time.Second)
	badCode, _ := splitEnvelope(bad)
	if bad.MsgID != uint32(framework.MsgReconnect) || badCode != int(framework.ErrTokenInvalid) {
		t.Fatalf("old token reuse should be 2002, got id=%d code=%d", bad.MsgID, badCode)
	}

	// —— 步骤 10：结算（两条蛇撞墙终局，可靠广播 + 落库 + 空房回收）——
	waitFor(t, c2, 10*time.Second,
		func(f *transport.Frame) bool { return f.MsgID == uint32(snake.MsgResult) }, nil)

	var result snake.ResultNtf
	if err := srv.Store().Get(context.Background(), "snake_result", room1, &result); err != nil {
		t.Fatalf("settlement not persisted: %v", err)
	}
	if result.RoomID != room1 || result.Rounds == 0 {
		t.Fatalf("settlement wrong: %+v", result)
	}
	if result.Winner != "p2" {
		t.Fatalf("expected p2 win (p1 disconnected and its snake flew into wall), got %q", result.Winner)
	}
	// 排行榜：p2 +1，正序排名第 0。
	if rank, err := srv.Store().Ranking().ZRank(context.Background(), "snake_score", "p2"); err != nil || rank != 0 {
		t.Fatalf("winner ranking: rank=%d err=%v", rank, err)
	}
	t.Logf("game settled: winner=%q rounds=%d", result.Winner, result.Rounds)

	// 房间空且已 OnEmpty→Close。
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Rooms().Rooms() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if srv.Rooms().Rooms() != 0 {
		t.Fatalf("room not recycled, active=%d", srv.Rooms().Rooms())
	}

	// —— 步骤 11：优雅停机由 t.Cleanup 调用 srv.Shutdown 隐式验证（不得卡死）——
}

// TestProfileEndToEnd 存档链路走查（架构文档 §10.4）：
// 预置档案 → 登录 Load → 对局结算 OnDestroy Patch（PlayerActor 单写者路由）→
// 断线宽限到期 OnRelease 退出强存 → admin /admin/players/{uid} 离线查询 →
// 再登录 Load 读回退出强存的档案。
func TestProfileEndToEnd(t *testing.T) {
	cfg := config.Default()
	cfg.TCP.Addr = "127.0.0.1:0"
	cfg.WS.Enabled = true
	cfg.WS.Addr = "127.0.0.1:0"
	cfg.WS.Path = "/ws"
	cfg.WS.AllowedOrigins = []string{"*"}
	cfg.Session.GracePeriod = config.Duration(400 * time.Millisecond)
	cfg.Session.SweepInterval = config.Duration(100 * time.Millisecond)
	cfg.Profile.CheckpointInterval = config.Duration(200 * time.Millisecond)
	cfg.Admin.Addr = "127.0.0.1:0"
	cfg.Log.Level = "error"

	srv, err := app.Build(&cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		[]framework.GameModule{snake.Module{}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	srv.Run(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	ctx := context.Background()

	// —— 步骤 1：预置 pb 档案（pa 首次登录为零档）——
	if err := srv.Store().SetSync(ctx, "player_profile", "pb", &framework.PlayerProfile{
		UID: "pb", Level: 3, Exp: 42,
		Extra: map[string]any{"snake_total_score": 7},
	}); err != nil {
		t.Fatalf("seed pb: %v", err)
	}

	// —— 步骤 2：登录（PlayerActor Spawn → Load → SetProfile）——
	c1 := dial(t, srv)
	c2 := dial(t, srv)
	login(t, c1, "pa")
	login(t, c2, "pb")

	// —— 步骤 3：匹配成桌 ——
	writeFrame(t, c1, uint32(framework.MsgMatch), 2, map[string]string{"module": "snake", "code": "ranked"})
	writeFrame(t, c2, uint32(framework.MsgMatch), 2, map[string]string{"module": "snake", "code": "ranked"})
	waitMatch := func(c *websocket.Conn) {
		t.Helper()
		waitFor(t, c, 3*time.Second, func(f *transport.Frame) bool {
			code, payload := splitEnvelope(f)
			var e basicEnv
			return f.MsgID == uint32(framework.MsgMatch) && json.Unmarshal(payload, &e) == nil &&
				code == 0 && e.RoomID != ""
		}, nil)
	}
	waitMatch(c1)
	waitMatch(c2)

	// —— 步骤 4：pa 转向上行，约 7 个 Tick 后撞墙结算。
	// 双端保持在线，验证 OnDestroy Patch 路由到存活 PlayerActor（单写者，§10.4.4）。——
	writeFrame(t, c1, uint32(snake.MsgMove), 3, map[string]int{"dir": snake.DirUp})

	// —— 步骤 5：等结算；期间持续记录每条蛇的分数（最后一帧 State = 终局分数）——
	scores := map[string]int{}
	waitFor(t, c2, 10*time.Second,
		func(f *transport.Frame) bool { return f.MsgID == uint32(snake.MsgResult) },
		func(f *transport.Frame) {
			if f.MsgID != uint32(snake.MsgState) {
				return
			}
			var st snake.StateNtf
			if json.Unmarshal(f.Body, &st) == nil {
				for _, s := range st.Snakes {
					scores[s.UID] = s.Score
				}
			}
		})
	t.Logf("settled, final scores: %v", scores)

	// waitProfile 轮询直到 profile 满足断言（异步写链路收敛）。
	waitProfile := func(uid string, check func(framework.PlayerProfile) bool) framework.PlayerProfile {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			var p framework.PlayerProfile
			err := srv.Store().Get(ctx, "player_profile", uid, &p)
			if err == nil && check(p) {
				return p
			}
			if time.Now().After(deadline) {
				t.Fatalf("profile %q not settled: err=%v profile=%+v want scores=%v", uid, err, p, scores)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	// —— 步骤 6：结算 Patch 已落库 ——
	paP := waitProfile("pa", func(p framework.PlayerProfile) bool {
		return p.Extra["snake_total_score"] == float64(scores["pa"])
	})
	pbP := waitProfile("pb", func(p framework.PlayerProfile) bool {
		return p.Extra["snake_total_score"] == float64(scores["pb"])
	})

	// —— 步骤 7：双端断开 → 宽限到期 OnRelease → PlayerActor OnStop 退出强存。
	// 退出强存不得回滚结算 Patch（单写者修复的回归点）。——
	_ = c1.Close()
	_ = c2.Close()
	time.Sleep(900 * time.Millisecond) // grace 400ms + sweep 100ms + 余量

	paP = waitProfile("pa", func(p framework.PlayerProfile) bool {
		return p.Extra["snake_total_score"] == float64(scores["pa"])
	})
	pbP = waitProfile("pb", func(p framework.PlayerProfile) bool {
		return p.Level == 3 && p.Exp == 42 &&
			p.Extra["snake_total_score"] == float64(scores["pb"])
	})
	if paP.UID != "pa" || pbP.UID != "pb" {
		t.Fatalf("uid not persisted: pa=%+v pb=%+v", paP, pbP)
	}
	t.Logf("after release: pa=%+v pb=%+v", paP, pbP)

	// —— 步骤 8：admin 离线查询（ProfileQueryService + /admin/players/{uid}）——
	for _, uid := range []string{"pa", "pb"} {
		resp, err := http.Get("http://" + srv.AdminAddr() + "/admin/players/" + uid)
		if err != nil {
			t.Fatalf("admin query %s: %v", uid, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("admin query %s: status=%d body=%s", uid, resp.StatusCode, body)
		}
		var got framework.PlayerProfile
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("admin query %s decode: %v body=%s", uid, err, body)
		}
		if got.UID != uid {
			t.Fatalf("admin query %s: got uid=%q", uid, got.UID)
		}
		if uid == "pb" && (got.Level != 3 || got.Exp != 42) {
			t.Fatalf("admin query pb: level/exp mismatch: %+v", got)
		}
		if got.Extra["snake_total_score"] != float64(scores[uid]) {
			t.Fatalf("admin query %s: extra mismatch: got=%v want=%v", uid,
				got.Extra["snake_total_score"], scores[uid])
		}
	}

	// —— 步骤 9：再登录 Load 读回（退出强存的档案必须被下次登录加载，
	// 否则 checkpoint/退出强存会把 extra 抹掉）——
	c3 := dial(t, srv)
	login(t, c3, "pa")
	time.Sleep(300 * time.Millisecond) // 覆盖一次 checkpoint（Save 已加载副本）
	_ = c3.Close()
	time.Sleep(900 * time.Millisecond)

	paP2 := waitProfile("pa", func(p framework.PlayerProfile) bool {
		return p.Extra["snake_total_score"] == float64(scores["pa"])
	})
	t.Logf("re-login roundtrip ok: %+v", paP2)
}
