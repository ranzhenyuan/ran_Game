package main_test

// §14 扩展流程验证：card（回合制卡牌）/ quiz（定时器答题）双玩法接入走查。
//
// 验证点：
//   - games/* 仅依赖 pkg/framework，cmd 装配即可用（零 internal 改动）；
//   - 消息 ID 段按 ModuleBase(index) 隔离（0x1100/0x1200）互不冲突；
//   - card：事件驱动回合 + OnLeave 判负 + OnDestroy 存档 Patch；
//   - quiz：RoomCtx.After 定时器出题 + 超时跳题 + 存档 Patch；
//   - 结算落库（card_result/quiz_result）与排行榜。

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/rangame/server/games/card"
	"github.com/rangame/server/games/quiz"
	"github.com/rangame/server/games/snake"
	"github.com/rangame/server/internal/app"
	"github.com/rangame/server/internal/config"
	"github.com/rangame/server/internal/transport"
	"github.com/rangame/server/pkg/framework"
)

// buildMultiGameServer 三模块装配（snake 段 0x1000 / card 0x1100 / quiz 0x1200）。
func buildMultiGameServer(t *testing.T) *app.Server {
	t.Helper()
	cfg := config.Default()
	cfg.TCP.Addr = "127.0.0.1:0"
	cfg.WS.Enabled = true
	cfg.WS.Addr = "127.0.0.1:0"
	cfg.WS.Path = "/ws"
	cfg.WS.AllowedOrigins = []string{"*"}
	cfg.Session.GracePeriod = config.Duration(400 * time.Millisecond)
	cfg.Session.SweepInterval = config.Duration(100 * time.Millisecond)
	cfg.Profile.CheckpointInterval = config.Duration(time.Hour) // 禁用周期 checkpoint，只验结算路径
	cfg.Log.Level = "error"

	srv, err := app.Build(&cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		[]framework.GameModule{snake.Module{}, card.Module{}, quiz.Module{}})
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

// TestModuleMsgIDSegments 消息段按 ModuleBase(index) 隔离（§14 第 2 步约束）。
func TestModuleMsgIDSegments(t *testing.T) {
	if base := framework.ModuleBase(1); card.MsgPlay != base || card.MsgDeal != base+3 {
		t.Fatalf("card msgIDs must start at ModuleBase(1)=%d, got play=%d", base, card.MsgPlay)
	}
	if base := framework.ModuleBase(2); quiz.MsgAnswer != base || quiz.MsgResult != base+2 {
		t.Fatalf("quiz msgIDs must start at ModuleBase(2)=%d, got answer=%d", base, quiz.MsgAnswer)
	}
	if uint32(card.MsgResult) >= uint32(framework.ModuleBase(2)) {
		t.Fatal("card/quiz segments overlap")
	}
}

// matchPair 两客户端匹配指定模块，返回房间 ID。
func matchPair(t *testing.T, srv *app.Server, module string) (string, *websocket.Conn, *websocket.Conn) {
	t.Helper()
	c1 := dial(t, srv)
	c2 := dial(t, srv)
	login(t, c1, "pa")
	login(t, c2, "pb")
	writeFrame(t, c1, uint32(framework.MsgMatch), 2, map[string]string{"module": module, "code": "ranked"})
	writeFrame(t, c2, uint32(framework.MsgMatch), 2, map[string]string{"module": module, "code": "ranked"})

	waitRoom := func(c *websocket.Conn) string {
		t.Helper()
		var roomID string
		waitFor(t, c, 3*time.Second, func(f *transport.Frame) bool {
			code, payload := splitEnvelope(f)
			var e basicEnv
			if f.MsgID == uint32(framework.MsgMatch) && json.Unmarshal(payload, &e) == nil &&
				code == 0 && e.RoomID != "" {
				roomID = e.RoomID
				return true
			}
			return false
		}, nil)
		return roomID
	}
	room1, room2 := waitRoom(c1), waitRoom(c2)
	if room1 == "" || room1 != room2 {
		t.Fatalf("%s: matched rooms differ: %q vs %q", module, room1, room2)
	}
	return room1, c1, c2
}

func TestCardEndToEnd(t *testing.T) {
	srv := buildMultiGameServer(t)
	roomID, c1, c2 := matchPair(t, srv, "card")

	// —— 开局发牌（可靠单推，两客户端各收一张 Deal）——
	for _, c := range []*websocket.Conn{c1, c2} {
		waitFor(t, c, 2*time.Second,
			func(f *transport.Frame) bool { return f.MsgID == uint32(card.MsgDeal) }, nil)
	}

	// —— 5 回合：双方各出一张（下标不重复），等回合结果（按回合号匹配）——
	for round := 0; round < card.HandCards; round++ {
		writeFrame(t, c1, uint32(card.MsgPlay), uint32(10+round), map[string]int{"idx": round})
		writeFrame(t, c2, uint32(card.MsgPlay), uint32(10+round), map[string]int{"idx": round})

		for _, c := range []*websocket.Conn{c1, c2} {
			waitFor(t, c, 2*time.Second,
				func(f *transport.Frame) bool {
					if f.MsgID != uint32(card.MsgRound) {
						return false
					}
					var r card.RoundNtf
					return json.Unmarshal(f.Body, &r) == nil && r.Round == round+1
				}, nil)
		}
	}

	// —— 结算：可靠广播 + 落库 + 排行榜 ——
	var res card.ResultNtf
	for _, c := range []*websocket.Conn{c1, c2} {
		f := waitFor(t, c, 3*time.Second,
			func(f *transport.Frame) bool { return f.MsgID == uint32(card.MsgResult) }, nil)
		if err := json.Unmarshal(f.Body, &res); err != nil {
			t.Fatalf("decode card result: %v body=%s", err, f.Body)
		}
	}
	if res.RoomID != roomID || res.Rounds != card.HandCards {
		t.Fatalf("card result wrong: %+v", res)
	}
	t.Logf("card settled: winner=%q scores=%v timeout=%v", res.Winner, res.Scores, res.Timeout)

	var stored card.ResultNtf
	if err := srv.Store().Get(context.Background(), "card_result", roomID, &stored); err != nil {
		t.Fatalf("card result not persisted: %v", err)
	}
	if stored.Winner != res.Winner {
		t.Fatalf("stored winner mismatch: %q vs %q", stored.Winner, res.Winner)
	}

	// —— OnDestroy 存档 Patch：extra.card_last_score = 本局得分（单写者路由）——
	waitProfile := func(uid string, check func(framework.PlayerProfile) bool) framework.PlayerProfile {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			var p framework.PlayerProfile
			if err := srv.Store().Get(context.Background(), "player_profile", uid, &p); err == nil && check(p) {
				return p
			} else if time.Now().After(deadline) {
				t.Fatalf("card profile %q not settled", uid)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for _, s := range res.Scores {
		p := waitProfile(s.UID, func(p framework.PlayerProfile) bool {
			return p.Extra["card_last_score"] == float64(s.Score)
		})
		t.Logf("card profile %s: extra.card_last_score=%v", s.UID, p.Extra["card_last_score"])
	}

	// —— 房间回收 ——
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && srv.Rooms().Rooms() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := srv.Rooms().Rooms(); n != 0 {
		t.Fatalf("card room not recycled, active=%d", n)
	}
}

func TestQuizEndToEnd(t *testing.T) {
	srv := buildMultiGameServer(t)
	roomID, c1, c2 := matchPair(t, srv, "quiz")

	// —— 5 题：双方按 Bank 确定性作答，全员答对（验证平局结算路径）——
	var res quiz.ResultNtf
	for round := 0; round < quiz.Questions; round++ {
		// 等双方收到当前题（round 0 已由开局广播发出）。
		for _, c := range []*websocket.Conn{c1, c2} {
			waitFor(t, c, 2*time.Second,
				func(f *transport.Frame) bool {
					if f.MsgID != uint32(quiz.MsgQuestion) {
						return false
					}
					var q quiz.QuestionNtf
					return json.Unmarshal(f.Body, &q) == nil && q.Idx == round
				}, nil)
		}
		ans := quiz.Bank[round].Answer
		writeFrame(t, c1, uint32(quiz.MsgAnswer), uint32(20+round),
			map[string]int{"idx": round, "option": ans})
		writeFrame(t, c2, uint32(quiz.MsgAnswer), uint32(20+round),
			map[string]int{"idx": round, "option": ans})
	}

	// 最后一题答完 → 结算（全员答对 → 双方同分 → 平局）。
	for _, c := range []*websocket.Conn{c1, c2} {
		f := waitFor(t, c, 3*time.Second,
			func(f *transport.Frame) bool { return f.MsgID == uint32(quiz.MsgResult) }, nil)
		if err := json.Unmarshal(f.Body, &res); err != nil {
			t.Fatalf("decode quiz result: %v body=%s", err, f.Body)
		}
	}
	if res.RoomID != roomID || res.Winner != "" || len(res.Scores) != 2 {
		t.Fatalf("quiz result wrong: %+v", res)
	}
	for _, s := range res.Scores {
		if s.Score != quiz.Questions {
			t.Fatalf("quiz score %s=%d, want %d", s.UID, s.Score, quiz.Questions)
		}
	}
	t.Logf("quiz settled (all-correct tie): scores=%v", res.Scores)

	var stored quiz.ResultNtf
	if err := srv.Store().Get(context.Background(), "quiz_result", roomID, &stored); err != nil {
		t.Fatalf("quiz result not persisted: %v", err)
	}

	// —— OnDestroy 存档 Patch：extra.quiz_last_score ——
	deadline := time.Now().Add(2 * time.Second)
	for {
		var p framework.PlayerProfile
		err := srv.Store().Get(context.Background(), "player_profile", "pa", &p)
		if err == nil && p.Extra["quiz_last_score"] == float64(quiz.Questions) {
			t.Logf("quiz profile pa: extra.quiz_last_score=%v", p.Extra["quiz_last_score"])
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("quiz profile not settled: err=%v extra=%v", err, p.Extra)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestQuizAnswerTimeoutSkip 超时跳题：仅一方作答，另一题靠 After 超时推进。
func TestQuizAnswerTimeoutSkip(t *testing.T) {
	srv := buildMultiGameServer(t)
	_, c1, c2 := matchPair(t, srv, "quiz")

	// 第 0 题：仅 c1 作答，c2 不答 → 超时跳题后应收到第 1 题。
	for _, c := range []*websocket.Conn{c1, c2} {
		waitFor(t, c, 2*time.Second,
			func(f *transport.Frame) bool {
				if f.MsgID != uint32(quiz.MsgQuestion) {
					return false
				}
				var q quiz.QuestionNtf
				return json.Unmarshal(f.Body, &q) == nil && q.Idx == 0
			}, nil)
	}
	writeFrame(t, c1, uint32(quiz.MsgAnswer), 30, map[string]int{"idx": 0, "option": quiz.Bank[0].Answer})

	for _, c := range []*websocket.Conn{c1, c2} {
		waitFor(t, c, 4*time.Second, // > TimeLimitMS(2s)
			func(f *transport.Frame) bool {
				if f.MsgID != uint32(quiz.MsgQuestion) {
					return false
				}
				var q quiz.QuestionNtf
				return json.Unmarshal(f.Body, &q) == nil && q.Idx == 1
			}, nil)
	}
	t.Logf("quiz timeout skip ok: both clients advanced to question 1")
}
