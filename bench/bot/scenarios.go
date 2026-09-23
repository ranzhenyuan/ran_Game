package bot

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rangame/server/internal/protocol"
	"github.com/rangame/server/internal/transport"
)

// ScenarioConfig 压测场景配置。
type ScenarioConfig struct {
	Addr         string // ws://host:port
	Codec        byte   // 0=JSON, 1=PB
	Bots         int    // 连接数
	Duration     time.Duration
	HeartbeatMS  int    // 心跳间隔（ms）
	SlowPct      int    // 慢消费者百分比（0-100）
	ReconnectPct int    // 每 5s 踢的百分比
	Module       string // 匹配模块
	Code         string // 匹配码
	Logger       *slog.Logger
}

// DefaultConfig 返回适合本地测试的默认配置。
func DefaultConfig(addr string) ScenarioConfig {
	return ScenarioConfig{
		Addr:         addr,
		Codec:        1, // PB
		Bots:         50,
		Duration:     10 * time.Second,
		HeartbeatMS:  1000,
		SlowPct:      5,
		ReconnectPct: 1,
		Module:       "snake",
		Code:         "ranked",
		Logger:       slog.Default(),
	}
}

// Result 场景结果汇总。
type Result struct {
	Config       ScenarioConfig
	Duration     time.Duration
	TotalBots    int
	ActiveBots   int
	TotalPings   int64
	TotalPongs   int64
	TotalSends   int64
	TotalRecvs   int64
	TotalErrors  int64
	Reconnects   int64
	LatencyAvgMS float64
	LatencyMaxMS float64
}

func (r Result) String() string {
	return fmt.Sprintf(
		"bots=%d active=%d pings=%d pongs=%d sends=%d recvs=%d errors=%d reconnects=%d lat_avg=%.1fms lat_max=%.1fms",
		r.TotalBots, r.ActiveBots, r.TotalPings, r.TotalPongs,
		r.TotalSends, r.TotalRecvs, r.TotalErrors, r.Reconnects,
		r.LatencyAvgMS, r.LatencyMaxMS,
	)
}

// RunHeartbeat 场景 1：心跳基线（§15.1）。
// N 连接仅 ping/pong，观察连接稳定性和延迟基线。
func RunHeartbeat(ctx context.Context, cfg ScenarioConfig) Result {
	return runScenario(ctx, cfg, "heartbeat")
}

// RunBroadcast 场景 2：广播风暴。
// 匹配成桌后接收 20fps 快照广播，观察下行 QPS 和延迟。
func RunBroadcast(ctx context.Context, cfg ScenarioConfig) Result {
	return runScenario(ctx, cfg, "broadcast")
}

// RunRandomLoad 场景 3：随机负载。
// 全员按真实序列随机发消息，注入慢消费者，观察 Drop/Kick 策略。
func RunRandomLoad(ctx context.Context, cfg ScenarioConfig) Result {
	return runScenario(ctx, cfg, "random")
}

// RunReconnect 场景 4：断线重连。
// 每 5s 随机踢 cfg.ReconnectPct% 连接并重连，观察重放正确性和泄漏。
func RunReconnect(ctx context.Context, cfg ScenarioConfig) Result {
	return runScenario(ctx, cfg, "reconnect")
}

// snake 帧消息 ID（bot 不 import games 包，保持解耦；与 games/snake/messages.go 对齐）。
const (
	msgSnakeState  = 0x1001 // 下行 20fps 快照（收到即开局）
	msgSnakeResult = 0x1002 // 下行结算
)

// PlayOutcome play 场景对局结果（存档 e2e 验证用）。
type PlayOutcome struct {
	UIDs     []string // 参局 bot UID（供 admin /admin/players/{uid} 查存档）
	Winner   string   // 空=平局
	Rounds   int64
	Finihed  bool // 是否正常收到结算（false=超时兜底）
	Duration time.Duration
}

// RunPlayMatch 存档链路场景：2 bot 匹配成桌 → 对局（随机转向直至撞墙结算）→
// 收到 MsgResult 后主动断开（触发宽限→OnRelease 退出强存）。
// 结算时 OnDestroy Patch 已由 PlayerActor 单写者 SaveSync 落库；
// 断开后约 grace+sweep 再查档案可验证退出强存不回滚（架构 §10.4.4）。
func RunPlayMatch(ctx context.Context, cfg ScenarioConfig) PlayOutcome {
	start := time.Now()
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Bots != 2 {
		cfg.Bots = 2 // 一桌两人
	}

	ts := time.Now().UnixNano() % 100000
	out := PlayOutcome{}

	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < 2; i++ {
		uid := fmt.Sprintf("play-%d-%d", i, ts)
		out.UIDs = append(out.UIDs, uid)
		wg.Add(1)
		go func(idx int, uid string) {
			defer wg.Done()
			b := New(Config{ID: idx, UID: uid, Codec: cfg.Codec, Logger: logger})
			if err := b.ConnectTo(ctx, cfg.Addr); err != nil {
				logger.Error("connect", "uid", uid, "err", err)
				return
			}
			defer func() {
				// 结算后主动断开：触发服务端宽限期 → OnRelease → 退出强存
				_ = b.Close()
			}()

			frames := make(chan *transport.Frame, 256)
			recvCtx, cancelRecv := context.WithCancel(ctx)
			defer cancelRecv()
			go func() { _ = b.RecvLoop(recvCtx, func(f *transport.Frame) { frames <- f }) }()

			if err := b.SendMatch(cfg.Module, cfg.Code); err != nil {
				logger.Error("match", "uid", uid, "err", err)
				return
			}

			// 状态机：等开局（首帧快照）→ 周期转向 → 等结算
			started := false
			ticker := time.NewTicker(300 * time.Millisecond)
			defer ticker.Stop()
			dir := 1
			for {
				select {
				case f := <-frames:
					switch f.MsgID {
					case msgSnakeState:
						started = true
					case msgSnakeResult:
						c, _ := protocol.Get(cfg.Codec)
						var res struct {
							RoomID string `json:"room_id" protobuf:"bytes,1,opt,name=room_id,proto3"`
							Winner string `json:"winner" protobuf:"bytes,2,opt,name=winner,proto3"`
							Rounds int64  `json:"rounds" protobuf:"varint,3,opt,name=rounds,proto3"`
						}
						_ = c.Unmarshal(f.Body, &res)
						mu.Lock()
						out.Winner, out.Rounds, out.Finihed = res.Winner, res.Rounds, true
						mu.Unlock()
						logger.Info("game settled", "uid", uid, "winner", res.Winner, "rounds", res.Rounds)
						return
					}
				case <-ticker.C:
					if started {
						// 随机转向（1-4），撞墙结算由服务端判定
						dir = dir%4 + 1
						_ = b.SendMove(dir)
					}
				case <-ctx.Done():
					return
				}
			}
		}(i, uid)
	}
	wg.Wait()
	out.Duration = time.Since(start)
	return out
}

// doudizhu 帧消息 ID（bot 不 import games 包，保持解耦；与 games/doudizhu/messages.go 对齐）。
const (
	msgDDZDeal   = 0x1310 // 下行发牌（单推本人手牌）
	msgDDZStage  = 0x1311 // 下行阶段切换
	msgDDZCall   = 0x1312 // 下行叫分事件
	msgDDZPlayed = 0x1313 // 下行出牌/过牌事件
	msgDDZTurn   = 0x1314 // 下行轮到谁
	msgDDZResult = 0x1316 // 下行结算
)

// 斗地主阶段（与 StageWaiting/Bidding/Playing/Finished 对齐）。
const (
	ddzStageBidding = 1
	ddzStagePlaying = 2
)

// ddzCard 客户端侧牌结构（与 doudizhu.Card 字段对齐）。
type ddzCard struct {
	Suit  int `json:"suit" protobuf:"varint,1,opt,name=suit,proto3"`
	Rank  int `json:"rank" protobuf:"varint,2,opt,name=rank,proto3"`
	Joker int `json:"joker" protobuf:"varint,3,opt,name=joker,proto3"`
}

// cardID 与 doudizhu.Card.ID() 保持一致：用于按 ID 出牌。
func ddzCardID(c ddzCard) int {
	if c.Joker >= 0 {
		return 100 + c.Joker
	}
	return c.Suit*100 + c.Rank
}

// DoudizhuOutcome 斗地主对局结果（存档 e2e 验证用）。
type DoudizhuOutcome struct {
	UIDs        []string // 参局 bot UID
	Landlord    string   // 地主 UID
	LandlordWin bool     // 地主是否胜利
	BaseScore   int      // 叫分底分
	Finished    bool     // 是否正常收到结算（false=超时兜底）
	Duration    time.Duration
}

// RunDoudizhuMatch 斗地主端到端场景：3 bot 匹配成桌 → 叫分 → 轮转出牌 → 结算。
// 策略（验证同步链路，非智能 AI）：叫分按当前最高分 +1 直至封顶；出牌只出单张，
// 压不过则过。收到 MsgResult 后主动断开，触发宽限→退出强存（§10.4.4）。
func RunDoudizhuMatch(ctx context.Context, cfg ScenarioConfig) DoudizhuOutcome {
	start := time.Now()
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cfg.Module = "doudizhu" // 锁定模块
	if cfg.Bots != 3 {
		cfg.Bots = 3 // 一桌三人
	}

	ts := time.Now().UnixNano() % 100000
	out := DoudizhuOutcome{}

	// 整体超时兜底：避免对局逻辑卡死时 bot 永久阻塞；超时后打印各 bot 诊断信息。
	// naive bot 只出单张且频繁过牌，完整对局可能接近一分钟，故留足余量。
	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	type diag struct {
		uid  string
		n    int
		last uint32
		hand int
	}
	diags := make([]diag, 3)

	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < 3; i++ {
		uid := fmt.Sprintf("ddz-%d-%d", i, ts)
		out.UIDs = append(out.UIDs, uid)
		wg.Add(1)
		go func(idx int, uid string) {
			defer wg.Done()
			b := New(Config{ID: idx, UID: uid, Codec: cfg.Codec, Logger: logger})
			if err := b.ConnectTo(ctx, cfg.Addr); err != nil {
				logger.Error("connect", "uid", uid, "err", err)
				return
			}
			defer func() { _ = b.Close() }()

			c, _ := protocol.Get(cfg.Codec)
			frames := make(chan *transport.Frame, 256)
			recvCtx, cancelRecv := context.WithCancel(runCtx)
			defer cancelRecv()
			frameCount := 0
			var lastMsg uint32
			var hand []ddzCard // 提前声明：defer 诊断需捕获，且在本 goroutine 内串行访问
			go func() {
				_ = b.RecvLoop(recvCtx, func(f *transport.Frame) {
					frameCount++
					lastMsg = f.MsgID
					frames <- f
				})
			}()
			defer func() {
				mu.Lock()
				diags[idx] = diag{uid: uid, n: frameCount, last: lastMsg, hand: len(hand)}
				mu.Unlock()
			}()

			if err := b.SendMatch(cfg.Module, cfg.Code); err != nil {
				logger.Error("match", "uid", uid, "err", err)
				return
			}

			// 单 bot 状态：只在本 goroutine 访问（hand 已提前声明）。
			var (
				stage      int
				curUID     string
				maxScore   int
				lastPlayed *struct {
					UID   string
					Cards []ddzCard
					Pass  bool
				}
			)

			// act 在轮到本人时按阶段发请求。
			act := func() {
				if curUID != uid {
					return
				}
				if stage == ddzStageBidding {
					score := 0
					if maxScore < 3 {
						score = maxScore + 1 // 叫分必须高于当前最高分
					}
					_ = b.SendCall(score)
					return
				}
				if stage == ddzStagePlaying {
					ids := pickSinglePlay(hand, lastPlayed, uid)
					_ = b.SendPlay(ids)
				}
			}

			for {
				select {
				case f := <-frames:
					switch f.MsgID {
					case msgDDZDeal:
						var h struct {
							Hand      []ddzCard `json:"hand" protobuf:"bytes,1,rep,name=hand,proto3"`
							Landlord3 []ddzCard `json:"landlord3,omitempty" protobuf:"bytes,2,rep,name=landlord3,proto3"`
						}
						if err := c.Unmarshal(f.Body, &h); err == nil {
							// 发牌（含流局重发）是全新一手：必须替换而非追加，否则持有过期牌 ID 会被服务端拒绝。
							hand = append([]ddzCard{}, h.Hand...)
							hand = append(hand, h.Landlord3...)
						}
					case msgDDZStage:
						var s struct {
							Stage int `json:"stage" protobuf:"varint,1,opt,name=stage,proto3"`
						}
						if c.Unmarshal(f.Body, &s) == nil {
							stage = s.Stage
						}
					case msgDDZCall:
						var cl struct {
							UID   string `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
							Score int    `json:"score" protobuf:"varint,2,opt,name=score,proto3"`
						}
						if c.Unmarshal(f.Body, &cl) == nil && cl.Score > maxScore {
							maxScore = cl.Score
						}
					case msgDDZPlayed:
						var p struct {
							UID   string    `json:"uid" protobuf:"bytes,1,opt,name=uid,proto3"`
							Cards []ddzCard `json:"cards,omitempty" protobuf:"bytes,2,rep,name=cards,proto3"`
							Type  string    `json:"type" protobuf:"bytes,3,opt,name=type,proto3"`
							Pass  bool      `json:"pass" protobuf:"varint,4,opt,name=pass,proto3"`
						}
						if c.Unmarshal(f.Body, &p) == nil {
							if p.Pass {
								// 过牌不改变「上一手真牌」：服务端 lastPlayed 仍指向最近一次实牌，
								// 此处若覆盖为空会导致 bot 误判压牌/自由出牌，发出被服务端拒绝的牌。
							} else {
								lastPlayed = &struct {
									UID   string
									Cards []ddzCard
									Pass  bool
								}{UID: p.UID, Cards: p.Cards}
								if p.UID == uid {
									hand = removeCards(hand, p.Cards) // 服务端确认后移除本地手牌
								}
							}
						}
					case msgDDZTurn:
						var t struct {
							CurUID string `json:"cur_uid" protobuf:"bytes,1,opt,name=cur_uid,proto3"`
						}
						if c.Unmarshal(f.Body, &t) == nil {
							curUID = t.CurUID
							act()
						}
					case msgDDZResult:
						var r struct {
							RoomID      string `json:"room_id" protobuf:"bytes,1,opt,name=room_id,proto3"`
							Landlord    string `json:"landlord" protobuf:"bytes,2,opt,name=landlord,proto3"`
							LandlordWin bool   `json:"landlord_win" protobuf:"varint,4,opt,name=landlord_win,proto3"`
							BaseScore   int    `json:"base_score" protobuf:"varint,5,opt,name=base_score,proto3"`
							Timeout     bool   `json:"timeout" protobuf:"varint,6,opt,name=timeout,proto3"`
						}
						if c.Unmarshal(f.Body, &r) == nil {
							mu.Lock()
							out.Landlord, out.LandlordWin, out.BaseScore, out.Finished =
								r.Landlord, r.LandlordWin, r.BaseScore, true
							mu.Unlock()
							logger.Info("ddz settled", "uid", uid, "landlord", r.Landlord,
								"win", r.LandlordWin, "base", r.BaseScore, "timeout", r.Timeout)
						}
						return
					}
				case <-runCtx.Done():
					logger.Warn("ddz bot timeout", "uid", uid, "frames", frameCount, "last", lastMsg, "hand", len(hand))
					return
				}
			}
		}(i, uid)
	}
	wg.Wait()
	out.Duration = time.Since(start)
	// 诊断：打印各 bot 收到的帧数、最后一帧 msgID、剩余手牌数，定位卡在哪一步。
	for _, d := range diags {
		fmt.Printf("[diag] uid=%s frames=%d last=0x%x hand=%d\n", d.uid, d.n, d.last, d.hand)
	}
	return out
}

// pickSinglePlay 单张出牌策略：自由出牌（上家为自己/无 last）出最小单张；
// 否则出能压过上家单张的最小单张，压不过返回 nil（过牌）。
func pickSinglePlay(hand []ddzCard, last *struct {
	UID   string
	Cards []ddzCard
	Pass  bool
}, uid string) []int {
	if len(hand) == 0 {
		return nil
	}
	cp := append([]ddzCard(nil), hand...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Rank < cp[j].Rank })

	freeLead := last == nil || last.Pass || last.UID == uid
	if freeLead {
		return []int{ddzCardID(cp[0])}
	}
	// 求上家主 Rank（单张策略下取最大）。
	lastMain := 0
	for _, c := range last.Cards {
		if c.Rank > lastMain {
			lastMain = c.Rank
		}
	}
	for _, c := range cp {
		if c.Rank > lastMain {
			return []int{ddzCardID(c)}
		}
	}
	return nil // 压不过，过牌
}

// removeCards 从手牌移除已打出的牌（按 ID 匹配）。
func removeCards(hand []ddzCard, played []ddzCard) []ddzCard {
	rm := make(map[int]bool, len(played))
	for _, c := range played {
		rm[ddzCardID(c)] = true
	}
	kept := hand[:0]
	for _, c := range hand {
		if !rm[ddzCardID(c)] {
			kept = append(kept, c)
		}
	}
	return kept
}

func runScenario(ctx context.Context, cfg ScenarioConfig, mode string) Result {
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	bots := make([]*Bot, cfg.Bots)
	var wg sync.WaitGroup
	var activeBots atomic.Int64

	// 启动所有 bot
	for i := 0; i < cfg.Bots; i++ {
		uid := fmt.Sprintf("bot-%d-%d", i, time.Now().UnixNano()%100000)
		bot := New(Config{
			ID:     i,
			UID:    uid,
			Codec:  cfg.Codec,
			Logger: logger,
		})
		bots[i] = bot

		wg.Add(1)
		go func(b *Bot, idx int) {
			defer wg.Done()
			if err := b.ConnectTo(ctx, cfg.Addr); err != nil {
				logger.Warn("connect failed", "id", idx, "err", err)
				return
			}
			activeBots.Add(1)

			// 接收循环
			recvDone := make(chan struct{})
			go func() {
				defer close(recvDone)
				_ = b.RecvLoop(ctx, nil)
			}()

			// 场景行为
			ticker := time.NewTicker(time.Duration(cfg.HeartbeatMS) * time.Millisecond)
			defer ticker.Stop()

			matchSent := false
			for {
				select {
				case <-ctx.Done():
					<-recvDone
					_ = b.Close()
					return
				case <-ticker.C:
					switch mode {
					case "heartbeat":
						_ = b.Ping()
					case "broadcast":
						if !matchSent {
							_ = b.SendMatch(cfg.Module, cfg.Code)
							matchSent = true
						} else {
							_ = b.Ping()
						}
					case "random":
						// 5% 慢消费者：跳过部分心跳
						if idx*100/cfg.Bots >= cfg.SlowPct {
							_ = b.Ping()
						}
						if !matchSent && idx%2 == 0 {
							_ = b.SendMatch(cfg.Module, cfg.Code)
							matchSent = true
						}
					case "reconnect":
						_ = b.Ping()
					}
				}
			}
		}(bot, i)

		// 错开启动避免 SYN 风暴
		time.Sleep(time.Millisecond * 10)
	}

	// 场景 4：断线重连
	if mode == "reconnect" && cfg.ReconnectPct > 0 {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					kick := cfg.Bots * cfg.ReconnectPct / 100
					if kick < 1 {
						kick = 1
					}
					for j := 0; j < kick; j++ {
						idx := int(time.Now().UnixNano()) % cfg.Bots
						if bots[idx] != nil && bots[idx].conn != nil {
							_ = bots[idx].conn.Close()
							go func(b *Bot) {
								time.Sleep(500 * time.Millisecond)
								_ = b.Reconnect(ctx)
							}(bots[idx])
						}
					}
				}
			}
		}()
	}

	wg.Wait()

	// 汇总指标
	var res Result
	res.Config = cfg
	res.TotalBots = cfg.Bots
	res.ActiveBots = int(activeBots.Load())
	res.Duration = cfg.Duration

	for _, b := range bots {
		if b == nil {
			continue
		}
		m := b.Metrics()
		res.TotalPings += m.PingsSent
		res.TotalPongs += m.PongsRecv
		res.TotalSends += m.MsgsSent
		res.TotalRecvs += m.MsgsRecv
		res.TotalErrors += m.Errors
		res.Reconnects += m.Reconnects
		if m.LatencyMaxMS > res.LatencyMaxMS {
			res.LatencyMaxMS = m.LatencyMaxMS
		}
	}
	totalCount := int64(0)
	totalSum := int64(0)
	for _, b := range bots {
		if b == nil {
			continue
		}
		totalSum += b.latencySum.Load()
		totalCount += b.latencyCount.Load()
	}
	if totalCount > 0 {
		res.LatencyAvgMS = float64(totalSum) / float64(totalCount) / float64(time.Millisecond)
	}

	return res
}
