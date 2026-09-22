package bot

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
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
