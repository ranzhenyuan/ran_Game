// Command benchbot 压测客户端 CLI（架构文档 §15.1）。
//
// 四场景：heartbeat / broadcast / random / reconnect / all（顺序跑全部）。
// 默认连本地 7001 端口，50 连接，10s。示例：
//
//	go run ./cmd/benchbot -scenario heartbeat -bots 100 -duration 30s
//	go run ./cmd/benchbot -scenario all -addr ws://127.0.0.1:7001 -codec pb
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/rangame/server/bench/bot"
)

func main() {
	addr := flag.String("addr", "ws://127.0.0.1:7001", "WS 服务器地址")
	scenario := flag.String("scenario", "heartbeat", "场景: heartbeat|broadcast|random|reconnect|all")
	bots := flag.Int("bots", 50, "连接数")
	duration := flag.Duration("duration", 10*time.Second, "压测时长")
	codec := flag.String("codec", "pb", "序列化: json|pb")
	heartbeatMS := flag.Int("heartbeat-ms", 1000, "心跳间隔(ms)")
	slowPct := flag.Int("slow-pct", 5, "慢消费者百分比(0-100)")
	reconnectPct := flag.Int("reconnect-pct", 1, "每5s踢的百分比(0-100)")
	module := flag.String("module", "snake", "匹配模块")
	code := flag.String("code", "ranked", "匹配码")
	verbose := flag.Bool("v", false, "打印每 bot 的明细指标")
	flag.Parse()

	var codecByte byte = 1 // PB
	switch *codec {
	case "json", "JSON":
		codecByte = 0
	case "pb", "PB", "protobuf":
		codecByte = 1
	default:
		fmt.Fprintf(os.Stderr, "unknown codec: %s\n", *codec)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if !*verbose {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	cfg := bot.ScenarioConfig{
		Addr:         *addr,
		Codec:        codecByte,
		Bots:         *bots,
		Duration:     *duration,
		HeartbeatMS:  *heartbeatMS,
		SlowPct:      *slowPct,
		ReconnectPct: *reconnectPct,
		Module:       *module,
		Code:         *code,
		Logger:       logger,
	}

	scenarios := []struct {
		name string
		fn   func(context.Context, bot.ScenarioConfig) bot.Result
	}{
		{"heartbeat", bot.RunHeartbeat},
		{"broadcast", bot.RunBroadcast},
		{"random", bot.RunRandomLoad},
		{"reconnect", bot.RunReconnect},
	}

	fmt.Printf("bench: addr=%s codec=%s bots=%d duration=%s\n", *addr, *codec, *bots, *duration)
	fmt.Println("-----------------------------------------------------------")

	for _, sc := range scenarios {
		if *scenario != "all" && *scenario != sc.name {
			continue
		}
		fmt.Printf("[%s] start\n", sc.name)
		res := sc.fn(context.Background(), cfg)
		fmt.Printf("[%s] %s\n", sc.name, res.String())
		fmt.Println("-----------------------------------------------------------")
	}

	fmt.Println("done.")
}
