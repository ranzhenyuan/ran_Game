// Package config 负责框架强类型配置的加载、默认值填充与校验。
// 启动后配置只读（架构文档 §7.3 红线 3：全局只读数据启动后只读）。
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration 包装 time.Duration，使其支持 YAML 中的 "30s" / "2ms" 文本写法。
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

// UnmarshalText 实现 encoding.TextUnmarshaler，yaml.v3 会调用该接口。
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(text), err)
	}
	*d = Duration(v)
	return nil
}

// Config 全局配置根。
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	TCP     TCPConfig     `yaml:"tcp"`
	WS      WSConfig      `yaml:"ws"`
	Session SessionConfig `yaml:"session"`
	Storage StorageConfig `yaml:"storage"`
	Cluster ClusterConfig `yaml:"cluster"`
	Admin   AdminConfig   `yaml:"admin"`
	Log     LogConfig     `yaml:"log"`
	PProf   PProfConfig   `yaml:"pprof"`
}

// AdminConfig 运维面 HTTP server 配置（§11）。
type AdminConfig struct {
	// Addr admin server 监听地址（必须内网，§11.4）。
	Addr string `yaml:"addr"`
	// TrustedCIDRs 信任的来源 IP CIDR；空表示仅 127.0.0.1。
	TrustedCIDRs []string `yaml:"trusted_cidrs"`
}

type ServerConfig struct {
	// Role 进程角色：standalone（默认单机形态）| gateway | logic（演进态）
	Role string `yaml:"role"`
}

// ClusterConfig 集群演进态配置（§12/§13）。
// 仅在 server.role = gateway | logic 时使用。
type ClusterConfig struct {
	// NodeID 节点 ID（StatefulSet：logic-{module}-{ordinal}；Deployment：gateway 可空自动生成）。
	NodeID string `yaml:"node_id"`
	// Modules 本节点服务的 module 列表（Logic 必填；Gateway 留空）。
	Modules []string `yaml:"modules"`
	// DrainDeadline 排水截止时间（§13.3，默认 20min）。
	DrainDeadline Duration `yaml:"drain_deadline"`
	// RedisAddr 注册表 Redis 地址；空=用进程内 MemRegistry（演进态骨架）。
	RedisAddr string `yaml:"redis_addr"`
	// RedisDB 注册表 Redis DB。
	RedisDB int `yaml:"redis_db"`
	// NodeTTL 节点条目 TTL（§13.2，默认 15s）。
	NodeTTL Duration `yaml:"node_ttl"`
}

type TCPConfig struct {
	Addr               string   `yaml:"addr"`
	ReadTimeout        Duration `yaml:"read_timeout"`
	WriteChannelSize   int      `yaml:"write_channel_size"`
	WriteFlushInterval Duration `yaml:"write_flush_interval"`
	MaxFrameSize       int      `yaml:"max_frame_size"`
	TCPNoDelay         bool     `yaml:"tcp_nodelay"`
}

type LogConfig struct {
	// Level: debug | info | warn | error
	Level string `yaml:"level"`
}

// WSConfig WebSocket 接入配置（架构文档 §3 / §11.4）。
type WSConfig struct {
	Enabled          bool     `yaml:"enabled"`
	Addr             string   `yaml:"addr"`
	Path             string   `yaml:"path"`
	AllowedOrigins   []string `yaml:"allowed_origins"` // 空=仅同源；"*"=放开（仅测试）
	ReadTimeout      Duration `yaml:"read_timeout"`
	WriteChannelSize int      `yaml:"write_channel_size"`
	MaxFrameSize     int      `yaml:"max_frame_size"`
}

// SessionConfig 会话与重连配置（§5）。
type SessionConfig struct {
	// GracePeriod 断线后会话保留宽限期，默认 60s。
	GracePeriod Duration `yaml:"grace_period"`
	// ReplayBuffer 可靠通道环形缓冲容量，默认 256。
	ReplayBuffer int `yaml:"replay_buffer"`
	// HeartbeatMS 下发给客户端的心跳间隔。
	HeartbeatMS int `yaml:"heartbeat_ms"`
	// MaxLoginBody 登录/重连包体上限（§11.4，默认 4KB）。
	MaxLoginBody int `yaml:"max_login_body"`
	// SweepInterval 宽限期扫描间隔。
	SweepInterval Duration `yaml:"sweep_interval"`
}

type PProfConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
}

// StorageConfig 持久化配置（§10）。Driver=memory 时其余字段忽略。
type StorageConfig struct {
	// Driver: memory（默认，零外部依赖）| redis | mysql
	Driver string `yaml:"driver"`
	// RedisDSN redis 地址（redis 驱动），如 127.0.0.1:6379。
	RedisAddr string `yaml:"redis_addr"`
	RedisDB   int    `yaml:"redis_db"`
	// MySQLDSN MySQL DSN（mysql 驱动），如 user:pass@tcp(127.0.0.1:3306)/game?parseTime=true。
	MySQLDSN string `yaml:"mysql_dsn"`

	// 异步写回（§10.2）。
	WriteBackQueue int      `yaml:"writeback_queue"` // 有界队列容量，默认 4096
	FlushInterval  Duration `yaml:"flush_interval"`  // 批量 flush 间隔，默认 10ms
	FlushBatch     int      `yaml:"flush_batch"`     // 单次 flush 上限，默认 256
	FlushRetry     int      `yaml:"flush_retry"`     // 单批 flush 失败重试次数，默认 2
	WALPath        string   `yaml:"wal_path"`        // WAL 目录；空=禁用 WAL（队列溢出直接死信+熔断）
	// BreakerCooldown 熔断后多久进入半开探测，默认 5s。
	BreakerCooldown Duration `yaml:"breaker_cooldown"`
	// SyncTimeout SetSync/IncrBy/Get 等同步路径超时，默认 2s。
	SyncTimeout Duration `yaml:"sync_timeout"`
}

// Default 返回带默认值的配置（默认值与架构文档一致）。
func Default() Config {
	return Config{
		Server: ServerConfig{Role: "standalone"},
		TCP: TCPConfig{
			Addr:               ":7000",
			ReadTimeout:        Duration(30 * time.Second),
			WriteChannelSize:   256,
			WriteFlushInterval: Duration(2 * time.Millisecond),
			MaxFrameSize:       64 * 1024,
			TCPNoDelay:         true,
		},
		Log:   LogConfig{Level: "info"},
		PProf: PProfConfig{Enabled: false, Addr: "127.0.0.1:6060"},
		WS: WSConfig{
			Enabled:          false,
			Addr:             ":7001",
			Path:             "/ws",
			ReadTimeout:      Duration(30 * time.Second),
			WriteChannelSize: 256,
			MaxFrameSize:     64 * 1024,
		},
		Session: SessionConfig{
			GracePeriod:   Duration(60 * time.Second),
			ReplayBuffer:  256,
			HeartbeatMS:   15000,
			MaxLoginBody:  4096,
			SweepInterval: Duration(5 * time.Second),
		},
		Storage: StorageConfig{
			Driver:          "memory",
			WriteBackQueue:  4096,
			FlushInterval:   Duration(10 * time.Millisecond),
			FlushBatch:      256,
			FlushRetry:      2,
			WALPath:         "./data/wal",
			BreakerCooldown: Duration(5 * time.Second),
			SyncTimeout:     Duration(2 * time.Second),
		},
		Cluster: ClusterConfig{
			DrainDeadline: Duration(20 * time.Minute),
			NodeTTL:       Duration(15 * time.Second),
		},
		Admin: AdminConfig{
			Addr: "127.0.0.1:7100",
		},
	}
}

// Load 从 path 读取 YAML 配置，未显式给出的字段沿用 Default。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	cfg := Default()
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

// Validate 校验必填项与取值范围。
func (c *Config) Validate() error {
	switch c.Server.Role {
	case "standalone", "gateway", "logic":
	default:
		return fmt.Errorf("server.role must be one of standalone|gateway|logic, got %q", c.Server.Role)
	}
	if c.TCP.Addr == "" {
		return fmt.Errorf("tcp.addr is required")
	}
	if c.TCP.ReadTimeout.Std() <= 0 {
		return fmt.Errorf("tcp.read_timeout must be positive")
	}
	if c.TCP.WriteChannelSize <= 0 {
		return fmt.Errorf("tcp.write_channel_size must be positive")
	}
	if c.TCP.WriteFlushInterval.Std() <= 0 {
		return fmt.Errorf("tcp.write_flush_interval must be positive")
	}
	if c.TCP.MaxFrameSize < 64 {
		return fmt.Errorf("tcp.max_frame_size too small: %d", c.TCP.MaxFrameSize)
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level must be one of debug|info|warn|error, got %q", c.Log.Level)
	}
	if c.PProf.Enabled && c.PProf.Addr == "" {
		return fmt.Errorf("pprof.addr is required when pprof.enabled=true")
	}
	if c.WS.Enabled {
		if c.WS.Addr == "" {
			return fmt.Errorf("ws.addr is required when ws.enabled=true")
		}
		if c.WS.Path == "" {
			return fmt.Errorf("ws.path is required when ws.enabled=true")
		}
	}
	if c.Session.ReplayBuffer <= 0 {
		return fmt.Errorf("session.replay_buffer must be positive")
	}
	if c.Session.GracePeriod.Std() <= 0 {
		return fmt.Errorf("session.grace_period must be positive")
	}
	if c.Session.MaxLoginBody <= 0 {
		return fmt.Errorf("session.max_login_body must be positive")
	}
	switch c.Storage.Driver {
	case "memory", "redis", "mysql":
	case "":
		return fmt.Errorf("storage.driver is required")
	default:
		return fmt.Errorf("storage.driver must be one of memory|redis|mysql, got %q", c.Storage.Driver)
	}
	if c.Storage.Driver == "redis" && c.Storage.RedisAddr == "" {
		return fmt.Errorf("storage.redis_addr is required when driver=redis")
	}
	if c.Storage.Driver == "mysql" && c.Storage.MySQLDSN == "" {
		return fmt.Errorf("storage.mysql_dsn is required when driver=mysql")
	}
	// Cluster 配置校验（仅 gateway/logic 角色必填项）
	if c.Server.Role != "standalone" {
		if c.Cluster.NodeID == "" {
			return fmt.Errorf("cluster.node_id is required when role=%s", c.Server.Role)
		}
		if c.Server.Role == "logic" && len(c.Cluster.Modules) == 0 {
			return fmt.Errorf("cluster.modules is required when role=logic")
		}
		if c.Cluster.DrainDeadline.Std() <= 0 {
			return fmt.Errorf("cluster.drain_deadline must be positive")
		}
		if c.Cluster.NodeTTL.Std() <= 0 {
			return fmt.Errorf("cluster.node_ttl must be positive")
		}
	}
	return nil
}
