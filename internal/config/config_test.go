package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAppliesDefaultsAndYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.yaml")
	content := `
server:
  role: standalone
tcp:
  addr: ":9999"
  read_timeout: 5s
log:
  level: debug
pprof:
  enabled: false
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write yaml: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.TCP.Addr != ":9999" {
		t.Fatalf("addr mismatch: %s", cfg.TCP.Addr)
	}
	if cfg.TCP.ReadTimeout.Std() != 5*time.Second {
		t.Fatalf("read timeout mismatch: %s", cfg.TCP.ReadTimeout.Std())
	}
	if cfg.Log.Level != "debug" {
		t.Fatalf("level mismatch: %s", cfg.Log.Level)
	}
	// YAML 未给出的字段应沿用默认值。
	if cfg.TCP.WriteChannelSize != 256 {
		t.Fatalf("default write_channel_size mismatch: %d", cfg.TCP.WriteChannelSize)
	}
	if cfg.TCP.MaxFrameSize != 64*1024 {
		t.Fatalf("default max_frame_size mismatch: %d", cfg.TCP.MaxFrameSize)
	}
}

func TestValidateRejectsBadRole(t *testing.T) {
	cfg := Default()
	cfg.Server.Role = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatal("bad role should fail validation")
	}
}

func TestValidateRejectsBadDuration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("tcp:\n  read_timeout: abc\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("invalid duration should fail")
	}
}
