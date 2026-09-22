package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// walRecord WAL 中的一条写操作（JSON Lines 格式）。
type walRecord struct {
	TS      int64           `json:"ts"`
	Table   string          `json:"table"`
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value,omitempty"` // 已序列化 JSON；Delete 时为空
	Deleted bool            `json:"deleted,omitempty"`
}

// WAL 本地写前日志（§10.3 二级降级）：队列溢出时追加落盘，后端恢复后重放并清空。
//
// 实现说明：不使用 O_APPEND——Windows 下该打开方式只授予 FILE_APPEND_DATA，
// 后续 Truncate 会被拒绝。改为普通读写句柄 + 自维护 endOffset 的 WriteAt
// （全部访问在 mutex 内串行），跨平台语义一致。
type WAL struct {
	dir  string
	file string

	mu     sync.Mutex
	f      *os.File
	end    int64
	closed bool
}

// OpenWAL 打开（必要时创建）目录与 wal.log。dir 为空返回 nil（表示禁用 WAL）。
func OpenWAL(dir string) (*WAL, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage/wal: mkdir %q: %w", dir, err)
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage/wal: open %q: %w", path, err)
	}
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WAL{dir: dir, file: path, f: f, end: off}, nil
}

// Append 追加一条记录并落盘（降级路径低频，每条 fsync 保证崩溃不丢）。
func (w *WAL) Append(rec walRecord) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("storage/wal: closed")
	}
	n, err := w.f.WriteAt(line, w.end)
	if err != nil {
		return err
	}
	w.end += int64(n)
	return w.f.Sync()
}

// Replay 逐条读出记录交给 handle；handle 全部成功后截断清空日志。
// 任一条失败立即中止并保留文件，等待下次重试（后端可能尚未恢复）。
func (w *WAL) Replay(handle func(rec walRecord) error) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("storage/wal: closed")
	}

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(w.f)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024) // 单行上限 8MB
	var records []walRecord
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return len(records), fmt.Errorf("storage/wal: corrupt record: %w", err)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	for _, rec := range records {
		if err := handle(rec); err != nil {
			return len(records), err
		}
	}
	if len(records) > 0 {
		if err := w.f.Truncate(0); err != nil {
			return len(records), err
		}
		if _, err := w.f.Seek(0, io.SeekStart); err != nil {
			return len(records), err
		}
		w.end = 0
	}
	return len(records), nil
}

// Close 关闭文件（幂等）。
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.f.Close()
}

// ErrWALDisabled 未配置 WAL 目录。
var ErrWALDisabled = errors.New("storage/wal: disabled (empty path)")
