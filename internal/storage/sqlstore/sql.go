// Package sqlstore 是 storage.Backend 的 SQL 适配（架构文档 §10.2：
// 账号、资产流水、对局记录落 MySQL，强一致可审计）。
//
// 三张表：
//   - kv(tbl,k,val)         任意 JSON 值（upsert）
//   - kv_counter(tbl,k,val) 原子计数器（货币等）
//   - zset(tbl,member,score) 排行榜有序表（score DESC, member ASC）
//
// 方言层 Dialect 隔离 MySQL / SQLite 差异（后者供纯 Go 集成测试，无 cgo）。
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // 生产 MySQL 驱动注册

	"github.com/rangame/server/internal/storage"
	"github.com/rangame/server/pkg/framework"
)

// Dialect 方言隔离点。
type Dialect interface {
	Name() string
	// UpsertKV / UpsertCounter 参数：tbl,key,val
	UpsertKV() string
	UpsertCounter() string
	// UpsertZAdd 参数：tbl,member,score
	UpsertZAdd() string
	// UpsertZIncr 参数：tbl,member,delta；冲突时 score=score+delta。
	UpsertZIncr() string
	// Placeholder 第 n 个（从 1 起）参数占位符。
	Placeholder(n int) string
}

// schemaDDL 三种存储结构（标准 SQL，MySQL/SQLite 均兼容；tbl 为列名避开 TABLE 保留字）。
const schemaDDL = `
CREATE TABLE IF NOT EXISTS kv (
  tbl VARCHAR(128) NOT NULL,
  k   VARCHAR(256) NOT NULL,
  val BLOB NOT NULL,
  PRIMARY KEY (tbl, k)
);
CREATE TABLE IF NOT EXISTS kv_counter (
  tbl VARCHAR(128) NOT NULL,
  k   VARCHAR(256) NOT NULL,
  val BIGINT NOT NULL,
  PRIMARY KEY (tbl, k)
);
CREATE TABLE IF NOT EXISTS zset (
  tbl    VARCHAR(128) NOT NULL,
  member VARCHAR(256) NOT NULL,
  score  BIGINT NOT NULL,
  PRIMARY KEY (tbl, member)
);
CREATE INDEX IF NOT EXISTS idx_zset_rank ON zset (tbl, score DESC, member);
`

// mysqlSchema MySQL 不支持 CREATE INDEX IF NOT EXISTS，索引随表内联。
const mysqlSchema = `
CREATE TABLE IF NOT EXISTS kv (
  tbl VARCHAR(128) NOT NULL,
  k   VARCHAR(256) NOT NULL,
  val BLOB NOT NULL,
  PRIMARY KEY (tbl, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS kv_counter (
  tbl VARCHAR(128) NOT NULL,
  k   VARCHAR(256) NOT NULL,
  val BIGINT NOT NULL,
  PRIMARY KEY (tbl, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS zset (
  tbl    VARCHAR(128) NOT NULL,
  member VARCHAR(256) NOT NULL,
  score  BIGINT NOT NULL,
  PRIMARY KEY (tbl, member),
  KEY idx_zset_rank (tbl, score, member)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// Open 打开数据库（driverName: "mysql" | "sqlite"），Ping + 自动建表。
func Open(ctx context.Context, driverName, dsn string) (*Backend, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: open: %w", err)
	}
	d, err := dialectFor(driverName)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlstore: ping: %w", err)
	}
	b := &Backend{db: db, d: d}
	if err := b.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return b, nil
}

// Backend SQL 存储后端。
type Backend struct {
	db *sql.DB
	d  Dialect
}

// DB 暴露底层连接池（Raw 逃生口）。
func (b *Backend) DB() *sql.DB { return b.db }

func (b *Backend) migrate(ctx context.Context) error {
	ddl := schemaDDL
	if b.d.Name() == "mysql" {
		ddl = mysqlSchema
	}
	if _, err := b.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("sqlstore: migrate: %w", err)
	}
	return nil
}

func (b *Backend) Get(ctx context.Context, table, key string) ([]byte, error) {
	var raw []byte
	err := b.db.QueryRowContext(ctx,
		`SELECT val FROM kv WHERE tbl=? AND k=?`, table, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, framework.ErrStorageNotFound
	}
	return raw, err
}

func (b *Backend) Put(ctx context.Context, table, key string, raw []byte) error {
	_, err := b.db.ExecContext(ctx, b.d.UpsertKV(), table, key, raw)
	return err
}

func (b *Backend) Delete(ctx context.Context, table, key string) error {
	_, err := b.db.ExecContext(ctx, `DELETE FROM kv WHERE tbl=? AND k=?`, table, key)
	return err
}

func (b *Backend) IncrBy(ctx context.Context, table, key string, delta int64) (int64, error) {
	if _, err := b.db.ExecContext(ctx, b.d.UpsertCounter(), table, key, delta); err != nil {
		return 0, err
	}
	var v int64
	err := b.db.QueryRowContext(ctx,
		`SELECT val FROM kv_counter WHERE tbl=? AND k=?`, table, key).Scan(&v)
	return v, err
}

func (b *Backend) Ranking() framework.Ranking     { return &ranking{b: b} }
func (b *Backend) Ping(ctx context.Context) error { return b.db.PingContext(ctx) }
func (b *Backend) Raw() any                       { return b.db }
func (b *Backend) Close() error                   { return b.db.Close() }

// ranking SQL 有序表实现：score DESC, member ASC 与 Redis ZREVRANGE 确定性一致。
type ranking struct{ b *Backend }

func (r *ranking) ZAdd(ctx context.Context, table, member string, score int64) error {
	_, err := r.b.db.ExecContext(ctx, r.b.d.UpsertZAdd(), table, member, score)
	return err
}

// ZIncrBy 事务内"upsert 增量 + 读回"，保证并发加分不丢。
func (r *ranking) ZIncrBy(ctx context.Context, table, member string, delta int64) (int64, error) {
	tx, err := r.b.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, r.b.d.UpsertZIncr(), table, member, delta); err != nil {
		return 0, err
	}
	var score int64
	if err := tx.QueryRowContext(ctx,
		`SELECT score FROM zset WHERE tbl=? AND member=?`, table, member).Scan(&score); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return score, nil
}

func (r *ranking) ZRevRange(ctx context.Context, table string, start, stop int64) ([]framework.RankItem, error) {
	// Redis 闭区间语义：stop=-1 表示到末尾。
	var limit int64 = -1
	if stop >= 0 {
		if stop < start {
			return nil, fmt.Errorf("sqlstore: invalid range [%d,%d]", start, stop)
		}
		limit = stop - start + 1
	}
	q := `SELECT member, score FROM zset WHERE tbl=? ORDER BY score DESC, member ASC LIMIT ? OFFSET ?`
	rows, err := r.b.db.QueryContext(ctx, q, table, limit, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]framework.RankItem, 0)
	for rows.Next() {
		var it framework.RankItem
		if err := rows.Scan(&it.Member, &it.Score); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

func (r *ranking) ZRank(ctx context.Context, table, member string) (int64, error) {
	// 正序名次 = 严格排在前面的成员数（分数小者在前；同分 member 字典序在前）。
	const q = `
SELECT COUNT(*) FROM zset a, zset b
WHERE a.tbl=? AND a.member=? AND b.tbl=a.tbl
  AND (b.score < a.score OR (b.score = a.score AND b.member < a.member))`
	var n int64
	err := r.b.db.QueryRowContext(ctx, q, table, member).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, framework.ErrStorageNotFound
	}
	// member 不存在时 COUNT 为 0 且无行错误——需要单独确认存在性。
	if err != nil {
		return 0, err
	}
	var exists int
	if err := r.b.db.QueryRowContext(ctx,
		`SELECT 1 FROM zset WHERE tbl=? AND member=?`, table, member).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, framework.ErrStorageNotFound
		}
		return 0, err
	}
	return n, nil
}

var (
	_ storage.Backend   = (*Backend)(nil)
	_ framework.Ranking = (*ranking)(nil)
)
