package sqlstore

import (
	"fmt"
	"strings"
)

// ---- MySQL 方言 ----

type mysqlDialect struct{}

func (mysqlDialect) Name() string           { return "mysql" }
func (mysqlDialect) Placeholder(int) string { return "?" }

func (mysqlDialect) UpsertKV() string {
	return `INSERT INTO kv (tbl,k,val) VALUES (?,?,?)
ON DUPLICATE KEY UPDATE val=VALUES(val)`
}

func (mysqlDialect) UpsertCounter() string {
	return `INSERT INTO kv_counter (tbl,k,val) VALUES (?,?,?)
ON DUPLICATE KEY UPDATE val=val+VALUES(val)`
}

func (mysqlDialect) UpsertZAdd() string {
	return `INSERT INTO zset (tbl,member,score) VALUES (?,?,?)
ON DUPLICATE KEY UPDATE score=VALUES(score)`
}

func (mysqlDialect) UpsertZIncr() string {
	return `INSERT INTO zset (tbl,member,score) VALUES (?,?,?)
ON DUPLICATE KEY UPDATE score=score+VALUES(score)`
}

// ---- SQLite 方言（纯 Go 驱动 modernc.org/sqlite，集成测试用；SQL 行为贴近 MySQL） ----

type sqliteDialect struct{}

func (sqliteDialect) Name() string             { return "sqlite" }
func (sqliteDialect) Placeholder(n int) string { return fmt.Sprintf("?%d", n) }

func (sqliteDialect) UpsertKV() string {
	return `INSERT INTO kv (tbl,k,val) VALUES (?,?,?)
ON CONFLICT(tbl,k) DO UPDATE SET val=excluded.val`
}

func (sqliteDialect) UpsertCounter() string {
	return `INSERT INTO kv_counter (tbl,k,val) VALUES (?,?,?)
ON CONFLICT(tbl,k) DO UPDATE SET val=kv_counter.val+excluded.val`
}

func (sqliteDialect) UpsertZAdd() string {
	return `INSERT INTO zset (tbl,member,score) VALUES (?,?,?)
ON CONFLICT(tbl,member) DO UPDATE SET score=excluded.score`
}

func (sqliteDialect) UpsertZIncr() string {
	return `INSERT INTO zset (tbl,member,score) VALUES (?,?,?)
ON CONFLICT(tbl,member) DO UPDATE SET score=zset.score+excluded.score`
}

func dialectFor(name string) (Dialect, error) {
	switch strings.ToLower(name) {
	case "mysql":
		return mysqlDialect{}, nil
	case "sqlite":
		return sqliteDialect{}, nil
	default:
		return nil, fmt.Errorf("sqlstore: unsupported driver/dialect %q", name)
	}
}
