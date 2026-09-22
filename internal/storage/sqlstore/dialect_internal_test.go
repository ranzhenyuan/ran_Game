package sqlstore

import (
	"strings"
	"testing"
)

// 同包测试：方言构造与关键 SQL 子句契约（CI 无 MySQL 服务，防止方言回归）。
func TestDialectFor(t *testing.T) {
	my, err := dialectFor("mysql")
	if err != nil {
		t.Fatal(err)
	}
	if my.Name() != "mysql" || my.Placeholder(3) != "?" {
		t.Fatalf("mysql dialect basics: %s %s", my.Name(), my.Placeholder(3))
	}
	for name, q := range map[string]string{
		"kv":      my.UpsertKV(),
		"counter": my.UpsertCounter(),
		"zadd":    my.UpsertZAdd(),
		"zincr":   my.UpsertZIncr(),
	} {
		if !strings.Contains(q, "ON DUPLICATE KEY UPDATE") {
			t.Fatalf("mysql %s upsert missing clause: %s", name, q)
		}
	}
	// 计数器/zset 增量必须是累加而非覆盖。
	if !strings.Contains(my.UpsertCounter(), "val+VALUES(val)") {
		t.Fatal("mysql counter must accumulate")
	}
	if !strings.Contains(my.UpsertZIncr(), "score+VALUES(score)") {
		t.Fatal("mysql zincr must accumulate")
	}

	li, err := dialectFor("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	for name, q := range map[string]string{
		"kv":      li.UpsertKV(),
		"counter": li.UpsertCounter(),
		"zadd":    li.UpsertZAdd(),
		"zincr":   li.UpsertZIncr(),
	} {
		if !strings.Contains(q, "ON CONFLICT") || !strings.Contains(q, "DO UPDATE") {
			t.Fatalf("sqlite %s upsert missing clause: %s", name, q)
		}
	}
	if li.Placeholder(2) != "?2" {
		t.Fatalf("sqlite placeholder: %s", li.Placeholder(2))
	}
	if _, err := dialectFor("oracle"); err == nil {
		t.Fatal("unknown dialect must error")
	}
}
