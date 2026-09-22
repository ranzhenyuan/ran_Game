package sqlstore_test

import (
	"context"
	"sync"
	"testing"

	"github.com/rangame/server/internal/storage/sqlstore"
	"github.com/rangame/server/pkg/framework"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动（测试专用，无 cgo）
)

func openSQLite(t *testing.T) *sqlstore.Backend {
	t.Helper()
	// cache=shared 让连接池各连接看到同一内存库；busy_timeout 保证并发写不返回 BUSY。
	dsn := "file::memory:?cache=shared&_pragma=busy_timeout(5000)"
	b, err := sqlstore.Open(context.Background(), "sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestSQLKVAndCounter(t *testing.T) {
	b := openSQLite(t)
	ctx := context.Background()

	if _, err := b.Get(ctx, "account", "nobody"); err != framework.ErrStorageNotFound {
		t.Fatalf("missing get err = %v", err)
	}
	if err := b.Put(ctx, "account", "p1", []byte(`{"name":"ran"}`)); err != nil {
		t.Fatal(err)
	}
	raw, err := b.Get(ctx, "account", "p1")
	if err != nil || string(raw) != `{"name":"ran"}` {
		t.Fatalf("get: %s %v", raw, err)
	}
	// upsert 覆盖。
	if err := b.Put(ctx, "account", "p1", []byte(`{"name":"ran2"}`)); err != nil {
		t.Fatal(err)
	}
	if raw, _ := b.Get(ctx, "account", "p1"); string(raw) != `{"name":"ran2"}` {
		t.Fatalf("upsert overwrite: %s", raw)
	}
	if err := b.Delete(ctx, "account", "p1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "account", "p1"); err != framework.ErrStorageNotFound {
		t.Fatal("delete should remove row")
	}

	// 计数器：首行插入 + 累加。
	v, err := b.IncrBy(ctx, "wallet", "p1:coin", 100)
	if err != nil || v != 100 {
		t.Fatalf("incr1 = %d %v", v, err)
	}
	v, err = b.IncrBy(ctx, "wallet", "p1:coin", -30)
	if err != nil || v != 70 {
		t.Fatalf("incr2 = %d %v", v, err)
	}
}

func TestSQLRanking(t *testing.T) {
	b := openSQLite(t)
	ctx := context.Background()
	rk := b.Ranking()

	for _, z := range []struct {
		member string
		score  int64
	}{
		{"a", 10}, {"b", 30}, {"c", 20}, {"d", 20},
	} {
		if err := rk.ZAdd(ctx, "score", z.member, z.score); err != nil {
			t.Fatal(err)
		}
	}
	// ZAdd 覆盖分数。
	if err := rk.ZAdd(ctx, "score", "a", 50); err != nil {
		t.Fatal(err)
	}

	top, err := rk.ZRevRange(ctx, "score", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c", "d"} // 50,30,20,20（同分 c<d 字典序）
	for i, m := range want {
		if top[i].Member != m {
			t.Fatalf("top[%d]=%s want %s; full=%+v", i, top[i].Member, m, top)
		}
	}
	// 分页：前两名。
	page, err := rk.ZRevRange(ctx, "score", 0, 1)
	if err != nil || len(page) != 2 || page[0].Member != "a" || page[1].Member != "b" {
		t.Fatalf("page: %+v %v", page, err)
	}
	// ZIncrBy 事务累加。
	v, err := rk.ZIncrBy(ctx, "score", "d", 15)
	if err != nil || v != 35 {
		t.Fatalf("zincr = %d %v", v, err)
	}
	// 正序排名（a=50 最后一名 rank=3；c=20 最前 rank=0）。
	if n, err := rk.ZRank(ctx, "score", "c"); err != nil || n != 0 {
		t.Fatalf("zrank c = %d %v", n, err)
	}
	if n, err := rk.ZRank(ctx, "score", "a"); err != nil || n != 3 {
		t.Fatalf("zrank a = %d %v", n, err)
	}
	if _, err := rk.ZRank(ctx, "score", "ghost"); err != framework.ErrStorageNotFound {
		t.Fatalf("zrank missing = %v", err)
	}
}

// 并发加分：事务保证不丢更新（模拟对局结算并发写排行榜）。
func TestSQLRankingConcurrentIncr(t *testing.T) {
	b := openSQLite(t)
	ctx := context.Background()
	rk := b.Ranking()

	const goroutines, perG = 20, 5
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				if _, err := rk.ZIncrBy(ctx, "score", "winner", 1); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent incr: %v", err)
	}
	v, err := rk.ZIncrBy(ctx, "score", "winner", 0)
	if err != nil || v != goroutines*perG {
		t.Fatalf("lost updates: score=%d err=%v", v, err)
	}
}

// 数据库故障：关闭后操作返回错误（门面层将转 WAL/熔断，此处验证后端错误语义）。
func TestSQLBackendClosed(t *testing.T) {
	b := openSQLite(t)
	ctx := context.Background()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, "t", "k", []byte("1")); err == nil {
		t.Fatal("put on closed db must fail")
	}
	if err := b.Ping(ctx); err == nil {
		t.Fatal("ping on closed db must fail")
	}
}

// MySQL 方言语句契约（CI 无 MySQL 服务，校验关键 upsert 子句，防止方言回归）。
// 真正执行路径由 sqlite 方言的各用例覆盖（schema 与表结构两者共用）。
// （见 dialect_internal_test.go 同包测试）
