package storage_test

import (
	"context"
	"testing"

	"github.com/rangame/server/internal/storage"
)

type doc struct {
	Name string `json:"name"`
	V    int    `json:"v"`
}

func TestMemoryKV(t *testing.T) {
	ctx := context.Background()
	s := storage.NewMemoryStorage()

	if err := s.Set(ctx, "player", "p1", doc{Name: "ran", V: 1}); err != nil {
		t.Fatal(err)
	}
	var got doc
	if err := s.Get(ctx, "player", "p1", &got); err != nil || got.Name != "ran" || got.V != 1 {
		t.Fatalf("get: %+v err=%v", got, err)
	}

	if err := s.Get(ctx, "player", "missing", &got); !storage.IsNotFound(err) {
		t.Fatalf("missing key err = %v", err)
	}

	if err := s.Del(ctx, "player", "p1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Get(ctx, "player", "p1", &got); !storage.IsNotFound(err) {
		t.Fatalf("deleted key err = %v", err)
	}

	v, err := s.IncrBy(ctx, "counter", "p1:win", 3)
	if err != nil || v != 3 {
		t.Fatalf("incr1 = %d, %v", v, err)
	}
	v, err = s.IncrBy(ctx, "counter", "p1:win", 2)
	if err != nil || v != 5 {
		t.Fatalf("incr2 = %d, %v", v, err)
	}
}

func TestMemoryRanking(t *testing.T) {
	ctx := context.Background()
	s := storage.NewMemoryStorage()
	rk := s.Ranking()

	if err := rk.ZAdd(ctx, "score", "a", 10); err != nil {
		t.Fatal(err)
	}
	if err := rk.ZAdd(ctx, "score", "b", 30); err != nil {
		t.Fatal(err)
	}
	if v, err := rk.ZIncrBy(ctx, "score", "c", 20); err != nil || v != 20 {
		t.Fatalf("zincr c = %d, %v", v, err)
	}
	if v, err := rk.ZIncrBy(ctx, "score", "c", 5); err != nil || v != 25 {
		t.Fatalf("zincr c2 = %d, %v", v, err)
	}

	// 倒序：b(30) c(25) a(10)。
	top, err := rk.ZRevRange(ctx, "score", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 3 || top[0].Member != "b" || top[1].Member != "c" || top[2].Member != "a" {
		t.Fatalf("rank desc wrong: %+v", top)
	}
	page, err := rk.ZRevRange(ctx, "score", 0, 1)
	if err != nil || len(page) != 2 || page[1].Member != "c" {
		t.Fatalf("rank page wrong: %+v %v", page, err)
	}

	// 正序排名：a=0 c=1 b=2。
	if n, err := rk.ZRank(ctx, "score", "c"); err != nil || n != 1 {
		t.Fatalf("zrank c = %d, %v", n, err)
	}
	if _, err := rk.ZRank(ctx, "score", "nobody"); !storage.IsNotFound(err) {
		t.Fatalf("zrank missing = %v", err)
	}
}
