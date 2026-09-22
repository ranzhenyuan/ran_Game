package transport

import (
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewTokenBucket(10, 5, now) // 10 tokens/s，桶容量 5，初始满

	for i := 0; i < 5; i++ {
		if !b.Allow(now) {
			t.Fatalf("token %d should be allowed", i)
		}
	}
	if b.Allow(now) {
		t.Fatal("6th token at same instant should be denied")
	}

	// 0.5s 后补充 5 个令牌。
	if b.Allow(now.Add(500 * time.Millisecond)) {
		// 补 5 个，消耗 1 个成功
	} else {
		t.Fatal("token after refill should be allowed")
	}
}

func TestTokenBucketUnlimited(t *testing.T) {
	b := NewTokenBucket(0, 0, time.Unix(0, 0))
	for i := 0; i < 1000; i++ {
		if !b.Allow(time.Unix(0, 0)) {
			t.Fatal("rate<=0 means unlimited")
		}
	}
}
