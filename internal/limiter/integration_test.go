//go:build integration

package limiter

// Integration tests run against a real Redis (REDIS_ADDR, default
// localhost:6379): go test -tags integration ./...

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func realRedis(tb testing.TB) *RedisLimiter {
	tb.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		tb.Fatalf("redis at %s not reachable: %v", addr, err)
	}
	tb.Cleanup(func() { _ = rdb.Close() })
	return NewRedis(rdb, "itest:"+tb.Name()+":"+time.Now().Format("150405.000000"))
}

var allAlgorithms = []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter}

// TestConcurrentAtomicity fires many concurrent requests from many goroutines
// (standing in for many app replicas) and checks that exactly Limit get in.
// Without the Lua scripts' atomicity, read-modify-write races would let more
// requests through.
func TestConcurrentAtomicity(t *testing.T) {
	for _, alg := range allAlgorithms {
		t.Run(string(alg), func(t *testing.T) {
			l := realRedis(t)
			rule := Rule{Algorithm: alg, Limit: 100, Window: time.Hour}
			key := l.Key("atomic", "client")

			var allowed atomic.Int64
			var wg sync.WaitGroup
			for range 50 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 10 {
						res, err := l.Allow(context.Background(), key, rule, 1)
						if err != nil {
							t.Error(err)
							return
						}
						if res.Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			if got := allowed.Load(); got != 100 {
				t.Fatalf("allowed %d of 500 concurrent requests, want exactly 100", got)
			}
		})
	}
}

func TestRealClockRefill(t *testing.T) {
	l := realRedis(t)
	rule := Rule{Algorithm: TokenBucket, Limit: 10, Window: time.Second} // 1 token / 100ms
	key := l.Key("refill", "c")
	for range 10 {
		if res, _ := l.Allow(context.Background(), key, rule, 1); !res.Allowed {
			t.Fatal("bucket should start full")
		}
	}
	res, _ := l.Allow(context.Background(), key, rule, 1)
	if res.Allowed {
		t.Fatal("bucket should be empty")
	}
	time.Sleep(res.RetryAfter + 20*time.Millisecond)
	if res, _ := l.Allow(context.Background(), key, rule, 1); !res.Allowed {
		t.Fatal("request after RetryAfter should be allowed")
	}
}

func TestSlidingLogRealExpiry(t *testing.T) {
	l := realRedis(t)
	rule := Rule{Algorithm: SlidingWindowLog, Limit: 2, Window: 200 * time.Millisecond}
	key := l.Key("expiry", "c")
	for range 2 {
		l.Allow(context.Background(), key, rule, 1)
	}
	res, _ := l.Allow(context.Background(), key, rule, 1)
	if res.Allowed || res.RetryAfter <= 0 || res.RetryAfter > 200*time.Millisecond {
		t.Fatalf("got %+v, want rejected with 0 < RetryAfter <= 200ms", res)
	}
	time.Sleep(res.RetryAfter + 20*time.Millisecond)
	if res, _ := l.Allow(context.Background(), key, rule, 1); !res.Allowed {
		t.Fatal("request after RetryAfter should be allowed")
	}
}

func BenchmarkAllow(b *testing.B) {
	for _, alg := range allAlgorithms {
		b.Run(string(alg), func(b *testing.B) {
			l := realRedis(b)
			rule := Rule{Algorithm: alg, Limit: 1 << 40, Window: time.Minute}
			b.RunParallel(func(pb *testing.PB) {
				key := l.Key("bench", requestID()[:6])
				for pb.Next() {
					if _, err := l.Allow(context.Background(), key, rule, 1); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
