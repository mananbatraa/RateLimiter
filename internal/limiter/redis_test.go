package limiter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var t0 = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func setup(t *testing.T) (*RedisLimiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(t0)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedis(rdb, "test"), mr
}

func allowN(t *testing.T, l *RedisLimiter, key string, r Rule, n int) (allowed int, last Result) {
	t.Helper()
	for i := 0; i < n; i++ {
		res, err := l.Allow(context.Background(), key, r, 1)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if res.Allowed {
			allowed++
		}
		last = res
	}
	return allowed, last
}

func TestRuleValidate(t *testing.T) {
	cases := []struct {
		name string
		rule Rule
		ok   bool
	}{
		{"valid", Rule{Algorithm: TokenBucket, Limit: 10, Window: time.Second}, true},
		{"unknown algorithm", Rule{Algorithm: "leaky", Limit: 10, Window: time.Second}, false},
		{"zero limit", Rule{Algorithm: TokenBucket, Limit: 0, Window: time.Second}, false},
		{"tiny window", Rule{Algorithm: SlidingWindowLog, Limit: 1, Window: time.Microsecond}, false},
		{"negative burst", Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second, Burst: -1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.rule.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
			if err != nil && !errors.Is(err, ErrInvalidRule) {
				t.Fatalf("error %v does not wrap ErrInvalidRule", err)
			}
		})
	}
}

func TestAllowRejectsBadCost(t *testing.T) {
	l, _ := setup(t)
	r := Rule{Algorithm: TokenBucket, Limit: 5, Window: time.Second}
	for _, cost := range []int64{0, -1, 6} {
		if _, err := l.Allow(context.Background(), "k", r, cost); !errors.Is(err, ErrInvalidRule) {
			t.Fatalf("cost %d: err = %v, want ErrInvalidRule", cost, err)
		}
	}
}

func TestTokenBucket(t *testing.T) {
	l, mr := setup(t)
	r := Rule{Algorithm: TokenBucket, Limit: 10, Window: 10 * time.Second} // 1 token/s

	got, last := allowN(t, l, "tb", r, 12)
	if got != 10 {
		t.Fatalf("allowed %d of 12, want 10", got)
	}
	if last.Allowed || last.Remaining != 0 {
		t.Fatalf("last = %+v, want rejected with 0 remaining", last)
	}
	if last.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %v, want 1s", last.RetryAfter)
	}
	if last.ResetAfter != 10*time.Second {
		t.Fatalf("ResetAfter = %v, want 10s", last.ResetAfter)
	}

	mr.SetTime(t0.Add(3 * time.Second)) // refills 3 tokens
	if got, _ := allowN(t, l, "tb", r, 5); got != 3 {
		t.Fatalf("after 3s allowed %d, want 3", got)
	}

	mr.SetTime(t0.Add(time.Hour)) // refill is capped at capacity
	if got, _ := allowN(t, l, "tb", r, 15); got != 10 {
		t.Fatalf("after 1h allowed %d, want 10 (capacity)", got)
	}
}

func TestTokenBucketBurst(t *testing.T) {
	l, mr := setup(t)
	// 2 req/s sustained with bursts of 5.
	r := Rule{Algorithm: TokenBucket, Limit: 2, Window: time.Second, Burst: 5}
	got, last := allowN(t, l, "burst", r, 6)
	if got != 5 || last.Limit != 5 {
		t.Fatalf("allowed %d limit %d, want 5/5", got, last.Limit)
	}
	if last.RetryAfter != 500*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 500ms", last.RetryAfter)
	}
	mr.SetTime(t0.Add(500 * time.Millisecond))
	if got, _ := allowN(t, l, "burst", r, 2); got != 1 {
		t.Fatalf("after 500ms allowed %d, want 1", got)
	}
}

func TestTokenBucketCost(t *testing.T) {
	l, _ := setup(t)
	r := Rule{Algorithm: TokenBucket, Limit: 10, Window: time.Second}
	res, err := l.Allow(context.Background(), "cost", r, 7)
	if err != nil || !res.Allowed || res.Remaining != 3 {
		t.Fatalf("first = %+v, %v; want allowed with 3 remaining", res, err)
	}
	res, err = l.Allow(context.Background(), "cost", r, 4)
	if err != nil || res.Allowed {
		t.Fatalf("second = %+v, %v; want rejected", res, err)
	}
	if res.RetryAfter != 100*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 100ms", res.RetryAfter)
	}
}

func TestSlidingWindowLog(t *testing.T) {
	l, mr := setup(t)
	r := Rule{Algorithm: SlidingWindowLog, Limit: 3, Window: 10 * time.Second}

	allowN(t, l, "swl", r, 1) // t=0
	mr.SetTime(t0.Add(4 * time.Second))
	allowN(t, l, "swl", r, 2) // t=4, t=4
	mr.SetTime(t0.Add(6 * time.Second))
	got, last := allowN(t, l, "swl", r, 1)
	if got != 0 {
		t.Fatal("4th request inside window was allowed")
	}
	// Oldest entry (t=0) expires at t=10, i.e. in 4s.
	if last.RetryAfter != 4*time.Second {
		t.Fatalf("RetryAfter = %v, want 4s", last.RetryAfter)
	}

	// Exactly at expiry of the first entry, one slot frees up; no fixed-window
	// boundary burst is possible.
	mr.SetTime(t0.Add(10*time.Second + time.Microsecond))
	if got, _ := allowN(t, l, "swl", r, 2); got != 1 {
		t.Fatalf("at t=10s allowed %d, want 1", got)
	}
	mr.SetTime(t0.Add(14*time.Second + time.Microsecond))
	if got, _ := allowN(t, l, "swl", r, 3); got != 2 {
		t.Fatalf("at t=14s allowed %d, want 2", got)
	}
}

func TestSlidingWindowLogCostRetry(t *testing.T) {
	l, mr := setup(t)
	r := Rule{Algorithm: SlidingWindowLog, Limit: 4, Window: 10 * time.Second}
	allowN(t, l, "c", r, 1) // t=0
	mr.SetTime(t0.Add(2 * time.Second))
	allowN(t, l, "c", r, 3) // t=2 x3
	mr.SetTime(t0.Add(5 * time.Second))
	res, err := l.Allow(context.Background(), "c", r, 2)
	if err != nil || res.Allowed {
		t.Fatalf("got %+v, %v; want rejected", res, err)
	}
	// Needs 2 slots: entries at t=0 and t=2 must expire -> t=12 -> 7s.
	if res.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want 7s", res.RetryAfter)
	}
}

func TestSlidingWindowCounter(t *testing.T) {
	l, mr := setup(t)
	r := Rule{Algorithm: SlidingWindowCounter, Limit: 10, Window: 10 * time.Second}
	// t0 is aligned to a 10s boundary.
	if got, _ := allowN(t, l, "swc", r, 12); got != 10 {
		t.Fatalf("first window allowed %d, want 10", got)
	}

	// 25% into the next window, the previous window still weighs 75%:
	// estimate = 10*0.75 = 7.5, so 2 more fit (7.5+2 <= 10, 7.5+3 > 10).
	mr.SetTime(t0.Add(12500 * time.Millisecond))
	got, last := allowN(t, l, "swc", r, 3)
	if got != 2 {
		t.Fatalf("25%% into window 2 allowed %d, want 2", got)
	}
	if last.Allowed {
		t.Fatal("last request should be rejected")
	}

	// Two full windows later, the old counts no longer matter.
	mr.SetTime(t0.Add(30 * time.Second))
	if got, _ := allowN(t, l, "swc", r, 10); got != 10 {
		t.Fatalf("fresh window allowed %d, want 10", got)
	}
}

func TestSlidingWindowCounterRetryDecay(t *testing.T) {
	l, mr := setup(t)
	r := Rule{Algorithm: SlidingWindowCounter, Limit: 10, Window: 10 * time.Second}
	allowN(t, l, "d", r, 10)
	mr.SetTime(t0.Add(10 * time.Second)) // start of next window, prev weight 1.0
	res, err := l.Allow(context.Background(), "d", r, 1)
	if err != nil || res.Allowed {
		t.Fatalf("got %+v, %v; want rejected", res, err)
	}
	// Need 1 unit: prev decays at 10/10s = 1 unit/s -> 1s.
	if res.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %v, want 1s", res.RetryAfter)
	}
	mr.SetTime(t0.Add(11 * time.Second))
	if res, _ := l.Allow(context.Background(), "d", r, 1); !res.Allowed {
		t.Fatal("request after RetryAfter was rejected")
	}
}

func TestKeysAreIsolated(t *testing.T) {
	l, _ := setup(t)
	r := Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Minute}
	for _, k := range []string{l.Key("api", "alice"), l.Key("api", "bob"), l.Key("login", "alice")} {
		if got, _ := allowN(t, l, k, r, 2); got != 1 {
			t.Fatalf("key %s allowed %d, want 1", k, got)
		}
	}
}

func TestKeysExpire(t *testing.T) {
	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		t.Run(string(alg), func(t *testing.T) {
			l, mr := setup(t)
			r := Rule{Algorithm: alg, Limit: 5, Window: time.Second}
			allowN(t, l, "exp", r, 1)
			if ttl := mr.TTL(StorageKey("exp", alg)); ttl <= 0 || ttl > 5*time.Second {
				t.Fatalf("TTL = %v, want (0, 5s]", ttl)
			}
		})
	}
}

func TestRedisDown(t *testing.T) {
	l, mr := setup(t)
	mr.Close()
	_, err := l.Allow(context.Background(), "k", Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second}, 1)
	if err == nil {
		t.Fatal("expected error when Redis is unreachable")
	}
}

func TestKeyFormat(t *testing.T) {
	l := NewRedis(nil, "")
	if got, want := l.Key("login", "1.2.3.4"), "rl:{login:1.2.3.4}"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
}

func TestAlgorithmChangeUsesFreshState(t *testing.T) {
	// A hot reload can change a rule's algorithm while its keys still exist.
	// Each algorithm stores a different Redis type, so without per-algorithm
	// keys the next script call would fail with WRONGTYPE.
	l, _ := setup(t)
	key := l.Key("api", "alice")
	for _, alg := range []Algorithm{SlidingWindowLog, TokenBucket, SlidingWindowCounter} {
		r := Rule{Algorithm: alg, Limit: 1, Window: time.Minute}
		if got, _ := allowN(t, l, key, r, 2); got != 1 {
			t.Fatalf("%s allowed %d, want 1", alg, got)
		}
	}
	// Switching back finds the original algorithm's state still in force.
	log := Rule{Algorithm: SlidingWindowLog, Limit: 1, Window: time.Minute}
	if got, _ := allowN(t, l, key, log, 1); got != 0 {
		t.Fatalf("sliding log allowed %d after switching back, want 0", got)
	}
}

func TestStorageKeyKeepsHashTag(t *testing.T) {
	l := NewRedis(nil, "")
	k := l.Key("login", "1.2.3.4")
	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		sk := StorageKey(k, alg)
		if len(sk) <= len(k) || sk[:len(k)] != k {
			t.Fatalf("%s: storage key %q must extend %q outside the hash tag", alg, sk, k)
		}
	}
}
