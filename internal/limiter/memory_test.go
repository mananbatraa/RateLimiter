package limiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock safe for concurrent reads.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: t0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func memAllowN(t *testing.T, m *MemoryLimiter, key string, r Rule, n int) (allowed int, last Result) {
	t.Helper()
	for range n {
		res, err := m.Allow(context.Background(), key, r, 1)
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

func TestMemoryTokenBucket(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.Now)
	r := Rule{Algorithm: TokenBucket, Limit: 10, Window: time.Second, Burst: 5}

	got, last := memAllowN(t, m, "k", r, 8)
	if got != 5 {
		t.Fatalf("allowed %d of burst, want 5", got)
	}
	if last.Allowed || last.Limit != 5 || last.Remaining != 0 {
		t.Fatalf("last = %+v", last)
	}
	if last.RetryAfter != 100*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 100ms (one token at 10/s)", last.RetryAfter)
	}
	if last.ResetAfter != 500*time.Millisecond {
		t.Fatalf("ResetAfter = %v, want 500ms", last.ResetAfter)
	}

	clk.Advance(300 * time.Millisecond)
	if got, _ := memAllowN(t, m, "k", r, 5); got != 3 {
		t.Fatalf("after 300ms allowed %d, want 3", got)
	}
}

func TestMemorySlidingWindowLog(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.Now)
	r := Rule{Algorithm: SlidingWindowLog, Limit: 3, Window: time.Second}

	memAllowN(t, m, "k", r, 1)
	clk.Advance(400 * time.Millisecond)
	got, last := memAllowN(t, m, "k", r, 4)
	if got != 2 {
		t.Fatalf("allowed %d, want 2", got)
	}
	// The first request (t=0) expires at t=1s; we are at t=400ms.
	if last.RetryAfter != 600*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 600ms", last.RetryAfter)
	}
	if last.ResetAfter != time.Second {
		t.Fatalf("ResetAfter = %v, want 1s", last.ResetAfter)
	}
	clk.Advance(600 * time.Millisecond)
	if got, _ := memAllowN(t, m, "k", r, 2); got != 1 {
		t.Fatalf("after oldest expired allowed %d, want 1", got)
	}
}

func TestMemorySlidingWindowCounter(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.Now)
	r := Rule{Algorithm: SlidingWindowCounter, Limit: 10, Window: time.Second}

	// t0 is on a whole second, so this fills the current fixed window.
	if got, _ := memAllowN(t, m, "k", r, 12); got != 10 {
		t.Fatalf("first window allowed %d, want 10", got)
	}
	// 25% into the next window the previous one still weighs 75% (7.5).
	clk.Advance(1250 * time.Millisecond)
	got, last := memAllowN(t, m, "k", r, 5)
	if got != 2 {
		t.Fatalf("allowed %d, want 2 (10*0.75 + 2 <= 10)", got)
	}
	// est = 9.5, need = 0.5 → 0.5 * 1s / 10 = 50ms.
	if last.RetryAfter != 50*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 50ms", last.RetryAfter)
	}
	// Two windows later everything has aged out.
	clk.Advance(2 * time.Second)
	if got, _ := memAllowN(t, m, "k", r, 10); got != 10 {
		t.Fatalf("after idle allowed %d, want 10", got)
	}
}

func TestMemoryCostAndValidation(t *testing.T) {
	m := NewMemory(nil)
	r := Rule{Algorithm: SlidingWindowLog, Limit: 10, Window: time.Minute}
	for i, want := range []bool{true, true, false} {
		res, err := m.Allow(context.Background(), "c", r, 4)
		if err != nil || res.Allowed != want {
			t.Fatalf("call %d: allowed=%v err=%v, want %v", i, res.Allowed, err, want)
		}
	}
	bad := []struct {
		rule Rule
		cost int64
	}{
		{Rule{Algorithm: "nope", Limit: 1, Window: time.Second}, 1},
		{r, 0},
		{r, 11},
	}
	for _, b := range bad {
		if _, err := m.Allow(context.Background(), "c", b.rule, b.cost); !errors.Is(err, ErrInvalidRule) {
			t.Errorf("Allow(%+v, cost %d) err = %v, want ErrInvalidRule", b.rule, b.cost, err)
		}
	}
}

func TestMemoryAlgorithmChangeResetsState(t *testing.T) {
	m := NewMemory(newFakeClock().Now)
	log := Rule{Algorithm: SlidingWindowLog, Limit: 1, Window: time.Minute}
	memAllowN(t, m, "k", log, 1)
	tb := Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Minute}
	if got, _ := memAllowN(t, m, "k", tb, 1); got != 1 {
		t.Fatal("switching algorithm should start from fresh state")
	}
}

func TestMemoryEviction(t *testing.T) {
	clk := newFakeClock()
	m := NewMemory(clk.Now)
	r := Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second}
	for i := range 100 {
		memAllowN(t, m, string(rune('a'+i%26))+string(rune('0'+i/26)), r, 1)
	}
	if m.Len() != 100 {
		t.Fatalf("Len = %d, want 100", m.Len())
	}
	clk.Advance(time.Minute)
	m.Sweep()
	if m.Len() != 0 {
		t.Fatalf("Len after sweep = %d, want 0", m.Len())
	}

	// Lazy sweeping bounds growth without an explicit Sweep call.
	for i := range 5 * sweepEvery * memoryShards / 4 {
		clk.Advance(time.Second)
		_, _ = m.Allow(context.Background(), string(rune(i)), r, 1)
	}
	if n := m.Len(); n > 5*sweepEvery*memoryShards/4/2 {
		t.Fatalf("Len = %d: expired keys are not being swept", n)
	}
}

func TestMemoryConcurrentNeverExceedsLimit(t *testing.T) {
	m := NewMemory(newFakeClock().Now) // frozen clock: no refill during the test
	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		t.Run(string(alg), func(t *testing.T) {
			r := Rule{Algorithm: alg, Limit: 50, Window: time.Minute}
			var allowed atomic.Int64
			var wg sync.WaitGroup
			for range 20 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 20 {
						res, err := m.Allow(context.Background(), "hot:"+string(alg), r, 1)
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
			if got := allowed.Load(); got != 50 {
				t.Fatalf("allowed %d of 400 concurrent requests, want exactly 50", got)
			}
		})
	}
}

func BenchmarkMemoryAllow(b *testing.B) {
	for _, alg := range []Algorithm{TokenBucket, SlidingWindowLog, SlidingWindowCounter} {
		b.Run(string(alg), func(b *testing.B) {
			m := NewMemory(nil)
			r := Rule{Algorithm: alg, Limit: 1_000_000, Window: time.Second}
			keys := make([]string, 1024)
			for i := range keys {
				keys[i] = fmt.Sprintf("bench:%d", i)
			}
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_, _ = m.Allow(context.Background(), keys[i%len(keys)], r, 1)
					i++
				}
			})
		})
	}
}
