package limiter

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// The property tests drive random traffic through a limiter on a simulated
// clock and check, after the fact, that the admitted requests never exceed
// what the algorithm promises. They run against both backends: the Lua
// scripts (in miniredis) and the in-process MemoryLimiter.

type step struct {
	advance time.Duration
	cost    int64
}

type admitted struct {
	at   time.Duration // offset from the start
	cost int64
}

type backend struct {
	name string
	// new returns a limiter and a function that moves its clock forward.
	new func(t *testing.T) (Limiter, func(time.Duration))
}

var backends = []backend{
	{"memory", func(*testing.T) (Limiter, func(time.Duration)) {
		clk := newFakeClock()
		return NewMemory(clk.Now), clk.Advance
	}},
	{"redis_lua", func(t *testing.T) (Limiter, func(time.Duration)) {
		mr := miniredis.RunT(t)
		now := t0
		mr.SetTime(now)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return NewRedis(rdb, "prop"), func(d time.Duration) {
			now = now.Add(d)
			mr.SetTime(now)
		}
	}},
}

func randomSteps(rng *rand.Rand, n int, window time.Duration, maxCost int64) []step {
	steps := make([]step, n)
	for i := range steps {
		var adv time.Duration
		switch rng.IntN(4) {
		case 0: // burst: same instant
		case 1:
			adv = time.Duration(rng.Int64N(int64(window/20))) + time.Millisecond
		case 2:
			adv = time.Duration(rng.Int64N(int64(window/2))) + time.Millisecond
		default:
			adv = time.Duration(rng.Int64N(int64(2 * window)))
		}
		steps[i] = step{advance: adv.Truncate(time.Millisecond), cost: rng.Int64N(maxCost) + 1}
	}
	return steps
}

func run(t *testing.T, l Limiter, advance func(time.Duration), rule Rule, steps []step) []admitted {
	t.Helper()
	var (
		now time.Duration
		out []admitted
	)
	for _, s := range steps {
		advance(s.advance)
		now += s.advance
		res, err := l.Allow(context.Background(), "prop", rule, s.cost)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if res.Remaining < 0 || res.Remaining > rule.Capacity() {
			t.Fatalf("Remaining %d outside [0, %d]", res.Remaining, rule.Capacity())
		}
		if res.Allowed && res.RetryAfter != 0 {
			t.Fatalf("allowed request has RetryAfter %v", res.RetryAfter)
		}
		if !res.Allowed && res.RetryAfter <= 0 {
			t.Fatalf("rejected request has RetryAfter %v", res.RetryAfter)
		}
		if res.Allowed {
			out = append(out, admitted{now, s.cost})
		}
	}
	return out
}

// sumIn returns the cost admitted in the half-open interval (from, to].
func sumIn(adm []admitted, from, to time.Duration) int64 {
	var n int64
	for _, a := range adm {
		if a.at > from && a.at <= to {
			n += a.cost
		}
	}
	return n
}

func forEachSeed(t *testing.T, fn func(t *testing.T, rng *rand.Rand)) {
	seeds := 12
	if testing.Short() {
		seeds = 5
	}
	for seed := range seeds {
		rng := rand.New(rand.NewPCG(uint64(seed), 0x5eed))
		fn(t, rng)
		if t.Failed() {
			t.Fatalf("failed with seed %d", seed)
		}
	}
}

func TestPropertySlidingLogNeverExceedsLimitInAnyWindow(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			forEachSeed(t, func(t *testing.T, rng *rand.Rand) {
				rule := Rule{Algorithm: SlidingWindowLog, Limit: rng.Int64N(20) + 1, Window: time.Second}
				l, adv := b.new(t)
				adm := run(t, l, adv, rule, randomSteps(rng, 300, rule.Window, rule.Limit))
				for _, a := range adm {
					// Every window ending at an admission is the worst case.
					if got := sumIn(adm, a.at-rule.Window, a.at); got > rule.Limit {
						t.Fatalf("%d admitted in window ending %v, limit %d", got, a.at, rule.Limit)
					}
				}
			})
		})
	}
}

func TestPropertyTokenBucketRespectsBurstPlusRefill(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			forEachSeed(t, func(t *testing.T, rng *rand.Rand) {
				rule := Rule{
					Algorithm: TokenBucket,
					Limit:     rng.Int64N(50) + 1,
					Window:    time.Second,
					Burst:     rng.Int64N(20) + 1,
				}
				l, adv := b.new(t)
				adm := run(t, l, adv, rule, randomSteps(rng, 300, rule.Window, rule.Capacity()))
				rate := float64(rule.Limit) / rule.Window.Seconds()
				for i, first := range adm {
					for _, last := range adm[i:] {
						span := (last.at - first.at).Seconds()
						got := sumIn(adm, first.at-1, last.at)
						// +1e-3 absorbs the 6-decimal rounding of stored tokens.
						if float64(got) > float64(rule.Capacity())+rate*span+1e-3 {
							t.Fatalf("%d admitted over %.3fs, bound %.3f", got, span,
								float64(rule.Capacity())+rate*span)
						}
					}
				}
			})
		})
	}
}

func TestPropertySlidingCounterBoundedPerFixedWindow(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			forEachSeed(t, func(t *testing.T, rng *rand.Rand) {
				rule := Rule{Algorithm: SlidingWindowCounter, Limit: rng.Int64N(20) + 1, Window: time.Second}
				l, adv := b.new(t)
				adm := run(t, l, adv, rule, randomSteps(rng, 300, rule.Window, rule.Limit))
				// t0 sits on a window boundary, so offsets align with fixed windows.
				perWindow := map[int64]int64{}
				for _, a := range adm {
					perWindow[int64(a.at/rule.Window)] += a.cost
				}
				for w, n := range perWindow {
					// The estimate counts the current window in full, so it can
					// never admit more than the limit in one fixed window...
					if n > rule.Limit {
						t.Fatalf("window %d admitted %d, limit %d", w, n, rule.Limit)
					}
					// ...and any sliding window spans at most two fixed ones.
					if n2 := n + perWindow[w+1]; n2 > 2*rule.Limit {
						t.Fatalf("windows %d-%d admitted %d", w, w+1, n2)
					}
				}
			})
		})
	}
}

// TestMemoryMatchesLua checks that the fallback limiter makes the same
// decisions as the Redis scripts, so degrading to it changes where state
// lives, not how clients are treated.
func TestMemoryMatchesLua(t *testing.T) {
	for _, alg := range []Algorithm{SlidingWindowLog, SlidingWindowCounter, TokenBucket} {
		t.Run(string(alg), func(t *testing.T) {
			forEachSeed(t, func(t *testing.T, rng *rand.Rand) {
				// Limits that divide the window evenly keep token arithmetic
				// exact in both float64 and the scripts' 6-decimal storage.
				rule := Rule{Algorithm: alg, Limit: []int64{1, 2, 4, 5, 8, 10, 20}[rng.IntN(7)], Window: time.Second}
				steps := randomSteps(rng, 200, rule.Window, rule.Capacity())
				mem, advM := backends[0].new(t)
				lua, advL := backends[1].new(t)
				for i, s := range steps {
					advM(s.advance)
					advL(s.advance)
					a, err1 := mem.Allow(context.Background(), "k", rule, s.cost)
					b, err2 := lua.Allow(context.Background(), "k", rule, s.cost)
					if err1 != nil || err2 != nil {
						t.Fatalf("errors: %v, %v", err1, err2)
					}
					if a.Allowed != b.Allowed || a.Remaining != b.Remaining {
						t.Fatalf("step %d (%+v): memory %+v, lua %+v", i, s, a, b)
					}
				}
			})
		})
	}
}
