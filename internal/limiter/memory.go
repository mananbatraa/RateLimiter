package limiter

import (
	"context"
	"fmt"
	"hash/maphash"
	"math"
	"sync"
	"time"
)

const (
	memoryShards = 64
	// sweepEvery is how many calls a shard serves between expiry sweeps.
	sweepEvery = 1024
)

// MemoryLimiter implements the same three algorithms as RedisLimiter, in
// process. State is not shared between replicas, so it is meant as a
// degraded-mode fallback while Redis is unavailable (see config failure_policy
// "local"), or for single-instance deployments and tests.
//
// It is safe for concurrent use. Keys are spread over shards, each with its
// own mutex, and idle keys are evicted lazily once their state has expired.
type MemoryLimiter struct {
	now    func() time.Time
	seed   maphash.Seed
	shards [memoryShards]memShard
}

type memShard struct {
	mu    sync.Mutex
	calls int
	keys  map[string]*memState
}

// memState holds the state of one key. Only the fields used by the key's
// algorithm are set.
type memState struct {
	algo    Algorithm
	expires time.Time

	// token bucket
	tokens float64
	ts     time.Time

	// sliding window log: one admission time per consumed unit, oldest first
	log []time.Time

	// sliding window counter
	start      time.Time
	curr, prev int64
}

// NewMemory returns an in-process limiter. now may be nil to use time.Now.
func NewMemory(now func() time.Time) *MemoryLimiter {
	if now == nil {
		now = time.Now
	}
	m := &MemoryLimiter{now: now, seed: maphash.MakeSeed()}
	for i := range m.shards {
		m.shards[i].keys = make(map[string]*memState)
	}
	return m
}

// Allow implements Limiter.
func (m *MemoryLimiter) Allow(_ context.Context, key string, rule Rule, cost int64) (Result, error) {
	if err := rule.Validate(); err != nil {
		return Result{}, err
	}
	if cost <= 0 {
		return Result{}, fmt.Errorf("%w: cost must be > 0", ErrInvalidRule)
	}
	capacity := rule.Capacity()
	if cost > capacity {
		return Result{}, fmt.Errorf("%w: cost %d exceeds capacity %d", ErrInvalidRule, cost, capacity)
	}

	sh := &m.shards[maphash.String(m.seed, key)%memoryShards]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := m.now()
	sh.calls++
	if sh.calls%sweepEvery == 0 {
		sh.sweep(now)
	}

	st := sh.keys[key]
	if st == nil || st.algo != rule.Algorithm || !now.Before(st.expires) {
		st = &memState{algo: rule.Algorithm}
		sh.keys[key] = st
	}

	var res Result
	switch rule.Algorithm {
	case TokenBucket:
		res = st.tokenBucket(now, rule, capacity, cost)
	case SlidingWindowLog:
		res = st.slidingLog(now, rule, cost)
	case SlidingWindowCounter:
		res = st.slidingCounter(now, rule, cost)
	}
	res.Limit = capacity
	return res, nil
}

// Len reports how many keys are currently tracked, including expired keys
// that have not been swept yet.
func (m *MemoryLimiter) Len() int {
	n := 0
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		n += len(sh.keys)
		sh.mu.Unlock()
	}
	return n
}

// Sweep evicts every expired key. Allow calls it lazily; it is exported for
// callers that want to bound memory on a timer.
func (m *MemoryLimiter) Sweep() {
	now := m.now()
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		sh.sweep(now)
		sh.mu.Unlock()
	}
}

func (sh *memShard) sweep(now time.Time) {
	for k, st := range sh.keys {
		if !now.Before(st.expires) {
			delete(sh.keys, k)
		}
	}
}

func (st *memState) tokenBucket(now time.Time, rule Rule, capacity, cost int64) Result {
	rate := float64(rule.Limit) / rule.Window.Seconds() // tokens per second
	if st.ts.IsZero() {
		st.tokens, st.ts = float64(capacity), now
	}
	if elapsed := now.Sub(st.ts); elapsed > 0 {
		st.tokens = math.Min(float64(capacity), st.tokens+elapsed.Seconds()*rate)
		// Round to 6 decimals like the Lua script stores it. Without this,
		// float drift can leave 0.9999999 tokens where 1 is due.
		st.tokens = math.Round(st.tokens*1e6) / 1e6
	}
	st.ts = now

	res := Result{}
	if st.tokens >= float64(cost) {
		st.tokens -= float64(cost)
		res.Allowed = true
	} else {
		res.RetryAfter = secondsCeilUS((float64(cost) - st.tokens) / rate)
	}
	refill := secondsCeilUS(float64(capacity) / rate)
	st.expires = now.Add(refill + time.Second)
	res.Remaining = int64(math.Floor(st.tokens))
	res.ResetAfter = secondsCeilUS((float64(capacity) - st.tokens) / rate)
	return res
}

func (st *memState) slidingLog(now time.Time, rule Rule, cost int64) Result {
	// Drop entries at or before now-window, matching ZREMRANGEBYSCORE -inf..now-window.
	cutoff := now.Add(-rule.Window)
	i := 0
	for i < len(st.log) && !st.log[i].After(cutoff) {
		i++
	}
	st.log = st.log[i:]
	count := int64(len(st.log))

	res := Result{}
	if count+cost <= rule.Limit {
		for range cost {
			st.log = append(st.log, now)
		}
		count += cost
		res.Allowed = true
	} else {
		oldest := st.log[count+cost-rule.Limit-1]
		res.RetryAfter = oldest.Add(rule.Window).Sub(now)
	}
	res.Remaining = max(rule.Limit-count, 0)
	if n := len(st.log); n > 0 {
		res.ResetAfter = st.log[n-1].Add(rule.Window).Sub(now)
	}
	st.expires = now.Add(rule.Window + time.Second)
	return res
}

func (st *memState) slidingCounter(now time.Time, rule Rule, cost int64) Result {
	window := rule.Window
	start := now.Truncate(window)
	if !st.start.Equal(start) {
		if st.start.Equal(start.Add(-window)) {
			st.prev = st.curr
		} else {
			st.prev = 0
		}
		st.curr = 0
		st.start = start
	}

	into := now.Sub(start)
	weight := float64(window-into) / float64(window)
	est := float64(st.prev)*weight + float64(st.curr)

	res := Result{}
	if est+float64(cost) <= float64(rule.Limit) {
		st.curr += cost
		est += float64(cost)
		res.Allowed = true
	} else {
		need := est + float64(cost) - float64(rule.Limit)
		if st.prev > 0 && float64(st.prev)*weight >= need {
			res.RetryAfter = time.Duration(math.Ceil(need * float64(window) / float64(st.prev)))
		} else {
			res.RetryAfter = window - into
		}
	}
	res.Remaining = max(int64(math.Floor(float64(rule.Limit)-est)), 0)
	res.ResetAfter = window - into
	if st.curr > 0 {
		res.ResetAfter += window
	}
	st.expires = start.Add(2*window + time.Second)
	return res
}

func secondsCeilUS(s float64) time.Duration {
	return time.Duration(math.Ceil(s*1e6)) * time.Microsecond
}
