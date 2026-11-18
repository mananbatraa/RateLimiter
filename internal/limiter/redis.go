package limiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLimiter evaluates rules atomically inside Redis. It is safe for
// concurrent use and can be shared by any number of application replicas
// pointing at the same Redis.
type RedisLimiter struct {
	client redis.Scripter
	prefix string
}

// NewRedis returns a limiter that stores state under keys beginning with prefix.
func NewRedis(client redis.Scripter, prefix string) *RedisLimiter {
	if prefix == "" {
		prefix = "rl"
	}
	return &RedisLimiter{client: client, prefix: prefix}
}

// Key builds the Redis key for a client key under a named rule. The whole
// suffix is a hash tag so that, on Redis Cluster, each key maps to one slot.
func (l *RedisLimiter) Key(ruleName, key string) string {
	return l.prefix + ":{" + ruleName + ":" + key + "}"
}

// Allow implements Limiter. key must already be namespaced (see Key).
func (l *RedisLimiter) Allow(ctx context.Context, key string, rule Rule, cost int64) (Result, error) {
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

	windowUS := rule.Window.Microseconds()
	var (
		raw any
		err error
	)
	switch rule.Algorithm {
	case TokenBucket:
		rate := float64(rule.Limit) / rule.Window.Seconds()
		raw, err = tokenBucketScript.Run(ctx, l.client, []string{key},
			capacity, strconv.FormatFloat(rate, 'f', -1, 64), cost).Result()
	case SlidingWindowLog:
		raw, err = slidingLogScript.Run(ctx, l.client, []string{key},
			rule.Limit, windowUS, cost, requestID()).Result()
	case SlidingWindowCounter:
		raw, err = slidingCounterScript.Run(ctx, l.client, []string{key},
			rule.Limit, windowUS, cost).Result()
	}
	if err != nil {
		return Result{}, fmt.Errorf("limiter: redis: %w", err)
	}
	return parseResult(raw, capacity)
}

func parseResult(raw any, capacity int64) (Result, error) {
	vals, ok := raw.([]any)
	if !ok || len(vals) != 4 {
		return Result{}, fmt.Errorf("limiter: unexpected script reply %#v", raw)
	}
	allowed, err1 := toInt(vals[0])
	remaining, err2 := toInt(vals[1])
	retry, err3 := toInt(vals[2])
	reset, err4 := toInt(vals[3])
	for _, err := range []error{err1, err2, err3, err4} {
		if err != nil {
			return Result{}, fmt.Errorf("limiter: parse script reply: %w", err)
		}
	}
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:    allowed == 1,
		Limit:      capacity,
		Remaining:  remaining,
		RetryAfter: time.Duration(max(retry, 0)) * time.Microsecond,
		ResetAfter: time.Duration(max(reset, 0)) * time.Microsecond,
	}, nil
}

func toInt(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, err
		}
		return int64(math.Floor(f)), nil
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func requestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
