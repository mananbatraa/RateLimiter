// Package limiter implements distributed rate limiting algorithms backed by Redis.
//
// Every algorithm runs as a single Lua script so that the read-modify-write
// cycle is atomic on the Redis server, and every script reads the clock with
// Redis TIME so that all application replicas agree on "now" regardless of
// local clock skew.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Algorithm names a rate limiting strategy.
type Algorithm string

const (
	// TokenBucket allows bursts up to Burst and refills at Limit/Window.
	TokenBucket Algorithm = "token_bucket"
	// SlidingWindowLog is exact: it stores one entry per request in a sorted set.
	SlidingWindowLog Algorithm = "sliding_window_log"
	// SlidingWindowCounter approximates a sliding window with two fixed-window
	// counters, using O(1) memory per key.
	SlidingWindowCounter Algorithm = "sliding_window_counter"
)

// ErrInvalidRule is returned when a Rule cannot be evaluated.
var ErrInvalidRule = errors.New("limiter: invalid rule")

// Rule describes how many requests a key may make.
type Rule struct {
	Algorithm Algorithm
	// Limit is the number of requests allowed per Window.
	Limit int64
	// Window is the period Limit applies to.
	Window time.Duration
	// Burst is the token bucket capacity. Defaults to Limit. Ignored by the
	// sliding window algorithms.
	Burst int64
}

// Validate reports whether the rule is usable.
func (r Rule) Validate() error {
	switch r.Algorithm {
	case TokenBucket, SlidingWindowLog, SlidingWindowCounter:
	default:
		return fmt.Errorf("%w: unknown algorithm %q", ErrInvalidRule, r.Algorithm)
	}
	if r.Limit <= 0 {
		return fmt.Errorf("%w: limit must be > 0", ErrInvalidRule)
	}
	if r.Window < time.Millisecond {
		return fmt.Errorf("%w: window must be >= 1ms", ErrInvalidRule)
	}
	if r.Burst < 0 {
		return fmt.Errorf("%w: burst must be >= 0", ErrInvalidRule)
	}
	return nil
}

// Capacity is the maximum number of requests that can be served at once.
func (r Rule) Capacity() int64 {
	if r.Algorithm == TokenBucket && r.Burst > 0 {
		return r.Burst
	}
	return r.Limit
}

// Result is the outcome of a single Allow call.
type Result struct {
	Allowed bool
	// Limit is the rule's capacity, suitable for a RateLimit-Limit header.
	Limit int64
	// Remaining is how many more requests could be made right now.
	Remaining int64
	// RetryAfter is how long to wait before the request would be allowed.
	// Zero when Allowed is true.
	RetryAfter time.Duration
	// ResetAfter is how long until the key is back at full capacity.
	ResetAfter time.Duration
}

// Limiter decides whether a request identified by key may proceed.
type Limiter interface {
	// Allow consumes cost units for key under rule.
	Allow(ctx context.Context, key string, rule Rule, cost int64) (Result, error)
}
