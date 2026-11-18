package limiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned by Breaker while it is skipping the backend.
var ErrCircuitOpen = errors.New("limiter: circuit open, backend skipped")

// KeyedLimiter is a Limiter that also namespaces client keys per rule.
type KeyedLimiter interface {
	Limiter
	Key(ruleName, key string) string
}

// BreakerOptions tunes a Breaker.
type BreakerOptions struct {
	// Failures is how many consecutive backend errors open the circuit.
	// Defaults to 5.
	Failures int
	// Cooldown is how long the circuit stays open before one trial request
	// is let through. Defaults to 1s.
	Cooldown time.Duration
	// OnStateChange, if set, is called (outside the lock) whenever the
	// circuit opens or closes.
	OnStateChange func(open bool)
	// Now overrides the clock in tests.
	Now func() time.Time
}

// Breaker is a circuit breaker around a backend limiter. When Redis is down,
// every call would otherwise wait for the full timeout before the failure
// policy kicks in; once the circuit is open, calls fail immediately with
// ErrCircuitOpen so the failure policy is applied without added latency.
//
// After Cooldown, a single trial call goes to the backend (half-open). Its
// success closes the circuit; its failure re-opens it for another Cooldown.
type Breaker struct {
	next KeyedLimiter
	opts BreakerOptions

	mu       sync.Mutex
	failures int
	openTill time.Time
	open     bool
	trial    bool // a half-open trial call is in flight
}

// NewBreaker wraps next.
func NewBreaker(next KeyedLimiter, opts BreakerOptions) *Breaker {
	if opts.Failures <= 0 {
		opts.Failures = 5
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Breaker{next: next, opts: opts}
}

// Key implements KeyedLimiter.
func (b *Breaker) Key(ruleName, key string) string { return b.next.Key(ruleName, key) }

// Open reports whether the circuit is currently open.
func (b *Breaker) Open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

// Allow implements Limiter.
func (b *Breaker) Allow(ctx context.Context, key string, rule Rule, cost int64) (Result, error) {
	if !b.acquire() {
		return Result{}, ErrCircuitOpen
	}
	res, err := b.next.Allow(ctx, key, rule, cost)
	b.release(err)
	return res, err
}

// acquire reports whether the call may go to the backend.
func (b *Breaker) acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return true
	}
	if b.trial || b.opts.Now().Before(b.openTill) {
		return false
	}
	b.trial = true
	return true
}

func (b *Breaker) release(err error) {
	var changed, nowOpen bool
	b.mu.Lock()
	wasTrial := b.trial
	b.trial = false
	switch {
	case err == nil:
		b.failures = 0
		if b.open {
			b.open, changed = false, true
		}
	case !countsAsFailure(err):
		// Neither healthy nor unhealthy: leave the state alone. A cancelled
		// trial frees the slot so the next call can try again.
	default:
		b.failures++
		if wasTrial || (!b.open && b.failures >= b.opts.Failures) {
			changed = !b.open
			b.open = true
			b.openTill = b.opts.Now().Add(b.opts.Cooldown)
		}
	}
	nowOpen = b.open
	b.mu.Unlock()
	if changed && b.opts.OnStateChange != nil {
		b.opts.OnStateChange(nowOpen)
	}
}

// countsAsFailure separates backend failures from caller mistakes: an invalid
// rule or a client that hung up says nothing about Redis health.
func countsAsFailure(err error) bool {
	return !errors.Is(err, ErrInvalidRule) && !errors.Is(err, context.Canceled)
}
