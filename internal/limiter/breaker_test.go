package limiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// stubLimiter returns err (when set) or an allowed result, counting calls.
type stubLimiter struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (s *stubLimiter) Allow(context.Context, string, Rule, int64) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return Result{}, s.err
	}
	return Result{Allowed: true}, nil
}

func (s *stubLimiter) Key(rule, key string) string { return rule + "/" + key }

func (s *stubLimiter) set(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *stubLimiter) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

var anyRule = Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	clk := newFakeClock()
	stub := &stubLimiter{err: errors.New("dial tcp: connection refused")}
	var changes []bool
	b := NewBreaker(stub, BreakerOptions{
		Failures: 3, Cooldown: time.Second, Now: clk.Now,
		OnStateChange: func(open bool) { changes = append(changes, open) },
	})
	ctx := context.Background()

	for i := range 3 {
		if _, err := b.Allow(ctx, "k", anyRule, 1); errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call %d: circuit opened too early", i)
		}
	}
	if !b.Open() {
		t.Fatal("circuit should be open after 3 failures")
	}
	// While open, the backend is not called at all.
	for range 10 {
		if _, err := b.Allow(ctx, "k", anyRule, 1); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("err = %v, want ErrCircuitOpen", err)
		}
	}
	if stub.count() != 3 {
		t.Fatalf("backend called %d times, want 3", stub.count())
	}

	// After the cooldown one trial goes through; it fails, so the circuit
	// re-opens for another cooldown.
	clk.Advance(time.Second)
	if _, err := b.Allow(ctx, "k", anyRule, 1); errors.Is(err, ErrCircuitOpen) {
		t.Fatal("trial call should reach the backend")
	}
	if _, err := b.Allow(ctx, "k", anyRule, 1); !errors.Is(err, ErrCircuitOpen) {
		t.Fatal("failed trial should re-open the circuit")
	}

	// Backend recovers: the next trial closes the circuit.
	stub.set(nil)
	clk.Advance(time.Second)
	if res, err := b.Allow(ctx, "k", anyRule, 1); err != nil || !res.Allowed {
		t.Fatalf("trial: %+v, %v", res, err)
	}
	if b.Open() {
		t.Fatal("successful trial should close the circuit")
	}
	if fmt.Sprint(changes) != "[true false]" {
		t.Fatalf("state changes = %v, want [true false]", changes)
	}
}

func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	stub := &stubLimiter{}
	b := NewBreaker(stub, BreakerOptions{Failures: 2})
	ctx := context.Background()
	for range 5 {
		stub.set(errors.New("timeout"))
		_, _ = b.Allow(ctx, "k", anyRule, 1)
		stub.set(nil)
		_, _ = b.Allow(ctx, "k", anyRule, 1)
	}
	if b.Open() {
		t.Fatal("non-consecutive failures must not open the circuit")
	}
}

func TestBreakerIgnoresCallerErrors(t *testing.T) {
	stub := &stubLimiter{}
	b := NewBreaker(stub, BreakerOptions{Failures: 1})
	ctx := context.Background()
	for _, err := range []error{
		fmt.Errorf("%w: cost too high", ErrInvalidRule),
		fmt.Errorf("limiter: redis: %w", context.Canceled),
	} {
		stub.set(err)
		_, _ = b.Allow(ctx, "k", anyRule, 1)
		if b.Open() {
			t.Fatalf("%v should not open the circuit", err)
		}
	}
	stub.set(fmt.Errorf("limiter: redis: %w", context.DeadlineExceeded))
	_, _ = b.Allow(ctx, "k", anyRule, 1)
	if !b.Open() {
		t.Fatal("a timeout should open the circuit")
	}
}

func TestBreakerSingleTrialWhenHalfOpen(t *testing.T) {
	clk := newFakeClock()
	slow := &blockingLimiter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	stub := &stubLimiter{err: errors.New("down")}
	b := NewBreaker(stub, BreakerOptions{Failures: 1, Cooldown: time.Second, Now: clk.Now})
	_, _ = b.Allow(context.Background(), "k", anyRule, 1)
	if !b.Open() {
		t.Fatal("expected open")
	}

	// Swap in a backend that blocks, then start the half-open trial.
	b.next = slow
	clk.Advance(time.Second)
	done := make(chan error, 1)
	go func() {
		_, err := b.Allow(context.Background(), "k", anyRule, 1)
		done <- err
	}()
	<-slow.entered
	// The trial is in flight: everyone else is still short-circuited.
	if _, err := b.Allow(context.Background(), "k", anyRule, 1); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("concurrent call during trial: err = %v, want ErrCircuitOpen", err)
	}
	close(slow.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.Open() {
		t.Fatal("successful trial should close the circuit")
	}
}

func TestBreakerDelegatesKey(t *testing.T) {
	b := NewBreaker(&stubLimiter{}, BreakerOptions{})
	if got := b.Key("r", "k"); got != "r/k" {
		t.Fatalf("Key = %q", got)
	}
}

// blockingLimiter signals entered, then waits on release before allowing.
type blockingLimiter struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingLimiter) Allow(context.Context, string, Rule, int64) (Result, error) {
	b.entered <- struct{}{}
	<-b.release
	return Result{Allowed: true}, nil
}

func (b *blockingLimiter) Key(rule, key string) string { return rule + ":" + key }
