package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/mananbatraa/RateLimiter/internal/config"
	"github.com/mananbatraa/RateLimiter/internal/limiter"
	"github.com/mananbatraa/RateLimiter/internal/metrics"
)

const rulesYAML = `
rules:
  - name: login
    path_prefix: /login
    methods: [POST]
    algorithm: sliding_window_log
    limit: 2
    window: 1m
  - name: keyed
    path_prefix: /keyed
    limit: 1
    window: 1m
    key: header:X-API-Key
  - name: shared
    path_prefix: /shared
    limit: 1
    window: 1m
    key: global
default:
  limit: 3
  window: 1s
`

type harness struct {
	handler http.Handler
	metrics *metrics.Metrics
	mr      *miniredis.Miniredis
}

func newHarness(t *testing.T, rules string, lim KeyedLimiter) *harness {
	t.Helper()
	rs, err := config.ParseRules([]byte(rules))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{}
	if lim == nil {
		h.mr = miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: h.mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		lim = limiter.NewRedis(rdb, "t")
	}
	h.metrics = metrics.New(prometheus.NewRegistry())
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h.handler = RateLimit(Options{Limiter: lim, Rules: rs, Metrics: h.metrics, Timeout: time.Second})(ok)
	return h
}

func (h *harness) do(method, path, remote string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.RemoteAddr = remote
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func TestLimitsAndHeaders(t *testing.T) {
	h := newHarness(t, rulesYAML, nil)
	for i, want := range []int{200, 200, 429} {
		rec := h.do("POST", "/login", "10.0.0.1:1234", nil)
		if rec.Code != want {
			t.Fatalf("request %d: status %d, want %d", i, rec.Code, want)
		}
		if rec.Header().Get("RateLimit-Limit") != "2" {
			t.Fatalf("RateLimit-Limit = %q", rec.Header().Get("RateLimit-Limit"))
		}
		if rec.Header().Get("RateLimit-Policy") != "2;w=60" {
			t.Fatalf("RateLimit-Policy = %q", rec.Header().Get("RateLimit-Policy"))
		}
	}
	rec := h.do("POST", "/login", "10.0.0.1:1234", nil)
	if rec.Header().Get("Retry-After") != "60" || rec.Header().Get("RateLimit-Remaining") != "0" {
		t.Fatalf("429 headers: %v", rec.Header())
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil || body["error"] != "rate_limited" {
		t.Fatalf("body = %v, err %v", body, err)
	}

	// Another IP has its own budget.
	if rec := h.do("POST", "/login", "10.0.0.2:1234", nil); rec.Code != 200 {
		t.Fatalf("second IP status %d", rec.Code)
	}
	if got := testutil.ToFloat64(h.metrics.Decisions.WithLabelValues("login", "sliding_window_log", "limited")); got != 2 {
		t.Fatalf("limited metric = %v, want 2", got)
	}
	if got := testutil.ToFloat64(h.metrics.Decisions.WithLabelValues("login", "sliding_window_log", "allowed")); got != 3 {
		t.Fatalf("allowed metric = %v, want 3", got)
	}
}

func TestDefaultRuleAndMethodFallthrough(t *testing.T) {
	h := newHarness(t, rulesYAML, nil)
	// GET /login does not match the POST-only login rule, so default (3/s) applies.
	codes := []int{}
	for range 4 {
		codes = append(codes, h.do("GET", "/login", "10.0.0.1:1", nil).Code)
	}
	if codes[2] != 200 || codes[3] != 429 {
		t.Fatalf("codes = %v, want 3x200 then 429", codes)
	}
}

func TestNoMatchPassesThrough(t *testing.T) {
	h := newHarness(t, "rules: [{name: only, path_prefix: /x, limit: 1, window: 1m}]", nil)
	for range 5 {
		rec := h.do("GET", "/y", "1.1.1.1:1", nil)
		if rec.Code != 200 || rec.Header().Get("RateLimit-Limit") != "" {
			t.Fatalf("unmatched request was limited: %d %v", rec.Code, rec.Header())
		}
	}
}

func TestHeaderKey(t *testing.T) {
	h := newHarness(t, rulesYAML, nil)
	if rec := h.do("GET", "/keyed", "1.1.1.1:1", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing key: status %d, want 400", rec.Code)
	}
	a := map[string]string{"X-API-Key": "alice"}
	b := map[string]string{"X-API-Key": "bob"}
	if h.do("GET", "/keyed", "1.1.1.1:1", a).Code != 200 ||
		h.do("GET", "/keyed", "2.2.2.2:1", a).Code != 429 || // same key, different IP
		h.do("GET", "/keyed", "1.1.1.1:1", b).Code != 200 {
		t.Fatal("API key limiting is not keyed on the header")
	}
}

func TestGlobalKey(t *testing.T) {
	h := newHarness(t, rulesYAML, nil)
	if h.do("GET", "/shared", "1.1.1.1:1", nil).Code != 200 || h.do("GET", "/shared", "2.2.2.2:1", nil).Code != 429 {
		t.Fatal("global rule should share one budget across clients")
	}
}

type failingLimiter struct{}

func (failingLimiter) Allow(context.Context, string, limiter.Rule, int64) (limiter.Result, error) {
	return limiter.Result{}, errors.New("connection refused")
}
func (failingLimiter) Key(r, k string) string { return r + k }

func TestFailurePolicy(t *testing.T) {
	open := newHarness(t, "failure_policy: open\ndefault: {limit: 1, window: 1s}", failingLimiter{})
	if rec := open.do("GET", "/", "1.1.1.1:1", nil); rec.Code != 200 {
		t.Fatalf("fail-open status %d, want 200", rec.Code)
	}
	if got := testutil.ToFloat64(open.metrics.Errors.WithLabelValues("default")); got != 1 {
		t.Fatalf("backend error metric = %v", got)
	}

	closed := newHarness(t, "failure_policy: closed\ndefault: {limit: 1, window: 1s}", failingLimiter{})
	rec := closed.do("GET", "/", "1.1.1.1:1", nil)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("fail-closed status %d headers %v", rec.Code, rec.Header())
	}
}

func TestRedisOutageFailsOpen(t *testing.T) {
	h := newHarness(t, "default: {limit: 1, window: 1m}", nil)
	h.do("GET", "/", "1.1.1.1:1", nil)
	h.mr.Close()
	if rec := h.do("GET", "/", "1.1.1.1:1", nil); rec.Code != 200 {
		t.Fatalf("status %d during outage, want 200 (fail open)", rec.Code)
	}
}

func TestClientKey(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.0.2.1:5555"
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")

	if k, _ := ClientKey(req, "ip", false); k != "192.0.2.1" {
		t.Errorf("untrusted proxy: %q", k)
	}
	if k, _ := ClientKey(req, "ip", true); k != "203.0.113.9" {
		t.Errorf("trusted proxy: %q", k)
	}
	req.Header.Set("X-Forwarded-For", "not-an-ip")
	if k, _ := ClientKey(req, "ip", true); k != "192.0.2.1" {
		t.Errorf("garbage XFF should fall back to RemoteAddr, got %q", k)
	}
	req.RemoteAddr = "unix-socket"
	if k, _ := ClientKey(req, "ip", false); k != "unix-socket" {
		t.Errorf("RemoteAddr without port: %q", k)
	}
	if k, ok := ClientKey(req, "header:X-Key", false); ok || k != "" {
		t.Errorf("missing header: %q %v", k, ok)
	}
}

func TestCeilSeconds(t *testing.T) {
	for d, want := range map[time.Duration]int64{0: 0, -time.Second: 0, time.Millisecond: 1, time.Second: 1, 1500 * time.Millisecond: 2} {
		if got := ceilSeconds(d); got != want {
			t.Errorf("ceilSeconds(%v) = %d, want %d", d, got, want)
		}
	}
}

func newLocalHarness(t *testing.T, rules string, lim KeyedLimiter) (*harness, *limiter.MemoryLimiter) {
	t.Helper()
	rs, err := config.ParseRules([]byte(rules))
	if err != nil {
		t.Fatal(err)
	}
	mem := limiter.NewMemory(nil)
	h := &harness{metrics: metrics.New(prometheus.NewRegistry())}
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h.handler = RateLimit(Options{Limiter: lim, Fallback: mem, Rules: rs, Metrics: h.metrics})(ok)
	return h, mem
}

func TestFailLocalUsesFallbackLimiter(t *testing.T) {
	h, _ := newLocalHarness(t, `
failure_policy: local
local_replicas: 2
default: {limit: 4, window: 1m}
`, failingLimiter{})

	// 4 per minute across 2 replicas: this replica enforces 2 on its own.
	codes := []int{}
	for range 3 {
		codes = append(codes, h.do("GET", "/", "1.1.1.1:1", nil).Code)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("codes = %v, want 200 200 429", codes)
	}
	rec := h.do("GET", "/", "1.1.1.1:1", nil)
	if rec.Header().Get("RateLimit-Limit") != "2" || rec.Header().Get("RateLimit-Policy") != "2;w=60" ||
		rec.Header().Get("Retry-After") == "" {
		t.Fatalf("fallback headers: %v", rec.Header())
	}
	// Other clients keep their own local budget.
	if h.do("GET", "/", "2.2.2.2:1", nil).Code != 200 {
		t.Fatal("second client should be allowed")
	}
	for outcome, want := range map[string]float64{"fallback_allowed": 3, "fallback_limited": 2, "error_allowed": 0} {
		if got := testutil.ToFloat64(h.metrics.Decisions.WithLabelValues("default", "token_bucket", outcome)); got != want {
			t.Errorf("%s = %v, want %v", outcome, got, want)
		}
	}
}

func TestFailLocalWithoutFallbackFailsOpen(t *testing.T) {
	h := newHarness(t, "failure_policy: local\ndefault: {limit: 1, window: 1s}", failingLimiter{})
	for range 3 {
		if rec := h.do("GET", "/", "1.1.1.1:1", nil); rec.Code != 200 {
			t.Fatalf("status %d, want 200 when no fallback is configured", rec.Code)
		}
	}
}

func TestRedisOutageFallsBackLocallyThroughBreaker(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	br := limiter.NewBreaker(limiter.NewRedis(rdb, "t"), limiter.BreakerOptions{Failures: 1, Cooldown: time.Hour})
	h, _ := newLocalHarness(t, "failure_policy: local\ndefault: {limit: 3, window: 1m}", br)

	if h.do("GET", "/", "1.1.1.1:1", nil).Code != 200 {
		t.Fatal("first request should be served by Redis")
	}
	mr.Close()
	codes := []int{}
	for range 4 {
		codes = append(codes, h.do("GET", "/", "1.1.1.1:1", nil).Code)
	}
	// The local limiter starts fresh, so 3 more are allowed, then 429.
	if codes[2] != 200 || codes[3] != 429 {
		t.Fatalf("codes during outage = %v, want 3x200 then 429", codes)
	}
	if !br.Open() {
		t.Fatal("breaker should be open during the outage")
	}
}

type reloadable struct{ rules *config.Rules }

func (r *reloadable) Current() *config.Rules { return r.rules }

func TestRulesAreReadPerRequest(t *testing.T) {
	first, err := config.ParseRules([]byte("default: {limit: 1, window: 1m}"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := config.ParseRules([]byte("rules: [{name: open, path_prefix: /, limit: 100, window: 1m}]"))
	if err != nil {
		t.Fatal(err)
	}
	src := &reloadable{rules: first}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := &harness{handler: RateLimit(Options{Limiter: limiter.NewRedis(rdb, "t"), Rules: src})(ok)}

	if h.do("GET", "/", "1.1.1.1:1", nil).Code != 200 || h.do("GET", "/", "1.1.1.1:1", nil).Code != 429 {
		t.Fatal("first rules should allow one request")
	}
	src.rules = second // what a config.Store reload does
	rec := h.do("GET", "/", "1.1.1.1:1", nil)
	if rec.Code != 200 || rec.Header().Get("RateLimit-Limit") != "100" {
		t.Fatalf("after swap: %d %v", rec.Code, rec.Header())
	}
}
