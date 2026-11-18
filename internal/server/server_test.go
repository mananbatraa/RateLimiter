package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/mananbatraa/RateLimiter/internal/config"
	"github.com/mananbatraa/RateLimiter/internal/limiter"
)

type pinger struct{ err error }

func (p pinger) Ping(context.Context) error { return p.err }

func newServer(t *testing.T, settings config.Server, health Pinger) http.Handler {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	rules, err := config.ParseRules([]byte("default: {limit: 2, window: 1m}"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(Deps{
		Limiter:  limiter.NewRedis(rdb, "t"),
		Rules:    rules,
		Settings: settings,
		Registry: prometheus.NewRegistry(),
		Health:   health,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", path, nil)
	req.RemoteAddr = "1.2.3.4:5"
	h.ServeHTTP(rec, req)
	return rec
}

func TestOpsEndpointsAreNotLimited(t *testing.T) {
	h := newServer(t, config.Server{}, pinger{})
	for range 5 {
		for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
			if rec := get(h, p); rec.Code != 200 {
				t.Fatalf("%s: %d", p, rec.Code)
			}
		}
	}
}

func TestReadyzReportsRedis(t *testing.T) {
	h := newServer(t, config.Server{}, pinger{err: errors.New("down")})
	if rec := get(h, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", rec.Code)
	}
}

func TestDemoAPIIsLimitedAndMetricsExported(t *testing.T) {
	h := newServer(t, config.Server{}, pinger{})
	codes := []int{get(h, "/api/hello").Code, get(h, "/api/hello").Code, get(h, "/api/hello").Code}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("codes = %v", codes)
	}
	body := get(h, "/metrics").Body.String()
	for _, want := range []string{
		`ratelimiter_decisions_total{algorithm="token_bucket",outcome="allowed",rule="default"} 2`,
		`ratelimiter_decisions_total{algorithm="token_bucket",outcome="limited",rule="default"} 1`,
		`ratelimiter_decision_duration_seconds_count{algorithm="token_bucket",rule="default"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestUpstreamProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "upstream:"+r.URL.Path)
	}))
	defer upstream.Close()

	h := newServer(t, config.Server{UpstreamURL: upstream.URL}, pinger{})
	rec := get(h, "/anything")
	if rec.Code != 200 || rec.Body.String() != "upstream:/anything" {
		t.Fatalf("proxy: %d %q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("RateLimit-Remaining") != "1" {
		t.Fatalf("proxied response missing rate limit headers: %v", rec.Header())
	}
}

func TestBadUpstreamURL(t *testing.T) {
	_, err := New(Deps{
		Rules:    &config.Rules{},
		Settings: config.Server{UpstreamURL: "://bad"},
		Registry: prometheus.NewRegistry(),
	})
	if err == nil {
		t.Fatal("expected error for invalid upstream URL")
	}
}
