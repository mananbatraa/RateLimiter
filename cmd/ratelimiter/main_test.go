package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/mananbatraa/RateLimiter/internal/config"
)

const testRules = `
failure_policy: local
rules:
  - name: login
    path_prefix: /api/login
    methods: [POST]
    algorithm: sliding_window_log
    limit: 2
    window: 1m
default: {algorithm: token_bucket, limit: 100, window: 1s, burst: 100}
`

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func writeRules(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// startApp builds the real wiring against miniredis and serves it on a
// random port. It returns the base URL, the Redis server, the rules file and
// the reload channel; the app is shut down when the test ends.
func startApp(t *testing.T) (string, *miniredis.Miniredis, string, chan os.Signal) {
	t.Helper()
	mr := miniredis.RunT(t)
	rulesPath := filepath.Join(t.TempDir(), "rules.yaml")
	writeRules(t, rulesPath, testRules)

	env := map[string]string{
		"REDIS_ADDR":       mr.Addr(),
		"RULES_PATH":       rulesPath,
		"ADMIN_TOKEN":      "tok",
		"BREAKER_FAILURES": "1",
		"BREAKER_COOLDOWN": "1h",
	}
	settings, err := config.FromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	a, err := build(settings, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reload := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() { done <- a.serve(ctx, ln, reload) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve returned %v after shutdown", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("serve did not shut down")
		}
		a.Close()
	})
	return "http://" + ln.Addr().String(), mr, rulesPath, reload
}

func do(t *testing.T, method, url string, hdr map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	return res
}

func TestWiringLimitsThroughRedis(t *testing.T) {
	base, mr, _, _ := startApp(t)

	for i, want := range []int{200, 200, 429} {
		if res := do(t, "POST", base+"/api/login", nil); res.StatusCode != want {
			t.Fatalf("login #%d: %d, want %d", i+1, res.StatusCode, want)
		}
	}
	if len(mr.Keys()) == 0 {
		t.Fatal("expected limiter state in Redis")
	}
	if res := do(t, "GET", base+"/readyz", nil); res.StatusCode != 200 {
		t.Fatalf("readyz: %d", res.StatusCode)
	}
	res := do(t, "GET", base+"/metrics", nil)
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "ratelimiter_decisions_total") || !strings.Contains(string(body), "go_goroutines") {
		t.Fatal("metrics endpoint is missing limiter or runtime metrics")
	}
}

func TestWiringFallsBackLocallyWhenRedisDies(t *testing.T) {
	base, mr, _, _ := startApp(t)
	mr.Close()

	if res := do(t, "GET", base+"/readyz", nil); res.StatusCode != 503 {
		t.Fatalf("readyz with Redis down: %d", res.StatusCode)
	}
	// failure_policy: local keeps enforcing the login limit in process.
	for i, want := range []int{200, 200, 429} {
		if res := do(t, "POST", base+"/api/login", nil); res.StatusCode != want {
			t.Fatalf("login #%d during outage: %d, want %d", i+1, res.StatusCode, want)
		}
	}
	res := do(t, "GET", base+"/metrics", nil)
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "ratelimiter_breaker_open 1") {
		t.Fatal("breaker should be open after Redis failures")
	}
}

func adminSHA(t *testing.T, base string) string {
	t.Helper()
	res := do(t, "GET", base+"/admin/rules", map[string]string{"Authorization": "Bearer tok"})
	if res.StatusCode != 200 {
		t.Fatalf("admin/rules: %d", res.StatusCode)
	}
	var v struct {
		SHA256 string `json:"sha256"`
	}
	if err := json.NewDecoder(res.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v.SHA256
}

func TestWiringReloadsRulesOnSignal(t *testing.T) {
	base, _, rulesPath, reload := startApp(t)
	before := adminSHA(t, base)

	writeRules(t, rulesPath, "failure_policy: closed\ndefault: {limit: 1, window: 1m}\n")
	reload <- syscall.SIGHUP

	deadline := time.Now().Add(3 * time.Second)
	for adminSHA(t, base) == before {
		if time.Now().After(deadline) {
			t.Fatal("rules were not reloaded after SIGHUP")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i, want := range []int{200, 429} {
		if res := do(t, "GET", base+"/api/hello", nil); res.StatusCode != want {
			t.Fatalf("hello #%d after reload: %d, want %d", i+1, res.StatusCode, want)
		}
	}
}

func TestRunRejectsBadConfig(t *testing.T) {
	cases := map[string]map[string]string{
		"bad env":            {"BREAKER_FAILURES": "0"},
		"missing rules file": {"RULES_PATH": filepath.Join(t.TempDir(), "nope.yaml")},
		"bad upstream":       {"RULES_PATH": "../../config/rules.yaml", "UPSTREAM_URL": "://bad"},
		"bad listen addr":    {"RULES_PATH": "../../config/rules.yaml", "LISTEN_ADDR": "not-an-addr"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			err := run(context.Background(), func(k string) string { return env[k] }, nil, quietLogger())
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestRunServesUntilCancelled(t *testing.T) {
	mr := miniredis.RunT(t)
	env := map[string]string{
		"REDIS_ADDR":  mr.Addr(),
		"RULES_PATH":  "../../config/rules.yaml",
		"LISTEN_ADDR": "127.0.0.1:0",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := run(ctx, func(k string) string { return env[k] }, nil, quietLogger()); err != nil {
		t.Fatalf("run: %v", err)
	}
}
