package config

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mananbatraa/RateLimiter/internal/limiter"
)

const sample = `
failure_policy: closed
rules:
  - name: login
    path_prefix: /api/login
    methods: [post]
    algorithm: sliding_window_log
    limit: 5
    window: 1m
  - name: api
    path_prefix: /api
    limit: 100
    window: 1m
    key: header:X-API-Key
default:
  limit: 10
  window: 1s
  burst: 20
`

func TestParseRules(t *testing.T) {
	rs, err := ParseRules([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if rs.FailurePolicy != FailClosed {
		t.Fatalf("FailurePolicy = %q", rs.FailurePolicy)
	}
	login := rs.Rules[0]
	if login.Methods[0] != "POST" || login.Window != time.Minute || login.Key != "ip" || login.Cost != 1 {
		t.Fatalf("login rule not normalized: %+v", login)
	}
	api := rs.Rules[1]
	if api.Algorithm != limiter.TokenBucket {
		t.Fatalf("default algorithm = %q, want token_bucket", api.Algorithm)
	}
	if rs.Default.Name != "default" || rs.Default.Burst != 20 {
		t.Fatalf("default rule = %+v", rs.Default)
	}
}

func TestMatch(t *testing.T) {
	rs, err := ParseRules([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ method, path, want string }{
		{"POST", "/api/login", "login"},
		{"GET", "/api/login", "api"}, // method mismatch falls through
		{"GET", "/api/items", "api"},
		{"GET", "/other", "default"},
	}
	for _, tc := range cases {
		got := rs.Match(httptest.NewRequest(tc.method, tc.path, nil))
		if got == nil || got.Name != tc.want {
			t.Errorf("%s %s matched %v, want %s", tc.method, tc.path, got, tc.want)
		}
	}

	noDefault := &Rules{Rules: rs.Rules}
	if got := noDefault.Match(httptest.NewRequest("GET", "/other", nil)); got != nil {
		t.Errorf("expected no match, got %s", got.Name)
	}
}

func TestParseRulesErrors(t *testing.T) {
	cases := map[string]string{
		"empty":           `failure_policy: open`,
		"bad policy":      "failure_policy: maybe\ndefault: {limit: 1, window: 1s}",
		"missing name":    "rules: [{path_prefix: /, limit: 1, window: 1s}]",
		"duplicate name":  "rules: [{name: a, limit: 1, window: 1s}, {name: a, limit: 1, window: 1s}]",
		"bad algorithm":   "rules: [{name: a, algorithm: leaky, limit: 1, window: 1s}]",
		"zero limit":      "rules: [{name: a, limit: 0, window: 1s}]",
		"bad key":         "rules: [{name: a, limit: 1, window: 1s, key: cookie}]",
		"empty header":    "rules: [{name: a, limit: 1, window: 1s, key: 'header:'}]",
		"cost > capacity": "rules: [{name: a, limit: 2, window: 1s, cost: 3}]",
		"unknown field":   "rules: [{name: a, limit: 1, window: 1s, limt: 5}]",
		"bad duration":    "rules: [{name: a, limit: 1, window: soon}]",
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseRules([]byte(doc)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLoadRules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, []byte(sample), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRules(path); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRules(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestShippedRulesFileIsValid(t *testing.T) {
	if _, err := LoadRules("../../config/rules.yaml"); err != nil {
		t.Fatalf("config/rules.yaml: %v", err)
	}
}

func TestFromEnv(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }

	s, err := FromEnv(get)
	if err != nil {
		t.Fatal(err)
	}
	if s.ListenAddr != ":8080" || s.RedisAddr != "localhost:6379" || s.RedisTimeout != 50*time.Millisecond || s.TrustProxy {
		t.Fatalf("defaults wrong: %+v", s)
	}

	env = map[string]string{"REDIS_DB": "2", "TRUST_PROXY": "true", "REDIS_TIMEOUT": "10ms", "UPSTREAM_URL": "http://app:3000"}
	s, err = FromEnv(get)
	if err != nil {
		t.Fatal(err)
	}
	if s.RedisDB != 2 || !s.TrustProxy || s.RedisTimeout != 10*time.Millisecond || s.UpstreamURL != "http://app:3000" {
		t.Fatalf("overrides wrong: %+v", s)
	}

	for k, v := range map[string]string{"REDIS_DB": "x", "TRUST_PROXY": "perhaps", "REDIS_TIMEOUT": "fast"} {
		env = map[string]string{k: v}
		if _, err := FromEnv(get); err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("%s=%s: err = %v, want error mentioning %s", k, v, err, k)
		}
	}
}
