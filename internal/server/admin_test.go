package server

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/mananbatraa/RateLimiter/internal/config"
	"github.com/mananbatraa/RateLimiter/internal/limiter"
)

const adminRulesYAML = `
failure_policy: local
local_replicas: 2
rules:
  - name: login
    path_prefix: /api/login
    methods: [POST]
    algorithm: sliding_window_log
    limit: 5
    window: 1m
default: {algorithm: token_bucket, limit: 10, window: 1s, burst: 20}
`

func newAdminServer(t *testing.T, token string, src config.Source) *httptest.Server {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	h, err := New(Deps{
		Limiter:  limiter.NewRedis(rdb, "t"),
		Rules:    src,
		Settings: config.Server{AdminToken: token},
		Registry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestAdminRulesAuth(t *testing.T) {
	rules, err := config.ParseRules([]byte(adminRulesYAML))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, token, auth string
		want              int
	}{
		{"disabled without token", "", "Bearer ", 404},
		{"missing header", "s3cret", "", 401},
		{"wrong scheme", "s3cret", "Basic s3cret", 401},
		{"wrong token", "s3cret", "Bearer nope", 401},
		{"prefix of token", "s3cret", "Bearer s3c", 401},
		{"valid", "s3cret", "Bearer s3cret", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newAdminServer(t, tc.token, rules)
			// Repeat well past every limit: the admin route is never limited.
			for range 30 {
				req := httptest.NewRequest("GET", srv.URL+"/admin/rules", nil)
				req.RequestURI = ""
				if tc.auth != "" {
					req.Header.Set("Authorization", tc.auth)
				}
				res, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = res.Body.Close()
				if res.StatusCode != tc.want {
					t.Fatalf("status %d, want %d", res.StatusCode, tc.want)
				}
				if tc.want == 401 && res.Header.Get("WWW-Authenticate") == "" {
					t.Fatal("401 without WWW-Authenticate")
				}
			}
		})
	}
}

func fetchRules(t *testing.T, srv *httptest.Server) rulesView {
	t.Helper()
	req := httptest.NewRequest("GET", srv.URL+"/admin/rules", nil)
	req.RequestURI = ""
	req.Header.Set("Authorization", "Bearer tok")
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != 200 || res.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("status %d, cache-control %q", res.StatusCode, res.Header.Get("Cache-Control"))
	}
	var v rulesView
	if err := json.NewDecoder(res.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestAdminRulesShowsActiveRulesAndReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, []byte(adminRulesYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	srv := newAdminServer(t, "tok", store)

	v := fetchRules(t, srv)
	if v.SHA256 != store.Snapshot().SHA256 || v.LoadedAt == nil {
		t.Fatalf("hash/loaded_at missing: %+v", v)
	}
	if v.FailurePolicy != "local" || v.LocalReplicas != 2 || len(v.Rules) != 1 || v.Default == nil {
		t.Fatalf("unexpected view: %+v", v)
	}
	login := v.Rules[0]
	if login.Name != "login" || login.Algorithm != "sliding_window_log" || login.Window != "1m0s" ||
		login.Limit != 5 || login.Key != "ip" || login.Cost != 1 || len(login.Methods) != 1 {
		t.Fatalf("login rule: %+v", login)
	}
	if v.Default.Burst != 20 || v.Default.Window != "1s" {
		t.Fatalf("default rule: %+v", v.Default)
	}

	if err := os.WriteFile(path, []byte("failure_policy: closed\ndefault: {limit: 3, window: 1s}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if res, err := store.Reload(); err != nil || res != config.Reloaded {
		t.Fatalf("reload: %v %v", res, err)
	}
	v2 := fetchRules(t, srv)
	if v2.SHA256 == v.SHA256 || v2.FailurePolicy != "closed" || len(v2.Rules) != 0 || v2.Default.Limit != 3 {
		t.Fatalf("view not updated after reload: %+v", v2)
	}
}

func TestAdminRulesFixedRulesHaveNoHash(t *testing.T) {
	rules, err := config.ParseRules([]byte(adminRulesYAML))
	if err != nil {
		t.Fatal(err)
	}
	v := fetchRules(t, newAdminServer(t, "tok", rules))
	if v.SHA256 != "" || v.LoadedAt != nil || len(v.Rules) != 1 {
		t.Fatalf("unexpected view for fixed rules: %+v", v)
	}
}
