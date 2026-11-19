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

func TestShadowRulesDoNotTakeOverMatching(t *testing.T) {
	rs, err := ParseRules([]byte(`
rules:
  - {name: trial-a, path_prefix: /api, shadow: true, limit: 1, window: 1s}
  - {name: api, path_prefix: /api, limit: 10, window: 1s}
  - {name: trial-b, path_prefix: /api/items, shadow: true, limit: 2, window: 1s}
default: {limit: 5, window: 1s}
`))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/items", nil)
	if got := rs.Match(req); got == nil || got.Name != "api" {
		t.Fatalf("Match = %v, want the enforcing rule api", got)
	}
	shadows := rs.ShadowMatches(req)
	if len(shadows) != 2 || shadows[0].Name != "trial-a" || shadows[1].Name != "trial-b" {
		t.Fatalf("ShadowMatches = %v, want trial-a, trial-b", shadows)
	}
	other := httptest.NewRequest("GET", "/other", nil)
	if got := rs.Match(other); got.Name != "default" || rs.ShadowMatches(other) != nil {
		t.Fatalf("/other: Match = %s, shadows = %v", got.Name, rs.ShadowMatches(other))
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
	if s.ListenAddr != ":8080" || s.RedisAddr != "localhost:6379" || s.RedisTimeout != 50*time.Millisecond || s.TrustProxy ||
		s.RulesReloadInterval != 0 || s.BreakerFailures != 5 || s.BreakerCooldown != time.Second || s.AdminToken != "" {
		t.Fatalf("defaults wrong: %+v", s)
	}

	env = map[string]string{"REDIS_DB": "2", "TRUST_PROXY": "true", "REDIS_TIMEOUT": "10ms", "UPSTREAM_URL": "http://app:3000", "ADMIN_TOKEN": "s3cret"}
	s, err = FromEnv(get)
	if err != nil {
		t.Fatal(err)
	}
	if s.RedisDB != 2 || !s.TrustProxy || s.RedisTimeout != 10*time.Millisecond || s.UpstreamURL != "http://app:3000" || s.AdminToken != "s3cret" {
		t.Fatalf("overrides wrong: %+v", s)
	}

	env = map[string]string{"RULES_RELOAD_INTERVAL": "15s", "BREAKER_FAILURES": "3", "BREAKER_COOLDOWN": "250ms"}
	s, err = FromEnv(get)
	if err != nil {
		t.Fatal(err)
	}
	if s.RulesReloadInterval != 15*time.Second || s.BreakerFailures != 3 || s.BreakerCooldown != 250*time.Millisecond {
		t.Fatalf("reload/breaker overrides wrong: %+v", s)
	}

	for k, v := range map[string]string{
		"REDIS_DB": "x", "TRUST_PROXY": "perhaps", "REDIS_TIMEOUT": "fast",
		"RULES_RELOAD_INTERVAL": "-1s", "BREAKER_FAILURES": "0", "BREAKER_COOLDOWN": "soon",
	} {
		env = map[string]string{k: v}
		if _, err := FromEnv(get); err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("%s=%s: err = %v, want error mentioning %s", k, v, err, k)
		}
	}
}

func TestLocalPolicyAndReplicas(t *testing.T) {
	rs, err := ParseRules([]byte(`
failure_policy: local
local_replicas: 4
rules:
  - name: tb
    path_prefix: /tb
    limit: 100
    window: 1s
    burst: 40
  - name: log
    path_prefix: /log
    algorithm: sliding_window_log
    limit: 10
    window: 1m
    cost: 5
  - name: tiny
    path_prefix: /tiny
    limit: 2
    window: 1s
`))
	if err != nil {
		t.Fatal(err)
	}
	if rs.FailurePolicy != FailLocal || rs.LocalReplicas != 4 {
		t.Fatalf("policy=%q replicas=%d", rs.FailurePolicy, rs.LocalReplicas)
	}
	cases := []struct {
		rule         int
		limit, burst int64
	}{
		{0, 25, 10}, // 100/4, 40/4
		{1, 5, 0},   // 10/4=2 is below the cost of 5, so it is raised to 5
		{2, 1, 0},   // never scaled to zero
	}
	for _, c := range cases {
		lr := rs.LocalRule(&rs.Rules[c.rule])
		if lr.Limit != c.limit || lr.Burst != c.burst {
			t.Errorf("%s: local limit=%d burst=%d, want %d/%d", rs.Rules[c.rule].Name, lr.Limit, lr.Burst, c.limit, c.burst)
		}
		if err := lr.Validate(); err != nil {
			t.Errorf("%s: scaled rule invalid: %v", rs.Rules[c.rule].Name, err)
		}
	}

	one, err := ParseRules([]byte("default: {limit: 7, window: 1s}"))
	if err != nil {
		t.Fatal(err)
	}
	if one.LocalReplicas != 1 || one.LocalRule(one.Default).Limit != 7 {
		t.Fatalf("default replicas should be 1 and leave limits alone: %+v", one)
	}
	if _, err := ParseRules([]byte("local_replicas: -2\ndefault: {limit: 1, window: 1s}")); err == nil {
		t.Fatal("negative local_replicas should be rejected")
	}
	if one.Current() != one {
		t.Fatal("Rules.Current should return itself")
	}
}

// FuzzParseRules checks that the parser never panics and that anything it
// accepts is fully usable: every rule is valid, has its defaults applied and
// can be evaluated by the limiter.
func FuzzParseRules(f *testing.F) {
	f.Add([]byte(sample))
	f.Add([]byte("default: {limit: 1, window: 1s}"))
	f.Add([]byte("failure_policy: local\nlocal_replicas: 3\nrules: [{name: a, limit: 5, window: 1m, cost: 2, key: global}]"))
	f.Add([]byte("rules: [{name: a, algorithm: sliding_window_counter, limit: 1, window: 1ms, key: 'header:X'}]"))
	f.Add([]byte("rules: [{name: a}, {name: a}]"))
	f.Add([]byte("{"))
	f.Fuzz(func(t *testing.T, data []byte) {
		rs, err := ParseRules(data)
		if err != nil {
			if rs != nil {
				t.Fatal("non-nil rules returned with an error")
			}
			return
		}
		switch rs.FailurePolicy {
		case FailOpen, FailClosed, FailLocal:
		default:
			t.Fatalf("accepted failure policy %q", rs.FailurePolicy)
		}
		if rs.LocalReplicas < 1 {
			t.Fatalf("accepted local_replicas %d", rs.LocalReplicas)
		}
		all := append([]Rule(nil), rs.Rules...)
		if rs.Default != nil {
			all = append(all, *rs.Default)
		}
		if len(all) == 0 {
			t.Fatal("accepted a rules file with no rules")
		}
		for i := range all {
			r := &all[i]
			if r.Name == "" || r.Key == "" || r.Cost < 1 {
				t.Fatalf("defaults not applied: %+v", r)
			}
			if err := r.Limiter().Validate(); err != nil {
				t.Fatalf("accepted invalid rule %+v: %v", r, err)
			}
			if r.Cost > r.Limiter().Capacity() {
				t.Fatalf("accepted cost %d above capacity %d", r.Cost, r.Limiter().Capacity())
			}
			if lr := rs.LocalRule(r); lr.Validate() != nil || r.Cost > lr.Capacity() {
				t.Fatalf("local rule unusable: %+v (cost %d)", lr, r.Cost)
			}
		}
	})
}

func TestFromEnvRedisModes(t *testing.T) {
	ok := []struct {
		env   map[string]string
		mode  RedisMode
		addrs int
	}{
		{map[string]string{}, RedisStandalone, 1},
		{map[string]string{"REDIS_MODE": "Cluster", "REDIS_ADDR": "a:7000, b:7001,c:7002,"}, RedisCluster, 3},
		{map[string]string{"REDIS_MODE": "sentinel", "REDIS_ADDR": "s1:26379,s2:26379", "REDIS_MASTER_NAME": "mymaster", "REDIS_DB": "1"}, RedisSentinel, 2},
	}
	for _, tc := range ok {
		s, err := FromEnv(func(k string) string { return tc.env[k] })
		if err != nil {
			t.Fatalf("%v: %v", tc.env, err)
		}
		if s.RedisMode != tc.mode || len(s.RedisAddrs()) != tc.addrs {
			t.Fatalf("%v: mode %q addrs %v", tc.env, s.RedisMode, s.RedisAddrs())
		}
	}

	bad := map[string]map[string]string{
		"unknown mode":             {"REDIS_MODE": "ring"},
		"empty addr list":          {"REDIS_ADDR": " , "},
		"standalone with many":     {"REDIS_ADDR": "a:1,b:2"},
		"standalone with master":   {"REDIS_MASTER_NAME": "m"},
		"sentinel without master":  {"REDIS_MODE": "sentinel", "REDIS_ADDR": "s:26379"},
		"cluster with db":          {"REDIS_MODE": "cluster", "REDIS_DB": "3"},
		"cluster with master name": {"REDIS_MODE": "cluster", "REDIS_MASTER_NAME": "m"},
	}
	for name, env := range bad {
		if _, err := FromEnv(func(k string) string { return env[k] }); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
