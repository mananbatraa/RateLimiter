// Package config loads rate limiting rules from YAML and server settings
// from environment variables.
package config

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/mananbatraa/RateLimiter/internal/limiter"
)

// FailurePolicy decides what happens to a request when Redis is unavailable.
type FailurePolicy string

const (
	// FailOpen lets requests through when the limiter errors.
	FailOpen FailurePolicy = "open"
	// FailClosed rejects requests with 503 when the limiter errors.
	FailClosed FailurePolicy = "closed"
	// FailLocal falls back to an in-process limiter on each replica when
	// the limiter errors, so abusive clients stay limited during an outage.
	FailLocal FailurePolicy = "local"
)

// Rule is one entry in the rules file.
type Rule struct {
	Name       string            `yaml:"name"`
	PathPrefix string            `yaml:"path_prefix"`
	Methods    []string          `yaml:"methods"`
	Algorithm  limiter.Algorithm `yaml:"algorithm"`
	Limit      int64             `yaml:"limit"`
	Window     time.Duration     `yaml:"window"`
	Burst      int64             `yaml:"burst"`
	// Key selects the client identity: "ip", "header:<Name>" or "global".
	Key string `yaml:"key"`
	// Cost is how many units each request consumes. Defaults to 1.
	Cost int64 `yaml:"cost"`
}

// Limiter converts the rule to the limiter package's representation.
func (r Rule) Limiter() limiter.Rule {
	return limiter.Rule{Algorithm: r.Algorithm, Limit: r.Limit, Window: r.Window, Burst: r.Burst}
}

// Matches reports whether the rule applies to the request.
func (r Rule) Matches(req *http.Request) bool {
	if !strings.HasPrefix(req.URL.Path, r.PathPrefix) {
		return false
	}
	return len(r.Methods) == 0 || slices.Contains(r.Methods, req.Method)
}

// Rules is the parsed rules file.
type Rules struct {
	FailurePolicy FailurePolicy `yaml:"failure_policy"`
	// LocalReplicas is how many replicas share traffic. Under the "local"
	// policy each replica enforces limit/LocalReplicas (at least 1), so the
	// fleet-wide total stays close to the configured limit. Defaults to 1.
	LocalReplicas int64 `yaml:"local_replicas"`
	// Rules are matched in order; the first match wins.
	Rules []Rule `yaml:"rules"`
	// Default applies when no rule matches. Optional.
	Default *Rule `yaml:"default"`
}

// Current returns rs itself, so a fixed *Rules can be used wherever a
// reloadable source of rules is expected.
func (rs *Rules) Current() *Rules { return rs }

// LocalRule returns r scaled down for one of LocalReplicas replicas.
func (rs *Rules) LocalRule(r *Rule) limiter.Rule {
	lr := r.Limiter()
	n := max(rs.LocalReplicas, 1)
	if n == 1 {
		return lr
	}
	lr.Limit = max(lr.Limit/n, 1)
	if lr.Burst > 0 {
		lr.Burst = max(lr.Burst/n, 1)
	}
	// Keep a request's cost admissible after scaling.
	if lr.Capacity() < r.Cost {
		if lr.Burst > 0 {
			lr.Burst = r.Cost
		} else {
			lr.Limit = r.Cost
		}
	}
	return lr
}

// Match returns the first rule matching req, then the default, or nil.
func (rs *Rules) Match(req *http.Request) *Rule {
	for i := range rs.Rules {
		if rs.Rules[i].Matches(req) {
			return &rs.Rules[i]
		}
	}
	return rs.Default
}

// ParseRules decodes and validates a rules document.
func ParseRules(data []byte) (*Rules, error) {
	var rs Rules
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&rs); err != nil {
		return nil, fmt.Errorf("config: parse rules: %w", err)
	}
	if err := rs.normalize(); err != nil {
		return nil, err
	}
	return &rs, nil
}

// LoadRules reads and parses a rules file.
func LoadRules(path string) (*Rules, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return ParseRules(data)
}

func (rs *Rules) normalize() error {
	switch rs.FailurePolicy {
	case "":
		rs.FailurePolicy = FailOpen
	case FailOpen, FailClosed, FailLocal:
	default:
		return fmt.Errorf("config: failure_policy must be %q, %q or %q", FailOpen, FailClosed, FailLocal)
	}
	switch {
	case rs.LocalReplicas == 0:
		rs.LocalReplicas = 1
	case rs.LocalReplicas < 0:
		return errors.New("config: local_replicas must be >= 1")
	}
	seen := map[string]bool{}
	var errs []error
	for i := range rs.Rules {
		r := &rs.Rules[i]
		if r.Name == "" {
			errs = append(errs, fmt.Errorf("config: rule %d: name is required", i))
			continue
		}
		if seen[r.Name] {
			errs = append(errs, fmt.Errorf("config: rule %q: duplicate name", r.Name))
		}
		seen[r.Name] = true
		if err := normalizeRule(r); err != nil {
			errs = append(errs, err)
		}
	}
	if rs.Default != nil {
		if rs.Default.Name == "" {
			rs.Default.Name = "default"
		}
		if err := normalizeRule(rs.Default); err != nil {
			errs = append(errs, err)
		}
	}
	if len(rs.Rules) == 0 && rs.Default == nil {
		errs = append(errs, errors.New("config: at least one rule or a default is required"))
	}
	return errors.Join(errs...)
}

func normalizeRule(r *Rule) error {
	if r.Algorithm == "" {
		r.Algorithm = limiter.TokenBucket
	}
	if r.Key == "" {
		r.Key = "ip"
	}
	if r.Cost == 0 {
		r.Cost = 1
	}
	for i, m := range r.Methods {
		r.Methods[i] = strings.ToUpper(m)
	}
	if r.Key != "ip" && r.Key != "global" && !strings.HasPrefix(r.Key, "header:") {
		return fmt.Errorf("config: rule %q: key must be ip, global or header:<Name>", r.Name)
	}
	if strings.HasPrefix(r.Key, "header:") && strings.TrimPrefix(r.Key, "header:") == "" {
		return fmt.Errorf("config: rule %q: header key needs a header name", r.Name)
	}
	if err := r.Limiter().Validate(); err != nil {
		return fmt.Errorf("config: rule %q: %w", r.Name, err)
	}
	if r.Cost < 0 || r.Cost > r.Limiter().Capacity() {
		return fmt.Errorf("config: rule %q: cost must be between 1 and capacity", r.Name)
	}
	return nil
}

// Server holds process settings read from the environment.
type Server struct {
	ListenAddr    string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RulesPath     string
	KeyPrefix     string
	// UpstreamURL, when set, turns the server into a rate limiting reverse proxy.
	UpstreamURL string
	// TrustProxy makes the ip key use the first X-Forwarded-For address.
	TrustProxy bool
	// RedisTimeout bounds every limiter call.
	RedisTimeout time.Duration
	// RulesReloadInterval polls the rules file for changes. Zero disables
	// polling; SIGHUP always triggers a reload.
	RulesReloadInterval time.Duration
	// BreakerFailures consecutive Redis errors open the circuit breaker for
	// BreakerCooldown, during which the failure policy applies immediately.
	BreakerFailures int
	BreakerCooldown time.Duration
	// AdminToken enables GET /admin/rules for bearers of this token. Empty
	// disables the endpoint.
	AdminToken string
}

// FromEnv reads Server settings, applying defaults.
func FromEnv(getenv func(string) string) (Server, error) {
	get := func(k, def string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return def
	}
	s := Server{
		ListenAddr:    get("LISTEN_ADDR", ":8080"),
		RedisAddr:     get("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getenv("REDIS_PASSWORD"),
		RulesPath:     get("RULES_PATH", "config/rules.yaml"),
		KeyPrefix:     get("KEY_PREFIX", "rl"),
		UpstreamURL:   getenv("UPSTREAM_URL"),
		AdminToken:    getenv("ADMIN_TOKEN"),
	}
	var err error
	if _, err = fmt.Sscanf(get("REDIS_DB", "0"), "%d", &s.RedisDB); err != nil {
		return s, fmt.Errorf("config: REDIS_DB: %w", err)
	}
	if s.RedisTimeout, err = time.ParseDuration(get("REDIS_TIMEOUT", "50ms")); err != nil {
		return s, fmt.Errorf("config: REDIS_TIMEOUT: %w", err)
	}
	if s.RulesReloadInterval, err = time.ParseDuration(get("RULES_RELOAD_INTERVAL", "0s")); err != nil || s.RulesReloadInterval < 0 {
		return s, fmt.Errorf("config: RULES_RELOAD_INTERVAL: invalid duration %q", getenv("RULES_RELOAD_INTERVAL"))
	}
	if _, err = fmt.Sscanf(get("BREAKER_FAILURES", "5"), "%d", &s.BreakerFailures); err != nil || s.BreakerFailures < 1 {
		return s, fmt.Errorf("config: BREAKER_FAILURES: must be an integer >= 1")
	}
	if s.BreakerCooldown, err = time.ParseDuration(get("BREAKER_COOLDOWN", "1s")); err != nil || s.BreakerCooldown <= 0 {
		return s, fmt.Errorf("config: BREAKER_COOLDOWN: invalid duration %q", getenv("BREAKER_COOLDOWN"))
	}
	switch strings.ToLower(get("TRUST_PROXY", "false")) {
	case "1", "true", "yes":
		s.TrustProxy = true
	case "0", "false", "no":
	default:
		return s, errors.New("config: TRUST_PROXY must be true or false")
	}
	return s, nil
}
