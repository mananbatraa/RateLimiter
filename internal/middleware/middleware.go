// Package middleware applies rate limiting rules to HTTP requests.
package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mananbatraa/RateLimiter/internal/config"
	"github.com/mananbatraa/RateLimiter/internal/limiter"
	"github.com/mananbatraa/RateLimiter/internal/metrics"
)

// KeyedLimiter is a Limiter that also namespaces keys per rule.
type KeyedLimiter = limiter.KeyedLimiter

// Options configures the middleware.
type Options struct {
	Limiter KeyedLimiter
	// Rules is read on every request, so a reloadable *config.Store takes
	// effect without a restart. A plain *config.Rules works too.
	Rules config.Source
	// Fallback is consulted when Limiter fails and the failure policy is
	// "local". Typically a *limiter.MemoryLimiter.
	Fallback   limiter.Limiter
	Metrics    *metrics.Metrics
	Logger     *slog.Logger
	TrustProxy bool
	// Timeout bounds each limiter call. Zero means no extra timeout.
	Timeout time.Duration
}

// RateLimit returns middleware that enforces opts.Rules on every request.
func RateLimit(opts Options) func(http.Handler) http.Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rules := opts.Rules.Current()
			for _, sr := range rules.ShadowMatches(r) {
				opts.shadow(r, rules, sr)
			}

			rule := rules.Match(r)
			if rule == nil {
				next.ServeHTTP(w, r)
				return
			}
			if rule.Shadow { // a shadow default: observe only
				opts.shadow(r, rules, rule)
				next.ServeHTTP(w, r)
				return
			}
			clientKey, ok := ClientKey(r, rule.Key, opts.TrustProxy)
			if !ok {
				writeError(w, http.StatusBadRequest, "missing_client_key",
					"request is missing the "+strings.TrimPrefix(rule.Key, "header:")+" header")
				return
			}

			d := opts.decide(r, rules, rule, clientKey)
			switch {
			case d.err != nil && rules.FailurePolicy == config.FailClosed:
				record(opts.Metrics, rule, "error_rejected")
				w.Header().Set("Retry-After", "1")
				writeError(w, http.StatusServiceUnavailable, "limiter_unavailable",
					"rate limiter is unavailable, try again shortly")
			case d.err != nil:
				record(opts.Metrics, rule, "error_allowed")
				next.ServeHTTP(w, r)
			case !d.res.Allowed:
				setHeaders(w.Header(), d.res, d.rule)
				record(opts.Metrics, rule, d.prefix+"limited")
				w.Header().Set("Retry-After", strconv.FormatInt(ceilSeconds(d.res.RetryAfter), 10))
				writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests")
			default:
				setHeaders(w.Header(), d.res, d.rule)
				record(opts.Metrics, rule, d.prefix+"allowed")
				next.ServeHTTP(w, r)
			}
		})
	}
}

// shadow evaluates a shadow rule for its metrics and log line only. It never
// writes to the response, and a request without the rule's key is skipped.
// Every outcome it records contains "shadow" (shadow_allowed, shadow_limited,
// shadow_error, with the fallback_ prefix when the local limiter decided), so
// dashboards can leave shadow decisions out of request totals: a request on a
// route with a shadow rule is one request, not two.
func (opts Options) shadow(r *http.Request, rules *config.Rules, rule *config.Rule) {
	clientKey, ok := ClientKey(r, rule.Key, opts.TrustProxy)
	if !ok {
		return
	}
	d := opts.decide(r, rules, rule, clientKey)
	switch {
	case d.err != nil:
		record(opts.Metrics, rule, "shadow_error")
	case d.res.Allowed:
		record(opts.Metrics, rule, d.prefix+"shadow_allowed")
	default:
		record(opts.Metrics, rule, d.prefix+"shadow_limited")
		opts.Logger.Info("shadow rule would limit request", "rule", rule.Name,
			"client", clientKey, "retry_after", d.res.RetryAfter)
	}
}

// decision is the outcome of asking the limiters about one rule.
type decision struct {
	res  limiter.Result
	rule limiter.Rule // the rule actually applied (scaled down on fallback)
	// prefix is "fallback_" when the local fallback limiter decided.
	prefix string
	// err is set when no limiter could decide.
	err error
}

// decide asks the primary limiter and, under the "local" failure policy,
// falls back to the in-process limiter when the primary fails.
func (opts Options) decide(r *http.Request, rules *config.Rules, rule *config.Rule, clientKey string) decision {
	ctx := r.Context()
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	key := opts.Limiter.Key(rule.Name, clientKey)
	d := decision{rule: rule.Limiter()}
	start := time.Now()
	d.res, d.err = opts.Limiter.Allow(ctx, key, d.rule, rule.Cost)
	if opts.Metrics != nil {
		opts.Metrics.Latency.WithLabelValues(rule.Name, string(d.rule.Algorithm)).Observe(time.Since(start).Seconds())
	}
	if d.err == nil {
		return d
	}

	if errors.Is(d.err, limiter.ErrCircuitOpen) {
		opts.Logger.Debug("rate limiter circuit open", "rule", rule.Name)
	} else {
		opts.Logger.Error("rate limiter backend error", "rule", rule.Name, "err", d.err)
	}
	if opts.Metrics != nil {
		opts.Metrics.Errors.WithLabelValues(rule.Name).Inc()
	}
	if rules.FailurePolicy == config.FailLocal && opts.Fallback != nil {
		d.rule = rules.LocalRule(rule)
		d.prefix = "fallback_"
		d.res, d.err = opts.Fallback.Allow(ctx, key, d.rule, rule.Cost)
	}
	return d
}

// setHeaders writes the IETF draft RateLimit-* headers.
func setHeaders(h http.Header, res limiter.Result, rule limiter.Rule) {
	h.Set("RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
	h.Set("RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
	h.Set("RateLimit-Reset", strconv.FormatInt(ceilSeconds(res.ResetAfter), 10))
	h.Set("RateLimit-Policy", strconv.FormatInt(rule.Limit, 10)+";w="+strconv.FormatInt(ceilSeconds(rule.Window), 10))
}

func record(m *metrics.Metrics, rule *config.Rule, outcome string) {
	if m != nil {
		m.Decisions.WithLabelValues(rule.Name, string(rule.Algorithm), outcome).Inc()
	}
}

func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}

// ClientKey extracts the identity a rule limits on. It returns false when the
// identity is missing (for example, a required header is absent).
func ClientKey(r *http.Request, spec string, trustProxy bool) (string, bool) {
	switch {
	case spec == "global":
		return "global", true
	case strings.HasPrefix(spec, "header:"):
		v := strings.TrimSpace(r.Header.Get(strings.TrimPrefix(spec, "header:")))
		return v, v != ""
	default:
		return clientIP(r, trustProxy), true
	}
}

func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			first, _, _ := strings.Cut(xff, ",")
			if ip := net.ParseIP(strings.TrimSpace(first)); ip != nil {
				return ip.String()
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "message": msg})
}
