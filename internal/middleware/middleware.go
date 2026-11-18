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
			rule := rules.Match(r)
			if rule == nil {
				next.ServeHTTP(w, r)
				return
			}
			clientKey, ok := ClientKey(r, rule.Key, opts.TrustProxy)
			if !ok {
				writeError(w, http.StatusBadRequest, "missing_client_key",
					"request is missing the "+strings.TrimPrefix(rule.Key, "header:")+" header")
				return
			}

			ctx := r.Context()
			if opts.Timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
				defer cancel()
			}
			lr := rule.Limiter()
			start := time.Now()
			res, err := opts.Limiter.Allow(ctx, opts.Limiter.Key(rule.Name, clientKey), lr, rule.Cost)
			if opts.Metrics != nil {
				opts.Metrics.Latency.WithLabelValues(rule.Name, string(lr.Algorithm)).Observe(time.Since(start).Seconds())
			}

			outcome := ""
			if err != nil {
				if errors.Is(err, limiter.ErrCircuitOpen) {
					opts.Logger.Debug("rate limiter circuit open", "rule", rule.Name)
				} else {
					opts.Logger.Error("rate limiter backend error", "rule", rule.Name, "err", err)
				}
				if opts.Metrics != nil {
					opts.Metrics.Errors.WithLabelValues(rule.Name).Inc()
				}
				switch {
				case rules.FailurePolicy == config.FailClosed:
					record(opts.Metrics, rule, "error_rejected")
					w.Header().Set("Retry-After", "1")
					writeError(w, http.StatusServiceUnavailable, "limiter_unavailable",
						"rate limiter is unavailable, try again shortly")
					return
				case rules.FailurePolicy == config.FailLocal && opts.Fallback != nil:
					lr = rules.LocalRule(rule)
					res, err = opts.Fallback.Allow(ctx, opts.Limiter.Key(rule.Name, clientKey), lr, rule.Cost)
					outcome = "fallback_"
				}
				if err != nil {
					record(opts.Metrics, rule, "error_allowed")
					next.ServeHTTP(w, r)
					return
				}
			}

			setHeaders(w.Header(), res, lr)
			if !res.Allowed {
				record(opts.Metrics, rule, outcome+"limited")
				w.Header().Set("Retry-After", strconv.FormatInt(ceilSeconds(res.RetryAfter), 10))
				writeError(w, http.StatusTooManyRequests, "rate_limited", "too many requests")
				return
			}
			record(opts.Metrics, rule, outcome+"allowed")
			next.ServeHTTP(w, r)
		})
	}
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
