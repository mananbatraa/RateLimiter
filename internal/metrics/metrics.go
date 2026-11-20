// Package metrics defines the Prometheus metrics exported by the limiter.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics groups the collectors so tests can use an isolated registry.
type Metrics struct {
	Decisions *prometheus.CounterVec
	Latency   *prometheus.HistogramVec
	Errors    *prometheus.CounterVec
	// BreakerOpen is 1 while the Redis circuit breaker is open.
	BreakerOpen prometheus.Gauge
	// RuleReloads counts rules file reloads by result (reloaded, unchanged, rejected).
	RuleReloads *prometheus.CounterVec
}

// New creates the collectors and registers them with reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ratelimiter",
			Name:      "decisions_total",
			Help:      "Rate limit decisions by rule, algorithm and outcome (allowed, limited, fallback_allowed, fallback_limited, error_allowed, error_rejected; shadow rules record shadow_allowed, shadow_limited, shadow_error, fallback_shadow_allowed, fallback_shadow_limited).",
		}, []string{"rule", "algorithm", "outcome"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ratelimiter",
			Name:      "decision_duration_seconds",
			Help:      "Time spent deciding, including the Redis round trip.",
			Buckets:   []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1},
		}, []string{"rule", "algorithm"}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ratelimiter",
			Name:      "backend_errors_total",
			Help:      "Limiter backend (Redis) errors by rule.",
		}, []string{"rule"}),
		BreakerOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ratelimiter",
			Name:      "breaker_open",
			Help:      "1 while the Redis circuit breaker is open and the failure policy applies without calling Redis.",
		}),
		RuleReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ratelimiter",
			Name:      "rule_reloads_total",
			Help:      "Rules file reload attempts by result (reloaded, unchanged, rejected).",
		}, []string{"result"}),
	}
	reg.MustRegister(m.Decisions, m.Latency, m.Errors, m.BreakerOpen, m.RuleReloads)
	return m
}
