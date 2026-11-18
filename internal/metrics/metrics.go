// Package metrics defines the Prometheus metrics exported by the limiter.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics groups the collectors so tests can use an isolated registry.
type Metrics struct {
	Decisions *prometheus.CounterVec
	Latency   *prometheus.HistogramVec
	Errors    *prometheus.CounterVec
}

// New creates the collectors and registers them with reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ratelimiter",
			Name:      "decisions_total",
			Help:      "Rate limit decisions by rule, algorithm and outcome (allowed, limited, error_allowed, error_rejected).",
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
	}
	reg.MustRegister(m.Decisions, m.Latency, m.Errors)
	return m
}
