// Package server wires the limiter, middleware, metrics and routes together.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mananbatraa/RateLimiter/internal/config"
	"github.com/mananbatraa/RateLimiter/internal/metrics"
	"github.com/mananbatraa/RateLimiter/internal/middleware"
)

// Pinger reports backend health.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Deps are the collaborators the HTTP handler needs.
type Deps struct {
	Limiter  middleware.KeyedLimiter
	Rules    *config.Rules
	Settings config.Server
	Registry *prometheus.Registry
	Health   Pinger
	Logger   *slog.Logger
}

// New builds the root handler. /healthz, /readyz and /metrics are never rate
// limited; everything else goes through the middleware and then either the
// upstream proxy or the built-in demo API.
func New(d Deps) (http.Handler, error) {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	m := metrics.New(d.Registry)

	var app http.Handler = demoAPI()
	if d.Settings.UpstreamURL != "" {
		u, err := url.Parse(d.Settings.UpstreamURL)
		if err != nil {
			return nil, err
		}
		app = httputil.NewSingleHostReverseProxy(u)
	}

	limited := middleware.RateLimit(middleware.Options{
		Limiter:    d.Limiter,
		Rules:      d.Rules,
		Metrics:    m,
		Logger:     d.Logger,
		TrustProxy: d.Settings.TrustProxy,
		Timeout:    d.Settings.RedisTimeout,
	})(app)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if d.Health != nil {
			if err := d.Health.Ping(ctx); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "redis unavailable"})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}))
	mux.Handle("/", limited)
	return mux, nil
}

// demoAPI is a stand-in upstream so the project runs on its own.
func demoAPI() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/hello", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"message": "hello"})
	})
	mux.HandleFunc("POST /api/login", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"token": "demo"})
	})
	mux.HandleFunc("GET /api/search", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"query": r.URL.Query().Get("q"), "results": []string{}})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
