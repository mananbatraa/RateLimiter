// Command ratelimiter runs the distributed rate limiting gateway.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/mananbatraa/RateLimiter/internal/config"
	"github.com/mananbatraa/RateLimiter/internal/limiter"
	"github.com/mananbatraa/RateLimiter/internal/metrics"
	"github.com/mananbatraa/RateLimiter/internal/server"
)

// shutdownTimeout bounds how long in-flight requests get to finish.
const shutdownTimeout = 10 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// SIGHUP reloads the rules file; RULES_RELOAD_INTERVAL also polls it.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	if err := run(ctx, os.Getenv, hup, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// run reads settings from getenv, builds the app, and serves until ctx is
// done. A value on reload re-reads the rules file.
func run(ctx context.Context, getenv func(string) string, reload <-chan os.Signal, logger *slog.Logger) error {
	settings, err := config.FromEnv(getenv)
	if err != nil {
		return err
	}
	a, err := build(settings, logger)
	if err != nil {
		return err
	}
	defer a.Close()

	ln, err := net.Listen("tcp", settings.ListenAddr)
	if err != nil {
		return err
	}
	return a.serve(ctx, ln, reload)
}

// app is the fully wired gateway, ready to serve.
type app struct {
	handler  http.Handler
	store    *config.Store
	settings config.Server
	logger   *slog.Logger
	closers  []func() error
}

// Close releases the Redis connection pool.
func (a *app) Close() {
	for _, c := range a.closers {
		_ = c()
	}
}

type redisPinger struct{ c *redis.Client }

func (p redisPinger) Ping(ctx context.Context) error { return p.c.Ping(ctx).Err() }

// build wires settings -> rule store -> Redis -> breaker -> HTTP handler. It
// does not touch the network beyond creating the (lazy) Redis client, so a
// missing Redis at startup is handled by the failure policy, not by a crash.
func build(settings config.Server, logger *slog.Logger) (*app, error) {
	store, err := config.NewStore(settings.RulesPath)
	if err != nil {
		return nil, err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     settings.RedisAddr,
		Password: settings.RedisPassword,
		DB:       settings.RedisDB,
	})
	a := &app{store: store, settings: settings, logger: logger, closers: []func() error{rdb.Close}}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	store.OnReload = func(res config.ReloadResult, err error) {
		m.RuleReloads.WithLabelValues(string(res)).Inc()
		switch res {
		case config.Reloaded:
			snap := store.Snapshot()
			logger.Info("rules reloaded", "rules", len(snap.Rules.Rules),
				"failure_policy", snap.Rules.FailurePolicy, "sha256", snap.SHA256)
		case config.Rejected:
			logger.Error("rules reload rejected, keeping previous rules", "err", err)
		}
	}

	breaker := limiter.NewBreaker(limiter.NewRedis(rdb, settings.KeyPrefix), limiter.BreakerOptions{
		Failures: settings.BreakerFailures,
		Cooldown: settings.BreakerCooldown,
		OnStateChange: func(open bool) {
			if open {
				m.BreakerOpen.Set(1)
				logger.Warn("redis circuit breaker opened", "cooldown", settings.BreakerCooldown)
			} else {
				m.BreakerOpen.Set(0)
				logger.Info("redis circuit breaker closed")
			}
		},
	})

	a.handler, err = server.New(server.Deps{
		Limiter:  breaker,
		Fallback: limiter.NewMemory(nil),
		Rules:    store,
		Settings: settings,
		Registry: reg,
		Metrics:  m,
		Health:   redisPinger{rdb},
		Logger:   logger,
	})
	if err != nil {
		a.Close()
		return nil, err
	}
	return a, nil
}

// serve handles requests on ln and watches the rules file until ctx is done,
// then shuts down gracefully.
func (a *app) serve(ctx context.Context, ln net.Listener, reload <-chan os.Signal) error {
	srv := &http.Server{
		Handler:           a.handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go a.store.Watch(watchCtx, a.settings.RulesReloadInterval, reload)

	errCh := make(chan error, 1)
	go func() {
		snap := a.store.Snapshot()
		a.logger.Info("listening", "addr", ln.Addr().String(), "rules", len(snap.Rules.Rules),
			"failure_policy", snap.Rules.FailurePolicy, "sha256", snap.SHA256,
			"upstream", a.settings.UpstreamURL, "rules_reload_interval", a.settings.RulesReloadInterval,
			"admin_endpoint", a.settings.AdminToken != "")
		errCh <- srv.Serve(ln)
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		a.logger.Info("shutting down")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
