// Command ratelimiter runs the distributed rate limiting gateway.
package main

import (
	"context"
	"errors"
	"log/slog"
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

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

type redisPinger struct{ c *redis.Client }

func (p redisPinger) Ping(ctx context.Context) error { return p.c.Ping(ctx).Err() }

func run(logger *slog.Logger) error {
	settings, err := config.FromEnv(os.Getenv)
	if err != nil {
		return err
	}
	store, err := config.NewStore(settings.RulesPath)
	if err != nil {
		return err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     settings.RedisAddr,
		Password: settings.RedisPassword,
		DB:       settings.RedisDB,
	})
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := metrics.New(reg)

	store.OnReload = func(res config.ReloadResult, err error) {
		m.RuleReloads.WithLabelValues(string(res)).Inc()
		switch res {
		case config.Reloaded:
			cur := store.Current()
			logger.Info("rules reloaded", "rules", len(cur.Rules), "failure_policy", cur.FailurePolicy)
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

	handler, err := server.New(server.Deps{
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
		return err
	}

	srv := &http.Server{
		Addr:              settings.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// SIGHUP reloads the rules file; RULES_RELOAD_INTERVAL also polls it.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go store.Watch(ctx, settings.RulesReloadInterval, hup)

	errCh := make(chan error, 1)
	go func() {
		rules := store.Current()
		logger.Info("listening", "addr", settings.ListenAddr, "rules", len(rules.Rules),
			"failure_policy", rules.FailurePolicy, "upstream", settings.UpstreamURL,
			"rules_reload_interval", settings.RulesReloadInterval)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		logger.Info("shutting down")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
