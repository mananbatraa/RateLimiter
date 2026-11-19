//go:build integration

package main

// Topology tests run the real wiring against Redis Cluster and Sentinel.
// Start both with scripts/redis-topologies.sh up and export the variables it
// prints (REDIS_CLUSTER_ADDRS, REDIS_SENTINEL_ADDRS, REDIS_SENTINEL_MASTER).
// CI does this in the "topologies" job.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mananbatraa/RateLimiter/internal/config"
)

const topologyRules = `
failure_policy: closed
rules:
  - name: login
    path_prefix: /api/login
    algorithm: sliding_window_log
    limit: 3
    window: 1m
  - name: bucket
    path_prefix: /api/hello
    algorithm: token_bucket
    limit: 3
    window: 1h
  - name: counter
    path_prefix: /api/search
    algorithm: sliding_window_counter
    limit: 3
    window: 1m
default: {limit: 1000, window: 1s}
`

// serveWith builds and serves the app for env, returning its base URL.
func serveWith(t *testing.T, env map[string]string) string {
	t.Helper()
	rulesPath := filepath.Join(t.TempDir(), "rules.yaml")
	writeRules(t, rulesPath, topologyRules)
	env["RULES_PATH"] = rulesPath
	env["TRUST_PROXY"] = "true"
	env["REDIS_TIMEOUT"] = "2s"
	// A unique prefix per run keeps reruns from seeing old counters.
	env["KEY_PREFIX"] = fmt.Sprintf("topo%d", time.Now().UnixNano())
	settings, err := config.FromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	a, err := build(settings, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.serve(ctx, ln, nil) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return "http://" + ln.Addr().String()
}

// checkExactLimits sends 20 concurrent requests per client for many
// clients and every algorithm, and requires exactly 3 to get through for
// each. Clients hash to different slots, so on a cluster the scripts run on
// every master. failure_policy is closed, so a Redis error shows up as a 503
// rather than as a silently allowed request.
func checkExactLimits(t *testing.T, base string) {
	t.Helper()
	const clients, perClient, limit = 24, 20, 3
	// One route of the demo API per algorithm (see topologyRules).
	routes := []struct{ name, method, path string }{
		{"login", http.MethodPost, "/api/login"},
		{"bucket", http.MethodGet, "/api/hello"},
		{"counter", http.MethodGet, "/api/search"},
	}
	for _, route := range routes {
		var allowed [clients]atomic.Int64
		var unexpected atomic.Int64
		var wg sync.WaitGroup
		for c := range clients {
			for range perClient {
				wg.Go(func() {
					req, err := http.NewRequestWithContext(context.Background(), route.method, base+route.path, nil)
					if err != nil {
						t.Error(err)
						return
					}
					req.Header.Set("X-Forwarded-For", fmt.Sprintf("10.0.%d.%d", c/250, c%250+1))
					res, err := http.DefaultClient.Do(req)
					if err != nil {
						t.Error(err)
						return
					}
					_ = res.Body.Close()
					switch res.StatusCode {
					case http.StatusOK:
						allowed[c].Add(1)
					case http.StatusTooManyRequests:
					default:
						unexpected.Add(1)
					}
				})
			}
		}
		wg.Wait()
		if n := unexpected.Load(); n > 0 {
			t.Fatalf("%s: %d responses were neither 200 nor 429 (Redis errors under fail-closed)", route.name, n)
		}
		for c := range clients {
			if got := allowed[c].Load(); got != limit {
				t.Errorf("%s: client %d got %d requests through, want exactly %d", route.name, c, got, limit)
			}
		}
	}
}

func TestClusterTopology(t *testing.T) {
	addrs := os.Getenv("REDIS_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("REDIS_CLUSTER_ADDRS not set; run scripts/redis-topologies.sh up")
	}
	base := serveWith(t, map[string]string{"REDIS_MODE": "cluster", "REDIS_ADDR": addrs})
	checkExactLimits(t, base)

	// The keys must actually be spread over the cluster, not piled on one node.
	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addrs, ",")})
	t.Cleanup(func() { _ = cc.Close() })
	var mu sync.Mutex
	nonEmpty := 0
	err := cc.ForEachMaster(context.Background(), func(ctx context.Context, c *redis.Client) error {
		n, err := c.DBSize(ctx).Result()
		if n > 0 {
			mu.Lock()
			nonEmpty++
			mu.Unlock()
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if nonEmpty < 2 {
		t.Fatalf("limiter keys landed on %d master(s), want them spread over at least 2", nonEmpty)
	}
}

func TestSentinelTopology(t *testing.T) {
	addrs, master := os.Getenv("REDIS_SENTINEL_ADDRS"), os.Getenv("REDIS_SENTINEL_MASTER")
	if addrs == "" || master == "" {
		t.Skip("REDIS_SENTINEL_ADDRS/REDIS_SENTINEL_MASTER not set; run scripts/redis-topologies.sh up")
	}
	base := serveWith(t, map[string]string{"REDIS_MODE": "sentinel", "REDIS_ADDR": addrs, "REDIS_MASTER_NAME": master})
	checkExactLimits(t, base)

	res, err := http.Get(base + "/readyz") //nolint:noctx // test helper
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("readyz via sentinel = %d, want 200", res.StatusCode)
	}
}
