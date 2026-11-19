# Progress

Build window: Nov 18–19, 2026 (scheduled runs at 10:40 AM, 4:40 PM and 10:40 PM America/Phoenix).

## Done

### Nov 18, run 1 (10:40 AM)
- Limiter core: token bucket, sliding window log and sliding window counter as atomic Lua scripts using Redis TIME.
- Strict YAML rules (path prefix, methods, ip/global/header keys, cost) and env config.
- net/http middleware: RateLimit-* headers, Retry-After, 400 for a missing key header, fail-open/closed.
- Prometheus metrics, /healthz, /readyz, demo API, reverse proxy mode, graceful shutdown.
- Dockerfile (distroless), docker-compose (Redis + 2 replicas + Prometheus), k6 scenario, GitHub Actions CI, README.

### Nov 18, run 2 (4:40 PM)
- First CI run on main was green (gofmt, vet, unit, integration, Docker build).
- `MemoryLimiter`: the three algorithms in process (64 mutex shards, lazy TTL eviction), matching the Lua arithmetic.
- `failure_policy: local` + `local_replicas`: per-replica fallback limiting during a Redis outage, limits divided across replicas.
- `Breaker`: circuit breaker around Redis (consecutive failures, cooldown, single half-open trial); `BREAKER_FAILURES`, `BREAKER_COOLDOWN`.
- Hot reload: `config.Store` with atomic swap, SHA-256 change detection, SIGHUP plus `RULES_RELOAD_INTERVAL` polling; bad files are rejected.
- Redis keys now carry an algorithm suffix (`:tb`, `:swl`, `:swc`) outside the hash tag, so a reload that changes a rule's algorithm cannot hit WRONGTYPE.
- New metrics: `ratelimiter_breaker_open`, `ratelimiter_rule_reloads_total`, `fallback_*` decision outcomes.
- Grafana 12 provisioned in docker-compose with a RateLimiter dashboard; rules dir mounted for live reload.
- Tests: property tests on both backends (log: never > limit in any window; bucket: ≤ burst + rate·d; counter: ≤ limit per fixed window), differential test memory vs Lua, breaker tests, store/reload tests, fuzz target for the rules parser (also run 15s in CI), middleware fallback/reload tests.
- Smoke-tested the binary: login 5×200 then 429 via Redis; after killing Redis, 2×200 then 429 from the local fallback (5 / 2 replicas); SIGHUP reload counted in metrics.
- README: outage policies, circuit breaker, hot reload, property tests, new benchmarks (Redis ~30 µs vs in-memory ~0.1–0.2 µs per decision).

### Nov 18, run 3 (10:40 PM)
- CI for run 2's push was green.
- `GET /admin/rules`: active rules, failure policy, SHA-256 of the rules file and load time, behind `ADMIN_TOKEN` (404 when unset, 401 on a bad token, constant-time compare, never rate limited). `config.Store` now swaps rules + hash + load time as one atomic `Snapshot`.
- `cmd/ratelimiter` split into `run(ctx, getenv, reload, logger)`, `build(settings)` and `app.serve(ctx, listener, reload)`; `main` returns an exit code so defers run. New end-to-end tests drive the real wiring on a random port against miniredis: Redis limiting, `/metrics`, Redis killed mid-test (readyz 503, breaker open, local fallback still enforces login limit), SIGHUP reload visible through `/admin/rules`, bad config errors, graceful shutdown.
- golangci-lint v2 (`.golangci.yml`: gosec, errorlint, revive, gocritic, noctx, bodyclose, ...), new CI `lint` job, `make lint`; fixed all findings (an integration test ignored errors, `os.Exit` after defer, listener without context).
- First run of the docker-compose stack in the sandbox (Docker daemon available this time). The Dockerfile's `go mod download` cannot pass the sandbox TLS proxy, so the stack ran from a locally built binary in the same distroless base; CI still builds the real Dockerfile.
- k6 results recorded in the README: shared limit 319 allowed / 320 expected across 2 replicas, p99 1.49 ms; new `loadtest/outage.js` + `outage.sh` (`make load-outage`): Redis stopped 20–40 s, allowed/s 10.9 → 10.9 → 11.2, zero 5xx, p99 4.64 ms.
- Grafana dashboard screenshot at `docs/grafana-dashboard.png`, embedded in the README.

### Nov 19, run 1 (10:40 AM)
- CI for run 3's push was green (lint, test, Docker build).
- Shadow rules: `shadow: true` on a rule. Every matching shadow rule is evaluated alongside the first matching enforcing rule (`Rules.Match` now skips shadow rules, `Rules.ShadowMatches` returns them), so a trial limit never takes a route away from the limit in force. Over-limit requests pass and are counted as `shadow_limited` / `fallback_shadow_limited` and logged; no RateLimit headers, no 400 for a missing key header, never fails closed. Shown in `/admin/rules`. Example `search-ip-trial` rule in `config/rules.yaml`.
- Middleware refactored around one `decide` function (primary limiter, then local fallback) shared by enforcing and shadow rules.
- `REDIS_MODE=standalone|sentinel|cluster`, comma-separated `REDIS_ADDR`, `REDIS_MASTER_NAME`; validated at startup (multi-address without a multi-node mode, Sentinel without master, Cluster with `REDIS_DB` != 0 all rejected). The app builds a `redis.UniversalClient` per mode.
- Topology integration tests (`cmd/ratelimiter/topology_integration_test.go`): real app against a 3-master cluster and a Sentinel, 24 clients x 20 concurrent requests per algorithm under fail-closed, exactly 3 allowed per client, no 503s, cluster keys spread over >= 2 masters. `scripts/redis-topologies.sh up|down`, `make topologies`, new CI `topologies` job (apt redis-server). Both pass locally 3/3 runs.
- README: shadow rules and Sentinel/Cluster sections, new env vars, metric outcomes, test counts.

## Test status
- 135 test cases passing (82 tests + 53 subtests) with `go test -race -tags integration ./...` against Redis 7 plus the local cluster and Sentinel, plus `FuzzParseRules` (15 s smoke clean).
- Coverage: config 98.8%, limiter 97.2%, middleware 100%, server 95.5%, cmd 76.7%.
- gofmt, go vet and golangci-lint v2.15 (with and without the integration tag): 0 issues.
- Sandbox note: `go test -cover ./...` exits 1 here only because the sandbox Go toolchain lacks `covdata` for the test-less metrics package; all tests pass, and CI runs the same command green.

## Next steps (final run is Nov 19 10:40 PM; the 4:40 PM run comes first)
1. Check CI for this push, especially the new `topologies` job (first run of apt redis-server + the script on GitHub runners).
2. Optional: Grafana panel for `shadow_limited` by rule; optional Sentinel failover test (kill the master, promote a replica, check limits keep working).
3. Final polish pass on the last run: README read-through, make sure every number in it is still current, and report ALL TESTS PASSING / TESTS FAILING.
