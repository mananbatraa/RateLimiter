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

## Test status
- 104 test cases passing (64 tests + 40 subtests) with `go test -race -tags integration ./...` against Redis 7, plus `FuzzParseRules` (30s local run, no failures).
- Coverage: config 98.5%, limiter 97.2%, middleware 100%, server 92.3%.
- gofmt and go vet (with and without the integration tag) clean.
- Not verified locally: Docker image build, docker-compose stack with Grafana, and the k6 run (no Docker daemon or k6 in the build sandbox). CI builds the image.

## Next steps (final run, Nov 19 10:40 PM is the last of this slot)
1. Check CI for this push (new fuzz step).
2. Add a k6 outage scenario (stop Redis mid-run) and record results for the README.
3. Consider a `cmd/` test for `run()` wiring (env -> store -> breaker -> server), e.g. by extracting a `build(settings)` function.
4. Optional: Redis Cluster / Sentinel client option (`REDIS_MODE`), and an `/admin/rules` read-only endpoint showing the active rules and their hash.
5. Final polish pass: README screenshots of the Grafana dashboard if a run can start Docker.
