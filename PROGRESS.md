# Progress

Build window: Nov 18–19, 2026 (scheduled runs at 10:40 AM, 4:40 PM and 10:40 PM America/Phoenix).

## Done (Nov 18, first session)
- Limiter core: token bucket, sliding window log and sliding window counter as atomic Lua scripts using Redis TIME.
- Strict YAML rules (path prefix, methods, ip/global/header keys, cost) and env config.
- net/http middleware: RateLimit-* headers, Retry-After, 400 for a missing key header, fail-open/closed.
- Prometheus metrics, /healthz, /readyz, demo API, reverse proxy mode, graceful shutdown.
- Dockerfile (distroless), docker-compose (Redis + 2 replicas + Prometheus), k6 scenario, GitHub Actions CI, README.

## Test status
- 58 tests passing (`go test -race -tags integration ./...` against Redis 7). Coverage: config 98.6%, limiter 89.3%, middleware 100%, server 91.9%.
- Not verified locally: the Docker image build and the k6 run (no Docker daemon or k6 in the build sandbox). CI builds the image.

## Next steps
1. Check the first CI run on GitHub, and fix it if Docker or the Go version in the action fails.
2. Add an in-process local fallback limiter (per replica) for when Redis is down, as a third failure policy.
3. Add hot reload of rules.yaml (SIGHUP or file watch) with tests.
4. Add a Grafana dashboard JSON + provisioning in docker-compose.
5. Add a fuzz test for the config parser and property tests for the algorithms (never allow more than limit in any window).
6. Record a k6 run in the README (allowed/limited counts, p99 latency).
