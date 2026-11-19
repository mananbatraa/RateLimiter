// k6 load test: hammers the default (token bucket) rule from one client IP
// across both replicas and checks that the shared limit holds.
//
//   k6 run loadtest/k6.js
//   k6 run -e TARGETS=http://localhost:8081,http://localhost:8082 loadtest/k6.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const targets = (__ENV.TARGETS || 'http://localhost:8081,http://localhost:8082').split(',');
const allowed = new Counter('limiter_allowed');
const limited = new Counter('limiter_limited');

export const options = {
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 200, // requests per second, well above the 10 req/s default rule
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    // Every response is either served or rate limited; nothing else.
    checks: ['rate==1.0'],
    // The limiter decision must stay cheap under load.
    http_req_duration: ['p(99)<50'],
  },
};

export default function () {
  const url = targets[Math.floor(Math.random() * targets.length)] + '/api/hello';
  const res = http.get(url);
  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'has RateLimit headers': (r) => r.headers['Ratelimit-Limit'] !== undefined,
  });
  if (res.status === 200) allowed.add(1);
  if (res.status === 429) limited.add(1);
}

export function handleSummary(data) {
  const ok = data.metrics.limiter_allowed ? data.metrics.limiter_allowed.values.count : 0;
  const no = data.metrics.limiter_limited ? data.metrics.limiter_limited.values.count : 0;
  // Default rule: burst 20 + 10/s * 30s = ~320 allowed in total across replicas.
  const d = data.metrics.http_req_duration.values;
  const summary =
    `allowed=${ok} limited=${no} (expected allowed ~= 20 + 10*30 = 320)\n` +
    `p95=${d['p(95)'].toFixed(2)}ms p99=${d['p(99)'].toFixed(2)}ms checks=${(data.metrics.checks.values.rate * 100).toFixed(2)}%\n`;
  return { stdout: summary };
}
