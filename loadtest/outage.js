// k6 outage scenario: steady traffic while Redis is stopped and restarted.
// Run with loadtest/outage.sh, which stops Redis at ~20s and starts it again
// at ~40s. With failure_policy: local each replica keeps limiting on its own
// (limit / local_replicas), so the fleet-wide allowed rate should stay close
// to the configured 10 req/s and no request should fail with a 5xx.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const targets = (__ENV.TARGETS || 'http://localhost:8081,http://localhost:8082').split(',');
const OUTAGE_START = Number(__ENV.OUTAGE_START || 20);
const OUTAGE_END = Number(__ENV.OUTAGE_END || 40);
const DURATION = Number(__ENV.DURATION || 60);

const phases = ['before', 'outage', 'after'];
const counters = {};
for (const p of phases) {
  counters[p] = {
    allowed: new Counter(`${p}_allowed`),
    limited: new Counter(`${p}_limited`),
    failed: new Counter(`${p}_failed`),
  };
}

export const options = {
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: 100,
      timeUnit: '1s',
      duration: `${DURATION}s`,
      preAllocatedVUs: 30,
      maxVUs: 100,
    },
  },
  thresholds: {
    // The local fallback means no request should see a 5xx, outage or not.
    checks: ['rate==1.0'],
    http_req_duration: ['p(99)<100'],
  },
};

export function setup() {
  return { start: Date.now() };
}

function phase(start) {
  const t = (Date.now() - start) / 1000;
  if (t < OUTAGE_START) return 'before';
  if (t < OUTAGE_END) return 'outage';
  return 'after';
}

export default function (data) {
  const url = targets[Math.floor(Math.random() * targets.length)] + '/api/hello';
  const res = http.get(url);
  check(res, { 'status is 200 or 429': (r) => r.status === 200 || r.status === 429 });
  const c = counters[phase(data.start)];
  if (res.status === 200) c.allowed.add(1);
  else if (res.status === 429) c.limited.add(1);
  else c.failed.add(1);
}

function count(data, name) {
  const m = data.metrics[name];
  return m ? m.values.count : 0;
}

export function handleSummary(data) {
  const secs = { before: OUTAGE_START, outage: OUTAGE_END - OUTAGE_START, after: DURATION - OUTAGE_END };
  let out = 'phase    seconds  allowed  allowed/s  limited  failed\n';
  for (const p of phases) {
    const a = count(data, `${p}_allowed`);
    out += `${p.padEnd(8)} ${String(secs[p]).padStart(7)}  ${String(a).padStart(7)}  ${(a / secs[p]).toFixed(1).padStart(9)}  ${String(count(data, `${p}_limited`)).padStart(7)}  ${String(count(data, `${p}_failed`)).padStart(6)}\n`;
  }
  const d = data.metrics.http_req_duration.values;
  out += `p95=${d['p(95)'].toFixed(2)}ms p99=${d['p(99)'].toFixed(2)}ms checks=${(data.metrics.checks.values.rate * 100).toFixed(2)}%\n`;
  return { stdout: out };
}
