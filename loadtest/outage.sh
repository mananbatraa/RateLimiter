#!/usr/bin/env bash
# Runs loadtest/outage.js against the docker-compose stack and stops Redis
# for the middle 20 seconds. Needs the stack running (make up) and k6, or
# Docker to run the grafana/k6 image.
set -euo pipefail
cd "$(dirname "$0")/.."

run_k6() {
  if command -v k6 >/dev/null; then
    k6 run -q loadtest/outage.js
  else
    docker run --rm --network host -v "$PWD/loadtest:/scripts:ro" grafana/k6:1.3.0 run -q /scripts/outage.js
  fi
}

run_k6 &
k6_pid=$!
sleep 20
echo ">> stopping redis" >&2
docker compose stop redis >/dev/null
sleep 20
echo ">> starting redis" >&2
docker compose start redis >/dev/null
wait "$k6_pid"
