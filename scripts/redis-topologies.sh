#!/usr/bin/env bash
# Starts a throwaway 3-node Redis Cluster (ports 7000-7002) and a Redis
# master with one replica watched by one Sentinel (6390 + 6391 / 26390,
# master name "rlmaster") for the topology integration tests. The replica
# lets the failover test promote a new master. Needs redis-server and redis-cli on PATH.
#
#   scripts/redis-topologies.sh up     # start, then print the env to export
#   scripts/redis-topologies.sh down   # stop and clean up
set -euo pipefail

DIR="${REDIS_TOPOLOGY_DIR:-/tmp/ratelimiter-redis-topologies}"
CLUSTER_PORTS=(7000 7001 7002)

up() {
  down # start from a clean slate so `up` can be rerun
  mkdir -p "$DIR"
  for p in "${CLUSTER_PORTS[@]}"; do
    mkdir -p "$DIR/$p"
    redis-server --port "$p" --cluster-enabled yes --cluster-config-file "$DIR/$p/nodes.conf" \
      --dir "$DIR/$p" --save '' --appendonly no --daemonize yes --logfile "$DIR/$p/redis.log"
  done
  for p in "${CLUSTER_PORTS[@]}"; do
    until redis-cli -p "$p" ping >/dev/null 2>&1; do sleep 0.1; done
  done
  redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 --cluster-yes >/dev/null
  until redis-cli -p 7000 cluster info | grep -q 'cluster_state:ok'; do sleep 0.2; done

  redis-server --port 6390 --save '' --appendonly no --daemonize yes --logfile "$DIR/master.log"
  until redis-cli -p 6390 ping >/dev/null 2>&1; do sleep 0.1; done
  redis-server --port 6391 --replicaof 127.0.0.1 6390 --save '' --appendonly no \
    --daemonize yes --logfile "$DIR/replica.log"
  until redis-cli -p 6391 info replication 2>/dev/null | grep -q 'master_link_status:up'; do sleep 0.1; done
  cat >"$DIR/sentinel.conf" <<CONF
port 26390
daemonize yes
logfile $DIR/sentinel.log
sentinel monitor rlmaster 127.0.0.1 6390 1
sentinel down-after-milliseconds rlmaster 5000
CONF
  redis-server "$DIR/sentinel.conf" --sentinel
  until redis-cli -p 26390 sentinel get-master-addr-by-name rlmaster 2>/dev/null | grep -q 6390; do sleep 0.1; done
  # Sentinel learns about replicas from the master's INFO (every 10 s at
  # first); wait for it so a failover has somewhere to go.
  until redis-cli -p 26390 sentinel replicas rlmaster 2>/dev/null | grep -q 6391; do sleep 0.2; done

  echo "export REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002"
  echo "export REDIS_SENTINEL_ADDRS=127.0.0.1:26390 REDIS_SENTINEL_MASTER=rlmaster"
}

down() {
  for p in "${CLUSTER_PORTS[@]}" 6390 6391 26390; do
    redis-cli -p "$p" shutdown nosave >/dev/null 2>&1 || true
  done
  rm -rf "$DIR"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
