#!/usr/bin/env bash
# The Durable Object numbers of the README: the cluster of scripts/run.sh as processes with the real engine and the
# counter template, every request a POST through g1 (handler, object, a synced write in the store on c1).
#   warm:   one object at 200 a second, POSTs and then GETs (which write nothing).
#   rates:  one object, then 1000 objects in turn, at rising rates for 10 s each, over 64 accounts (one account has
#           1000 requests a second).
#   idle:   the runners started again with object_idle=2; the first POST after the object ended idle (3 s apart)
#           and the POST after it; then the first POST of ten objects that never ran.
# The failover time is fault.sh's. Stops what it starts.
#
#   cargo build --release -p edge-workers && examples/edge-workers/scripts/objects-bench.sh
set -euo pipefail
cd "$(dirname "$0")/../../.."
export EDGE_BIN=${EDGE_BIN:-${CARGO_TARGET_DIR:-target}/release/edge-workers}
export EDGE_DIR=${EDGE_DIR:-$(mktemp -d)}
export EDGE_PORT=${EDGE_PORT:-7700}
R=examples/edge-workers/scripts/run.sh
EDGE=$(dirname "$EDGE_BIN")/edge
G1=http://127.0.0.1:$((EDGE_PORT + 1))
trap '$R down' EXIT

post() { curl -s -o /dev/null -w '%{http_code} %{time_total}\n' -X POST -H 'x-account: bench' "$G1/w/counter/$1"; }
counting() { until [ "$(post "$1" | cut -d' ' -f1)" = 200 ]; do sleep 0.2; done; }
load() {
  "$EDGE" load --gateway "$G1" --mix counter --accounts 64 --json "$@" |
    jq -c '{sent, rps: (.rps | round), statuses, p50_ms: (.p50_us / 1000), p99_ms: (.p99_us / 1000), max_ms: (.max_us / 1000)}'
}

$R up >/dev/null
$R edge init counter "$EDGE_DIR/counter.js" >/dev/null
$R edge deploy "$EDGE_DIR/counter.js" --object COUNTER=Counter
counting warm
echo "== warm: one object, 200 a second for 20 s"
echo "POST $(load --method POST --path /warm --rps 200 --duration 20)"
echo "GET  $(load --path /warm --rps 200 --duration 20)"

echo "== rates: one object, 10 s each"
for rps in 500 1000 1500 2000 3000 4000; do echo "$rps: $(load --method POST --path /one --rps "$rps" --duration 10)"; done
echo "== rates: 1000 objects in turn; first each object's first request, at 500 a second"
echo "first: $(load --method POST --path /o --paths 1000 --rps 500 --duration 2)"
for rps in 1000 2000 3000 4000 6000 8000; do
  echo "$rps: $(load --method POST --path /o --paths 1000 --rps "$rps" --duration 10)"
done
$R edge metrics counter

# The runners again, with objects that end after 2 s idle. `stop` returns once the process is gone.
for runner in r1 r2 r3; do
  $R stop "$runner"
  EDGE_NODE_ARGS="object_idle=2" $R start "$runner"
done
counting warm
echo "== idle: the first POST after the object ended idle, and the next (status, seconds)"
for _ in $(seq 10); do
  sleep 3
  echo "after idle: $(post warm)   next: $(post warm)"
done
echo "== idle: the first POST of an object that never ran"
for i in $(seq 10); do echo "new: $(post "new-$i")"; done
