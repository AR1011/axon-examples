#!/usr/bin/env bash
# A queue node and WORKERS workers as processes, a producer of N jobs, then the status once all have ended.
# usage: examples/task-queue/run.sh [n=10000] [sleep-ms]   (from the repository root; logs in target/task-queue/)
set -euo pipefail
n=${1:-10000}
cargo build --release -p task-queue
bin=target/release/task-queue
logs=target/task-queue
mkdir -p "$logs"
export SEED_PORT=${SEED_PORT:-7101} SEEDS=127.0.0.1:${SEED_PORT:-7101}
sum='import json, sys; print(sum(q["done"] + q["dead"] for q in json.load(sys.stdin)["Status"]["queues"].values()))'
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT
"$bin" queue --plaintext > "$logs/queue.log" 2>&1 & pids+=($!)
for i in $(seq 1 "${WORKERS:-3}"); do
  NODE_ID=worker-$i "$bin" worker --plaintext > "$logs/worker-$i.log" 2>&1 & pids+=($!)
done
until "$bin" status --plaintext > /dev/null 2>&1; do sleep 0.2; done # the seed server answers 5 s after it starts
start=$(date +%s.%N)
"$bin" produce --plaintext "$n" ${2:+"$2"}
while :; do
  ended=$("$bin" status --plaintext | python3 -c "$sum")
  [ "$ended" -ge "$n" ] && break
  sleep 0.2
done
end=$(date +%s.%N)
"$bin" status --plaintext
awk -v n="$n" -v s="$start" -v e="$end" 'BEGIN { printf "%d jobs ended in %.2f s: %.0f jobs/s\n", n, e - s, n / (e - s) }'
