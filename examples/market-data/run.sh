#!/usr/bin/env bash
# Starts the four nodes in the background, logs in target/market-data/, and stops them on Ctrl-C.
# Needs `docker compose up -d` first. Then: cargo run -p market-data -- load
set -euo pipefail
cd "$(dirname "$0")/../.."
curl -fsS "${CONSUL:-http://127.0.0.1:18500}/v1/status/leader" >/dev/null || { echo "no Consul: docker compose up -d"; exit 1; }
curl -fsS "${CLICKHOUSE:-http://127.0.0.1:18123}/ping" >/dev/null || { echo "no ClickHouse: docker compose up -d"; exit 1; }
cargo build -p market-data
mkdir -p target/market-data
pids=()
for role in consumer processor ws console; do
    target/debug/market-data "$role" >"target/market-data/$role.log" 2>&1 &
    pids+=($!)
done
trap 'kill "${pids[@]}" 2>/dev/null; wait' EXIT
echo "consumer, processor, ws (ws://127.0.0.1:9000) and console (http://127.0.0.1:8080) are up"
echo "logs: target/market-data/*.log    load: cargo run -p market-data -- load    stop: Ctrl-C"
wait
