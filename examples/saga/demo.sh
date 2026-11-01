#!/usr/bin/env bash
# A seed, three nodes (n1 with the console), a load of orders, and n2 killed with SIGKILL in the middle of it.
# The cluster stays up afterwards for the console until Enter; then everything it started is stopped.
#   examples/saga/demo.sh [sagas]        (builds the release binary first)
set -euo pipefail
cd "$(dirname "$0")/../.."
cargo build --release -p saga >&2
bin="${CARGO_TARGET_DIR:-target}/release/saga"
data="$(mktemp -d)"
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null; rm -rf "$data"' EXIT
"$bin" init --data "$data"

first_line() { # file: waits for the process writing it to print its first line
  for _ in $(seq 300); do [ -s "$1" ] && { head -1 "$1"; return; }; sleep 0.1; done
  echo "no output in $1" >&2; exit 1
}
"$bin" seed > "$data/seed.out" & pids+=($!)
seed="$(first_line "$data/seed.out" | cut -d' ' -f2)"
declare -A node
for id in n1 n2 n3; do
  extra=(); [ "$id" = n1 ] && extra=(--console 127.0.0.1:0)
  "$bin" node --id "$id" --seeds "$seed" --acceptors n1,n2,n3 --data "$data" "${extra[@]}" > "$data/$id.out" & pids+=($!)
  node[$id]=$!
done
for id in n1 n2 n3; do first_line "$data/$id.out"; done

"$bin" load --seeds "$seed" --sagas "${1:-1500}" & load=$!
sleep 2
kill -9 "${node[n2]}"; echo "killed n2 (pid ${node[n2]})"
wait "$load"
"$bin" check --data "$data" | grep -E 'sagas|done|compensated|unfinished|moved|violations'
read -rp "console: $(first_line "$data/n1.out" | grep -o 'http://[^ ]*') -- Enter stops the cluster"
