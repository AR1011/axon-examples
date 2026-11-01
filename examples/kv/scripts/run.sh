#!/usr/bin/env bash
# A kv cluster on loopback, each node with a seed server of its own that the others list.
#   scripts/run.sh up [memory|durable] [1|3]   start it in $KV_DIR (default target/kv), wait until each node is ready and
#                                      every shard answers; RESP on $KV_PORT (default 6379) and the ports after it
#   scripts/run.sh addrs               the nodes' RESP addresses, comma-separated, for `kv check --addrs`
#   scripts/run.sh status              INFO kv of each node, as JSON
#   scripts/run.sh stop|kill|start <id>   one node (n1, n2, n3): SIGTERM, SIGKILL, or started again
#   scripts/run.sh console             `kv console` for these nodes on $KV_CONSOLE (default 127.0.0.1:7380)
#   scripts/run.sh down                stop what `up` and `console` started, by pid
#   redis-cli -c -p 6379               a client; any node takes any key
# Ports of node i (from 1): RESP $KV_PORT+i-1, cluster +100, seed +200, health +300, all on 127.0.0.1, so each node
# runs with --plaintext (kv has no TLS between nodes). The shared secret is made on the first `up` and kept in
# $KV_DIR/secret. Each node keeps its leases, WAL and snapshots under $KV_DIR/<id>; `up` on a $KV_DIR that has them
# reads them back. $KV_ARGS go to every node (`--reads lease`, `--start-timeout 10s`). $KV_AXON_CONSOLE (an address)
# starts axon's console on n1 there. $KV_CGROUP (a name; Linux and sudo) starts node i in the cgroup
# $KV_CGROUP/n<i>, for fault.sh partition.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=${KV_DIR:-target/kv}
PORT=${KV_PORT:-6379}
BIN=${KV_BIN:-${CARGO_TARGET_DIR:-target}/release/kv}

port() { echo $((PORT + $2 - 1 + $1)); } # offset node
count() { cat "$DIR/nodes"; }
ids() { local i out=""; for i in $(seq "$(count)"); do out+="${out:+,}n$i"; done; echo "$out"; }
addrs() { local i out=""; for i in $(seq "$(count)"); do out+="${out:+,}127.0.0.1:$(port 0 "$i")"; done; echo "$out"; }

node() { # i
  local i=$1 seeds="" j in=() more=()
  for j in $(seq "$(count)"); do seeds+="${seeds:+,}n$j-seed@127.0.0.1:$(port 200 "$j")"; done
  mkdir -p "$DIR/n$i"
  if [ "$i" = 1 ] && [ -n "${KV_AXON_CONSOLE:-}" ]; then more=(--console "$KV_AXON_CONSOLE"); fi
  if [ -n "${KV_CGROUP:-}" ]; then
    # In a cgroup of its own, which fault.sh partition matches with iptables. The shell execs the node: same pid.
    sudo -n mkdir -p "/sys/fs/cgroup/$KV_CGROUP/n$i"
    in=(bash -c 'echo $$ | sudo -n tee "$0/cgroup.procs" >/dev/null && exec "$@"' "/sys/fs/cgroup/$KV_CGROUP/n$i")
  fi
  KV_SECRET=$(cat "$DIR/secret") nohup ${in[@]+"${in[@]}"} \
    "$BIN" node --plaintext --id "n$i" --nodes "$(ids)" --mode "$(cat "$DIR/mode")" --resp "127.0.0.1:$(port 0 "$i")" \
    --bind "127.0.0.1:$(port 100 "$i")" --seed-bind "127.0.0.1:$(port 200 "$i")" \
    --health "127.0.0.1:$(port 300 "$i")" --seeds "$seeds" --dir "$DIR/n$i" ${more[@]+"${more[@]}"} ${KV_ARGS:-} \
    >>"$DIR/n$i.log" 2>&1 &
  echo $! >"$DIR/n$i.pid"
}

ready() { # i
  local started=$SECONDS
  until curl -sf "http://127.0.0.1:$(port 300 "$1")/readyz" >/dev/null; do
    kill -0 "$(cat "$DIR/n$1.pid")" 2>/dev/null || { echo "n$1 ended:" >&2; tail -n 5 "$DIR/n$1.log" >&2; exit 1; }
    [ $((SECONDS - started)) -lt 60 ] || { echo "n$1 not ready within 60 s" >&2; exit 1; }
    sleep 0.2
  done
}

case ${1:-} in
up)
  mode=${2:-memory} n=${3:-3}
  [ "$mode" = memory ] || [ "$mode" = durable ] || { echo "memory or durable" >&2; exit 2; }
  [ "$n" = 1 ] || [ "$n" = 3 ] || { echo "1 or 3 nodes" >&2; exit 2; }
  [ -x "$BIN" ] || cargo build --release -p kv
  mkdir -p "$DIR"
  [ -f "$DIR/secret" ] || od -An -N16 -tx1 /dev/urandom | tr -d ' \n' >"$DIR/secret"
  echo "$mode" >"$DIR/mode"
  echo "$n" >"$DIR/nodes"
  for i in $(seq "$n"); do node "$i"; done
  for i in $(seq "$n"); do ready "$i"; done
  "$BIN" warm --addrs "$(addrs)" --within 90s
  ;;
addrs) addrs ;;
status) exec "$BIN" status --addrs "$(addrs)" ;;
stop | kill)
  pid=$(cat "$DIR/$2.pid")
  signal=TERM
  [ "$1" = kill ] && signal=KILL
  kill -s "$signal" "$pid" && rm -f "$DIR/$2.pid"
  while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
  ;;
console)
  at=${KV_CONSOLE:-127.0.0.1:7380}
  # A second one could not bind, and the wait below would find the first one: `down` would then miss the first.
  if [ -f "$DIR/console.pid" ] && kill -0 "$(cat "$DIR/console.pid")" 2>/dev/null; then
    echo "a console runs already (pid $(cat "$DIR/console.pid")): http://$at/" >&2
    exit 1
  fi
  nohup "$BIN" console --addrs "$(addrs)" --bind "$at" >>"$DIR/console.log" 2>&1 &
  echo $! >"$DIR/console.pid"
  started=$SECONDS
  until curl -sf "http://$at/api/cluster" >/dev/null; do
    kill -0 "$(cat "$DIR/console.pid")" 2>/dev/null || { echo "the console ended:" >&2; tail -n 5 "$DIR/console.log" >&2; exit 1; }
    [ $((SECONDS - started)) -lt 10 ] || { echo "the console did not answer within 10 s" >&2; exit 1; }
    sleep 0.2
  done
  echo "http://$at/"
  ;;
start)
  i=${2#n}
  node "$i"
  ready "$i"
  ;;
down)
  # Waits until each has ended, so an `up` right after finds its ports free.
  pids=()
  for file in "$DIR"/*.pid; do
    [ -f "$file" ] || continue
    pids+=("$(cat "$file")")
    rm -f "$file"
  done
  [ ${#pids[@]} -gt 0 ] || exit 0
  kill "${pids[@]}" 2>/dev/null || true
  alive() { local p; for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && return 0; done; return 1; }
  for _ in $(seq 100); do
    alive || exit 0
    sleep 0.1
  done
  kill -s KILL "${pids[@]}" 2>/dev/null || true
  ;;
*)
  sed -n '2,16p' "$0" >&2
  exit 2
  ;;
esac
