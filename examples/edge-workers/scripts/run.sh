#!/usr/bin/env bash
# A cluster on loopback: a seed server, one control node, two gateways, three runners. There is no TLS between them,
# so each process starts with --plaintext.
#   scripts/run.sh up            start it in $EDGE_DIR (default target/edge-workers), wait until every node is up
#   scripts/run.sh status|stats [name=]   what is deployed, and every runner's counters, asked as a cluster member
#   scripts/run.sh edge <command>..   the `edge` command line against c1's admin API on $((EDGE_PORT+3)), signed in on
#                                first use with EDGE_PASSWORD (default edge-dev, the password `up` gives c1)
#   curl localhost:$((EDGE_PORT+1))/w/<name>/   a request through gateway g1 (g2 is on the next port)
#   scripts/run.sh serving       what each gateway and runner serves, from its log
#   scripts/run.sh stop|start <id>   one node (c1, g1, g2, r1, r2, r3): SIGTERM, or started again with what it kept
#   scripts/run.sh down          stop what `up` started, by pid; the directories stay, so `up` starts with them
# Needs the engine (scripts/fetch-engine.sh).
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=${EDGE_DIR:-target/edge-workers}
PORT=${EDGE_PORT:-7700}
BIN=${EDGE_BIN:-${CARGO_TARGET_DIR:-target}/debug/edge-workers}
EDGE=$(dirname "$BIN")/edge
export EDGE_PASSWORD=${EDGE_PASSWORD:-edge-dev}
SEEDS=127.0.0.1:$PORT
NODES="c1:control g1:gateway g2:gateway r1:runner r2:runner r3:runner"

# Waits for a line of a process's log, and stops waiting when the process is gone.
await() { # log pattern pid
  until grep -q "$2" "$1" 2>/dev/null; do
    kill -0 "$3" 2>/dev/null || { echo "$1: the process ended" >&2; tail -5 "$1" >&2; exit 1; }
    sleep 0.2
  done
}

start() { # name args..
  local name=$1; shift
  nohup "$BIN" "$@" >"$DIR/$name.log" 2>&1 &
  echo $! >"$DIR/$name.pid"
}

node() { # id:roles
  local id=${1%%:*}
  local http=()
  case $id in
  g1) http=(http="127.0.0.1:$((PORT + 1))") ;;
  g2) http=(http="127.0.0.1:$((PORT + 2))") ;;
  c1)
    http=(admin="127.0.0.1:$((PORT + 3))")
    EDGE_ADMIN_HASH=$(cat "$DIR/admin.hash")
    export EDGE_ADMIN_HASH
    ;;
  esac
  # The objects' leases: the control node and the gateways grant them, the same list on every node.
  start "$id" node --plaintext id="$id" roles="${1##*:}" seeds="$SEEDS" dir="$DIR/$id" acceptors=c1,g1,g2 ${http[@]+"${http[@]}"} ${EDGE_NODE_ARGS:-}
}

case ${1:-} in
up)
  [ -x "$BIN" ] && [ -x "$EDGE" ] || cargo build -p edge-workers
  mkdir -p "$DIR"
  [ -f "$DIR/admin.hash" ] || "$EDGE" hash-password >"$DIR/admin.hash"
  start seed seed --plaintext bind="$SEEDS"
  await "$DIR/seed.log" '"seed"' "$(cat "$DIR/seed.pid")"
  for node in $NODES; do node "$node"; done
  for node in $NODES; do
    id=${node%%:*}
    await "$DIR/$id.log" '"node"' "$(cat "$DIR/$id.pid")"
    grep '"node"' "$DIR/$id.log" | head -1
  done
  ;;
status|stats)
  exec "$BIN" "$@" --plaintext id="cli-$$" seeds="$SEEDS"
  ;;
edge)
  shift
  export EDGE_CONFIG="$DIR/credentials.json" EDGE_URL="http://127.0.0.1:$((PORT + 3))"
  [ -f "$EDGE_CONFIG" ] || "$EDGE" login >/dev/null
  exec "$EDGE" "$@"
  ;;
stop)
  pid=$(cat "$DIR/$2.pid")
  kill "$pid" && rm -f "$DIR/$2.pid"
  # Gone before this returns, so a `start` right after finds its directories free.
  while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
  ;;
start)
  for node in $NODES; do
    [ "${node%%:*}" = "$2" ] || continue
    node "$node"
    await "$DIR/$2.log" '"node"' "$(cat "$DIR/$2.pid")"
    exit 0
  done
  echo "no node $2" >&2; exit 2
  ;;
serving)
  for id in g1 g2 r1 r2 r3; do grep '"serving"' "$DIR/$id.log" | tail -1; done
  ;;
down)
  for pid in "$DIR"/*.pid; do
    [ -f "$pid" ] || continue
    kill "$(cat "$pid")" 2>/dev/null || true
    rm -f "$pid"
  done
  ;;
*)
  sed -n '2,12p' "$0" >&2; exit 2
  ;;
esac
