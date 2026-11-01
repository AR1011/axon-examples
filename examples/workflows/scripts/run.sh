#!/usr/bin/env bash
# A cluster on loopback: history nodes h1..h3 (each a seed server, a lease acceptor and an api node), engines e1 and
# e2 (api nodes too), and workers w1 and w2 for the samples' queues. There is no TLS between them, so each node starts
# with --plaintext.
#   scripts/run.sh up              start it in $WF_DIR (default target/workflows), wait until /ready answers 200
#   scripts/run.sh wf <command>..  `wf` against e1's API on $((WF_PORT+4)), signed in on first use with WF_PASSWORD
#                                  (default wf-dev, the password `up` gives the api nodes); WF_API=h1 for h1's
#   scripts/run.sh stop|start <id> one node: SIGTERM (its workers drain), or started again with what it kept
#   scripts/run.sh kill <id>       SIGKILL
#   scripts/run.sh roll            SIGTERM w1, w2, e1 and e2 in turn, each started again once it is gone
#   scripts/run.sh down            stop what `up` started, by pid; the directories stay, so `up` starts with them
# Each node logs to $WF_DIR/<id>.log and, as each activity attempt starts, to $WF_DIR/witness/<id>.jsonl.
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=${WF_DIR:-target/workflows}
PORT=${WF_PORT:-7700}
BIN=${WF_BIN:-${CARGO_TARGET_DIR:-target}/debug/workflows}
WF=$(dirname "$BIN")/wf
export WF_PASSWORD=${WF_PASSWORD:-wf-dev}
SEEDS=127.0.0.1:$((PORT + 11)),127.0.0.1:$((PORT + 12)),127.0.0.1:$((PORT + 13))
NODES="h1 h2 h3 e1 e2 w1 w2"

# The node's API port, if it has one: h1..h3 on PORT+1..3, e1 and e2 on PORT+4 and 5.
http_port() {
  case $1 in
  h[1-3]) echo $((PORT + ${1#h})) ;;
  e[1-2]) echo $((PORT + 3 + ${1#e})) ;;
  esac
}

# Waits for a line of a process's log, and stops waiting when the process is gone.
await() { # log pattern pid
  until grep -q "$2" "$1" 2>/dev/null; do
    kill -0 "$3" 2>/dev/null || { echo "$1: the process ended" >&2; tail -5 "$1" >&2; exit 1; }
    sleep 0.2
  done
}

node() { # id
  local id=$1 args=()
  case $id in
  h*) args=(history=yes api=yes seed="127.0.0.1:$((PORT + 10 + ${id#h}))") ;;
  e*) args=(engine=yes api=yes) ;;
  w*) args=(worker=yes) ;;
  esac
  local port
  port=$(http_port "$id")
  [ -z "$port" ] || args+=(http="127.0.0.1:$port")
  WF_ADMIN_HASH=$(cat "$DIR/admin.hash")
  export WF_ADMIN_HASH
  mkdir -p "$DIR/$id" "$DIR/witness"
  nohup "$BIN" node --plaintext id="$id" dir="$DIR/$id" seeds="$SEEDS" history_nodes=h1,h2,h3 key="$DIR/api.key" \
    witness="$DIR/witness/$id.jsonl" "${args[@]}" ${WF_NODE_ARGS:-} >>"$DIR/$id.log" 2>&1 &
  echo $! >"$DIR/$id.pid"
}

up_one() { # id
  local lines
  lines=$(grep -c '"node"' "$DIR/$1.log" 2>/dev/null || true)
  node "$1"
  until [ "$(grep -c '"node"' "$DIR/$1.log" 2>/dev/null || true)" -gt "${lines:-0}" ]; do
    kill -0 "$(cat "$DIR/$1.pid")" 2>/dev/null || { echo "$1: the process ended" >&2; tail -5 "$DIR/$1.log" >&2; exit 1; }
    sleep 0.2
  done
}

stop_one() { # id signal
  [ -f "$DIR/$1.pid" ] || return 0
  local pid
  pid=$(cat "$DIR/$1.pid")
  kill "-$2" "$pid" 2>/dev/null || true
  rm -f "$DIR/$1.pid"
  # Gone before this returns, so a `start` right after finds its directories free.
  while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
}

ready() { # every api node answers /ready with 200
  for id in h1 h2 h3 e1 e2; do
    curl -sf -o /dev/null "http://127.0.0.1:$(http_port "$id")/ready" || return 1
  done
}

case ${1:-} in
up)
  [ -x "$BIN" ] && [ -x "$WF" ] || cargo build -p workflows
  for port in $(seq $((PORT + 1)) $((PORT + 5))) $((PORT + 11)) $((PORT + 12)) $((PORT + 13)); do
    ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null || { echo "port $port is in use: set WF_PORT" >&2; exit 1; }
  done
  mkdir -p "$DIR"
  [ -f "$DIR/admin.hash" ] || "$WF" hash-password >"$DIR/admin.hash"
  for id in $NODES; do node "$id"; done
  for id in $NODES; do
    await "$DIR/$id.log" '"node"' "$(cat "$DIR/$id.pid")"
  done
  started=$SECONDS
  until ready; do
    [ $((SECONDS - started)) -lt 60 ] || { echo "the api nodes not ready within 60 s" >&2; exit 1; }
    sleep 0.2
  done
  echo "up: e1 at http://127.0.0.1:$(http_port e1), password $WF_PASSWORD"
  ;;
wf)
  shift
  api=${WF_API:-e1}
  export WF_CONFIG="$DIR/credentials-$api.json" WF_URL="http://127.0.0.1:$(http_port "$api")"
  [ -f "$WF_CONFIG" ] || "$WF" login >/dev/null
  exec "$WF" "$@"
  ;;
stop) stop_one "$2" TERM ;;
kill) stop_one "$2" KILL ;;
start) up_one "$2" ;;
roll)
  for id in w1 w2 e1 e2; do
    stop_one "$id" TERM
    up_one "$id"
    echo "rolled $id"
  done
  ;;
down)
  for id in $NODES; do stop_one "$id" TERM & done
  wait
  ;;
*)
  sed -n '2,11p' "$0" >&2; exit 2
  ;;
esac
