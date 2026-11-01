#!/usr/bin/env bash
# A cluster on loopback: a seed server, three homes h1..h3 that also serve the web (h1 keeps the accounts), and a
# web node w1. Every node signs with one key, so a sign-in on one web node holds on the others. There is no TLS between
# the nodes, so each starts with --plaintext.
#   scripts/run.sh up            start it in $MDPAD_DIR (default target/mdpad), with the users load, ann, ben and
#                                the operator dev (password <name>-pw) on the first start; wait until every node is up
#   open http://127.0.0.1:$((MDPAD_PORT+1))   h1's web; h2, h3 and w1 are on the next ports
#   open http://127.0.0.1:$((MDPAD_PORT+100)) axon's console, on h1; each node's /healthz, /readyz and /metrics
#                                on PORT+20+n
#   scripts/run.sh stop|start <id>   one node (h1, h2, h3, w1): SIGTERM, or started again with what it kept
#   scripts/run.sh kill <id>     SIGKILL
#   scripts/run.sh http          the web nodes' addresses, comma-separated, for `mdpad load http=`
#   scripts/run.sh invite <name> an operator's invite link that makes the account <name>, on h1's web
#   scripts/run.sh down          stop what `up` started, by pid, and wait until it is gone; the directories stay,
#                                so `up` starts with them
set -euo pipefail
cd "$(dirname "$0")/../../.."
DIR=${MDPAD_DIR:-target/mdpad}
PORT=${MDPAD_PORT:-7300}
BIN=${MDPAD_BIN:-${CARGO_TARGET_DIR:-target}/debug/mdpad}
SEEDS=127.0.0.1:$PORT
NODES="h1:web,home,accounts h2:web,home h3:web,home w1:web"
HOMES=h1,h2,h3

# The node's offset from $PORT: its web on PORT+n, its cluster port on PORT+10+n. A home's cluster port is fixed, so
# its record from before a crash is its own and the boot check lets it start again.
offset() { case $1 in h1) echo 1 ;; h2) echo 2 ;; h3) echo 3 ;; w1) echo 4 ;; *) echo "no node $1" >&2; exit 2 ;; esac; }

# Waits for a line of a process's log, and stops waiting when the process is gone.
await() { # log pattern pid
  until grep -q "$2" "$1" 2>/dev/null; do
    kill -0 "$3" 2>/dev/null || { echo "$1: the process ended" >&2; tail -5 "$1" >&2; exit 1; }
    sleep 0.02
  done
}

start() { # name args..
  local name=$1; shift
  # The log of the start before is kept beside, so `await` reads this start's.
  [ ! -f "$DIR/$name.log" ] || mv "$DIR/$name.log" "$DIR/$name.$(date +%s%N).log"
  nohup "$BIN" "$@" >"$DIR/$name.log" 2>&1 &
  echo $! >"$DIR/$name.pid"
}

node() { # id:roles
  local id=${1%%:*} n console=
  n=$(offset "$id")
  [ "$id" != h1 ] || console=console=127.0.0.1:$((PORT + 100))
  start "$id" node --plaintext id="$id" roles="${1##*:}" homes=$HOMES seeds="$SEEDS" dir="$DIR/$id" key="$DIR/key" \
    http="127.0.0.1:$((PORT + n))" cluster="127.0.0.1:$((PORT + 10 + n))" health="127.0.0.1:$((PORT + 20 + n))" \
    $console ${MDPAD_NODE_ARGS:-}
}

up_node() { # id
  for node in $NODES; do
    [ "${node%%:*}" = "$1" ] || continue
    node "$node"
    await "$DIR/$1.log" "\"node\":\"$1\"" "$(cat "$DIR/$1.pid")"
    return 0
  done
  echo "no node $1" >&2; exit 2
}

case ${1:-} in
up)
  [ -x "$BIN" ] || cargo build -p mdpad
  mkdir -p "$DIR/h1"
  if [ ! -f "$DIR/key" ]; then
    (umask 077 && head -c 32 /dev/urandom | od -An -tx1 -v | tr -d ' \n' >"$DIR/key")
    # `load` for `mdpad load`; ann and ben for the browser suite (ui/tests-live); dev for /admin.
    for user in load ann ben dev; do "$BIN" user add name=$user password=$user-pw dir="$DIR/h1" >/dev/null; done
    "$BIN" user operator name=dev dir="$DIR/h1" >/dev/null
  fi
  start seed seed --plaintext addr="$SEEDS"
  await "$DIR/seed.log" '"seed"' "$(cat "$DIR/seed.pid")"
  for node in $NODES; do node "$node"; done
  for node in $NODES; do
    id=${node%%:*}
    await "$DIR/$id.log" "\"node\":\"$id\"" "$(cat "$DIR/$id.pid")"
    grep "\"node\":\"$id\"" "$DIR/$id.log" | tail -1
  done
  ;;
http)
  list=
  for node in $NODES; do list=$list,127.0.0.1:$((PORT + $(offset "${node%%:*}"))); done
  echo "${list#,}"
  ;;
invite)
  "$BIN" user invite name="$2" key="$DIR/key" url="http://127.0.0.1:$((PORT + 1))"
  ;;
stop|kill)
  pid=$(cat "$DIR/$2.pid")
  # Gone already: nothing to stop.
  if [ "$1" = kill ]; then kill -9 "$pid" 2>/dev/null || true; else kill "$pid" 2>/dev/null || true; fi
  rm -f "$DIR/$2.pid"
  # Gone before this returns, so a `start` right after finds its directories free.
  while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
  ;;
start)
  up_node "$2"
  ;;
down)
  pids=
  for file in "$DIR"/*.pid; do
    [ -f "$file" ] || continue
    pid=$(cat "$file")
    kill "$pid" 2>/dev/null || true
    rm -f "$file"
    pids="$pids $pid"
  done
  # Gone before this returns, so their directories can be unmounted or used again.
  for pid in $pids; do
    while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
  done
  ;;
*)
  sed -n '2,15p' "$0" >&2; exit 2
  ;;
esac
