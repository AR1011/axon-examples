#!/usr/bin/env bash
# kill -9 one chat node while thousands of sessions follow rooms and posts flow, then check what must hold.
#
#   examples/chat/fault.sh [out-dir]        (after cargo build --release -p chat)
#
# Env: SESSIONS (2000), ROOMS (20), SECS (20), RATE (200), KILL_AT (8, seconds after every session joined),
#      NODES (3), KILL (1: which node to kill; 0 runs the console; none: a plain load run), SLOW (0: sessions
#      that stop reading while posts flow), RESTART (none: seconds after the kill to start the killed node again,
#      with its id, so its rooms move back to it while the load runs; give SECS room for it).
# Exits non-zero when a must-hold figure is off. The load report is in <out-dir>/load.json.
set -euo pipefail

BIN=${BIN:-${CARGO_TARGET_DIR:-target}/release/chat}
OUT=${1:-$(mktemp -d)}
SESSIONS=${SESSIONS:-2000} ROOMS=${ROOMS:-20} SECS=${SECS:-20} RATE=${RATE:-200} KILL_AT=${KILL_AT:-8}
NODES=${NODES:-3} KILL=${KILL:-1} SLOW=${SLOW:-0} RESTART=${RESTART:-none}
mkdir -p "$OUT"
ulimit -n "$(ulimit -Hn)" # a socket per session, on both ends
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

# The first line a process prints is its address; wait for it, bounded.
first_line() { timeout 20 bash -c "until [ -s '$1' ]; do sleep 0.1; done"; head -1 "$1"; }

"$BIN" seed --plaintext --id seed-1 >"$OUT/seed.out" 2>"$OUT/seed.err" & pids+=($!)
seed=$(first_line "$OUT/seed.out" | jq -r .seed)

# start_node <i> <name>: node n<i> in the background (n0 with the console), its output in $OUT/<name>.out and .err.
start_node() {
  local console=()
  [ "$1" = 0 ] && console=(--console 127.0.0.1:0)
  "$BIN" node --plaintext --id "n$1" --seeds "$seed" --workers 4 "${console[@]}" >"$OUT/$2.out" 2>"$OUT/$2.err" &
  pids+=($!)
}

http=() node_pids=()
for i in $(seq 0 $((NODES - 1))); do
  start_node "$i" "n$i"
  node_pids+=($!)
  http+=("$(first_line "$OUT/n$i.out" | jq -r .http)")
done
echo "seed $seed, nodes ${http[*]}, console $(jq -r .console "$OUT/n0.out")" >&2

nodes=$(IFS=,; echo "${http[*]}")
"$BIN" load --nodes "$nodes" --sessions "$SESSIONS" --rooms "$ROOMS" --secs "$SECS" --rate "$RATE" --slow "$SLOW" \
  >"$OUT/load.json" 2>"$OUT/load.err" & load=$!
pids+=($load)
timeout 120 bash -c "until grep -q joined '$OUT/load.err'; do sleep 0.2; done"
echo "all $SESSIONS sessions joined at $(date +%T)" >&2
if [ "$KILL" != none ]; then
  sleep "$KILL_AT"
  kill -9 "${node_pids[$KILL]}"
  echo "killed n$KILL (pid ${node_pids[$KILL]}) at $(date +%T)" >&2
  if [ "$RESTART" != none ]; then
    sleep "$RESTART"
    start_node "$KILL" "n$KILL-again"
    again=$(first_line "$OUT/n$KILL-again.out" | jq -r .http)
    [ -n "$again" ] || { echo "FAIL: n$KILL did not start again" >&2; exit 1; }
    echo "n$KILL again at $again, $(date +%T)" >&2
  fi
fi
wait "$load"

jq . "$OUT/load.json"
jq -e --arg kill "$KILL" '.duplicates == 0 and .gaps == 0 and .lost_in_steady_rooms == 0
  and .lost_in_last_generation == 0 and .slow_missing == 0 and (.reconnects > 0 or $kill == "none")' "$OUT/load.json" >/dev/null \
  || { echo "FAIL: a must-hold figure is off" >&2; exit 1; }
echo "PASS: no duplicates, no silent gaps, nothing lost in rooms that did not restart or in the restarted rooms' new generation; $(jq .lost_in_dead_generations "$OUT/load.json") acknowledged posts of the dead host's generations missed by some session; $(jq .reconnects "$OUT/load.json") reconnects, restarted rooms $(jq -c .restarted_rooms "$OUT/load.json")" >&2
