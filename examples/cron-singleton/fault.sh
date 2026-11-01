#!/usr/bin/env bash
# The fault run: a run log (also the seed server and an acceptor) and three schedulers s1..s3 (s2, s3 are acceptors;
# rendezvous hashing puts the singleton on s3 first, so the faults hit a node that is holder and acceptor at once).
# SIGSTOP the singleton's node past its lease, wake it; kill -9 the next holder and start it again; SIGSTOP the run
# log itself (the seed server and an acceptor too) and wake it; stop everything
# and audit the run log: every tick once, none skipped, no deposed owner firing after its successor claimed.
# usage: fault.sh [binary] [out dir]      exits non-zero when the audit fails
set -euo pipefail
BIN=${1:-target/release/cron-singleton}
OUT=${2:-out}
mkdir -p "$OUT"
rm -f "$OUT"/*.out
export ACCEPTORS=runlog,s2,s3 LEASE_MS=6000 JOBS=beat:1000:0:1024,digest:5000:0:1,burst:700:100:4
declare -A pid
say() { echo "$(date +%T) $*" | tee -a "$OUT/fault.log"; }
cleanup() { for p in "${pid[@]}"; do kill -CONT "$p" 2>/dev/null || true; kill "$p" 2>/dev/null || true; done; wait || true; }
trap cleanup EXIT

# the node of the last fire the log accepted
holder() { awk '$1 == "FIRE" { n = $6 } END { print n }' "$OUT/runlog.out"; }
fires() { grep -c '^FIRE' "$OUT/runlog.out" || true; }
# waits up to $2 s for a fire from a node other than $1
new_holder() {
  for _ in $(seq $(( $2 * 10 ))); do
    local h; h=$(holder)
    if [ -n "$h" ] && [ "$h" != "$1" ]; then echo "$h"; return; fi
    sleep 0.1
  done
  say "FAIL: no new holder within $2 s" >&2; return 1
}
sched() { NODE_ID=$1 SEEDS=$SEED "$BIN" sched --plaintext >> "$OUT/$1.out" 2>&1 & pid[$1]=$!; }

SEED_PORT=0 NODE_ID=runlog "$BIN" runlog --plaintext > "$OUT/runlog.out" 2>&1 & pid[runlog]=$!
for _ in $(seq 50); do grep -q '^seed ' "$OUT/runlog.out" && break; sleep 0.1; done
SEED=$(awk '$1 == "seed" { print $2; exit }' "$OUT/runlog.out")
for s in s1 s2 s3; do sched $s; done
first=$(new_holder none 30)
say "first holder $first (the acceptors warm up for one lease first)"
sleep 10

h=$(holder)
say "SIGSTOP $h for 15 s, past its 6 s lease ($(fires) fires so far)"
kill -STOP "${pid[$h]}"
t0=$(date +%s%N)
next=$(new_holder "$h" 30)
say "new holder $next after $(( ($(date +%s%N) - t0) / 1000000 )) ms"
sleep $(( 15 - ($(date +%s%N) - t0) / 1000000000 ))
kill -CONT "${pid[$h]}"
say "SIGCONT $h"
sleep 12

h=$(holder)
say "kill -9 $h ($(fires) fires so far)"
kill -9 "${pid[$h]}"
wait "${pid[$h]}" 2>/dev/null || true
t0=$(date +%s%N)
next=$(new_holder "$h" 30)
say "new holder $next after $(( ($(date +%s%N) - t0) / 1000000 )) ms"
sleep 5
say "restart $h"
sched "$h"
sleep 15

n=$(fires)
say "SIGSTOP the run log for 10 s: calls time out and are sent again ($n fires so far)"
kill -STOP "${pid[runlog]}"
sleep 10
kill -CONT "${pid[runlog]}"
for _ in $(seq 100); do [ "$(fires)" -gt "$n" ] && break; sleep 0.1; done
say "SIGCONT the run log; fires resume ($(fires))"
sleep 10

say "stopping ($(fires) fires)"
for n in s1 s2 s3 runlog; do kill -INT "${pid[$n]}"; wait "${pid[$n]}" || true; unset "pid[$n]"; done
grep -h -E 'claimed|fenced|stopped' "$OUT"/s?.out | tee -a "$OUT/fault.log"
"$BIN" audit < "$OUT/runlog.out" | tee -a "$OUT/fault.log"
