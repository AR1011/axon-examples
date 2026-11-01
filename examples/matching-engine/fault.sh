#!/usr/bin/env bash
# The fault scenario, as processes: a seed server, five engines (the Paxos acceptors), and a load client on four
# symbols, HOT with 32 traders. At t=10 the node owning HOT is killed with SIGKILL; at t=25 the node owning it then
# is stopped with SIGSTOP, and continued at t=37. The load client runs 50 s, then checks every book against its
# journal, and the market-data view of HOT against the book. Exit status 0 when every check held.
#
#   cargo build --release -p matching-engine && examples/matching-engine/fault.sh [out-dir]
set -euo pipefail

BIN=${BIN:-${CARGO_TARGET_DIR:-target}/release/matching-engine}
OUT=${1:-$(mktemp -d)}
mkdir -p "$OUT/journal"
rm -rf "$OUT/journal"/*
SECS=${SECS:-50}

declare -A pid_of
pids=()
cleanup() {
    for pid in "${pids[@]}"; do
        kill -CONT "$pid" 2>/dev/null || true
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

field() { python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(eval('d'+sys.argv[2]))" "$1" "$2"; }
# The load client prints one line a second; this waits for second $1 and prints that line.
at() {
    local line
    until line=$(grep -m1 "\"t\":$1," "$OUT/load.log" 2>/dev/null); do
        kill -0 "$LOAD" 2>/dev/null || { echo "load client exited early" >&2; tail -5 "$OUT/load.log" >&2; exit 1; }
        sleep 0.2
    done
    echo "$line"
}
# The node owning HOT at second $1 or the first second after it that has one.
owner_at() {
    local t=$1 line
    while true; do
        line=$(at "$t")
        if node=$(field "$line" "['hot']['node']" 2>/dev/null); then
            echo "$node"
            return
        fi
        t=$((t + 1))
    done
}

"$BIN" seed --plaintext >"$OUT/seed.log" 2>&1 &
pids+=($!)
until grep -q '"seed"' "$OUT/seed.log"; do sleep 0.1; done
SEED=$(field "$(head -1 "$OUT/seed.log")" "['seed']")

ENGINES=(e1 e2 e3 e4 e5)
ACCEPTORS=$(IFS=,; echo "${ENGINES[*]}")
for id in "${ENGINES[@]}"; do
    "$BIN" engine --plaintext id="$id" seeds="$SEED" acceptors="$ACCEPTORS" dir="$OUT/journal" >"$OUT/$id.log" 2>&1 &
    pid_of[$id]=$!
    pids+=($!)
done

"$BIN" load --plaintext id=load seeds="$SEED" symbols=HOT,AAA,BBB,CCC hot=HOT secs="$SECS" window=32 dir="$OUT/journal" \
    >"$OUT/load.log" 2>&1 &
LOAD=$!
pids+=($LOAD)

first=$(owner_at 10)
echo "t=10 SIGKILL $first (pid ${pid_of[$first]}), the owner of HOT"
kill -KILL "${pid_of[$first]}"

second=$(owner_at 25)
[ "$second" != "$first" ] || { echo "HOT is still on $first after its kill" >&2; exit 1; }
echo "t=25 SIGSTOP $second (pid ${pid_of[$second]}), the owner of HOT now"
kill -STOP "${pid_of[$second]}"
at 37 >/dev/null
echo "t=37 SIGCONT $second"
kill -CONT "${pid_of[$second]}"

status=0
wait "$LOAD" || status=$?

python3 - "$OUT/load.log" <<'EOF'
import json, sys
lines = [json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
seconds = [l for l in lines if "t" in l]
owner = None
for l in seconds:
    hot = l["hot"]
    where = f"{hot['node']} epoch {hot['epoch']}" if isinstance(hot, dict) else f"no answer: {hot[:60]}"
    if where != owner or l["busy"] or not isinstance(hot, dict):
        by = " ".join(f"{k} {v:>6}" for k, v in l["by"].items())
        print(f"t={l['t']:>2} HOT {where[:40]:<40} accepted/s {by} busy {l['busy']:>5} resent {l['resent']:>4} found {l['found']:>3} md seq {l['md'].get('seq')} snapshots {l['md'].get('snapshots')}")
    owner = where
dark = [l["t"] for l in seconds if not isinstance(l["hot"], dict)]
print("HOT without an answer at t =", dark)
final = lines[-1]
print(json.dumps(final, indent=1))
EOF
echo "logs in $OUT"
exit $status
