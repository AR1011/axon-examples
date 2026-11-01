#!/usr/bin/env bash
# The fault run: a seed, three backends, two gateways, and load on both gateways.
#   phase stop:      SIGSTOP backend b2 while load runs, SIGCONT it 10 s later.
#   phase partition: cut every connection into backend b3 (its relay, SIGUSR1), heal it 10 s later (SIGUSR2).
#   phase overload:  far more concurrent requests than the backends and gateways take.
# Checks that nothing hangs, that the gateway sheds with 429/503 rather than queueing, and that the
# faulted backend serves again. Exits non-zero if a check fails. Stops everything it starts.
set -euo pipefail
cd "$(dirname "$0")/../.."
BIN=${BIN:-${CARGO_TARGET_DIR:-target}/release/api-gateway}
OUT=${OUT:-$(mktemp -d)}
SECS=${SECS:-30}
DEADLINE=500
echo "logs in $OUT"
ulimit -n 65536 # 4096 client connections, and as many on the gateways
pids=()
# Gateways, then backends, then the seed, so every node can still unregister as it leaves.
cleanup() {
  local started=$SECONDS
  for ((i = ${#pids[@]} - 1; i >= 0; i--)); do
    kill -CONT "${pids[i]}" 2>/dev/null || true; kill "${pids[i]}" 2>/dev/null || true; wait "${pids[i]}" 2>/dev/null || true
  done
  echo "stopped every node in $((SECONDS - started)) s"
}
trap cleanup EXIT

# Starts a node and waits for the line it prints when ready; sets $LINE to that line.
start() {
  local name=$1 ready=$2; shift 2
  "$BIN" "$@" >"$OUT/$name.log" 2>&1 &
  pids+=($!)
  eval "pid_$name=$!"
  # The tail ends by its timeout at the latest; grep returns at the line.
  LINE=$(grep -m1 "$ready" <(timeout 40 tail -n +1 -f "$OUT/$name.log")) || { echo "$name never printed $ready"; cat "$OUT/$name.log"; exit 1; }
}

start seed '^seed ' seed --plaintext --id seed-1
SEEDS=${LINE#seed }
for b in b1 b2 b3; do
  start "$b" '^ready' backend --plaintext --id "$b" --seeds "$SEEDS" --work "${WORK:-2000}" --mailbox "${MAILBOX:-256}" --cuttable "$([ $b = b3 ] && echo true || echo false)"
done
for g in g1 g2; do
  console=(); [ $g = g1 ] && [ -n "${DASHBOARD:-}" ] && console=(--dashboard "$DASHBOARD")
  start "$g" '^listening' gateway --plaintext --id "$g" --seeds "$SEEDS" --wait-backends 3 --deadline-ms $DEADLINE --max-in-flight "${MAX_IN_FLIGHT:-1024}" "${console[@]}"
  eval "url_$g=${LINE#listening }"
done
URLS="$url_g1,$url_g2"
echo "gateways $URLS"

# fault <phase> <node> <signal on> <signal off>: load for $SECS, the fault from 8 s to 18 s.
fault() {
  local phase=$1 node=$2 on=$3 off=$4 pid
  pid=$(eval echo "\$pid_$node")
  echo "== phase $phase: $on $node at 8 s, $off at 18 s, ${SECS} s of load"
  "$BIN" load --url "$URLS" --concurrency "${CONCURRENCY:-64}" --secs "$SECS" --deadline-ms $DEADLINE --orders-every 10 >"$OUT/$phase.txt" &
  local load=$!
  sleep 8; kill "-$on" "$pid"
  sleep 1; curl -s "$url_g1/stats" >"$OUT/$phase-during.json"
  sleep 9; kill "-$off" "$pid"
  wait $load
  curl -s "$url_g1/stats" >"$OUT/$phase-after.json"
  grep -v '^t=' "$OUT/$phase.txt"
  echo "g1 a second into the fault: $(jq -c .backends "$OUT/$phase-during.json")"
  echo "g1 at the end: $(jq -c . "$OUT/$phase-after.json")"
  # Evidence it serves again: which backends answer 60 quotes after the fault.
  for i in $(seq 60); do curl -s "$url_g1/v1/quote/$i"; echo; done | jq -r .backend | sort | uniq -c | tr '\n' ' ' >"$OUT/$phase-answers.txt"
  echo "answered by: $(cat "$OUT/$phase-answers.txt")"
}
fault stop b2 STOP CONT
fault partition b3 USR1 USR2

echo "== phase overload: ${OVERLOAD:-4096} concurrent for 10 s"
"$BIN" load --url "$URLS" --concurrency "${OVERLOAD:-4096}" --secs 10 --deadline-ms $DEADLINE >"$OUT/overload.txt"
grep -v '^t=' "$OUT/overload.txt"
curl -s "$url_g1/stats" | jq -c .

python3 - "$OUT" "$DEADLINE" <<'PY'
import json, re, sys
out, deadline = sys.argv[1], int(sys.argv[2])
fails = []
def load(name):
    text = open(f"{out}/{name}.txt").read()
    rows = []
    for line in text.splitlines():
        if line.startswith("t="):
            rows.append({k: float(v) for k, v in (kv.split("=") for kv in line.split()[1:])} | {"t": int(line[2:5])})
    return json.loads(re.search(r"^summary (.*)$", text, re.M).group(1)), rows
for phase, node in (("stop", "b2"), ("partition", "b3")):
    s, rows = load(phase)
    def ok_share(ts):
        sel = [r for r in rows if r["t"] in ts]
        total = sum(r[c] for r in sel for c in ("ok", "429", "503", "504", "other", "hung", "conn_err"))
        return sum(r["ok"] for r in sel) / max(total, 1)
    during, after = ok_share(range(8, 18)), ok_share(range(22, 30))
    later = json.load(open(f"{out}/{phase}-after.json"))
    answers = open(f"{out}/{phase}-answers.txt").read()
    print(f"{phase}: success while faulted {during:.4f}, after {after:.4f}; answers after it: {answers}")
    if s["hung"] or s["conn_err"]: fails.append(f"{phase}: {s['hung']} hung, {s['conn_err']} connection errors")
    if s["max_ms"] > deadline + 250: fails.append(f"{phase}: slowest request {s['max_ms']} ms, deadline {deadline}")
    if during < 0.95: fails.append(f"{phase}: success while faulted {during:.4f} < 0.95")
    if after < 0.99: fails.append(f"{phase}: success after the fault {after:.4f} < 0.99")
    if f" {node}" not in answers: fails.append(f"{phase}: {node} did not serve again")
    if later["retries"] > 10 + 0.2 * later["requests"]: fails.append(f"{phase}: retries exceeded the budget")
o, _ = load("overload")
if o["hung"] or o["conn_err"]: fails.append(f"overload: {o['hung']} hung, {o['conn_err']} connection errors")
if o["max_ms"] > deadline + 250: fails.append(f"overload: slowest request {o['max_ms']} ms")
if o["429"] + o["503"] == 0: fails.append("overload shed nothing with 429/503")
print("FAIL\n  " + "\n  ".join(fails) if fails else "PASS")
sys.exit(1 if fails else 0)
PY
