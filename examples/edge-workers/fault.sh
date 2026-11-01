#!/usr/bin/env bash
# The fault run, as processes with the real engine: the cluster of scripts/run.sh (a seed, c1, g1, g2, r1..r3) with
# workers/hello.js deployed, and in each phase two load generators at once, GETs through g1 and POSTs through g2.
#   phase kill:     SIGKILL the runner that runs the worker at 8 s, start it again at 18 s.
#   phase control:  SIGKILL the control node at 8 s, start it again at 18 s, then deploy.
#   phase object:   the counter template's Durable Object under four callers, 50 POSTs a second each (scripts/count.py);
#                   SIGKILL the runner that runs the object at 8 s, start it again at 18 s.
# Checks that every request got an answer within its deadline, that no GET failed, that the only POSTs to fail are
# 502s for what the killed runner had in hand, that the runner serves again, that requests are served while the
# control node is down, and that the deploy after it reaches the runners; of the object, that no count was told
# twice, that every count told is in what it holds at the end, that it failed only from the kill until it ran on
# another runner, and how long that was. Exits non-zero if a check fails. Stops
# what it starts. Nothing can be deployed while the control node is down: the operator's API is on it.
#
#   examples/edge-workers/scripts/fetch-engine.sh && cargo build --release -p edge-workers
#   examples/edge-workers/fault.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
export EDGE_BIN=${EDGE_BIN:-${CARGO_TARGET_DIR:-target}/release/edge-workers}
export EDGE_DIR=${EDGE_DIR:-$(mktemp -d)}
export EDGE_PORT=${EDGE_PORT:-7700}
RPS=${RPS:-200}
SECS=${SECS:-30}
R=examples/edge-workers/scripts/run.sh
EDGE=$(dirname "$EDGE_BIN")/edge
G1=http://127.0.0.1:$((EDGE_PORT + 1))
G2=http://127.0.0.1:$((EDGE_PORT + 2))
OUT=$EDGE_DIR
echo "logs and results in $OUT"
trap '$R down' EXIT

$R up >/dev/null
$R edge deploy examples/edge-workers/workers/hello.js

# header <gateway> <name>: one response header of a request for the worker, empty while there is none.
header() { curl -s -o /dev/null -D - "$1/w/hello/" | tr -d '\r' | awk -v name="$2:" 'tolower($1) == name { print $2 }'; }
# await <what> <command..>: until the command succeeds, for 30 s.
await() {
  local what=$1 started=$SECONDS; shift
  until "$@"; do
    [ $((SECONDS - started)) -lt 30 ] || { echo "not $what within 30 s"; exit 1; }
    sleep 0.2
  done
}
serves() { [ "$(header "$1" x-edge-version)" = "$2" ]; }
await "version 1 answered through g1" serves "$G1" 1
await "version 1 answered through g2" serves "$G2" 1

# loads <phase>: GETs through g1 and POSTs through g2 for $SECS, in the background.
loads() {
  "$EDGE" load --gateway "$G1" --mix hello --rps "$RPS" --duration "$SECS" --account gets --json >"$OUT/$1-get.json" &
  get=$!
  "$EDGE" load --gateway "$G2" --mix hello --rps "$RPS" --duration "$SECS" --account posts --method POST --json >"$OUT/$1-post.json" &
  post=$!
}
said() { jq -c '{sent, statuses, p50_ms: (.p50_us / 1000), p99_ms: (.p99_us / 1000), max_ms: (.max_us / 1000)}' "$1"; }
report() {
  wait "$get" "$post"
  echo "GETs  through g1: $(said "$OUT/$1-get.json")"
  echo "POSTs through g2: $(said "$OUT/$1-post.json")"
}

victim=$(header "$G1" x-edge-node)
echo "== phase kill: SIGKILL $victim, which runs the worker, at 8 s; started again at 18 s; $SECS s of $RPS GETs and $RPS POSTs a second"
loads kill
sleep 8
kill -9 "$(cat "$EDGE_DIR/$victim.pid")"; rm "$EDGE_DIR/$victim.pid"
sleep 1
echo "a second after the kill the worker runs on $(header "$G1" x-edge-node)"
sleep 9
$R start "$victim"
report kill
for _ in $(seq 60); do header "$G1" x-edge-node; done | sort | uniq -c | tr -s ' \n' ' ' >"$OUT/kill-answers.txt"
echo "after the load, 60 requests were answered by:$(cat "$OUT/kill-answers.txt")"

echo "== phase control: SIGKILL c1 at 8 s, started again at 18 s; the same load"
loads control
sleep 8
kill -9 "$(cat "$EDGE_DIR/c1.pid")"; rm "$EDGE_DIR/c1.pid"
sleep 10
$R start c1
report control
$R edge list
await "the control node taking a deploy again" $R edge deploy examples/edge-workers/workers/hello.js --var GREETING=again
started=$(date +%s%N)
await "version 2 answered through g1" serves "$G1" 2
await "version 2 answered through g2" serves "$G2" 2
echo "version 2 answered through both gateways $(( ($(date +%s%N) - started) / 1000000 )) ms after the deploy: $(curl -s "$G1/w/hello/?name=fault.sh")"
$R edge metrics hello

# The object's runner and lease, from `edge objects`: "Counter fault  on r2, epoch 17..".
object() { $R edge objects counter | awk '$2 == "fault" { gsub(",", "", $4); print $4, $6 }'; }
$R edge init counter "$OUT/counter.js" >/dev/null
$R edge deploy "$OUT/counter.js" --object COUNTER=Counter
counts() { [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'x-account: counter' "$1/w/counter/fault")" = 200 ]; }
await "the counter counting through g1" counts "$G1"
await "the counter counting through g2" counts "$G2"
read -r owner epoch <<<"$(object)"
echo "== phase object: SIGKILL $owner, which runs the object under epoch $epoch, at 8 s; started again at 18 s; $SECS s of 4 callers, 50 POSTs a second each"
examples/edge-workers/scripts/count.py "$G1" fault "$SECS" 4 50 "$OUT/object.jsonl" &
count=$!
sleep 8
date +%s.%N >"$OUT/object-killed.txt"
kill -9 "$(cat "$EDGE_DIR/$owner.pid")"; rm "$EDGE_DIR/$owner.pid"
sleep 10
echo "10 s after the kill the object is on: $(object)"
$R start "$owner"
wait "$count"
echo "after the load the object is on: $(object)"
curl -s "$G1/w/counter/fault" >"$OUT/object-final.json"
$R edge object get counter Counter fault

python3 - "$OUT" "$victim" <<'PY'
import json, sys
out, victim = sys.argv[1], sys.argv[2]
fails = []
def load(name):
    return json.load(open(f"{out}/{name}.json"))
for phase in ("kill", "control"):
    for method in ("get", "post"):
        s = load(f"{phase}-{method}")
        bad = {k: v for k, v in s["statuses"].items() if k != "200"}
        if s["skipped"] or s["answered"] != s["sent"]:
            fails.append(f"{phase} {method}: {s['sent']} sent, {s['answered']} answered, {s['skipped']} not sent")
        # The gateway's deadline is 10 s.
        if s["max_us"] > 10_250_000:
            fails.append(f"{phase} {method}: the slowest answer took {s['max_us'] / 1e6:.1f} s")
        if phase == "kill" and method == "post":
            # A runner has at most 32 runs of one worker in hand (InvokerConfig::per_worker).
            unknown = bad.pop("502", 0)
            if unknown > 32:
                fails.append(f"kill post: {unknown} answered 502, more than a runner has in hand")
        if bad:
            fails.append(f"{phase} {method}: {bad}")
if f" {victim} " not in open(f"{out}/kill-answers.txt").read() + " ":
    fails.append(f"{victim} did not serve again")

posts = [json.loads(line) for line in open(f"{out}/object.jsonl")]
killed = float(open(f"{out}/object-killed.txt").read())
final = json.load(open(f"{out}/object-final.json"))["count"]
told = [p["count"] for p in posts if p["status"] == 200]
failed = [p for p in posts if p["status"] != 200]
if len(set(told)) != len(told):
    fails.append("object: a count was told twice")
# The two POSTs that waited for it to count are in it too.
if not (told and max(told) <= final <= len(posts) + 2 and final >= len(told)):
    fails.append(f"object: {len(told)} counts told up to {max(told, default=0)}, and it holds {final} of {len(posts)} POSTs")
slowest = max(p["answered"] - p["sent"] for p in posts)
if slowest > 10.25:
    fails.append(f"object: the slowest answer took {slowest:.1f} s")
before = [p for p in posts if p["status"] == 200 and p["answered"] <= killed]
after = [p for p in posts if p["status"] == 200 and p["sent"] >= killed]
if not before or not after:
    fails.append("object: it did not count before and after the kill")
else:
    back = min(p["answered"] for p in after)
    early = [p for p in failed if p["answered"] < killed]
    late = [p for p in failed if p["sent"] > back]
    if early or late:
        fails.append(f"object: {len(early)} requests failed before the kill and {len(late)} after it counted again")
    took = sorted(p["answered"] - p["sent"] for p in before)
    statuses = {}
    for p in failed:
        statuses[p["status"]] = statuses.get(p["status"], 0) + 1
    print(f"object: {len(posts)} POSTs, {len(told)} counted, holds {final}; before the kill p50 "
          f"{took[len(took) // 2] * 1e3:.1f} ms, p99 {took[len(took) * 99 // 100] * 1e3:.1f} ms; "
          f"unavailable {back - killed:.2f} s from the kill to the next count; failed meanwhile: {statuses}")
    # The lease is 6 s: the object cannot run elsewhere before it has run out, and should soon after.
    if back - killed > 12:
        fails.append(f"object: {back - killed:.1f} s without a count")
print("FAIL\n  " + "\n  ".join(fails) if fails else "PASS")
sys.exit(1 if fails else 0)
PY
