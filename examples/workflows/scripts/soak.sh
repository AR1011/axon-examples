#!/usr/bin/env bash
# The soak: the cluster of scripts/run.sh in a release build for HOURS, through h1's API.
#   First SLEEPERS runs of `sleeper` that sleep 30 days, started at SLEEP_RATE a second: they stay in the index and
#   the waker's timers, and hold no lease. With them in, the console's list (GET /api/runs) is timed on its first
#   page and its 50th, to $WF_DIR/console.txt.
#   Then windows of WINDOW s each: `nine_steps` at RATE a second, and at a seeded random time in the window one of
#   e1 e2 w1 w2 h2 h3 killed with SIGKILL and started again 10 s later. Each window waits until its runs ended.
#   Every minute, each node's RSS and the bytes of its directory go to $WF_DIR/soak.csv.
# Finished runs are kept RETENTION (default 20m), so the journals' bytes must stop growing once the first windows
# are past it. At the end every node stops and `wf check` reads the history nodes with the witness logs and the acks
# of the windows that started within the retention (older runs are removed, as they should be). Prints PASS, or FAIL
# and exits 1, when: a window's runs did not all complete, a start was refused, the checker finds a violation, a
# sleeper is not running, or a node's RSS grew 5 % or more from a third of the soak (or two retentions and a window
# in, if later) to the end.
#
#   cargo build --release -p workflows && examples/workflows/scripts/soak.sh
#   HOURS=8 SLEEPERS=1000000 RATE=500 (the defaults); SEED=n replays the choice of kills.
set -euo pipefail
cd "$(dirname "$0")/../../.."
export WF_BIN=${WF_BIN:-${CARGO_TARGET_DIR:-target}/release/workflows}
export WF_DIR=${WF_DIR:-$(mktemp -d)}
export WF_PORT=${WF_PORT:-7700}
export WF_API=h1
HOURS=${HOURS:-8}
SLEEPERS=${SLEEPERS:-1000000}
SLEEP_RATE=${SLEEP_RATE:-1000}
RATE=${RATE:-500}
WINDOW=${WINDOW:-600}
RETENTION=${RETENTION:-20m}
SEED=${SEED:-$$}
export WF_PASSWORD=${WF_PASSWORD:-wf-dev}
export WF_NODE_ARGS="retention=$RETENTION failed_retention=$RETENTION start_rate=100000 ${WF_NODE_ARGS:-}"
R=examples/workflows/scripts/run.sh
WF=$(dirname "$WF_BIN")/wf
OUT=$WF_DIR
RANDOM=$SEED
echo "soak: $HOURS h, $SLEEPERS sleepers, nine_steps at $RATE/s, a kill every $WINDOW s, seed $SEED; in $OUT"

sampler=
cleanup() {
  [ -z "$sampler" ] || kill "$sampler" 2>/dev/null || true
  $R down
}
trap cleanup EXIT

# One line a minute per node: seconds since the start, node, RSS in KiB, bytes of its directory.
sample() {
  local begun=$SECONDS
  echo "t_s,node,rss_kib,dir_bytes" >"$OUT/soak.csv"
  while true; do
    for id in h1 h2 h3 e1 e2 w1 w2; do
      local pid rss
      pid=$(cat "$OUT/$id.pid" 2>/dev/null || true)
      rss=$( [ -n "$pid" ] && ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || true)
      echo "$((SECONDS - begun)),$id,${rss:-0},$(du -sb "$OUT/$id" 2>/dev/null | cut -f1)" >>"$OUT/soak.csv"
    done
    sleep 60
  done
}

$R up
$R wf cluster >/dev/null
sample &
sampler=$!

echo "== $SLEEPERS sleepers at $SLEEP_RATE/s"
$R wf load sleeper rate="$SLEEP_RATE" secs="$((SLEEPERS / SLEEP_RATE))" prefix=soak-sleep input='{"secs": 2592000}' \
  >"$OUT/sleepers.json" 2>"$OUT/sleepers.err"
jq -c '{sent, statuses, ack_p99_ms, started_per_s}' "$OUT/sleepers.json"

# The console's list with the sleepers in the index: GET /api/runs on h1, 20 times each, its first page and its 50th.
python3 - "http://127.0.0.1:$((WF_PORT + 1))" "$WF_PASSWORD" <<'PY' | tee "$OUT/console.txt"
import json, sys, time, urllib.parse, urllib.request
api, password = sys.argv[1], sys.argv[2]
login = urllib.request.Request(f"{api}/api/login", json.dumps({"password": password}).encode(), {"content-type": "application/json"})
cookie = urllib.request.urlopen(login).headers["set-cookie"].split(";")[0]
def get(query):
    t = time.perf_counter()
    with urllib.request.urlopen(urllib.request.Request(f"{api}/api/runs?{query}", headers={"cookie": cookie})) as r:
        body = json.load(r)
    return time.perf_counter() - t, body
for name, query in (("no filter", "limit=50"), ("status=running", "limit=50&status=running"), ("workflow=sleeper", "limit=50&workflow=sleeper")):
    cursor, at = None, {}
    for page in range(1, 51):
        q = query + (f"&cursor={urllib.parse.quote(cursor)}" if cursor else "")
        if page in (1, 50):
            times = sorted(get(q)[0] for _ in range(20))
            at[page] = f"p50 {times[9] * 1000:.0f} ms, p95 {times[18] * 1000:.0f} ms"
        cursor = get(q)[1].get("next")
        if not cursor:
            break
    print(f"GET /api/runs {name}: page 1 {at.get(1)}; page 50 {at.get(50, 'no page 50')}")
PY

windows=$((HOURS * 3600 / WINDOW))
started=$SECONDS
for i in $(seq 1 "$windows"); do
  nodes=(e1 e2 w1 w2 h2 h3)
  node=${nodes[$((RANDOM % 6))]}
  at=$((60 + RANDOM % (WINDOW - 120)))
  date +%s >"$OUT/window-$i.started"
  $R wf load nine_steps rate="$RATE" secs="$WINDOW" prefix="soak-$i" acks="$OUT/acks-$i.jsonl" wait=300 \
    >"$OUT/window-$i.json" 2>"$OUT/window-$i.err" &
  load=$!
  sleep "$at"
  $R kill "$node"
  sleep 10
  $R start "$node"
  wait "$load" || true
  echo "$i $((SECONDS - started)) s: $node killed at $at s; $(jq -c '{statuses, ack_p99_ms, ended: .ended.by_status, end_p99_ms: .ended.end_p99_ms}' "$OUT/window-$i.json")"
  date +%s >"$OUT/window-$i.ended"
done

$R wf workflows --json | jq '[.workflows[] | select(.name == "sleeper") | .runs.running // 0] | add // 0' \
  >"$OUT/sleepers-running" || echo 0 >"$OUT/sleepers-running"
kill "$sampler" 2>/dev/null || true
sampler=
$R down

# The acks of the windows whose runs retention has not removed yet: a run is removed RETENTION after it ends, so
# the window's first runs count, not its last.
keep_s=$(python3 -c "import sys; t=sys.argv[1]; print(int(t[:-1]) * {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[t[-1]])" "$RETENTION")
acks=()
for i in $(seq 1 "$windows"); do
  [ $(($(date +%s) - $(cat "$OUT/window-$i.started"))) -ge $((keep_s - 120)) ] || acks+=(--acks "$OUT/acks-$i.jsonl")
done
witness=()
for file in "$OUT"/witness/*.jsonl; do witness+=(--witness "$file"); done
"$WF" check "$OUT/h1" "$OUT/h2" "$OUT/h3" "${witness[@]}" "${acks[@]}" --json >"$OUT/check.json" || true
jq -c '{runs, entries, acks, witnessed, violations: (.violations | length)}' "$OUT/check.json"

python3 - "$OUT" "$windows" "$SLEEPERS" "$keep_s" "$WINDOW" <<'PY'
import csv, json, sys
out, windows, sleepers, keep_s, window = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
fails = []
check = json.load(open(f"{out}/check.json"))
for v in check["violations"][:20]:
    fails.append(f"{v['invariant']} run {v['run']}: {v['what']}")
for i in range(1, windows + 1):
    said = json.load(open(f"{out}/window-{i}.json"))
    ended = said.get("ended", {})
    if not ended.get("complete") or set(ended.get("by_status", {})) - {"completed"}:
        fails.append(f"window {i}: {ended.get('by_status')}, open {ended.get('open')}")
    refused = {k: v for k, v in said["statuses"].items() if k not in ("200", "201")}
    if refused:
        fails.append(f"window {i}: starts refused {refused}")
running = int(open(f"{out}/sleepers-running").read().strip() or 0)
if running != sleepers:
    fails.append(f"{running} of {sleepers} sleepers running")
rows = [r for r in csv.DictReader(open(f"{out}/soak.csv")) if int(r["rss_kib"]) > 0]
end = max(int(r["t_s"]) for r in rows)
# A history node's memory grows until the runs removed in the last retention have gone from its log's keys too, about
# two retentions and a window in: growth counts from there, or from a third of the soak if later.
first = max(end / 3, 2 * keep_s + window)
print(f"node   rss MiB at {first:.0f} s   at end   growth   dir MiB max   at end")
for node in ("h1", "h2", "h3", "e1", "e2", "w1", "w2"):
    mine = [r for r in rows if r["node"] == node]
    # The last sample before the third, and the last: a node killed in between starts its RSS again.
    a = [r for r in mine if int(r["t_s"]) <= first][-1]
    b = mine[-1]
    ra, rb = int(a["rss_kib"]) / 1024, int(b["rss_kib"]) / 1024
    growth = (rb - ra) / ra * 100 if ra else 0
    dirs = [int(r["dir_bytes"] or 0) / 2**20 for r in mine]
    print(f"{node:<6} {ra:>16.0f} {rb:>8.0f} {growth:>7.1f} % {max(dirs):>12.0f} {dirs[-1]:>8.0f}")
    if growth >= 5:
        fails.append(f"{node}: RSS grew {growth:.1f} % from {ra:.0f} to {rb:.0f} MiB")
print("FAIL\n  " + "\n  ".join(fails) if fails else "PASS")
sys.exit(1 if fails else 0)
PY
