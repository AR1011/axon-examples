#!/usr/bin/env bash
# The fault run, as processes: the cluster of scripts/run.sh under three loads through h1's API at once, for $SECS
# each phase: checkouts with their approvals (one in ten declined), nine-step runs and sleepers of 5 s.
#   phase engine:   SIGKILL e1 at 5 s, start it again at 15 s.
#   phase worker:   SIGKILL w1 at 5 s, start it again at 15 s.
#   phase history:  SIGKILL h3 at 5 s, start it again at 15 s.
#   phase pause:    SIGSTOP e2 at 5 s, SIGCONT at 15 s.
#   phase roll:     from 5 s, SIGTERM w1, w2, e1 and e2 in turn, each started again once it is gone.
#   phase restart:  once the starts are sent, SIGTERM every node, then start them all again.
# Each phase waits until every run it started ended. Then every node stops, and `wf check` reads the history nodes'
# directories with the witness logs and the acks the loads got (I2-I8, I10, I14-I16). Checks that each phase's runs
# all ended as their input says, that no start was refused while one history node was down, that no attempt failed
# in the rolling phase (I18) and that the checker finds nothing. Prints PASS, or FAIL and exits 1. Stops what it
# starts.
#
#   cargo build --release -p workflows && examples/workflows/fault.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
export WF_BIN=${WF_BIN:-${CARGO_TARGET_DIR:-target}/release/workflows}
export WF_DIR=${WF_DIR:-$(mktemp -d)}
export WF_PORT=${WF_PORT:-7700}
export WF_API=h1
SECS=${SECS:-20}
R=examples/workflows/scripts/run.sh
WF=$(dirname "$WF_BIN")/wf
OUT=$WF_DIR
echo "logs and results in $OUT"
cleanup() {
  for pid in "$OUT"/*.pid; do [ -f "$pid" ] && kill -CONT "$(cat "$pid")" 2>/dev/null || true; done
  $R down
}
trap cleanup EXIT

$R up
$R wf cluster >/dev/null # signs in once, before the loads share the credentials

# loads <phase>: the three loads in the background, each until its runs ended.
loads() {
  pids=()
  for load in checkout:20 nine_steps:20 sleeper:10; do
    local workflow=${load%%:*}
    $R wf load "$workflow" rate="${load##*:}" secs="$SECS" prefix="$1-$workflow" acks="$OUT/acks.jsonl" wait=240 \
      >"$OUT/$1-$workflow.json" 2>"$OUT/$1-$workflow.err" &
    pids+=($!)
  done
}
report() {
  wait "${pids[@]}"
  for workflow in checkout nine_steps sleeper; do
    echo "  $workflow: $(jq -c '{statuses, ack_p50_ms, ack_p99_ms, ended: .ended.by_status, end_p99_ms: .ended.end_p99_ms}' "$OUT/$1-$workflow.json")"
  done
}

for phase in engine worker history pause; do
  case $phase in
  engine) node=e1 ;;
  worker) node=w1 ;;
  history) node=h3 ;;
  pause) node=e2 ;;
  esac
  echo "== phase $phase: $node at 5 s, back at 15 s; $SECS s of load"
  loads "$phase"
  sleep 5
  if [ "$phase" = pause ]; then
    kill -STOP "$(cat "$OUT/$node.pid")"
    sleep 10
    kill -CONT "$(cat "$OUT/$node.pid")"
  else
    $R kill "$node"
    sleep 10
    $R start "$node"
  fi
  report "$phase"
done

echo "== phase roll: SIGTERM w1, w2, e1 and e2 in turn from 5 s; $SECS s of load"
loads roll
sleep 5
$R roll
report roll

echo "== phase restart: every node stopped and started again once the starts are sent"
loads restart
sleep "$((SECS + 1))"
$R down
$R up
report restart

$R down
witness=()
for file in "$OUT"/witness/*.jsonl; do witness+=(--witness "$file"); done
"$WF" check "$OUT/h1" "$OUT/h2" "$OUT/h3" "${witness[@]}" --acks "$OUT/acks.jsonl" --json >"$OUT/check.json" || true
jq -c '{runs, entries, acks, witnessed, violations: (.violations | length)}' "$OUT/check.json"

python3 - "$OUT" <<'PY'
import json, sys
out = sys.argv[1]
fails = []
check = json.load(open(f"{out}/check.json"))
for v in check["violations"][:20]:
    fails.append(f"{v['invariant']} run {v['run']}: {v['what']}")
for phase in ("engine", "worker", "history", "pause", "roll", "restart"):
    for workflow in ("checkout", "nine_steps", "sleeper"):
        said = json.load(open(f"{out}/{phase}-{workflow}.json"))
        ended = said.get("ended", {})
        by = ended.get("by_status", {})
        if not ended.get("complete"):
            fails.append(f"{phase} {workflow}: not every run ended: {by}, the open ones say {ended.get('open')}")
        allowed = {"completed", "failed"} if workflow == "checkout" else {"completed"}
        if set(by) - allowed:
            fails.append(f"{phase} {workflow}: {by}")
        statuses = said["statuses"]
        refused = {k: v for k, v in statuses.items() if k not in ("200", "201")}
        if refused:
            fails.append(f"{phase} {workflow}: starts refused {refused}")
        group = check["groups"].get(f"{phase}-{workflow}", {})
        if phase == "roll" and group.get("failed_attempts"):
            fails.append(f"roll {workflow}: {group['failed_attempts']} attempts failed (I18)")
print("FAIL\n  " + "\n  ".join(fails) if fails else "PASS")
sys.exit(1 if fails else 0)
PY
