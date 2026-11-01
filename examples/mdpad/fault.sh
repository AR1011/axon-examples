#!/usr/bin/env bash
# The fault run, as processes: the cluster of scripts/run.sh (a seed, homes h1..h3 that also serve the web, h1 with
# the accounts, and a web node w1), and in each phase `mdpad load` on six documents across the homes, three editors
# and a viewer on each through every web node, and $HOT more viewers on the first.
#   phase web:      SIGKILL w1 at 8 s, start it again at 14 s.
#   phase home:     SIGKILL h2 at 8 s, start it again at 12 s.
#   phase disk:     fill h3's disk (a tmpfs; needs sudo) at 8 s, free it at 16 s.
#   phase accounts: SIGKILL h1, which keeps the accounts, at 8 s; start it again at 14 s.
# After each phase `mdpad check` reads the bots' logs back: nothing acked or seen is lost, every copy is the server's,
# and every cursor left. The disk phase must also have told the editors it cannot save. Each phase prints how long the
# clients took to be synced again (p99 and max), and a phase that starts a node again how long after its ready line
# every editor of its documents saved again; the web phase gates on design §7's 3 s p99, the home phase on its 2 s.
# Prints PASS, or FAIL and what failed, and exits non-zero. Stops what it starts.
#
#   cargo build --release -p mdpad && examples/mdpad/fault.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
export MDPAD_BIN=${MDPAD_BIN:-${CARGO_TARGET_DIR:-target}/release/mdpad}
export MDPAD_DIR=${MDPAD_DIR:-$(mktemp -d)}
export MDPAD_PORT=${MDPAD_PORT:-7300}
SECS=${SECS:-30}
HOT=${HOT:-500}
R=examples/mdpad/scripts/run.sh
OUT=$MDPAD_DIR
echo "logs and results in $OUT"
cleanup() {
  $R down
  ! mountpoint -q "$OUT/h3" || sudo umount "$OUT/h3"
}
trap cleanup EXIT

# Each write grows its file, so the full disk is seen at the next write, not when a new segment is made.
export MDPAD_NODE_ARGS=preallocate=false
mkdir -p "$OUT/h3"
sudo mount -t tmpfs -o size=64m,mode=0777 tmpfs "$OUT/h3"
$R up >/dev/null
HTTP=$($R http)
fails=()

load() { # phase
  "$MDPAD_BIN" load http="$HTTP" user=load password=load-pw docs=6 editors=3 viewers=1 hot="$HOT" rate=5 \
    secs="$SECS" run="$OUT/$1" >"$OUT/$1.json" 2>"$OUT/$1.err" &
  bots=$!
}
check() { # phase [ready_ms resync_p99_limit_ms saving_again_limit_ms]
  wait "$bots" || true
  local said
  if said=$("$MDPAD_BIN" check "$OUT/$1" 2>&1); then
    echo "$1: $said"
  else
    echo "$1: $said"
    fails+=("$1: $said")
  fi
  local gate
  gate=$(python3 - "$OUT/$1.json" "${2:-0}" "${3:-0}" "${4:-0}" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
ready, resync_limit, again_limit = (int(x) for x in sys.argv[2:5])
print(f"  {r['acked']} acked, notices {r['notices']}, {r['resyncs']} resyncs p99 {r['resync_p99_ms']:.0f} ms max {r['resync_max_ms']:.0f} ms", file=sys.stderr)
if resync_limit and r["resync_p99_ms"] > resync_limit:
    print(f"resync p99 {r['resync_p99_ms']:.0f} ms over {resync_limit} ms")
if ready:
    after = [t - ready for t in r["resumed_unix_ms"] if ready < t <= ready + 30_000]
    print(f"  every editor saving again {max(after, default=0)} ms after the ready line", file=sys.stderr)
    if again_limit and max(after, default=0) > again_limit:
        print(f"saving again {max(after)} ms after the ready line, over {again_limit} ms")
PY
) || gate="no report"
  [ -z "$gate" ] || fails+=("$1: $gate")
}

echo "== phase web: SIGKILL w1 at 8 s, started again at 14 s"
load web
sleep 8
$R kill w1
sleep 6
$R start w1
check web "$(date +%s%3N)" 3000

echo "== phase home: SIGKILL h2 at 8 s, started again at 12 s"
load home
sleep 8
$R kill h2
sleep 4
$R start h2
check home "$(date +%s%3N)" 0 2000

echo "== phase disk: h3's disk full from 8 s to 16 s"
load disk
sleep 8
dd if=/dev/zero of="$OUT/h3/fill" bs=1M 2>/dev/null || true
sleep 8
rm -f "$OUT/h3/fill"
check disk
python3 -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1])).get("notices", {}).get("cannot_save", 0) > 0 else 1)' \
  "$OUT/disk.json" || fails+=("disk: no editor was told it cannot save")

echo "== phase accounts: SIGKILL h1 at 8 s, started again at 14 s"
load accounts
sleep 8
$R kill h1
sleep 6
$R start h1
check accounts "$(date +%s%3N)"

if [ ${#fails[@]} -eq 0 ]; then
  echo PASS
else
  printf 'FAIL\n'; printf '  %s\n' "${fails[@]}"; exit 1
fi
