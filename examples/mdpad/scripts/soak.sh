#!/usr/bin/env bash
# The soak (design §8), box only: the cluster of scripts/run.sh under `mdpad load` on 200 documents, 5 editors at one
# key each 5 s and 5 viewers on each, for $SOAK_MIN minutes (120), with fault.sh's faults on a 10-minute cycle. Each
# editor removes its oldest token once it has 100, so a text stays near 6 KB and memory that grows is not the text's:
#   minute 2: SIGKILL w1, started again 6 s later      minute 4: SIGKILL h2, started again 4 s later
#   minute 6: h3's disk (a tmpfs; needs sudo) full 8 s  minute 8: SIGKILL h1, the accounts, started again 6 s later
# Each node's RSS is sampled every minute into $OUT/rss.csv. The gates, then PASS or FAIL and exit 1:
#   - `mdpad check`: no acked or seen token lost, every copy the server's, no cursor left;
#   - h3, the one home never killed: its RSS at the end within 10 % of its RSS at minute 20, when every document
#     is open;
#   - each home's log under 1 GiB (the log's limit is 4 GiB; a log that only grows reaches it);
#   - once the load ends and the documents stop (idle=120), no document stays open and no stream stays on any node;
#   - h3's documents saved an automatic version at least every 31 minutes of the load (design §5.2: every 30).
#
#   cargo build --release -p mdpad && examples/mdpad/scripts/soak.sh
set -euo pipefail
cd "$(dirname "$0")/../../.."
export MDPAD_BIN=${MDPAD_BIN:-${CARGO_TARGET_DIR:-target}/release/mdpad}
export MDPAD_DIR=${MDPAD_DIR:-$(mktemp -d)}
export MDPAD_PORT=${MDPAD_PORT:-7300}
export MDPAD_NODE_ARGS="preallocate=false idle=120"
SOAK_MIN=${SOAK_MIN:-120}
R=examples/mdpad/scripts/run.sh
OUT=$MDPAD_DIR
echo "logs and results in $OUT"
cleanup() {
  $R down
  ! mountpoint -q "$OUT/h3" || sudo umount "$OUT/h3"
}
trap cleanup EXIT
mkdir -p "$OUT/h3"
sudo mount -t tmpfs -o size=2g,mode=0777 tmpfs "$OUT/h3"
$R up >/dev/null
HTTP=$($R http)
# The command line as dev, the operator, for `cluster`, and as load, who owns the documents, for `versions`.
for user in dev load; do
  MDPAD_CONFIG=$OUT/$user.config.json "$MDPAD_BIN" login url="http://127.0.0.1:$((MDPAD_PORT + 4))" name=$user password=$user-pw >/dev/null
done

rss() { awk '/^VmRSS/ {print $2}' "/proc/$(cat "$MDPAD_DIR/$1.pid" 2>/dev/null || echo 0)/status" 2>/dev/null || echo 0; }
sample() { # minute
  local row=$1
  for n in h1 h2 h3 w1; do row="$row,$(rss $n)"; done
  echo "$row" >>"$OUT/rss.csv"
}

echo "minute,h1,h2,h3,w1" >"$OUT/rss.csv"
"$MDPAD_BIN" load http="$HTTP" user=load password=load-pw docs=200 editors=5 viewers=5 rate=0.2 keep=100 \
  secs=$((SOAK_MIN * 60)) run="$OUT/run" >"$OUT/load.json" 2>"$OUT/load.err" &
bots=$!
started_ms=$(date +%s%3N)
start=$SECONDS
at() { # second of the run: waits until then
  local wait=$(($1 - (SECONDS - start)))
  [ "$wait" -le 0 ] || sleep "$wait"
}
for minute in $(seq 0 $((SOAK_MIN - 1))); do
  at $((minute * 60))
  sample "$minute"
  case $((minute % 10)) in
  2) $R kill w1; sleep 6; $R start w1 >/dev/null ;;
  4) $R kill h2; sleep 4; $R start h2 >/dev/null ;;
  6) dd if=/dev/zero of="$OUT/h3/fill" bs=1M 2>/dev/null || true; sleep 8; rm -f "$OUT/h3/fill" ;;
  8) $R kill h1; sleep 6; $R start h1 >/dev/null ;;
  esac
  echo "minute $minute: $(tail -n 1 "$OUT/rss.csv")"
done
wait $bots || true
ended_ms=$(date +%s%3N)
sample "$SOAK_MIN"
fails=()
said=$("$MDPAD_BIN" check "$OUT/run" 2>&1) || fails+=("check: $said")
echo "check: $said"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print({k: r[k] for k in ["typed","acked","ack_p99_ms","reconnects","reasons","resyncs","resync_p99_ms","resync_max_ms"]})' "$OUT/load.json" || fails+=("no load report")

# The documents stop 120 s after their last call; then nothing stays open.
sleep 180
MDPAD_CONFIG=$OUT/dev.config.json "$MDPAD_BIN" cluster --json >"$OUT/cluster.json"
MDPAD_CONFIG=$OUT/dev.config.json "$MDPAD_BIN" cluster
gate=$(python3 - "$OUT/rss.csv" "$OUT/cluster.json" <<'PY'
import csv, json, sys
rows = list(csv.DictReader(open(sys.argv[1])))
warm = next((int(r["h3"]) for r in rows if int(r["minute"]) == 20), None)
end = int(rows[-1]["h3"])
if warm is None:
    print("no RSS at minute 20")
elif end > warm * 1.1:
    print(f"h3 RSS {end} KiB at the end, over 110 % of {warm} KiB at minute 20")
print(f"  h3 RSS {warm} KiB at minute 20, {end} KiB at the end", file=sys.stderr)
c = json.load(open(sys.argv[2]))
for home in c["homes"]:
    if "error" in home:
        print(f"{home['home']} did not answer: {home['error']}")
        continue
    if home["log_bytes"] > 1 << 30:
        print(f"{home['home']} log {home['log_bytes']} bytes")
    if home["open"] or home.get("streams"):
        print(f"{home['home']}: {home['open']} open, {home.get('streams')} streams after the documents stopped")
    print(f"  {home['home']}: log {home['log_bytes'] >> 20} MiB, {home['stored']} stored", file=sys.stderr)
for web in c["webs"]:
    if web.get("streams"):
        print(f"web {web['node']}: {web['streams']} streams after the documents stopped")
PY
)
[ -z "$gate" ] || while read -r line; do fails+=("$line"); done <<<"$gate"

# Versions of h3's documents: an automatic one at most 31 minutes after the start of the load or the one before it.
versions=$(MDPAD_CONFIG=$OUT/load.config.json python3 - "$OUT/run/texts" "$MDPAD_BIN" "$started_ms" "$ended_ms" <<'PY'
import json, subprocess, sys
import os
ids = sorted(f[:-3] for f in os.listdir(sys.argv[1]) if f.startswith("h3-"))[:10]
start, end, gap = int(sys.argv[3]), int(sys.argv[4]), 31 * 60_000
counts, short = [], []
for doc in ids:
    out = subprocess.run([sys.argv[2], "versions", doc, "--json"], capture_output=True, text=True, check=True).stdout
    listed = json.loads(out)
    listed = listed.get("versions", listed) if isinstance(listed, dict) else listed
    times = sorted(v["at_ms"] for v in listed if v.get("kind") == "auto")
    counts.append(len(times))
    edges = [start] + [t for t in times if t <= end] + [end]
    longest = max(b - a for a, b in zip(edges, edges[1:]))
    if longest > gap:
        short.append(f"{doc}: {len(times)} automatic versions, {longest // 60_000} minutes with none")
print(f"  automatic versions of h3's documents: {counts}", file=sys.stderr)
print("\n".join(short))
PY
) || versions="the versions could not be read"
[ -z "$versions" ] || while read -r line; do fails+=("$line"); done <<<"$versions"

if [ ${#fails[@]} -eq 0 ]; then
  echo PASS
else
  printf 'FAIL\n'; printf '  %s\n' "${fails[@]}"; exit 1
fi
