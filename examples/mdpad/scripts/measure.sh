#!/usr/bin/env bash
# The numbers of design §7, on the cluster of scripts/run.sh (release build), one step after another. Each step prints
# one JSON line, also kept in $OUT/measure.jsonl; a step's bots and logs are in $OUT/<step>. A failed step is named
# on stderr and the next one runs; the script then exits 1.
#   remote   10 editors of one document at 5 keys/s for 60 s, over the four web nodes: a token typed through one web
#            node until an editor on another hears it, and each edit until saved
#   idle     a quiet document, 10 editors and no typing for 60 s: bytes in and out per client and second
#   offline  one editor types 5,000 updates offline while another types, then joins: until merged and saved
#   cold     100,000 characters, its home started again: GET .../markdown, the first call that loads it
#   open     $OPEN documents of 20 KiB, one editor each at 1 key/s through w1 alone, for 120 s: saved p99, and each
#            home's peak RSS above its RSS before, per document it has (h1's also holds the console's metrics)
#   hot      one document, 50 editors at 5 keys/s and 500 viewers for $HOT_SECS s (600): remote p99, slow-consumer
#            closes, and the CPU of the home that has it. Each editor types in a line of its own, keeps its last 100
#            tokens and removes the one before, so the text stays near 65 KB and the state under the 256 KiB cap.
#   kills    $KILLS (100) SIGKILLs of h2 under load, each started again: acked tokens lost, and per kill how long
#            after h2 printed its ready line every editor of h2 saved again
#   stored   $STORED documents (300,000: about 100,000 a home), each home started again (time to its ready line),
#            then 200 lists of the first page of the user's documents (the user owns all of them)
#
#   cargo build --release -p mdpad && examples/mdpad/scripts/measure.sh [step ..]
set -euo pipefail
cd "$(dirname "$0")/../../.."
export MDPAD_BIN=${MDPAD_BIN:-${CARGO_TARGET_DIR:-target}/release/mdpad}
export MDPAD_DIR=${MDPAD_DIR:-$(mktemp -d)}
export MDPAD_PORT=${MDPAD_PORT:-7300}
# A document nobody calls stops after 30 s, so the stored step's new documents do not all stay open.
export MDPAD_NODE_ARGS=${MDPAD_NODE_ARGS:-idle=30}
OPEN=${OPEN:-2000} HOT_SECS=${HOT_SECS:-600} KILLS=${KILLS:-100} STORED=${STORED:-300000}
R=examples/mdpad/scripts/run.sh
OUT=$MDPAD_DIR
STEPS=${*:-remote idle offline cold open hot kills stored}
echo "logs and results in $OUT" >&2
trap '$R down' EXIT
$R up >/dev/null
HTTP=$($R http)
WEB=http://127.0.0.1:$((MDPAD_PORT + 1))

# A step that fails is named in $OUT/failed and the next one runs; the script exits 1 at the end if any failed.
failed() { # step why
  echo "measure: step $1 failed: $2" >&2
  echo "$1" >>"$OUT/failed"
}
say() { # step json
  if python3 -c 'import json, sys; assert isinstance(json.loads(sys.argv[1]), dict)' "$2" 2>/dev/null; then
    echo "{\"step\":\"$1\",${2#\{}" | tee -a "$OUT/measure.jsonl"
  else
    failed "$1" "not one JSON object: ${2:-no output}"
  fi
}
load() { # step args..
  local step=$1; shift
  "$MDPAD_BIN" load http="${LOAD_HTTP:-$HTTP}" user=load password=load-pw run="$OUT/$step" "$@" 2>"$OUT/$step.err" ||
    failed "$step" "load exited $?, see $OUT/$step.err"
}
rss() { # node: its resident memory in KiB
  awk '/^VmRSS/ {print $2}' "/proc/$(cat "$MDPAD_DIR/$1.pid")/status"
}
cpu() { # node: its user and system CPU time in clock ticks
  awk '{print $14 + $15}' "/proc/$(cat "$MDPAD_DIR/$1.pid")/stat"
}
now_ms() { date +%s%3N; }

for step in $STEPS; do
  case $step in
  remote) say remote "$(load remote docs=1 editors=10 rate=5 secs=60)" ;;
  idle) say idle "$(load idle docs=1 editors=10 rate=0.01 secs=60)" ;;
  offline) say offline "$("$MDPAD_BIN" load http="$HTTP" user=load password=load-pw offline=5000)" ;;
  cold)
    say cold "$(python3 - "$WEB" "$MDPAD_DIR" "$R" <<'PY'
import json, subprocess, sys, time, urllib.request
web, dir, run = sys.argv[1:4]
def call(method, path, body=None, token=None):
    req = urllib.request.Request(web + path, method=method, data=json.dumps(body).encode() if body else None)
    req.add_header("content-type", "application/json")
    if token: req.add_header("authorization", f"Bearer {token}")
    with urllib.request.urlopen(req) as r: return r.read()
token = json.loads(call("POST", "/api/login", {"name": "load", "password": "load-pw", "token": True}))["token"]
line = "A line of the long text, to make it one hundred thousand characters.\n"
text = "# Cold open\n\n" + line * (100_000 // len(line) + 1)
doc = json.loads(call("POST", "/api/docs", {"markdown": text}, token))["id"]
home = doc.split("-")[0]
subprocess.run([run, "stop", home], check=True)
start = time.monotonic()
subprocess.run([run, "start", home], check=True, stdout=subprocess.DEVNULL)
ready = time.monotonic() - start
start = time.monotonic()
got = call("GET", f"/api/docs/{doc}/markdown", token=token).decode()
first = time.monotonic() - start
start = time.monotonic()
call("GET", f"/api/docs/{doc}/markdown", token=token)
again = time.monotonic() - start
print(json.dumps({"doc": doc, "chars": len(text), "equal": got == text, "home_ready_s": round(ready, 3),
                  "first_get_ms": round(first * 1000, 1), "loaded_get_ms": round(again * 1000, 1)}))
PY
)"
    ;;
  open)
    before=(); for h in h1 h2 h3; do before+=("$(rss $h)"); done
    # Through w1 only, so the homes hold no sessions and no relayed copies: their memory is the documents'.
    LOAD_HTTP=127.0.0.1:$((MDPAD_PORT + 4)) load open docs="$OPEN" editors=1 rate=1 size=20480 secs=120 >"$OUT/open.json" &
    bots=$!
    peak=(0 0 0)
    while kill -0 $bots 2>/dev/null; do
      i=0; for h in h1 h2 h3; do r=$(rss $h); [ "$r" -le "${peak[$i]}" ] || peak[$i]=$r; i=$((i + 1)); done
      sleep 5
    done
    say open "$(python3 - "$OUT/open.json" "${before[*]}" "${peak[*]}" "$OUT/open/texts" <<'PY'
import json, os, sys
report = json.load(open(sys.argv[1]))
before, peak = [int(x) for x in sys.argv[2].split()], [int(x) for x in sys.argv[3].split()]
ids = [f.split("-")[0] for f in os.listdir(sys.argv[4])]
homes = {}
for h, b, p in zip(["h1", "h2", "h3"], before, peak):
    n = ids.count(h)
    homes[h] = {"docs": n, "rss_before_kib": b, "rss_peak_kib": p, "kib_per_doc": round((p - b) / max(n, 1), 1)}
print(json.dumps({"docs": len(ids), "ack_p50_ms": report["ack_p50_ms"], "ack_p99_ms": report["ack_p99_ms"],
                  "ack_max_ms": report["ack_max_ms"], "lost_acked": report["lost_acked"],
                  "reconnects": report["reconnects"], "homes": homes}))
PY
)"
    ;;
  hot)
    c0=(); for h in h1 h2 h3; do c0+=("$(cpu $h)"); done
    t0=$(now_ms)
    load hot docs=1 editors=50 hot=500 rate=5 keep=100 secs="$HOT_SECS" >"$OUT/hot.json"
    t1=$(now_ms)
    c1=(); for h in h1 h2 h3; do c1+=("$(cpu $h)"); done
    say hot "$(python3 - "$OUT/hot.json" "${c0[*]}" "${c1[*]}" "$t0" "$t1" "$(getconf CLK_TCK)" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
c0, c1 = [int(x) for x in sys.argv[2].split()], [int(x) for x in sys.argv[3].split()]
secs, tck = (int(sys.argv[5]) - int(sys.argv[4])) / 1000, int(sys.argv[6])
cores = {h: round((b - a) / tck / secs, 3) for h, a, b in zip(["h1", "h2", "h3"], c0, c1)}
keep = ["typed", "acked", "ack_p99_ms", "remote_n", "remote_p50_ms", "remote_p99_ms", "remote_max_ms", "reconnects",
        "reasons", "lost_acked", "seen_lost", "diverged", "presence_left"]
print(json.dumps({**{k: r[k] for k in keep}, "home_cores": cores, "secs_with_settle": round(secs, 1)}))
PY
)"
    ;;
  kills)
    load kills docs=6 editors=2 viewers=1 rate=5 secs=$((KILLS * 7 + 20)) >"$OUT/kills.json" &
    bots=$!
    sleep 5
    : >"$OUT/kills.ready"
    : >"$OUT/kills.killed"
    for _ in $(seq "$KILLS"); do
      now_ms >>"$OUT/kills.killed"
      $R kill h2
      sleep 1
      $R start h2 >/dev/null
      now_ms >>"$OUT/kills.ready"
      sleep 4
    done
    wait $bots
    say kills "$(python3 - "$OUT/kills.json" "$OUT/kills.ready" "$OUT/kills.killed" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
ready = [int(x) for x in open(sys.argv[2]).read().split()]
killed = [int(x) for x in open(sys.argv[3]).read().split()]
resumed = sorted(r["resumed_unix_ms"])
back = []
# Up to the next kill: h2 is a web node too, and its next kill's clients resume on another before h2 is ready.
for i, t in enumerate(ready):
    end = killed[i + 1] if i + 1 < len(killed) else t + 30_000
    after = [x for x in resumed if t < x <= end]
    if after: back.append(max(after) - t)
back.sort()
pick = lambda p: back[min(len(back) - 1, int(len(back) * p))] if back else None
print(json.dumps({"kills": len(ready), "acked": r["acked"], "lost_acked": r["lost_acked"], "seen_lost": r["seen_lost"],
                  "diverged": r["diverged"], "kills_with_a_stall": len(back), "saving_again_p50_ms": pick(0.5),
                  "saving_again_p99_ms": pick(0.99), "saving_again_max_ms": back[-1] if back else None,
                  "resync_p99_ms": r["resync_p99_ms"]}))
PY
)"
    ;;
  stored)
    created=$("$MDPAD_BIN" load http="$HTTP" user=load password=load-pw create="$STORED")
    ready=()
    for h in h1 h2 h3; do
      $R stop $h
      t0=$(now_ms); $R start $h >/dev/null; ready+=($(($(now_ms) - t0)))
    done
    lists=$("$MDPAD_BIN" load http="$HTTP" user=load password=load-pw lists=200)
    say stored "$(python3 -c 'import json,sys; c=json.loads(sys.argv[1]); l=json.loads(sys.argv[2]); print(json.dumps({"created": c["created"], "created_per_sec": round(c["created_per_sec"]), "refused": c["refused"], "reasons": c["reasons"], "home_ready_ms": dict(zip(["h1","h2","h3"], map(int, sys.argv[3].split()))), **{k: l[k] for k in ["lists","list_p50_ms","list_p99_ms","list_max_ms"]}}))' "$created" "$lists" "${ready[*]}")"
    ;;
  *) echo "no step $step" >&2; exit 2 ;;
  esac
done
if [ -s "$OUT/failed" ]; then
  echo "measure: failed: $(sort -u "$OUT/failed" | xargs)" >&2
  exit 1
fi
