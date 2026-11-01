#!/usr/bin/env bash
# The fault run, as processes: the cluster of scripts/run.sh with three nodes, and `kv check` against all three.
# T is the lease (INFO kv lease_ms); a shard that moves must take writes again within T + 3 s, away and back.
#   memory-control (I7)  memory mode; SIGKILL n3 at 10 s, start it again at 20 s. The shards n3 held start empty on
#                        the survivors, so the check must find acknowledged writes lost (`--expect-loss`): a check
#                        that cannot see this loss could not see any.
#   kill                 SIGKILL the owner of writer 0's first key at 10 s, start it again at 25 s.
#   stop                 SIGSTOP that owner at 10 s, SIGCONT at 25 s: it wakes past its lease, deposed.
#   stop-lease           stop with `--reads lease`: no stale read while monotonic clocks run through the stop (I5).
#   follower             the check writes only to shards n1 and n2 own; SIGKILL n3 at 10 s, start it at 25 s. No
#                        shard the check writes may move (I12).
#   restart-all          check for 20 s, SIGKILL all three, start them at once (about 85 shards recover together on
#                        each), then read back every key the check wrote (`kv check --verify`).
#   quorum-loss          check for 20 s, SIGKILL n2 and n3; from T + 1 s on, a check against n1 alone for 10 s
#                        must see nothing acknowledged and only CLUSTERDOWN; start them again, read back every key.
#   partition            Linux and sudo only. Cut the owner of writer 0's first key off from the other two nodes at
#                        10 s, heal at 25 s: iptables drops its packets on the cluster and seed ports, both ways. The
#                        check goes through the other two, and the cut node's shards must move to them; a second
#                        check through the cut node alone must lose no acknowledged write and read nothing stale.
#   all                  the phases above but partition, in that order.
#   soak [min]           workload A at half its saturation for min minutes (default 60), go-ycsb from bench.sh
#                        setup, and back-to-back checks of 2 to 5 min, each with a kill or a stop of a random node:
#                        every check passes, each node's WAL stays bounded, and each node's RSS is flat.
# Durable mode, three nodes, every phase but memory-control; each from empty data directories. PASS also needs the
# three Logs' digests equal at the end. Each phase writes its history to $KV_DIR/<phase>.jsonl (the verdict on its
# first line) and one line to $KV_DIR/faults.jsonl. Exits non-zero if a phase fails. Stops what it starts.
#
#   cargo build --release -p kv
#   examples/kv/fault.sh [phase..]       e.g. `fault.sh all all`: every phase twice in a row
set -euo pipefail
cd "$(dirname "$0")/../.."
export KV_BIN=${KV_BIN:-${CARGO_TARGET_DIR:-target}/release/kv}
export KV_DIR=${KV_DIR:-$(mktemp -d)}
SECS=${SECS:-40}
R=examples/kv/scripts/run.sh
TOOLS=${KV_TOOLS:-$HOME/axon/kv-tools}
echo "logs and results in $KV_DIR"
trap '$R down; [ -z "${KV_CGROUP:-}" ] || unpartition' EXIT

kv() { "$KV_BIN" "$@"; }
addrs() { $R addrs; }
started() { kv status --addrs "$(addrs)" | jq '[.nodes[].info.shard_starts // 0] | add'; }
pid_of() { cat "$KV_DIR/$1.pid"; }

# The lease times INFO kv gives, as JSON: T, hold(), lease_grace and the renew interval. With no node to give them,
# T is taken as 0 and broken notes it, so the phase fails.
times() {
  local t
  t=$(kv status --addrs "$(addrs)" | jq -c '[.nodes[].info | select(.lease_ms) |
    {T_ms: .lease_ms, hold_ms: .lease_hold_ms, grace_ms: .lease_grace_ms, renew_ms: .lease_renew_ms}] | first')
  if [ -z "$t" ] || [ "$t" = null ]; then
    echo "no node gave its lease times" >>"$KV_DIR/broken"
    t='{"T_ms": 0}'
  fi
  echo "$t"
}

# windows <history> <ms>: the shards' windows without a write, those that began before ms (the move away) and after
# (the move back), as count, p50 and max in ms. The probes write every 10 ms, so each is good to about 10 ms.
windows() {
  jq -sc --argjson at "$2" '[.[] | select(.window != null)] as $w
    | def stats: map(.ms) | sort | {n: length, p50_ms: (.[length / 2 | floor] // 0), max_ms: (max // 0)};
    {away: ($w | map(select(.from_ms < $at)) | stats), back: ($w | map(select(.from_ms >= $at)) | stats)}' "$1"
}

# digests <phase>: the Logs' per-shard digests, asked again until equal or 30 s pass.
digests() {
  kv status --digest --addrs "$(addrs)" --within 30s >"$KV_DIR/$1-digest.json" || true
  jq -c .digest "$KV_DIR/$1-digest.json"
  [ "$(jq .digest.differ "$KV_DIR/$1-digest.json")" = 0 ]
}

# recoveries: each node's shards and how long their starts took to serve, from INFO kv.
recoveries() {
  kv status --addrs "$(addrs)" | jq -c '.nodes[] | {addr, shards_here: .info.shards_here,
    recoveries: .info.recoveries, p50_ms: .info.recovery_p50_ms, p99_ms: .info.recovery_p99_ms, max_ms: .info.recovery_max_ms}'
}

verdict() { jq -c '{summary, lost, stale, bad, unread, writes_ok, writes_failed, unknown: (.errors["ERR outcome unknown"] // 0), longest_window_ms}' "$1"; }

# record <phase> <pass 0|1> <json>: one line of faults.jsonl, and PASS or FAIL. A node that did not start (as when
# another process holds its port) fails the phase.
record() {
  local pass=$2 more=$3
  if [ -s "$KV_DIR/broken" ]; then
    pass=0
    more=$(jq -c --rawfile b "$KV_DIR/broken" '. + {broken: $b}' <<<"$more")
    echo "$1: $(tr '\n' ' ' <"$KV_DIR/broken")"
  fi
  jq -nc --arg phase "$1" --argjson pass "$pass" --argjson more "$more" '{phase: $phase, pass: ($pass == 1)} + $more' \
    >>"$KV_DIR/faults.jsonl"
  if [ "$pass" = 1 ]; then echo "$1: PASS"; else echo "$1: FAIL"; return 1; fi
}

# node_up <run.sh args>: up or start, noted in $KV_DIR/broken when it fails (a phase runs without errexit, and a
# start may run in the background).
node_up() { $R "$@" >/dev/null || echo "run.sh $* failed" >>"$KV_DIR/broken"; }

# within <windows json> <bound ms>: 1 when every window away and back ended within the bound.
within() { jq --argjson b "$2" 'if .away.max_ms <= $b and .back.max_ms <= $b then 1 else 0 end' <<<"$1"; }

# fault <kill|stop> <node>: SIGKILL it and start it again 15 s later, or SIGSTOP it and SIGCONT it 15 s later.
fault() {
  if [ "$1" = kill ]; then
    $R kill "$2"
    sleep 15
    node_up start "$2"
  else
    kill -STOP "$(pid_of "$2")"
    sleep 15
    kill -CONT "$(pid_of "$2")"
  fi
}

fresh() { # mode: empty data directories, three nodes up
  $R down
  rm -rf "$KV_DIR"/n[123] "$KV_DIR/broken"
  node_up up "$1" 3
}

memory_control() {
  fresh memory
  local before rc=0 w
  before=$(started)
  kv check --addrs "$(addrs)" --secs "$SECS" --expect-loss --out "$KV_DIR/memory-control.jsonl" \
    >"$KV_DIR/memory-control.json" &
  local check=$!
  sleep 10
  $R kill n3
  sleep 10
  node_up start n3
  wait "$check" || rc=1
  echo "memory-control: $(verdict "$KV_DIR/memory-control.json")"
  echo "memory-control: shards started $before before the kill, $(started) after"
  w=$(windows "$KV_DIR/memory-control.jsonl" 20000)
  echo "$w"
  $R down
  record memory-control $((rc == 0)) "{\"check\": $(verdict "$KV_DIR/memory-control.json"), \"windows\": $w}"
}

# one_node <phase> <kill|stop>: a durable phase around one node, the fault at 10 s, healed at 25 s.
one_node() {
  local phase=$1 how=$2 victim rc=0 t w bound
  fresh durable
  t=$(times)
  bound=$(($(jq .T_ms <<<"$t") + 3000))
  victim=$(kv status --owner kvc:w0:0 --addrs "$(addrs)" | jq -r .owner)
  kv check --addrs "$(addrs)" --secs "$SECS" --out "$KV_DIR/$phase.jsonl" >"$KV_DIR/$phase.json" &
  local check=$!
  sleep 10
  fault "$how" "$victim"
  wait "$check" || rc=1
  echo "$phase $victim: $(verdict "$KV_DIR/$phase.json")"
  w=$(windows "$KV_DIR/$phase.jsonl" 25000)
  echo "$w (bound $bound ms)"
  [ "$(within "$w" "$bound")" = 1 ] || { echo "$phase: a shard took longer than T + 3 s"; rc=1; }
  recoveries
  digests "$phase" || rc=1
  $R down
  record "$phase" $((rc == 0)) "{\"victim\": \"$victim\", \"check\": $(verdict "$KV_DIR/$phase.json"), \"windows\": $w, \"bound_ms\": $bound, \"lease\": $t}"
}

follower() {
  local rc=0 before after shards w moved
  fresh durable
  # Where each shard runs and under which lease epoch: a shard that moves, or starts again under a new lease, differs.
  before=$(kv status --hosts --addrs "$(addrs)" | jq -c .hosts)
  shards=$(jq -r '[to_entries[] | select(.value | test("^n[12]@[0-9]+$")) | .key] | join(",")' <<<"$before")
  kv check --addrs "$(addrs)" --secs "$SECS" --shards "$shards" --out "$KV_DIR/follower.jsonl" >"$KV_DIR/follower.json" &
  local check=$!
  sleep 10
  $R kill n3
  sleep 15
  node_up start n3
  wait "$check" || rc=1
  after=$(kv status --hosts --addrs "$(addrs)" | jq -c .hosts)
  moved=$(jq -n --argjson a "$before" --argjson b "$after" --arg s "$shards" \
    '[$s | split(",")[] | tonumber | select($a[.] != $b[.])] | length' || echo null)
  echo "follower: $(verdict "$KV_DIR/follower.json")"
  w=$(windows "$KV_DIR/follower.jsonl" 25000)
  echo "follower: $(jq -r 'split(",") | length' <<<"\"$shards\"") shards written, $moved moved; windows $w"
  [ -n "$shards" ] && [ "$moved" = 0 ] || rc=1
  digests follower || rc=1
  $R down
  record follower $((rc == 0)) "{\"check\": $(verdict "$KV_DIR/follower.json"), \"moved\": $moved, \"windows\": $w}"
}

restart_all() {
  local rc=0 i began answered
  fresh durable
  kv check --addrs "$(addrs)" --secs 20 --save "$KV_DIR/restart-all.keys" --out "$KV_DIR/restart-all.jsonl" \
    >"$KV_DIR/restart-all.json" || rc=1
  echo "restart-all, before: $(verdict "$KV_DIR/restart-all.json")"
  for i in 1 2 3; do $R kill "n$i"; done
  began=$SECONDS
  for i in 1 2 3; do node_up start "n$i" & done
  wait
  kv warm --addrs "$(addrs)" --within 90s >/dev/null
  answered=$((SECONDS - began))
  echo "restart-all: every shard answers $answered s after the starts"
  kv check --addrs "$(addrs)" --verify "$KV_DIR/restart-all.keys" >"$KV_DIR/restart-all-verify.json" || rc=1
  echo "restart-all, read back: $(cat "$KV_DIR/restart-all-verify.json")"
  recoveries | tee "$KV_DIR/restart-all-recoveries.jsonl"
  digests restart-all || rc=1
  $R down
  record restart-all $((rc == 0)) "{\"check\": $(verdict "$KV_DIR/restart-all.json"), \"verify\": $(cat "$KV_DIR/restart-all-verify.json"), \"answered_s\": $answered, \"recoveries\": $(jq -sc . "$KV_DIR/restart-all-recoveries.jsonl")}"
}

quorum_loss() {
  local rc=0 t n1 down ok kinds
  fresh durable
  t=$(times)
  kv check --addrs "$(addrs)" --secs 20 --save "$KV_DIR/quorum-loss.keys" --out "$KV_DIR/quorum-loss.jsonl" \
    >"$KV_DIR/quorum-loss.json" || rc=1
  echo "quorum-loss, before: $(verdict "$KV_DIR/quorum-loss.json")"
  n1=$(addrs | cut -d, -f1)
  $R kill n2
  $R kill n3
  # Until its lease lapses, an owner on n1 may still take a write and fail to commit it (outcome unknown).
  sleep $(($(jq .T_ms <<<"$t") / 1000 + 1))
  kv check --addrs "$n1" --prefix kvq --writers 4 --keys 1 --readers 2 --secs 10 --timeout 3s \
    --out "$KV_DIR/quorum-loss-down.jsonl" >"$KV_DIR/quorum-loss-down.json" || true
  down=$(jq -c '{writes_ok, reads_ok, probes_ok, writes_failed, reads_failed, probes_failed, errors}' "$KV_DIR/quorum-loss-down.json")
  echo "quorum-loss, two down: $down"
  ok=$(jq '.writes_ok + .reads_ok + .probes_ok' <<<"$down")
  kinds=$(jq -r '.errors | keys | map(select(startswith("CLUSTERDOWN") | not)) | join("; ")' <<<"$down")
  [ "$ok" = 0 ] || { echo "quorum-loss: $ok acknowledged with two of three down"; rc=1; }
  [ -z "$kinds" ] || { echo "quorum-loss: answers other than CLUSTERDOWN: $kinds"; rc=1; }
  node_up start n2 &
  node_up start n3 &
  wait
  kv warm --addrs "$(addrs)" --within 90s >/dev/null
  kv check --addrs "$(addrs)" --verify "$KV_DIR/quorum-loss.keys" >"$KV_DIR/quorum-loss-verify.json" || rc=1
  echo "quorum-loss, read back: $(cat "$KV_DIR/quorum-loss-verify.json")"
  kv check --addrs "$(addrs)" --secs 10 --prefix kvh --out "$KV_DIR/quorum-loss-after.jsonl" \
    >"$KV_DIR/quorum-loss-after.json" || rc=1
  echo "quorum-loss, after the heal: $(verdict "$KV_DIR/quorum-loss-after.json")"
  digests quorum-loss || rc=1
  $R down
  record quorum-loss $((rc == 0)) "{\"check\": $(verdict "$KV_DIR/quorum-loss.json"), \"down\": $down, \"verify\": $(cat "$KV_DIR/quorum-loss-verify.json"), \"after\": $(verdict "$KV_DIR/quorum-loss-after.json"), \"lease\": $t}"
}

# soak [minutes]: see the header. Samples each node's RSS, WAL and snapshots every 30 s into soak.tsv.
soak() {
  local minutes=${1:-60} rc=0 ycsb=$TOOLS/bin/go-ycsb wl=$TOOLS/src/go-ycsb/workloads/workloada
  [ -x "$ycsb" ] || { echo "soak needs go-ycsb: examples/kv/bench.sh setup" >&2; return 1; }
  fresh durable
  local redis_addrs props ops rate end i
  redis_addrs=$(addrs | tr , ';')
  props=(-p "redis.addr=$redis_addrs" -p redis.mode=cluster -p redis.datatype=string -p redis.max_retries=0
    -p threadcount=64 -p "recordcount=${SOAK_RECORDS:-1000000}" -p fieldcount=10 -p fieldlength=100
    -p writeallfields=true -p readallfields=true)
  "$ycsb" load redis -P "$wl" "${props[@]}" >"$KV_DIR/soak-load.log" 2>&1
  "$ycsb" run redis -P "$wl" "${props[@]}" -p operationcount=1000000000 -p maxexecutiontime=20 \
    >"$KV_DIR/soak-sat.log" 2>&1
  ops=$(awk '/^TOTAL/ { for (i = 1; i <= NF; i++) if ($i == "OPS:") v = $(i + 1) } END { sub(",", "", v); print int(v) }' "$KV_DIR/soak-sat.log")
  rate=${SOAK_RATE:-$((ops / 2))}
  echo "soak: workload A saturates at $ops ops/s; $minutes min at $rate ops/s"
  [ "$rate" -gt 0 ] || { echo "soak: go-ycsb gave no rate: $KV_DIR/soak-sat.log"; $R down; return 1; }
  "$ycsb" run redis -P "$wl" "${props[@]}" -p operationcount=1000000000 -p maxexecutiontime=$((minutes * 60)) \
    -p target="$rate" >"$KV_DIR/soak-run.log" 2>&1 &
  local load=$!
  printf 'secs\tnode\trss_kb\twal_bytes\tsnapshot_bytes\n' >"$KV_DIR/soak.tsv"
  (
    start=$SECONDS
    while kill -0 "$load" 2>/dev/null; do
      for n in 1 2 3; do
        pid=$(cat "$KV_DIR/n$n.pid" 2>/dev/null) || continue
        rss=$(awk '/^VmRSS/ { print $2 }' "/proc/$pid/status" 2>/dev/null) || continue
        info=$(kv status --addrs "$(addrs | cut -d, -f$n)" | jq -r '.nodes[0].info | "\(.wal_bytes // "")\t\(.snapshot_bytes // "")"')
        printf '%s\tn%s\t%s\t%s\n' $((SECONDS - start)) "$n" "$rss" "$info" >>"$KV_DIR/soak.tsv"
      done
      sleep 30
    done
  ) &
  local sampler=$!
  end=$((SECONDS + minutes * 60 - 150))
  i=0
  : >"$KV_DIR/soak-checks.jsonl"
  while [ $((SECONDS + 120)) -le $end ]; do
    i=$((i + 1))
    local secs=$((120 + RANDOM % 181)) at victim how
    [ $((SECONDS + secs)) -le $end ] || secs=$((end - SECONDS))
    at=$((30 + RANDOM % (secs - 90)))
    victim=n$((1 + RANDOM % 3))
    how=$([ $((RANDOM % 2)) = 0 ] && echo kill || echo stop)
    kv check --addrs "$(addrs)" --secs "$secs" --writers 16 --readers 4 --probe 50ms --prefix "kvs$i" \
      --out "$KV_DIR/soak-$i.jsonl" >"$KV_DIR/soak-$i.json" &
    local check=$!
    sleep "$at"
    fault "$how" "$victim"
    local crc=0
    wait "$check" || crc=1
    rc=$((rc | crc))
    jq -c --arg how "$how" --arg victim "$victim" --argjson at "$at" --argjson i "$i" \
      '{i: $i, fault: $how, victim: $victim, at_s: $at, summary, lost, stale, bad, unread, writes_ok, writes_failed, longest_window_ms}' \
      "$KV_DIR/soak-$i.json" | tee -a "$KV_DIR/soak-checks.jsonl"
  done
  wait "$load" || true
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  digests soak || rc=1
  local judged
  judged=$(python3 - "$KV_DIR/soak.tsv" <<'PY'
import csv, json, sys
rows = [r for r in csv.DictReader(open(sys.argv[1]), delimiter="\t") if r["rss_kb"] and r["wal_bytes"]]
seg, floor = 64 << 20, 256 << 20
# The target, and four segments: the one being written, the one a snapshot round has yet to free, and two more for a
# restarted node, whose Installs take the snapshot slots before the rounds that trim (an 8-minute run went 20 MiB past
# two segments once, right after a restart, and came back).
over = [r for r in rows if int(r["wal_bytes"]) > max(floor, int(r["snapshot_bytes"] or 0)) + 4 * seg]
# RSS per node, by its own samples: flat when the last third's most is within 20 % of the middle third's. Judged per
# node, as a sum over the nodes would hide one node's rise behind another's restart.
by = {}
for r in rows:
    by.setdefault(r["node"], []).append(int(r["rss_kb"]) >> 10)
nodes = {}
for node, rss in sorted(by.items()):
    third = len(rss) // 3
    if third:
        mid, last = max(rss[third:2 * third]), max(rss[2 * third:])
        nodes[node] = {"mid_mib": mid, "last_mib": last, "ratio": round(last / mid, 3)}
print(json.dumps({"samples": len(rows), "wal_most_mib": max(int(r["wal_bytes"]) for r in rows) >> 20,
                  "wal_over": len(over), "rss": nodes,
                  "rss_flat": len(nodes) == 3 and all(n["ratio"] <= 1.2 for n in nodes.values()),
                  "wal_bounded": not over and bool(rows)}))
PY
)
  echo "soak: $judged"
  [ "$(jq '.rss_flat and .wal_bounded' <<<"$judged")" = true ] || rc=1
  echo "soak: $(grep -c PASS "$KV_DIR/soak-checks.jsonl") of $i checks passed; go-ycsb: $(grep '^TOTAL\|_ERROR' "$KV_DIR/soak-run.log" | tail -n 3 | tr '\n' ' ')"
  $R down
  record soak $((rc == 0)) "{\"minutes\": $minutes, \"rate\": $rate, \"saturation\": $ops, \"checks\": $(jq -sc . "$KV_DIR/soak-checks.jsonl"), \"judged\": $judged}"
}

# The nodes' cluster and seed ports, from run.sh's layout, comma-separated: of node i, or of all three.
ports() {
  local p=${KV_PORT:-6379} i out=""
  for i in ${1:-1 2 3}; do out+="${out:+,}$((p + 99 + i)),$((p + 199 + i))"; done
  echo "$out"
}

# Node i dials the others from 127.0.0.(10 + i): its cgroup tells its packets apart, and its address the packets
# back to it, which go through OUTPUT as another process's. Set before the nodes start, so every connection has it.
partitionable() {
  local i
  export KV_CGROUP=kv-fault-$$
  sudo -n iptables -t nat -N "$KV_CGROUP" || return 1
  sudo -n iptables -t nat -I POSTROUTING -o lo -j "$KV_CGROUP" || return 1
  for i in 1 2 3; do
    sudo -n mkdir -p "/sys/fs/cgroup/$KV_CGROUP/n$i" || return 1
    sudo -n iptables -t nat -A "$KV_CGROUP" -p tcp -m cgroup --path "$KV_CGROUP/n$i" -m multiport --dports "$(ports)" \
      -j SNAT --to-source "127.0.0.$((10 + i))" || return 1
  done
}

# A phase runs without errexit (it is called with ||), so each rule that fails is said: half a cut must not pass.
isolate() { # i
  sudo -n iptables -N "$KV_CGROUP" &&
    sudo -n iptables -A "$KV_CGROUP" -m conntrack --ctrepldst "127.0.0.$((10 + $1))" -j DROP &&
    sudo -n iptables -A "$KV_CGROUP" -p tcp -m multiport --dports "$(ports "$1")" -j DROP &&
    sudo -n iptables -A "$KV_CGROUP" -p tcp -m multiport --sports "$(ports "$1")" -j DROP &&
    sudo -n iptables -I OUTPUT -o lo -j "$KV_CGROUP"
}

heal() {
  sudo -n iptables -D OUTPUT -o lo -j "$KV_CGROUP" 2>/dev/null || true
  sudo -n iptables -F "$KV_CGROUP" 2>/dev/null || true
  sudo -n iptables -X "$KV_CGROUP" 2>/dev/null || true
}

# Every rule and cgroup that partitionable and isolate made. The nodes have ended: run.sh down waits for them.
unpartition() {
  local i
  heal
  sudo -n iptables -t nat -D POSTROUTING -o lo -j "$KV_CGROUP" 2>/dev/null || true
  sudo -n iptables -t nat -F "$KV_CGROUP" 2>/dev/null || true
  sudo -n iptables -t nat -X "$KV_CGROUP" 2>/dev/null || true
  for i in 1 2 3; do sudo -n rmdir "/sys/fs/cgroup/$KV_CGROUP/n$i" 2>/dev/null || true; done
  sudo -n rmdir "/sys/fs/cgroup/$KV_CGROUP" 2>/dev/null || true
  unset KV_CGROUP
}

partition() {
  local rc=0 t bound victim v w majority dropped
  if [ "$(uname)" != Linux ] || ! sudo -n iptables -S OUTPUT >/dev/null 2>&1; then
    echo "partition: needs Linux, iptables and sudo without a password" >&2
    return 1
  fi
  partitionable || { echo "partition: the cgroups or the NAT rules could not be made" >&2; unpartition; return 1; }
  fresh durable
  t=$(times)
  bound=$(($(jq .T_ms <<<"$t") + 3000))
  victim=$(kv status --owner kvc:w0:0 --addrs "$(addrs)" | jq -r .owner)
  v=${victim#n}
  majority=$(addrs | tr , '\n' | sed "${v}d" | paste -sd, -)
  kv check --addrs "$majority" --secs "$SECS" --out "$KV_DIR/partition.jsonl" >"$KV_DIR/partition.json" &
  local check=$!
  kv check --addrs "$(addrs | cut -d, -f"$v")" --secs "$SECS" --prefix kvv --writers 8 --readers 4 --keys 4 \
    --out "$KV_DIR/partition-cut.jsonl" >"$KV_DIR/partition-cut.json" &
  local alone=$!
  sleep 10
  isolate "$v" || { echo "partition: a DROP rule could not be made"; rc=1; }
  sleep 15
  dropped=$(sudo -n iptables -L "$KV_CGROUP" -v -n -x | awk 'NR > 2 { n += $1 } END { print n + 0 }')
  heal
  echo "partition: $victim cut off from 10 s to 25 s, $dropped packets dropped"
  [ "$dropped" -gt 0 ] || { echo "partition: no packet dropped, so nothing was cut"; rc=1; }
  wait "$check" || rc=1
  wait "$alone" || rc=1
  echo "partition $victim, through the others: $(verdict "$KV_DIR/partition.json")"
  echo "partition $victim, through it alone: $(verdict "$KV_DIR/partition-cut.json")"
  w=$(windows "$KV_DIR/partition.jsonl" 25000)
  echo "$w (bound $bound ms)"
  [ "$(within "$w" "$bound")" = 1 ] || { echo "partition: a shard took longer than T + 3 s"; rc=1; }
  # The cut node's shards must have moved: a cut it could still renew its leases through would pass the rest.
  [ "$(jq .away.n <<<"$w")" -gt 0 ] || { echo "partition: no shard moved away from $victim, so the cut did not take"; rc=1; }
  digests partition || rc=1
  $R down
  unpartition
  record partition $((rc == 0)) "{\"victim\": \"$victim\", \"check\": $(verdict "$KV_DIR/partition.json"), \"cut_off\": $(verdict "$KV_DIR/partition-cut.json"), \"dropped\": $dropped, \"windows\": $w, \"bound_ms\": $bound, \"lease\": $t}"
}

fail=0
phases=("$@")
[ ${#phases[@]} -gt 0 ] || phases=(memory-control)
i=0
while [ $i -lt ${#phases[@]} ]; do
  phase=${phases[$i]}
  i=$((i + 1))
  case $phase in
    memory-control) memory_control || fail=1 ;;
    kill) one_node kill kill || fail=1 ;;
    stop) one_node stop stop || fail=1 ;;
    stop-lease)
      args=${KV_ARGS:-}
      export KV_ARGS="$args --reads lease"
      one_node stop-lease stop || fail=1
      export KV_ARGS=$args
      ;;
    follower) follower || fail=1 ;;
    restart-all) restart_all || fail=1 ;;
    quorum-loss) quorum_loss || fail=1 ;;
    partition) partition || fail=1 ;;
    all) phases=("${phases[@]:0:$i}" memory-control kill stop stop-lease follower restart-all quorum-loss "${phases[@]:$i}") ;;
    soak)
      minutes=60
      if [[ ${phases[$i]:-} =~ ^[0-9]+$ ]]; then minutes=${phases[$i]}; i=$((i + 1)); fi
      soak "$minutes" || fail=1
      ;;
    *) echo "no phase $phase" >&2; exit 2 ;;
  esac
done
exit $fail
