#!/usr/bin/env bash
# kv's benchmark: kv against Redis and etcd under go-ycsb, each target at the same durability in its own table.
#
#   bench.sh setup              pinned Redis, etcd, Go, go-ycsb and fio into $KV_TOOLS (default ~/axon/kv-tools),
#                               each checked against bench/versions.txt; nothing system-wide, no root
#   bench.sh quick [target..]   one short round: 15 s phases, 200k records; results/<date>-<host>-quick/
#   bench.sh full [target..]    3 rounds, 60 s phases, 1M records, the target order turned each round; ROUND_FROM=n
#                               and ROUND_TO=m run rounds n to m in the same RUN directory, LARGE=0 and PARITY=0
#                               leave those out, so one run can go in parts
#   bench.sh summary <dir>      results.csv and summary.md again from a run's raw/ (and fault/faults.jsonl)
#   bench.sh check-summary <dir>  the same in memory: exits 1 unless the files there are that, byte for byte
#   bench.sh golden             tests/conformance.golden again, from the pinned Redis
#   bench.sh smoke              redis-cli and redis-benchmark --cluster -r against kv-memory-1 and kv-memory-3
#   bench.sh walsoak            kv-durable-1 under workload A for 10 minutes: the WAL stays bounded
#
# Targets: basic stub etcd-check redis redis-io-threads redis-cluster-3 redis-aof etcd-1 etcd-3 kv-memory-1 kv-memory-3
# kv-durable-1 kv-durable-3 kv-durable-3-lease; the large-value table (1, 5, 10 MB) runs for the server targets unless
# LARGE=0. redis-io-threads is redis with 4 I/O threads that also read (--io-threads 4 --io-threads-do-reads yes).
# Env: KV_TOOLS, KV_WORK (data directories; default /data/ubuntu/kv-work when it exists, the RAID0 device on
# build-1), KV_BIN (a built kv), SECS, RECORDS, ROUNDS, THREADS, KV_COMMIT (the examples commit, for a copy with no
# .git), KV_CONDITIONS (what the box ran besides, for summary.md's first line; default: the load at the start). The
# fault run's faults.jsonl, copied to <run>/fault/, gives summary.md its Faults table.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
TOOLS=${KV_TOOLS:-$HOME/axon/kv-tools}
BIN=$TOOLS/bin
VERSIONS=$HERE/bench/versions.txt
if [[ -z ${KV_WORK:-} ]]; then
  if [[ -w /data/ubuntu ]]; then KV_WORK=/data/ubuntu/kv-work; else KV_WORK=$HOME/axon/kv-work; fi
fi
WORK=$KV_WORK
export PATH=$BIN:$TOOLS/go/bin:$PATH
export GOPATH=$TOOLS/gopath GOMODCACHE=$TOOLS/gopath/mod GOCACHE=$TOOLS/gocache GOTOOLCHAIN=local

say() { printf '%s %s\n' "$(date +%T)" "$*" >&2; }
die() { say "error: $*"; exit 1; }
ver() { awk -v n="$1" '$1 == n { print $2, $3, $4 }' "$VERSIONS"; }

fetch() { # url file sha256
  local file=$TOOLS/dl/$2
  if [[ ! -f $file ]] || ! echo "$3  $file" | sha256sum -c --status; then
    curl -sfL -o "$file.part" "$1" || die "download $1"
    mv "$file.part" "$file"
  fi
  echo "$3  $file" | sha256sum -c --status || { rm -f "$file"; die "checksum of $2 is not $3"; }
}

setup() {
  mkdir -p "$TOOLS"/{dl,bin,src}
  local v url sha
  read -r v url sha < <(ver redis)
  fetch "$url" "redis-$v.tar.gz" "$sha"
  if [[ ! -f $BIN/.redis-$v ]]; then
    say "building redis $v"
    rm -rf "$TOOLS/src/redis-$v" && tar -xzf "$TOOLS/dl/redis-$v.tar.gz" -C "$TOOLS/src"
    make -C "$TOOLS/src/redis-$v" -j8 >"$TOOLS/src/redis-build.log" 2>&1 || die "redis build: $TOOLS/src/redis-build.log"
    cp "$TOOLS/src/redis-$v/src/"{redis-server,redis-cli,redis-benchmark} "$BIN/" && touch "$BIN/.redis-$v"
  fi
  read -r v url sha < <(ver etcd)
  fetch "$url" "etcd-$v.tar.gz" "$sha"
  if [[ ! -f $BIN/.etcd-$v ]]; then
    rm -rf "$TOOLS/src/etcd-$v-linux-amd64" && tar -xzf "$TOOLS/dl/etcd-$v.tar.gz" -C "$TOOLS/src"
    cp "$TOOLS/src/etcd-$v-linux-amd64/"{etcd,etcdctl} "$BIN/" && touch "$BIN/.etcd-$v"
  fi
  read -r v url sha < <(ver go)
  fetch "$url" "go$v.tar.gz" "$sha"
  if [[ ! -f $TOOLS/.go-$v ]]; then
    rm -rf "$TOOLS/go" && tar -xzf "$TOOLS/dl/go$v.tar.gz" -C "$TOOLS" && touch "$TOOLS/.go-$v"
  fi
  local commit stamp
  read -r v url commit < <(ver go-ycsb)
  stamp=$BIN/.go-ycsb-$commit-$(sha256sum "$HERE/bench/go-ycsb.patch" | cut -c1-12)
  if [[ ! -f $stamp ]]; then
    say "building go-ycsb $v"
    [[ -d $TOOLS/src/go-ycsb ]] || git clone -q "$url" "$TOOLS/src/go-ycsb"
    git -C "$TOOLS/src/go-ycsb" fetch -q origin
    git -C "$TOOLS/src/go-ycsb" checkout -q -f "$commit"
    git -C "$TOOLS/src/go-ycsb" apply "$HERE/bench/go-ycsb.patch"
    (cd "$TOOLS/src/go-ycsb" && go build -o "$BIN/go-ycsb" ./cmd/go-ycsb) >"$TOOLS/src/go-ycsb-build.log" 2>&1 \
      || die "go-ycsb build: $TOOLS/src/go-ycsb-build.log"
    touch "$stamp"
  fi
  read -r v url sha < <(ver fio)
  fetch "$url" "fio-$v.tar.gz" "$sha"
  if [[ ! -f $BIN/.fio-$v ]]; then
    say "building fio $v"
    rm -rf "$TOOLS/src/fio-fio-$v" && tar -xzf "$TOOLS/dl/fio-$v.tar.gz" -C "$TOOLS/src"
    (cd "$TOOLS/src/fio-fio-$v" && ./configure && make -j8) >"$TOOLS/src/fio-build.log" 2>&1 || die "fio build: $TOOLS/src/fio-build.log"
    cp "$TOOLS/src/fio-fio-$v/fio" "$BIN/" && touch "$BIN/.fio-$v"
  fi
  redis-server --version
  etcd --version | head -1
  go version
  fio --version
  echo "go-ycsb $commit"
  echo "setup ok: $TOOLS"
}

# ---------------------------------------------------------------- machine

cpusets() {
  # CPUs grouped by their L3: on a 9900X the two CCDs. Servers on the first, go-ycsb on the second.
  mapfile -t groups < <(lscpu -p=CPU,CACHE | grep -v '^#' |
    awk -F, '{ split($2, c, ":"); g[c[4]] = g[c[4]] (g[c[4]] == "" ? "" : ",") $1 } END { for (k in g) print k, g[k] }' |
    sort -n | cut -d' ' -f2)
  if (( ${#groups[@]} >= 2 )); then
    SERVER_CPUS=${groups[0]} CLIENT_CPUS=${groups[1]} SPLIT="by L3 cache (one CCD each)"
  else
    local n; n=$(nproc)
    SERVER_CPUS=0-$((n / 2 - 1)) CLIENT_CPUS=$((n / 2))-$((n - 1)) SPLIT="one L3: the CPUs cut in half"
  fi
  SERVER_THREADS=$(taskset -c "$SERVER_CPUS" nproc)
}

cpu_s() { # pids..: user + system seconds of each, summed
  local total=0 pid t
  for pid in "$@"; do
    t=$(awk '{ sub(/.*\) /, ""); print $12 + $13 }' "/proc/$pid/stat" 2>/dev/null || echo 0)
    total=$((total + t))
  done
  awk -v t="$total" -v hz="$(getconf CLK_TCK)" 'BEGIN { printf "%.2f", t / hz }'
}

fio_line() { # out.json: a 4 KiB write with an fsync after each, on the work device
  mkdir -p "$WORK/fio"
  fio --name=fsync --directory="$WORK/fio" --size=64m --bs=4k --rw=write --fsync=1 --ioengine=sync \
    --runtime=5 --time_based --output-format=json >"$1" 2>/dev/null || echo '{}' >"$1"
  rm -rf "$WORK/fio"
}

# The box's state for summary.md's first line, unless KV_CONDITIONS gives it: the load at the start. Each part's line
# in env.txt has the load when it began; work that comes later only KV_CONDITIONS can name.
conditions() { # load: the 1-minute load before the run's own builds
  if awk -v l="$1" 'BEGIN { exit !(l < 1) }'; then
    echo "$(hostname -s) was quiet at the start, load $1"
  else
    echo "$(hostname -s) had other work at the start, load $1"
  fi
}

env_txt() {
  local f=$RUN/env.txt patch commit
  # A copy synced to a box has no .git: the caller names the commit in KV_COMMIT.
  commit=${KV_COMMIT:-$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)}
  {
    echo "date: $(date -Is)"
    echo "host: $(hostname -s)"
    echo "kind: $KIND (indicative: ${KV_CONDITIONS:-$(conditions "$START_LOAD")})"
    echo "kernel: $(uname -r)"
    grep PRETTY_NAME /etc/os-release
    echo "cpu: $(lscpu | sed -n 's/^Model name: *//p')"
    echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown) (the design asks for performance; changing it needs root, so it is recorded, not set)"
    echo "load at start: $(cat /proc/loadavg)"
    echo "memory: $(free -g | awk '/Mem:/ { print $2 " GB" }')"
    echo "server cpus: $SERVER_CPUS ($SERVER_THREADS threads), client cpus: $CLIENT_CPUS, split $SPLIT"
    echo "work dir: $WORK on $(findmnt -no SOURCE,FSTYPE,OPTIONS -T "$WORK")"
    echo "raid: $(grep -A1 "^$(findmnt -no SOURCE -T "$WORK" | sed 's|/dev/||') " /proc/mdstat | tr -s ' \n' ' ')"
    for d in /sys/block/nvme*n1; do echo "nvme: $(basename "$d") $(cat "$d/device/model" 2>/dev/null | xargs)"; done
    echo "nvme-cli: $(command -v nvme || echo 'not installed: temperature and power-loss protection not recorded')"
    echo "fstrim: not run (needs root)"
    echo "examples commit: $commit"
    # The rev Cargo.toml pins. A box builds it through a path [patch] in .cargo/config.toml, which leaves no rev in the
    # lock file, so the path is named too.
    patch=$(grep -m1 -o 'path = "[^"]*' "$REPO/.cargo/config.toml" 2>/dev/null | cut -d'"' -f2)
    echo "axon: $(grep -m1 -o 'axon.git", rev = "[0-9a-f]*' "$REPO/Cargo.toml" | grep -o '[0-9a-f]*$')${patch:+, built from the [patch] path $patch}"
    echo "kv: release profile, RUSTFLAGS=${RUSTFLAGS:-none}, $KV_BIN"
    echo "redis: $(redis-server --version)"
    echo "etcd: $(etcd --version | head -1), GOMAXPROCS = server threads / processes"
    echo "go: $(go version)"
    echo "go-ycsb: $(ver go-ycsb | cut -d' ' -f3) with bench/go-ycsb.patch"
    echo "fio: $(fio --version)"
    echo "client: go-ycsb, $THREADS threads, recordcount $RECORDS, fieldcount 10, fieldlength 100, writeallfields, readallfields, redis.datatype string, redis.max_retries 0"
    echo "phases: ${SECS}s each; rounds: $ROUNDS"
  } >"$f.new"
  # A run in parts keeps its first env.txt and adds a line for each part.
  [[ -f $f ]] || mv "$f.new" "$f"
  rm -f "$f.new"
  echo "part: $(date -Is) rounds ${ROUND_FROM:-1} to ${ROUND_TO:-$ROUNDS} of ${targets[*]}, large ${LARGE:-1}, parity ${PARITY:-1}, load $(cut -d' ' -f1-3 /proc/loadavg), examples commit $commit" >>"$f"
}

# ---------------------------------------------------------------- targets

PIDS=()
PORTS=() # RESP or etcd client ports of the running target
STRACE=""

launch() { # name cmd..: one server process on the server cpus, under strace when STRACE is set
  local name=$1; shift
  if [[ -n $STRACE ]]; then
    taskset -c "$SERVER_CPUS" strace -f --seccomp-bpf -c -e trace=fsync,fdatasync,sync_file_range \
      -o "$RAW/strace-$TARGET-$name.txt" "$@" >"$RAW/$TARGET-$name.log" 2>&1 &
    local tracer=$! child=""
    for _ in $(seq 100); do child=$(pgrep -P "$tracer" | head -1) && [[ -n $child ]] && break; sleep 0.1; done
    PIDS+=("${child:-$tracer}")
  else
    taskset -c "$SERVER_CPUS" "$@" >"$RAW/$TARGET-$name.log" 2>&1 &
    PIDS+=("$!")
  fi
}

wait_for() { # what cmd..: until cmd succeeds, at most 90 s
  local what=$1; shift
  for _ in $(seq 900); do "$@" >/dev/null 2>&1 && return 0; sleep 0.1; done
  say "$what did not come up"
  return 1
}

stop_target() {
  local pid
  for pid in "${PIDS[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]}"; do
    for _ in $(seq 150); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -KILL "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  PIDS=() PORTS=()
}

kv_bin() {
  if [[ -z ${KV_BIN:-} ]]; then
    (cd "$REPO" && cargo build --release -p kv -q) || die "kv build"
    KV_BIN=$(cd "$REPO" && cargo metadata --format-version 1 --no-deps |
      python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')/release/kv
  fi
  [[ -x $KV_BIN ]] || die "no kv binary at $KV_BIN"
}

start_redis() { # nodes extra-args..
  local n=$1; shift
  local i port
  for i in $(seq "$n"); do
    port=$((5378 + i)) && mkdir -p "$WORK/$TARGET/r$i"
    local cluster=()
    # The bus port too, which is the port + 10000 unless set: above 10000 a passing connection can hold it.
    (( n > 1 )) && cluster=(--cluster-enabled yes --cluster-config-file nodes.conf --cluster-port $((5478 + i)))
    launch "r$i" redis-server --port "$port" --bind 127.0.0.1 --dir "$WORK/$TARGET/r$i" "${cluster[@]}" "$@"
    PORTS+=("$port")
    wait_for "redis $port" redis-cli -p "$port" ping || return 1
  done
  if (( n > 1 )); then
    local addrs=()
    for port in "${PORTS[@]}"; do addrs+=("127.0.0.1:$port"); done
    redis-cli --cluster create "${addrs[@]}" --cluster-replicas 0 --cluster-yes >"$RAW/$TARGET-create.log" 2>&1 ||
      return 1
    # Each node, not the first only: until every one has the slot map, a write to it is CLUSTERDOWN.
    for port in "${PORTS[@]}"; do
      wait_for "redis cluster on $port" bash -c "redis-cli -p $port cluster info | grep -q cluster_state:ok" || return 1
    done
  fi
}

start_etcd() { # nodes
  local n=$1 i cluster="" compaction=(--auto-compaction-mode=revision --auto-compaction-retention=10000) extra=()
  # Large values: revision mode compacts every 5 min, and 15 s of 5 MB writes pass the 8 GiB quota before it does.
  [[ ${LARGE_RUN:-0} == 1 ]] &&
    compaction=(--auto-compaction-mode=periodic --auto-compaction-retention=5s) extra=(--max-request-bytes=33554432)
  for i in $(seq "$n"); do cluster+="${cluster:+,}e$i=http://127.0.0.1:$((2380 + 10 * i))"; done
  for i in $(seq "$n"); do
    local client=$((2379 + 10 * i)) peer=$((2380 + 10 * i))
    mkdir -p "$WORK/$TARGET/e$i"
    GOMAXPROCS=$((SERVER_THREADS / n)) launch "e$i" etcd --name "e$i" --data-dir "$WORK/$TARGET/e$i" \
      --listen-client-urls "http://127.0.0.1:$client" --advertise-client-urls "http://127.0.0.1:$client" \
      --listen-peer-urls "http://127.0.0.1:$peer" --initial-advertise-peer-urls "http://127.0.0.1:$peer" \
      --initial-cluster "$cluster" --initial-cluster-state new \
      "${compaction[@]}" --quota-backend-bytes=8589934592 "${extra[@]}"
    PORTS+=("$client")
  done
  for client in "${PORTS[@]}"; do wait_for "etcd $client" curl -sf "http://127.0.0.1:$client/health" || return 1; done
}

start_kv() { # nodes mode [node flags..]
  local n=$1 mode=$2 i ids="" seeds=""
  shift 2
  KV_SECRET=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
  export KV_SECRET
  for i in $(seq "$n"); do ids+="${ids:+,}n$i" seeds+="${seeds:+,}n$i-seed@127.0.0.1:$((6590 + i))"; done
  for i in $(seq "$n"); do
    rm -rf "$WORK/$TARGET/n$i" && mkdir -p "$WORK/$TARGET/n$i"
    launch "n$i" "$KV_BIN" node --plaintext --id "n$i" --resp "127.0.0.1:$((6390 + i))" --bind "127.0.0.1:$((6490 + i))" \
      --seed-bind "127.0.0.1:$((6590 + i))" --seeds "$seeds" --nodes "$ids" --dir "$WORK/$TARGET/n$i" \
      --health "127.0.0.1:$((6690 + i))" --workers $((SERVER_THREADS / n)) --mode "$mode" "$@"
    PORTS+=("$((6390 + i))")
  done
  for i in $(seq "$n"); do wait_for "kv n$i" curl -sf "http://127.0.0.1:$((6690 + i))/readyz" || return 1; done
  local addrs=""
  for port in "${PORTS[@]}"; do addrs+="${addrs:+,}127.0.0.1:$port"; done
  "$KV_BIN" warm --addrs "$addrs" --within 90s >"$RAW/$TARGET-warm.json" || { say "kv warm failed"; return 1; }
  # Shards per node at the round's start, for env.txt's balance line.
  "$KV_BIN" status --addrs "$addrs" | jq -c '[.nodes[] | {addr, shards_here: .info.shards_here}]' \
    >"$RAW/$TARGET-r${ROUND:-1}-shards.json" || true
}

# Fixed ports sit below 10000 where they can: build-1's ip_local_port_range starts at 10000, so a passing
# connection can hold a port above it, and a target that did not come up is started once more.
start_target() {
  start_once && return 0
  stop_target
  say "starting $TARGET again"
  start_once || die "$TARGET did not come up twice"
}

start_once() {
  rm -rf "${WORK:?}/$TARGET"
  mkdir -p "$WORK/$TARGET"
  case $TARGET in
    basic) ;;
    stub)
      launch stub "$KV_BIN" stub --resp 127.0.0.1:6390
      PORTS+=(6390)
      wait_for stub redis-cli -p 6390 ping || return 1
      ;;
    redis) start_redis 1 --save "" --appendonly no ;;
    redis-io-threads) start_redis 1 --save "" --appendonly no --io-threads 4 --io-threads-do-reads yes ;;
    redis-cluster-3) start_redis 3 --save "" --appendonly no ;;
    redis-aof) start_redis 1 --save "" --appendonly yes --appendfsync always ;;
    etcd-1 | etcd-check) start_etcd 1 ;;
    etcd-3) start_etcd 3 ;;
    kv-memory-1) start_kv 1 memory ;;
    kv-memory-3) start_kv 3 memory ;;
    kv-durable-1) start_kv 1 durable ;;
    kv-durable-3) start_kv 3 durable ;;
    kv-durable-3-lease) start_kv 3 durable --reads lease ;;
    *) die "no target $TARGET" ;;
  esac
}

table_of() {
  case $1 in
    basic | stub | etcd-check) echo ceilings ;;
    redis | redis-io-threads | redis-cluster-3 | kv-memory-1 | kv-memory-3) echo M ;;
    redis-aof | etcd-1 | kv-durable-1) echo S1 ;;
    etcd-3 | kv-durable-3) echo R ;;
    kv-durable-3-lease) echo R-lease ;;
  esac
}

# The go-ycsb driver and its properties for the running target.
db_props() {
  local addrs="" port
  case $TARGET in
    basic) echo "basic -p basicdb.verbose=false" ;;
    etcd*)
      for port in "${PORTS[@]}"; do addrs+="${addrs:+,}127.0.0.1:$port"; done
      local serial=false
      [[ $TARGET == etcd-check ]] && serial=true
      echo "etcd -p etcd.endpoints=$addrs -p etcd.serializable_reads=$serial"
      ;;
    *)
      for port in "${PORTS[@]}"; do addrs+="${addrs:+;}127.0.0.1:$port"; done
      local mode=single
      (( ${#PORTS[@]} > 1 )) && mode=cluster
      echo "redis -p redis.addr=$addrs -p redis.mode=$mode -p redis.datatype=string -p redis.max_retries=0"
      ;;
  esac
}

# Server commands so far, for the commands per operation: INFO commandstats of each node, or etcd's counters.
cmds() { # out
  local port
  : >"$1"
  for port in "${PORTS[@]}"; do
    case $TARGET in
      etcd*) curl -s "http://127.0.0.1:$port/metrics" | grep -E '^etcd_mvcc_(put|range|delete)_total|^etcd_server_is_leader' | sed "s/^/$port /" >>"$1" ;;
      basic) ;;
      *) { redis-cli -p "$port" info clients; redis-cli -p "$port" info commandstats
           if [[ $TARGET == kv-* ]]; then redis-cli -p "$port" info kv; fi; } | tr -d '\r' | sed "s/^/$port /" >>"$1" ;;
    esac
  done
}

# The round and this invocation's start time go in each phase's id: a part run later into the same RUN counts N from
# 1 again, and its ids must not overwrite another part's files.
N=0 PART=$(date +%H%M%S)
phase() { # workload phase [rate] [extra -p..]: one go-ycsb run, recorded in raw/
  local workload=$1 ph=$2 rate=${3:-0}; shift 3 || shift $#
  N=$((N + 1))
  local id; id=$(printf 'r%s-%s-%03d-%s-%s-%s' "$ROUND" "$PART" "$N" "$TARGET" "$workload" "$ph")
  local mode=run secs=$SECS records=${PHASE_RECORDS:-$RECORDS}
  local args=(-p threadcount="$THREADS" -p recordcount="$records" -p fieldcount="${FIELDS:-10}"
    -p fieldlength="${FIELDLEN:-100}" -p writeallfields=true -p readallfields=true -p dropdata=false)
  if [[ $ph == load ]]; then
    mode=load secs=0
    args+=(-p insertcount="$records")
  else
    args+=(-p operationcount=1000000000 -p maxexecutiontime="$secs")
  fi
  (( rate > 0 )) && args+=(-p target="$rate")
  local cpu0 cpu1 rc=0
  cmds "$RAW/$id.cmds0"
  cpu0=$(cpu_s "${PIDS[@]}")
  # shellcheck disable=SC2046
  # A phase that runs on past its time is cut: the summary flags it from its Takes(s).
  timeout $((secs > 0 ? secs + 120 : 1800)) taskset -c "$CLIENT_CPUS" go-ycsb "$mode" $(db_props) -P "$TOOLS/src/go-ycsb/workloads/$workload" "${args[@]}" "$@" \
    >"$RAW/$id.log" 2>&1 || { rc=$?; say "go-ycsb exited $rc on $id"; }
  cpu1=$(cpu_s "${PIDS[@]}")
  cmds "$RAW/$id.cmds1"
  printf '{"id":"%s","round":%s,"target":"%s","table":"%s","nodes":%s,"workload":"%s","phase":"%s","rate_target":%s,"threads":%s,"records":%s,"secs":%s,"cpu_s":%s,"fio":"%s","value_bytes":%s,"load":"%s","rc":%s}\n' \
    "$id" "$ROUND" "$TARGET" "${TABLE:-$(table_of "$TARGET")}" "${#PORTS[@]}" "$workload" "$ph" "$rate" "$THREADS" \
    "$records" "$secs" "$(awk -v a="$cpu0" -v b="$cpu1" 'BEGIN { printf "%.2f", b - a }')" "$FIO" "${VALUE_BYTES:-0}" \
    "$(cut -d' ' -f1 /proc/loadavg)" "$rc" >>"$RAW/runs.jsonl"
  say "$id done"
}

saturation() { # the round's saturation phases on the running target
  case $TARGET in
    basic | stub) phase workloada sat; phase workloadc sat ;;
    etcd-check) phase workloada load 0; phase workloadc sat ;;
    *)
      phase workloada load 0
      phase workloada warmup 0 -p maxexecutiontime=$((SECS < 30 ? 10 : 30))
      for w in workloada workloadb workloadc workloadf workloadd; do phase $w sat; done
      ;;
  esac
}

sat_ops() { # target workload: the round's saturation ops/s
  python3 "$HERE/bench/summary.py" ops "$RUN" "$ROUND" "$1" "$2"
}

peers_of() { # the targets whose fixed-rate rows run at the same rate: one-node or three-node ones in M, all of S1
  case $1 in
    redis) echo kv-memory-1 redis-io-threads ;; kv-memory-1) echo redis redis-io-threads ;;
    redis-io-threads) echo redis kv-memory-1 ;;
    redis-cluster-3) echo kv-memory-3 ;; kv-memory-3) echo redis-cluster-3 ;;
    redis-aof | etcd-1 | kv-durable-1) echo redis-aof etcd-1 kv-durable-1 ;;
    etcd-3 | kv-durable-3 | kv-durable-3-lease) echo etcd-3 kv-durable-3 ;;
    *) echo "$1" ;;
  esac
}

fixed_rate() { # A and C at 50 % and 80 % of the slowest peer's saturation, the same rate for every peer
  local peer w ops slow pct
  phase workloada load 0
  phase workloada warmup 0 -p maxexecutiontime=5
  for w in workloada workloadc; do
    ops=$(sat_ops "$TARGET" $w)
    for peer in $(peers_of "$TARGET"); do ops+=",$(sat_ops "$peer" $w)"; done
    slow=$(python3 -c "o = [x for x in ($ops,) if x > 0]; print(int(min(o)) if o else 0)")
    (( slow > 0 )) || continue
    for pct in 50 80; do phase $w "fixed$pct" $((slow * pct / 100)); done
  done
}

# The values of the large-value table, in bytes as stored, and the go-ycsb field that gives them: go-ycsb stores
# its fields as JSON with the bytes in base64, so one field of 3/4 the size. About 512 MB of records, and at least
# one for each thread, as go-ycsb refuses a load with fewer.
large() {
  local size bytes records
  for size in 1 5 10; do
    bytes=$((size * 1000 * 1000)) records=$((512 * 1000 * 1000 / bytes))
    (( records >= THREADS )) || records=$THREADS
    VALUE_BYTES=$bytes FIELDS=1 FIELDLEN=$((bytes * 3 / 4)) PHASE_RECORDS=$records TABLE=large
    export VALUE_BYTES FIELDS FIELDLEN PHASE_RECORDS TABLE
    phase workloada load 0
    phase workloadb "large${size}mb" 0
    phase workloada "large${size}mb" 0
    unset VALUE_BYTES FIELDS FIELDLEN PHASE_RECORDS TABLE
  done
}

# A durable target under strace for a 20 s phase of A: the sync calls it made, against the writes acked.
parity() {
  STRACE=1
  start_target
  PHASE_RECORDS=20000 TABLE=parity phase workloada load 0
  PHASE_RECORDS=20000 TABLE=parity phase workloada parity 0 -p maxexecutiontime=20
  stop_target
  STRACE=""
}

run() { # kind targets..
  KIND=$1; shift
  START_LOAD=$(cut -d' ' -f1 /proc/loadavg) # before setup and kv_bin, which build
  case $KIND in
    quick) SECS=${SECS:-15} RECORDS=${RECORDS:-200000} ROUNDS=${ROUNDS:-1} ;;
    full) SECS=${SECS:-60} RECORDS=${RECORDS:-1000000} ROUNDS=${ROUNDS:-3} ;;
  esac
  THREADS=${THREADS:-64}
  local targets=("$@")
  (( ${#targets[@]} )) || targets=(basic stub etcd-check redis redis-io-threads kv-memory-1 redis-cluster-3 kv-memory-3
    redis-aof etcd-1 kv-durable-1 etcd-3 kv-durable-3 kv-durable-3-lease)
  setup >&2 # builds only what its stamp says is missing or stale, the go-ycsb patch included
  cpusets
  kv_bin
  RUN=${RUN:-$HERE/results/$(date +%F)-$(hostname -s)-$KIND}
  RAW=$RUN/raw
  mkdir -p "$RAW" "$WORK"
  env_txt
  trap 'stop_target' EXIT
  for ROUND in $(seq "${ROUND_FROM:-1}" "${ROUND_TO:-$ROUNDS}"); do
    local order=("${targets[@]:$(( (ROUND - 1) % ${#targets[@]} ))}" "${targets[@]:0:$(( (ROUND - 1) % ${#targets[@]} ))}")
    say "round $ROUND: ${order[*]}"
    for TARGET in "${order[@]}"; do
      FIO=$RAW/fio-r$ROUND-$TARGET.json
      fio_line "$FIO"
      start_target
      saturation
      stop_target
    done
    for TARGET in "${order[@]}"; do
      [[ $(table_of "$TARGET") == ceilings ]] && continue
      FIO=$RAW/fio-r$ROUND-$TARGET.json
      start_target
      fixed_rate
      stop_target
    done
  done
  ROUND=1
  if [[ ${LARGE:-1} == 1 ]]; then
    LARGE_RUN=1
    for TARGET in "${targets[@]}"; do
      [[ $(table_of "$TARGET") == ceilings ]] && continue
      FIO=$RAW/fio-r1-$TARGET.json
      start_target
      large
      stop_target
    done
    LARGE_RUN=0
  fi
  if [[ ${PARITY:-1} == 1 ]]; then
    for TARGET in "${targets[@]}"; do
      case $TARGET in
        redis-aof | etcd-1 | etcd-3 | kv-durable-1 | kv-durable-3) FIO=$RAW/fio-r1-$TARGET.json; parity ;;
      esac
    done
  fi
  trap - EXIT
  python3 "$HERE/bench/summary.py" build "$RUN"
  say "done: $RUN"
}

# The conformance goldens, from the pinned Redis in cluster mode with every slot on one node.
golden() {
  cpusets
  TARGET=golden RAW=$WORK/golden
  rm -rf "$RAW" && mkdir -p "$RAW/r"
  launch r redis-server --port 5399 --bind 127.0.0.1 --dir "$RAW/r" --cluster-enabled yes \
    --cluster-config-file nodes.conf --cluster-port 5499 --save "" --appendonly no
  trap 'stop_target' EXIT
  wait_for "redis 5399" redis-cli -p 5399 ping
  redis-cli -p 5399 cluster addslotsrange 0 16383 >/dev/null
  wait_for "redis cluster" bash -c "redis-cli -p 5399 cluster info | grep -q cluster_state:ok"
  (cd "$REPO" && KV_GOLDEN_REDIS=127.0.0.1:5399 cargo test -p kv --test conformance -q)
  say "recorded examples/kv/tests/conformance.golden"
}

# redis-cli and redis-benchmark against kv: one node, and three in cluster mode.
smoke() {
  cpusets
  kv_bin
  RUN=$WORK/smoke RAW=$WORK/smoke
  rm -rf "$RAW" && mkdir -p "$RAW"
  trap 'stop_target' EXIT
  local out=$RAW/smoke.txt fail=0
  for TARGET in kv-memory-1 kv-memory-3; do
    start_target
    # kv is in cluster mode on one node too, so MSET's keys must share a slot: redis-benchmark's --cluster tags them,
    # and it refuses a cluster of one node, so there MSET goes as a command of its own with a hash tag.
    local load=(--cluster -t set,get,mset)
    [[ $TARGET == kv-memory-1 ]] && load=(-t set,get)
    {
      echo "== $TARGET"
      redis-cli -c -p 6391 set smoke:a 1
      redis-cli -c -p 6391 get smoke:a
      redis-cli -c -p 6391 mset '{s}a' 1 '{s}b' 2
      redis-cli -c -p 6391 mget '{s}a' '{s}b'
      redis-cli -c -p 6391 dbsize
      redis-cli -c -p 6391 cluster info | head -2
      redis-benchmark -p 6391 "${load[@]}" -r 100000 -n 200000 -c 50 -q || echo "error: redis-benchmark exit $?"
      if [[ $TARGET == kv-memory-1 ]]; then
        redis-benchmark -p 6391 -r 100000 -n 200000 -c 50 -q mset '{t}a:__rand_int__' x '{t}b:__rand_int__' y ||
          echo "error: redis-benchmark exit $?"
      fi
    } >>"$out" 2>&1 || echo "error: exit $?" >>"$out"
    grep -qiE 'error|refused|warning' "$out" && fail=1
    stop_target
  done
  cat "$out"
  (( fail == 0 )) || die "the smoke run saw errors: $out"
  say "smoke ok"
}

# kv-durable-1 under workload A for SOAK_SECS (default 600 s) after a load of RECORDS (default 1M): every 10 s, the
# WAL's bytes, the snapshots' and the node's RSS from INFO kv and /proc. The WAL is bounded when no sample is past
# the larger of the 256 MiB floor and the snapshots, plus two 64 MiB segments: the one being written and the one a
# snapshot round has yet to free.
walsoak() {
  cpusets
  kv_bin
  TARGET=kv-durable-1 ROUND=1 THREADS=${THREADS:-64} RECORDS=${RECORDS:-1000000} SECS=${SOAK_SECS:-600} FIO=""
  RUN=${RUN:-$HERE/results/$(date +%F)-$(hostname -s)-walsoak}
  RAW=$RUN/raw
  mkdir -p "$RAW" "$WORK"
  trap 'stop_target' EXIT
  start_target
  phase workloada load 0
  local samples=$RAW/wal.tsv pid=${PIDS[0]}
  printf 'secs\twal_bytes\tsnapshot_bytes\twal_segments\tsnapshots\trss_kb\n' >"$samples"
  (
    start=$SECONDS
    while kill -0 "$pid" 2>/dev/null; do
      info=$(redis-cli -p 6391 info kv | tr -d '\r') || true
      field() { awk -F: -v k="$1" '$1 == k { print $2 }' <<<"$info"; }
      rss=$(awk '/^VmRSS/ { print $2 }' "/proc/$pid/status") || true
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' $((SECONDS - start)) "$(field wal_bytes)" "$(field snapshot_bytes)" \
        "$(field wal_segments)" "$(field snapshots)" "$rss" >>"$samples"
      sleep 10
    done
  ) &
  local sampler=$!
  phase workloada soak 0
  kill "$sampler" 2>/dev/null || true
  stop_target
  trap - EXIT
  python3 - "$samples" <<'PY'
import csv, sys
rows = [r for r in csv.DictReader(open(sys.argv[1]), delimiter="\t") if r["wal_bytes"] and r["rss_kb"]]
seg, floor = 64 << 20, 256 << 20
over = [r for r in rows if int(r["wal_bytes"]) > max(floor, int(r["snapshot_bytes"])) + 2 * seg]
mib = lambda b: f"{int(b) / (1 << 20):.0f} MiB"
print(f"samples {len(rows)}, WAL most {mib(max(int(r['wal_bytes']) for r in rows))}, last {mib(rows[-1]['wal_bytes'])}, "
      f"snapshots last {mib(rows[-1]['snapshot_bytes'])} after {rows[-1]['snapshots']} shard snapshots, "
      f"RSS first {int(rows[0]['rss_kb']) >> 10} MiB, last {int(rows[-1]['rss_kb']) >> 10} MiB")
half = len(rows) // 2
print(f"WAL most in the first half {mib(max(int(r['wal_bytes']) for r in rows[:half] or rows))}, "
      f"in the second {mib(max(int(r['wal_bytes']) for r in rows[half:]))}")
print("WAL bounded" if not over else f"WAL past its bound in {len(over)} samples, first at {over[0]['secs']} s")
sys.exit(1 if over else 0)
PY
}

case ${1:-} in
  setup) setup ;;
  walsoak) walsoak ;;
  golden) golden ;;
  smoke) smoke ;;
  quick | full) run "$@" ;;
  summary) python3 "$HERE/bench/summary.py" build "${2:?a run directory}" ;;
  check-summary) python3 "$HERE/bench/summary.py" check "${2:?a run directory}" ;;
  *) awk 'NR > 1 && /^set /{ exit } NR > 1' "$0"; exit 2 ;;
esac
