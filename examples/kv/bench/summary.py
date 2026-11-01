#!/usr/bin/env python3
"""results.csv and summary.md from a bench run's raw/ (Python standard library only).

  summary.py build <run>                         results.csv and summary.md
  summary.py check <run>                         the same in memory: exit 1 unless the files are that, byte for byte
  summary.py ops <run> <round> <target> <wl>     the saturation ops/s of one phase, for bench.sh's fixed rates
"""

import csv
import io
import json
import os
import re
import statistics
import sys
from collections import defaultdict

LINE = re.compile(r"^(?P<op>[A-Z_]+)\s+- Takes\(s\): (?P<takes>[\d.]+), Count: (?P<count>\d+), OPS: (?P<ops>[\d.]+), (?P<rest>.*)$")
DATA = {"get", "set", "mget", "mset", "del", "exists"}
SYNCS = {"fsync", "fdatasync", "sync_file_range"}
NAMES = {"workloada": "A", "workloadb": "B", "workloadc": "C", "workloadd": "D", "workloadf": "F"}
COLUMNS = [
    "date", "host", "run_kind", "round", "target", "table", "nodes", "client_mode", "workload", "phase",
    "rate_target", "threads", "connections", "records", "secs", "ops_ok", "errors", "ops_s", "p50_us", "p99_us",
    "p999_us", "cpu_s", "ops_per_cpu_s", "srv_cmds_per_op", "fio_p50_us", "ended_early", "bound", "value_bytes",
    "mb_s", "box_load",
]


def ycsb(path):
    """The final block of a go-ycsb log: per operation its count, ops/s and percentiles."""
    ops = {}
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return ops
    tail = text.rsplit("Run finished", 1)[-1] if "Run finished" in text else ""
    for line in tail.splitlines():
        m = LINE.match(line.strip())
        if not m:
            continue
        fields = dict(kv.split(": ") for kv in m["rest"].split(", ") if ": " in kv)
        ops[m["op"]] = {
            "takes": float(m["takes"]),
            "count": int(m["count"]),
            "ops": float(m["ops"]),
            "p50": fields.get("50th(us)"),
            "p99": fields.get("99th(us)"),
            "p999": fields.get("99.9th(us)"),
        }
    return ops


def commands(path):
    """Server data commands and client connections in a snapshot: Redis or kv INFO, or etcd's counters."""
    data, conns, put, ranges = 0, 0, {}, 0
    try:
        lines = open(path).read().splitlines()
    except OSError:
        return None, None
    for line in lines:
        port, _, rest = line.partition(" ")
        m = re.match(r"cmdstat_(\w+):calls=(\d+)", rest)
        if m and m[1] in DATA:
            data += int(m[2])
        m = re.match(r"connected_clients:(\d+)", rest)
        if m:
            conns += int(m[1]) - 1  # less the redis-cli that asked
        m = re.match(r"etcd_mvcc_put_total (\S+)", rest)
        if m:
            put[port] = float(m[1])
        m = re.match(r"etcd_mvcc_range_total (\S+)", rest)
        if m:
            ranges += float(m[1])
    if put:
        # Every member applies each put; a read is served by one.
        return list(put.values())[0] + ranges, None
    return data, conns


def kv_info(path, field):
    """The median over a kv target's nodes of one INFO kv field in a snapshot, or "" where none has it above 0."""
    try:
        lines = open(path).read().splitlines()
    except OSError:
        return ""
    values = []
    for line in lines:
        m = re.match(r"\d+ " + field + r":(\d+)$", line)
        if m and int(m[1]) > 0:
            values.append(int(m[1]))
    return statistics.median(values) if values else ""


def fio_p50(path):
    try:
        job = json.load(open(path))["jobs"][0]
        return round(job["sync"]["lat_ns"]["percentile"]["50.000000"] / 1000, 1)
    except (OSError, KeyError, IndexError, ValueError):
        return ""


def strace_syncs(path):
    total = 0
    try:
        for line in open(path):
            parts = line.split()
            if parts and parts[-1] in SYNCS:
                total += int(parts[3])
    except OSError:
        return None
    return total


def env_field(run, name):
    try:
        for line in open(os.path.join(run, "env.txt")):
            if line.startswith(name + ":"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return ""


def rows(run):
    raw = os.path.join(run, "raw")
    kind = os.path.basename(run.rstrip("/")).rsplit("-", 1)[-1]
    date, host = env_field(run, "date")[:10], env_field(run, "host")
    out = []
    for line in open(os.path.join(raw, "runs.jsonl")):
        meta = json.loads(line)
        ops = ycsb(os.path.join(raw, meta["id"] + ".log"))
        total = ops.get("TOTAL", {})
        errors = sum(v["count"] for k, v in ops.items() if k.endswith("_ERROR"))
        if meta.get("rc", 0) != 0 or not total:
            errors = max(errors, 1)  # go-ycsb refused or failed the phase: no counts to trust
        ok = total.get("count", 0)
        c0, _ = commands(os.path.join(raw, meta["id"] + ".cmds0"))
        c1, conns = commands(os.path.join(raw, meta["id"] + ".cmds1"))
        per_op = round((c1 - c0) / (ok + errors), 3) if c0 is not None and c1 is not None and ok + errors else ""
        cpu = float(meta["cpu_s"])
        ops_s = total.get("ops", 0.0)
        value = int(meta.get("value_bytes", 0))
        out.append({
            "date": date, "host": host, "run_kind": kind, "round": meta["round"], "target": meta["target"],
            "table": meta["table"], "nodes": meta["nodes"],
            "client_mode": "cluster" if meta["nodes"] > 1 and not meta["target"].startswith("etcd") else "single",
            "workload": meta["workload"], "phase": meta["phase"], "rate_target": meta["rate_target"],
            "threads": meta["threads"], "connections": conns if conns is not None else "", "records": meta["records"],
            "secs": meta["secs"], "ops_ok": ok, "errors": errors, "ops_s": round(ops_s, 1),
            "p50_us": total.get("p50", ""), "p99_us": total.get("p99", ""), "p999_us": total.get("p999", ""),
            "cpu_s": cpu, "ops_per_cpu_s": round(ok / cpu, 1) if cpu > 0 else "", "srv_cmds_per_op": per_op,
            # By name in raw/: runs.jsonl holds the path on the box, and the run is read again elsewhere.
            "fio_p50_us": fio_p50(os.path.join(raw, os.path.basename(meta["fio"]))) if meta.get("fio") else "",
            "ended_early": int(meta["secs"] > 0 and total.get("takes", 0) < 0.95 * meta["secs"]),
            "bound": "", "value_bytes": value or "", "mb_s": round(ops_s * value / 1e6, 1) if value else "",
            "writes": sum(ops.get(op, {}).get("count", 0) for op in ("INSERT", "UPDATE")),
            "append_rtt_us": kv_info(os.path.join(raw, meta["id"] + ".cmds1"), "append_rtt_p50_us"),
            "box_load": float(meta.get("load") or 0),
        })
    return out


def bounds(table):
    """Marks rows near a ceiling of the same workload and round."""
    ceiling = defaultdict(dict)
    for r in table:
        if r["phase"] == "sat" and r["target"] in ("basic", "stub", "etcd-check"):
            ceiling[(r["round"], r["workload"])][r["target"]] = r["ops_s"]
    for r in table:
        if r["phase"] != "sat" or r["table"] == "ceilings":
            continue
        c = ceiling.get((r["round"], r["workload"]), {})
        marks = []
        if c.get("basic") and r["ops_s"] >= 0.8 * c["basic"]:
            marks.append("client-bound")
        if r["target"].startswith("kv-") and c.get("stub") and r["ops_s"] >= 0.8 * c["stub"]:
            marks.append("front-end-bound")
        if r["target"].startswith("redis") and c.get("stub") and r["ops_s"] > c["stub"]:
            marks.append("stub-bound, not client-bound")
        if r["target"].startswith("etcd") and c.get("etcd-check") and r["ops_s"] >= 0.8 * c["etcd-check"]:
            marks.append("etcd-driver-bound")
        r["bound"] = "; ".join(marks)


def fmt(n):
    return f"{n:,.0f}" if isinstance(n, (int, float)) and n != "" else str(n)


def median_cell(values):
    values = [v for v in values if v != ""]
    if not values:
        return ""
    med = statistics.median(values)
    if len(values) == 1:
        return fmt(med)
    spread = (max(values) - min(values)) / med if med else 0
    flag = " (range > 10 %)" if spread > 0.10 else ""
    return f"{fmt(med)} [{', '.join(fmt(v) for v in values)}]{flag}"


def summary(run, table):
    clean = [r for r in table if r["errors"] == 0]
    dirty = [r for r in table if r["errors"] > 0 and r["phase"] not in ("warmup",)]
    md = [f"# kv bench: {os.path.basename(run.rstrip('/'))}", ""]
    md += [
        "**Indicative.** " + env_field(run, "kind"),
        "",
        f"- CPU: {env_field(run, 'cpu')}; governor {env_field(run, 'governor').split(' ')[0]}; "
        f"{env_field(run, 'server cpus')}",
        f"- Work dir: {env_field(run, 'work dir')}",
        f"- Client: {env_field(run, 'client')}",
        f"- Phases: {env_field(run, 'phases')}",
        "- Box load (1-min average at each phase's end, 24 threads, other work included), highest per target: "
        + ", ".join(f"{t} {max(r['box_load'] for r in table if r['target'] == t):.0f}"
                    for t in dict.fromkeys(r["target"] for r in table)),
        "",
        "Saturation rows are closed-loop throughput with 64 requests outstanding: their percentiles are not a latency"
        " comparison. Workload F counts each read and each write of a read-modify-write as one operation.",
        "",
    ]

    def sat_table(name, title):
        rows_ = [r for r in clean if r["table"] == name and r["phase"] == "sat"]
        if not rows_:
            return
        workloads = [w for w in NAMES if any(r["workload"] == w for r in rows_)]
        md.extend([f"## {title}", "", "| target | " + " | ".join(NAMES[w] + " ops/s" for w in workloads)
                   + " | A ops per CPU-s | cmds/op | bound |", "|---|" + "---|" * (len(workloads) + 3)])
        for target in dict.fromkeys(r["target"] for r in rows_):
            cells = [median_cell([r["ops_s"] for r in rows_ if r["target"] == target and r["workload"] == w])
                     for w in workloads]
            a = [r for r in rows_ if r["target"] == target and r["workload"] == "workloada"]
            cpu = median_cell([r["ops_per_cpu_s"] for r in a])
            cmds = sorted({r["srv_cmds_per_op"] for r in rows_ if r["target"] == target and r["srv_cmds_per_op"] != ""})
            bound = "; ".join(sorted({r["bound"] for r in rows_ if r["target"] == target and r["bound"]}))
            md.append(f"| {target} | " + " | ".join(cells) + f" | {cpu} | {', '.join(map(str, cmds))} | {bound} |")
        md.append("")

    sat_table("ceilings", "Ceilings")
    sat_table("M", "M: acked from memory")
    sat_table("S1", "S1: one local fsync")
    sat_table("R", "R: a majority fsyncs (3 processes, one disk, loopback)")
    sat_table("R-lease", "R, a weaker read guarantee: kv's lease reads (alone, not compared)")

    fixed = [r for r in clean if r["phase"].startswith("fixed")]
    if fixed:
        md += ["## Fixed rate", "", "Coordinated omission is reduced here, not removed.", "",
               "| target | workload | rate | ops/s | p50 us | p99 us | p99.9 us |", "|---|---|---|---|---|---|---|"]
        for r in fixed:
            md.append(f"| {r['target']} | {NAMES.get(r['workload'], r['workload'])} {r['phase'][5:]} % | "
                      f"{fmt(r['rate_target'])} | {fmt(r['ops_s'])} | {r['p50_us']} | {r['p99_us']} | {r['p999_us']} |")
        md.append("")

    large = [r for r in table if r["table"] == "large" and r["phase"].startswith("large")]
    if large:
        md += ["## Large values", "",
               "Values stored as one go-ycsb field; B is 95 % reads, A 50 % writes. etcd runs with "
               "--max-request-bytes=33554432, compaction every 5 s, and its client patched to send 64 MiB "
               "(bench/go-ycsb.patch). go-ycsb makes and encodes every value on the client cpus: where the memory "
               "targets of one size agree, they may all be bound by the client, not the server.", "",
               "| target | value | mix | ops/s | MB/s | p50 us | p99 us | errors |", "|---|---|---|---|---|---|---|---|"]
        for r in large:
            mix = "read-heavy (B)" if r["workload"] == "workloadb" else "write-heavy (A)"
            md.append(f"| {r['target']} | {r['value_bytes'] // 1000000} MB | {mix} | {fmt(r['ops_s'])} | "
                      f"{fmt(r['mb_s'])} | {r['p50_us']} | {r['p99_us']} | {r['errors']} |")
        md.append("")

    parity = [r for r in table if r["table"] == "parity"]
    if parity:
        md += ["## Sync parity", "", "| target | sync calls | acked writes | syncs per acked write | acked writes per sync |",
               "|---|---|---|---|---|"]
        for target in dict.fromkeys(r["target"] for r in parity):
            writes = sum(r["writes"] for r in parity if r["target"] == target)
            raw = os.path.join(run, "raw")
            syncs = sum(strace_syncs(os.path.join(raw, f)) or 0 for f in os.listdir(raw)
                        if f.startswith(f"strace-{target}-"))
            per = f"{writes / syncs:.2f}" if syncs else ""
            md.append(f"| {target} | {fmt(syncs)} | {fmt(writes)} | {syncs / writes:.2f} | {per} |" if writes else
                      f"| {target} | {fmt(syncs)} | 0 | | |")
        md.append("")

    md += predicted(clean)
    md += faults(run)

    if dirty:
        md += ["## Rows with errors (left out above)", "", "| target | workload | phase | ops_ok | errors |",
               "|---|---|---|---|---|"]
        md += [f"| {r['target']} | {r['workload']} | {r['phase']} | {r['ops_ok']} | {r['errors']} |" for r in dirty]
        md.append("")
    return "\n".join(md)


# What a durable target's closed loop should give, from inputs measured in the same run:
# ops/s = T / (u * t_w + (1 - u) * t_r), t_w from k * f + h + t_r with k from 1 to 2, capped by its peer in M.
CAP = {"kv-durable-1": "kv-memory-1", "kv-durable-3": "kv-memory-3", "kv-durable-3-lease": "kv-memory-3",
       "redis-aof": "redis"}
UPDATES = {"workloada": 0.5, "workloadb": 0.05}


def predicted(clean):
    def med(values):
        values = [float(v) for v in values if v not in ("", None)]
        return statistics.median(values) if values else None

    targets = [t for t in dict.fromkeys(r["target"] for r in clean)
               if any(r["target"] == t and r["table"] in ("S1", "R", "R-lease") for r in clean)]
    out = []
    for target in targets:
        mine = [r for r in clean if r["target"] == target]
        f = med(r["fio_p50_us"] for r in mine)
        t_r = med(r["p50_us"] for r in mine if r["phase"] == "fixed50" and r["workload"] == "workloadc")
        h = med(r["append_rtt_us"] for r in mine if r["phase"] == "sat" and r["workload"] == "workloada") or 0
        if not f or not t_r:
            continue
        threads = mine[0]["threads"]
        for workload, u in UPDATES.items():
            measured = med(r["ops_s"] for r in mine if r["phase"] == "sat" and r["workload"] == workload)
            if measured is None:
                continue
            bounds_ = []
            for k, extra in ((1, 0), (2, h)):
                t_w = k * f + extra + t_r
                # Redis answers nothing while its event loop syncs: a read waits as long as a write.
                read = t_w if target == "redis-aof" else t_r
                bounds_.append(threads / ((u * t_w + (1 - u) * read) / 1e6))
            low, high = min(bounds_), max(bounds_)
            cap = med(r["ops_s"] for r in clean if r["target"] == CAP.get(target) and r["phase"] == "sat"
                      and r["workload"] == workload)
            if cap:
                low, high = min(low, cap), min(high, cap)
            where = "in range" if low * 0.9 <= measured <= high * 1.1 else ("below" if measured < low else "above")
            out.append(f"| {target} | {NAMES[workload]} | {f:,.0f} | {t_r:,.0f} | {h:,.0f} | {fmt(low)} to "
                       f"{fmt(high)} | {fmt(measured)} | {where} |")
    if not out:
        return []
    return ["## Predicted against measured", "",
            "Saturation ops/s from T / (u t_w + (1 - u) t_r): T the threads, u the update share (A 0.5, B 0.05), "
            "f the fio fsync p50, t_r the target's read p50 at half load (fixed-rate C 50 %), t_w = k f + t_r with "
            "k = 1, up to 2 f + h + t_r; h kv's Append to another node's Log until its answer (its hop and that "
            "Log's sync), the p50 since the node started, read at the end of A's saturation phase; 0 where not "
            "measured. Capped by the same nodes' memory target. redis-aof's reads wait for "
            "its sync. \"in range\" allows 10 %.", "",
            "| target | workload | f us | t_r us | h us | predicted ops/s | measured ops/s | |",
            "|---|---|---|---|---|---|---|---|"] + out + [""]


def faults(run):
    """The fault run's phases from fault/faults.jsonl, one row per phase run."""
    try:
        lines = [json.loads(line) for line in open(os.path.join(run, "fault", "faults.jsonl")) if line.strip()]
    except OSError:
        return []
    md = ["## Faults", "",
          "fault.sh on 3 durable nodes (memory-control on memory nodes), `kv check` with 64 writers, 16 readers "
          "and a probe write to every shard each 10 ms. Windows: how long a shard took no write, p50 / max in ms, "
          "for the moves away and back; each must end within T + 3 s. Unknown: writes answered `ERR outcome "
          "unknown`, never counted lost.", "",
          "| phase | verdict | acked writes | failed | unknown | lost | stale | away p50 / max | back p50 / max | note |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for f in lines:
        c = f.get("check", {})
        w = f.get("windows", {})
        win = lambda side: (f"{w[side]['p50_ms']:,} / {w[side]['max_ms']:,} ({w[side]['n']})"
                            if w.get(side, {}).get("n") else "")
        note = ""
        if f["phase"] == "follower":
            note = f"{f.get('moved', '?')} written shards moved"
        elif f["phase"] in ("restart-all", "quorum-loss"):
            v = f.get("verify", {})
            note = f"read back {v.get('keys', '?')} keys: lost {v.get('lost', '?')}"
            if f["phase"] == "quorum-loss":
                d = f.get("down", {})
                note += (f"; two down: {d.get('writes_ok', '?')} acked of {d.get('writes_failed', 0) + d.get('writes_ok', 0)}"
                         f" writes, answers {', '.join(sorted(d.get('errors', {})))}")
            else:
                note += f"; every shard answered {f.get('answered_s', '?')} s after the starts"
        elif f["phase"] == "soak":
            j = f.get("judged", {})
            checks = f.get("checks", [])
            if "rss" in j:
                rss = "RSS last third / middle third " + ", ".join(f"{n} {v['ratio']}" for n, v in j["rss"].items())
            else:  # a soak judged before the rule went per node
                rss = f"RSS most {fmt(j.get('rss_mid_mib'))} then {fmt(j.get('rss_last_mib'))} MiB"
            note = (f"{f.get('minutes')} min at {fmt(f.get('rate'))} ops/s; {sum(x['summary'] == 'PASS' for x in checks)}"
                    f" of {len(checks)} checks passed; WAL at most {fmt(j.get('wal_most_mib'))} MiB; {rss}")
            c = {k: sum(x.get(k, 0) for x in checks) for k in ("writes_ok", "writes_failed", "lost", "stale")}
        elif f.get("victim"):
            note = f"{f['victim']}; T {f.get('lease', {}).get('T_ms', '?')} ms"
        md.append(f"| {f['phase']} | {'PASS' if f['pass'] else 'FAIL'} | {fmt(c.get('writes_ok', ''))} | "
                  f"{fmt(c.get('writes_failed', ''))} | {fmt(c.get('unknown', ''))} | {c.get('lost', '')} | "
                  f"{c.get('stale', '')} | {win('away')} | {win('back')} | {note} |")
    return md + [""]


def outputs(run):
    table = rows(run)
    bounds(table)
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
    w.writeheader()
    w.writerows(table)
    return {"results.csv": buf.getvalue(), "summary.md": summary(run, table)}


def build(run):
    for name, text in outputs(run).items():
        with open(os.path.join(run, name), "w", newline="") as f:
            f.write(text)
    print(os.path.join(run, "summary.md"))


def check(run):
    differ = []
    for name, text in outputs(run).items():
        try:
            same = open(os.path.join(run, name), newline="").read() == text
        except OSError:
            same = False
        if not same:
            differ.append(name)
    if differ:
        sys.exit(f"{run}: {', '.join(differ)} differ from what raw/ gives")
    print(f"{run}: results.csv and summary.md are what raw/ gives, byte for byte")


def ops(run, rnd, target, workload):
    raw = os.path.join(run, "raw")
    best = 0.0
    for line in open(os.path.join(raw, "runs.jsonl")):
        meta = json.loads(line)
        if (str(meta["round"]), meta["target"], meta["workload"], meta["phase"]) == (rnd, target, workload, "sat"):
            best = ycsb(os.path.join(raw, meta["id"] + ".log")).get("TOTAL", {}).get("ops", 0.0)
    print(int(best))


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "build":
        build(sys.argv[2])
    elif len(sys.argv) == 3 and sys.argv[1] == "check":
        check(sys.argv[2])
    elif len(sys.argv) == 6 and sys.argv[1] == "ops":
        ops(*sys.argv[2:])
    else:
        sys.exit(__doc__)
