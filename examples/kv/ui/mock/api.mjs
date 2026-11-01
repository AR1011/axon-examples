// A mock of `kv console`'s API, for the page's tests and for `npm run dev:mock`: the same shapes as src/console.rs.
// Three nodes with load that changes in time, and the bench runs of ../../tests/runs as they are committed.
// Each browser has its own cluster, by a cookie, so that tests that run at once do not see each other's faults:
//   GET /api/_mock/down?node=n3   the node stops answering; its shards are on no node, then 3 s later on the others
//   GET /api/_mock/up?node=n3     it answers again, with no shards; 3 s later its own shards come back to it
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const RESULTS = fileURLToPath(new URL("../../tests/runs", import.meta.url));
const SHARDS = 256;
const SLOTS_PER_SHARD = 64;
const EVERY = 1000;
const MOVE = 3000;
const IDS = ["n1", "n2", "n3"];

/** The node a shard has when all nodes answer: spread by a hash, as rendezvous hashing spreads them. */
function home(shard) {
  let x = shard + 1;
  x = Math.imul(x ^ (x >>> 16), 0x85ebca6b);
  x = Math.imul(x ^ (x >>> 13), 0xc2b2ae35);
  return IDS[((x ^ (x >>> 16)) >>> 0) % IDS.length];
}

function cluster() {
  const started = Date.now();
  return {
    started,
    nodes: IDS.map((id, i) => ({ id, addr: `127.0.0.1:${6379 + i}`, down_ms: null, seen_ms: started, pid: 41000 + i * 17, up_since: started - 3 * 3600_000 - i * 61_000 })),
    // Each shard: its node and lease epoch, and the time it got there (a move is seen from then on).
    shards: Array.from({ length: SHARDS }, (_, s) => ({ node: home(s), epoch: 1, from: 0, gap: null })),
  };
}

const clusters = new Map();

function stateOf(req, res) {
  const cookie = /(?:^|;\s*)kvmock=([\w-]+)/.exec(req.headers.cookie ?? "")?.[1];
  if (cookie && clusters.has(cookie)) return clusters.get(cookie);
  const id = cookie ?? Math.random().toString(36).slice(2);
  res.setHeader("set-cookie", `kvmock=${id}; Path=/; SameSite=Lax`);
  const state = cluster();
  clusters.set(id, state);
  return state;
}

/** The node of each shard now: a move shows as no node until MOVE has passed. */
function hosts(state, now) {
  const up = new Set(state.nodes.filter((n) => n.down_ms == null).map((n) => n.id));
  return state.shards.map((s) => (s.gap != null && now < s.gap ? [] : up.has(s.node) ? [{ node: s.node, epoch: s.epoch }] : []));
}

function down(state, id, now) {
  const node = state.nodes.find((n) => n.id === id);
  if (!node || node.down_ms != null) return;
  node.down_ms = now;
  const others = IDS.filter((n) => n !== id && state.nodes.find((x) => x.id === n).down_ms == null);
  state.shards.forEach((s, i) => {
    if (s.node !== id || others.length === 0) return;
    Object.assign(s, { node: others[i % others.length], epoch: s.epoch + 1, gap: now + MOVE });
  });
}

function up(state, id, now) {
  const node = state.nodes.find((n) => n.id === id);
  if (!node || node.down_ms == null) return;
  Object.assign(node, { down_ms: null, seen_ms: now, up_since: now, pid: node.pid + 1 });
  state.shards.forEach((s, i) => {
    if (home(i) === id && s.node !== id) Object.assign(s, { node: id, epoch: s.epoch + 1, gap: now + MOVE });
  });
}

/** A node's rates at a time: a slow wave, different for each node, and a few failures while shards move. */
function rate(state, i, at) {
  const node = state.nodes[i];
  if (node.down_ms != null && at >= node.down_ms) return null;
  const wave = Math.sin(at / 47_000 + i * 1.9) * 0.25 + Math.sin(at / 9_000 + i) * 0.08 + 1;
  const moving = state.shards.some((s) => s.gap != null && at < s.gap && at >= s.gap - MOVE);
  return { reads: Math.round(2400 * wave * 10) / 10, writes: Math.round(800 * wave * 10) / 10, failed: moving ? 6.5 : 0 };
}

function nodeJson(state, node, i, now, placed) {
  const isUp = node.down_ms == null;
  const here = placed.filter((h) => h.some((x) => x.node === node.id)).length;
  const info = {
    node: node.id,
    mode: "durable",
    storage_set: IDS.join(","),
    shards: String(SHARDS),
    process_id: String(node.pid),
    uptime_in_seconds: String(Math.round((now - node.up_since) / 1000)),
    connected_clients: String(isUp ? 3 + i : 0),
    reads: "quorum",
    lease_ms: "6000",
    wal_writes: String(812_400 + i * 1000),
    wal_bytes: String(1_204_000_000 + i * 7_000_000),
    shards_here: String(here),
  };
  return {
    addr: node.addr,
    id: node.id,
    up: isUp,
    error: isUp ? null : "connection refused (os error 111)",
    seen_ms: isUp ? now : node.down_ms,
    down_ms: node.down_ms,
    mode: "durable",
    reads: "quorum",
    pid: node.pid,
    uptime_s: Math.round((now - node.up_since) / 1000),
    clients: isUp ? 3 + i : null,
    shards_here: isUp ? here : null,
    rate: isUp ? rate(state, i, now) : null,
    info,
  };
}

function clusterJson(state) {
  const now = Date.now();
  const placed = hosts(state, now);
  return { at_ms: now, every_ms: EVERY, slots_per_shard: SLOTS_PER_SHARD, nodes: state.nodes.map((n, i) => nodeJson(state, n, i, now, placed)), shards: placed.map((h) => ({ hosts: h })) };
}

function opsJson(state, minutes) {
  const window = Math.min(60, Math.max(1, Number.isFinite(minutes) ? minutes : 5)) * 60_000;
  const bucket = Math.ceil(Math.max(EVERY, window / 360) / EVERY) * EVERY;
  const now = Date.now();
  const nodes = state.nodes.map((node, i) => {
    const points = [];
    for (let at = Math.floor((now - window) / bucket) * bucket + bucket; at <= now; at += bucket) {
      const r = rate(state, i, at);
      points.push(r ? [at, r.reads, r.writes, r.failed] : [at, null, null, null]);
    }
    return { addr: node.addr, id: node.id, points };
  });
  return { every_ms: bucket, nodes };
}

// The runs, read as src/console.rs reads them.
export function parseCsv(text) {
  const lines = text.split("\n").filter((l) => l.length > 0);
  const split = (line) => {
    const out = [];
    let field = "";
    let quoted = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (quoted && c === '"' && line[i + 1] === '"') {
        field += '"';
        i++;
      } else if (c === '"') quoted = !quoted;
      else if (c === "," && !quoted) {
        out.push(field);
        field = "";
      } else field += c;
    }
    out.push(field);
    return out;
  };
  const [head, ...rows] = lines.map(split);
  return { columns: head ?? [], rows };
}

const plain = (name) => name && !name.startsWith(".") && !/[/\\]/.test(name) && existsSync(join(RESULTS, name)) && statSync(join(RESULTS, name)).isDirectory();
const read = (path) => (existsSync(path) ? readFileSync(path, "utf8") : null);

function runFiles(name) {
  const dir = join(RESULTS, name);
  const csv = read(join(dir, "results.csv"));
  const faults = (read(join(dir, "fault", "faults.jsonl")) ?? "")
    .split("\n")
    .filter((l) => l.trim())
    .flatMap((l) => {
      try {
        return [JSON.parse(l)];
      } catch {
        return [];
      }
    });
  return { env: read(join(dir, "env.txt")), summary: read(join(dir, "summary.md")), csv: csv == null ? { columns: [], rows: [] } : parseCsv(csv), faults };
}

function entry(name, files) {
  const env = (key) => files.env?.split("\n").find((l) => l.startsWith(`${key}: `))?.slice(key.length + 2) ?? null;
  const { columns, rows } = files.csv;
  const values = (col) => (columns.indexOf(col) < 0 ? [] : rows.map((r) => r[columns.indexOf(col)]).filter((v) => v != null));
  const rounds = values("round").map(Number).filter(Number.isInteger);
  const soak = files.faults.find((f) => f.phase === "soak");
  const phases = files.faults.filter((f) => f.phase !== "soak");
  return {
    name,
    date: name.slice(0, 10),
    host: env("host") ?? values("host")[0] ?? null,
    kind: env("kind")?.split(" ")[0] ?? values("run_kind")[0] ?? null,
    rounds: rounds.length ? Math.max(...rounds) : null,
    rows: rows.length,
    error_rows: rows.filter((r) => r[columns.indexOf("phase")] !== "warmup" && Number(r[columns.indexOf("errors")]) > 0).length,
    targets: [...new Set(values("target"))],
    faults: phases.length || soak ? { phases: phases.length, passed: phases.filter((f) => f.pass === true).length, soak: soak ? soak.pass : null } : null,
    summary: files.summary != null,
  };
}

function send(res, status, body) {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" }).end(JSON.stringify(body));
}

/** Answers a request to /api/; false for any other path. */
export async function handle(req, res) {
  const url = new URL(req.url, "http://mock");
  if (!url.pathname.startsWith("/api/")) return false;
  const state = stateOf(req, res);
  const path = url.pathname.slice("/api/".length);
  const now = Date.now();
  if (path === "_mock/down" || path === "_mock/up") {
    (path === "_mock/down" ? down : up)(state, url.searchParams.get("node"), now);
    send(res, 200, { ok: true });
  } else if (path === "cluster") send(res, 200, clusterJson(state));
  else if (path === "ops") send(res, 200, opsJson(state, Number(url.searchParams.get("minutes") ?? 5)));
  else if (path === "runs") {
    const names = existsSync(RESULTS) ? readdirSync(RESULTS).filter(plain).sort().reverse() : [];
    send(res, 200, { dir: RESULTS, runs: names.map((n) => entry(n, runFiles(n))) });
  } else if (path.startsWith("runs/")) {
    const name = decodeURIComponent(path.slice("runs/".length));
    if (!plain(name)) send(res, 404, { error: "not_found", message: `no run ${name}` });
    else {
      const files = runFiles(name);
      send(res, 200, { ...entry(name, files), env: files.env, summary: files.summary, columns: files.csv.columns, rows: files.csv.rows, faults: files.faults });
    }
  } else send(res, 404, { error: "not_found", message: "no such route" });
  return true;
}
