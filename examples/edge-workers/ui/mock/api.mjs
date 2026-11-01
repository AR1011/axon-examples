// A stand-in for the control node's admin API (../../src/admin.rs), for the console's development and tests.
// Every body has the shape admin.rs produces. Each session gets a world of its own, so tests do not meet.
// It runs the JavaScript it is sent in this node process: use it on your own machine only.
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";

export const PASSWORD = "edge";
const COOKIE = "edge_session";
const SAMPLE_MS = 10_000;
/** A lease's epoch is a large number, not a count from 1. */
const EPOCH = 1_791_093_947_000_000;
const OUTCOMES = ["ok", "exception", "cpu_limit", "memory_limit", "wall_timeout", "engine", "response_too_large", "storage"];
const RUNNERS = ["r1", "r2", "r3"];
const NODES = [
  { id: "c1", address: "127.0.0.1:7600", roles: ["control"] },
  { id: "g1", address: "127.0.0.1:7601", roles: ["gateway"], url: "http://127.0.0.1:7701" },
  { id: "g2", address: "127.0.0.1:7602", roles: ["gateway"], url: "http://127.0.0.1:7702" },
  { id: "r1", address: "127.0.0.1:7611", roles: ["runner"] },
  { id: "r2", address: "127.0.0.1:7612", roles: ["runner"] },
  { id: "r3", address: "127.0.0.1:7613", roles: ["runner"] },
];
const LIMITS = { cpu_ms: 50, wall_ms: 30_000, memory_mb: 128, subrequests: 50 };

const template = (name, description, objects = {}) => ({
  name,
  description,
  source: readFileSync(new URL(`../../templates/${name}.js`, import.meta.url), "utf8"),
  objects,
});
const TEMPLATES = [
  template("hello", "One answer for every request"),
  template("json-api", "A JSON API with routes by method and path"),
  template("redirect", "Redirects by path, and to the TARGET variable"),
  template("proxy", "Passes each request to the UPSTREAM variable"),
  template("feature-flag", "A feature turned on by the NEW_CHECKOUT variable, for all or for a percent"),
  template("counter", "A Durable Object: a counter per name, kept across runners (bind COUNTER=Counter)", { COUNTER: "Counter" }),
];

class Refusal extends Error {
  constructor(status, code, message) {
    super(message);
    Object.assign(this, { status, code });
  }
}
const notFound = (what) => new Refusal(404, "not_found", what);
const invalid = (why) => new Refusal(400, "invalid", `invalid: ${why}`);

/** A number in 0..1 that is the same for the same text. */
const dice = (text) => createHash("sha256").update(text).digest().readUInt32BE(0) / 2 ** 32;
const hex = (bytes) => randomBytes(bytes).toString("hex");

// ---- the user's code ------------------------------------------------------------------------------------------

const running = new AsyncLocalStorage();
const say = (prefix) => (...args) => running.getStore()?.logs.push(`${prefix}: ${args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ")}`);
globalThis.__edgeConsole = { log: say("Log"), info: say("Info"), debug: say("Debug"), warn: say("Warn"), error: say("Error") };

async function load(source, objects) {
  let module;
  try {
    // On the first line, so the engine's line numbers stay the source's.
    module = await import(`data:text/javascript;base64,${Buffer.from(`const console = globalThis.__edgeConsole; ${source}`).toString("base64")}`);
  } catch (e) {
    throw new Refusal(422, "load", `load: ${e.message}`);
  }
  if (typeof module.default?.fetch !== "function") throw new Refusal(422, "load", "load: the default export has no fetch(request, env)");
  for (const [name, cls] of Object.entries(objects)) {
    if (typeof module[cls] !== "function") throw new Refusal(422, "load", `load: ${name} is bound to ${cls}, which the module does not export`);
  }
  return module;
}

/** One request to a loaded module: the response, or the failure as a response, with what the run used. */
async function run(module, ask, env) {
  const logs = [];
  const started = performance.now();
  const used = () => Math.max(1, Math.round((performance.now() - started) * 1000));
  const withBody = !["GET", "HEAD"].includes(ask.method) && ask.body;
  try {
    const response = await running.run({ logs }, async () => {
      const request = new Request(`http://localhost${ask.url}`, { method: ask.method, headers: ask.headers, body: withBody ? ask.body : undefined });
      const timeout = new Promise((_, reject) => setTimeout(() => reject(Object.assign(new Error("the run took too long"), { outcome: "wall_timeout" })), 2000).unref());
      const answer = await Promise.race([module.default.fetch(request, env, {}), timeout]);
      if (!(answer instanceof Response)) throw new Error("the handler did not return a Response");
      return { status: answer.status, headers: [...answer.headers], body: await answer.text() };
    });
    const wall = used();
    return { ...response, logs, cpu_us: Math.max(1, Math.round(wall * 0.8)), wall_us: wall, outcome: "ok", error: null };
  } catch (e) {
    const outcome = e.outcome ?? "exception";
    const wall = used();
    // As the engine says it: the console lines end with the exception and its stack, and a line with nothing.
    if (outcome === "exception") logs.push(`Error while running request handler: ${e.message}`, "Stack:", ...String(e.stack ?? "").split("\n").slice(1, 3), "");
    return { status: outcome === "exception" ? 500 : 503, headers: [], body: "", logs, cpu_us: Math.max(1, Math.round(wall * 0.8)), wall_us: wall, outcome, error: outcome === "exception" ? `threw: ${e.message}` : "exceeded its wall time limit" };
  }
}

// ---- a world: what one session sees ---------------------------------------------------------------------------

function newWorld() {
  const world = { seq: 0, workers: new Map(), records: [], tails: new Set(), objects: new Map(), live: new Map(), started: Date.now() - 3_600_000 };
  const make = (name, uploads, rate) => {
    for (const upload of uploads) change(world, name, upload);
    const worker = world.workers.get(name);
    [worker.rate, worker.created] = [rate, world.started];
    // Newest first: a day between versions, the latest two hours ago.
    worker.versions.forEach((v, i) => (v.created_ms = Date.now() - 7_200_000 - i * 86_400_000 - Math.round(dice(name + v.id) * 3_600_000)));
  };
  const source = (name) => TEMPLATES.find((t) => t.name === name).source;
  make("hello", [
    { source: source("hello"), message: "first version" },
    { source: source("hello").replace('"hello"', '"hi"'), vars: { GREETING: "hello" }, message: "set GREETING" },
    { source: source("hello").replace('"world"', '"you"'), secrets: { API_KEY: "sk-mock-0001" }, message: "a new default name" },
  ], 14);
  make("api", [
    { source: source("json-api"), message: "first version" },
    { source: source("json-api").replace('"GET /time", ', '"GET /time", "GET /health", '), message: "list /health", deploy: false },
  ], 6);
  deploy(world, "api", 1, [2, 10]);
  make("counter", [{ source: source("counter"), objects: { COUNTER: "Counter" }, message: "first version" }], 2);
  make("draft", [{ source: source("redirect"), message: "not ready", deploy: false }], 0);
  for (const [id, count, node] of [["lobby", 42, "r2"], ["visits", 1207, "r1"], ["2026/10", 3, null]]) {
    world.objects.set(`counter/Counter/${id}`, { epoch: EPOCH + (node ? 3 : 1), node, entries: new Map([["count", JSON.stringify(count)]]) });
  }
  world.objects.set("counter/Counter/empty", { epoch: EPOCH + 1, node: "r3", entries: new Map() });
  // The last ten minutes, oldest first.
  for (let i = 60; i > 0; i--) {
    const name = ["hello", "hello", "api", "counter"][i % 4];
    world.records.push(synthetic(world, name, Date.now() - i * 10_000 - Math.round(dice(`at${i}`) * 9000), i));
  }
  return world;
}

function get(world, name) {
  const worker = world.workers.get(name);
  if (!worker) throw notFound("no such worker");
  return worker;
}
const version = (worker, id) => {
  const found = worker.versions.find((v) => v.id === id);
  if (!found) throw notFound(`no version ${id}`);
  return found;
};
/** The version a change builds on: the one that takes the traffic, or the latest before any deployment. */
const base = (worker) => version(worker, worker.deployment?.stable ?? worker.latest);

function deploy(world, name, stable, canary) {
  const worker = get(world, name);
  version(worker, stable);
  if (canary) {
    version(worker, canary[0]);
    if (canary[0] === stable) throw invalid("the canary is the stable version");
    if (!(canary[1] >= 1 && canary[1] <= 99)) throw invalid("a canary takes 1 to 99 percent");
  }
  worker.deployment = { seq: ++world.seq, stable, canary: canary ?? null };
  worker.deployed = Date.now();
  return worker.deployment.seq;
}

/** An upload: a new version, of a new worker too. What it leaves out is kept from the version it builds on. */
function change(world, name, upload) {
  if (!/^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$/.test(name)) throw invalid("a name is 1 to 64 of a-z, 0-9 and -, with no - at an end");
  let worker = world.workers.get(name);
  const from = worker && base(worker);
  if (!from && upload.source == null) throw notFound("no such worker");
  const made = {
    id: (worker?.latest ?? 0) + 1,
    created_ms: Date.now(),
    message: upload.message ?? "",
    source: upload.source ?? from.source,
    vars: { ...(upload.vars ?? from?.vars ?? {}) },
    secrets: { ...(from?.secrets ?? {}) },
    objects: { ...(upload.objects ?? from?.objects ?? {}) },
    limits: { ...(from?.limits ?? LIMITS), ...(upload.limits ?? {}) },
  };
  for (const [key, value] of Object.entries(upload.set ?? {})) value == null ? delete made.vars[key] : (made.vars[key] = value);
  for (const [key, value] of Object.entries(upload.secrets ?? {})) value == null ? delete made.secrets[key] : (made.secrets[key] = value);
  if (Object.keys(made.vars).some((key) => key in made.secrets)) throw invalid("a name is both a variable and a secret");
  if (Buffer.byteLength(made.source) > 512 << 10) throw invalid("a source over 524288 bytes");
  made.bundle = createHash("sha256").update(made.source).digest("hex");
  if (!worker) world.workers.set(name, (worker = { name, latest: 0, deployment: null, versions: [], rate: 0, created: Date.now() }));
  worker.versions.unshift(made);
  worker.latest = made.id;
  const seq = upload.deploy === false ? null : deploy(world, name, made.id, null);
  return { version: made.id, seq };
}

/** What the engine builds is its own runtime and the source: about 11.5 MiB of any source. */
const BUILD_BYTES = 12_058_624;
const limitsJson = (limits) => ({ cpu_ms: limits.cpu_ms, wall_ms: limits.wall_ms, memory_mb: limits.memory_mb, subrequests: limits.subrequests });
const versionJson = (v) => ({ id: v.id, created_ms: v.created_ms, message: v.message, bundle: v.bundle, source_bytes: Buffer.byteLength(v.source), build_bytes: BUILD_BYTES + Buffer.byteLength(v.source), limits: limitsJson(v.limits) });
function workerJson(worker) {
  const d = worker.deployment;
  const urls = NODES.filter((node) => node.url).map((node) => ({ gateway: node.id, url: `${node.url}/w/${worker.name}/` }));
  return { name: worker.name, latest: worker.latest, deployment: d && { seq: d.seq, stable: d.stable, canary: d.canary && { version: d.canary[0], percent: d.canary[1] } }, urls };
}

// ---- numbers: a steady made-up load, plus every call the session made itself ------------------------------------

/** What a worker did in the 10 s that end at `slot * SAMPLE_MS`, over every runner. */
function point(worker, slot) {
  const at_ms = slot * SAMPLE_MS;
  if (!worker.deployment || !worker.rate || at_ms < worker.created) return { at_ms, requests: 0, errors: 0, cpu_p50_us: null, cpu_p99_us: null, wall_p50_us: null, wall_p99_us: null };
  const wave = 1 + 0.35 * Math.sin(slot / 9) + 0.2 * (dice(`${worker.name}${slot}`) - 0.5);
  const requests = Math.max(1, Math.round(worker.rate * 10 * wave));
  const cpu = Math.round(280 + 900 * dice(worker.name) + 60 * Math.sin(slot / 5));
  const spike = dice(`${worker.name}s${slot}`) > 0.93 ? 4 : 1;
  return { at_ms, requests, errors: dice(`${worker.name}e${slot}`) > 0.86 ? Math.ceil(requests / 40) : 0, cpu_p50_us: cpu, cpu_p99_us: cpu * 6 * spike, wall_p50_us: Math.round(cpu * 1.4), wall_p99_us: cpu * 11 * spike };
}

function series(worker) {
  const last = Math.floor(Date.now() / SAMPLE_MS);
  const first = Math.max(last - 359, Math.ceil(worker.created / SAMPLE_MS));
  const points = [];
  for (let slot = first; slot <= last; slot++) points.push(point(worker, slot));
  return points;
}

/** A worker's counters on one runner, or over all of them when `node` is null: as `Totals::json` says them. */
function totals(world, worker, node) {
  const share = node ? [0.42, 0.33, 0.25][RUNNERS.indexOf(node)] : 1;
  const made = series(worker);
  const outcomes = Object.fromEntries(OUTCOMES.map((o) => [o, 0]));
  const errors = Math.round(made.reduce((sum, p) => sum + p.errors, 0) * share);
  outcomes.ok = Math.round(made.reduce((sum, p) => sum + p.requests, 0) * share) - errors;
  outcomes.exception = Math.ceil(errors * 0.7);
  outcomes.cpu_limit = errors - outcomes.exception;
  const own = world.records.filter((r) => r.own && r.worker === worker.name && (!node || r.node === node));
  for (const record of own) outcomes[record.outcome] += 1;
  const requests = Object.values(outcomes).reduce((a, b) => a + b, 0);
  const sorted = (key) => own.map((r) => r[key]).sort((a, b) => a - b);
  const p = (key, q) => (made.some((x) => x.requests) ? Math.round(made.at(-1)[`${key}_p${q === 0.5 ? 50 : 99}_us`] * (node ? 0.9 + 0.2 * dice(node) : 1)) : own.length ? sorted(`${key}_us`)[Math.min(own.length - 1, Math.floor(own.length * q))] : null);
  return {
    requests,
    errors: requests - outcomes.ok,
    outcomes,
    busy: worker.rate ? Math.round(requests / 900) : 0,
    cold_starts: requests ? (node ? 1 : RUNNERS.length) : 0,
    in_flight: worker.rate && worker.deployment ? Math.round(worker.rate / (node ? 12 : 4)) : 0,
    cpu_p50_us: p("cpu", 0.5),
    cpu_p99_us: p("cpu", 0.99),
    wall_p50_us: p("wall", 0.5),
    wall_p99_us: p("wall", 0.99),
  };
}

/** A made-up invocation of the steady load. */
function synthetic(world, name, at_ms, n) {
  const worker = world.workers.get(name);
  const roll = dice(`${name}${n}r`);
  const path = { hello: [`/?name=${["ada", "lin", "sam", "kit"][n % 4]}`, "/"], api: ["/", "/time", "/echo", "/missing"], counter: ["/lobby", "/visits"] }[name] ?? ["/"];
  const url = path[n % path.length];
  const failed = roll > 0.93;
  const status = failed ? 500 : url === "/missing" ? 404 : url === "/echo" ? 201 : 200;
  const cpu = Math.round(250 + 1200 * dice(`${name}${n}c`));
  const d = worker?.deployment;
  return {
    at_ms,
    worker: name,
    version: d ? (d.canary && roll < d.canary[1] / 100 ? d.canary[0] : d.stable) : 1,
    node: RUNNERS[n % 3],
    trace: hex(8),
    method: url === "/echo" || (name === "counter" && n % 3 === 0) ? "POST" : "GET",
    url,
    status,
    outcome: failed ? "exception" : "ok",
    cpu_us: cpu,
    wall_us: Math.round(cpu * (1.2 + dice(`${name}${n}w`))),
    logs: failed ? ["Log: reading the body", "Error while running request handler: body is not JSON", "Stack:", "  fetch@worker.js:14:20", "  @index.js:60:31", ""] : name === "hello" ? [`Log: greeting ${new URLSearchParams(url.slice(2)).get("name") ?? "world"}`] : [],
    truncated: false,
  };
}

function record(world, made) {
  world.records.push(made);
  if (world.records.length > 2048) world.records.shift();
  for (const tail of world.tails) if (admits(tail.filter, made)) tail.send("invocation", JSON.stringify(made));
}

function admits(filter, r) {
  const status = (wanted) => wanted === String(r.status) || wanted === r.outcome || (wanted.endsWith("xx") && wanted.slice(0, -2) === String(r.status)[0]);
  return (
    (!filter.worker || filter.worker === r.worker) &&
    (!filter.node || filter.node === r.node) &&
    (!filter.trace || filter.trace === r.trace) &&
    (!filter.status || status(filter.status)) &&
    (!filter.q || r.url.includes(filter.q) || r.logs.some((line) => line.includes(filter.q)))
  );
}

const filterOf = (query, name) => ({ worker: name, status: query.get("status") || undefined, q: query.get("q") || undefined, node: query.get("node") || undefined, trace: query.get("trace") || undefined, limit: Number(query.get("limit")) || undefined });

function recent(world, filter) {
  const found = world.records.filter((r) => admits(filter, r)).sort((a, b) => b.at_ms - a.at_ms);
  return found.slice(0, Math.min(filter.limit ?? 100, 1000));
}

// ---- objects --------------------------------------------------------------------------------------------------

/** `env` for a deployed run: variables, secrets, and a namespace for each binding. */
function bound(world, worker, v, module, node) {
  const env = { ...v.vars, ...v.secrets };
  for (const [name, cls] of Object.entries(v.objects)) {
    const stub = (id) => ({
      fetch: async (input, init) => {
        const key = `${worker.name}/${cls}/${id}`;
        let stored = world.objects.get(key);
        if (!stored) world.objects.set(key, (stored = { epoch: EPOCH, node: null, entries: new Map() }));
        let object = world.live.get(key);
        if (!object) {
          [stored.epoch, stored.node] = [stored.epoch + 1, node];
          const storage = {
            get: async (k) => (stored.entries.has(k) ? JSON.parse(stored.entries.get(k)) : undefined),
            put: async (k, value) => void stored.entries.set(k, JSON.stringify(value)),
            delete: async (k) => stored.entries.delete(k),
          };
          world.live.set(key, (object = new module[cls]({ id: { name: id, toString: () => id }, storage }, env)));
        }
        return object.fetch(input instanceof Request ? input : new Request(input, init));
      },
    });
    env[name] = { idFromName: (id) => id, get: stub };
  }
  return env;
}

/** `env` for a preview: the variables and none of the secrets; a binding is there, and a call of one fails. */
function previewEnv(v, extra) {
  const env = { ...(v?.vars ?? {}), ...extra };
  const refuse = async () => {
    throw new Error("a preview has no objects: deploy the version to call one");
  };
  for (const name of Object.keys(v?.objects ?? {})) env[name] = { idFromName: (id) => id, get: () => ({ fetch: refuse }) };
  return env;
}

// ---- HTTP -----------------------------------------------------------------------------------------------------

const worlds = new Map();

function target(ask) {
  const path = ask.path ?? "/";
  if (!path.startsWith("/")) throw invalid("a path starts with /");
  return { method: (ask.method ?? "GET").toUpperCase(), url: path, headers: ask.headers ?? {}, body: ask.body ?? "" };
}

async function route(world, method, parts, query, body, sse) {
  const [head, name, sub, a, b] = parts;
  const is = (m, ...shape) => m === method && shape.length === parts.length && shape.every((part, i) => part === "*" || part === parts[i]);

  if (is("GET", "templates")) return { templates: TEMPLATES };
  if (is("GET", "cluster")) {
    const nodes = NODES.map((node) => {
      const runner = node.roles.includes("runner");
      const serving = {};
      let in_flight = 0;
      for (const worker of world.workers.values()) {
        if (!worker.deployment) continue;
        const d = worker.deployment;
        serving[worker.name] = { seq: d.seq, stable: d.stable, canary: d.canary && { version: d.canary[0], percent: d.canary[1] }, warm: worker.rate > 0 || world.records.some((r) => r.own && r.worker === worker.name && r.node === node.id) };
        in_flight += totals(world, worker, node.id).in_flight;
      }
      return { ...node, url: node.url ?? null, available: true, reachable: true, serving: runner ? serving : null, in_flight: runner ? in_flight : null, logs_dropped: runner ? 0 : null };
    });
    return { node: "c1", nodes, silent: [] };
  }
  if (is("GET", "workers")) {
    const workers = [...world.workers.values()].sort((x, y) => x.name.localeCompare(y.name)).map((worker) => ({ ...workerJson(worker), metrics: totals(world, worker, null) }));
    return { workers, silent: [] };
  }
  if (is("POST", "workers")) {
    if (typeof body.name !== "string") throw invalid("a worker has a name");
    const chosen = body.source == null ? TEMPLATES.find((t) => t.name === (body.template ?? "hello")) : null;
    if (body.source == null && !chosen) throw invalid(`no template "${body.template}"`);
    if (world.workers.has(body.name)) throw new Refusal(409, "exists", `${body.name} exists: a new version of it is POST /api/workers/${body.name}/versions`);
    const objects = body.objects ?? (chosen && Object.keys(chosen.objects).length ? chosen.objects : undefined);
    const source = body.source ?? chosen.source;
    await load(source, objects ?? {});
    const made = change(world, body.name, { source, vars: body.vars, secrets: body.secrets, objects, message: body.message, deploy: body.deploy });
    return [201, { name: body.name, ...made }];
  }
  if (is("GET", "invocations")) return { invocations: recent(world, filterOf(query)) };
  if (is("GET", "invocations", "*")) {
    const found = recent(world, { trace: name, limit: 1 })[0];
    if (!found) throw notFound(`no invocation ${name} is kept`);
    return found;
  }
  if (is("GET", "tail")) return sse(filterOf(query));
  if (head !== "workers" || !name) throw notFound("no such route");

  if (is("GET", "workers", "*", "tail")) return sse(filterOf(query, name));
  if (is("POST", "workers", "*", "preview")) {
    const worker = world.workers.get(name);
    const from = worker && base(worker);
    const module = await load(body.source ?? "", from?.objects ?? {});
    const ran = await run(module, target(body.request ?? {}), previewEnv(from, body.env ?? {}));
    const { outcome: _, ...shown } = ran;
    return { ...shown, body_truncated: false, node: "c1" };
  }
  if (is("POST", "workers", "*", "versions")) {
    const worker = world.workers.get(name);
    const limits = body.cpu_ms != null || body.memory_mb != null ? { ...(body.cpu_ms != null && { cpu_ms: body.cpu_ms }), ...(body.memory_mb != null && { memory_mb: body.memory_mb }) } : undefined;
    if (body.source != null) await load(body.source, body.objects ?? (worker ? base(worker).objects : {}));
    return [201, change(world, name, { ...body, limits })];
  }
  const worker = get(world, name);

  if (is("GET", "workers", "*") || is("GET", "workers", "*", "versions")) return { ...workerJson(worker), versions: worker.versions.map(versionJson) };
  if (is("DELETE", "workers", "*")) {
    world.workers.delete(name);
    return { deleted: name };
  }
  if (is("GET", "workers", "*", "versions", "*")) {
    const v = version(worker, Number(a));
    return { ...versionJson(v), source: v.source, vars: v.vars, secrets: Object.keys(v.secrets), objects: v.objects };
  }
  if (is("POST", "workers", "*", "deploy")) {
    const canary = body.canary ? [body.canary.version, body.canary.percent] : null;
    return { seq: deploy(world, name, body.version, canary), version: body.version };
  }
  if (is("POST", "workers", "*", "rollback")) {
    const conflict = (why) => new Refusal(409, "nothing_to_roll_back", `${name} ${why}`);
    if (!worker.deployment) throw conflict("is not deployed");
    const stable = worker.deployment.stable;
    const older = worker.versions.map((v) => v.id).filter((id) => id < stable);
    const to = body?.version ?? (older.length ? Math.max(...older) : null);
    if (to == null) throw conflict("has no version older than the one deployed");
    return { seq: deploy(world, name, to, null), version: to, from: stable };
  }
  if (is("POST", "workers", "*", "invoke")) {
    const ask = target(body ?? {});
    const trace = hex(8);
    if (!worker.deployment) return { status: 503, headers: [["content-type", "application/json"]], body: JSON.stringify({ error: "unavailable", message: "no runner serves the worker now" }), node: null, version: null, trace, cpu_us: null, wall_us: null, error: "unavailable", retry_after_ms: 1000 };
    const d = worker.deployment;
    const v = version(worker, d.canary && Math.random() * 100 < d.canary[1] ? d.canary[0] : d.stable);
    const node = RUNNERS[Math.floor(dice(trace) * RUNNERS.length)];
    const module = await load(v.source, v.objects);
    const ran = await run(module, ask, bound(world, worker, v, module, node));
    record(world, { at_ms: Date.now(), worker: name, version: v.id, node, trace, method: ask.method, url: ask.url, status: ran.status, outcome: ran.outcome, cpu_us: ran.cpu_us, wall_us: ran.wall_us, logs: ran.logs, truncated: false, own: true });
    return { status: ran.status, headers: ran.headers, body: ran.outcome === "exception" ? `the worker ${ran.error}\n` : ran.body, node, version: v.id, trace, cpu_us: ran.cpu_us, wall_us: ran.wall_us, error: ran.outcome === "ok" ? null : ran.outcome, retry_after_ms: null };
  }
  if (is("GET", "workers", "*", "vars")) {
    const v = base(worker);
    const plain = Object.entries(v.vars).map(([key, value]) => ({ name: key, type: "plain", value }));
    return { version: v.id, vars: [...plain, ...Object.keys(v.secrets).map((key) => ({ name: key, type: "secret" }))] };
  }
  if (is("PUT", "workers", "*", "vars", "*")) {
    if (typeof body?.value !== "string") throw new Refusal(422, "invalid", "missing field `value`");
    const kind = body.type ?? "plain";
    if (kind !== "plain" && kind !== "secret") throw invalid(`a variable is plain or secret, not "${kind}"`);
    return change(world, name, kind === "plain" ? { set: { [a]: body.value }, message: `set ${a}` } : { secrets: { [a]: body.value }, message: `set secret ${a}` });
  }
  if (is("DELETE", "workers", "*", "vars", "*")) {
    const v = base(worker);
    if (!(a in v.vars) && !(a in v.secrets)) throw notFound(`${name} has no variable ${a}`);
    return change(world, name, { set: { [a]: null }, secrets: { [a]: null }, message: `removed ${a}` });
  }
  if (is("GET", "workers", "*", "metrics")) {
    const by_node = worker.deployment ? RUNNERS.map((node) => ({ ...totals(world, worker, node), node })).filter((row) => row.requests || worker.rate) : [];
    return { worker: name, total: totals(world, worker, null), by_node, silent: [], series: series(worker), sample_ms: SAMPLE_MS };
  }
  if (is("GET", "workers", "*", "invocations")) return { invocations: recent(world, filterOf(query, name)) };
  if (is("GET", "workers", "*", "objects")) {
    const objects = [...world.objects.entries()]
      .filter(([key]) => key.startsWith(`${name}/`))
      .sort(([x], [y]) => (x < y ? -1 : 1))
      .map(([key, o]) => {
        const [, cls, ...id] = key.split("/");
        return { stored: o.entries.size > 0, node: o.node, epoch: o.node ? o.epoch : null, class: cls, id: id.join("/") };
      })
      .filter((o) => o.stored || o.node);
    return { objects, silent: [] };
  }
  if (sub === "objects" && parts.length === 5) {
    const key = `${name}/${a}/${b}`;
    const stored = world.objects.get(key);
    if (method === "GET") {
      const entries = [...(stored?.entries ?? [])].map(([k, value]) => ({ key: k, value: JSON.parse(value) }));
      const bytes = [...(stored?.entries ?? [])].reduce((sum, [k, value]) => sum + k.length + value.length, 0);
      // The epoch of the last write: 0 for an object that stores nothing.
      return { class: a, id: b, epoch: entries.length ? stored.epoch : 0, bytes, entries };
    }
    if (method === "DELETE") {
      // The store forgets it; the actor is fenced, and its next request starts from nothing.
      // The store forgets it. An object that runs stays on its runner, and in the list, until it is idle.
      if (stored) stored.entries = new Map();
      world.live.delete(key);
      return { cleared: { class: a, id: b } };
    }
  }
  throw notFound("no such route");
}

const send = (res, status, body, headers = {}) => {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store", ...headers });
  res.end(JSON.stringify(body));
};

/** Answers a request under `/api/`. Returns false for any other path. */
export async function handle(req, res) {
  const url = new URL(req.url, "http://mock");
  if (!url.pathname.startsWith("/api/")) return false;
  const parts = url.pathname.slice(5).split("/").filter(Boolean).map(decodeURIComponent);
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  let body;
  try {
    body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : undefined;
  } catch (e) {
    return send(res, 400, { error: "invalid", message: `the body is not JSON: ${e.message}` }), true;
  }
  const session = (req.headers.cookie ?? "").split(";").map((pair) => pair.trim().split("=")).find(([key]) => key === COOKIE)?.[1];
  const path = parts.join("/");

  if (req.method === "POST" && path === "login") {
    if (body?.password !== PASSWORD) return send(res, 401, { error: "unauthorized", message: "wrong password" }), true;
    const expires_ms = Date.now() + 12 * 3_600_000;
    const sealed = `${expires_ms}.${hex(16)}`;
    worlds.set(sealed, newWorld());
    return send(res, 200, { expires_ms }, { "set-cookie": `${COOKIE}=${sealed}; Path=/; Max-Age=43200; HttpOnly; SameSite=Strict` }), true;
  }
  const world = session && worlds.get(session);
  if (!world) return send(res, 401, { error: "unauthorized", message: "sign in first: POST /api/login" }), true;
  if (req.method === "GET" && path === "session") return send(res, 200, { expires_ms: Number(session.split(".")[0]) }), true;
  if (req.method === "POST" && path === "logout") {
    worlds.delete(session);
    return send(res, 200, {}, { "set-cookie": `${COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict` }), true;
  }

  const sse = (filter) => {
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    res.write(": connected\n\n");
    const tail = { filter, send: (event, data) => res.write(`event: ${event}\ndata: ${data}\n\n`) };
    world.tails.add(tail);
    // The steady load goes on while somebody listens.
    let n = 1000;
    const load = setInterval(() => {
      const busy = [...world.workers.values()].filter((worker) => worker.rate && worker.deployment && (!filter.worker || filter.worker === worker.name));
      if (busy.length) record(world, synthetic(world, busy[n % busy.length].name, Date.now(), n++));
    }, 700);
    req.on("close", () => {
      clearInterval(load);
      world.tails.delete(tail);
    });
    return sse;
  };
  try {
    const answer = await route(world, req.method, parts, url.searchParams, body, sse);
    if (answer === sse) return true;
    const [status, json] = Array.isArray(answer) ? answer : [200, answer];
    send(res, status, json);
  } catch (e) {
    if (e instanceof Refusal) send(res, e.status, { error: e.code, message: e.message });
    else send(res, 500, { error: "internal", message: String(e?.stack ?? e) });
  }
  return true;
}
