// A stand-in for an api node (../../src/api.rs), for the console's development and tests.
// Every body has the shape api.rs gives. Each session gets a world of its own, so tests do not meet.
import { randomBytes } from "node:crypto";

export const PASSWORD = "wf";
const COOKIE = "wf_session";
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;
/** A lease's epoch is a large number, not a count from 1. */
const EPOCH = 1_791_093_947_000_000;
/** The versions of each workflow in "this build". */
const BUILD = { checkout: [1, 2], onboarding: [1], batch: [1], item: [1], nine_steps: [1], one_step: [1], sleeper: [1] };
const FINISHED = ["completed", "failed", "cancelled", "terminated"];
/** How many index rows a list reads at most before it stops (`cut`). */
const SCAN = 10_000;

class Refusal extends Error {
  constructor(status, code, message) {
    super(message);
    Object.assign(this, { status, code });
  }
}
const notFound = (id) => new Refusal(404, "not_found", `no run ${id}`);
const invalid = (why) => new Refusal(400, "invalid", why);
const us = (ms) => Math.round(ms * 1000);

// ---- runs -----------------------------------------------------------------------------------------------------

/** Builds a run's steps and events in journal order, each entry after the one before in time. */
function journal(started_ms, owner) {
  const j = { seq: 1, at: started_ms, steps: [], events: [] };
  j.event = (kind, text, after = 0) => {
    j.at += after;
    j.events.push({ seq: j.seq++, at_us: us(j.at), kind, text });
    return j;
  };
  j.step = (kind, name, status, { took = 120, input = null, output = null, error = null, attempts = 1, wake_ms = null } = {}) => {
    const start = j.at;
    const ends = ["done", "failed", "timed_out"].includes(status);
    if (ends) j.at += took;
    j.steps.push({ step: j.steps.length, kind, name, status, attempts, wake_at_us: wake_ms && us(wake_ms), error, output, input, seq: j.seq, started_us: us(start), ended_us: ends ? us(j.at) : null });
    j.seq += ends ? 2 : 1;
    j.at += 15;
    return j;
  };
  j.event("opened", owner);
  return j;
}

function makeRun(world, { id, workflow, version = 1, status = "running", input = {}, tags = [], parent = null, owner = "e1", opened = owner, started_ms, build, output = null, error = null, parked = null, custom_status = null }) {
  const j = journal(started_ms, opened);
  build?.(j);
  if (FINISHED.includes(status) && !j.events.some((e) => e.kind === status)) j.event(status, error ?? "", 40);
  const run = { id, workflow, version, status, input, tags, parent, owner: FINISHED.includes(status) ? "" : owner, epoch: EPOCH + world.runs.size, started_us: us(started_ms), steps: j.steps, events: j.events, output, error, parked, custom_status, updated_ms: j.at, signals: new Set() };
  world.runs.set(id, run);
  return run;
}

const due = (run) => run.steps.filter((s) => s.wake_at_us != null && !s.ended_us).map((s) => s.wake_at_us / 1000)[0] ?? null;

/** A row of the list, as the index gives it. */
function row(run) {
  const due_ms = run.status === "running" ? due(run) : null;
  const waiting = run.status !== "running" ? null : due_ms ? "timer" : run.steps.some((s) => s.status === "waiting") ? "call" : null;
  return {
    id: run.id, workflow: run.workflow, version: run.version, status: run.status, tags: run.tags, parent: run.parent,
    started_ms: Math.round(run.started_us / 1000), updated_ms: run.updated_ms, due_ms, waiting,
    owner: run.status === "running" && !waiting ? run.owner : null, parked_version: run.parked != null && !BUILD[run.workflow]?.includes(run.version),
  };
}

/** The run as `GET /api/runs/:id` gives it. */
function view(run) {
  const { signals: _s, input: _i, updated_ms: _u, finish_at: _f, ...shown } = run;
  return { ...shown, due_us: run.status === "running" && due(run) ? us(due(run)) : null };
}

/** The journal as the history nodes keep it, built from the steps: an entry for each start and end. */
function history(run) {
  const entries = [{ seq: 0, entry: { Started: { workflow: run.workflow, version: run.version, input: run.input, tags: run.tags, parent: run.parent, run_timeout_ms: null, at_us: run.started_us } } }];
  for (const s of run.steps) {
    const at = (kind, body) => entries.push({ seq: kind === "end" ? s.seq + 1 : s.seq, entry: body });
    if (s.kind === "activity") {
      at("start", { ActivityScheduled: { step: s.step, name: s.name, queue: s.name === "charge" ? "payments" : "local", input: s.input, at_us: s.started_us } });
      if (s.ended_us) at("end", s.error && s.status === "failed" ? { ActivityFailed: { step: s.step, attempt: s.attempts, error: s.error, retry_at_us: null, at_us: s.ended_us } } : { ActivityDone: { step: s.step, attempt: s.attempts, output: s.output, at_us: s.ended_us } });
    } else if (s.kind === "sleep") {
      at("start", { TimerStarted: { step: s.step, wake_at_us: s.wake_at_us, at_us: s.started_us } });
      if (s.ended_us) at("end", { TimerFired: { step: s.step } });
    } else if (s.kind === "child") {
      at("start", { ChildStarted: { step: s.step, id: s.name, workflow: "item", at_us: s.started_us } });
      if (s.ended_us) at("end", { ChildDone: { step: s.step, result: { Ok: s.output }, at_us: s.ended_us } });
    } else if (s.kind === "signal") {
      at("start", { SignalWait: { step: s.step, name: s.name } });
      if (s.ended_us) at("end", { SignalConsumed: { step: s.step, name: s.name, payload: s.output, at_us: s.ended_us } });
    }
  }
  for (const e of run.events.filter((e) => !["opened", "reopened", "moved"].includes(e.kind))) entries.push({ seq: e.seq, entry: { [e.kind[0].toUpperCase() + e.kind.slice(1)]: { text: e.text, at_us: e.at_us } } });
  return entries.sort((a, b) => a.seq - b.seq).map((e) => e.entry);
}

/** A run the console started: its one activity runs a second, then the run completes. */
function settle(run, now = Date.now()) {
  if (!run.finish_at || now < run.finish_at || run.status !== "running") return;
  delete run.finish_at;
  for (const s of run.steps.filter((s) => !s.ended_us)) Object.assign(s, { status: "done", ended_us: us(now), output: s.output ?? { ok: true } });
  run.status = "completed";
  run.output = { ok: true };
  run.owner = "";
  run.updated_ms = now;
  run.events.push({ seq: nextSeq(run), at_us: us(now), kind: "completed", text: "" });
}

const nextSeq = (run) => Math.max(0, ...run.steps.map((s) => s.seq + 1), ...run.events.map((e) => e.seq)) + 1;
const addEvent = (run, kind, text = "") => {
  run.events.push({ seq: nextSeq(run), at_us: us(Date.now()), kind, text });
  run.updated_ms = Date.now();
};

// ---- a world: what one session sees ---------------------------------------------------------------------------

function newWorld() {
  const world = { runs: new Map(), started: Date.now() - 2 * HOUR };
  const now = Date.now();
  const order = { sku: "mug", qty: 2, account: "acct-7", parcels: [{ to: "Oslo" }] };

  // Waits for its approval. Its owner e1 crashed after the charge; e2 took the run.
  makeRun(world, {
    id: "checkout-1042", workflow: "checkout", input: order, tags: [["team", "shop"], ["env", "prod"]], owner: "e2", opened: "e1", started_ms: now - 25 * MINUTE,
    build: (j) =>
      j
        .step("activity", "reserve", "done", { input: { sku: "mug", qty: 2 }, output: { id: "hold-1042-0", cents: 500 }, took: 80 })
        .step("activity", "charge", "done", { input: { account: "acct-7", cents: 500, ms: 0 }, output: { charge: "ch-1042-1", cents: 500 }, attempts: 2, error: "timeout: the bank did not answer", took: 4_200 })
        .event("moved", "e2", 90_000)
        .step("signal", "approve", "waiting"),
  });
  makeRun(world, {
    id: "checkout-1040", workflow: "checkout", status: "failed", input: { ...order, account: "declined" }, tags: [["team", "shop"], ["env", "prod"]], started_ms: now - 50 * MINUTE, error: "card_declined: the bank said no",
    build: (j) =>
      j
        .step("activity", "reserve", "done", { input: { sku: "mug", qty: 2 }, output: { id: "hold-1040-0", cents: 500 } })
        .step("activity", "charge", "failed", { input: { account: "declined", cents: 500, ms: 0 }, error: "card_declined: the bank said no", took: 300 })
        .step("activity", "release", "done", { input: { hold: "hold-1040-0" }, output: null }),
  });
  makeRun(world, {
    id: "checkout-0999", workflow: "checkout", version: 3, status: "parked", input: order, tags: [["team", "shop"]], started_ms: now - 3 * DAY, parked: "version 3 of checkout is not in this build",
    build: (j) => j.step("activity", "reserve", "done", { output: { id: "hold-0999-0", cents: 500 } }).event("parked", "version 3 of checkout is not in this build", 1000),
  });
  makeRun(world, {
    id: "onboarding-ana", workflow: "onboarding", input: { user: "ana" }, tags: [["team", "growth"]], started_ms: now - 26 * HOUR, custom_status: { follow_up_s: 259200 },
    build: (j) => j.step("activity", "welcome", "done", { input: { user: "ana", day: 0 }, output: "mail of day 0 sent to ana" }).step("sleep", "", "waiting", { wake_ms: now - 26 * HOUR + 3 * DAY }),
  });
  makeRun(world, {
    id: "onboarding-old", workflow: "onboarding", input: { user: "old" }, tags: [["team", "growth"]], started_ms: now - 9 * DAY, custom_status: { follow_up_s: 2592000 },
    build: (j) => j.step("activity", "welcome", "done", { output: "mail of day 0 sent to old" }).step("sleep", "", "waiting", { wake_ms: now + 21 * DAY }),
  });
  makeRun(world, { id: "sleeper-paused", workflow: "sleeper", status: "paused", input: { secs: 600 }, started_ms: now - 12 * MINUTE, build: (j) => j.step("sleep", "", "waiting", { wake_ms: now + 8 * MINUTE }).event("paused", "", 30_000) });

  // A batch and its children: child ids are the parent's and the step's.
  const batch = makeRun(world, {
    id: "batch-7", workflow: "batch", input: { count: 12 }, tags: [["team", "data"]], started_ms: now - 40 * MINUTE,
    build: (j) => {
      for (let n = 0; n < 12; n++) j.step("child", `batch-7/${n}`, n < 9 ? "done" : n === 11 ? "failed" : "running", { input: n, output: n < 9 ? n * n : null, error: n === 11 ? "square: the worker stopped" : null, took: 2000 + n * 300 });
    },
  });
  // The children run side by side.
  batch.steps.forEach((s, n) => {
    s.started_us = batch.started_us + us(200);
    if (s.ended_us) s.ended_us = s.started_us + us(2000 + n * 300);
  });
  for (let n = 0; n < 12; n++) {
    makeRun(world, {
      id: `batch-7/${n}`, workflow: "item", parent: "batch-7", input: n, started_ms: now - 40 * MINUTE + 200, status: n < 9 ? "completed" : n === 11 ? "failed" : "running", output: n < 9 ? n * n : null, error: n === 11 ? "square: the worker stopped" : null,
      build: (j) => j.step("activity", "square", n < 9 ? "done" : n === 11 ? "failed" : "running", { input: { n }, output: n < 9 ? n * n : null, error: n === 11 ? "square: the worker stopped" : null, took: 2000 + n * 300 }),
    });
  }

  // A long run: ten thousand steps.
  makeRun(world, {
    id: "noop-10k", workflow: "nine_steps", status: "completed", input: null, tags: [["team", "perf"]], started_ms: now - 3 * HOUR, output: 49995000,
    build: (j) => {
      for (let i = 0; i < 10_000; i++) j.step("activity", "noop", "done", { input: { i }, output: i, took: 3 });
    },
  });

  // Many short runs, for the list's pages and filters.
  const kinds = ["one_step", "sleeper", "onboarding", "checkout"];
  for (let i = 0; i < 140; i++) {
    const workflow = kinds[i % kinds.length];
    const status = i % 23 === 0 ? "cancelled" : i % 31 === 0 ? "terminated" : i % 17 === 0 ? "failed" : i % 9 === 0 ? "running" : "completed";
    makeRun(world, {
      id: `${workflow}-${String(i).padStart(4, "0")}`, workflow, status, input: {}, tags: [["team", i % 2 ? "shop" : "growth"], ["env", i % 5 ? "prod" : "test"]], started_ms: now - i * 31 * MINUTE - 5 * MINUTE,
      error: status === "failed" ? "noop: the worker stopped" : status === "terminated" ? "stopped by hand" : null, output: status === "completed" ? i : null,
      build: (j) => j.step("activity", workflow === "sleeper" ? "noop" : "welcome", status === "running" ? "running" : status === "failed" ? "failed" : "done", { output: status === "completed" ? i : null, error: status === "failed" ? "noop: the worker stopped" : null }),
    });
  }
  return world;
}

const worlds = new Map();

// ---- the routes -----------------------------------------------------------------------------------------------

function duration(text) {
  const m = /^(\d+)(ms|s|m|h|d)?$/.exec(text ?? "");
  if (!m) throw invalid("since is a duration such as 30s, 5m, 2h or 7d");
  return Number(m[1]) * { ms: 1, s: 1000, m: MINUTE, h: HOUR, d: DAY, undefined: 1 }[m[2]];
}

function list(world, query) {
  const known = ["workflow", "status", "tag", "parent", "prefix", "since", "since_ms", "cursor", "limit"];
  const unknown = [...query.keys()].find((k) => !known.includes(k));
  if (unknown) throw invalid(`no filter ${unknown}: ${known.join(", ")}`);
  if (query.has("prefix") && query.has("parent")) throw invalid("prefix or parent, not both");
  const status = query.get("status");
  if (status && !["running", "paused", "parked", ...FINISHED].includes(status)) throw invalid("status is running, paused, parked, completed, failed, cancelled or terminated");
  const tag = query.get("tag");
  if (tag && !tag.includes(":")) throw invalid("tag is key:value");
  const limit = Number(query.get("limit") ?? 50);
  if (!(limit >= 1 && limit <= 1000)) throw invalid("limit is 1 to 1000");
  const prefix = query.get("prefix") ?? (query.has("parent") ? `${query.get("parent")}/` : null);
  const since = query.has("since") ? Date.now() - duration(query.get("since")) : null;
  let runs = [...world.runs.values()].map((run) => (settle(run), row(run)));
  runs = runs.filter((r) => (!query.get("workflow") || r.workflow === query.get("workflow")) && (!status || r.status === status) && (!prefix || r.id.startsWith(prefix)) && (since == null || r.updated_ms >= since) && (!tag || r.tags.some(([k, v]) => `${k}:${v}` === tag)));
  runs.sort(prefix ? (a, b) => (a.id < b.id ? -1 : 1) : (a, b) => b.updated_ms - a.updated_ms || (a.id < b.id ? -1 : 1));
  const from = Number(query.get("cursor") ?? 0);
  const page = runs.slice(from, from + limit);
  // The index is larger than this world: a tag filter with no time bound reads past 10,000 rows and stops.
  const cut = !!tag && since == null;
  return { runs: page, next: from + limit < runs.length ? String(from + limit) : null, cut, replica: "h1" };
}

function workflows(world) {
  const out = new Map(Object.entries(BUILD).map(([name, versions]) => [name, { name, versions, runs: {}, old_version: 0, missing_version: 0 }]));
  for (const run of world.runs.values()) {
    const w = out.get(run.workflow) ?? out.set(run.workflow, { name: run.workflow, versions: [], runs: {}, old_version: 0, missing_version: 0 }).get(run.workflow);
    w.runs[run.status] = (w.runs[run.status] ?? 0) + 1;
    if (FINISHED.includes(run.status)) continue;
    if (BUILD[run.workflow] && run.version < Math.max(...BUILD[run.workflow])) w.old_version++;
    if (!BUILD[run.workflow]?.includes(run.version)) w.missing_version++;
  }
  return { workflows: [...out.values()].sort((a, b) => (a.name < b.name ? -1 : 1)), counted: true };
}

function metrics(world) {
  const now = Date.now();
  const points = [];
  // A point a minute: rates per minute.
  for (let at = Math.max(world.started, now - HOUR); at <= now; at += MINUTE) {
    const wave = Math.sin(at / (7 * MINUTE)) * 4;
    points.push({ at_ms: at, started: Math.max(0, 22 + wave), completed: Math.max(0, 20 + wave), failed: at % 5 === 0 ? 1 : 0.2 });
  }
  const rows = [...world.runs.values()].map(row);
  const n = (status) => rows.filter((r) => r.status === status).length;
  const sleeping = rows.filter((r) => r.status === "running" && r.waiting === "timer").length;
  return {
    points, since_ms: world.started,
    counts: { running: n("running") - sleeping, sleeping, paused: n("paused"), parked: n("parked"), failed: n("failed"), failed_hour: rows.filter((r) => r.status === "failed" && r.updated_ms > now - HOUR).length, old: rows.filter((r) => !FINISHED.includes(r.status) && r.updated_ms < now - 7 * DAY).length, stuck: 0 },
  };
}

const QUEUES = () => ({
  queues: [
    { name: "local", backlog: 0, backlog_bytes: 0, oldest_ms: 0, pollers: 6, refused: 0, handed: 18_204, workers: ["w2"], partitions_missing: 0, slots: 8, running: 1, failed_minute: 0 },
    { name: "payments", backlog: 14, backlog_bytes: 2_310, oldest_ms: 95_000, pollers: 0, refused: 3, handed: 4_017, workers: ["w1", "w2"], partitions_missing: 0, slots: 6, running: 6, failed_minute: 4 },
  ],
});
const WORKERS = { workers: [
  { node: "w1", queues: ["payments"], slots: { payments: 4 }, available: true, draining: false, running: { payments: 4 }, failed_minute: { payments: 4 }, last_poll_ms: { payments: 41_000 } },
  { node: "w2", queues: ["payments", "local"], slots: { payments: 2, local: 8 }, available: true, draining: true, running: { payments: 2, local: 1 }, failed_minute: { payments: 0, local: 0 }, last_poll_ms: { payments: 0, local: 0 } },
] };
const CLUSTER = {
  replicas: [{ node: "h1", up: true }, { node: "h2", up: true }, { node: "h3", up: false }],
  quorum: true, warming: false, isolated: false,
  members: [
    { node: "h1", roles: ["history"], available: true },
    { node: "h2", roles: ["history"], available: true },
    { node: "e1", roles: ["engine", "api"], available: true },
    { node: "e2", roles: ["engine", "api"], available: true },
    { node: "w1", roles: ["worker"], available: true },
    { node: "w2", roles: ["worker"], available: false },
  ],
};

function start(world, body) {
  if (!body?.workflow) throw new Refusal(422, "invalid", "missing field `workflow`");
  const versions = BUILD[body.workflow];
  if (!versions) throw new Refusal(422, "invalid", `this build has no workflow ${body.workflow}`);
  const version = body.version ?? Math.max(...versions);
  if (!versions.includes(version)) throw new Refusal(422, "invalid", `this build has no version ${version} of ${body.workflow}`);
  const id = body.id || `run-${randomBytes(8).toString("hex")}`;
  if (world.runs.has(id)) return [200, { id, version: world.runs.get(id).version, created: false }];
  const run = makeRun(world, { id, workflow: body.workflow, version, input: body.input ?? null, tags: body.tags ?? [], started_ms: Date.now(), build: (j) => j.step("activity", "noop", "running", { input: body.input ?? null }) });
  run.finish_at = Date.now() + 1500;
  return [201, { id, version, created: true }];
}

function act(run, what, body) {
  const ended = FINISHED.includes(run.status);
  const refuse = (why) => new Refusal(409, "conflict", why);
  if (what === "pause") {
    if (run.status !== "running") throw refuse(`the run is ${run.status}`);
    run.status = "paused";
    addEvent(run, "paused");
  } else if (what === "resume") {
    if (run.status !== "paused" && run.status !== "parked") throw refuse(`the run is ${run.status}`);
    if (run.status === "parked" && !BUILD[run.workflow]?.includes(run.version)) throw refuse(`this build has no version ${run.version} of ${run.workflow}`);
    run.status = "running";
    run.parked = null;
    addEvent(run, "resumed");
  } else if (what === "retry") {
    if (run.status !== "failed") throw refuse(`the run is ${run.status}: only a failed run takes a retry`);
    const failed = run.steps.find((s) => s.status === "failed");
    run.events = run.events.filter((e) => e.kind !== "failed");
    run.status = "running";
    run.error = null;
    run.owner = "e1";
    addEvent(run, "retried", String(failed?.step ?? 0));
    if (failed) Object.assign(failed, { status: "running", attempts: failed.attempts + 1, ended_us: null, error: null, seq: nextSeq(run) });
  } else if (what === "cancel" || what === "terminate") {
    if (ended) throw refuse(`the run is ${run.status}`);
    const reason = body?.reason ?? "";
    if (what === "cancel") addEvent(run, "cancel", reason);
    run.status = what === "cancel" ? "cancelled" : "terminated";
    run.error = what === "terminate" ? reason : null;
    run.owner = "";
    addEvent(run, run.status, reason);
  }
  return [202, {}];
}

function signal(run, name, body) {
  if (FINISHED.includes(run.status)) throw new Refusal(409, "conflict", `the run is ${run.status}`);
  const id = body?.signal_id || `sig-${randomBytes(8).toString("hex")}`;
  if (run.signals.has(id)) return [202, { signal_id: id, duplicate: true }];
  run.signals.add(id);
  addEvent(run, "signal", name);
  const waits = run.steps.find((s) => s.kind === "signal" && s.name === name && s.status === "waiting");
  if (waits) {
    Object.assign(waits, { status: "done", ended_us: us(Date.now()), output: body?.payload ?? null });
    // The checkout ships after its approval.
    if (run.workflow === "checkout") {
      run.steps.push({ step: run.steps.length, kind: "activity", name: "ship", status: "running", attempts: 1, wake_at_us: null, error: null, output: null, input: { to: "Oslo" }, seq: nextSeq(run), started_us: us(Date.now()), ended_us: null });
      run.finish_at = Date.now() + 1500;
    }
  }
  return [202, { signal_id: id, duplicate: false }];
}

function route(world, method, parts, query, body) {
  const is = (m, ...shape) => m === method && shape.length === parts.length && shape.every((s, i) => s === "*" || s === parts[i]);
  if (is("GET", "workflows")) return workflows(world);
  if (is("GET", "queues")) return QUEUES();
  if (is("GET", "workers")) return WORKERS;
  if (is("GET", "cluster")) return CLUSTER;
  if (is("GET", "metrics")) return metrics(world);
  if (is("GET", "runs")) return list(world, query);
  if (is("POST", "runs")) return start(world, body);
  if (parts[0] !== "runs" || parts.length < 2) throw new Refusal(404, "not_found", "no such route");
  const id = parts[1];
  const run = world.runs.get(id);
  if (!run) throw notFound(id);
  settle(run);
  if (is("GET", "runs", "*")) return view(run);
  if (is("GET", "runs", "*", "history")) {
    const after = Number(query.get("after") ?? 0);
    const limit = Number(query.get("limit") ?? 100);
    const entries = history(run);
    return { entries: entries.slice(after, after + limit), next: after + limit < entries.length ? after + limit : null };
  }
  if (is("DELETE", "runs", "*")) {
    if (!FINISHED.includes(run.status)) throw new Refusal(409, "conflict", `the run is ${run.status}: cancel or terminate it first`);
    world.runs.delete(id);
    return [202, {}];
  }
  if (is("POST", "runs", "*", "signals", "*")) return signal(run, parts[3], body);
  if (method === "POST" && parts.length === 3) return act(run, parts[2], body);
  throw new Refusal(404, "not_found", "no such route");
}

const send = (res, status, body, headers = {}) => {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store", ...headers });
  res.end(JSON.stringify(body));
};

/** Answers a request under `/api/` and `GET /ready`. Returns false for any other path. */
export async function handle(req, res) {
  const url = new URL(req.url, "http://mock");
  if (url.pathname === "/ready") return send(res, 200, { journal: true, warming: false, isolated: false, quotas_starting: false }), true;
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
  const world = session && worlds.get(session);
  const path = parts.join("/");

  if (req.method === "POST" && path === "login") {
    if (body?.password !== PASSWORD) return send(res, 401, { error: "unauthorized", message: "wrong password" }), true;
    const expires_ms = Date.now() + 12 * HOUR;
    const sealed = `${expires_ms}.${randomBytes(16).toString("hex")}`;
    worlds.set(sealed, newWorld());
    return send(res, 200, { expires_ms }, { "set-cookie": `${COOKIE}=${sealed}; Path=/; Max-Age=43200; HttpOnly; SameSite=Strict` }), true;
  }
  if (req.method === "GET" && path === "session") return send(res, 200, world ? { name: "admin", scope: "admin", expires_ms: Number(session.split(".")[0]) } : { name: null, scope: null, expires_ms: null }), true;
  if (req.method === "POST" && path === "logout") {
    worlds.delete(session);
    return send(res, 200, {}, { "set-cookie": `${COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict` }), true;
  }
  if (!world) return send(res, 401, { error: "unauthorized", message: "sign in first: POST /api/login" }), true;
  try {
    const answer = route(world, req.method, parts, url.searchParams, body);
    const [status, json] = Array.isArray(answer) ? answer : [200, answer];
    send(res, status, json);
  } catch (e) {
    if (e instanceof Refusal) send(res, e.status, { error: e.code, message: e.message });
    else send(res, 500, { error: "internal", message: String(e?.stack ?? e) });
  }
  return true;
}
