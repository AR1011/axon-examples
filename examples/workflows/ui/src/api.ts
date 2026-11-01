/** Where the console is mounted, from the page's `<base href>`: "/" or for example "/ops/wf/". */
export const base = new URL(document.baseURI).pathname.replace(/[^/]*$/, "");
/** Where the API is, from `<meta name="wf-api">`: "/api/" unless the server that serves the page says otherwise. */
export const api = (document.querySelector<HTMLMetaElement>('meta[name="wf-api"]')?.content || "/api/").replace(/\/*$/, "/");
/** `GET /ready` is beside the API, with no session. */
export const readyUrl = api.replace(/api\/$/, "") + "ready";

/** A refusal as the API says it: the status, a code to act on, and a sentence to read. */
export class Refusal extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

/** Back to the login page, which returns here after the sign-in. */
export function toLogin() {
  const here = location.pathname.slice(base.length) + location.search;
  if (here.startsWith("login")) return;
  location.assign(`${base}login${here ? `?next=${encodeURIComponent(here)}` : ""}`);
}

export async function call<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  let answer: Response;
  try {
    answer = await fetch(api + path, {
      method,
      signal,
      credentials: "same-origin",
      headers: body === undefined ? undefined : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    if (signal?.aborted) throw e;
    throw new Refusal(0, "offline", "The browser cannot connect to the api node.");
  }
  if (answer.ok) return (await answer.json()) as T;
  // A refusal of the API is `{error, message}`. A body the API could not read is refused with a line of text.
  const text = await answer.text().catch(() => "");
  const said = ((): { error?: unknown; message?: unknown } | null => {
    try {
      return JSON.parse(text) as { error?: unknown; message?: unknown } | null;
    } catch {
      return null; // not JSON: the text is the message
    }
  })();
  const refusal = new Refusal(answer.status, typeof said?.error === "string" ? said.error : "error", typeof said?.message === "string" ? said.message : (!said && text.trim().slice(0, 300)) || answer.statusText || "The request failed.");
  if (answer.status === 401 && path !== "login") toLogin();
  throw refusal;
}

export const get = <T>(path: string, signal?: AbortSignal) => call<T>("GET", path, undefined, signal);
/** One path segment: a run id may hold a slash. */
export const seg = encodeURIComponent;
export const runPath = (id: string, rest = "") => `runs/${seg(id)}${rest}`;

export const STATUSES = ["running", "paused", "parked", "completed", "failed", "cancelled", "terminated"] as const;
export type RunStatus = (typeof STATUSES)[number];
export const finished = (status: RunStatus) => ["completed", "failed", "cancelled", "terminated"].includes(status);

/** A row of `GET /api/runs`, from a history replica's index. */
export interface RunRow {
  id: string;
  workflow: string;
  version: number;
  status: RunStatus;
  tags: [string, string][];
  parent: string | null;
  started_ms: number;
  /** Its last entry. For a finished run, when it ended. */
  updated_ms: number;
  /** Its next durable timer: a sleep's end, or an activity's next attempt. */
  due_ms: number | null;
  /** What a running run waits for: a timer, or a call (a signal, a result) with no timer. */
  waiting: "timer" | "call" | null;
  owner: string | null;
  parked_version: boolean;
}

export interface RunList {
  runs: RunRow[];
  next: string | null;
  /** The replica stopped at 10,000 rows it scanned. */
  cut: boolean;
  replica: string;
}

/** A step of the run's code, from its journal. Times are µs of the wall clock of the node that journaled them. */
export interface Step {
  step: number;
  kind: "activity" | "sleep" | "signal" | "child" | "now" | "random" | "uuid";
  name: string;
  status: "running" | "retrying" | "waiting" | "done" | "failed" | "timed_out";
  attempts: number;
  wake_at_us: number | null;
  error: string | null;
  output: unknown;
  input: unknown;
  /** The index of its first journal entry: steps and events show in this order. */
  seq: number;
  started_us: number | null;
  ended_us: number | null;
}

/** What the run went through besides its steps. */
export interface RunEvent {
  seq: number;
  at_us: number | null;
  kind: "opened" | "reopened" | "moved" | "signal" | "cancel" | "paused" | "resumed" | "retried" | "parked" | "completed" | "failed" | "cancelled" | "terminated";
  /** The node, the signal's name, the reason or the error. */
  text: string;
}

export interface Run {
  id: string;
  workflow: string;
  version: number;
  status: RunStatus;
  output: unknown;
  error: string | null;
  parked: string | null;
  due_us: number | null;
  custom_status: unknown;
  tags: [string, string][];
  parent: string | null;
  owner: string;
  epoch: number;
  started_us: number | null;
  steps: Step[];
  events: RunEvent[];
}

export interface History {
  entries: unknown[];
  /** Where the next page starts, or null at the end. */
  next: number | null;
}

export interface Workflow {
  name: string;
  /** The versions this build has. */
  versions: number[];
  runs: Partial<Record<RunStatus, number>>;
  /** Unfinished runs on a version before the latest. */
  old_version: number;
  /** Unfinished runs on a version this build does not have. */
  missing_version: number;
}

export interface WorkflowList {
  workflows: Workflow[];
  /** False when no replica answered: the counts are not there. */
  counted: boolean;
}

export interface Queue {
  name: string;
  backlog: number;
  backlog_bytes: number;
  oldest_ms: number;
  pollers: number;
  refused: number;
  handed: number;
  workers: string[];
  partitions_missing: number;
  /** Over the workers that answered: their slots for the queue, the slots in use, and attempts failed in the last minute. */
  slots: number;
  running: number;
  failed_minute: number;
}

export interface Worker {
  node: string;
  queues: string[];
  slots: Record<string, number>;
  available: boolean;
  draining: boolean;
  /** Per queue, from the worker itself; a queue whose worker did not answer is not there. */
  running: Record<string, number>;
  failed_minute: Record<string, number>;
  /** Since the last poll came back: 0 while one is out. */
  last_poll_ms: Record<string, number>;
}

export interface Point {
  at_ms: number;
  /** Per minute. */
  started: number;
  completed: number;
  failed: number;
}

export interface Counts {
  running: number;
  sleeping: number;
  paused: number;
  parked: number;
  failed: number;
  failed_hour: number;
  /** Unfinished and waiting over 7 days. */
  old: number;
  stuck: number;
}

export interface Metrics {
  points: Point[];
  /** When this api node began to collect. */
  since_ms: number;
  counts: Counts | null;
}

export interface Cluster {
  replicas: { node: string; up: boolean }[];
  quorum: boolean;
  warming: boolean;
  isolated: boolean;
  members: { node: string; roles: string[]; available: boolean }[];
}

export interface Ready {
  journal: boolean;
  warming: boolean;
  isolated: boolean;
  quotas_starting: boolean;
}

/** `GET /ready`: 200 or 503, both with the body. */
export async function ready(signal?: AbortSignal): Promise<Ready> {
  const answer = await fetch(readyUrl, { signal, credentials: "same-origin" });
  return (await answer.json()) as Ready;
}
