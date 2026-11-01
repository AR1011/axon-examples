/** Where the console is mounted, from the page's `<base href>`: "/" or for example "/ops/edge/". */
export const base = new URL(document.baseURI).pathname.replace(/[^/]*$/, "");
/** Where the admin API is, from `<meta name="edge-api">`: "/api/" unless the server that serves the page says otherwise. */
export const api = (document.querySelector<HTMLMetaElement>('meta[name="edge-api"]')?.content || "/api/").replace(/\/*$/, "/");

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
    throw new Refusal(0, "offline", "The browser cannot connect to the control node.");
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
/** One path segment: an object's id may hold a slash. */
export const seg = encodeURIComponent;

/** The collection of Workers. */
export const WORKERS = "workers";
/** The path of one Worker, and of what is below it. */
export const workerPath = (name: string, rest = "") => `${WORKERS}/${seg(name)}${rest}`;
/** The role of a node that runs Workers. */
export const RUNNER = "runner";

export interface Limits {
  cpu_ms: number;
  wall_ms: number;
  memory_mb: number;
  subrequests: number;
}

export interface VersionInfo {
  id: number;
  created_ms: number;
  message: string;
  /** The hash of the bundle. */
  bundle: string;
  /** The length of the source. */
  source_bytes: number;
  /** The length of what the engine built from the source. */
  build_bytes: number;
  limits: Limits;
}

export interface VersionDetail extends VersionInfo {
  source: string;
  vars: Record<string, string>;
  /** Names only: no call returns a secret's value. */
  secrets: string[];
  /** The name in `env`, and the class the source exports. */
  objects: Record<string, string>;
}

export interface Deployment {
  seq: number;
  stable: number;
  canary: { version: number; percent: number } | null;
}

/** Where a gateway serves a Worker. */
export interface WorkerUrl {
  gateway: string;
  url: string;
}

export interface WorkerInfo {
  name: string;
  latest: number;
  deployment: Deployment | null;
  /** One for each gateway that is available and says where clients reach it. */
  urls: WorkerUrl[];
}

/** Counters since each runner started. `errors` are runs with no response of the Worker's own. */
export interface Totals {
  requests: number;
  errors: number;
  outcomes: Record<string, number>;
  busy: number;
  cold_starts: number;
  in_flight: number;
  cpu_p50_us: number | null;
  cpu_p99_us: number | null;
  wall_p50_us: number | null;
  wall_p99_us: number | null;
}

export interface WorkerList {
  workers: (WorkerInfo & { metrics: Totals })[];
  /** Runners that did not answer. */
  silent: string[];
}

export interface WorkerDetail extends WorkerInfo {
  versions: VersionInfo[];
}

export interface Template {
  name: string;
  description: string;
  source: string;
  objects: Record<string, string>;
}

export interface Point {
  at_ms: number;
  requests: number;
  errors: number;
  cpu_p50_us: number | null;
  cpu_p99_us: number | null;
  wall_p50_us: number | null;
  wall_p99_us: number | null;
}

export interface Metrics {
  worker: string;
  total: Totals;
  by_node: (Totals & { node: string })[];
  silent: string[];
  series: Point[];
  sample_ms: number;
}

/** One request, as a preview and an invocation take it. */
export interface Ask {
  method: string;
  path: string;
  headers: Record<string, string>;
  body: string;
}

export interface Previewed {
  status: number;
  headers: [string, string][];
  body: string;
  body_truncated: boolean;
  logs: string[];
  cpu_us: number;
  wall_us: number;
  error: string | null;
  node: string;
}

export interface Invoked {
  status: number;
  headers: [string, string][];
  body: string;
  node: string | null;
  version: number | null;
  trace: string;
  cpu_us: number | null;
  wall_us: number | null;
  error: string | null;
  retry_after_ms: number | null;
}

export interface Variable {
  name: string;
  type: "plain" | "secret";
  /** Of a plain variable only. */
  value?: string;
}

export interface LogRecord {
  at_ms: number;
  worker: string;
  version: number;
  node: string;
  trace: string | null;
  method: string;
  url: string;
  status: number;
  outcome: string;
  cpu_us: number;
  wall_us: number;
  logs: string[];
  truncated: boolean;
}

export interface ObjectRow {
  class: string;
  id: string;
  /** Null for an object that only stores something. */
  node: string | null;
  epoch: number | null;
  stored: boolean;
}

export interface ObjectStorage {
  class: string;
  id: string;
  epoch: number;
  bytes: number;
  entries: { key: string; value: unknown }[];
}

export interface Served {
  seq: number;
  stable: number;
  canary: { version: number; percent: number } | null;
  warm: boolean;
}

export interface ClusterNode {
  id: string;
  address: string;
  roles: string[];
  /** Of a gateway: where clients reach it. */
  url: string | null;
  available: boolean;
  reachable: boolean;
  /** Null for a node that is not a runner, or a runner that did not answer. */
  serving: Record<string, Served> | null;
  in_flight: number | null;
  logs_dropped: number | null;
}

export interface ClusterView {
  /** The node that answered: the control node. */
  node: string;
  /** The members only: a node that stopped is in `silent` for about a second, then in neither. */
  nodes: ClusterNode[];
  silent: string[];
}

export const OUTCOMES = ["ok", "exception", "cpu_limit", "memory_limit", "wall_timeout", "engine", "response_too_large", "storage"];
