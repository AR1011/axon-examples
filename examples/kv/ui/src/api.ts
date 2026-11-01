/** Where the page is mounted, from its `<base href>`: "/" unless a server says otherwise. */
export const base = new URL(document.baseURI).pathname.replace(/[^/]*$/, "");
/** `kv console`'s API: always at `/api/`, beside the page. */
export const api = "/api/";

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

export async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  let answer: Response;
  try {
    answer = await fetch(api + path, { signal, credentials: "same-origin" });
  } catch (e) {
    if (signal?.aborted) throw e;
    throw new Refusal(0, "offline", "The browser cannot connect to kv console.");
  }
  if (answer.ok) return (await answer.json()) as T;
  const text = await answer.text().catch(() => "");
  const said = ((): { error?: unknown; message?: unknown } | null => {
    try {
      return JSON.parse(text) as { error?: unknown; message?: unknown } | null;
    } catch {
      return null; // not JSON: the text is the message
    }
  })();
  throw new Refusal(answer.status, typeof said?.error === "string" ? said.error : "error", typeof said?.message === "string" ? said.message : (!said && text.trim().slice(0, 300)) || answer.statusText || "The request failed.");
}

/** Per second, between the last two samples of a node. */
export interface Rate {
  reads: number;
  writes: number;
  failed: number;
}

/** A node as `kv console` last saw it. `id` and the counters are null until it answers once. */
export interface KvNode {
  addr: string;
  id: string | null;
  up: boolean;
  error: string | null;
  /** When it last answered, and when it first did not after that (ms since the epoch). */
  seen_ms: number | null;
  down_ms: number | null;
  mode: string | null;
  /** Durable mode: "quorum" or "lease". */
  reads: string | null;
  pid: number | null;
  uptime_s: number | null;
  clients: number | null;
  /** Null while it does not answer. */
  shards_here: number | null;
  /** Null for the first sample after a start, and while it does not answer. */
  rate: Rate | null;
  /** Its `INFO`: the fields as the node gives them, as text. */
  info: Record<string, string>;
}

export interface Host {
  node: string;
  /** The lease epoch the shard started under. */
  epoch: number;
}

export interface Cluster {
  at_ms: number;
  every_ms: number;
  slots_per_shard: number;
  nodes: KvNode[];
  /** One per shard, by its number: the nodes it runs on now. None while it moves; two for a short time. */
  shards: { hosts: Host[] }[];
}

/** `[at_ms, reads/s, writes/s, failed/s]`, the rates null where the node did not answer. */
export type Point = [number, number | null, number | null, number | null];

export interface Ops {
  every_ms: number;
  nodes: { addr: string; id: string | null; points: Point[] }[];
}

export interface FaultsSummary {
  phases: number;
  passed: number;
  soak: boolean | null;
}

export interface RunEntry {
  name: string;
  date: string | null;
  host: string | null;
  kind: string | null;
  rounds: number | null;
  rows: number;
  /** The rows with errors, which summary.md leaves out of its tables and lists apart. */
  error_rows: number;
  targets: string[];
  faults: FaultsSummary | null;
  summary: boolean;
}

export interface Runs {
  /** Where the console reads them. */
  dir: string;
  runs: RunEntry[];
}

/** One line of `fault/faults.jsonl`, as `fault.sh` writes it. */
export interface Fault {
  phase: string;
  pass: boolean;
  victim?: string;
  check?: { lost: number; stale: number; writes_ok: number; writes_failed: number; unknown: number; longest_window_ms: number };
  windows?: { away: { n: number; p50_ms: number; max_ms: number }; back: { n: number; p50_ms: number; max_ms: number } };
}

export interface Run extends Omit<RunEntry, "summary" | "faults" | "rows"> {
  env: string | null;
  /** `summary.md`, as `summary.py` writes it. */
  summary: string | null;
  columns: string[];
  rows: string[][];
  faults: Fault[];
}
