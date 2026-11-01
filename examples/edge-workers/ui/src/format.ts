export const UNKNOWN = "—";

const known = (n: number | null | undefined): n is number => n != null && Number.isFinite(n);
const pad = (n: number) => String(n).padStart(2, "0");

function scaled(n: number, units: string[], step: number): string {
  let i = 0;
  while (Math.abs(n) >= step && i < units.length - 1) {
    n /= step;
    i++;
  }
  return `${i === 0 || Math.abs(n) >= 100 ? Math.round(n) : n.toFixed(1)} ${units[i]}`;
}

export const bytes = (n?: number | null) => (known(n) ? scaled(n, ["B", "KiB", "MiB", "GiB"], 1024) : UNKNOWN);

export function count(n?: number | null): string {
  if (!known(n)) return UNKNOWN;
  if (Math.abs(n) < 10_000) return Number.isInteger(n) ? String(n) : n.toFixed(1);
  return scaled(n, ["", "k", "M", "B"], 1000).replace(" ", "");
}

export const perSecond = (n?: number | null) => (known(n) ? `${Math.abs(n) < 100 ? n.toFixed(1) : count(Math.round(n))}/s` : UNKNOWN);
export const percent = (n?: number | null) => (known(n) ? `${n.toFixed(n !== 0 && Math.abs(n) < 1 ? 2 : 1)}%` : UNKNOWN);
/** A time the API gives in microseconds. */
export const micros = (us?: number | null) => (known(us) ? scaled(us, ["µs", "ms", "s"], 1000) : UNKNOWN);

export function ago(ms: number): string {
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86_400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86_400)}d ago`;
}

export function clock(ms: number, seconds = false): string {
  const d = new Date(ms);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}${seconds ? `:${pad(d.getSeconds())}` : ""}`;
}

export type Tone = "success" | "warning" | "danger" | "muted";
/** The colour of a response status, and of a run that gave no response of its own. */
export const statusTone = (status: number, outcome = "ok"): Tone => (outcome !== "ok" || status >= 500 ? "danger" : status >= 400 ? "warning" : "success");
