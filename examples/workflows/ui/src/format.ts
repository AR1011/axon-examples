import type { RunStatus } from "./api";

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
  if (Math.abs(n) < 10_000) return Number.isInteger(n) ? n.toLocaleString("en-US") : n.toFixed(1);
  return scaled(n, ["", "k", "M", "B"], 1000).replace(" ", "");
}

export const perMinute = (n?: number | null) => (known(n) ? `${Math.abs(n) < 100 ? n.toFixed(1) : count(Math.round(n))}/min` : UNKNOWN);

/** A duration in ms: "850 ms", "12 s", "4m 10s", "3h 5m", "2d 4h". */
export function span(ms?: number | null): string {
  if (!known(ms)) return UNKNOWN;
  ms = Math.max(0, ms);
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${(ms / 1000).toFixed(s < 10 ? 1 : 0)} s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86_400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86_400)}d ${Math.floor((s % 86_400) / 3600)}h`;
}

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

/** A time of day, with the date in front when it is not today: "15:02", "2026-10-12 09:00". */
export function when(ms: number): string {
  const d = new Date(ms);
  const today = new Date().toDateString() === d.toDateString();
  return today ? clock(ms) : `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${clock(ms)}`;
}

export type Tone = "success" | "warning" | "danger" | "muted" | "info";

export const STATUS_TONE: Record<RunStatus, Tone> = {
  running: "info",
  paused: "warning",
  parked: "warning",
  completed: "success",
  failed: "danger",
  cancelled: "muted",
  terminated: "danger",
};

/** A value as the API gave it: a string as it is, anything else as JSON. */
export const shown = (value: unknown) => (value == null ? "" : typeof value === "string" ? value : JSON.stringify(value, null, 2));
