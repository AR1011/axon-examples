import { useSyncExternalStore } from "react";

let now = Date.now();
const listeners = new Set<() => void>();
let ticker: ReturnType<typeof setInterval> | undefined;

function onTick(listener: () => void) {
  listeners.add(listener);
  ticker ??= setInterval(() => {
    now = Date.now();
    listeners.forEach((l) => l());
  }, 1000);
  return () => {
    listeners.delete(listener);
    if (!listeners.size) {
      clearInterval(ticker);
      ticker = undefined;
    }
  };
}

/** One shared second-hand; only the components that show relative time subscribe to it. */
export const useNow = () => useSyncExternalStore(onTick, () => now);

const THEME = "edge-theme";
type Theme = "dark" | "light";
const root = document.documentElement;
const current = (): Theme => (root.dataset.mode === "dark" ? "dark" : "light");

/** Light unless this browser picked dark. */
export function applyTheme() {
  let stored: string | null = null;
  try {
    stored = localStorage.getItem(THEME);
  } catch {
    /* no storage: light */
  }
  root.dataset.mode = stored === "dark" ? "dark" : "light";
}

function onTheme(listener: () => void) {
  const observer = new MutationObserver(listener);
  observer.observe(root, { attributes: true, attributeFilter: ["data-mode"] });
  return () => observer.disconnect();
}

export function useTheme(): [Theme, () => void] {
  const theme = useSyncExternalStore(onTheme, current);
  return [
    theme,
    () => {
      const next = theme === "dark" ? "light" : "dark";
      try {
        localStorage.setItem(THEME, next);
      } catch {
        /* no storage: the choice holds for this page only */
      }
      root.dataset.mode = next;
    },
  ];
}
