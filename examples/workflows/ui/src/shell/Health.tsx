import { useQuery, useQueryClient, type QueryCacheNotifyEvent } from "@tanstack/react-query";
import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { ready, Refusal } from "../api";
import { Notice } from "../components/ui";
import { clock } from "../format";
import { POLL } from "../queries";

/** Why a change is not possible now, or null. */
const Writes = createContext<string | null>(null);
/** When the data a page shows is from, while its polls fail; else null. */
const Stale = createContext<number | null>(null);

/** Why the buttons that change a run are off now: null while they are on. */
export const useOff = () => useContext(Writes);

export const NO_QUORUM = "No quorum of history nodes. You cannot change runs now.";

/** A failed poll: the api node did not answer, or answered that it cannot. A refusal of the request is not one. */
const failed = (error: unknown) => !(error instanceof Refusal) || error.status === 0 || error.status >= 500;

/**
 * The polls of the page that failed two times in a row: when the oldest data they show is from. A poll that works
 * again clears its count.
 */
function useStaleSince(): number | null {
  const cache = useQueryClient().getQueryCache();
  const [since, setSince] = useState<number | null>(null);
  useEffect(() => {
    const fails = new Map<string, number>();
    return cache.subscribe((event: QueryCacheNotifyEvent) => {
      if (event.type === "removed") fails.delete(event.query.queryHash);
      if (event.type !== "updated") return;
      const { query, action } = event;
      if (action.type === "error" && failed(action.error)) fails.set(query.queryHash, (fails.get(query.queryHash) ?? 0) + 1);
      else if (action.type === "success") fails.delete(query.queryHash);
      else return;
      const stale = cache.getAll().filter((q) => (fails.get(q.queryHash) ?? 0) >= 2 && q.getObserversCount() > 0);
      const times = stale.map((q) => q.state.dataUpdatedAt).filter((at) => at > 0);
      const next = stale.length ? (times.length ? Math.min(...times) : 0) : null;
      setSince((now) => (now === next ? now : next));
    });
  }, [cache]);
  return since;
}

/** Polls `GET /ready` and the health of the page's polls, for the pages below it. */
export function Health({ children }: { children: ReactNode }) {
  const health = useQuery({ queryKey: ["ready"], queryFn: ({ signal }) => ready(signal), refetchInterval: POLL });
  const stale = useStaleSince();
  const off = health.data && !health.data.journal ? NO_QUORUM : null;
  return (
    <Writes.Provider value={off}>
      <Stale.Provider value={stale}>{children}</Stale.Provider>
    </Writes.Provider>
  );
}

/** The banners of the shell: old data, and no quorum. Each page shows them below its bar. */
export function Banners() {
  const off = useOff();
  const stale = useContext(Stale);
  if (stale == null && !off) return null;
  return (
    <div className="flex flex-col gap-2">
      {stale != null && <Notice tone="warning">{stale ? `Data from ${clock(stale, true)}. Retrying.` : "No data. Retrying."}</Notice>}
      {off && <Notice tone="danger">{off}</Notice>}
    </div>
  );
}
