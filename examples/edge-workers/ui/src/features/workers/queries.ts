import { useQuery, useQueryClient } from "@tanstack/react-query";
import { get, type ClusterNode, type ClusterView, type Deployment, type VersionDetail, type WorkerDetail, type WorkerList, workerPath, WORKERS } from "../../api";

export const useWorkers = () => useQuery({ queryKey: ["workers"], queryFn: ({ signal }) => get<WorkerList>(WORKERS, signal), refetchInterval: 5000 });

export const useWorker = (name: string) => useQuery({ queryKey: ["worker", name], queryFn: ({ signal }) => get<WorkerDetail>(workerPath(name), signal), refetchInterval: 5000 });

/** A version does not change, so it is asked for once. */
export const useVersion = (name: string, id: number | undefined) =>
  useQuery({ queryKey: ["version", name, id], queryFn: ({ signal }) => get<VersionDetail>(workerPath(name, `/versions/${id}`), signal), enabled: id != null, staleTime: Infinity });

/** A node this page saw in the cluster that is not in it now, and when the page saw that. */
export interface Gone {
  node: ClusterNode;
  since_ms: number;
}

// The API lists the members only, so a node that stopped is not in its next answer. The page keeps what it saw.
const seen = new Map<string, ClusterNode>();
const gone = new Map<string, Gone>();

async function watchCluster(signal: AbortSignal): Promise<ClusterView & { gone: Gone[] }> {
  const view = await get<ClusterView>("cluster", signal);
  const now = new Set(view.nodes.map((node) => node.id));
  for (const [id, node] of seen) if (!now.has(id) && !gone.has(id)) gone.set(id, { node, since_ms: Date.now() });
  for (const node of view.nodes) {
    seen.set(node.id, node);
    gone.delete(node.id);
  }
  return { ...view, gone: [...gone.values()].sort((a, b) => a.node.id.localeCompare(b.node.id)) };
}

export const useCluster = () => useQuery({ queryKey: ["cluster"], queryFn: ({ signal }) => watchCluster(signal), refetchInterval: 5000 });

/** After a change: everything this console shows of the Worker is asked for again. */
export function useRefresh(name: string) {
  const queries = useQueryClient();
  return () =>
    Promise.all([
      queries.invalidateQueries({ queryKey: ["workers"] }),
      queries.invalidateQueries({ queryKey: ["worker", name] }),
      queries.invalidateQueries({ queryKey: ["vars", name] }),
      queries.invalidateQueries({ queryKey: ["cluster"] }),
    ]);
}

/** The version a change builds on, and the one the editor opens: the one that takes the traffic, or the latest. */
export const activeVersion = (worker: { latest: number; deployment: Deployment | null }) => worker.deployment?.stable ?? worker.latest;

export function deploymentText(deployment: Deployment | null): string {
  if (!deployment) return "Not deployed";
  const { stable, canary } = deployment;
  return canary ? `v${stable}, canary v${canary.version} at ${canary.percent}%` : `v${stable} deployed`;
}
