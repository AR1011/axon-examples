import { useQuery } from "@tanstack/react-query";
import { get, runPath, type Cluster, type Metrics, type Queue, type Run, type RunList, type Worker, type WorkflowList } from "./api";

/** A visible page asks again every 2 s; a hidden one does not ask. */
export const POLL = 2000;

export const useWorkflows = () => useQuery({ queryKey: ["workflows"], queryFn: ({ signal }) => get<WorkflowList>("workflows", signal), refetchInterval: POLL });

export const useRuns = (query: string) => useQuery({ queryKey: ["runs", query], queryFn: ({ signal }) => get<RunList>(`runs?${query}`, signal), refetchInterval: POLL, placeholderData: (last) => last });

export const useRun = (id: string) => useQuery({ queryKey: ["run", id], queryFn: ({ signal }) => get<Run>(runPath(id), signal), refetchInterval: POLL });

export const useMetrics = () => useQuery({ queryKey: ["metrics"], queryFn: ({ signal }) => get<Metrics>("metrics", signal), refetchInterval: POLL });

export const useQueues = () => useQuery({ queryKey: ["queues"], queryFn: ({ signal }) => get<{ queues: Queue[] }>("queues", signal), refetchInterval: POLL });

export const useWorkers = () => useQuery({ queryKey: ["workers"], queryFn: ({ signal }) => get<{ workers: Worker[] }>("workers", signal), refetchInterval: POLL });

export const useCluster = () => useQuery({ queryKey: ["cluster"], queryFn: ({ signal }) => get<Cluster>("cluster", signal), refetchInterval: POLL });
