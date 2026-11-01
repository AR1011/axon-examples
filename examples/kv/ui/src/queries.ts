import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { get, type Cluster, type Ops, type Run, type Runs } from "./api";

/** How often the page asks again: about twice a sample. */
const EVERY = 2000;

export const useCluster = () => useQuery({ queryKey: ["cluster"], queryFn: ({ signal }) => get<Cluster>("cluster", signal), refetchInterval: EVERY });

export const useOps = (minutes: number) =>
  useQuery({ queryKey: ["ops", minutes], queryFn: ({ signal }) => get<Ops>(`ops?minutes=${minutes}`, signal), refetchInterval: EVERY, placeholderData: keepPreviousData });

export const useRuns = () => useQuery({ queryKey: ["runs"], queryFn: ({ signal }) => get<Runs>("runs", signal) });

export const useRun = (name: string) => useQuery({ queryKey: ["run", name], queryFn: ({ signal }) => get<Run>(`runs/${encodeURIComponent(name)}`, signal) });
