import { ChartPalette } from "@cloudflare/kumo";
import type { Cluster, KvNode } from "../api";

/** The name to show for a node: its id, or its address until it answers once. */
export const nodeName = (node: Pick<KvNode, "id" | "addr">) => node.id ?? node.addr;

/** The colour of a node's line and shards: by its place in the list, so it stays the same while it is down. */
export const nodeColor = (index: number, dark: boolean) => ChartPalette.categorical(index, dark);

/** Each node's place in the list, by its id. */
export const placeOf = (cluster: Cluster) => new Map(cluster.nodes.flatMap((node, i) => (node.id ? [[node.id, i] as const] : [])));
