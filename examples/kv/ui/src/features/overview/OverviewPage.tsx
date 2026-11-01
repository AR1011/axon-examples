import { Table } from "@cloudflare/kumo";
import { lazy, Suspense, useEffect, useState } from "react";
import type { KvNode, Ops } from "../../api";
import type { Line } from "../../charts/Timeseries";
import { Card, Empty, Failed, Loading, MONO, Notice, NUM, Segmented, Stat, Stats, Status } from "../../components/ui";
import { ago, perSecond, span, UNKNOWN } from "../../format";
import { useNow, useTheme } from "../../hooks";
import { useCluster, useOps } from "../../queries";
import { Page } from "../../shell/Page";
import { nodeColor, nodeName } from "../nodes";
import { NodeSheet } from "./NodeSheet";

const loadCharts = () => import("../../charts/Timeseries");
const Timeseries = lazy(loadCharts);

const WINDOWS = [
  { value: "5", label: "5 min" },
  { value: "60", label: "1 hour" },
];

const sum = (values: (number | null | undefined)[]) => values.reduce<number>((total, v) => total + (v ?? 0), 0);

/** The lines of the two charts: the operations of each node, and of each kind over all nodes. */
function lines(ops: Ops): { byNode: Line[]; byKind: Line[] } {
  const byNode = ops.nodes.map((node, i) => ({
    name: nodeName(node),
    color: (dark: boolean) => nodeColor(i, dark),
    points: node.points.map(([at, r, w, f]): [number, number | null] => [at, r == null ? null : r + (w ?? 0) + (f ?? 0)]),
  }));
  // The buckets start at the same times on every node: add them up by time.
  const totals = new Map<number, [number, number, number]>();
  for (const node of ops.nodes)
    for (const [at, r, w, f] of node.points) {
      if (r == null) continue;
      const t = totals.get(at) ?? [0, 0, 0];
      totals.set(at, [t[0] + r, t[1] + (w ?? 0), t[2] + (f ?? 0)]);
    }
  const times = [...totals.keys()].sort((a, b) => a - b);
  const kind = (name: string, k: number, color: (dark: boolean) => string): Line => ({ name, color, points: times.map((at) => [at, totals.get(at)![k]]) });
  const byKind = [kind("Reads", 0, (dark) => nodeColor(0, dark)), kind("Writes", 1, (dark) => nodeColor(1, dark)), kind("Failed", 2, () => "#e5484d")];
  return { byNode, byKind };
}

const HELP = (
  <>
    <p>This page shows the kv nodes that this console asks, the load on each node, and the shards that each node runs.</p>
    <h3>Operations</h3>
    <ul>
      <li>An operation is one command that reads or writes data, for example GET or SET. The commands of this console are not operations.</li>
      <li>The rate is the change of the node's counters between two samples. The console takes one sample each second.</li>
      <li>Failed operations got an error, for example because the shard was moving.</li>
    </ul>
    <h3>Nodes</h3>
    <ul>
      <li>"No answer": the node did not answer the last sample. It can be stopped, paused or cut off.</li>
      <li>Shards: the number of shards that the node runs now. A cluster has 256 shards.</li>
      <li>Click a node to see all of its fields.</li>
    </ul>
  </>
);

export function OverviewPage() {
  // The charts' code loads while the page waits for its first answers, not after them. A failed load is the lazy chart's.
  useEffect(() => void loadCharts().catch(() => {}), []);
  const cluster = useCluster();
  const [minutes, setMinutes] = useState("5");
  const ops = useOps(Number(minutes));
  const [open, setOpen] = useState<string | null>(null);
  const [theme] = useTheme();
  const nodes = cluster.data?.nodes ?? [];
  const up = nodes.filter((n) => n.up);
  const down = nodes.filter((n) => !n.up);
  const placed = cluster.data?.shards.filter((s) => s.hosts.length > 0).length;
  const rate = (k: "reads" | "writes" | "failed") => (cluster.data ? sum(up.map((n) => n.rate?.[k])) : null);
  const total = cluster.data ? sum([rate("reads"), rate("writes"), rate("failed")]) : null;
  const failed = rate("failed");
  const charts = ops.data && lines(ops.data);
  const selected = nodes.find((n) => n.addr === open);

  // The page comes in whole, once both have answered: a part that came in later would push the parts under it down.
  const body = (
    <>
      {down.length > 0 && (
        <Notice tone="warning">
          {down.length === 1 ? "1 node does not answer" : `${down.length} nodes do not answer`}: {down.map(nodeName).join(", ")}.
        </Notice>
      )}
      <Stats label="Totals" className="grid-cols-4 max-md:grid-cols-2">
        <Stat label="Nodes" value={cluster.data ? up.length : UNKNOWN} sub={cluster.data ? `of ${nodes.length}` : undefined} tone={down.length ? "warning" : undefined} hint="The nodes that answered the last sample, of the nodes that this console asks." />
        <Stat label="Shards" value={placed ?? UNKNOWN} sub={cluster.data ? `of ${cluster.data.shards.length}` : undefined} tone={placed != null && cluster.data && placed < cluster.data.shards.length ? "warning" : undefined} hint="The shards that run on a node that answers. A shard that moves runs on no node for a short time." />
        <Stat label="Operations" value={perSecond(total)} hint="The reads, writes and failed operations each second, on all nodes." />
        <Stat label="Failed" value={perSecond(failed)} tone={failed ? "danger" : undefined} hint="The operations each second that got an error." />
      </Stats>

      <Card title="Operations per second" label="Operations per second" controls={<Segmented label="Time" value={minutes} options={WINDOWS} onChange={setMinutes} />}>
        {!charts ? (
          <Failed what="the operations" error={ops.error} />
        ) : (
          <div className="grid grid-cols-2 divide-x divide-kumo-line max-lg:grid-cols-1 max-lg:divide-x-0 max-lg:divide-y">
            <div>
              <h3 className="px-4 pt-3 text-sm text-kumo-subtle">By node</h3>
              <Timeseries lines={charts.byNode} label="Operations per second of each node" />
            </div>
            <div>
              <h3 className="px-4 pt-3 text-sm text-kumo-subtle">By kind</h3>
              <Timeseries lines={charts.byKind} label="Reads, writes and failed operations per second" />
            </div>
          </div>
        )}
      </Card>

      <Card title="Nodes" label="Nodes">
        {!cluster.data ? (
          <Failed what="the nodes" error={cluster.error} />
        ) : nodes.length === 0 ? (
          <Empty title="No nodes" detail="Give the console the addresses of the nodes with --addrs." />
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[860px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Node</Table.Head>
                  <Table.Head>Address</Table.Head>
                  <Table.Head>State</Table.Head>
                  <Table.Head className="text-right">Shards</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Reads/s</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Writes/s</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Failed/s</Table.Head>
                  <Table.Head className="text-right">Clients</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Up for</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {nodes.map((node, i) => (
                  <NodeRow key={node.addr} node={node} color={nodeColor(i, theme === "dark")} onOpen={() => setOpen(node.addr)} />
                ))}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
      {selected && <NodeSheet node={selected} onClose={() => setOpen(null)} />}
    </>
  );
  const loading = <Loading what="the cluster" />;

  return (
    <Page crumbs={[{ label: "Overview" }]} title="Overview" help={HELP}>
      {cluster.isPending || ops.isPending ? loading : <Suspense fallback={loading}>{body}</Suspense>}
    </Page>
  );
}

function NodeRow({ node, color, onOpen }: { node: KvNode; color: string; onOpen: () => void }) {
  const name = nodeName(node);
  return (
    <Table.Row className={node.up ? "cursor-pointer" : "cursor-pointer text-kumo-subtle"} onClick={onOpen}>
      <Table.Cell className={`${MONO} font-medium`}>
        <button type="button" onClick={onOpen} aria-label={`Show the fields of ${name}`} className="flex cursor-pointer items-center gap-2 rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-kumo-ring">
          <span aria-hidden="true" className="size-2.5 shrink-0 rounded-full" style={{ background: color }} />
          {name}
        </button>
      </Table.Cell>
      <Table.Cell className={MONO}>{node.addr}</Table.Cell>
      <Table.Cell>
        {node.up ? (
          <Status tone="success">Answers</Status>
        ) : (
          <span title={node.error ?? undefined}>
            <Status tone="danger">No answer</Status>
            {node.down_ms != null && <Ago ms={node.down_ms} />}
          </span>
        )}
      </Table.Cell>
      <Table.Cell className={NUM}>{node.shards_here ?? UNKNOWN}</Table.Cell>
      <Table.Cell className={NUM}>{perSecond(node.rate?.reads)}</Table.Cell>
      <Table.Cell className={NUM}>{perSecond(node.rate?.writes)}</Table.Cell>
      <Table.Cell className={NUM}>{perSecond(node.rate?.failed)}</Table.Cell>
      <Table.Cell className={NUM}>{node.up ? (node.clients ?? UNKNOWN) : UNKNOWN}</Table.Cell>
      <Table.Cell className={NUM}>{node.up ? span(node.uptime_s) : UNKNOWN}</Table.Cell>
    </Table.Row>
  );
}

/** How long ago, kept current: only this subscribes to the second-hand, so the page and its charts do not draw again each second. */
function Ago({ ms }: { ms: number }) {
  const now = useNow();
  return <span className="ml-2 text-sm text-kumo-subtle">{ago(now - ms)}</span>;
}
