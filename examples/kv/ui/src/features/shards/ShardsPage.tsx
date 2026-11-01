import { cn, Table } from "@cloudflare/kumo";
import { useState } from "react";
import type { Cluster } from "../../api";
import { Card, Failed, Loading, MONO, NUM, Pick, Segmented, Stat, Stats } from "../../components/ui";
import { UNKNOWN } from "../../format";
import { useTheme } from "../../hooks";
import { useCluster } from "../../queries";
import { Page } from "../../shell/Page";
import { nodeColor, nodeName, placeOf } from "../nodes";

const VIEWS = [
  { value: "map", label: "Map" },
  { value: "list", label: "List" },
];
const ALL = "all";

const HELP = (
  <>
    <p>The keys of kv are in 256 shards. Each shard runs on one node at a time, under a lease. This page shows the node of each shard.</p>
    <ul>
      <li>A shard has 64 of the 16384 slots. The slot of a key is CRC16 of the key, modulo 16384, as in Redis Cluster.</li>
      <li>When a node stops, its leases end. Then its shards start on the other nodes.</li>
      <li>A shard on no node moves now. A shard on two nodes is in a handover: the old node did not yet stop it.</li>
      <li>The lease epoch increases each time the shard starts on a node.</li>
      <li>Select a node to see only its shards. Click a shard to see its slots.</li>
    </ul>
  </>
);

/** The state of a shard, in words: its node, none, or the two in a handover. */
function where(hosts: { node: string }[]): string {
  return hosts.length === 0 ? "no node" : hosts.map((h) => h.node).join(" and ");
}

export function ShardsPage() {
  const cluster = useCluster();
  return (
    <Page crumbs={[{ label: "Shards" }]} title="Shards" help={HELP}>
      {cluster.isPending ? (
        <Card>
          <Loading what="the shards" />
        </Card>
      ) : cluster.error && !cluster.data ? (
        <Card>
          <Failed what="the shards" error={cluster.error} />
        </Card>
      ) : (
        <Shards cluster={cluster.data!} />
      )}
    </Page>
  );
}

function Shards({ cluster }: { cluster: Cluster }) {
  const [view, setView] = useState("map");
  const [only, setOnly] = useState(ALL);
  const [picked, setPicked] = useState<number | null>(null);
  const [theme] = useTheme();
  const dark = theme === "dark";
  const place = placeOf(cluster);
  const color = (node: string) => nodeColor(place.get(node) ?? 0, dark);
  const total = cluster.shards.length;
  const none = cluster.shards.filter((s) => s.hosts.length === 0).length;
  const two = cluster.shards.filter((s) => s.hosts.length > 1).length;
  const named = cluster.nodes.filter((n) => n.id);
  const options = [{ value: ALL, label: "All nodes" }, ...named.map((n) => ({ value: n.id!, label: n.id! }))];
  const shown = (hosts: { node: string }[]) => only === ALL || hosts.some((h) => h.node === only);
  const shard = picked == null ? null : cluster.shards[picked];
  const slots = cluster.slots_per_shard;

  return (
    <>
      <Stats label="Shards" className="grid-cols-3 max-sm:grid-cols-1">
        <Stat label="Placed" value={total - none} sub={`of ${total}`} tone={none ? "warning" : undefined} hint="The shards that run on a node that answers." />
        <Stat label="On no node" value={none} tone={none ? "warning" : undefined} hint="The shards that move now, or that were on a node that does not answer." />
        <Stat label="On two nodes" value={two} tone={two ? "warning" : undefined} hint="The shards in a handover. The old node stops its copy in a short time." />
      </Stats>

      <Card title="Shards per node" label="Shards per node">
        <div className="grid grid-cols-3 gap-x-6 gap-y-4 p-4 max-md:grid-cols-1">
          {cluster.nodes.map((node) => {
            const here = node.up ? (node.shards_here ?? 0) : 0;
            return (
              <div key={node.addr} role="meter" aria-label={nodeName(node)} aria-valuenow={here} aria-valuemin={0} aria-valuemax={total} className="flex min-w-0 flex-col gap-1.5">
                <div className="flex items-baseline justify-between gap-2 text-sm">
                  <span className="truncate font-mono">
                    {nodeName(node)}
                    {!node.up && <span className="ml-2 font-sans text-kumo-danger">No answer</span>}
                  </span>
                  <span className="shrink-0 tabular-nums">
                    {here} of {total}
                  </span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-kumo-recessed">
                  <div className="h-full rounded-full transition-[width] duration-300" style={{ width: `${(here / total) * 100}%`, background: node.id ? color(node.id) : undefined }} />
                </div>
              </div>
            );
          })}
        </div>
      </Card>

      <Card
        title="Shards"
        label="Shard map"
        controls={
          <>
            <Pick label="Node" value={only} options={options} onChange={setOnly} className="w-36" />
            <Segmented label="View" value={view} options={VIEWS} onChange={setView} />
          </>
        }
        footer={
          shard ? (
            <span aria-live="polite" className="font-mono">
              Shard {picked} · slots {picked! * slots} to {picked! * slots + slots - 1} · on {where(shard.hosts)}
              {shard.hosts.map((h) => ` · ${h.node} epoch ${h.epoch}`).join("")}
            </span>
          ) : (
            <span aria-live="polite">Click a shard to see its slots and its lease.</span>
          )
        }
      >
        {view === "map" ? (
          <div className="flex flex-col gap-3 p-4">
            <div role="grid" aria-label="Shards by number" className="mx-auto grid w-full max-w-[640px] grid-cols-16 gap-[3px]">
              {cluster.shards.map((s, i) => {
                const host = s.hosts[0]?.node;
                return (
                  <button
                    key={i}
                    type="button"
                    aria-label={`Shard ${i} on ${where(s.hosts)}`}
                    aria-pressed={picked === i}
                    onClick={() => setPicked(picked === i ? null : i)}
                    className={cn(
                      "aspect-square min-w-0 cursor-pointer rounded-[3px] outline-none transition-opacity focus-visible:ring-2 focus-visible:ring-kumo-ring",
                      !host && "bg-kumo-recessed ring-1 ring-kumo-line ring-inset",
                      !shown(s.hosts) && "opacity-15",
                      s.hosts.length > 1 && "ring-2 ring-kumo-default ring-inset",
                      picked === i && "ring-2 ring-kumo-default ring-offset-1 ring-offset-kumo-base",
                    )}
                    style={host ? { background: color(host) } : undefined}
                  />
                );
              })}
            </div>
            <div className="flex flex-wrap justify-center gap-x-4 gap-y-1 text-sm text-kumo-subtle">
              {named.map((n) => (
                <span key={n.id} className="flex items-center gap-1.5">
                  <span aria-hidden="true" className="size-2.5 rounded-[2px]" style={{ background: color(n.id!) }} />
                  {n.id}
                </span>
              ))}
              <span className="flex items-center gap-1.5">
                <span aria-hidden="true" className="size-2.5 rounded-[2px] bg-kumo-recessed ring-1 ring-kumo-line ring-inset" />
                no node
              </span>
            </div>
          </div>
        ) : (
          <div className="max-h-[560px] overflow-auto">
            <Table className="min-w-[480px]">
              <Table.Header className="sticky top-0 z-[1] bg-kumo-base">
                <Table.Row>
                  <Table.Head className="text-right">Shard</Table.Head>
                  <Table.Head>Slots</Table.Head>
                  <Table.Head>Node</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Lease epoch</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {cluster.shards.map((s, i) =>
                  shown(s.hosts) ? (
                    <Table.Row key={i} className={s.hosts.length === 0 ? "text-kumo-subtle" : ""}>
                      <Table.Cell className={NUM}>{i}</Table.Cell>
                      <Table.Cell className={`${MONO} whitespace-nowrap`}>
                        {i * slots}–{i * slots + slots - 1}
                      </Table.Cell>
                      <Table.Cell className={MONO}>{where(s.hosts)}</Table.Cell>
                      <Table.Cell className={NUM}>{s.hosts.map((h) => h.epoch).join(", ") || UNKNOWN}</Table.Cell>
                    </Table.Row>
                  ) : null,
                )}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
    </>
  );
}
