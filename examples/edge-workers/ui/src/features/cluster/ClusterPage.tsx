import { Badge, Link, Table } from "@cloudflare/kumo";
import { RUNNER, type ClusterNode } from "../../api";
import { Card, CopyButton, Empty, Failed, Loading, MONO, NUM, Stat, Stats, Status, TONES } from "../../components/ui";
import { count } from "../../format";
import { Page } from "../../shell/Page";
import { useCluster } from "../workers/queries";
import { Quiet } from "./Quiet";

function state(node: ClusterNode, gone: boolean): { label: string; tone: "success" | "warning" | "danger" } {
  if (gone) return { label: "Not in the cluster", tone: "danger" };
  if (!node.reachable) return { label: "Not reachable", tone: "danger" };
  if (!node.available) return { label: "Not available", tone: "warning" };
  if (node.roles.includes(RUNNER) && !node.serving) return { label: "No answer", tone: "warning" };
  return { label: "Available", tone: "success" };
}

export function ClusterPage() {
  const cluster = useCluster();
  const nodes = cluster.data?.nodes ?? [];
  const gone = cluster.data?.gone ?? [];
  // A node that left keeps its row, with what the page saw of it last.
  const rows = [...nodes.map((node) => ({ node, gone: false })), ...gone.map((g) => ({ node: g.node, gone: true }))].sort((a, b) => a.node.id.localeCompare(b.node.id));
  const having = (role: string) => nodes.filter((node) => node.roles.includes(role)).length;
  return (
    <Page
      crumbs={[{ label: "Cluster" }]}
      title="Cluster"
    >
      <Quiet effect="A node that starts again shows as available." />
      {cluster.data && (
        <Stats label="Totals" className="grid-cols-4 max-sm:grid-cols-2">
          <Stat label="Nodes" value={nodes.length} />
          <Stat label="Control" value={having("control")} />
          <Stat label="Gateways" value={having("gateway")} />
          <Stat label="Runners" value={having(RUNNER)} />
        </Stats>
      )}
      <Card label="Nodes">
        {cluster.isPending ? (
          <Loading what="the cluster" />
        ) : cluster.error ? (
          <Failed what="the cluster" error={cluster.error} />
        ) : rows.length === 0 ? (
          <Empty title="No nodes" detail="The control node sees no node." />
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[880px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Node</Table.Head>
                  <Table.Head>Role</Table.Head>
                  <Table.Head className="whitespace-nowrap">Address and URL</Table.Head>
                  <Table.Head>State</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">In flight</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Logs dropped</Table.Head>
                  <Table.Head className="w-[30%]">Serves</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {rows.map(({ node, gone }) => {
                  const { label, tone } = state(node, gone);
                  const serving = gone ? [] : Object.entries(node.serving ?? {});
                  return (
                    <Table.Row key={node.id} className={gone ? "text-kumo-subtle" : ""}>
                      <Table.Cell className={`${MONO} font-medium`}>{node.id}</Table.Cell>
                      <Table.Cell>{node.roles.join(", ") || "—"}</Table.Cell>
                      <Table.Cell className={MONO}>
                        {node.address}
                        {node.url && (
                          <span className="flex items-center gap-1">
                            <Link href={node.url} target="_blank" rel="noreferrer" className="font-mono text-sm">
                              {node.url}
                            </Link>
                            <CopyButton text={node.url} label={`Copy the URL of ${node.id}`} />
                          </span>
                        )}
                      </Table.Cell>
                      <Table.Cell>
                        <Status tone={tone}>{label}</Status>
                      </Table.Cell>
                      <Table.Cell className={NUM}>{count(gone ? null : node.in_flight)}</Table.Cell>
                      <Table.Cell className={`${NUM} ${node.logs_dropped && !gone ? TONES.warning : ""}`}>{count(gone ? null : node.logs_dropped)}</Table.Cell>
                      <Table.Cell>
                        {!node.roles.includes(RUNNER) ? (
                          <span className="text-kumo-subtle">—</span>
                        ) : serving.length === 0 ? (
                          <span className="text-kumo-subtle">{gone ? "—" : node.serving ? "No Workers" : "No answer"}</span>
                        ) : (
                          <span className="flex flex-wrap gap-1.5">
                            {serving.map(([name, served]) => (
                              <Link key={name} href={`/workers/${name}`} variant="plain" title={`deployment ${served.seq}${served.warm ? ", loaded" : ", not loaded"}`}>
                                <Badge variant="outline" className={served.warm ? "font-mono" : "bg-kumo-recessed font-mono text-kumo-inactive"}>
                                  {name} v{served.stable}
                                  {served.canary && ` + v${served.canary.version} ${served.canary.percent}%`}
                                </Badge>
                              </Link>
                            ))}
                          </span>
                        )}
                      </Table.Cell>
                    </Table.Row>
                  );
                })}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
    </Page>
  );
}
