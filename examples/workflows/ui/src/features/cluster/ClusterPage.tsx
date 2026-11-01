import { Badge, Table } from "@cloudflare/kumo";
import type { ReactNode } from "react";
import { Card, Empty, Failed, Loading, MONO, Notice, Stat, Stats, Status } from "../../components/ui";
import { count } from "../../format";
import { useCluster } from "../../queries";
import { Page } from "../../shell/Page";

const HELP: ReactNode = (
  <dl>
    <dt>History nodes</dt>
    <dd>The nodes that keep the journals and the queues. Each change to a run goes to all of them, and is kept when most of them have it: the quorum.</dd>
    <dt>No quorum</dt>
    <dd>Less than half of the history nodes are up. Runs stop until more come back. You can read the runs that the nodes up have.</dd>
    <dt>Warming</dt>
    <dd>This api node started a short time ago and cannot take leases yet. It continues after one lease.</dd>
    <dt>Isolated</dt>
    <dd>This api node cannot reach most of the lease acceptors.</dd>
    <dt>Roles</dt>
    <dd>History keeps journals and queues. Engine runs the code of the runs. Api serves this console and the HTTP API. Worker runs the activities.</dd>
  </dl>
);

export function ClusterPage() {
  const cluster = useCluster();
  const c = cluster.data;
  const up = c?.replicas.filter((r) => r.up).length ?? 0;
  return (
    <Page crumbs={[{ label: "Cluster" }]} title="Cluster" help={HELP}>
      {cluster.isPending ? (
        <Card label="Cluster">
          <Loading what="the cluster" />
        </Card>
      ) : !c ? (
        <Card label="Cluster">
          <Failed what="the cluster" error={cluster.error} />
        </Card>
      ) : (
        <>
          {c.isolated && <Notice tone="warning">This api node cannot reach most of the lease acceptors.</Notice>}
          <Stats label="State" className="grid-cols-4 max-md:grid-cols-2">
            <Stat label="History nodes up" value={`${up} of ${c.replicas.length}`} tone={c.quorum ? undefined : "danger"} />
            <Stat label="Quorum" value={c.quorum ? "yes" : "no"} tone={c.quorum ? "success" : "danger"} />
            <Stat label="Warming" value={c.warming ? "yes" : "no"} tone={c.warming ? "warning" : undefined} hint="This api node cannot take leases for a short time after it starts" />
            <Stat label="Members" value={count(c.members.length)} />
          </Stats>
          <Card label="History nodes" title="History nodes">
            <ul className="flex flex-wrap gap-x-6 gap-y-2 px-4 py-3">
              {c.replicas.map((r) => (
                <li key={r.node} className="flex items-center gap-2">
                  <span className={MONO}>{r.node}</span>
                  <Status tone={r.up ? "success" : "danger"}>{r.up ? "up" : "down"}</Status>
                </li>
              ))}
            </ul>
          </Card>
          <Card label="Members" title="Members">
            {c.members.length === 0 ? (
              <Empty title="No members" />
            ) : (
              <div className="overflow-x-auto">
                <Table className="min-w-[480px]">
                  <Table.Header>
                    <Table.Row>
                      <Table.Head>Node</Table.Head>
                      <Table.Head>State</Table.Head>
                      <Table.Head>Roles</Table.Head>
                    </Table.Row>
                  </Table.Header>
                  <Table.Body>
                    {c.members.map((m) => (
                      <Table.Row key={m.node}>
                        <Table.Cell className={MONO}>{m.node}</Table.Cell>
                        <Table.Cell>
                          <Status tone={m.available ? "success" : "warning"}>{m.available ? "up" : "leaving"}</Status>
                        </Table.Cell>
                        <Table.Cell>
                          <span className="flex flex-wrap gap-1">
                            {m.roles.map((role) => (
                              <Badge key={role} variant="outline">
                                {role}
                              </Badge>
                            ))}
                          </span>
                        </Table.Cell>
                      </Table.Row>
                    ))}
                  </Table.Body>
                </Table>
              </div>
            )}
          </Card>
        </>
      )}
    </Page>
  );
}
