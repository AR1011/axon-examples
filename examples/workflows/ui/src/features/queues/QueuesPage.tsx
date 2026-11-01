import { Badge, Meter, Table, Tooltip } from "@cloudflare/kumo";
import type { ReactNode } from "react";
import { Card, CLIP, Empty, Failed, Loading, MONO, Notice, NUM, Status, TONES } from "../../components/ui";
import { ago, bytes, count, span } from "../../format";
import type { Worker } from "../../api";
import { useQueues, useWorkers } from "../../queries";
import { Page } from "../../shell/Page";

const HELP: ReactNode = (
  <dl>
    <dt>Backlog</dt>
    <dd>The tasks that wait in the queue for a worker. The queue keeps them in memory on the engines, in four partitions. When an engine stops, each run gives its task again after 10 s.</dd>
    <dt>Oldest</dt>
    <dd>How long the oldest task waits. When it increases, the workers are too few or too slow.</dd>
    <dt>Pollers</dt>
    <dd>The workers that wait for a task now. No pollers and a backlog: no worker takes the tasks.</dd>
    <dt>Slots in use</dt>
    <dd>The tasks that the workers run now, of the most they run at the same time. All slots in use and a backlog: add workers or slots.</dd>
    <dt>Failed, last minute</dt>
    <dd>Attempts that ended with an error in the last minute. The run tries again as its retry policy tells.</dd>
    <dt>Refused</dt>
    <dd>Tasks the queue did not take because it was full, since its partitions started.</dd>
    <dt>Last poll</dt>
    <dd>When the worker last asked for tasks. A worker with all its slots in use does not ask.</dd>
    <dt>Draining</dt>
    <dd>The worker completes its tasks and takes no new ones, or it left the cluster.</dd>
  </dl>
);

function Queues() {
  const queues = useQueues();
  const rows = queues.data?.queues ?? [];
  const most = Math.max(1, ...rows.map((q) => q.backlog));
  return (
    <Card label="Queues" title="Queues">
      {queues.isPending ? (
        <Loading what="the queues" />
      ) : queues.error && !queues.data ? (
        <Failed what="the queues" error={queues.error} />
      ) : rows.length === 0 ? (
        <Empty title="No queues" detail="No workflow sends a task to a queue yet." />
      ) : (
        <div className="overflow-x-auto">
          <Table className="min-w-[1000px]">
            <Table.Header>
              <Table.Row>
                <Table.Head>Queue</Table.Head>
                <Table.Head className="w-[220px]">Backlog</Table.Head>
                <Table.Head className="text-right">Oldest</Table.Head>
                <Table.Head className="text-right">Pollers</Table.Head>
                <Table.Head className="w-[160px]">Slots in use</Table.Head>
                <Table.Head className="text-right">Failed, last minute</Table.Head>
                <Table.Head className="text-right">Refused</Table.Head>
                <Table.Head>Workers</Table.Head>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {rows.map((q) => (
                <Table.Row key={q.name}>
                  <Table.Cell className={MONO}>
                    {q.name}
                    {q.partitions_missing > 0 && (
                      <Tooltip content="These partitions did not answer: the numbers do not include them" render={<span className={`ml-2 cursor-help text-sm ${TONES.warning}`} />}>
                        {q.partitions_missing} missing
                      </Tooltip>
                    )}
                  </Table.Cell>
                  <Table.Cell>
                    <Meter label="Tasks" value={(q.backlog / most) * 100} customValue={`${count(q.backlog)} · ${bytes(q.backlog_bytes)}`} />
                  </Table.Cell>
                  <Table.Cell className={`${NUM} ${q.oldest_ms > 60_000 ? TONES.warning : ""}`}>{q.backlog ? span(q.oldest_ms) : "—"}</Table.Cell>
                  <Table.Cell className="text-right">
                    {q.pollers === 0 ? (
                      <Badge variant="warning" className="whitespace-nowrap">
                        0 pollers
                      </Badge>
                    ) : (
                      <span className={NUM}>{count(q.pollers)}</span>
                    )}
                  </Table.Cell>
                  <Table.Cell>
                    <Meter label="Slots" value={q.slots ? (q.running / q.slots) * 100 : 0} customValue={`${count(q.running)} of ${count(q.slots)}`} />
                  </Table.Cell>
                  <Table.Cell className={`${NUM} ${q.failed_minute ? TONES.warning : ""}`}>{count(q.failed_minute)}</Table.Cell>
                  <Table.Cell className={`${NUM} ${q.refused ? TONES.warning : ""}`}>{count(q.refused)}</Table.Cell>
                  <Table.Cell className={`${CLIP} ${MONO}`} title={q.workers.join(", ")}>
                    {q.workers.length ? q.workers.join(", ") : <span className={TONES.warning}>none</span>}
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table>
        </div>
      )}
    </Card>
  );
}

/** The most recent poll over the worker's queues: "now" while one is out. */
function lastPoll(w: Worker): string {
  const ages = Object.values(w.last_poll_ms ?? {});
  if (ages.length === 0) return "—";
  const least = Math.min(...ages);
  return least === 0 ? "now" : ago(least);
}

function Workers() {
  const workers = useWorkers();
  const rows = workers.data?.workers ?? [];
  return (
    <Card label="Workers" title="Workers">
      {workers.isPending ? (
        <Loading what="the workers" />
      ) : workers.error && !workers.data ? (
        <Failed what="the workers" error={workers.error} />
      ) : rows.length === 0 ? (
        <Empty title="No workers" detail="No node has the worker role. Tasks wait in their queues." />
      ) : (
        <div className="overflow-x-auto">
          <Table className="min-w-[620px]">
            <Table.Header>
              <Table.Row>
                <Table.Head>Node</Table.Head>
                <Table.Head>State</Table.Head>
                <Table.Head>Queues and slots in use</Table.Head>
                <Table.Head className="w-[120px] text-right">Last poll</Table.Head>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {rows.map((w) => (
                <Table.Row key={w.node}>
                  <Table.Cell className={MONO}>{w.node}</Table.Cell>
                  <Table.Cell>{!w.available ? <Status tone="danger">gone</Status> : w.draining ? <Status tone="warning">draining</Status> : <Status tone="success">up</Status>}</Table.Cell>
                  <Table.Cell className={MONO}>
                    <span className="flex flex-wrap gap-1">
                      {w.queues.map((queue) => (
                        <Badge key={queue} variant="outline" className="font-mono">
                          {queue} {w.running[queue] ?? "?"} of {w.slots[queue] ?? "?"}
                        </Badge>
                      ))}
                    </span>
                  </Table.Cell>
                  <Table.Cell className={NUM}>{lastPoll(w)}</Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table>
        </div>
      )}
    </Card>
  );
}

export function QueuesPage() {
  const queues = useQueues();
  const idle = (queues.data?.queues ?? []).filter((q) => q.backlog > 0 && q.pollers === 0);
  return (
    <Page crumbs={[{ label: "Queues and workers" }]} title="Queues and workers" help={HELP}>
      {idle.length > 0 && <Notice tone="warning">No worker polls {idle.map((q) => q.name).join(", ")}. Its tasks wait.</Notice>}
      <Queues />
      <Workers />
    </Page>
  );
}
