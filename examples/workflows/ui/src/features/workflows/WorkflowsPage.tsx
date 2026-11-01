import { Badge, Button, Link, Table, Tooltip } from "@cloudflare/kumo";
import { useState, type ReactNode } from "react";
import { STATUSES, type WorkflowList } from "../../api";
import { Card, Empty, Failed, Loading, MONO, Notice, NUM, Off, TONES } from "../../components/ui";
import { count } from "../../format";
import { useWorkflows } from "../../queries";
import { useOff } from "../../shell/Health";
import { Page } from "../../shell/Page";
import { StartDialog } from "../runs/StartDialog";

const HELP: ReactNode = (
  <dl>
    <dt>Versions</dt>
    <dd>The versions of each workflow that this build has. A new run gets the latest one.</dd>
    <dt>Old version</dt>
    <dd>Runs that did not finish and that started on a version before the latest. They continue on their version.</dd>
    <dt>Missing version</dt>
    <dd>Runs that did not finish, on a version this build does not have. They are parked until a build with that version resumes them.</dd>
    <dt>Counts</dt>
    <dd>From the first history node that answers.</dd>
  </dl>
);

export function WorkflowsPage() {
  const list = useWorkflows();
  const off = useOff();
  const [starting, setStarting] = useState<string | null>(null);
  // A poll that no history node counted keeps the last counts: the table does not empty and fill again.
  const [counted, setCounted] = useState<WorkflowList>();
  if (list.data?.counted && list.data !== counted) setCounted(list.data);
  const data = list.data?.counted ? list.data : (counted ?? list.data);
  const rows = data?.workflows ?? [];
  return (
    <Page crumbs={[{ label: "Workflows" }]} title="Workflows" help={HELP}>
      <Card label="Workflows">
        {list.isPending ? (
          <Loading what="the workflows" />
        ) : list.error && !data ? (
          <Failed what="the workflows" error={list.error} />
        ) : rows.length === 0 ? (
          <Empty title="No workflows" detail="This build has no workflows, and no run names one." />
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[860px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Name</Table.Head>
                  <Table.Head>Versions here</Table.Head>
                  {STATUSES.map((status) => (
                    <Table.Head key={status} className="text-right capitalize">
                      {status}
                    </Table.Head>
                  ))}
                  <Table.Head className="text-right whitespace-nowrap">Old version</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Missing version</Table.Head>
                  <Table.Head />
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {rows.map((w) => (
                  <Table.Row key={w.name}>
                    <Table.Cell>
                      <Link href={`/runs?workflow=${encodeURIComponent(w.name)}`} variant="plain" className="font-mono text-sm font-medium">
                        {w.name}
                      </Link>
                    </Table.Cell>
                    <Table.Cell className={MONO}>
                      {w.versions.length ? (
                        <span className="flex flex-wrap gap-1">
                          {w.versions.map((v) => (
                            <Badge key={v} variant="outline" className="font-mono">
                              v{v}
                            </Badge>
                          ))}
                        </span>
                      ) : (
                        <Tooltip content="Runs name this workflow, and this build does not have it" render={<span className={`cursor-help ${TONES.warning}`} />}>
                          none
                        </Tooltip>
                      )}
                    </Table.Cell>
                    {STATUSES.map((status) => (
                      <Table.Cell key={status} className={NUM}>
                        {w.runs[status] ? (
                          <Link href={`/runs?workflow=${encodeURIComponent(w.name)}&status=${status}`} variant="plain" className={status === "failed" || status === "parked" ? TONES.warning : ""}>
                            {count(w.runs[status])}
                          </Link>
                        ) : (
                          <span className="text-kumo-subtle">0</span>
                        )}
                      </Table.Cell>
                    ))}
                    <Table.Cell className={NUM}>{count(w.old_version)}</Table.Cell>
                    <Table.Cell className={`${NUM} ${w.missing_version ? TONES.danger : ""}`}>{count(w.missing_version)}</Table.Cell>
                    <Table.Cell className="text-right">
                      {w.versions.length > 0 &&
                        (off ? (
                          <Off reason={off}>
                            <Button variant="secondary" size="sm" disabled>
                              Start run
                            </Button>
                          </Off>
                        ) : (
                          <Button variant="secondary" size="sm" onClick={() => setStarting(w.name)}>
                            Start run
                          </Button>
                        ))}
                    </Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
      {/* Below the table, so that it moves nothing when it comes and goes. */}
      {data && !data.counted && <Notice tone="warning">No history node gave the counts.</Notice>}
      {starting && <StartDialog workflow={starting} onClose={() => setStarting(null)} />}
    </Page>
  );
}
