import { Link, Table } from "@cloudflare/kumo";
import type { RunEntry } from "../../api";
import { Card, Empty, Failed, Loading, MONO, NUM, Status } from "../../components/ui";
import { UNKNOWN, whole } from "../../format";
import { useRuns } from "../../queries";
import { Page } from "../../shell/Page";

const HELP = (
  <>
    <p>A bench run is a directory in the results directory. bench.sh writes it. The console reads it again each time you open this page.</p>
    <ul>
      <li>Rows: the lines of results.csv. Each line is one phase of one workload on one target.</li>
      <li>Rows with errors: the rows with failed operations. The summary leaves them out of its tables.</li>
      <li>Faults: the fault phases that passed, of all fault phases, and the soak. fault.sh writes them.</li>
      <li>A run of a shared box is indicative. Its summary says so.</li>
    </ul>
  </>
);

function Faults({ faults }: { faults: RunEntry["faults"] }) {
  if (!faults) return <span className="text-kumo-subtle">{UNKNOWN}</span>;
  const all = faults.passed === faults.phases && faults.soak !== false;
  const soak = faults.soak == null ? "" : faults.soak ? ", soak passes" : ", soak fails";
  return (
    <Status tone={all ? "success" : "danger"}>
      {faults.passed} of {faults.phases} pass{soak}
    </Status>
  );
}

export function RunsPage() {
  const runs = useRuns();
  const list = runs.data?.runs ?? [];
  return (
    <Page crumbs={[{ label: "Runs" }]} title="Bench runs" help={HELP}>
      <Card label="Runs" footer={runs.data && <span className="font-mono break-all">{runs.data.dir}</span>}>
        {runs.isPending ? (
          <Loading what="the runs" />
        ) : runs.error ? (
          <Failed what="the runs" error={runs.error} />
        ) : list.length === 0 ? (
          <Empty title="No runs" detail="Run bench.sh, or give the console a results directory with --results." />
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[760px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Run</Table.Head>
                  <Table.Head>Box</Table.Head>
                  <Table.Head>Kind</Table.Head>
                  <Table.Head className="text-right">Rounds</Table.Head>
                  <Table.Head className="text-right">Rows</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Rows with errors</Table.Head>
                  <Table.Head>Faults</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {list.map((run) => (
                  <Table.Row key={run.name}>
                    <Table.Cell className={`${MONO} whitespace-nowrap`}>
                      <Link href={`/runs/${encodeURIComponent(run.name)}`}>{run.name}</Link>
                    </Table.Cell>
                    <Table.Cell className={MONO}>{run.host ?? UNKNOWN}</Table.Cell>
                    <Table.Cell>{run.kind ?? UNKNOWN}</Table.Cell>
                    <Table.Cell className={NUM}>{run.rounds ?? UNKNOWN}</Table.Cell>
                    <Table.Cell className={NUM}>{whole(run.rows)}</Table.Cell>
                    <Table.Cell className={NUM}>{whole(run.error_rows)}</Table.Cell>
                    <Table.Cell>
                      <Faults faults={run.faults} />
                    </Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
    </Page>
  );
}
