import { Table } from "@cloudflare/kumo";
import { lazy, Suspense, useState } from "react";
import { useParams } from "react-router";
import { Refusal, type Run } from "../../api";
import type { Bars } from "../../charts/Bars";
import { Block, Card, Empty, Failed, Loading, MONO, NUM, Segmented, Stat, Stats, Status } from "../../components/ui";
import { UNKNOWN, whole } from "../../format";
import { useRun } from "../../queries";
import { Page } from "../../shell/Page";
import { Summary } from "./Summary";

const BarsChart = lazy(() => import("../../charts/Bars"));

/** The tables of `bench.sh` in the order of its summary, and their names. */
const TABLES: Record<string, string> = { ceilings: "Ceilings", M: "M", S1: "S1", R: "R", "R-lease": "R, lease reads" };
const WORKLOADS = ["a", "b", "c", "d", "e", "f"];

const median = (values: number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

/** The saturation rows of each table: per target, the median ops/s of each workload over the rounds. */
function saturation(run: Run): Map<string, Bars> {
  const at = (name: string) => run.columns.indexOf(name);
  const [table, target, workload, phase, ops, errors] = ["table", "target", "workload", "phase", "ops_s", "errors"].map(at);
  const tables = new Map<string, Map<string, Map<string, number[]>>>();
  if ([table, target, workload, phase, ops].some((c) => c < 0)) return new Map();
  for (const row of run.rows) {
    const value = Number(row[ops]);
    // As summary.md: a row with errors is left out.
    if (row[phase] !== "sat" || Number(row[errors]) > 0 || !Number.isFinite(value) || !/^workload[a-f]$/.test(row[workload])) continue;
    const targets = tables.get(row[table]) ?? new Map<string, Map<string, number[]>>();
    tables.set(row[table], targets);
    const workloads = targets.get(row[target]) ?? new Map<string, number[]>();
    targets.set(row[target], workloads);
    const w = row[workload].slice("workload".length);
    workloads.set(w, [...(workloads.get(w) ?? []), value]);
  }
  const out = new Map<string, Bars>();
  for (const name of Object.keys(TABLES).filter((t) => tables.has(t)).concat([...tables.keys()].filter((t) => !(t in TABLES)))) {
    const targets = tables.get(name)!;
    const groups = [...targets.keys()];
    const used = WORKLOADS.filter((w) => groups.some((g) => targets.get(g)!.has(w)));
    out.set(name, { groups, series: used.map((w) => ({ name: w.toUpperCase(), values: groups.map((g) => (targets.get(g)!.has(w) ? median(targets.get(g)!.get(w)!) : null)) })) });
  }
  return out;
}

const HELP = (
  <>
    <p>This page shows one bench run: its numbers, its summary and the box it ran on.</p>
    <ul>
      <li>Saturation: each target with 64 requests outstanding. A bar is the median of the rounds. A letter is a YCSB workload: A is 50 % writes, B 5 % writes, C reads only, D reads of the latest writes, F read-modify-write.</li>
      <li>M: the write is acknowledged from memory. S1: after one local fsync. R: after a majority of three fsyncs.</li>
      <li>The tables below are summary.md, as summary.py wrote it. A range in brackets gives the value of each round.</li>
      <li>Faults: each fault phase of fault.sh, and whether its check passed. Lost writes or stale reads make a phase fail.</li>
    </ul>
  </>
);

export function RunPage() {
  const name = useParams().name ?? "";
  const run = useRun(name);
  const missing = run.error instanceof Refusal && run.error.status === 404;
  return (
    <Page crumbs={[{ label: "Runs", href: "/runs" }, { label: name }]} title={<span className="font-mono text-xl">{name}</span>} help={HELP}>
      {run.isPending ? (
        <Card>
          <Loading what="the run" />
        </Card>
      ) : missing ? (
        <Card>
          <Empty title="Run not found" detail="The results directory has no run of this name." />
        </Card>
      ) : run.error ? (
        <Card>
          <Failed what="the run" error={run.error} />
        </Card>
      ) : (
        <RunView run={run.data!} />
      )}
    </Page>
  );
}

function RunView({ run }: { run: Run }) {
  const tables = saturation(run);
  const names = [...tables.keys()];
  const [picked, setPicked] = useState<string | null>(null);
  const table = picked && tables.has(picked) ? picked : (names.find((n) => n === "M") ?? names[0]);
  // The soak is not a phase: it shows apart, as on the list of runs.
  const phases = run.faults.filter((f) => f.phase !== "soak");
  const passed = phases.filter((f) => f.pass).length;
  const soak = run.faults.find((f) => f.phase === "soak");
  return (
    <>
      <Stats label="Run" className="grid-cols-4 max-md:grid-cols-2">
        <Stat label="Box" value={<span className="font-mono text-lg">{run.host ?? UNKNOWN}</span>} />
        <Stat label="Kind" value={run.kind ?? UNKNOWN} sub={run.rounds ? `${run.rounds} ${run.rounds === 1 ? "round" : "rounds"}` : undefined} />
        <Stat label="Rows" value={whole(run.rows.length)} sub={`${whole(run.error_rows)} with errors`} tone={run.error_rows ? "warning" : undefined} hint="The lines of results.csv, and the lines with failed operations. The summary leaves those out of its tables." />
        <Stat
          label="Faults"
          value={phases.length ? `${passed} of ${phases.length}` : UNKNOWN}
          sub={soak ? (soak.pass ? "pass, soak passes" : "pass, soak fails") : phases.length ? "pass" : undefined}
          tone={phases.length || soak ? (passed === phases.length && soak?.pass !== false ? "success" : "danger") : undefined}
          hint="The fault phases whose check passed, and the soak."
        />
      </Stats>

      {table && (
        <Card
          title="Saturation"
          label="Saturation"
          controls={names.length > 1 && <Segmented label="Table" value={table} options={names.map((n) => ({ value: n, label: n }))} onChange={setPicked} />}
          footer={`${TABLES[table] ?? table}: operations per second with 64 requests outstanding, the median of the rounds.`}
        >
          <Suspense fallback={<div className="h-[200px]" />}>
            <BarsChart bars={tables.get(table)!} label={`Saturation of ${TABLES[table] ?? table}, operations per second`} unit="ops/s" />
          </Suspense>
        </Card>
      )}

      {/* The summary has its own faults table, where summary.py made one. */}
      {run.faults.length > 0 && !run.summary?.includes("\n## Faults") && (
        <Card title="Faults" label="Faults">
          <div className="overflow-x-auto">
            <Table className="min-w-[720px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Phase</Table.Head>
                  <Table.Head>Node</Table.Head>
                  <Table.Head>Check</Table.Head>
                  <Table.Head className="text-right">Lost</Table.Head>
                  <Table.Head className="text-right">Stale</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Writes</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Longest gap</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {run.faults.map((f, i) => (
                  <Table.Row key={i}>
                    <Table.Cell className={MONO}>{f.phase}</Table.Cell>
                    <Table.Cell className={MONO}>{f.victim ?? UNKNOWN}</Table.Cell>
                    <Table.Cell>
                      <Status tone={f.pass ? "success" : "danger"}>{f.pass ? "Pass" : "Fail"}</Status>
                    </Table.Cell>
                    <Table.Cell className={NUM}>{whole(f.check?.lost)}</Table.Cell>
                    <Table.Cell className={NUM}>{whole(f.check?.stale)}</Table.Cell>
                    <Table.Cell className={NUM}>{whole(f.check?.writes_ok)}</Table.Cell>
                    <Table.Cell className={NUM}>{f.check ? `${whole(f.check.longest_window_ms)} ms` : UNKNOWN}</Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table>
          </div>
        </Card>
      )}

      {run.summary ? <Summary text={run.summary} /> : !table && <Empty title="No summary" detail="This run has no summary.md and no saturation rows." />}

      {run.env && (
        <Card title="Box" label="Box">
          <div className="p-4">
            <Block title="env.txt">{run.env}</Block>
          </div>
        </Card>
      )}
    </>
  );
}
