import { ChartPalette, Link, Table } from "@cloudflare/kumo";
import { useQuery } from "@tanstack/react-query";
import { lazy, Suspense, useMemo } from "react";
import { get, OUTCOMES, RUNNER, workerPath, type Metrics, type Point, type WorkerDetail } from "../../api";
import type { Line, Unit } from "../../charts/Timeseries";
import { Card, Empty, Failed, Loading, MONO, Notice, NUM, Stat, Stats, Status, TONES } from "../../components/ui";
import { count, micros, percent } from "../../format";
import { useCluster } from "./queries";

// The charts load when a page shows one.
const Timeseries = lazy(() => import("../../charts/Timeseries"));

function Plot({ title, lines, unit }: { title: string; lines: Line[]; unit: Unit }) {
  const label = `${title}, the last hour, all runners`;
  const empty = lines.every((line) => line.points.every((p) => !p[1]));
  return (
    <Card title={title} label={label}>
      <div className="flex min-h-[250px] flex-col justify-center">
        {empty ? (
          <Empty title="No requests in the last hour" />
        ) : (
          <Suspense fallback={<Loading what="the chart" />}>
            <Timeseries lines={lines} unit={unit} label={label} />
          </Suspense>
        )}
      </div>
    </Card>
  );
}

const first: Line["color"] = (dark) => ChartPalette.categorical(0, dark);
const second: Line["color"] = (dark) => ChartPalette.categorical(1, dark);
const failed: Line["color"] = (dark) => ChartPalette.semantic("Attention", dark);
const line = (series: Point[], name: string, color: Line["color"], value: (p: Point) => number | null): Line => ({ name, color, points: series.map((p) => [p.at_ms, value(p)]) });

function Charts({ metrics }: { metrics: Metrics }) {
  const { series, sample_ms } = metrics;
  // The charts redraw when a new answer arrives, not on every render.
  const lines = useMemo(() => {
    const seconds = sample_ms / 1000;
    return {
      requests: [line(series, "Requests", first, (p) => p.requests / seconds), line(series, "Errors", failed, (p) => p.errors / seconds)],
      cpu: [line(series, "p50", first, (p) => p.cpu_p50_us), line(series, "p99", second, (p) => p.cpu_p99_us)],
      wall: [line(series, "p50", first, (p) => p.wall_p50_us), line(series, "p99", second, (p) => p.wall_p99_us)],
    };
  }, [series, sample_ms]);
  return (
    <div className="grid grid-cols-3 gap-4 max-xl:grid-cols-1">
      <Plot title="Requests and errors" lines={lines.requests} unit="/s" />
      <Plot title="CPU time" lines={lines.cpu} unit="us" />
      <Plot title="Wall time" lines={lines.wall} unit="us" />
    </div>
  );
}

export function Overview({ worker }: { worker: WorkerDetail }) {
  const metrics = useQuery({ queryKey: ["metrics", worker.name], queryFn: ({ signal }) => get<Metrics>(workerPath(worker.name, "/metrics"), signal), refetchInterval: 10_000 });
  const cluster = useCluster();
  if (metrics.isPending) return <Loading what="the numbers" />;
  if (metrics.error) return <Failed what="the numbers" error={metrics.error} />;
  const { total, by_node, silent } = metrics.data;
  const failures = OUTCOMES.slice(1).filter((outcome) => total.outcomes[outcome]);
  const left = (cluster.data?.gone ?? []).map((g) => g.node);
  const runners = [...(cluster.data?.nodes ?? []), ...left].filter((node) => node.roles.includes(RUNNER)).sort((a, b) => a.id.localeCompare(b.id));
  const d = worker.deployment;

  return (
    <div className="flex flex-col gap-4">
      {!d && (
        <Notice tone="warning" action={<Link href={`/workers/${worker.name}/versions`}>Deploy a version</Link>}>
          This Worker is not deployed. No runner runs it.
        </Notice>
      )}
      {silent.length > 0 && <Notice tone="warning">These runners did not answer: {silent.join(", ")}. The numbers do not include them.</Notice>}
      <Stats label="Totals" className="grid-cols-4 max-sm:grid-cols-2 2xl:grid-cols-8">
        <Stat label="Requests" value={count(total.requests)} hint="Runs that ended, from the start of each runner" />
        <Stat label="Errors" value={count(total.errors)} sub={total.requests ? percent((total.errors / total.requests) * 100) : undefined} tone={total.errors ? "danger" : undefined} hint="Runs that gave no response of their own" />
        <Stat label="Refused, busy" value={count(total.busy)} hint="Requests refused at the limit of the Worker on a runner" />
        <Stat label="In flight" value={count(total.in_flight)} />
        <Stat label="CPU p50" value={micros(total.cpu_p50_us)} />
        <Stat label="CPU p99" value={micros(total.cpu_p99_us)} />
        <Stat label="Wall p50" value={micros(total.wall_p50_us)} />
        <Stat label="Wall p99" value={micros(total.wall_p99_us)} />
      </Stats>
      <Charts metrics={metrics.data} />
      <div className="grid grid-cols-[minmax(0,3fr)_minmax(0,2fr)] items-start gap-4 max-2xl:grid-cols-1">
        <Card
          title="By runner"
          label="Numbers by runner"
          footer={
            failures.length
              ? `Errors by cause: ${failures.map((outcome) => `${outcome.replaceAll("_", " ")} ${count(total.outcomes[outcome])}`).join(" · ")}`
              : undefined
          }
        >
          {by_node.length === 0 ? (
            <Empty title="No runner ran this Worker" detail="A runner shows here after its first request for the Worker." />
          ) : (
            <div className="overflow-x-auto">
              <Table className="min-w-[620px]">
                <Table.Header>
                  <Table.Row>
                    <Table.Head>Runner</Table.Head>
                    <Table.Head className="text-right">Requests</Table.Head>
                    <Table.Head className="text-right">Errors</Table.Head>
                    <Table.Head className="text-right">Cold starts</Table.Head>
                    <Table.Head className="text-right">CPU p50</Table.Head>
                    <Table.Head className="text-right">CPU p99</Table.Head>
                    <Table.Head className="text-right">Wall p99</Table.Head>
                  </Table.Row>
                </Table.Header>
                <Table.Body>
                  {by_node.map((row) => (
                    <Table.Row key={row.node}>
                      <Table.Cell className={MONO}>{row.node}</Table.Cell>
                      <Table.Cell className={NUM}>{count(row.requests)}</Table.Cell>
                      <Table.Cell className={`${NUM} ${row.errors ? TONES.danger : ""}`}>{count(row.errors)}</Table.Cell>
                      <Table.Cell className={NUM}>{count(row.cold_starts)}</Table.Cell>
                      <Table.Cell className={NUM}>{micros(row.cpu_p50_us)}</Table.Cell>
                      <Table.Cell className={NUM}>{micros(row.cpu_p99_us)}</Table.Cell>
                      <Table.Cell className={NUM}>{micros(row.wall_p99_us)}</Table.Cell>
                    </Table.Row>
                  ))}
                </Table.Body>
              </Table>
            </div>
          )}
        </Card>
        <Card title="Deployment" label="Deployment" footer={d ? `Deployment ${d.seq}` : undefined}>
          {cluster.isPending ? (
            <Loading what="the runners" />
          ) : cluster.error ? (
            <Failed what="the runners" error={cluster.error} />
          ) : runners.length === 0 ? (
            <Empty title="No runners" detail="The control node sees no node that runs Workers." />
          ) : (
            <Table>
              <Table.Header>
                <Table.Row>
                  <Table.Head>Runner</Table.Head>
                  <Table.Head>Serves</Table.Head>
                  <Table.Head>State</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {runners.map((node) => {
                  const gone = left.includes(node);
                  const served = gone ? undefined : node.serving?.[worker.name];
                  const behind = !!served && !!d && served.seq < d.seq;
                  return (
                    <Table.Row key={node.id}>
                      <Table.Cell className={MONO}>{node.id}</Table.Cell>
                      <Table.Cell className={MONO}>{served ? `v${served.stable}${served.canary ? `, canary v${served.canary.version} at ${served.canary.percent}%` : ""}` : "—"}</Table.Cell>
                      <Table.Cell>
                        {gone ? (
                          <Status tone="danger">Not in the cluster</Status>
                        ) : !node.serving ? (
                          <Status tone="warning">No answer</Status>
                        ) : !served ? (
                          <Status tone="muted">Not served</Status>
                        ) : behind ? (
                          <Status tone="warning">Old deployment</Status>
                        ) : (
                          <Status tone={served.warm ? "success" : "muted"}>{served.warm ? "Loaded" : "Idle"}</Status>
                        )}
                      </Table.Cell>
                    </Table.Row>
                  );
                })}
              </Table.Body>
            </Table>
          )}
        </Card>
      </div>
    </div>
  );
}
