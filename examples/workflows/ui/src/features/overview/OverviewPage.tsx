import { Link } from "@cloudflare/kumo";
import { lazy, Suspense, useState, type ReactNode } from "react";
import type { Counts } from "../../api";
import type { Line } from "../../charts/Timeseries";
import { Card, Empty, Failed, Loading, Notice, Stats } from "../../components/ui";
import { clock, count } from "../../format";
import { useMetrics } from "../../queries";
import { Page } from "../../shell/Page";

const Timeseries = lazy(() => import("../../charts/Timeseries"));

const COLORS = {
  started: (dark: boolean) => (dark ? "#60a5fa" : "#2563eb"),
  completed: (dark: boolean) => (dark ? "#4ade80" : "#16a34a"),
  failed: (dark: boolean) => (dark ? "#f87171" : "#dc2626"),
};

/** A count that goes to the runs it counts. */
function Count({ label, value, to, hint }: { label: string; value: number | undefined; to: string; hint: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-1 bg-kumo-base px-4 py-3">
      <span className="truncate text-sm text-kumo-subtle" title={hint}>
        {label}
      </span>
      <Link href={to} variant="plain" className="w-fit text-xl font-semibold tabular-nums hover:underline">
        {count(value)}
      </Link>
    </div>
  );
}

const HELP: ReactNode = (
  <dl>
    <dt>Chart</dt>
    <dd>Runs that started, completed and failed, per minute, for the last hour. This api node takes a point each 10 s, from a history node. It keeps points in memory only: after it starts, the chart is empty for a short time.</dd>
    <dt>Running</dt>
    <dd>Runs that have an owner and do work now.</dd>
    <dt>Sleeping</dt>
    <dd>Runs that wait for a timer, a signal or a result, with no owner. A timer starts them again.</dd>
    <dt>Paused</dt>
    <dd>Runs that an operator paused. They take calls, but do no work until a resume.</dd>
    <dt>Parked</dt>
    <dd>Runs that stopped because the code does not agree with the journal, or this build does not have their version.</dd>
    <dt>Stuck</dt>
    <dd>Runs that did not finish, did not stop, and wrote nothing to the journal for 60 s. Each history node tries to open them again every 30 s.</dd>
    <dt>Waiting over 7 days</dt>
    <dd>Runs that did not finish and had no change for 7 days.</dd>
  </dl>
);

/** The counts, with a dash for each while they load, so nothing below them moves. */
function Numbers({ counts }: { counts: Counts | null | undefined }) {
  return (
    <Stats label="Counts" className="grid-cols-4 max-lg:grid-cols-2 [&>:last-child]:col-span-2">
      <Count label="Running" value={counts?.running} to="/runs?status=running" hint="Runs that do work now" />
      <Count label="Sleeping" value={counts?.sleeping} to="/runs?status=running" hint="Runs that wait, with no owner" />
      <Count label="Paused" value={counts?.paused} to="/runs?status=paused" hint="Runs that an operator paused" />
      <Count label="Parked" value={counts?.parked} to="/runs?status=parked" hint="Runs that stopped: the code does not agree with the journal" />
      <Count label="Stuck" value={counts?.stuck} to="/runs?status=running" hint="Runs with no journal write for 60 s that did not stop" />
      <Count label="Failed, last hour" value={counts?.failed_hour} to="/runs?status=failed&since=1h" hint="Runs that failed in the last hour" />
      <Count label="Waiting over 7 days" value={counts?.old} to="/runs?status=running" hint="Runs with no change for 7 days" />
    </Stats>
  );
}

export function OverviewPage() {
  const metrics = useMetrics();
  // A poll that no history node counted keeps the last counts, so the numbers and the banner do not blink.
  const [last, setLast] = useState<Counts>();
  const now = metrics.data?.counts;
  if (now && now !== last) setLast(now);
  const counts = now ?? last;
  const points = metrics.data?.points ?? [];
  const lines: Line[] = (["started", "completed", "failed"] as const).map((name) => ({ name, color: COLORS[name], points: points.map((p) => [p.at_ms, p[name]]) }));
  return (
    <Page crumbs={[{ label: "Overview" }]} title="Overview" help={HELP}>
      <Numbers counts={counts} />
      <Card label="Runs per minute" title="Runs per minute">
        {metrics.isPending ? (
          <Loading what="the chart" />
        ) : metrics.error ? (
          <Failed what="the chart" error={metrics.error} />
        ) : points.length < 2 ? (
          <Empty title={`Collecting since ${clock(metrics.data.since_ms)}`} detail="A point comes each 10 s." />
        ) : (
          <Suspense fallback={<Loading what="the chart" />}>
            <Timeseries lines={lines} unit="/min" label="Runs started, completed and failed per minute, last hour" />
          </Suspense>
        )}
      </Card>
      {/* Below the chart, so that it moves nothing when it comes and goes. */}
      {metrics.data && !counts && <Notice tone="warning">No history node gave the counts.</Notice>}
    </Page>
  );
}
