import { Badge, Button, cn, Link, Table } from "@cloudflare/kumo";
import { CaretDownIcon, CaretRightIcon } from "@phosphor-icons/react";
import { Fragment, useState } from "react";
import { finished, type Run, type RunEvent, type Step } from "../../api";
import { Empty, NUM, PRE } from "../../components/ui";
import { clock, count, shown, span, when } from "../../format";
import { useNow } from "../../hooks";

/** The rows it shows first, and how many more each "Show more" adds. */
export const ROWS = 200;

type Row = { seq: number; step: Step; event?: undefined } | { seq: number; event: RunEvent; step?: undefined };

const STEP_BADGE: Record<Step["status"], "success" | "info" | "warning" | "secondary" | "error"> = {
  done: "success",
  running: "info",
  retrying: "warning",
  waiting: "secondary",
  failed: "error",
  timed_out: "error",
};

function stepLabel(step: Step, run: Run) {
  switch (step.kind) {
    case "activity":
      return <>activity {step.name}</>;
    case "sleep":
      return <>sleep</>;
    case "signal":
      return <>signal {step.name}</>;
    case "child":
      return step.name.startsWith(`${run.id}/`) ? (
        <>
          child{" "}
          <Link href={`/runs/${step.name}`} variant="plain" onClick={(e) => e.stopPropagation()} className="font-mono">
            {step.name}
          </Link>
        </>
      ) : (
        <>child {step.name}</>
      );
    default:
      return <>value {step.kind}</>;
  }
}

function eventLabel(event: RunEvent): string {
  const reason = event.text ? `: ${event.text}` : "";
  switch (event.kind) {
    case "opened":
      return `opened on ${event.text}`;
    case "reopened":
      return `opened again on ${event.text}`;
    case "moved":
      return `moved to ${event.text}`;
    case "signal":
      return `signal ${event.text} came in`;
    case "cancel":
      return `cancel asked${reason}`;
    case "retried":
      return `retry from step ${event.text}`;
    default:
      return `${event.kind}${reason}`;
  }
}

/** Where a time is between the run's first and last, in percent. */
const at = (us: number, from: number, to: number) => Math.min(100, Math.max(0, ((us - from) / Math.max(1, to - from)) * 100));

/** A step's time as a bar, and an event's as a mark, on the run's span. Plain divs. */
function Bar({ from, to, start, end, tone }: { from: number; to: number; start: number | null; end: number | null; tone: string }) {
  if (start == null) return null;
  const left = at(start, from, to);
  return (
    <div className="relative h-2 w-full min-w-[120px] rounded-full bg-kumo-recessed" aria-hidden="true">
      {end == null || end > start ? <div className={cn("absolute inset-y-0 min-w-1 rounded-full", tone)} style={{ left: `${left}%`, width: `${Math.max(0, at(end ?? to, from, to) - left)}%` }} /> : <div className={cn("absolute inset-y-0 w-0.5", tone)} style={{ left: `${left}%` }} />}
    </div>
  );
}

const BAR_TONE: Record<Step["status"], string> = {
  done: "bg-kumo-success",
  running: "bg-kumo-info",
  retrying: "bg-kumo-warning",
  waiting: "bg-kumo-badge-neutral",
  failed: "bg-kumo-danger",
  timed_out: "bg-kumo-danger",
};

function Detail({ step }: { step: Step }) {
  const parts: [string, unknown][] = [
    ["Input", step.input],
    ["Output", step.output],
    ["Error", step.error],
  ];
  return (
    <div className="flex flex-col gap-3 py-2">
      <dl className="flex flex-wrap gap-x-6 gap-y-1 text-sm">
        <div className="flex gap-2">
          <dt className="text-kumo-subtle">Step</dt>
          <dd className="font-mono">{step.step}</dd>
        </div>
        {step.kind === "activity" && (
          <div className="flex gap-2">
            <dt className="text-kumo-subtle">Attempts</dt>
            <dd className="font-mono">{count(step.attempts)}</dd>
          </div>
        )}
        {step.wake_at_us != null && (
          <div className="flex gap-2">
            <dt className="text-kumo-subtle">{step.status === "retrying" ? "Next attempt" : "Wakes"}</dt>
            <dd className="font-mono">{when(step.wake_at_us / 1000)}</dd>
          </div>
        )}
      </dl>
      {parts
        .filter(([, value]) => value != null)
        .map(([name, value]) => (
          <section key={name} aria-label={name} className="flex flex-col gap-1">
            <h4 className={cn("text-sm font-medium", name === "Error" ? "text-kumo-danger" : "text-kumo-subtle")}>{step.kind === "activity" && name === "Error" && step.attempts > 1 ? "Error of the last attempt" : name}</h4>
            <pre className={PRE}>{shown(value)}</pre>
          </section>
        ))}
    </div>
  );
}

/** The run's steps and events in journal order, each with its time and a bar on the run's span. */
export function Timeline({ run }: { run: Run }) {
  const now = useNow();
  const [limit, setLimit] = useState(ROWS);
  const [open, setOpen] = useState<Set<number>>(new Set());
  const rows: Row[] = [...run.steps.map((step): Row => ({ seq: step.seq, step })), ...run.events.map((event): Row => ({ seq: event.seq, event }))]
    // Steps the journal has not named yet share the end's seq: they keep their step order.
    .sort((a, b) => a.seq - b.seq || (a.event ? -1 : b.event ? 1 : (a.step?.step ?? 0) - (b.step?.step ?? 0)));
  if (!rows.length) return <Empty title="Nothing yet" detail="The run has no steps." />;
  const times = rows.flatMap((r) => (r.step ? [r.step.started_us, r.step.ended_us] : [r.event.at_us])).filter((t): t is number => t != null);
  const from = run.started_us ?? Math.min(...times);
  const to = finished(run.status) ? Math.max(from + 1, ...times) : Math.max(now * 1000, ...times);
  const toggle = (step: number) => {
    const next = new Set(open);
    if (!next.delete(step)) next.add(step);
    setOpen(next);
  };
  return (
    <>
      <div className="overflow-x-auto">
        <Table className="min-w-[760px] table-fixed">
          <colgroup>
            <col className="w-[38%]" />
            <col className="w-[110px]" />
            <col className="w-[90px]" />
            <col className="w-[100px]" />
            <col />
          </colgroup>
          <Table.Header>
            <Table.Row>
              <Table.Head>Step or event</Table.Head>
              <Table.Head>Status</Table.Head>
              <Table.Head>At</Table.Head>
              <Table.Head className="text-right">Took</Table.Head>
              <Table.Head>Time in the run</Table.Head>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {rows.slice(0, limit).map((row) => {
              if (row.event) {
                const e = row.event;
                const moved = e.kind === "moved";
                return (
                  <Table.Row key={`e${row.seq}`} className="text-kumo-subtle">
                    <Table.Cell className="truncate pl-10 text-sm" title={eventLabel(e)}>
                      <span className={cn(moved && "font-medium text-kumo-default")}>{eventLabel(e)}</span>
                    </Table.Cell>
                    <Table.Cell />
                    <Table.Cell className="font-mono text-sm">{e.at_us != null ? clock(e.at_us / 1000, true) : "—"}</Table.Cell>
                    <Table.Cell />
                    <Table.Cell>
                      <Bar from={from} to={to} start={e.at_us} end={e.at_us} tone={moved ? "bg-kumo-warning" : "bg-kumo-badge-neutral"} />
                    </Table.Cell>
                  </Table.Row>
                );
              }
              const s = row.step;
              const expanded = open.has(s.step);
              const end = s.ended_us ?? (finished(run.status) ? null : now * 1000);
              return (
                <Fragment key={`s${s.step}`}>
                  <Table.Row className="cursor-pointer hover:bg-kumo-tint" onClick={() => toggle(s.step)}>
                    <Table.Cell className="truncate">
                      <button type="button" aria-expanded={expanded} aria-label={`Step ${s.step}: ${s.kind} ${s.name}`} className="mr-2 inline-flex size-6 items-center justify-center rounded text-kumo-subtle outline-none hover:bg-kumo-tint focus-visible:ring-2 focus-visible:ring-kumo-ring" onClick={(e) => (e.stopPropagation(), toggle(s.step))}>
                        {expanded ? <CaretDownIcon size={14} /> : <CaretRightIcon size={14} />}
                      </button>
                      <span className="font-mono text-sm">{stepLabel(s, run)}</span>
                    </Table.Cell>
                    <Table.Cell>
                      <Badge variant={STEP_BADGE[s.status]} className="whitespace-nowrap">
                        {s.status === "retrying" ? `retry ${s.attempts}` : s.status.replace("_", " ")}
                      </Badge>
                    </Table.Cell>
                    <Table.Cell className="font-mono text-sm">{s.started_us != null ? clock(s.started_us / 1000, true) : "—"}</Table.Cell>
                    <Table.Cell className={NUM}>{s.started_us != null && end != null ? span((end - s.started_us) / 1000) : "—"}</Table.Cell>
                    <Table.Cell>
                      <Bar from={from} to={to} start={s.started_us} end={end} tone={BAR_TONE[s.status]} />
                    </Table.Cell>
                  </Table.Row>
                  {expanded && (
                    <Table.Row>
                      <Table.Cell colSpan={5} className="bg-kumo-elevated pl-12">
                        <Detail step={s} />
                      </Table.Cell>
                    </Table.Row>
                  )}
                </Fragment>
              );
            })}
          </Table.Body>
        </Table>
      </div>
      {rows.length > limit && (
        <footer className="flex items-center gap-3 border-t border-kumo-line px-4 py-2.5 text-sm text-kumo-subtle">
          {limit.toLocaleString("en")} of {rows.length.toLocaleString("en")} rows
          <Button variant="secondary" size="sm" className="ml-auto" onClick={() => setLimit(limit + ROWS)}>
            Show more
          </Button>
        </footer>
      )}
    </>
  );
}
