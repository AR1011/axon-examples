import { Link, Tabs } from "@cloudflare/kumo";
import { useState, type ReactNode } from "react";
import { useParams, useSearchParams } from "react-router";
import { Refusal, runPath, STATUSES, type Run } from "../../api";
import { Card, CopyButton, Empty, Failed, Field, Loading, Notice, PRE, RunBadge } from "../../components/ui";
import { count, shown, when } from "../../format";
import { useRun, useRuns } from "../../queries";
import { Page } from "../../shell/Page";
import { curl } from "../runs/StartDialog";
import { Actions } from "./Actions";
import { History } from "./History";
import { Timeline } from "./Timeline";

const TABS = [
  { value: "timeline", label: "Timeline" },
  { value: "history", label: "History" },
];

const HELP: ReactNode = (
  <dl>
    <dt>Timeline</dt>
    <dd>Each step of the code, and each event of the run, in the order of the journal. A bar shows when a step ran in the time of the run. Click a step to see its input, output and error.</dd>
    <dt>Opened, moved</dt>
    <dd>An engine node took the run: it read the journal and ran the code again to the last step. Moved: another node than the last one took it, after a crash, a hand-off or a lost lease.</dd>
    <dt>Times</dt>
    <dd>Each time is the clock of the node that wrote the entry. Two nodes can show a small difference.</dd>
    <dt>History</dt>
    <dd>The journal entries as the history nodes keep them.</dd>
    <dt>Epoch</dt>
    <dd>The lease of the owner. Each new owner has a larger one.</dd>
  </dl>
);

/** The run's children: how many in each status, and a link to them in Runs. Only a run whose code starts children. */
function Children({ id }: { id: string }) {
  const children = useRuns(new URLSearchParams({ parent: id, limit: "200" }).toString());
  const runs = children.data?.runs ?? [];
  const by = STATUSES.map((status) => [status, runs.filter((r) => r.status === status).length] as const).filter(([, n]) => n);
  return (
    <Card label="Children" title="Children" controls={<Link href={`/runs?parent=${encodeURIComponent(id)}`}>Show in Runs</Link>}>
      <div className="flex min-h-12 flex-wrap items-center gap-x-5 gap-y-2 px-4 py-3">
        {children.isPending && <span className="text-sm text-kumo-subtle">Loading…</span>}
        {children.data && !runs.length && <span className="text-sm text-kumo-subtle">None yet</span>}
        {by.map(([status, n]) => (
          <span key={status} className="flex items-center gap-2">
            <RunBadge status={status} />
            <span className="font-mono text-sm tabular-nums">
              {count(n)}
              {children.data?.next && status === by[by.length - 1][0] ? "+" : ""}
            </span>
          </span>
        ))}
      </div>
    </Card>
  );
}

function Summary({ run }: { run: Run }) {
  return (
    <Card label="Summary">
      <dl className="grid grid-cols-2 gap-x-8 px-4 py-1 max-md:grid-cols-1">
        <div>
          <Field label="Workflow">
            {run.workflow}@{run.version}
          </Field>
          <Field label="Owner">{run.owner || "—"}</Field>
          <Field label="Epoch">{run.epoch || "—"}</Field>
          <Field label="Parent">
            {run.parent ? (
              <Link href={`/runs/${run.parent}`} variant="plain" className="font-mono">
                {run.parent}
              </Link>
            ) : (
              "—"
            )}
          </Field>
        </div>
        <div>
          <Field label="Started">{run.started_us ? when(run.started_us / 1000) : "—"}</Field>
          <Field label="Next due">{run.due_us ? when(run.due_us / 1000) : "—"}</Field>
          <Field label="Tags">{run.tags.length ? run.tags.map(([k, v]) => `${k}:${v}`).join(", ") : "—"}</Field>
          <Field label="Custom status">{run.custom_status == null ? "—" : JSON.stringify(run.custom_status)}</Field>
        </div>
      </dl>
      {run.output != null && (
        <section aria-label="Output" className="flex flex-col gap-1 border-t border-kumo-line px-4 py-3">
          <h3 className="text-sm font-medium text-kumo-subtle">Output</h3>
          <pre className={PRE}>{shown(run.output)}</pre>
        </section>
      )}
    </Card>
  );
}

export function RunPage() {
  const id = useParams()["*"] ?? "";
  const [params, setParams] = useSearchParams();
  const tab = params.get("tab") === "history" ? "history" : "timeline";
  const run = useRun(id);
  const [error, setError] = useState<unknown>(null);
  const crumbs = [{ label: "Runs", href: "/runs" }, { label: id }];
  const gone = run.error instanceof Refusal && run.error.status === 404;

  if (run.isPending) {
    return (
      <Page crumbs={crumbs} title={id}>
        <Loading what="the run" />
      </Page>
    );
  }
  if (gone || !run.data) {
    return (
      <Page crumbs={crumbs} title={id}>
        <Card label="Run">{gone ? <Empty title="Run not found or deleted" detail="No run has this id. A deleted run is not kept." /> : <Failed what="the run" error={run.error} />}</Card>
      </Page>
    );
  }
  const r = run.data;
  return (
    <Page
      crumbs={crumbs}
      title={<span className="font-mono">{r.id}</span>}
      badge={<RunBadge status={r.status} />}
      detail={
        <span className="flex items-center gap-1">
          <CopyButton text={r.id} label="Copy the run id" />
          <CopyButton text={curl("GET", runPath(r.id))} label="Copy a curl command for the run" />
        </span>
      }
      actions={<Actions run={r} onError={setError} />}
      help={HELP}
    >
      <Notice error={error} />
      {r.parked && <Notice tone="warning">Parked: {r.parked}</Notice>}
      {r.error && (r.status === "failed" || r.status === "terminated") && <Notice tone="danger">{r.error}</Notice>}
      <Summary run={r} />
      {r.steps.some((s) => s.kind === "child") && <Children id={r.id} />}
      <Card
        label={tab === "history" ? "History" : "Timeline"}
        controls={null}
        title={
          <div role="group" aria-label="Views">
            <Tabs variant="segmented" tabs={TABS} value={tab} onValueChange={(next) => setParams(next === "history" ? { tab: "history" } : {}, { replace: true })} />
          </div>
        }
      >
        {tab === "history" ? <History id={r.id} /> : <Timeline run={r} />}
      </Card>
    </Page>
  );
}
