import { Button, Input, Link, Table, Tooltip } from "@cloudflare/kumo";
import { PlusIcon } from "@phosphor-icons/react";
import { useState, type ReactNode } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { finished, STATUSES, type RunList, type RunRow } from "../../api";
import { Card, Empty, Failed, Loading, MONO, Notice, NUM, Off, Pick, RunBadge, Segmented, Stamp } from "../../components/ui";
import { clock, span, when } from "../../format";
import { useNow } from "../../hooks";
import { useRuns, useWorkflows } from "../../queries";
import { useOff } from "../../shell/Health";
import { Page } from "../../shell/Page";
import { StartDialog } from "./StartDialog";

const SINCE = [
  { value: "1h", label: "1 h" },
  { value: "24h", label: "24 h" },
  { value: "7d", label: "7 d" },
  { value: "all", label: "All" },
];
/** The filters the API takes, as the page's URL holds them. */
const FILTERS = ["workflow", "status", "tag", "parent", "prefix", "since"] as const;
const ALL = "all";
const PAGE = 50;

/** The rows of `fresh` that are new or changed since `held` was read. */
function changes(held: RunRow[], fresh: RunRow[]): number {
  const was = new Map(held.map((run) => [run.id, run.updated_ms]));
  return fresh.filter((run) => was.get(run.id) !== run.updated_ms).length;
}

/** What a running run waits for. */
function Waits({ run }: { run: RunRow }) {
  if (run.status !== "running" || !run.waiting) return <span className="text-kumo-subtle">—</span>;
  if (run.waiting === "timer" && run.due_ms != null) {
    return (
      <Tooltip content="A durable timer: a sleep, or an activity's next attempt" render={<span className="cursor-help whitespace-nowrap" />}>
        until {when(run.due_ms)}
      </Tooltip>
    );
  }
  return (
    <Tooltip content="A signal, or the result of an activity or a child" render={<span className="cursor-help whitespace-nowrap" />}>
      a call
    </Tooltip>
  );
}

function Duration({ run }: { run: RunRow }) {
  const now = useNow();
  return <>{span((finished(run.status) ? run.updated_ms : now) - run.started_ms)}</>;
}

/** A text filter: it applies on Enter and when the field loses focus. */
function TextFilter({ label, value, placeholder, onApply }: { label: string; value: string; placeholder: string; onApply: (value: string) => void }) {
  const [text, setText] = useState(value);
  const [shown, setShown] = useState(value);
  if (shown !== value) {
    setShown(value);
    setText(value);
  }
  return (
    <Input
      aria-label={label}
      value={text}
      placeholder={placeholder}
      onChange={(e) => setText(e.target.value)}
      onBlur={() => text !== value && onApply(text.trim())}
      onKeyDown={(e) => e.key === "Enter" && onApply(text.trim())}
      className="w-40 font-mono max-sm:w-full"
    />
  );
}

const HELP: ReactNode = (
  <dl>
    <dt>Order</dt>
    <dd>The newest change first. With a parent or an id prefix, the runs are in id order.</dd>
    <dt>Changes</dt>
    <dd>The list does not move while you read it. When runs start or change, a button shows how many. Click it to see them.</dd>
    <dt>Where the list comes from</dt>
    <dd>The first history node that answers. It can be one change behind.</dd>
    <dt>Waits</dt>
    <dd>For a running run: the time of its next timer, or a call (a signal, or a result of an activity or a child).</dd>
    <dt>Tag</dt>
    <dd>key:value, for example customer:a1.</dd>
    <dt>Parent</dt>
    <dd>The id of a run: its children are the runs it started with ctx.child.</dd>
  </dl>
);

export function RunsPage() {
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const off = useOff();
  const workflows = useWorkflows();
  // Each page's cursor: the first page has none.
  const [cursors, setCursors] = useState<string[]>([]);
  const filter = (key: (typeof FILTERS)[number]) => params.get(key) ?? "";
  const set = (key: string, value: string) => {
    const next = new URLSearchParams(params);
    if (value && value !== ALL) next.set(key, value);
    else next.delete(key);
    setParams(next, { replace: true });
    setCursors([]);
  };
  const query = new URLSearchParams();
  for (const key of FILTERS) if (filter(key)) query.set(key, filter(key));
  query.set("limit", String(PAGE));
  if (cursors.length) query.set("cursor", cursors[cursors.length - 1]);
  const key = query.toString();
  const list = useRuns(key);
  // The answer the page shows: a poll does not move its rows, and a newer one waits behind a button.
  const [held, setHeld] = useState<{ key: string; data: RunList; at: number }>();
  const fresh = list.data && !list.isPlaceholderData && list.isFetchedAfterMount ? list.data : undefined;
  const take = () => fresh && setHeld({ key, data: fresh, at: list.dataUpdatedAt });
  if (fresh && (held?.key !== key || (!held.data.runs.length && fresh.runs.length))) take();
  const data = held?.key === key ? held.data : list.data;
  const waiting = fresh && held?.key === key ? changes(held.data.runs, fresh.runs) : 0;
  const rows = data?.runs ?? [];
  const starting = params.has("start");
  const setStarting = (on: boolean) => {
    const next = new URLSearchParams(params);
    if (on) next.set("start", "");
    else next.delete("start");
    setParams(next, { replace: true });
  };
  const filtered = FILTERS.some((key) => filter(key));

  const startButton = off ? (
    <Off reason={off}>
      <Button variant="primary" icon={PlusIcon} disabled>
        Start run
      </Button>
    </Off>
  ) : (
    <Button variant="primary" icon={PlusIcon} onClick={() => setStarting(true)}>
      Start run
    </Button>
  );

  return (
    <Page crumbs={[{ label: "Runs" }]} title="Runs" actions={startButton} help={HELP}>
      <div role="group" aria-label="Filters" className="flex flex-wrap items-center gap-2">
        <Pick label="Workflow" value={filter("workflow") || ALL} options={[{ value: ALL, label: "All workflows" }, ...(workflows.data?.workflows ?? []).map((w) => ({ value: w.name, label: w.name }))]} onChange={(v) => set("workflow", v)} className="w-44 max-sm:w-full" />
        <Pick label="Status" value={filter("status") || ALL} options={[{ value: ALL, label: "All statuses" }, ...STATUSES.map((s) => ({ value: s, label: s }))]} onChange={(v) => set("status", v)} className="w-40 max-sm:w-full" />
        <TextFilter label="Tag" value={filter("tag")} placeholder="key:value" onApply={(v) => set("tag", v)} />
        <TextFilter label="Parent" value={filter("parent")} placeholder="Parent id" onApply={(v) => set("parent", v)} />
        <TextFilter label="Id prefix" value={filter("prefix")} placeholder="Id prefix" onApply={(v) => set("prefix", v)} />
        <Segmented label="Started in" value={filter("since") || ALL} options={SINCE} onChange={(v) => set("since", v)} />
      </div>
      {data?.cut && <Notice tone="warning">Scanned 10,000 rows. Narrow the filter.</Notice>}
      <div className="flex h-8 items-center gap-3 text-sm text-kumo-subtle">
        {held?.key === key && <span className="tabular-nums">Read at {clock(held.at, true)}</span>}
        {waiting > 0 && (
          <Button variant="secondary" size="sm" onClick={take}>
            Show {waiting} {waiting === 1 ? "change" : "changes"}
          </Button>
        )}
      </div>
      <Card label="Runs">
        {list.error && !data ? (
          <Failed what="the runs" error={list.error} />
        ) : !data ? (
          <Loading what="the runs" />
        ) : rows.length === 0 ? (
          <Empty title="No runs" detail={filtered ? "No run agrees with the filter." : "Start a run here, with the API, or with wf start."}>
            {!filtered && startButton}
          </Empty>
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[920px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Id</Table.Head>
                  <Table.Head>Workflow</Table.Head>
                  <Table.Head>Status</Table.Head>
                  <Table.Head>Waits for</Table.Head>
                  <Table.Head>Started</Table.Head>
                  <Table.Head className="text-right">Duration</Table.Head>
                  <Table.Head>Owner</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {rows.map((run) => (
                  <Table.Row key={run.id} className="cursor-pointer hover:bg-kumo-tint" onClick={() => void navigate(`/runs/${run.id}`)}>
                    <Table.Cell className="max-w-[280px] truncate">
                      <Link href={`/runs/${run.id}`} variant="plain" onClick={(e) => e.stopPropagation()} className="font-mono text-sm font-medium" title={run.id}>
                        {run.id}
                      </Link>
                    </Table.Cell>
                    <Table.Cell className={MONO}>
                      {run.workflow}@{run.version}
                    </Table.Cell>
                    <Table.Cell>
                      <RunBadge status={run.status} />
                    </Table.Cell>
                    <Table.Cell className="text-sm">
                      <Waits run={run} />
                    </Table.Cell>
                    <Table.Cell>
                      <Stamp ms={run.started_ms} />
                    </Table.Cell>
                    <Table.Cell className={NUM}>
                      <Duration run={run} />
                    </Table.Cell>
                    <Table.Cell className={MONO}>{run.owner ?? <span className="text-kumo-subtle">—</span>}</Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table>
          </div>
        )}
        {(cursors.length > 0 || data?.next) && (
          <footer className="flex items-center justify-end gap-2 border-t border-kumo-line px-4 py-2.5">
            <span className="mr-auto text-sm text-kumo-subtle">Page {cursors.length + 1}</span>
            <Button variant="secondary" size="sm" disabled={!cursors.length} onClick={() => setCursors(cursors.slice(0, -1))}>
              Previous
            </Button>
            <Button variant="secondary" size="sm" disabled={!data?.next} onClick={() => data?.next && setCursors([...cursors, data.next])}>
              Next
            </Button>
          </footer>
        )}
      </Card>
      {starting && <StartDialog workflow={filter("workflow") || undefined} onClose={() => setStarting(false)} />}
    </Page>
  );
}
