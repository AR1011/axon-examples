import { Badge, Button, Input, Table } from "@cloudflare/kumo";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router";
import { api, get, seg, workerPath, type LogRecord } from "../../api";
import { Block, Card, CLIP, Code, Empty, Failed, Field, Loading, Modal, MONO, Notice, NUM, Pick, Segmented, Stamp, Status } from "../../components/ui";
import { micros, statusTone } from "../../format";
import { Quiet } from "../cluster/Quiet";

/** As many lines as the page keeps of a tail. */
const KEPT = 500;
const ALL = "all";
const STATUSES = ["2xx", "3xx", "4xx", "5xx", "exception", "cpu_limit", "memory_limit", "wall_timeout", "engine", "response_too_large", "storage"];
const STATUS_OPTIONS = [{ value: ALL, label: "All statuses" }, ...STATUSES.map((s) => ({ value: s, label: s.replaceAll("_", " ") }))];

/** The API's own filter (`Filter::admits`), for the lines of a tail the page already has. */
function admits(r: LogRecord, status: string, q: string): boolean {
  const code = String(r.status);
  const byStatus = !status || status === code || status === r.outcome || (status.endsWith("xx") && status[0] === code[0]);
  return byStatus && (!q || r.url.includes(q) || r.logs.some((line) => line.includes(q)));
}

const pathOf = (url: string) => {
  try {
    const parsed = new URL(url);
    return parsed.pathname + parsed.search;
  } catch {
    return url;
  }
};

type Link = "connecting" | "open" | "lost";

/** The Worker's tail: every invocation that ends from now on, newest first. */
function useTail(name: string, live: boolean) {
  const [records, setRecords] = useState<LogRecord[]>([]);
  const [link, setLink] = useState<Link>("connecting");
  const [missed, setMissed] = useState(0);

  useEffect(() => {
    if (!live) return;
    const source = new EventSource(api + workerPath(name, "/tail"));
    source.onopen = () => setLink("open");
    source.onerror = () => {
      setLink("lost");
      // A session that ended looks the same to an EventSource: this call goes to the login page on a 401.
      void get("session").catch(() => null);
    };
    source.addEventListener("invocation", (event) => {
      const record = JSON.parse((event as MessageEvent<string>).data) as LogRecord;
      setRecords((had) => [record, ...had].slice(0, KEPT));
    });
    source.addEventListener("missed", (event) => setMissed((n) => n + (Number((event as MessageEvent<string>).data) || 0)));
    return () => {
      source.close();
      setLink("connecting");
    };
  }, [name, live]);

  return { records, link, missed, clear: () => (setRecords([]), setMissed(0)) };
}

function Detail({ trace, known, onClose }: { trace: string | null; known: LogRecord | null; onClose: () => void }) {
  const fetched = useQuery({ queryKey: ["invocation", trace], queryFn: ({ signal }) => get<LogRecord>(`invocations/${seg(trace!)}`, signal), enabled: !!trace, staleTime: Infinity });
  const r = fetched.data ?? known;
  return (
    <Modal sheet title={trace ? `Invocation ${trace}` : "Invocation"} onClose={onClose}>
      {!r ? (
        fetched.error ? (
          <Failed what="the invocation" error={fetched.error} />
        ) : (
          <Loading what="the invocation" />
        )
      ) : (
        <>
          <div className="flex items-center gap-3">
            <Code tone={statusTone(r.status, r.outcome)}>{r.status}</Code>
            <span className="min-w-0 font-mono text-sm break-all">
              {r.method} {pathOf(r.url)}
            </span>
          </div>
          <dl>
            <Field label="Time">{new Date(r.at_ms).toLocaleString()}</Field>
            <Field label="Result">{r.outcome.replaceAll("_", " ")}</Field>
            <Field label="Version">v{r.version}</Field>
            <Field label="Runner">{r.node}</Field>
            <Field label="CPU time">{micros(r.cpu_us)}</Field>
            <Field label="Wall time">{micros(r.wall_us)}</Field>
            <Field label="Trace">{r.trace ?? "—"}</Field>
            <Field label="URL">{r.url}</Field>
          </dl>
          <Block title="Console" empty="No console lines" note={r.truncated ? "The runner cut the console lines at its limit." : undefined}>
            {r.logs.join("\n")}
          </Block>
        </>
      )}
    </Modal>
  );
}

export function Logs({ name }: { name: string }) {
  const [params, setParams] = useSearchParams();
  const trace = params.get("trace");
  const [mode, setMode] = useState<"live" | "recent">(trace ? "recent" : "live");
  const [status, setStatus] = useState("");
  const [q, setQ] = useState("");
  // While paused the page shows the lines it had, and the tail goes on behind it.
  const [frozen, setFrozen] = useState<LogRecord[] | null>(null);
  const [selected, setSelected] = useState<LogRecord | null>(null);
  const tail = useTail(name, mode === "live");
  const paused = frozen != null;
  const shown = frozen ?? tail.records;
  const waiting = !frozen ? 0 : frozen.length ? tail.records.indexOf(frozen[0]) : tail.records.length;

  const query = new URLSearchParams({ limit: "200", ...(status && { status }), ...(q && { q }) });
  const recent = useQuery({ queryKey: ["invocations", name, status, q], queryFn: ({ signal }) => get<{ invocations: LogRecord[] }>(workerPath(name, `/invocations?${query}`), signal), enabled: mode === "recent", refetchInterval: 5000 });

  const rows = useMemo(() => (mode === "live" ? shown.filter((r) => admits(r, status, q)) : (recent.data?.invocations ?? [])), [mode, shown, status, q, recent.data]);
  const open = selected != null || trace != null;

  function select(record: LogRecord) {
    setSelected(record);
    setParams(record.trace ? { trace: record.trace } : {}, { replace: true });
  }
  function close() {
    setSelected(null);
    setParams({}, { replace: true });
  }

  return (
    <>
      <Card
        label="Logs"
        title={
          <>
            <Segmented label="Source" value={mode} options={[{ value: "live", label: "Live tail" }, { value: "recent", label: "Recent" }]} onChange={setMode} />
            <Badge variant="outline">{rows.length}</Badge>
            {mode === "live" && <Status tone={paused ? "warning" : tail.link === "open" ? "success" : tail.link === "lost" ? "danger" : "muted"}>{paused ? "Paused" : tail.link === "open" ? "Connected" : tail.link === "lost" ? "Connecting again" : "Connecting"}</Status>}
          </>
        }
        controls={
          <>
            <Pick label="Filter by status" value={status || ALL} onChange={(next) => setStatus(next === ALL ? "" : next)} options={STATUS_OPTIONS} className="w-[170px]" />
            <Input value={q} onChange={(e) => setQ(e.target.value)} type="search" aria-label="Filter by text" placeholder="Text in the URL or the console" className="w-[240px] max-xl:w-[170px]" />
            {mode === "live" && (
              <>
                <Button variant="secondary" aria-pressed={paused} onClick={() => setFrozen(paused ? null : tail.records)}>
                  {paused ? `Resume${waiting > 0 ? ` (${waiting} new)` : ""}` : "Pause"}
                </Button>
                <Button variant="secondary" onClick={() => (tail.clear(), setFrozen(paused ? [] : null))} disabled={!tail.records.length}>
                  Clear
                </Button>
              </>
            )}
          </>
        }
      >
        <Quiet runners effect="The console gets no new lines from them." className="border-b border-kumo-line p-3" />
        {mode === "live" && tail.missed > 0 && (
          <div className="border-b border-kumo-line p-3">
            <Notice tone="warning">The tail was too slow and did not get {tail.missed} invocations.</Notice>
          </div>
        )}
        {mode === "recent" && recent.isPending ? (
          <Loading what="the invocations" />
        ) : mode === "recent" && recent.error ? (
          <Failed what="the invocations" error={recent.error} />
        ) : rows.length === 0 ? (
          mode === "live" ? (
            shown.length ? (
              <Empty title="No line agrees with the filter" detail="Change the status or the text." />
            ) : (
              <Empty title="No invocations yet" detail="A line shows here when a request for this Worker ends. Send a request from the Code tab, or to the URL of the Worker." />
            )
          ) : (
            <Empty title="No invocations" detail={status || q ? "No kept invocation agrees with the filter." : "The control node keeps no invocation of this Worker."} />
          )
        ) : (
          <div className="max-h-[max(360px,calc(100dvh-15.75rem))] overflow-auto">
            <Table className="min-w-[900px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Time</Table.Head>
                  <Table.Head>Status</Table.Head>
                  <Table.Head>Method</Table.Head>
                  <Table.Head className="w-[22%]">Path</Table.Head>
                  <Table.Head className="w-[24%]">Console</Table.Head>
                  <Table.Head>Version</Table.Head>
                  <Table.Head>Runner</Table.Head>
                  <Table.Head className="text-right">CPU</Table.Head>
                  <Table.Head className="text-right">Wall</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {rows.map((r, i) => (
                  <Table.Row
                    key={r.trace ?? `${r.at_ms}-${r.node}-${i}`}
                    tabIndex={0}
                    variant={(trace != null && r.trace === trace) || r === selected ? "selected" : "default"}
                    className="cursor-pointer hover:bg-kumo-tint"
                    onClick={() => select(r)}
                    onKeyDown={(e) => e.key === "Enter" && select(r)}
                  >
                    <Table.Cell className="whitespace-nowrap">
                      <Stamp ms={r.at_ms} />
                    </Table.Cell>
                    <Table.Cell>
                      <Code tone={statusTone(r.status, r.outcome)}>
                        {r.status}
                        {r.outcome !== "ok" && ` ${r.outcome.replaceAll("_", " ")}`}
                      </Code>
                    </Table.Cell>
                    <Table.Cell className={MONO}>{r.method}</Table.Cell>
                    <Table.Cell className={`${MONO} ${CLIP}`} title={r.url}>
                      {pathOf(r.url)}
                    </Table.Cell>
                    <Table.Cell className={`${MONO} ${CLIP} text-kumo-subtle`} title={r.logs.join("\n")}>
                      {r.logs[0]?.split("\n")[0] ?? ""}
                      {r.logs.length > 1 && ` (+${r.logs.length - 1})`}
                    </Table.Cell>
                    <Table.Cell className={MONO}>v{r.version}</Table.Cell>
                    <Table.Cell className={MONO}>{r.node}</Table.Cell>
                    <Table.Cell className={NUM}>{micros(r.cpu_us)}</Table.Cell>
                    <Table.Cell className={NUM}>{micros(r.wall_us)}</Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
      {open && <Detail trace={trace} known={selected} onClose={close} />}
    </>
  );
}
