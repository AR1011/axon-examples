import { Badge, Button, cn, Input, InputArea, Link, Tooltip } from "@cloudflare/kumo";
import { useMutation } from "@tanstack/react-query";
import { lazy, useState } from "react";
import { useSearchParams } from "react-router";
import { call, workerPath, type Ask, type Invoked, type Previewed, type WorkerDetail } from "../../api";
import { Block, Card, Code as Value, CopyButton, Empty, Failed, Field, Loading, Notice, Pick, useNotify } from "../../components/ui";
import { micros, statusTone } from "../../format";
import { activeVersion, useRefresh, useVersion } from "./queries";

const Editor = lazy(() => import("./Editor"));

const METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"].map((m) => ({ value: m, label: m }));
/** On a wide window the editor and the preview take the height the page leaves. */
const HIGH = "max-xl:h-[520px] xl:min-h-0";

/** "name: value" on each line. */
function parseHeaders(text: string): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const row of text.split("\n")) {
    const at = row.indexOf(":");
    if (at > 0) headers[row.slice(0, at).trim()] = row.slice(at + 1).trim();
  }
  return headers;
}

type Ran = { kind: "preview"; answer: Previewed } | { kind: "deployed"; answer: Invoked };

function Result({ ran, name }: { ran: Ran; name: string }) {
  const { answer } = ran;
  return (
    <div className="flex flex-col gap-3" aria-label="Response" role="region">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <Value tone={statusTone(answer.status, answer.error ? "error" : "ok")}>{answer.status}</Value>
        <span className="text-sm text-kumo-subtle">{ran.kind === "preview" ? "Preview of the code in the editor" : `Deployed version${ran.answer.version != null ? ` v${ran.answer.version}` : ""}`}</span>
      </div>
      {answer.error && <Notice tone="danger">{answer.error}</Notice>}
      <dl>
        <Field label="CPU time">{micros(answer.cpu_us)}</Field>
        <Field label="Wall time">{micros(answer.wall_us)}</Field>
        <Field label="Node">{answer.node ?? "—"}</Field>
        {ran.kind === "deployed" && (
          <Field label="Trace">
            <Link href={`/workers/${name}/logs?trace=${ran.answer.trace}`}>{ran.answer.trace}</Link>
          </Field>
        )}
        {ran.kind === "deployed" && ran.answer.retry_after_ms != null && <Field label="Try again after">{ran.answer.retry_after_ms} ms</Field>}
      </dl>
      <Block title="Headers" empty="No headers">
        {answer.headers.map(([key, value]) => `${key}: ${value}`).join("\n")}
      </Block>
      <Block title="Body" empty="Empty body" note={ran.kind === "preview" && ran.answer.body_truncated ? "The body is longer. This is the first part." : undefined}>
        {answer.body}
      </Block>
      {ran.kind === "preview" ? (
        <Block title="Console" empty="No console lines">
          {ran.answer.logs.join("\n")}
        </Block>
      ) : (
        <p className="text-sm text-kumo-subtle">The console lines of a deployed call are in the Logs tab. Select the trace.</p>
      )}
    </div>
  );
}

export function Code({ worker }: { worker: WorkerDetail }) {
  const [params, setParams] = useSearchParams();
  const asked = Number(params.get("version"));
  const id = worker.versions.some((v) => v.id === asked) ? asked : activeVersion(worker);
  const version = useVersion(worker.name, id);
  const refresh = useRefresh(worker.name);
  const notify = useNotify();
  // The draft belongs to the version it was typed over.
  const [draft, setDraft] = useState<{ id: number; text: string } | null>(null);
  const text = draft?.id === id ? draft.text : version.data?.source;
  const dirty = draft?.id === id && draft.text !== version.data?.source;
  const [message, setMessage] = useState("");
  const [ask, setAsk] = useState({ method: "GET", path: "/", headers: "", body: "" });
  const [ran, setRan] = useState<Ran | null>(null);
  // A version that is saved and not deployed: the page offers to deploy it.
  const [saved, setSaved] = useState<number | null>(null);
  const request = (): Ask => ({ method: ask.method, path: ask.path, headers: parseHeaders(ask.headers), body: ask.body });
  const deployed = (made: { version: number }) => notify(`Version ${made.version} is deployed. It takes all the traffic.`);

  const save = useMutation({
    mutationFn: (deploy: boolean) => call<{ version: number; seq: number | null }>("POST", workerPath(worker.name, "/versions"), { source: text, message, deploy }),
    onSuccess: async (made) => {
      await refresh();
      setSaved(made.seq == null ? made.version : null);
      if (made.seq != null) deployed(made);
      setMessage("");
      setParams({ version: String(made.version) }, { replace: true });
    },
  });
  const deploy = useMutation({
    mutationFn: (to: number) => call<{ seq: number; version: number }>("POST", workerPath(worker.name, "/deploy"), { version: to }),
    onSuccess: async (made) => {
      await refresh();
      setSaved(null);
      deployed(made);
    },
  });
  const preview = useMutation({
    mutationFn: () => call<Previewed>("POST", workerPath(worker.name, "/preview"), { source: text, request: request() }),
    onSuccess: (answer) => setRan({ kind: "preview", answer }),
    onError: () => setRan(null),
  });
  const invoke = useMutation({
    mutationFn: () => call<Invoked>("POST", workerPath(worker.name, "/invoke"), request()),
    onSuccess: (answer) => setRan({ kind: "deployed", answer }),
    onError: () => setRan(null),
  });
  const running = preview.isPending || invoke.isPending;
  const stable = worker.deployment?.stable;
  // The request of the form, as a client sends it to the first gateway.
  const at = worker.urls[0] && worker.urls[0].url.replace(/\/$/, "") + (ask.path.startsWith("/") ? ask.path : `/${ask.path}`);

  return (
    <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,3fr)_minmax(360px,2fr)] grid-rows-[minmax(0,1fr)] gap-4 max-xl:grid-cols-1 max-xl:grid-rows-none">
      <Card
        label="Code"
        className={HIGH}
        title={
          <>
            <Pick
              label="Version in the editor"
              value={String(id)}
              onChange={(next) => {
                setSaved(null);
                setParams({ version: next }, { replace: true });
              }}
              options={worker.versions.map((v) => ({ value: String(v.id), label: `v${v.id}${v.id === stable ? " (active)" : worker.deployment?.canary?.version === v.id ? " (canary)" : ""}` }))}
              className="min-w-[130px] font-mono"
            />
            {dirty && <Badge variant="warning">Changes not saved</Badge>}
          </>
        }
        controls={
          <>
            <Input value={message} onChange={(e) => setMessage(e.target.value)} placeholder="Version message" aria-label="Message for the new version" maxLength={200} className="w-[200px] max-2xl:w-[150px]" />
            <Button variant="secondary" disabled={text == null || save.isPending} onClick={() => (deploy.reset(), save.mutate(false))}>
              Save as new version
            </Button>
            <Button variant="primary" disabled={text == null || save.isPending} onClick={() => (deploy.reset(), save.mutate(true))}>
              Save and deploy
            </Button>
          </>
        }
      >
        {(save.error || deploy.error || saved != null) && (
          <div className="shrink-0 border-b border-kumo-line p-3">
            {save.error || deploy.error ? (
              <Notice error={save.error ?? deploy.error} />
            ) : (
              <Notice
                tone="muted"
                action={
                  <Button variant="secondary" size="sm" disabled={deploy.isPending} onClick={() => deploy.mutate(saved!)}>
                    Deploy version {saved}
                  </Button>
                }
              >
                Version {saved} is saved. It is not deployed.
              </Notice>
            )}
          </div>
        )}
        <div className="min-h-0 flex-1">
          {version.isPending ? (
            <Loading what={`version ${id}`} />
          ) : version.error ? (
            <Failed what={`version ${id}`} error={version.error} />
          ) : (
            <Editor key={`${worker.name}@${id}`} initial={text ?? ""} label="Source" onChange={(next) => setDraft({ id, text: next })} />
          )}
        </div>
      </Card>
      <Card label="Preview" title="Preview" className={HIGH}>
        <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto p-4">
          <form
            className="flex flex-col gap-2.5"
            onSubmit={(e) => {
              e.preventDefault();
              invoke.reset();
              preview.mutate();
            }}
          >
            <div className="flex gap-2">
              <Pick label="Method" value={ask.method} onChange={(method) => setAsk({ ...ask, method })} options={METHODS} className="w-[110px] shrink-0 font-mono" />
              <Input value={ask.path} onChange={(e) => setAsk({ ...ask, path: e.target.value })} aria-label="Path" placeholder="/path?query" required pattern="/.*" className="min-w-0 flex-1 font-mono" />
            </div>
            <InputArea value={ask.headers} onChange={(e) => setAsk({ ...ask, headers: e.target.value })} aria-label="Request headers" placeholder={"Headers, one on each line\ncontent-type: application/json"} rows={2} className="font-mono" />
            <InputArea value={ask.body} onChange={(e) => setAsk({ ...ask, body: e.target.value })} aria-label="Request body" placeholder="Body" rows={3} className="font-mono" />
            <div className="flex flex-wrap gap-2">
              <Button type="submit" variant="primary" disabled={text == null || running} loading={preview.isPending}>
                Run preview
              </Button>
              <Tooltip
                content={worker.deployment ? "Sends the request to the deployed version, as a gateway does" : "The Worker is not deployed"}
                render={<Button type="button" variant="secondary" disabled={!worker.deployment || running} loading={invoke.isPending} onClick={() => (preview.reset(), invoke.mutate())} />}
              >
                Call deployed version
              </Tooltip>
            </div>
            {at && (
              <p aria-label="URL of this request" role="group" className="flex min-w-0 flex-wrap items-center gap-x-1 text-sm text-kumo-subtle">
                <span className="shrink-0">Deployed at</span>
                <Link href={at} target="_blank" rel="noreferrer" className="min-w-0 font-mono break-all">
                  {at}
                </Link>
                <CopyButton text={at} label="Copy the URL of this request" />
              </p>
            )}
          </form>
          <Notice error={preview.error ?? invoke.error} />
          {ran ? (
            // The last response stays, dimmed, until the next one is here.
            <div className={cn("transition-opacity duration-150", running && "opacity-50")} aria-busy={running}>
              <Result ran={ran} name={worker.name} />
            </div>
          ) : (
            !preview.error && !invoke.error && <Empty title="No response" detail="Run preview sends the request to the code in the editor. The code is not saved, and no runner runs it. A preview has the variables. It does not have the secrets." />
          )}
        </div>
      </Card>
    </div>
  );
}
