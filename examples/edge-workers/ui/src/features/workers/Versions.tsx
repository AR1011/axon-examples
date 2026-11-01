import { Button, Checkbox, Input, LinkButton, Table } from "@cloudflare/kumo";
import { useMutation } from "@tanstack/react-query";
import { lazy, Suspense, useState, type FormEvent } from "react";
import { call, workerPath, type WorkerDetail } from "../../api";
import { Card, CLIP, Confirm, Empty, Failed, Field, Loading, Modal, MONO, Notice, NUM, Pick, Stamp, Status, useNotify } from "../../components/ui";
import { bytes } from "../../format";
import { deploymentText, useRefresh, useVersion } from "./queries";

const Editor = lazy(() => import("./Editor"));

type Deploy = { version: number; canary?: { version: number; percent: number } };

function Source({ worker, id, onClose }: { worker: WorkerDetail; id: number; onClose: () => void }) {
  const stable = worker.deployment?.stable;
  const version = useVersion(worker.name, id);
  const active = useVersion(worker.name, stable);
  const comparable = stable != null && stable !== id;
  const [compare, setCompare] = useState(false);
  const original = compare && comparable ? active.data?.source : undefined;
  return (
    <Modal title={`Version ${id}`} onClose={onClose} size="xl">
      {version.isPending ? (
        <Loading what={`version ${id}`} />
      ) : version.error ? (
        <Failed what={`version ${id}`} error={version.error} />
      ) : (
        <>
          <dl>
            <Field label="Message">{version.data.message || "—"}</Field>
            <Field label="Size">{bytes(version.data.source_bytes)}</Field>
            <Field label="Build">{bytes(version.data.build_bytes)}</Field>
            <Field label="Variables">{Object.keys(version.data.vars).join(", ") || "—"}</Field>
            <Field label="Secrets">{version.data.secrets.join(", ") || "—"}</Field>
            <Field label="Object bindings">
              {Object.entries(version.data.objects)
                .map(([binding, cls]) => `${binding} = ${cls}`)
                .join(", ") || "—"}
            </Field>
            <Field label="Limits">{`${version.data.limits.cpu_ms} ms CPU · ${version.data.limits.memory_mb} MB`}</Field>
          </dl>
          <div className="flex flex-wrap items-center gap-3">
            {comparable && <Checkbox label={`Show the changes from the active version (v${stable})`} checked={compare} onCheckedChange={(checked) => setCompare(!!checked)} />}
            <LinkButton href={`/workers/${worker.name}/code?version=${id}`} variant="secondary" size="sm" className="ml-auto">
              Open in the editor
            </LinkButton>
          </div>
          {compare && active.error ? (
            <Notice error={active.error} />
          ) : (
            <div className="h-[360px] shrink-0 overflow-hidden rounded-lg ring ring-kumo-line">
              <Suspense fallback={<Loading what="the editor" />}>
                <Editor key={`${id}:${original == null ? "plain" : "diff"}`} initial={version.data.source} label={`Source of version ${id}`} readOnly original={original} />
              </Suspense>
            </div>
          )}
        </>
      )}
    </Modal>
  );
}

export function Versions({ worker }: { worker: WorkerDetail }) {
  const refresh = useRefresh(worker.name);
  const notify = useNotify();
  const d = worker.deployment;
  const [viewing, setViewing] = useState<number | null>(null);
  const others = worker.versions.filter((v) => v.id !== d?.stable);
  const [canary, setCanary] = useState({ version: "", percent: "10" });
  const canaryVersion = Number(canary.version) || d?.canary?.version || others[0]?.id;

  const deploy = useMutation({
    mutationFn: (to: Deploy) => call<{ seq: number; version: number }>("POST", workerPath(worker.name, "/deploy"), to),
    onSuccess: async (made, to) => {
      await refresh();
      notify(to.canary ? `Version ${to.canary.version} is the canary. It takes ${to.canary.percent}% of the traffic.` : `Version ${made.version} is deployed. It takes all the traffic.`);
    },
  });
  const rollback = useMutation({
    mutationFn: () => call<{ seq: number; version: number; from: number }>("POST", workerPath(worker.name, "/rollback"), {}),
    onSuccess: async (made) => {
      await refresh();
      notify(`Rolled back from version ${made.from} to version ${made.version}.`);
    },
  });
  const busy = deploy.isPending || rollback.isPending;
  const act = (run: () => void) => {
    deploy.reset();
    rollback.reset();
    run();
  };

  function startCanary(event: FormEvent) {
    event.preventDefault();
    if (d && canaryVersion != null) act(() => deploy.mutate({ version: d.stable, canary: { version: canaryVersion, percent: Number(canary.percent) } }));
  }

  return (
    <div className="flex flex-col gap-4">
      <Card
        title="Deployment"
        label="Deployment"
        controls={
          d && (
            <>
              {d.canary && (
                <>
                  <Button variant="secondary" size="sm" disabled={busy} onClick={() => act(() => deploy.mutate({ version: d.canary!.version }))}>
                    Promote canary v{d.canary.version}
                  </Button>
                  <Button variant="secondary" size="sm" disabled={busy} onClick={() => act(() => deploy.mutate({ version: d.stable }))}>
                    Stop canary
                  </Button>
                </>
              )}
              <Confirm plain label="Roll back" question={`Roll back from v${d.stable} to the version before it?`} yes="Roll back" busy={busy} onConfirm={() => act(() => rollback.mutate())} />
            </>
          )
        }
      >
        <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 px-4 py-3.5">
          <span className="text-base font-medium">{d ? deploymentText(d) : "Not deployed. No runner runs this Worker."}</span>
          {d && <span className="font-mono text-sm text-kumo-subtle">deployment {d.seq}</span>}
        </div>
        {d && (
          <form onSubmit={startCanary} className="flex flex-wrap items-center gap-2.5 border-t border-kumo-line px-4 py-3 text-base">
            <span className="font-medium">Canary</span>
            {others.length === 0 ? (
              <span className="text-kumo-subtle">A canary needs a second version. Save one in the Code tab.</span>
            ) : (
              <>
                <Pick label="Canary version" value={String(canaryVersion)} onChange={(version) => setCanary({ ...canary, version })} options={others.map((v) => ({ value: String(v.id), label: `v${v.id}` }))} className="w-[90px] font-mono" />
                <span className="text-kumo-subtle">takes</span>
                <Input type="number" min={1} max={99} required value={canary.percent} onChange={(e) => setCanary({ ...canary, percent: e.target.value })} aria-label="Canary percent" className="w-[76px] font-mono" />
                <span className="text-kumo-subtle">% of the traffic. v{d.stable} takes the remainder.</span>
                <Button type="submit" variant="secondary" disabled={busy}>
                  {d.canary ? "Change canary" : "Start canary"}
                </Button>
              </>
            )}
          </form>
        )}
        {(deploy.error || rollback.error) && (
          <div className="border-t border-kumo-line p-3">
            <Notice error={deploy.error ?? rollback.error} />
          </div>
        )}
      </Card>
      <Card title="Versions" label="Versions">
        {worker.versions.length === 0 ? (
          <Empty title="No versions" />
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[920px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Version</Table.Head>
                  <Table.Head>Created</Table.Head>
                  <Table.Head className="w-[34%]">Message</Table.Head>
                  <Table.Head className="text-right">Size</Table.Head>
                  <Table.Head>Bundle</Table.Head>
                  <Table.Head>State</Table.Head>
                  <Table.Head>
                    <span className="sr-only">Actions</span>
                  </Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {worker.versions.map((v) => {
                  const active = d?.stable === v.id;
                  const isCanary = d?.canary?.version === v.id;
                  return (
                    <Table.Row key={v.id} variant={viewing === v.id ? "selected" : "default"}>
                      <Table.Cell className={MONO}>v{v.id}</Table.Cell>
                      <Table.Cell>
                        <Stamp ms={v.created_ms} />
                      </Table.Cell>
                      <Table.Cell className={CLIP} title={v.message}>
                        {v.message || <span className="text-kumo-subtle">—</span>}
                      </Table.Cell>
                      <Table.Cell className={NUM}>{bytes(v.source_bytes)}</Table.Cell>
                      <Table.Cell className={`${MONO} text-kumo-subtle`} title={v.bundle}>
                        {v.bundle.slice(0, 12)}
                      </Table.Cell>
                      <Table.Cell>{active ? <Status tone="success">Active</Status> : isCanary ? <Status tone="warning">Canary {d?.canary?.percent}%</Status> : <span className="text-kumo-subtle">—</span>}</Table.Cell>
                      <Table.Cell>
                        <span className="flex items-center justify-end gap-2">
                          <Button variant="secondary" size="sm" onClick={() => setViewing(v.id)}>
                            View v{v.id}
                          </Button>
                          {!(active && !d?.canary) && <Confirm plain label={`Deploy v${v.id}`} question={`Deploy v${v.id}?`} detail="It takes all the traffic." yes={`Deploy v${v.id}`} busy={busy} onConfirm={() => act(() => deploy.mutate({ version: v.id }))} />}
                        </span>
                      </Table.Cell>
                    </Table.Row>
                  );
                })}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
      {viewing != null && <Source worker={worker} id={viewing} onClose={() => setViewing(null)} />}
    </div>
  );
}
