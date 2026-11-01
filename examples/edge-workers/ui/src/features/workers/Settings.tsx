import { Badge, Button, Input, Table } from "@cloudflare/kumo";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useRef, useState, type FormEvent } from "react";
import { useNavigate } from "react-router";
import { call, get, seg, workerPath, type Variable, type WorkerDetail } from "../../api";
import { Card, CLIP, Confirm, Empty, Failed, Loading, MONO, Notice, Segmented, useNotify } from "../../components/ui";
import { activeVersion, useRefresh, useVersion } from "./queries";

type Made = { version: number; seq: number | null };

function Variables({ worker }: { worker: WorkerDetail }) {
  const refresh = useRefresh(worker.name);
  const notify = useNotify();
  const path = workerPath(worker.name, "/vars");
  const vars = useQuery({ queryKey: ["vars", worker.name], queryFn: ({ signal }) => get<{ version: number; vars: Variable[] }>(path, signal) });
  const [form, setForm] = useState<{ name: string; type: "plain" | "secret"; value: string }>({ name: "", type: "plain", value: "" });
  const value = useRef<HTMLInputElement>(null);
  const set = useMutation({
    mutationFn: () => call<Made>("PUT", `${path}/${seg(form.name)}`, { type: form.type, value: form.value }),
    onSuccess: async (made) => {
      await refresh();
      notify(`${form.type === "secret" ? "The secret" : "The variable"} ${form.name} is set. Version ${made.version} is deployed.`);
      // A secret's value leaves the page at once.
      setForm({ name: "", type: form.type, value: "" });
    },
  });
  const remove = useMutation({
    mutationFn: (name: string) => call<Made>("DELETE", `${path}/${seg(name)}`),
    onSuccess: async (made, name) => {
      await refresh();
      notify(`${name} is removed. Version ${made.version} is deployed.`);
    },
  });
  const act = (run: () => void) => {
    set.reset();
    remove.reset();
    run();
  };
  function edit(row: Variable) {
    setForm({ name: row.name, type: row.type, value: row.type === "plain" ? (row.value ?? "") : "" });
    value.current?.focus();
  }
  function submit(event: FormEvent) {
    event.preventDefault();
    act(() => set.mutate());
  }
  const busy = set.isPending || remove.isPending;

  return (
    <Card title="Variables and secrets" label="Variables and secrets">
      {vars.isPending ? (
        <Loading what="the variables" />
      ) : vars.error ? (
        <Failed what="the variables" error={vars.error} />
      ) : vars.data.vars.length === 0 ? (
        <Empty title="No variables" detail="The code reads a variable or a secret from env, for example env.GREETING." />
      ) : (
        <div className="overflow-x-auto">
          <Table className="min-w-[720px]">
            <Table.Header>
              <Table.Row>
                <Table.Head className="w-[28%]">Name</Table.Head>
                <Table.Head>Type</Table.Head>
                <Table.Head className="w-[40%]">Value</Table.Head>
                <Table.Head>
                  <span className="sr-only">Actions</span>
                </Table.Head>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {vars.data.vars.map((row) => (
                <Table.Row key={row.name}>
                  <Table.Cell className={MONO}>{row.name}</Table.Cell>
                  <Table.Cell>
                    <Badge variant={row.type === "secret" ? "orange" : "outline"}>{row.type === "secret" ? "Secret" : "Plain"}</Badge>
                  </Table.Cell>
                  <Table.Cell className={`${MONO} ${CLIP}`} title={row.type === "plain" ? row.value : undefined}>
                    {row.type === "secret" ? <span className="font-sans text-kumo-subtle">Hidden</span> : row.value}
                  </Table.Cell>
                  <Table.Cell>
                    <span className="flex items-center justify-end gap-2">
                      <Button variant="secondary" size="sm" onClick={() => edit(row)} aria-label={`${row.type === "secret" ? "Replace" : "Edit"} ${row.name}`}>
                        {row.type === "secret" ? "Replace" : "Edit"}
                      </Button>
                      <Confirm label="Remove" question={`Remove ${row.name}?`} detail="A new version without it is deployed." yes="Remove" busy={busy} onConfirm={() => act(() => remove.mutate(row.name))} />
                    </span>
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table>
        </div>
      )}
      <form onSubmit={submit} className="flex flex-wrap items-center gap-2.5 border-t border-kumo-line px-4 py-3">
        <Input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required aria-label="Name" placeholder="NAME" className="w-[200px] font-mono" />
        <Segmented label="Type" value={form.type} options={[{ value: "plain", label: "Plain" }, { value: "secret", label: "Secret" }]} onChange={(type) => setForm({ ...form, type })} />
        <Input ref={value} value={form.value} onChange={(e) => setForm({ ...form, value: e.target.value })} type={form.type === "secret" ? "password" : "text"} autoComplete="off" aria-label="Value" placeholder={form.type === "secret" ? "Secret value" : "Value"} className="min-w-[180px] flex-1 font-mono" />
        <Button type="submit" variant="primary" disabled={busy} loading={set.isPending}>
          Save and deploy
        </Button>
      </form>
      {(set.error || remove.error) && (
        <div className="border-t border-kumo-line p-3">
          <Notice error={set.error ?? remove.error} />
        </div>
      )}
    </Card>
  );
}

function LimitsForm({ worker }: { worker: WorkerDetail }) {
  const refresh = useRefresh(worker.name);
  const id = activeVersion(worker);
  const limits = worker.versions.find((v) => v.id === id)?.limits;
  const detail = useVersion(worker.name, id);
  const [typed, setTyped] = useState<{ cpu_ms?: string; memory_mb?: string }>({});
  const cpu = typed.cpu_ms ?? String(limits?.cpu_ms ?? "");
  const memory = typed.memory_mb ?? String(limits?.memory_mb ?? "");
  const save = useMutation({
    mutationFn: () => call<Made>("POST", workerPath(worker.name, "/versions"), { cpu_ms: Number(cpu), memory_mb: Number(memory), message: `limits: ${cpu} ms CPU, ${memory} MB` }),
    onSuccess: async () => {
      await refresh();
      setTyped({});
    },
  });
  const bindings = Object.entries(detail.data?.objects ?? {});
  return (
    <Card title="Limits and bindings" label="Limits and bindings">
      <form
        onSubmit={(e) => {
          e.preventDefault();
          save.mutate();
        }}
        className="flex flex-wrap items-end gap-4 px-4 py-4"
      >
        <Input label="CPU time for a request, ms" type="number" min={1} required value={cpu} onChange={(e) => setTyped({ ...typed, cpu_ms: e.target.value })} className="w-[180px] font-mono" />
        <Input label="Memory, MB" type="number" min={1} required value={memory} onChange={(e) => setTyped({ ...typed, memory_mb: e.target.value })} className="w-[180px] font-mono" />
        <Button type="submit" variant="secondary" disabled={save.isPending || (typed.cpu_ms == null && typed.memory_mb == null)}>
          Save limits and deploy
        </Button>
        <span className="ml-auto self-center font-mono text-sm text-kumo-subtle">
          wall time {limits ? limits.wall_ms / 1000 : "—"} s · {limits?.subrequests ?? "—"} subrequests
        </span>
      </form>
      {save.error && (
        <div className="border-t border-kumo-line p-3">
          <Notice error={save.error} />
        </div>
      )}
      <div className="flex flex-wrap items-center gap-2 border-t border-kumo-line px-4 py-3">
        <span className="text-base font-medium">Durable Object bindings</span>
        {detail.error ? (
          <Notice error={detail.error} />
        ) : bindings.length === 0 ? (
          <span className="text-kumo-subtle">{detail.isPending ? "Loading…" : "None"}</span>
        ) : (
          bindings.map(([binding, cls]) => (
            <Badge key={binding} variant="outline" className="font-mono">
              env.{binding} = {cls}
            </Badge>
          ))
        )}
      </div>
    </Card>
  );
}

export function Settings({ worker }: { worker: WorkerDetail }) {
  const navigate = useNavigate();
  const refresh = useRefresh(worker.name);
  const remove = useMutation({
    mutationFn: () => call<{ deleted: string }>("DELETE", workerPath(worker.name)),
    onSuccess: async () => {
      void navigate("/workers");
      await refresh();
    },
  });
  return (
    <div className="flex flex-col gap-4">
      <Variables worker={worker} />
      <LimitsForm worker={worker} />
      <Card title="Delete the Worker" label="Delete the Worker">
        <div className="flex flex-wrap items-center gap-3 px-4 py-4">
          <span className="text-kumo-subtle">The Worker, its versions and its deployment go. The runners stop it.</span>
          <span className="ml-auto">
            <Confirm label="Delete Worker" size="base" question={`Delete ${worker.name} and all its versions?`} detail="You cannot get them back." yes="Delete Worker" busy={remove.isPending} onConfirm={() => remove.mutate()} />
          </span>
        </div>
        {remove.error && (
          <div className="border-t border-kumo-line p-3">
            <Notice error={remove.error} />
          </div>
        )}
      </Card>
    </div>
  );
}
