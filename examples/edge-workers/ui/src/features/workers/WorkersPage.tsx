import { Button, Checkbox, Input, Link, Radio, Table, Tooltip } from "@cloudflare/kumo";
import { PlusIcon } from "@phosphor-icons/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { call, get, WORKERS, type Template, type WorkerUrl } from "../../api";
import { Card, Empty, Failed, Loading, Modal, Notice, NUM, Status, TONES, WorkerLink } from "../../components/ui";
import { count, micros, percent } from "../../format";
import { Page } from "../../shell/Page";
import { deploymentText, useWorkers } from "./queries";

const NAME = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;
const RULE = "Use a-z, 0-9 and -. Do not start or end with -.";

function Create({ onClose }: { onClose: () => void }) {
  const navigate = useNavigate();
  const queries = useQueryClient();
  const templates = useQuery({ queryKey: ["templates"], queryFn: ({ signal }) => get<{ templates: Template[] }>("templates", signal), staleTime: Infinity });
  const [name, setName] = useState("");
  const [template, setTemplate] = useState("hello");
  const [deploy, setDeploy] = useState(true);
  const create = useMutation({
    mutationFn: () => call<{ name: string; version: number; seq: number | null }>("POST", WORKERS, { name, template, message: `from the template ${template}`, deploy }),
    onSuccess: async (made) => {
      await queries.invalidateQueries({ queryKey: ["workers"] });
      void navigate(`/workers/${made.name}/code`);
    },
  });
  const badName = name !== "" && !NAME.test(name);
  const chosen = templates.data?.templates.find((t) => t.name === template);

  function submit(event: FormEvent) {
    event.preventDefault();
    create.mutate();
  }

  return (
    <Modal title="Create a Worker" onClose={onClose}>
      <form onSubmit={submit} className="flex flex-col gap-5">
        <Input label="Name" value={name} onChange={(e) => setName(e.target.value)} required autoFocus maxLength={64} placeholder="my-worker" className="font-mono" description={badName ? undefined : RULE} error={badName ? RULE : undefined} />
        {templates.isPending && <Loading what="the templates" />}
        {templates.error && <Failed what="the templates" error={templates.error} />}
        {templates.data && (
          <Radio.Group legend="Template" appearance="card" value={template} onValueChange={(next) => setTemplate(String(next))}>
            {templates.data.templates.map((t) => (
              <Radio.Item key={t.name} value={t.name} label={<span className="font-mono">{t.name}</span>} description={t.description} />
            ))}
          </Radio.Group>
        )}
        {chosen && Object.keys(chosen.objects).length > 0 && (
          <p className="text-sm text-kumo-subtle">
            This template has a Durable Object. The Worker gets the binding{" "}
            {Object.entries(chosen.objects)
              .map(([binding, cls]) => `${binding} = ${cls}`)
              .join(", ")}
            .
          </p>
        )}
        <Checkbox label="Deploy version 1 now" checked={deploy} onCheckedChange={(checked) => setDeploy(!!checked)} />
        <Notice error={create.error} />
        <div className="flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" loading={create.isPending} disabled={!name || badName || !templates.data}>
            Create Worker
          </Button>
        </div>
      </form>
    </Modal>
  );
}

/** A Worker's URL at the first gateway, and how many more gateways serve it. */
function Urls({ urls }: { urls: WorkerUrl[] }) {
  if (!urls.length) return <span className="text-sm text-kumo-subtle">No gateway</span>;
  return (
    <span className="flex min-w-0 items-center gap-1">
      <WorkerLink at={urls[0]} />
      {urls.length > 1 && (
        <Tooltip
          content={
            <span className="flex flex-col gap-0.5 font-mono text-xs">
              {urls.slice(1).map((at) => (
                <span key={at.gateway}>{at.url}</span>
              ))}
            </span>
          }
          render={<span className="shrink-0 cursor-help text-xs text-kumo-subtle" />}
        >
          +{urls.length - 1}
        </Tooltip>
      )}
    </span>
  );
}

export function WorkersPage() {
  const list = useWorkers();
  const navigate = useNavigate();
  const [params, setParams] = useSearchParams();
  const creating = params.has("create");
  const setCreating = (on: boolean) => setParams(on ? { create: "" } : {}, { replace: true });
  const rows = list.data?.workers ?? [];

  const createButton = (
    <Button variant="primary" icon={PlusIcon} onClick={() => setCreating(true)}>
      Create Worker
    </Button>
  );

  return (
    <Page crumbs={[{ label: "Workers" }]} title="Workers" actions={createButton}>
      {list.data && list.data.silent.length > 0 && <Notice tone="warning">These runners did not answer: {list.data.silent.join(", ")}. The numbers do not include them.</Notice>}
      <Card label="Workers">
        {list.isPending ? (
          <Loading what="the Workers" />
        ) : list.error ? (
          <Failed what="the Workers" error={list.error} />
        ) : rows.length === 0 ? (
          <Empty title="No Workers" detail="Create a Worker from a template. You can change its code after that.">
            {createButton}
          </Empty>
        ) : (
          <div className="overflow-x-auto">
            <Table className="min-w-[860px]">
              <Table.Header>
                <Table.Row>
                  <Table.Head>Name and URL</Table.Head>
                  <Table.Head>Active version</Table.Head>
                  <Table.Head className="text-right">Requests</Table.Head>
                  <Table.Head className="text-right">Errors</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">CPU p50</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">CPU p99</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap max-2xl:hidden">Wall p50</Table.Head>
                  <Table.Head className="text-right whitespace-nowrap">Wall p99</Table.Head>
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {rows.map((worker) => {
                  const m = worker.metrics;
                  return (
                    <Table.Row key={worker.name} className="cursor-pointer hover:bg-kumo-tint" onClick={() => void navigate(`/workers/${worker.name}`)}>
                      <Table.Cell className="w-[340px] max-w-[340px]">
                        <Link href={`/workers/${worker.name}`} variant="plain" onClick={(e) => e.stopPropagation()} className="text-base font-medium">
                          {worker.name}
                        </Link>
                        <Urls urls={worker.urls} />
                      </Table.Cell>
                      <Table.Cell>
                        <Status tone={!worker.deployment ? "muted" : worker.deployment.canary ? "warning" : "success"}>{deploymentText(worker.deployment)}</Status>
                      </Table.Cell>
                      <Table.Cell className={NUM}>{count(m.requests)}</Table.Cell>
                      <Table.Cell className={`${NUM} ${m.errors ? TONES.danger : ""}`}>
                        {count(m.errors)}
                        {m.requests > 0 && <span className="text-kumo-subtle"> · {percent((m.errors / m.requests) * 100)}</span>}
                      </Table.Cell>
                      <Table.Cell className={NUM}>{micros(m.cpu_p50_us)}</Table.Cell>
                      <Table.Cell className={NUM}>{micros(m.cpu_p99_us)}</Table.Cell>
                      <Table.Cell className={`${NUM} max-2xl:hidden`}>{micros(m.wall_p50_us)}</Table.Cell>
                      <Table.Cell className={NUM}>{micros(m.wall_p99_us)}</Table.Cell>
                    </Table.Row>
                  );
                })}
              </Table.Body>
            </Table>
          </div>
        )}
      </Card>
      {creating && <Create onClose={() => setCreating(false)} />}
    </Page>
  );
}
