import { Button, Input, InputArea } from "@cloudflare/kumo";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router";
import { api, call } from "../../api";
import { CopyButton, Modal, Notice, Pick, PRE, useNotify } from "../../components/ui";
import { useWorkflows } from "../../queries";

/** A shell word in single quotes. */
const quoted = (text: string) => `'${text.replaceAll("'", `'\\''`)}'`;

export function curl(method: string, path: string, body?: unknown) {
  const url = new URL(api + path, location.origin).href;
  const parts = [`curl -X ${method} ${quoted(url)}`, `-H "authorization: Bearer $WF_TOKEN"`];
  if (body !== undefined) parts.push(`-H "content-type: application/json"`, `-d ${quoted(JSON.stringify(body))}`);
  return parts.join(" \\\n  ");
}

function parse(text: string): { value?: unknown; error?: string } {
  if (!text.trim()) return { value: null };
  try {
    return { value: JSON.parse(text) as unknown };
  } catch (e) {
    return { error: `The input is not JSON: ${(e as Error).message}` };
  }
}

/** Starts a run: a workflow, a version and an id if you want them, and the input in JSON. */
export function StartDialog({ workflow: first, onClose }: { workflow?: string; onClose: () => void }) {
  const workflows = useWorkflows();
  const names = (workflows.data?.workflows ?? []).filter((w) => w.versions.length).map((w) => w.name);
  const [workflow, setWorkflow] = useState(first ?? "");
  const [version, setVersion] = useState("");
  const [id, setId] = useState("");
  const [input, setInput] = useState("{}");
  const navigate = useNavigate();
  const queries = useQueryClient();
  const notify = useNotify();
  const chosen = workflow || names[0] || "";
  const parsed = parse(input);
  const body = { workflow: chosen, ...(version ? { version: Number(version) } : {}), ...(id ? { id } : {}), input: parsed.value ?? null };
  const start = useMutation({
    mutationFn: () => call<{ id: string; created: boolean }>("POST", "runs", body),
    onSuccess: async (made) => {
      notify(made.created ? "Run started" : "This run exists with the same input");
      await queries.invalidateQueries({ queryKey: ["runs"] });
      void navigate(`/runs/${made.id}`);
    },
  });
  const versions = workflows.data?.workflows.find((w) => w.name === chosen)?.versions ?? [];

  function submit(event: FormEvent) {
    event.preventDefault();
    start.mutate();
  }

  return (
    <Modal title="Start a run" onClose={onClose}>
      <form onSubmit={submit} className="flex flex-col gap-4">
        <div className="grid grid-cols-[minmax(0,1fr)_120px] gap-3 max-sm:grid-cols-1">
          <div className="flex min-w-0 flex-col gap-1.5">
            <span className="text-base">Workflow</span>
            <Pick label="Workflow" value={chosen} options={names.map((n) => ({ value: n, label: n }))} onChange={setWorkflow} className="w-full" />
          </div>
          <div className="flex min-w-0 flex-col gap-1.5">
            <span className="text-base">Version</span>
            <Input aria-label="Version" value={version} onChange={(e) => setVersion(e.target.value.replace(/\D/g, ""))} placeholder={versions.length ? `${Math.max(...versions)}` : "latest"} inputMode="numeric" className="w-full font-mono" />
          </div>
        </div>
        <Input label="Id" value={id} onChange={(e) => setId(e.target.value)} placeholder="A new id" maxLength={128} className="font-mono" description="Empty: the engine makes one. The same id and input start one run only." />
        <InputArea label="Input" value={input} onChange={(e) => setInput(e.target.value)} rows={6} className="font-mono" error={parsed.error} />
        <Notice error={start.error} />
        <section aria-label="The same with curl" className="flex flex-col gap-1.5">
          <div className="flex items-center gap-2">
            <h3 className="text-sm font-medium text-kumo-subtle">The same with curl</h3>
            <CopyButton text={curl("POST", "runs", body)} label="Copy the curl command" />
          </div>
          <pre className={PRE}>{curl("POST", "runs", body)}</pre>
        </section>
        <div className="flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" loading={start.isPending} disabled={!chosen || !!parsed.error}>
            Start run
          </Button>
        </div>
      </form>
    </Modal>
  );
}
