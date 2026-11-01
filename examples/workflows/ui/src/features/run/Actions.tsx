import { Button, Input, InputArea } from "@cloudflare/kumo";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router";
import { call, finished, runPath, type Run } from "../../api";
import { Confirm, Modal, Notice, Off, useNotify } from "../../components/ui";
import { useOff } from "../../shell/Health";

const newSignalId = () => `ui-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

function SignalDialog({ id, onClose }: { id: string; onClose: () => void }) {
  const [name, setName] = useState("");
  const [signalId, setSignalId] = useState(newSignalId);
  const [payload, setPayload] = useState("{}");
  const notify = useNotify();
  const queries = useQueryClient();
  let parsed: unknown = null;
  let bad: string | undefined;
  try {
    parsed = payload.trim() ? JSON.parse(payload) : null;
  } catch (e) {
    bad = `The payload is not JSON: ${(e as Error).message}`;
  }
  const send = useMutation({
    mutationFn: () => call<{ duplicate: boolean }>("POST", runPath(id, `/signals/${encodeURIComponent(name)}`), { signal_id: signalId, payload: parsed }),
    onSuccess: async (said) => {
      notify(said.duplicate ? "The run has this signal id: nothing changed" : "Signal sent");
      await queries.invalidateQueries({ queryKey: ["run", id] });
      onClose();
    },
  });
  function submit(event: FormEvent) {
    event.preventDefault();
    send.mutate();
  }
  return (
    <Modal title="Send a signal" onClose={onClose} size="base">
      <form onSubmit={submit} className="flex flex-col gap-4">
        <Input label="Name" value={name} onChange={(e) => setName(e.target.value)} required autoFocus placeholder="approve" className="font-mono" />
        <Input label="Signal id" value={signalId} onChange={(e) => setSignalId(e.target.value)} required className="font-mono" description="The run takes each signal id one time only." />
        <InputArea label="Payload" value={payload} onChange={(e) => setPayload(e.target.value)} rows={4} className="font-mono" error={bad} />
        <Notice error={send.error} />
        <div className="flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" loading={send.isPending} disabled={!name || !signalId || !!bad}>
            Send signal
          </Button>
        </div>
      </form>
    </Modal>
  );
}

function TerminateDialog({ onConfirm, onClose }: { onConfirm: (reason: string) => void; onClose: () => void }) {
  const [reason, setReason] = useState("");
  return (
    <Modal title="Terminate the run?" onClose={onClose} size="base">
      <p className="text-base text-kumo-subtle">The run stops now. Its code does not run its cleanup.</p>
      <Input label="Reason" value={reason} onChange={(e) => setReason(e.target.value)} autoFocus placeholder="Stopped by hand" />
      <div className="flex justify-end gap-2">
        <Button variant="secondary" onClick={onClose}>
          Cancel
        </Button>
        <Button variant="destructive" onClick={() => onConfirm(reason.trim() || "terminated from the console")}>
          Terminate
        </Button>
      </div>
    </Modal>
  );
}

/** The changes a run takes in its status now. Each one is off, with the reason, while the journal has no quorum. */
export function Actions({ run, onError }: { run: Run; onError: (error: unknown) => void }) {
  const off = useOff();
  const notify = useNotify();
  const queries = useQueryClient();
  const navigate = useNavigate();
  const [dialog, setDialog] = useState<"signal" | "terminate" | null>(null);
  const act = useMutation({
    mutationFn: ({ path, method = "POST", body }: { path: string; method?: string; body?: unknown; done: string }) => call(method, runPath(run.id, path), body),
    onMutate: () => onError(null),
    onSuccess: async (_, { done, method }) => {
      notify(done);
      if (method === "DELETE") return void navigate("/runs");
      await queries.invalidateQueries({ queryKey: ["run", run.id] });
      await queries.invalidateQueries({ queryKey: ["runs"] });
    },
    onError,
  });
  const ended = finished(run.status);
  /** A button that is off, with the reason, while the journal has no quorum. */
  const button = (label: string, variant: "primary" | "secondary" | "secondary-destructive", onClick: () => void, loading = false) => {
    const shown = (
      <Button variant={variant} size="sm" disabled={!!off} loading={loading} onClick={onClick}>
        {label}
      </Button>
    );
    return off ? <Off reason={off}>{shown}</Off> : shown;
  };
  const plain = (label: string, path: string, done: string) => button(label, "secondary", () => act.mutate({ path, done }), act.isPending && act.variables?.path === path);
  return (
    <div className="flex flex-wrap items-center gap-2">
      {!ended && button("Signal", "primary", () => setDialog("signal"))}
      {run.status === "running" && plain("Pause", "/pause", "Run paused")}
      {(run.status === "paused" || run.status === "parked") && plain("Resume", "/resume", "Run resumed")}
      {run.status === "failed" && plain("Retry", "/retry", "Run retried from the failed step")}
      {!ended && <Confirm label="Cancel" question="Cancel the run?" detail="The run stops at its next step and runs its cleanup. Its children get the cancel too." yes="Cancel run" off={off} onConfirm={() => act.mutate({ path: "/cancel", done: "Cancel sent" })} />}
      {!ended && button("Terminate", "secondary-destructive", () => setDialog("terminate"))}
      {ended && <Confirm label="Delete" question="Delete the run?" detail="The run and its journal go from the history nodes. You cannot undo this." yes="Delete run" off={off} onConfirm={() => act.mutate({ path: "", method: "DELETE", done: "Run deleted" })} />}
      {dialog === "signal" && <SignalDialog id={run.id} onClose={() => setDialog(null)} />}
      {dialog === "terminate" && (
        <TerminateDialog
          onClose={() => setDialog(null)}
          onConfirm={(reason) => {
            setDialog(null);
            act.mutate({ path: "/terminate", body: { reason }, done: "Run terminated" });
          }}
        />
      )}
    </div>
  );
}
