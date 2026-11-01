import { Tabs } from "@cloudflare/kumo";
import { lazy, Suspense } from "react";
import { useNavigate, useParams } from "react-router";
import { Empty, Failed, Loading, Status, WorkerLink } from "../../components/ui";
import { Page } from "../../shell/Page";
import { Logs } from "./Logs";
import { Objects } from "./Objects";
import { Overview } from "./Overview";
import { deploymentText, useWorker } from "./queries";
import { Settings } from "./Settings";
import { Versions } from "./Versions";

// The editor is the largest part of the console: it loads when the Code tab opens.
const Code = lazy(() => import("./Code").then((m) => ({ default: m.Code })));

const TABS = ["overview", "code", "versions", "settings", "logs", "objects"] as const;
type Tab = (typeof TABS)[number];
const WIDTHS: Partial<Record<Tab, "narrow" | "full" | "fill">> = { overview: "full", code: "fill", versions: "narrow", logs: "full" };
const LABELS: Record<Tab, string> = { overview: "Overview", code: "Code", versions: "Versions", settings: "Settings", logs: "Logs", objects: "Durable Objects" };

export function WorkerPage() {
  const { name = "", tab = "overview" } = useParams();
  const navigate = useNavigate();
  const worker = useWorker(name);
  const d = worker.data?.deployment;

  return (
    <Page
      crumbs={[{ label: "Workers", href: "/workers" }, { label: name }]}
      title={<span className="font-mono">{name}</span>}
      badge={
        worker.data && (
          <span role="status" aria-label="Deployment">
            <Status tone={!d ? "muted" : d.canary ? "warning" : "success"}>{deploymentText(d ?? null)}</Status>
          </span>
        )
      }
      detail={
        worker.data && (
          <div aria-label="URL of the Worker" role="group" className="ml-2 flex min-w-0 flex-wrap items-center gap-x-4 gap-y-1">
            {worker.data.urls.length === 0 ? <span className="text-sm text-kumo-subtle">No gateway is available, so the Worker has no URL now.</span> : worker.data.urls.map((at) => <WorkerLink key={at.gateway} at={at} tag />)}
          </div>
        )
      }
      width={WIDTHS[tab as Tab]}
      tabs={<Tabs variant="segmented" value={tab} tabs={TABS.map((value) => ({ value, label: LABELS[value] }))} onValueChange={(next) => void navigate(`/workers/${name}/${next}`)} />}
    >
      {worker.isPending ? (
        <Loading what="the Worker" />
      ) : worker.error ? (
        <Failed what={`the Worker ${name}`} error={worker.error} />
      ) : !TABS.includes(tab as Tab) ? (
        <Empty title="Page not found" detail="Select a tab." />
      ) : (
        <Suspense fallback={<Loading what="the editor" />}>
          {tab === "overview" && <Overview worker={worker.data} />}
          {tab === "code" && <Code worker={worker.data} />}
          {tab === "versions" && <Versions worker={worker.data} />}
          {tab === "settings" && <Settings worker={worker.data} />}
          {tab === "logs" && <Logs name={name} />}
          {tab === "objects" && <Objects name={name} />}
        </Suspense>
      )}
    </Page>
  );
}
