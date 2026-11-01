import { lazy } from "react";
import { Navigate, Route, Routes } from "react-router";
import { Empty } from "./components/ui";
import { LoginPage } from "./features/login/LoginPage";
import { RunsPage } from "./features/runs/RunsPage";
import { Page } from "./shell/Page";
import { Shell } from "./shell/Shell";

const OverviewPage = lazy(() => import("./features/overview/OverviewPage").then((m) => ({ default: m.OverviewPage })));
const RunPage = lazy(() => import("./features/run/RunPage").then((m) => ({ default: m.RunPage })));
const WorkflowsPage = lazy(() => import("./features/workflows/WorkflowsPage").then((m) => ({ default: m.WorkflowsPage })));
const QueuesPage = lazy(() => import("./features/queues/QueuesPage").then((m) => ({ default: m.QueuesPage })));
const ClusterPage = lazy(() => import("./features/cluster/ClusterPage").then((m) => ({ default: m.ClusterPage })));

export function App() {
  return (
    <Routes>
      <Route path="login" element={<LoginPage />} />
      <Route element={<Shell />}>
        <Route index element={<Navigate to="overview" replace />} />
        <Route path="overview" element={<OverviewPage />} />
        <Route path="runs" element={<RunsPage />} />
        {/* A run id may hold a slash: a child's is its parent's, a slash and its step. */}
        <Route path="runs/*" element={<RunPage />} />
        <Route path="workflows" element={<WorkflowsPage />} />
        <Route path="queues" element={<QueuesPage />} />
        <Route path="cluster" element={<ClusterPage />} />
        <Route
          path="*"
          element={
            <Page crumbs={[{ label: "Page not found" }]} title="Page not found">
              <Empty title="Page not found" detail="Use the navigation to go to a page." />
            </Page>
          }
        />
      </Route>
    </Routes>
  );
}
