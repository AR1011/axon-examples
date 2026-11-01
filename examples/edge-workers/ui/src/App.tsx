import { lazy } from "react";
import { Navigate, Route, Routes } from "react-router";
import { LoginPage } from "./features/login/LoginPage";
import { WorkersPage } from "./features/workers/WorkersPage";
import { Page } from "./shell/Page";
import { Shell } from "./shell/Shell";
import { Empty } from "./components/ui";

const WorkerPage = lazy(() => import("./features/workers/WorkerPage").then((m) => ({ default: m.WorkerPage })));
const ClusterPage = lazy(() => import("./features/cluster/ClusterPage").then((m) => ({ default: m.ClusterPage })));

export function App() {
  return (
    <Routes>
      <Route path="login" element={<LoginPage />} />
      <Route element={<Shell />}>
        <Route index element={<Navigate to="workers" replace />} />
        <Route path="workers" element={<WorkersPage />} />
        <Route path="workers/:name" element={<WorkerPage />} />
        <Route path="workers/:name/:tab" element={<WorkerPage />} />
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
