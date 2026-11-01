import { lazy } from "react";
import { Route, Routes } from "react-router";
import { Empty } from "./components/ui";
import { OverviewPage } from "./features/overview/OverviewPage";
import { Page } from "./shell/Page";
import { Shell } from "./shell/Shell";

const ShardsPage = lazy(() => import("./features/shards/ShardsPage").then((m) => ({ default: m.ShardsPage })));
const RunsPage = lazy(() => import("./features/runs/RunsPage").then((m) => ({ default: m.RunsPage })));
const RunPage = lazy(() => import("./features/runs/RunPage").then((m) => ({ default: m.RunPage })));

export function App() {
  return (
    <Routes>
      <Route element={<Shell />}>
        <Route index element={<OverviewPage />} />
        <Route path="shards" element={<ShardsPage />} />
        <Route path="runs" element={<RunsPage />} />
        <Route path="runs/:name" element={<RunPage />} />
        <Route
          path="*"
          element={
            <Page crumbs={[{ label: "Page not found" }]} title="Page not found">
              <Empty title="Page not found" detail="Use the sidebar to go to a page." />
            </Page>
          }
        />
      </Route>
    </Routes>
  );
}
