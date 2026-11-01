import { defineConfig, devices } from "@playwright/test";

/**
 * Runs against a cluster of ../scripts/run.sh, whose api nodes serve the console they were built with: `npm run e2e:live`.
 *   WF_URL        an api node's address (default http://127.0.0.1:7701, h1 of run.sh: the test kills an engine)
 *   WF_PASSWORD   the admin password (default wf-dev, as run.sh sets it)
 *   WF_KILL_CMD   a command that kills -9 the engine that owns a run: `{node}` in it is the owner's id
 *   WF_START_CMD  a command that starts that engine again, with `{node}` as above; without both, that test is skipped
 *   SHOTS=1       writes screenshots/*.png
 * The tests start runs of the samples with ids of their own. They run in order, in one page.
 */
export default defineConfig({
  testDir: "./tests-live",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 180_000,
  expect: { timeout: 20_000 },
  use: { ...devices["Desktop Chrome"], baseURL: process.env.WF_URL ?? "http://127.0.0.1:7701", viewport: { width: 1280, height: 760 }, deviceScaleFactor: 1, trace: "retain-on-failure" },
});
