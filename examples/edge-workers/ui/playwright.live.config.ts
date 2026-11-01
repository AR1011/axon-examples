import { defineConfig, devices } from "@playwright/test";

/**
 * Runs against a real control node, which serves the console it was built with: `npm run e2e:live`.
 *   EDGE_URL       the control node's admin address (default http://127.0.0.1:7703, as scripts/run.sh gives c1)
 *   EDGE_PASSWORD  the admin password (default edge-dev, as scripts/run.sh sets it)
 *   EDGE_GATEWAY   a gateway's address (default http://127.0.0.1:7701)
 *   EDGE_LOAD_CMD  a command that sends load to the Worker `hello` (`edge load ..`); without it the tests send it
 *   EDGE_KILL_CMD, EDGE_START_CMD   commands that kill -9 the runner r3 and start it again; without them that test is skipped
 *   SHOTS=1        writes screenshots/*.png
 * The tests make, change and delete the Workers `hello`, `counter` and `draft`. They run in order, in one page.
 */
export default defineConfig({
  testDir: "./tests-live",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 120_000,
  expect: { timeout: 15_000 },
  use: { ...devices["Desktop Chrome"], baseURL: process.env.EDGE_URL ?? "http://127.0.0.1:7703", viewport: { width: 1280, height: 760 }, deviceScaleFactor: 1, trace: "retain-on-failure" },
});
