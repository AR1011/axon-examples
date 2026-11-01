import { defineConfig, devices } from "@playwright/test";

/**
 * Runs against a real `kv console` in front of a real cluster: `npm run e2e:live`.
 *   KV_URL          the console (default http://127.0.0.1:7380, as `scripts/run.sh console` gives it)
 *   KV_LOAD_CMD     a command that sends load for about 20 s and ends (`kv check --secs 20 ..`); without it that test is skipped
 *   KV_KILL_CMD, KV_START_CMD   commands that kill -9 node n3 and start it again; without them that test is skipped
 *   KV_AXON_URL     axon's console on n1, when run.sh started it (KV_AXON_CONSOLE); without it tests-live/axon.spec.ts is skipped
 *   SHOTS=1         writes screenshots/live-*.png at desktop and phone width
 * The tests run in order, in one page: the kill test changes the cluster.
 */
export default defineConfig({
  testDir: "./tests-live",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 180_000,
  expect: { timeout: 15_000 },
  use: { ...devices["Desktop Chrome"], baseURL: process.env.KV_URL ?? "http://127.0.0.1:7380", viewport: { width: 1280, height: 760 }, deviceScaleFactor: 1, trace: "retain-on-failure" },
});
