import { defineConfig, devices } from "@playwright/test";

/**
 * The editor against a real cluster, `scripts/run.sh up` (release build): `npm run e2e:live`.
 *   MDPAD_URL        a web node that is no home (default http://127.0.0.1:7304, w1 of scripts/run.sh)
 *   MDPAD_KILL_CMD   a command that kill -9s the home `{home}` (h2 or h3), and MDPAD_START_CMD one that starts it
 *                    again, as `scripts/run.sh kill {home}` and `start {home}`; without them the home kill is skipped
 *   MDPAD_INVITE_CMD a command that prints the invite of the new user `{name}`, as `scripts/run.sh invite {name}`;
 *                    without it the journeys that need one are skipped
 *   SHOTS=1         writes screenshots/*.png at desktop and phone width
 * The users ann and ben (password <name>-pw) are the ones `run.sh up` makes. Each test makes its own documents.
 */
export default defineConfig({
  testDir: "./tests-live",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 120_000,
  expect: { timeout: 15_000 },
  use: { ...devices["Desktop Chrome"], baseURL: process.env.MDPAD_URL ?? "http://127.0.0.1:7304", viewport: { width: 1280, height: 760 }, deviceScaleFactor: 1, trace: "retain-on-failure" },
});
