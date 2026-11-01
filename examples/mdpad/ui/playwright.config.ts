import { defineConfig, devices } from "@playwright/test";

const mock = (port: number, base: string) => ({ command: `node mock/server.mjs ${port} ${base}`, url: `http://127.0.0.1:${port}${base}login`, reuseExistingServer: false });

/** Runs against the built page (dist) with the mock API: `npm run build` first. */
export default defineConfig({
  testDir: "./tests",
  fullyParallel: true,
  retries: process.env.CI ? 1 : 0,
  expect: { timeout: 10_000 },
  use: { ...devices["Desktop Chrome"], baseURL: "http://127.0.0.1:7899", viewport: { width: 1280, height: 760 }, trace: "retain-on-failure" },
  // The same build, mounted at the root and under a path.
  webServer: [mock(7899, "/"), mock(7898, "/ops/pad/")],
});
