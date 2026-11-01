import { defineConfig, devices } from "@playwright/test";

/** Runs against the built page (dist) with the mock API, at desktop and at phone width: `npm run build` first. */
export default defineConfig({
  testDir: "./tests",
  fullyParallel: true,
  retries: process.env.CI ? 1 : 0,
  use: { baseURL: "http://127.0.0.1:7389", trace: "retain-on-failure" },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"], viewport: { width: 1280, height: 720 } } },
    { name: "phone", use: { ...devices["Pixel 7"], viewport: { width: 375, height: 812 } } },
  ],
  webServer: { command: "node mock/server.mjs 7389", url: "http://127.0.0.1:7389/", reuseExistingServer: false },
});
