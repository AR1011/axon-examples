import { execSync } from "node:child_process";
import { expect, test, type Page } from "@playwright/test";
import { shots } from "./shots";

const { KV_LOAD_CMD, KV_KILL_CMD, KV_START_CMD } = process.env;

const row = (page: Page, node: string) => page.getByRole("region", { name: "Nodes" }).getByRole("row").filter({ has: page.getByRole("button", { name: `Show the fields of ${node}` }) });
const stat = (page: Page, name: string) => page.getByRole("group", { name, exact: true });

/** Opens a page with the pointer off the sidebar, which opens over the page while the pointer is on it. */
async function open(page: Page, path: string) {
  await page.goto(path);
  await page.mouse.move(page.viewportSize()!.width - 20, 400);
}

test.describe.configure({ mode: "serial" });

test("three nodes answer and every shard runs on one of them", async ({ page }) => {
  await open(page, "/");
  for (const node of ["n1", "n2", "n3"]) await expect(row(page, node).getByText("Answers")).toBeVisible();
  await expect(stat(page, "Shards")).toContainText("256of 256");
  await open(page, "/shards");
  await expect(page.getByRole("grid", { name: "Shards by number" }).getByRole("button", { name: / on n\d$/ })).toHaveCount(256);
  await shots(page, "shards");
});

test("load shows as operations on the nodes", async ({ page }) => {
  test.skip(!KV_LOAD_CMD, "KV_LOAD_CMD is not set");
  await open(page, "/");
  // This blocks the test while the load runs: the page and the console keep asking meanwhile.
  execSync(KV_LOAD_CMD!, { stdio: "inherit" });
  await page.reload();
  const total = page.getByRole("region", { name: "Operations per second" }).getByRole("img", { name: "Reads, writes and failed operations per second" });
  await expect(total.locator("canvas")).toBeVisible();
  const writes = await page.request.get("/api/ops?minutes=5").then((r) => r.json());
  const most = Math.max(...writes.nodes.flatMap((n: { points: (number | null)[][] }) => n.points.map((p) => p[2] ?? 0)));
  expect(most).toBeGreaterThan(100);
  await shots(page, "overview");
});

test("a node killed shows as not answering, its shards move, and it comes back", async ({ page }) => {
  test.skip(!KV_KILL_CMD || !KV_START_CMD, "KV_KILL_CMD and KV_START_CMD are not set");
  await open(page, "/");
  await expect(stat(page, "Shards")).toContainText("256of 256");
  execSync(KV_KILL_CMD!, { stdio: "inherit" });
  await expect(row(page, "n3").getByText("No answer")).toBeVisible();
  await expect(page.getByRole("status").filter({ hasText: "does not answer: n3" })).toBeVisible();
  // The leases of n3 end, and its shards start on n1 and n2.
  await expect(stat(page, "Shards")).toContainText("256of 256", { timeout: 60_000 });
  await open(page, "/shards");
  const map = page.getByRole("grid", { name: "Shards by number" });
  await expect(map.getByRole("button", { name: / on n[12]$/ })).toHaveCount(256);
  await expect(map.getByRole("button", { name: / on n3$/ })).toHaveCount(0);
  await shots(page, "shards-down");
  execSync(KV_START_CMD!, { stdio: "inherit" });
  await open(page, "/");
  await expect(row(page, "n3").getByText("Answers")).toBeVisible({ timeout: 60_000 });
  await expect(stat(page, "Nodes")).toContainText("3of 3");
});

test("the committed bench runs are listed, and a run shows its results", async ({ page }) => {
  await open(page, "/runs");
  const runs = page.getByRole("region", { name: "Runs" });
  await runs.getByRole("link", { name: "2026-10-11-epyc-4484px-full" }).click();
  await expect(page.getByRole("region", { name: "Saturation" }).locator("canvas")).toBeVisible();
  await expect(page.getByRole("region", { name: "Ceilings" }).getByRole("table")).toBeVisible();
  await shots(page, "run");
  // A run of fault phases only: no summary, its faults as a table.
  await open(page, "/runs/2026-10-11-epyc-4484px-review");
  await expect(page.getByRole("region", { name: "Faults" }).getByRole("row")).not.toHaveCount(0);
});
