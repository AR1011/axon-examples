import { expect, test, type Page, type Route } from "@playwright/test";

const RUN = "2026-10-11-epyc-4484px-full";

/** Asks the mock (mock/api.mjs) to stop or start a node of this browser's cluster. */
async function mock(page: Page, what: "down" | "up", node: string) {
  expect((await page.request.get(`/api/_mock/${what}?node=${node}`)).ok()).toBe(true);
}

/**
 * Opens a page with the pointer off the sidebar. Headless Chromium puts the pointer at 0,0, over the sidebar, which
 * then opens over the left of the page.
 */
async function open(page: Page, path: string) {
  await page.goto(path);
  await page.mouse.move(page.viewportSize()!.width - 20, 400);
}

const nodes = (page: Page) => page.getByRole("region", { name: "Nodes" });
const row = (page: Page, node: string) => nodes(page).getByRole("row").filter({ has: page.getByRole("button", { name: `Show the fields of ${node}` }) });
const stat = (page: Page, name: string) => page.getByRole("group", { name, exact: true });

/** Neither the document nor the scrolling part of the page is wider than the window. */
async function noSidewaysScroll(page: Page) {
  const wide = await page.evaluate(() => {
    const main = document.querySelector("main")!;
    return { doc: document.documentElement.scrollWidth - document.documentElement.clientWidth, main: main.scrollWidth - main.clientWidth };
  });
  expect(wide).toEqual({ doc: 0, main: 0 });
}

/** From the page's load on, adds up its layout shifts that no input caused; `shift` reads the sum. */
async function watchShift(page: Page) {
  await page.addInitScript(() => {
    const w = window as unknown as { shift: number };
    w.shift = 0;
    new PerformanceObserver((list) => {
      for (const e of list.getEntries() as (PerformanceEntry & { value: number; hadRecentInput: boolean })[]) if (!e.hadRecentInput) w.shift += e.value;
    }).observe({ type: "layout-shift", buffered: true });
  });
}

const shift = (page: Page) => page.evaluate(() => (window as unknown as { shift: number }).shift);

async function shot(page: Page, name: string) {
  if (!process.env.SHOTS) return;
  await page.waitForTimeout(1200); // the charts draw their lines in about a second
  await page.screenshot({ path: `screenshots/${test.info().project.name}-${name}.png` });
}

test("the overview shows each node, its shards and its load", async ({ page }) => {
  await open(page, "/");
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  for (const node of ["n1", "n2", "n3"]) await expect(row(page, node).getByText("Answers")).toBeVisible();
  await expect(stat(page, "Nodes")).toContainText("3of 3");
  await expect(stat(page, "Shards")).toContainText("256of 256");
  await expect(stat(page, "Operations")).toContainText("/s");
  // The shards of the three nodes add up to every shard once.
  const counts = await Promise.all(["n1", "n2", "n3"].map(async (n) => Number(await row(page, n).getByRole("cell").nth(3).innerText())));
  expect(counts.reduce((a, b) => a + b)).toBe(256);
  const ops = page.getByRole("region", { name: "Operations per second" });
  await expect(ops.getByRole("img", { name: "Operations per second of each node" }).locator("canvas")).toBeVisible();
  await expect(ops.getByRole("img", { name: "Reads, writes and failed operations per second" }).locator("canvas")).toBeVisible();
  await ops.getByRole("tab", { name: "1 hour" }).click();
  await expect(ops.getByRole("tab", { name: "1 hour" })).toHaveAttribute("aria-selected", "true");
  await expect(ops.locator("canvas").first()).toBeVisible();
  await noSidewaysScroll(page);
  await shot(page, "overview");
});

test("a node that stops shows as not answering, and its shards move to the others", async ({ page }) => {
  await open(page, "/");
  await expect(stat(page, "Shards")).toContainText("256of 256");
  await mock(page, "down", "n3");
  await expect(page.getByRole("status").filter({ hasText: "1 node does not answer: n3." })).toBeVisible();
  await expect(row(page, "n3").getByText("No answer")).toBeVisible();
  await expect(stat(page, "Nodes")).toContainText("2of 3");
  // Its shards are on no node while they move, then on n1 and n2.
  await expect(stat(page, "Shards")).not.toContainText("256of 256");
  await shot(page, "overview-down");
  await expect(stat(page, "Shards")).toContainText("256of 256", { timeout: 10_000 });
  await expect(row(page, "n3").getByRole("cell").nth(3)).toHaveText("—");
  await mock(page, "up", "n3");
  await expect(row(page, "n3").getByText("Answers")).toBeVisible();
  await expect(page.getByRole("status").filter({ hasText: "does not answer" })).toHaveCount(0);
});

test("a node opens in a sheet with all of its fields", async ({ page }) => {
  await open(page, "/");
  await page.getByRole("button", { name: "Show the fields of n2" }).click();
  const sheet = page.getByRole("dialog", { name: "n2" });
  await expect(sheet).toBeVisible();
  await expect(sheet.getByRole("definition").filter({ hasText: "n1,n2,n3" })).toBeVisible();
  await expect(sheet.getByText("127.0.0.1:6380")).toBeVisible();
  await shot(page, "node");
  await page.keyboard.press("Escape");
  await expect(sheet).toHaveCount(0);
});

test("the shard map gives the node of each shard, by node, and in a list", async ({ page }) => {
  await open(page, "/shards");
  const map = page.getByRole("grid", { name: "Shards by number" });
  await expect(map.getByRole("button")).toHaveCount(256);
  await expect(stat(page, "Placed")).toContainText("256of 256");
  const cell = map.getByRole("button", { name: /^Shard 5 on n\d$/ });
  const node = (await cell.getAttribute("aria-label"))!.split(" on ")[1];
  await cell.click();
  await expect(cell).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByText(`Shard 5 · slots 320 to 383 · on ${node}`)).toBeVisible();
  await noSidewaysScroll(page);
  await shot(page, "shards");

  // Only one node's shards: the list then has as many rows as the node has shards.
  await page.getByRole("combobox", { name: "Node" }).click();
  await page.getByRole("option", { name: "n2", exact: true }).click();
  const n2 = await map.getByRole("button", { name: / on n2$/ }).count();
  expect(n2).toBeGreaterThan(40);
  await page.getByRole("region", { name: "Shard map" }).getByRole("tab", { name: "List" }).click();
  const list = page.getByRole("region", { name: "Shard map" }).getByRole("table");
  await expect(list.getByRole("row")).toHaveCount(n2 + 1);
  await expect(list.getByRole("row").nth(1).getByRole("cell").nth(2)).toHaveText("n2");
  await noSidewaysScroll(page);
});

test("a stopped node's shards show on no node, then on the others", async ({ page }) => {
  await open(page, "/shards");
  await expect(stat(page, "On no node")).toContainText("0");
  await mock(page, "down", "n1");
  await expect(page.getByRole("button", { name: / on no node$/ }).first()).toBeVisible();
  await expect(page.getByRole("meter", { name: "n1" })).toContainText("No answer");
  await expect(stat(page, "On no node")).toContainText("0", { timeout: 10_000 });
  await expect(page.getByRole("button", { name: / on n1$/ })).toHaveCount(0);
});

test("the runs list each bench run, and a run shows its results", async ({ page }) => {
  await open(page, "/runs");
  const runs = page.getByRole("region", { name: "Runs" });
  await expect(runs.getByRole("link").first()).toBeVisible();
  const names = await runs.getByRole("row").locator("td:first-child").allInnerTexts();
  expect(names.length).toBeGreaterThan(1);
  expect(names).toEqual([...names].sort().reverse());
  const full = runs.getByRole("row").filter({ hasText: RUN });
  await expect(full).toContainText("epyc-4484px");
  // 14 fault phases and the soak, all PASS; the soak is not one of the phases.
  await expect(full.getByText("14 of 14 pass, soak passes", { exact: true })).toBeVisible();
  // The rows that summary.md lists apart, not a sum of their errors.
  await expect(full.getByRole("cell").nth(5)).toHaveText("12");
  await noSidewaysScroll(page);
  await shot(page, "runs");

  await full.getByRole("link", { name: RUN }).click();
  await expect(page).toHaveURL(`/runs/${RUN}`);
  await expect(page.getByRole("heading", { name: RUN, level: 1 })).toBeVisible();
  await expect(stat(page, "Box")).toContainText("epyc-4484px");
  await expect(stat(page, "Faults")).toContainText("14 of 14pass, soak passes");
  await expect(stat(page, "Rows")).toContainText("12 with errors");
  const saturation = page.getByRole("region", { name: "Saturation" });
  await expect(saturation.getByRole("img", { name: "Saturation of M, operations per second" }).locator("canvas")).toBeVisible();
  await saturation.getByRole("tab", { name: "R", exact: true }).click();
  await expect(saturation.getByRole("img", { name: "Saturation of R, operations per second" })).toBeVisible();
  // summary.md as tables, and the box it ran on.
  await expect(page.getByRole("status").filter({ hasText: "Indicative." })).toBeVisible();
  const ceilings = page.getByRole("region", { name: "Ceilings" });
  await expect(ceilings.getByRole("columnheader", { name: "A ops/s" })).toBeVisible();
  await expect(ceilings.getByRole("row").filter({ hasText: "etcd-check" })).toBeVisible();
  await expect(page.getByRole("region", { name: "Faults" }).getByRole("table")).toBeVisible();
  await expect(page.getByRole("region", { name: "Box" }).getByText("cpu: AMD EPYC 4484PX")).toBeVisible();
  await noSidewaysScroll(page);
  await shot(page, "run");
});

test("a run that is not there, and a page that is not there", async ({ page }) => {
  await open(page, "/runs/nothing-here");
  await expect(page.getByText("Run not found")).toBeVisible();
  await open(page, "/nothing");
  await expect(page.getByRole("heading", { name: "Page not found", level: 1 })).toBeVisible();
});

test("the sidebar goes to each page", async ({ page }) => {
  await open(page, "/");
  const sidebar = page.getByRole("navigation", { name: "Sidebar" });
  await sidebar.getByRole("link", { name: "Shards" }).click();
  await expect(page.getByRole("heading", { name: "Shards", level: 1 })).toBeVisible();
  await sidebar.getByRole("link", { name: "Runs" }).click();
  await expect(page.getByRole("heading", { name: "Bench runs", level: 1 })).toBeVisible();
  await expect(sidebar.getByRole("link", { name: "Runs" })).toHaveAttribute("aria-current", "page");
  await sidebar.getByRole("link", { name: "Overview" }).click();
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
});

test("each page has help, and the theme stays after a reload", async ({ page }) => {
  for (const [path, text] of [
    ["/", "An operation is one command"],
    ["/shards", "Each shard runs on one node at a time"],
    ["/runs", "A bench run is a directory"],
    [`/runs/${RUN}`, "A bar is the median of the rounds"],
  ]) {
    await open(page, path);
    await page.getByRole("button", { name: "Help" }).click();
    await expect(page.getByRole("dialog", { name: "Help" })).toContainText(text);
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog")).toHaveCount(0);
  }
  await page.getByRole("button", { name: "Use the dark theme" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-mode", "dark");
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-mode", "dark");
  await expect(page.getByRole("button", { name: "Use the light theme" })).toBeVisible();
});

test("the overview does not move or flash while it asks again", async ({ page }) => {
  await watchShift(page);
  await open(page, "/");
  await expect(row(page, "n3").getByText("Answers")).toBeVisible();
  await expect(page.getByRole("region", { name: "Operations per second" }).locator("canvas").first()).toBeVisible();
  await page.waitForTimeout(1000);
  const before = await shift(page);
  // Three refreshes of the page's data.
  const fewest = await page.evaluate(
    () =>
      new Promise<number>((done) => {
        let fewest = Infinity;
        const rows = setInterval(() => (fewest = Math.min(fewest, document.querySelectorAll("section[aria-label=Nodes] tbody tr").length)), 50);
        setTimeout(() => {
          clearInterval(rows);
          done(fewest);
        }, 6500);
      }),
  );
  expect(fewest).toBe(3);
  expect((await shift(page)) - before).toBeLessThan(0.01);
});

test("the overview comes in whole, and nothing moves while it loads", async ({ page }) => {
  // A node that does not answer puts a notice above the cards. The cluster answers late and the operations later, as on
  // a busy console: a part that came in by itself would push the parts under it down.
  await mock(page, "down", "n3");
  const late = (ms: number) => async (route: Route) => {
    await new Promise((done) => setTimeout(done, ms));
    await route.continue();
  };
  await page.route("**/api/cluster", late(600));
  await page.route("**/api/ops*", late(1200));
  await watchShift(page);
  await open(page, "/");
  await expect(page.getByRole("status").filter({ hasText: "1 node does not answer: n3." })).toBeVisible();
  await expect(page.getByRole("region", { name: "Operations per second" }).locator("canvas").first()).toBeVisible();
  await page.waitForTimeout(1500);
  expect(await shift(page)).toBeLessThan(0.01);
});

test("the time labels of a chart do not touch", async ({ page }) => {
  await open(page, "/");
  const charts = page.getByRole("region", { name: "Operations per second" }).locator("canvas");
  await expect(charts).toHaveCount(2);
  await page.mouse.move(page.viewportSize()!.width - 5, 5); // off the charts too: a pointer on one draws on its canvas
  await page.waitForTimeout(1500); // the lines draw in about a second
  for (const canvas of await charts.all()) {
    // The labels are on the canvas: find the runs of drawn columns in the band under the axis, a gap of 4 px or more
    // between two runs. Each run is one label, not two or more that touch.
    const runs = await canvas.evaluate((c: HTMLCanvasElement) => {
      const scale = c.width / c.clientWidth;
      const top = Math.round((c.clientHeight - 15) * scale);
      const rows = Math.round(10 * scale);
      const px = c.getContext("2d")!.getImageData(0, top, c.width, rows).data;
      const ink = (x: number) => [...Array(rows).keys()].some((y) => px[(y * c.width + x) * 4 + 3] > 0);
      const out: number[] = [];
      let start = -1;
      let gap = 0;
      for (let x = 0; x <= c.width; x++) {
        if (x < c.width && ink(x)) {
          if (start < 0) start = x;
          gap = 0;
        } else if (start >= 0 && ++gap >= 4 * scale) {
          out.push((x - gap + 1 - start) / scale);
          start = -1;
        }
      }
      return out;
    });
    expect(runs.length).toBeGreaterThanOrEqual(2);
    // "00:00:00" at 12 px is about 48 px wide.
    for (const width of runs) expect(width).toBeLessThan(60);
  }
});
