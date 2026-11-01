import { execSync } from "node:child_process";
import { expect, test, type BrowserContext, type Page } from "@playwright/test";
import { choose, fits, head, moved, nav, shifts } from "../tests/helpers";

const PASSWORD = process.env.WF_PASSWORD ?? "wf-dev";
const { WF_KILL_CMD, WF_START_CMD } = process.env;
/** Ids of this run of the suite, so a second run starts new runs. */
const TAG = Date.now().toString(36);
const ORDER = { sku: "mug", qty: 1, account: "acct-live", parcels: [{ to: "Oslo" }, { to: "Bergen" }] };

const sh = (command: string) => execSync(command, { stdio: "pipe", timeout: 180_000 }).toString();

let context: BrowserContext;
let page: Page;

const shot = async (name: string, of: Page = page, fullPage = false) => {
  if (!process.env.SHOTS) return;
  const { width, height } = of.viewportSize()!;
  await of.mouse.move(width / 2, height - 2); // off the sidebar, which opens while the pointer is on it
  await of.waitForTimeout(400); // a dialog and a tab's line move to their place
  await of.screenshot({ path: `screenshots/${name}.png`, fullPage });
};
const timeline = () => page.getByRole("region", { name: "Timeline" });
const toasts = () => page.getByRole("region", { name: "Notifications" }).getByRole("dialog");
const status = (text: string) => head(page).getByText(text, { exact: true });

/** Starts a run from the dialog of the runs page and waits for its page. */
async function start(workflow: string, id: string, input: unknown, name?: string) {
  await page.goto("/runs?start");
  const dialog = page.getByRole("dialog", { name: "Start a run" });
  await choose(page, "Workflow", workflow, dialog);
  await dialog.getByRole("textbox", { name: "Id" }).fill(id);
  await dialog.getByRole("textbox", { name: "Input" }).fill(JSON.stringify(input, null, 2));
  if (name) await shot(name);
  await dialog.getByRole("button", { name: "Start run" }).click();
  await expect(page).toHaveURL(`/runs/${id}`);
}

test.describe.configure({ mode: "serial" });

test.beforeAll(async ({ browser }) => {
  context = await browser.newContext();
  page = await context.newPage();
  page.on("dialog", () => {
    throw new Error("the console asks in the page, never with a browser dialog");
  });
});

test.afterAll(async () => {
  await context?.close();
});

test("the api node serves the console; a deep link goes through the login page and comes back", async () => {
  expect((await page.request.get("/api/runs")).status()).toBe(401);
  await page.goto("/workflows");
  await expect(page).toHaveURL("/login?next=workflows");
  await page.getByLabel("Password").fill("wrong");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("alert")).toHaveText("Wrong password.");
  await shot("login");
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/workflows");
  await expect(page.getByRole("link", { name: "checkout", exact: true })).toBeVisible();
});

test("a checkout waits for its approval; the signal completes it", async () => {
  const id = `live-${TAG}-ok`;
  await start("checkout", id, ORDER, "start");
  await expect(timeline().getByRole("row").filter({ hasText: "signal approve" })).toContainText("waiting");
  await expect(timeline().getByRole("button", { name: "Step 2: activity charge" })).toBeVisible();
  await expect(status("running")).toBeVisible();
  await shot("run-waiting");
  // The start's toast goes first (5 s), so the shots after the signal show its toast alone, not stacked on that one.
  await expect(toasts()).toHaveCount(0);

  await page.getByRole("button", { name: "Signal", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Send a signal" });
  await dialog.getByRole("textbox", { name: "Name" }).fill("approve");
  await dialog.getByRole("textbox", { name: "Payload" }).fill('{"by": "live test"}');
  await shot("signal");
  await dialog.getByRole("button", { name: "Send signal" }).click();
  await expect(toasts()).toHaveText(["Signal sent"]);
  await expect(status("completed")).toBeVisible();
  await expect(timeline().getByText("signal approve came in")).toBeVisible();
  await expect(timeline().getByRole("button", { name: /activity ship/ })).toHaveCount(2);
  await expect(page.getByRole("region", { name: "Output" })).toContainText("trk-");
  await timeline().getByRole("button", { name: "Step 2: activity charge" }).click();
  await shot("run-completed", page, true);
  await page.getByRole("tab", { name: "History" }).click();
  await expect(page.getByRole("list", { name: "Journal entries" }).getByRole("listitem").first()).toContainText('"Started"');
  await shot("history");
});

test("a declined charge fails the run; a retry runs the step again", async () => {
  const id = `live-${TAG}-declined`;
  await start("checkout", id, { ...ORDER, account: "declined" });
  await expect(status("failed")).toBeVisible();
  await expect(page.getByRole("status").filter({ hasText: "card_declined" })).toBeVisible();
  // The stock was released after the failed charge.
  await expect(timeline().getByRole("button", { name: /activity release/ })).toBeVisible();
  await shot("run-failed");
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(timeline().getByText("retry from step 2")).toBeVisible();
  // The bank still says no.
  await expect(status("failed")).toBeVisible();
});

test("a run started on the list's page shows in the list with its status and owner", async () => {
  await start("sleeper", `live-${TAG}-sleep`, { secs: 3600 });
  await expect(timeline().getByRole("row").filter({ hasText: "sleep" }).first()).toContainText("waiting");
  await page.goto(`/runs?prefix=live-${TAG}`);
  const sleeper = page.getByRole("row").filter({ has: page.getByRole("link", { name: `live-${TAG}-sleep`, exact: true }) });
  await expect(sleeper).toContainText("until");
  await expect(page.getByRole("row")).toHaveCount(4);
  await page.goto("/runs");
  await shot("runs");
  await page.getByRole("link", { name: `live-${TAG}-sleep`, exact: true }).click();
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Cancel run" }).click();
  await expect(status("cancelled")).toBeVisible();
});

test("when the engine that owns a run is killed, the run moves to the other engine, and the timeline shows it", async () => {
  test.skip(!WF_KILL_CMD || !WF_START_CMD, "WF_KILL_CMD and WF_START_CMD kill and start an engine");
  const id = `live-${TAG}-move`;
  await start("checkout", id, ORDER);
  await expect(timeline().getByRole("row").filter({ hasText: "signal approve" })).toContainText("waiting");
  const owner = ((await (await page.request.get(`/api/runs/${encodeURIComponent(id)}`)).json()) as { owner: string }).owner;
  expect(owner).toMatch(/^e[12]$/);
  const other = owner === "e1" ? "e2" : "e1";
  sh(WF_KILL_CMD!.replaceAll("{node}", owner));
  try {
    // The lease of the dead engine runs out, and the next read opens the run on the other one.
    await expect(timeline().getByText(`moved to ${other}`)).toBeVisible({ timeout: 60_000 });
    await expect(page.getByRole("region", { name: "Summary" })).toContainText(other);
    await shot("run-moved", page, true);
    // The run goes on where it was.
    await page.getByRole("button", { name: "Signal", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "Send a signal" });
    await dialog.getByRole("textbox", { name: "Name" }).fill("approve");
    await dialog.getByRole("button", { name: "Send signal" }).click();
    await expect(status("completed")).toBeVisible();
    await expect(timeline().getByRole("button", { name: /activity charge/ })).toHaveCount(1);
  } finally {
    sh(WF_START_CMD!.replaceAll("{node}", owner));
  }
});

test("the overview, workflows, queues and cluster pages show the cluster", async () => {
  await page.goto("/overview");
  await expect(page.getByRole("region", { name: "Counts" }).getByRole("link").first()).not.toHaveText("—");
  await page.waitForTimeout(1_500); // the chart draws its lines from the left
  await shot("overview");
  await nav(page, "Workflows").click();
  await expect(page.getByRole("link", { name: "checkout", exact: true })).toBeVisible();
  await shot("workflows");
  await nav(page, "Queues and workers").click();
  await expect(page.getByRole("cell", { name: "payments" }).first()).toBeVisible();
  await shot("queues");
  await nav(page, "Cluster").click();
  await expect(page.getByRole("region", { name: "State" })).toContainText("3 of 3");
  await shot("cluster");
  await page.getByRole("button", { name: "Use the dark theme" }).click();
  await shot("cluster-dark");
  await page.getByRole("button", { name: "Use the light theme" }).click();
});

test("no page shifts as it polls, and at phone width none scrolls sideways", async () => {
  const pages = { overview: "/overview", runs: "/runs", run: `/runs/live-${TAG}-ok`, workflows: "/workflows", queues: "/queues", cluster: "/cluster" };
  const failed: string[] = [];
  page.on("response", (r) => {
    if (r.url().includes("/api/") && r.status() >= 400) failed.push(`${r.status()} ${new URL(r.url()).pathname}`);
  });
  page.on("requestfailed", (r) => failed.push(`${r.failure()?.errorText} ${new URL(r.url()).pathname}`));
  for (const phone of [false, true]) {
    await page.setViewportSize(phone ? { width: 390, height: 844 } : { width: 1280, height: 760 });
    for (const [name, path] of Object.entries(pages)) {
      await page.goto(path);
      await expect(page.locator("h1")).toBeVisible();
      await page.waitForTimeout(600);
      const shift = await shifts(page, () => page.waitForTimeout(4_500));
      expect(shift, `${name} shifts as it polls: ${await moved(page)}; failed requests: ${failed.join(", ")}`).toBeLessThan(0.01);
      if (phone) {
        await fits(page);
        await shot(`phone-${name}`, page, true);
      }
    }
  }
  await page.setViewportSize({ width: 1280, height: 760 });
});
