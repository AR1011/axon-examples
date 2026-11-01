import { expect, test } from "@playwright/test";
import { choose, fits, head, nav, PASSWORD, shifts, signIn } from "./helpers";

const row = (page: import("@playwright/test").Page, id: string) => page.getByRole("row").filter({ has: page.getByRole("link", { name: id, exact: true }) });

test("a deep link goes through the login page and comes back; signing out ends the session", async ({ page }) => {
  await page.goto("/runs?status=failed");
  await expect(page).toHaveURL("/login?next=runs%3Fstatus%3Dfailed");
  await expect(page.getByRole("heading", { name: "Sign in to workflows" })).toBeVisible();
  await page.getByLabel("Password").fill("wrong");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("alert")).toHaveText("Wrong password.");
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/runs?status=failed");
  await expect(page.getByRole("combobox", { name: "Status" })).toHaveText(/failed/);
  await expect(row(page, "checkout-1040")).toBeVisible();

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page).toHaveURL("/login");
  expect((await page.request.get("/api/runs")).status()).toBe(401);
});

test("a 401 on any page goes back to the login page", async ({ page, context }) => {
  await signIn(page);
  await expect(page).toHaveURL("/overview");
  await context.clearCookies();
  // The page's next poll meets the 401. A click here would race it.
  await expect(page).toHaveURL("/login?next=overview");
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/overview");
  await context.clearCookies();
  await page.goto("/cluster");
  await expect(page).toHaveURL("/login?next=cluster");
});

test("the overview counts link to the runs they count", async ({ page }) => {
  await signIn(page);
  const counts = page.getByRole("region", { name: "Counts" });
  await expect(counts.getByRole("link")).toHaveCount(7);
  await expect(counts.getByText("Stuck", { exact: true })).toBeVisible();
  const paused = counts.getByRole("link").nth(2);
  await expect(paused).toHaveText("1");
  await paused.click();
  await expect(page).toHaveURL("/runs?status=paused");
  await expect(row(page, "sleeper-paused")).toBeVisible();
  await expect(page.getByRole("row")).toHaveCount(2);
  await page.goBack();
  const parked = counts.getByRole("link").nth(3);
  await expect(parked).toHaveText("1");
  await parked.click();
  await expect(page).toHaveURL("/runs?status=parked");
  await expect(row(page, "checkout-0999")).toBeVisible();
  await expect(page.getByRole("row")).toHaveCount(2);
});

test("the runs list filters by workflow, status, tag, parent and time, and goes page by page", async ({ page }) => {
  await signIn(page, "/runs");
  await expect(page.getByRole("button", { name: "Next" })).toBeEnabled();
  await expect(page.getByText("Page 1")).toBeVisible();
  await page.getByRole("button", { name: "Next" }).click();
  await expect(page.getByText("Page 2")).toBeVisible();
  await page.getByRole("button", { name: "Previous" }).click();
  await expect(page.getByText("Page 1")).toBeVisible();

  await choose(page, "Workflow", "checkout");
  await choose(page, "Status", "running");
  await expect(page).toHaveURL(/workflow=checkout&status=running/);
  await expect(row(page, "checkout-1042")).toContainText("a call");
  await expect(row(page, "checkout-1042").getByRole("cell").nth(6)).toHaveText("—");

  // A tag with no time bound reads past the rows a list scans.
  await page.goto("/runs?tag=env:test");
  await expect(page.getByText("Scanned 10,000 rows. Narrow the filter.")).toBeVisible();
  await page.getByRole("group", { name: "Started in" }).getByRole("tab", { name: "24 h" }).click();
  await expect(page).toHaveURL(/since=24h/);
  await expect(page.getByText("Scanned 10,000 rows. Narrow the filter.")).toBeHidden();

  await page.goto("/runs");
  await page.getByRole("textbox", { name: "Parent" }).fill("batch-7");
  await page.getByRole("textbox", { name: "Parent" }).press("Enter");
  await expect(page).toHaveURL("/runs?parent=batch-7");
  // In id order under a parent.
  await expect(page.getByRole("row").nth(1)).toContainText("batch-7/0");
  await expect(page.getByRole("row")).toHaveCount(13);

  await page.getByRole("textbox", { name: "Parent" }).fill("nobody");
  await page.getByRole("textbox", { name: "Parent" }).press("Enter");
  await expect(page.getByText("No run agrees with the filter.")).toBeVisible();
});

test("the runs list holds its rows while runs change, and shows the changes on a click", async ({ page }) => {
  await signIn(page, "/runs?workflow=sleeper");
  await expect(page.getByText(/^Read at /)).toBeVisible();
  const rows = await page.getByRole("row").count();
  const started = await page.request.post("/api/runs", { data: { workflow: "sleeper", id: "nap-held", input: { secs: 60 } } });
  expect(started.status()).toBe(201);
  const show = page.getByRole("button", { name: /^Show \d+ changes?$/ });
  await expect(show).toBeVisible();
  await expect(page.getByRole("row")).toHaveCount(rows);
  await expect(row(page, "nap-held")).toHaveCount(0);
  await show.click();
  await expect(row(page, "nap-held")).toBeVisible();
  await expect(page.getByRole("row")).toHaveCount(rows + 1);
});

test("the start dialog shows the same request as curl, refuses bad JSON, and opens the new run", async ({ page }) => {
  await signIn(page, "/runs");
  await page.getByRole("button", { name: "Start run" }).click();
  const dialog = page.getByRole("dialog", { name: "Start a run" });
  await choose(page, "Workflow", "sleeper", dialog);
  await dialog.getByRole("textbox", { name: "Id" }).fill("nap-1");
  await dialog.getByRole("textbox", { name: "Input" }).fill('{"secs": ');
  await expect(dialog.getByText(/The input is not JSON/)).toBeVisible();
  await expect(dialog.getByRole("button", { name: "Start run" })).toBeDisabled();
  await dialog.getByRole("textbox", { name: "Input" }).fill('{"secs": 1}');
  const curl = dialog.getByRole("region", { name: "The same with curl" });
  await expect(curl).toContainText("curl -X POST 'http://127.0.0.1:7789/api/runs'");
  await expect(curl).toContainText(`-d '{"workflow":"sleeper","id":"nap-1","input":{"secs":1}}'`);
  await dialog.getByRole("button", { name: "Start run" }).click();
  await expect(page).toHaveURL("/runs/nap-1");
  await expect(page.getByRole("heading", { name: "nap-1" })).toBeVisible();
  await expect(page.getByRole("button", { name: /Step 0: activity noop/ })).toBeVisible();
  // The mock's run completes after a second; the page polls.
  await expect(head(page).getByText("completed", { exact: true })).toBeVisible({ timeout: 10_000 });

  // The same id again starts nothing.
  await page.goto("/runs?start");
  await choose(page, "Workflow", "sleeper", page.getByRole("dialog"));
  await page.getByRole("dialog").getByRole("textbox", { name: "Id" }).fill("nap-1");
  await page.getByRole("dialog").getByRole("button", { name: "Start run" }).click();
  await expect(page.getByText("This run exists with the same input")).toBeVisible();
});

test("a run's timeline shows its steps, the move to another node, and each step's detail; a signal completes it", async ({ page }) => {
  await signIn(page, "/runs/checkout-1042");
  const timeline = page.getByRole("region", { name: "Timeline" });
  await expect(timeline.getByText("opened on e1")).toBeVisible();
  await expect(timeline.getByText("moved to e2")).toBeVisible();
  await expect(timeline.getByText("moved to e2")).toHaveClass(/font-medium/);
  const charge = timeline.getByRole("button", { name: "Step 1: activity charge" });
  await charge.click();
  await expect(charge).toHaveAttribute("aria-expanded", "true");
  await expect(timeline.getByRole("region", { name: "Error" })).toContainText("timeout: the bank did not answer");
  await expect(timeline.getByText("Error of the last attempt")).toBeVisible();
  await expect(timeline.getByRole("region", { name: "Output" })).toContainText('"charge": "ch-1042-1"');
  await expect(timeline.getByRole("row").filter({ hasText: "signal approve" })).toContainText("waiting");

  await page.getByRole("button", { name: "Signal", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Send a signal" });
  await dialog.getByRole("textbox", { name: "Name" }).fill("approve");
  await dialog.getByRole("textbox", { name: "Signal id" }).fill("ok-1");
  await dialog.getByRole("textbox", { name: "Payload" }).fill('{"by": "ana"}');
  await dialog.getByRole("button", { name: "Send signal" }).click();
  await expect(page.getByText("Signal sent")).toBeVisible();
  await expect(timeline.getByText("signal approve came in")).toBeVisible();
  await expect(timeline.getByRole("row").filter({ hasText: "signal approve" }).first()).toContainText("done");
  await expect(head(page).getByText("completed", { exact: true })).toBeVisible({ timeout: 10_000 });
});

test("a run of ten thousand steps shows 200 rows, and more on request", async ({ page }) => {
  await signIn(page, "/runs/noop-10k");
  const timeline = page.getByRole("region", { name: "Timeline" });
  await expect(timeline.getByText("200 of 10,002 rows")).toBeVisible();
  await expect(timeline.getByRole("button", { name: /^Step \d+: activity noop/ })).toHaveCount(199);
  await timeline.getByRole("button", { name: "Show more" }).click();
  await expect(timeline.getByText("400 of 10,002 rows")).toBeVisible();
});

test("the history tab pages through the journal", async ({ page }) => {
  await signIn(page, "/runs/noop-10k");
  await page.getByRole("tab", { name: "History" }).click();
  await expect(page).toHaveURL("/runs/noop-10k?tab=history");
  const entries = page.getByRole("list", { name: "Journal entries" });
  await expect(entries.getByRole("listitem")).toHaveCount(50);
  await expect(entries.getByRole("listitem").first()).toContainText('"Started"');
  await page.getByRole("button", { name: "Next" }).click();
  await expect(page.getByText("Entries 50 to 99")).toBeVisible();
  await page.getByRole("button", { name: "Previous" }).click();
  await expect(page.getByText("Entries 0 to 49")).toBeVisible();
});

test("a run is paused and resumed, a failed run retried, a running one terminated and a finished one deleted", async ({ page }) => {
  await signIn(page, "/runs/sleeper-paused");
  const header = head(page);
  await page.getByRole("button", { name: "Resume" }).click();
  await expect(header.getByText("running", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Pause" }).click();
  await expect(header.getByText("paused", { exact: true })).toBeVisible();

  await page.goto("/runs/checkout-1040");
  await expect(page.getByRole("status").filter({ hasText: "card_declined" })).toBeVisible();
  await page.getByRole("button", { name: "Retry" }).click();
  await expect(header.getByText("running", { exact: true })).toBeVisible();
  await expect(page.getByRole("region", { name: "Timeline" }).getByText("retry from step 1")).toBeVisible();

  await page.getByRole("button", { name: "Terminate" }).click();
  await page.getByRole("dialog", { name: "Terminate the run?" }).getByRole("textbox", { name: "Reason" }).fill("test over");
  await page.getByRole("dialog").getByRole("button", { name: "Terminate" }).click();
  await expect(header.getByText("terminated", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "Delete" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Delete run" }).click();
  await expect(page).toHaveURL("/runs");
  await page.goto("/runs/checkout-1040");
  await expect(page.getByText("Run not found or deleted")).toBeVisible();
});

test("a batch shows its children by status, and a child's id holds a slash", async ({ page }) => {
  await signIn(page, "/runs/batch-7");
  const children = page.getByRole("region", { name: "Children" });
  await expect(children).toContainText("completed9");
  await expect(children).toContainText("running2");
  await expect(children).toContainText("failed1");
  await page.getByRole("region", { name: "Timeline" }).getByRole("link", { name: "batch-7/11" }).click();
  await expect(page).toHaveURL("/runs/batch-7/11");
  await expect(page.getByRole("heading", { name: "batch-7/11" })).toBeVisible();
  await expect(page.getByRole("link", { name: "batch-7", exact: true })).toBeVisible();
});

test("without a quorum of history nodes every change is off, with the reason", async ({ page }) => {
  await page.route("**/ready", (route) => route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ journal: false, warming: false, isolated: false, quotas_starting: false }) }));
  await signIn(page, "/runs/checkout-1042");
  await expect(page.getByText("No quorum of history nodes. You cannot change runs now.").first()).toBeVisible();
  await expect(page.getByRole("button", { name: "Signal", exact: true })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Pause" })).toBeDisabled();
  await page.goto("/runs");
  await expect(page.getByRole("button", { name: "Start run" })).toBeDisabled();
  // The runs are still there to read.
  await expect(row(page, "checkout-1042")).toBeVisible();
});

test("when the api node stops answering, the page keeps its data and says from when", async ({ page }) => {
  await signIn(page, "/queues");
  await expect(page.getByRole("cell", { name: "payments", exact: true })).toBeVisible();
  await page.route("**/api/**", (route) => route.abort());
  await expect(page.getByText(/^Data from \d\d:\d\d:\d\d\. Retrying\.$/)).toBeVisible({ timeout: 15_000 });
  await expect(page.getByRole("cell", { name: "payments", exact: true })).toBeVisible();
  await page.unroute("**/api/**");
  await expect(page.getByText(/Retrying/)).toBeHidden({ timeout: 15_000 });
});

test("workflows, queues and cluster show what needs a look", async ({ page }) => {
  await signIn(page, "/workflows");
  const checkout = page.getByRole("row").filter({ has: page.getByRole("link", { name: "checkout", exact: true }) });
  await expect(checkout).toContainText("v1v2");
  // Unfinished runs on version 1, and one parked on version 3, which this build does not have.
  await expect(checkout.getByRole("cell").nth(9)).not.toHaveText("0");
  await expect(checkout.getByRole("cell").nth(10)).toHaveText("1");
  await checkout.getByRole("cell").nth(4).getByRole("link").click();
  await expect(page).toHaveURL("/runs?workflow=checkout&status=parked");
  await expect(row(page, "checkout-0999")).toBeVisible();

  await nav(page, "Queues and workers").click();
  await expect(page.getByText("No worker polls payments. Its tasks wait.")).toBeVisible();
  const payments = page.getByRole("row").filter({ hasText: "payments" }).first();
  await expect(payments).toContainText("0 pollers");
  await expect(payments).toContainText("6 of 6");
  await expect(payments.getByRole("cell").nth(5)).toHaveText("4");
  const worker = (node: string) => page.getByRole("region", { name: "Workers" }).getByRole("row").filter({ hasText: node });
  await expect(worker("w1")).toContainText("payments 4 of 4");
  await expect(worker("w1")).toContainText("41s ago");
  await expect(worker("w2")).toContainText("draining");
  await expect(worker("w2")).toContainText("now");

  await nav(page, "Cluster").click();
  await expect(page.getByRole("region", { name: "State" })).toContainText("2 of 3");
  await expect(page.getByRole("region", { name: "History nodes" }).getByRole("listitem").filter({ hasText: "h3" })).toContainText("down");
});

test("each page has a help dialog", async ({ page }) => {
  await signIn(page);
  for (const [link, title] of [["Overview", "Overview"], ["Runs", "Runs"], ["Workflows", "Workflows"], ["Queues and workers", "Queues and workers"], ["Cluster", "Cluster"]]) {
    await nav(page, link).click();
    await page.getByRole("button", { name: "Help" }).click();
    await expect(page.getByRole("dialog", { name: `Help: ${title}` })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog")).toBeHidden();
  }
});

test("the polls move nothing on the page", async ({ page }) => {
  await signIn(page, "/runs/checkout-1042");
  await expect(page.getByText("moved to e2")).toBeVisible();
  // Two polls of the run, the runs of its children and /ready.
  expect(await shifts(page, () => page.waitForTimeout(4_500))).toBeLessThan(0.01);
  await page.goto("/overview");
  await expect(page.getByRole("region", { name: "Counts" }).getByRole("link").first()).not.toHaveText("—");
  expect(await shifts(page, () => page.waitForTimeout(4_500))).toBeLessThan(0.01);
});

test("at phone width no page scrolls sideways", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await signIn(page);
  for (const path of ["/overview", "/runs", "/runs?start", "/runs/checkout-1042", "/runs/batch-7?tab=history", "/workflows", "/queues", "/cluster"]) {
    await page.goto(path);
    await expect(page.locator("h1")).toBeVisible();
    await page.waitForLoadState("networkidle");
    await fits(page);
    if (process.env.SHOTS) await page.screenshot({ path: `test-results/shots/phone${path.replaceAll(/[/?=]/g, "-")}.png`, fullPage: true });
  }
});

test("desktop shots of each page", async ({ page }) => {
  test.skip(!process.env.SHOTS, "SHOTS=1 writes them");
  await signIn(page);
  for (const path of ["/overview", "/runs", "/runs/checkout-1042", "/runs/batch-7", "/runs/noop-10k?tab=history", "/workflows", "/queues", "/cluster"]) {
    await page.goto(path);
    await expect(page.locator("h1")).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(1200); // the chart draws its lines
    await page.screenshot({ path: `test-results/shots/desktop${path.replaceAll(/[/?=]/g, "-")}.png`, fullPage: true });
  }
  await page.goto("/runs?start");
  await page.waitForTimeout(400);
  await page.screenshot({ path: "test-results/shots/desktop-start.png" });
});

test.describe("mounted under a path", () => {
  test.use({ baseURL: "http://127.0.0.1:7788" });

  test("the console works under /ops/wf/ and its links stay there", async ({ page }) => {
    await page.goto("/ops/wf/runs/batch-7/3");
    await expect(page).toHaveURL("/ops/wf/login?next=runs%2Fbatch-7%2F3");
    await page.getByLabel("Password").fill(PASSWORD);
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(page.getByRole("heading", { name: "batch-7/3" })).toBeVisible();
    await page.getByRole("link", { name: "batch-7", exact: true }).click();
    await expect(page).toHaveURL("/ops/wf/runs/batch-7");
    await nav(page, "Runs").click();
    await expect(page).toHaveURL("/ops/wf/runs");
    await expect(row(page, "checkout-1042")).toBeVisible();
    await nav(page, "Cluster").click();
    await expect(page).toHaveURL("/ops/wf/cluster");
  });
});
