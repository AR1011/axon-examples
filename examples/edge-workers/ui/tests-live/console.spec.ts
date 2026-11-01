import { execSync } from "node:child_process";
import { expect, request, test, type BrowserContext, type Page } from "@playwright/test";
import { choose, type } from "../tests/helpers";

const PASSWORD = process.env.EDGE_PASSWORD ?? "edge-dev";
const GATEWAY = process.env.EDGE_GATEWAY ?? "http://127.0.0.1:7701";
const { EDGE_LOAD_CMD, EDGE_KILL_CMD, EDGE_START_CMD } = process.env;
const NAMES = ["hello", "counter", "draft"];
const SECRET = "s3cr3t-0f-tw3nty-ch4r";

/** What the tests deploy as `hello`: a console line for each request, the variable and the secret, and a path that throws. */
const source = (text: string) => `export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    console.log("asked " + request.method + " " + url.pathname);
    if (url.pathname === "/fail") throw new Error("asked to fail");
    return new Response("${text} " + (env.GREETING ?? "none") + " " + (env.API_KEY ?? "no-secret"), { status: 202, headers: { "x-draft": "yes" } });
  },
};
`;

/** One request through a gateway, as a caller sends it. */
const through = (name: string, path: string, method = "GET") => fetch(`${GATEWAY}/w/${name}${path}`, { method, headers: { "x-account": "console-live" } });

/** Asks until the gateway and a runner give the answer that is wanted: a deployment takes a moment to reach them. */
async function eventually(name: string, path: string, method: string, wanted: (status: number, body: string) => boolean): Promise<Response> {
  let last!: Response;
  await expect(async () => {
    last = await through(name, path, method);
    const body = await last.clone().text();
    expect(wanted(last.status, body), `${method} ${path}: ${last.status} ${body}`).toBe(true);
  }).toPass({ timeout: 60_000, intervals: [300] });
  return last;
}

const run = (command: string) => execSync(command, { stdio: "pipe", timeout: 180_000 }).toString();

let context: BrowserContext;
let page: Page;
/** Every answer of the API the page read, but the tails. */
const bodies: string[] = [];

const shot = async (name: string, of: Page = page) => {
  if (!process.env.SHOTS) return;
  await of.waitForTimeout(400); // a tab's line and a dialog move to their place
  await of.screenshot({ path: `screenshots/${name}.png` });
};
/** The same page in the dark theme, then back. */
const dark = async (name: string) => {
  if (!process.env.SHOTS) return;
  await page.getByRole("button", { name: "Use the dark theme" }).click();
  await page.waitForTimeout(1200); // the charts draw again
  await shot(name);
  await page.getByRole("button", { name: "Use the light theme" }).click();
};
const row = (cell: string, of: Page = page) => of.getByRole("row").filter({ has: of.getByRole("cell", { name: cell, exact: true }) });
const deployment = () => page.getByRole("status", { name: "Deployment" });

test.describe.configure({ mode: "serial" });

test.beforeAll(async ({ browser, baseURL }) => {
  // What an earlier run left: the Workers of these tests, and what their objects store.
  const api = await request.newContext({ baseURL });
  const login = await api.post("/api/login", { data: { password: PASSWORD, token: true } });
  expect(login.status(), "EDGE_PASSWORD is the admin password of EDGE_URL").toBe(200);
  const headers = { authorization: `Bearer ${((await login.json()) as { token: string }).token}` };
  for (const name of NAMES) {
    const listed = await api.get(`/api/workers/${name}/objects`, { headers });
    if (listed.ok()) {
      for (const object of ((await listed.json()) as { objects: { class: string; id: string }[] }).objects) {
        await api.delete(`/api/workers/${name}/objects/${object.class}/${encodeURIComponent(object.id)}`, { headers });
      }
    }
    expect([200, 404]).toContain((await api.delete(`/api/workers/${name}`, { headers })).status());
  }
  await api.dispose();
  context = await browser.newContext();
  page = await context.newPage();
  page.on("dialog", () => {
    throw new Error("the console asks in the page, never with a browser dialog");
  });
  page.on("response", (response) => {
    if (response.url().includes("/api/") && !response.url().includes("/tail")) void response.text().then((text) => bodies.push(text), () => null);
  });
});

test.afterAll(async () => {
  await context?.close();
});

test("the control node serves the console; a deep link goes through the login page and comes back", async () => {
  expect((await page.request.get("/api/workers")).status()).toBe(401);
  await page.goto("/workers/hello/versions");
  await expect(page).toHaveURL("/login?next=workers%2Fhello%2Fversions");
  await expect(page.getByRole("heading", { name: "Sign in to edge workers" })).toBeVisible();
  await page.getByLabel("Password").fill("wrong");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("alert")).toHaveText("Wrong password.");
  await shot("login");
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/workers/hello/versions");
  // The worker is not there yet: the API's refusal, as it says it.
  await expect(page.getByRole("alert")).toHaveText("404 not_found: no such worker");
});

test("a Worker is made from a template", async () => {
  await page.goto("/workers");
  await page.getByRole("button", { name: "Create Worker" }).first().click();
  const dialog = page.getByRole("dialog", { name: "Create a Worker", exact: true });
  await expect(dialog.getByRole("radio")).toHaveCount(6);
  await dialog.getByRole("textbox", { name: "Name" }).fill("hello");
  await shot("create");
  await dialog.getByRole("button", { name: "Create Worker" }).click();
  await expect(page).toHaveURL("/workers/hello/code");
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("greeting");
  await expect(deployment()).toContainText("v1 deployed");

  // The same name again is refused as the API says it.
  await page.goto("/workers?create");
  await dialog.getByRole("textbox", { name: "Name" }).fill("hello");
  await dialog.getByRole("button", { name: "Create Worker" }).click();
  await expect(dialog.getByRole("alert")).toHaveText("409 exists: hello exists: a new version of it is POST /api/workers/hello/versions");

  // One that is not deployed: no runner runs it, and the pages say so.
  await dialog.getByRole("textbox", { name: "Name" }).fill("draft");
  await dialog.getByRole("checkbox", { name: "Deploy version 1 now" }).uncheck();
  await dialog.getByRole("button", { name: "Create Worker" }).click();
  await expect(page).toHaveURL("/workers/draft/code");
  await expect(deployment()).toContainText("Not deployed");
  await expect(page.getByRole("button", { name: "Call deployed version" })).toBeDisabled();
  await page.getByRole("tab", { name: "overview" }).click();
  await expect(page.getByText("This Worker is not deployed.")).toBeVisible();
  await expect(page.getByText("No requests in the last hour").first()).toBeVisible();
  await expect(page.getByRole("region", { name: "Deployment" }).getByRole("row")).toHaveCount(4);
  await shot("overview-not-deployed");
  await page.getByRole("tab", { name: "logs" }).click();
  await expect(page.getByText("No invocations yet")).toBeVisible();
  await expect(page.getByText("Connected")).toBeVisible();
  await shot("logs-empty");
});

test("code is changed, runs in the preview, and is saved and deployed", async () => {
  await page.goto("/workers/hello/code");
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("greeting");
  // The page says where the gateways serve it, and that URL answers.
  const urls = page.getByRole("group", { name: "URL of the Worker" });
  await expect(urls.getByRole("link")).toHaveCount(2);
  await expect(urls.getByRole("link").first()).toHaveText(`${GATEWAY}/w/hello/`);
  await expect(urls.getByRole("button", { name: "Copy the URL at g1" })).toBeVisible();
  await shot("code");
  await dark("code-dark");
  await type(page, source("draft"));
  await expect(page.getByText("Changes not saved")).toBeVisible();
  await choose(page, "Method", "POST");
  await page.getByLabel("Path").fill("/p?x=1");
  await page.getByLabel("Request headers").fill("x-test: 1");
  await page.getByLabel("Request body").fill("body");
  await page.getByRole("button", { name: "Run preview" }).click();
  const response = page.getByRole("region", { name: "Response" });
  await expect(response.getByRole("region", { name: "Body" })).toContainText("draft none no-secret");
  await expect(response).toContainText("202");
  await expect(response.getByRole("region", { name: "Headers" })).toContainText("x-draft: yes");
  await expect(response.getByRole("region", { name: "Console" })).toContainText("Log: asked POST /p");
  await expect(response).toContainText(/Node\s*c1/);
  await expect(response).toContainText(/CPU time\s*\d/);
  await shot("code-preview");

  // The deployed version did not change.
  await page.getByRole("button", { name: "Call deployed version" }).click();
  await expect(response.getByRole("region", { name: "Body" })).toContainText("hello, world");
  await expect(response).toContainText("Deployed version v1");

  // Code that throws: the engine's word for it, and the console lines up to it.
  await choose(page, "Method", "GET");
  await page.getByLabel("Path").fill("/fail");
  await page.getByRole("button", { name: "Run preview" }).click();
  await expect(response).toContainText("500");
  await expect(response.getByRole("status")).toHaveText("threw: asked to fail");
  await expect(response.getByRole("region", { name: "Console" })).toContainText("Error while running request handler: asked to fail");

  // A source that does not load is refused with the engine's reason, each line of it.
  await type(page, "export default {");
  await page.getByRole("button", { name: "Run preview" }).click();
  await expect(page.getByRole("alert")).toContainText("422 load: the source did not load:");
  await expect(page.getByRole("alert")).toContainText("SyntaxError");
  // The response of the run before is gone, so the page does not show an old answer under a new refusal.
  await expect(page.getByText("threw: asked to fail")).toHaveCount(0);
  await shot("code-load-error");
  await page.getByRole("button", { name: "Save and deploy" }).click();
  await expect(page.getByRole("alert")).toContainText("422 load:");
  await expect(deployment()).toContainText("v1 deployed");

  await type(page, source("next"));
  await page.getByLabel("Message for the new version").fill("a new answer");
  await page.getByRole("button", { name: "Save and deploy" }).click();
  await expect(page.getByText("Version 2 is deployed. It takes all the traffic.")).toBeVisible();
  await expect(deployment()).toContainText("v2 deployed");
  await expect(page.getByRole("combobox", { name: "Version in the editor" })).toHaveText("v2 (active)");
  await page.getByLabel("Path").fill("/");
  await expect(async () => {
    await page.getByRole("button", { name: "Call deployed version" }).click();
    await expect(response).toContainText("Deployed version v2", { timeout: 2000 });
  }).toPass();
  await expect(response.getByRole("region", { name: "Body" })).toContainText("next none no-secret");
  await expect(response).toContainText(/Node\s*r[123]/);
});

test("a call through a gateway shows in the live tail and in the recent invocations, with its console lines", async () => {
  await page.goto("/workers/hello/logs");
  await expect(page.getByText("Connected")).toBeVisible();
  const called = await eventually("hello", "/from-gateway?x=1", "GET", (status) => status === 202);
  const trace = called.headers.get("x-trace-id")!;
  expect(trace).toMatch(/^[0-9a-f]{16}$/);
  const line = page.getByRole("row").filter({ hasText: "/from-gateway?x=1" }).first();
  await expect(line).toContainText("Log: asked GET /from-gateway");
  await expect(line).toContainText("202");
  await expect(line).toContainText("v2");
  // One that throws, for the list.
  await eventually("hello", "/fail", "POST", (status) => status === 500);
  await expect(page.getByRole("row").filter({ hasText: "500 exception" }).first()).toContainText("/fail");
  await page.getByRole("button", { name: "Pause" }).click();
  await shot("logs");

  await page.getByRole("tab", { name: "Recent" }).click();
  const kept = page.getByRole("row").filter({ hasText: "/from-gateway?x=1" }).first();
  await expect(kept).toContainText("Log: asked GET /from-gateway");
  await kept.click();
  await expect(page).toHaveURL(new RegExp(`logs\\?trace=${trace}$`));
  const drawer = page.getByRole("dialog", { name: `Invocation ${trace}`, exact: true });
  await expect(drawer.getByRole("region", { name: "Console" })).toContainText("Log: asked GET /from-gateway");
  await expect(drawer).toContainText(/Runner\s*r[123]/);
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();

  // The API filters the kept ones.
  await choose(page, "Filter by status", "exception");
  await expect(page.getByRole("row").filter({ hasText: "/from-gateway" })).toHaveCount(0);
  await page.getByRole("row").filter({ hasText: "500 exception" }).first().click();
  await expect(page.getByRole("dialog", { name: /^Invocation [0-9a-f]{16}$/ })).toContainText("Error while running request handler: asked to fail");
  await shot("logs-invocation");
});

test("a Durable Object counts, shows with its node and epoch, and its storage is read and deleted", async () => {
  await page.goto("/workers?create");
  const dialog = page.getByRole("dialog", { name: "Create a Worker", exact: true });
  await dialog.getByRole("textbox", { name: "Name" }).fill("counter");
  await dialog.getByRole("radio", { name: /counter/ }).check();
  await expect(dialog).toContainText("COUNTER = Counter");
  await dialog.getByRole("button", { name: "Create Worker" }).click();
  await expect(page).toHaveURL("/workers/counter/code");
  await page.getByRole("tab", { name: "objects" }).click();
  await expect(page.getByText("No objects", { exact: true })).toBeVisible();

  await eventually("counter", "/lobby", "POST", (status, body) => status === 200 && body.includes('"count":1'));
  for (const count of [2, 3]) expect(await (await through("counter", "/lobby", "POST")).json()).toEqual({ name: "lobby", count });
  await eventually("counter", "/hall", "GET", (status) => status === 200);
  await expect(row("lobby")).toContainText("Counter");
  await expect(row("lobby")).toContainText("Running");
  await expect(row("lobby").getByRole("cell").nth(3)).toHaveText(/^r[123]$/);
  await expect(row("lobby").getByRole("cell").nth(4)).toHaveText(/^\d+$/);
  await expect(row("lobby")).toContainText("Has data");
  // It runs, and has written nothing.
  await expect(row("hall")).toContainText("Empty");
  await shot("objects");

  await row("lobby").click();
  const drawer = page.getByRole("dialog", { name: "Counter lobby", exact: true });
  const keys = drawer.getByRole("table", { name: "Storage keys" });
  await expect(keys.getByRole("row").filter({ hasText: "count" })).toContainText("3");
  await drawer.getByRole("button", { name: "Delete storage" }).click();
  await expect(page.getByRole("alertdialog")).toContainText("Delete all that Counter lobby stores?");
  await page.getByRole("alertdialog").getByRole("button", { name: "Cancel" }).click();
  await shot("objects-storage");
  await drawer.getByRole("button", { name: "Delete storage" }).click();
  await shot("objects-delete");
  await page.getByRole("alertdialog").getByRole("button", { name: "Delete storage" }).click();
  await expect(page.getByText("The storage of Counter lobby is deleted.")).toBeVisible();
  // The object stays on its runner until it is idle, with nothing stored.
  await expect(row("lobby")).toContainText("Empty");
  await row("lobby").click();
  await expect(drawer.getByText("No keys")).toBeVisible();
  await page.keyboard.press("Escape");
  // Its next request fails, and the one after starts from nothing.
  await eventually("counter", "/lobby", "POST", (status, body) => status === 200 && body.includes('"count":1'));

  await page.getByRole("tab", { name: "settings" }).click();
  await expect(page.getByText("No variables")).toBeVisible();
  await expect(page.getByText("env.COUNTER = Counter")).toBeVisible();
  await shot("settings-empty");
});

test("after load the overview shows the numbers and the charts, and the list its counters", async () => {
  test.setTimeout(240_000);
  if (EDGE_LOAD_CMD) {
    console.log(run(EDGE_LOAD_CMD));
  } else {
    // 25 s at about 20 a second: more than two samples of the control node.
    for (let second = 0; second < 25; second++) {
      await Promise.all([...Array.from({ length: 20 }, (_, i) => through("hello", `/load?i=${i}`)), new Promise((done) => setTimeout(done, 1000))]);
    }
  }
  for (let i = 0; i < 12; i++) await through("hello", "/fail");

  await page.goto("/workers/hello");
  type Metrics = { total: { requests: number; errors: number }; series: { requests: number; errors: number; cpu_p50_us: number | null }[] };
  const metrics = async () => (await (await page.request.get("/api/workers/hello/metrics")).json()) as Metrics;
  // The control node asks the runners every 10 s: a point holds what ran between two answers.
  await expect.poll(async () => (await metrics()).series.filter((point) => point.requests > 0).length, { timeout: 60_000 }).toBeGreaterThan(1);
  await expect.poll(async () => (await metrics()).series.some((point) => point.errors > 0), { timeout: 30_000 }).toBe(true);
  const { total } = await metrics();
  expect(total.requests).toBeGreaterThan(200);
  expect(total.errors).toBeGreaterThan(10);

  await page.reload();
  for (const chart of [/Requests and errors/, /CPU time/, /Wall time/]) await expect(page.getByRole("img", { name: chart }).locator("canvas")).toBeVisible();
  const totals = page.getByRole("region", { name: "Totals" });
  await expect(totals).toContainText("Requests");
  await expect(totals).not.toContainText("NaN");
  // A runner has a row once it has counters for the Worker; the one that took the load has them.
  await expect(page.getByRole("region", { name: "Numbers by runner" }).getByRole("row").filter({ hasText: /^r[123]/ }).first()).toBeVisible();
  await expect(page.getByRole("region", { name: "Numbers by runner" })).toContainText("Errors by cause: exception");
  for (const runner of ["r1", "r2", "r3"]) await expect(page.getByRole("region", { name: "Deployment" }).getByRole("row").filter({ hasText: runner })).toContainText("v2");
  await page.waitForTimeout(500); // the charts draw after their data
  await shot("overview");
  await dark("overview-dark");

  await page.getByRole("navigation", { name: "breadcrumb" }).getByRole("link", { name: "Workers" }).click();
  const listed = page.getByRole("row").filter({ has: page.getByRole("link", { name: "hello", exact: true }) });
  await expect(listed).toContainText("v2 deployed");
  await expect(listed.getByRole("cell").nth(2)).not.toHaveText("0");
  await expect(listed.getByRole("cell").nth(4)).toHaveText(/\d (µs|ms)$/);
  // Where a gateway serves it: the URL in the list is the one that answers.
  const url = await listed.getByRole("link", { name: /\/w\/hello\/$/ }).getAttribute("href");
  expect(url).toBe(`${GATEWAY}/w/hello/`);
  expect((await fetch(url!)).status).toBe(202);
  await expect(page.getByRole("row").filter({ has: page.getByRole("link", { name: "draft", exact: true }) })).toContainText("Not deployed");
  await shot("workers");
  await dark("workers-dark");
});

test("a rollback, a canary on every runner, and its promotion", async () => {
  await page.goto("/workers/hello/versions");
  await expect(page.getByRole("row").filter({ hasText: "a new answer" })).toContainText("Active");
  await page.getByRole("button", { name: "Roll back" }).click();
  await expect(page.getByRole("alertdialog")).toContainText("Roll back from v2 to the version before it?");
  await page.getByRole("alertdialog").getByRole("button", { name: "Roll back" }).click();
  await expect(page.getByText("Rolled back from version 2 to version 1.")).toBeVisible();
  await expect(deployment()).toContainText("v1 deployed");
  await eventually("hello", "/", "GET", (status, body) => status === 200 && body.includes("hello, world"));

  // Nothing older than version 1: the API's refusal as it is.
  await expect(page.getByRole("alertdialog")).toBeHidden();
  await page.getByRole("button", { name: "Roll back" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Roll back" }).click();
  await expect(page.getByRole("alert")).toHaveText("409 nothing_to_roll_back: hello has no version older than the one deployed");

  await choose(page, "Canary version", "v2");
  await page.getByLabel("Canary percent").fill("25");
  await page.getByRole("button", { name: "Start canary" }).click();
  await expect(page.getByText("Version 2 is the canary. It takes 25% of the traffic.")).toBeVisible();
  await expect(deployment()).toContainText("v1, canary v2 at 25%");
  await expect(page.getByRole("row").filter({ hasText: "a new answer" })).toContainText("Canary 25%");
  await shot("versions");
  // Both versions answer.
  const seen = new Set<string>();
  await expect(async () => {
    seen.add((await through("hello", `/?n=${seen.size}${Math.random()}`)).headers.get("x-edge-version") ?? "");
    expect([...seen].sort()).toEqual(["1", "2"]);
  }).toPass({ timeout: 60_000, intervals: [50] });
  // What each runner serves, as the cluster says it and as the overview says it.
  await page.getByRole("tab", { name: "overview" }).click();
  for (const runner of ["r1", "r2", "r3"]) await expect(page.getByRole("region", { name: "Deployment" }).getByRole("row").filter({ hasText: runner })).toContainText("v1, canary v2 at 25%");
  await page.getByRole("link", { name: "cluster" }).click();
  await expect(page.getByRole("heading", { name: "Cluster" })).toBeVisible();
  for (const runner of ["r1", "r2", "r3"]) await expect(row(runner)).toContainText("hello v1 + v2 25%");

  await page.goto("/workers/hello/versions");
  await page.getByRole("button", { name: "Promote canary v2" }).click();
  await expect(deployment()).toContainText("v2 deployed");

  await expect(page.getByRole("table")).not.toContainText("MiB");
  await page.getByRole("button", { name: "View v1" }).click();
  const drawer = page.getByRole("dialog", { name: "Version 1", exact: true });
  await expect(drawer).toContainText(/Size\d+ B/);
  await expect(drawer).toContainText(/Build\d+\.\d MiB/);
  await expect(drawer.getByRole("textbox", { name: "Source of version 1" })).toContainText("greeting");
  await drawer.getByRole("checkbox", { name: /Show the changes/ }).check();
  // Version 1 begins with a line version 2 does not have, so the first chunk removes nothing.
  await expect(drawer.locator(".cm-deletedChunk").filter({ hasText: "asked to fail" })).toBeVisible();
  await shot("versions-diff");
  await drawer.getByRole("button", { name: "Close" }).click();
});

test("a variable and a secret reach the Worker, and the secret is never shown", async () => {
  await page.goto("/workers/hello/settings");
  await expect(page.getByText("No variables")).toBeVisible();
  await page.getByLabel("Name").fill("GREETING");
  await page.getByLabel("Value").fill("hi");
  await page.getByRole("button", { name: "Save and deploy" }).click();
  await expect(page.getByText("The variable GREETING is set. Version 3 is deployed.")).toBeVisible();
  await expect(row("GREETING")).toContainText("hi");

  await page.getByLabel("Name").fill("API_KEY");
  await page.getByRole("tab", { name: "Secret", exact: true }).click();
  await expect(page.getByLabel("Value")).toHaveAttribute("type", "password");
  await page.getByLabel("Value").fill(SECRET);
  await page.getByRole("button", { name: "Save and deploy" }).click();
  await expect(page.getByText("The secret API_KEY is set. Version 4 is deployed.")).toBeVisible();
  await expect(row("API_KEY")).toContainText("Hidden");
  await expect(page.getByLabel("Value")).toHaveValue("");
  await expect(deployment()).toContainText("v4 deployed");
  await shot("settings");

  // The worker has both. This answer comes from a gateway, not from the API.
  await eventually("hello", "/", "GET", (status, body) => status === 202 && body === `next hi ${SECRET}`);
  // A preview has the variable and not the secret.
  await page.getByRole("tab", { name: "code" }).click();
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("asked to fail");
  await page.getByRole("button", { name: "Run preview" }).click();
  await expect(page.getByRole("region", { name: "Body" })).toContainText("next hi no-secret");
  await page.getByRole("tab", { name: "versions" }).click();
  await page.getByRole("button", { name: "View v4" }).click();
  await expect(page.getByRole("dialog", { name: "Version 4", exact: true })).toContainText(/Secrets\s*API_KEY/);
  await page.keyboard.press("Escape");

  await page.getByRole("tab", { name: "settings" }).click();
  await page.reload();
  await expect(row("API_KEY")).toContainText("Hidden");
  await expect(page.locator("body")).not.toContainText(SECRET);
  expect(bodies.length).toBeGreaterThan(20);
  expect(bodies.filter((body) => body.includes(SECRET)), "no answer of the API holds the secret").toEqual([]);
});

test("the cluster page shows the six nodes, their roles and the versions each runner serves", async () => {
  await page.goto("/cluster");
  await expect(page.getByRole("heading", { name: "Cluster" })).toBeVisible();
  await expect(page.getByRole("row")).toHaveCount(7);
  await expect(row("c1")).toContainText("control");
  for (const gateway of ["g1", "g2"]) await expect(row(gateway)).toContainText("gateway");
  for (const runner of ["r1", "r2", "r3"]) {
    await expect(row(runner)).toContainText("runner");
    await expect(row(runner)).toContainText("Available");
    await expect(row(runner)).toContainText("hello v4");
    await expect(row(runner)).toContainText("counter v1");
    await expect(row(runner).getByRole("cell").nth(2)).toHaveText(/^127\.0\.0\.1:\d+$/);
  }
  const totals = page.getByRole("region", { name: "Totals" });
  await expect(totals).toContainText(/Nodes\s*6/);
  await expect(totals).toContainText(/Runners\s*3/);
  // Each gateway says where clients reach it.
  await expect(row("g1").getByRole("link", { name: GATEWAY })).toHaveAttribute("href", GATEWAY);
  await expect(row("g2").getByRole("link", { name: /^http:/ })).toBeVisible();
  await shot("cluster");
  await dark("cluster-dark");

  // The sidebar: a rail of icons, open above the page under the pointer, and open beside the page when it is kept open.
  const sidebar = page.getByRole("navigation", { name: "Sidebar" });
  await page.mouse.move(700, 400);
  await expect(sidebar).toHaveAttribute("data-state", "closed");
  await shot("sidebar-closed");
  await dark("sidebar-closed-dark");
  await sidebar.hover();
  await expect(sidebar).toHaveAttribute("data-state", "over");
  await shot("sidebar-over");
  await sidebar.getByRole("button", { name: "Keep the sidebar open" }).click();
  await expect(sidebar).toHaveAttribute("data-state", "pinned");
  await page.mouse.move(700, 400);
  await shot("sidebar-open");
  await dark("sidebar-open-dark");
  await sidebar.getByRole("button", { name: "Close the sidebar" }).click();
  await page.mouse.move(700, 400);
  await expect(sidebar).toHaveAttribute("data-state", "closed");
  await page.getByRole("button", { name: "Use the dark theme" }).click();
  await sidebar.hover();
  await shot("sidebar-over-dark");
  await page.mouse.move(700, 400);
  await page.getByRole("button", { name: "Use the light theme" }).click();
  await row("r2").getByRole("link", { name: /counter v1/ }).click();
  await expect(page).toHaveURL("/workers/counter");
});

test("a runner that is killed shows as not in the cluster on both pages, and as available when it starts again", async () => {
  test.skip(!EDGE_KILL_CMD || !EDGE_START_CMD, "set EDGE_KILL_CMD and EDGE_START_CMD");
  test.setTimeout(240_000);
  const cluster = await context.newPage();
  await page.goto("/workers/hello/logs");
  await cluster.goto("/cluster");
  await expect(page.getByText("Connected")).toBeVisible();
  await expect(row("r3", cluster)).toContainText("Available");

  run(EDGE_KILL_CMD!);
  await expect(row("r3", cluster)).toContainText("Not in the cluster", { timeout: 30_000 });
  await expect(cluster.getByRole("status").filter({ hasText: "These nodes are not in the cluster now: r3" })).toBeVisible();
  await expect(cluster.getByRole("row")).toHaveCount(7);
  await expect(row("r1", cluster)).toContainText("Available");
  await shot("cluster-runner-gone", cluster);
  // The tail is the control node's, so it stays; the page says which runner gives it nothing, and the others go on.
  await expect(page.getByRole("status").filter({ hasText: "These runners are not in the cluster now: r3" })).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("Connected")).toBeVisible();
  await eventually("hello", "/after-the-kill", "GET", (status) => status === 202);
  await expect(page.getByRole("row").filter({ hasText: "/after-the-kill" }).first()).toContainText(/r[12]/);
  await shot("logs-runner-gone");
  await page.getByRole("tab", { name: "overview" }).click();
  await expect(page.getByRole("region", { name: "Deployment" }).getByRole("row").filter({ hasText: "r3" })).toContainText("Not in the cluster");
  await page.getByRole("tab", { name: "logs" }).click();

  run(EDGE_START_CMD!);
  await expect(row("r3", cluster)).toContainText("Available", { timeout: 60_000 });
  await expect(row("r3", cluster)).toContainText("hello v4", { timeout: 60_000 });
  await expect(cluster.getByText("not in the cluster now")).toHaveCount(0);
  await expect(page.getByText("not in the cluster now")).toHaveCount(0);
  await expect(page.getByText("Connected")).toBeVisible();
  await shot("cluster-runner-back", cluster);
  await cluster.close();
});

test("a narrow window, and signing out", async () => {
  await page.setViewportSize({ width: 640, height: 760 });
  await page.goto("/workers/hello/code");
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("asked to fail");
  await shot("code-narrow");
  await page.setViewportSize({ width: 1280, height: 760 });
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page).toHaveURL("/login");
  expect((await page.request.get("/api/workers")).status()).toBe(401);
});
