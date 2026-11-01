import { expect, test, type Locator } from "@playwright/test";
import { answer, choose, PASSWORD, signIn, type } from "./helpers";

test("a deep link goes through the login page and comes back; signing out ends the session", async ({ page }) => {
  await page.goto("/workers/hello/versions");
  await expect(page).toHaveURL("/login?next=workers%2Fhello%2Fversions");
  await expect(page.getByRole("heading", { name: "Sign in to edge workers" })).toBeVisible();
  await page.getByLabel("Password").fill("wrong");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("alert")).toHaveText("Wrong password.");
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/workers/hello/versions");
  await expect(page.getByRole("tab", { name: "versions" })).toHaveAttribute("aria-selected", "true");

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page).toHaveURL("/login");
  expect((await page.request.get("/api/workers")).status()).toBe(401);
});

test("a 401 on any page goes back to the login page", async ({ page, context }) => {
  await signIn(page);
  await expect(page.getByRole("link", { name: "hello", exact: true })).toBeVisible();
  await context.clearCookies();
  await page.getByRole("link", { name: "cluster" }).click();
  await expect(page).toHaveURL("/login?next=cluster");
});

test("the list shows each Worker with its numbers and its active version", async ({ page }) => {
  await signIn(page);
  const row = (name: string) => page.getByRole("row").filter({ has: page.getByRole("link", { name, exact: true }) });
  await expect(row("hello")).toContainText("v3 deployed");
  await expect(row("api")).toContainText("v1, canary v2 at 10%");
  await expect(row("draft")).toContainText("Not deployed");
  await expect(row("hello").getByRole("cell").nth(2)).not.toHaveText("0");
  await expect(row("draft").getByRole("cell").nth(4)).toHaveText("—");
});

test("each Worker shows the URL a gateway serves it at, in the list and on its page", async ({ page }) => {
  await signIn(page);
  const row = page.getByRole("row").filter({ has: page.getByRole("link", { name: "hello", exact: true }) });
  await expect(row.getByRole("link", { name: "http://127.0.0.1:7701/w/hello/" })).toHaveAttribute("href", "http://127.0.0.1:7701/w/hello/");
  await expect(row.getByRole("button", { name: "Copy the URL at g1" })).toBeVisible();

  await row.getByRole("link", { name: "hello", exact: true }).click();
  const urls = page.getByRole("group", { name: "URL of the Worker" });
  await expect(urls.getByRole("link")).toHaveText(["http://127.0.0.1:7701/w/hello/", "http://127.0.0.1:7702/w/hello/"]);
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
  await urls.getByRole("button", { name: "Copy the URL at g2" }).click();
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe("http://127.0.0.1:7702/w/hello/");

  // The Code tab says where the request of the form goes when it is sent to the deployed version.
  await page.getByRole("tab", { name: "Code" }).click();
  await page.getByLabel("Path").fill("/p?x=1");
  await expect(page.getByRole("group", { name: "URL of this request" }).getByRole("link")).toHaveText("http://127.0.0.1:7701/w/hello/p?x=1");
});

test("a Worker is made from a template, and a name that exists is refused as the API says it", async ({ page }) => {
  await signIn(page);
  await page.getByRole("button", { name: "Create Worker" }).first().click();
  const dialog = page.getByRole("dialog", { name: "Create a Worker", exact: true });
  await expect(dialog.getByRole("radio")).toHaveCount(6);

  await dialog.getByRole("textbox", { name: "Name" }).fill("hello");
  await dialog.getByRole("button", { name: "Create Worker" }).click();
  await expect(dialog.getByRole("alert")).toHaveText("409 exists: hello exists: a new version of it is POST /api/workers/hello/versions");

  await dialog.getByRole("textbox", { name: "Name" }).fill("-bad");
  await expect(dialog.getByRole("button", { name: "Create Worker" })).toBeDisabled();

  await dialog.getByRole("textbox", { name: "Name" }).fill("orders");
  await dialog.getByRole("radio", { name: /json-api/ }).check();
  await dialog.getByRole("button", { name: "Create Worker" }).click();
  await expect(page).toHaveURL("/workers/orders/code");
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("A small JSON API");
  await expect(page.getByRole("status", { name: "Deployment" })).toContainText("v1 deployed");

  await page.getByRole("navigation", { name: "breadcrumb" }).getByRole("link", { name: "Workers" }).click();
  await expect(page.getByRole("link", { name: "orders", exact: true })).toBeVisible();
});

test("unsaved code runs in the preview, and a source that does not load is refused with the reason", async ({ page }) => {
  await signIn(page, "/workers/hello/code");
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("greeting");
  await expect(page.getByText("No response", { exact: true })).toBeVisible();

  await type(page, answer("draft"));
  await expect(page.getByText("Changes not saved")).toBeVisible();
  await choose(page, "Method", "POST");
  await page.getByLabel("Path").fill("/p?x=1");
  await page.getByLabel("Request headers").fill("x-test: 1");
  await page.getByLabel("Request body").fill("body");
  await page.getByRole("button", { name: "Run preview" }).click();
  const response = page.getByRole("region", { name: "Response" });
  // The variables, and none of the secrets.
  await expect(response.getByRole("region", { name: "Body" })).toContainText("draft hello no-secret");
  await expect(response).toContainText("202");
  await expect(response.getByRole("region", { name: "Headers" })).toContainText("x-draft: yes");
  await expect(response.getByRole("region", { name: "Console" })).toContainText("Log: asked POST /p");
  await expect(response).toContainText("c1");
  await expect(response).toContainText(/CPU time\s*\d/);

  // The deployed version did not change.
  await page.getByRole("button", { name: "Call deployed version" }).click();
  await expect(response.getByRole("region", { name: "Body" })).toContainText("hello, you");
  await expect(response).toContainText("Deployed version v3");

  await type(page, "export default {");
  await page.getByRole("button", { name: "Run preview" }).click();
  await expect(page.getByRole("alert")).toContainText("422 load:");
});

test("new code is saved as a version, then deployed", async ({ page }) => {
  await signIn(page, "/workers/hello/code");
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("greeting");
  await type(page, answer("next"));
  await page.getByLabel("Message for the new version").fill("a new answer");
  await page.getByRole("button", { name: "Save as new version" }).click();
  await expect(page.getByText("Version 4 is saved. It is not deployed.")).toBeVisible();
  await expect(page.getByRole("status", { name: "Deployment" })).toContainText("v3 deployed");
  await expect(page.getByRole("combobox", { name: "Version in the editor" })).toHaveText("v4");

  await page.getByRole("button", { name: "Deploy version 4" }).click();
  await expect(page.getByText("Version 4 is deployed. It takes all the traffic.")).toBeVisible();
  await expect(page.getByRole("status", { name: "Deployment" })).toContainText("v4 deployed");

  await page.getByRole("button", { name: "Call deployed version" }).click();
  await expect(page.getByRole("region", { name: "Body" })).toContainText("next hello sk-mock-0001");

  await page.getByRole("tab", { name: "versions" }).click();
  await expect(page.getByRole("row").filter({ hasText: "a new answer" })).toContainText("Active");
});

test("a rollback, a canary, and a rollback with nothing older", async ({ page }) => {
  await signIn(page, "/workers/hello/versions");
  await expect(page.getByRole("row").filter({ hasText: "v3" }).first()).toContainText("Active");

  await page.getByRole("button", { name: "Roll back" }).click();
  await expect(page.getByRole("alertdialog")).toContainText("Roll back from v3 to the version before it?");
  await page.getByRole("alertdialog").getByRole("button", { name: "Roll back" }).click();
  await expect(page.getByText("Rolled back from version 3 to version 2.")).toBeVisible();
  await expect(page.getByRole("status", { name: "Deployment" })).toContainText("v2 deployed");

  await choose(page, "Canary version", "v3");
  await page.getByLabel("Canary percent").fill("25");
  await page.getByRole("button", { name: "Start canary" }).click();
  await expect(page.getByText("Version 3 is the canary. It takes 25% of the traffic.")).toBeVisible();
  await expect(page.getByRole("status", { name: "Deployment" })).toContainText("v2, canary v3 at 25%");
  await page.getByRole("button", { name: "Promote canary v3" }).click();
  await expect(page.getByRole("status", { name: "Deployment" })).toContainText("v3 deployed");

  // A version's size is its source's. What the engine built from it shows apart, with the version.
  await expect(page.getByRole("row").filter({ has: page.getByRole("button", { name: "View v1" }) })).toContainText(/\d+ B/);
  await expect(page.getByRole("table")).not.toContainText("MiB");
  // A version's source, and what changed from the active one.
  await page.getByRole("button", { name: "View v1" }).click();
  const drawer = page.getByRole("dialog", { name: "Version 1", exact: true });
  await expect(drawer).toContainText(/Size\d+ B/);
  await expect(drawer).toContainText(/Build11\.5 MiB/);
  await expect(drawer.getByRole("textbox", { name: "Source of version 1" })).toContainText('"world"');
  await drawer.getByRole("checkbox", { name: /Show the changes/ }).check();
  await expect(drawer.locator(".cm-deletedChunk").first()).toBeVisible();
  await drawer.getByRole("button", { name: "Close" }).click();

  // The API's refusal is shown as it is.
  await page.goto("/workers/counter/versions");
  await page.getByRole("button", { name: "Roll back" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Roll back" }).click();
  await expect(page.getByRole("alert")).toHaveText("409 nothing_to_roll_back: counter has no version older than the one deployed");
});

test("a secret is set and its value is not shown", async ({ page }) => {
  const SECRET = "s3cr3t-0f-tw3nty-ch4r";
  const bodies: string[] = [];
  page.on("response", (response) => response.url().includes("/api/") && !response.url().endsWith("/tail") && void response.text().then((text) => bodies.push(text), () => null));
  await signIn(page, "/workers/hello/settings");
  const row = (name: string) => page.getByRole("row").filter({ has: page.getByRole("cell", { name, exact: true }) });
  await expect(row("GREETING")).toContainText("hello");
  await expect(row("API_KEY")).toContainText("Hidden");

  await page.getByLabel("Name").fill("TOKEN");
  await page.getByRole("tab", { name: "Secret", exact: true }).click();
  await expect(page.getByLabel("Value")).toHaveAttribute("type", "password");
  await page.getByLabel("Value").fill(SECRET);
  await page.getByRole("button", { name: "Save and deploy" }).click();
  await expect(page.getByText("The secret TOKEN is set. Version 4 is deployed.")).toBeVisible();
  await expect(row("TOKEN")).toContainText("Hidden");
  await expect(page.getByLabel("Value")).toHaveValue("");
  await expect(page.locator("body")).not.toContainText(SECRET);
  await expect(page.getByRole("status", { name: "Deployment" })).toContainText("v4 deployed");

  // A plain variable is shown, and can go.
  await page.getByLabel("Name").fill("MODE");
  await page.getByRole("tab", { name: "Plain", exact: true }).click();
  await page.getByLabel("Value").fill("fast");
  await page.getByRole("button", { name: "Save and deploy" }).click();
  await expect(row("MODE")).toContainText("fast");
  await row("MODE").getByRole("button", { name: "Remove" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Remove" }).click();
  await expect(page.getByText("MODE is removed. Version 6 is deployed.")).toBeVisible();
  await expect(row("MODE")).toHaveCount(0);

  await page.reload();
  await expect(row("TOKEN")).toContainText("Hidden");
  await expect(page.locator("body")).not.toContainText(SECRET);
  expect(bodies.length).toBeGreaterThan(5);
  expect(bodies.filter((body) => body.includes(SECRET)), "no answer of the API holds the secret").toEqual([]);
});

test("the live tail receives lines, pauses, filters, and opens an invocation by its trace", async ({ page }) => {
  await signIn(page, "/workers/hello/logs");
  const rows = page.getByRole("row").filter({ has: page.getByRole("cell", { name: "GET" }) });
  await expect(page.getByText("Connected")).toBeVisible();
  await expect(rows.first()).toBeVisible();

  await page.getByRole("button", { name: "Pause" }).click();
  const held = await rows.count();
  await expect(page.getByRole("button", { name: /Resume \(\d+ new\)/ })).toBeVisible();
  expect(await rows.count()).toBe(held);
  await page.getByRole("button", { name: /Resume/ }).click();
  await expect.poll(() => rows.count()).toBeGreaterThan(held);

  await page.getByLabel("Filter by text").fill("no-line-has-this");
  await expect(page.getByText("No line agrees with the filter")).toBeVisible();
  await page.getByLabel("Filter by text").fill("");

  await page.getByRole("button", { name: "Pause" }).click();
  await rows.first().click();
  const drawer = page.getByRole("dialog", { name: /^Invocation [0-9a-f]{16}$/ });
  await expect(drawer).toContainText("Console");
  await expect(drawer).toContainText("Log: greeting");
  await expect(page).toHaveURL(/logs\?trace=[0-9a-f]{16}$/);
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();

  // The kept invocations, filtered by the API.
  await page.getByRole("tab", { name: "Recent" }).click();
  await choose(page, "Filter by status", "exception");
  const failed = page.getByRole("row").filter({ hasText: "500 exception" });
  await expect(failed.first()).toBeVisible();
  await expect(page.getByRole("row").filter({ hasText: /^.*\b200\b.*GET/ })).toHaveCount(0);
  await failed.first().click();
  await expect(page.getByRole("dialog", { name: /^Invocation/ })).toContainText("Error while running request handler: body is not JSON");
});

test("a Worker's objects are listed, an object's storage is read and deleted after a question in the page", async ({ page }) => {
  page.on("dialog", () => {
    throw new Error("the console asks in the page, never with a browser dialog");
  });
  await signIn(page, "/workers/counter/objects");
  const row = (id: string) => page.getByRole("row").filter({ has: page.getByRole("cell", { name: id, exact: true }) });
  await expect(row("lobby")).toContainText("Counter");
  await expect(row("lobby")).toContainText("r2");
  // A lease's epoch is a large number; the cell shows all of it.
  await expect(row("lobby").getByRole("cell").nth(4)).toHaveText("1791093947000003");
  await expect(row("2026/10")).toContainText("Not running");

  await row("lobby").click();
  const drawer = page.getByRole("dialog", { name: "Counter lobby", exact: true });
  await expect(drawer.getByRole("table", { name: "Storage keys" })).toContainText("count");
  await expect(drawer.getByRole("table", { name: "Storage keys" })).toContainText("42");
  await drawer.getByRole("button", { name: "Delete storage" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Cancel" }).click();
  await expect(drawer.getByRole("table", { name: "Storage keys" })).toContainText("42");
  await drawer.getByRole("button", { name: "Delete storage" }).click();
  await expect(page.getByRole("alertdialog")).toContainText("Delete all that Counter lobby stores?");
  await page.getByRole("alertdialog").getByRole("button", { name: "Delete storage" }).click();
  await expect(page.getByText("The storage of Counter lobby is deleted.")).toBeVisible();
  // The object runs on its runner until it is idle, so it stays in the list, with nothing stored.
  await expect(row("lobby")).toContainText("Empty");
  await expect(row("lobby")).toContainText("Running");

  // An id with a slash is one path segment.
  await row("2026/10").click();
  await expect(page.getByRole("dialog", { name: "Counter 2026/10", exact: true }).getByRole("table", { name: "Storage keys" })).toContainText("3");
  await page.keyboard.press("Escape");

  await page.goto("/workers/hello/objects");
  await expect(page.getByText("No objects", { exact: true })).toBeVisible();
});

test("the cluster page shows each node, its roles and what each runner serves", async ({ page }) => {
  await signIn(page, "/cluster");
  await expect(page.getByRole("heading", { name: "Cluster" })).toBeVisible();
  const row = (id: string) => page.getByRole("row").filter({ has: page.getByRole("cell", { name: id, exact: true }) });
  await expect(page.getByRole("row")).toHaveCount(7);
  await expect(row("c1")).toContainText("control");
  await expect(row("g2")).toContainText("gateway");
  await expect(row("r1")).toContainText("runner");
  await expect(row("g2").getByRole("link", { name: "http://127.0.0.1:7702" })).toBeVisible();
  await expect(row("r1")).toContainText("hello v3");
  await expect(row("r1")).toContainText("api v1 + v2 10%");
  await expect(row("r3")).toContainText("Available");
  await row("r2").getByRole("link", { name: /counter v1/ }).click();
  await expect(page).toHaveURL("/workers/counter");
});

test("the overview shows the numbers, the charts and each runner's version; a Worker not deployed says so", async ({ page }) => {
  await signIn(page, "/workers/api");
  await expect(page.getByRole("region", { name: "Totals" })).toContainText("Requests");
  await expect(page.getByRole("img", { name: /Requests and errors/ }).locator("canvas")).toBeVisible();
  await expect(page.getByRole("img", { name: /CPU time/ }).locator("canvas")).toBeVisible();
  await expect(page.getByRole("region", { name: "Deployment" }).getByRole("row").filter({ hasText: "r2" })).toContainText("v1, canary v2 at 10%");
  await expect(page.getByRole("region", { name: "Numbers by runner" }).getByRole("row")).toHaveCount(4);

  await page.goto("/workers/draft");
  await expect(page.getByText("This Worker is not deployed.")).toBeVisible();
  await expect(page.getByText("No requests in the last hour").first()).toBeVisible();
  await page.goto("/workers/absent");
  await expect(page.getByRole("alert")).toHaveText("404 not_found: no such worker");
});

test("the same build works when it is mounted under a path", async ({ page }) => {
  const MOUNTED = "http://127.0.0.1:7798/ops/edge/";
  await page.goto(`${MOUNTED}workers/hello/code`);
  await expect(page).toHaveURL(`${MOUNTED}login?next=workers%2Fhello%2Fcode`);
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL(`${MOUNTED}workers/hello/code`);
  await expect(page.getByRole("textbox", { name: "Source" })).toContainText("greeting");
  await page.getByRole("link", { name: "cluster" }).click();
  await expect(page).toHaveURL(`${MOUNTED}cluster`);
  await expect(page.getByRole("row")).toHaveCount(7);
});

test("the sidebar is a rail of icons that opens above the page, and its button keeps it open beside the page", async ({ page }) => {
  await signIn(page);
  const sidebar = page.getByRole("navigation", { name: "Sidebar" });
  const content = page.locator("#content");
  const box = async (of: Locator) => {
    await page.waitForTimeout(250); // the width's 150 ms
    return of.boundingBox();
  };
  const away = () => page.mouse.move(700, 400);

  // Closed at first: icons only, and the page beside the rail.
  await away();
  await expect(sidebar).toHaveAttribute("data-state", "closed");
  const toggle = sidebar.getByRole("button", { name: "Keep the sidebar open" });
  await expect(toggle).toHaveAttribute("aria-expanded", "false");
  const closed = { rail: await box(sidebar), toggle: await box(toggle), content: await box(content) };
  expect(closed.rail?.width).toBe(56);
  expect(closed.content?.x).toBe(56);
  await expect(sidebar.getByText("Compute")).toHaveCSS("opacity", "0");

  // The pointer opens it above the page: the page and the button stay where they are.
  await sidebar.hover({ position: { x: 28, y: 300 } });
  await expect(sidebar).toHaveAttribute("data-state", "over");
  await expect(sidebar.getByText("Compute")).toHaveCSS("opacity", "1");
  await expect(sidebar.getByRole("link", { name: "Workers", exact: true })).toHaveAttribute("aria-current", "page");
  expect((await box(sidebar))?.width).toBe(240);
  expect(await box(content)).toEqual(closed.content);
  expect(await box(toggle)).toEqual(closed.toggle);
  await away();
  await expect(sidebar).toHaveAttribute("data-state", "closed");
  expect((await box(sidebar))?.width).toBe(56);

  // The keyboard opens it too.
  await page.keyboard.press("Tab");
  await expect(sidebar.getByRole("link", { name: "edge workers" })).toBeFocused();
  await expect(sidebar).toHaveAttribute("data-state", "over");
  expect(await box(content)).toEqual(closed.content);
  await page.keyboard.press("Tab");
  await page.keyboard.press("Tab");
  await page.keyboard.press("Tab");
  await page.keyboard.press("Tab");
  await expect(toggle).toBeFocused();
  expect(await box(toggle)).toEqual(closed.toggle);

  // The button keeps it open: the page moves beside it, the button does not move.
  await page.keyboard.press("Enter");
  const pinned = sidebar.getByRole("button", { name: "Close the sidebar" });
  await expect(pinned).toHaveAttribute("aria-expanded", "true");
  await expect(sidebar).toHaveAttribute("data-state", "pinned");
  await away();
  await expect(sidebar).toHaveAttribute("data-state", "pinned");
  expect(await box(pinned)).toEqual(closed.toggle);
  expect((await box(content))?.x).toBe(240);

  // The browser remembers it.
  await page.reload();
  await expect(sidebar).toHaveAttribute("data-state", "pinned");
  expect(await box(sidebar.getByRole("button", { name: "Close the sidebar" }))).toEqual(closed.toggle);
  await sidebar.getByRole("link", { name: "Cluster" }).click();
  await expect(page).toHaveURL("/cluster");
  await expect(sidebar.getByRole("link", { name: "Cluster" })).toHaveAttribute("aria-current", "page");
  await sidebar.getByRole("button", { name: "Close the sidebar" }).click();
  await away();
  await expect(sidebar).toHaveAttribute("data-state", "closed");
  await page.reload();
  await away();
  await expect(sidebar).toHaveAttribute("data-state", "closed");
  expect(await box(sidebar.getByRole("button", { name: "Keep the sidebar open" }))).toEqual(closed.toggle);
});
