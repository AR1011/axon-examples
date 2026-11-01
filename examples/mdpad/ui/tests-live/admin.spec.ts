// Journey 8 and the published page on a real cluster (design §3, §4.1): the operator dev, made by `run.sh up`.
import { expect, test } from "@playwright/test";
import { editor, newDoc, open, shot, signIn } from "./helpers";

const fresh = (prefix: string) => `${prefix}-${Date.now().toString(36)}${Math.floor(Math.random() * 1e4)}`;

test("journey 8: dev sees the open documents on each home, and a disable closes the user's editor", async ({ browser }) => {
  const [dev, user] = [await browser.newContext(), await browser.newContext()];
  await signIn(dev, "dev", "dev-pw");
  const admin = await dev.newPage();
  await admin.goto("/admin");
  await expect(admin.getByRole("heading", { name: "Admin" })).toBeVisible();

  // An invite from the page makes the account.
  const name = fresh("eve");
  await admin.getByLabel("New user").fill(name);
  await admin.getByRole("button", { name: "Invite" }).click();
  const link = await admin.getByText("/i/").first().textContent();
  const page = await user.newPage();
  await page.goto(new URL(link!.trim()).pathname);
  await page.getByLabel("Password", { exact: true }).fill(`${name}-pw`);
  await page.getByLabel("Password again").fill(`${name}-pw`);
  await page.getByRole("button", { name: "Set password" }).click();
  await expect(page.getByRole("heading", { name: "Documents", exact: true })).toBeVisible();

  // Her document on h2 is open there while her editor is.
  const title = `Report of ${name}`;
  const id = await newDoc(user, `# ${title}\n\nText.\n`, ["h2"]);
  await open(page, id);
  await expect(editor(page)).toContainText("Text.");
  // The list of users is read when the page opens: she is in it once the page is opened again.
  await admin.reload();
  const h2 = admin.getByRole("table", { name: "Open documents on h2" });
  await expect(h2.getByRole("row").filter({ hasText: title })).toContainText(name, { timeout: 20_000 });
  for (const home of ["h1", "h3"]) await expect(admin.getByText(new RegExp(`^${home}: \\d+ open, \\d+ stored, log `))).toBeVisible();
  await expect(admin.getByText(/did not answer/)).toHaveCount(0);
  await shot(admin, "j8-admin");
  await admin.setViewportSize({ width: 390, height: 844 });
  await shot(admin, "j8-admin-phone");
  await admin.setViewportSize({ width: 1280, height: 760 });

  const users = admin.getByRole("table", { name: "Users" });
  await expect(users.getByRole("row").filter({ hasText: "dev" }).first()).toContainText("Operator");
  await users.getByRole("button", { name: `Disable ${name}` }).click();
  await expect(users.getByRole("row").filter({ hasText: name })).toContainText("Disabled");
  await expect(page).toHaveURL(/\/login\?next=/);
  expect((await user.request.post("/api/login", { data: { name, password: `${name}-pw` } })).status()).toBe(401);
  await users.getByRole("button", { name: `Enable ${name}` }).click();
  await expect(users.getByRole("row").filter({ hasText: name })).toContainText("Active");
  await signIn(user, name);
  await dev.close();
  await user.close();
});

test("a published document is read at /p/<id> on any web node with no sign-in, until it is unpublished", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const id = await newDoc(ann, "# Release notes\n\nVersion 2 is out.\n", ["h3"]);
  expect((await ann.request.put(`/api/docs/${id}/share`, { data: { published: true } })).ok()).toBe(true);
  const reader = await browser.newContext();
  const page = await reader.newPage();
  await page.goto(`/p/${id}`);
  await expect(page.getByRole("heading", { name: "Release notes" })).toBeVisible();
  await expect(page.getByText("Version 2 is out.")).toBeVisible();
  await shot(page, "published");
  expect((await ann.request.put(`/api/docs/${id}/share`, { data: { published: false } })).ok()).toBe(true);
  const later = await browser.newContext();
  const gone = await later.newPage();
  await gone.goto(`/p/${id}`);
  await expect(gone.getByRole("heading", { name: "This page is not published" })).toBeVisible();
  await ann.close();
  await reader.close();
  await later.close();
});

test("axon's console on h1 shows every node", async ({ browser }) => {
  const console = process.env.MDPAD_CONSOLE;
  test.skip(!console, "MDPAD_CONSOLE is not set");
  const page = await (await browser.newContext()).newPage();
  await page.goto(console!);
  for (const node of ["h1", "h2", "h3", "w1"]) await expect(page.getByText(node, { exact: true }).first()).toBeVisible({ timeout: 30_000 });
  await shot(page, "j8-console");
});
