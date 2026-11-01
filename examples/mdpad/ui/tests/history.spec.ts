import { expect, test } from "@playwright/test";
import { another, api, badge, editor, mock, newDoc, open, signIn, someone } from "./helpers";

const PLAN = "# Plan\n\nIntro.\n";
const DRAFT = "# Plan\n\nIntro.\n## Draft\nOld idea.\n";

test("journey 7: history shows the versions, a version's text and its changes; restore asks, and everyone sees it", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, PLAN);
  await api(page, "PUT", `docs/${id}/members/${ben}`, { level: "edit" });
  await open(page, id);
  await page.getByRole("button", { name: "History" }).click();
  const history = page.getByTestId("history");
  await expect(history.getByText("No versions yet")).toBeVisible();
  await history.getByRole("button", { name: "Name this version" }).click();
  await page.getByLabel("Name", { exact: true }).fill("10:40");
  await page.getByRole("button", { name: "Save version" }).click();
  await expect(page.getByText("Saved version 10:40")).toBeVisible();
  const named = history.getByRole("button", { name: /10:40/ });
  await expect(named).toContainText("Named");
  await expect(named).toContainText("+15 B");

  const other = await another(browser, baseURL, ben, `/d/${id}`);
  await expect(badge(other)).toContainText("Saved");
  await mock(page, id, "type", { user: ana, text: "## Draft\nOld idea.\n" });
  await expect(editor(page)).toContainText("Old idea.");

  await named.click();
  const version = page.getByTestId("version");
  await expect(version.getByTestId("version-rendered").getByRole("heading", { name: "Plan" })).toBeVisible();
  await expect(version.getByTestId("version-rendered")).not.toContainText("Old idea.");
  await expect(editor(page)).toBeHidden();
  await version.getByRole("tab", { name: "Changes" }).click();
  await expect(version.getByTestId("changes").locator(".cm-deletedChunk")).toContainText("Old idea.");

  await version.getByRole("button", { name: "Restore" }).click();
  const ask = page.getByRole("alertdialog");
  await expect(ask).toContainText("Restore 10:40?");
  await ask.getByRole("button", { name: "Restore" }).click();
  await expect(page.getByText(/Restored\. The text before it is version 2\./)).toBeVisible();
  await expect(editor(page)).toBeVisible();
  await expect(editor(page)).not.toContainText("Old idea.");
  await expect(editor(other)).not.toContainText("Old idea.");
  await expect(other.getByText("Restored version 1")).toBeVisible();
  expect(await mock(page, id, "text")).toBe(PLAN);

  const before = history.getByRole("button").filter({ hasText: "Before restore" });
  await expect(before).toContainText(`${ana}`);
  await before.click();
  await expect(page.getByTestId("version-rendered")).toContainText("Old idea.");
  await page.getByRole("button", { name: "Back to the text" }).click();
  await page.getByRole("button", { name: "Close history" }).click();
  await expect(history).toHaveCount(0);
  await expect(page.getByTestId("preview-pane")).toBeVisible();
});

test("a view member reads the history but cannot restore or name a version", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, PLAN);
  await page.request.put(`/api/docs/${id}/markdown`, { headers: { "content-type": "text/markdown" }, data: DRAFT });
  await api(page, "PUT", `docs/${id}/members/${ben}`, { level: "view" });
  const other = await another(browser, baseURL, ben, `/d/${id}`);
  await expect(badge(other)).toContainText("Read-only");
  await other.getByRole("button", { name: "History" }).click();
  const history = other.getByTestId("history");
  await expect(history.getByRole("button", { name: "Name this version" })).toHaveCount(0);
  await history.getByRole("button").filter({ hasText: "Before import" }).click();
  await expect(other.getByTestId("version-rendered")).toContainText("Intro.");
  await expect(other.getByTestId("version").getByRole("button", { name: "Restore" })).toHaveCount(0);
});

test("Import (replace) asks, keeps the text before as a version, and the editor shows the file", async ({ page }) => {
  await signIn(page, someone("ana"));
  const id = await newDoc(page, PLAN);
  await open(page, id);
  await page.getByRole("button", { name: "More" }).click();
  const chooser = page.waitForEvent("filechooser");
  await page.getByRole("menuitem", { name: "Import (replace)" }).click();
  await (await chooser).setFiles({ name: "draft.md", mimeType: "text/markdown", buffer: Buffer.from(DRAFT) });
  const ask = page.getByRole("alertdialog");
  await expect(ask).toContainText("Replace the text with draft.md?");
  await ask.getByRole("button", { name: "Replace" }).click();
  await expect(page.getByText("Imported. The text before it is version 1.")).toBeVisible();
  await expect(editor(page)).toContainText("Old idea.");
  await page.getByRole("button", { name: "History" }).click();
  await expect(page.getByTestId("history").getByRole("button").filter({ hasText: "Before import" })).toBeVisible();
});

test("a fresh copy opens a new document with the text and the same people", async ({ page }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, DRAFT);
  await api(page, "PUT", `docs/${id}/members/${ben}`, { level: "view" });
  await open(page, id);
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Make a fresh copy" }).click();
  await expect(page).not.toHaveURL(new RegExp(`/d/${id}$`));
  await expect(editor(page)).toContainText("Old idea.");
  const copy = page.url().split("/d/")[1];
  const share = await api<{ members: { user: string }[] }>(page, "GET", `docs/${copy}/share`);
  expect(share.members.map((m) => m.user)).toEqual([ben]);
});

test("the badge's tooltip shows the size against the maximum", async ({ page }) => {
  await signIn(page, someone("ana"));
  const id = await newDoc(page, PLAN);
  await mock(page, id, "max", { max: 1000 });
  await open(page, id);
  await badge(page).hover();
  const meter = page.getByRole("meter", { name: "Size" });
  await expect(meter).toBeVisible();
  await expect(meter).toHaveAttribute("aria-valuenow", /^1\.5/);
  await expect(page.getByText("15 B of 1000 B")).toBeVisible();
});
