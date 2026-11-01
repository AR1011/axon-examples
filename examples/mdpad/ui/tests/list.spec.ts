import { expect, test } from "@playwright/test";
import { another, api, badge, editor, newDoc, signIn, someone } from "./helpers";

test("journey 1: an empty list, a new document whose heading is its title in the list", async ({ page }) => {
  await signIn(page, someone("ana"));
  await expect(page.getByText("No documents")).toBeVisible();
  await expect(page.getByText("Create one or import a .md file.")).toBeVisible();
  await page.getByRole("button", { name: "New document" }).click();
  await expect(page).toHaveURL(/\/d\/h1-/);
  await expect(editor(page)).toContainText("# Untitled");
  await expect(badge(page)).toContainText("Saved");
  await editor(page).click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText("# Plan for May\n\nIntro.\n");
  await expect(badge(page)).toContainText("Saved");
  await expect(page.getByTestId("title")).toHaveText("Plan for May");
  await page.getByRole("link", { name: "Documents" }).click();
  const row = page.getByRole("row").filter({ hasText: "Plan for May" });
  await expect(row).toContainText("Owner");
  await expect(row).toContainText(/by ana-/);
  await expect(row).toContainText("23 B");
});

test("a shared document is under Shared with me, and its member removes it from their list", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, "# Shared notes\n");
  await api(page, "PUT", `docs/${id}/members/${ben}`, { level: "view" });
  const other = await another(browser, baseURL, ben);
  await expect(other.getByText("No documents")).toBeVisible();
  await other.getByRole("tab", { name: "Shared with me" }).click();
  await expect(other).toHaveURL(/tab=shared/);
  const row = other.getByRole("row").filter({ hasText: "Shared notes" });
  await expect(row).toContainText("View");
  await row.getByRole("button", { name: "Actions of Shared notes" }).click();
  await expect(other.getByRole("menuitem", { name: "Move to trash" })).toHaveCount(0);
  await other.getByRole("menuitem", { name: "Remove from my list" }).click();
  await expect(other.getByText("Shared notes removed from your list")).toBeVisible();
  await expect(row).toHaveCount(0);
  // The owner's list keeps it.
  await page.reload();
  await expect(page.getByRole("row").filter({ hasText: "Shared notes" })).toBeVisible();
});

test("trash: move, restore, and delete for good after a question", async ({ page }) => {
  await signIn(page, someone("ana"));
  await newDoc(page, "# Old draft\n");
  await newDoc(page, "# Keep me\n");
  await page.reload();
  const actions = (title: string) => page.getByRole("button", { name: `Actions of ${title}` });
  await actions("Old draft").click();
  await page.getByRole("menuitem", { name: "Move to trash" }).click();
  await expect(page.getByText("Old draft moved to trash")).toBeVisible();
  await expect(page.getByRole("row").filter({ hasText: "Old draft" })).toHaveCount(0);

  await page.getByRole("tab", { name: "Trash" }).click();
  await expect(page.getByRole("columnheader", { name: "Moved to trash" })).toBeVisible();
  await expect(page.getByRole("row").filter({ hasText: "Keep me" })).toHaveCount(0);
  await actions("Old draft").click();
  await page.getByRole("menuitem", { name: "Restore" }).click();
  await expect(page.getByText("The trash is empty")).toBeVisible();
  await page.getByRole("tab", { name: "Mine" }).click();
  await actions("Old draft").click();
  await page.getByRole("menuitem", { name: "Move to trash" }).click();

  await page.getByRole("tab", { name: "Trash" }).click();
  await actions("Old draft").click();
  await page.getByRole("menuitem", { name: "Delete for good" }).click();
  const ask = page.getByRole("alertdialog");
  await expect(ask).toContainText("Delete Old draft for good?");
  await expect(ask).toContainText("You cannot undo this.");
  await ask.getByRole("button", { name: "Delete for good" }).click();
  await expect(page.getByText("Old draft deleted")).toBeVisible();
  await expect(page.getByText("The trash is empty")).toBeVisible();
});

test("import a .md file as a new document, export it, copy its link, and filter by title", async ({ page }) => {
  await signIn(page, someone("ana"));
  await newDoc(page, "# Other\n");
  await page.reload();
  await page.getByLabel("Markdown file").setInputFiles({ name: "notes.md", mimeType: "text/markdown", buffer: Buffer.from("# Imported notes\n\nLine.\n") });
  await expect(page).toHaveURL(/\/d\/h1-/);
  await expect(editor(page)).toContainText("Line.");
  await page.getByRole("link", { name: "Documents" }).click();

  await page.getByLabel("Filter by title").fill("import");
  await expect(page.getByRole("row")).toHaveCount(2);
  await page.getByLabel("Filter by title").fill("nothing like it");
  await expect(page.getByText("No documents with this title")).toBeVisible();
  await page.getByLabel("Filter by title").fill("");

  await page.getByRole("button", { name: "Actions of Imported notes" }).click();
  const download = page.waitForEvent("download");
  await page.getByRole("menuitem", { name: "Export .md" }).click();
  const file = await download;
  expect(file.suggestedFilename()).toBe("Imported notes.md");
  const chunks: Buffer[] = [];
  for await (const chunk of await file.createReadStream()) chunks.push(chunk as Buffer);
  expect(Buffer.concat(chunks).toString()).toBe("# Imported notes\n\nLine.\n");

  await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.getByRole("button", { name: "Actions of Imported notes" }).click();
  await page.getByRole("menuitem", { name: "Copy link" }).click();
  await expect(page.getByText("Link copied")).toBeVisible();
  expect(await page.evaluate(() => navigator.clipboard.readText())).toMatch(/^http:\/\/127\.0\.0\.1:7899\/d\/h1-/);
});

test("a file over the import limit is refused before it is sent", async ({ page }) => {
  await signIn(page, someone("ana"));
  await page.getByLabel("Markdown file").setInputFiles({ name: "big.md", mimeType: "text/markdown", buffer: Buffer.alloc(129 * 1024, "a") });
  await expect(page.getByRole("alert")).toContainText("The file is larger than 128 KB.");
  await expect(page).not.toHaveURL(/\/d\//);
});

test("100 rows a page, and the next page from the cursor", async ({ page }) => {
  await signIn(page, someone("ana"));
  for (let i = 0; i < 101; i++) await newDoc(page, `# Doc ${i}\n`);
  await page.reload();
  await expect(page.getByRole("row")).toHaveCount(101);
  // Newest first: the first made is the one on page 2.
  await expect(page.getByRole("row").filter({ hasText: "Doc 0" })).toHaveCount(0);
  await page.getByRole("button", { name: /next/i }).click();
  await expect(page.getByRole("row")).toHaveCount(2);
  await expect(page.getByRole("row").filter({ hasText: "Doc 0" })).toBeVisible();
  await page.getByRole("button", { name: /previous/i }).click();
  await expect(page.getByRole("row")).toHaveCount(101);
});

test("the account menu changes the password, which signs out the other sign-ins, and signs out", async ({ page, browser, baseURL }) => {
  const ana = someone("ana");
  await signIn(page, ana);
  const other = await another(browser, baseURL, ana);
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Change password" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Current password").fill("wrong-one");
  await dialog.getByLabel("New password", { exact: true }).fill("a-new-secret");
  await dialog.getByLabel("New password again").fill("a-new-secret");
  await dialog.getByRole("button", { name: "Change password" }).click();
  await expect(dialog.getByRole("alert")).toContainText("The current password is wrong.");
  await dialog.getByLabel("Current password").fill(`pw-${ana}`);
  await dialog.getByRole("button", { name: "Change password" }).click();
  await expect(page.getByText("Password changed. Your other sign-ins stopped.")).toBeVisible();

  await other.reload();
  await expect(other).toHaveURL(/\/login/);
  await page.reload();
  await expect(page.getByRole("heading", { name: "Documents", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Sign out" }).click();
  await expect(page).toHaveURL(/\/login/);
});

test("sign-out asks first when a document has changes the server does not have", async ({ page, context }) => {
  const ana = someone("ana");
  await signIn(page, ana);
  const id = await newDoc(page, "# Draft\n");
  await page.goto(`/d/${id}`);
  await expect(badge(page)).toContainText("Saved");
  await context.setOffline(true);
  await editor(page).click();
  await page.keyboard.press("ControlOrMeta+End");
  await page.keyboard.insertText("more");
  await expect(badge(page)).toContainText("Offline, 1 not saved");
  await page.getByRole("link", { name: "Documents" }).click();
  await expect(page.getByRole("alert")).toContainText("Cannot connect to the server.");
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Sign out" }).click();
  const ask = page.getByRole("alertdialog");
  await expect(ask).toContainText("1 document has unsaved changes. Sign out anyway?");
  await ask.getByRole("button", { name: "Cancel" }).click();
  await expect(ask).toHaveCount(0);
});
