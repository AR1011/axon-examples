// The list, sharing, invites, guests, history and import on a real cluster (design §3 journeys 1, 2, 4 and 7).
import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
import { badge, editor, newDoc, open, serverText, shot, signIn, typeAtEnd } from "./helpers";

const PLAN = "# Plan\n\nIntro.\n";

/** A user name no run before used: the accounts stay between runs of the suite on one cluster. */
const fresh = (prefix: string) => `${prefix}-${Date.now().toString(36)}${Math.floor(Math.random() * 1e4)}`;

/** The operator's `mdpad user invite`: the path of the link it prints. */
function invite(name: string): string {
  const command = process.env.MDPAD_INVITE_CMD;
  if (!command) throw new Error("MDPAD_INVITE_CMD is not set");
  const { link } = JSON.parse(execSync(command.replaceAll("{name}", name), { encoding: "utf8" })) as { link: string };
  return new URL(link).pathname;
}

async function shareLink(page: Page, id: string): Promise<string> {
  const answer = await page.request.get(`/api/docs/${id}/share`);
  expect(answer.ok()).toBe(true);
  return `/s/${((await answer.json()) as { token: string }).token}`;
}

test("journey 1: the empty list, a new document, its heading the title in the list", async ({ browser }) => {
  test.skip(!process.env.MDPAD_INVITE_CMD, "MDPAD_INVITE_CMD is not set");
  const ana = fresh("ana");
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(invite(ana));
  await page.getByLabel("Password", { exact: true }).fill(`${ana}-pw`);
  await page.getByLabel("Password again").fill(`${ana}-pw`);
  await page.getByRole("button", { name: "Set password" }).click();
  await expect(page.getByText("No documents")).toBeVisible();
  await expect(page.getByText("Create one or import a .md file.")).toBeVisible();
  await shot(page, "j1-empty");
  await page.getByRole("button", { name: "New document" }).click();
  await expect(editor(page)).toContainText("# Untitled");
  await expect(badge(page)).toHaveText("Saved");
  await editor(page).click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText("# Plan for May\n\nIntro.\n");
  await expect(badge(page)).toHaveText("Saved");
  await page.getByRole("link", { name: "Documents" }).click();
  const row = page.getByRole("row").filter({ hasText: "Plan for May" });
  await expect(row).toContainText("Owner");
  await shot(page, "j1-list");
  await context.close();
});

test("journey 2: an edit link, the invite, Join, co-editing, and the member shown via link", async ({ browser }) => {
  test.skip(!process.env.MDPAD_INVITE_CMD, "MDPAD_INVITE_CMD is not set");
  const [ann, bo] = [await browser.newContext(), await browser.newContext()];
  await signIn(ann, "ann");
  const id = await newDoc(ann, PLAN);
  const page = await ann.newPage();
  await open(page, id);
  await page.getByRole("button", { name: "Share" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByRole("radio", { name: "Can edit" }).click();
  await expect(dialog.getByRole("button", { name: "Reset link" })).toBeVisible();
  const link = await shareLink(page, id);
  await expect(dialog).toContainText(link);
  await shot(page, "j2-share");
  await dialog.getByRole("button", { name: "Close" }).click();

  // Bo has no account: the login page says so, and the operator's invite makes one.
  const name = fresh("bo");
  const other = await bo.newPage();
  await other.goto(link);
  await expect(other.getByText("You need an account. Ask your operator.")).toBeVisible();
  await shot(other, "j2-need-account");
  await other.goto(invite(name));
  await expect(other.getByText(`You are ${name}.`)).toBeVisible();
  await shot(other, "j2-invite");
  await other.getByLabel("Password", { exact: true }).fill(`${name}-pw`);
  await other.getByLabel("Password again").fill(`${name}-pw`);
  await other.getByRole("button", { name: "Set password" }).click();
  await expect(other.getByRole("heading", { name: "Documents", exact: true })).toBeVisible();
  await other.goto(link);
  await expect(other.getByText("The link lets you edit it.")).toBeVisible();
  await shot(other, "j2-join");
  await other.getByRole("button", { name: "Join" }).click();
  await expect(other).toHaveURL(new RegExp(`/d/${id}$`));
  await expect(badge(other)).toHaveText("Saved");

  // A second tab of Bo's: Ann sees him once.
  const second = await bo.newPage();
  await open(second, id);
  await typeAtEnd(other, "Bo was here.");
  await typeAtEnd(page, "\nAnn too.");
  await expect.poll(() => serverText(ann, id)).toMatch(/Bo was here\.[^]*Ann too\.|Ann too\.[^]*Bo was here\./);
  for (const p of [page, other, second]) {
    await expect(editor(p)).toContainText("Bo was here.");
    await expect(editor(p)).toContainText("Ann too.");
  }
  await expect(page.getByTestId("avatars")).toHaveAttribute("aria-label", "People here: 1 other editor");

  await page.getByRole("button", { name: "Share" }).click();
  const row = page.getByRole("dialog").getByRole("row").filter({ hasText: name });
  await expect(row).toContainText("via link");
  await expect(row.getByRole("combobox")).toContainText("Can edit");
  await shot(page, "j2-people");
  await other.getByRole("link", { name: "Documents" }).click();
  await other.getByRole("tab", { name: "Shared with me" }).click();
  await expect(other.getByRole("row").filter({ hasText: "Plan" })).toContainText("Edit");
  await shot(other, "j2-shared-with-me");
  await ann.close();
  await bo.close();
});

test("journeys 4 and 7: a guest reads live; restore while Ben is offline keeps his sentence, exactly; link off closes the guest", async ({ browser }) => {
  const [ann, ben, cleo] = [await browser.newContext(), await browser.newContext(), await browser.newContext()];
  await signIn(ann, "ann");
  await signIn(ben, "ben");
  const id = await newDoc(ann, PLAN);
  const a = await ann.newPage();
  await open(a, id);

  // 10:40: a named version of the plan.
  await a.getByRole("button", { name: "History" }).click();
  const history = a.getByTestId("history");
  await history.getByRole("button", { name: "Name this version" }).click();
  await a.getByLabel("Name", { exact: true }).fill("10:40");
  await a.getByRole("button", { name: "Save version" }).click();
  await expect(history.getByRole("button", { name: /10:40/ })).toContainText("Named");

  // Ben joins by the edit link; then the link is for viewing, and Cleo opens it as a guest.
  expect((await ann.request.put(`/api/docs/${id}/share`, { data: { link: "edit" } })).ok()).toBe(true);
  const token = (await shareLink(a, id)).slice(3);
  expect((await ben.request.post(`/api/s/${token}/join`)).ok()).toBe(true);
  expect((await ann.request.put(`/api/docs/${id}/share`, { data: { link: "view" } })).ok()).toBe(true);
  const c = await cleo.newPage();
  await c.goto(`/s/${token}`);
  await expect(c).toHaveURL(new RegExp(`/g/${token}$`));
  await shot(c, "j4-guest");
  await c.getByLabel("Your name").fill("Cleo");
  await c.getByRole("button", { name: "Open" }).click();
  await expect(badge(c)).toHaveText("Read-only");
  await expect(editor(c)).toHaveAttribute("contenteditable", "false");

  // 10:50: the draft. Everyone sees it live.
  await typeAtEnd(a, "## Draft\nOld idea.\n");
  const b = await ben.newPage();
  await open(b, id);
  await expect(editor(b)).toContainText("Old idea.");
  await expect(editor(c)).toContainText("Old idea.");
  await expect.poll(() => serverText(ann, id)).toBe(`${PLAN}## Draft\nOld idea.\n`);

  // 10:55: Ben is offline and types after "Old idea.".
  await ben.setOffline(true);
  await expect(badge(b)).toHaveText("Offline");
  await editor(b).click();
  await b.keyboard.press("ControlOrMeta+End");
  await b.keyboard.press("ArrowLeft");
  await b.keyboard.insertText(" Better idea.");
  await expect(badge(b)).toHaveText("Offline, 1 not saved");

  // 11:00: Ann restores 10:40 after looking at the changes.
  await history.getByRole("button", { name: /10:40/ }).click();
  const version = a.getByTestId("version");
  await version.getByRole("tab", { name: "Changes" }).click();
  await expect(version.getByTestId("changes").locator(".cm-deletedChunk")).toContainText("Old idea.");
  await shot(a, "j7-changes");
  await version.getByRole("button", { name: "Restore" }).click();
  await shot(a, "j7-restore-ask");
  await a.getByRole("alertdialog").getByRole("button", { name: "Restore" }).click();
  await expect(a.getByText(/Restored\. The text before it is version \d+\./)).toBeVisible();
  await expect.poll(() => serverText(ann, id)).toBe(PLAN);
  await expect(editor(c)).not.toContainText("Old idea.");
  await expect(history.getByRole("button").filter({ hasText: "Before restore" })).toBeVisible();
  await shot(a, "j7-restored");

  // Ben is back: his sentence stays, the draft does not come back.
  await ben.setOffline(false);
  await expect(b.getByText("Merged 1 offline change")).toBeVisible();
  await expect(badge(b)).toHaveText("Saved");
  await expect.poll(() => serverText(ann, id)).toBe("# Plan\n\nIntro.\n Better idea.");
  await expect(editor(a)).toContainText("Better idea.");
  await expect(editor(a)).not.toContainText("Old idea.");
  await shot(b, "j7-ben-merged");

  // The link off: Cleo stops at once; Ben, a member via link, stays until Ann removes him.
  await a.getByRole("button", { name: "Share" }).click();
  const dialog = a.getByRole("dialog");
  await dialog.getByRole("radio", { name: "Off" }).click();
  await expect(c.getByTestId("notices")).toContainText("Access removed.");
  await expect(badge(c)).toHaveText("No access");
  await shot(c, "j7-guest-removed");
  await expect(badge(b)).toHaveText("Saved");
  const row = dialog.getByRole("row").filter({ hasText: "ben" });
  await expect(row).toContainText("via link");
  await row.getByRole("button", { name: "Remove ben" }).click();
  await expect(b.getByTestId("notices")).toContainText("Access removed.");
  await ann.close();
  await ben.close();
  await cleo.close();
});

test("import, export, a fresh copy, the size meter, trash and remove from my list", async ({ browser }) => {
  const [ann, ben] = [await browser.newContext(), await browser.newContext()];
  await signIn(ann, "ann");
  await signIn(ben, "ben");
  const page = await ann.newPage();
  await page.goto("/");
  await page.getByLabel("Markdown file").setInputFiles({ name: "notes.md", mimeType: "text/markdown", buffer: Buffer.from("# Imported notes\n\nLine.\n") });
  await expect(page).toHaveURL(/\/d\//);
  await expect(editor(page)).toContainText("Line.");
  await expect(badge(page)).toHaveText("Saved");
  const id = page.url().split("/d/")[1];
  const answer = await ann.request.put(`/api/docs/${id}/members/ben`, { data: { level: "view" } });
  expect(answer.ok()).toBe(true);

  // Replace the text with a file: the text before is a version.
  await page.getByRole("button", { name: "More" }).click();
  const chooser = page.waitForEvent("filechooser");
  await page.getByRole("menuitem", { name: "Import (replace)" }).click();
  await (await chooser).setFiles({ name: "draft.md", mimeType: "text/markdown", buffer: Buffer.from("# Imported notes\n\nSecond draft.\n") });
  await page.getByRole("alertdialog").getByRole("button", { name: "Replace" }).click();
  await expect(page.getByText(/Imported\. The text before it is version \d+\./)).toBeVisible();
  await expect(editor(page)).toContainText("Second draft.");
  expect(await serverText(ann, id)).toBe("# Imported notes\n\nSecond draft.\n");

  await badge(page).hover();
  await expect(page.getByRole("meter", { name: "Size" })).toBeVisible();
  await shot(page, "size-meter");

  await page.getByRole("button", { name: "More" }).click();
  const download = page.waitForEvent("download");
  await page.getByRole("menuitem", { name: "Export .md" }).click();
  const file = await download;
  expect(file.suggestedFilename()).toBe("Imported notes.md");
  expect(readFileSync(await file.path(), "utf8")).toBe("# Imported notes\n\nSecond draft.\n");

  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Make a fresh copy" }).click();
  await expect(page).not.toHaveURL(new RegExp(`/d/${id}$`));
  await expect(editor(page)).toContainText("Second draft.");
  const copy = page.url().split("/d/")[1];
  const people = (await (await ann.request.get(`/api/docs/${copy}/share`)).json()) as { members: { user: string }[] };
  expect(people.members.map((m) => m.user)).toEqual(["ben"]);

  // The copy goes to the trash, back, and then for good.
  await page.getByRole("link", { name: "Documents" }).click();
  const actions = page.getByRole("row").filter({ hasText: "Imported notes" }).first().getByRole("button", { name: "Actions of Imported notes" });
  await expect(page.getByRole("row").filter({ hasText: "Imported notes" })).toHaveCount(2);
  await actions.click();
  await page.getByRole("menuitem", { name: "Move to trash" }).click();
  await expect(page.getByRole("row").filter({ hasText: "Imported notes" })).toHaveCount(1);
  await page.getByRole("tab", { name: "Trash" }).click();
  await expect(page.getByRole("row").filter({ hasText: "Imported notes" })).toHaveCount(1);
  await shot(page, "trash");
  await page.getByRole("button", { name: "Actions of Imported notes" }).click();
  await page.getByRole("menuitem", { name: "Delete for good" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Delete for good" }).click();
  await expect(page.getByRole("row").filter({ hasText: "Imported notes" })).toHaveCount(0);

  // Ben takes the original off his list; Ann keeps it.
  const other = await ben.newPage();
  await other.goto("/?tab=shared");
  const row = other.getByRole("row").filter({ hasText: "Imported notes" });
  await expect(row).toContainText("View");
  await row.getByRole("button", { name: "Actions of Imported notes" }).click();
  await other.getByRole("menuitem", { name: "Remove from my list" }).click();
  await expect(row).toHaveCount(0);
  await page.getByRole("tab", { name: "Mine" }).click();
  await expect(page.getByRole("row").filter({ hasText: "Imported notes" })).toHaveCount(1);
  await ann.close();
  await ben.close();
});
