// The should surfaces of design §4.1: the Publish switch and /p/<id>, the admin page, the outline and scroll sync.
import { expect, test, type Page } from "@playwright/test";
import { another, api, editor, newDoc, open, signIn, someone } from "./helpers";

test("publish: anyone reads /p/<id> rendered, with nothing in it that runs, until it is unpublished", async ({ page, browser, baseURL }) => {
  const ana = someone("ana");
  await signIn(page, ana);
  const id = await newDoc(page, '# Notes for all\n\nRead <img src=x onerror="window.ran=1"> and [this](javascript:window.ran=1).\n\n<script>window.ran=1</script>\n');
  await open(page, id);
  await page.getByRole("button", { name: "Share" }).click();
  const dialog = page.getByRole("dialog", { name: "Share" });
  const publish = dialog.getByRole("switch", { name: "Publish" });
  await expect(publish).not.toBeChecked();
  await expect(dialog.getByText(`/p/${id}`)).toHaveCount(0);
  await publish.click();
  await expect(publish).toBeChecked();
  await expect(dialog).toContainText(`/p/${id}`);
  await expect(dialog.getByRole("radio", { name: "Off" })).toBeChecked();

  const reader = await another(browser, baseURL);
  const dialogs: string[] = [];
  reader.on("dialog", (d) => void dialogs.push(d.message()));
  await reader.goto(`/p/${id}`);
  await expect(reader.getByRole("heading", { name: "Notes for all" })).toBeVisible();
  await expect(reader).toHaveTitle("Notes for all - mdpad");
  await expect(reader.getByRole("textbox")).toHaveCount(0);
  expect(await reader.evaluate(() => (window as { ran?: number }).ran)).toBeUndefined();
  await expect(reader.locator("article").locator('[href^="javascript"], [onerror], script')).toHaveCount(0);
  expect(dialogs).toEqual([]);

  await publish.click();
  await expect(publish).not.toBeChecked();
  // The reader's browser may keep the page 10 s (private, max-age=10): a new reader gets the 404.
  const later = await another(browser, baseURL);
  await later.goto(`/p/${id}`);
  await expect(later.getByRole("heading", { name: "This page is not published" })).toBeVisible();
});

test("a member sees Publish read-only; the editor's More menu opens the published page", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, "# Shared and published\n");
  await api(page, "PUT", `docs/${id}/members/${ben}`, { level: "edit" });
  await api(page, "PUT", `docs/${id}/share`, { published: true });
  const other = await another(browser, baseURL, ben, `/d/${id}`);
  await other.getByRole("button", { name: "Share" }).click();
  await expect(other.getByRole("dialog").getByRole("switch", { name: "Publish" })).toBeDisabled();
  await expect(other.getByRole("dialog").getByRole("switch", { name: "Publish" })).toBeChecked();
  await other.getByRole("dialog").getByRole("button", { name: "Close" }).click();

  await open(page, id);
  await page.getByRole("button", { name: "More" }).click();
  const opened = page.waitForEvent("popup");
  await page.getByRole("menuitem", { name: "Open published page" }).click();
  const published = await opened;
  await expect(published).toHaveURL(new RegExp(`/p/${id}$`));
  await expect(published.getByRole("heading", { name: "Shared and published" })).toBeVisible();
});

test("the admin page is for operators only, and lists, invites, disables and enables users", async ({ page, browser, baseURL }) => {
  const [ana, op, gus] = [someone("ana"), someone("op"), someone("gus")];
  await signIn(page, ana);
  await page.getByRole("button", { name: "Account" }).click();
  await expect(page.getByRole("menuitem", { name: "Change password" })).toBeVisible();
  await expect(page.getByRole("menuitem", { name: "Admin" })).toHaveCount(0);
  await page.keyboard.press("Escape");
  await page.goto("/admin");
  await expect(page.getByRole("heading", { name: "Operators only" })).toBeVisible();
  const id = await newDoc(page, `# Notes of ${ana}\n`);
  await open(page, id);

  const admin = await another(browser, baseURL, op);
  await admin.getByRole("button", { name: "Account" }).click();
  await admin.getByRole("menuitem", { name: "Admin" }).click();
  await expect(admin).toHaveURL(/\/admin$/);
  await expect(admin.getByRole("heading", { name: "Admin" })).toBeVisible();
  const users = admin.getByRole("table", { name: "Users" });
  await expect(users.getByRole("row").filter({ hasText: ana })).toContainText("Active");
  await expect(users.getByRole("row").filter({ hasText: op })).toContainText("Operator");
  await expect(users.getByRole("row").filter({ hasText: op }).getByRole("button")).toHaveCount(0);

  await admin.getByLabel("New user").fill(gus);
  await admin.getByRole("button", { name: "Invite" }).click();
  await expect(admin.getByText(`/i/inv-${gus}`)).toBeVisible();
  await admin.getByLabel("New user").fill(ana);
  await admin.getByRole("button", { name: "Invite" }).click();
  await expect(admin.getByRole("alert")).toContainText(`${ana} exists.`);

  const homes = admin.getByRole("table", { name: "Open documents on h1" });
  await expect(homes.getByRole("row").filter({ hasText: `Notes of ${ana}` })).toContainText(ana);
  await expect(admin.getByText("h2 did not answer: The node did not answer.")).toBeVisible();

  await users.getByRole("row").filter({ hasText: ana }).getByRole("button", { name: `Disable ${ana}` }).click();
  await expect(users.getByRole("row").filter({ hasText: ana })).toContainText("Disabled");
  await expect(page).toHaveURL(/\/login\?next=/);
  await users.getByRole("row").filter({ hasText: ana }).getByRole("button", { name: `Enable ${ana}` }).click();
  await expect(users.getByRole("row").filter({ hasText: ana })).toContainText("Active");
});

test("an operator gives a document a new owner; the owner before stays as an edit member", async ({ page, browser, baseURL }) => {
  const [ana, ben, op] = [someone("ana"), someone("ben"), someone("op")];
  await another(browser, baseURL, ben);
  await signIn(page, ana);
  const id = await newDoc(page, "# Handover\n");
  const admin = await another(browser, baseURL, op, "/admin");
  await admin.getByLabel("Document id").fill(id);
  await admin.getByLabel("New owner").fill(ben);
  await admin.getByRole("button", { name: "Change owner" }).click();
  await expect(admin.getByText(`${id} is ${ben}'s now.`)).toBeVisible();
  const share = await api<{ owner: string; members: { user: string; level: string }[] }>(page, "GET", `docs/${id}/share`);
  expect(share.owner).toBe(ben);
  expect(share.members).toEqual([{ user: ana, level: "edit", via: "invited" }]);
  await admin.getByLabel("Document id").fill(id);
  await admin.getByLabel("New owner").fill("nobody-here");
  await admin.getByRole("button", { name: "Change owner" }).click();
  await expect(admin.getByRole("alert")).toContainText("No such user.");
});

/** A text of `n` sections, each a heading and some lines, long enough to scroll. */
const sections = (n: number) => Array.from({ length: n }, (_, i) => `## Part ${i + 1}\n\n${Array.from({ length: 8 }, (_, j) => `Line ${j + 1} of part ${i + 1}.`).join("\n\n")}\n`).join("\n");

async function both(page: Page, markdown: string) {
  await signIn(page, someone("ana"));
  const id = await newDoc(page, markdown);
  await open(page, id);
  await page.getByRole("tab", { name: "Both" }).click();
  return id;
}

test("the outline lists the headings, jumps to one, and marks the one in view", async ({ page }) => {
  await both(page, `# Book\n\n${sections(12)}`);
  await page.getByRole("button", { name: "Outline" }).click();
  const outline = page.getByRole("navigation", { name: "Outline" });
  await expect(outline.getByRole("link")).toHaveCount(13);
  await outline.getByRole("link", { name: "Part 9" }).click();
  const heading = page.getByTestId("preview").getByRole("heading", { name: "Part 9" });
  await expect(heading).toBeInViewport();
  await expect(outline.getByRole("link", { name: "Part 9" })).toHaveAttribute("aria-current", "true");
  // The editor follows: the heading's line is in view.
  await expect(editor(page).getByText("## Part 9")).toBeInViewport();
  // The outline follows the text.
  await editor(page).click();
  await page.keyboard.press("ControlOrMeta+End");
  await page.keyboard.insertText("\n\n## Last words\n");
  await expect(outline.getByRole("link", { name: "Last words" })).toBeVisible();
  // It stays open after a reload.
  await page.reload();
  await expect(page.getByRole("navigation", { name: "Outline" })).toBeVisible();
});

/** The text of the first block whose bottom is below the top of `selector`'s box, of those `blocks` picks. */
const topBlock = (page: Page, selector: string, blocks: string) =>
  page.evaluate(
    ([selector, blocks]) => {
      const box = document.querySelector(selector)!;
      const top = box.getBoundingClientRect().top + 4;
      const all = [...box.querySelectorAll<HTMLElement>(blocks)].filter((b) => b.textContent!.trim() !== "");
      return (all.find((b) => b.getBoundingClientRect().bottom > top)?.textContent ?? "").replace(/^#+ /, "");
    },
    [selector, blocks],
  );

test("scroll sync: the preview follows the editor, and the editor the preview", async ({ page }) => {
  await both(page, `# Long\n\n${sections(20)}`);
  const scroller = page.locator(".cm-scroller");
  const pane = page.getByTestId("preview-pane");
  await scroller.hover();
  await page.mouse.wheel(0, 2400);
  await expect.poll(() => pane.evaluate((el) => el.scrollTop)).toBeGreaterThan(1000);
  const atTop = await topBlock(page, ".cm-scroller", ".cm-line");
  expect(atTop).toMatch(/of part|Part/);
  await expect.poll(() => topBlock(page, '[data-testid="preview-pane"]', "[data-line]")).toBe(atTop);

  // Back: a scroll of the preview moves the editor to the same block. A person takes a moment to move to the preview.
  await page.waitForTimeout(300);
  await pane.hover();
  await page.mouse.wheel(0, -1000);
  await expect.poll(() => scroller.evaluate((el) => el.scrollTop)).toBeLessThan(2000);
  const previewTop = await topBlock(page, '[data-testid="preview-pane"]', "[data-line]");
  await expect.poll(() => topBlock(page, ".cm-scroller", ".cm-line")).toBe(previewTop);
});
