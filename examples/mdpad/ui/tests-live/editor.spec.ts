// The editor page on a real cluster (design §8, ui/tests-live).
import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";
import { badge, editor, newDoc, open, serverText, share, shot, signIn, typeAtEnd } from "./helpers";

test("two people co-edit, with emoji and CJK; one user in two tabs is one avatar; cursors show and fade", async ({ browser }) => {
  const [ann, ben] = [await browser.newContext(), await browser.newContext()];
  await signIn(ann, "ann");
  await signIn(ben, "ben");
  const id = await newDoc(ann, "# Plan\n\nIntro.");
  await share(ann, id, "ben", "edit");
  const [a, a2, b] = [await ann.newPage(), await ann.newPage(), await ben.newPage()];
  await open(a, id);
  await open(a2, id);
  await open(b, id);

  await typeAtEnd(a, "\nAnn 👋 says 你好.");
  await expect(editor(b)).toContainText("Ann 👋 says 你好.");
  await typeAtEnd(b, "\nBen ✨ says こんにちは.");
  await expect(editor(a)).toContainText("Ben ✨ says こんにちは.");
  await expect(badge(a)).toHaveText("Saved");
  await expect(badge(b)).toHaveText("Saved");
  await expect.poll(() => serverText(ann, id)).toBe("# Plan\n\nIntro.\nAnn 👋 says 你好.\nBen ✨ says こんにちは.");
  await expect(a.getByTestId("preview")).toContainText("Ben ✨ says こんにちは.");

  // Ann has two tabs: Ben sees her once, and she sees only Ben.
  await expect(b.getByTestId("avatars")).toHaveAttribute("aria-label", "People here: 1 other editor");
  await expect(a.getByTestId("avatars")).toHaveAttribute("aria-label", "People here: 1 other editor");
  await b.getByTestId("avatars").click();
  await expect(b.getByRole("dialog").getByText("ann", { exact: true })).toBeVisible();
  await b.keyboard.press("Escape");

  // Ben's caret shows in Ann's editor with his name, which fades 1.5 s after it moved.
  await b.locator(".cm-line").first().click();
  const caret = a.locator('.md-caret[data-name="ben"]');
  await expect(caret).toHaveClass(/md-caret-recent/);
  await shot(a, "co-edit");
  await expect(caret).not.toHaveClass(/md-caret-recent/, { timeout: 4_000 });
  await expect(caret).toBeAttached();
  await ann.close();
  await ben.close();
});

test("a view member cannot type", async ({ browser }) => {
  const [ann, ben] = [await browser.newContext(), await browser.newContext()];
  await signIn(ann, "ann");
  await signIn(ben, "ben");
  const id = await newDoc(ann, "# Read me\n");
  await share(ann, id, "ben", "view");
  const page = await ben.newPage();
  await open(page, id);
  await expect(badge(page)).toHaveText("Read-only");
  await expect(page.locator('[data-notice="view"]')).toContainText("You can only read this document.");
  await expect(editor(page)).toHaveAttribute("contenteditable", "false");
  await editor(page).click();
  await page.keyboard.insertText("not mine");
  await expect(editor(page)).not.toContainText("not mine");
  expect(await serverText(ann, id)).toBe("# Read me\n");

  // A viewer shows as a count, to the others only.
  const owner = await ann.newPage();
  await open(owner, id);
  await expect(owner.getByTestId("avatars")).toHaveAttribute("aria-label", "People here: 1 viewer");
  await expect(page.getByTestId("avatars")).toHaveAttribute("aria-label", "People here: 1 other editor");
  await owner.getByTestId("avatars").click();
  await expect(owner.getByRole("dialog")).toContainText("1 viewer");
  await shot(owner, "viewers");
  await ann.close();
  await ben.close();
});

test("offline: typing, a reload with no network, then the merge on reconnect", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const id = await newDoc(ann, "# Train");
  const page = await ann.newPage();
  await open(page, id);
  // The service worker keeps the page's files once it controls the page.
  await page.waitForFunction(() => navigator.serviceWorker?.controller != null, null, { timeout: 15_000 });

  await ann.setOffline(true);
  await expect(badge(page)).toHaveText("Offline");
  await typeAtEnd(page, "a");
  await typeAtEnd(page, "b");
  await typeAtEnd(page, "c");
  await expect(badge(page)).toHaveText("Offline, 3 not saved");
  await shot(page, "offline");
  // The copy is written to IndexedDB as each change is made; a moment for the last write.
  await page.waitForTimeout(500);
  await page.reload();
  await expect(editor(page)).toContainText("# Trainabc");
  await expect(badge(page)).toHaveText("Offline, 3 not saved");

  await ann.setOffline(false);
  await expect(page.getByText("Merged 3 offline changes")).toBeVisible();
  await expect(badge(page)).toHaveText("Saved");
  expect(await serverText(ann, id)).toBe("# Trainabc");
  await ann.close();
});

test("a session that ends while offline: sign in with next=, and the edits are still there", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const id = await newDoc(ann, "# Cookie");
  const page = await ann.newPage();
  await open(page, id);
  await ann.setOffline(true);
  await typeAtEnd(page, " kept");
  await expect(badge(page)).toHaveText("Offline, 1 not saved");
  await ann.clearCookies();
  await page.waitForTimeout(500);
  await ann.setOffline(false);

  await page.waitForURL(/\/login\?next=/);
  await page.getByLabel("Name").fill("ann");
  await page.getByLabel("Password").fill("ann-pw");
  await page.getByRole("button", { name: "Sign in" }).click();
  await page.waitForURL(`**/d/${id}`);
  await expect(editor(page)).toContainText("# Cookie kept");
  await expect(badge(page)).toHaveText("Saved");
  expect(await serverText(ann, id)).toBe("# Cookie kept");
  await ann.close();
});

test("the home killed: the banner, typing goes on, a reload in the outage, and nothing saved is lost", async ({ browser }) => {
  const [kill, start] = [process.env.MDPAD_KILL_CMD, process.env.MDPAD_START_CMD];
  test.skip(!kill || !start, "MDPAD_KILL_CMD and MDPAD_START_CMD are not set");
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const id = await newDoc(ann, "# Outage", ["h2", "h3"]);
  const home = id.split("-")[0];
  const page = await ann.newPage();
  await open(page, id);
  await typeAtEnd(page, "\nbefore");
  await expect(badge(page)).toHaveText("Saved");

  execSync(kill!.replaceAll("{home}", home), { stdio: "inherit" });
  try {
    await expect(page.locator('[data-notice="unreachable"]')).toContainText("Server unreachable. Your edits are kept here.", { timeout: 40_000 });
    await expect(badge(page)).toHaveText("Server unreachable");
    await shot(page, "unreachable");
    await typeAtEnd(page, "\nduring");
    await page.waitForTimeout(500);
    await page.reload();
    await expect(editor(page)).toContainText("during");
  } finally {
    execSync(start!.replaceAll("{home}", home), { stdio: "inherit" });
  }
  await expect(badge(page)).toHaveText("Saved", { timeout: 60_000 });
  await expect(page.getByTestId("notices")).toHaveCount(0);
  expect(await serverText(ann, id)).toBe("# Outage\nbefore\nduring");
  await ann.close();
});

test("access removed: the banner, the download of unsaved edits, and no new connection", async ({ browser }) => {
  const [ann, ben] = [await browser.newContext(), await browser.newContext()];
  await signIn(ann, "ann");
  await signIn(ben, "ben");
  const id = await newDoc(ann, "# Shared");
  await share(ann, id, "ben", "edit");
  const page = await ben.newPage();
  await open(page, id);
  await ben.setOffline(true);
  await typeAtEnd(page, " by ben");
  const removed = await ann.request.delete(`/api/docs/${id}/members/ben`);
  expect(removed.ok()).toBe(true);
  let sockets = 0;
  page.on("websocket", () => sockets++);
  await ben.setOffline(false);

  await expect(page.locator('[data-notice="forbidden"]')).toContainText("Access removed.");
  await expect(badge(page)).toHaveText("No access");
  await expect(editor(page)).toHaveAttribute("contenteditable", "false");
  await shot(page, "access-removed");
  const [download] = await Promise.all([page.waitForEvent("download"), page.getByRole("button", { name: "Download your text (.md)" }).click()]);
  expect(download.suggestedFilename()).toBe("Shared by ben.md");
  expect(readFileSync(await download.path(), "utf8")).toBe("# Shared by ben");
  const after = sockets;
  await page.waitForTimeout(3_000);
  expect(sockets).toBe(after);
  await ann.close();
  await ben.close();
});

test("trash and purge while open: the trash, restore, then not found", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const id = await newDoc(ann, "# Bin\n");
  const page = await ann.newPage();
  await open(page, id);
  expect((await ann.request.delete(`/api/docs/${id}`)).ok()).toBe(true);
  await expect(page.getByText("This document is in the trash")).toBeVisible();
  await page.getByRole("button", { name: "Restore" }).click();
  await expect(badge(page)).toHaveText("Saved");
  await expect(editor(page)).toContainText("# Bin");

  expect((await ann.request.delete(`/api/docs/${id}`)).ok()).toBe(true);
  await expect(page.getByText("This document is in the trash")).toBeVisible();
  expect((await ann.request.delete(`/api/docs/${id}?purge=1`)).ok()).toBe(true);
  await page.reload();
  await expect(page.getByText("This document does not exist")).toBeVisible();
  await ann.close();
});

test("the preview runs nothing in the text", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const markdown = [
    "# XSS",
    '<img src="x" onerror="window.__xss = 1">',
    "<script>window.__xss = 2</script>",
    "[link](javascript:window.__xss=3)",
    '<a href="javascript:window.__xss=4">a</a> <svg><script>window.__xss = 5</script></svg>',
    "",
  ].join("\n\n");
  const id = await newDoc(ann, markdown);
  const page = await ann.newPage();
  await open(page, id);
  await expect(page.getByTestId("preview").locator("h1")).toHaveText("XSS");
  for (const link of await page.getByTestId("preview").locator("a").all()) await link.click({ force: true }).catch(() => undefined);
  await page.waitForTimeout(500);
  expect(await page.evaluate(() => (window as unknown as { __xss?: number }).__xss)).toBeUndefined();
  await expect(page.getByTestId("preview").locator("script, [onerror]")).toHaveCount(0);
  await ann.close();
});

test("the preview patches in place: no layout shift while it renders", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const blocks = Array.from({ length: 30 }, (_, i) => `## Part ${i}\n\nSome text of part ${i}, long enough to wrap in the preview pane once or twice.\n`);
  const id = await newDoc(ann, `# Long\n\n${blocks.join("\n")}`);
  const page = await ann.newPage();
  await open(page, id);
  const pane = page.getByTestId("preview-pane");
  await expect(pane.locator("h2").nth(29)).toHaveText("Part 29");
  await pane.evaluate((el) => {
    el.scrollTop = 400;
    (window as unknown as { __shifts: unknown[] }).__shifts = [];
    (el.querySelectorAll("h2")[5] as unknown as { __mark: boolean }).__mark = true;
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries() as unknown as { value: number; sources: { node?: Node }[] }[]) {
        if (entry.sources.some((s) => s.node && el.contains(s.node))) (window as unknown as { __shifts: unknown[] }).__shifts.push(entry.value);
      }
    }).observe({ type: "layout-shift" });
  });
  for (const word of ["one ", "two ", "three ", "four ", "five ", "six ", "seven ", "eight "]) {
    await typeAtEnd(page, word);
    await page.waitForTimeout(120);
  }
  await expect(pane.locator("p").last()).toContainText("eight");
  await page.waitForTimeout(700);
  // Typing at the end scrolls the editor there, and the preview follows it (scroll sync): to its end, not a jump back.
  const after = await pane.evaluate((el) => ({
    atEnd: Math.abs(el.scrollTop - (el.scrollHeight - el.clientHeight)) < 2,
    shifts: (window as unknown as { __shifts: number[] }).__shifts,
    kept: (el.querySelectorAll("h2")[5] as unknown as { __mark?: boolean }).__mark === true,
  }));
  expect(after).toEqual({ atEnd: true, shifts: [], kept: true });
  await ann.close();
});

test("a reconnect does not flicker: the badge, the banners and the avatars stay", async ({ browser }) => {
  const [ann, ben] = [await browser.newContext(), await browser.newContext()];
  await signIn(ann, "ann");
  await signIn(ben, "ben");
  const id = await newDoc(ann, "# Steady\n");
  await share(ann, id, "ben", "edit");
  // Keeps each socket the page opens, so the test can break one.
  await ann.addInitScript(() => {
    const Native = window.WebSocket;
    const kept: WebSocket[] = [];
    (window as unknown as { __sockets: WebSocket[] }).__sockets = kept;
    window.WebSocket = class extends Native {
      constructor(url: string | URL, protocols?: string | string[]) {
        super(url, protocols);
        kept.push(this);
      }
    };
  });
  const [page, other] = [await ann.newPage(), await ben.newPage()];
  await open(page, id);
  await open(other, id);
  await other.locator(".cm-line").first().click();
  await expect(page.getByTestId("avatars")).toHaveAttribute("aria-label", "People here: 1 other editor");
  await expect(page.locator('.md-caret[data-name="ben"]')).toBeAttached();

  await page.evaluate(() => {
    const seen = new Set<string>();
    (window as unknown as { __seen: Set<string> }).__seen = seen;
    const look = () => {
      seen.add(`badge:${document.querySelector('[data-testid="badge"]')?.textContent}`);
      seen.add(`avatars:${document.querySelector('[data-testid="avatars"]')?.getAttribute("aria-label")}`);
      seen.add(`carets:${document.querySelectorAll(".md-caret").length}`);
      if (document.querySelector('[data-testid="notices"]')) seen.add("notices");
      if (document.querySelector('[data-testid="loading"]')) seen.add("loading");
    };
    new MutationObserver(look).observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true });
    look();
  });
  const before = await page.evaluate(() => (window as unknown as { __sockets: WebSocket[] }).__sockets.length);
  await page.evaluate(() => (window as unknown as { __sockets: WebSocket[] }).__sockets.at(-1)!.close());
  await expect.poll(() => page.evaluate(() => (window as unknown as { __sockets: WebSocket[] }).__sockets.length)).toBe(before + 1);
  await page.waitForTimeout(2_500);
  await typeAtEnd(page, " still");
  await expect(editor(other)).toContainText("# Steady still");
  const seen = await page.evaluate(() => [...(window as unknown as { __seen: Set<string> }).__seen].sort());
  expect(seen).toEqual(["avatars:People here: 1 other editor", "badge:Saved", "carets:1"].concat(seen.includes("badge:Saving 1") ? ["badge:Saving 1"] : []).sort());
  await ann.close();
  await ben.close();
});

test("a phone: edit and view as tabs, and no sideways scroll", async ({ browser }) => {
  const ann = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 2 });
  await signIn(ann, "ann");
  const id = await newDoc(ann, "# On a phone\n\n- [x] a task\n- [ ] another\n\n```js\nconst x = 1;\n```\n");
  const page = await ann.newPage();
  await open(page, id);
  await expect(page.getByRole("tab")).toHaveText(["Edit", "View"]);
  await shot(page, "phone-edit");
  await page.getByRole("tab", { name: "View" }).click();
  await expect(page.getByTestId("preview").locator("input[type=checkbox]")).toHaveCount(2);
  await shot(page, "phone-view");
  expect(await page.evaluate(() => document.scrollingElement!.scrollWidth <= innerWidth)).toBe(true);
  // The header fits: History and Share are in the More menu, and the tabs do not scroll.
  await expect(page.getByRole("button", { name: /Scroll tabs/ })).toHaveCount(0);
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "History" }).click();
  await expect(page.getByTestId("history")).toBeVisible();
  await shot(page, "phone-history");
  await page.getByRole("button", { name: "Close history" }).click();
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Share" }).click();
  await expect(page.getByRole("dialog", { name: "Share" })).toBeVisible();
  await shot(page, "phone-share");
  await ann.close();
});

test("sign-out clears this user's copies in the browser", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const id = await newDoc(ann, "# Mine\n");
  const page = await ann.newPage();
  await open(page, id);
  const copies = () => page.evaluate(async () => (await indexedDB.databases()).map((db) => db.name).filter((name) => name?.startsWith("mdpad/ann/")));
  await expect.poll(copies).toEqual([`mdpad/ann/${id}`]);
  await page.getByRole("link", { name: "Documents" }).click();
  await expect(page.getByRole("tab", { name: "Mine" })).toBeVisible();
  await shot(page, "documents");
  await page.getByRole("button", { name: "Account" }).click();
  await page.getByRole("menuitem", { name: "Sign out" }).click();
  await page.waitForURL(/\/login/);
  expect(await copies()).toEqual([]);
  await ann.close();
});

test("a wrong path: not found, and the documents with no editor code loaded", async ({ browser }) => {
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const page = await ann.newPage();
  const scripts: string[] = [];
  page.on("request", (request) => void (request.resourceType() === "script" && scripts.push(new URL(request.url()).pathname)));
  await page.goto("/no/such/page");
  await expect(page.getByText("Page not found")).toBeVisible();
  await page.getByRole("link", { name: "Documents" }).click();
  await expect(page).toHaveURL(/\/$/);
  await expect(page.getByRole("heading", { name: "Documents", exact: true })).toBeVisible();
  expect(scripts.filter((path) => /\/(EditorPage|editor|preview)-/.test(path)), scripts.join(" ")).toEqual([]);
  await ann.close();
});
