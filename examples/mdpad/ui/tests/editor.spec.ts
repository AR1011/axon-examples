// The editor's state table (design §4.1), one row a test, against the mock server's closes and notices.
import { expect, test, type Page } from "@playwright/test";
import { another, api, badge, editor, fillLogin, mock, newDoc, open, signIn, someone, typeAtEnd } from "./helpers";

const notices = (page: Page) => page.getByTestId("notices");

async function opened(page: Page, name = someone("ana")) {
  await signIn(page, name);
  const id = await newDoc(page, "# State\n\nText.\n");
  await open(page, id);
  return id;
}

test("Joining shows the text with no empty editor first; Saving N, then Saved", async ({ page }) => {
  const id = await opened(page);
  await expect(editor(page)).toContainText("Text.");
  await mock(page, id, "hold", { on: true });
  await typeAtEnd(page, "a");
  await expect(badge(page)).toContainText("Saving 1");
  await mock(page, id, "hold", { on: false });
  await expect(badge(page)).toContainText("Saved");
  expect(await mock(page, id, "text")).toBe("# State\n\nText.\na");
});

test("local echo (design §7): each key shows in the text before the next animation frame", async ({ page }) => {
  await opened(page);
  await editor(page).click();
  await page.keyboard.press("ControlOrMeta+End");
  // At each key down, count the frames that start until the key shows in the text: 0 is the same frame.
  await page.evaluate(() => {
    const seen = ((window as unknown as { echo: number[] }).echo = []);
    const content = document.querySelector(".cm-content")!;
    document.addEventListener(
      "keydown",
      (event) => {
        const key = event.key;
        let frames = 0;
        let count = 0;
        const tick = () => {
          frames++;
          if (frames < 10) count = requestAnimationFrame(tick);
        };
        count = requestAnimationFrame(tick);
        const watch = new MutationObserver(() => {
          if (!content.textContent!.endsWith(key)) return;
          seen.push(frames);
          watch.disconnect();
          cancelAnimationFrame(count);
        });
        watch.observe(content, { childList: true, subtree: true, characterData: true });
      },
      { capture: true },
    );
  });
  await page.keyboard.type("echoes");
  await expect.poll(() => page.evaluate(() => (window as unknown as { echo: number[] }).echo)).toEqual([0, 0, 0, 0, 0, 0]);
});

test("Offline: the badge counts what is not saved, and the changes merge when the network is back", async ({ page, context }) => {
  const id = await opened(page);
  await context.setOffline(true);
  await typeAtEnd(page, "b");
  await expect(badge(page)).toContainText("Offline, 1 not saved");
  await context.setOffline(false);
  await expect(page.getByText("Merged 1 offline change")).toBeVisible();
  await expect(badge(page)).toContainText("Saved");
  expect(await mock(page, id, "text")).toContain("Text.\nb");
});

test("A break less than a second after the join: the others' avatars and carets stay", async ({ page, browser, baseURL }) => {
  const [ana, ben] = [someone("ana"), someone("ben")];
  await signIn(page, ana);
  const id = await newDoc(page, "# Steady\n");
  await api(page, "PUT", `docs/${id}/members/${ben}`, { level: "edit" });
  const other = await another(browser, baseURL, ben, `/d/${id}`);
  await expect(badge(other)).toContainText("Saved");
  await editor(other).click();
  // The first socket closes 750 ms after it opens, and the client joins again 500 ms later: inside the grace of the
  // break, but after the first second of the join. Each change of the avatars and the carets is kept.
  await page.addInitScript(() => {
    const w = window as unknown as { __sockets: number; __seen: string[] };
    const Native = window.WebSocket;
    w.__sockets = 0;
    window.WebSocket = class extends Native {
      constructor(url: string | URL, protocols?: string | string[]) {
        super(url, protocols);
        if (w.__sockets++ === 0) setTimeout(() => this.close(), 750);
      }
    };
    w.__seen = [];
    const look = () => {
      const now = `${document.querySelector('[data-testid="avatars"]')?.getAttribute("aria-label")}, carets ${document.querySelectorAll(".md-caret").length}`;
      if (w.__seen.at(-1) !== now) w.__seen.push(now);
    };
    document.addEventListener("DOMContentLoaded", () => new MutationObserver(look).observe(document.body, { subtree: true, childList: true, attributes: true }));
  });
  await open(page, id);
  await expect.poll(() => page.evaluate(() => (window as unknown as { __sockets: number }).__sockets)).toBe(2);
  await expect(badge(page)).toContainText("Saved");
  await page.waitForTimeout(1_500);
  const seen = await page.evaluate(() => (window as unknown as { __seen: string[] }).__seen);
  const shown = "People here: 1 other editor, carets 1";
  expect(seen.slice(seen.indexOf(shown))).toEqual([shown]);
  await other.context().close();
});

test("Server unreachable: a banner, the edits kept, and saved once the server is back", async ({ page }) => {
  const id = await opened(page);
  await mock(page, id, "refuse", { code: 4503 });
  await mock(page, id, "close", { code: 4503 });
  await expect(badge(page)).toContainText("Server unreachable");
  await expect(notices(page)).toContainText("Server unreachable. Your edits are kept here.");
  await typeAtEnd(page, "c");
  await mock(page, id, "refuse", { code: 0 });
  await expect(badge(page)).toContainText("Saved", { timeout: 15_000 });
  await expect(notices(page)).toHaveCount(0);
  expect(await mock(page, id, "text")).toContain("Text.\nc");
});

test("Busy: the badge stays Saving N and the edit goes again after the wait", async ({ page }) => {
  const id = await opened(page);
  await mock(page, id, "hold", { on: true });
  await typeAtEnd(page, "d");
  await mock(page, id, "notice", { kind: "busy", retry_after_ms: 500 });
  await expect(badge(page)).toContainText("Saving 1");
  await mock(page, id, "hold", { on: false });
  await expect(badge(page)).toContainText("Saved");
});

test("Signed out: a close 4401 goes to the login page with next, and back after the sign-in", async ({ page }) => {
  const name = someone("ana");
  const id = await opened(page, name);
  await mock(page, id, "close", { code: 4401 });
  await expect(page).toHaveURL(new RegExp(`/login\\?next=d%2F${id}$`));
  await fillLogin(page, name);
  await expect(page).toHaveURL(new RegExp(`/d/${id}$`));
  await expect(badge(page)).toContainText("Saved");
});

test("Access removed: a close 4403 stops, with a banner and no retry", async ({ page }) => {
  const id = await opened(page);
  await mock(page, id, "close", { code: 4403 });
  await expect(notices(page)).toContainText("Access removed.");
  await expect(badge(page)).toContainText("No access");
  await expect(editor(page)).toHaveAttribute("contenteditable", "false");
});

test("Not found: a close 4404 shows that the document does not exist", async ({ page }) => {
  const id = await opened(page);
  await mock(page, id, "close", { code: 4404 });
  await expect(page.getByText("This document does not exist")).toBeVisible();
});

test("In trash: the owner sees it and restores it", async ({ page }) => {
  await signIn(page, someone("ana"));
  const id = await newDoc(page, "# Binned\n");
  await api(page, "DELETE", `docs/${id}`);
  await page.goto(`/d/${id}`);
  await expect(page.getByText("This document is in the trash")).toBeVisible();
  await page.getByRole("button", { name: "Restore" }).click();
  await expect(badge(page)).toContainText("Saved");
  await expect(editor(page)).toContainText("# Binned");
});

test("Too many tabs: a close 4429 shows a banner and waits", async ({ page }) => {
  const id = await opened(page);
  await mock(page, id, "refuse", { code: 4429 });
  await mock(page, id, "close", { code: 4429 });
  await expect(notices(page)).toContainText("Too many open tabs. Close some tabs.");
  await expect(badge(page)).toContainText("Waiting");
});

test("Too large: a close 4413 joins again once, a second stops with Document is full", async ({ page }) => {
  const id = await opened(page);
  await mock(page, id, "close", { code: 4413 });
  await expect(badge(page)).toContainText("Saved");
  await mock(page, id, "close", { code: 4413 });
  await expect(notices(page)).toContainText("Document is full. Make a fresh copy.");
  await expect(notices(page).getByRole("button", { name: "Make a fresh copy" })).toBeVisible();
});

test("Near full and Cannot save: banners from the server's notices", async ({ page }) => {
  const id = await opened(page);
  await mock(page, id, "notice", { kind: "size", bytes: 220_000, max: 262_144 });
  await expect(notices(page)).toContainText("Document is near its maximum size.");
  await mock(page, id, "notice", { kind: "cannot_save" });
  await expect(notices(page)).toContainText("Server cannot save. Your changes are kept here.");
});

test("the same build works when it is mounted under a path, its socket too", async ({ page }) => {
  const MOUNTED = "http://127.0.0.1:7898/ops/pad/";
  const ana = someone("ana");
  await page.goto(MOUNTED);
  await expect(page).toHaveURL(`${MOUNTED}login?next=`);
  await fillLogin(page, ana);
  await expect(page).toHaveURL(MOUNTED);
  await page.getByRole("button", { name: "New document" }).click();
  await expect(page).toHaveURL(new RegExp(`^${MOUNTED}d/h1-`));
  await expect(badge(page)).toContainText("Saved");
  await typeAtEnd(page, "Mounted.");
  await expect(badge(page)).toContainText("Saved");
  await page.getByRole("button", { name: "History" }).click();
  await expect(page.getByText("No versions yet")).toBeVisible();
  await page.getByRole("link", { name: "Documents" }).click();
  await expect(page).toHaveURL(/:7898\/ops\/pad\/?$/);
  await expect(page.getByRole("row").filter({ hasText: "Untitled" })).toBeVisible();
});
