import { expect, type Locator, type Page } from "@playwright/test";

/** The mock's password (mock/api.mjs). Each sign-in gets a world of its own, so tests do not meet. */
export const PASSWORD = "wf";

export async function signIn(page: Page, to = "/") {
  await page.goto(to);
  await page.getByLabel("Password").fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).not.toHaveURL(/\/login/);
}

/** Picks an option of a select of the console. It is a listbox, not the browser's own select. */
export async function choose(page: Page, label: string, option: string | RegExp, within: Page | Locator = page) {
  await within.getByRole("combobox", { name: label }).click();
  await page.getByRole("option", { name: option, exact: true }).click();
}

/** The sum of the layout shifts while `during` runs, as the browser counts them (CLS without the windows). */
export async function shifts(page: Page, during: () => Promise<void>): Promise<number> {
  await page.evaluate(() => {
    const w = window as unknown as { shift: number; moved: string[] };
    w.shift = 0;
    w.moved = [];
    type Rect = { x: number; y: number; width: number; height: number };
    type Shift = { value: number; hadRecentInput: boolean; sources: { node?: Node; previousRect: Rect; currentRect: Rect }[] };
    const at = (r: Rect) => `${Math.round(r.x)},${Math.round(r.y)} ${Math.round(r.width)}x${Math.round(r.height)}`;
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries() as unknown as Shift[]) {
        if (entry.hadRecentInput) continue;
        w.shift += entry.value;
        const notices = [...document.querySelectorAll("main [role=status], main [role=alert]")].map((n) => n.textContent);
        w.moved.push(`${entry.value.toFixed(4)} with notices [${notices.join(" | ")}]:`);
        for (const { node, previousRect, currentRect } of entry.sources) {
          w.moved.push(`${node?.nodeName ?? "?"} "${(node?.textContent ?? "").slice(0, 24)}" ${at(previousRect)} to ${at(currentRect)}`);
        }
      }
    }).observe({ type: "layout-shift", buffered: false });
  });
  await during();
  return page.evaluate(() => (window as unknown as { shift: number }).shift);
}

/** What moved in the last `shifts`: each node's name and the start of its text. */
export const moved = (page: Page) => page.evaluate(() => (window as unknown as { moved: string[] }).moved.slice(0, 24).join("; "));

/** The page is no wider than the window: nothing scrolls sideways but the tables, inside their cards. */
export async function fits(page: Page) {
  const [scroll, width, main, inside] = await page.evaluate(() => {
    const main = document.querySelector("main")!;
    return [document.documentElement.scrollWidth, window.innerWidth, main.scrollWidth, main.clientWidth];
  });
  expect(scroll, "the page scrolls sideways").toBeLessThanOrEqual(width);
  expect(main, "the content scrolls sideways").toBeLessThanOrEqual(inside);
}

/** The sidebar's link to a page. */
export const nav = (page: Page, name: string) => page.getByRole("navigation", { name: "Sidebar" }).getByRole("link", { name, exact: true });

/** The page's own header: its title, the run's status, its actions. */
export const head = (page: Page) => page.locator("header").filter({ has: page.locator("h1") });
