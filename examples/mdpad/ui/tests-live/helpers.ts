import { expect, type BrowserContext, type Page } from "@playwright/test";

export { badge, editor, open, typeAtEnd } from "../tests/helpers";

// The live suites' helpers: each test signs in its own contexts and makes its own documents, so the order does not
// matter.

const SHOTS = !!process.env.SHOTS;

export async function signIn(context: BrowserContext, name: string, password = `${name}-pw`): Promise<void> {
  const answer = await context.request.post("/api/login", { data: { name, password } });
  expect(answer.ok(), await answer.text()).toBe(true);
}

/** A new document of `owner`'s with `markdown`, on one of `homes` (a document's id starts with its home). */
export async function newDoc(context: BrowserContext, markdown: string, homes = ["h1", "h2", "h3"]): Promise<string> {
  for (let i = 0; i < 30; i++) {
    const answer = await context.request.post("/api/docs", { data: { markdown } });
    expect(answer.ok(), await answer.text()).toBe(true);
    const { id } = (await answer.json()) as { id: string };
    if (homes.includes(id.split("-")[0])) return id;
  }
  throw new Error(`no document on ${homes}`);
}

export async function share(owner: BrowserContext, id: string, user: string, level: "edit" | "view"): Promise<void> {
  const answer = await owner.request.put(`/api/docs/${id}/members/${user}`, { data: { level } });
  expect(answer.ok(), await answer.text()).toBe(true);
}

export const serverText = async (context: BrowserContext, id: string) => (await context.request.get(`/api/docs/${id}/markdown`)).text();
export async function shot(page: Page, name: string): Promise<void> {
  if (!SHOTS) return;
  // A popover or a tab indicator still moving is not what the page shows. A paused animation never finishes: 2 s.
  await page.evaluate(() =>
    Promise.race([
      Promise.all(
        document
          .getAnimations()
          .filter((a) => a.effect?.getComputedTiming().iterations !== Infinity)
          .map((a) => a.finished.catch(() => undefined)),
      ),
      new Promise((done) => setTimeout(done, 2_000)),
    ]),
  );
  await page.screenshot({ path: `screenshots/${name}.png` });
}
