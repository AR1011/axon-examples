// Journey 3: a late joiner opens a 60,000-character document while 12 tabs type in it, and sees text, never an empty editor.
import { expect, test } from "@playwright/test";
import { badge, editor, newDoc, open, shot, signIn } from "./helpers";

test("journey 3: a late joiner never sees an empty editor", async ({ browser }) => {
  test.setTimeout(180_000);
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const line = "Some words of a long document, typed by many people at once. ";
  const id = await newDoc(ann, `# Big\n\n${Array.from({ length: 980 }, (_, i) => `${i} ${line}`).join("\n")}\n`);
  await ann.request.put(`/api/docs/${id}/members/ben`, { data: { level: "edit" } });
  const tabs = [];
  for (let i = 0; i < 12; i++) {
    const page = await ann.newPage();
    await open(page, id);
    tabs.push(page);
  }
  const typing = tabs.map(async (page, i) => {
    for (let k = 0; k < 20; k++) {
      await editor(page).click();
      await page.keyboard.insertText(`t${i}.${k} `);
      await page.waitForTimeout(150);
    }
  });
  const ben = await browser.newContext();
  await signIn(ben, "ben");
  const late = await ben.newPage();
  const frames: string[] = [];
  await late.exposeFunction("seen", (what: string) => frames.push(what));
  await late.addInitScript(() => {
    const look = () => {
      const text = document.querySelector(".cm-content")?.textContent ?? "";
      const loading = !!document.querySelector('[data-testid="loading"]');
      (window as unknown as { seen: (s: string) => void }).seen(loading ? "loading" : text.length === 0 ? (document.querySelector(".cm-content") ? "empty" : "none") : "text");
    };
    new MutationObserver(look).observe(document, { subtree: true, childList: true, characterData: true });
  });
  const started = Date.now();
  await late.goto(`/d/${id}`);
  await expect(badge(late)).toHaveText("Saved");
  const ms = Date.now() - started;
  await shot(late, "j3-late-joiner");
  await Promise.all(typing);
  console.log(JSON.stringify({ chars: (await editor(late).textContent())?.length, ms, empty: frames.includes("empty"), kinds: [...new Set(frames)] }));
  expect(frames).not.toContain("empty");
  await ann.close();
  await ben.close();
});
