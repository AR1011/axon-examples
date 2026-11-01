// A target of design §7 that only a browser measures: a cold open of 100,000 characters shows text in under 1 s.
// A release build's number, so it runs only with MDPAD_MEASURE=1 (the gate's live run is a debug build).
import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import { newDoc, signIn } from "./helpers";

test("cold open: 100,000 characters, its home just started and a browser with nothing kept, shows text in < 1 s", async ({ browser }) => {
  const [kill, start] = [process.env.MDPAD_KILL_CMD, process.env.MDPAD_START_CMD];
  test.skip(!process.env.MDPAD_MEASURE || !kill || !start, "MDPAD_MEASURE, MDPAD_KILL_CMD or MDPAD_START_CMD is not set");
  const ann = await browser.newContext();
  await signIn(ann, "ann");
  const line = "A line of the long text, to make it one hundred thousand characters.\n";
  const id = await newDoc(ann, `# Cold open\n\n${line.repeat(Math.ceil(100_000 / line.length))}`, ["h2"]);
  // The home starts again, so the document is not loaded on the server.
  execSync(kill!.replaceAll("{home}", "h2"), { stdio: "inherit" });
  execSync(start!.replaceAll("{home}", "h2"), { stdio: "inherit" });
  await ann.close();

  const reader = await browser.newContext();
  await signIn(reader, "ann");
  const page = await reader.newPage();
  // When the editor first shows the text, from the start of the navigation.
  await page.addInitScript(() => {
    const watch = new MutationObserver(() => {
      if (!document.querySelector(".cm-content")?.textContent?.includes("Cold open")) return;
      (window as unknown as { shown: number }).shown = performance.now();
      watch.disconnect();
    });
    watch.observe(document, { childList: true, subtree: true, characterData: true });
  });
  await page.goto(`/d/${id}`);
  const shown = await page.waitForFunction(() => (window as unknown as { shown?: number }).shown, null, { timeout: 30_000 });
  const ms = (await shown.jsonValue()) as number;
  console.log(`cold open of ${id}: text shown ${ms.toFixed(0)} ms after the navigation started`);
  test.info().annotations.push({ type: "cold open ms", description: ms.toFixed(0) });
  expect(ms).toBeLessThan(1000);
  await reader.close();
});
