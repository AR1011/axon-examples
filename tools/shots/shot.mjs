// Takes an example's README screenshots from a plan, against a running instance of it.
//
//   node tools/shots/shot.mjs <plan.mjs> [--base URL] [--out DIR] [--only a,b]
//
// The plan is an ES module (examples/<ex>/docs/shots.mjs) whose default export is
//   { base, out, login(page, ctx), shots: [{ name, path, wait, steps(page, ctx), phone, dark, full, clip }] }
//   base   the URL the paths are under (--base wins; so does SHOTS_BASE)
//   out    where the PNGs go, relative to the repository (default: the plan's directory/img)
//   login  run once per browser context before its first shot (each theme and width has a context)
//   name   the file: <out>/<name>.png      path   opened with page.goto(base + path) before the steps
//   wait   a selector that must be visible before the shot      steps   anything else, with Playwright's page
//   phone  390 x 844 in place of 1280 x 800      dark   prefers-color-scheme: dark      full   the whole page
//   clip   a selector: the shot is of that element only
// ctx is { base, env, sh(cmd) }: sh runs a shell command where this script runs and returns its output.
// Each PNG must be at most SHOTS_MAX_KB (250) KB: a larger one fails the run (take a clip or a smaller page).
import { execSync } from "node:child_process";
import { statSync, mkdirSync } from "node:fs";
import { dirname, resolve, relative } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { chromium } from "playwright";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const args = process.argv.slice(2);
const flag = (name) => {
  const i = args.indexOf(name);
  return i < 0 ? undefined : args.splice(i, 2)[1];
};
const baseArg = flag("--base"), outArg = flag("--out"), only = flag("--only")?.split(",");
if (args.length !== 1) {
  console.error("usage: node tools/shots/shot.mjs <plan.mjs> [--base URL] [--out DIR] [--only a,b]");
  process.exit(2);
}
const planPath = resolve(args[0]);
const plan = (await import(pathToFileURL(planPath).href)).default;
const base = (baseArg ?? process.env.SHOTS_BASE ?? plan.base ?? "").replace(/\/?$/, "/");
const out = resolve(ROOT, outArg ?? plan.out ?? relative(ROOT, resolve(dirname(planPath), "img")));
const maxKb = Number(process.env.SHOTS_MAX_KB ?? 250);
const DESKTOP = { width: 1280, height: 800 }, PHONE = { width: 390, height: 844 };
const ctx = { base, env: process.env, sh: (cmd) => execSync(cmd, { stdio: "pipe", timeout: 180_000 }).toString() };

mkdirSync(out, { recursive: true });
const browser = await chromium.launch();
const contexts = new Map();
const pageFor = async (shot) => {
  const key = `${shot.phone ? "phone" : "desktop"}-${shot.dark ? "dark" : "light"}`;
  if (!contexts.has(key)) {
    const context = await browser.newContext({
      viewport: shot.phone ? PHONE : DESKTOP,
      deviceScaleFactor: 1,
      colorScheme: shot.dark ? "dark" : "light",
      baseURL: base,
      reducedMotion: "reduce",
    });
    const page = await context.newPage();
    if (plan.login) await plan.login(page, ctx);
    contexts.set(key, page);
  }
  return contexts.get(key);
};

let failed = 0;
for (const shot of plan.shots) {
  if (only && !only.includes(shot.name)) continue;
  const page = await pageFor(shot);
  try {
    if (shot.path !== undefined) await page.goto(base + shot.path.replace(/^\//, ""));
    if (shot.steps) await shot.steps(page, ctx);
    if (shot.wait) await page.locator(shot.wait).first().waitFor({ state: "visible", timeout: 30_000 });
    await page.evaluate(() => document.fonts.ready);
    const { width, height } = page.viewportSize();
    await page.mouse.move(width / 2, height - 2); // no hover state in the shot
    await page.waitForTimeout(shot.settle ?? 600);
    const file = `${out}/${shot.name}.png`;
    const opts = { path: file, animations: "disabled", caret: "hide", fullPage: !!shot.full };
    if (shot.clip) await page.locator(shot.clip).first().screenshot(opts);
    else await page.screenshot(opts);
    const kb = Math.round(statSync(file).size / 1024);
    if (kb > maxKb) {
      console.error(`${relative(ROOT, file)}: ${kb} KB, over ${maxKb} KB: take a clip or a smaller page`);
      failed++;
    } else console.log(`${relative(ROOT, file)}: ${kb} KB`);
  } catch (e) {
    console.error(`${shot.name}: ${e.message.split("\n")[0]}`);
    failed++;
  }
}
await browser.close();
process.exit(failed ? 1 : 0);
