// The README's screenshots: node tools/shots/shot.mjs examples/workflows/docs/shots.mjs --base http://127.0.0.1:<h1>/
// Against a cluster of scripts/run.sh with some load on it, from the repository root on the same machine, with
// WF_PORT, WF_DIR and WF_BIN as the cluster was started with: the shot of the move kills an engine and starts it again.
const PASSWORD = process.env.WF_PASSWORD ?? "wf-dev";
const TAG = Date.now().toString(36);
const ORDER = { sku: "mug", qty: 1, account: "acct-readme", parcels: [{ to: "Oslo" }, { to: "Bergen" }] };
const RUN = "examples/workflows/scripts/run.sh";

const start = async (page, id, input) => {
  const r = await page.request.post("api/runs", { data: { workflow: "checkout", id, input } });
  if (!r.ok()) throw new Error(`start ${id}: ${r.status()} ${await r.text()}`);
};
const timeline = (page) => page.getByRole("region", { name: "Timeline" });
const waiting = (page) => timeline(page).getByRole("row").filter({ hasText: "signal approve" }).filter({ hasText: "waiting" });

export default {
  async login(page) {
    await page.goto("login");
    await page.getByLabel("Password").fill(PASSWORD);
    await page.getByRole("button", { name: "Sign in" }).click();
    await page.waitForURL((url) => !url.pathname.endsWith("/login"));
  },
  shots: [
    { name: "overview", path: "overview", settle: 2000, wait: "h1" },
    { name: "runs", path: "runs", wait: "table" },
    {
      name: "run-failed",
      async steps(page) {
        const id = `readme-${TAG}-declined`;
        await start(page, id, { ...ORDER, account: "declined" });
        await page.goto(`runs/${id}`);
        await page.getByRole("status").filter({ hasText: "card_declined" }).waitFor({ timeout: 30_000 });
        await timeline(page).getByRole("button", { name: /activity release/ }).waitFor();
      },
    },
    { name: "queues", path: "queues", wait: "table" },
    { name: "cluster", path: "cluster", wait: "table" },
    {
      name: "run-moved",
      async steps(page, ctx) {
        const id = `readme-${TAG}-move`;
        await start(page, id, ORDER);
        await page.goto(`runs/${id}`);
        await waiting(page).waitFor({ timeout: 30_000 });
        const { owner } = await (await page.request.get(`api/runs/${encodeURIComponent(id)}`)).json();
        const other = owner === "e1" ? "e2" : "e1";
        ctx.sh(`${RUN} kill ${owner}`);
        try {
          // The dead engine's lease runs out, and the next read opens the run on the other engine.
          await timeline(page).getByText(`moved to ${other}`).waitFor({ timeout: 60_000 });
          await page.getByRole("region", { name: "Summary" }).getByText(other).first().waitFor();
          await waiting(page).waitFor();
        } finally {
          ctx.sh(`${RUN} start ${owner}`);
        }
      },
    },
  ],
};
