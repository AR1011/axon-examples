// The README's screenshots: node tools/shots/shot.mjs examples/kv/docs/shots.mjs --base http://127.0.0.1:<console>/
// Against `kv console` in front of three durable nodes, with `kv check`'s load started a minute or two before.
// KV_LIVE is a script that takes `kill n3` for that cluster (scripts/run.sh does, with its own ports). The shot
// "overview" kills n3 and leaves it down for the last one: start it again after the run.
const LIVE = process.env.KV_LIVE ?? "examples/kv/scripts/run.sh";

const onSurvivors = async (page) => {
  for (let waited = 0; ; waited += 500) {
    const { shards } = await (await page.request.get("api/cluster")).json();
    if (shards.every((s) => s.hosts.length === 1 && s.hosts[0].node !== "n3")) return;
    if (waited > 60_000) throw new Error("n3's shards did not move within 60 s");
    await page.waitForTimeout(500);
  }
};

export default {
  shots: [
    {
      name: "shards",
      path: "shards",
      async steps(page) {
        await page.getByRole("grid", { name: "Shards by number" }).getByRole("button").nth(255).waitFor();
      },
    },
    {
      name: "overview",
      settle: 2500,
      async steps(page, ctx) {
        ctx.sh(`${LIVE} kill n3`);
        // n3's leases end, and its shards start on n1 and n2; then a few seconds of load on the two.
        await onSurvivors(page);
        await page.waitForTimeout(15_000);
        await page.goto(ctx.base);
        await page.getByRole("region", { name: "Operations per second" }).locator("canvas").first().waitFor();
      },
    },
    {
      name: "shards-down",
      path: "shards",
      async steps(page) {
        await page.getByRole("grid", { name: "Shards by number" }).getByRole("button").nth(255).waitFor();
      },
    },
  ],
};
