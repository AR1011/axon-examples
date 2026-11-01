// The README's screenshots: node tools/shots/shot.mjs examples/edge-workers/docs/shots.mjs --base http://127.0.0.1:<admin>/
// Against the console of c1 on a cluster of scripts/run.sh (admin on EDGE_PORT+3), with the workers hello (a canary
// of 10 %), json-api, feature-flag and counter (--object COUNTER=Counter) deployed, and `edge load` on them: GETs
// through g1 and POSTs to counters /room-0 to /room-19 through g2.
const PASSWORD = process.env.EDGE_PASSWORD ?? "edge-dev";

export default {
  async login(page) {
    await page.goto("login");
    await page.getByLabel("Password").fill(PASSWORD);
    await page.getByRole("button", { name: "Sign in" }).click();
    await page.waitForURL((url) => !url.pathname.endsWith("/login"));
  },
  shots: [
    { name: "overview", path: "workers/hello", wait: "canvas", settle: 2500 },
    { name: "workers", path: "workers", wait: "table", settle: 1500 },
    {
      name: "code-preview",
      path: "workers/hello/code",
      async steps(page) {
        await page.getByLabel("Path").fill("/?name=preview");
        await page.getByRole("button", { name: "Run preview" }).click();
        await page.getByRole("region", { name: "Response" }).getByRole("region", { name: "Body" }).waitFor();
      },
    },
    { name: "versions", path: "workers/hello/versions", wait: "table" },
    { name: "logs", path: "workers/hello/logs", wait: "table", settle: 2500 },
    {
      name: "objects",
      path: "workers/counter/objects",
      async steps(page) {
        await page.getByRole("cell", { name: "room-1", exact: true }).click();
        await page.getByRole("dialog", { name: "Counter room-1", exact: true }).getByRole("table", { name: "Storage keys" }).waitFor();
      },
    },
    { name: "cluster", path: "cluster", wait: "table", settle: 1500 },
  ],
};
