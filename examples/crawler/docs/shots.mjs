// The README's screenshots: node tools/shots/shot.mjs examples/crawler/docs/shots.mjs --base http://127.0.0.1:<port>/
// Against axon's console on the control node of a held demo, after its kill, from the repository root on the same
// machine. The shots were taken during and after this run:
//   crawler demo --domains 64 --pages 200 --rate 5 --kill-at 0.4 --console 127.0.0.1:<port> --hold 300
// The console has no users on a loopback bind, so there is no sign-in.
export default {
  shots: [
    { name: "console-overview", path: "overview", settle: 3000, wait: "h1" },
    {
      name: "console-keys",
      async steps(page) {
        await page.goto("placement?node=n0");
        await page.locator('[aria-label="Node"] [aria-label="Keys"] tbody tr').first().waitFor();
      },
      settle: 1500,
    },
  ],
};
