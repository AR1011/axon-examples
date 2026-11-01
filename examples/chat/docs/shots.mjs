// The README's screenshots of axon's console on chat's cluster:
//   CHAT_KILL_PID=<pid of n1> node tools/shots/shot.mjs examples/chat/docs/shots.mjs --base http://127.0.0.1:<console>/
// Against a seed and three nodes (n0 with --console) and `chat load` running, so the pages are not empty. The console
// on a loopback bind has no users, so there is no sign-in. "moved" kills the node whose pid is in CHAT_KILL_PID with
// SIGKILL first, so it is taken last.
const nodes = (page) => page.getByText(/keys on \d+ of \d+ nodes/);

export default {
  shots: [
    { name: "overview", path: "overview", settle: 4000 },
    {
      name: "streams",
      async steps(page) {
        await page.goto("streams");
        await page.getByRole("button", { name: "Table" }).click();
        await page.getByRole("cell", { name: "origin" }).first().waitFor();
      },
      settle: 2000,
    },
    { name: "placement", path: "placement", wait: "[aria-label^='Placement board']", settle: 2000 },
    {
      name: "moved",
      async steps(page, ctx) {
        ctx.sh(`kill -9 ${ctx.env.CHAT_KILL_PID}`);
        await page.goto("placement");
        // The seeds drop the dead node after 8 to 10 s. Its rooms start again on the next node when their
        // followers' subscriptions say the origin is lost.
        await nodes(page).filter({ hasText: "of 2 nodes" }).waitFor({ timeout: 60_000 });
        await page.waitForTimeout(15_000);
      },
      settle: 2000,
    },
  ],
};
