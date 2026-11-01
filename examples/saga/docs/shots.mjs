// The README's screenshots, all of axon's console on the saga cluster:
//   node tools/shots/shot.mjs examples/saga/docs/shots.mjs --base http://127.0.0.1:<console>/
// Take them against a seed and nodes n1, n2, n3, with n1 started with --console on loopback (no sign-in there),
// right after `saga load` ran with n2 killed by SIGKILL during it, as in demo.sh.
export default {
  shots: [
    {
      name: "events",
      async steps(page) {
        // The sagas of the killed node, started again on the others with no caller. Each node's event log keeps its
        // newest 256 events, so a burst of starts between two of the collector's pulls can push these out: take the
        // shot again on a new run if none shows.
        await page.goto("events?ek=eager_restart");
        await page.getByText(/for n2, epoch/).first().waitFor({ timeout: 10_000 });
      },
    },
    { name: "placement", path: "placement", settle: 2000 },
    { name: "actors", path: "actors", settle: 2000, wait: "table" },
    { name: "overview", path: "overview", settle: 3000, wait: "h1, h2" },
  ],
};
