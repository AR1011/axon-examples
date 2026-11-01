// The README's screenshots of axon's console on g1:
//   B3_PID=<pid of b3> node tools/shots/shot.mjs examples/api-gateway/docs/shots.mjs --base http://127.0.0.1:<console>/
// Against the cluster of "Run it" (a seed, b1 to b3 with b3 cuttable, g1 with --dashboard, g2) with load on both
// gateways, from the repository root on the same machine. The cut shot sends b3 SIGUSR1, which cuts every connection
// into b3; heal it afterwards with SIGUSR2.
const B3 = process.env.B3_PID;
const pause = (ms) => new Promise((done) => setTimeout(done, ms));

export default {
  shots: [
    { name: "console-topology", path: "overview/topology", settle: 3000 },
    { name: "console-actors", path: "actors", settle: 2000, wait: "table" },
    {
      name: "console-cut",
      async steps(page, ctx) {
        if (!B3) throw new Error("set B3_PID");
        ctx.sh(`kill -USR1 ${B3}`);
        // Membership marks b3 unavailable about 5.5 s after the cut.
        await pause(9000);
        await page.goto("overview");
      },
      settle: 3000,
    },
  ],
};
