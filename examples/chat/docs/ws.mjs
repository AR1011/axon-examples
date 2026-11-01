// A small websocket client for chat, for the README's terminal captures. Node 22 or later, no packages.
//
//   node examples/chat/docs/ws.mjs <user>=<host:port> .. -- '<step>' ..
//
// Each user is one websocket to the node at host:port, opened at the user's first step. A step is
//   '<user> join <room> [<gen>,<seq>]'   join, after that cursor if given; waits for the room's `ready`
//   '<user> post <room> <text>'          post; waits for the `ack` or `nack`
//   '<user> leave <room>'                leave
//   'wait <ms>'                          wait
// `$gen` in a cursor is the generation of the last `ack`. Each frame sent prints as `<user> > <frame>`, each frame
// received as `<user> < <frame>`. After the last step it waits half a second for what is still on its way.
const args = process.argv.slice(2);
const split = args.indexOf("--");
if (split < 1) {
  console.error("usage: node ws.mjs <user>=<host:port> .. -- '<step>' ..");
  process.exit(2);
}
const nodes = new Map(args.slice(0, split).map((a) => a.split("=")));
const steps = args.slice(split + 1);
const users = new Map();
let gen = null;

const open = (user) => {
  if (users.has(user)) return users.get(user);
  const addr = nodes.get(user);
  if (!addr) throw new Error(`no node for ${user}`);
  const u = { ws: new WebSocket(`ws://${addr}/ws?user=${user}`), waiters: [], ids: 0 };
  u.opened = new Promise((ok, fail) => {
    u.ws.onopen = ok;
    u.ws.onerror = () => fail(new Error(`${user}: cannot connect to ${addr}`));
  });
  u.ws.onmessage = (e) => {
    const frame = JSON.parse(e.data);
    console.log(`${user} < ${e.data}`);
    if (frame.t === "ack") gen = frame.gen;
    u.waiters = u.waiters.filter((w) => !w(frame));
  };
  users.set(user, u);
  return u;
};

const send = async (user, frame, until) => {
  const u = open(user);
  await u.opened;
  const done = until
    ? new Promise((ok, fail) => {
        const timer = setTimeout(() => fail(new Error(`${user}: no answer to ${JSON.stringify(frame)}`)), 10_000);
        u.waiters.push((f) => until(f) && (clearTimeout(timer), ok(), true));
      })
    : null;
  const text = JSON.stringify(frame);
  console.log(`${user} > ${text}`);
  u.ws.send(text);
  await done;
};

for (const step of steps) {
  const [who, verb, room, ...rest] = step.split(" ");
  if (who === "wait") {
    await new Promise((ok) => setTimeout(ok, Number(verb)));
  } else if (verb === "join") {
    const after = rest[0]?.replace("$gen", String(gen)).split(",").map(Number);
    const frame = after ? { t: "join", room, after } : { t: "join", room };
    await send(who, frame, (f) => f.t === "ready" && f.room === room);
  } else if (verb === "post") {
    const id = ++open(who).ids;
    await send(who, { t: "post", room, id, text: rest.join(" ") }, (f) => ["ack", "nack"].includes(f.t) && f.id === id);
  } else if (verb === "leave") {
    await send(who, { t: "leave", room });
  } else {
    throw new Error(`unknown step: ${step}`);
  }
}
await new Promise((ok) => setTimeout(ok, 500));
for (const u of users.values()) u.ws.close();
