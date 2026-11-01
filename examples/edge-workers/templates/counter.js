// A Durable Object: one counter per name, in the whole cluster. Bind the class when you deploy:
//   edge deploy counter.js --object COUNTER=Counter
// POST /<name> adds one, GET /<name> reads, DELETE /<name> starts again from zero.
const json = (value, status = 200) =>
  new Response(JSON.stringify(value) + "\n", { status, headers: { "content-type": "application/json" } });

export class Counter {
  constructor(state, env) {
    this.storage = state.storage; // what this object keeps: it outlives the runner that runs it
    this.name = state.id.name;
  }

  // One request at a time, so reading and then writing loses no count.
  async fetch(request) {
    let count = (await this.storage.get("count")) ?? 0;
    if (request.method === "POST") {
      count += 1;
      await this.storage.put("count", count);
    } else if (request.method === "DELETE") {
      count = 0;
      await this.storage.delete("count");
    }
    return json({ name: this.name, count });
  }
}

export default {
  async fetch(request, env) {
    const name = new URL(request.url).pathname.slice(1) || "default";
    // The same name is the same object, whichever runner runs this handler.
    const counter = env.COUNTER.get(env.COUNTER.idFromName(name));
    try {
      return await counter.fetch(request);
    } catch (e) {
      // The object failed or could not be reached. A POST may have counted: only its answer is lost.
      return json({ error: e.message }, 503);
    }
  },
};
