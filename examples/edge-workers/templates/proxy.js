// Passes each request to UPSTREAM (a variable, such as https://example.com) and marks the answer.
export default {
  async fetch(request, env) {
    if (!env.UPSTREAM) return new Response("set the UPSTREAM variable\n", { status: 500 });
    const url = new URL(request.url);
    const headers = new Headers(request.headers);
    headers.delete("host");
    const body = ["GET", "HEAD"].includes(request.method) ? undefined : await request.arrayBuffer();
    let answer;
    try {
      answer = await fetch(env.UPSTREAM + url.pathname + url.search, { method: request.method, headers, body });
    } catch (e) {
      console.error("upstream: " + e);
      return new Response("the upstream did not answer\n", { status: 502 });
    }
    const out = new Headers(answer.headers);
    out.set("x-proxied-by", "edge-workers");
    return new Response(await answer.arrayBuffer(), { status: answer.status, headers: out });
  },
};
