// A small JSON API: routes by method and path, reads a JSON body, answers JSON.
const json = (body, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/") {
      return json({ routes: ["GET /", "GET /time", "POST /echo"] });
    }
    if (request.method === "GET" && url.pathname === "/time") {
      return json({ now: new Date().toISOString() });
    }
    if (request.method === "POST" && url.pathname === "/echo") {
      let body;
      try {
        body = await request.json();
      } catch {
        return json({ error: "the body is not JSON" }, 400);
      }
      return json({ received: body, id: crypto.randomUUID() }, 201);
    }
    return json({ error: "not found", path: url.pathname }, 404);
  },
};
