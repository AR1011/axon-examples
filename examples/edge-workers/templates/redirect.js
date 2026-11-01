// Redirects by path. TARGET is where a path with no rule of its own goes: set it as a variable.
const RULES = {
  "/docs": "https://example.com/documentation",
  "/blog": "https://example.com/news",
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const to = RULES[url.pathname] ?? (env.TARGET ? env.TARGET + url.pathname + url.search : null);
    if (!to) return new Response("no redirect for " + url.pathname + "\n", { status: 404 });
    return new Response(null, { status: 302, headers: { location: to } });
  },
};
