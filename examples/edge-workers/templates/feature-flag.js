// A feature behind a variable. NEW_CHECKOUT is "on", "off", or a percent of the visitors ("25"): change it with
// `edge var set`, which makes a new version, and roll it back like any deploy.
function enabled(flag, visitor) {
  if (flag === "on") return true;
  const percent = Number(flag);
  if (!Number.isFinite(percent) || percent <= 0) return false;
  // The same visitor gets the same answer every time.
  let hash = 0;
  for (const c of visitor) hash = (hash * 31 + c.charCodeAt(0)) >>> 0;
  return hash % 100 < percent;
}

export default {
  async fetch(request, env) {
    const visitor = request.headers.get("x-visitor") ?? "anonymous";
    const on = enabled(env.NEW_CHECKOUT ?? "off", visitor);
    const body = { checkout: on ? "new" : "classic", flag: env.NEW_CHECKOUT ?? "off", visitor };
    return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
  },
};
