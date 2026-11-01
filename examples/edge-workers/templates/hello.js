// The smallest worker: one answer for every request.
export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") ?? "world";
    console.log(`greeting ${name}`);
    return new Response(`${env.GREETING ?? "hello"}, ${name}\n`);
  },
};
