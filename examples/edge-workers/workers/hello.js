export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") ?? "world";
    return new Response(`${env.GREETING ?? "hello"}, ${name}\n`);
  },
};
