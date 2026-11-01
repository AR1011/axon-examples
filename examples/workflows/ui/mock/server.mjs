// Serves the built console (../dist) and the mock API on one port, as the api node does.
//   node mock/server.mjs [port] [base]     base is where the console is mounted, "/" or for example "/ops/wf/"
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { handle, PASSWORD } from "./api.mjs";

const port = Number(process.argv[2] ?? 7789);
const base = process.argv[3] ?? "/";
const dist = fileURLToPath(new URL("../dist", import.meta.url));
const TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".woff2": "font/woff2", ".svg": "image/svg+xml" };

createServer(async (req, res) => {
  if (await handle(req, res)) return;
  const path = new URL(req.url, "http://mock").pathname;
  if (!path.startsWith(base)) {
    res.writeHead(404).end("not found");
    return;
  }
  const file = normalize(path.slice(base.length));
  if (!file.startsWith("..") && file.startsWith("assets/")) {
    try {
      const body = await readFile(join(dist, file));
      res.writeHead(200, { "content-type": TYPES[extname(file)] ?? "application/octet-stream" }).end(body);
    } catch {
      res.writeHead(404).end("not found");
    }
    return;
  }
  // Every other path is a page of the console: the index, with the base it is mounted at.
  const index = await readFile(join(dist, "index.html"), "utf8");
  res.writeHead(200, { "content-type": TYPES[".html"], "cache-control": "no-store" }).end(index.replace('<base href="/" />', `<base href="${base}" />`));
}).listen(port, "127.0.0.1", () => console.log(`mock console on http://127.0.0.1:${port}${base} (password: ${PASSWORD})`));
