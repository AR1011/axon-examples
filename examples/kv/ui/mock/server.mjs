// Serves the built console (../dist) and the mock API on one port, as `kv console` does.
//   node mock/server.mjs [port]
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { handle } from "./api.mjs";

const port = Number(process.argv[2] ?? 7389);
const dist = fileURLToPath(new URL("../dist", import.meta.url));
const TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".woff2": "font/woff2", ".svg": "image/svg+xml" };

createServer(async (req, res) => {
  if (await handle(req, res)) return;
  const file = normalize(new URL(req.url, "http://mock").pathname.slice(1));
  if (!file.startsWith("..") && extname(file)) {
    try {
      res.writeHead(200, { "content-type": TYPES[extname(file)] ?? "application/octet-stream" }).end(await readFile(join(dist, file)));
    } catch {
      res.writeHead(404).end("not found");
    }
    return;
  }
  // Every other path is a page of the console.
  res.writeHead(200, { "content-type": TYPES[".html"], "cache-control": "no-store" }).end(await readFile(join(dist, "index.html")));
}).listen(port, "127.0.0.1", () => console.log(`mock kv console on http://127.0.0.1:${port}/`));
