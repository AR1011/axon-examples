import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";

// A digest of the sources in the page, so every source change changes dist, even one that builds to the same bundle.
const sources: Plugin = {
  name: "sources",
  transformIndexHtml: {
    order: "pre",
    handler(html) {
      const files = readdirSync("src", { recursive: true, withFileTypes: true }).filter((f) => f.isFile() && !f.name.startsWith("."));
      const digest = createHash("sha256").update(html);
      for (const path of files.map((f) => `${f.parentPath}/${f.name}`).sort()) {
        digest.update(path).update(readFileSync(path));
      }
      return html.replace("</head>", `  <meta name="sources" content="${digest.digest("hex").slice(0, 16)}" />\n  </head>`);
    },
  },
};

// EDGE_MOCK=1: the mock API answers /api in the dev server, and no control node is needed.
const mock: Plugin = {
  name: "mock-api",
  async configureServer(server) {
    const { handle } = (await import("./mock/api.mjs" as string)) as { handle: (req: unknown, res: unknown) => Promise<boolean> };
    server.middlewares.use((req, res, next) => void handle(req, res).then((done) => done || next(), next));
  },
};

const mocked = !!process.env.EDGE_MOCK;

export default defineConfig({
  base: "./",
  plugins: [react(), tailwindcss(), sources, ...(mocked ? [mock] : [])],
  // EDGE_API: the control node's admin address (admin=addr).
  server: { proxy: mocked ? {} : { "/api": process.env.EDGE_API ?? "http://127.0.0.1:7703" } },
  build: {
    sourcemap: false,
    assetsInlineLimit: 0,
    rollupOptions: {
      output: {
        manualChunks: (id) => (/node_modules\/(echarts|zrender)\//.test(id) ? "charts" : /node_modules\/(@codemirror|@lezer|codemirror|crelt|style-mod|w3c-keyname)\//.test(id) ? "editor" : undefined),
      },
    },
  },
});
