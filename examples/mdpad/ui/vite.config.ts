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

// The service worker keeps every file of this build: its list, and a cache name that changes with it.
const offline: Plugin = {
  name: "offline",
  apply: "build",
  generateBundle(_, bundle) {
    const sw = bundle["sw.js"];
    if (!sw || sw.type !== "chunk") throw new Error("the build has no sw.js");
    const files = Object.keys(bundle).filter((name) => name !== "sw.js").sort();
    const version = createHash("sha256").update(files.join("\n")).digest("hex").slice(0, 12);
    const code = sw.code.replace(/(["'`])__FILES__\1/, JSON.stringify(files)).replace("__VERSION__", version);
    if (code.includes("__FILES__") || code.includes("__VERSION__")) throw new Error("sw.js: a placeholder is left");
    sw.code = code;
  },
};

export default defineConfig({
  base: "./",
  plugins: [react(), tailwindcss(), sources, offline],
  // MDPAD_API: a web node's address (http=addr) for the dev server.
  server: { proxy: { "/api": { target: process.env.MDPAD_API ?? "http://127.0.0.1:8080", ws: true } } },
  build: {
    sourcemap: false,
    assetsInlineLimit: 0,
    // CodeMirror with the Markdown language, which brings HTML, CSS and JavaScript for the blocks in it.
    chunkSizeWarningLimit: 700,
    rollupOptions: {
      input: { index: "index.html", sw: "src/sw.ts" },
      output: {
        entryFileNames: (chunk) => (chunk.name === "sw" ? "sw.js" : "assets/[name]-[hash].js"),
        // The editor page's libraries, apart from the sign-in and list pages, which load none of them.
        manualChunks: (id) =>
          /node_modules\/(@codemirror|@lezer|codemirror|crelt|style-mod|w3c-keyname|y-codemirror|yjs|lib0|y-protocols|y-indexeddb)/.test(id)
            ? "editor"
            : /node_modules\/(markdown-it|highlight\.js|dompurify|morphdom|mdurl|uc\.micro|linkify-it|entities|punycode)/.test(id)
              ? "preview"
              : undefined,
      },
    },
  },
});
