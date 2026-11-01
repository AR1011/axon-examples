import DOMPurify from "dompurify";
import hljs from "highlight.js/lib/common";
import MarkdownIt, { type Token } from "markdown-it";
import taskLists from "markdown-it-task-lists";
import morphdom from "morphdom";
import { useEffect, useRef } from "react";
import type * as Y from "yjs";

const md: MarkdownIt = new MarkdownIt({
  html: true,
  linkify: true,
  highlight: (code, language) => {
    if (!language || !hljs.getLanguage(language)) return "";
    try {
      return hljs.highlight(code, { language, ignoreIllegals: true }).value;
    } catch {
      return "";
    }
  },
})
  .use(taskLists)
  .use(lines);

/** A heading of the text: its level, its words and the id of its element in the preview. */
export interface Heading {
  level: number;
  text: string;
  id: string;
  /** Its line in the text, from 0. */
  line: number;
}

/** The headings of parsed tokens, each with an id made from its words: the same words later get -2, -3. */
function headingsOf(tokens: Token[]): (Heading & { token: Token })[] {
  const seen = new Map<string, number>();
  return tokens.flatMap((token, i) => {
    if (token.type !== "heading_open" || !token.map) return [];
    const text = (tokens[i + 1]?.children ?? []).filter((t) => t.type === "text" || t.type === "code_inline").map((t) => t.content).join("").trim();
    const slug = `md-${text.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, "-").replace(/^-|-$/g, "") || "h"}`;
    const n = (seen.get(slug) ?? 0) + 1;
    seen.set(slug, n);
    return [{ level: Number(token.tag.slice(1)), text, id: n === 1 ? slug : `${slug}-${n}`, line: token.map[0], token }];
  });
}

/** Each block's first line as `data-line`, for scroll sync, and an id on each heading, for the outline. */
function lines(markdown: MarkdownIt) {
  markdown.core.ruler.push("lines", (state) => {
    for (const token of state.tokens) if (token.map && token.block && token.nesting !== -1) token.attrSet("data-line", String(token.map[0]));
    for (const heading of headingsOf(state.tokens)) heading.token.attrSet("id", heading.id);
  });
}

/** The headings of the text, as the preview gives them ids. */
export const headings = (text: string): Heading[] => headingsOf(md.parse(text, {})).map(({ token: _, ...heading }) => heading);

/** The text as HTML, with nothing in it that runs: no script, no handler, no `javascript:` link. */
export function render(text: string): string {
  return DOMPurify.sanitize(md.render(text), { ADD_ATTR: ["target"], FORBID_TAGS: ["style", "form"] });
}

/** Renders 150 ms after typing stops, and at most each 500 ms while it goes on. */
const QUIET_MS = 150;
const EVERY_MS = 500;

/**
 * The rendered text, patched in place: an unchanged block keeps its element, so the scroll position, an image's
 * load and the height stay, and nothing moves that did not change.
 */
export function Preview({ text, onPaint }: { text: Y.Text; onPaint?: (host: HTMLElement) => void }) {
  const host = useRef<HTMLDivElement>(null);
  const painted = useRef(onPaint);
  useEffect(() => {
    painted.current = onPaint;
  }, [onPaint]);

  useEffect(() => {
    let quiet: ReturnType<typeof setTimeout> | null = null;
    let last = 0;
    const paint = () => {
      quiet = null;
      last = Date.now();
      const next = document.createElement("div");
      next.innerHTML = render(text.toString());
      if (!host.current) return;
      morphdom(host.current, next, { childrenOnly: true });
      painted.current?.(host.current);
    };
    const changed = () => {
      if (quiet) clearTimeout(quiet);
      quiet = setTimeout(paint, Math.max(0, Math.min(QUIET_MS, last + EVERY_MS - Date.now())));
    };
    paint();
    text.observe(changed);
    return () => {
      text.unobserve(changed);
      if (quiet) clearTimeout(quiet);
    };
  }, [text]);

  return <div ref={host} className="md-preview mx-auto max-w-[760px] px-6 py-4" data-testid="preview" />;
}
