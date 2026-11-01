import { EditorView } from "@codemirror/view";

/** A block of the preview: its first line in the text and its top in the pane's scrolled content. */
interface Block {
  line: number;
  top: number;
}

/** The preview's blocks with a `data-line`, in text order: a block inside another of the same line is left out. */
function blocks(pane: HTMLElement, lines: number): Block[] {
  const origin = pane.getBoundingClientRect().top - pane.scrollTop;
  const found: Block[] = [{ line: 0, top: 0 }];
  for (const el of pane.querySelectorAll<HTMLElement>("[data-line]")) {
    const line = Number(el.dataset.line);
    if (line > found.at(-1)!.line) found.push({ line, top: el.getBoundingClientRect().top - origin });
  }
  found.push({ line: lines, top: pane.scrollHeight });
  return found;
}

/** Where the document of the editor starts in its scrolled content: the content's top padding. */
const documentOffset = (view: EditorView) => view.documentTop - view.scrollDOM.getBoundingClientRect().top + view.scrollDOM.scrollTop;

/** The line at the top of the editor, from 0, with the part of it scrolled past as a fraction. */
function editorLine(view: EditorView): number {
  const height = view.scrollDOM.scrollTop - documentOffset(view);
  const block = view.lineBlockAtHeight(Math.max(0, height));
  const line = view.state.doc.lineAt(block.from).number - 1;
  return line + Math.min(1, Math.max(0, (height - block.top) / Math.max(1, block.height)));
}

/** The editor's scroll position that puts `line` (with a fraction) at its top. */
function editorTop(view: EditorView, line: number): number {
  const doc = view.state.doc;
  const n = Math.min(doc.lines, Math.floor(line) + 1);
  const block = view.lineBlockAt(doc.line(n).from);
  return documentOffset(view) + block.top + (line - (n - 1)) * block.height;
}

/** How long any scroll of a side we moved is taken as ours: the editor corrects its own position in the frame after,
 * as it draws lines whose heights it had estimated. After that, only a scroll to where we put it is ours. */
const DRAW_MS = 50;
const OURS_MS = 150;

/**
 * Keeps the preview's top on the block at the editor's top, and the other way: whichever the user scrolls leads. A
 * side we moved does not lead for a moment. Returns the stop, and `again` to call after the preview paints, as its
 * heights change.
 */
export function syncScroll(view: EditorView, pane: HTMLElement): { stop: () => void; again: () => void } {
  const moved = new Map<HTMLElement, { to: number; at: number }>();
  let leader: "editor" | "preview" = "editor";
  let frame = 0;
  const ours = (el: HTMLElement) => {
    const last = moved.get(el);
    const since = last ? performance.now() - last.at : Infinity;
    return since < DRAW_MS || (since < OURS_MS && Math.abs(el.scrollTop - last!.to) < 2);
  };
  const move = (el: HTMLElement, top: number) => {
    const to = Math.round(Math.max(0, Math.min(top, el.scrollHeight - el.clientHeight)));
    if (Math.abs(el.scrollTop - to) < 1) return;
    moved.set(el, { to, at: performance.now() });
    el.scrollTop = to;
  };
  const toPreview = () => {
    const line = editorLine(view);
    const all = blocks(pane, view.state.doc.lines);
    const i = Math.max(0, all.findLastIndex((b) => b.line <= line));
    const [a, b] = [all[i], all[Math.min(i + 1, all.length - 1)]];
    move(pane, a.top + (b.line > a.line ? ((line - a.line) / (b.line - a.line)) * (b.top - a.top) : 0));
  };
  const toEditor = () => {
    const y = pane.scrollTop;
    const all = blocks(pane, view.state.doc.lines);
    const i = Math.max(0, all.findLastIndex((b) => b.top <= y));
    const [a, b] = [all[i], all[Math.min(i + 1, all.length - 1)]];
    const line = a.line + (b.top > a.top ? ((y - a.top) / (b.top - a.top)) * (b.line - a.line) : 0);
    move(view.scrollDOM, editorTop(view, line));
    // The lines the editor had not drawn have estimated heights: once it draws the line, place it by where it shows.
    const n = Math.min(view.state.doc.lines, Math.floor(line) + 1);
    let tries = 4;
    cancelAnimationFrame(frame);
    const place = () => {
      const at = view.coordsAtPos(view.state.doc.line(n).from);
      if (!at) {
        if (--tries > 0) frame = requestAnimationFrame(place);
        return;
      }
      move(view.scrollDOM, view.scrollDOM.scrollTop + at.top - view.scrollDOM.getBoundingClientRect().top - (line - (n - 1)) * (at.bottom - at.top));
    };
    frame = requestAnimationFrame(place);
  };
  const onEditor = () => {
    if (ours(view.scrollDOM)) return;
    leader = "editor";
    cancelAnimationFrame(frame);
    toPreview();
  };
  const onPreview = () => {
    if (ours(pane)) return;
    leader = "preview";
    toEditor();
  };
  view.scrollDOM.addEventListener("scroll", onEditor, { passive: true });
  pane.addEventListener("scroll", onPreview, { passive: true });
  return {
    stop: () => {
      cancelAnimationFrame(frame);
      view.scrollDOM.removeEventListener("scroll", onEditor);
      pane.removeEventListener("scroll", onPreview);
    },
    // Whoever types leads: the line being typed stays where it is.
    again: () => (leader === "editor" || view.hasFocus ? toPreview() : toEditor()),
  };
}

/** Scrolls the editor so `line` (from 0) is at its top. */
export function scrollEditorTo(view: EditorView, line: number) {
  const at = view.state.doc.line(Math.min(view.state.doc.lines, line + 1)).from;
  view.dispatch({ effects: EditorView.scrollIntoView(at, { y: "start" }) });
}

/** Scrolls the preview so `target` is at its top. */
export function scrollPaneTo(pane: HTMLElement, target: HTMLElement) {
  pane.scrollTop += target.getBoundingClientRect().top - pane.getBoundingClientRect().top;
}
