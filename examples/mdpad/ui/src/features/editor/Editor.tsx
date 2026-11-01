import { defaultKeymap, indentWithTab } from "@codemirror/commands";
import { markdown } from "@codemirror/lang-markdown";
import { defaultHighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { Compartment, EditorState } from "@codemirror/state";
import { drawSelection, EditorView, keymap, placeholder } from "@codemirror/view";
import { useEffect, useRef } from "react";
import { yCollab, yUndoManagerKeymap } from "y-codemirror.next";
import { carets } from "./carets";
import type { DocSession } from "./session";

/** The text editor, bound to the session's Y.Text: what it shows is the doc, and typing changes the doc. */
export function Editor({ session, readOnly, onView }: { session: DocSession; readOnly: boolean; onView?: (view: EditorView | null) => void }) {
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<{ view: EditorView; mode: Compartment } | null>(null);

  useEffect(() => {
    const mode = new Compartment();
    const editor = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc: session.text.toString(),
        extensions: [
          keymap.of([...yUndoManagerKeymap, ...defaultKeymap, indentWithTab]),
          drawSelection(),
          EditorView.lineWrapping,
          markdown(),
          syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
          placeholder("Write Markdown here."),
          EditorView.contentAttributes.of({ "aria-label": "Markdown text" }),
          yCollab(session.text, null, { undoManager: session.undo }),
          carets(session.text, session.awareness),
          mode.of(modeOf(readOnly)),
        ],
      }),
    });
    view.current = { view: editor, mode };
    onView?.(editor);
    return () => {
      view.current = null;
      onView?.(null);
      editor.destroy();
    };
    // The view lives as long as the session: the read-only change below goes through its compartment.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session]);

  useEffect(() => {
    view.current?.view.dispatch({ effects: view.current.mode.reconfigure(modeOf(readOnly)) });
  }, [readOnly]);

  return <div ref={host} className="h-full min-h-0" data-testid="editor" />;
}

const modeOf = (readOnly: boolean) => [EditorState.readOnly.of(readOnly), EditorView.editable.of(!readOnly)];
