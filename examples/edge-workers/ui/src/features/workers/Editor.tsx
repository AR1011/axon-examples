import { javascript } from "@codemirror/lang-javascript";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { unifiedMergeView } from "@codemirror/merge";
import { EditorState } from "@codemirror/state";
import { EditorView } from "@codemirror/view";
import { tags } from "@lezer/highlight";
import { basicSetup } from "codemirror";
import { useEffect, useRef } from "react";

// Colours come from the page's tokens, so the editor follows the mode with no rebuild.
const TEXT = "var(--text-color-kumo-default)";
const SUBTLE = "var(--text-color-kumo-subtle)";
const mix = (token: string, percent: number) => `color-mix(in srgb, var(--color-kumo-${token}) ${percent}%, transparent)`;
const look = EditorView.theme({
  "&": { color: TEXT, backgroundColor: "var(--color-kumo-base)" },
  ".cm-content": { caretColor: TEXT },
  ".cm-cursor": { borderLeftColor: TEXT },
  ".cm-gutters": { backgroundColor: "var(--color-kumo-elevated)", color: SUBTLE, border: "none", borderRight: "1px solid var(--color-kumo-line)" },
  ".cm-activeLine": { backgroundColor: mix("tint", 60) },
  ".cm-activeLineGutter": { backgroundColor: "var(--color-kumo-tint)", color: TEXT },
  "&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground, ::selection": { backgroundColor: mix("brand", 24) },
  ".cm-selectionMatch": { backgroundColor: mix("brand", 14) },
  "&.cm-focused .cm-matchingBracket": { backgroundColor: mix("brand", 26), outline: "none" },
  ".cm-tooltip, .cm-panels": { backgroundColor: "var(--color-kumo-base)", color: TEXT, border: "1px solid var(--color-kumo-line)" },
  ".cm-tooltip-autocomplete ul li[aria-selected]": { backgroundColor: "var(--color-kumo-tint)", color: TEXT },
  ".cm-foldPlaceholder": { backgroundColor: "var(--color-kumo-tint)", border: "none", color: SUBTLE },
  ".cm-textfield, .cm-button": { backgroundColor: "var(--color-kumo-control)", backgroundImage: "none", border: "1px solid var(--color-kumo-line)", color: TEXT },
  ".cm-changedLine, &.cm-merge-b .cm-changedLine": { backgroundColor: mix("success", 12) },
  ".cm-deletedChunk": { backgroundColor: mix("danger", 12) },
  ".cm-changedText, &.cm-merge-b .cm-changedText": { background: "none", backgroundColor: mix("success", 28) },
  ".cm-deletedChunk .cm-deletedText, .cm-deletedText": { background: "none", backgroundColor: mix("danger", 28) },
});

const colours = HighlightStyle.define([
  { tag: [tags.keyword, tags.operatorKeyword, tags.modifier], color: "var(--code-keyword)" },
  { tag: [tags.string, tags.special(tags.string), tags.regexp], color: "var(--code-string)" },
  { tag: [tags.number, tags.bool, tags.null, tags.atom], color: "var(--code-number)" },
  { tag: [tags.function(tags.variableName), tags.function(tags.propertyName), tags.definition(tags.variableName)], color: "var(--code-name)" },
  { tag: [tags.className, tags.typeName, tags.namespace], color: "var(--code-type)" },
  { tag: [tags.comment, tags.meta], color: SUBTLE, fontStyle: "italic" },
  { tag: tags.invalid, color: "var(--text-color-kumo-danger)" },
]);

interface Props {
  /** The text the editor starts with. Give the component a new `key` to load another text. */
  initial: string;
  label: string;
  onChange?: (text: string) => void;
  readOnly?: boolean;
  /** Shows the changes from this text, line by line. */
  original?: string;
}

export default function Editor({ initial, label, onChange, readOnly, original }: Props) {
  const host = useRef<HTMLDivElement>(null);
  const changed = useRef(onChange);

  useEffect(() => {
    changed.current = onChange;
  }, [onChange]);

  useEffect(() => {
    const view = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc: initial,
        extensions: [
          basicSetup,
          javascript(),
          look,
          syntaxHighlighting(colours),
          EditorState.readOnly.of(!!readOnly),
          EditorView.contentAttributes.of({ "aria-label": label }),
          EditorView.updateListener.of((update) => update.docChanged && changed.current?.(update.state.doc.toString())),
          ...(original == null ? [] : [unifiedMergeView({ original, mergeControls: false, gutter: true })]),
        ],
      }),
    });
    return () => view.destroy();
    // The text is the editor's own after the first render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [readOnly, original, label]);

  return <div ref={host} className="size-full min-h-0 overflow-hidden" />;
}
