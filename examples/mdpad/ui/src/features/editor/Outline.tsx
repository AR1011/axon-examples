import { TableOfContents, useTableOfContentsActiveId } from "@cloudflare/kumo";
import { useEffect, useState } from "react";
import type * as Y from "yjs";
import { headings, type Heading } from "./Preview";

const INDENT = ["pl-4", "pl-4", "pl-7", "pl-10"];

/** The headings of the text, made again 300 ms after the typing stops. */
function useHeadings(text: Y.Text): Heading[] {
  const [shown, setShown] = useState(() => headings(text.toString()));
  useEffect(() => {
    let quiet: ReturnType<typeof setTimeout> | null = null;
    const changed = () => {
      if (quiet) clearTimeout(quiet);
      quiet = setTimeout(() => setShown(headings(text.toString())), 300);
    };
    text.observe(changed);
    return () => {
      text.unobserve(changed);
      if (quiet) clearTimeout(quiet);
    };
  }, [text]);
  return shown;
}

/**
 * The outline: the headings to level 3, the one at the top of the preview marked. `painted` are the heading ids the
 * preview shows now, as the marking watches those elements.
 */
export function Outline({ text, pane, painted, onJump }: { text: Y.Text; pane: HTMLElement | null; painted: string[]; onJump: (heading: Heading) => void }) {
  const all = useHeadings(text).filter((h) => h.level <= 3);
  const { activeId, selectSection } = useTableOfContentsActiveId({ ids: pane ? painted : [], root: pane, trackHash: false });
  return (
    <TableOfContents aria-label="Outline" className="w-56 shrink-0 overflow-y-auto border-r border-kumo-hairline px-3 py-4" data-testid="outline">
      {all.length ? (
        <TableOfContents.List className="gap-1.5">
          {all.map((heading) => (
            <TableOfContents.Item
              key={heading.id}
              href={`#${heading.id}`}
              className={INDENT[heading.level]}
              active={activeId === heading.id}
              onClick={(event) => {
                event.preventDefault();
                selectSection(heading.id);
                onJump(heading);
              }}
            >
              {heading.text || "Untitled"}
            </TableOfContents.Item>
          ))}
        </TableOfContents.List>
      ) : (
        <p className="px-1 text-sm text-kumo-subtle">No headings.</p>
      )}
    </TableOfContents>
  );
}
