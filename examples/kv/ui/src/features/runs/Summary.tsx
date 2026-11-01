import { Table } from "@cloudflare/kumo";
import { Fragment, type ReactNode } from "react";
import { Card, Notice } from "../../components/ui";

/** A part of `summary.md`: text, a list or a table. */
type Part = { kind: "text"; text: string } | { kind: "list"; items: string[] } | { kind: "table"; head: string[]; rows: string[][] };

interface Section {
  title: string | null;
  parts: Part[];
}

const cells = (line: string) =>
  line
    .trim()
    .replace(/^\||\|$/g, "")
    .split("|")
    .map((c) => c.trim());

/**
 * The few kinds of Markdown that `summary.py` writes: headings, paragraphs, lists and tables. The first `#` heading
 * is the run's name, which the page has already.
 */
export function parse(text: string): Section[] {
  const sections: Section[] = [{ title: null, parts: [] }];
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const parts = sections[sections.length - 1].parts;
    const last = parts[parts.length - 1];
    if (line.startsWith("## ")) sections.push({ title: line.slice(3).trim(), parts: [] });
    else if (line.startsWith("# ") || !line.trim()) continue;
    else if (line.startsWith("|")) {
      const head = cells(line);
      const rows: string[][] = [];
      // The second line is the rule under the head.
      for (i += 2; i < lines.length && lines[i].startsWith("|"); i++) rows.push(cells(lines[i]));
      i--;
      parts.push({ kind: "table", head, rows });
    } else if (line.startsWith("- ")) {
      if (last?.kind === "list") last.items.push(line.slice(2));
      else parts.push({ kind: "list", items: [line.slice(2)] });
    } else parts.push({ kind: "text", text: line });
  }
  return sections.filter((s) => s.parts.length > 0);
}

/** `**bold**` as bold; the rest as it is. */
function inline(text: string): ReactNode {
  return text.split(/(\*\*[^*]+\*\*)/).map((piece, i) => (piece.startsWith("**") && piece.endsWith("**") ? <strong key={i}>{piece.slice(2, -2)}</strong> : <Fragment key={i}>{piece}</Fragment>));
}

const NUMERIC = /^[\d,.\s[\]()%>-]+$|^\d/;

function Part({ part }: { part: Part }) {
  if (part.kind === "text") {
    // `summary.py` starts with the line that says how far to trust the numbers.
    if (part.text.startsWith("**Indicative.**")) return (
        <div className="px-4">
          <Notice tone="warning">{inline(part.text)}</Notice>
        </div>
      );
    return <p className="px-4 text-base">{inline(part.text)}</p>;
  }
  if (part.kind === "list")
    return (
      <ul className="list-disc px-4 pl-9 text-base">
        {part.items.map((item, i) => (
          <li key={i} className="break-words">
            {inline(item)}
          </li>
        ))}
      </ul>
    );
  const numeric = part.head.map((_, c) => c > 0 && part.rows.every((row) => !row[c] || NUMERIC.test(row[c])));
  return (
    <div className="overflow-x-auto">
      <Table className="min-w-[640px]">
        <Table.Header>
          <Table.Row>
            {part.head.map((h, c) => (
              <Table.Head key={c} className={numeric[c] ? "text-right whitespace-nowrap" : "whitespace-nowrap"}>
                {h}
              </Table.Head>
            ))}
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {part.rows.map((row, r) => (
            <Table.Row key={r}>
              {part.head.map((_, c) => (
                <Table.Cell key={c} className={numeric[c] ? "text-right font-mono text-sm tabular-nums whitespace-nowrap" : c === 0 ? "font-mono text-sm whitespace-nowrap" : "text-sm"}>
                  {row[c] ?? ""}
                </Table.Cell>
              ))}
            </Table.Row>
          ))}
        </Table.Body>
      </Table>
    </div>
  );
}

/** `summary.md` as cards: the notes at its top, then a card for each of its sections. */
export function Summary({ text }: { text: string }) {
  return (
    <>
      {parse(text).map((section, i) => (
        <Card key={i} title={section.title ?? "Notes"} label={section.title ?? "Notes"}>
          <div className="flex flex-col gap-3 py-3 [&>.overflow-x-auto]:-mb-3">
            {section.parts.map((part, j) => (
              <Part key={j} part={part} />
            ))}
          </div>
        </Card>
      ))}
    </>
  );
}
