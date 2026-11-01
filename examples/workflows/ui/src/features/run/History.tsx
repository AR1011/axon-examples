import { Button } from "@cloudflare/kumo";
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { get, runPath, type History as Page } from "../../api";
import { Empty, Failed, Loading, PRE } from "../../components/ui";

const LIMIT = 50;

/** The run's journal as the replicas hold it, one entry a block, a page at a time. */
export function History({ id }: { id: string }) {
  // Where each page starts: the first at 0.
  const [starts, setStarts] = useState<number[]>([0]);
  const after = starts[starts.length - 1];
  const page = useQuery({ queryKey: ["history", id, after], queryFn: ({ signal }) => get<Page>(runPath(id, `/history?after=${after}&limit=${LIMIT}`), signal), placeholderData: (last) => last });
  if (page.isPending) return <Loading what="the journal" />;
  if (page.error && !page.data) return <Failed what="the journal" error={page.error} />;
  const entries = page.data?.entries ?? [];
  if (!entries.length && after === 0) return <Empty title="No entries" />;
  return (
    <>
      <ol className="flex flex-col gap-2 p-4" aria-label="Journal entries">
        {entries.map((entry, i) => (
          <li key={after + i} className="flex gap-3">
            <span className="w-10 shrink-0 pt-3 text-right font-mono text-sm text-kumo-subtle">{after + i}</span>
            <pre className={`${PRE} min-w-0 flex-1`}>{JSON.stringify(entry, null, 2)}</pre>
          </li>
        ))}
      </ol>
      <footer className="flex items-center justify-end gap-2 border-t border-kumo-line px-4 py-2.5">
        <span className="mr-auto text-sm text-kumo-subtle">
          Entries {after} to {after + Math.max(0, entries.length - 1)}
        </span>
        <Button variant="secondary" size="sm" disabled={starts.length < 2} onClick={() => setStarts(starts.slice(0, -1))}>
          Previous
        </Button>
        <Button variant="secondary" size="sm" disabled={page.data?.next == null} onClick={() => page.data?.next != null && setStarts([...starts, page.data.next])}>
          Next
        </Button>
      </footer>
    </>
  );
}
