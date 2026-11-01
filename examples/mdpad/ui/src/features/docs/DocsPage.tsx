import { Badge, Banner, Button, Dialog, DropdownMenu, Empty, Input, Loader, Pagination, Table, Tabs, useKumoToastManager } from "@cloudflare/kumo";
import { DotsThreeIcon, FileArrowUpIcon, PlusIcon, WarningCircleIcon } from "@phosphor-icons/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRef, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router";
import { bytes, call, download, fileName, message, pageUrl, readImport, signInOn401, when, type DocRow } from "../../api";
import { TopBar, useMe } from "../../shell/TopBar";

/** Rows on a page of the list. */
const PAGE = 100;

type Tab = "mine" | "shared" | "trash";
const TABS: { value: Tab; label: string }[] = [
  { value: "mine", label: "Mine" },
  { value: "shared", label: "Shared with me" },
  { value: "trash", label: "Trash" },
];
const ACCESS: Record<DocRow["level"], { label: string; variant: "primary" | "secondary" | "outline" }> = {
  owner: { label: "Owner", variant: "primary" },
  edit: { label: "Edit", variant: "secondary" },
  view: { label: "View", variant: "outline" },
  none: { label: "None", variant: "outline" },
};

interface Page {
  docs: DocRow[];
  partial: boolean;
  next: string | null;
}

/** The documents: Mine, Shared with me and Trash, a page of 100 rows at a time. */
export function DocsPage() {
  const navigate = useNavigate();
  const [search, setSearch] = useSearchParams();
  const tab = TABS.find((t) => t.value === search.get("tab"))?.value ?? "mine";
  // The `next` of each page before this one: page n starts after cursors[n - 2].
  const [cursors, setCursors] = useState<string[]>([]);
  const [filter, setFilter] = useState("");
  const me = useMe();
  const after = cursors.at(-1);
  const docs = useQuery<Page>({
    queryKey: ["docs", tab, after ?? ""],
    queryFn: signInOn401(() => call<Page>("GET", `docs?tab=${tab}&limit=${PAGE}${after ? `&after=${encodeURIComponent(after)}` : ""}`)),
    enabled: me.isSuccess,
    // Back from the editor, the list shows the titles and times the editing gave.
    refetchOnMount: "always",
    // A home tells its catalog a title at most every 10 s while the text is edited.
    refetchInterval: 10_000,
    // The page before stays while the next loads; another tab's rows do not.
    placeholderData: (previous, query) => (query?.queryKey[1] === tab ? previous : undefined),
  });
  const create = useMutation({ mutationFn: (markdown: string) => call<{ id: string }>("POST", "docs", { markdown }), onSuccess: ({ id }) => navigate(`/d/${id}`) });
  const file = useRef<HTMLInputElement>(null);
  const [importError, setImportError] = useState<string | null>(null);
  const choose = (next: string) => {
    setSearch(next === "mine" ? {} : { tab: next }, { replace: true });
    setCursors([]);
  };
  const importFile = async (chosen: File | undefined) => {
    setImportError(null);
    if (!chosen) return;
    try {
      create.mutate(await readImport(chosen));
    } catch (e) {
      setImportError(message(e));
    }
  };
  const shown = (docs.data?.docs ?? []).filter((d) => (d.title || "Untitled").toLowerCase().includes(filter.trim().toLowerCase()));
  const failed = create.error ?? docs.error ?? me.error;
  const error = importError ?? (failed ? message(failed) : null);
  const page = cursors.length + 1;
  const turn = (to: number) => {
    const next = docs.data?.next;
    setCursors(to > page && next ? [...cursors, next] : cursors.slice(0, Math.max(0, to - 1)));
  };

  return (
    <div className="flex min-h-dvh flex-col">
      <TopBar me={me.data} />
      <main className="mx-auto flex w-full max-w-[1040px] flex-col gap-4 px-4 py-6">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="mr-auto text-xl font-semibold">Documents</h1>
          <input ref={file} type="file" accept=".md,.markdown,text/markdown,text/plain" className="hidden" aria-label="Markdown file" onChange={(e) => void importFile(e.target.files?.[0]).finally(() => (e.target.value = ""))} />
          <div className="flex gap-2">
            <Button variant="secondary" icon={FileArrowUpIcon} disabled={create.isPending} onClick={() => file.current?.click()}>
              Import .md
            </Button>
            <Button variant="primary" icon={PlusIcon} loading={create.isPending} onClick={() => create.mutate("# Untitled\n\n")}>
              New document
            </Button>
          </div>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <Tabs variant="underline" value={tab} onValueChange={choose} tabs={TABS} className="min-w-0 flex-1" />
          <Input aria-label="Filter by title" placeholder="Filter by title" value={filter} onChange={(e) => setFilter(e.target.value)} className="w-full sm:w-56" size="sm" />
        </div>
        {error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />}
        {docs.data?.partial && <Banner size="sm" variant="alert" icon={<WarningCircleIcon weight="fill" />} description="Some documents are not in the list. A server did not answer." />}
        {docs.isPending ? (
          <div className="flex justify-center p-8">
            <Loader />
          </div>
        ) : shown.length ? (
          <Rows tab={tab} rows={shown} />
        ) : (
          docs.data && <Empty title={filter ? "No documents with this title" : tab === "trash" ? "The trash is empty" : "No documents"} description={filter ? "Change the filter." : tab === "mine" ? "Create one or import a .md file." : tab === "shared" ? "No one shared a document with you." : undefined} />
        )}
        {(page > 1 || docs.data?.next) && (
          <Pagination page={page} perPage={PAGE} hasNextPage={!!docs.data?.next} setPage={turn}>
            <Pagination.Controls controls="simple" />
          </Pagination>
        )}
      </main>
    </div>
  );
}

function Rows({ tab, rows }: { tab: Tab; rows: DocRow[] }) {
  return (
    <div className="overflow-x-auto">
      <Table className="table-fixed sm:min-w-[640px]">
        <Table.Header>
          <Table.Row>
            <Table.Head className="sm:w-[42%]">Title</Table.Head>
            <Table.Head className="hidden sm:table-cell">{tab === "trash" ? "Moved to trash" : "Last edited"}</Table.Head>
            <Table.Head className="hidden w-24 text-right sm:table-cell">Size</Table.Head>
            <Table.Head className="w-24">Access</Table.Head>
            <Table.Head className="w-14">
              <span className="sr-only">Actions</span>
            </Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {rows.map((doc) => {
            const title = doc.title || "Untitled";
            const at = when(tab === "trash" && doc.trashed_ms ? doc.trashed_ms : doc.edited_ms);
            return (
              <Table.Row key={doc.id}>
                <Table.Cell className="truncate">
                  {tab === "trash" ? (
                    <span className="font-medium">{title}</span>
                  ) : (
                    <Link to={`/d/${doc.id}`} className="font-medium hover:text-kumo-link">
                      {title}
                    </Link>
                  )}
                  <span className="block truncate text-sm text-kumo-subtle sm:hidden">{at}</span>
                </Table.Cell>
                <Table.Cell className="hidden truncate text-sm sm:table-cell">
                  {at}
                  {tab !== "trash" && doc.edited_by && <span className="text-kumo-subtle"> by {doc.edited_by}</span>}
                </Table.Cell>
                <Table.Cell className="hidden text-right text-sm tabular-nums sm:table-cell">{bytes(doc.size)}</Table.Cell>
                <Table.Cell>
                  <Badge variant={ACCESS[doc.level].variant}>{ACCESS[doc.level].label}</Badge>
                </Table.Cell>
                <Table.Cell className="text-right">{tab === "trash" ? <TrashActions doc={doc} /> : <Actions doc={doc} />}</Table.Cell>
              </Table.Row>
            );
          })}
        </Table.Body>
      </Table>
    </div>
  );
}

/** Done: the lists again, and a toast. */
function useDone() {
  const queries = useQueryClient();
  const toasts = useKumoToastManager();
  return async (title: string) => {
    await queries.invalidateQueries({ queryKey: ["docs"] });
    toasts.add({ title, variant: "success" });
  };
}

function useFailed() {
  const toasts = useKumoToastManager();
  return (title: string) => (e: unknown) => void toasts.add({ title, description: message(e), variant: "error" });
}

function Actions({ doc }: { doc: DocRow }) {
  const navigate = useNavigate();
  const done = useDone();
  const failed = useFailed();
  const path = `docs/${encodeURIComponent(doc.id)}`;
  const title = doc.title || "Untitled";
  const exportText = () =>
    fetch(pageUrl(`api/${path}/markdown`), { credentials: "same-origin" })
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error("The server did not give the text."))))
      .then((text) => download(fileName(doc.title, doc.id), text), failed("No file was made"));
  const copyLink = () => navigator.clipboard.writeText(pageUrl(`d/${doc.id}`)).then(() => done("Link copied"), failed("The link was not copied"));
  return (
    <DropdownMenu>
      <DropdownMenu.Trigger render={<Button variant="ghost" size="sm" shape="square" icon={DotsThreeIcon} aria-label={`Actions of ${title}`} />} />
      <DropdownMenu.Content>
        <DropdownMenu.Item onClick={() => navigate(`/d/${doc.id}`)}>Open</DropdownMenu.Item>
        <DropdownMenu.Item onClick={() => void copyLink()}>Copy link</DropdownMenu.Item>
        <DropdownMenu.Item onClick={() => void exportText()}>Export .md</DropdownMenu.Item>
        <DropdownMenu.Separator />
        {doc.level === "owner" ? (
          <DropdownMenu.Item onClick={() => void call("DELETE", path).then(() => done(`${title} moved to trash`), failed("Not moved to trash"))}>Move to trash</DropdownMenu.Item>
        ) : (
          <DropdownMenu.Item onClick={() => void call("POST", `${path}/hide`).then(() => done(`${title} removed from your list`), failed("Not removed from your list"))}>Remove from my list</DropdownMenu.Item>
        )}
      </DropdownMenu.Content>
    </DropdownMenu>
  );
}

function TrashActions({ doc }: { doc: DocRow }) {
  const done = useDone();
  const failed = useFailed();
  const [asking, setAsking] = useState(false);
  const [busy, setBusy] = useState(false);
  const path = `docs/${encodeURIComponent(doc.id)}`;
  const title = doc.title || "Untitled";
  const purge = async () => {
    setBusy(true);
    try {
      await call("DELETE", `${path}?purge=1`);
      setAsking(false);
      await done(`${title} deleted`);
    } catch (e) {
      failed("Not deleted")(e);
    }
    setBusy(false);
  };
  return (
    <>
      <DropdownMenu>
        <DropdownMenu.Trigger render={<Button variant="ghost" size="sm" shape="square" icon={DotsThreeIcon} aria-label={`Actions of ${title}`} />} />
        <DropdownMenu.Content>
          <DropdownMenu.Item onClick={() => void call("POST", `${path}/untrash`).then(() => done(`${title} restored`), failed("Not restored"))}>Restore</DropdownMenu.Item>
          <DropdownMenu.Separator />
          <DropdownMenu.Item onClick={() => setAsking(true)}>Delete for good</DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu>
      <Dialog.Root role="alertdialog" open={asking} onOpenChange={setAsking}>
        <Dialog size="base" className="p-6 text-left">
          <Dialog.Title className="text-lg font-semibold">Delete {title} for good?</Dialog.Title>
          <Dialog.Description className="mt-2 text-base text-kumo-subtle">The text and its history go. You cannot undo this.</Dialog.Description>
          <div className="mt-6 flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setAsking(false)}>
              Cancel
            </Button>
            <Button variant="destructive" loading={busy} onClick={() => void purge()}>
              Delete for good
            </Button>
          </div>
        </Dialog>
      </Dialog.Root>
    </>
  );
}
