import { Badge, Banner, Button, Dialog, Input, Loader, Popover, Tabs, useKumoToastManager } from "@cloudflare/kumo";
import { markdown } from "@codemirror/lang-markdown";
import { defaultHighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { unifiedMergeView } from "@codemirror/merge";
import { EditorState } from "@codemirror/state";
import { EditorView } from "@codemirror/view";
import { ArrowLeftIcon, WarningCircleIcon, XIcon } from "@phosphor-icons/react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { bytes, call, message, when, type Version, type VersionKind } from "../../api";
import { render } from "./Preview";

const KIND: Record<VersionKind, { label: string; variant: "primary" | "secondary" | "outline" }> = {
  auto: { label: "Auto", variant: "outline" },
  named: { label: "Named", variant: "primary" },
  before_restore: { label: "Before restore", variant: "secondary" },
  before_offline: { label: "Before offline changes", variant: "secondary" },
  before_import: { label: "Before import", variant: "secondary" },
};

const versionsOf = (id: string) => `docs/${encodeURIComponent(id)}/versions`;

/** The document's versions, newest first. `refresh` changes when they may have changed. */
export function useVersions(id: string, refresh: number) {
  const queries = useQueryClient();
  const versions = useQuery({ queryKey: ["versions", id], queryFn: () => call<{ versions: Version[] }>("GET", versionsOf(id)).then((a) => a.versions), refetchOnMount: "always" });
  useEffect(() => {
    if (refresh) void queries.invalidateQueries({ queryKey: ["versions", id] });
  }, [id, queries, refresh]);
  return versions;
}

/** The list of versions: a panel at the right of the editor, or the whole page on a phone. */
export function HistoryPanel({ id, refresh, picked, canEdit, onPick, onClose }: { id: string; refresh: number; picked: number | null; canEdit: boolean; onPick: (version: Version) => void; onClose: () => void }) {
  const versions = useVersions(id, refresh);
  const list = versions.data ?? [];
  return (
    <aside className="flex min-h-0 w-full flex-col border-kumo-hairline bg-kumo-elevated md:w-[360px] md:shrink-0 md:border-l" aria-label="History" data-testid="history">
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-kumo-hairline pr-2 pl-4">
        <h2 className="mr-auto text-base font-semibold">History</h2>
        {canEdit && <NameVersion id={id} />}
        <Button variant="ghost" size="sm" shape="square" icon={XIcon} aria-label="Close history" onClick={onClose} />
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        {versions.error ? (
          <Banner className="m-3" size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={message(versions.error)} />
        ) : versions.isPending ? (
          <div className="flex justify-center p-6">
            <Loader />
          </div>
        ) : !list.length ? (
          <div className="flex flex-col gap-1 px-4 py-5">
            <p className="text-base font-medium">No versions yet</p>
            <p className="text-sm text-kumo-subtle">A version is saved after 10 minutes with no edits.</p>
          </div>
        ) : (
          <ol className="flex flex-col py-1">
            {list.map((v, i) => {
              const older = list[i + 1];
              const change = older ? v.bytes - older.bytes : v.bytes;
              const selected = v.n === picked;
              return (
                <li key={v.n}>
                  <button
                    type="button"
                    aria-current={selected || undefined}
                    data-version={v.n}
                    onClick={() => onPick(v)}
                    className={`flex w-full flex-col gap-1 px-4 py-2.5 text-left hover:bg-kumo-tint ${selected ? "bg-kumo-tint" : ""}`}
                  >
                    <span className="flex items-center gap-2">
                      <span className="font-medium">{v.name || when(v.at_ms)}</span>
                      <span className="flex-1" />
                      <Badge variant={KIND[v.kind].variant}>{KIND[v.kind].label}</Badge>
                    </span>
                    <span className="flex gap-2 text-sm text-kumo-subtle">
                      <span className="min-w-0 truncate">{[v.name ? when(v.at_ms) : null, v.authors.join(", ") || null].filter(Boolean).join(" · ")}</span>
                      <span className="ml-auto shrink-0 tabular-nums">{change >= 0 ? `+${bytes(change)}` : `-${bytes(-change)}`}</span>
                    </span>
                  </button>
                </li>
              );
            })}
          </ol>
        )}
      </div>
    </aside>
  );
}

function NameVersion({ id }: { id: string }) {
  const [open, setOpen] = useState(false);
  const queries = useQueryClient();
  const toasts = useKumoToastManager();
  const name = useMutation({
    mutationFn: (name: string) => call<{ version: Version }>("POST", versionsOf(id), { name }),
    onSuccess: async ({ version }) => {
      setOpen(false);
      await queries.invalidateQueries({ queryKey: ["versions", id] });
      toasts.add({ title: `Saved version ${version.name}`, variant: "success" });
    },
  });
  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const value = String(new FormData(event.currentTarget).get("name") ?? "").trim();
    if (value) name.mutate(value);
  };
  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (!next) name.reset();
      }}
    >
      <Popover.Trigger render={<Button variant="secondary" size="sm" />}>Name this version</Popover.Trigger>
      <Popover.Content className="w-72">
        <form onSubmit={submit} className="flex flex-col gap-3">
          <Input label="Name" name="name" maxLength={80} required autoFocus autoComplete="off" />
          {name.error && <Banner size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={message(name.error)} />}
          <div className="flex justify-end">
            <Button type="submit" variant="primary" size="sm" loading={name.isPending}>
              Save version
            </Button>
          </div>
        </form>
      </Popover.Content>
    </Popover>
  );
}

/** One version, read-only, in place of the editor: as rendered, or as the changes from the current text. */
export function VersionView({ id, version, current, canEdit, onBack, onRestored }: { id: string; version: Version; current: () => string; canEdit: boolean; onBack: () => void; onRestored: (before: Version) => void }) {
  const [tab, setTab] = useState<"rendered" | "changes">("rendered");
  const [asking, setAsking] = useState(false);
  const text = useQuery({
    queryKey: ["version", id, version.n],
    queryFn: () => call<{ markdown: string }>("GET", `${versionsOf(id)}/${version.n}`).then((a) => a.markdown),
    staleTime: Infinity,
  });
  // The text now, as it was when the version was picked: the changes do not move while others type.
  const now = useMemo(() => current(), [current, version.n]); // eslint-disable-line react-hooks/exhaustive-deps
  const restore = useMutation({
    mutationFn: () => call<{ before: Version }>("POST", `${versionsOf(id)}/${version.n}/restore`),
    onSuccess: ({ before }) => {
      setAsking(false);
      onRestored(before);
    },
  });
  const title = version.name || when(version.at_ms);
  return (
    <section className="flex min-h-0 min-w-0 flex-1 flex-col" aria-label={`Version ${title}`} data-testid="version">
      <div className="flex shrink-0 flex-wrap items-center gap-2 border-b border-kumo-hairline px-2 py-1.5 sm:px-3">
        <Button variant="ghost" size="sm" icon={ArrowLeftIcon} onClick={onBack}>
          Back to the text
        </Button>
        <span className="min-w-0 truncate font-medium">{title}</span>
        <span className="flex-1" />
        <Tabs
          variant="segmented"
          size="sm"
          value={tab}
          onValueChange={(next) => setTab(next as typeof tab)}
          tabs={[
            { value: "rendered", label: "Rendered" },
            { value: "changes", label: "Changes" },
          ]}
        />
        {canEdit && (
          <Button variant="primary" size="sm" disabled={text.data === undefined} onClick={() => setAsking(true)}>
            Restore
          </Button>
        )}
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        {text.error ? (
          <Banner className="m-3" size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={message(text.error)} />
        ) : text.data === undefined ? (
          <div className="flex justify-center p-8">
            <Loader />
          </div>
        ) : tab === "rendered" ? (
          <div className="md-preview mx-auto max-w-[760px] px-6 py-4" data-testid="version-rendered" dangerouslySetInnerHTML={{ __html: render(text.data) }} />
        ) : (
          <Diff original={now} text={text.data} />
        )}
      </div>
      <Dialog.Root role="alertdialog" open={asking} onOpenChange={setAsking}>
        <Dialog size="base" className="p-6">
          <Dialog.Title className="text-lg font-semibold">Restore {title}?</Dialog.Title>
          <Dialog.Description className="mt-2 text-base text-kumo-subtle">The text now is kept as a version first. Everyone sees the restored text.</Dialog.Description>
          {restore.error && <Banner className="mt-4" size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={message(restore.error)} />}
          <div className="mt-6 flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setAsking(false)}>
              Cancel
            </Button>
            <Button variant="primary" loading={restore.isPending} onClick={() => restore.mutate()}>
              Restore
            </Button>
          </div>
        </Dialog>
      </Dialog.Root>
    </section>
  );
}

/** The version's text, with what differs from the text now marked: what a restore changes. */
function Diff({ original, text }: { original: string; text: string }) {
  const host = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const view = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc: text,
        extensions: [
          EditorView.lineWrapping,
          markdown(),
          syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
          EditorState.readOnly.of(true),
          EditorView.editable.of(false),
          EditorView.contentAttributes.of({ "aria-label": "Changes" }),
          unifiedMergeView({ original, mergeControls: false, gutter: true }),
        ],
      }),
    });
    return () => view.destroy();
  }, [original, text]);
  return <div ref={host} className="min-h-full" data-testid="changes" />;
}
