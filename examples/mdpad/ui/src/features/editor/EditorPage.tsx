import { Badge, Banner, Button, Dialog, DropdownMenu, Empty, Loader, Meter, Tabs, Tooltip, useKumoToastManager } from "@cloudflare/kumo";
import { ArrowLeftIcon, ClockCounterClockwiseIcon, DotsThreeIcon, InfoIcon, ListBulletsIcon, ShareNetworkIcon, WarningCircleIcon, WarningIcon } from "@phosphor-icons/react";
import { useQueryClient } from "@tanstack/react-query";
import type { EditorView } from "@codemirror/view";
import { useCallback, useEffect, useRef, useState, useSyncExternalStore, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";
import { Link, useNavigate, useParams } from "react-router";
import { at, bytes, call, download as save, fileName, guestPass, knownUser, loginPath, message, pageUrl, readImport, Refusal, rememberUser, store, stored, whoAmI, type GuestPass, type Version } from "../../api";
import { Avatars } from "./Avatars";
import { titleOf, type Notice, type Tone } from "./describe";
import { Editor } from "./Editor";
import { HistoryPanel, VersionView } from "./History";
import { Outline } from "./Outline";
import { Preview, type Heading } from "./Preview";
import { scrollEditorTo, scrollPaneTo, syncScroll } from "./scroll";
import { DocSession, type Snapshot } from "./session";
import { Share } from "./Share";

type Mode = "edit" | "both" | "view";

const BADGE: Record<Tone, "success" | "warning" | "error" | "neutral"> = { success: "success", warning: "warning", danger: "error", neutral: "neutral" };
const BANNER = { info: ["default", InfoIcon], warning: ["alert", WarningIcon], danger: ["error", WarningCircleIcon] } as const;

function useNarrow(): boolean {
  const query = "(max-width: 767px)";
  return useSyncExternalStore(
    (changed) => {
      const list = matchMedia(query);
      list.addEventListener("change", changed);
      return () => list.removeEventListener("change", changed);
    },
    () => matchMedia(query).matches,
  );
}

/** The editor's page: who the user is first, as the copy in this browser is theirs. A guest of a view link has no
 * account: the guest page gave this tab a token for the document. */
export function EditorPage() {
  const { id = "" } = useParams();
  const [user, setUser] = useState<string | null | undefined>(undefined);
  const [guest, setGuest] = useState<GuestPass | null>(null);

  useEffect(() => {
    whoAmI().then(
      ({ user }) => {
        rememberUser(user);
        setUser(user);
      },
      (e) => {
        const pass = guestPass(id);
        if (pass) {
          setGuest(pass);
          return setUser(pass.name);
        }
        if (e instanceof Refusal && e.status === 401) return location.replace(loginPath());
        // No server: the page opens the copy of the user who signed in last.
        setUser(knownUser());
      },
    );
  }, [id]);

  if (user === undefined) return <main className="h-dvh" aria-busy="true" />;
  if (user === null) {
    return (
      <main className="flex h-dvh items-center justify-center p-4">
        <Empty title="Sign in to open this document" description="This browser keeps no copy of it." contents={<Button onClick={() => location.assign(loginPath())}>Sign in</Button>} />
      </main>
    );
  }
  return <Opened key={`${user}/${id}`} id={id} user={user} guest={guest} />;
}

function Opened({ id, user, guest }: { id: string; user: string; guest: GuestPass | null }) {
  const [session, setSession] = useState<DocSession | null>(null);
  // Counts the restores: the history lists the versions again after each.
  const [restores, setRestores] = useState(0);
  const toasts = useKumoToastManager();
  // The manager is a new object each render: the session reads the latest, and is not made again for it.
  const toast = useRef(toasts);
  useEffect(() => {
    toast.current = toasts;
  }, [toasts]);

  useEffect(() => {
    const opened = new DocSession(id, user, guest);
    opened.onMerged = (count) => toast.current.add({ title: `Merged ${count} offline ${count === 1 ? "change" : "changes"}`, variant: "success" });
    opened.onRestored = (version) => {
      setRestores((n) => n + 1);
      toast.current.add({ title: `Restored version ${version}`, variant: "success" });
    };
    let mounted = true;
    // Shown once the copy in IndexedDB is in the doc: the editor never shows an empty text first.
    void opened.start().then(() => mounted && setSession(opened));
    return () => {
      mounted = false;
      opened.destroy();
    };
  }, [id, user, guest]);

  if (!session) return <main className="h-dvh" aria-busy="true" />;
  return <Page session={session} restores={restores} />;
}

function Page({ session, restores }: { session: DocSession; restores: number }) {
  const snapshot = useSyncExternalStore(session.subscribe, session.read);
  const { shown, title } = snapshot;
  const navigate = useNavigate();
  const narrow = useNarrow();
  const [mode, setMode] = useState<Mode>(() => {
    const kept = stored("mdpad:mode");
    return kept === "edit" || kept === "view" ? kept : "both";
  });
  const shownMode: Mode = narrow && mode === "both" ? "edit" : mode;
  const download = useCallback(() => {
    // The title of the text now: the shown one follows the typing by 200 ms.
    const text = session.text.toString();
    save(fileName(titleOf(text), session.id), text);
  }, [session]);
  const [copying, setCopying] = useState(false);
  const toasts = useKumoToastManager();
  const queries = useQueryClient();
  const path = `docs/${encodeURIComponent(session.id)}`;
  const copy = useCallback(async () => {
    setCopying(true);
    try {
      const { id } = await call<{ id: string }>("POST", `${path}/copy`);
      // The server has not got every change of this copy: the new document takes the text as it is here.
      if (session.provider?.pending) await call("PUT", `docs/${encodeURIComponent(id)}/markdown`, undefined, session.text.toString());
      navigate(`/d/${id}`);
    } catch (e) {
      toasts.add({ title: "No copy was made", description: message(e), variant: "error" });
      setCopying(false);
    }
  }, [navigate, path, session, toasts]);
  const guest = session.guest !== null;
  const canEdit = snapshot.view.level === "edit" && !snapshot.shown.readOnly;
  const [dialog, setDialog] = useState<"share" | "import" | null>(null);
  const [history, setHistory] = useState(false);
  const [outline, setOutline] = useState(() => stored("mdpad:outline") === "on");
  const toggleOutline = () => {
    store("mdpad:outline", outline ? null : "on");
    setOutline(!outline);
  };
  const [picked, setPicked] = useState<Version | null>(null);
  const closeHistory = () => {
    setHistory(false);
    setPicked(null);
  };
  const restored = (before: Version) => {
    setPicked(null);
    void queries.invalidateQueries({ queryKey: ["versions", session.id] });
    toasts.add({ title: `Restored. The text before it is version ${before.n}.`, variant: "success" });
  };
  const file = useRef<HTMLInputElement>(null);
  const [importing, setImporting] = useState<{ name: string; text: string } | null>(null);
  const choose = async (chosen: File | undefined) => {
    if (!chosen) return;
    try {
      setImporting({ name: chosen.name, text: await readImport(chosen) });
      setDialog("import");
    } catch (e) {
      toasts.add({ title: "Not imported", description: message(e), variant: "error" });
    }
  };

  useEffect(() => {
    document.title = `${title ?? "Untitled"} - mdpad`;
  }, [title]);

  useEffect(() => {
    // A guest's token ended: the guest page of the link gives a new one.
    if (shown.screen === "login") location.replace(session.guest ? at(`g/${session.guest.link}`) : loginPath());
  }, [session.guest, shown.screen]);

  const pickMode = (next: string) => {
    setMode(next as Mode);
    store("mdpad:mode", next);
  };
  const size = snapshot.view.size;
  // History and Share are for a signed-in user on the open editor; a phone has them in the More menu.
  const member = !guest && shown.screen === "editor";

  return (
    <div className="flex h-dvh flex-col bg-kumo-base">
      <header className="flex h-12 shrink-0 items-center gap-2 border-b border-kumo-hairline px-2 sm:px-3">
        <Link to="/" aria-label="Documents" className="flex size-8 shrink-0 items-center justify-center rounded-md text-kumo-subtle hover:bg-kumo-tint hover:text-kumo-default">
          <ArrowLeftIcon size={18} />
        </Link>
        <h1 className="min-w-0 truncate text-base font-semibold" data-testid="title">
          {title ?? "Untitled"}
        </h1>
        <Tooltip
          content={
            <span className="flex max-w-64 flex-col gap-2">
              {shown.badge.tip}
              {size && size.max > 0 && <Meter label="Size" value={Math.min(100, (size.bytes * 100) / size.max)} customValue={`${bytes(size.bytes)} of ${bytes(size.max)}`} />}
            </span>
          }
          render={<span tabIndex={0} className="shrink-0 rounded-full outline-offset-2" data-testid="badge" />}
        >
          <Badge variant={BADGE[shown.badge.tone]} appearance="dot" className="whitespace-nowrap">
            {shown.badge.label}
          </Badge>
        </Tooltip>
        <span className="flex-1" />
        <Avatars peers={snapshot.peers} viewers={snapshot.viewers} />
        <Tabs
          variant="segmented"
          size="sm"
          className="shrink-0"
          value={shownMode}
          onValueChange={pickMode}
          tabs={narrow ? [{ value: "edit", label: "Edit" }, { value: "view", label: "View" }] : [{ value: "edit", label: "Edit" }, { value: "both", label: "Both" }, { value: "view", label: "View" }]}
        />
        {shown.screen === "editor" && !narrow && (
          <Tooltip
            content="Outline"
            render={<Button variant={outline ? "secondary" : "ghost"} size="sm" shape="square" icon={ListBulletsIcon} aria-label="Outline" aria-pressed={outline} onClick={toggleOutline} />}
          />
        )}
        {member && !narrow && (
          <>
            <Tooltip
              content="History"
              render={<Button variant={history ? "secondary" : "ghost"} size="sm" shape="square" icon={ClockCounterClockwiseIcon} aria-label="History" aria-pressed={history} onClick={() => (history ? closeHistory() : setHistory(true))} />}
            />
            <Button variant="secondary" size="sm" icon={ShareNetworkIcon} aria-label="Share" onClick={() => setDialog("share")}>
              <span className="hidden sm:inline">Share</span>
            </Button>
          </>
        )}
        <DropdownMenu>
          <DropdownMenu.Trigger render={<Button variant="ghost" size="sm" shape="square" icon={DotsThreeIcon} aria-label="More" />} />
          <DropdownMenu.Content>
            {member && narrow && (
              <>
                <DropdownMenu.Item onClick={() => (history ? closeHistory() : setHistory(true))}>{history ? "Close history" : "History"}</DropdownMenu.Item>
                <DropdownMenu.Item onClick={() => setDialog("share")}>Share</DropdownMenu.Item>
              </>
            )}
            <DropdownMenu.Item onClick={download}>Export .md</DropdownMenu.Item>
            {!guest && <DropdownMenu.Item onClick={() => window.open(pageUrl(`p/${encodeURIComponent(session.id)}`), "_blank", "noopener")}>Open published page</DropdownMenu.Item>}
            {!guest && (
              <>
                <DropdownMenu.Item disabled={!canEdit || !snapshot.view.online} onClick={() => file.current?.click()}>
                  Import (replace)
                </DropdownMenu.Item>
                <DropdownMenu.Item disabled={copying || !snapshot.view.online} onClick={() => void copy()}>
                  Make a fresh copy
                </DropdownMenu.Item>
              </>
            )}
          </DropdownMenu.Content>
        </DropdownMenu>
        <input ref={file} type="file" accept=".md,.markdown,text/markdown,text/plain" className="hidden" aria-label="Markdown file" onChange={(e) => void choose(e.target.files?.[0]).finally(() => (e.target.value = ""))} />
      </header>
      {shown.screen === "editor" && <Notices notices={shown.notices} download={download} copy={copy} copying={copying} />}
      {shown.screen === "missing" ? (
        <Gone pending={snapshot.view.pending} download={download} />
      ) : shown.screen === "trash" ? (
        <Trashed session={session} owner={snapshot.owner} />
      ) : (
        <Body
          session={session}
          snapshot={snapshot}
          mode={shownMode}
          narrow={narrow}
          outline={outline && !narrow && !history}
          panel={history && <HistoryPanel id={session.id} refresh={restores} picked={picked?.n ?? null} canEdit={canEdit} onPick={setPicked} onClose={closeHistory} />}
          version={history && picked && <VersionView key={picked.n} id={session.id} version={picked} current={() => session.text.toString()} canEdit={canEdit} onBack={() => setPicked(null)} onRestored={restored} />}
        />
      )}
      {dialog === "share" && <Share id={session.id} onClose={() => setDialog(null)} />}
      {importing && (
        <Import
          path={path}
          file={importing}
          open={dialog === "import"}
          onClose={() => {
            setDialog(null);
            setImporting(null);
          }}
        />
      )}
    </div>
  );
}

/** Asks before an import replaces the text, then sends it: the server keeps the text before it as a version. */
function Import({ path, file, open, onClose }: { path: string; file: { name: string; text: string }; open: boolean; onClose: () => void }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const toasts = useKumoToastManager();
  const send = async () => {
    setBusy(true);
    setError(null);
    try {
      const { before } = await call<{ before: Version }>("PUT", `${path}/markdown`, undefined, file.text);
      toasts.add({ title: `Imported. The text before it is version ${before.n}.`, variant: "success" });
      onClose();
    } catch (e) {
      setError(message(e));
      setBusy(false);
    }
  };
  return (
    <Dialog.Root role="alertdialog" open={open} onOpenChange={(next) => !next && onClose()}>
      <Dialog size="base" className="p-6">
        <Dialog.Title className="text-lg font-semibold">Replace the text with {file.name}?</Dialog.Title>
        <Dialog.Description className="mt-2 text-base text-kumo-subtle">The text now is kept as a version first. Everyone sees the new text.</Dialog.Description>
        {error && <Banner className="mt-4" size="sm" variant="error" icon={<WarningCircleIcon weight="fill" />} role="alert" description={error} />}
        <div className="mt-6 flex justify-end gap-2">
          <Button variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="primary" loading={busy} onClick={() => void send()}>
            Replace
          </Button>
        </div>
      </Dialog>
    </Dialog.Root>
  );
}

function Notices({ notices, download, copy, copying }: { notices: Notice[]; download: () => void; copy: () => void; copying: boolean }) {
  if (!notices.length) return null;
  return (
    <div className="flex shrink-0 flex-col gap-1 border-b border-kumo-hairline px-2 py-1.5 sm:px-3" data-testid="notices">
      {notices.map((notice) => {
        const [variant, Icon] = BANNER[notice.tone];
        const action =
          notice.action === "download" ? (
            <Button size="sm" variant="secondary" onClick={download}>
              Download your text (.md)
            </Button>
          ) : notice.action === "copy" ? (
            <Button size="sm" variant="secondary" loading={copying} onClick={copy}>
              Make a fresh copy
            </Button>
          ) : undefined;
        return <Banner key={notice.key} size="sm" variant={variant} icon={<Icon weight="fill" />} role={notice.tone === "danger" ? "alert" : "status"} description={notice.text} action={action} data-notice={notice.key} />;
      })}
    </div>
  );
}

function Gone({ pending, download }: { pending: number; download: () => void }) {
  const navigate = useNavigate();
  return (
    <main className="flex flex-1 items-center justify-center p-4">
      <Empty
        title="This document does not exist"
        description={pending ? `${pending} of your changes were not saved. Download them to keep them.` : "Someone deleted it, or the link is wrong."}
        contents={pending ? <Button onClick={download}>Download your text (.md)</Button> : <Button onClick={() => navigate("/")}>Documents</Button>}
      />
    </main>
  );
}

function Trashed({ session, owner }: { session: DocSession; owner: boolean }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const restore = async () => {
    setBusy(true);
    setError(null);
    try {
      await call("POST", `docs/${encodeURIComponent(session.id)}/untrash`);
      session.restored();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <main className="flex flex-1 items-center justify-center p-4">
      <Empty
        title="This document is in the trash"
        description={error ?? (owner ? "Restore it to open it." : "Its owner can restore it.")}
        contents={
          owner ? (
            <Button loading={busy} onClick={() => void restore()}>
              Restore
            </Button>
          ) : undefined
        }
      />
    </main>
  );
}

/** The editor and the preview side by side, with a divider whose place the browser keeps. With the history open, its
 * panel takes the preview's place and a picked version the editor's; on a phone each takes the whole page. */
function Body({ session, snapshot, mode, narrow, outline, panel, version }: { session: DocSession; snapshot: Snapshot; mode: Mode; narrow: boolean; outline: boolean; panel: ReactNode; version: ReactNode }) {
  const main = useRef<HTMLElement>(null);
  const [view, setView] = useState<EditorView | null>(null);
  const [pane, setPane] = useState<HTMLElement | null>(null);
  // The heading ids the preview shows: the outline marks the one at the top among them.
  const [painted, setPainted] = useState<string[]>([]);
  const sync = useRef<{ stop: () => void; again: () => void } | null>(null);
  const onPaint = useCallback((host: HTMLElement) => {
    const ids = [...host.querySelectorAll<HTMLElement>(":is(h1, h2, h3)[id]")].map((h) => h.id);
    setPainted((before) => (before.join("\0") === ids.join("\0") ? before : ids));
    sync.current?.again();
  }, []);
  const [split, setSplit] = useState(() => {
    const at = Number(stored("mdpad:split"));
    return at >= 20 && at <= 80 ? at : 50;
  });
  const drag = (event: ReactPointerEvent<HTMLDivElement>) => {
    const box = main.current?.getBoundingClientRect();
    if (!box) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    let last = split;
    const move = (e: PointerEvent) => {
      last = Math.min(80, Math.max(20, ((e.clientX - box.left) * 100) / box.width));
      setSplit(last);
    };
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
      store("mdpad:split", String(Math.round(last)));
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };
  const loading = !snapshot.view.joined && snapshot.empty;
  const history = !!panel;
  const both = mode === "both" && !history;
  useEffect(() => {
    if (!both || !view || !pane) return;
    const synced = syncScroll(view, pane);
    sync.current = synced;
    return () => {
      synced.stop();
      sync.current = null;
    };
  }, [both, view, pane]);
  const jump = (heading: Heading) => {
    const target = pane?.querySelector<HTMLElement>(`#${CSS.escape(heading.id)}`);
    // The preview leads where it shows, and the editor follows it; else the editor goes to the line.
    if (target && pane) scrollPaneTo(pane, target);
    else if (view) scrollEditorTo(view, heading.line);
  };
  // The editor stays mounted while a version shows: its view and its place in the text are kept.
  const editor = !version && (history ? !narrow : mode !== "view");
  return (
    <main ref={main} className="flex min-h-0 flex-1">
      {outline && !version && <Outline text={session.text} pane={mode === "edit" ? null : pane} painted={painted} onJump={jump} />}
      {version}
      <section className={editor ? "relative min-w-0" : "hidden"} style={{ flexBasis: both ? `${split}%` : "100%", flexGrow: both ? 0 : 1, flexShrink: both ? 0 : 1 }} aria-label="Editor">
        <Editor session={session} readOnly={snapshot.shown.readOnly} onView={setView} />
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-kumo-base" data-testid="loading">
            <Loader />
          </div>
        )}
      </section>
      {both && <div role="separator" aria-orientation="vertical" aria-label="Move the divider" className="w-1 shrink-0 cursor-col-resize border-l border-kumo-hairline hover:bg-kumo-tint" onPointerDown={drag} />}
      {history && !(narrow && version) && panel}
      {!history && mode !== "edit" && (
        <section ref={setPane} className="min-w-0 flex-1 overflow-auto [overflow-anchor:auto]" aria-label="Preview" data-testid="preview-pane">
          {loading ? null : <Preview text={session.text} onPaint={onPaint} />}
        </section>
      )}
    </main>
  );
}
