/** What the API refused: its status, its code (design §4.2) and its sentence. Status 0: the server was not reached. */
export class Refusal extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    /** The whole answer: some refusals say more. */
    readonly detail: Record<string, unknown> = {},
  ) {
    super(message);
  }
}

/** What an error says, to show. */
export const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

/** A path of the page, under the base the server gave it, with its query. */
export function at(path: string): string {
  const url = new URL(path.replace(/^\//, ""), document.baseURI);
  return url.pathname + url.search;
}

/** A call of the API with a JSON body, or with `text` as the body as it is (an import's Markdown). */
export async function call<T>(method: string, path: string, body?: unknown, text?: string): Promise<T> {
  let response: Response;
  try {
    response = await fetch(at(`api/${path}`), {
      method,
      credentials: "same-origin",
      headers: text !== undefined ? { "content-type": "text/markdown; charset=utf-8" } : body === undefined ? {} : { "content-type": "application/json" },
      body: text ?? (body === undefined ? undefined : JSON.stringify(body)),
    });
  } catch {
    throw new Refusal(0, "offline", "Cannot connect to the server.");
  }
  const answer = await response.text();
  let json: Record<string, unknown> = {};
  try {
    json = answer ? JSON.parse(answer) : {};
  } catch {
    /* a proxy's page, not the API's answer */
  }
  if (!response.ok) {
    throw new Refusal(response.status, String(json.error ?? "unavailable"), String(json.message ?? response.statusText), json);
  }
  return json as T;
}

/** Gives the login page the place to come back to when the API says to sign in. */
export function signInOn401<T>(ask: () => Promise<T>): () => Promise<T> {
  return () =>
    ask().catch((e) => {
      if (e instanceof Refusal && e.status === 401) location.replace(loginPath());
      throw e;
    });
}

/** The page for signing in, and back to `here` after: a path under the base, this page's by default. */
export function loginPath(here?: string): string {
  const base = at("/");
  const path = location.pathname + location.search;
  const next = here ?? (path.startsWith(base) ? path.slice(base.length) : path);
  return `${at("login")}?next=${encodeURIComponent(next.replace(/^\//, ""))}`;
}

/** A value this browser keeps in localStorage; without storage (a private window) nothing is kept. */
export function stored(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}
export function store(key: string, value: string | null): void {
  try {
    if (value === null) localStorage.removeItem(key);
    else localStorage.setItem(key, value);
  } catch {
    /* not kept */
  }
}

/** The user of the last sign-in in this browser: a page opened offline uses it to find its saved copies. */
export const knownUser = () => stored("mdpad:user");
export const rememberUser = (user: string | null) => store("mdpad:user", user);

export interface DocInfo {
  id: string;
  title: string;
  owner: string;
  level: "owner" | "edit" | "view" | "none";
  size: number;
  max_size: number;
  trashed_ms: number | null;
}

export interface DocRow {
  id: string;
  title: string;
  owner: string;
  edited_ms: number;
  /** Who changed the text last; empty when not known. */
  edited_by: string;
  size: number;
  level: "owner" | "edit" | "view" | "none";
  trashed_ms: number | null;
}

export type VersionKind = "auto" | "named" | "before_restore" | "before_offline" | "before_import";

export interface Version {
  n: number;
  at_ms: number;
  kind: VersionKind;
  name: string;
  /** Who changed the text since the version before. */
  authors: string[];
  /** The Markdown's length in bytes. */
  bytes: number;
}

export interface Member {
  user: string;
  level: "view" | "edit";
  via: "invited" | "link";
}

export interface Share {
  owner: string;
  link: "off" | "view" | "edit";
  /** Anyone may read the text at /p/<id>. */
  published: boolean;
  /** The link's secret: the owner's to see only. */
  token: string | null;
  members: Member[];
}

/** The full address of a page of this app. */
export const pageUrl = (path: string) => new URL(at(path), location.origin).href;

/** A guest's token for one document, from the guest page, for this tab only. */
export interface GuestPass {
  token: string;
  name: string;
  /** The link's token: its guest page gives a new pass when this one ends. */
  link: string;
}
export function guestPass(id: string): GuestPass | null {
  try {
    const kept = JSON.parse(sessionStorage.getItem(`mdpad:guest:${id}`) ?? "null");
    return kept && typeof kept.token === "string" ? kept : null;
  } catch {
    return null;
  }
}
export function keepGuestPass(id: string, pass: GuestPass): void {
  try {
    sessionStorage.setItem(`mdpad:guest:${id}`, JSON.stringify(pass));
  } catch {
    /* the editor asks again */
  }
}

/** Saves `text` as a file the browser downloads. */
export function download(name: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/markdown;charset=utf-8" }));
  const link = Object.assign(document.createElement("a"), { href: url, download: name });
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

/** A file name from a document's title, or its id. */
export const fileName = (title: string | null | undefined, id: string) => `${(title ?? "").replace(/[\\/:*?"<>|]+/g, " ").trim().slice(0, 80) || id}.md`;

/** A size in bytes, short. */
export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(n < 10 * 1024 ? 1 : 0)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

/** A time, short: the time today, else the date. */
export function when(ms: number): string {
  const date = new Date(ms);
  const today = new Date().toDateString() === date.toDateString();
  return today ? date.toLocaleTimeString(undefined, { timeStyle: "short" }) : date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

/** The largest Markdown file an import takes. */
const MAX_IMPORT = 128 * 1024;

/** The text of a Markdown file to import: refused past the size an import takes. */
export async function readImport(file: File): Promise<string> {
  if (file.size > MAX_IMPORT) throw new Error(`The file is larger than ${bytes(MAX_IMPORT)}.`);
  return file.text();
}

/** Who is signed in. An operator also has the admin page. */
export interface Me {
  user: string;
  operator: boolean;
}

/** Who is signed in, or a 401 `Refusal`. With no sign-in at all the server answers `user: null`, not 401, so a page
 * that asks before it knows shows no refused call in the console. */
export async function whoAmI(): Promise<Me> {
  const me = await call<Me | { user: null }>("GET", "me");
  if (me.user === null) throw new Refusal(401, "unauthorized", "Sign in.");
  return me as Me;
}
