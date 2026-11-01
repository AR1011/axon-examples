// A stand-in for the web node's API (../../src/web.rs) and its websocket (design §4.3), for the page's development and
// its tests. Every body has the shape web.rs gives. Any name signs in with the password "pw-<name>", so each test
// takes names of its own and does not meet another. A name that starts with "op-" is an operator's. Under /mock, a test sets what the server does: close the sockets
// with a code, send a notice, hold the saves, refuse the next joins.

import { randomBytes } from "node:crypto";
import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import { WebSocketServer } from "ws";
import * as Y from "yjs";

const COOKIE = "mdpad_session";
const MAX_SIZE = 256 * 1024;
const MAX_IMPORT = MAX_SIZE / 2;
const COLOURS = ["#2f6fdb", "#c2410c", "#15803d", "#9333ea", "#b45309", "#0e7490", "#be123c"];
const [SYNC, AWARENESS, HELLO, WELCOME, EDIT, SAVED, UPDATE, NOTICE] = [0, 1, 16, 17, 18, 19, 20, 21];

class Refusal extends Error {
  constructor(status, code, message, more = {}) {
    super(message);
    Object.assign(this, { status, code, more });
  }
}
const unauthorized = () => new Refusal(401, "unauthorized", "Sign in.");
const forbidden = (message = "You may not do this.") => new Refusal(403, "forbidden", message);
const notFound = (message = "No such document.") => new Refusal(404, "not_found", message);
const invalid = (message) => new Refusal(400, "invalid", message);

/** Passwords set by an invite or a change; any other name signs in with "pw-<name>". */
const passwords = new Map();
/** Each user's sign-in version: a new password ends the other sign-ins. */
const versions = new Map();
/** Invites used, by token. */
const usedInvites = new Set();
/** Users an operator disabled, and the users an operator added or that signed in: the admin page lists them. */
const disabled = new Set();
const known = new Set();
const isOperator = (name) => name.startsWith("op-");
const docs = new Map();

const titleOf = (text) => /^ {0,3}# +(.*?)(?: +#+)? *$/m.exec(text)?.[1].trim() ?? "";
const size = (doc) => Buffer.byteLength(doc.text.toString());
const b64 = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
const unb64 = (text) => {
  try {
    return JSON.parse(Buffer.from(text, "base64url").toString());
  } catch {
    return null;
  }
};

function newDoc(owner, markdown) {
  const id = `h1-${randomBytes(6).toString("hex")}`;
  const ydoc = new Y.Doc();
  ydoc.getText("md").insert(0, markdown);
  const doc = {
    id,
    owner,
    ydoc,
    text: ydoc.getText("md"),
    gen: 1,
    seq: 0,
    edited_ms: Date.now(),
    edited_by: owner,
    trashed_ms: null,
    members: new Map(),
    link: "off",
    published: false,
    linkVersion: 1,
    hidden: new Set(),
    versions: [],
    authors: new Set(),
    sockets: new Set(),
    hold: false,
    refuse: 0,
    max: MAX_SIZE,
  };
  docs.set(id, doc);
  return doc;
}

function levelOf(doc, who) {
  if (who.guest) return doc.link === "view" && doc.linkVersion === who.linkVersion ? "view" : "none";
  if (doc.owner === who.user) return "owner";
  return doc.members.get(who.user)?.level ?? "none";
}

/** The document, if `who` may `act`: read, edit or own. A trashed one opens for its owner only, to be restored. */
function allowed(id, who, act = "read") {
  const doc = docs.get(id);
  if (!doc) throw notFound();
  const level = levelOf(doc, who);
  if (doc.trashed_ms !== null && !(level === "owner" && act === "own")) throw new Refusal(403, "forbidden", "This document is in the trash.", { trashed: true });
  const may = { read: level !== "none", edit: level === "owner" || level === "edit", own: level === "owner" }[act];
  if (!may) throw forbidden();
  return doc;
}

function rowOf(doc, user) {
  const level = levelOf(doc, { user });
  return { id: doc.id, title: titleOf(doc.text.toString()), owner: doc.owner, edited_ms: doc.edited_ms, size: size(doc), level, trashed_ms: doc.trashed_ms, edited_by: doc.edited_by };
}

function saveVersion(doc, kind, name = "") {
  const text = doc.text.toString();
  const version = { n: (doc.versions.at(-1)?.n ?? 0) + 1, at_ms: Date.now(), kind, name, authors: [...doc.authors], bytes: Buffer.byteLength(text), markdown: text };
  doc.authors.clear();
  doc.versions.push(version);
  return version;
}
const publicVersion = ({ markdown: _, ...version }) => version;

// ---- the websocket ---------------------------------------------------------------------------------------------

const frame = (write) => {
  const e = encoding.createEncoder();
  write(e);
  return encoding.toUint8Array(e);
};
const json = (type, value) => frame((e) => (encoding.writeUint8(e, type), encoding.writeVarString(e, JSON.stringify(value))));
const update = (doc, bytes) =>
  frame((e) => {
    encoding.writeUint8(e, UPDATE);
    encoding.writeVarUint(e, doc.gen);
    encoding.writeVarUint(e, doc.seq);
    encoding.writeVarUint8Array(e, bytes);
  });

/** Applies an update to the document as `by`, and sends it to every socket but `from`, which gets its position. */
function edited(doc, bytes, by, from = null) {
  Y.applyUpdate(doc.ydoc, bytes);
  doc.seq += 1;
  doc.edited_ms = Date.now();
  doc.edited_by = by;
  doc.authors.add(by);
  for (const socket of doc.sockets) if (socket.joined) socket.send(update(doc, socket === from ? new Uint8Array() : bytes));
}

/** The text becomes `markdown`, as a forward edit by the server's own client. */
function rewrite(doc, markdown, by) {
  const before = Y.encodeStateVector(doc.ydoc);
  doc.ydoc.transact(() => {
    doc.text.delete(0, doc.text.length);
    doc.text.insert(0, markdown);
  });
  const diff = Y.encodeStateAsUpdate(doc.ydoc, before);
  doc.seq += 1;
  doc.edited_ms = Date.now();
  doc.edited_by = by;
  doc.authors.add(by);
  for (const socket of doc.sockets) if (socket.joined) socket.send(update(doc, diff));
}

const notice = (doc, value) => doc.sockets.forEach((socket) => socket.joined && socket.send(json(NOTICE, value)));

/** Closes the sockets `which` picks with `code`. */
const close = (doc, code, which = () => true) => [...doc.sockets].filter(which).forEach((socket) => socket.close(code));

/** Ends `user`'s sign-ins and closes their sockets on every document with 4401, as the server does. */
function signOut(user) {
  versions.set(user, (versions.get(user) ?? 1) + 1);
  for (const doc of docs.values()) close(doc, 4401, (socket) => socket.who === user && !socket.guest);
}

function viewers(doc) {
  const count = [...doc.sockets].filter((s) => s.joined && s.level === "view").length;
  notice(doc, { kind: "viewers", count });
}

/** The name and colour the server gives each entry of an awareness update: what a client says of itself is not shown. */
function renamed(bytes, name, colour) {
  const d = decoding.createDecoder(bytes);
  const entries = Array.from({ length: decoding.readVarUint(d) }, () => [decoding.readVarUint(d), decoding.readVarUint(d), decoding.readVarString(d)]);
  return frame((e) => {
    encoding.writeUint8(e, AWARENESS);
    encoding.writeVarUint8Array(
      e,
      frame((inner) => {
        encoding.writeVarUint(inner, entries.length);
        for (const [client, clock, state] of entries) {
          encoding.writeVarUint(inner, client);
          encoding.writeVarUint(inner, clock);
          const value = JSON.parse(state);
          encoding.writeVarString(inner, value === null ? "null" : JSON.stringify({ ...value, user: { name, color: colour } }));
        }
      }),
    );
  });
}

const sockets = new WebSocketServer({ noServer: true });

/** A websocket upgrade of `/api/docs/:id/ws`: refused with a close code after the upgrade, as web.rs does. */
export function upgrade(req, socket, head, base = "/") {
  const url = new URL(req.url, "http://mock");
  const match = url.pathname.slice(base.length - 1).match(/^\/api\/docs\/([^/]+)\/ws$/);
  if (!match) return socket.destroy();
  sockets.handleUpgrade(req, socket, head, (ws) => join(ws, decodeURIComponent(match[1]), whoOf(req, url)));
}

function join(ws, id, who) {
  const doc = docs.get(id);
  const refuse = (code) => ws.close(code);
  if (!who) return refuse(4401);
  if (!doc) return refuse(4404);
  const level = levelOf(doc, who);
  if (level === "none" || doc.trashed_ms !== null) return refuse(4403);
  if (doc.refuse) return refuse(doc.refuse);
  const name = who.guest ? who.name : who.user;
  Object.assign(ws, { joined: false, level: level === "view" ? "view" : "edit", who: name, colour: COLOURS[[...name].reduce((a, c) => a + c.charCodeAt(0), 0) % COLOURS.length], guest: !!who.guest, held: [] });
  doc.sockets.add(ws);
  ws.on("close", () => {
    doc.sockets.delete(ws);
    if (ws.clientId !== undefined) {
      const gone = frame((e) => (encoding.writeVarUint(e, 1), encoding.writeVarUint(e, ws.clientId), encoding.writeVarUint(e, 1 << 30), encoding.writeVarString(e, "null")));
      for (const other of doc.sockets) if (other.joined) other.send(frame((e) => (encoding.writeUint8(e, AWARENESS), encoding.writeVarUint8Array(e, gone))));
    }
    viewers(doc);
  });
  ws.on("message", (data) => {
    const d = decoding.createDecoder(new Uint8Array(data));
    const type = decoding.readUint8(d);
    if (type === HELLO) {
      ws.send(json(WELCOME, { level: ws.level, user: name, colour: ws.colour, max_update: 64 * 1024, size: size(doc), max_size: doc.max, resumed: false, generation: doc.gen }));
      ws.send(frame((e) => (encoding.writeUint8(e, SYNC), encoding.writeUint8(e, 1), encoding.writeVarUint8Array(e, Y.encodeStateAsUpdate(doc.ydoc)))));
      ws.send(frame((e) => (encoding.writeUint8(e, SYNC), encoding.writeUint8(e, 0), encoding.writeVarUint8Array(e, Y.encodeStateVector(doc.ydoc)))));
      ws.send(update(doc, new Uint8Array()));
      ws.joined = true;
      for (const other of doc.sockets) if (other !== ws && other.joined && other.aware) ws.send(other.aware);
      viewers(doc);
    } else if (type === EDIT) {
      const [seq, , bytes] = [decoding.readVarUint(d), decoding.readVarUint(d), decoding.readVarUint8Array(d)];
      // A changed client of a viewer sends an edit: the document refuses it (journey 4).
      if (ws.level !== "edit") return ws.close(4403);
      if (size(doc) + bytes.length > doc.max) return notice(doc, { kind: "full" });
      edited(doc, bytes, name, ws);
      const saved = frame((e) => (encoding.writeUint8(e, SAVED), encoding.writeVarUint(e, seq)));
      if (doc.hold) ws.held.push(saved);
      else ws.send(saved);
      if (size(doc) * 5 >= doc.max * 4) ws.send(json(NOTICE, { kind: "size", bytes: size(doc), max: doc.max }));
    } else if (type === AWARENESS && ws.level === "edit") {
      const bytes = decoding.readVarUint8Array(d);
      const entries = decoding.createDecoder(bytes);
      decoding.readVarUint(entries);
      ws.clientId ??= decoding.readVarUint(entries);
      ws.aware = renamed(bytes, name, ws.colour);
      for (const other of doc.sockets) if (other !== ws && other.joined) other.send(ws.aware);
    }
  });
}

// ---- the HTTP API ----------------------------------------------------------------------------------------------

function cookie(req) {
  return (req.headers.cookie ?? "").split(";").map((pair) => pair.trim().split("=")).find(([key]) => key === COOKIE)?.[1];
}

/** The signed-in user, or a guest of the document's view link. */
function whoOf(req, url) {
  const guest = url.searchParams.get("guest");
  const signed = unb64(cookie(req) ?? "");
  if (signed && (versions.get(signed.user) ?? 1) === signed.version && !disabled.has(signed.user)) return { user: signed.user };
  const pass = guest && unb64(guest);
  if (pass?.guest) return { guest: true, id: pass.id, linkVersion: pass.v, name: pass.name };
  return null;
}

function signIn(user, extra = {}) {
  known.add(user);
  const value = b64({ user, version: versions.get(user) ?? 1 });
  return [200, { user, ...extra }, { "set-cookie": `${COOKIE}=${value}; Path=/; HttpOnly; SameSite=Lax` }];
}

const linkToken = (doc) => b64({ id: doc.id, v: doc.linkVersion });
function openLink(token) {
  const link = unb64(token);
  const doc = link && docs.get(link.id);
  if (!doc || doc.linkVersion !== link.v || doc.link === "off" || doc.trashed_ms !== null) throw notFound("No such link.");
  return doc;
}

async function route(method, parts, url, body, text, req) {
  const path = parts.join("/");
  const who = whoOf(req, url);
  const user = () => {
    if (!who?.user) throw unauthorized();
    return who.user;
  };

  if (method === "POST" && path === "login") {
    const { name = "", password = "" } = body ?? {};
    if (!/^[a-z0-9_-]{1,32}$/.test(name) || disabled.has(name) || password !== (passwords.get(name) ?? `pw-${name}`)) throw new Refusal(401, "unauthorized", "Wrong name or password.");
    return signIn(name);
  }
  if (method === "POST" && path === "logout") return [200, {}, { "set-cookie": `${COOKIE}=; Path=/; Max-Age=0` }];
  if (method === "GET" && path === "me") return [200, cookie(req) === undefined ? { user: null, operator: false } : { user: user(), operator: isOperator(user()) }];
  if (parts[0] === "p" && parts.length === 2 && method === "GET") {
    const doc = docs.get(parts[1]);
    if (!doc?.published || doc.trashed_ms !== null) throw notFound("This page is not published.");
    return [200, doc.text.toString(), { "content-type": "text/markdown; charset=utf-8", "cache-control": "private, max-age=10" }];
  }
  if (parts[0] === "admin") return admin(method, parts.slice(1), body, user(), req);
  if (method === "POST" && path === "me/password") {
    const name = user();
    if (body?.old !== (passwords.get(name) ?? `pw-${name}`)) throw forbidden("The current password is wrong.");
    if ((body?.new ?? "").length < 8) throw invalid("A password has at least 8 characters.");
    passwords.set(name, body.new);
    versions.set(name, (versions.get(name) ?? 1) + 1);
    return signIn(name);
  }
  // An invite token is "inv-<name>": the mock's operator invites anyone once.
  if (parts[0] === "invite" && parts.length === 2) {
    const name = parts[1].startsWith("inv-") ? parts[1].slice(4) : "";
    if (!name || usedInvites.has(parts[1])) throw notFound("This invite is not valid. Ask your operator for a new one.");
    if (method === "GET") return [200, { user: name, new: !passwords.has(name) }];
    if ((body?.password ?? "").length < 8) throw invalid("A password has at least 8 characters.");
    usedInvites.add(parts[1]);
    passwords.set(name, body.password);
    return signIn(name);
  }
  if (method === "POST" && path === "guest") {
    const name = (body?.name ?? "").trim();
    if (!name || name.length > 40) throw invalid("Give a name of 1 to 40 characters.");
    const doc = openLink(body?.link ?? "");
    if (doc.link !== "view") throw forbidden("This link does not let guests in.");
    return [200, { id: doc.id, token: b64({ guest: true, id: doc.id, v: doc.linkVersion, name }) }];
  }
  if (parts[0] === "s" && parts.length >= 2) {
    const doc = openLink(parts[1]);
    if (method === "GET" && parts.length === 2) return [200, { id: doc.id, level: doc.link }];
    if (method === "POST" && parts[2] === "join") {
      const name = user();
      const level = levelOf(doc, { user: name });
      if (level !== "owner" && (level === "none" || (level === "view" && doc.link === "edit"))) doc.members.set(name, { level: doc.link, via: "link" });
      doc.hidden.delete(name);
      return [200, { id: doc.id }];
    }
  }
  if (path === "docs" && method === "GET") {
    const name = user();
    const tab = url.searchParams.get("tab") ?? "mine";
    const limit = Math.min(200, Math.max(1, Number(url.searchParams.get("limit") ?? 50)));
    const after = url.searchParams.get("after");
    const key = (doc) => `${String(Number.MAX_SAFE_INTEGER - doc.edited_ms).padStart(16, "0")}.${doc.id}`;
    const from = after ? `${String(Number.MAX_SAFE_INTEGER - Number(after.split(".")[0])).padStart(16, "0")}.${after.split(".").slice(1).join(".")}` : "";
    const rows = [...docs.values()]
      .filter((doc) => {
        const level = levelOf(doc, { user: name });
        if (tab === "trash") return level === "owner" && doc.trashed_ms !== null;
        if (doc.trashed_ms !== null) return false;
        return tab === "shared" ? level !== "owner" && level !== "none" && !doc.hidden.has(name) : level === "owner";
      })
      .sort((a, b) => key(a).localeCompare(key(b)))
      .filter((doc) => key(doc) > from)
      .slice(0, limit);
    const last = rows.at(-1);
    return [200, { docs: rows.map((doc) => rowOf(doc, name)), partial: false, next: rows.length === limit ? `${last.edited_ms}.${last.id}` : null }];
  }
  if (path === "docs" && method === "POST") {
    const markdown = body?.markdown ?? "";
    if (Buffer.byteLength(markdown) > MAX_IMPORT) throw new Refusal(413, "too_large", "The text is too large.");
    return [200, { id: newDoc(user(), markdown).id }];
  }
  if (parts[0] !== "docs" || parts.length < 2) throw notFound("No such route.");

  const id = parts[1];
  const rest = parts.slice(2).join("/");
  if (!who) throw unauthorized();
  if (!rest && method === "GET") {
    const doc = docs.get(id);
    // The owner sees a trashed document's info, to restore it.
    const shown = doc && levelOf(doc, who) === "owner" ? doc : allowed(id, who);
    return [200, { id, title: titleOf(shown.text.toString()), owner: shown.owner, level: levelOf(shown, who), size: size(shown), max_size: shown.max, trashed_ms: shown.trashed_ms }];
  }
  if (!rest && method === "DELETE") {
    const name = user();
    const doc = allowed(id, { user: name }, "own");
    if (url.searchParams.get("purge") === "1") {
      if (doc.trashed_ms === null) throw forbidden("Move the document to the trash first.");
      close(doc, 4404);
      docs.delete(id);
    } else {
      doc.trashed_ms = Date.now();
      close(doc, 4403);
    }
    return [200, {}];
  }
  if (rest === "untrash" && method === "POST") {
    allowed(id, { user: user() }, "own").trashed_ms = null;
    return [200, {}];
  }
  if (rest === "hide" && method === "POST") {
    const name = user();
    const doc = allowed(id, { user: name });
    if (levelOf(doc, { user: name }) === "owner") throw forbidden("Only a document shared with you can be removed from your list.");
    doc.hidden.add(name);
    return [200, {}];
  }
  if (rest === "markdown" && method === "GET") return [200, allowed(id, who).text.toString(), { "content-type": "text/markdown; charset=utf-8" }];
  if (rest === "markdown" && method === "PUT") {
    const name = user();
    const doc = allowed(id, { user: name }, "edit");
    if (Buffer.byteLength(text) > MAX_IMPORT) throw new Refusal(413, "too_large", "The text is too large.");
    const before = saveVersion(doc, "before_import");
    rewrite(doc, text, `${name} (import)`);
    return [200, { before: publicVersion(before) }];
  }
  if (rest === "copy" && method === "POST") {
    const name = user();
    const doc = allowed(id, { user: name });
    const copy = newDoc(name, doc.text.toString());
    for (const [member, m] of doc.members) if (member !== name) copy.members.set(member, { level: m.level, via: "invited" });
    if (doc.owner !== name) copy.members.set(doc.owner, { level: "edit", via: "invited" });
    return [200, { id: copy.id }];
  }
  if (rest === "versions" && method === "GET") {
    if (who.guest) throw forbidden();
    return [200, { versions: allowed(id, who).versions.map(publicVersion).reverse() }];
  }
  if (rest === "versions" && method === "POST") {
    const doc = allowed(id, { user: user() }, "edit");
    if (!(body?.name ?? "").trim()) throw invalid("Give the version a name.");
    return [200, { version: publicVersion(saveVersion(doc, "named", body.name.trim().slice(0, 80))) }];
  }
  if (parts[2] === "versions" && parts.length === 4 && method === "GET") {
    if (who.guest) throw forbidden();
    const version = allowed(id, who).versions.find((v) => v.n === Number(parts[3]));
    if (!version) throw notFound("No such version.");
    return [200, { version: publicVersion(version), markdown: version.markdown }];
  }
  if (parts[2] === "versions" && parts[4] === "restore" && method === "POST") {
    const name = user();
    const doc = allowed(id, { user: name }, "edit");
    const version = doc.versions.find((v) => v.n === Number(parts[3]));
    if (!version) throw notFound("No such version.");
    const before = saveVersion(doc, "before_restore");
    rewrite(doc, version.markdown, `${name} (restore)`);
    notice(doc, { kind: "restored", version: version.n });
    return [200, { before: publicVersion(before) }];
  }
  if (rest.startsWith("share") || rest.startsWith("members")) {
    const name = user();
    if (rest === "share" && method === "GET") {
      const doc = allowed(id, { user: name });
      const owner = doc.owner === name;
      return [200, { owner: doc.owner, link: doc.link, published: doc.published, token: owner ? linkToken(doc) : null, members: [...doc.members].sort(([a], [b]) => a.localeCompare(b)).map(([member, m]) => ({ user: member, ...m })) }];
    }
    const doc = allowed(id, { user: name }, "own");
    const guests = () => close(doc, 4403, (socket) => socket.guest);
    if (rest === "share" && method === "PUT") {
      if (body?.link === undefined && body?.published === undefined) throw invalid("Give link or published.");
      if (body.published !== undefined) doc.published = !!body.published;
      if (body.link !== undefined) {
        if (!["off", "view", "edit"].includes(body.link)) throw invalid("link is off, view or edit.");
        doc.link = body.link;
        if (doc.link !== "view") guests();
      }
    } else if (rest === "share/reset" && method === "POST") {
      doc.linkVersion += 1;
      guests();
    } else if (rest === "share/remove-link-members" && method === "POST") {
      const gone = [...doc.members].filter(([, m]) => m.via === "link").map(([member]) => member);
      gone.forEach((member) => doc.members.delete(member));
      close(doc, 4403, (socket) => gone.includes(socket.who) && !socket.guest);
    } else if (parts[2] === "members" && parts.length === 4) {
      const member = parts[3];
      if (method === "PUT") {
        if (!["view", "edit"].includes(body?.level)) throw invalid("level is view or edit.");
        if (!/^[a-z0-9_-]{1,32}$/.test(member) || member.startsWith("nobody")) throw notFound("No such user.");
        if (member === doc.owner) throw invalid("The owner has every right.");
        doc.members.set(member, { level: body.level, via: doc.members.get(member)?.via ?? "invited" });
        // As the doc does: an open socket keeps going at the new level, and its next edit is refused.
        for (const socket of doc.sockets) if (socket.who === member && !socket.guest) socket.level = body.level;
      } else if (method === "DELETE") {
        doc.members.delete(member);
        close(doc, 4403, (socket) => socket.who === member && !socket.guest);
      } else throw notFound("No such route.");
    } else throw notFound("No such route.");
    return [200, {}];
  }
  throw notFound("No such route.");
}

/** The operators' routes, as web.rs answers them. */
function admin(method, parts, body, name, req) {
  if (!isOperator(name)) throw forbidden("Operators only.");
  const path = parts.join("/");
  if (path === "users" && method === "GET") {
    const users = [...new Set([...known, ...[...docs.values()].map((d) => d.owner)])].sort();
    return [200, { users: users.map((user) => ({ name: user, disabled: disabled.has(user), operator: isOperator(user) })) }];
  }
  if (path === "users" && method === "POST") {
    const user = body?.name ?? "";
    if (!/^[a-z0-9_-]{1,32}$/.test(user)) throw invalid("A name is 1 to 32 of a-z, 0-9, - and _.");
    if (known.has(user)) throw new Refusal(409, "invalid", `${user} exists.`);
    const path = `/i/inv-${user}`;
    return [200, { name: user, path, link: `http://${req.headers.host}${path}`, expires_ms: Date.now() + 7 * 86_400_000 }];
  }
  if (parts[0] === "users" && parts.length === 2 && method === "PUT") {
    const user = parts[1];
    if (!known.has(user)) throw notFound("No such user.");
    if (body?.disabled === true) {
      disabled.add(user);
      for (const doc of docs.values()) close(doc, 4401, (socket) => socket.who === user && !socket.guest);
    } else if (body?.disabled === false) disabled.delete(user);
    return [200, {}];
  }
  if (parts[0] === "docs" && parts[2] === "owner" && method === "PUT") {
    const doc = docs.get(parts[1]);
    if (!doc) throw notFound();
    const user = body?.user ?? "";
    if (!known.has(user)) throw notFound("No such user.");
    if (user !== doc.owner) {
      doc.members.delete(user);
      doc.members.set(doc.owner, { level: "edit", via: "invited" });
      doc.owner = user;
    }
    return [200, {}];
  }
  if (path === "homes" && method === "GET") {
    const open = [...docs.values()].filter((d) => d.sockets.size > 0);
    const home = { home: "h1", open: open.length, docs: open.map((d) => ({ id: d.id, title: titleOf(d.text.toString()), owner: d.owner })), more: false, stored: docs.size, saved: 0, log_bytes: 4096, log_live_bytes: 2048, log_segments: 1, failed_writes: 0, streams: open.length * 2 };
    const sessions = open.reduce((n, d) => n + d.sockets.size, 0);
    return [200, { nodes: [{ id: "h1", roles: ["home", "web", "accounts"], available: true, address: req.headers.host }], homes: [home, { home: "h2", error: "The node did not answer." }], webs: [{ node: "h1", sessions, users: known.size, streams: open.length * 2 }] }];
  }
  throw notFound("No such route.");
}

/** The test hooks: `POST /mock/docs/:id/<what>`. */
function hook(parts, body) {
  const doc = docs.get(parts[2]);
  if (parts[1] !== "docs" || !doc) throw notFound();
  const what = parts[3];
  if (what === "text") return [200, { text: doc.text.toString() }];
  if (what === "close") {
    // The server closes 4401 only once a session has ended: end the sessions of the users on it too.
    if (body.code === 4401) new Set([...doc.sockets].filter((s) => !s.guest).map((s) => s.who)).forEach(signOut);
    close(doc, body.code);
  } else if (what === "notice") notice(doc, body);
  else if (what === "refuse") doc.refuse = body.code;
  else if (what === "max") doc.max = body.max;
  else if (what === "version") saveVersion(doc, "auto");
  else if (what === "type") {
    // Another user types at the end of the text.
    const other = new Y.Doc();
    Y.applyUpdate(other, Y.encodeStateAsUpdate(doc.ydoc));
    const before = Y.encodeStateVector(other);
    other.getText("md").insert(other.getText("md").length, body.text);
    edited(doc, Y.encodeStateAsUpdate(other, before), body.user ?? "ben");
  } else if (what === "hold") {
    doc.hold = body.on;
    if (!doc.hold) for (const socket of doc.sockets) socket.held.splice(0).forEach((saved) => socket.send(saved));
  } else throw notFound("No such hook.");
  return [200, { text: doc.text.toString() }];
}

function send(res, [status, body, headers = {}]) {
  const text = typeof body === "string";
  res.writeHead(status, { "content-type": text ? "text/plain" : "application/json", "cache-control": "no-store", ...headers }).end(text ? body : JSON.stringify(body));
}

/** Answers a request of the API or a test hook, under `base`; false for any other path. */
export async function handle(req, res, base = "/") {
  const url = new URL(req.url, "http://mock");
  const local = url.pathname.slice(base.length - 1);
  const api = local.startsWith("/api/");
  if (!api && !local.startsWith("/mock/")) return false;
  const parts = local.slice(api ? 5 : 1).split("/").filter(Boolean).map(decodeURIComponent);
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const text = Buffer.concat(chunks).toString();
  try {
    const isJson = (req.headers["content-type"] ?? "").includes("json");
    const body = isJson && text ? JSON.parse(text) : undefined;
    send(res, api ? await route(req.method, parts, url, body, text, req) : hook(parts, body ?? JSON.parse(text || "{}")));
  } catch (e) {
    if (!(e instanceof Refusal)) console.error(e);
    send(res, e instanceof Refusal ? [e.status, { error: e.code, message: e.message, ...e.more }] : [500, { error: "unavailable", message: String(e) }]);
  }
  return true;
}
