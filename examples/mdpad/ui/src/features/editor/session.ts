// One open document: its Y.Doc, the copy in IndexedDB, the socket, and the others' cursors. The page reads it as a
// snapshot (`useSyncExternalStore`), so one change of state renders once. Each load has a new Y.Doc, so a new client
// id: two tabs never write as one client.

import * as decoding from 'lib0/decoding'
import { applyAwarenessUpdate, Awareness, encodeAwarenessUpdate, removeAwarenessStates } from 'y-protocols/awareness'
import * as Y from 'yjs'
import { at, call, Refusal, type DocInfo, type GuestPass } from '../../api'
import { Close, Provider, TEXT, type Level } from '../../editor/provider'
import { Copy } from './cache'
import { describe, GRACE_MS, titleOf, type Shown, type View } from './describe'

/** Another user with the document open: their tabs count once. */
export interface Peer {
  name: string
  colour: string
}

export interface Snapshot {
  shown: Shown
  view: View
  /** The doc has no text. */
  empty: boolean
  title: string | null
  user: string
  /** The user owns the document: they may restore it from the trash. */
  owner: boolean
  peers: Peer[]
  viewers: number
}

/** Awareness goes to the server at most this often. */
const AWARE_EVERY_MS = 100

export class DocSession {
  readonly doc = new Y.Doc()
  readonly text = this.doc.getText(TEXT)
  readonly awareness = new Awareness(this.doc)
  readonly undo = new Y.UndoManager(this.text)
  provider: Provider | null = null

  private readonly copy: Copy
  private readonly listeners = new Set<() => void>()
  private readonly timers = new Set<ReturnType<typeof setTimeout>>()
  private snapshot: Snapshot
  private state = {
    level: null as Level | null,
    unreachable: false,
    cannotSave: false,
    near: false,
    size: null as View['size'],
    trashed: false,
    missing: false,
    owner: false,
    joined: false,
    troubleSince: null as number | null,
    title: null as string | null,
  }
  private recheck: ReturnType<typeof setTimeout> | null = null
  private titleDue = false
  private awareDue = false
  private awareSent = 0
  /** When the server last named each client: an entry it sends again unchanged leaves the awareness untouched. */
  private readonly heard = new Map<number, number>()
  /** When the socket last opened: the server names every cursor it has after that, before or after the state. */
  private joinedAt = 0
  /** Times the socket went live: a break's grace that ends after the socket came back clears nothing. */
  private lives = 0
  private destroyed = false
  /** Called with N when N changes made offline are saved. */
  onMerged: (count: number) => void = () => {}
  /** Called with N when someone restores version N. */
  onRestored: (version: number) => void = () => {}

  /** `guest`: a guest of a view link, under the name they gave; their copies are apart from every user's. */
  constructor(
    readonly id: string,
    readonly user: string,
    readonly guest: GuestPass | null = null,
  ) {
    this.copy = new Copy(guest ? '~guest' : user, id, this.doc)
    this.awareness.setLocalState({})
    this.awareness.on('update', ({ added, updated, removed }: Record<string, number[]>, origin: unknown) => {
      if (origin === 'local' && [...added, ...updated, ...removed].includes(this.doc.clientID)) this.sendAwareness()
      for (const client of removed) this.heard.delete(client)
    })
    // A caret that moves, this client's too, changes no avatar: the page renders again only when the people change.
    this.awareness.on('change', () => {
      if (key(this.peers()) !== key(this.snapshot.peers)) this.changed()
    })
    this.doc.on('update', () => this.textChanged())
    this.snapshot = this.make()
  }

  async start(): Promise<void> {
    const kept = await this.copy.load().catch(() => ({ pending: 0, title: null, level: null }))
    if (this.destroyed) return
    this.state.title = titleOf(this.text.toString()) ?? kept.title
    this.state.level = this.state.level ?? (kept.level as Level | null)
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws'
    const provider = new Provider({
      url: `${scheme}://${location.host}${at(`api/docs/${encodeURIComponent(this.id)}/ws${this.asGuest}`)}`,
      doc: this.doc,
      pending: kept.pending,
      connect: navigator.onLine,
    })
    this.provider = provider
    provider.on('change', () => {
      this.copy.keep({ pending: provider.pending })
      this.changed()
    })
    provider.on('welcome', (welcome) => {
      Object.assign(this.state, { level: welcome.level, unreachable: false, size: { bytes: welcome.size, max: welcome.max_size } })
      this.copy.keep({ level: welcome.level })
    })
    provider.on('status', (status) => {
      if (status === 'joining') this.joinedAt = Date.now()
      if (status === 'live') {
        this.lives++
        this.state.joined = true
        this.sendAwareness(true)
        this.forgetStale(this.joinedAt)
      } else {
        const lives = this.lives
        this.later(GRACE_MS, () => this.lives === lives && this.forgetOthers())
      }
      if (status === 'stopped' && provider.stopCode === Close.forbidden) void this.loadInfo()
    })
    provider.on('saved', () => (this.state.cannotSave = false))
    provider.on('merged', (count) => this.onMerged(count))
    provider.on('awareness', (update) => {
      const now = Date.now()
      for (const client of clientsOf(update)) this.heard.set(client, now)
      applyAwarenessUpdate(this.awareness, update, 'server')
    })
    provider.on('notice', (notice) => {
      if (notice.kind === 'unreachable') this.state.unreachable = true
      if (notice.kind === 'reachable') this.state.unreachable = false
      if (notice.kind === 'cannot_save') this.state.cannotSave = true
      if (notice.kind === 'size') Object.assign(this.state, { near: true, size: { bytes: notice.bytes, max: notice.max } })
      if (notice.kind === 'restored') this.onRestored(notice.version)
    })
    window.addEventListener('online', this.online)
    window.addEventListener('offline', this.offline)
    void this.loadInfo()
    this.changed()
  }

  /** What the API says of the document: its level, size and title, and whether it is in the trash or gone. */
  private async loadInfo(): Promise<void> {
    const s = this.state
    try {
      const info = await call<DocInfo>('GET', `docs/${encodeURIComponent(this.id)}${this.asGuest}`)
      s.trashed = info.trashed_ms !== null
      s.owner = info.level === 'owner'
      s.level ??= info.level === 'owner' ? 'edit' : info.level
      s.size ??= { bytes: info.size, max: info.max_size }
      s.title ??= info.title || null
    } catch (e) {
      // Anything else the socket says too, or the page works on from its copy.
      if (e instanceof Refusal && e.code === 'not_found') s.missing = true
      if (e instanceof Refusal && e.detail.trashed === true) s.trashed = true
    }
    this.changed()
  }

  /** The query that makes a call a guest's. */
  get asGuest(): string {
    return this.guest ? `?guest=${encodeURIComponent(this.guest.token)}` : ''
  }

  /** The owner restored the document from the trash: join again. */
  restored(): void {
    this.state.trashed = false
    this.provider?.connect()
    this.changed()
  }

  subscribe = (listener: () => void) => {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  read = () => this.snapshot

  destroy(): void {
    this.destroyed = true
    window.removeEventListener('online', this.online)
    window.removeEventListener('offline', this.offline)
    for (const timer of this.timers) clearTimeout(timer)
    if (this.recheck) clearTimeout(this.recheck)
    this.provider?.destroy()
    this.awareness.destroy()
    this.undo.destroy()
    this.copy.destroy()
    this.doc.destroy()
  }

  private readonly online = () => {
    // A stop is for a cause the network does not change: its banner stays.
    if (this.provider?.status !== 'stopped') this.provider?.connect()
    this.changed()
  }

  private readonly offline = () => {
    this.provider?.disconnect()
    this.changed()
  }

  private textChanged(): void {
    if (this.titleDue) return
    this.titleDue = true
    this.later(200, () => {
      this.titleDue = false
      const title = titleOf(this.text.toString())
      if (title) this.copy.keep({ title })
      this.state.title = title
      this.changed()
    })
  }

  /** This client's cursor, at most one each 100 ms; `now` after a join, as the server knows nothing of it. */
  private sendAwareness(now = false): void {
    const provider = this.provider
    if (!provider?.welcome || provider.welcome.level !== 'edit' || (this.awareDue && !now)) return
    const send = () => {
      this.awareDue = false
      this.awareSent = Date.now()
      provider.sendAwareness(encodeAwarenessUpdate(this.awareness, [this.doc.clientID]))
    }
    const wait = this.awareSent + AWARE_EVERY_MS - Date.now()
    if (now || wait <= 0) return send()
    this.awareDue = true
    this.later(wait, send)
  }

  /** Cursors of the others stay through a short break, so a reconnect does not flicker; a longer one clears them. */
  private forgetOthers(): void {
    const others = [...this.awareness.getStates().keys()].filter((client) => client !== this.doc.clientID)
    removeAwarenessStates(this.awareness, others, 'server')
  }

  /** After a join the server sends every cursor it has: one it has not named since the socket opened, within a
   * second of the join, has left. */
  private forgetStale(joined: number): void {
    this.later(GRACE_MS, () => {
      const stale = [...this.awareness.getStates().keys()].filter((client) => client !== this.doc.clientID && (this.heard.get(client) ?? 0) < joined)
      removeAwarenessStates(this.awareness, stale, 'server')
    })
  }

  private later(ms: number, f: () => void): void {
    const timer = setTimeout(() => {
      this.timers.delete(timer)
      if (!this.destroyed) f()
    }, ms)
    this.timers.add(timer)
  }

  private changed(): void {
    if (this.destroyed) return
    const provider = this.provider
    const trouble = !!provider && (provider.status !== 'live' || this.state.unreachable)
    if (!trouble) this.state.troubleSince = null
    else this.state.troubleSince ??= Date.now()
    this.snapshot = this.make()
    const at = this.snapshot.shown.recheckAt
    if (this.recheck) clearTimeout(this.recheck)
    this.recheck = at === null ? null : setTimeout(() => this.changed(), Math.max(0, at - Date.now()))
    for (const listener of this.listeners) listener()
  }

  private make(): Snapshot {
    const provider = this.provider
    const s = this.state
    const view: View = {
      status: provider?.status ?? 'connecting',
      stopCode: provider?.stopCode ?? null,
      lastClose: provider?.lastClose ?? null,
      online: navigator.onLine,
      level: s.level,
      pending: provider?.pending ?? 0,
      unreachable: s.unreachable,
      cannotSave: s.cannotSave,
      size: s.size,
      near: s.near,
      trashed: s.trashed,
      missing: s.missing,
      joined: s.joined,
      troubleSince: s.troubleSince,
      now: Date.now(),
    }
    return {
      shown: describe(view),
      view,
      empty: this.text.length === 0,
      title: s.title,
      user: this.me(),
      owner: s.owner,
      peers: this.peers(),
      // The server counts every viewer, this one too.
      viewers: Math.max(0, (provider?.viewers ?? 0) - (s.level === 'view' ? 1 : 0)),
    }
  }

  private me(): string {
    return this.provider?.welcome?.user ?? this.user
  }

  /** The other users with the document open, by name: their tabs count once. */
  private peers(): Peer[] {
    const [me, peers] = [this.me(), new Map<string, Peer>()]
    for (const [client, state] of this.awareness.getStates()) {
      const name = state.user?.name
      if (client === this.doc.clientID || typeof name !== 'string' || name === me) continue
      peers.set(name, { name, colour: String(state.user.color ?? '#888') })
    }
    return [...peers.values()].sort((a, b) => a.name.localeCompare(b.name))
  }
}

const key = (peers: Peer[]) => peers.map((p) => `${p.name} ${p.colour}`).join('\n')

/** The client ids an awareness update names. */
function clientsOf(update: Uint8Array): number[] {
  const d = decoding.createDecoder(update)
  return Array.from({ length: decoding.readVarUint(d) }, () => {
    const client = decoding.readVarUint(d)
    decoding.readVarUint(d)
    decoding.readVarString(d)
    return client
  })
}
