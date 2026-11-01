// The client side of the websocket protocol (design §4.3): a Y.Doc kept in step with one document. It sends local
// changes one edit at a time and keeps each until the server says it is saved, follows the document's stream by
// position and joins again on a gap, waits when the server is busy, resumes after a break, and stops on a close code
// that a retry cannot fix. An edit too large for the server, or refused as full, makes it join again once to send
// what the server lacks; a second time it stops with 4413. The Rust twin is `ClientCore` in src/follow.rs, which the
// simulation and `mdpad load` run.

import * as decoding from 'lib0/decoding'
import * as encoding from 'lib0/encoding'
import * as Y from 'yjs'

/** The name of the document's one Y.Text, the same on the server. */
export const TEXT = 'md'

export const Close = {
  unauthorized: 4401,
  forbidden: 4403,
  notFound: 4404,
  tooLarge: 4413,
  limited: 4429,
  unavailable: 4503,
} as const

const SYNC = 0
const AWARENESS = 1
const STEP1 = 0
const STEP2 = 1
const HELLO = 16
const WELCOME = 17
const EDIT = 18
const SAVED = 19
const UPDATE = 20
const NOTICE = 21

export type Level = 'none' | 'view' | 'edit'

export interface Welcome {
  level: Level
  user: string
  colour: string
  max_update: number
  size: number
  max_size: number
  /** The server went on from this copy's position. */
  resumed: boolean
  generation: number
}

export type Notice =
  | { kind: 'busy'; retry_after_ms: number }
  | { kind: 'size'; bytes: number; max: number }
  | { kind: 'full' }
  | { kind: 'cannot_save' }
  | { kind: 'resync' }
  | { kind: 'unreachable' }
  | { kind: 'reachable' }
  | { kind: 'viewers'; count: number }
  | { kind: 'restored'; version: number }

/** `stopped` comes after a close code that joining again cannot fix; `stopCode` says which. */
export type Status = 'connecting' | 'joining' | 'live' | 'offline' | 'stopped'

interface Events {
  status: Status
  welcome: Welcome
  /** The server holds every edit up to this one. */
  saved: number
  notice: Notice
  /** A y-protocols awareness update from the server: the others' entries, with the name and colour it set. */
  awareness: Uint8Array
  /** Changes made while not joined are saved: how many. */
  merged: number
  /** Anything above changed: the status, the unsaved count, a notice. */
  change: void
}

interface Unsaved {
  seq: number
  /** What the frame says: changes made offline, at least 1 for the state sent after a join. */
  offline: number
  /** Local changes in it, and of those, how many were made while not joined. */
  changes: number
  merged: number
  update: Uint8Array
}

export interface ProviderOptions {
  /** The socket's URL, credentials included. */
  url: string
  doc: Y.Doc
  /** The first wait before joining again after a break, doubled each time up to `maxBackoffMs`, with jitter. */
  backoffMs?: number
  maxBackoffMs?: number
  /** The wait after 4429 (too many sockets), doubled up to `maxLimitedMs`. */
  limitedMs?: number
  maxLimitedMs?: number
  /** Changes not saved that an earlier copy of this document kept, in IndexedDB: they count as unsaved and offline. */
  pending?: number
  /** False: start offline, until `connect`. */
  connect?: boolean
  WebSocket?: typeof WebSocket
}

/** The browser's close code for a frame over the server's limit. */
const MESSAGE_TOO_BIG = 1009
const ABNORMAL = 1006

/** Closes after which the provider joins again: a broken connection, and the server's own "later". */
const retries = (code: number) => code < 4000 || code === Close.limited || code === Close.unavailable

export class Provider {
  url: string
  readonly doc: Y.Doc
  status: Status = 'offline'
  stopCode: number | null = null
  /** The close code of the last break that the provider joins again after. */
  lastClose: number | null = null
  welcome: Welcome | null = null
  /** The last record of the document's stream this copy holds: generation and seq. */
  position: [number, number] | null = null
  /** The server refused an insert at the size cap. */
  full = false
  /** The others' viewers and guests, on every web node. */
  viewers = 0

  private readonly Socket: typeof WebSocket
  private readonly backoff: [number, number]
  private readonly limited: [number, number]
  private socket: WebSocket | null = null
  private open = false
  private expectPosition = false
  private awaitingState = true
  private nextSeq = 1
  private outbox: Unsaved[] = []
  private sent = false
  /** The server said busy: nothing goes until its wait has passed, whatever comes meanwhile. */
  private busy = false
  private offline = 0
  /** Changes an earlier copy kept: unsaved, but in no edit yet. */
  private carried = 0
  /** Joined again once for an edit too large: the next one stops. */
  private resynced = false
  private failures = 0
  private timer: ReturnType<typeof setTimeout> | null = null
  private held = false
  private destroyed = false
  private listeners = new Map<keyof Events, Set<(value: never) => void>>()
  private readonly onUpdate = (update: Uint8Array, origin: unknown) => {
    if (origin !== this) this.local(update)
  }

  constructor(options: ProviderOptions) {
    this.url = options.url
    this.doc = options.doc
    this.Socket = options.WebSocket ?? WebSocket
    this.backoff = [options.backoffMs ?? 500, options.maxBackoffMs ?? 8_000]
    this.limited = [options.limitedMs ?? 8_000, options.maxLimitedMs ?? 30_000]
    this.carried = this.offline = options.pending ?? 0
    this.doc.on('update', this.onUpdate)
    if (options.connect ?? true) this.connect()
  }

  on<K extends keyof Events>(event: K, listener: (value: Events[K]) => void): () => void {
    const set = this.listeners.get(event) ?? new Set()
    this.listeners.set(event, set)
    set.add(listener)
    return () => set.delete(listener)
  }

  /** Edits not yet saved. */
  get unsaved(): number {
    return this.outbox.length
  }

  /** Local changes not yet saved: each keystroke or paste is one. */
  get pending(): number {
    return this.outbox.reduce((sum, edit) => sum + edit.changes, this.carried)
  }

  /** Joined, the server's state applied, and nothing unsaved. */
  get synced(): boolean {
    return this.status === 'live' && this.position !== null && this.outbox.length === 0 && this.carried === 0
  }

  /** Joins, or joins again at once after `disconnect` or a stop. */
  connect(): void {
    if (this.destroyed || this.socket) return
    this.clearTimer()
    this.held = false
    this.stopCode = null
    this.setStatus('connecting')
    const socket = new this.Socket(this.url)
    socket.binaryType = 'arraybuffer'
    this.socket = socket
    socket.onopen = () => {
      if (socket !== this.socket) return
      ;[this.open, this.sent, this.busy, this.awaitingState, this.welcome] = [true, false, false, true, null]
      this.setStatus('joining')
      const e = encoding.createEncoder()
      encoding.writeUint8(e, HELLO)
      if (this.position) {
        encoding.writeUint8(e, 1)
        encoding.writeVarUint(e, this.position[0])
        encoding.writeVarUint(e, this.position[1])
      } else {
        encoding.writeUint8(e, 0)
      }
      socket.send(encoding.toUint8Array(e))
    }
    socket.onmessage = (event: MessageEvent) => {
      if (socket !== this.socket) return
      try {
        this.frame(new Uint8Array(event.data as ArrayBuffer))
      } catch {
        this.rejoin() // a frame this client cannot read
      }
    }
    socket.onclose = (event: CloseEvent) => this.closed(socket, event.code)
    // Node's WebSocket ends a refused dial with an error and no close; a browser's close after it is ignored.
    socket.onerror = () => this.closed(socket, ABNORMAL)
  }

  /** Goes offline and stays there until `connect`: local changes wait in the outbox. */
  disconnect(): void {
    this.held = true
    this.drop()
    this.setStatus('offline')
  }

  /** This client's y-protocols awareness update, while joined: the caller sends at most one each 100 ms. */
  sendAwareness(update: Uint8Array): void {
    if (!this.open || !this.welcome) return
    const e = encoding.createEncoder()
    encoding.writeUint8(e, AWARENESS)
    encoding.writeVarUint8Array(e, update)
    this.socket?.send(encoding.toUint8Array(e))
  }

  destroy(): void {
    this.destroyed = true
    this.doc.off('update', this.onUpdate)
    this.drop()
    this.clearTimer()
  }

  private local(update: Uint8Array): void {
    const offline = this.open && this.welcome ? 0 : 1
    this.offline += offline
    const last = this.outbox.at(-1)
    const mergeable = !(this.sent && this.outbox.length === 1)
    const max = this.welcome?.max_update ?? 256 * 1024
    if (last && mergeable && last.update.length + update.length < max / 2) {
      last.update = Y.mergeUpdates([last.update, update])
      last.offline += offline
      last.changes += 1
      last.merged += offline
    } else {
      this.outbox.push({ seq: this.nextSeq++, offline, changes: 1, merged: offline, update })
    }
    this.flush()
    this.emit('change', undefined)
  }

  /** Sends the first unsaved edit, unless one is out, the server's state is still to come, or the server is busy. */
  private flush(): void {
    if (!this.open || !this.welcome || this.awaitingState || this.sent || this.busy || !this.outbox.length) return
    const first = this.outbox[0]
    // The server would close the socket: one edit cannot be split, so the document is full for this copy.
    if (first.update.length > this.welcome.max_update) return this.tooLarge()
    this.sent = true
    const e = encoding.createEncoder()
    encoding.writeUint8(e, EDIT)
    encoding.writeVarUint(e, first.seq)
    encoding.writeVarUint(e, first.offline)
    encoding.writeVarUint8Array(e, first.update)
    this.socket?.send(encoding.toUint8Array(e))
  }

  private frame(bytes: Uint8Array): void {
    const d = decoding.createDecoder(bytes)
    const type = decoding.readUint8(d)
    const sub = type === SYNC ? decoding.readUint8(d) : null
    if (sub === STEP2) {
      Y.applyUpdate(this.doc, decoding.readVarUint8Array(d), this)
    } else if (sub === STEP1) {
      this.step1(decoding.readVarUint8Array(d))
    } else if (type === WELCOME) {
      const welcome = JSON.parse(decoding.readVarString(d)) as Welcome
      this.awaitingState = !welcome.resumed
      if (!welcome.resumed) this.position = null
      this.welcome = welcome
      this.failures = 0
      if (welcome.resumed) this.setStatus('live')
      this.emit('welcome', welcome)
      this.emit('change', undefined)
    } else if (type === SAVED) {
      const seq = decoding.readVarUint(d)
      let [saved, merged] = [false, 0]
      while (this.outbox.length && this.outbox[0].seq <= seq) {
        merged += this.outbox.shift()!.merged
        ;[this.sent, saved, this.resynced] = [false, true, false]
      }
      if (saved) this.emit('saved', seq)
      if (merged) this.emit('merged', merged)
      if (saved) this.emit('change', undefined)
    } else if (type === UPDATE) {
      const [generation, seq, update] = [decoding.readVarUint(d), decoding.readVarUint(d), decoding.readVarUint8Array(d)]
      const next = this.position && this.position[0] === generation && seq === this.position[1] + 1
      if (!this.expectPosition && !next) return this.rejoin()
      this.expectPosition = false
      this.position = [generation, seq]
      if (update.length) Y.applyUpdate(this.doc, update, this)
    } else if (type === AWARENESS) {
      return this.emit('awareness', decoding.readVarUint8Array(d))
    } else if (type === NOTICE) {
      const notice = JSON.parse(decoding.readVarString(d)) as Notice
      if (notice.kind === 'viewers') this.viewers = notice.count
      this.emit('notice', notice)
      this.emit('change', undefined)
      if (notice.kind === 'full') return this.tooLarge()
      if (notice.kind === 'busy') {
        ;[this.sent, this.busy] = [false, true]
        this.clearTimer()
        this.timer = setTimeout(() => {
          this.busy = false
          this.flush()
        }, notice.retry_after_ms)
        return
      }
      if (notice.kind === 'resync') [this.sent, this.awaitingState] = [false, true]
    } else {
      throw new Error(`frame type ${type}`)
    }
    this.flush()
  }

  /** The server's state vector: everything it lacks, the unsaved edits among it, goes as one edit. */
  private step1(stateVector: Uint8Array): void {
    const changes = this.pending
    this.outbox = []
    if (changes > 0 || ahead(this.doc, stateVector)) {
      const update = Y.encodeStateAsUpdate(this.doc, stateVector)
      const offline = Math.max(this.offline, 1)
      this.outbox.push({ seq: this.nextSeq++, offline, changes: Math.max(changes, 1), merged: this.offline, update })
    }
    ;[this.offline, this.carried, this.sent, this.awaitingState, this.expectPosition] = [0, 0, false, false, true]
    this.setStatus('live')
  }

  /** An edit the server cannot take for its size: join again once and send what it lacks, then stop with 4413. */
  private tooLarge(): void {
    if (this.resynced) {
      this.full = true
      this.drop()
      this.stopCode = Close.tooLarge
      return this.setStatus('stopped')
    }
    this.resynced = true
    this.rejoin()
  }

  /** A gap or a generation this copy did not follow: join again at once. */
  private rejoin(): void {
    this.drop()
    this.connect()
  }

  private drop(): void {
    const socket = this.socket
    this.socket = null
    ;[this.open, this.sent, this.welcome] = [false, false, null]
    socket?.close(1000)
  }

  private closed(socket: WebSocket, code: number): void {
    if (socket !== this.socket) return
    this.drop()
    if (this.destroyed || this.held) return
    if (code === Close.tooLarge || code === MESSAGE_TOO_BIG) return this.tooLarge()
    if (!retries(code)) {
      this.stopCode = code
      return this.setStatus('stopped')
    }
    this.lastClose = code
    this.setStatus('offline')
    const [first, max] = code === Close.limited ? this.limited : this.backoff
    const wait = Math.min(max, first * 2 ** this.failures)
    this.failures = Math.min(this.failures + 1, 16)
    // Jitter, but never under the first wait: 4429 waits 8 to 30 s, a break 0.5 to 8 s.
    this.timer = setTimeout(() => this.connect(), Math.max(first, wait * (0.5 + Math.random() / 2)))
  }

  private clearTimer(): void {
    if (this.timer) clearTimeout(this.timer)
    this.timer = null
  }

  private setStatus(status: Status): void {
    if (status === this.status) return
    this.status = status
    if (status === 'live') this.lastClose = null
    this.emit('status', status)
    this.emit('change', undefined)
  }

  private emit<K extends keyof Events>(event: K, value: Events[K]): void {
    for (const listener of this.listeners.get(event) ?? []) (listener as (value: Events[K]) => void)(value)
  }
}

/** Whether `doc` holds an insert a peer at `stateVector` lacks. */
function ahead(doc: Y.Doc, stateVector: Uint8Array): boolean {
  const theirs = Y.decodeStateVector(stateVector)
  for (const [client, clock] of Y.decodeStateVector(Y.encodeStateVector(doc))) {
    if ((theirs.get(client) ?? 0) < clock) return true
  }
  return false
}
