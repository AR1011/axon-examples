// provider.ts in Node against `mdpad serve` (design §8): join, offline edits sent as type 18, resume, a restart of
// the server, and the close codes that stop it. A gap and `busy` are hard to make a real server send, so a scripted
// socket stands in for those two. MDPAD_BIN names the binary; by default the workspace's debug build.

import { strict as assert } from 'node:assert'
import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { after, before, test } from 'node:test'
import * as decoding from 'lib0/decoding'
import * as encoding from 'lib0/encoding'
import * as Y from 'yjs'
import { Close, Provider, TEXT } from '../src/editor/provider.ts'

const bin = process.env.MDPAD_BIN ?? new URL('../../../../target/debug/mdpad', import.meta.url).pathname
const dir = mkdtempSync(join(tmpdir(), 'mdpad-node-'))
let server: ChildProcess | null = null
let http = ''

/** Starts `mdpad serve` on `dir` and waits for its line. */
async function serve(): Promise<void> {
  const child = spawn(bin, ['serve', `dir=${dir}`, 'http=127.0.0.1:0'], { stdio: ['ignore', 'pipe', 'inherit'] })
  const line = await new Promise<string>((resolve, reject) => {
    child.once('exit', (code) => reject(new Error(`mdpad serve exited: ${code}`)))
    createInterface({ input: child.stdout! }).once('line', resolve)
  })
  server = child
  http = JSON.parse(line).http
}

async function stop(): Promise<void> {
  const child = server
  server = null
  if (!child || child.exitCode !== null) return
  const exited = new Promise((resolve) => child.once('exit', resolve))
  child.kill('SIGTERM')
  await exited
}

async function call(token: string | null, method: string, path: string, body?: unknown): Promise<any> {
  const headers: Record<string, string> = { 'content-type': 'application/json' }
  if (token) headers.authorization = `Bearer ${token}`
  const response = await fetch(`http://${http}${path}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) })
  const json = await response.json()
  if (!response.ok) throw new Error(`${response.status} ${json.error}`)
  return json
}

/** The document's text as the server holds it. */
const markdown = async (id: string, token: string) =>
  (await fetch(`http://${http}/api/docs/${id}/markdown`, { headers: { authorization: `Bearer ${token}` } })).text()

const login = async (name: string) =>
  (await call(null, 'POST', '/api/login', { name, password: `pw-${name}`, token: true })).token as string

const url = (id: string, token: string) => `ws://${http}/api/docs/${id}/ws?token=${token}`

/** Polls `check` every 10 ms until it holds, or fails after `ms`. */
async function until(what: string, check: () => boolean, ms = 10_000): Promise<void> {
  const deadline = Date.now() + ms
  while (!check()) {
    if (Date.now() > deadline) throw new Error(`${what}: not within ${ms} ms`)
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

/** A WebSocket that records the type and fields of each frame it sends. */
function recording(sent: number[][]): typeof WebSocket {
  return class extends WebSocket {
    override send(data: Parameters<WebSocket['send']>[0]) {
      const d = decoding.createDecoder(data as Uint8Array)
      const type = decoding.readUint8(d)
      sent.push(type === 18 ? [type, decoding.readVarUint(d), decoding.readVarUint(d)] : [type])
      super.send(data)
    }
  } as typeof WebSocket
}

let ann = ''
let ben = ''

before(async () => {
  for (const name of ['ann', 'ben']) {
    execFileSync(bin, ['user', 'add', `name=${name}`, `password=pw-${name}`, `dir=${dir}`], { stdio: 'ignore' })
  }
  await serve()
  ann = await login('ann')
  ben = await login('ben')
})

after(async () => {
  await stop()
  rmSync(dir, { recursive: true, force: true })
})

test('two providers join, edit at once and end with the server text', async () => {
  const { id } = await call(ann, 'POST', '/api/docs', { markdown: '# Join\n' })
  const [a, b] = [new Y.Doc(), new Y.Doc()]
  const [pa, pb] = [new Provider({ url: url(id, ann), doc: a }), new Provider({ url: url(id, ann), doc: b })]
  await until('both joined', () => pa.synced && pb.synced)
  assert.equal(a.getText(TEXT).toString(), '# Join\n')
  for (let i = 0; i < 20; i++) {
    a.getText(TEXT).insert(a.getText(TEXT).length, `a${i} `)
    b.getText(TEXT).insert(0, `b${i} `)
  }
  await until('saved', () => pa.unsaved === 0 && pb.unsaved === 0)
  const server = await markdown(id, ann)
  await until('the same text', () => a.getText(TEXT).toString() === server && b.getText(TEXT).toString() === server)
  assert.match(server, /a19 /)
  assert.match(server, /b19 /)
  pa.destroy()
  pb.destroy()
})

/** One awareness entry, as y-protocols encodes it. */
function aware(client: number, clock: number, state: unknown): Uint8Array {
  const e = encoding.createEncoder()
  encoding.writeVarUint(e, 1)
  encoding.writeVarUint(e, client)
  encoding.writeVarUint(e, clock)
  encoding.writeVarString(e, JSON.stringify(state))
  return encoding.toUint8Array(e)
}

/** Each entry of an awareness update: client, clock and state. */
function entries(update: Uint8Array): [number, number, any][] {
  const d = decoding.createDecoder(update)
  const n = decoding.readVarUint(d)
  return Array.from({ length: n }, () => [decoding.readVarUint(d), decoding.readVarUint(d), JSON.parse(decoding.readVarString(d))])
}

test('a cursor reaches the others with the name the server sets, and goes when it leaves', async () => {
  const { id } = await call(ann, 'POST', '/api/docs', { markdown: '# Cursors\n' })
  const [pa, pb] = [new Provider({ url: url(id, ann), doc: new Y.Doc() }), new Provider({ url: url(id, ann), doc: new Y.Doc() })]
  const seen = new Map<number, any>()
  pb.on('awareness', (update) => {
    for (const [client, , state] of entries(update)) seen.set(client, state)
  })
  await until('both joined', () => pa.synced && pb.synced)
  const client = pa.doc.clientID
  pa.sendAwareness(aware(client, 1, { cursor: { anchor: 3 }, user: { name: 'mallory', color: '#000' } }))
  await until('the cursor shows', () => seen.get(client)?.cursor?.anchor === 3)
  assert.equal(seen.get(client).user.name, 'ann', 'the server sets the name')
  pa.destroy()
  await until('it goes when the socket closes', () => seen.has(client) && seen.get(client) === null, 3000)
  pb.destroy()
})

test('edits made offline go as type 18 with their count, and a resume goes on from the position', async () => {
  const { id } = await call(ann, 'POST', '/api/docs', { markdown: '# Offline\n' })
  const sent: number[][] = []
  const doc = new Y.Doc()
  const p = new Provider({ url: url(id, ann), doc, WebSocket: recording(sent) })
  await until('joined', () => p.synced)
  const welcomes: boolean[] = []
  p.on('welcome', (w) => welcomes.push(w.resumed))

  p.disconnect()
  const t = doc.getText(TEXT)
  t.insert(t.length, 'one ')
  t.insert(t.length, 'two ')
  assert.equal(p.unsaved, 1, 'merged while waiting')
  sent.length = 0
  p.connect()
  await until('saved', () => p.synced)
  assert.deepEqual(welcomes, [true], 'resumed: nobody wrote meanwhile')
  const edits = sent.filter((f) => f[0] === 18)
  assert.equal(edits.length, 1)
  assert.equal(edits[0][2], 2, 'two changes made offline')
  const { level } = await call(ann, 'GET', `/api/docs/${id}`)
  assert.equal(level, 'owner')
  assert.equal(await markdown(id, ann), '# Offline\none two ')
  p.destroy()
})

test('a restart of the server: the provider waits, joins the new generation and sends what it lacks', async () => {
  const { id } = await call(ann, 'POST', '/api/docs', { markdown: '# Restart\n' })
  const doc = new Y.Doc()
  const p = new Provider({ url: url(id, ann), doc, backoffMs: 50, maxBackoffMs: 200 })
  await until('joined', () => p.synced)
  const generation = p.position![0]
  await stop()
  await until('offline', () => p.status === 'offline')
  doc.getText(TEXT).insert(0, 'while down ')
  await serve()
  p.url = url(id, ann)
  await until('joined again and saved', () => p.synced, 15_000)
  assert.ok(p.position![0] > generation, `a new generation: ${p.position} after ${generation}`)
  assert.equal(doc.getText(TEXT).toString(), 'while down # Restart\n')
  p.destroy()
})

test('a close code a retry cannot fix stops the provider', async () => {
  const { id } = await call(ann, 'POST', '/api/docs', { markdown: '# Codes\n' })
  const stops = async (address: string, edit = false) => {
    const doc = new Y.Doc()
    const p = new Provider({ url: address, doc, backoffMs: 20 })
    if (edit) {
      await until('joined', () => p.synced)
      doc.getText(TEXT).insert(0, 'not mine ')
    }
    await until('stopped', () => p.status === 'stopped')
    await new Promise((resolve) => setTimeout(resolve, 200))
    assert.equal(p.status, 'stopped', 'and it stays stopped')
    p.destroy()
    return p.stopCode
  }
  assert.equal(await stops(url(id, 'not-a-token')), Close.unauthorized)
  assert.equal(await stops(url(id.slice(0, -1) + (id.endsWith('a') ? 'b' : 'a'), ann)), Close.notFound)
  await call(ann, 'PUT', `/api/docs/${id}/members/ben`, { level: 'view' })
  assert.equal(await stops(url(id, ben), true), Close.forbidden)
})

/** A scripted socket: what the provider sends is kept, and the test plays the server. */
class Script {
  static last: Script | null = null
  static count = 0
  binaryType = 'arraybuffer'
  sent: Uint8Array[] = []
  onopen: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  constructor() {
    Script.last = this
    Script.count++
    setTimeout(() => this.onopen?.(), 0)
  }
  send(data: Uint8Array) {
    this.sent.push(data)
  }
  close(code: number) {
    setTimeout(() => this.onclose?.({ code }), 0)
  }
  deliver(...parts: ((e: encoding.Encoder) => void)[]) {
    const e = encoding.createEncoder()
    for (const part of parts) part(e)
    const bytes = encoding.toUint8Array(e)
    this.onmessage?.({ data: bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer })
  }
}

const u8 = (n: number) => (e: encoding.Encoder) => encoding.writeUint8(e, n)
const uint = (n: number) => (e: encoding.Encoder) => encoding.writeVarUint(e, n)
const bytes = (b: Uint8Array) => (e: encoding.Encoder) => encoding.writeVarUint8Array(e, b)
const str = (s: string) => (e: encoding.Encoder) => encoding.writeVarString(e, s)
const welcome = { level: 'edit', user: 'ann', colour: '#000', max_update: 262144, size: 0, max_size: 262144, resumed: false, generation: 1 }

/** Joins a scripted server at generation 1, seq 5, holding nothing. */
async function scripted(): Promise<{ p: Provider; doc: Y.Doc; s: Script }> {
  const doc = new Y.Doc()
  const p = new Provider({ url: 'ws://script', doc, WebSocket: Script as unknown as typeof WebSocket, backoffMs: 10 })
  await until('open', () => p.status === 'joining')
  const s = Script.last!
  s.deliver(u8(17), str(JSON.stringify(welcome)))
  s.deliver(u8(0), u8(1), bytes(Y.encodeStateAsUpdate(new Y.Doc())))
  s.deliver(u8(0), u8(0), bytes(Y.encodeStateVector(new Y.Doc())))
  s.deliver(u8(20), uint(1), uint(5), bytes(new Uint8Array()))
  assert.deepEqual(p.position, [1, 5])
  return { p, doc, s }
}

test('a gap in the positions closes the socket and joins again', async () => {
  const { p, s } = await scripted()
  s.deliver(u8(20), uint(1), uint(6), bytes(new Uint8Array()))
  assert.equal(Script.last, s, 'the next one is no gap')
  const count = Script.count
  s.deliver(u8(20), uint(1), uint(8), bytes(new Uint8Array()))
  await until('a new socket', () => Script.count === count + 1)
  await until('hello again', () => Script.last!.sent.length === 1)
  const hello = decoding.createDecoder(Script.last!.sent[0])
  assert.deepEqual([decoding.readUint8(hello), decoding.readUint8(hello), decoding.readVarUint(hello)], [16, 1, 1])
  p.destroy()
})

test('busy: the edit waits as long as the server says, then goes again', async () => {
  const { p, doc, s } = await scripted()
  doc.getText(TEXT).insert(0, 'x')
  assert.equal(s.sent.filter((f) => f[0] === 18).length, 1)
  s.deliver(u8(21), str(JSON.stringify({ kind: 'busy', retry_after_ms: 150 })))
  const at = Date.now()
  await until('sent again', () => s.sent.filter((f) => f[0] === 18).length === 2)
  assert.ok(Date.now() - at >= 140, `after ${Date.now() - at} ms`)
  const [first, again] = s.sent.filter((f) => f[0] === 18)
  assert.deepEqual(again, first, 'the same edit')
  s.deliver(u8(19), uint(1))
  assert.equal(p.unsaved, 0)
  p.destroy()
})

test('busy: a local change meanwhile waits too', async () => {
  const { p, doc, s } = await scripted()
  doc.getText(TEXT).insert(0, 'x')
  s.deliver(u8(21), str(JSON.stringify({ kind: 'busy', retry_after_ms: 150 })))
  const at = Date.now()
  doc.getText(TEXT).insert(1, 'y')
  assert.equal(s.sent.filter((f) => f[0] === 18).length, 1, 'held')
  await until('sent again', () => s.sent.filter((f) => f[0] === 18).length === 2)
  assert.ok(Date.now() - at >= 140, `after ${Date.now() - at} ms`)
  p.destroy()
})
