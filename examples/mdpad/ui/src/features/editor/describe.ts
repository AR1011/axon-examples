// What the editor shows for the state of its copy (design §4.1): the badge, the banners under the header, and
// whether the editor shows at all. Pure, so each row of the state table has a test (tests-node/describe.test.ts).

import { Close, type Level, type Status } from '../../editor/provider.ts'

export interface View {
  status: Status
  stopCode: number | null
  lastClose: number | null
  /** The browser says it has a network. */
  online: boolean
  /** From the welcome, or from what the copy kept. */
  level: Level | null
  /** Local changes the server has not saved. */
  pending: number
  /** The server said the document's home is out of reach, and has not said it is back. */
  unreachable: boolean
  /** The server said it cannot write its disk, and has saved nothing since. */
  cannotSave: boolean
  /** The last size the server gave. */
  size: { bytes: number; max: number } | null
  /** The server said the document is near its maximum size. */
  near: boolean
  /** The document is in the trash. */
  trashed: boolean
  /** The document does not exist: the API said so, or the socket closed with 4404. */
  missing: boolean
  /** Joined once since the page opened. */
  joined: boolean
  /** Since when the copy is not live, or the home out of reach; null when neither. */
  troubleSince: number | null
  now: number
}

export type Tone = 'success' | 'warning' | 'danger' | 'neutral'

export interface Notice {
  key: 'unreachable' | 'tabs' | 'view' | 'forbidden' | 'full' | 'near' | 'cannot_save' | 'stopped'
  tone: 'info' | 'warning' | 'danger'
  text: string
  action?: 'download' | 'copy'
}

export interface Shown {
  screen: 'editor' | 'missing' | 'trash' | 'login'
  badge: { label: string; tone: Tone; tip: string }
  notices: Notice[]
  readOnly: boolean
  /** The time this changes by itself: describe again then. */
  recheckAt: number | null
}

/** A break shorter than this shows nothing, so a reconnect does not flicker. */
export const GRACE_MS = 1_000

const KEPT = 'Your changes are kept in this browser.'

export function describe(v: View): Shown {
  const code = v.status === 'stopped' ? v.stopCode : null
  const percent = v.size && v.size.max ? Math.round((v.size.bytes * 100) / v.size.max) : null
  const size = percent === null ? '' : ` Size: ${percent} % of the maximum.`
  const notices: Notice[] = []
  const unsaved = v.pending ? 'download' : undefined
  const done = (screen: Shown['screen'], badge: Shown['badge']): Shown => ({ screen, badge, notices, readOnly: true, recheckAt: null })

  if (code === Close.unauthorized) return done('login', { label: 'Signed out', tone: 'neutral', tip: 'Sign in again.' })
  if (v.missing || code === Close.notFound) return done('missing', { label: 'Not found', tone: 'neutral', tip: 'This document does not exist.' })
  if (v.trashed) return done('trash', { label: 'In trash', tone: 'neutral', tip: 'This document is in the trash.' })

  // A short break shows as live: the badge and the banners wait until it has lasted.
  const grace = v.joined && v.troubleSince !== null && v.online && code === null && v.now - v.troubleSince < GRACE_MS
  const live = v.status === 'live' || grace
  const unreachable = v.unreachable && !grace
  let badge: Shown['badge']
  if (code === Close.forbidden) {
    badge = { label: 'No access', tone: 'danger', tip: 'You cannot open this document now.' }
    notices.push({ key: 'forbidden', tone: 'danger', text: 'Access removed.', action: unsaved })
  } else if (code === Close.tooLarge) {
    badge = { label: 'Full', tone: 'danger', tip: `The server does not take more text in this document.${size}` }
    notices.push({ key: 'full', tone: 'danger', text: 'Document is full. Make a fresh copy.', action: 'copy' })
  } else if (code !== null) {
    badge = { label: 'Stopped', tone: 'danger', tip: `The server closed the connection (${code}). Open the page again.` }
    notices.push({ key: 'stopped', tone: 'danger', text: 'Connection stopped. Open the page again.', action: unsaved })
  } else if (!v.online) {
    const label = v.pending ? `Offline, ${v.pending} not saved` : 'Offline'
    badge = { label, tone: 'warning', tip: `No network. ${KEPT} They go to the server when the network is back.` }
  } else if (unreachable || (!live && v.lastClose === Close.unavailable)) {
    badge = { label: 'Server unreachable', tone: 'danger', tip: `The server cannot get to this document now. ${KEPT} The page tries again.` }
    notices.push({ key: 'unreachable', tone: 'warning', text: 'Server unreachable. Your edits are kept here.' })
  } else if (!live && v.lastClose === Close.limited) {
    badge = { label: 'Waiting', tone: 'warning', tip: `You have too many open tabs. ${KEPT} The page tries again.` }
    notices.push({ key: 'tabs', tone: 'warning', text: 'Too many open tabs. Close some tabs.' })
  } else if (!live) {
    const label = v.pending ? `Connecting, ${v.pending} not saved` : 'Connecting'
    badge = { label, tone: 'neutral', tip: `The page connects to the server. ${v.pending ? KEPT : ''}`.trim() }
  } else if (v.level === 'view') {
    badge = { label: 'Read-only', tone: 'neutral', tip: `You can read this document. You cannot change it.${size}` }
  } else if (v.pending) {
    badge = { label: `Saving ${v.pending}`, tone: 'neutral', tip: `${v.pending} changes are not on the server's disk yet.${size}` }
  } else {
    badge = { label: 'Saved', tone: 'success', tip: `The server has every change on its disk.${size}` }
  }

  const stopped = code !== null
  if (v.level === 'view' && !stopped) notices.push({ key: 'view', tone: 'info', text: 'You can only read this document.' })
  if (v.cannotSave && !stopped) notices.push({ key: 'cannot_save', tone: 'danger', text: 'Server cannot save. Your changes are kept here.' })
  if (v.near && code !== Close.tooLarge) notices.push({ key: 'near', tone: 'warning', text: 'Document is near its maximum size.' })
  return {
    screen: 'editor',
    badge,
    notices,
    readOnly: stopped || v.level === 'view' || v.level === 'none',
    recheckAt: grace ? v.troubleSince! + GRACE_MS : null,
  }
}

/** The title of a Markdown text: its first `#` heading, or null. */
export function titleOf(text: string): string | null {
  const heading = /^ {0,3}# +(.*?)(?: +#+)? *$/m.exec(text)
  return heading?.[1].trim() || null
}
