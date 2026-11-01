// Each row of the editor state table (design §4.1) as the page shows it: the badge, the banners, whether the editor
// shows and takes typing. The live suite (tests-live/editor.spec.ts) makes the rows a real cluster can make.

import { strict as assert } from 'node:assert'
import { test } from 'node:test'
import { Close } from '../src/editor/provider.ts'
import { describe, GRACE_MS, titleOf, type View } from '../src/features/editor/describe.ts'

const base: View = {
  status: 'live',
  stopCode: null,
  lastClose: null,
  online: true,
  level: 'edit',
  pending: 0,
  unreachable: false,
  cannotSave: false,
  size: { bytes: 620, max: 1000 },
  near: false,
  trashed: false,
  missing: false,
  joined: true,
  troubleSince: null,
  now: 10_000,
}
const show = (change: Partial<View>) => describe({ ...base, ...change })
const keys = (change: Partial<View>) => show(change).notices.map((n) => n.key)

test('joining: connecting, with the cached text in the editor', () => {
  const shown = show({ status: 'joining', joined: false, troubleSince: null })
  assert.equal(shown.screen, 'editor')
  assert.equal(shown.badge.label, 'Connecting')
  assert.deepEqual(keys({ status: 'joining', joined: false }), [])
  assert.equal(show({ status: 'connecting', joined: false, pending: 3 }).badge.label, 'Connecting, 3 not saved')
})

test('saved and saving N, with the size in the tooltip', () => {
  assert.equal(show({}).badge.label, 'Saved')
  assert.match(show({}).badge.tip, /62 % of the maximum/)
  assert.equal(show({ pending: 4 }).badge.label, 'Saving 4')
  assert.equal(show({ pending: 4 }).readOnly, false)
})

test('offline: the unsaved count, and no banner', () => {
  const shown = show({ status: 'offline', online: false, pending: 42, troubleSince: 9_900 })
  assert.equal(shown.badge.label, 'Offline, 42 not saved')
  assert.deepEqual(shown.notices, [])
  assert.equal(shown.readOnly, false)
})

test('server unreachable: after 4503, or while the server says the home is out of reach', () => {
  assert.deepEqual(keys({ status: 'offline', lastClose: Close.unavailable, troubleSince: 0 }), ['unreachable'])
  assert.equal(show({ unreachable: true, troubleSince: 0 }).badge.label, 'Server unreachable')
  assert.equal(show({ unreachable: true, troubleSince: 0 }).readOnly, false)
})

test('a break shorter than the grace shows nothing: no flicker on reconnect', () => {
  const now = base.now
  const brief = show({ status: 'connecting', lastClose: Close.unavailable, troubleSince: now - 300 })
  assert.equal(brief.badge.label, 'Saved')
  assert.deepEqual(brief.notices, [])
  assert.equal(brief.recheckAt, now - 300 + GRACE_MS)
  assert.equal(show({ unreachable: true, troubleSince: now - 300 }).badge.label, 'Saved')
  assert.equal(show({ status: 'connecting', troubleSince: now - GRACE_MS }).badge.label, 'Connecting')
})

test('busy: the badge stays saving N', () => {
  assert.equal(show({ pending: 2 }).badge.label, 'Saving 2')
})

test('read-only: a banner, and the editor takes no typing', () => {
  const shown = show({ level: 'view' })
  assert.equal(shown.badge.label, 'Read-only')
  assert.deepEqual(keys({ level: 'view' }), ['view'])
  assert.equal(shown.readOnly, true)
})

test('signed out: the login page, the copy kept', () => {
  assert.equal(show({ status: 'stopped', stopCode: Close.unauthorized }).screen, 'login')
})

test('access removed: a banner, and the download only with unsaved edits', () => {
  const shown = show({ status: 'stopped', stopCode: Close.forbidden, pending: 3 })
  assert.equal(shown.screen, 'editor')
  assert.deepEqual(shown.notices, [{ key: 'forbidden', tone: 'danger', text: 'Access removed.', action: 'download' }])
  assert.equal(shown.readOnly, true)
  assert.equal(show({ status: 'stopped', stopCode: Close.forbidden }).notices[0].action, undefined)
})

test('not found: its own screen, from the socket or the API', () => {
  assert.equal(show({ status: 'stopped', stopCode: Close.notFound }).screen, 'missing')
  assert.equal(show({ missing: true }).screen, 'missing')
})

test('in trash: its own screen', () => {
  assert.equal(show({ trashed: true, status: 'stopped', stopCode: Close.forbidden }).screen, 'trash')
})

test('too many tabs: a banner while the page waits', () => {
  assert.deepEqual(keys({ status: 'offline', lastClose: Close.limited, troubleSince: 0 }), ['tabs'])
})

test('full: make a fresh copy, after the one resync', () => {
  const shown = show({ status: 'stopped', stopCode: Close.tooLarge, near: true })
  assert.deepEqual(shown.notices.map((n) => [n.key, n.action]), [['full', 'copy']])
  assert.equal(shown.readOnly, true)
})

test('near full and cannot save: banners beside the badge', () => {
  assert.deepEqual(keys({ near: true }), ['near'])
  assert.deepEqual(keys({ cannotSave: true, pending: 1 }), ['cannot_save'])
  assert.equal(show({ cannotSave: true, pending: 1 }).badge.label, 'Saving 1')
})

test('the title is the first # heading', () => {
  assert.equal(titleOf('intro\n# Plan  \n## Draft\n'), 'Plan')
  assert.equal(titleOf('## Only a second level\n'), null)
  assert.equal(titleOf('# 計画 ✨ #\n'), '計画 ✨')
  assert.equal(titleOf('#\n'), null)
})
