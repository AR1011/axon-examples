// The other half of tests/yrs.rs: the fixtures are what the pinned Yjs makes, and what yrs made applies in Yjs.
import { strict as assert } from 'node:assert'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import * as Y from 'yjs'
import { fixtures } from './fixtures.mjs'

const read = (name) => JSON.parse(readFileSync(new URL(`../../tests/fixtures/${name}`, import.meta.url), 'utf8'))
const bytes = (hex) => Buffer.from(hex, 'hex')

test('yjs.json is what this Yjs makes', () => {
  assert.deepEqual(read('yjs.json'), fixtures())
})

test('an update yrs made applies in Yjs, at the same UTF-16 offsets', () => {
  const js = read('yjs.json')
  const rs = read('yrs.json')
  const doc = new Y.Doc()
  Y.applyUpdate(doc, bytes(js.state))
  Y.applyUpdate(doc, bytes(rs.update))
  assert.equal(doc.getText('md').toString(), rs.text)
  assert.equal(doc.getText('md').length, rs.length)

  const whole = new Y.Doc()
  Y.applyUpdate(whole, bytes(rs.state))
  assert.equal(whole.getText('md').toString(), rs.text)
  assert.equal(Buffer.from(Y.encodeStateVector(whole)).toString('hex'), Buffer.from(Y.encodeStateVector(doc)).toString('hex'))
})
