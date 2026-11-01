// Writes ../../tests/fixtures/yjs.json: the updates the pinned Yjs makes for a few edits with emoji and CJK, which
// tests/yrs.rs applies with yrs. `npm test` checks the file is what this Yjs makes.
import { writeFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
import * as Y from 'yjs'

export const hex = (bytes) => Buffer.from(bytes).toString('hex')

export function fixtures() {
  const doc = new Y.Doc()
  doc.clientID = 1
  const text = doc.getText('md')
  const updates = []
  const record = (u) => updates.push(u)
  doc.on('update', record)
  text.insert(0, '# Notes\n\nA😀B中\n')
  // '# Notes\n\nA' is 10 UTF-16 units and the emoji 2, so 12 is right after the emoji.
  text.insert(12, 'x')
  text.delete(10, 2)
  text.insert(text.length, '👨‍👩‍👧 中文 é\n')
  const partial = 2
  doc.off('update', record)

  // A second tab: another client that starts from the first one's state.
  const tab = new Y.Doc()
  tab.clientID = 7
  for (const u of updates) Y.applyUpdate(tab, u)
  tab.on('update', record)
  tab.getText('md').insert(0, '> ')
  Y.applyUpdate(doc, updates.at(-1))

  return {
    yjs: '13.6.33',
    text: text.toString(),
    length: text.length,
    updates: updates.map(hex),
    partial,
    state: hex(Y.encodeStateAsUpdate(doc)),
    state_vector: hex(Y.encodeStateVector(doc)),
  }
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  writeFileSync(new URL('../../tests/fixtures/yjs.json', import.meta.url), JSON.stringify(fixtures(), null, 2) + '\n')
}
