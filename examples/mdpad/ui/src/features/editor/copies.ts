// The names of the copies a browser keeps of documents, in IndexedDB: one database per user and document, so a
// user's sign-out clears only theirs and another user of the browser never opens them. Apart from cache.ts, so the
// documents page clears them without loading Yjs and the editor.

const PREFIX = 'mdpad/'

export const database = (user: string, id: string) => `${PREFIX}${user}/${id}`

/** Removes every copy `user` keeps in this browser. */
export async function clearUser(user: string): Promise<void> {
  const names = (await indexedDB.databases()).map((db) => db.name ?? '').filter((name) => name.startsWith(`${PREFIX}${user}/`))
  const remove = (name: string) =>
    new Promise<void>((done) => {
      const ask = indexedDB.deleteDatabase(name)
      ask.onsuccess = () => done()
      ask.onerror = () => done()
      ask.onblocked = () => done()
    })
  await Promise.all(names.map(remove))
}

/** The documents of `user` whose copy in this browser holds changes the server has not saved. */
export async function unsaved(user: string): Promise<number> {
  const names = (await indexedDB.databases()).map((db) => db.name ?? '').filter((name) => name.startsWith(`${PREFIX}${user}/`))
  // y-indexeddb keeps the values a copy sets in its "custom" store.
  const pending = (name: string) =>
    new Promise<number>((done) => {
      const open = indexedDB.open(name)
      open.onerror = () => done(0)
      open.onsuccess = () => {
        const db = open.result
        try {
          const read = db.transaction('custom', 'readonly').objectStore('custom').get('pending')
          read.onsuccess = () => done(typeof read.result === 'number' ? read.result : 0)
          read.onerror = () => done(0)
        } catch {
          done(0)
        } finally {
          db.close()
        }
      }
    })
  const counts = await Promise.all(names.map(pending))
  return counts.filter((n) => n > 0).length
}
