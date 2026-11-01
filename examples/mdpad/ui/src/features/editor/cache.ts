// The copy a browser keeps of a document, in its IndexedDB database (copies.ts). Beside the Y.Doc's updates,
// y-indexeddb's own store keeps how many changes are not saved, and the title and level last seen, for a page opened
// offline.

import { IndexeddbPersistence } from 'y-indexeddb'
import type * as Y from 'yjs'
import { database } from './copies'

export interface Kept {
  /** Changes not saved when the copy was last written. */
  pending: number
  title: string | null
  level: string | null
}

export class Copy {
  private readonly persistence: IndexeddbPersistence
  private last: Kept = { pending: 0, title: null, level: null }

  constructor(user: string, id: string, doc: Y.Doc) {
    this.persistence = new IndexeddbPersistence(database(user, id), doc)
  }

  /** The doc holds what the copy kept: the updates, and what `keep` wrote. */
  async load(): Promise<Kept> {
    await this.persistence.whenSynced
    const [pending, title, level] = await Promise.all(['pending', 'title', 'level'].map((key) => this.persistence.get(key).catch(() => undefined)))
    this.last = {
      pending: typeof pending === 'number' ? pending : 0,
      title: typeof title === 'string' ? title : null,
      level: typeof level === 'string' ? level : null,
    }
    return this.last
  }

  /** Writes what changed of `kept`; a failed write is tried again with the next. */
  keep(kept: Partial<Kept>): void {
    for (const [key, value] of Object.entries(kept) as [keyof Kept, Kept[keyof Kept]][]) {
      if (value === undefined || value === null || value === this.last[key]) continue
      const last = this.last as unknown as Record<string, unknown>
      last[key] = value
      this.persistence.set(key, value).catch(() => (last[key] = undefined))
    }
  }

  destroy(): void {
    void this.persistence.destroy()
  }
}
