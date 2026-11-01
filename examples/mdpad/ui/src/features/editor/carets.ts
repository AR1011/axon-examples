// The others' carets and selections, in the colour and with the name the server set, and this client's own cursor
// into the awareness state. A caret's name shows for 1.5 s after it moves, and on hover. In place of
// y-codemirror.next's yRemoteSelections, whose widget compares only the colour, so a name tag never changes, and
// whose tag shows on hover only.

import { Annotation, RangeSet, type Range } from '@codemirror/state'
import { Decoration, EditorView, ViewPlugin, WidgetType, type DecorationSet, type ViewUpdate } from '@codemirror/view'
import type { Awareness } from 'y-protocols/awareness'
import * as Y from 'yjs'

/** How long a caret's name shows after it moves. */
export const TAG_MS = 1_500

const redraw = Annotation.define<true>()

class Caret extends WidgetType {
  constructor(
    readonly colour: string,
    readonly name: string,
    readonly recent: boolean,
  ) {
    super()
  }

  toDOM(): HTMLElement {
    const caret = document.createElement('span')
    caret.className = this.recent ? 'md-caret md-caret-recent' : 'md-caret'
    caret.style.borderColor = this.colour
    caret.setAttribute('aria-hidden', 'true')
    caret.dataset.name = this.name
    const tag = caret.appendChild(document.createElement('span'))
    tag.className = 'md-caret-tag'
    tag.style.backgroundColor = this.colour
    tag.textContent = this.name
    caret.append('⁠')
    return caret
  }

  eq(other: Caret): boolean {
    return other.colour === this.colour && other.name === this.name && other.recent === this.recent
  }

  ignoreEvent(): boolean {
    return true
  }
}

export function carets(text: Y.Text, awareness: Awareness) {
  return ViewPlugin.fromClass(
    class {
      decorations: DecorationSet = RangeSet.empty
      /** When each client's caret last moved, and where it was. */
      private moved = new Map<number, { at: number; head: string }>()
      private timer: ReturnType<typeof setTimeout> | null = null
      private readonly listener = ({ added, updated, removed }: Record<string, number[]>) => {
        if ([...added, ...updated, ...removed].some((client) => client !== awareness.clientID)) this.soon(0)
      }

      constructor(readonly view: EditorView) {
        awareness.on('change', this.listener)
      }

      update(update: ViewUpdate): void {
        this.setOwn(update)
        this.decorations = this.draw(update.view)
      }

      destroy(): void {
        awareness.off('change', this.listener)
        if (this.timer) clearTimeout(this.timer)
      }

      private soon(ms: number): void {
        if (this.timer) clearTimeout(this.timer)
        this.timer = setTimeout(() => {
          this.timer = null
          this.view.dispatch({ annotations: redraw.of(true) })
        }, ms)
      }

      /** This client's cursor, while the editor has the focus. */
      private setOwn(update: ViewUpdate): void {
        const local = awareness.getLocalState()
        if (!local || !(update.selectionSet || update.focusChanged || update.docChanged)) return
        const focused = update.view.hasFocus && update.view.dom.ownerDocument.hasFocus()
        if (!focused) return
        const { anchor, head } = update.state.selection.main
        const next = { anchor: Y.createRelativePositionFromTypeIndex(text, anchor), head: Y.createRelativePositionFromTypeIndex(text, head) }
        const now = local.cursor as { anchor: object; head: object } | undefined
        const same =
          now &&
          Y.compareRelativePositions(Y.createRelativePositionFromJSON(now.anchor), next.anchor) &&
          Y.compareRelativePositions(Y.createRelativePositionFromJSON(now.head), next.head)
        if (!same) awareness.setLocalStateField('cursor', next)
      }

      private draw(view: EditorView): DecorationSet {
        const doc = text.doc
        if (!doc) return RangeSet.empty
        const ranges: Range<Decoration>[] = []
        const now = Date.now()
        let next = Infinity
        for (const [client, state] of awareness.getStates()) {
          const cursor = state.cursor as { anchor?: object; head?: object } | null | undefined
          if (client === awareness.clientID || !cursor?.anchor || !cursor.head || !state.user) continue
          const anchor = Y.createAbsolutePositionFromRelativePosition(Y.createRelativePositionFromJSON(cursor.anchor), doc)
          const head = Y.createAbsolutePositionFromRelativePosition(Y.createRelativePositionFromJSON(cursor.head), doc)
          if (!anchor || !head || anchor.type !== text || head.type !== text) continue
          const length = view.state.doc.length
          const [a, h] = [Math.min(anchor.index, length), Math.min(head.index, length)]
          const colour = /^#[0-9a-f]{6}$/i.test(state.user.color) ? state.user.color : '#888888'
          const name = String(state.user.name ?? '')
          const key = JSON.stringify(cursor.head)
          const last = this.moved.get(client)
          if (!last || last.head !== key) this.moved.set(client, { at: now, head: key })
          const since = now - (this.moved.get(client)?.at ?? 0)
          if (since < TAG_MS) next = Math.min(next, TAG_MS - since)
          if (a !== h) {
            ranges.push(Decoration.mark({ attributes: { style: `background-color: ${colour}33` } }).range(Math.min(a, h), Math.max(a, h)))
          }
          ranges.push(Decoration.widget({ widget: new Caret(colour, name, since < TAG_MS), side: h > a ? -1 : 1 }).range(h))
        }
        for (const client of this.moved.keys()) if (!awareness.getStates().has(client)) this.moved.delete(client)
        if (next !== Infinity) this.soon(next + 20)
        return Decoration.set(ranges, true)
      }
    },
    { decorations: (plugin) => plugin.decorations },
  )
}
