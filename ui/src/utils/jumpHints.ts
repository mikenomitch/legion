import { createEffect, createMemo, createSignal, onCleanup } from 'solid-js'

/**
 * Hold-to-reveal jump hints.
 *
 * Holding the platform's command modifier (⌘ on macOS, Ctrl elsewhere) labels
 * every row of the page's index with a digit and every expanded sidebar item
 * with a letter, so the label *is* the documentation — you learn the shortcut
 * by reaching for the modifier.
 *
 * Rows fire on the modifier alone (⌘1 … ⌘0). Sidebar items need Shift as well
 * (⌘⇧O) because plain modifier+letter is spoken for by the browser and the OS
 * on every platform — ⌘S, ⌘W, ⌘R and friends are not ours to take.
 */

export const isMacPlatform =
  typeof navigator !== 'undefined' && /mac|iphone|ipad|ipod/i.test(navigator.platform || navigator.userAgent)

/** How the reveal modifier is written in help text: `⌘` or `Ctrl`. */
export const JUMP_MODIFIER_LABEL = isMacPlatform ? '⌘' : 'Ctrl'

/** Digits label the first ten rows; `0` reads as "the tenth", as in a dock. */
const INDEX_HINT_KEYS: readonly string[] = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']

/** The key that opens the row at `index`, or nothing past the tenth. */
export const indexHintKey = (index: number): string | undefined => INDEX_HINT_KEYS[index]

/** Row position a digit refers to, or -1 when it labels nothing. */
export const indexHintPosition = (digit: string): number => INDEX_HINT_KEYS.indexOf(digit)

// Letters the browser or the OS keeps for itself on ⌘⇧ / Ctrl+Shift and will
// not hand back to a page: tab and window lifecycle (n/t/w), macOS log-out (q),
// and the devtools trio (c/i/j). Assignment skips them so no sidebar item ends
// up with a hint that silently does something else.
const RESERVED_NAV_LETTERS = new Set(['c', 'i', 'j', 'n', 'q', 't', 'w'])

/**
 * Picks one letter per sidebar label, in sidebar order.
 *
 * Word initials first so hints stay mnemonic where the label allows it
 * ("Rollouts" → R, "KV Store" → K), then the label's remaining letters, then
 * any free letter at all. Returns `''` for an item we could not place, which
 * callers render as no hint rather than as a wrong one.
 */
export function assignNavHintLetters(labels: readonly string[]): string[] {
  const taken = new Set<string>()
  const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('').filter((letter) => !RESERVED_NAV_LETTERS.has(letter))

  return labels.map((label) => {
    const words = label.toLowerCase().split(/[^a-z]+/).filter(Boolean)
    const candidates = [...words.map((word) => word[0]), ...words.join('')]
    const letter =
      candidates.find((candidate) => !RESERVED_NAV_LETTERS.has(candidate) && !taken.has(candidate)) ??
      alphabet.find((candidate) => !taken.has(candidate))
    if (letter) taken.add(letter)
    return letter ?? ''
  })
}

const [jumpHintsVisible, setJumpHintsVisible] = createSignal(false)

export { jumpHintsVisible, setJumpHintsVisible }

/** True when this event carries the reveal modifier for the current platform. */
export const hasJumpModifier = (event: KeyboardEvent): boolean =>
  isMacPlatform ? event.metaKey : event.ctrlKey

const DIGIT_CODE = /^(?:Digit|Numpad)([0-9])$/
const LETTER_CODE = /^Key([A-Z])$/

// `code` is the physical key, so the chords survive non-US layouts and the
// Shift-mangled `key` values ("1" becomes "!"). Layouts and IMEs that leave
// `code` empty still report the character in `key`.
export const jumpDigitFromEvent = (event: KeyboardEvent): string | undefined => {
  const byCode = DIGIT_CODE.exec(event.code ?? '')
  if (byCode) return byCode[1]
  return /^[0-9]$/.test(event.key) ? event.key : undefined
}

export const jumpLetterFromEvent = (event: KeyboardEvent): string | undefined => {
  const byCode = LETTER_CODE.exec(event.code ?? '')
  if (byCode) return byCode[1].toLowerCase()
  return /^[a-z]$/i.test(event.key) ? event.key.toLowerCase() : undefined
}

// One index owns the digit hints at a time. The owner token stops the outgoing
// page's teardown from clearing the incoming page's registration when a route
// change interleaves them.
type IndexJumpRegistration = { owner: symbol; hrefs: string[] }

const [indexJumpRegistration, setIndexJumpRegistration] = createSignal<IndexJumpRegistration | null>(null)

/** Hrefs behind ⌘1 … ⌘0 on the current page, in row order. */
export const indexJumpTargets = createMemo(() => indexJumpRegistration()?.hrefs ?? [])

/**
 * Registers a table's rows as the page's digit-hint targets for as long as the
 * calling component is mounted. Call it once, from the page's primary index.
 */
export function useIndexJumpTargets(hrefs: () => readonly string[]) {
  const owner = Symbol('index-jump-targets')
  createEffect(() => {
    setIndexJumpRegistration({ owner, hrefs: Array.from(hrefs()) })
  })
  onCleanup(() => {
    setIndexJumpRegistration((current) => (current?.owner === owner ? null : current))
  })
}
