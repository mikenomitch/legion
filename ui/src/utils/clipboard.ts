/**
 * Copies text that has to be fetched first, without it ever reaching the
 * page: the caller's fetcher resolves the text and it goes straight to the
 * clipboard. Used to copy a hidden secret (a KV value) without displaying it.
 *
 * Browsers only let a page write the clipboard during a user gesture. Handing
 * `ClipboardItem` a *promise* keeps the gesture alive across the fetch (Safari
 * requires this; Chromium and Firefox accept it); where that path is missing
 * or refused, the text is written once the fetch settles, which is still
 * inside the gesture window for an ordinary round trip.
 *
 * Rejects with the fetcher's own error when the fetch fails, and with a
 * readable error when the page has no clipboard access (an insecure origin).
 */
export async function copyFetchedText(fetchText: () => Promise<string>): Promise<void> {
  const clipboard = typeof navigator !== 'undefined' ? navigator.clipboard : undefined
  if (!clipboard) {
    throw new Error('Clipboard access needs a secure origin (HTTPS or localhost)')
  }

  const text = fetchText()
  if (typeof ClipboardItem !== 'undefined' && typeof clipboard.write === 'function') {
    try {
      const blob = text.then((value) => new Blob([value], { type: 'text/plain' }))
      blob.catch(() => undefined) // reported through `text` below, not as unhandled
      await clipboard.write([new ClipboardItem({ 'text/plain': blob })])
      return
    } catch {
      // Fall through: a fetch failure re-throws below, a refused promise
      // item is retried as plain text.
    }
  }
  await clipboard.writeText(await text)
}
