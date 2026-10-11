/*
 * A small line diff for showing what a submission would change.
 *
 * Classic LCS over lines, after trimming the common prefix and suffix: specs
 * that differ in a few places (the usual case) leave a tiny middle to
 * compare, so the quadratic table stays small. Inputs whose differing middle
 * would still need a large table are reported as too large rather than
 * freezing the tab — the caller falls back to showing the full text.
 */

export type DiffLineKind = 'context' | 'add' | 'remove'

export type DiffLine = {
  kind: DiffLineKind
  text: string
  /** 1-based line number in the old text (context and remove lines). */
  oldNo?: number
  /** 1-based line number in the new text (context and add lines). */
  newNo?: number
}

export type DiffHunk = {
  /** Unified-diff range header, e.g. `@@ -12,7 +12,8 @@`. */
  header: string
  lines: DiffLine[]
}

export type LineDiff =
  | { kind: 'ok'; hunks: DiffHunk[]; added: number; removed: number }
  | { kind: 'too-large'; oldLines: number; newLines: number }

/** LCS table cells allowed for the differing middle (~5 MB as Uint16). */
export const MAX_DIFF_CELLS = 2_500_000
/** Total input lines beyond which the result is too long to read anyway. */
export const MAX_DIFF_LINES = 20_000

const splitLines = (text: string): string[] => {
  if (text === '') return []
  const lines = text.split('\n')
  // A trailing newline ends the last line; it does not start an empty one.
  if (lines[lines.length - 1] === '') lines.pop()
  return lines
}

/**
 * Unified line diff of `oldText` -> `newText` with `context` unchanged lines
 * around each change. Hunks whose context would touch or overlap are merged,
 * as `diff -u` does. Identical inputs give `{ kind: 'ok', hunks: [] }`.
 */
export function diffLines(oldText: string, newText: string, context = 3): LineDiff {
  const a = splitLines(oldText)
  const b = splitLines(newText)
  if (a.length + b.length > MAX_DIFF_LINES) {
    return { kind: 'too-large', oldLines: a.length, newLines: b.length }
  }

  let start = 0
  while (start < a.length && start < b.length && a[start] === b[start]) start += 1
  let endA = a.length
  let endB = b.length
  while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) {
    endA -= 1
    endB -= 1
  }

  const n = endA - start
  const m = endB - start
  if (n * m > MAX_DIFF_CELLS) {
    return { kind: 'too-large', oldLines: a.length, newLines: b.length }
  }

  // table[i][j] = LCS length of a[start+i..endA) and b[start+j..endB).
  // Row n and column m stay 0. Values never exceed min(n, m), which the
  // cell cap keeps far below Uint16's ceiling.
  const width = m + 1
  const table = new Uint16Array((n + 1) * width)
  for (let i = n - 1; i >= 0; i -= 1) {
    for (let j = m - 1; j >= 0; j -= 1) {
      table[i * width + j] =
        a[start + i] === b[start + j]
          ? table[(i + 1) * width + j + 1] + 1
          : Math.max(table[(i + 1) * width + j], table[i * width + j + 1])
    }
  }

  const lines: DiffLine[] = []
  for (let k = 0; k < start; k += 1) {
    lines.push({ kind: 'context', text: a[k], oldNo: k + 1, newNo: k + 1 })
  }
  let i = 0
  let j = 0
  while (i < n && j < m) {
    if (a[start + i] === b[start + j]) {
      lines.push({ kind: 'context', text: a[start + i], oldNo: start + i + 1, newNo: start + j + 1 })
      i += 1
      j += 1
    } else if (table[(i + 1) * width + j] >= table[i * width + j + 1]) {
      // Ties drop the old line first, so a replaced line reads as
      // "- old" then "+ new" rather than the other way round.
      lines.push({ kind: 'remove', text: a[start + i], oldNo: start + i + 1 })
      i += 1
    } else {
      lines.push({ kind: 'add', text: b[start + j], newNo: start + j + 1 })
      j += 1
    }
  }
  for (; i < n; i += 1) lines.push({ kind: 'remove', text: a[start + i], oldNo: start + i + 1 })
  for (; j < m; j += 1) lines.push({ kind: 'add', text: b[start + j], newNo: start + j + 1 })
  for (let k = 0; endA + k < a.length; k += 1) {
    lines.push({ kind: 'context', text: a[endA + k], oldNo: endA + k + 1, newNo: endB + k + 1 })
  }

  let added = 0
  let removed = 0
  const changes: number[] = []
  lines.forEach((line, index) => {
    if (line.kind === 'add') added += 1
    if (line.kind === 'remove') removed += 1
    if (line.kind !== 'context') changes.push(index)
  })
  if (changes.length === 0) return { kind: 'ok', hunks: [], added: 0, removed: 0 }

  // Lines of each side consumed before index k, for the range headers.
  const oldBefore: number[] = []
  const newBefore: number[] = []
  let oldSeen = 0
  let newSeen = 0
  for (const line of lines) {
    oldBefore.push(oldSeen)
    newBefore.push(newSeen)
    if (line.kind !== 'add') oldSeen += 1
    if (line.kind !== 'remove') newSeen += 1
  }

  const hunks: DiffHunk[] = []
  const pushHunk = (from: number, to: number) => {
    const slice = lines.slice(from, to)
    const oldCount = slice.filter((line) => line.kind !== 'add').length
    const newCount = slice.filter((line) => line.kind !== 'remove').length
    // diff -u convention: an empty side names the line *before* the hunk.
    const oldStart = oldCount === 0 ? oldBefore[from] : oldBefore[from] + 1
    const newStart = newCount === 0 ? newBefore[from] : newBefore[from] + 1
    hunks.push({ header: `@@ -${oldStart},${oldCount} +${newStart},${newCount} @@`, lines: slice })
  }

  let hunkStart = Math.max(0, changes[0] - context)
  let hunkEnd = Math.min(lines.length, changes[0] + context + 1)
  for (const index of changes.slice(1)) {
    if (index - context <= hunkEnd) {
      hunkEnd = Math.min(lines.length, index + context + 1)
    } else {
      pushHunk(hunkStart, hunkEnd)
      hunkStart = index - context
      hunkEnd = Math.min(lines.length, index + context + 1)
    }
  }
  pushHunk(hunkStart, hunkEnd)

  return { kind: 'ok', hunks, added, removed }
}

const sortKeysDeep = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(sortKeysDeep)
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    return Object.fromEntries(
      Object.keys(record)
        .sort()
        .map((key) => [key, sortKeysDeep(record[key])]),
    )
  }
  return value
}

/**
 * JSON with object keys sorted at every depth. Two documents that differ only
 * in key order print identically, so a diff of them shows real changes only.
 * `indent` 0 gives the compact form, useful as an equality key.
 */
export const canonicalJson = (value: unknown, indent = 2): string =>
  JSON.stringify(sortKeysDeep(value), null, indent || undefined)
