/*
 * Manager config snapshots stamp `updated_at` with the store's own format,
 * `2026-10-12 01:38:19 +00:00:00` (space separator, offset with seconds),
 * which `new Date()` rejects — so every relative-time label read "—". This
 * normalises that shape (and passes RFC 3339 through) to an ISO string the
 * shared `toDate` / `RelativeTime` helpers understand.
 */

const STORE_TIMESTAMP =
  /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)\s*(?:(Z|UTC)|([+-])(\d{2}):?(\d{2})(?::?(\d{2}))?)?$/i

/** Parses a store or RFC 3339 timestamp; undefined when it is neither. */
export function parseManagerTimestamp(value?: string | null): Date | undefined {
  const raw = (value ?? '').trim()
  if (!raw) return undefined
  const match = STORE_TIMESTAMP.exec(raw)
  if (match) {
    const [, day, time, zulu, sign, hours, minutes, seconds] = match
    // No zone at all: the store always writes UTC, so read it as UTC rather
    // than the browser's local zone.
    const offset = zulu || !sign ? 'Z' : `${sign}${hours}:${minutes}`
    const parsed = new Date(`${day}T${time}${offset}`)
    if (Number.isNaN(parsed.getTime())) return undefined
    // ISO offsets stop at minutes; apply a (rare) seconds component by hand.
    const extra = sign && seconds ? Number(seconds) * 1000 * (sign === '-' ? 1 : -1) : 0
    return new Date(parsed.getTime() + extra)
  }
  const fallback = new Date(raw)
  return Number.isNaN(fallback.getTime()) ? undefined : fallback
}

/**
 * The same timestamp as an ISO string, ready for `RelativeTime` /
 * `formatRelativeTime`. Returns the input untouched when it cannot be parsed,
 * so the shared helpers still render their "—" placeholder.
 */
export function normalizeManagerTimestamp(value?: string | null): string | undefined {
  const parsed = parseManagerTimestamp(value)
  return parsed ? parsed.toISOString() : (value ?? undefined)
}
