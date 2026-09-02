import { createSignal } from 'solid-js'

/*
 * The last few detail pages this browser opened, for ⌘K. Operators bounce
 * between the same handful of jobs and workers during an incident; a recent
 * list puts them one keystroke away without typing the name again.
 *
 * Stored per browser in localStorage. Recording happens in the layout from
 * the URL, so no page needs to know this exists.
 */

export type RecentItem = {
  href: string
  label: string
  /** Where it came from — "Jobs", "Workers", … — shown as the group. */
  kind: string
  at: number
}

const STORAGE_KEY = 'legion-recent-pages'
const MAX_RECENTS = 8

const isRecent = (value: unknown): value is RecentItem =>
  typeof value === 'object' &&
  value !== null &&
  typeof (value as RecentItem).href === 'string' &&
  typeof (value as RecentItem).label === 'string' &&
  typeof (value as RecentItem).kind === 'string'

const load = (): RecentItem[] => {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    const parsed: unknown = raw ? JSON.parse(raw) : []
    return Array.isArray(parsed) ? parsed.filter(isRecent) : []
  } catch {
    return []
  }
}

const [recents, setRecents] = createSignal<RecentItem[]>(load())

const persist = (items: RecentItem[]) => {
  setRecents(items)
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(items))
  } catch {
    // Storage full or unavailable: the in-memory list still works this session.
  }
}

export { recents }

export const recordRecent = (item: Omit<RecentItem, 'at'>) => {
  const rest = recents().filter((entry) => entry.href !== item.href)
  persist([{ ...item, at: Date.now() }, ...rest].slice(0, MAX_RECENTS))
}

export const clearRecents = () => persist([])
