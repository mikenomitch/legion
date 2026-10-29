import { Code, ConnectError } from '@connectrpc/connect'
import { createResource, createRoot, createSignal } from 'solid-js'
import { TOKEN_CHANGED_EVENT, managerClient } from '../grpcClient'
import type { AppViewContext, UiExtension } from '../gen/legion_pb'

/*
 * App UI extensions, tier 1 (docs/apps.md, "Views and actions";
 * docs/ui-architecture.md, "App extensions").
 *
 * Installed apps register slots (a sidebar page, an overview card, a panel on
 * job, task, and node pages, a badge on task rows) and answer each with a
 * declarative view: a `legion-view/1` JSON document built from a closed set
 * of blocks, which this SPA draws with its own components. App code never
 * runs here. The manager authorizes every request as the viewer before it
 * asks the app, and validates what the app answers, so everything below is
 * data that has already passed the schema; the renderer still treats it as
 * untrusted text (no innerHTML anywhere).
 */

export type ViewTone = 'neutral' | 'info' | 'ok' | 'warn' | 'danger'

export type Stat = { label: string; value: string; tone?: ViewTone; hint?: string }
export type SeriesLine = { name: string; axis?: 'left' | 'right'; unit?: string; points: Array<[number, number]> }
export type Column = { key: string; label: string; type?: 'text' | 'number' | 'time' | 'duration' }
export type RichText = { text: string; route?: string; href?: string; tone?: ViewTone }
export type Cell = RichText | number | boolean | string
export type TimelineItem = { at: number; title: string; detail?: string; tone?: ViewTone }
export type KeyValueItem = { key: string; value: string; tone?: ViewTone; route?: string }
export type ActionButton = { action: string; label?: string; inputs?: Record<string, unknown> }

export type Block =
  | { type: 'stats'; title?: string; items: Stat[] }
  | { type: 'series'; title?: string; series: SeriesLine[] }
  | { type: 'table'; title?: string; columns: Column[]; rows: Array<Record<string, Cell>>; empty?: string }
  | { type: 'timeline'; title?: string; items: TimelineItem[] }
  | { type: 'keyValue'; title?: string; items: KeyValueItem[] }
  | { type: 'notice'; tone?: ViewTone; title?: string; text: string }
  | { type: 'markdown'; text: string }
  | { type: 'actions'; items: ActionButton[] }

export type ViewDocument = {
  view?: string
  hidden?: boolean
  title?: string
  badge?: { text: string; tone?: ViewTone }
  blocks?: Block[]
}

export type InputDecl = {
  type: 'string' | 'number' | 'duration' | 'enum' | 'boolean'
  default?: unknown
  values?: string[]
  label?: string
  required?: boolean
}

export type Slot = 'nav' | 'overview' | 'job' | 'task' | 'node'

/** A context for `slot`, as the RPCs take it. */
export function viewContext(slot: Slot, fields: Partial<Omit<AppViewContext, 'slot'>> = {}): Partial<AppViewContext> {
  return { slot, namespace: '', job: '', group: '', task: '', node: '', ...fields }
}

/** Parse a view document the manager already validated. */
export function parseView(json: string): ViewDocument {
  try {
    const parsed = JSON.parse(json) as ViewDocument
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

export function parseInputs(json: string): Record<string, InputDecl> {
  try {
    const parsed = JSON.parse(json) as Record<string, InputDecl>
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch {
    return {}
  }
}

/** An in-app route the SPA may navigate to: an absolute path, one slash. */
export const isRoute = (value: string | undefined): value is string =>
  typeof value === 'string' && value.startsWith('/') && !value.startsWith('//') && !/[\s\\]/.test(value)

/** An external link the SPA may open: http(s) only. */
export const isHref = (value: string | undefined): value is string => {
  if (typeof value !== 'string') return false
  try {
    const url = new URL(value)
    return url.protocol === 'http:' || url.protocol === 'https:'
  } catch {
    return false
  }
}

/** Whether an RPC failure means "this viewer may not see it". */
export const isDenied = (error: unknown): boolean =>
  error instanceof ConnectError && (error.code === Code.PermissionDenied || error.code === Code.Unauthenticated)

/** A short sentence for an RPC failure. */
export const errorText = (error: unknown): string =>
  error instanceof ConnectError ? error.rawMessage : error instanceof Error ? error.message : String(error)

// The registry at login: every entry the viewer may see anywhere. The sidebar
// and the overview read it; job, task, and node pages ask again with their
// context, which filters precisely.
const registry = createRoot(() => {
  const [version, setVersion] = createSignal(0)
  const [entries] = createResource(
    () => version() + 1,
    async (): Promise<UiExtension[]> => {
      try {
        return (await managerClient.listUiExtensions({})).extensions
      } catch {
        // No app:read, or an older manager: no extensions, not an error.
        return []
      }
    },
  )
  const reload = () => setVersion((value) => value + 1)
  // A login or logout changes what the viewer may see.
  if (typeof window !== 'undefined') window.addEventListener(TOKEN_CHANGED_EVENT, reload)
  return { entries, reload }
})

/** The extensions the viewer may see somewhere, fetched once per login. */
export const uiExtensions = (): UiExtension[] => registry.entries() ?? []

/** Fetch the registry again (after a login, or an app install). */
export const reloadUiExtensions = () => registry.reload()

/** Where an app's nav page lives. */
export const appPagePath = (app: string, view: string) =>
  `/apps/${encodeURIComponent(app)}/${encodeURIComponent(view)}`

/** Where an app's sandboxed page (`ui.pages`, tier 2) lives. */
export const appSandboxPath = (app: string, page: string) =>
  `/apps/${encodeURIComponent(app)}/pages/${encodeURIComponent(page)}`

/** Where a sidebar entry (a view, or a sandboxed page) opens. */
export const navEntryPath = (entry: UiExtension) =>
  entry.page ? appSandboxPath(entry.app, entry.page) : appPagePath(entry.app, entry.view)

/** A unix time, in the viewer's locale. */
export function formatTime(unixSeconds: number): string {
  if (!Number.isFinite(unixSeconds)) return ''
  return new Date(unixSeconds * 1000).toLocaleString()
}

export function formatDuration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return ''
  const units: Array<[string, number]> = [
    ['d', 86400],
    ['h', 3600],
    ['m', 60],
    ['s', 1],
  ]
  let rest = Math.round(seconds)
  const parts: string[] = []
  for (const [unit, size] of units) {
    if (rest >= size) {
      parts.push(`${Math.floor(rest / size)}${unit}`)
      rest %= size
    }
    if (parts.length === 2) break
  }
  return parts.length > 0 ? parts.join(' ') : '0s'
}

export function formatNumber(value: number): string {
  if (!Number.isFinite(value)) return ''
  if (Number.isInteger(value)) return String(value)
  return value.toFixed(Math.abs(value) < 10 ? 2 : 1)
}
