import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js'
import { managerClient } from '../grpcClient'

type LogSignal = {
  raw: string
  pretty: string
  searchText: string
  title: string
  subtitle: string
  timestampText: string
  uid: string
  timestampMs: number | undefined
}

function prettyJsonLine(line: string): string {
  try {
    return JSON.stringify(JSON.parse(line), null, 2)
  } catch {
    return line
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function toStringValue(value: unknown): string | undefined {
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  return undefined
}

function extractField(
  value: unknown,
  candidates: string[],
  depth = 0,
): unknown | undefined {
  if (!isRecord(value) || depth > 2) {
    return undefined
  }

  for (const key of candidates) {
    if (key in value) {
      const found = value[key]
      if (found !== undefined && found !== null) {
        return found
      }
    }
  }

  const nestedKeys = ['fields', 'span', 'resource', 'metadata', 'body', 'record', 'context']
  for (const nestedKey of nestedKeys) {
    const nestedValue = value[nestedKey]
    const nested = extractField(nestedValue, candidates, depth + 1)
    if (nested !== undefined) {
      return nested
    }
  }

  return undefined
}

function normalizeNumericEpoch(value: number): number | undefined {
  if (!Number.isFinite(value)) {
    return undefined
  }
  if (value > 1e18) {
    return value / 1_000_000
  }
  if (value > 1e16) {
    return value / 1_000
  }
  if (value > 1e12) {
    return value
  }
  if (value > 1e9) {
    return value * 1_000
  }
  return undefined
}

function parseIntegerTimestamp(value: string): number | undefined {
  if (!/^-?\d+$/.test(value)) {
    return undefined
  }
  const trimmed = value.trim()
  if (trimmed.length > 16) {
    try {
      return Number(BigInt(trimmed) / 1_000_000n)
    } catch {
      return undefined
    }
  }
  if (trimmed.length > 13) {
    try {
      return Number(BigInt(trimmed) / 1_000n)
    } catch {
      return undefined
    }
  }
  return Number(trimmed)
}

function normalizeTimestamp(value: unknown): number | undefined {
  if (typeof value === 'number') {
    return normalizeNumericEpoch(value)
  }
  if (typeof value !== 'string') {
    return undefined
  }
  const trimmed = value.trim()
  if (!trimmed) {
    return undefined
  }
  const numeric = parseIntegerTimestamp(trimmed)
  if (numeric !== undefined) {
    return numeric
  }
  const parsed = Date.parse(trimmed)
  return Number.isNaN(parsed) ? undefined : parsed
}

function formatLogTimestamp(value: unknown): string {
  const normalized = normalizeTimestamp(value)
  if (normalized === undefined) {
    return toStringValue(value) ?? ''
  }
  const date = new Date(normalized)
  if (Number.isNaN(date.getTime())) {
    return toStringValue(value) ?? ''
  }
  return date.toLocaleString(undefined, {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
  })
}

function makeLogSignalUid(raw: string, timestampMs: number | undefined): string {
  let hash = 0
  const base = `${timestampMs ?? ''}|${raw}`
  for (let i = 0; i < base.length; i++) {
    hash = (hash * 31 + base.charCodeAt(i)) % 0x7fffffff
  }
  return `${timestampMs ?? 'no-ts'}-${base.length}-${hash}`
}

function normalizeSignal(line: string): LogSignal {
  let parsed: Record<string, unknown> | undefined
  try {
    const value = JSON.parse(line)
    if (isRecord(value)) {
      parsed = value
    }
  } catch {
    parsed = undefined
  }

  const message = toStringValue(extractField(parsed, ['message', 'msg', 'event', 'msg_template'])) || line
  const level = toStringValue(extractField(parsed, ['level', 'severity', 'severity_text', 'severityText']))
  const source = toStringValue(extractField(parsed, ['target', 'component', 'service', 'module', 'logger_name', 'loggerName']))
  const rawTimestamp = extractField(parsed, ['timestamp', 'time', 'ts'])
  const timestampMs = normalizeTimestamp(rawTimestamp)
  const timestampText = formatLogTimestamp(timestampMs ?? rawTimestamp)

  return {
    raw: line,
    pretty: prettyJsonLine(line),
    searchText: [line, message, level, source, timestampText].join(' ').toLowerCase(),
    title: message,
    subtitle: [level, source].filter(Boolean).join(' · '),
    timestampText,
    uid: makeLogSignalUid(line, timestampMs),
    timestampMs,
  }
}

function logLevelClass(level: string | undefined): string {
  const normalized = (level ?? '').toLowerCase()
  if (normalized.includes('error') || normalized.includes('fatal') || normalized.includes('critical')) {
    return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
  }
  if (normalized.includes('warn')) {
    return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
  }
  if (normalized.includes('info')) {
    return 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'
  }
  if (normalized.includes('debug')) {
    return 'bg-neutral-100 text-neutral-700 dark:bg-neutral-900/70 dark:text-neutral-200'
  }
  return 'bg-neutral-100 text-neutral-700 dark:bg-neutral-900/70 dark:text-neutral-200'
}

function LogSignalRow(props: {
  signal: LogSignal
  expanded: boolean
  onToggle: (id: string) => void
}) {
  const [level, source] = props.signal.subtitle.split(' · ')

  return (
    <article class="overflow-hidden border-b border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-950">
      <button
        type="button"
        class="grid w-full cursor-pointer grid-cols-[8.5rem_1fr_auto] items-start gap-3 px-3 py-2 text-left"
        onClick={() => props.onToggle(props.signal.uid)}
        aria-expanded={props.expanded}
      >
        <span class="shrink-0 text-[11px] leading-5 font-mono text-neutral-500 tabular-nums">
          {props.signal.timestampText || '—'}
        </span>
        <span class="min-w-0 text-sm break-words font-mono text-neutral-900 dark:text-neutral-100">
          {props.signal.title}
        </span>
        <span class="inline-flex items-center justify-center gap-1 text-[11px] font-medium text-neutral-500">
          <Show when={level}>
            <span class={`rounded px-2 py-0.5 font-mono ${logLevelClass(level)}`}>{level}</span>
          </Show>
          <Show when={source}>
            <span class="hidden text-[10px] uppercase text-neutral-400 md:inline">{source}</span>
          </Show>
          <span>{props.expanded ? '▾' : '▸'}</span>
        </span>
      </button>
      <Show when={props.expanded}>
        <section class="border-t border-neutral-200 bg-neutral-50 px-3 py-2 dark:border-neutral-800 dark:bg-neutral-900/70">
          <p class="mb-2 text-[11px] font-mono text-neutral-500">{props.signal.subtitle || 'Log entry attributes'}</p>
          <pre class="max-h-56 overflow-auto rounded bg-neutral-100 p-2 text-[11px] font-mono leading-5 dark:bg-neutral-900">{props.signal.pretty}</pre>
          <Show when={props.signal.timestampMs !== undefined}>
            <p class="mt-2 text-[10px] text-neutral-500">epoch_ms: {props.signal.timestampMs}</p>
          </Show>
        </section>
      </Show>
    </article>
  )
}

function SignalCard(props: {
  signal: LogSignal
  expanded: boolean
  onToggle: (id: string) => void
}) {
  return <LogSignalRow signal={props.signal} expanded={props.expanded} onToggle={props.onToggle} />
}

export default function Observability() {
  const [o11y, o11yActions] = createResource(async () => managerClient.getObservability({ maxLines: 200 }))
  const [liveUpdating, setLiveUpdating] = createSignal(true)
  const [searchTerm, setSearchTerm] = createSignal('')
  const [expandedLogIds, setExpandedLogIds] = createSignal<Set<string>>(new Set())

  createEffect(() => {
    if (!liveUpdating()) {
      return
    }
    const timer = setInterval(() => {
      void o11yActions.refetch()
    }, 2000)
    onCleanup(() => clearInterval(timer))
  })

  const parsedLogs = createMemo(() =>
    (o11y()?.logs ?? [])
      .map((line) => normalizeSignal(line))
      .sort((a, b) => (b.timestampMs ?? 0) - (a.timestampMs ?? 0)),
  )
  const query = createMemo(() => searchTerm().trim().toLowerCase())
  const filteredLogs = createMemo(() => {
    const queryValue = query()
    if (!queryValue) {
      return parsedLogs()
    }
    return parsedLogs().filter((entry) => entry.searchText.includes(queryValue))
  })

  createEffect(() => {
    const visibleLogIds = new Set(parsedLogs().map((entry) => entry.uid))
    setExpandedLogIds((prev) => {
      const next = new Set(prev)
      next.forEach((id) => {
        if (!visibleLogIds.has(id)) {
          next.delete(id)
        }
      })
      return next
    })
  })

  const isLogExpanded = (id: string) => expandedLogIds().has(id)
  const toggleLogExpanded = (id: string) => {
    setExpandedLogIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }

  return (
    <section class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 dark:border-neutral-800 dark:bg-neutral-900/60">
      <article class="space-y-1">
        <p class="text-sm text-neutral-600 dark:text-neutral-300">Live manager logs are pulled from the observability endpoint.</p>
      </article>

      <Show
        when={o11y()?.o11yEnabled}
        fallback={<p class="text-sm text-neutral-500">Enable manager observability with <code>--o11y</code> to populate logs.</p>}
      >
        <div class="flex flex-wrap items-end justify-between gap-3">
          <p class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Logs</p>
          <div class="flex flex-wrap items-center justify-end gap-3">
            <label class="inline-flex items-center gap-2 text-xs text-neutral-500">
              <input
                type="checkbox"
                class="h-3.5 w-3.5 rounded border-neutral-300 text-neutral-900 dark:border-neutral-600 dark:text-neutral-100 dark:bg-neutral-950"
                checked={liveUpdating()}
                onInput={(event) => setLiveUpdating((event.currentTarget as HTMLInputElement).checked)}
              />
              <span>Live update</span>
            </label>
            <label class="text-xs text-neutral-500">
              <span class="sr-only">Search logs</span>
              <input
                type="search"
                class="w-72 rounded border border-neutral-200 bg-white px-3 py-2 text-sm outline-none ring-0 focus:border-neutral-900 dark:border-neutral-700 dark:bg-neutral-950"
                placeholder="Search message, level, source..."
                value={searchTerm()}
                onInput={(event) => setSearchTerm((event.target as HTMLInputElement).value)}
              />
            </label>
          </div>
        </div>

        <p class="text-xs text-neutral-500">
          Showing {filteredLogs().length} logs
          <Show when={query().length > 0}> matching "{query()}"</Show>
        </p>

        <Show when={filteredLogs().length > 0} fallback={<p class="text-sm text-neutral-500">No logs matched your search.</p>}>
          <section class="max-h-[540px] overflow-auto rounded border border-neutral-200 bg-white text-sm dark:border-neutral-800 dark:bg-neutral-950">
            <For each={filteredLogs()}>{(signal) => <SignalCard signal={signal} expanded={isLogExpanded(signal.uid)} onToggle={toggleLogExpanded} />}</For>
          </section>
        </Show>
      </Show>
    </section>
  )
}
