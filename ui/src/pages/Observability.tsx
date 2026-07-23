import { For, Show, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js'
import { managerClient } from '../grpcClient'
import { EmptyState, Icon, Note, SectionCard, checkboxClass, cx, inputClass } from '../components/ui'
import { badgeToneClasses, statusTone } from '../utils/status'

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
  // `fractionalSecondDigits` is valid at runtime but missing from the ES2020
  // DateTimeFormatOptions typings this project targets, hence the cast.
  return date.toLocaleString(undefined, {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
  } as Intl.DateTimeFormatOptions)
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

// Log levels share the central status vocabulary (error→danger, warn→warn…).
const logLevelClass = (level: string | undefined): string =>
  badgeToneClasses[level?.toLowerCase().includes('info') ? 'info' : statusTone(level)]

function LogSignalRow(props: {
  signal: LogSignal
  expanded: boolean
  onToggle: (id: string) => void
}) {
  const [level, source] = props.signal.subtitle.split(' · ')

  return (
    <article>
      <button
        type="button"
        class="grid w-full cursor-pointer grid-cols-[8.5rem_1fr_auto] items-start gap-3 px-3 py-2 text-left transition-colors hover:bg-surface2/50"
        onClick={() => props.onToggle(props.signal.uid)}
        aria-expanded={props.expanded}
      >
        <span class="shrink-0 font-mono text-2xs leading-5 tabular-nums text-faint">
          {props.signal.timestampText || '—'}
        </span>
        <span class="min-w-0 break-words font-mono text-xs leading-5 text-ink">
          {props.signal.title}
        </span>
        <span class="inline-flex items-center justify-center gap-1.5 text-2xs font-medium text-faint">
          <Show when={level}>
            <span class={`rounded-full px-2 py-0.5 font-mono ${logLevelClass(level)}`}>{level}</span>
          </Show>
          <Show when={source}>
            <span class="hidden text-[10px] uppercase md:inline">{source}</span>
          </Show>
          <Icon name="chevronRight" size={12} class={cx('transition-transform', props.expanded && 'rotate-90')} />
        </span>
      </button>
      <Show when={props.expanded}>
        <section class="border-y border-line bg-surface2/40 px-3 py-2">
          <p class="mb-2 font-mono text-2xs text-faint">{props.signal.subtitle || 'Log entry attributes'}</p>
          <pre class="max-h-56 overflow-auto rounded-md border border-line bg-surface p-2 font-mono text-2xs leading-5 text-muted">{props.signal.pretty}</pre>
          <Show when={props.signal.timestampMs !== undefined}>
            <p class="mt-2 text-[10px] text-faint">epoch_ms: {props.signal.timestampMs}</p>
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
    <Show
      when={o11y()?.o11yEnabled || o11y() === undefined}
      fallback={
        <Note tone="info">
          Enable manager observability with <code class="rounded bg-surface2 px-1 font-mono text-xs">--o11y</code> to populate logs.
        </Note>
      }
    >
      <div class="space-y-3">
        <div class="flex flex-wrap items-center gap-2">
          <div class="relative min-w-[14rem] flex-1">
            <Icon name="search" size={14} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
            <input
              type="search"
              class={cx(inputClass, 'pl-8')}
              placeholder="Search message, level, source…"
              value={searchTerm()}
              onInput={(event) => setSearchTerm((event.target as HTMLInputElement).value)}
              aria-label="Search logs"
            />
          </div>
          <label class="inline-flex cursor-pointer items-center gap-2 rounded-md border border-line bg-surface px-2.5 py-1.5 text-xs font-medium text-muted">
            <input
              type="checkbox"
              class={checkboxClass}
              checked={liveUpdating()}
              onInput={(event) => setLiveUpdating((event.currentTarget as HTMLInputElement).checked)}
            />
            <span>Live update</span>
          </label>
        </div>

        <SectionCard
          title="Manager logs"
          description={
            <>
              {filteredLogs().length} log{filteredLogs().length === 1 ? '' : 's'}
              <Show when={query().length > 0}> matching "{query()}"</Show>
            </>
          }
          flush
        >
          <Show
            when={filteredLogs().length > 0}
            fallback={<EmptyState icon="eye" title="No logs matched" body="Waiting for manager log output." />}
          >
            <section class="max-h-[560px] divide-y divide-line overflow-auto text-sm">
              <For each={filteredLogs()}>{(signal) => <SignalCard signal={signal} expanded={isLogExpanded(signal.uid)} onToggle={toggleLogExpanded} />}</For>
            </section>
          </Show>
        </SectionCard>
      </div>
    </Show>
  )
}
