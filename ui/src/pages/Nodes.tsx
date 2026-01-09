import { For, Show, createMemo, createSignal, type JSX, type Resource } from 'solid-js'
import type { NodeMsg } from '../gen/legion_pb'
import { formatRelativeTime } from '../utils/format'

interface NodesProps {
  nodes: Resource<readonly NodeMsg[] | undefined>
}

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const parseAttributes = (value?: string): unknown => {
  if (!value) return undefined
  try {
    return JSON.parse(value)
  } catch (error) {
    console.warn('Failed to parse attributes', error)
    return value
  }
}

const prettifyData = (value: unknown): string => {
  if (value === undefined || value === null) return ''

  if (typeof value === 'string') {
    try {
      return JSON.stringify(JSON.parse(value), null, 2)
    } catch {
      return value
    }
  }

  if (isPlainObject(value) || Array.isArray(value)) {
    try {
      return JSON.stringify(value, null, 2)
    } catch {
      return String(value)
    }
  }

  return String(value)
}

const extractField = (value: unknown, key: string): unknown =>
  isPlainObject(value) && key in value ? (value as Record<string, unknown>)[key] : undefined

const renderValue = (value: unknown): JSX.Element => {
  if (value === null || value === undefined) return <span class="text-neutral-500">—</span>
  if (Array.isArray(value)) {
    return (
      <ul class="list-disc space-y-1 pl-4 text-sm text-neutral-800 dark:text-neutral-200">
        <For each={value}>{(item) => <li>{renderValue(item)}</li>}</For>
      </ul>
    )
  }

  if (isPlainObject(value)) {
    const entries = Object.entries(value)
    if (!entries.length) return <span class="text-neutral-500">—</span>
    return (
      <div class="space-y-2">
        <For each={entries}>
          {([key, val]) => (
            <div class="rounded border border-neutral-200 bg-white px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
              <div class="text-[11px] uppercase tracking-wide text-neutral-500 dark:text-neutral-400">{key}</div>
              <div class="break-words text-sm text-neutral-800 dark:text-neutral-200">{renderValue(val)}</div>
            </div>
          )}
        </For>
      </div>
    )
  }

  return <span class="break-words font-mono text-sm text-neutral-800 dark:text-neutral-200">{String(value)}</span>
}

interface KeyValueGridProps {
  title: string
  data: unknown
  emptyLabel: string
  showTitle?: boolean
}

const KeyValueGrid = (props: KeyValueGridProps) => (
  <div class="space-y-2">
    <Show when={props.showTitle ?? true}>
      <h4 class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">{props.title}</h4>
    </Show>
    <div class="rounded border border-neutral-200 bg-neutral-50 p-3 dark:border-neutral-800 dark:bg-neutral-900/60">
      <Show
        when={(() => {
          if (props.data === undefined || props.data === null) return false
          if (typeof props.data === 'string' && props.data.trim() === '') return false
          if (isPlainObject(props.data)) return Object.keys(props.data).length > 0
          return true
        })()}
        fallback={<p class="text-sm text-neutral-500">{props.emptyLabel}</p>}
      >
        {isPlainObject(props.data) ? (
          <div class="grid gap-2 sm:grid-cols-2">
            <For each={Object.entries(props.data)}>
              {([key, value]) => (
                <div class="rounded border border-neutral-200 bg-white px-2 py-1 text-sm dark:border-neutral-800 dark:bg-neutral-950/60">
                  <div class="text-[11px] uppercase tracking-wide text-neutral-500 dark:text-neutral-400">{key}</div>
                  <div class="mt-1 break-words text-neutral-800 dark:text-neutral-200">{renderValue(value)}</div>
                </div>
              )}
            </For>
          </div>
        ) : (
          <div class="break-words text-sm text-neutral-800 dark:text-neutral-200">{renderValue(props.data)}</div>
        )}
      </Show>
    </div>
  </div>
)

interface CollapsibleSectionProps {
  title: string
  children: JSX.Element
  defaultOpen?: boolean
}

const CollapsibleSection = (props: CollapsibleSectionProps) => {
  const [open, setOpen] = createSignal(props.defaultOpen ?? true)

  return (
    <div class="rounded border border-neutral-200 bg-white/90 px-3 py-2 shadow-sm dark:border-neutral-800 dark:bg-neutral-900/50">
      <button
        type="button"
        class="flex w-full items-center justify-between text-left"
        onClick={() => setOpen((prev) => !prev)}
        aria-expanded={open()}
      >
        <span class="flex items-center gap-2 text-sm font-semibold text-neutral-900 dark:text-neutral-100">
          <span
            class={`inline-block text-lg leading-none transition-transform ${open() ? 'rotate-90' : 'rotate-0'}`}
            aria-hidden="true"
          >
            ›
          </span>
          {props.title}
        </span>
        <span class="text-xs uppercase tracking-[0.1em] text-neutral-500 dark:text-neutral-400">{open() ? 'Hide' : 'Show'}</span>
      </button>
      <Show when={open()}>
        <div class="mt-2">{props.children}</div>
      </Show>
    </div>
  )
}

export default function Nodes(props: NodesProps) {
  const [statusFilter, setStatusFilter] = createSignal('All')

  const statusOptions = createMemo(() => {
    const options = new Set<string>()
    ;(props.nodes() ?? []).forEach((node) => options.add(node.status || 'Unknown'))
    return ['All', ...Array.from(options).sort((a, b) => a.localeCompare(b))]
  })

  const filteredNodes = createMemo(() => {
    const list = props.nodes() ?? []
    const filtered =
      statusFilter() === 'All'
        ? list
        : list.filter((node) => (node.status || 'Unknown').toLowerCase() === statusFilter().toLowerCase())

    const priority = (node: NodeMsg) => (node.status?.toLowerCase() === 'active' ? 0 : 1)
    return [...filtered].sort((a, b) => {
      const diff = priority(a) - priority(b)
      if (diff !== 0) return diff
      return (a.id || '').localeCompare(b.id || '')
    })
  })

  return (
    <Show when={!props.nodes.loading} fallback={<p class="text-neutral-500">Loading nodes…</p>}>
      <Show when={!props.nodes.error} fallback={<p class="text-red-500">Failed to load nodes.</p>}>
        {filteredNodes().length > 0 ? (
          <div class="space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3 rounded border border-neutral-200 bg-white/80 p-4 text-sm shadow-[4px_4px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:text-neutral-200 dark:shadow-[4px_4px_0_#0b0d12]">
              <div>
                <p class="text-xs uppercase tracking-[0.14em] text-neutral-500 dark:text-neutral-400">Node Status</p>
                <p class="text-base font-semibold text-neutral-900 dark:text-neutral-100">{filteredNodes().length} visible</p>
              </div>
              <label class="flex flex-wrap items-center gap-2 text-xs font-medium uppercase tracking-[0.12em] text-neutral-600 dark:text-neutral-300">
                <span>Filter</span>
                <select
                  class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm font-normal lowercase text-neutral-800 shadow-[3px_3px_0_#d4d4d8] transition hover:-translate-y-0.5 focus:outline-none dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100 dark:shadow-[3px_3px_0_#0b0d12]"
                  value={statusFilter()}
                  onInput={(event) => setStatusFilter(event.currentTarget.value)}
                >
                  <For each={statusOptions()}>{(status) => <option value={status}>{status}</option>}</For>
                </select>
              </label>
            </div>

            <div class="grid gap-3 lg:grid-cols-2">
              <For each={filteredNodes()}>
                {(node) => {
                const configData = parseAttributes(node.configAttrs)
                const dynamicData = parseAttributes(node.dynamicAttrs)
                const fingerprintData = parseAttributes(node.fingerprintAttrs)
                const networksData = extractField(dynamicData, 'networks') ?? extractField(fingerprintData, 'networks')
                const signalsData = extractField(dynamicData, 'signals') ?? extractField(fingerprintData, 'signals')
                  const healthData = parseAttributes(node.healthChecks)
                  const formattedHealth = prettifyData(healthData ?? node.healthChecks)

                  return (
                    <article class="rounded border border-neutral-200 bg-white/90 p-4 shadow-[5px_5px_0_#d4d4d8] transition dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
                      <div class="flex flex-wrap items-center justify-between gap-3">
                        <div>
                          <p class="text-xs uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Node</p>
                          <p class="text-lg font-semibold text-neutral-900 dark:text-neutral-100">{node.id || 'Unknown node'}</p>
                          <p class="text-sm text-neutral-500 dark:text-neutral-400">Pool: {node.pool || '—'}</p>
                        </div>
                        <div class="flex flex-col items-end gap-2 text-right">
                          <span class="rounded border border-neutral-300 px-2 py-1 text-[11px] uppercase tracking-wide text-neutral-800 dark:border-neutral-700 dark:text-neutral-200">
                            {node.status || 'Unknown'}
                          </span>
                          <span class="text-xs text-neutral-600 dark:text-neutral-400">Last heartbeat {formatRelativeTime(node.lastHeartbeat)}</span>
                        </div>
                      </div>

                      <div class="mt-3 grid gap-2 text-sm text-neutral-700 dark:text-neutral-300 sm:grid-cols-2 lg:grid-cols-3">
                        <p class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-950/60">
                          Availability: {node.availability || '—'}
                        </p>
                      </div>

                      <div class="mt-4 space-y-3">
                        <div class="grid gap-3 lg:grid-cols-2">
                          <KeyValueGrid
                            title="Static Config"
                            data={configData}
                            emptyLabel="No static config provided"
                          />
                          <KeyValueGrid
                            title="Dynamic Config"
                            data={dynamicData}
                            emptyLabel="No dynamic config provided"
                          />
                        </div>

                        <CollapsibleSection title="Fingerprint" defaultOpen={false}>
                          <KeyValueGrid
                            title="Fingerprint"
                            showTitle={false}
                            data={fingerprintData}
                            emptyLabel="No fingerprint data"
                          />
                        </CollapsibleSection>

                        <CollapsibleSection title="Networks" defaultOpen={false}>
                          <KeyValueGrid
                            title="Networks"
                            showTitle={false}
                            data={networksData}
                            emptyLabel="No network data"
                          />
                        </CollapsibleSection>

                        <CollapsibleSection title="Signals" defaultOpen={false}>
                          <KeyValueGrid
                            title="Signals"
                            showTitle={false}
                            data={signalsData}
                            emptyLabel="No signal data"
                          />
                        </CollapsibleSection>

                        <CollapsibleSection title="Health Checks" defaultOpen={false}>
                          <div class="rounded border border-neutral-200 bg-neutral-50 p-3 shadow-sm dark:border-neutral-800 dark:bg-neutral-900/60">
                            <div class="flex items-center justify-between">
                              <h4 class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Health Checks</h4>
                              <span class="text-[11px] uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Status</span>
                            </div>
                            <Show
                              when={formattedHealth.trim().length > 0}
                              fallback={<p class="mt-2 text-sm text-neutral-500">No health check data</p>}
                            >
                              <pre class="mt-2 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded bg-white/70 p-3 font-mono text-xs leading-relaxed text-neutral-800 shadow-inner dark:bg-neutral-950 dark:text-neutral-100">
                                {formattedHealth}
                              </pre>
                            </Show>
                          </div>
                        </CollapsibleSection>
                      </div>
                    </article>
                  )
                }}
              </For>
            </div>
          </div>
        ) : (
          <p class="text-neutral-500">No nodes discovered yet.</p>
        )}
      </Show>
    </Show>
  )
}
