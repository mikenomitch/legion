import { For, Show, createMemo, createSignal, type JSX, type Resource } from 'solid-js'
import type { LedgerAllocation, LedgerNode, NodeMsg } from '../gen/legion_pb'
import { formatRelativeTime } from '../utils/format'
import { healthToneClasses, summarizeHealthChecks } from '../utils/health'

interface NodesProps {
  nodes: Resource<readonly NodeMsg[] | undefined>
  ledger: Resource<readonly LedgerNode[] | undefined>
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

const BUILTIN_RESOURCES = new Set(['memory', 'cpu', 'cores', 'allocations'])

type ResourceUsageEntry = {
  name: string
  label: string
  total: number
  used: number
  kind: 'amount' | 'items'
}

const formatResourceLabel = (name: string): string => {
  if (name === 'memory') return 'Memory'
  if (name === 'cpu') return 'CPU'
  if (name === 'cores') return 'Cores'
  return name.replace(/[_-]+/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())
}

const toNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const parsed = Number.parseFloat(value)
    return Number.isFinite(parsed) ? parsed : null
  }
  return null
}

const toItems = (value: unknown): string[] | null => {
  if (typeof value === 'string') return [value]
  if (Array.isArray(value)) {
    return value.map((item) => (typeof item === 'string' ? item : JSON.stringify(item)))
  }
  return null
}

const formatAmount = (name: string, value: number): string => {
  const formatted = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 }).format(value)
  if (name === 'memory') {
    const gib = value / 1024
    return `${new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(gib)} GiB`
  }
  if (name === 'cpu') return `${formatted} MHz`
  if (name === 'cores') return `${formatted} cores`
  return formatted
}

const formatUsage = (entry: ResourceUsageEntry): string => {
  if (entry.kind === 'items') {
    return `${entry.used} / ${entry.total} items`
  }
  return `${formatAmount(entry.name, entry.used)} / ${formatAmount(entry.name, entry.total)}`
}

const resourcePercent = (entry: ResourceUsageEntry): number => {
  if (entry.total <= 0) return 0
  return Math.min(100, Math.round((entry.used / entry.total) * 100))
}

const accumulateUsage = (
  resources: unknown,
  usage: {
    amounts: Map<string, number>
    items: Map<string, Set<string>>
  },
) => {
  if (!isPlainObject(resources)) return

  for (const [name, value] of Object.entries(resources)) {
    const amount = toNumber(value)
    if (amount !== null) {
      usage.amounts.set(name, (usage.amounts.get(name) ?? 0) + amount)
      continue
    }

    const items = toItems(value)
    if (items) {
      const set = usage.items.get(name) ?? new Set<string>()
      items.forEach((item) => set.add(item))
      usage.items.set(name, set)
    }
  }
}

const buildResourceUsage = (
  configData: unknown,
  fingerprintData: unknown,
  allocations: readonly LedgerAllocation[],
): ResourceUsageEntry[] => {
  const totals = new Map<string, { kind: 'amount' | 'items'; total: number }>()

  const memoryTotalRaw = extractField(fingerprintData, 'memory_total')
  const memoryTotalKiB = toNumber(memoryTotalRaw)
  if (memoryTotalKiB !== null && Number.isFinite(memoryTotalKiB)) {
    totals.set('memory', { kind: 'amount', total: memoryTotalKiB / 1024 })
  }

  const cpuData = extractField(fingerprintData, 'cpus')
  if (isPlainObject(cpuData)) {
    let cpuTotal = 0
    let coresTotal = 0
    for (const value of Object.values(cpuData)) {
      if (isPlainObject(value)) {
        const frequency = toNumber(value.frequency)
        if (frequency !== null) {
          cpuTotal += frequency
        }
      }
      coresTotal += 1
    }

    if (cpuTotal > 0) {
      totals.set('cpu', { kind: 'amount', total: cpuTotal })
    }
    if (coresTotal > 0) {
      totals.set('cores', { kind: 'amount', total: coresTotal })
    }
  }

  const configResources = extractField(configData, 'resources')
  if (isPlainObject(configResources)) {
    for (const [name, value] of Object.entries(configResources)) {
      if (BUILTIN_RESOURCES.has(name)) continue
      const amount = toNumber(value)
      if (amount !== null) {
        totals.set(name, { kind: 'amount', total: amount })
        continue
      }
      const items = toItems(value)
      if (items) {
        totals.set(name, { kind: 'items', total: items.length })
      }
    }
  }

  const usage = {
    amounts: new Map<string, number>(),
    items: new Map<string, Set<string>>(),
  }

  const reservedResources =
    extractField(configData, 'reservedResources') ?? extractField(configData, 'reserved_resources')
  accumulateUsage(reservedResources, usage)

  allocations.forEach((allocation) => {
    const allocResources = parseAttributes(allocation.resources)
    accumulateUsage(allocResources, usage)
  })

  const entries: ResourceUsageEntry[] = []
  for (const [name, details] of totals) {
    if (!Number.isFinite(details.total) || details.total <= 0) continue
    const used =
      details.kind === 'items'
        ? usage.items.get(name)?.size ?? 0
        : usage.amounts.get(name) ?? 0
    entries.push({
      name,
      label: formatResourceLabel(name),
      total: details.total,
      used,
      kind: details.kind,
    })
  }

  const priority = (name: string) => {
    if (name === 'memory') return 0
    if (name === 'cpu') return 1
    if (name === 'cores') return 2
    return 3
  }

  return entries.sort((a, b) => {
    const diff = priority(a.name) - priority(b.name)
    if (diff !== 0) return diff
    return a.label.localeCompare(b.label)
  })
}

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
  open?: boolean
  onToggle?: (open: boolean) => void
}

const CollapsibleSection = (props: CollapsibleSectionProps) => {
  const [localOpen, setLocalOpen] = createSignal(props.defaultOpen ?? true)
  const isControlled = () => props.open !== undefined
  const isOpen = () => props.open ?? localOpen()

  const toggle = () => {
    const next = !isOpen()
    if (props.onToggle) props.onToggle(next)
    if (!isControlled()) setLocalOpen(next)
  }

  return (
    <div class="rounded border border-neutral-200 bg-white/90 px-3 py-2 shadow-sm dark:border-neutral-800 dark:bg-neutral-900/50">
      <button
        type="button"
        class="flex w-full items-center justify-between text-left"
        onClick={toggle}
        aria-expanded={isOpen()}
      >
        <span class="flex items-center gap-2 text-sm font-semibold text-neutral-900 dark:text-neutral-100">
          <span
            class={`inline-block text-lg leading-none transition-transform ${isOpen() ? 'rotate-90' : 'rotate-0'}`}
            aria-hidden="true"
          >
            ›
          </span>
          {props.title}
        </span>
        <span class="text-xs uppercase tracking-[0.1em] text-neutral-500 dark:text-neutral-400">
          {isOpen() ? 'Hide' : 'Show'}
        </span>
      </button>
      <Show when={isOpen()}>
        <div class="mt-2">{props.children}</div>
      </Show>
    </div>
  )
}

type NodeSectionKey = 'fingerprint' | 'networks' | 'signals' | 'health'

const DEFAULT_SECTION_STATE: Record<NodeSectionKey, boolean> = {
  fingerprint: false,
  networks: false,
  signals: false,
  health: false,
}

export default function Nodes(props: NodesProps) {
  const [statusFilter, setStatusFilter] = createSignal('All')
  const [sectionOpen, setSectionOpen] = createSignal(new Map<string, Record<NodeSectionKey, boolean>>())
  const allocationsByNode = createMemo(() => {
    const map = new Map<string, readonly LedgerAllocation[]>()
    ;(props.ledger() ?? []).forEach((entry) => {
      if (entry.node?.id) {
        map.set(entry.node.id, entry.allocations)
      }
    })
    return map
  })

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

  const getSectionOpen = (nodeId: string, section: NodeSectionKey, defaultOpen: boolean): boolean => {
    const stored = sectionOpen().get(nodeId)
    if (stored) return stored[section]
    return defaultOpen
  }

  const setSectionState = (nodeId: string, section: NodeSectionKey, open: boolean) => {
    setSectionOpen((prev) => {
      const next = new Map(prev)
      const existing = next.get(nodeId) ?? DEFAULT_SECTION_STATE
      next.set(nodeId, { ...existing, [section]: open })
      return next
    })
  }

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
                  const nodeId = node.id || 'unknown'
                  const configData = parseAttributes(node.configAttrs)
                  const dynamicData = parseAttributes(node.dynamicAttrs)
                  const fingerprintData = parseAttributes(node.fingerprintAttrs)
                  const networksData = extractField(dynamicData, 'networks') ?? extractField(fingerprintData, 'networks')
                  const signalsData = extractField(dynamicData, 'signals') ?? extractField(fingerprintData, 'signals')
                  const healthData = parseAttributes(node.healthChecks)
                  const formattedHealth = prettifyData(healthData ?? node.healthChecks)
                  const healthSummary = summarizeHealthChecks(node.healthChecks)
                  const allocations = allocationsByNode().get(node.id) ?? []
                  const resourceUsage = buildResourceUsage(configData, fingerprintData, allocations)

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
                        <div class="rounded border border-neutral-200 bg-neutral-50 p-3 shadow-sm dark:border-neutral-800 dark:bg-neutral-900/60">
                          <div class="flex items-center justify-between">
                            <h4 class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Allocations</h4>
                            <span class="text-[11px] uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">
                              {allocations.length} total
                            </span>
                          </div>
                          <Show
                            when={allocations.length > 0}
                            fallback={<p class="mt-2 text-sm text-neutral-500">No allocations assigned.</p>}
                          >
                            <div class="mt-3 space-y-2">
                              <For each={allocations}>
                                {(allocation) => (
                                  <div class="rounded border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-800 shadow-sm dark:border-neutral-800 dark:bg-neutral-950/60 dark:text-neutral-200">
                                    <div class="flex flex-wrap items-center justify-between gap-2">
                                      <span class="font-semibold">Allocation #{allocation.id}</span>
                                      <div class="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-wide">
                                        <span class={`rounded border px-2 py-1 ${healthToneClasses(healthSummary.tone)}`}>
                                          {healthSummary.label}
                                        </span>
                                        <span class="rounded border border-neutral-300 px-2 py-1 dark:border-neutral-700">
                                          {allocation.status || 'Unknown'}
                                        </span>
                                      </div>
                                    </div>
                                    <div class="mt-2 flex flex-wrap gap-2 text-xs text-neutral-600 dark:text-neutral-400">
                                      <span class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                        Job: {allocation.jobId || '—'}
                                      </span>
                                      <span class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                        Job Config: {allocation.jobConfigId || '—'}
                                      </span>
                                      <span class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                        Task Config: {allocation.taskConfigId || '—'}
                                      </span>
                                    </div>
                                  </div>
                                )}
                              </For>
                            </div>
                          </Show>
                        </div>
                        <div class="rounded border border-neutral-200 bg-neutral-50 p-3 shadow-sm dark:border-neutral-800 dark:bg-neutral-900/60">
                          <div class="flex items-center justify-between">
                            <h4 class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Resource Usage</h4>
                            <span class="text-[11px] uppercase tracking-[0.12em] text-neutral-500 dark:text-neutral-400">Used</span>
                          </div>
                          <Show
                            when={resourceUsage.length > 0}
                            fallback={<p class="mt-2 text-sm text-neutral-500">No resource data available.</p>}
                          >
                            <div class="mt-3 space-y-3">
                              <For each={resourceUsage}>
                                {(entry) => (
                                  <div class="space-y-2">
                                    <div class="flex items-center justify-between text-sm">
                                      <div>
                                        <p class="font-medium text-neutral-800 dark:text-neutral-100">{entry.label}</p>
                                        <p class="text-xs text-neutral-500 dark:text-neutral-400">{formatUsage(entry)}</p>
                                      </div>
                                      <span class="text-xs font-semibold text-neutral-700 dark:text-neutral-200">
                                        {resourcePercent(entry)}%
                                      </span>
                                    </div>
                                    <div class="h-2 w-full overflow-hidden rounded bg-neutral-200 dark:bg-neutral-800">
                                      <div
                                        class="h-full rounded bg-emerald-500 dark:bg-emerald-400"
                                        style={{ width: `${resourcePercent(entry)}%` }}
                                      />
                                    </div>
                                  </div>
                                )}
                              </For>
                            </div>
                          </Show>
                        </div>
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

                        <CollapsibleSection
                          title="Fingerprint"
                          defaultOpen={false}
                          open={getSectionOpen(nodeId, 'fingerprint', DEFAULT_SECTION_STATE.fingerprint)}
                          onToggle={(open) => setSectionState(nodeId, 'fingerprint', open)}
                        >
                          <KeyValueGrid
                            title="Fingerprint"
                            showTitle={false}
                            data={fingerprintData}
                            emptyLabel="No fingerprint data"
                          />
                        </CollapsibleSection>

                        <CollapsibleSection
                          title="Networks"
                          defaultOpen={false}
                          open={getSectionOpen(nodeId, 'networks', DEFAULT_SECTION_STATE.networks)}
                          onToggle={(open) => setSectionState(nodeId, 'networks', open)}
                        >
                          <KeyValueGrid
                            title="Networks"
                            showTitle={false}
                            data={networksData}
                            emptyLabel="No network data"
                          />
                        </CollapsibleSection>

                        <CollapsibleSection
                          title="Signals"
                          defaultOpen={false}
                          open={getSectionOpen(nodeId, 'signals', DEFAULT_SECTION_STATE.signals)}
                          onToggle={(open) => setSectionState(nodeId, 'signals', open)}
                        >
                          <KeyValueGrid
                            title="Signals"
                            showTitle={false}
                            data={signalsData}
                            emptyLabel="No signal data"
                          />
                        </CollapsibleSection>

                        <CollapsibleSection
                          title="Health Checks"
                          defaultOpen={false}
                          open={getSectionOpen(nodeId, 'health', DEFAULT_SECTION_STATE.health)}
                          onToggle={(open) => setSectionState(nodeId, 'health', open)}
                        >
                          <div class="rounded border border-neutral-200 bg-neutral-50 p-3 shadow-sm dark:border-neutral-800 dark:bg-neutral-900/60">
                            <div class="flex items-center justify-between">
                              <h4 class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Health Checks</h4>
                              <span class={`rounded border px-2 py-1 text-[11px] uppercase tracking-wide ${healthToneClasses(healthSummary.tone)}`}>
                                {healthSummary.label}
                              </span>
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
