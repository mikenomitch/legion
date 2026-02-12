import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal, type JSX, type Resource } from 'solid-js'
import { DetailBadge, DetailSection } from './DetailPrimitives'
import type { LedgerAllocation, LedgerNode, NodeMsg } from '../gen/legion_pb'
import { formatRelativeTime } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'

export interface NodeDeepViewProps {
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

type NodeHealthTone = 'healthy' | 'unhealthy' | 'unknown'

type NodeHealthEntry = {
  key: string
  label: string
  status: string
  tone: NodeHealthTone
  details?: unknown
}

const HEALTH_FAIL_KEYWORDS = ['fail', 'failed', 'error', 'unhealthy', 'critical', 'down']
const HEALTH_PASS_KEYWORDS = ['pass', 'passed', 'ok', 'healthy', 'up', 'ready']

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

const titleize = (value: string): string =>
  value
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())

const collectHealthStrings = (value: unknown, bucket: string[]) => {
  if (value === null || value === undefined) return

  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    bucket.push(String(value))
    return
  }

  if (Array.isArray(value)) {
    value.forEach((item) => collectHealthStrings(item, bucket))
    return
  }

  if (isPlainObject(value)) {
    Object.values(value).forEach((item) => collectHealthStrings(item, bucket))
  }
}

const inferHealthTone = (value: unknown): NodeHealthTone => {
  const bucket: string[] = []
  collectHealthStrings(value, bucket)
  const normalized = bucket.map((entry) => entry.trim().toLowerCase())
  if (normalized.some((entry) => HEALTH_FAIL_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'unhealthy'
  }
  if (normalized.some((entry) => HEALTH_PASS_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'healthy'
  }
  return 'unknown'
}

const firstStatusValue = (value: unknown): string | undefined => {
  if (value === null || value === undefined) return undefined
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  if (!isPlainObject(value)) return undefined

  const preferredKeys = ['status', 'state', 'result', 'health', 'readiness']
  for (const key of preferredKeys) {
    if (!(key in value)) continue
    const current = (value as Record<string, unknown>)[key]
    if (typeof current === 'string' || typeof current === 'number' || typeof current === 'boolean') {
      return String(current)
    }
  }

  const values = Object.values(value)
  if (values.length === 1) {
    const current = values[0]
    if (typeof current === 'string' || typeof current === 'number' || typeof current === 'boolean') {
      return String(current)
    }
  }
  return undefined
}

const buildHealthEntries = (value: unknown): NodeHealthEntry[] => {
  if (!isPlainObject(value)) return []

  return Object.entries(value).map(([key, rawValue]) => {
    const status = firstStatusValue(rawValue) ?? 'Details available'
    const details =
      isPlainObject(rawValue) || Array.isArray(rawValue)
        ? rawValue
        : undefined

    return {
      key,
      label: titleize(key),
      status,
      tone: inferHealthTone(rawValue),
      details,
    }
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
  health: true,
}

export default function NodeDeepView(props: NodeDeepViewProps) {
  const [sectionOpen, setSectionOpen] = createSignal(DEFAULT_SECTION_STATE)
  const allocationsByNode = createMemo(() => {
    const map = new Map<string, readonly LedgerAllocation[]>()
    ;(props.ledger() ?? []).forEach((entry) => {
      if (entry.node?.id) {
        map.set(entry.node.id, entry.allocations)
      }
    })
    return map
  })

  const node = createMemo(() => {
    const list = props.nodes() ?? []
    return list[0]
  })

  const allocations = createMemo(() => {
    const nodeId = node()?.id
    if (!nodeId) return [] as readonly LedgerAllocation[]
    return allocationsByNode().get(nodeId) ?? []
  })

  const setSectionState = (section: NodeSectionKey, open: boolean) => {
    setSectionOpen((prev) => {
      return { ...prev, [section]: open }
    })
  }

  const parsedNodeData = createMemo(() => {
    const current = node()
    if (!current) return undefined
    const configData = parseAttributes(current.configAttrs)
    const dynamicData = parseAttributes(current.dynamicAttrs)
    const fingerprintData = parseAttributes(current.fingerprintAttrs)
    const networksData = extractField(dynamicData, 'networks') ?? extractField(fingerprintData, 'networks')
    const signalsData = extractField(dynamicData, 'signals') ?? extractField(fingerprintData, 'signals')
    const healthData = parseAttributes(current.healthChecks)
    const formattedHealth = prettifyData(healthData ?? current.healthChecks)
    const healthSummary = summarizeHealthChecks(current.healthChecks)
    return {
      configData,
      dynamicData,
      fingerprintData,
      networksData,
      signalsData,
      formattedHealth,
      healthSummary,
    }
  })

  const resourceUsage = createMemo(() => {
    const data = parsedNodeData()
    if (!data) return [] as ResourceUsageEntry[]
    return buildResourceUsage(data.configData, data.fingerprintData, allocations())
  })

  const badgeToneForHealth = (tone: string) => {
    if (tone === 'healthy') return 'success' as const
    if (tone === 'degraded') return 'warning' as const
    if (tone === 'unhealthy') return 'danger' as const
    return 'neutral' as const
  }

  const healthEntries = createMemo(() => {
    const parsed = parsedNodeData()
    if (!parsed) return [] as NodeHealthEntry[]
    return buildHealthEntries(parseAttributes(node()?.healthChecks))
  })

  return (
    <Show when={!props.nodes.loading} fallback={<p class="text-neutral-500">Loading workers…</p>}>
      <Show when={!props.nodes.error} fallback={<p class="text-red-500">Failed to load workers.</p>}>
        <Show when={node()} fallback={<p class="text-neutral-500">Worker not found.</p>}>
          {(currentNode) => {
            const current = currentNode()
            const parsed = parsedNodeData()
            if (!parsed) return null
            return (
              <div class="space-y-4">
                <DetailSection>
                  <div class="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <h2 class="text-2xl font-semibold">{current.id || 'Unknown worker'}</h2>
                      <p class="text-sm text-neutral-500">
                        Pool {current.pool || '—'} · Availability {current.availability || '—'}
                      </p>
                    </div>
                    <div class="flex flex-wrap items-center gap-2">
                      <DetailBadge tone={badgeToneForHealth(parsed.healthSummary.tone)}>{current.status || 'Unknown'}</DetailBadge>
                      <span class="text-xs text-neutral-500">Last heartbeat {formatRelativeTime(current.lastHeartbeat)}</span>
                    </div>
                  </div>
                  <div class="mt-3 flex flex-wrap gap-2 text-xs">
                    <span class="rounded border border-neutral-300 px-2 py-1 text-neutral-700 dark:border-neutral-700 dark:text-neutral-200">
                      Allocations {allocations().length}
                    </span>
                    <span class="rounded border border-neutral-300 px-2 py-1 text-neutral-700 dark:border-neutral-700 dark:text-neutral-200">
                      Health {parsed.healthSummary.label}
                    </span>
                  </div>
                </DetailSection>

                <DetailSection title="Allocations">
                  <Show when={allocations().length > 0} fallback={<p class="text-sm text-neutral-500">No allocations assigned.</p>}>
                    <div class="space-y-2">
                      <For each={allocations()}>
                        {(allocation) => (
                          <div class="rounded border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-800 dark:border-neutral-700 dark:bg-neutral-950/60 dark:text-neutral-200">
                            <div class="flex flex-wrap items-center justify-between gap-2">
                              <span class="font-semibold">Allocation #{String(allocation.id)}</span>
                              <DetailBadge tone="neutral">{allocation.status || 'Unknown'}</DetailBadge>
                            </div>
                            <div class="mt-2 flex flex-wrap gap-2 text-xs text-neutral-600 dark:text-neutral-400">
                              <span class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                Job <A class="underline" href={`/jobs/${encodeURIComponent(allocation.jobId || '')}`}>{allocation.jobId || '—'}</A>
                              </span>
                              <span class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                Spec{' '}
                                <A class="underline" href={`/jobs/${encodeURIComponent(allocation.jobId || '')}/specs/${encodeURIComponent(allocation.jobConfigId || '')}`}>
                                  {allocation.jobConfigId || '—'}
                                </A>
                              </span>
                              <span class="rounded border border-neutral-200 bg-neutral-50 px-2 py-1 dark:border-neutral-800 dark:bg-neutral-900">
                                Task Config {allocation.taskConfigId ? String(allocation.taskConfigId) : '—'}
                              </span>
                            </div>
                          </div>
                        )}
                      </For>
                    </div>
                  </Show>
                </DetailSection>

                <DetailSection title="Resource usage">
                  <Show when={resourceUsage().length > 0} fallback={<p class="text-sm text-neutral-500">No resource data available.</p>}>
                    <div class="space-y-3">
                      <For each={resourceUsage()}>
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
                </DetailSection>

                <DetailSection title="Worker metadata">
                  <div class="grid gap-3 lg:grid-cols-2">
                    <KeyValueGrid
                      title="Static Config"
                      data={parsed.configData}
                      emptyLabel="No static config provided"
                    />
                    <KeyValueGrid
                      title="Dynamic Config"
                      data={parsed.dynamicData}
                      emptyLabel="No dynamic config provided"
                    />
                  </div>

                  <div class="mt-3 space-y-3">
                    <CollapsibleSection
                      title="Health Checks"
                      defaultOpen={true}
                      open={sectionOpen().health}
                      onToggle={(open) => setSectionState('health', open)}
                    >
                      <div class="space-y-3 rounded border border-neutral-200 bg-neutral-50 p-3 shadow-sm dark:border-neutral-800 dark:bg-neutral-900/60">
                        <div class="flex items-center justify-between">
                          <p class="text-xs uppercase tracking-[0.1em] text-neutral-500 dark:text-neutral-400">Overall status</p>
                          <DetailBadge tone={badgeToneForHealth(parsed.healthSummary.tone)}>{parsed.healthSummary.label}</DetailBadge>
                        </div>
                        <Show
                          when={healthEntries().length > 0}
                          fallback={<p class="text-sm text-neutral-500">No health check data</p>}
                        >
                          <div class="grid gap-2 md:grid-cols-2">
                            <For each={healthEntries()}>
                              {(entry) => (
                                <article class="rounded border border-neutral-200 bg-white p-3 dark:border-neutral-700 dark:bg-neutral-950/60">
                                  <div class="flex items-start justify-between gap-2">
                                    <p class="text-sm font-semibold text-neutral-900 dark:text-neutral-100">{entry.label}</p>
                                    <DetailBadge tone={badgeToneForHealth(entry.tone)}>{entry.status}</DetailBadge>
                                  </div>
                                  <Show when={entry.details}>
                                    <div class="mt-2 text-xs text-neutral-700 dark:text-neutral-300">
                                      {renderValue(entry.details)}
                                    </div>
                                  </Show>
                                </article>
                              )}
                            </For>
                          </div>
                        </Show>
                      </div>
                    </CollapsibleSection>

                    <CollapsibleSection
                      title="Fingerprint"
                      defaultOpen={false}
                      open={sectionOpen().fingerprint}
                      onToggle={(open) => setSectionState('fingerprint', open)}
                    >
                      <KeyValueGrid
                        title="Fingerprint"
                        showTitle={false}
                        data={parsed.fingerprintData}
                        emptyLabel="No fingerprint data"
                      />
                    </CollapsibleSection>

                    <CollapsibleSection
                      title="Networks"
                      defaultOpen={false}
                      open={sectionOpen().networks}
                      onToggle={(open) => setSectionState('networks', open)}
                    >
                      <KeyValueGrid
                        title="Networks"
                        showTitle={false}
                        data={parsed.networksData}
                        emptyLabel="No network data"
                      />
                    </CollapsibleSection>

                    <CollapsibleSection
                      title="Signals"
                      defaultOpen={false}
                      open={sectionOpen().signals}
                      onToggle={(open) => setSectionState('signals', open)}
                    >
                      <KeyValueGrid
                        title="Signals"
                        showTitle={false}
                        data={parsed.signalsData}
                        emptyLabel="No signal data"
                      />
                    </CollapsibleSection>
                  </div>
                </DetailSection>
              </div>
            )
          }}
        </Show>
      </Show>
    </Show>
  )
}
