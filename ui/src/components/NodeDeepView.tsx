import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal, type JSX, type Resource } from 'solid-js'
import { Badge, Icon, MonoBadge, Note, PageHeader, SectionCard, Spinner, StatusBadge, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'
import type { LedgerAllocation, LedgerNode, NodeMsg } from '../gen/legion_pb'
import { formatRelativeTime } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'
import { parseArtifactStates } from '../utils/jobspec'
import type { Tone } from '../utils/status'

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

type NodeHealthEntry = {
  key: string
  label: string
  status: string
  tone: Tone
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

const resourceBarClass = (percent: number): string => {
  if (percent >= 90) return 'bg-red-500'
  if (percent >= 70) return 'bg-amber-500'
  return 'bg-emerald-500'
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

const inferHealthTone = (value: unknown): Tone => {
  const bucket: string[] = []
  collectHealthStrings(value, bucket)
  const normalized = bucket.map((entry) => entry.trim().toLowerCase())
  if (normalized.some((entry) => HEALTH_FAIL_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'danger'
  }
  if (normalized.some((entry) => HEALTH_PASS_KEYWORDS.some((keyword) => entry.includes(keyword)))) {
    return 'ok'
  }
  return 'neutral'
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
  if (value === null || value === undefined) return <span class="text-faint">—</span>
  if (Array.isArray(value)) {
    return (
      <ul class="list-disc space-y-1 pl-4 text-sm">
        <For each={value}>{(item) => <li>{renderValue(item)}</li>}</For>
      </ul>
    )
  }

  if (isPlainObject(value)) {
    const entries = Object.entries(value)
    if (!entries.length) return <span class="text-faint">—</span>
    return (
      <div class="space-y-1.5">
        <For each={entries}>
          {([key, val]) => (
            <div class="rounded-md border border-line bg-surface px-2 py-1">
              <div class="text-2xs uppercase tracking-wide text-faint">{key}</div>
              <div class="break-words text-sm">{renderValue(val)}</div>
            </div>
          )}
        </For>
      </div>
    )
  }

  return <span class="break-words font-mono text-xs">{String(value)}</span>
}

interface KeyValueGridProps {
  data: unknown
  emptyLabel: string
}

const KeyValueGrid = (props: KeyValueGridProps) => (
  <Show
    when={(() => {
      if (props.data === undefined || props.data === null) return false
      if (typeof props.data === 'string' && props.data.trim() === '') return false
      if (isPlainObject(props.data)) return Object.keys(props.data).length > 0
      return true
    })()}
    fallback={<p class="text-sm text-muted">{props.emptyLabel}</p>}
  >
    {isPlainObject(props.data) ? (
      <div class="grid gap-2 sm:grid-cols-2">
        <For each={Object.entries(props.data)}>
          {([key, value]) => (
            <div class="min-w-0 rounded-md border border-line bg-surface2/40 px-2.5 py-1.5 text-sm">
              <div class="text-2xs uppercase tracking-wide text-faint">{key}</div>
              <div class="mt-0.5 break-words">{renderValue(value)}</div>
            </div>
          )}
        </For>
      </div>
    ) : (
      <div class="break-words text-sm">{renderValue(props.data)}</div>
    )}
  </Show>
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
    <div class="overflow-hidden rounded-lg border border-line bg-surface">
      <button
        type="button"
        class="flex w-full items-center justify-between px-3.5 py-2.5 text-left transition-colors hover:bg-surface2/60"
        onClick={toggle}
        aria-expanded={isOpen()}
      >
        <span class="flex items-center gap-2 text-sm font-medium text-ink">
          <Icon name="chevronRight" size={14} class={cx('text-faint transition-transform', isOpen() && 'rotate-90')} />
          {props.title}
        </span>
        <span class="text-2xs uppercase tracking-wide text-faint">{isOpen() ? 'Hide' : 'Show'}</span>
      </button>
      <Show when={isOpen()}>
        <div class="border-t border-line p-3.5">{props.children}</div>
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
    const healthSummary = summarizeHealthChecks(current.healthChecks)
    return {
      configData,
      dynamicData,
      fingerprintData,
      networksData,
      signalsData,
      healthSummary,
    }
  })

  const resourceUsage = createMemo(() => {
    const data = parsedNodeData()
    if (!data) return [] as ResourceUsageEntry[]
    return buildResourceUsage(data.configData, data.fingerprintData, allocations())
  })

  const healthEntries = createMemo(() => {
    const parsed = parsedNodeData()
    if (!parsed) return [] as NodeHealthEntry[]
    return buildHealthEntries(parseAttributes(node()?.healthChecks))
  })

  return (
    <Show when={!props.nodes.loading} fallback={<div class="py-6"><Spinner label="Loading worker…" /></div>}>
      <Show when={!props.nodes.error} fallback={<Note tone="danger">Failed to load worker.</Note>}>
        <Show when={node()} fallback={<Note tone="warn">Worker not found.</Note>}>
          {(currentNode) => {
            const current = currentNode()
            const parsed = parsedNodeData()
            if (!parsed) return null
            return (
              <div>
                <PageHeader
                  eyebrow="Worker"
                  title={current.id || 'Unknown worker'}
                  badges={
                    <>
                      <StatusBadge status={current.status || 'unknown'} />
                      <Badge tone={parsed.healthSummary.tone} dot>{parsed.healthSummary.label}</Badge>
                      <Badge tone="neutral">{allocations().length} allocation{allocations().length === 1 ? '' : 's'}</Badge>
                    </>
                  }
                  subtitle={
                    <>
                      Pool {current.pool || 'default'} · availability {current.availability || '—'} · heartbeat {formatRelativeTime(current.lastHeartbeat)}
                    </>
                  }
                />

                <div class="space-y-4">
                  <SectionCard title="Allocations" flush>
                    <Show when={allocations().length > 0} fallback={<p class="px-4 py-4 text-sm text-muted">No allocations assigned.</p>}>
                      <TableShell>
                        <table class={tableClass}>
                          <thead class={theadClass}>
                            <tr>
                              <th class={thClass}>ID</th>
                              <th class={thClass}>Instance</th>
                              <th class={thClass}>Status</th>
                              <th class={thClass}>Job</th>
                              <th class={thClass}>Spec</th>
                              <th class={thClass}>Task config</th>
                            </tr>
                          </thead>
                          <tbody class="divide-y divide-line">
                            <For each={allocations()}>
                              {(allocation) => {
                                const artifactRows = parseArtifactStates(allocation.artifactStates)
                                const unstagedCount = artifactRows.filter((row) => row.state !== 'staged').length
                                return (
                                  <tr class={trHoverClass}>
                                    <td class={tdClass + ' font-mono text-xs'}>#{String(allocation.id)}</td>
                                    <td class={tdClass}>
                                      <Show when={allocation.instanceId} fallback={<span class="text-faint">—</span>}>
                                        <MonoBadge title={`Stateful instance ${allocation.instanceId}`}>{allocation.instanceId}</MonoBadge>
                                      </Show>
                                    </td>
                                    <td class={tdClass}>
                                      <span class="inline-flex flex-wrap items-center gap-1.5">
                                        <StatusBadge status={allocation.status || 'unknown'} />
                                        <Show when={unstagedCount > 0}>
                                          <Badge tone={artifactRows.some((row) => row.state === 'failed') ? 'danger' : 'warn'}>
                                            staging {unstagedCount}
                                          </Badge>
                                        </Show>
                                      </span>
                                    </td>
                                    <td class={tdClass}>
                                      <A class="font-medium hover:text-brass-strong" href={`/jobs/${encodeURIComponent(allocation.jobId || '')}`}>{allocation.jobId || '—'}</A>
                                    </td>
                                    <td class={tdClass}>
                                      <A class="font-mono text-xs text-muted hover:text-brass-strong" href={`/jobs/${encodeURIComponent(allocation.jobId || '')}/specs/${encodeURIComponent(allocation.jobConfigId || '')}`}>
                                        {allocation.jobConfigId || '—'}
                                      </A>
                                    </td>
                                    <td class={tdClass + ' font-mono text-xs text-muted'}>{allocation.taskConfigId ? String(allocation.taskConfigId) : '—'}</td>
                                  </tr>
                                )
                              }}
                            </For>
                          </tbody>
                        </table>
                      </TableShell>
                    </Show>
                  </SectionCard>

                  <SectionCard title="Resource usage">
                    <Show when={resourceUsage().length > 0} fallback={<p class="text-sm text-muted">No resource data available.</p>}>
                      <div class="grid gap-x-8 gap-y-4 md:grid-cols-2">
                        <For each={resourceUsage()}>
                          {(entry) => (
                            <div>
                              <div class="flex items-baseline justify-between text-sm">
                                <p class="font-medium text-ink">{entry.label}</p>
                                <span class="font-mono text-xs tabular-nums text-muted">
                                  {formatUsage(entry)} · {resourcePercent(entry)}%
                                </span>
                              </div>
                              <div class="mt-1.5 h-1.5 w-full overflow-hidden rounded-full bg-surface2">
                                <div
                                  class={cx('h-full rounded-full transition-all', resourceBarClass(resourcePercent(entry)))}
                                  style={{ width: `${resourcePercent(entry)}%` }}
                                />
                              </div>
                            </div>
                          )}
                        </For>
                      </div>
                    </Show>
                  </SectionCard>

                  <SectionCard title="Health checks" flush>
                    <div class="p-3.5">
                      <div class="mb-3 flex items-center justify-between">
                        <p class="text-xs text-faint">Overall status</p>
                        <Badge tone={parsed.healthSummary.tone} dot>{parsed.healthSummary.label}</Badge>
                      </div>
                      <Show
                        when={healthEntries().length > 0}
                        fallback={<p class="text-sm text-muted">No health check data.</p>}
                      >
                        <div class="grid gap-2 md:grid-cols-2">
                          <For each={healthEntries()}>
                            {(entry) => (
                              <article class="rounded-md border border-line bg-surface2/40 p-3">
                                <div class="flex items-start justify-between gap-2">
                                  <p class="text-sm font-medium text-ink">{entry.label}</p>
                                  <Badge tone={entry.tone} dot>{entry.status}</Badge>
                                </div>
                                <Show when={entry.details}>
                                  <div class="mt-2 text-xs text-muted">
                                    {renderValue(entry.details)}
                                  </div>
                                </Show>
                              </article>
                            )}
                          </For>
                        </div>
                      </Show>
                    </div>
                  </SectionCard>

                  <SectionCard title="Metadata">
                    <div class="grid gap-4 lg:grid-cols-2">
                      <div>
                        <p class="eyebrow mb-2">Static config</p>
                        <KeyValueGrid data={parsed.configData} emptyLabel="No static config provided." />
                      </div>
                      <div>
                        <p class="eyebrow mb-2">Dynamic config</p>
                        <KeyValueGrid data={parsed.dynamicData} emptyLabel="No dynamic config provided." />
                      </div>
                    </div>

                    <div class="mt-4 space-y-2.5">
                      <CollapsibleSection
                        title="Fingerprint"
                        defaultOpen={false}
                        open={sectionOpen().fingerprint}
                        onToggle={(open) => setSectionState('fingerprint', open)}
                      >
                        <KeyValueGrid data={parsed.fingerprintData} emptyLabel="No fingerprint data." />
                      </CollapsibleSection>

                      <CollapsibleSection
                        title="Networks"
                        defaultOpen={false}
                        open={sectionOpen().networks}
                        onToggle={(open) => setSectionState('networks', open)}
                      >
                        <KeyValueGrid data={parsed.networksData} emptyLabel="No network data." />
                      </CollapsibleSection>

                      <CollapsibleSection
                        title="Signals"
                        defaultOpen={false}
                        open={sectionOpen().signals}
                        onToggle={(open) => setSectionState('signals', open)}
                      >
                        <KeyValueGrid data={parsed.signalsData} emptyLabel="No signal data." />
                      </CollapsibleSection>
                    </div>
                  </SectionCard>
                </div>
              </div>
            )
          }}
        </Show>
      </Show>
    </Show>
  )
}
