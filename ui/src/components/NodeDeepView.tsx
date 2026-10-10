import { A } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, type JSX, type Resource } from 'solid-js'
import { Badge, CodeBlock, Icon, MonoBadge, Note, PageHeader, RelativeTime, SectionCard, Spinner, StatusBadge, TableShell, cx, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'
import NodeAvailabilityControls, { type AvailabilityOutcome } from './NodeAvailabilityControls'
import { NodeUsageCard } from './NodeUsagePanel'
import { PressureHistoryCard } from './PressureHistory'
import { AllocationPorts } from './ServiceEndpoints'
import type { LedgerAllocation, LedgerNode, NodeMsg } from '../gen/legion_pb'
import { parseAllocNetwork } from '../utils/allocations'
import { formatBytes, heartbeatFreshnessHint, pluralize } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'
import { parseArtifactStates } from '../utils/jobspec'
import { managerClient } from '../grpcClient'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../utils/polling'
import { fetchNodeUsage } from '../utils/usage'
import type { Tone } from '../utils/status'
import { shortVolumeId, volumePinsPlacement, volumeStateTone } from '../utils/volumes'

export interface NodeDeepViewProps {
  nodes: Resource<readonly NodeMsg[] | undefined>
  ledger: Resource<readonly LedgerNode[] | undefined>
  // Called after an operator action (cordon/drain/uncordon) so the owner can
  // refetch right away instead of waiting for the next poll.
  onChanged?: () => void
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

  // Allocation resources are declared in MiB, so the node total has to be MiB
  // too for the bar to mean anything. The fingerprint reports bytes.
  const memoryTotalBytes = toNumber(extractField(fingerprintData, 'memory_total'))
  if (memoryTotalBytes !== null && Number.isFinite(memoryTotalBytes)) {
    totals.set('memory', { kind: 'amount', total: memoryTotalBytes / (1024 * 1024) })
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

// --- Host facts -----------------------------------------------------------
// The fingerprint is a flat bag of ~15 keys plus three deep nested objects
// (cpus, disks, networks) and a 30-entry signals array. Dumping it verbatim
// buries the handful of facts an operator actually reads, so pull those out
// and leave the rest behind a disclosure.

type HostFact = { label: string; value: string }

const readFingerprintNumber = (data: unknown, key: string): number | undefined =>
  toNumber(extractField(data, key)) ?? undefined

// Interface name -> MAC, for the networks table.
const buildNetworkRows = (networks: unknown): Array<{ name: string; mac: string }> => {
  if (!isPlainObject(networks)) return []
  return Object.entries(networks)
    .map(([name, value]) => ({
      name,
      mac: isPlainObject(value) ? String(value.mac_address ?? value.macAddress ?? '') : '',
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

const buildSignalList = (signals: unknown): string[] => {
  if (Array.isArray(signals)) return signals.map((signal) => String(signal))
  if (isPlainObject(signals)) return Object.keys(signals)
  return []
}

const buildHostFacts = (fingerprintData: unknown, configData: unknown): HostFact[] => {
  const facts: HostFact[] = []
  const os = extractField(fingerprintData, 'os')
  const arch = extractField(fingerprintData, 'arch')
  if (typeof os === 'string' || typeof arch === 'string') {
    facts.push({ label: 'Platform', value: [os, arch].filter(Boolean).join(' · ') })
  }

  const cpus = extractField(fingerprintData, 'cpus')
  if (isPlainObject(cpus)) {
    const vendor = Object.values(cpus).find(isPlainObject)?.vendor_id
    const cores = Object.keys(cpus).length
    facts.push({ label: 'CPU', value: `${cores} core${cores === 1 ? '' : 's'}${vendor ? ` · ${String(vendor)}` : ''}` })
  }

  // sysinfo reports these in bytes.
  const memoryTotal = readFingerprintNumber(fingerprintData, 'memory_total')
  const memoryUsed = readFingerprintNumber(fingerprintData, 'memory_used')
  if (memoryTotal !== undefined) {
    const used = memoryUsed === undefined ? '' : ` · ${formatBytes(memoryUsed)} used`
    facts.push({ label: 'Memory', value: `${formatBytes(memoryTotal)}${used}` })
  }

  const swapTotal = readFingerprintNumber(fingerprintData, 'memory_swap_total')
  const swapFree = readFingerprintNumber(fingerprintData, 'memory_swap_free')
  if (swapTotal !== undefined && swapTotal > 0) {
    const free = swapFree === undefined ? '' : ` · ${formatBytes(swapFree)} free`
    facts.push({ label: 'Swap', value: `${formatBytes(swapTotal)}${free}` })
  }

  const diskTotal = readFingerprintNumber(fingerprintData, 'disk_total_space')
  const diskFree = readFingerprintNumber(fingerprintData, 'disk_available_space')
  if (diskTotal !== undefined) {
    const free = diskFree === undefined ? '' : ` · ${formatBytes(diskFree)} free`
    facts.push({ label: 'Disk', value: `${formatBytes(diskTotal)}${free}` })
  }

  const workerAddress = extractField(configData, 'workerAddress')
  if (typeof workerAddress === 'string' && workerAddress) {
    facts.push({ label: 'Worker address', value: workerAddress })
  }

  return facts
}

// Scheduling attributes only: the resource maps have their own card, and the
// nested fingerprint blobs have their own disclosures.
const OMITTED_ATTRIBUTE_KEYS = new Set([
  'resources',
  'reservedResources',
  'reserved_resources',
  'workerAddress',
])

const buildAttributeRows = (data: unknown): Array<{ key: string; value: string }> => {
  if (!isPlainObject(data)) return []
  return Object.entries(data)
    .filter(([key, value]) => {
      if (OMITTED_ATTRIBUTE_KEYS.has(key)) return false
      if (isPlainObject(value) && Object.keys(value).length === 0) return false
      return true
    })
    .map(([key, value]) => ({
      key,
      value: typeof value === 'string' ? value : JSON.stringify(value),
    }))
    .sort((a, b) => a.key.localeCompare(b.key))
}

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

// Attributes are flat key/value pairs the scheduler matches on, so they read
// best as aligned rows rather than the boxed grid used for arbitrary blobs.
const AttributeList = (props: { rows: Array<{ key: string; value: string }>; emptyLabel: string }) => (
  <Show when={props.rows.length > 0} fallback={<p class="text-sm text-muted">{props.emptyLabel}</p>}>
    <dl class="divide-y divide-line rounded-md border border-line">
      <For each={props.rows}>
        {(row) => (
          <div class="flex items-baseline justify-between gap-3 px-2.5 py-1.5">
            <dt class="min-w-0 truncate font-mono text-xs text-muted" title={row.key}>{row.key}</dt>
            <dd class="min-w-0 truncate text-right font-mono text-xs text-ink" title={row.value}>{row.value}</dd>
          </div>
        )}
      </For>
    </dl>
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
  const [outcome, setOutcome] = createSignal<AvailabilityOutcome | null>(null)
  const volumeTick = usePoll()
  // Volumes living on this node. Losing the node means losing (or at least
  // stranding) these, so they belong on the node page, not just the job's.
  const [nodeVolumes] = createResource(
    () => [props.nodes.latest?.[0]?.id, volumeTick()] as const,
    retainOnError(
      async ([nodeId]: readonly [string | undefined, number]) => {
        if (!nodeId) return []
        const reply = await managerClient.listVolumes({ nodeId })
        return reply.volumes.filter((volume) => volume.state !== 'destroyed')
      },
      ([nodeId]) => nodeId ?? '',
    ),
  )
  // Measured usage and the memory pressure response (GetNodeUsage). `null`
  // means the manager has no such RPC or the caller cannot read the node, and
  // the page falls back to the reservation-only card below.
  const [nodeUsage] = createResource(
    () => [props.nodes.latest?.[0]?.id, volumeTick()] as const,
    retainOnError(
      async ([nodeId]: readonly [string | undefined, number]) => (nodeId ? fetchNodeUsage(nodeId) : null),
      ([nodeId]) => nodeId ?? '',
    ),
  )
  // What this node's pressure response did lately. Needs operator:read, so a
  // denial just means no card.
  const eventTick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [pressureEvents] = createResource(
    () => [props.nodes.latest?.[0]?.id, eventTick()] as const,
    async ([nodeId]) =>
      nodeId
        ? managerClient
            .listEvents({ kind: 'node.pressure', nodeId, limit: 20 })
            .then((reply) => reply.events)
            .catch(() => [])
        : [],
  )
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
    const entries = buildResourceUsage(data.configData, data.fingerprintData, allocations())
    // With GetNodeUsage answering, memory and CPU live in the Usage card
    // (reservations against the scheduler's real capacity, plus measured
    // use). Only custom resources, which usage does not measure, stay here.
    return nodeUsage() ? entries.filter((entry) => !BUILTIN_RESOURCES.has(entry.name)) : entries
  })

  const healthEntries = createMemo(() => {
    const parsed = parsedNodeData()
    if (!parsed) return [] as NodeHealthEntry[]
    return buildHealthEntries(parseAttributes(node()?.healthChecks))
  })

  const hostFacts = createMemo(() => {
    const parsed = parsedNodeData()
    if (!parsed) return [] as HostFact[]
    return buildHostFacts(parsed.fingerprintData, parsed.configData)
  })

  const staticAttributes = createMemo(() => buildAttributeRows(parsedNodeData()?.configData))
  const dynamicAttributes = createMemo(() => buildAttributeRows(parsedNodeData()?.dynamicData))
  const networkRows = createMemo(() => buildNetworkRows(parsedNodeData()?.networksData))
  const signalList = createMemo(() => buildSignalList(parsedNodeData()?.signalsData))
  const rawFingerprint = createMemo(() => {
    const data = parsedNodeData()?.fingerprintData
    if (data === undefined) return '{}'
    return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
  })

  return (
    <Show when={!props.nodes.error} fallback={<Note tone="danger">Failed to load worker: {String(props.nodes.error?.message ?? props.nodes.error)}</Note>}>
      <Show when={props.nodes() !== undefined} fallback={<div class="py-6"><Spinner label="Loading worker…" /></div>}>
        <Show when={node()} fallback={<Note tone="warn">Worker not found.</Note>}>
          {(currentNode) => {
            // Accessors, not snapshots: the page polls, and a value captured
            // here once would show the node as it was on first render.
            const current = () => currentNode()
            const parsed = () => parsedNodeData()!
            return (
              <div>
                <PageHeader
                  eyebrow="Worker"
                  title={current().id || 'Unknown worker'}
                  badges={
                    <>
                      <StatusBadge status={current().status || 'unknown'} />
                      <Badge tone={parsed().healthSummary.tone} dot class="uppercase tracking-wide">{parsed().healthSummary.label}</Badge>
                      <Show when={current().availability && current().availability !== 'available'}>
                        <Badge
                          tone={current().availability === 'drained' ? 'warn' : 'danger'}
                          dot
                          class="uppercase tracking-wide"
                          title="Availability: the scheduler only places new work on available nodes"
                        >
                          {current().availability}
                        </Badge>
                      </Show>
                      <Badge tone="neutral">{pluralize(allocations().length, 'allocation')}</Badge>
                    </>
                  }
                  subtitle={
                    <>
                      Pool {current().pool || 'default'} · availability {current().availability || '—'} · heartbeat{' '}
                      <RelativeTime value={current().lastHeartbeat} title={heartbeatFreshnessHint(current().heartbeatSource)} />
                    </>
                  }
                  actions={
                    <NodeAvailabilityControls
                      node={current()}
                      onOutcome={(result) => {
                        setOutcome(result)
                        props.onChanged?.()
                      }}
                    />
                  }
                />

                <div class="space-y-4">
                  <Show when={outcome()}>
                    {(result) => (
                      <div aria-live="polite">
                        <Note tone={result().failed ? 'danger' : 'ok'} onDismiss={() => setOutcome(null)}>{result().message}</Note>
                      </div>
                    )}
                  </Show>
                  <Show when={current().availability === 'drained'}>
                    <Note tone="warn">
                      <span class="font-medium">This node is out of the scheduling pool.</span> Nothing new is placed
                      here until it is uncordoned. Allocations that were evicted by a drain reschedule elsewhere;
                      stateful ones stay under their <code class="font-mono text-xs">stateful.onLost</code> policy.
                    </Note>
                  </Show>
                  <SectionCard title="Allocations" flush>
                    <Show when={allocations().length > 0} fallback={<p class="px-4 py-4 text-sm text-muted">No allocations assigned.</p>}>
                      <TableShell>
                        <table class={tableClass}>
                          <thead class={theadClass}>
                            <tr>
                              <th class={thClass}>ID</th>
                              <th class={thClass}>Instance</th>
                              <th class={thClass}>Status</th>
                              <th class={thClass}>Ports</th>
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
                                      <AllocationPorts network={parseAllocNetwork(allocation.network)} />
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

                  <Show when={nodeUsage()}>{(usage) => <NodeUsageCard usage={usage()} />}</Show>

                  <Show when={(pressureEvents() ?? []).length > 0}>
                    <PressureHistoryCard events={pressureEvents() ?? []} scope="node" />
                  </Show>

                  <Show when={!nodeUsage() || resourceUsage().length > 0}>
                    <SectionCard
                      title={nodeUsage() ? 'Other resources' : 'Resource usage'}
                      description={nodeUsage() ? 'Custom resources declared by the worker, against what allocations reserve.' : 'Reservations against fingerprinted capacity. This manager does not report measured usage.'}
                    >
                      <Show when={resourceUsage().length > 0} fallback={<p class="text-sm text-muted">No resource data available.</p>}>
                        <div class="grid gap-x-8 gap-y-4 md:grid-cols-2 xl:grid-cols-3">
                          <For each={resourceUsage()}>
                            {(entry) => (
                              <div>
                                <div class="flex items-baseline justify-between text-sm">
                                  <p class="font-medium text-ink">{entry.label}</p>
                                  <span class="font-mono text-xs tabular-nums text-muted">
                                    {formatUsage(entry)} · {resourcePercent(entry)}%
                                  </span>
                                </div>
                                <div class="mt-1.5 h-1.5 w-full overflow-hidden rounded-full bg-line/70">
                                  <div
                                    class={cx('h-full rounded-full transition-all', resourceBarClass(resourcePercent(entry)), entry.used > 0 && 'min-w-[3px]')}
                                    style={{ width: `${resourcePercent(entry)}%` }}
                                  />
                                </div>
                              </div>
                            )}
                          </For>
                        </div>
                      </Show>
                    </SectionCard>
                  </Show>

                  <SectionCard title="Health checks" flush>
                    <div class="p-3.5">
                      <div class="mb-3 flex items-center justify-between">
                        <p class="text-xs text-muted">Overall status</p>
                        <Badge tone={parsed().healthSummary.tone} dot class="uppercase tracking-wide">{parsed().healthSummary.label}</Badge>
                      </div>
                      <Show
                        when={healthEntries().length > 0}
                        fallback={<p class="text-sm text-muted">No health check data.</p>}
                      >
                        {/* CSS columns give a masonry layout so tall cards don't leave holes. */}
                        <div class="gap-2 md:columns-2 [&>*]:mb-2 [&>*]:break-inside-avoid">
                          <For each={healthEntries()}>
                            {(entry) => (
                              <article class="rounded-md border border-line bg-surface2/40 p-3">
                                <div class="flex items-start justify-between gap-2">
                                  <p class="text-sm font-medium text-ink">{entry.label}</p>
                                  <Badge tone={entry.tone} dot class="uppercase tracking-wide">{entry.status}</Badge>
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

                  <Show when={(nodeVolumes() ?? []).length > 0}>
                    <SectionCard
                      title="Volumes"
                      description="Storage materialized on this host. Node-local volumes pin their instance here — if this node goes away, those instances wait for it."
                      flush
                    >
                      <TableShell>
                        <table class={tableClass}>
                          <thead class={theadClass}>
                            <tr>
                              <th class={thClass}>Volume</th>
                              <th class={thClass}>Driver</th>
                              <th class={thClass}>State</th>
                              <th class={thClass}>Owner</th>
                            </tr>
                          </thead>
                          <tbody class="divide-y divide-line">
                            <For each={nodeVolumes() ?? []}>
                              {(volume) => (
                                <tr class={trHoverClass}>
                                  <td class={tdClass}>
                                    <A
                                      href={`/volumes/${encodeURIComponent(volume.volumeId)}`}
                                      class="font-mono text-xs text-ink hover:text-brass-strong"
                                      title={volume.volumeId}
                                    >
                                      {shortVolumeId(volume.volumeId)}
                                    </A>
                                    <div class="text-2xs text-faint">{volume.name}</div>
                                  </td>
                                  <td class={tdClass}>
                                    <Badge tone={volume.nodeLocal ? 'neutral' : 'ok'}>{volume.driver}</Badge>
                                  </td>
                                  <td class={tdClass}>
                                    <Badge tone={volumeStateTone(volume)} dot class="uppercase tracking-wide">
                                      {volume.state}
                                    </Badge>
                                  </td>
                                  <td class={tdClass}>
                                    <Show when={volume.jobId} fallback={<span class="text-faint">—</span>}>
                                      <A href={`/jobs/${encodeURIComponent(volume.jobId!)}`} class="text-muted hover:text-brass-strong">
                                        {volume.jobId}
                                      </A>
                                    </Show>
                                    <Show when={volume.instanceId}>
                                      <div class="font-mono text-2xs text-faint">{volume.instanceId}</div>
                                    </Show>
                                  </td>
                                </tr>
                              )}
                            </For>
                          </tbody>
                        </table>
                      </TableShell>
                      <Show when={(nodeVolumes() ?? []).some(volumePinsPlacement)}>
                        <p class="border-t border-line px-4 py-2 text-2xs text-faint">
                          Draining or losing this node will strand the node-local volumes above until it returns.
                        </p>
                      </Show>
                    </SectionCard>
                  </Show>

                  <SectionCard
                    title="Metadata"
                    description="What this host is, and the attributes the scheduler matches against."
                  >
                    <Show when={hostFacts().length > 0}>
                      <dl class="grid gap-x-6 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
                        <For each={hostFacts()}>
                          {(fact) => (
                            <div class="min-w-0">
                              <dt class="eyebrow">{fact.label}</dt>
                              <dd class="mt-0.5 truncate text-sm text-ink" title={fact.value}>{fact.value}</dd>
                            </div>
                          )}
                        </For>
                      </dl>
                    </Show>

                    <div class="mt-5 grid gap-4 lg:grid-cols-2">
                      <div>
                        <p class="eyebrow mb-2">
                          Static attributes
                          <span class="ml-2 normal-case tracking-normal text-faint">from the worker config</span>
                        </p>
                        <AttributeList
                          rows={staticAttributes()}
                          emptyLabel="None declared."
                        />
                      </div>
                      <div>
                        <p class="eyebrow mb-2">
                          Dynamic attributes
                          <span class="ml-2 normal-case tracking-normal text-faint">via legion node update</span>
                        </p>
                        <AttributeList rows={dynamicAttributes()} emptyLabel="None set." />
                      </div>
                    </div>

                    <div class="mt-4 space-y-2.5">
                      <CollapsibleSection
                        title={`Network interfaces (${networkRows().length})`}
                        defaultOpen={false}
                        open={sectionOpen().networks}
                        onToggle={(open) => setSectionState('networks', open)}
                      >
                        <Show when={networkRows().length > 0} fallback={<p class="text-sm text-muted">No network data.</p>}>
                          <TableShell>
                            <table class={tableClass}>
                              <thead class={theadClass}>
                                <tr>
                                  <th class={thClass}>Interface</th>
                                  <th class={thClass}>MAC address</th>
                                </tr>
                              </thead>
                              <tbody class="divide-y divide-line">
                                <For each={networkRows()}>
                                  {(row) => (
                                    <tr class={trHoverClass}>
                                      <td class={cx(tdClass, 'font-mono text-xs')}>{row.name}</td>
                                      <td class={cx(tdClass, 'font-mono text-xs text-muted')}>{row.mac || '—'}</td>
                                    </tr>
                                  )}
                                </For>
                              </tbody>
                            </table>
                          </TableShell>
                        </Show>
                      </CollapsibleSection>

                      <CollapsibleSection
                        title={`Supported signals (${signalList().length})`}
                        defaultOpen={false}
                        open={sectionOpen().signals}
                        onToggle={(open) => setSectionState('signals', open)}
                      >
                        <Show when={signalList().length > 0} fallback={<p class="text-sm text-muted">No signal data.</p>}>
                          <div class="flex flex-wrap gap-1.5">
                            <For each={signalList()}>{(signal) => <Badge tone="neutral" mono>{signal}</Badge>}</For>
                          </div>
                        </Show>
                      </CollapsibleSection>

                      {/* Everything above is a curated view; this is the escape
                          hatch for whatever the fingerprint reports that we do
                          not model yet. */}
                      <CollapsibleSection
                        title="Raw fingerprint"
                        defaultOpen={false}
                        open={sectionOpen().fingerprint}
                        onToggle={(open) => setSectionState('fingerprint', open)}
                      >
                        <CodeBlock code={rawFingerprint()} maxHClass="max-h-96" />
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
