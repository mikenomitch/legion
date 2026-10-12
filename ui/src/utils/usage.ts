import { Code, ConnectError } from '@connectrpc/connect'
import type { AllocationUsage, GetNodeUsageReply, MemoryPressureStatus, NodeAllocationUsage, NodeResourceUsage, SnapshotBudget } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { formatBytes } from './format'
import type { Tone } from './status'

/*
 * Measured usage, reservations, and the memory pressure response
 * (docs/resource-enforcement.md, "Measuring usage" and "Memory pressure").
 *
 * The manager holds one usage sample per node, in memory, and serves it from
 * GetNodeUsage. Everything here is a pure function of that reply (or of the
 * `usage` an instance carries), so the same helpers drive the worker page,
 * the workers index, the job page, and the fixtures at /testing.
 */

const MIB = 1024 * 1024

export const bytesToMiB = (bytes: bigint | number): number => Number(bytes) / MIB

const mibFormat = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })

/** `1830` — MiB without a unit, for `res/max/used` columns that carry it in the header. */
export const formatMiB = (mib: number): string => mibFormat.format(Math.round(mib))

/** A resource amount with its unit: GiB above 1 GiB for memory, MHz for CPU. */
export const formatAmount = (resource: 'memory' | 'cpu', value: number): string => {
  if (resource === 'cpu') return `${mibFormat.format(Math.round(value))} MHz`
  return formatBytes(value * MIB)
}

/** `×1.5`, or undefined at the default ratio of 1. */
export const ratioLabel = (ratio: number): string | undefined => {
  if (!Number.isFinite(ratio) || ratio <= 0 || Math.abs(ratio - 1) < 1e-9) return undefined
  return `×${Number(ratio.toFixed(2))}`
}

/**
 * Capacity before the oversubscription ratio. `capacity` already has the
 * ratio folded in (it is what the scheduler packs against), so used memory
 * is compared to this instead: a node cannot use more than it physically has.
 */
export const physicalCapacity = (resource: NodeResourceUsage): number => {
  const ratio = resource.oversubscribeRatio > 0 ? resource.oversubscribeRatio : 1
  return resource.capacity / ratio
}

export const percentOf = (part: number, whole: number): number => {
  if (!Number.isFinite(part) || !Number.isFinite(whole) || whole <= 0) return 0
  return Math.round((part / whole) * 100)
}

/** Bar color for a fill level, matching the old reservation bars. */
export const fillTone = (percent: number): Tone => {
  if (percent >= 90) return 'danger'
  if (percent >= 70) return 'warn'
  return 'ok'
}

export type PressureSummary = { label: string; tone: Tone; detail?: string; title?: string }

/**
 * One-line status of a node's memory pressure response, as the CLI's
 * PRESSURE column reads it: `ok`, `under pressure`, `response off`, or
 * `status unknown` when the worker predates the response.
 */
export const pressureSummary = (memory: NodeResourceUsage | undefined, reported: boolean): PressureSummary => {
  if (!reported || !memory) return { label: 'not reported', tone: 'neutral' }
  const response = memory.pressureResponse
  const free = memory.availableBytes > 0n ? `free ${formatBytes(Number(memory.availableBytes))}` : undefined
  const stall = memory.pressureAvailable ? `stall ${memory.pressureSomeAvg10.toFixed(1)}%` : undefined
  const detail = [free, stall].filter(Boolean).join(', ') || undefined
  if (!response) {
    return { label: 'status unknown', tone: 'neutral', detail, title: 'This worker predates the memory pressure response, or did not report its status.' }
  }
  const title = pressureThresholds(response)
  if (!response.enabled) return { label: 'response off', tone: 'neutral', detail, title }
  if (response.underPressure) return { label: 'under pressure', tone: 'danger', detail, title }
  return { label: 'ok', tone: 'ok', detail, title }
}

export const pressureThresholds = (response: MemoryPressureStatus): string => {
  const parts = [
    `Acts when free memory drops below ${formatBytes(Number(response.minFreeBytes))}`,
    `or stall rises above ${response.maxStallPercent}%`,
    `settles ${String(response.settleSeconds)}s between steps`,
  ]
  const steps = response.steps.length > 0 ? `Steps: ${response.steps.join(' → ')}` : 'No steps enabled'
  return `${parts.join(', ')}. ${steps}.`
}

/**
 * Tone per pressure step. Named steps only, never numbers: a step name reads
 * on its own in a chip, an event, or a stop reason. Unknown names (a newer
 * worker) render neutral rather than breaking.
 */
export const stepTone = (step: string): Tone => {
  switch (step) {
    case 'reclaim':
      return 'info'
    case 'suspend':
      return 'accent'
    case 'evict-bursting':
      return 'warn'
    case 'evict-reserved':
      return 'danger'
    default:
      return 'neutral'
  }
}

/** What a step does, for chip tooltips. */
export const stepDescription = (step: string): string | undefined => {
  switch (step) {
    case 'reclaim':
      return 'Asks the kernel to reclaim memory above the reservation. Nothing stops.'
    case 'suspend':
      return 'Checkpoints and stops instances whose task sets suspend.onPressure, with their state intact. They resume later.'
    case 'evict-bursting':
      return 'Stops allocations using more than their reservation. They reschedule with cause node_pressure.'
    case 'evict-reserved':
      return 'Stops allocations within their reservation. Only on oversubscribed nodes.'
    default:
      return undefined
  }
}

/** `2m41s`, `1h05m`, or `-` for an allocation active in the last interval. */
export const formatIdle = (seconds: number | bigint): string => {
  const total = Number(seconds)
  if (!Number.isFinite(total) || total <= 0) return '-'
  const hours = Math.floor(total / 3600)
  const minutes = Math.floor((total % 3600) / 60)
  const secs = Math.floor(total % 60)
  if (hours > 0) return `${hours}h${String(minutes).padStart(2, '0')}m`
  if (minutes > 0) return `${minutes}m${String(secs).padStart(2, '0')}s`
  return `${secs}s`
}

/** Seconds since an allocation's last activity, from an instance's `usage`. */
export const idleSecondsFromUsage = (usage: AllocationUsage, nowMs = Date.now()): number => {
  const last = Number(usage.lastActivityUnix)
  if (!Number.isFinite(last) || last <= 0) return 0
  // A sample within the last report interval (10s) counts as active, the
  // same rule the manager applies to `idle_seconds`.
  const idle = Math.floor(nowMs / 1000) - last
  return idle > 10 ? idle : 0
}

/** `0.25` cores from millicores, or `-` when nothing was measured. */
export const formatCores = (millicores: bigint | number | undefined): string => {
  if (millicores === undefined) return '-'
  return (Number(millicores) / 1000).toFixed(2)
}

/** `512/4096`, or just `512` when the reservation is the limit. */
export const reservationLabel = (reservedMiB: number | undefined, maxMiB: number | undefined): string => {
  if (reservedMiB === undefined || reservedMiB <= 0) return '-'
  if (maxMiB === undefined || maxMiB <= 0 || maxMiB === reservedMiB) return formatMiB(reservedMiB)
  return `${formatMiB(reservedMiB)}/${formatMiB(maxMiB)}`
}

/** Allocations in the node's pressure order, first-acted-on first. */
export const pressureOrder = (allocations: readonly NodeAllocationUsage[]): NodeAllocationUsage[] =>
  allocations.filter((allocation) => allocation.pressureRank > 0).sort((a, b) => a.pressureRank - b.pressureRank)

/** True when this allocation is using more than it reserved. */
export const isBursting = (allocation: NodeAllocationUsage): boolean =>
  allocation.usage !== undefined && bytesToMiB(allocation.usage.memoryCurrentBytes) > allocation.reservedMemoryMib

/**
 * Why a node has no usage, for the empty state. The proto lists the causes
 * (`GetNodeUsageReply.usage_reported`); the UI cannot tell them apart, so it
 * names them all.
 */
export const UNREPORTED_REASONS = [
  'the worker has resourceEnforcement off (always the case on macOS)',
  'the worker predates usage reporting',
  'it has not reported since this manager started',
  'it sends its heartbeats to another manager',
]

/**
 * GetNodeUsage, with the two failures the UI treats as "no data" rather than
 * an error: an older manager without the RPC, and a caller without
 * `node:read`. Everything else (an outage) still rejects, so the page's
 * `retainOnError` keeps the last good sample.
 */
export const fetchNodeUsage = async (nodeId: string): Promise<GetNodeUsageReply | null> => {
  try {
    return await managerClient.getNodeUsage({ nodeId })
  } catch (error) {
    if (
      error instanceof ConnectError &&
      [Code.Unimplemented, Code.PermissionDenied, Code.Unauthenticated, Code.NotFound].includes(error.code)
    ) {
      return null
    }
    throw error
  }
}

/**
 * Usage for every allocation on a set of nodes, keyed by allocation id. Used
 * by the job page, which knows its allocations' nodes but not their usage.
 * A node that fails to answer contributes nothing.
 */
export const fetchAllocationUsageByNode = async (nodeIds: readonly string[]): Promise<Map<number, NodeAllocationUsage>> => {
  const replies = await Promise.all([...new Set(nodeIds)].map((nodeId) => fetchNodeUsage(nodeId).catch(() => null)))
  const byAllocation = new Map<number, NodeAllocationUsage>()
  for (const reply of replies) {
    for (const allocation of reply?.allocations ?? []) {
      byAllocation.set(Number(allocation.allocationId), allocation)
    }
  }
  return byAllocation
}

/**
 * Usage for a page of nodes in one call (ListNodeUsage), keyed by node id.
 * Falls back to one GetNodeUsage per node on a manager that predates the
 * batch RPC. A caller without `node:read` gets an empty map, and an outage
 * still rejects so the page keeps its last good sample.
 */
export const fetchNodeUsageBatch = async (nodeIds: readonly string[]): Promise<Map<string, GetNodeUsageReply | null>> => {
  if (nodeIds.length === 0) return new Map()
  try {
    const reply = await managerClient.listNodeUsage({ nodeIds: [...nodeIds] })
    const byId = new Map(reply.nodes.map((node) => [node.nodeId, node]))
    return new Map(nodeIds.map((id) => [id, byId.get(id) ?? null]))
  } catch (error) {
    if (error instanceof ConnectError && error.code === Code.Unimplemented) {
      const replies = await Promise.all(nodeIds.map((id) => fetchNodeUsage(id).catch(() => null)))
      return new Map(nodeIds.map((id, index) => [id, replies[index]]))
    }
    if (error instanceof ConnectError && [Code.PermissionDenied, Code.Unauthenticated].includes(error.code)) {
      return new Map()
    }
    throw error
  }
}

/**
 * CPU capacity as the node can honestly state it: MHz when the platform
 * reports a clock speed, else the core count (Apple Silicon reports cores
 * but no usable MHz), else nothing.
 */
export const cpuCapacityLabel = (cpu: NodeResourceUsage): string | undefined => {
  if (cpu.capacity > 0) return undefined
  if (cpu.cores > 0) return `${Number(cpu.cores.toFixed(1))} cores`
  return undefined
}

/** Cores in use, from `used_millicores`, for nodes without a MHz figure. */
export const usedCoresLabel = (cpu: NodeResourceUsage): string => `${(Number(cpu.usedMillicores) / 1000).toFixed(2)} cores`

/**
 * One content-store budget class for the worker page's content card. `note`
 * is a line under the bar, and `tight` tints it as a warning.
 */
export type ContentBudget = { name: string; usedBytes: number; budgetBytes: number; note?: string; tight?: boolean }

/**
 * Room for one more memory checkpoint on a node, as the scheduler computes
 * it (proto `SnapshotBudget`): the smaller of the budget less what is
 * pinned, and the disk's free space plus evictable content less
 * `minFreeDisk`. Without disk numbers only the budget counts.
 */
export function checkpointRoom(budget: SnapshotBudget): { bytes: number; limitedBy: 'budget' | 'disk' } {
  const budgetRoom = Math.max(0, Number(budget.budgetBytes) - Number(budget.pinnedBytes))
  if (budget.diskFreeBytes === undefined) return { bytes: budgetRoom, limitedBy: 'budget' }
  const diskRoom = Math.max(0, Number(budget.diskFreeBytes) + Number(budget.reclaimableBytes) - Number(budget.minFreeDiskBytes))
  return diskRoom < budgetRoom ? { bytes: diskRoom, limitedBy: 'disk' } : { bytes: budgetRoom, limitedBy: 'budget' }
}

/**
 * The `snapshots` content budget a worker reports on GetNodeUsage, shaped
 * for the content card, with the room left for a new checkpoint. Pinned
 * bytes (local suspend checkpoints) are part of used. Empty when the node
 * has not reported a budget.
 */
export function snapshotBudgets(usage: GetNodeUsageReply | null | undefined): ContentBudget[] {
  const budget = usage?.suspended?.snapshotBudget
  if (!budget || Number(budget.budgetBytes) <= 0) return []
  const room = checkpointRoom(budget)
  const note =
    room.limitedBy === 'disk'
      ? `${formatBytes(room.bytes)} of room for a new checkpoint, limited by disk: ${formatBytes(Number(budget.diskFreeBytes))} free + ${formatBytes(Number(budget.reclaimableBytes))} evictable - ${formatBytes(Number(budget.minFreeDiskBytes))} minFreeDisk`
      : `${formatBytes(room.bytes)} of room for a new checkpoint (${formatBytes(Number(budget.pinnedBytes))} pinned)`
  return [{ name: 'snapshots', usedBytes: Number(budget.usedBytes), budgetBytes: Number(budget.budgetBytes), note, tight: room.bytes === 0 || room.limitedBy === 'disk' }]
}

/** "512 MiB and 250 MHz held", or undefined when nothing is held. */
export function heldLabel(memoryMib: number | undefined, cpuMhz: number | undefined): string | undefined {
  const parts = [memoryMib ? `${formatMiB(memoryMib)} MiB` : undefined, cpuMhz ? `${Math.round(cpuMhz)} MHz` : undefined].filter(Boolean)
  return parts.length > 0 ? `${parts.join(' and ')} held` : undefined
}
