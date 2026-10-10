import type { SchedulerEvent } from '../gen/legion_pb'

/*
 * Typed views of the event metadata the UI renders as more than raw JSON.
 * Event metadata is a JSON blob whose keys are stable per kind (see
 * src/storage/events.rs). Every field is optional here because an older
 * manager may not send it, and a missing field must render as absent, never
 * crash the row.
 */

export const parseEventMeta = (event: SchedulerEvent): Record<string, unknown> => {
  try {
    const parsed = JSON.parse(event.metadataJson || '{}') as unknown
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : {}
  } catch {
    return {}
  }
}

const str = (value: unknown): string | undefined => (typeof value === 'string' && value !== '' ? value : undefined)
const num = (value: unknown): number | undefined => (typeof value === 'number' && Number.isFinite(value) ? value : undefined)

/** `node.pressure`: one action of a node's memory pressure response. */
export type PressureEventMeta = {
  step?: string
  detail?: string
  hint?: string
  cause?: string
  freedBytes?: number
  usedBytes?: number
  reservedBytes?: number
  availableBytes?: number
  freeBelowBytes?: number
}

export const pressureMeta = (event: SchedulerEvent): PressureEventMeta => {
  const meta = parseEventMeta(event)
  return {
    step: str(meta.step),
    detail: str(meta.detail),
    hint: str(meta.hint),
    cause: str(meta.cause),
    freedBytes: num(meta.freed_bytes),
    usedBytes: num(meta.used_bytes),
    reservedBytes: num(meta.reserved_bytes),
    availableBytes: num(meta.available_bytes),
    freeBelowBytes: num(meta.free_below_bytes),
  }
}

/** `snapshot.*`: a snapshot was captured, failed, deleted, or restored from. */
export type SnapshotEventMeta = {
  snapshotId?: string
  instanceId?: string
  kind?: string
  purpose?: string
  name?: string
  totalBytes?: number
  durationMs?: number
  error?: string
}

export const snapshotMeta = (event: SchedulerEvent): SnapshotEventMeta => {
  const meta = parseEventMeta(event)
  return {
    snapshotId: str(meta.snapshotId),
    instanceId: str(meta.instanceId),
    kind: str(meta.kind),
    purpose: str(meta.purpose),
    name: str(meta.name),
    totalBytes: num(meta.totalBytes),
    durationMs: num(meta.durationMs),
    error: str(meta.error),
  }
}

/** Newest first, the order a history card reads in. */
export const newestFirst = (events: readonly SchedulerEvent[]): SchedulerEvent[] =>
  [...events].sort((a, b) => (a.id === b.id ? 0 : a.id < b.id ? 1 : -1))
