import { Code, ConnectError } from '@connectrpc/connect'
import { createEffect, createSignal, on, onCleanup, type Accessor } from 'solid-js'
import type { InstanceStop, SchedulerEvent } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { parseEventMeta, pressureMeta } from './events'
import { stopCauseLabel, stopCauseTone } from './instances'
import type { JobTaskCheck } from './jobspec'
import type { Tone } from './status'

/*
 * Data helpers for the allocation page (pages/details/AllocationDetail.tsx).
 *
 * There is no GetAllocation RPC: an allocation's facts are spread across the
 * ledger (node:read), the state store (state:read), the job's rollouts and
 * spec (job:read), the node's usage report (node:read), and the event journal
 * (operator:read). Every reader here degrades on denial instead of failing,
 * so the page shows whatever the caller's token can see and says what it
 * cannot.
 */

const DENIED_CODES = new Set([Code.PermissionDenied, Code.Unauthenticated])
// Missing pieces that mean "nothing to show" rather than an outage.
const ABSENT_CODES = new Set([...DENIED_CODES, Code.NotFound, Code.Unimplemented])

export const isAclDenied = (error: unknown): boolean => error instanceof ConnectError && DENIED_CODES.has(error.code)

/**
 * Resolves to `fallback` when the call was denied, names something that is
 * gone, or hits an older manager; every other failure (an outage) still
 * rejects, so the page's `retainOnError` keeps the last good value.
 */
export const orAbsent = async <T, F>(promise: Promise<T>, fallback: F): Promise<T | F> => {
  try {
    return await promise
  } catch (error) {
    if (error instanceof ConnectError && ABSENT_CODES.has(error.code)) return fallback
    throw error
  }
}

/** The route's id as an allocation id, or undefined for anything else. */
export const parseAllocationId = (raw: string | undefined): bigint | undefined => {
  const value = (raw ?? '').trim()
  if (!/^\d{1,18}$/.test(value)) return undefined
  const id = BigInt(value)
  return id > 0n ? id : undefined
}

// ---------------------------------------------------------------------------
// Status and health
// ---------------------------------------------------------------------------

/** Statuses of an allocation whose process is (or is about to be) running. */
const RUNNING_STATUSES = new Set(['placed', 'running', 'active'])

export const isRunningStatus = (status: string | undefined): boolean => RUNNING_STATUSES.has((status ?? '').toLowerCase())

/**
 * `failed:<reason>` is how a failed job stamps its allocations. The badge
 * shows the word, the reason goes beside it.
 */
export const splitStatus = (status: string | undefined): { word: string; detail?: string } => {
  const value = (status ?? '').trim()
  const colon = value.indexOf(':')
  if (colon <= 0) return { word: value || 'unknown' }
  return { word: value.slice(0, colon), detail: value.slice(colon + 1).trim() || undefined }
}

export type HealthView = {
  label: 'healthy' | 'unhealthy' | 'starting' | 'restarting' | 'unknown'
  tone: Tone
  /** The worker's own words, e.g. `http failed: connection refused`. */
  status?: string
}

/**
 * One allocation's health from its latest snapshot. Three cases the badge must
 * not blur: no snapshot yet is `unknown` (not unhealthy), a task still coming
 * up reports `healthy = false` with `starting`-style text and is `starting`,
 * and a process the supervisor is restarting in place is `restarting`.
 */
export const healthView = (healthy: boolean | undefined, status: string | undefined): HealthView => {
  const text = (status ?? '').trim() || undefined
  if (healthy === undefined) return { label: 'unknown', tone: 'warn', status: text }
  if (healthy) return { label: 'healthy', tone: 'ok', status: text }
  const lower = (text ?? '').toLowerCase()
  if (lower === 'starting' || lower === 'health checks starting' || lower.startsWith('staged') || lower === 'restored') {
    return { label: 'starting', tone: 'info', status: text }
  }
  if (lower.endsWith('restarting')) return { label: 'restarting', tone: 'warn', status: text }
  return { label: 'unhealthy', tone: 'danger', status: text }
}

/**
 * The name a worker gives a check when reporting it (`TaskCheck::default_name`
 * in src/structs/job.rs): the declared name, or one built from its kind.
 */
export const checkReportName = (check: JobTaskCheck): string | undefined => {
  if (check.name) return check.name
  switch (check.kind) {
    case 'http':
      return `HTTP Check ${check.path ?? '/'}`
    case 'tcp':
      return `TCP Check ${check.port ?? 'unknown'}`
    default:
      // Exec checks are named after their command, which the parsed spec
      // does not carry; matched by prefix below.
      return undefined
  }
}

export type CheckReport = { ok: boolean; reason?: string }

/**
 * What the latest health snapshot says about one declared check. A worker
 * reports one status per allocation — the result of whichever health or boot
 * check ran last (`<name> passed` / `<name> failed: <reason>`) — so at most
 * one check has a result at a time. Readiness results are never reported.
 */
export const checkReport = (check: JobTaskCheck, status: string | undefined): CheckReport | undefined => {
  if (!status || check.type === 'readiness') return undefined
  const name = checkReportName(check)
  if (name) {
    if (status === `${name} passed`) return { ok: true }
    if (status.startsWith(`${name} failed: `)) return { ok: false, reason: status.slice(name.length + ' failed: '.length) }
    return undefined
  }
  const match = /^Exec Check \S+ (passed|failed: (.*))$/.exec(status)
  if (!match) return undefined
  return match[1] === 'passed' ? { ok: true } : { ok: false, reason: match[2] }
}

// ---------------------------------------------------------------------------
// Reserved resources
// ---------------------------------------------------------------------------

export type ResourceEntry = { key: string; label: string; value: string; title?: string }

const RESOURCE_LABELS: Record<string, { label: string; unit?: string; title: string }> = {
  memory: { label: 'Memory', unit: 'MiB', title: 'resources.memory: the reservation the scheduler packs on, and the cgroup memory.low' },
  memoryMax: { label: 'Memory ceiling', unit: 'MiB', title: 'resources.memoryMax: the burst ceiling, enforced as memory.max' },
  cpu: { label: 'CPU share', unit: 'MHz', title: 'resources.cpu: a proportional weight that only matters under contention' },
  cpus: { label: 'CPU share', unit: 'MHz', title: 'resources.cpus: an older alias for cpu, also in MHz' },
  cores: { label: 'Exclusive cores', title: 'resources.cores: whole CPUs pinned to this task' },
}

/**
 * The task's `resources` block as stored on the allocation (as declared;
 * scheduler defaults are applied when packing, not written here).
 */
export const parseAllocationResources = (raw: string | undefined): ResourceEntry[] => {
  if (!raw) return []
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return []
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return []
  return Object.entries(parsed as Record<string, unknown>)
    .filter(([, value]) => value !== null && value !== undefined)
    .map(([key, value]) => {
      const known = RESOURCE_LABELS[key]
      const text = Array.isArray(value)
        ? value.map((item) => (typeof item === 'string' ? item : JSON.stringify(item))).join(', ')
        : typeof value === 'number' || typeof value === 'string'
          ? `${value}${known?.unit ? ` ${known.unit}` : ''}`
          : JSON.stringify(value)
      return { key, label: known?.label ?? key, value: text, title: known?.title ?? `resources.${key}` }
    })
}

// ---------------------------------------------------------------------------
// State store (state:read)
// ---------------------------------------------------------------------------

/**
 * Store timestamps come as `2026-10-12 01:38:19.123+00:00` (space separator,
 * sometimes an unpadded hour or a seconds offset), which `Date` rejects.
 * Returns an ISO string, or undefined when the value is neither that nor
 * RFC 3339.
 */
export const parseStoreTime = (value: unknown): string | undefined => {
  if (typeof value !== 'string' || !value.trim()) return undefined
  const raw = value.trim()
  const match = /^(\d{4}-\d{2}-\d{2})[ T](\d{1,2}):(\d{2}):(\d{2})(\.\d+)?\s*(Z|UTC|[+-]\d{2}:?\d{2}(?::?\d{2})?)?$/i.exec(raw)
  if (match) {
    const [, day, hour, minute, second, fraction, zone] = match
    const millis = fraction ? fraction.slice(0, 4).padEnd(4, '0') : ''
    let offset = 'Z'
    if (zone && !/^(z|utc)$/i.test(zone)) {
      const digits = zone.replace(/:/g, '')
      offset = `${digits.slice(0, 3)}:${digits.slice(3, 5)}`
    }
    const parsed = new Date(`${day}T${hour.padStart(2, '0')}:${minute}:${second}${millis}${offset}`)
    return Number.isNaN(parsed.getTime()) ? undefined : parsed.toISOString()
  }
  const fallback = new Date(raw)
  return Number.isNaN(fallback.getTime()) ? undefined : fallback.toISOString()
}

/** One allocation row, joined to its task config and health snapshot. */
export type AllocationStateRow = {
  status: string
  nodeId: string
  jobConfigId: string
  taskConfigId: bigint
  instanceId?: string
  /** When the scheduler placed it (ISO). Unset on rows older than the column. */
  placedAt?: string
  taskName?: string
  group?: string
  jobId?: string
  lifecycle?: string
  healthy?: boolean
  healthStatus?: string
  /** When the worker last reported health (ISO). */
  healthAt?: string
  /** JSON, same shapes as `LedgerAllocation.network` / `.resources`. */
  network?: string
  resources?: string
}

/**
 * The short-lived record of why an allocation was removed
 * (`allocation_stop_notice`), kept so the worker can tell the task why it is
 * being stopped. Survives the allocation row for a while, then is pruned.
 */
export type StopNotice = { cause: string; nodeId?: string; at?: string; preemptorJob?: string; preemptorBand?: string }

export type AllocationStateLookup = {
  /**
   * The store could not be read: no state:read, or a query an older schema
   * rejects. Nothing below is known, and absence proves nothing.
   */
  denied: boolean
  row?: AllocationStateRow
  stop?: StopNotice
}

const str = (value: unknown): string | undefined => (typeof value === 'string' && value !== '' ? value : undefined)
const num = (value: unknown): number | undefined => (typeof value === 'number' && Number.isFinite(value) ? value : undefined)

// JSONB columns arrive as text or as parsed JSON depending on the driver.
const jsonText = (value: unknown): string | undefined => {
  if (value === null || value === undefined) return undefined
  return typeof value === 'string' ? value || undefined : JSON.stringify(value)
}

const queryRows = async (sql: string): Promise<Record<string, unknown>[]> => {
  const reply = await managerClient.queryStateStore({ sql })
  const rows: Record<string, unknown>[] = []
  for (const row of reply.rows) {
    try {
      const parsed = JSON.parse(row.json) as unknown
      if (parsed && typeof parsed === 'object') rows.push(parsed as Record<string, unknown>)
    } catch {
      // ignore malformed rows
    }
  }
  return rows
}

/**
 * The allocation's row and stop notice, straight from the state store. This
 * is the only reader that knows when an allocation was placed and when its
 * health was last reported. A denial (no state:read) or an older schema
 * returns `denied` / empty rather than failing.
 */
export const fetchAllocationState = async (id: bigint): Promise<AllocationStateLookup> => {
  const allocationSql =
    'SELECT a.status AS status, a.node_id AS node_id, a.job_config_id AS job_config_id, ' +
    'a.task_config_id AS task_config_id, a.instance_id AS instance_id, a.created_at_unix AS created_at_unix, ' +
    'a.network AS network, a.resources AS resources, ' +
    'tc.name AS task_name, tc.group_name AS group_name, tc.job_id AS job_id, tc.lifecycle AS lifecycle, ' +
    'ah.healthy AS healthy, ah.status AS health_status, ah.updated_at AS health_updated_at ' +
    'FROM allocation a ' +
    'LEFT JOIN task_config tc ON tc.id = a.task_config_id ' +
    'LEFT JOIN allocation_health ah ON ah.allocation_id = a.id ' +
    `WHERE a.id = ${id.toString()}`
  const stopSql =
    'SELECT cause, node_id, created_at, preemptor_job, preemptor_band FROM allocation_stop_notice ' +
    `WHERE allocation_id = ${id.toString()}`

  let rows: Record<string, unknown>[]
  try {
    rows = await queryRows(allocationSql)
  } catch {
    // Denied, or SQL an older manager's schema rejects. Garnish either way:
    // the ledger answers the same question for anyone with node:read, and an
    // outage fails that read too.
    return { denied: true }
  }
  // A manager that predates the stop-notice table answers with an error;
  // the row is still worth having.
  const stopRows = await queryRows(stopSql).catch(() => [] as Record<string, unknown>[])

  const raw = rows[0]
  const row: AllocationStateRow | undefined = raw
    ? {
        status: str(raw.status) ?? '',
        nodeId: str(raw.node_id) ?? '',
        jobConfigId: str(raw.job_config_id) ?? '',
        taskConfigId: BigInt(num(raw.task_config_id) ?? 0),
        instanceId: str(raw.instance_id),
        placedAt: num(raw.created_at_unix) ? new Date(num(raw.created_at_unix)! * 1000).toISOString() : undefined,
        taskName: str(raw.task_name),
        group: str(raw.group_name),
        jobId: str(raw.job_id),
        lifecycle: str(raw.lifecycle),
        healthy: raw.healthy === null || raw.healthy === undefined ? undefined : Boolean(raw.healthy),
        healthStatus: str(raw.health_status),
        healthAt: parseStoreTime(raw.health_updated_at),
        network: jsonText(raw.network),
        resources: jsonText(raw.resources),
      }
    : undefined
  const stopRaw = stopRows[0]
  const stop: StopNotice | undefined = stopRaw
    ? {
        cause: str(stopRaw.cause) ?? 'stop',
        nodeId: str(stopRaw.node_id),
        at: parseStoreTime(stopRaw.created_at),
        preemptorJob: str(stopRaw.preemptor_job),
        preemptorBand: str(stopRaw.preemptor_band),
      }
    : undefined
  return { denied: false, row, stop }
}

export type TaskConfigInfo = { name: string; group?: string; lifecycle?: string; jobId?: string }

/**
 * A task config by id, for an allocation whose row is gone: its events name
 * the task config, not the task. Task configs outlive their allocations (they
 * belong to a job config version). Undefined on denial or a miss.
 */
export const fetchTaskConfigInfo = async (taskConfigId: bigint): Promise<TaskConfigInfo | undefined> => {
  try {
    const rows = await queryRows(
      `SELECT name, group_name, lifecycle, job_id FROM task_config WHERE id = ${taskConfigId.toString()}`,
    )
    const raw = rows[0]
    const name = raw ? str(raw.name) : undefined
    if (!raw || !name) return undefined
    return { name, group: str(raw.group_name), lifecycle: str(raw.lifecycle), jobId: str(raw.job_id) }
  } catch {
    return undefined
  }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/**
 * Batch events (`allocation.placed`, `.stopping`, `.preempted`,
 * `.preempt_planned`) name the allocations they cover in a metadata list
 * rather than in `allocation_id`; this finds this allocation's entry.
 */
export const eventAllocationEntry = (event: SchedulerEvent, id: bigint): Record<string, unknown> | undefined => {
  const meta = parseEventMeta(event)
  const target = Number(id)
  for (const key of ['placements', 'allocations', 'victims']) {
    const list = meta[key]
    if (!Array.isArray(list)) continue
    for (const item of list) {
      if (item && typeof item === 'object' && (item as Record<string, unknown>).allocationId === target) {
        return item as Record<string, unknown>
      }
    }
  }
  return undefined
}

export const eventMentionsAllocation = (event: SchedulerEvent, id: bigint): boolean =>
  event.allocationId === id || eventAllocationEntry(event, id) !== undefined

/** Facts the events carry about an allocation the manager no longer has. */
export type EventFacts = {
  jobId?: string
  nodeId?: string
  jobConfigId?: string
  taskConfigId?: bigint
  instanceId?: string
  placedAt?: string
}

export const factsFromEvents = (events: readonly SchedulerEvent[], id: bigint): EventFacts => {
  const facts: EventFacts = {}
  // Newest first, so the latest word wins for node and config.
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index]
    const meta = parseEventMeta(event)
    const entry = event.allocationId === id ? undefined : eventAllocationEntry(event, id)
    facts.jobId ??= event.jobId
    facts.nodeId ??= str(entry?.nodeId) ?? (event.allocationId === id ? event.nodeId : undefined)
    facts.jobConfigId ??= str(meta.jobConfigId)
    const taskConfigId = num(entry?.taskConfigId) ?? num(meta.taskConfigId)
    if (facts.taskConfigId === undefined && taskConfigId !== undefined) facts.taskConfigId = BigInt(taskConfigId)
    facts.instanceId ??= str(meta.instanceId) ?? str(meta.instance_id)
    if (event.kind === 'allocation.placed' && entry) facts.placedAt = event.createdAt
  }
  return facts
}

export type StopReason = {
  /** Stop-notice vocabulary (`node_pressure`, `preempted`, ...) or `failed`/`completed`. */
  cause: string
  label: string
  tone: Tone
  detail?: string
  hint?: string
  at?: string
  preemptorJob?: string
  /** Where this came from, for the tooltip. */
  source: string
}

// One event's account of a stop, or undefined when the event is not one.
// `generic` marks `allocation.stopping`, which says a stop began and with
// what grace, but not the worker's reason.
const stopFromEvent = (event: SchedulerEvent, id: bigint): (StopReason & { generic?: boolean; graceSeconds?: number }) | undefined => {
  const meta = parseEventMeta(event)
  switch (event.kind) {
    case 'allocation.failed':
      if (event.allocationId !== id) return undefined
      return { cause: 'failed', label: 'failed', tone: 'danger', detail: str(meta.reason) ?? event.message, at: event.createdAt, source: 'allocation.failed event' }
    case 'allocation.terminated': {
      if (event.allocationId !== id) return undefined
      const success = meta.success === true
      return {
        cause: success ? 'completed' : 'terminated',
        label: success ? 'completed' : 'exited without success',
        tone: success ? 'ok' : 'danger',
        detail: str(meta.message) ?? event.message,
        at: event.createdAt,
        source: 'allocation.terminated event',
      }
    }
    case 'node.pressure': {
      if (event.allocationId !== id) return undefined
      const pressure = pressureMeta(event)
      // `reclaim` stops nothing; it is history, not a stop.
      if (pressure.step === 'reclaim') return undefined
      const cause = pressure.cause ?? 'node_pressure'
      return { cause, label: stopCauseLabel(cause), tone: stopCauseTone(cause), detail: pressure.detail ?? event.message, hint: pressure.hint, at: event.createdAt, source: 'node.pressure event' }
    }
    case 'allocation.preempted':
      if (!eventAllocationEntry(event, id)) return undefined
      return {
        cause: 'preempted',
        label: stopCauseLabel('preempted'),
        tone: stopCauseTone('preempted'),
        detail: event.message,
        preemptorJob: str(meta.preemptorJob),
        at: event.createdAt,
        source: 'allocation.preempted event',
      }
    case 'allocation.stopping': {
      const entry = eventAllocationEntry(event, id)
      if (!entry) return undefined
      const cause = str(meta.cause) ?? 'stop'
      const grace = num(entry.graceSeconds)
      return {
        cause,
        label: stopCauseLabel(cause),
        tone: stopCauseTone(cause),
        detail: grace !== undefined ? `Asked to stop (${cause.replace(/_/g, ' ')}) with up to ${grace}s to exit.` : `Asked to stop (${cause.replace(/_/g, ' ')}).`,
        at: event.createdAt,
        source: 'allocation.stopping event',
        generic: true,
        graceSeconds: grace,
      }
    }
    default:
      return undefined
  }
}

// A worker's own report (an eviction, a failure) and the manager's
// `allocation.stopping` for the same stop land moments apart, in either order.
const SAME_STOP_MS = 10 * 60_000

/**
 * Why the allocation last stopped (or is stopping), from the most specific
 * record available: its own events first (they carry the worker's reason and
 * hint, so they win over the generic `allocation.stopping` for the same stop),
 * then the stop notice, then the instance's `last_stop`.
 */
export const deriveStopReason = (
  events: readonly SchedulerEvent[],
  id: bigint,
  notice: StopNotice | undefined,
  instanceStop: InstanceStop | undefined,
): StopReason | undefined => {
  let newest: ReturnType<typeof stopFromEvent>
  let specific: ReturnType<typeof stopFromEvent>
  for (let index = events.length - 1; index >= 0 && !specific; index -= 1) {
    const stop = stopFromEvent(events[index], id)
    if (!stop) continue
    newest ??= stop
    if (!stop.generic) specific = stop
  }
  if (newest) {
    const time = (value?: string) => (value ? Date.parse(value) : Number.NaN)
    const sameStop = specific && (newest === specific || Math.abs(time(newest.at) - time(specific.at)) <= SAME_STOP_MS)
    if (!specific || !sameStop) return newest
    const base = (specific.detail ?? '').trim()
    const grace = newest.generic && newest.graceSeconds !== undefined ? `It had up to ${newest.graceSeconds}s to exit.` : ''
    const joined = base && grace ? `${base}${/[.!?]$/.test(base) ? '' : '.'} ${grace}` : base || grace
    return { ...specific, detail: joined || undefined }
  }
  if (notice) {
    return {
      cause: notice.cause,
      label: stopCauseLabel(notice.cause),
      tone: stopCauseTone(notice.cause),
      detail: notice.preemptorJob ? `Preempted to place job ${notice.preemptorJob}${notice.preemptorBand ? ` (${notice.preemptorBand})` : ''}.` : undefined,
      preemptorJob: notice.preemptorJob,
      at: notice.at,
      source: 'stop notice',
    }
  }
  if (instanceStop && instanceStop.allocationId === id) {
    return {
      cause: instanceStop.cause,
      label: stopCauseLabel(instanceStop.cause),
      tone: stopCauseTone(instanceStop.cause),
      detail: instanceStop.detail || undefined,
      hint: instanceStop.hint || undefined,
      at: instanceStop.at || undefined,
      preemptorJob: instanceStop.preemptorJob || undefined,
      source: "the instance's last stop",
    }
  }
  return undefined
}

const MAX_EVENTS = 500
const ALLOCATION_PAGE = 200

export type AllocationEventsStatus = 'loading' | 'ready' | 'denied' | 'error'

/**
 * The allocation's event history, polled incrementally on `tick`.
 *
 * The manager's `allocation_id` filter matches the events tagged with this
 * allocation (failures, exits, pressure actions, artifacts, snapshots) and
 * the batch events that list it in their metadata (placed, stopping,
 * preempted). An `after_id` cursor means a poll only moves new rows. Existing
 * event objects are never replaced, so a list keyed on them keeps its rows
 * (and their expanded state) across polls.
 */
export function useAllocationEvents(allocationId: Accessor<bigint | undefined>, tick: Accessor<number>) {
  const [events, setEvents] = createSignal<SchedulerEvent[]>([])
  const [status, setStatus] = createSignal<AllocationEventsStatus>('loading')
  const [error, setError] = createSignal<string | undefined>()

  const byId = new Map<string, SchedulerEvent>()
  let currentId: bigint | undefined
  let allocationCursor: bigint | undefined
  let generation = 0
  let busy = false

  const nextCursor = (cursor: bigint | undefined, reply: { events: readonly SchedulerEvent[]; latestId: bigint }, limit: number) => {
    let next = cursor
    for (const event of reply.events) if (next === undefined || event.id > next) next = event.id
    // A full page means more is waiting: resume after the last row read, not
    // at the journal's head, or the rest would be skipped.
    if (reply.events.length < limit && (next === undefined || reply.latestId > next)) next = reply.latestId
    return next
  }

  const poll = async (id: bigint) => {
    if (id !== currentId) {
      currentId = id
      allocationCursor = undefined
      byId.clear()
      generation += 1
      busy = false
      setEvents([])
      setStatus('loading')
      setError(undefined)
    }
    if (busy) return
    busy = true
    const run = generation
    try {
      const reply = await managerClient.listEvents({ allocationId: id, afterId: allocationCursor, limit: ALLOCATION_PAGE })
      if (run !== generation) return
      allocationCursor = nextCursor(allocationCursor, reply, ALLOCATION_PAGE)

      let added = false
      for (const event of reply.events) {
        const key = String(event.id)
        if (byId.has(key)) continue
        byId.set(key, event)
        added = true
      }
      if (added) {
        const sorted = [...byId.values()].sort((a, b) => (a.id === b.id ? 0 : a.id < b.id ? -1 : 1))
        const kept = sorted.slice(-MAX_EVENTS)
        if (kept.length < sorted.length) for (const dropped of sorted.slice(0, sorted.length - kept.length)) byId.delete(String(dropped.id))
        setEvents(kept)
      }
      setStatus('ready')
      setError(undefined)
    } catch (caught) {
      if (run !== generation) return
      if (isAclDenied(caught)) {
        setStatus('denied')
      } else if (byId.size === 0) {
        setStatus('error')
        setError(caught instanceof Error ? caught.message : String(caught))
      }
    } finally {
      if (run === generation) busy = false
    }
  }

  let deniedTicks = 0
  createEffect(
    on([allocationId, tick], ([id]) => {
      if (id === undefined) return
      // A denied identity is asked again only now and then (it may log in),
      // not on every tick.
      if (status() === 'denied' && id === currentId) {
        deniedTicks += 1
        if (deniedTicks % 15 !== 0) return
      }
      void poll(id)
    }),
  )
  onCleanup(() => {
    generation += 1
  })

  return { events, status, error }
}
