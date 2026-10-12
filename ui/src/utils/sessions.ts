import type { InstanceInfo } from '../gen/legion_pb'
import { allows, jobScope } from './acl'
import type { JobTask } from './jobspec'

/*
 * Session mode in the UI (docs/session-mode.md): which instances are
 * sessions, which allocations are envelopes, what Stop and Promote can do,
 * and the limits that keep a job with a million sessions from taking the
 * browser down.
 *
 * Sessions have no rows. ListInstances reads them from the managers'
 * in-memory session index and returns every one in a single reply: it has no
 * paging, no limit, and no node or envelope filter. So the UI never renders
 * a full listing, polls a large one only on request, and does not fetch one
 * automatically when the job's envelopes could hold more sessions than a
 * browser should decode at once.
 */

/** Rows an instances table renders before "Show more". */
export const INSTANCE_PAGE = 100

/** The most rows "Show more" will render. Past this, filter or use the CLI. */
export const INSTANCE_RENDER_MAX = 1000

/**
 * A listing at least this long is no longer re-polled every two seconds:
 * the job page keeps the last one and refreshes it on request.
 */
export const LARGE_LISTING = 5000

/**
 * When a job's envelopes could hold more sessions than this, the job page
 * does not fetch the listing until asked: ListInstances returns every
 * session in one reply.
 */
export const AUTO_LIST_MAX_SESSIONS = 50_000

/** Suffix of a session task's generated envelope task (`workspace-envelope`). */
export const ENVELOPE_SUFFIX = '-envelope'

/** The reserved runtime envelope allocations run on. */
export const ENVELOPE_RUNTIME = 'envelope'

export const isSession = (instance: InstanceInfo): boolean => instance.mode === 'session'

export const isSessionTask = (task: JobTask | undefined): boolean =>
  task?.stateful?.mode === 'session' || Boolean(task?.stateful?.envelope && Object.keys(task.stateful.envelope).length > 0)

/**
 * The session task an envelope task serves, or undefined when `taskName` is
 * not an envelope. With the job's tasks, the served task must be a session
 * task (the rendered spec never lists the generated envelope task itself).
 * Without them, the name alone decides: the jobspec refuses a task named
 * `<task>-envelope` beside a session task `<task>`, so the suffix is a
 * strong signal, though a job with no session task may still use it.
 */
export const envelopeServes = (taskName: string | undefined, tasks?: readonly JobTask[]): string | undefined => {
  if (!taskName || !taskName.endsWith(ENVELOPE_SUFFIX)) return undefined
  const served = taskName.slice(0, -ENVELOPE_SUFFIX.length)
  if (!served) return undefined
  if (!tasks || tasks.length === 0) return served
  if (tasks.some((task) => task.name === taskName && task.runtime !== ENVELOPE_RUNTIME)) return undefined
  return tasks.some((task) => task.name === served && isSessionTask(task)) ? served : undefined
}

export const envelopeTaskName = (sessionTask: string): string => `${sessionTask}${ENVELOPE_SUFFIX}`

/**
 * How many sessions of `task` one envelope admits: the fewest whole sessions
 * any resource the session declares allows. Undefined when the spec does not
 * say (no envelope, or no amount the two share).
 */
export const sessionsPerEnvelope = (task: JobTask | undefined): number | undefined => {
  const envelope = task?.stateful?.envelope
  const session = task?.resourceAmounts
  if (!envelope || !session) return undefined
  let fit: number | undefined
  for (const [key, amount] of Object.entries(session)) {
    if (amount <= 0 || envelope[key] === undefined) continue
    const count = Math.floor(envelope[key] / amount)
    fit = fit === undefined ? count : Math.min(fit, count)
  }
  return fit
}

/** `1,234,567`. */
export const formatCount = (value: number): string => value.toLocaleString('en-US')

/** What a session is, for tooltips beside its `session` badge. */
export const SESSION_COPY =
  'Session: started on a node with envelope room and held only by that node and the managers’ in-memory index. Not durable: it is lost with its node and not rescheduled. Promote it to make it a keyed instance.'

/** What an envelope allocation is, for tooltips beside its `envelope` badge. */
export const ENVELOPE_COPY =
  'Envelope: a placeholder allocation that reserves this node’s room for a session task’s sessions. It runs nothing; sessions are admitted against it.'

const writeBlocker = (jobName: string, verb: string) =>
  allows('job', 'write', jobScope(jobName)) ? undefined : `${verb} needs job:write on this job`

/** Why Promote is unavailable, or undefined. */
export const promoteBlocker = (instance: InstanceInfo, jobName: string): string | undefined => {
  if (!isSession(instance)) return 'Only a running session can be promoted'
  return writeBlocker(jobName, 'Promoting')
}

/**
 * Why a name is not a valid promoted id, or undefined (empty keeps the
 * session id). The manager also refuses the minted shape and any id an
 * instance ever had; it has the last word.
 */
export const promoteNameError = (name: string): string | undefined => {
  if (!name) return undefined
  if (name.length > 128) return 'At most 128 characters'
  if (!/^[A-Za-z0-9._-]+$/.test(name)) return 'Letters, digits, dot, dash, and underscore only'
  return undefined
}

/** Counts over a listing, in one pass. */
export type InstanceCounts = {
  total: number
  byState: Map<string, number>
  byMode: Map<string, number>
  sessions: number
  /** Sessions per node, for the busiest-nodes summary. */
  sessionsByNode: Map<string, number>
  /** Sessions per envelope allocation id. */
  sessionsByEnvelope: Map<string, number>
  /** Sessions per task. */
  sessionsByTask: Map<string, number>
}

const bump = (map: Map<string, number>, key: string) => map.set(key, (map.get(key) ?? 0) + 1)

export const countInstances = (instances: readonly InstanceInfo[]): InstanceCounts => {
  const counts: InstanceCounts = {
    total: instances.length,
    byState: new Map(),
    byMode: new Map(),
    sessions: 0,
    sessionsByNode: new Map(),
    sessionsByEnvelope: new Map(),
    sessionsByTask: new Map(),
  }
  for (const instance of instances) {
    bump(counts.byState, instance.state)
    bump(counts.byMode, instance.mode)
    if (!isSession(instance)) continue
    counts.sessions += 1
    bump(counts.sessionsByNode, instance.nodeId ?? '')
    if (instance.envelopeAllocId !== undefined) bump(counts.sessionsByEnvelope, String(instance.envelopeAllocId))
    bump(counts.sessionsByTask, instance.task)
  }
  return counts
}

/** The `n` largest entries of a count map, largest first, ties by key. */
export const topCounts = (map: ReadonlyMap<string, number>, n: number): Array<[string, number]> =>
  [...map.entries()].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)).slice(0, n)

/** Filters an instances listing can be narrowed by, also read from the job page's URL. */
export type InstanceFilter = {
  state?: string
  mode?: string
  node?: string
  envelope?: string
  task?: string
  /** Substring of the id, node, task, or promoted-from id. Case-sensitive, like ids: a million lowercase copies per keystroke is the cost it avoids. */
  text?: string
}

export const matchesFilter = (instance: InstanceInfo, filter: InstanceFilter): boolean => {
  if (filter.state && instance.state !== filter.state) return false
  if (filter.mode && instance.mode !== filter.mode) return false
  if (filter.node && instance.nodeId !== filter.node) return false
  if (filter.envelope && String(instance.envelopeAllocId ?? '') !== filter.envelope) return false
  if (filter.task && instance.task !== filter.task) return false
  if (filter.text) {
    const text = filter.text
    if (
      !instance.instanceId.includes(text) &&
      !(instance.nodeId ?? '').includes(text) &&
      !instance.task.includes(text) &&
      !(instance.promotedFrom ?? '').includes(text)
    ) {
      return false
    }
  }
  return true
}

/**
 * The first `limit` items that pass `keep`, and how many pass in all. One
 * pass, and only `limit` items are kept, so a million-row listing costs a
 * scan, not a copy.
 */
export const takeMatching = <T>(items: readonly T[], keep: (item: T) => boolean, limit: number): { shown: T[]; matching: number } => {
  const shown: T[] = []
  let matching = 0
  for (const item of items) {
    if (!keep(item)) continue
    matching += 1
    if (shown.length < limit) shown.push(item)
  }
  return { shown, matching }
}

/** The job page's link to its Instances card, narrowed by `filter`. */
export const jobInstancesHref = (jobId: string, filter: Omit<InstanceFilter, 'text' | 'state'> & { load?: boolean } = {}): string => {
  const params = new URLSearchParams()
  if (filter.mode) params.set('mode', filter.mode)
  if (filter.task) params.set('task', filter.task)
  if (filter.node) params.set('node', filter.node)
  if (filter.envelope) params.set('envelope', filter.envelope)
  if (filter.load) params.set('load', '1')
  const query = params.toString()
  return `/jobs/${encodeURIComponent(jobId)}${query ? `?${query}` : ''}#instances`
}

/** The CLI line that lists a job's instances in full. */
export const listCommand = (jobId: string): string => `legion instance list ${jobId} -j`
