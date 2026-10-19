import type { CheckpointSupport, InstanceInfo, InstanceStop } from '../gen/legion_pb'
import { allows, jobScope } from './acl'
import type { JobTask } from './jobspec'
import { memorySnapshotBlocker } from './jobspec'
import type { Tone } from './status'
import { jobPath } from './namespaces'

/*
 * Instance lifecycle vocabulary for suspend, resume, fork, stop, and start
 * (docs/suspend-and-resume.md), and the rules for which actions a row
 * offers. The manager enforces every one of these. The functions here only
 * decide what the UI enables and what a disabled control's tooltip says.
 *
 * Checkpoint eligibility comes from the manager (`checkpoint_support`),
 * which runs the RPCs' own preconditions, so a stored network policy or a
 * node without a VMM is already accounted for. The spec-derived fallback is
 * only for an older manager that does not send it.
 */

/** Suspend reason in words, for the state badge's tooltip. */
export const suspendReasonLabel = (reason: string | undefined): string | undefined => {
  switch (reason) {
    case 'manual':
      return 'suspended by a user (instance suspend)'
    case 'idle':
      return 'suspended by its idle policy (suspend.idle)'
    case 'pressure':
      return "suspended by its node's memory pressure response (suspend.onPressure)"
    default:
      return reason ? `suspended (${reason})` : undefined
  }
}

/** `suspending`, `resuming`, `forking`: an operation the manager is waiting on. */
export const pendingOpLabel = (op: string | undefined): string | undefined => {
  switch (op) {
    case 'suspending':
      return 'Capture requested; the node takes it on its next heartbeat'
    case 'resuming':
      return 'Restore requested; waiting for the node to report'
    case 'forking':
      return "Waiting for the origin's checkpoint to land before it is placed"
    default:
      return op ? `${op} in progress` : undefined
  }
}

/**
 * Stop causes the platform chose, which the instance row calls out. A user
 * stop, a rollout, or a suspend is expected and stays quiet.
 */
const PLATFORM_STOPS = new Set(['failed', 'node_pressure', 'preempted', 'node_lost', 'drain', 'reschedule'])

export const isPlatformStop = (stop: InstanceStop | undefined): stop is InstanceStop =>
  stop !== undefined && PLATFORM_STOPS.has(stop.cause)

export const stopCauseLabel = (cause: string): string => {
  switch (cause) {
    case 'node_pressure':
      return 'stopped by node pressure'
    case 'preempted':
      return 'preempted'
    case 'node_lost':
      return 'node lost'
    case 'drain':
      return 'moved by a drain'
    case 'reschedule':
      return 'rescheduled'
    case 'suspended':
      return 'suspended'
    case 'restore':
      return 'rolled back'
    case 'update':
      return 'replaced by an update'
    case 'stop':
      return 'stopped'
    default:
      return cause.replace(/_/g, ' ')
  }
}

export const stopCauseTone = (cause: string): Tone => {
  switch (cause) {
    case 'node_pressure':
    case 'node_lost':
      return 'warn'
    case 'failed':
    case 'preempted':
      return 'danger'
    default:
      return 'neutral'
  }
}

/** `740 ms`, `1.2 s`, or `-`. */
export const formatMs = (ms: bigint | number | undefined): string => {
  if (ms === undefined) return '-'
  const value = Number(ms)
  if (!Number.isFinite(value) || value <= 0) return '-'
  return value >= 1000 ? `${(value / 1000).toFixed(1)} s` : `${value} ms`
}

/**
 * Seconds until the idle sweep suspends the instance, from `idle_since` and
 * `idle_after_seconds`. Undefined when the task has no idle policy or the
 * instance is not idle. Zero or below means it is due on the next sweep.
 */
export const idleSecondsLeft = (instance: InstanceInfo, nowMs = Date.now()): number | undefined => {
  if (!instance.idleSince || instance.idleAfterSeconds === undefined) return undefined
  const since = Date.parse(instance.idleSince)
  if (!Number.isFinite(since)) return undefined
  const after = Number(instance.idleAfterSeconds)
  return Math.ceil((since + after * 1000 - nowMs) / 1000)
}

/** Seconds an instance has been idle by its policy's signals. */
export const idleSecondsSoFar = (instance: InstanceInfo, nowMs = Date.now()): number | undefined => {
  if (!instance.idleSince) return undefined
  const since = Date.parse(instance.idleSince)
  return Number.isFinite(since) ? Math.max(0, Math.floor((nowMs - since) / 1000)) : undefined
}

const keyed = (instance: InstanceInfo) => instance.mode === 'keyed'
const running = (instance: InstanceInfo) => instance.state === 'active' && instance.currentAllocId !== undefined
const writeBlocker = (jobName: string) => (allows('job', 'write', jobScope(jobName)) ? undefined : 'Needs job:write on this job')

/** The kinds a Suspend control offers, each with why not. */
export type SuspendChoice = { kind: 'memory' | 'disk'; blocker?: string }

export type SuspendPlan = {
  /** Why nothing can be suspended. */
  blocker?: string
  /** The kind the manager takes by default. */
  defaultKind?: 'memory' | 'disk'
  choices: SuspendChoice[]
}

const asKind = (value: string | undefined): 'memory' | 'disk' | undefined =>
  value === 'memory' || value === 'disk' ? value : undefined

/**
 * What Suspend can do for this instance. Memory needs `snapshot:write` on
 * top of `job:write`, since a memory checkpoint holds the workload's
 * secrets. A disk suspend leaves the volume in place and needs no more than
 * a keyed, running instance on a worker that can checkpoint.
 */
export const suspendPlan = (instance: InstanceInfo, jobName: string, task?: JobTask): SuspendPlan => {
  if (instance.mode === 'session') return { blocker: 'A session cannot be suspended: promote it first', choices: [] }
  if (!keyed(instance)) return { blocker: 'Only keyed instances can be suspended', choices: [] }
  if (instance.pendingOp) return { blocker: pendingOpLabel(instance.pendingOp), choices: [] }
  if (!running(instance)) return { blocker: `The instance is ${instance.state}; only an active instance can be suspended`, choices: [] }
  const write = writeBlocker(jobName)
  if (write) return { blocker: `Suspending ${write.toLowerCase()}`, choices: [] }
  const support = instance.checkpointSupport
  const snapshotWrite = allows('snapshot', 'write', jobScope(jobName)) ? undefined : 'A memory checkpoint also needs snapshot:write on this job'
  if (support) {
    if (!support.suspendDefault) return { blocker: support.suspendBlocker || 'This instance cannot be suspended', choices: [] }
    return {
      defaultKind: asKind(support.suspendDefault),
      choices: [
        { kind: 'memory', blocker: support.suspendMemory ? snapshotWrite : support.suspendMemoryBlocker || 'No memory checkpoint for this instance' },
        { kind: 'disk' },
      ],
    }
  }
  // An older manager: the spec decides, and the manager has the last word.
  const memory = memorySnapshotBlocker(task)
  return {
    defaultKind: memory ? 'disk' : 'memory',
    choices: [
      { kind: 'memory', blocker: memory ?? snapshotWrite },
      { kind: 'disk' },
    ],
  }
}

export type ForkPlan = {
  blocker?: string
  /** Spread is offered only when the manager says it would work. */
  spreadBlocker?: string
}

export const forkPlan = (instance: InstanceInfo, jobName: string): ForkPlan => {
  if (instance.mode === 'session') return { blocker: 'A session cannot be forked: promote it first' }
  if (!keyed(instance)) return { blocker: 'Only keyed instances can be forked' }
  if (instance.pendingOp) return { blocker: pendingOpLabel(instance.pendingOp) }
  if (!running(instance)) return { blocker: `The instance is ${instance.state}; only an active instance can be forked` }
  if (!allows('job', 'write', jobScope(jobName)) || !allows('snapshot', 'write', jobScope(jobName))) {
    return { blocker: 'Forking needs job:write and snapshot:write on this job' }
  }
  const support: CheckpointSupport | undefined = instance.checkpointSupport
  if (support && !support.forkMemory && !support.forkDisk) {
    return { blocker: support.forkMemoryBlocker || support.forkDiskBlocker || 'This instance cannot be forked' }
  }
  return {
    spreadBlocker: support ? (support.spread ? undefined : support.spreadBlocker || 'A spread fork is not possible here') : 'This manager does not report whether a spread fork is possible',
  }
}

/** Why Resume (and Resume cold) are unavailable, or undefined. */
export const resumeBlocker = (instance: InstanceInfo, jobName: string): string | undefined => {
  if (instance.state !== 'suspended') return undefined
  if (instance.pendingOp) return pendingOpLabel(instance.pendingOp)
  const write = writeBlocker(jobName)
  return write ? `Resuming ${write.toLowerCase()}` : undefined
}

// States StopInstance moves to `stopped`. A suspended instance can be
// stopped too, which discards its checkpoint (StopInstance in the proto).
const STOPPABLE = new Set(['pending', 'active', 'rescheduling', 'suspended'])

/**
 * Why Stop is unavailable, or undefined. Stop keeps the row, the id, and the
 * storage handle, so Start brings the same instance back; slot instances
 * follow `count.num` instead, and a destroyed one is a tombstone.
 */
export const stopBlocker = (instance: InstanceInfo, jobName: string): string | undefined => {
  // A session has no row to keep: Stop ends it (docs/session-mode.md).
  if (instance.mode === 'session') {
    const write = writeBlocker(jobName)
    return write ? `Ending a session ${write.toLowerCase()}` : undefined
  }
  if (!keyed(instance)) return 'Slot instances follow count.num: scale the task or stop the job instead'
  if (instance.state === 'terminated') return 'Destroyed instances are tombstones and cannot be stopped'
  if (instance.state === 'stopped') return 'Already stopped'
  if (!STOPPABLE.has(instance.state)) return `The instance is ${instance.state}`
  const write = writeBlocker(jobName)
  return write ? `Stopping ${write.toLowerCase()}` : undefined
}

/** Why Start is unavailable for a stopped instance, or undefined. A suspended one resumes instead. */
export const startBlocker = (instance: InstanceInfo, jobName: string): string | undefined => {
  if (instance.mode === 'session') return 'A session that ends is gone; start a new one with legion instance create'
  if (!keyed(instance)) return 'Slot instances follow count.num'
  if (instance.state !== 'stopped') return `Only a stopped instance can be started; this one is ${instance.state}`
  const write = writeBlocker(jobName)
  return write ? `Starting ${write.toLowerCase()}` : undefined
}

/** The instance's own page. */
export const instanceHref = (jobId: string, instanceId: string): string =>
  `${jobPath(jobId)}/instances/${encodeURIComponent(instanceId)}`

/**
 * The volume id inside a storage handle. A handle is `{plugin}/{volume_id}`;
 * plugin names never contain a slash, so the split is unambiguous.
 */
export const volumeIdFromHandle = (handle: string): string => handle.slice(handle.indexOf('/') + 1)

/**
 * Memory snapshot eligibility for the Take snapshot form: the manager's
 * answer when it sends one, else the spec-derived guess.
 */
export const memorySnapshotReason = (instance: InstanceInfo | undefined, task: JobTask | undefined): string | undefined => {
  const support = instance?.checkpointSupport
  if (support) return support.snapshotMemory ? undefined : support.snapshotMemoryBlocker || 'No memory snapshot for this instance'
  return memorySnapshotBlocker(task)
}

export const diskSnapshotReason = (instance: InstanceInfo | undefined, task: JobTask | undefined): string | undefined => {
  const support = instance?.checkpointSupport
  if (support) return support.snapshotDisk ? undefined : support.snapshotDiskBlocker || 'No disk snapshot for this instance'
  return task && !task.stateful?.storagePath ? 'Disk snapshots need a stateful.storage volume' : undefined
}

/** Whether a memory snapshot of this instance can be published (`--publish`). */
export const publishReason = (instance: InstanceInfo | undefined): string | undefined => {
  const support = instance?.checkpointSupport
  if (!support) return 'This manager does not report whether publishing is possible'
  return support.publish ? undefined : support.publishBlocker || 'This snapshot cannot be published'
}

/** The first line of a manager message, without its `Hint:` line. */
export const firstLine = (message: string): string => message.split('\n')[0] ?? message

/** Fork lineage: origin id → its forks, in id order. */
export const forksByOrigin = (instances: readonly InstanceInfo[]): Map<string, string[]> => {
  const lineage = new Map<string, string[]>()
  for (const instance of instances) {
    if (!instance.forkedFrom) continue
    const forks = lineage.get(instance.forkedFrom) ?? []
    forks.push(instance.instanceId)
    lineage.set(instance.forkedFrom, forks)
  }
  for (const forks of lineage.values()) forks.sort()
  return lineage
}

/**
 * Keep the previous object for every item that did not change, so keyed
 * lists do not remount rows (and disarm a half-confirmed button) on every
 * poll. `equals` is the generated message's own comparison.
 */
export function stableList<T>(key: (item: T) => string, equals: (a: T, b: T) => boolean) {
  let previous = new Map<string, T>()
  return (next: readonly T[]): T[] => {
    const merged = next.map((item) => {
      const before = previous.get(key(item))
      return before !== undefined && equals(before, item) ? before : item
    })
    previous = new Map(merged.map((item) => [key(item), item]))
    return merged
  }
}
