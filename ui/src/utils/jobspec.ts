import { statusTone } from './status'
import type { RolloutAllocationStatus, RolloutGroupStatus, Submission } from '../gen/legion_pb'

export type JobTaskDependency = {
  task: string
  condition?: string
}

// Parsed `stateful` block from a task spec (see docs/stateful-workloads.md).
export type JobTaskStateful = {
  mode: string
  idSource?: string
  storagePath?: string
}

// A declared task artifact, normalized from either the string shorthand or
// the full object form in the job spec.
export type JobTaskArtifact = {
  source: string
  name?: string
  dest?: string
  checksum?: string
  unpack?: string
  resolve?: string
  authLabel?: string
  mediaType?: string
  pinnedFrom?: string
}

// One per-artifact staging state row, decoded from the JSON-encoded
// `artifact_states` payload reported by workers via heartbeat.
export type ArtifactStateRow = {
  name?: string
  source: string
  state: string
  digest?: string
  bytesFetched?: number
  bytesTotal?: number
  attempts?: number
  durationMs?: number
  error?: string
}

export type JobTask = {
  name: string
  kind: string
  lifecycle: string
  group: string
  config?: Record<string, unknown>
  checks?: unknown
  dependsOn: JobTaskDependency[]
  stateful?: JobTaskStateful
  artifacts: JobTaskArtifact[]
}

export type JobSpecData = {
  name?: string
  tasks: JobTask[]
}

export type TaskRuntimeSummary = {
  statuses: string[]
  health: string[]
  healthy?: boolean
  // Full allocation statuses, including each allocation's optional
  // `artifactStates` JSON payload (decode with parseArtifactStates).
  allocations: RolloutAllocationStatus[]
}

const readTaskString = (value: unknown, fallback = ''): string =>
  typeof value === 'string' ? value : fallback

const normalizeDependency = (value: unknown): JobTaskDependency | null => {
  if (!value || typeof value !== 'object') return null
  const item = value as Record<string, unknown>
  const task = readTaskString(item.task)
  if (!task) return null
  return {
    task,
    condition: readTaskString(item.condition) || undefined,
  }
}

const normalizeStateful = (value: unknown): JobTaskStateful | undefined => {
  if (!value || typeof value !== 'object') return undefined
  const raw = value as Record<string, unknown>
  const mode = readTaskString(raw.mode)
  if (!mode) return undefined
  const storage = raw.storage && typeof raw.storage === 'object' ? (raw.storage as Record<string, unknown>) : undefined
  return {
    mode,
    idSource: readTaskString(raw.idSource) || undefined,
    storagePath: storage ? readTaskString(storage.path) || undefined : undefined,
  }
}

// Derives a display label for an artifact's auth: the KV path or identity
// name (never values), suffixed when the auth came from a cluster default.
const deriveArtifactAuthLabel = (auth: unknown, authOrigin: string): string => {
  if (auth && typeof auth === 'object') {
    const item = auth as Record<string, unknown>
    const suffix = authOrigin === 'cluster' ? ' (cluster default)' : ''
    const kv = readTaskString(item.kv)
    if (kv) return `kv: ${kv}${suffix}`
    const identity = readTaskString(item.identity)
    if (identity) return `identity: ${identity}${suffix}`
    const nodeIdentity = readTaskString(item.nodeIdentity)
    if (nodeIdentity) return `nodeIdentity: ${nodeIdentity}${suffix}`
  }
  return 'anonymous'
}

// Artifacts may be declared as a bare source string or a full object.
const normalizeArtifact = (value: unknown): JobTaskArtifact | null => {
  if (typeof value === 'string') {
    return value ? { source: value, authLabel: 'anonymous' } : null
  }
  if (!value || typeof value !== 'object') return null
  const raw = value as Record<string, unknown>
  const source = readTaskString(raw.source)
  if (!source) return null
  return {
    source,
    name: readTaskString(raw.name) || undefined,
    dest: readTaskString(raw.dest) || undefined,
    checksum: readTaskString(raw.checksum) || undefined,
    unpack: readTaskString(raw.unpack) || undefined,
    resolve: readTaskString(raw.resolve) || undefined,
    authLabel: deriveArtifactAuthLabel(raw.auth, readTaskString(raw.authOrigin)),
    mediaType: readTaskString(raw.mediaType) || undefined,
    pinnedFrom: readTaskString(raw.pinnedFrom) || undefined,
  }
}

const readStateNumber = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

const normalizeArtifactStateRow = (value: unknown): ArtifactStateRow | null => {
  if (!value || typeof value !== 'object') return null
  const raw = value as Record<string, unknown>
  const source = readTaskString(raw.source)
  const state = readTaskString(raw.state)
  if (!source && !state) return null
  return {
    name: readTaskString(raw.name) || undefined,
    source,
    state,
    digest: readTaskString(raw.digest) || undefined,
    bytesFetched: readStateNumber(raw.bytesFetched),
    bytesTotal: readStateNumber(raw.bytesTotal),
    attempts: readStateNumber(raw.attempts),
    durationMs: readStateNumber(raw.durationMs),
    error: readTaskString(raw.error) || undefined,
  }
}

// Safely decodes the JSON-encoded `artifact_states` payload carried on
// allocation statuses (heartbeat -> manager -> rollout status/ledger RPCs).
export const parseArtifactStates = (raw: string | undefined): ArtifactStateRow[] => {
  if (!raw) return []
  try {
    const parsed = JSON.parse(raw) as unknown
    if (!Array.isArray(parsed)) return []
    return parsed
      .map((row) => normalizeArtifactStateRow(row))
      .filter((row): row is ArtifactStateRow => Boolean(row))
  } catch {
    return []
  }
}

// Percent complete for an artifact fetch, when both byte counters are known.
export const artifactProgressPercent = (row: ArtifactStateRow): number | undefined => {
  if (row.bytesFetched === undefined || row.bytesTotal === undefined || row.bytesTotal <= 0) return undefined
  return Math.min(100, Math.round((row.bytesFetched / row.bytesTotal) * 100))
}

// Shortens "sha256:<64 hex>" digests to "sha256:abcdef12…" for display.
export const shortenArtifactDigest = (digest: string): string => {
  const separator = digest.indexOf(':')
  const prefix = separator >= 0 ? digest.slice(0, separator + 1) : ''
  const hex = separator >= 0 ? digest.slice(separator + 1) : digest
  if (hex.length <= 12) return digest
  return `${prefix}${hex.slice(0, 8)}…`
}

const normalizeTask = (value: unknown): JobTask | null => {
  if (!value || typeof value !== 'object') return null
  const raw = value as Record<string, unknown>
  const name = readTaskString(raw.name)
  if (!name) return null

  const dependsOnInput = Array.isArray(raw.dependsOn) ? raw.dependsOn : []
  const dependsOn = dependsOnInput
    .map((dependency) => normalizeDependency(dependency))
    .filter((dependency): dependency is JobTaskDependency => Boolean(dependency))

  const artifactsInput = Array.isArray(raw.artifacts) ? raw.artifacts : []
  const artifacts = artifactsInput
    .map((artifact) => normalizeArtifact(artifact))
    .filter((artifact): artifact is JobTaskArtifact => Boolean(artifact))

  return {
    name,
    kind: readTaskString(raw.kind, 'unknown'),
    lifecycle: readTaskString(raw.lifecycle, 'service'),
    group: readTaskString(raw.group, 'default'),
    config: raw.config && typeof raw.config === 'object' ? (raw.config as Record<string, unknown>) : undefined,
    checks: raw.checks,
    dependsOn,
    stateful: normalizeStateful(raw.stateful),
    artifacts,
  }
}

const parseSubmissionContent = (content: string): JobSpecData | null => {
  try {
    const parsed = JSON.parse(content) as unknown
    if (!parsed || typeof parsed !== 'object') return null
    const raw = parsed as Record<string, unknown>
    const tasksRaw = Array.isArray(raw.tasks) ? raw.tasks : []
    const tasks = tasksRaw
      .map((task) => normalizeTask(task))
      .filter((task): task is JobTask => Boolean(task))

    return {
      name: readTaskString(raw.name) || undefined,
      tasks,
    }
  } catch {
    return null
  }
}

export const extractJobSpecData = (submissions: readonly Submission[]): JobSpecData | null => {
  for (const submission of submissions) {
    const parsed = parseSubmissionContent(submission.content)
    if (parsed) return parsed
  }
  return null
}

export const buildTaskRuntimeByName = (
  groups: readonly RolloutGroupStatus[],
): Map<string, TaskRuntimeSummary> => {
  const runtime = new Map<string, TaskRuntimeSummary>()

  for (const group of groups) {
    for (const allocation of group.allocations) {
      const existing = runtime.get(allocation.taskName)
      if (!existing) {
        runtime.set(allocation.taskName, {
          statuses: allocation.status ? [allocation.status] : [],
          health: allocation.allocationHealth ? [allocation.allocationHealth] : [],
          healthy: allocation.allocationHealthy,
          allocations: [allocation],
        })
        continue
      }

      if (allocation.status && !existing.statuses.includes(allocation.status)) {
        existing.statuses.push(allocation.status)
      }
      if (allocation.allocationHealth && !existing.health.includes(allocation.allocationHealth)) {
        existing.health.push(allocation.allocationHealth)
      }
      if (existing.healthy === undefined) {
        existing.healthy = allocation.allocationHealthy
      } else if (allocation.allocationHealthy !== undefined && allocation.allocationHealthy === false) {
        existing.healthy = false
      }
      existing.allocations.push(allocation)
    }
  }

  return runtime
}

// Builds a runtime summary map from a plain task-name -> status map. Used to
// drive live node coloring in the DAG from the manager state store, which is
// authoritative even for jobs that were deployed without a rollout.
export const buildTaskRuntimeFromStatuses = (
  statusByName: ReadonlyMap<string, string>,
): Map<string, TaskRuntimeSummary> => {
  const runtime = new Map<string, TaskRuntimeSummary>()
  for (const [name, status] of statusByName) {
    runtime.set(name, {
      statuses: status ? [status] : [],
      health: [],
      healthy: undefined,
      allocations: [],
    })
  }
  return runtime
}

export const topoSortTaskNames = (tasks: readonly JobTask[]): string[][] => {
  const indegree = new Map<string, number>()
  const dependents = new Map<string, string[]>()

  for (const task of tasks) {
    indegree.set(task.name, 0)
    dependents.set(task.name, [])
  }

  for (const task of tasks) {
    for (const dependency of task.dependsOn) {
      if (!indegree.has(dependency.task)) continue
      indegree.set(task.name, (indegree.get(task.name) ?? 0) + 1)
      dependents.get(dependency.task)?.push(task.name)
    }
  }

  const queue = [...tasks.filter((task) => (indegree.get(task.name) ?? 0) === 0).map((task) => task.name)]
  const layers: string[][] = []
  const visited = new Set<string>()

  while (queue.length > 0) {
    const layerSize = queue.length
    const layer: string[] = []

    for (let i = 0; i < layerSize; i += 1) {
      const current = queue.shift()
      if (!current || visited.has(current)) continue
      visited.add(current)
      layer.push(current)

      for (const dependent of dependents.get(current) ?? []) {
        const nextInDegree = (indegree.get(dependent) ?? 0) - 1
        indegree.set(dependent, nextInDegree)
        if (nextInDegree === 0) queue.push(dependent)
      }
    }

    if (layer.length > 0) layers.push(layer)
  }

  const unvisited = tasks.map((task) => task.name).filter((name) => !visited.has(name))
  if (unvisited.length > 0) layers.push(unvisited)

  return layers
}

// DAG node tone, derived from the central status vocabulary so the graph
// always agrees with the status badges elsewhere on the page. The DAG only
// distinguishes four states, so warn and info both render as "in progress".
export const taskStatusTone = (status: string): 'success' | 'warning' | 'danger' | 'neutral' => {
  const tone = statusTone(status)
  if (tone === 'ok') return 'success'
  if (tone === 'danger') return 'danger'
  if (tone === 'warn' || tone === 'info') return 'warning'
  return 'neutral'
}
