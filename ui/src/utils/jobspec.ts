import { statusTone } from './status'
import type { RolloutAllocationStatus, RolloutGroupStatus, Submission } from '../gen/legion_pb'

export type JobTaskDependency = {
  task: string
  condition?: string
}

// Parsed `stateful` block from a task spec (see docs/stateful-workloads.md).
export type JobTaskStateful = {
  mode: string
  requireId?: boolean
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

// One declared health/readiness check (see docs/service-discovery.md). A
// check's `port` may be a literal number or a `network.ports` label.
export type JobTaskCheck = {
  type: string
  kind: string
  name?: string
  path?: string
  port?: string
  every?: string
  timeout?: string
  unhealthyAfterCount?: number
}

// A declared port from the task's `network.ports`.
export type JobTaskPort = {
  name: string
  staticPort?: number
  to?: number
}

// The task's `service` block: what it advertises to discovery.
export type JobTaskService = {
  name?: string
  port: string
  tags: string[]
  meta: Record<string, string>
}

// The task's `count`: a plain number, or `{ num, per }` — `num` instances per
// distinct value of a node attribute, so the total depends on the cluster.
export type JobTaskCount = {
  num: number
  per?: string
}

// The task's `resources` block, in the units the jobspec uses: `cpus` in
// cores, `memory` and `memoryMax` in MiB (docs/resource-enforcement.md).
// Every field is optional because the manager fills defaults at submit.
export type JobTaskResources = {
  cpus?: number
  memory?: number
  memoryMax?: number
}

// The task's `snapshots` block (docs/suspend-and-resume.md). Its presence is
// what opts a sandbox into memory snapshots, so the block itself matters, not
// only its fields.
export type JobTaskSnapshots = {
  ttl?: string
  keep?: number
  memory: boolean
}

export type JobTask = {
  name: string
  count?: JobTaskCount
  runtime: string
  lifecycle: string
  group: string
  config?: Record<string, unknown>
  checks: JobTaskCheck[]
  dependsOn: JobTaskDependency[]
  stateful?: JobTaskStateful
  artifacts: JobTaskArtifact[]
  networkMode?: string
  ports: JobTaskPort[]
  service?: JobTaskService
  resources?: JobTaskResources
  snapshots?: JobTaskSnapshots
  actions: JobTaskAction[]
}

// What the jobspec declares about an action beyond `ListJobActions`: the argv
// template the manager renders (`${param.<name>}` placeholders) and the
// optional wall-clock cap. Shown as a preview; the manager does the rendering.
export type JobTaskAction = {
  name: string
  command: string[]
  timeout?: string
}

export type JobSpecData = {
  name?: string
  tasks: JobTask[]
}

export type TaskInstanceSummary = {
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
    requireId: raw.requireId === true || undefined,
    storagePath: storage ? readTaskString(storage.path) || undefined : undefined,
  }
}

const readNumber = (value: unknown): number | undefined =>
  typeof value === 'number' && Number.isFinite(value) ? value : undefined

const normalizeResources = (value: unknown): JobTaskResources | undefined => {
  if (!value || typeof value !== 'object') return undefined
  const raw = value as Record<string, unknown>
  const resources = {
    cpus: readNumber(raw.cpus),
    memory: readNumber(raw.memory),
    memoryMax: readNumber(raw.memoryMax),
  }
  return Object.values(resources).some((entry) => entry !== undefined) ? resources : undefined
}

const normalizeSnapshots = (value: unknown): JobTaskSnapshots | undefined => {
  if (!value || typeof value !== 'object') return undefined
  const raw = value as Record<string, unknown>
  return {
    ttl: readTaskString(raw.ttl) || undefined,
    keep: readNumber(raw.keep),
    // `memory` defaults to true once the block is declared.
    memory: raw.memory !== false,
  }
}

/**
 * Whether the spec lets this task take a memory snapshot: a `sandbox` with a
 * `snapshots` block that does not turn memory off, and no gated networking.
 * Returns the reason when it cannot. The manager is the authority (a stored
 * network policy can gate a task the spec does not), so this only decides
 * what the UI offers.
 */
export const memorySnapshotBlocker = (task: JobTask | undefined): string | undefined => {
  if (!task) return 'The task is not in the current job spec'
  if (task.runtime !== 'sandbox') return `Memory snapshots need runtime "sandbox"; this task runs "${task.runtime}"`
  if (!task.snapshots) return 'Memory snapshots need a `snapshots` block on the task'
  if (!task.snapshots.memory) return 'The task sets snapshots.memory = false'
  if (task.networkMode === 'gated') return 'Memory snapshots do not support gated networking yet'
  return undefined
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

// Specs are hand-written Nickel, so accept both the camelCase the manager
// serializes and the snake_case people actually type.
const readEither = (raw: Record<string, unknown>, camel: string, snake: string): unknown =>
  raw[camel] ?? raw[snake]

const normalizeCheck = (value: unknown): JobTaskCheck | null => {
  if (!value || typeof value !== 'object') return null
  const raw = value as Record<string, unknown>
  const kind = readTaskString(raw.kind)
  if (!kind) return null
  const port = raw.port
  const count = readEither(raw, 'unhealthyAfterCount', 'unhealthy_after_count')
  return {
    type: readTaskString(raw.type, 'health'),
    kind,
    name: readTaskString(raw.name) || undefined,
    path: readTaskString(raw.path) || undefined,
    port: typeof port === 'number' ? String(port) : readTaskString(port) || undefined,
    every: readTaskString(raw.every) || undefined,
    timeout: readTaskString(raw.timeout) || undefined,
    unhealthyAfterCount: typeof count === 'number' ? count : undefined,
  }
}

const normalizePort = (value: unknown): JobTaskPort | null => {
  if (!value || typeof value !== 'object') return null
  const raw = value as Record<string, unknown>
  const name = readTaskString(raw.name)
  if (!name) return null
  const staticPort = readEither(raw, 'static', 'staticPort')
  return {
    name,
    staticPort: typeof staticPort === 'number' ? staticPort : undefined,
    to: typeof raw.to === 'number' ? raw.to : undefined,
  }
}

const normalizeService = (value: unknown): JobTaskService | undefined => {
  if (!value || typeof value !== 'object') return undefined
  const raw = value as Record<string, unknown>
  const port = typeof raw.port === 'number' ? String(raw.port) : readTaskString(raw.port)
  const meta: Record<string, string> = {}
  if (raw.meta && typeof raw.meta === 'object') {
    for (const [key, item] of Object.entries(raw.meta as Record<string, unknown>)) {
      meta[key] = typeof item === 'string' ? item : JSON.stringify(item)
    }
  }
  return {
    name: readTaskString(raw.name) || undefined,
    port,
    tags: Array.isArray(raw.tags) ? raw.tags.map((tag) => String(tag)) : [],
    meta,
  }
}

const normalizeCount = (value: unknown): JobTaskCount | undefined => {
  if (typeof value === 'number' && Number.isFinite(value)) return { num: value }
  if (!value || typeof value !== 'object') return undefined
  const raw = value as Record<string, unknown>
  if (typeof raw.num !== 'number' || !Number.isFinite(raw.num)) return undefined
  return { num: raw.num, per: readTaskString(raw.per) || undefined }
}

const normalizeAction = (value: unknown): JobTaskAction | null => {
  if (!value || typeof value !== 'object') return null
  const raw = value as Record<string, unknown>
  const name = readTaskString(raw.name)
  if (!name || !Array.isArray(raw.command)) return null
  return {
    name,
    command: raw.command.map((part) => String(part)),
    timeout: readTaskString(raw.timeout) || undefined,
  }
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

  const checksInput = Array.isArray(raw.checks) ? raw.checks : []
  const checks = checksInput
    .map((check) => normalizeCheck(check))
    .filter((check): check is JobTaskCheck => Boolean(check))

  const network = raw.network && typeof raw.network === 'object' ? (raw.network as Record<string, unknown>) : undefined
  const portsInput = network && Array.isArray(network.ports) ? network.ports : []
  const ports = portsInput
    .map((port) => normalizePort(port))
    .filter((port): port is JobTaskPort => Boolean(port))

  return {
    name,
    count: normalizeCount(raw.count),
    runtime: readTaskString(raw.runtime, 'unknown'),
    lifecycle: readTaskString(raw.lifecycle, 'service'),
    group: readTaskString(raw.group, 'default'),
    config: raw.config && typeof raw.config === 'object' ? (raw.config as Record<string, unknown>) : undefined,
    checks,
    dependsOn,
    stateful: normalizeStateful(raw.stateful),
    artifacts,
    networkMode: network ? readTaskString(network.mode) || undefined : undefined,
    ports,
    service: normalizeService(raw.service),
    resources: normalizeResources(raw.resources),
    snapshots: normalizeSnapshots(raw.snapshots),
    actions: (Array.isArray(raw.actions) ? raw.actions : [])
      .map((action) => normalizeAction(action))
      .filter((action): action is JobTaskAction => Boolean(action)),
  }
}

// Every submission gets the rendered spec under this path (submit_rendered_job
// in src/manager/server.rs); it is the one submission that is always the spec.
export const RENDERED_SPEC_SUBMISSION = 'job.rendered.json'

const parseSubmissionContent = (content: string): JobSpecData | null => {
  try {
    const parsed = JSON.parse(content) as unknown
    if (!parsed || typeof parsed !== 'object') return null
    const raw = parsed as Record<string, unknown>
    // Other JSON rides along with a spec — a template run records its
    // variables as `template.variables.json` — and is not a spec.
    if (!Array.isArray(raw.tasks)) return null
    const tasksRaw = raw.tasks
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

// Submissions come back in no particular order (the store query has no ORDER
// BY), so the rendered spec is preferred by name rather than by position.
export const extractJobSpecData = (submissions: readonly Submission[]): JobSpecData | null => {
  const rendered = submissions.find((submission) => submission.path === RENDERED_SPEC_SUBMISSION)
  const fromRendered = rendered ? parseSubmissionContent(rendered.content) : null
  if (fromRendered) return fromRendered
  for (const submission of submissions) {
    const parsed = parseSubmissionContent(submission.content)
    if (parsed) return parsed
  }
  return null
}

// The job's structure from a GetJobSpec/GetJobSpecByConfig reply. The
// manager's `renderedJobspec` is authoritative whatever the job was written
// in; submissions are the source as typed (usually Nickel, which is not
// JSON), so they only serve when a reply carries no rendered spec.
export const extractJobSpecFromReply = (reply: {
  renderedJobspec: string
  submissions: readonly Submission[]
}): JobSpecData | null =>
  (reply.renderedJobspec ? parseSubmissionContent(reply.renderedJobspec) : null) ?? extractJobSpecData(reply.submissions)

export const buildTaskInstancesByName = (
  groups: readonly RolloutGroupStatus[],
): Map<string, TaskInstanceSummary> => {
  const instances = new Map<string, TaskInstanceSummary>()

  for (const group of groups) {
    for (const allocation of group.allocations) {
      const existing = instances.get(allocation.taskName)
      if (!existing) {
        instances.set(allocation.taskName, {
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

  return instances
}

// Builds a per-task instance summary map from a plain task-name -> status map. Used to
// drive live node coloring in the DAG from the manager state store, which is
// authoritative even for jobs that were deployed without a rollout.
export const buildTaskInstancesFromStatuses = (
  statusByName: ReadonlyMap<string, string>,
): Map<string, TaskInstanceSummary> => {
  const instances = new Map<string, TaskInstanceSummary>()
  for (const [name, status] of statusByName) {
    instances.set(name, {
      statuses: status ? [status] : [],
      health: [],
      healthy: undefined,
      allocations: [],
    })
  }
  return instances
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
