import type { RolloutAllocationStatus, RolloutGroupStatus, Submission } from '../gen/legion_pb'

export type JobTaskDependency = {
  task: string
  condition?: string
}

export type JobTask = {
  name: string
  kind: string
  lifecycle: string
  group: string
  config?: Record<string, unknown>
  checks?: unknown
  dependsOn: JobTaskDependency[]
}

export type JobSpecData = {
  name?: string
  tasks: JobTask[]
}

export type TaskRuntimeSummary = {
  statuses: string[]
  health: string[]
  healthy?: boolean
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

const normalizeTask = (value: unknown): JobTask | null => {
  if (!value || typeof value !== 'object') return null
  const raw = value as Record<string, unknown>
  const name = readTaskString(raw.name)
  if (!name) return null

  const dependsOnInput = Array.isArray(raw.dependsOn) ? raw.dependsOn : []
  const dependsOn = dependsOnInput
    .map((dependency) => normalizeDependency(dependency))
    .filter((dependency): dependency is JobTaskDependency => Boolean(dependency))

  return {
    name,
    kind: readTaskString(raw.kind, 'unknown'),
    lifecycle: readTaskString(raw.lifecycle, 'service'),
    group: readTaskString(raw.group, 'default'),
    config: raw.config && typeof raw.config === 'object' ? (raw.config as Record<string, unknown>) : undefined,
    checks: raw.checks,
    dependsOn,
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

export const groupTasksByName = (tasks: readonly JobTask[]): Map<string, JobTask> =>
  new Map(tasks.map((task) => [task.name, task]))

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

export const taskStatusTone = (status: string): 'success' | 'warning' | 'danger' | 'neutral' => {
  const normalized = status.toLowerCase()
  if (['running', 'healthy', 'completed', 'success'].some((item) => normalized.includes(item))) return 'success'
  if (['failed', 'error', 'stopped', 'lost'].some((item) => normalized.includes(item))) return 'danger'
  if (['pending', 'starting', 'queued'].some((item) => normalized.includes(item))) return 'warning'
  return 'neutral'
}
