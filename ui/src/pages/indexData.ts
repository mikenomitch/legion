import type { Job, ManagerNodeConfig, NodeMsg, Rollout } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import type { EntityIndexItem, EntityIndexQuery, SortDirection } from './EntityIndex'

export interface DeploymentRecord {
  id: string
  jobId: string
  status: string
  updatedAt: string
}

const applySearch = <T,>(items: readonly T[], search: string, read: (item: T) => string): readonly T[] => {
  const normalized = search.trim().toLowerCase()
  if (!normalized) return items
  return items.filter((item) => read(item).toLowerCase().includes(normalized))
}

const compareValues = (a: string, b: string, direction: SortDirection) =>
  direction === 'asc' ? a.localeCompare(b) : b.localeCompare(a)

const paginate = <T,>(items: readonly T[], page: number, pageSize: number): readonly T[] => {
  const start = (page - 1) * pageSize
  return items.slice(start, start + pageSize)
}

const sortByUpdated = <T extends { updatedAt: string }>(items: readonly T[], direction: SortDirection): readonly T[] =>
  [...items].sort((a, b) => compareValues(a.updatedAt, b.updatedAt, direction))

export const fetchNodesIndexPage = async (query: EntityIndexQuery): Promise<{ items: readonly EntityIndexItem[]; total: number }> => {
  const response = await managerClient.getNodes({})
  const items = response.nodes.map((node) => ({
    id: node.id,
    name: node.id,
    health: node.status || 'unknown',
    updatedAt: node.lastHeartbeat || '',
    subtitle: node.pool ? `Pool: ${node.pool}` : undefined,
    href: `/nodes/${encodeURIComponent(node.id)}`,
  }))

  const searched = applySearch(items, query.search, (item) => `${item.name} ${item.subtitle ?? ''} ${item.health}`)
  const sorted = [...searched].sort((a, b) => {
    if (query.sortBy === 'name') return compareValues(a.name, b.name, query.sortDirection)
    if (query.sortBy === 'health') return compareValues(a.health, b.health, query.sortDirection)
    return compareValues(a.updatedAt, b.updatedAt, query.sortDirection)
  })
  return { items: paginate(sorted, query.page, query.pageSize), total: searched.length }
}

export const fetchJobsIndexPage = async (query: EntityIndexQuery): Promise<{ items: readonly EntityIndexItem[]; total: number }> => {
  const response = await managerClient.listJobs({})
  const items = response.jobs.map((job) => ({
    id: job.id,
    name: job.name || job.id,
    health: job.status || 'unknown',
    updatedAt: String(job.createdAt || ''),
    subtitle: `ID: ${job.id}`,
    href: `/jobs/${encodeURIComponent(job.name || job.id)}`,
  }))

  const searched = applySearch(items, query.search, (item) => `${item.name} ${item.subtitle ?? ''} ${item.health}`)
  const sorted = [...searched].sort((a, b) => {
    if (query.sortBy === 'name') return compareValues(a.name, b.name, query.sortDirection)
    if (query.sortBy === 'health') return compareValues(a.health, b.health, query.sortDirection)
    return compareValues(a.updatedAt, b.updatedAt, query.sortDirection)
  })
  return { items: paginate(sorted, query.page, query.pageSize), total: searched.length }
}

export const fetchRolloutsIndexPage = async (query: EntityIndexQuery): Promise<{ items: readonly EntityIndexItem[]; total: number }> => {
  const response = await managerClient.listRollouts({ activeOnly: false })
  const items = response.rollouts.map((rollout) => ({
    id: String(rollout.id),
    name: `#${String(rollout.id)} · ${rollout.jobId}`,
    health: rollout.status || 'unknown',
    updatedAt: rollout.updatedAt,
    subtitle: rollout.toJobConfigId,
    href: `/rollouts/${String(rollout.id)}`,
  }))

  const searched = applySearch(items, query.search, (item) => `${item.name} ${item.subtitle ?? ''} ${item.health}`)
  const sorted = [...searched].sort((a, b) => {
    if (query.sortBy === 'name') return compareValues(a.name, b.name, query.sortDirection)
    if (query.sortBy === 'health') return compareValues(a.health, b.health, query.sortDirection)
    return compareValues(a.updatedAt, b.updatedAt, query.sortDirection)
  })
  return { items: paginate(sorted, query.page, query.pageSize), total: searched.length }
}

export const fetchManagerNodesIndexPage = async (query: EntityIndexQuery): Promise<{ items: readonly EntityIndexItem[]; total: number }> => {
  const response = await managerClient.getManagerConfigs({})
  const items = response.configs.map((config) => ({
    id: config.nodeId,
    name: config.nodeId,
    health: 'healthy',
    updatedAt: config.updatedAt,
    subtitle: 'Manager config snapshot',
    href: `/manager/${encodeURIComponent(config.nodeId)}`,
  }))

  const searched = applySearch(items, query.search, (item) => `${item.name} ${item.subtitle ?? ''}`)
  const sorted = [...searched].sort((a, b) => {
    if (query.sortBy === 'name') return compareValues(a.name, b.name, query.sortDirection)
    return compareValues(a.updatedAt, b.updatedAt, query.sortDirection)
  })
  return { items: paginate(sorted, query.page, query.pageSize), total: searched.length }
}

const toDeploymentRecords = (rollouts: readonly Rollout[]): readonly DeploymentRecord[] => {
  const byConfig = new Map<string, DeploymentRecord>()

  for (const rollout of sortByUpdated(
    rollouts.map((item) => ({ id: String(item.id), jobId: item.jobId, status: item.status, updatedAt: item.updatedAt, toJobConfigId: item.toJobConfigId })),
    'desc',
  )) {
    if (byConfig.has(rollout.toJobConfigId)) continue
    byConfig.set(rollout.toJobConfigId, {
      id: rollout.toJobConfigId,
      jobId: rollout.jobId,
      status: rollout.status || 'unknown',
      updatedAt: rollout.updatedAt,
    })
  }

  return Array.from(byConfig.values())
}

export const fetchDeploymentsIndexPage = async (query: EntityIndexQuery): Promise<{ items: readonly EntityIndexItem[]; total: number }> => {
  const response = await managerClient.listRollouts({ activeOnly: false })
  const deployments = toDeploymentRecords(response.rollouts)

  const items = deployments.map((deployment) => ({
    id: deployment.id,
    name: deployment.id,
    health: deployment.status,
    updatedAt: deployment.updatedAt,
    subtitle: `Job: ${deployment.jobId}`,
    href: `/deployments/${encodeURIComponent(deployment.id)}`,
  }))

  const searched = applySearch(items, query.search, (item) => `${item.name} ${item.subtitle ?? ''} ${item.health}`)
  const sorted = [...searched].sort((a, b) => {
    if (query.sortBy === 'name') return compareValues(a.name, b.name, query.sortDirection)
    if (query.sortBy === 'health') return compareValues(a.health, b.health, query.sortDirection)
    return compareValues(a.updatedAt, b.updatedAt, query.sortDirection)
  })

  return { items: paginate(sorted, query.page, query.pageSize), total: searched.length }
}

export const fetchJob = async (id: string): Promise<Job | undefined> => {
  const response = await managerClient.getJobInfo({ name: id })
  return response.job
}

export const fetchNode = async (id: string): Promise<NodeMsg | undefined> => {
  const response = await managerClient.getNode({ id })
  return response.node
}

export const fetchManagerNodeConfig = async (id: string): Promise<ManagerNodeConfig | undefined> => {
  const response = await managerClient.getManagerConfigs({})
  return response.configs.find((config) => config.nodeId === id)
}

export const fetchRollout = async (id: bigint): Promise<Rollout | undefined> => {
  const response = await managerClient.getRollout({ id })
  return response.rollout
}
