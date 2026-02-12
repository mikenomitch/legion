import { createMemo, createResource } from 'solid-js'
import EntityIndex, { type IndexItem } from '../components/EntityIndex'
import type { Rollout } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'

export default function Deployments() {
  const [rollouts] = createResource(async () => {
    const response = await managerClient.listRollouts({ activeOnly: false })
    return response.rollouts
  })

  const items = createMemo<IndexItem[]>(() => {
    const latestByDeployment = new Map<string, Rollout>()
    for (const rollout of rollouts() ?? []) {
      const key = `${rollout.jobId}:${rollout.toJobConfigId}`
      const existing = latestByDeployment.get(key)
      if (!existing || rollout.updatedAt > existing.updatedAt) {
        latestByDeployment.set(key, rollout)
      }
    }
    return Array.from(latestByDeployment.values()).map((rollout) => ({
      id: `${rollout.jobId}:${rollout.toJobConfigId}`,
      name: `${rollout.jobId} / ${rollout.toJobConfigId}`,
      health: rollout.status,
      updatedAt: formatRelativeTime(rollout.updatedAt),
      searchText: [rollout.jobId, rollout.toJobConfigId, rollout.status].join(' '),
      href: `/deployments/${encodeURIComponent(rollout.jobId)}:${encodeURIComponent(rollout.toJobConfigId)}`,
    }))
  })

  return (
    <EntityIndex
      title="Deployments"
      description="Deployment targets grouped by job and config ID."
      items={items()}
      loading={rollouts.loading}
      error={rollouts.error}
      sortOptions={[
        { key: 'name', label: 'Name' },
        { key: 'health', label: 'Health' },
        { key: 'updatedAt', label: 'Updated' },
      ]}
    />
  )
}
