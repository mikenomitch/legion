import { createMemo, createResource } from 'solid-js'
import EntityIndex, { type IndexItem } from '../components/EntityIndex'
import { managerClient } from '../grpcClient'
import { asDisplay, formatRelativeTime } from '../utils/format'

export default function Rollouts() {
  const [rollouts] = createResource(async () => {
    const response = await managerClient.listRollouts({ activeOnly: false })
    return response.rollouts
  })

  const items = createMemo<IndexItem[]>(() =>
    (rollouts() ?? []).map((rollout) => ({
      id: String(rollout.id),
      name: `#${asDisplay(rollout.id)} ${rollout.jobId}`,
      health: rollout.status,
      updatedAt: formatRelativeTime(rollout.updatedAt),
      searchText: [rollout.id.toString(), rollout.jobId, rollout.status, rollout.toJobConfigId].join(' '),
      href: `/rollouts/${rollout.id}`,
    })),
  )

  return (
    <EntityIndex
      title="Rollouts"
      description="Rollout execution stream across all jobs."
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
