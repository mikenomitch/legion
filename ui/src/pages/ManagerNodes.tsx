import { createMemo, createResource } from 'solid-js'
import EntityIndex, { type IndexItem } from '../components/EntityIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'

export default function ManagerNodes() {
  const [configs] = createResource(async () => {
    const response = await managerClient.getManagerConfigs({})
    return response.configs
  })

  const items = createMemo<IndexItem[]>(() =>
    (configs() ?? []).map((config) => ({
      id: config.nodeId,
      name: config.nodeId,
      health: 'healthy',
      updatedAt: formatRelativeTime(config.updatedAt),
      searchText: [config.nodeId, config.updatedAt].join(' '),
      href: `/manager-nodes/${encodeURIComponent(config.nodeId)}`,
    })),
  )

  return (
    <EntityIndex
      title="Manager Nodes"
      description="Manager node configuration snapshots."
      items={items()}
      loading={configs.loading}
      error={configs.error}
      sortOptions={[
        { key: 'name', label: 'Name' },
        { key: 'health', label: 'Health' },
        { key: 'updatedAt', label: 'Updated' },
      ]}
    />
  )
}
