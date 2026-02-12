import { createMemo, createResource } from 'solid-js'
import EntityIndex, { type IndexItem } from '../components/EntityIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'

export default function Nodes() {
  const [nodes] = createResource(async () => {
    const response = await managerClient.getNodes({})
    return response.nodes
  })

  const items = createMemo<IndexItem[]>(() =>
    (nodes() ?? []).map((node) => {
      const health = summarizeHealthChecks(node.healthChecks).label
      return {
        id: node.id,
        name: node.id || 'unknown',
        health,
        updatedAt: formatRelativeTime(node.lastHeartbeat),
        searchText: [node.id, node.status, node.pool, health].join(' '),
        href: `/nodes/${encodeURIComponent(node.id)}`,
      }
    }),
  )

  return (
    <EntityIndex
      title="Nodes"
      description="Worker nodes with health and heartbeat visibility."
      items={items()}
      loading={nodes.loading}
      error={nodes.error}
      sortOptions={[
        { key: 'name', label: 'Name' },
        { key: 'health', label: 'Health' },
        { key: 'updatedAt', label: 'Updated' },
      ]}
    />
  )
}
