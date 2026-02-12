import EntityIndex from './EntityIndex'
import { fetchNodesIndexPage } from './indexData'

const SORT_OPTIONS = [
  { value: 'updatedAt', label: 'Updated' },
  { value: 'name', label: 'Name' },
  { value: 'health', label: 'Health' },
] as const

export default function Nodes() {
  return (
    <EntityIndex
      title="Nodes"
      description="Browse worker nodes by name, health, and latest heartbeat."
      emptyMessage="No nodes discovered yet."
      sortOptions={SORT_OPTIONS}
      fetchPage={fetchNodesIndexPage}
    />
  )
}
