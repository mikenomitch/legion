import EntityIndex from './EntityIndex'
import { fetchManagerNodesIndexPage } from './indexData'

const SORT_OPTIONS = [
  { value: 'updatedAt', label: 'Updated At' },
  { value: 'name', label: 'Node Name' },
] as const

export default function Manager() {
  return (
    <EntityIndex
      title="Manager Nodes"
      description="Manager config snapshots by node."
      emptyMessage="No manager configs reported yet."
      sortOptions={SORT_OPTIONS}
      fetchPage={fetchManagerNodesIndexPage}
    />
  )
}
