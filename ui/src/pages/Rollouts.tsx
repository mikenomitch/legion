import EntityIndex from './EntityIndex'
import { fetchRolloutsIndexPage } from './indexData'

const SORT_OPTIONS = [
  { value: 'updatedAt', label: 'Updated At' },
  { value: 'name', label: 'Name' },
  { value: 'health', label: 'Status' },
] as const

export default function Rollouts() {
  return (
    <EntityIndex
      title="Rollouts"
      description="Track rollout executions with sort/search and pagination controls."
      emptyMessage="No rollouts found."
      sortOptions={SORT_OPTIONS}
      fetchPage={fetchRolloutsIndexPage}
    />
  )
}
