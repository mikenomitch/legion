import EntityIndex from './EntityIndex'
import { fetchDeploymentsIndexPage } from './indexData'

const SORT_OPTIONS = [
  { value: 'updatedAt', label: 'Updated At' },
  { value: 'name', label: 'Deployment Name' },
  { value: 'health', label: 'Status' },
] as const

export default function Deployments() {
  return (
    <EntityIndex
      title="Deployments"
      description="Deployment history inferred from rollout targets."
      emptyMessage="No deployments found."
      sortOptions={SORT_OPTIONS}
      fetchPage={fetchDeploymentsIndexPage}
    />
  )
}
