import EntityIndex from './EntityIndex'
import { fetchJobsIndexPage } from './indexData'

const SORT_OPTIONS = [
  { value: 'updatedAt', label: 'Created At' },
  { value: 'name', label: 'Name' },
  { value: 'health', label: 'Status' },
] as const

export default function Jobs() {
  return (
    <EntityIndex
      title="Jobs"
      description="Browse jobs with fast search, sorting, and API-backed pagination."
      emptyMessage="No jobs reported."
      sortOptions={SORT_OPTIONS}
      fetchPage={fetchJobsIndexPage}
    />
  )
}
