import { createMemo, createResource } from 'solid-js'
import EntityIndex, { type IndexItem } from '../components/EntityIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'

export default function Jobs() {
  const [jobs] = createResource(async () => {
    const response = await managerClient.listJobs({})
    return response.jobs
  })

  const items = createMemo<IndexItem[]>(() =>
    (jobs() ?? []).map((job) => ({
      id: job.id,
      name: job.name || job.id,
      health: job.status || 'unknown',
      updatedAt: formatRelativeTime(job.createdAt),
      searchText: [job.id, job.name, job.status].join(' '),
      href: `/jobs/${encodeURIComponent(job.name || job.id)}`,
    })),
  )

  return (
    <EntityIndex
      title="Jobs"
      description="Cluster jobs with focused status details."
      items={items()}
      loading={jobs.loading}
      error={jobs.error}
      sortOptions={[
        { key: 'name', label: 'Name' },
        { key: 'health', label: 'Health' },
        { key: 'updatedAt', label: 'Updated' },
      ]}
    />
  )
}
