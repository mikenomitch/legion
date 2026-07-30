import { createEffect, createMemo, createResource, createSignal } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import SharedIndex from '../components/SharedIndex'
import { managerClient } from '../grpcClient'
import { debounced } from '../utils/debounce'
import { formatRelativeTime } from '../utils/format'

function JobsContent() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const searchQuery = debounced(search)

  createEffect(() => {
    search()
    sort()
    setPage(1)
  })

  // Allocation counts are garnish needing node:read; a job-scoped token
  // (job:read only) must still get the list, so denial degrades to empty.
  const [ledger] = createResource(async () => {
    try {
      return (await managerClient.getLedger({})).nodes
    } catch {
      return []
    }
  })
  const jobAllocationCounts = createMemo(() => {
    const map = new Map<string, number>()
    for (const node of ledger() ?? []) {
      for (const allocation of node.allocations) {
        map.set(allocation.jobId, (map.get(allocation.jobId) ?? 0) + 1)
      }
    }
    return map
  })

  const [data] = createResource(
    () => ({ search: searchQuery(), sort: sort(), page: page(), pageSize: pageSize() }),
    async (input) => {
      const [sortBy, sortOrder] = input.sort.split(':')
      return managerClient.listJobs({ query: input.search, sortBy, sortOrder, page: input.page, pageSize: input.pageSize })
    },
  )

  return (
    <SharedIndex
      entityLabel="Jobs"
      search={search()}
      onSearch={setSearch}
      sort={sort()}
      onSort={setSort}
      sortOptions={[
        { label: 'Updated (newest)', value: 'updated_at:desc' },
        { label: 'Created (newest)', value: 'created_at:desc' },
        { label: 'Created (oldest)', value: 'created_at:asc' },
        { label: 'Name (A-Z)', value: 'name:asc' },
      ]}
      columns={[{ key: 'allocations', header: 'Allocations', class: 'text-right' }]}
      loading={data.loading || ledger.loading}
      error={data.error ? 'Failed to load jobs.' : undefined}
      items={(data()?.jobs ?? []).map((job) => ({
        id: job.id,
        title: job.name || job.id,
        status: job.status || 'unknown',
        // The column is "Updated": show the job's mtime, not its birth time.
        // Equal to createdAt until the job is resubmitted or changes status.
        updatedAt: formatRelativeTime(job.updatedAt),
        href: `/jobs/${encodeURIComponent(job.name || job.id)}`,
        cells: {
          allocations: <span class="tabular-nums">{jobAllocationCounts().get(job.id) ?? 0}</span>,
        },
      }))}
      page={Number(data()?.page ?? page())}
      pageSize={Number(data()?.pageSize ?? pageSize())}
      totalCount={Number(data()?.totalCount ?? 0)}
      onPageChange={setPage}
      onPageSizeChange={(size) => {
        setPageSize(size)
        setPage(1)
      }}
      emptyLabel="No jobs yet"
      emptyHint="Submit a job with `legion job submit <spec>` and it will appear here."
    />
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function Jobs() {
  return (
    <AccessGate resource="job">
      <JobsContent />
    </AccessGate>
  )
}
