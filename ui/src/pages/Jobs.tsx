import { A, useSearchParams } from '@solidjs/router'
import { Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import SharedIndex from '../components/SharedIndex'
import { FilterChips, type FilterChip } from '../components/ui'
import { managerClient } from '../grpcClient'
import { debounced } from '../utils/debounce'
import { INDEX_POLL_INTERVAL_MS, retainOnError, swallowDenied, usePoll } from '../utils/polling'
import { statusTone } from '../utils/status'
import { useNamespaceSelection } from '../components/NamespaceSwitcher'
import { jobPath, selectionIsMulti } from '../utils/namespaces'

// The order the status chips appear in: the states an operator acts on
// first, then the quiet ones. Anything the manager emits that is not listed
// still gets a chip, after these.
const STATUS_ORDER = ['running', 'blocked', 'pending', 'failed', 'scheduled', 'complete', 'suspended', 'idle', 'stopped']

function JobsContent() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [status, setStatus] = createSignal('all')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const searchQuery = debounced(search)
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)
  // `?ns=` (the header switcher writes it); the All view for an identity
  // that reads several namespaces.
  const selection = useNamespaceSelection()
  const [, setParams] = useSearchParams()
  const multi = () => selectionIsMulti(selection())

  createEffect(() => {
    search()
    sort()
    status()
    selection()
    setPage(1)
  })

  // Allocation counts are garnish needing node:read; a job-scoped token
  // (job:read only) must still get the list, so denial degrades to empty.
  const [ledger] = createResource(
    () => tick() + 1,
    retainOnError(
      swallowDenied(async () => (await managerClient.getLedger({})).nodes, []),
      () => 'ledger',
    ),
  )
  const jobAllocationCounts = createMemo(() => {
    const map = new Map<string, number>()
    for (const node of ledger() ?? []) {
      for (const allocation of node.allocations) {
        map.set(allocation.jobId, (map.get(allocation.jobId) ?? 0) + 1)
      }
    }
    return map
  })

  // Retained across a failed poll (keyed on the query, so a new search still
  // shows its own outcome): the rows stay up and the banner reports the outage.
  const [data] = createResource(
    () => ({
      search: searchQuery(),
      sort: sort(),
      status: status(),
      page: page(),
      pageSize: pageSize(),
      namespaces: selection(),
      tick: tick(),
    }),
    retainOnError(
      async (input: {
        search: string
        sort: string
        status: string
        page: number
        pageSize: number
        namespaces: string[]
        tick: number
      }) => {
        const [sortBy, sortOrder] = input.sort.split(':')
        return managerClient.listJobs({
          query: input.search,
          sortBy,
          sortOrder,
          page: input.page,
          pageSize: input.pageSize,
          status: input.status === 'all' ? undefined : input.status,
          namespaces: input.namespaces,
        })
      },
      (input) =>
        `${input.search}|${input.sort}|${input.status}|${input.page}|${input.pageSize}|${input.namespaces.join(',')}`,
    ),
  )

  // Nothing here, but the caller can see jobs elsewhere: say where, so an
  // empty list in the wrong namespace is never mistaken for an empty cluster.
  const elsewhere = () => {
    const reply = data()
    if (!reply || reply.jobs.length > 0 || reply.elsewhereCount === 0) return undefined
    return { jobs: reply.elsewhereCount, namespaces: reply.elsewhereNamespaces }
  }

  // Chips come from the server's per-status counts over the whole
  // query-matched set, so they hold still while one is selected.
  const chips = createMemo<FilterChip[]>(() => {
    const counts = data()?.statusCounts ?? {}
    const known = Object.keys(counts)
    const ordered = [
      ...STATUS_ORDER.filter((entry) => known.includes(entry)),
      ...known.filter((entry) => !STATUS_ORDER.includes(entry)).sort(),
    ]
    const total = Object.values(counts).reduce((sum, count) => sum + count, 0)
    return [
      { id: 'all', label: 'All', count: total },
      ...ordered.map((entry) => ({ id: entry, label: entry, count: counts[entry], tone: statusTone(entry) })),
    ]
  })

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
      chips={<FilterChips label="Filter by status" chips={chips()} active={status()} onChange={setStatus} />}
      filtered={status() !== 'all'}
      onClearFilters={() => setStatus('all')}
      columns={[
        ...(multi() ? [{ key: 'namespace', header: 'Namespace' }] : []),
        { key: 'allocations', header: 'Allocations', class: 'text-right' },
      ]}
      loading={data.loading || ledger.loading}
      error={data.error ? 'Failed to load jobs.' : undefined}
      items={(data()?.jobs ?? []).map((job) => ({
        id: job.id,
        // The bare name: under the All view the namespace has its column.
        title: job.name || job.id,
        status: job.status || 'unknown',
        // The column is "Updated": show the job's mtime, not its birth time.
        // Equal to createdAt until the job is resubmitted or changes status.
        updatedAt: job.updatedAt,
        href: jobPath(job.id),
        cells: {
          namespace: (
            <A class="font-mono text-xs text-muted hover:text-brass-strong" href={`/jobs?ns=${encodeURIComponent(job.namespace)}`}>
              {job.namespace}
            </A>
          ),
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
      emptyLabel={elsewhere() ? 'No jobs in this namespace' : 'No jobs yet'}
      emptyHint={
        <Show
          when={elsewhere()}
          fallback="Submit one with `legion job run <spec.ncl>` — or run a template from the Templates page — and it will appear here."
        >
          {(other) => (
            <>
              {other().jobs} job(s) in {other().namespaces} other namespace(s) you can read.{' '}
              <button type="button" class="font-medium text-brass-strong hover:underline" onClick={() => setParams({ ns: '*' })}>
                Show all namespaces
              </button>
            </>
          )}
        </Show>
      }
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
