import { createEffect, createMemo, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { checkboxClass } from '../components/ui'
import { managerClient } from '../grpcClient'
import { debounced } from '../utils/debounce'
import { formatRelativeTime } from '../utils/format'

const isActiveStatus = (status: string) => !['completed', 'failed', 'cancelled', 'completed_incomplete'].includes(status.toLowerCase())

export default function Rollouts() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const searchQuery = debounced(search)
  const [activeOnly, setActiveOnly] = createSignal(false)

  createEffect(() => {
    search(); sort(); activeOnly(); setPage(1)
  })

  const [ledger] = createResource(async () => (await managerClient.getLedger({})).nodes)
  const placementCounts = createMemo(() => {
    const counts = new Map<string, number>()
    for (const node of ledger() ?? []) {
      for (const allocation of node.allocations) {
        counts.set(allocation.jobConfigId, (counts.get(allocation.jobConfigId) ?? 0) + 1)
      }
    }
    return counts
  })

  const [data] = createResource(
    () => ({ search: searchQuery(), sort: sort(), page: page(), pageSize: pageSize(), activeOnly: activeOnly() }),
    async (input) => {
      const [sortBy, sortOrder] = input.sort.split(':')
      return managerClient.listRollouts({ query: input.search, sortBy, sortOrder, page: input.page, pageSize: input.pageSize, activeOnly: input.activeOnly })
    },
  )

  const prioritized = createMemo(() => {
    const items = [...(data()?.rollouts ?? [])]
    return items.sort((a, b) => Number(isActiveStatus(b.status)) - Number(isActiveStatus(a.status)))
  })

  return (
    <SharedIndex
      entityLabel="Rollouts"
      search={search()}
      onSearch={setSearch}
      sort={sort()}
      onSort={setSort}
      sortOptions={[
        { label: 'Updated (newest)', value: 'updated_at:desc' },
        { label: 'Updated (oldest)', value: 'updated_at:asc' },
        { label: 'Status', value: 'status:asc' },
      ]}
      extraFilters={
        <label class="flex cursor-pointer items-center gap-2 rounded-md border border-line bg-surface px-2.5 py-1.5 text-xs font-medium text-muted">
          <input type="checkbox" class={checkboxClass} checked={activeOnly()} onChange={(event) => setActiveOnly(event.currentTarget.checked)} />
          Active only
        </label>
      }
      columns={[
        { key: 'target', header: 'Target config' },
        { key: 'placed', header: 'Placed', class: 'text-right' },
      ]}
      loading={data.loading || ledger.loading}
      error={data.error ? 'Failed to load rollouts.' : undefined}
      items={prioritized().map((rollout) => ({
        id: String(rollout.id),
        title: rollout.jobId,
        subtitle: `Rollout #${rollout.id}`,
        status: rollout.status,
        updatedAt: formatRelativeTime(rollout.updatedAt),
        href: `/rollouts/${rollout.id}`,
        cells: {
          target: <span class="font-mono text-xs">{rollout.toJobConfigId}</span>,
          placed: <span class="tabular-nums">{placementCounts().get(rollout.toJobConfigId) ?? 0}</span>,
        },
      }))}
      page={Number(data()?.page ?? page())}
      pageSize={Number(data()?.pageSize ?? pageSize())}
      totalCount={Number(data()?.totalCount ?? 0)}
      onPageChange={setPage}
      onPageSizeChange={(size) => { setPageSize(size); setPage(1) }}
      emptyLabel="No rollouts yet"
      emptyHint="Rollouts are created automatically when you submit a new version of a running job."
    />
  )
}
