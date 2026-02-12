import { createEffect, createMemo, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'
import { healthToneClasses } from '../utils/health'

const toneForStatus = (status: string) => {
  const normalized = status.toLowerCase()
  if (normalized.includes('failed') || normalized.includes('cancel')) return healthToneClasses('unhealthy')
  if (normalized.includes('completed') || normalized.includes('running')) return healthToneClasses('healthy')
  return healthToneClasses('unknown')
}

const isActiveStatus = (status: string) => !['completed', 'failed', 'cancelled', 'completed_incomplete'].includes(status.toLowerCase())

export default function Rollouts() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
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
    () => ({ search: search(), sort: sort(), page: page(), pageSize: pageSize(), activeOnly: activeOnly() }),
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
      extraFilters={<label class="flex items-center gap-2 text-sm"><input type="checkbox" checked={activeOnly()} onChange={(event) => setActiveOnly(event.currentTarget.checked)} />Active only</label>}
      loading={data.loading || ledger.loading}
      error={data.error ? 'Failed to load rollouts.' : undefined}
      items={prioritized().map((rollout) => ({
        id: String(rollout.id),
        title: rollout.jobId,
        subtitle: `Placed allocations: ${placementCounts().get(rollout.toJobConfigId) ?? 0}`,
        health: rollout.status,
        healthClass: toneForStatus(rollout.status),
        updatedAt: formatRelativeTime(rollout.updatedAt),
        href: `/rollouts/${rollout.id}`,
      }))}
      page={Number(data()?.page ?? page())}
      pageSize={Number(data()?.pageSize ?? pageSize())}
      totalCount={Number(data()?.totalCount ?? 0)}
      onPageChange={setPage}
      onPageSizeChange={(size) => { setPageSize(size); setPage(1) }}
      emptyLabel="No rollouts found."
    />
  )
}
