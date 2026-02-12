import { createEffect, createResource, createSignal } from 'solid-js'
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

export default function Rollouts() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const [activeOnly, setActiveOnly] = createSignal(true)

  createEffect(() => {
    search()
    sort()
    activeOnly()
    setPage(1)
  })

  const [data] = createResource(
    () => ({ search: search(), sort: sort(), page: page(), pageSize: pageSize(), activeOnly: activeOnly() }),
    async (input) => {
      const [sortBy, sortOrder] = input.sort.split(':')
      return managerClient.listRollouts({
        query: input.search,
        sortBy,
        sortOrder,
        page: input.page,
        pageSize: input.pageSize,
        activeOnly: input.activeOnly,
      })
    },
  )

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
        <label class="flex items-center gap-2 text-sm">
          <input type="checkbox" checked={activeOnly()} onChange={(event) => setActiveOnly(event.currentTarget.checked)} />
          Active only
        </label>
      }
      loading={data.loading}
      error={data.error ? 'Failed to load rollouts.' : undefined}
      items={(data()?.rollouts ?? []).map((rollout) => ({
        id: String(rollout.id),
        title: `#${rollout.id} ${rollout.jobId}`,
        subtitle: rollout.toJobConfigId,
        health: rollout.status,
        healthClass: toneForStatus(rollout.status),
        updatedAt: formatRelativeTime(rollout.updatedAt),
        href: `/rollouts/${rollout.id}`,
      }))}
      page={Number(data()?.page ?? page())}
      pageSize={Number(data()?.pageSize ?? pageSize())}
      totalCount={Number(data()?.totalCount ?? 0)}
      onPageChange={setPage}
      onPageSizeChange={(size) => {
        setPageSize(size)
        setPage(1)
      }}
      emptyLabel="No rollouts found."
    />
  )
}
