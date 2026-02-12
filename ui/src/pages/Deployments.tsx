import { createEffect, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'
import { healthToneClasses } from '../utils/health'

const tone = (status: string) =>
  status.toLowerCase().includes('fail') ? healthToneClasses('unhealthy') : healthToneClasses('healthy')

export default function Deployments() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)

  createEffect(() => {
    search()
    sort()
    setPage(1)
  })

  const [data] = createResource(
    () => ({ search: search(), sort: sort(), page: page(), pageSize: pageSize() }),
    async (input) => {
      const [sortBy, sortOrder] = input.sort.split(':')
      return managerClient.listRollouts({
        query: input.search,
        sortBy,
        sortOrder,
        page: input.page,
        pageSize: input.pageSize,
        activeOnly: false,
      })
    },
  )

  return (
    <SharedIndex
      entityLabel="Deployments"
      search={search()}
      onSearch={setSearch}
      sort={sort()}
      onSort={setSort}
      sortOptions={[
        { label: 'Updated (newest)', value: 'updated_at:desc' },
        { label: 'Updated (oldest)', value: 'updated_at:asc' },
        { label: 'Job', value: 'job_id:asc' },
      ]}
      loading={data.loading}
      error={data.error ? 'Failed to load deployments.' : undefined}
      items={(data()?.rollouts ?? []).map((rollout) => ({
        id: String(rollout.id),
        title: `${rollout.jobId} · ${rollout.toJobConfigId}`,
        subtitle: `Rollout #${rollout.id}`,
        health: rollout.status,
        healthClass: tone(rollout.status),
        updatedAt: formatRelativeTime(rollout.updatedAt),
        href: `/deployments/${rollout.id}`,
      }))}
      page={Number(data()?.page ?? page())}
      pageSize={Number(data()?.pageSize ?? pageSize())}
      totalCount={Number(data()?.totalCount ?? 0)}
      onPageChange={setPage}
      onPageSizeChange={(size) => {
        setPageSize(size)
        setPage(1)
      }}
      emptyLabel="No deployments found."
    />
  )
}
