import { createEffect, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'
import { healthToneClasses } from '../utils/health'

export default function Manager() {
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
      return managerClient.getManagerConfigs({
        query: input.search,
        sortBy,
        sortOrder,
        page: input.page,
        pageSize: input.pageSize,
      })
    },
  )

  return (
    <SharedIndex
      entityLabel="Managers"
      search={search()}
      onSearch={setSearch}
      sort={sort()}
      onSort={setSort}
      sortOptions={[
        { label: 'Updated (newest)', value: 'updated_at:desc' },
        { label: 'Updated (oldest)', value: 'updated_at:asc' },
        { label: 'Manager ID', value: 'node_id:asc' },
      ]}
      loading={data.loading}
      error={data.error ? 'Failed to load managers.' : undefined}
      items={(data()?.configs ?? []).map((config) => ({
        id: config.nodeId,
        title: config.nodeId,
        health: 'config',
        healthClass: healthToneClasses('healthy'),
        updatedAt: formatRelativeTime(config.updatedAt),
        href: `/manager/${encodeURIComponent(config.nodeId)}`,
      }))}
      page={Number(data()?.page ?? page())}
      pageSize={Number(data()?.pageSize ?? pageSize())}
      totalCount={Number(data()?.totalCount ?? 0)}
      onPageChange={setPage}
      onPageSizeChange={(size) => {
        setPageSize(size)
        setPage(1)
      }}
      emptyLabel="No managers found."
    />
  )
}
