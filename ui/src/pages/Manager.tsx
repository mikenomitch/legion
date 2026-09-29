import { createEffect, createResource, createSignal } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import SharedIndex from '../components/SharedIndex'
import { managerClient } from '../grpcClient'
import { debounced } from '../utils/debounce'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../utils/polling'

function ManagerContent() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('updated_at:desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const searchQuery = debounced(search)
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)

  createEffect(() => {
    search()
    sort()
    setPage(1)
  })

  const [data] = createResource(
    () => ({ search: searchQuery(), sort: sort(), page: page(), pageSize: pageSize(), tick: tick() }),
    retainOnError(
      async (input: { search: string; sort: string; page: number; pageSize: number; tick: number }) => {
        const [sortBy, sortOrder] = input.sort.split(':')
        return managerClient.getManagerConfigs({
          query: input.search,
          sortBy,
          sortOrder,
          page: input.page,
          pageSize: input.pageSize,
        })
      },
      (input) => `${input.search}|${input.sort}|${input.page}|${input.pageSize}`,
    ),
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
      hideStatus
      loading={data.loading}
      error={data.error ? 'Failed to load managers.' : undefined}
      items={(data()?.configs ?? []).map((config) => ({
        id: config.nodeId,
        title: config.nodeId,
        subtitle: 'Configuration snapshot',
        updatedAt: config.updatedAt,
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
      emptyLabel="No manager configs recorded"
      emptyHint="Manager nodes publish a config snapshot when they start."
    />
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function Manager() {
  return (
    <AccessGate resource="operator">
      <ManagerContent />
    </AccessGate>
  )
}
