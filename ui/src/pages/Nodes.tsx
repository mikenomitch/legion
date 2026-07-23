import { createEffect, createMemo, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'

export default function Nodes() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('id:asc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)

  createEffect(() => {
    search()
    sort()
    setPage(1)
  })

  const [ledger] = createResource(async () => (await managerClient.getLedger({})).nodes)

  const allocationCounts = createMemo(() => {
    const counts = new Map<string, number>()
    for (const entry of ledger() ?? []) {
      const nodeId = entry.node?.id
      if (!nodeId) continue
      counts.set(nodeId, entry.allocations.length)
    }
    return counts
  })

  const [data] = createResource(
    () => ({ search: search(), sort: sort(), page: page(), pageSize: pageSize() }),
    async (input) => {
      const [sortBy, sortOrder] = input.sort.split(':')
      return managerClient.getNodes({
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
      entityLabel="Workers"
      search={search()}
      onSearch={setSearch}
      sort={sort()}
      onSort={setSort}
      sortOptions={[
        { label: 'ID (A-Z)', value: 'id:asc' },
        { label: 'ID (Z-A)', value: 'id:desc' },
        { label: 'Last heartbeat (newest)', value: 'last_heartbeat:desc' },
      ]}
      columns={[
        { key: 'pool', header: 'Pool' },
        { key: 'availability', header: 'Availability' },
        { key: 'allocations', header: 'Allocations', class: 'text-right' },
      ]}
      loading={data.loading || ledger.loading}
      error={data.error ? 'Failed to load workers.' : undefined}
      items={(data()?.nodes ?? []).map((node) => {
        const health = summarizeHealthChecks(node.healthChecks)
        return {
          id: node.id,
          title: node.id,
          status: health.label,
          statusTone: health.tone,
          updatedAt: formatRelativeTime(node.lastHeartbeat),
          href: `/nodes/${encodeURIComponent(node.id)}`,
          cells: {
            pool: node.pool || 'default',
            availability: node.availability || '—',
            allocations: <span class="tabular-nums">{allocationCounts().get(node.id) ?? 0}</span>,
          },
        }
      })}
      page={Number(data()?.page ?? page())}
      pageSize={Number(data()?.pageSize ?? pageSize())}
      totalCount={Number(data()?.totalCount ?? 0)}
      onPageChange={setPage}
      onPageSizeChange={(size) => {
        setPageSize(size)
        setPage(1)
      }}
      emptyLabel="No workers connected"
      emptyHint="Join a worker with `legion agent --join <manager>` and it will appear here."
    />
  )
}
