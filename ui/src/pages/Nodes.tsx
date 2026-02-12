import { createEffect, createMemo, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'
import { healthToneClasses, summarizeHealthChecks } from '../utils/health'

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
      entityLabel="Nodes"
      search={search()}
      onSearch={setSearch}
      sort={sort()}
      onSort={setSort}
      sortOptions={[
        { label: 'ID (A-Z)', value: 'id:asc' },
        { label: 'ID (Z-A)', value: 'id:desc' },
        { label: 'Last heartbeat (newest)', value: 'last_heartbeat:desc' },
      ]}
      loading={data.loading || ledger.loading}
      error={data.error ? 'Failed to load nodes.' : undefined}
      items={(data()?.nodes ?? []).map((node) => {
        const health = summarizeHealthChecks(node.healthChecks)
        return {
          id: node.id,
          title: node.id,
          subtitle: `Pool: ${node.pool || 'default'} · Availability: ${node.availability || '—'} · Last heartbeat: ${formatRelativeTime(node.lastHeartbeat)} · Allocations: ${allocationCounts().get(node.id) ?? 0}`,
          health: health.label,
          healthClass: healthToneClasses(health.tone),
          updatedAt: formatRelativeTime(node.lastHeartbeat),
          href: `/nodes/${encodeURIComponent(node.id)}`,
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
      emptyLabel="No nodes found."
    />
  )
}
