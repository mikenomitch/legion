import { A } from '@solidjs/router'
import { AccessGate } from '../components/AccessGate'
import { Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { CreateJoinTokenForm } from '../components/acl/AdminPanels'
import { CompactNodeUsage } from '../components/NodeUsagePanel'
import { Badge, Button, SectionCard, StatusBadge, linkClass } from '../components/ui'
import { managerClient } from '../grpcClient'
import { debounced } from '../utils/debounce'
import { heartbeatFreshnessHint } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../utils/polling'
import { fetchNodeUsage } from '../utils/usage'

function NodesContent() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('id:asc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const [joinTokenOpen, setJoinTokenOpen] = createSignal(false)
  const searchQuery = debounced(search)
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)

  createEffect(() => {
    search()
    sort()
    setPage(1)
  })

  const [ledger] = createResource(
    () => tick() + 1,
    retainOnError(async () => (await managerClient.getLedger({})).nodes, () => 'ledger'),
  )

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
    () => ({ search: searchQuery(), sort: sort(), page: page(), pageSize: pageSize(), tick: tick() }),
    retainOnError(
      async (input: { search: string; sort: string; page: number; pageSize: number; tick: number }) => {
        const [sortBy, sortOrder] = input.sort.split(':')
        return managerClient.getNodes({
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

  // Memory used and reserved for the workers on this page, one GetNodeUsage
  // per row at the index cadence. There is no batch RPC yet (see the suspend
  // UI plan, §7.3), and the manager answers each from memory. A node that
  // fails to answer shows a blank cell.
  const [usageByNode] = createResource(
    () => [(data()?.nodes ?? []).map((node) => node.id).join(','), tick()] as const,
    async ([ids]) => {
      const nodeIds = ids ? ids.split(',') : []
      const replies = await Promise.all(nodeIds.map((id) => fetchNodeUsage(id).catch(() => null)))
      return new Map(nodeIds.map((id, index) => [id, replies[index]]))
    },
  )

  return (
    <div class="space-y-3">
      {/* Adding a worker starts here — a join token is the one command a new
          host needs — so the form lives next to the list it will fill. */}
      <Show when={joinTokenOpen()}>
        <SectionCard
          title="New worker join token"
          description={
            <>
              Run the printed command on the worker host. Existing tokens are listed under{' '}
              <A href="/access" class={linkClass}>Access → Worker Join</A>.
            </>
          }
          actions={<Button size="xs" icon="x" onClick={() => setJoinTokenOpen(false)} aria-label="Close join token form" />}
        >
          <CreateJoinTokenForm />
        </SectionCard>
      </Show>

      <SharedIndex
        entityLabel="Workers"
        actions={
          <Button icon="keySquare" onClick={() => setJoinTokenOpen((value) => !value)} aria-expanded={joinTokenOpen()}>
            Join token
          </Button>
        }
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
          { key: 'health', header: 'Health' },
          { key: 'pool', header: 'Pool' },
          { key: 'availability', header: 'Availability' },
          { key: 'memory', header: 'Memory used / reserved' },
          { key: 'allocations', header: 'Allocations', class: 'text-right' },
        ]}
        updatedLabel="Heartbeat"
        loading={data.loading || ledger.loading}
        error={data.error ? 'Failed to load workers.' : undefined}
        items={(data()?.nodes ?? []).map((node) => {
          const health = summarizeHealthChecks(node.healthChecks)
          return {
            id: node.id,
            title: node.id,
            // Liveness (ready / lost / down) is the status; the health checks
            // the worker reports are a separate column, as on the overview.
            status: node.status || 'unknown',
            updatedAt: node.lastHeartbeat,
            updatedAtTitle: heartbeatFreshnessHint(node.heartbeatSource),
            href: `/nodes/${encodeURIComponent(node.id)}`,
            cells: {
              health: <StatusBadge status={health.label} tone={health.tone} />,
              pool: node.pool || 'default',
              availability:
                node.availability && node.availability !== 'available' ? (
                  <Badge tone={node.availability === 'drained' ? 'warn' : 'danger'} dot class="uppercase tracking-wide">
                    {node.availability}
                  </Badge>
                ) : (
                  node.availability || '—'
                ),
              memory: <CompactNodeUsage usage={usageByNode()?.get(node.id)} />,
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
        emptyHint="Create a join token above, then run the printed `legion join <token>` command on the worker host."
      />
    </div>
  )
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function Nodes() {
  return (
    <AccessGate resource="node">
      <NodesContent />
    </AccessGate>
  )
}
