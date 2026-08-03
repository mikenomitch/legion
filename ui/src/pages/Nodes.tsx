import { A } from '@solidjs/router'
import { AccessGate } from '../components/AccessGate'
import { Show, createEffect, createMemo, createResource, createSignal } from 'solid-js'
import SharedIndex from '../components/SharedIndex'
import { CreateJoinTokenForm } from '../components/acl/AdminPanels'
import { Button, SectionCard, linkClass } from '../components/ui'
import { managerClient } from '../grpcClient'
import { debounced } from '../utils/debounce'
import { formatRelativeTime, heartbeatFreshnessHint } from '../utils/format'
import { summarizeHealthChecks } from '../utils/health'

function NodesContent() {
  const [search, setSearch] = createSignal('')
  const [sort, setSort] = createSignal('id:asc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(20)
  const [joinTokenOpen, setJoinTokenOpen] = createSignal(false)
  const searchQuery = debounced(search)

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
    () => ({ search: searchQuery(), sort: sort(), page: page(), pageSize: pageSize() }),
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
            updatedAtTitle: heartbeatFreshnessHint(node.heartbeatSource),
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
        emptyHint="Create a join token above, then run the printed command on the worker host."
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
