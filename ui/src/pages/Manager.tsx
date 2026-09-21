import { Show, createEffect, createResource, createSignal } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import SharedIndex from '../components/SharedIndex'
import { Card, Note, StatusBadge } from '../components/ui'
import { managerClient } from '../grpcClient'
import { debounced } from '../utils/debounce'
import { INDEX_POLL_INTERVAL_MS, retainOnError, usePoll } from '../utils/polling'
import type { Tone } from '../utils/status'

/** Tone for a keyring state (docs/kv-encryption.md): locked is the one that pages someone. */
function keyringTone(state?: string): Tone {
  switch (state) {
    case 'unlocked':
      return 'ok'
    case 'locked':
      return 'danger'
    case 'disabled':
      return 'neutral'
    case 'uninitialized':
    case 'pending_upgrade':
      return 'warn'
    default:
      return 'neutral'
  }
}

/**
 * KV encryption at rest, as the manager answering the request sees it. The
 * root key is per-manager state, so this card is about *this* manager; the
 * `hint` is the server's own one-line next step, shown verbatim.
 */
function KeyringCard() {
  const tick = usePoll(INDEX_POLL_INTERVAL_MS)
  const [status] = createResource(
    () => tick(),
    retainOnError(
      async () => managerClient.getKeyringStatus({}),
      () => 'keyring',
    ),
  )
  const state = () => status()?.state
  const label = () => (state() ? state()!.replace('_', ' ') : status.loading ? 'loading' : 'unavailable')
  return (
    <Card class="mb-4">
      <div class="flex flex-wrap items-center gap-2">
        <h2 class="text-sm font-semibold text-ink">KV encryption at rest</h2>
        <StatusBadge status={label()} tone={keyringTone(state())} loading={status.loading && !status()} title="Keyring state on the answering manager" />
      </div>
      <Show when={status()} fallback={<p class="mt-1 text-xs text-muted">{status.error ? 'Keyring status is unavailable.' : 'Loading keyring status…'}</p>}>
        {(s) => (
          <div class="mt-1 space-y-0.5 text-xs text-muted">
            <p>
              Root key <span class="font-mono text-ink">{s().rootKeyProvider}</span>{' '}
              <span class="font-mono">{s().rootKeyId || '(no key material)'}</span> at <span class="font-mono">{s().rootKeyLocation}</span>
            </p>
            <p>
              {Number(s().encryptedRows)} encrypted, {Number(s().plaintextRows)} plaintext · {s().keys.length} data key
              {s().keys.length === 1 ? '' : 's'} · rotates every {s().dataKeyRotationAge}
              <Show when={s().rewrap?.running}> · rewrap running ({Number(s().rewrap!.pending)} pending)</Show>
            </p>
            <Show when={s().hint}>
              <Note tone={keyringTone(s().state) === 'ok' ? 'info' : keyringTone(s().state)} class="mt-2">
                {s().hint}
              </Note>
            </Show>
          </div>
        )}
      </Show>
    </Card>
  )
}

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
    <>
    <KeyringCard />
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
    </>
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
