import { A } from '@solidjs/router'
import { For, Show, createMemo, createResource, createSignal, type JSX } from 'solid-js'

export type SortDirection = 'asc' | 'desc'

export interface IndexSortOption {
  value: string
  label: string
}

export interface EntityIndexItem {
  id: string
  name: string
  health: string
  updatedAt: string
  href: string
  subtitle?: string
}

export interface EntityIndexQuery {
  page: number
  pageSize: number
  search: string
  sortBy: string
  sortDirection: SortDirection
}

interface EntityIndexResult {
  items: readonly EntityIndexItem[]
  total: number
}

interface EntityIndexProps {
  title: string
  description: string
  emptyMessage: string
  sortOptions: readonly IndexSortOption[]
  fetchPage: (query: EntityIndexQuery) => Promise<EntityIndexResult>
}

const HEALTH_TONE: Record<string, string> = {
  healthy: 'border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950 dark:text-emerald-200',
  degraded: 'border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200',
  unhealthy: 'border-red-300 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950 dark:text-red-200',
  unknown: 'border-neutral-300 bg-neutral-50 text-neutral-700 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-200',
}

const healthTone = (value: string): string => {
  const normalized = value.trim().toLowerCase()
  if (normalized.includes('healthy') || normalized.includes('active') || normalized.includes('running') || normalized.includes('completed')) {
    return HEALTH_TONE.healthy
  }
  if (normalized.includes('failed') || normalized.includes('down') || normalized.includes('error') || normalized.includes('stopped')) {
    return HEALTH_TONE.unhealthy
  }
  if (normalized.includes('pending') || normalized.includes('paused') || normalized.includes('waiting')) {
    return HEALTH_TONE.degraded
  }
  return HEALTH_TONE.unknown
}

const formatUpdatedAt = (value: string): string => (value.trim().length > 0 ? value : '—')

export default function EntityIndex(props: EntityIndexProps): JSX.Element {
  const [search, setSearch] = createSignal('')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal(10)
  const [sortBy, setSortBy] = createSignal(props.sortOptions[0]?.value ?? 'updatedAt')
  const [sortDirection, setSortDirection] = createSignal<SortDirection>('desc')

  const query = createMemo<EntityIndexQuery>(() => ({
    page: page(),
    pageSize: pageSize(),
    search: search(),
    sortBy: sortBy(),
    sortDirection: sortDirection(),
  }))

  const [result, { refetch }] = createResource(query, props.fetchPage)

  const totalPages = createMemo(() => Math.max(1, Math.ceil((result()?.total ?? 0) / pageSize())))

  const setCurrentPage = (value: number) => {
    const next = Math.min(Math.max(1, value), totalPages())
    setPage(next)
  }

  const onSearchInput: JSX.EventHandlerUnion<HTMLInputElement, InputEvent> = (event) => {
    setSearch(event.currentTarget.value)
    setPage(1)
  }

  return (
    <section class="space-y-4">
      <header class="space-y-2 rounded border border-neutral-200 bg-white/90 p-4 shadow-[5px_5px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
        <h2 class="text-xl font-semibold text-neutral-900 dark:text-neutral-100">{props.title}</h2>
        <p class="text-sm text-neutral-500 dark:text-neutral-400">{props.description}</p>
        <div class="grid gap-2 md:grid-cols-[2fr_1fr_1fr_1fr]">
          <input
            value={search()}
            onInput={onSearchInput}
            placeholder="Search"
            class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
          />
          <select
            value={sortBy()}
            onChange={(event) => {
              setSortBy(event.currentTarget.value)
              setPage(1)
            }}
            class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
          >
            <For each={props.sortOptions}>{(option) => <option value={option.value}>{option.label}</option>}</For>
          </select>
          <select
            value={sortDirection()}
            onChange={(event) => {
              setSortDirection(event.currentTarget.value as SortDirection)
              setPage(1)
            }}
            class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
          >
            <option value="desc">Desc</option>
            <option value="asc">Asc</option>
          </select>
          <select
            value={String(pageSize())}
            onChange={(event) => {
              setPageSize(Number(event.currentTarget.value))
              setPage(1)
            }}
            class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm text-neutral-900 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-100"
          >
            <option value="10">10 / page</option>
            <option value="25">25 / page</option>
            <option value="50">50 / page</option>
          </select>
        </div>
      </header>

      <Show when={!result.loading} fallback={<p class="text-neutral-500">Loading…</p>}>
        <Show when={!result.error} fallback={<p class="text-red-500">Failed to load index data.</p>}>
          <Show when={(result()?.items.length ?? 0) > 0} fallback={<p class="text-neutral-500">{props.emptyMessage}</p>}>
            <div class="space-y-3">
              <For each={result()?.items ?? []}>
                {(item) => (
                  <A
                    href={item.href}
                    class="block rounded border border-neutral-200 bg-white/90 p-4 shadow-[4px_4px_0_#d4d4d8] transition hover:-translate-y-0.5 dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[4px_4px_0_#0b0d12]"
                  >
                    <div class="flex flex-wrap items-center justify-between gap-2">
                      <div>
                        <p class="text-base font-semibold text-neutral-900 dark:text-neutral-100">{item.name}</p>
                        <Show when={item.subtitle}>
                          <p class="text-xs text-neutral-500 dark:text-neutral-400">{item.subtitle}</p>
                        </Show>
                      </div>
                      <span class={`rounded border px-2 py-0.5 text-[11px] uppercase tracking-wide ${healthTone(item.health)}`}>
                        {item.health}
                      </span>
                    </div>
                    <p class="mt-2 text-xs text-neutral-500 dark:text-neutral-400">Updated {formatUpdatedAt(item.updatedAt)}</p>
                  </A>
                )}
              </For>
            </div>

            <div class="flex items-center justify-between rounded border border-neutral-200 bg-white/90 px-4 py-2 text-sm dark:border-neutral-800 dark:bg-neutral-900/60">
              <button
                type="button"
                onClick={() => setCurrentPage(page() - 1)}
                disabled={page() <= 1}
                class="rounded border border-neutral-300 px-2 py-1 disabled:opacity-50 dark:border-neutral-700"
              >
                Previous
              </button>
              <p>Page {page()} / {totalPages()}</p>
              <button
                type="button"
                onClick={() => setCurrentPage(page() + 1)}
                disabled={page() >= totalPages()}
                class="rounded border border-neutral-300 px-2 py-1 disabled:opacity-50 dark:border-neutral-700"
              >
                Next
              </button>
            </div>
          </Show>
        </Show>
      </Show>

      <button
        type="button"
        onClick={() => void refetch()}
        class="text-xs text-neutral-500 underline dark:text-neutral-400"
      >
        Refresh index
      </button>
    </section>
  )
}
