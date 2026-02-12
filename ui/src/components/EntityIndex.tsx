import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal } from 'solid-js'

export type IndexItem = {
  id: string
  name: string
  health: string
  updatedAt: string
  searchText: string
  href: string
}

interface EntityIndexProps {
  title: string
  description: string
  items: readonly IndexItem[]
  loading?: boolean
  error?: unknown
  sortOptions: readonly { key: 'name' | 'health' | 'updatedAt'; label: string }[]
}

const pageSizes = [10, 25, 50] as const

export default function EntityIndex(props: EntityIndexProps) {
  const [query, setQuery] = createSignal('')
  const [sortKey, setSortKey] = createSignal<'name' | 'health' | 'updatedAt'>('updatedAt')
  const [sortDirection, setSortDirection] = createSignal<'asc' | 'desc'>('desc')
  const [page, setPage] = createSignal(1)
  const [pageSize, setPageSize] = createSignal<number>(25)

  const filtered = createMemo(() => {
    const q = query().trim().toLowerCase()
    const next = q
      ? props.items.filter((item) => item.searchText.toLowerCase().includes(q))
      : props.items.slice()

    return next.sort((a, b) => {
      const direction = sortDirection() === 'asc' ? 1 : -1
      if (sortKey() === 'updatedAt') {
        return (a.updatedAt || '').localeCompare(b.updatedAt || '') * direction
      }
      return (a[sortKey()] || '').localeCompare(b[sortKey()] || '') * direction
    })
  })

  const totalPages = createMemo(() => Math.max(1, Math.ceil(filtered().length / pageSize())))

  const paged = createMemo(() => {
    const start = (page() - 1) * pageSize()
    return filtered().slice(start, start + pageSize())
  })

  const updatePage = (next: number) => setPage(Math.max(1, Math.min(totalPages(), next)))

  return (
    <section class="space-y-4">
      <header class="space-y-1">
        <h2 class="text-xl font-semibold text-neutral-900 dark:text-neutral-100">{props.title}</h2>
        <p class="text-sm text-neutral-500 dark:text-neutral-400">{props.description}</p>
      </header>

      <div class="grid gap-2 rounded border border-neutral-200 bg-white/90 p-3 md:grid-cols-[1fr_auto_auto_auto] dark:border-neutral-800 dark:bg-neutral-900/60">
        <input
          value={query()}
          onInput={(event) => {
            setQuery(event.currentTarget.value)
            setPage(1)
          }}
          placeholder={`Search ${props.title.toLowerCase()}...`}
          class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        />

        <select
          value={sortKey()}
          onChange={(event) => {
            setSortKey(event.currentTarget.value as 'name' | 'health' | 'updatedAt')
            setPage(1)
          }}
          class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        >
          <For each={props.sortOptions}>{(option) => <option value={option.key}>{option.label}</option>}</For>
        </select>

        <button
          type="button"
          onClick={() => {
            setSortDirection((current) => (current === 'asc' ? 'desc' : 'asc'))
            setPage(1)
          }}
          class="rounded border border-neutral-300 px-3 py-2 text-sm dark:border-neutral-700"
        >
          {sortDirection() === 'asc' ? 'Ascending' : 'Descending'}
        </button>

        <select
          value={String(pageSize())}
          onChange={(event) => {
            setPageSize(Number(event.currentTarget.value))
            setPage(1)
          }}
          class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        >
          <For each={pageSizes}>{(size) => <option value={size}>{size} / page</option>}</For>
        </select>
      </div>

      <Show when={!props.loading} fallback={<p class="text-sm text-neutral-500">Loading {props.title.toLowerCase()}…</p>}>
        <Show when={!props.error} fallback={<p class="text-sm text-red-500">Failed to load {props.title.toLowerCase()}.</p>}>
          <Show when={paged().length > 0} fallback={<p class="text-sm text-neutral-500">No results.</p>}>
            <div class="space-y-2">
              <For each={paged()}>
                {(item) => (
                  <A href={item.href} class="block rounded border border-neutral-200 bg-white/90 p-3 hover:border-neutral-400 dark:border-neutral-800 dark:bg-neutral-900/60">
                    <div class="flex items-center justify-between gap-2">
                      <p class="font-medium text-neutral-900 dark:text-neutral-100">{item.name}</p>
                      <span class="text-xs uppercase text-neutral-500">{item.health}</span>
                    </div>
                    <p class="mt-1 text-xs text-neutral-500">Updated {item.updatedAt}</p>
                  </A>
                )}
              </For>
            </div>
          </Show>

          <div class="flex items-center justify-between rounded border border-neutral-200 bg-white/90 px-3 py-2 text-sm dark:border-neutral-800 dark:bg-neutral-900/60">
            <span>Page {page()} of {totalPages()}</span>
            <div class="flex gap-2">
              <button type="button" onClick={() => updatePage(page() - 1)} disabled={page() <= 1} class="rounded border border-neutral-300 px-2 py-1 disabled:opacity-50 dark:border-neutral-700">Previous</button>
              <button type="button" onClick={() => updatePage(page() + 1)} disabled={page() >= totalPages()} class="rounded border border-neutral-300 px-2 py-1 disabled:opacity-50 dark:border-neutral-700">Next</button>
            </div>
          </div>
        </Show>
      </Show>
    </section>
  )
}
