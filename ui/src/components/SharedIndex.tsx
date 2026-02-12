import { A } from '@solidjs/router'
import { For, Show, type JSX } from 'solid-js'

export type IndexSortOption = { label: string; value: string }

export type IndexItem = {
  id: string
  title: string
  health: string
  healthClass: string
  updatedAt: string
  href: string
  subtitle?: string
}

type SharedIndexProps = {
  entityLabel: string
  search: string
  onSearch: (value: string) => void
  sort: string
  onSort: (value: string) => void
  sortOptions: readonly IndexSortOption[]
  items: readonly IndexItem[]
  loading: boolean
  error?: string
  page: number
  pageSize: number
  totalCount: number
  onPageChange: (page: number) => void
  onPageSizeChange: (size: number) => void
  emptyLabel: string
  extraFilters?: JSX.Element
}

export default function SharedIndex(props: SharedIndexProps) {
  const totalPages = () => Math.max(1, Math.ceil(props.totalCount / props.pageSize))

  return (
    <section class="space-y-4 rounded border border-neutral-200 bg-white/90 p-4 shadow-[5px_5px_0_#d4d4d8] dark:border-neutral-800 dark:bg-neutral-900/60 dark:shadow-[5px_5px_0_#0b0d12]">
      <header class="grid gap-2 md:grid-cols-[1fr_180px_130px]">
        <input
          value={props.search}
          onInput={(event) => props.onSearch(event.currentTarget.value)}
          placeholder={`Search ${props.entityLabel.toLowerCase()}...`}
          class="w-full rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        />
        <select
          value={props.sort}
          onChange={(event) => props.onSort(event.currentTarget.value)}
          class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        >
          <For each={props.sortOptions}>{(option) => <option value={option.value}>{option.label}</option>}</For>
        </select>
        <select
          value={String(props.pageSize)}
          onChange={(event) => props.onPageSizeChange(Number.parseInt(event.currentTarget.value, 10))}
          class="rounded border border-neutral-300 bg-white px-3 py-2 text-sm dark:border-neutral-700 dark:bg-neutral-900"
        >
          <option value="10">10 / page</option>
          <option value="20">20 / page</option>
          <option value="50">50 / page</option>
        </select>
      </header>

      <Show when={props.extraFilters}>{props.extraFilters}</Show>

      <Show when={!props.loading} fallback={<p class="text-sm text-neutral-500">Loading {props.entityLabel.toLowerCase()}…</p>}>
        <Show when={!props.error} fallback={<p class="text-sm text-rose-500">{props.error}</p>}>
          <Show when={props.items.length > 0} fallback={<p class="text-sm text-neutral-500">{props.emptyLabel}</p>}>
            <div class="space-y-2">
              <For each={props.items}>
                {(item) => (
                  <A
                    href={item.href}
                    class="block rounded border border-neutral-200 bg-white p-3 transition hover:border-neutral-400 dark:border-neutral-700 dark:bg-neutral-950"
                  >
                    <div class="flex items-center justify-between gap-2">
                      <div>
                        <div class="font-semibold">{item.title}</div>
                        <Show when={item.subtitle}>
                          <p class="text-xs text-neutral-500 dark:text-neutral-400">{item.subtitle}</p>
                        </Show>
                      </div>
                      <span class={`rounded border px-2 py-0.5 text-[11px] uppercase tracking-wide ${item.healthClass}`}>
                        {item.health}
                      </span>
                    </div>
                    <p class="mt-1 text-xs text-neutral-500 dark:text-neutral-400">Updated {item.updatedAt}</p>
                  </A>
                )}
              </For>
            </div>
          </Show>
        </Show>
      </Show>

      <footer class="flex items-center justify-between text-xs text-neutral-600 dark:text-neutral-300">
        <span>{props.totalCount} total</span>
        <div class="flex items-center gap-2">
          <button
            type="button"
            disabled={props.page <= 1}
            onClick={() => props.onPageChange(props.page - 1)}
            class="rounded border border-neutral-300 px-2 py-1 disabled:opacity-50 dark:border-neutral-700"
          >
            Prev
          </button>
          <span>
            Page {props.page} / {totalPages()}
          </span>
          <button
            type="button"
            disabled={props.page >= totalPages()}
            onClick={() => props.onPageChange(props.page + 1)}
            class="rounded border border-neutral-300 px-2 py-1 disabled:opacity-50 dark:border-neutral-700"
          >
            Next
          </button>
        </div>
      </footer>
    </section>
  )
}
