import { A, useNavigate } from '@solidjs/router'
import { createEffect, For, onCleanup, onMount, Show, createSignal, type JSX } from 'solid-js'
import { clearCommandPaletteIndexItems, setCommandPaletteIndexItems } from '../utils/commandPalette'
import { JUMP_MODIFIER_LABEL, indexHintKey, useIndexJumpTargets } from '../utils/jumpHints'
import { Button, EmptyState, Icon, JumpHint, LoadingRows, Note, SectionCard, StatusBadge, cx, inputClass, jumpHintGutterClass, selectClass, tableClass, tdClass, thClass, theadClass, trHoverClass } from './ui'
import type { Tone } from '../utils/status'

export type IndexSortOption = { label: string; value: string }

// Extra, entity-specific columns rendered between Status and Updated.
export type IndexColumn = { key: string; header: string; class?: string }

export type IndexItem = {
  id: string
  title: string
  subtitle?: string
  status?: string
  statusTone?: Tone
  updatedAt: string
  /// Optional tooltip on the relative time — used for the node list's
  /// heartbeat freshness caveat (docs/node-liveness.md).
  updatedAtTitle?: string
  href: string
  cells?: Record<string, JSX.Element>
}

type SharedIndexProps = {
  entityLabel: string
  search: string
  onSearch: (value: string) => void
  sort: string
  onSort: (value: string) => void
  sortOptions: readonly IndexSortOption[]
  columns?: readonly IndexColumn[]
  items: readonly IndexItem[]
  loading: boolean
  error?: string
  page: number
  pageSize: number
  totalCount: number
  onPageChange: (page: number) => void
  onPageSizeChange: (size: number) => void
  emptyLabel: string
  emptyHint?: string
  extraFilters?: JSX.Element
  // Page-level actions, rendered at the end of the filter row.
  actions?: JSX.Element
  // Hide the Status column for entities that have no meaningful status (e.g. manager config snapshots).
  hideStatus?: boolean
}

export default function SharedIndex(props: SharedIndexProps) {
  const totalPages = () => Math.max(1, Math.ceil(props.totalCount / props.pageSize))
  const [selectedIndex, setSelectedIndex] = createSignal(-1)
  const navigate = useNavigate()
  let indexListRef: HTMLTableSectionElement | undefined

  createEffect(() => {
    setCommandPaletteIndexItems(
      props.items.map((item) => ({
        id: `${props.entityLabel}:${item.id}`,
        label: item.title,
        subtitle: item.subtitle,
        href: item.href,
        source: props.entityLabel,
      })),
    )
  })

  useIndexJumpTargets(() => props.items.map((item) => item.href))

  createEffect(() => {
    const total = props.items.length
    if (total === 0) {
      setSelectedIndex(-1)
      return
    }
    setSelectedIndex((value) => (value >= total ? total - 1 : value))
  })

  const isTextInputTarget = (event: KeyboardEvent): boolean => {
    const target = event.target as HTMLElement | null
    if (!target || !target.closest) return false
    if (target.closest('[data-command-palette]')) return true
    if (target.closest('input, textarea, select')) return true
    if (target.closest('[contenteditable]')) return true
    return false
  }

  const scrollSelectedItemIntoView = () => {
    const index = selectedIndex()
    if (index < 0) return
    const item = indexListRef?.querySelector(`[data-index-item="${index}"]`) as HTMLElement | null
    item?.scrollIntoView({ block: 'nearest' })
  }

  const selectNextIndexItem = () => {
    setSelectedIndex((value) => {
      const total = props.items.length
      if (total === 0) return -1
      if (value < 0) return 0
      return Math.min(value + 1, total - 1)
    })
  }

  const selectPreviousIndexItem = () => {
    setSelectedIndex((value) => {
      const total = props.items.length
      if (total === 0) return -1
      if (value < 0) return total - 1
      return Math.max(value - 1, 0)
    })
  }

  const openSelectedIndexItem = () => {
    const item = props.items[selectedIndex()]
    if (!item) return
    navigate(item.href)
  }

  const onGlobalIndexNavigation = (event: KeyboardEvent) => {
    if (event.metaKey || event.ctrlKey || event.altKey) return
    if (document.querySelector('[data-command-palette]')) return
    if (isTextInputTarget(event)) return
    // Never steal activation from a focused link/button — Tab-focus and the
    // j/k virtual selection are separate systems and the focused element wins.
    const active = document.activeElement as HTMLElement | null
    if (active && active !== document.body && active.closest('a, button, [role="button"]')) return

    const key = event.key.toLowerCase()
    if (key === 'j' || event.key === 'ArrowDown') {
      event.preventDefault()
      selectNextIndexItem()
      return
    }
    if (key === 'k' || event.key === 'ArrowUp') {
      event.preventDefault()
      selectPreviousIndexItem()
      return
    }
    if (key === 'o' || event.key === 'Enter') {
      if (selectedIndex() < 0) return
      event.preventDefault()
      openSelectedIndexItem()
    }
  }

  onMount(() => {
    window.addEventListener('keydown', onGlobalIndexNavigation)
    onCleanup(() => {
      window.removeEventListener('keydown', onGlobalIndexNavigation)
    })
  })

  createEffect(() => {
    scrollSelectedItemIntoView()
  })

  onCleanup(() => {
    clearCommandPaletteIndexItems()
  })

  return (
    <div class="space-y-3">
      <div class="flex flex-wrap items-center gap-2">
        <div class="relative min-w-[14rem] flex-1">
          <Icon name="search" size={14} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
          <input
            value={props.search}
            onInput={(event) => props.onSearch(event.currentTarget.value)}
            placeholder={`Search ${props.entityLabel.toLowerCase()}…`}
            class={cx(inputClass, 'pl-8')}
            aria-label={`Search ${props.entityLabel.toLowerCase()}`}
          />
        </div>
        {props.extraFilters}
        <select
          value={props.sort}
          onChange={(event) => props.onSort(event.currentTarget.value)}
          class={cx(selectClass, 'w-auto')}
          aria-label="Sort"
        >
          <For each={props.sortOptions}>{(option) => <option value={option.value}>{option.label}</option>}</For>
        </select>
        {props.actions}
      </div>

      <SectionCard flush>
        {/* Keep the previous rows visible during a refetch — only show the
            skeleton when there is nothing to display yet. */}
        <Show when={!props.loading || props.items.length > 0} fallback={<LoadingRows rows={5} />}>
          <Show when={!props.error} fallback={<Note tone="danger" class="m-4">{props.error}</Note>}>
            <Show
              when={props.items.length > 0}
              fallback={
                <Show
                  when={props.search.trim()}
                  fallback={<EmptyState icon="search" title={props.emptyLabel} body={props.emptyHint} />}
                >
                  <EmptyState icon="search" title={`No ${props.entityLabel.toLowerCase()} match "${props.search.trim()}"`}>
                    <Button size="xs" class="mt-1" onClick={() => props.onSearch('')}>Clear search</Button>
                  </EmptyState>
                </Show>
              }
            >
              <div class="overflow-x-auto">
                <table class={tableClass}>
                  <thead class={theadClass}>
                    <tr>
                      <th class={cx(thClass, jumpHintGutterClass())}>{props.entityLabel.replace(/s$/, '')}</th>
                      <Show when={!props.hideStatus}>
                        <th class={thClass}>Status</th>
                      </Show>
                      <For each={props.columns ?? []}>{(column) => <th class={cx(thClass, column.class)}>{column.header}</th>}</For>
                      <th class={cx(thClass, 'text-right')}>Updated</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-line" ref={indexListRef} data-index-list>
                    <For each={props.items}>
                      {(item, index) => (
                        <tr
                          data-index-item={index()}
                          class={cx(trHoverClass, 'cursor-pointer', selectedIndex() === index() && 'bg-brass/[0.07]')}
                          onMouseEnter={() => setSelectedIndex(index())}
                          onClick={(event) => {
                            if ((event.target as HTMLElement).closest('a')) return
                            navigate(item.href)
                          }}
                        >
                          <td class={cx(tdClass, 'relative py-2.5', jumpHintGutterClass())}>
                            <JumpHint keys={indexHintKey(index())} class="absolute left-3 top-1/2 -translate-y-1/2" />
                            <A href={item.href} class="font-medium text-ink hover:text-brass-strong">
                              {item.title}
                            </A>
                            <Show when={item.subtitle}>
                              <p class="mt-0.5 text-xs text-faint">{item.subtitle}</p>
                            </Show>
                          </td>
                          <Show when={!props.hideStatus}>
                            <td class={tdClass}>
                              <Show when={item.status !== undefined} fallback={<span class="text-faint">—</span>}>
                                <StatusBadge status={item.status} tone={item.statusTone} />
                              </Show>
                            </td>
                          </Show>
                          <For each={props.columns ?? []}>
                            {(column) => <td class={cx(tdClass, 'text-muted', column.class)}>{item.cells?.[column.key] ?? '—'}</td>}
                          </For>
                          <td class={cx(tdClass, 'whitespace-nowrap text-right text-xs text-faint')} title={item.updatedAtTitle}>{item.updatedAt}</td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </Show>
          </Show>
        </Show>

        <footer class={cx('flex flex-wrap items-center justify-between gap-2 border-t border-line px-4 py-2 text-xs text-muted', props.totalCount === 0 && 'hidden')}>
          <span class="tabular-nums">
            {props.totalCount} total · page {props.page} of {totalPages()}
          </span>
          <div class="flex items-center gap-1.5">
            <select
              value={String(props.pageSize)}
              onChange={(event) => props.onPageSizeChange(Number.parseInt(event.currentTarget.value, 10))}
              class={cx(selectClass, 'w-auto py-1 text-xs')}
              aria-label="Rows per page"
            >
              <option value="10">10 / page</option>
              <option value="20">20 / page</option>
              <option value="50">50 / page</option>
            </select>
            <Button size="xs" icon="chevronLeft" disabled={props.page <= 1} onClick={() => props.onPageChange(props.page - 1)} aria-label="Previous page" />
            <Button size="xs" icon="chevronRight" disabled={props.page >= totalPages()} onClick={() => props.onPageChange(props.page + 1)} aria-label="Next page" />
          </div>
        </footer>
      </SectionCard>

      <p class="text-2xs text-faint">
        <kbd class="rounded border border-line bg-surface2 px-1 font-mono">j</kbd>/<kbd class="rounded border border-line bg-surface2 px-1 font-mono">k</kbd> to move ·{' '}
        <kbd class="rounded border border-line bg-surface2 px-1 font-mono">o</kbd> to open ·{' '}
        <kbd class="rounded border border-line bg-surface2 px-1 font-mono">{JUMP_MODIFIER_LABEL}</kbd> to number the rows ·{' '}
        <kbd class="rounded border border-line bg-surface2 px-1 font-mono">{JUMP_MODIFIER_LABEL}K</kbd> to search everything
      </p>
    </div>
  )
}
