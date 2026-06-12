import { For, Show, createMemo, createSignal, onCleanup, onMount } from 'solid-js'
import type { SchedulerEvent } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { formatRelativeTime } from '../utils/format'

const MAX_BUFFERED_EVENTS = 750
const RECONNECT_DELAY_MS = 2000

type ConnectionState = 'connecting' | 'live' | 'reconnecting'

const KIND_CATEGORIES = ['scheduler', 'eval', 'allocation', 'artifact', 'node', 'job', 'rollout'] as const

const levelClasses = (level: string): string => {
  if (level === 'error') {
    return 'border-rose-300 bg-rose-50 text-rose-700 dark:border-rose-600 dark:bg-rose-950 dark:text-rose-100'
  }
  if (level === 'warn') {
    return 'border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-600 dark:bg-amber-950 dark:text-amber-100'
  }
  return 'border-neutral-300 bg-neutral-50 text-neutral-600 dark:border-neutral-700 dark:bg-neutral-900 dark:text-neutral-300'
}

const connectionClasses = (state: ConnectionState): string => {
  if (state === 'live') return 'bg-emerald-500'
  if (state === 'reconnecting') return 'bg-rose-500'
  return 'bg-amber-500'
}

const eventCategory = (kind: string): string => kind.split('.')[0] ?? kind

export default function Events() {
  const [events, setEvents] = createSignal<SchedulerEvent[]>([])
  const [pendingWhilePaused, setPendingWhilePaused] = createSignal<SchedulerEvent[]>([])
  const [paused, setPaused] = createSignal(false)
  const [connection, setConnection] = createSignal<ConnectionState>('connecting')
  const [categoryFilter, setCategoryFilter] = createSignal('all')
  const [levelFilter, setLevelFilter] = createSignal('all')
  const [search, setSearch] = createSignal('')
  const [expandedId, setExpandedId] = createSignal<bigint | null>(null)

  let pausedNow = false
  const abortController = new AbortController()

  const appendEvents = (incoming: SchedulerEvent[]) => {
    if (incoming.length === 0) return
    if (pausedNow) {
      setPendingWhilePaused((current) => [...current, ...incoming].slice(-MAX_BUFFERED_EVENTS))
      return
    }
    setEvents((current) => [...current, ...incoming].slice(-MAX_BUFFERED_EVENTS))
  }

  const togglePaused = () => {
    const next = !paused()
    pausedNow = next
    setPaused(next)
    if (!next) {
      const pending = pendingWhilePaused()
      setPendingWhilePaused([])
      setEvents((current) => [...current, ...pending].slice(-MAX_BUFFERED_EVENTS))
    }
  }

  // Tail the manager's StreamEvents RPC forever, resuming from the last seen
  // event ID after any disconnect so the feed has no gaps.
  const streamForever = async () => {
    let cursor = 0n

    try {
      const backlog = await managerClient.listEvents({ limit: 200 }, { signal: abortController.signal })
      appendEvents([...backlog.events])
      cursor = backlog.latestId
    } catch (error) {
      if (abortController.signal.aborted) return
      console.error('Failed to load scheduler event backlog', error)
    }

    while (!abortController.signal.aborted) {
      try {
        setConnection((state) => (state === 'connecting' ? 'connecting' : 'reconnecting'))
        const stream = managerClient.streamEvents(
          { afterId: cursor },
          { signal: abortController.signal },
        )
        for await (const event of stream) {
          setConnection('live')
          cursor = event.id > cursor ? event.id : cursor
          appendEvents([event])
        }
      } catch (error) {
        if (abortController.signal.aborted) return
        console.error('Scheduler event stream interrupted; reconnecting', error)
      }
      setConnection('reconnecting')
      await new Promise((resolve) => setTimeout(resolve, RECONNECT_DELAY_MS))
    }
  }

  onMount(() => {
    void streamForever()
  })
  onCleanup(() => abortController.abort())

  const visibleEvents = createMemo(() => {
    const category = categoryFilter()
    const level = levelFilter()
    const query = search().trim().toLowerCase()

    const matches = events().filter((event) => {
      if (category !== 'all' && eventCategory(event.kind) !== category) return false
      if (level !== 'all' && event.level !== level) return false
      if (query) {
        const haystack = [
          event.kind,
          event.message,
          event.jobId ?? '',
          event.nodeId ?? '',
          event.evalId ?? '',
        ]
          .join(' ')
          .toLowerCase()
        if (!haystack.includes(query)) return false
      }
      return true
    })

    // Newest first for a tailing feed.
    return matches.reverse()
  })

  const connectionLabel = createMemo(() => {
    if (connection() === 'live') return 'Live'
    if (connection() === 'reconnecting') return 'Reconnecting…'
    return 'Connecting…'
  })

  return (
    <section class="space-y-4">
      <div class="flex flex-wrap items-center gap-3 rounded border border-neutral-200 bg-white/90 px-4 py-3 dark:border-neutral-800 dark:bg-neutral-900/60">
        <span class="flex items-center gap-2 text-sm">
          <span class={`h-2.5 w-2.5 rounded-full ${connectionClasses(connection())}`} />
          {connectionLabel()}
        </span>
        <select
          class="rounded border border-neutral-300 bg-white px-2 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-950"
          value={categoryFilter()}
          onChange={(event) => setCategoryFilter(event.currentTarget.value)}
          aria-label="Filter by event category"
        >
          <option value="all">All categories</option>
          <For each={[...KIND_CATEGORIES]}>
            {(category) => <option value={category}>{category}</option>}
          </For>
        </select>
        <select
          class="rounded border border-neutral-300 bg-white px-2 py-1.5 text-sm dark:border-neutral-700 dark:bg-neutral-950"
          value={levelFilter()}
          onChange={(event) => setLevelFilter(event.currentTarget.value)}
          aria-label="Filter by level"
        >
          <option value="all">All levels</option>
          <option value="info">info</option>
          <option value="warn">warn</option>
          <option value="error">error</option>
        </select>
        <input
          type="search"
          placeholder="Search message, job, node…"
          class="min-w-[14rem] flex-1 rounded border border-neutral-300 bg-white px-3 py-1.5 text-sm placeholder:text-neutral-500 dark:border-neutral-700 dark:bg-neutral-950 dark:placeholder:text-neutral-400"
          value={search()}
          onInput={(event) => setSearch(event.currentTarget.value)}
        />
        <button
          type="button"
          class="rounded border border-neutral-300 px-3 py-1.5 text-sm font-semibold text-neutral-700 hover:border-neutral-500 dark:border-neutral-700 dark:text-neutral-200"
          onClick={togglePaused}
        >
          {paused()
            ? `Resume${pendingWhilePaused().length > 0 ? ` (${pendingWhilePaused().length} new)` : ''}`
            : 'Pause'}
        </button>
      </div>

      <Show
        when={visibleEvents().length > 0}
        fallback={
          <p class="rounded border border-neutral-200 bg-white/90 px-4 py-6 text-sm text-neutral-500 dark:border-neutral-800 dark:bg-neutral-900/60 dark:text-neutral-400">
            No scheduler events yet. Submit a job or join a worker and activity will appear here live.
          </p>
        }
      >
        <ul class="space-y-1.5">
          <For each={visibleEvents()}>
            {(event) => (
              <li class="rounded border border-neutral-200 bg-white/90 px-3 py-2 text-sm dark:border-neutral-800 dark:bg-neutral-900/60">
                <button
                  type="button"
                  class="flex w-full flex-wrap items-center gap-2 text-left"
                  onClick={() => setExpandedId((current) => (current === event.id ? null : event.id))}
                >
                  <span class={`rounded border px-1.5 py-0.5 text-[11px] font-semibold uppercase ${levelClasses(event.level)}`}>
                    {event.level}
                  </span>
                  <span class="font-mono text-xs text-neutral-500 dark:text-neutral-400">{event.kind}</span>
                  <span class="min-w-0 flex-1 truncate text-neutral-900 dark:text-neutral-100">{event.message}</span>
                  <Show when={event.jobId}>
                    <span class="rounded bg-neutral-100 px-1.5 py-0.5 font-mono text-[11px] text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300">
                      job={event.jobId}
                    </span>
                  </Show>
                  <Show when={event.nodeId}>
                    <span class="rounded bg-neutral-100 px-1.5 py-0.5 font-mono text-[11px] text-neutral-600 dark:bg-neutral-800 dark:text-neutral-300">
                      node={event.nodeId}
                    </span>
                  </Show>
                  <span class="text-xs text-neutral-500 dark:text-neutral-400" title={event.createdAt}>
                    {formatRelativeTime(event.createdAt)}
                  </span>
                </button>
                <Show when={expandedId() === event.id}>
                  <div class="mt-2 space-y-1 border-t border-neutral-200 pt-2 text-xs dark:border-neutral-800">
                    <p class="text-neutral-600 dark:text-neutral-300">
                      <span class="font-semibold">id</span> {String(event.id)}
                      <Show when={event.evalId}>
                        <span class="ml-3 font-semibold">eval</span> {event.evalId}
                      </Show>
                      <Show when={event.allocationId !== undefined}>
                        <span class="ml-3 font-semibold">allocation</span> {String(event.allocationId)}
                      </Show>
                      <Show when={event.rolloutId !== undefined}>
                        <span class="ml-3 font-semibold">rollout</span> {String(event.rolloutId)}
                      </Show>
                      <span class="ml-3 font-semibold">at</span> {event.createdAt}
                    </p>
                    <pre class="overflow-auto rounded bg-neutral-100 p-2 font-mono text-[11px] text-neutral-700 dark:bg-neutral-950 dark:text-neutral-300">
                      {formatMetadata(event.metadataJson)}
                    </pre>
                  </div>
                </Show>
              </li>
            )}
          </For>
        </ul>
      </Show>
    </section>
  )
}

const formatMetadata = (metadataJson: string): string => {
  try {
    return JSON.stringify(JSON.parse(metadataJson), null, 2)
  } catch {
    return metadataJson
  }
}
