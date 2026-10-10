import { A } from '@solidjs/router'
import { For, Show, createMemo, createSignal, onCleanup, onMount } from 'solid-js'
import { AccessGate } from '../components/AccessGate'
import { PressureExplanation, SnapshotEventSummary } from '../components/PressureHistory'
import type { SchedulerEvent } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { Badge, Button, Dot, EmptyState, Icon, RelativeTime, SectionCard, cx, inputClass, selectClass } from '../components/ui'
import { statusTone, type Tone } from '../utils/status'

const MAX_BUFFERED_EVENTS = 750
const RECONNECT_DELAY_MS = 2000

type ConnectionState = 'connecting' | 'live' | 'reconnecting'

const KIND_CATEGORIES = ['scheduler', 'eval', 'allocation', 'artifact', 'node', 'job', 'rollout', 'snapshot'] as const

const levelTone = (level: string): Tone => statusTone(level) === 'info' ? 'neutral' : statusTone(level)

const connectionTone = (state: ConnectionState): Tone => {
  if (state === 'live') return 'ok'
  if (state === 'reconnecting') return 'danger'
  return 'warn'
}

const eventCategory = (kind: string): string => kind.split('.')[0] ?? kind

function EventsContent() {
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
      // gRPC-Web streams have no "open" signal, so treat a stream that
      // survives its first moments without erroring as live even while quiet.
      let liveTimer: ReturnType<typeof setTimeout> | undefined
      try {
        setConnection((state) => (state === 'connecting' ? 'connecting' : 'reconnecting'))
        const stream = managerClient.streamEvents(
          { afterId: cursor },
          { signal: abortController.signal },
        )
        liveTimer = setTimeout(() => setConnection('live'), 1500)
        for await (const event of stream) {
          setConnection('live')
          cursor = event.id > cursor ? event.id : cursor
          appendEvents([event])
        }
      } catch (error) {
        if (abortController.signal.aborted) return
        console.error('Scheduler event stream interrupted; reconnecting', error)
      } finally {
        if (liveTimer) clearTimeout(liveTimer)
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
    <div class="space-y-3">
      <div class="flex flex-wrap items-center gap-2">
        <span class="flex items-center gap-2 px-1 text-xs font-medium text-muted" role="status">
          <Dot tone={connectionTone(connection())} pulse={connection() === 'live'} />
          {connectionLabel()}
        </span>
        <select
          class={cx(selectClass, 'w-auto')}
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
          class={cx(selectClass, 'w-auto')}
          value={levelFilter()}
          onChange={(event) => setLevelFilter(event.currentTarget.value)}
          aria-label="Filter by level"
        >
          <option value="all">All levels</option>
          <option value="info">info</option>
          <option value="warn">warn</option>
          <option value="error">error</option>
        </select>
        <div class="relative min-w-[14rem] flex-1">
          <Icon name="search" size={14} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
          <input
            type="search"
            placeholder="Search message, job, node…"
            class={cx(inputClass, 'pl-8')}
            value={search()}
            onInput={(event) => setSearch(event.currentTarget.value)}
          />
        </div>
        <Button icon={paused() ? 'play' : 'pause'} onClick={togglePaused}>
          {paused()
            ? `Resume${pendingWhilePaused().length > 0 ? ` (${pendingWhilePaused().length} new)` : ''}`
            : 'Pause'}
        </Button>
      </div>

      <SectionCard flush>
        <Show
          when={visibleEvents().length > 0}
          fallback={
            <EmptyState
              icon="radio"
              title="No scheduler events yet"
              body="Submit a job or join a worker and activity will appear here live."
            />
          }
        >
          <ul class="divide-y divide-line">
            <For each={visibleEvents()}>
              {(event) => (
                <li>
                  <button
                    type="button"
                    class="flex w-full flex-wrap items-center gap-2 px-3.5 py-2 text-left text-sm transition-colors hover:bg-surface2/50"
                    onClick={() => setExpandedId((current) => (current === event.id ? null : event.id))}
                    aria-expanded={expandedId() === event.id}
                  >
                    <Badge tone={levelTone(event.level)} class="uppercase">{event.level}</Badge>
                    <span class="font-mono text-xs text-faint">{event.kind}</span>
                    <span class="min-w-0 flex-1 truncate text-ink">{event.message}</span>
                    <Show when={event.jobId}>
                      <A
                        href={`/jobs/${encodeURIComponent(event.jobId!)}`}
                        class="rounded bg-surface2 px-1.5 py-0.5 font-mono text-2xs text-muted hover:text-brass-strong"
                        onClick={(clickEvent) => clickEvent.stopPropagation()}
                      >
                        job={event.jobId}
                      </A>
                    </Show>
                    <Show when={event.nodeId}>
                      <A
                        href={`/nodes/${encodeURIComponent(event.nodeId!)}`}
                        class="rounded bg-surface2 px-1.5 py-0.5 font-mono text-2xs text-muted hover:text-brass-strong"
                        onClick={(clickEvent) => clickEvent.stopPropagation()}
                      >
                        node={event.nodeId}
                      </A>
                    </Show>
                    <RelativeTime value={event.createdAt} class="whitespace-nowrap text-xs text-faint" />
                  </button>
                  <Show when={expandedId() === event.id}>
                    <div class="space-y-2 border-t border-line bg-surface2/40 px-3.5 py-2.5 text-xs">
                      <p class="flex flex-wrap gap-x-4 gap-y-1 text-muted">
                        <span><span class="font-semibold text-faint">id</span> {String(event.id)}</span>
                        <Show when={event.evalId}>
                          <span><span class="font-semibold text-faint">eval</span> {event.evalId}</span>
                        </Show>
                        <Show when={event.allocationId !== undefined}>
                          <span><span class="font-semibold text-faint">allocation</span> {String(event.allocationId)}</span>
                        </Show>
                        <Show when={event.rolloutId !== undefined}>
                          <span><span class="font-semibold text-faint">rollout</span> {String(event.rolloutId)}</span>
                        </Show>
                        <span><span class="font-semibold text-faint">at</span> {event.createdAt}</span>
                      </p>
                      {/* Kinds whose metadata answers a question on its own
                          get it spelled out above the raw JSON. */}
                      <Show when={event.kind === 'node.pressure'}>
                        <PressureExplanation event={event} />
                      </Show>
                      <Show when={eventCategory(event.kind) === 'snapshot'}>
                        <SnapshotEventSummary event={event} />
                      </Show>
                      <pre class="overflow-auto rounded-md border border-line bg-surface p-2 font-mono text-2xs leading-relaxed text-muted">
                        {formatMetadata(event.metadataJson)}
                      </pre>
                    </div>
                  </Show>
                </li>
              )}
            </For>
          </ul>
        </Show>
      </SectionCard>
    </div>
  )
}

const formatMetadata = (metadataJson: string): string => {
  try {
    return JSON.stringify(JSON.parse(metadataJson), null, 2)
  } catch {
    return metadataJson
  }
}

// Permission gate lives here at the page level — never around the router
// outlet in the layout (see AccessGate).
export default function Events() {
  return (
    <AccessGate resource="operator">
      <EventsContent />
    </AccessGate>
  )
}
