import { Code, ConnectError } from '@connectrpc/connect'
import type { PartialMessage } from '@bufbuild/protobuf'
import { A } from '@solidjs/router'
import { For, Show, createEffect, createMemo, createSignal, on, type Accessor } from 'solid-js'
import type { ListEventsRequest, SchedulerEvent } from '../gen/legion_pb'
import { managerClient } from '../grpcClient'
import { canRead } from '../utils/acl'
import { managerIdentity } from '../utils/connection'
import { newestFirst, parseEventMeta } from '../utils/events'
import { formatDateTime, toDate } from '../utils/format'
import { instanceHref } from '../utils/instances'
import { now } from '../utils/polling'
import { dotToneClasses, statusTone, type Tone } from '../utils/status'
import { InstanceEventSummary, PressureExplanation, SnapshotEventSummary } from './PressureHistory'
import { Badge, Icon, RelativeTime, cx } from './ui'

/*
 * A scoped slice of the scheduler event journal (docs/event-stream.md) as a
 * timeline: the instance page's history, and any other page that wants the
 * events about one thing (`ListEvents` takes `instance_id`, `allocation_id`,
 * `job_id`, `node_id`, `kind`).
 *
 * `createEventFeed` loads the latest page once and then polls with
 * `after_id` = the newest id it holds, appending what arrives, so a row
 * never remounts and an expanded row stays open across polls. The journal
 * needs `operator:read`; without it the feed reports `denied` once and
 * stops asking, rather than failing the page around it.
 */

export type EventFeedFilter = Omit<PartialMessage<ListEventsRequest>, 'afterId' | 'limit'>

export type EventFeedStatus = 'loading' | 'ready' | 'denied' | 'error'

export type EventFeed = {
  /** Oldest first, as the manager returns them. */
  events: Accessor<readonly SchedulerEvent[]>
  status: Accessor<EventFeedStatus>
  /** The last failure's message, kept while older events stay on screen. */
  error: Accessor<string | undefined>
  /** The first page came back full, so older matching events may exist past it. */
  truncated: Accessor<boolean>
}

const keyOf = (filter: EventFeedFilter): string =>
  JSON.stringify(filter, (_key, value: unknown) => (typeof value === 'bigint' ? value.toString() : value))

const isDenied = (error: unknown) =>
  error instanceof ConnectError && (error.code === Code.PermissionDenied || error.code === Code.Unauthenticated)

/**
 * Events matching `filter`, kept current by `tick` (pass a `usePoll()`).
 * An undefined filter waits; a changed filter starts over.
 */
export function createEventFeed(
  filter: Accessor<EventFeedFilter | undefined>,
  tick: Accessor<number>,
  options: { limit?: number; max?: number } = {},
): EventFeed {
  const limit = options.limit ?? 200
  const max = options.max ?? 1000
  const [events, setEvents] = createSignal<readonly SchedulerEvent[]>([])
  const [status, setStatus] = createSignal<EventFeedStatus>('loading')
  const [error, setError] = createSignal<string>()
  const [truncated, setTruncated] = createSignal(false)

  let key: string | undefined
  // Exclusive cursor: the newest id held. Unset until something matched,
  // so the next poll asks for the latest page again instead of trusting the
  // manager's global high-water mark.
  let cursor: bigint | undefined
  let seen = new Set<bigint>()
  let generation = 0
  let inflight = false
  let loadedOnce = false
  // The identity the manager refused, so a token switch tries again.
  let deniedIdentity: unknown

  createEffect(
    on([filter, tick], ([current]) => {
      const nextKey = current ? keyOf(current) : undefined
      if (nextKey !== key) {
        key = nextKey
        generation += 1
        cursor = undefined
        seen = new Set()
        inflight = false
        loadedOnce = false
        deniedIdentity = undefined
        setEvents([])
        setStatus('loading')
        setError(undefined)
        setTruncated(false)
      }
      if (!current) return
      // Cosmetic gate first: the manager would refuse anyway.
      if (!canRead('operator')) {
        setStatus('denied')
        return
      }
      if (status() === 'denied' && deniedIdentity === managerIdentity()) return
      if (inflight) return
      inflight = true
      const run = generation
      const request: PartialMessage<ListEventsRequest> = { ...current, limit }
      if (cursor !== undefined) request.afterId = cursor
      managerClient
        .listEvents(request)
        .then((reply) => {
          if (run !== generation) return
          const fresh = reply.events.filter((event) => !seen.has(event.id))
          if (fresh.length > 0) {
            for (const event of fresh) {
              seen.add(event.id)
              if (cursor === undefined || event.id > cursor) cursor = event.id
            }
            setEvents((held) => {
              const merged = [...held, ...fresh]
              // Each reply is ordered, and later replies start past the
              // cursor, so this only sorts after an unusual interleave.
              const ordered = merged.every((event, index) => index === 0 || merged[index - 1].id < event.id)
                ? merged
                : merged.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
              return ordered.length > max ? ordered.slice(-max) : ordered
            })
          }
          if (!loadedOnce) {
            loadedOnce = true
            setTruncated(reply.events.length >= limit)
          }
          setStatus('ready')
          setError(undefined)
        })
        .catch((failure: unknown) => {
          if (run !== generation) return
          if (isDenied(failure)) {
            deniedIdentity = managerIdentity()
            setStatus('denied')
            return
          }
          setError(failure instanceof Error ? failure.message : String(failure))
          // Keep what is on screen: one failed poll is not an empty history.
          if (!loadedOnce) setStatus('error')
        })
        .finally(() => {
          if (run === generation) inflight = false
        })
    }),
  )

  return { events, status, error, truncated }
}

/** `instance.suspended` → `instance`. */
export const eventCategory = (kind: string): string => kind.split('.')[0] ?? kind

/**
 * The dot's color: the level when it is a warning or an error, else what
 * the kind's verb means (`placed` reads ok, `suspended` accent), else
 * neutral.
 */
export const eventTone = (event: SchedulerEvent): Tone => {
  if (event.level === 'error') return 'danger'
  if (event.level === 'warn') return 'warn'
  const verb = event.kind.split('.').slice(1).join('.')
  const tone = statusTone(verb.replace(/_/g, ' '))
  return tone === 'danger' || tone === 'warn' ? 'neutral' : tone
}

const levelTone = (level: string): Tone => (level === 'error' ? 'danger' : level === 'warn' ? 'warn' : 'neutral')

const formatMetadata = (raw: string): string => {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

/** Instances an event names besides `self`: a fork's origin or forks, the instances a make-room suspended. */
const relatedInstances = (event: SchedulerEvent, self: string | undefined): string[] => {
  const meta = parseEventMeta(event)
  const ids = new Set<string>()
  for (const key of ['instanceId', 'instance_id', 'origin']) {
    const value = meta[key]
    if (typeof value === 'string' && value) ids.add(value)
  }
  for (const key of ['instances', 'suspending']) {
    const value = meta[key]
    if (Array.isArray(value)) for (const item of value) if (typeof item === 'string' && item) ids.add(item)
  }
  if (self) ids.delete(self)
  return [...ids].sort()
}

const dayKey = (event: SchedulerEvent): string => toDate(event.createdAt)?.toDateString() ?? ''

const dayFormat = new Intl.DateTimeFormat('en', { weekday: 'short', month: 'short', day: 'numeric' })
const dayYearFormat = new Intl.DateTimeFormat('en', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' })

const dayLabel = (event: SchedulerEvent): string => {
  const date = toDate(event.createdAt)
  if (!date) return 'Unknown date'
  const today = new Date(now())
  const yesterday = new Date(today)
  yesterday.setDate(today.getDate() - 1)
  if (date.toDateString() === today.toDateString()) return 'Today'
  if (date.toDateString() === yesterday.toDateString()) return 'Yesterday'
  return (date.getFullYear() === today.getFullYear() ? dayFormat : dayYearFormat).format(date)
}

const chipClass = 'rounded bg-surface2 px-1.5 py-0.5 font-mono text-2xs text-muted transition-colors hover:text-brass-strong'

/**
 * Events newest first on a rail, grouped by day. Each row shows when (with
 * the exact time on hover), the kind, the level when it is not `info`, the
 * allocation and node it names, and the message; it expands to the fields
 * worth reading and the raw metadata.
 */
export function EventTimeline(props: {
  events: readonly SchedulerEvent[]
  /** The instance the page is about: other instances an event names become links. */
  instanceId?: string
  /** For instance links on events that carry no job. */
  jobId?: string
  class?: string
}) {
  const ordered = createMemo(() => newestFirst(props.events))
  const [expanded, setExpanded] = createSignal<ReadonlySet<string>>(new Set())
  const toggle = (id: string) =>
    setExpanded((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  return (
    <ol class={cx('relative', props.class)}>
      <For each={ordered()}>
        {(event, index) => {
          const id = String(event.id)
          const open = () => expanded().has(id)
          const firstOfDay = () => index() === 0 || dayKey(ordered()[index() - 1]) !== dayKey(event)
          const related = relatedInstances(event, props.instanceId)
          const job = event.jobId ?? props.jobId
          const category = eventCategory(event.kind)
          return (
            <>
              <Show when={firstOfDay()}>
                <li aria-hidden="true" class="relative border-b border-line bg-surface2/40 py-1 pl-9 pr-4">
                  <span class="eyebrow">{dayLabel(event)}</span>
                </li>
              </Show>
              <li class="relative border-b border-line py-2.5 pl-9 pr-4 last:border-b-0">
                {/* The rail and the dot. The dot carries the tone; the
                    level badge says it in words for warn and error. */}
                <span aria-hidden="true" class="absolute bottom-0 left-[17px] top-0 w-px bg-line" />
                <span
                  aria-hidden="true"
                  class={cx('absolute left-[13px] top-[15px] h-2 w-2 rounded-full ring-4 ring-surface', dotToneClasses[eventTone(event)])}
                />
                <div class="flex flex-wrap items-center gap-x-2 gap-y-1">
                  <button
                    type="button"
                    class="inline-flex min-w-0 items-center gap-1 rounded text-left font-mono text-xs font-medium text-ink transition-colors hover:text-brass-strong"
                    aria-expanded={open()}
                    onClick={() => toggle(id)}
                    title={open() ? 'Hide details' : 'Show details and metadata'}
                  >
                    <Icon name="chevronRight" size={12} class={cx('text-faint transition-transform', open() && 'rotate-90')} />
                    <span class="truncate">{event.kind}</span>
                  </button>
                  <Show when={event.level !== 'info'}>
                    <Badge tone={levelTone(event.level)} class="uppercase tracking-wide">
                      {event.level}
                    </Badge>
                  </Show>
                  <Show when={event.allocationId !== undefined}>
                    <A href={`/allocations/${String(event.allocationId)}`} class={chipClass} title="Open the allocation">
                      #{String(event.allocationId)}
                    </A>
                  </Show>
                  <Show when={event.nodeId}>
                    {(node) => (
                      <A href={`/nodes/${encodeURIComponent(node())}`} class={chipClass} title="Open the worker">
                        {node()}
                      </A>
                    )}
                  </Show>
                  <span class="ml-auto pl-2">
                    <RelativeTime value={event.createdAt} class="whitespace-nowrap text-2xs tabular-nums text-faint" />
                  </span>
                </div>
                <p class="mt-0.5 break-words text-sm text-muted">{event.message}</p>
                <Show when={related.length > 0 && job}>
                  <p class="mt-1 flex flex-wrap items-center gap-1.5 text-2xs text-faint">
                    <span>{event.kind.startsWith('instance.fork') ? 'Forks and origin' : 'Also about'}</span>
                    <For each={related}>
                      {(other) => (
                        <A href={instanceHref(job!, other)} class={chipClass}>
                          {other}
                        </A>
                      )}
                    </For>
                  </p>
                </Show>
                <Show when={open()}>
                  <div class="mt-2 space-y-2 rounded-md border border-line bg-surface2/40 p-2.5 text-xs">
                    <p class="flex flex-wrap gap-x-4 gap-y-1 text-muted">
                      <span>
                        <span class="font-semibold text-faint">at</span> {formatDateTime(event.createdAt)}
                      </span>
                      <span>
                        <span class="font-semibold text-faint">id</span> <span class="font-mono">{id}</span>
                      </span>
                      <Show when={event.evalId}>
                        <span>
                          <span class="font-semibold text-faint">eval</span> <span class="font-mono">{event.evalId}</span>
                        </span>
                      </Show>
                      <Show when={event.rolloutId !== undefined}>
                        <span>
                          <span class="font-semibold text-faint">rollout</span>{' '}
                          <A href={`/rollouts/${String(event.rolloutId)}`} class="font-mono hover:text-brass-strong">
                            #{String(event.rolloutId)}
                          </A>
                        </span>
                      </Show>
                    </p>
                    <Show when={event.kind === 'node.pressure'}>
                      <PressureExplanation event={event} />
                    </Show>
                    <Show when={category === 'snapshot'}>
                      <SnapshotEventSummary event={event} />
                    </Show>
                    <Show when={category === 'instance'}>
                      <InstanceEventSummary event={event} />
                    </Show>
                    <pre class="max-h-64 overflow-auto rounded-md border border-line bg-surface p-2 font-mono text-2xs leading-relaxed text-muted">
                      {formatMetadata(event.metadataJson)}
                    </pre>
                  </div>
                </Show>
              </li>
            </>
          )
        }}
      </For>
    </ol>
  )
}
