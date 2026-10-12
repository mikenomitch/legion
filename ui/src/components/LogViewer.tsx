import { Code, ConnectError } from '@connectrpc/connect'
import { For, Show, createEffect, createMemo, createSignal, onCleanup, onMount } from 'solid-js'
import { managerClient } from '../grpcClient'
import { AllocationLink } from './AllocationLink'
import { Button, Dot, Icon, Note, cx, inputClass, selectClass } from './ui'

// Live-tailing task log viewer.
//
// Tailing best practices implemented here:
// - polls the manager and appends only newly-written bytes (tracked per
//   allocation+stream byte offset), so the pane is incremental, not a re-render;
// - "follow" auto-scrolls to the newest line but pauses automatically when the
//   user scrolls up, and resumes when they return to the bottom;
// - stdout and stderr are visually distinguished;
// - line wrapping is toggleable; the buffer is bounded to keep the DOM light;
// - copy/download/clear and a per-allocation filter are available.

type LogViewerProps = {
  job: string
  // When set, restrict to these allocation ids (e.g. a single task's allocations).
  allocationIds?: bigint[]
  // Allocation id the allocation filter starts on (a deep link to one
  // instance). Read once at mount; the picker owns the filter after that.
  initialAllocation?: string
}

type LogLine = {
  id: number
  alloc: string
  stream: string
  text: string
}

const POLL_MS = 1200
const MAX_LINES = 5000
const WINDOW_BYTES = 256 * 1024

type StreamFilter = 'all' | 'stdout' | 'stderr'

export default function LogViewer(props: LogViewerProps) {
  const [lines, setLines] = createSignal<LogLine[]>([])
  const [streamFilter, setStreamFilter] = createSignal<StreamFilter>('all')
  const [allocFilter, setAllocFilter] = createSignal<string>(props.initialAllocation ?? 'all')
  const [knownAllocs, setKnownAllocs] = createSignal<string[]>([])
  const [follow, setFollow] = createSignal(true)
  const [wrap, setWrap] = createSignal(true)
  const [paused, setPaused] = createSignal(false)
  const [lastUpdated, setLastUpdated] = createSignal<number | undefined>()
  const [error, setError] = createSignal<string>('')
  const [truncated, setTruncated] = createSignal(false)
  // A grep over the buffer: matching lines only, with the hit highlighted.
  const [filter, setFilter] = createSignal('')

  // Non-reactive bookkeeping for incremental reads.
  const offsets = new Map<string, bigint>() // `${alloc}:${stream}` -> next byte offset
  const partials = new Map<string, string>() // trailing partial line per stream
  let counter = 0
  let container: HTMLDivElement | undefined
  let timer: ReturnType<typeof setInterval> | undefined

  const allowedAllocs = () => {
    const ids = props.allocationIds
    return ids && ids.length > 0 ? new Set(ids.map((id) => id.toString())) : undefined
  }
  // One allocation is read directly (`allocation_id`), so the manager proxies
  // a single worker read instead of one per allocation of the whole job.
  const singleAlloc = () => (props.allocationIds?.length === 1 ? props.allocationIds[0] : undefined)

  const appendChunk = (alloc: string, stream: string, chunk: string) => {
    const key = `${alloc}:${stream}`
    const combined = (partials.get(key) ?? '') + chunk
    const parts = combined.split('\n')
    partials.set(key, parts.pop() ?? '')
    if (parts.length === 0) return
    setLines((prev) => {
      const next = prev.concat(
        parts.map((text) => ({ id: counter++, alloc, stream, text })),
      )
      return next.length > MAX_LINES ? next.slice(next.length - MAX_LINES) : next
    })
  }

  const poll = async () => {
    if (paused()) return
    try {
      const resp = await managerClient.getAllocationLogs({
        job: props.job,
        allocationId: singleAlloc(),
        stream: streamFilter(),
        tail: true,
        maxBytes: BigInt(WINDOW_BYTES),
        offset: BigInt(0),
      })
      setError('')
      setLastUpdated(Date.now())

      const allowed = allowedAllocs()
      const seenAllocs = new Set(knownAllocs())
      let sawTruncation = false

      for (const alloc of resp.allocations) {
        const allocId = alloc.allocationId.toString()
        if (allowed && !allowed.has(allocId)) continue
        seenAllocs.add(allocId)

        if (alloc.error) {
          const key = `${allocId}:_error`
          if (!offsets.has(key)) {
            offsets.set(key, BigInt(1))
            appendChunk(allocId, 'stderr', `[legion] ${alloc.error}\n`)
          }
          continue
        }

        for (const s of alloc.streams) {
          if (s.truncated) sawTruncation = true
          const key = `${allocId}:${s.stream}`
          const prev = offsets.get(key)
          let chunk = ''
          if (prev === undefined) {
            chunk = s.content
          } else if (s.nextOffset <= prev) {
            chunk = ''
          } else if (prev >= s.startOffset) {
            chunk = s.content.slice(Number(prev - s.startOffset))
          } else {
            chunk = s.content
          }
          const newNext = s.nextOffset > (prev ?? BigInt(0)) ? s.nextOffset : (prev ?? BigInt(0))
          offsets.set(key, newNext)
          if (chunk) appendChunk(allocId, s.stream, chunk)
        }
      }

      setTruncated(sawTruncation)
      const allocList = Array.from(seenAllocs).sort()
      if (allocList.length !== knownAllocs().length) setKnownAllocs(allocList)
    } catch (err) {
      if (err instanceof ConnectError && err.code === Code.NotFound && singleAlloc() !== undefined) {
        setError(`Allocation #${String(singleAlloc())} no longer exists, so its logs can't be read through the manager.`)
        return
      }
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  onMount(() => {
    void poll()
    timer = setInterval(() => void poll(), POLL_MS)
  })
  onCleanup(() => {
    if (timer) clearInterval(timer)
  })

  // Restart the incremental read when filters change so offsets are consistent.
  createEffect(() => {
    streamFilter()
    offsets.clear()
    partials.clear()
    setLines([])
  })

  // Auto-scroll to the newest line while following.
  createEffect(() => {
    lines() // track: re-run whenever new lines arrive
    if (follow() && container) {
      container.scrollTop = container.scrollHeight
    }
  })

  const onScroll = () => {
    if (!container) return
    const nearBottom =
      container.scrollHeight - container.scrollTop - container.clientHeight < 24
    if (nearBottom !== follow()) setFollow(nearBottom)
  }

  // The picker and per-line prefix show whenever more than one allocation is
  // in view — a task can run several instances too. A filter seeded from
  // `initialAllocation` keeps the picker (and its own option) on screen even
  // before, or without, any output from it, so it can always be cleared.
  const multipleAllocs = () => knownAllocs().length > 1
  const showAllocPicker = () => multipleAllocs() || allocFilter() !== 'all'
  const allocOptions = () => {
    const known = knownAllocs()
    const selected = allocFilter()
    return selected === 'all' || known.includes(selected) ? known : [...known, selected]
  }

  const allocLines = () => {
    const af = allocFilter()
    return af === 'all' ? lines() : lines().filter((l) => l.alloc === af)
  }
  const needle = createMemo(() => filter().trim().toLowerCase())
  const visibleLines = () => {
    const q = needle()
    return q ? allocLines().filter((l) => l.text.toLowerCase().includes(q)) : allocLines()
  }

  // Split a line around each case-insensitive hit so the hit can be marked.
  const highlight = (text: string) => {
    const q = needle()
    if (!q) return text
    const parts: Array<{ text: string; hit: boolean }> = []
    const lower = text.toLowerCase()
    let from = 0
    for (;;) {
      const at = lower.indexOf(q, from)
      if (at < 0) break
      if (at > from) parts.push({ text: text.slice(from, at), hit: false })
      parts.push({ text: text.slice(at, at + q.length), hit: true })
      from = at + q.length
    }
    if (from < text.length) parts.push({ text: text.slice(from), hit: false })
    return (
      <For each={parts}>
        {(part) => (part.hit ? <mark class="rounded-sm bg-amber-400/40 text-inherit">{part.text}</mark> : part.text)}
      </For>
    )
  }

  const clear = () => {
    setLines([])
    partials.clear()
  }

  const download = () => {
    const text = visibleLines()
      .map((l) => l.text)
      .join('\n')
    const blob = new Blob([text], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = singleAlloc() !== undefined ? `${props.job}-alloc-${String(singleAlloc())}-logs.txt` : `${props.job}-logs.txt`
    a.click()
    URL.revokeObjectURL(url)
  }

  const filterBtn = (value: StreamFilter, label: string) => (
    <button
      type="button"
      onClick={() => setStreamFilter(value)}
      class={cx(
        'px-2.5 py-1 text-xs font-medium transition-colors first:rounded-l-md last:rounded-r-md',
        streamFilter() === value
          ? 'bg-ink text-canvas'
          : 'bg-surface text-muted hover:bg-surface2 hover:text-ink',
      )}
    >
      {label}
    </button>
  )

  return (
    <div class="space-y-2">
      <div class="flex flex-wrap items-center gap-2">
        <div class="inline-flex divide-x divide-line overflow-hidden rounded-md border border-line">
          {filterBtn('all', 'All')}
          {filterBtn('stdout', 'stdout')}
          {filterBtn('stderr', 'stderr')}
        </div>

        <Show when={showAllocPicker()}>
          <select
            class={cx(selectClass, 'w-auto py-1 text-xs')}
            value={allocFilter()}
            onChange={(e) => setAllocFilter(e.currentTarget.value)}
            aria-label="Filter by allocation"
          >
            <option value="all">All allocations</option>
            <For each={allocOptions()}>{(id) => <option value={id}>alloc #{id}</option>}</For>
          </select>
        </Show>

        <div class="relative min-w-[12rem] flex-1">
          <Icon name="search" size={13} class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-faint" />
          <input
            type="search"
            value={filter()}
            onInput={(e) => setFilter(e.currentTarget.value)}
            placeholder="Find in logs…"
            class={cx(inputClass, 'py-1 pl-8 text-xs')}
            aria-label="Find in logs"
          />
        </div>

        <div class="ml-auto flex items-center gap-1.5">
          <button
            type="button"
            onClick={() => setFollow((v) => !v)}
            class={cx(
              'flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium transition-colors',
              follow()
                ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
                : 'border-line text-muted hover:bg-surface2',
            )}
            title="Auto-scroll to the newest log line"
          >
            <Dot tone={follow() ? 'ok' : 'neutral'} pulse={follow()} />
            {follow() ? 'Following' : 'Follow'}
          </button>
          <Button size="xs" icon={paused() ? 'play' : 'pause'} onClick={() => setPaused((v) => !v)} title="Pause/resume polling">
            {paused() ? 'Resume' : 'Pause'}
          </Button>
          <Button size="xs" onClick={() => setWrap((v) => !v)}>{wrap() ? 'No wrap' : 'Wrap'}</Button>
          <Button size="xs" onClick={clear}>Clear</Button>
          <Button size="xs" onClick={download}>Download</Button>
        </div>
      </div>

      <Show when={error()}>
        <Note tone="danger" class="text-xs">{error()}</Note>
      </Show>
      <Show when={truncated()}>
        <p class="text-2xs text-amber-600 dark:text-amber-400">
          Showing the most recent {Math.round(WINDOW_BYTES / 1024)} KiB per stream; older lines were
          rotated out. Use external log shipping for full history.
        </p>
      </Show>

      <div
        ref={container}
        onScroll={onScroll}
        class="h-[28rem] overflow-auto rounded-md border border-line bg-[#0b0b0b] p-3 font-mono text-xs leading-relaxed text-neutral-200"
      >
        <Show
          when={visibleLines().length > 0}
          fallback={<p class="text-neutral-500">{needle() ? `No lines match "${filter().trim()}".` : 'Waiting for log output…'}</p>}
        >
          <For each={visibleLines()}>
            {(line) => (
              <div class={`flex gap-2 ${wrap() ? 'whitespace-pre-wrap break-words' : 'whitespace-pre'}`}>
                <Show when={multipleAllocs()}>
                  <AllocationLink
                    id={line.alloc}
                    job={props.job}
                    variant="inherit"
                    class="shrink-0 select-none text-neutral-600 hover:text-neutral-300"
                  />
                </Show>
                <span
                  class={`shrink-0 select-none ${
                    line.stream === 'stderr' ? 'text-red-400' : 'text-sky-500'
                  }`}
                >
                  {line.stream === 'stderr' ? 'E' : 'O'}
                </span>
                <span class={line.stream === 'stderr' ? 'text-red-300' : 'text-neutral-200'}>
                  {line.text ? highlight(line.text) : ' '}
                </span>
              </div>
            )}
          </For>
        </Show>
      </div>

      <div class="flex items-center justify-between text-2xs text-faint">
        <span class="tabular-nums">
          <Show when={needle()} fallback={<>{visibleLines().length} line{visibleLines().length === 1 ? '' : 's'} buffered</>}>
            {visibleLines().length} of {allocLines().length} lines match
          </Show>
          <Show when={allocLines().length >= MAX_LINES}> (capped at {MAX_LINES})</Show>
        </span>
        <Show when={lastUpdated()}>
          <span>updated {new Date(lastUpdated()!).toLocaleTimeString()}</span>
        </Show>
      </div>
    </div>
  )
}
