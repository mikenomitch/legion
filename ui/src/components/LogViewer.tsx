import { For, Show, createEffect, createSignal, onCleanup, onMount } from 'solid-js'
import { managerClient } from '../grpcClient'
import { Button, Dot, Note, cx, selectClass } from './ui'

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
  const [allocFilter, setAllocFilter] = createSignal<string>('all')
  const [knownAllocs, setKnownAllocs] = createSignal<string[]>([])
  const [follow, setFollow] = createSignal(true)
  const [wrap, setWrap] = createSignal(true)
  const [paused, setPaused] = createSignal(false)
  const [lastUpdated, setLastUpdated] = createSignal<number | undefined>()
  const [error, setError] = createSignal<string>('')
  const [truncated, setTruncated] = createSignal(false)

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

  const visibleLines = () => {
    const af = allocFilter()
    return af === 'all' ? lines() : lines().filter((l) => l.alloc === af)
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
    a.download = `${props.job}-logs.txt`
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

        <Show when={!props.allocationIds && knownAllocs().length > 1}>
          <select
            class={cx(selectClass, 'w-auto py-1 text-xs')}
            value={allocFilter()}
            onChange={(e) => setAllocFilter(e.currentTarget.value)}
            aria-label="Filter by allocation"
          >
            <option value="all">All allocations</option>
            <For each={knownAllocs()}>{(id) => <option value={id}>alloc #{id}</option>}</For>
          </select>
        </Show>

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
        class="h-[28rem] overflow-auto rounded-md border border-line bg-[#0d0c0a] p-3 font-mono text-xs leading-relaxed text-neutral-200"
      >
        <Show
          when={visibleLines().length > 0}
          fallback={<p class="text-neutral-500">Waiting for log output…</p>}
        >
          <For each={visibleLines()}>
            {(line) => (
              <div class={`flex gap-2 ${wrap() ? 'whitespace-pre-wrap break-words' : 'whitespace-pre'}`}>
                <Show when={!props.allocationIds && knownAllocs().length > 1}>
                  <span class="shrink-0 select-none text-neutral-600">#{line.alloc}</span>
                </Show>
                <span
                  class={`shrink-0 select-none ${
                    line.stream === 'stderr' ? 'text-red-400' : 'text-sky-500'
                  }`}
                >
                  {line.stream === 'stderr' ? 'E' : 'O'}
                </span>
                <span class={line.stream === 'stderr' ? 'text-red-300' : 'text-neutral-200'}>
                  {line.text || ' '}
                </span>
              </div>
            )}
          </For>
        </Show>
      </div>

      <div class="flex items-center justify-between text-2xs text-faint">
        <span class="tabular-nums">
          {visibleLines().length} line{visibleLines().length === 1 ? '' : 's'} buffered
          <Show when={visibleLines().length >= MAX_LINES}> (capped at {MAX_LINES})</Show>
        </span>
        <Show when={lastUpdated()}>
          <span>updated {new Date(lastUpdated()!).toLocaleTimeString()}</span>
        </Show>
      </div>
    </div>
  )
}
