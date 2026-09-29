import { Code, ConnectError } from '@connectrpc/connect'
import { createSignal, onCleanup, onMount, type Accessor } from 'solid-js'

/*
 * One polling story for the whole UI.
 *
 * Pages feed `usePoll()` into a resource source so the resource refetches on
 * every tick, and Solid keeps the previous value on screen while the next
 * fetch is in flight — nothing flashes. Ticks stop while the tab is hidden
 * and resume (with an immediate tick) when it is looked at again, so a
 * dashboard left open in a background tab neither hammers the manager nor
 * shows stale rows the moment it is brought forward.
 */

/** Live views: allocations, health, rollouts in progress. */
export const POLL_INTERVAL_MS = 2000
/** Index pages and detail pages that fan out into many RPCs per refresh. */
export const INDEX_POLL_INTERVAL_MS = 5000

/**
 * A counter that advances every `intervalMs` while the page is visible.
 *
 *   const tick = usePoll()
 *   const [data] = createResource(() => [id(), tick()] as const, fetch)
 *
 * The first tick is `0`; wrap it in a tuple or add one when it is the whole
 * source, because a bare `0` reads as "no source" to `createResource`.
 */
export function usePoll(intervalMs = POLL_INTERVAL_MS): Accessor<number> {
  const [tick, setTick] = createSignal(0)
  onMount(() => {
    let timer: ReturnType<typeof setInterval> | undefined
    const start = () => {
      if (timer === undefined) timer = setInterval(() => setTick((value) => value + 1), intervalMs)
    }
    const stop = () => {
      if (timer !== undefined) clearInterval(timer)
      timer = undefined
    }
    const onVisibility = () => {
      if (document.hidden) {
        stop()
        return
      }
      setTick((value) => value + 1)
      start()
    }
    if (!document.hidden) start()
    document.addEventListener('visibilitychange', onVisibility)
    onCleanup(() => {
      stop()
      document.removeEventListener('visibilitychange', onVisibility)
    })
  })
  return tick
}

// A shared clock for relative timestamps ("3 minutes ago"). Ticking once for
// the whole app means a label ages even on a page that never refetches, and
// a page that does refetch does not need its own timer for the text.
const [now, setNow] = createSignal(Date.now())
if (typeof window !== 'undefined') {
  window.setInterval(() => setNow(Date.now()), 30_000)
}
export { now }

/**
 * Wraps a resource fetcher so that, once it has succeeded for a source, a
 * later failure for the *same* source returns the last good value instead of
 * rejecting. A page that polls every few seconds should not tear itself down
 * to an error card because one refresh hit a restarting manager — the
 * connection watchdog already tells the operator contact was lost. The
 * first fetch (and the first fetch after the source changes to a different
 * entity) still rejects, so a missing job is still reported as missing.
 */
export function retainOnError<S, T>(fetcher: (source: S) => Promise<T>, key: (source: S) => string): (source: S) => Promise<T> {
  let last: { key: string; value: T } | undefined
  return async (source: S) => {
    const current = key(source)
    try {
      const value = await fetcher(source)
      last = { key: current, value }
      return value
    } catch (error) {
      if (last && last.key === current) return last.value
      throw error
    }
  }
}

/**
 * Turns an ACL denial into `fallback` and lets every other failure through.
 * Garnish RPCs (allocation counts on the job list, the overview's stats)
 * must degrade to empty for an identity that cannot read them, but an
 * outage should still be an outage — compose with `retainOnError` so the
 * last good value survives it.
 */
export function swallowDenied<S, T>(fetcher: (source: S) => Promise<T>, fallback: T): (source: S) => Promise<T> {
  return async (source: S) => {
    try {
      return await fetcher(source)
    } catch (error) {
      if (error instanceof ConnectError && (error.code === Code.PermissionDenied || error.code === Code.Unauthenticated)) {
        return fallback
      }
      throw error
    }
  }
}
