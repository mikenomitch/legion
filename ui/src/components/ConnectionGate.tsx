import { Match, Show, Switch, createSignal, onCleanup, onMount, type JSX } from 'solid-js'
import {
  SLOW_CONNECT_HINT_MS,
  connectToManager,
  connectionError,
  connectionStatus,
  managerAddress,
} from '../utils/connection'
import { Button, Icon, Spinner } from './ui'

// Stand-in for the page while the first manager call is in flight. It mirrors
// the shape most pages take (stat row, then a table) so the layout does not
// jump when the real content lands.
const PageSkeleton = () => (
  <div role="status" aria-label="Loading" class="space-y-5">
    <div class="grid grid-cols-2 gap-3 lg:grid-cols-4">
      {Array.from({ length: 4 }).map(() => (
        <div class="rounded-lg border border-line bg-surface p-3.5 shadow-card">
          <div class="skeleton h-2.5 w-20" />
          <div class="skeleton mt-3 h-6 w-16" />
        </div>
      ))}
    </div>
    <div class="overflow-hidden rounded-lg border border-line bg-surface shadow-card">
      <div class="border-b border-line px-4 py-3">
        <div class="skeleton h-2.5 w-28" />
      </div>
      <div class="space-y-2 p-4">
        {Array.from({ length: 5 }).map(() => (
          <div class="skeleton h-8" />
        ))}
      </div>
    </div>
  </div>
)

const UnreachableCard = (props: { onRetry: () => void }) => (
  <div class="flex justify-center py-10">
    <div class="w-full max-w-lg rounded-lg border border-line bg-surface p-6 shadow-card">
      <div class="flex items-start gap-3">
        <span class="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-red-500/10 text-red-600 dark:text-red-400">
          <Icon name="alertTriangle" size={18} />
        </span>
        <div class="min-w-0">
          <h2 class="font-display text-lg font-semibold text-ink">Can't reach the manager</h2>
          <p class="mt-1 text-sm text-muted">
            No response from <span class="break-all font-mono text-ink">{managerAddress}</span>.
          </p>
          <Show when={connectionError()}>
            <p class="mt-2 break-words rounded-md border border-line bg-surface2/60 px-2.5 py-1.5 font-mono text-xs text-muted">
              {connectionError()}
            </p>
          </Show>
          <ul class="mt-3 space-y-1 text-xs text-faint">
            <li>
              · Check the manager is running: <code class="font-mono text-muted">legion start manager</code>
            </li>
            <li>
              · Point the UI elsewhere with <code class="font-mono text-muted">VITE_MANAGER_ENDPOINT</code>
            </li>
          </ul>
          <Button variant="primary" size="md" icon="refresh" class="mt-4" onClick={props.onRetry}>
            Retry
          </Button>
        </div>
      </div>
    </div>
  </div>
)

// Blocks the app until the manager answers once. Everything below this point
// assumes a reachable manager, so failing here explicitly beats letting every
// page fail on its own with no address and no way to retry.
export function ConnectionGate(props: { children: JSX.Element }) {
  const [slow, setSlow] = createSignal(false)
  let hintTimer: ReturnType<typeof setTimeout> | undefined

  const clearHintTimer = () => {
    if (hintTimer) clearTimeout(hintTimer)
    hintTimer = undefined
  }

  const attempt = () => {
    setSlow(false)
    clearHintTimer()
    hintTimer = setTimeout(() => setSlow(true), SLOW_CONNECT_HINT_MS)
    void connectToManager().finally(() => {
      clearHintTimer()
      setSlow(false)
    })
  }

  onMount(attempt)
  onCleanup(clearHintTimer)

  return (
    <Switch>
      <Match when={connectionStatus() === 'connecting'}>
        <div class="space-y-4">
          {/* Only after a few seconds do we say anything — a fast connect
              should never flash a "connecting" message. */}
          <Show when={slow()}>
            <div class="flex justify-center" aria-live="polite">
              <Spinner label={`Trying to connect to the manager at ${managerAddress}…`} />
            </div>
          </Show>
          <PageSkeleton />
        </div>
      </Match>
      <Match when={connectionStatus() === 'unreachable'}>
        <UnreachableCard onRetry={attempt} />
      </Match>
      <Match when={connectionStatus() === 'connected'}>{props.children}</Match>
    </Switch>
  )
}
