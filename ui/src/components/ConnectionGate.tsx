import { Match, Show, Switch, createSignal, onCleanup, onMount, type JSX } from 'solid-js'
import {
  SLOW_CONNECT_HINT_MS,
  connectToManager,
  connectionError,
  connectionStatus,
  managerAddress,
} from '../utils/connection'
import { Button, Icon, PageSkeleton, Spinner } from './ui'

// How often the gate retries on its own while the manager is unreachable —
// a manager that is still starting should let the user in without a click.
const AUTO_RETRY_MS = 5_000

const UnreachableCard = (props: { onRetry: () => void; retrying: boolean }) => (
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
          <div class="mt-4 flex flex-wrap items-center gap-3">
            <Button variant="primary" size="md" icon="refresh" onClick={props.onRetry} disabled={props.retrying}>
              {props.retrying ? 'Retrying…' : 'Retry now'}
            </Button>
            <span class="text-xs text-faint" aria-live="polite">
              Retrying automatically every {AUTO_RETRY_MS / 1000} seconds.
            </span>
          </div>
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
  const [retrying, setRetrying] = createSignal(false)
  let hintTimer: ReturnType<typeof setTimeout> | undefined
  let retryTimer: ReturnType<typeof setTimeout> | undefined

  const clearHintTimer = () => {
    if (hintTimer) clearTimeout(hintTimer)
    hintTimer = undefined
  }
  const clearRetryTimer = () => {
    if (retryTimer) clearTimeout(retryTimer)
    retryTimer = undefined
  }

  const attempt = () => {
    clearRetryTimer()
    setSlow(false)
    clearHintTimer()
    hintTimer = setTimeout(() => setSlow(true), SLOW_CONNECT_HINT_MS)
    // The first attempt gets the skeleton; later ones keep the card up and
    // only mark its button, so the address and the error stay readable.
    setRetrying(connectionStatus() === 'unreachable')
    void connectToManager().finally(() => {
      clearHintTimer()
      setSlow(false)
      setRetrying(false)
      if (connectionStatus() === 'unreachable') {
        retryTimer = setTimeout(() => {
          if (!document.hidden) attempt()
          else retryTimer = setTimeout(attempt, AUTO_RETRY_MS)
        }, AUTO_RETRY_MS)
      }
    })
  }

  onMount(attempt)
  onCleanup(() => {
    clearHintTimer()
    clearRetryTimer()
  })

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
        <UnreachableCard onRetry={attempt} retrying={retrying()} />
      </Match>
      <Match when={connectionStatus() === 'connected'}>{props.children}</Match>
    </Switch>
  )
}
