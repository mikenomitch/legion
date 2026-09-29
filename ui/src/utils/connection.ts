import { Code, ConnectError } from '@connectrpc/connect'
import { createSignal } from 'solid-js'
import type { WhoAmIReply } from '../gen/legion_pb'
import { managerClient, managerEndpoint } from '../grpcClient'

/*
 * One probe decides whether the UI can talk to a manager at all. Every page
 * fetches from the manager, so without this the whole app renders blank (or a
 * dozen identical "failed to load" cards) when the address is wrong or the
 * manager is down.
 *
 * WhoAmI is the probe: it is an open endpoint that answers anonymous callers,
 * so it reports reachability without needing a token, and its reply is the
 * identity the ACL banner needs anyway.
 */

// Browsers give up on an unroutable address slowly (or never), so bound the
// probe ourselves. Connection *refused* still fails in milliseconds.
const CONNECT_TIMEOUT_MS = 15_000

// How long a connect attempt may run before we admit to the user that it is
// taking a while.
export const SLOW_CONNECT_HINT_MS = 5_000

export type ConnectionStatus = 'connecting' | 'connected' | 'unreachable'

// The address to show the user. An empty VITE_MANAGER_ENDPOINT means "same
// origin" (the Vite dev proxy), in which case the page origin is the truth.
export const managerAddress = managerEndpoint || window.location.origin

const [connectionStatus, setConnectionStatus] = createSignal<ConnectionStatus>('connecting')
const [connectionError, setConnectionError] = createSignal<string | null>(null)
const [managerIdentity, setManagerIdentity] = createSignal<WhoAmIReply | null>(null)
// Set when the manager answered but refused to introspect the identity —
// almost always a stale or invalid stored token.
const [identityError, setIdentityError] = createSignal<string | null>(null)

export { connectionError, connectionStatus, identityError, managerIdentity }

// Codes that mean the request never reached a manager. Any other code is a
// real reply — the manager is up, it just refused this call (a bad token
// answers Unauthenticated, for example), which is not a connection problem.
const UNREACHABLE_CODES = new Set<Code>([Code.Unknown, Code.Unavailable, Code.DeadlineExceeded])

const isUnreachable = (error: unknown): boolean =>
  !(error instanceof ConnectError) || UNREACHABLE_CODES.has(error.code)

const describeError = (error: unknown): string => {
  if (error instanceof ConnectError) return error.rawMessage || error.message
  return error instanceof Error ? error.message : String(error)
}

// Re-reads the identity after the stored credential changes (login/logout).
// Deliberately not a full reconnect: the manager is already known to be up, so
// this must not drop the app back to the connecting state.
export const refreshManagerIdentity = async (): Promise<void> => {
  try {
    setManagerIdentity(await managerClient.whoAmI({}, { timeoutMs: CONNECT_TIMEOUT_MS }))
    setIdentityError(null)
  } catch (error) {
    setManagerIdentity(null)
    setIdentityError(describeError(error))
  }
}

let inFlight: Promise<void> | undefined

export const connectToManager = (): Promise<void> => {
  if (inFlight) return inFlight

  setConnectionStatus('connecting')
  setConnectionError(null)

  inFlight = managerClient
    .whoAmI({}, { timeoutMs: CONNECT_TIMEOUT_MS })
    .then((reply) => {
      setManagerIdentity(reply)
      setIdentityError(null)
      setConnectionStatus('connected')
    })
    .catch((error: unknown) => {
      if (!isUnreachable(error)) {
        // Reached the manager; it just refused this call (usually a stale
        // token). Let the app in and report it where identity is shown.
        setManagerIdentity(null)
        setIdentityError(describeError(error))
        setConnectionStatus('connected')
        return
      }
      setConnectionError(describeError(error))
      setConnectionStatus('unreachable')
    })
    .finally(() => {
      inFlight = undefined
    })

  return inFlight
}

/*
 * The watchdog: once connected, keep asking whether the manager is still
 * there. The first probe gates the app; this one never does — pages keep
 * showing their last data, and a banner says the numbers may be stale until
 * contact returns. A restarted or partitioned manager is otherwise something
 * the operator only discovers when every card errors at once.
 */

export type ManagerLink = 'ok' | 'lost'

const [managerLink, setManagerLink] = createSignal<ManagerLink>('ok')
const [lostSince, setLostSince] = createSignal<number | null>(null)

export { lostSince, managerLink }

const WATCHDOG_INTERVAL_MS = 10_000
// Retry faster once contact is lost: the banner should clear within seconds
// of the manager coming back, not within a leisurely ten.
const WATCHDOG_LOST_INTERVAL_MS = 3_000
const WATCHDOG_TIMEOUT_MS = 6_000

let watchdogTimer: ReturnType<typeof setTimeout> | undefined
let probeInFlight: Promise<void> | undefined

/** One reachability probe; also what the banner's Retry button calls. */
export const probeManagerLink = (): Promise<void> => {
  if (probeInFlight) return probeInFlight
  probeInFlight = managerClient
    .whoAmI({}, { timeoutMs: WATCHDOG_TIMEOUT_MS })
    .then(() => {
      if (managerLink() === 'lost') {
        setManagerLink('ok')
        setLostSince(null)
        // Contact returned: the identity may have changed on the way (a
        // restarted manager with ACLs newly enabled, say).
        void refreshManagerIdentity()
      }
    })
    .catch((error: unknown) => {
      if (isUnreachable(error) && managerLink() === 'ok') {
        setManagerLink('lost')
        setLostSince(Date.now())
      }
    })
    .finally(() => {
      probeInFlight = undefined
    })
  return probeInFlight
}

const scheduleProbe = () => {
  if (watchdogTimer) clearTimeout(watchdogTimer)
  const delay = managerLink() === 'lost' ? WATCHDOG_LOST_INTERVAL_MS : WATCHDOG_INTERVAL_MS
  watchdogTimer = setTimeout(async () => {
    // A hidden tab neither needs the banner nor should keep the manager busy.
    if (!document.hidden && connectionStatus() === 'connected') await probeManagerLink()
    scheduleProbe()
  }, delay)
}

let watchdogStarted = false

/** Idempotent; the layout calls it once the first connect has succeeded. */
export const startConnectionWatchdog = () => {
  if (watchdogStarted) return
  watchdogStarted = true
  scheduleProbe()
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && connectionStatus() === 'connected') {
      void probeManagerLink().then(scheduleProbe)
    }
  })
}
