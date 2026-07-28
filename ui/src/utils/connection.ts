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
      const unreachable = !(error instanceof ConnectError) || UNREACHABLE_CODES.has(error.code)
      if (!unreachable) {
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
