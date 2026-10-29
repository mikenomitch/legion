import { useNavigate } from '@solidjs/router'
import { Show, createEffect, createMemo, createResource, createSignal, on, onCleanup, onMount } from 'solid-js'
import type { GetAppPageReply } from '../../gen/legion_pb'
import { managerClient } from '../../grpcClient'
import {
  APP_PAGE_HEADER,
  BRIDGE_PROTOCOL,
  CONFIRM_ARM_MS,
  MAX_IN_FLIGHT,
  MAX_PENDING_CONFIRMATIONS,
  PAGE_SANDBOX,
  aliasedField,
  bridgeScript,
  callTarget,
  contextFrom,
  contextTarget,
  errorCode,
  isUnary,
  managerMethod,
  pageSrcdoc,
} from '../../utils/appBridge'
import { errorText, isDenied, isRoute, parseView } from '../../utils/appViews'
import { Button, CodeBlock, Note, Spinner } from '../ui'

/*
 * An app's sandboxed page (tier 2; docs/ui-architecture.md, "Sandboxed
 * pages"). The bundle comes from GetAppPage, authorized as the viewer, and
 * runs in an opaque-origin iframe under a strict CSP. Everything it asks of
 * Legion arrives here as a postMessage, from that iframe only, and is
 * checked before anything is sent:
 *
 * - `call`: the RPC must be in the page's allowlist (GetAppPage's `rpcs`)
 *   and unary, and the request may not spell a field both ways (`nodeId`
 *   and `node_id`); a mutating one waits for the viewer's yes in a dialog
 *   this SPA draws, naming the app, the RPC, and the target of the message
 *   as it will be sent. The dialog arms after a moment, one confirmation
 *   at a time. The call carries `legion-app-page`, so the manager checks
 *   the allowlist again.
 * - `view` / `action`: the app's own tier-1 views and actions, through
 *   GetAppView and InvokeAppAction; actions are always confirmed.
 * - `navigate`: in-app routes only.
 *
 * Once the frame loads a second document (the page navigated itself away),
 * the bridge closes.
 */

type BridgeRequest = {
  legion: string
  id: number
  op: string
  method?: unknown
  request?: unknown
  name?: unknown
  context?: unknown
  inputs?: unknown
  route?: unknown
}

type Confirmation = {
  title: string
  target: string
  detail: string
  resolve: (yes: boolean) => void
}

class BridgeRefusal extends Error {
  constructor(
    public code: string,
    message: string,
  ) {
    super(message)
  }
}

const currentTheme = (): 'light' | 'dark' =>
  typeof document !== 'undefined' && document.documentElement.classList.contains('dark') ? 'dark' : 'light'

/** The page, mounted and bridged. */
export function SandboxedPage(props: { app: string; page: string; onTitle?: (title: string) => void }) {
  const navigate = useNavigate()
  const [reply] = createResource(
    () => [props.app, props.page] as const,
    async ([app, page]): Promise<{ page?: GetAppPageReply; error?: unknown }> => {
      try {
        const fetched = await managerClient.getAppPage({ app, page })
        props.onTitle?.(fetched.title)
        return { page: fetched }
      } catch (error) {
        return { error }
      }
    },
  )
  const page = () => reply()?.page
  const allowlist = createMemo(() => new Map((page()?.rpcs ?? []).map((rpc) => [rpc.method, rpc.mutating])))
  // The srcdoc is computed once per fetched bundle: a theme change is
  // posted to the page rather than reloading it.
  const srcdoc = createMemo(() => {
    const current = page()
    if (!current) return ''
    return pageSrcdoc(
      current.html,
      bridgeScript(
        current.app,
        current.page,
        currentTheme(),
        current.rpcs.map((rpc) => rpc.method),
      ),
    )
  })

  let frame: HTMLIFrameElement | undefined
  const [loads, setLoads] = createSignal(0)
  const closed = () => loads() > 1
  let inFlight = 0
  const [confirmations, setConfirmations] = createSignal<Confirmation[]>([])
  // "Run as me" stays disabled for a moment after each confirmation
  // appears, so a click (or the second half of a double-click) meant for
  // something else cannot approve it.
  const [armed, setArmed] = createSignal(false)
  let armTimer: ReturnType<typeof setTimeout> | undefined
  createEffect(
    on(
      () => confirmations()[0],
      (head) => {
        setArmed(false)
        clearTimeout(armTimer)
        if (head) armTimer = setTimeout(() => setArmed(true), CONFIRM_ARM_MS)
      },
    ),
  )
  onCleanup(() => clearTimeout(armTimer))

  const ask = (confirmation: Omit<Confirmation, 'resolve'>): Promise<boolean> => {
    if (confirmations().length >= MAX_PENDING_CONFIRMATIONS) {
      return Promise.reject(new BridgeRefusal('resource_exhausted', 'too many calls are waiting for your confirmation'))
    }
    return new Promise((resolve) => setConfirmations((queue) => [...queue, { ...confirmation, resolve }]))
  }
  const answer = (yes: boolean) => {
    const [head, ...rest] = confirmations()
    if (!head || (yes && !armed())) return
    setConfirmations(rest)
    head.resolve(yes)
  }

  const respond = (id: number, outcome: { ok: true; result: unknown } | { ok: false; code: string; message: string }) => {
    // The frame's origin is opaque, so `*` is the only target there is; the
    // window is the iframe this component created.
    const target = frame?.contentWindow
    if (!target || closed()) return
    const message =
      outcome.ok === true
        ? { legion: BRIDGE_PROTOCOL, id, ok: true, result: outcome.result }
        : { legion: BRIDGE_PROTOCOL, id, ok: false, error: { code: outcome.code, message: outcome.message } }
    target.postMessage(message, '*')
  }

  const appLabel = () => page()?.app ?? props.app

  const runCall = async (request: BridgeRequest): Promise<unknown> => {
    const name = typeof request.method === 'string' ? request.method : ''
    if (!allowlist().has(name)) {
      throw new BridgeRefusal('not_allowed', `RPC ${JSON.stringify(name)} is not in page ${props.app}/${props.page}'s allowlist (ui.pages[].rpcs)`)
    }
    const method = managerMethod(name)
    if (!method || !isUnary(method)) {
      throw new BridgeRefusal('unimplemented', `the bridge calls unary Manager RPCs only, and ${name} is not one`)
    }
    const aliased = aliasedField(method.I, request.request ?? {})
    if (aliased) {
      throw new BridgeRefusal('invalid_argument', `the request sets ${aliased} under both of its names (JSON and proto); set it once`)
    }
    let message
    try {
      message = method.I.fromJson((request.request ?? {}) as never)
    } catch (error) {
      throw new BridgeRefusal('invalid_argument', `the request is not a valid ${method.I.typeName}: ${error instanceof Error ? error.message : String(error)}`)
    }
    if (allowlist().get(name)) {
      // The confirmation names what will be sent: the decoded message, not
      // the page's own JSON.
      const sent = message.toJson({ emitDefaultValues: false })
      const target = callTarget(sent)
      const yes = await ask({
        title: `${appLabel()} wants to run ${name} on ${target} as you`,
        target,
        detail: JSON.stringify(sent ?? {}, null, 2),
      })
      if (!yes) throw new BridgeRefusal('cancelled', `you declined ${name}`)
    }
    const call = (managerClient as unknown as Record<string, (input: unknown, options: unknown) => Promise<{ toJson: (options?: unknown) => unknown }>>)[method.localName]
    const response = await call.call(managerClient, message, { headers: { [APP_PAGE_HEADER]: `${props.app}/${props.page}` } })
    return response.toJson({ emitDefaultValues: false })
  }

  const runView = async (request: BridgeRequest): Promise<unknown> => {
    const context = contextFrom(request.context)
    if (typeof request.name !== 'string' || !context) {
      throw new BridgeRefusal('invalid_argument', 'legion.view(name, context) takes a view name and a context with its slot')
    }
    const view = await managerClient.getAppView({ app: props.app, view: request.name, context })
    return {
      title: view.title,
      document: parseView(view.viewJson),
      actions: view.actions.map((action) => ({ name: action.name, title: action.title, allowed: action.allowed, mutating: action.mutating })),
    }
  }

  const runAction = async (request: BridgeRequest): Promise<unknown> => {
    const context = contextFrom(request.context)
    if (typeof request.name !== 'string' || !context) {
      throw new BridgeRefusal('invalid_argument', 'legion.action(name, inputs, context) takes an action name, its inputs, and the job, task, or node context it acts on')
    }
    const inputs = request.inputs && typeof request.inputs === 'object' ? request.inputs : {}
    const yes = await ask({
      title: `${appLabel()} wants to run its action “${request.name}” on ${contextTarget(context)} as you`,
      target: contextTarget(context),
      detail: JSON.stringify(inputs, null, 2),
    })
    if (!yes) throw new BridgeRefusal('cancelled', `you declined ${request.name}`)
    const outcome = await managerClient.invokeAppAction({
      app: props.app,
      action: request.name,
      context,
      inputsJson: JSON.stringify(inputs),
      idempotencyKey: typeof crypto !== 'undefined' && 'randomUUID' in crypto ? crypto.randomUUID() : undefined,
    })
    return { message: outcome.message, document: outcome.viewJson ? parseView(outcome.viewJson) : undefined }
  }

  const handle = async (request: BridgeRequest) => {
    switch (request.op) {
      case 'call':
        return runCall(request)
      case 'view':
        return runView(request)
      case 'action':
        return runAction(request)
      case 'navigate': {
        if (typeof request.route !== 'string' || !isRoute(request.route)) {
          throw new BridgeRefusal('invalid_argument', 'legion.navigate takes an in-app route: a path starting with a single /')
        }
        navigate(request.route)
        return null
      }
      default:
        throw new BridgeRefusal('invalid_argument', `unknown bridge operation ${JSON.stringify(request.op)}`)
    }
  }

  const onMessage = (event: MessageEvent) => {
    // Only the page's own frame, in its opaque origin, while it still holds
    // the bundle it was given.
    if (!frame || event.source !== frame.contentWindow || event.origin !== 'null' || closed()) return
    const data = event.data as BridgeRequest
    if (!data || typeof data !== 'object' || data.legion !== BRIDGE_PROTOCOL || typeof data.id !== 'number') return
    if (inFlight >= MAX_IN_FLIGHT) {
      respond(data.id, { ok: false, code: 'resource_exhausted', message: `at most ${MAX_IN_FLIGHT} bridge calls at once` })
      return
    }
    inFlight += 1
    handle(data)
      .then((result) => respond(data.id, { ok: true, result }))
      .catch((error: unknown) =>
        respond(
          data.id,
          error instanceof BridgeRefusal
            ? { ok: false, code: error.code, message: error.message }
            : { ok: false, code: errorCode(error), message: errorText(error) },
        ),
      )
      .finally(() => {
        inFlight -= 1
      })
  }

  onMount(() => {
    window.addEventListener('message', onMessage)
    // Tell the page when the SPA's theme changes.
    const observer = new MutationObserver(() => {
      if (closed()) return
      frame?.contentWindow?.postMessage({ legion: BRIDGE_PROTOCOL, event: 'theme', theme: currentTheme() }, '*')
    })
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
    onCleanup(() => {
      window.removeEventListener('message', onMessage)
      observer.disconnect()
      for (const pending of confirmations()) pending.resolve(false)
    })
  })

  return (
    <div class="space-y-3">
      <Show
        when={page()}
        fallback={
          <Show when={reply()?.error} fallback={<Spinner label={`Loading ${props.app}…`} />}>
            <Note tone={isDenied(reply()?.error) ? 'neutral' : 'warn'}>
              {isDenied(reply()?.error) ? 'You may not see this page.' : `The ${props.app} app could not serve this page: ${errorText(reply()?.error)}`}
            </Note>
          </Show>
        }
      >
        <Show when={closed()}>
          <Note tone="warn">
            The page left the bundle {props.app} served, so its bridge to Legion is closed. Reload to reopen it.
          </Note>
        </Show>
        <iframe
          ref={frame}
          title={`${page()!.title} (from the ${props.app} app, sandboxed)`}
          sandbox={PAGE_SANDBOX}
          srcdoc={srcdoc()}
          referrerpolicy="no-referrer"
          class="h-[calc(100vh-13rem)] min-h-[24rem] w-full rounded-md border border-line bg-surface"
          onLoad={() => setLoads((count) => count + 1)}
        />
        <p class="text-2xs text-faint">
          Sandboxed page from the {props.app} app. It may call{' '}
          {page()!.rpcs.length > 0 ? page()!.rpcs.map((rpc) => rpc.method + (rpc.mutating ? ' (asks first)' : '')).join(', ') : 'nothing'} as you.
        </p>
      </Show>
      <Show when={confirmations()[0]}>
        {(head) => (
          <div class="fixed inset-0 z-50 flex items-start justify-center bg-black/50 p-4 pt-[12vh] backdrop-blur-[2px]" role="presentation">
            <div
              role="alertdialog"
              aria-modal="true"
              aria-label="Confirm a call from a sandboxed page"
              class="w-full max-w-lg space-y-3 rounded-lg border border-line bg-surface p-4 shadow-xl"
            >
              <p class="eyebrow">Sandboxed page · {props.app}</p>
              <p class="text-sm font-semibold text-ink" data-testid="bridge-confirm-title">
                {head().title}
              </p>
              <CodeBlock code={head().detail} label="Request" maxHClass="max-h-48" />
              <Show when={confirmations().length > 1}>
                <p class="text-2xs text-faint">{confirmations().length - 1} more waiting.</p>
              </Show>
              <div class="flex justify-end gap-2">
                <Button variant="ghost" size="sm" autofocus onClick={() => answer(false)}>
                  Cancel
                </Button>
                <Button variant="primary" size="sm" disabled={!armed()} data-testid="bridge-confirm-run" onClick={() => answer(true)}>
                  Run as me
                </Button>
              </div>
            </div>
          </div>
        )}
      </Show>
    </div>
  )
}
