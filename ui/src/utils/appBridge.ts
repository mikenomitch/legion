import { Code, ConnectError } from '@connectrpc/connect'
import { MethodKind, type AnyMessage, type MessageType } from '@bufbuild/protobuf'
import { Manager } from '../gen/legion_connect'

/*
 * App UI extensions, tier 2: sandboxed pages (docs/apps.md, "Sandboxed
 * pages"; docs/ui-architecture.md, "Sandboxed pages").
 *
 * A page is an app's single-file HTML bundle, fetched with GetAppPage and
 * mounted in `<iframe sandbox="allow-scripts" srcdoc=...>`: an opaque origin
 * (no access to this SPA's storage, cookies, or DOM), under the CSP below
 * (no network, no remote code, no forms). Its only way out is the bridge: a
 * small script prepended to the bundle defines `window.legion`, which
 * `postMessage`s requests to this SPA. The SPA runs them as the viewer,
 * limited to the page's RPC allowlist, and asks the viewer before every call
 * that changes something.
 */

/** The protocol tag on every bridge message, both ways. */
export const BRIDGE_PROTOCOL = 'legion-bridge/1'

/**
 * Prepended to every page, exactly as apps plan §6.5 lists it. A page can
 * add its own CSP (policies only ever narrow), never loosen this one.
 */
export const PAGE_CSP =
  "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'none'; form-action 'none'; base-uri 'none'"

/** The iframe's sandbox: scripts, and nothing else (no same-origin, forms, popups, modals, or top navigation). */
export const PAGE_SANDBOX = 'allow-scripts'

/** The gRPC header every bridged call carries, which the manager checks against the page's allowlist. */
export const APP_PAGE_HEADER = 'legion-app-page'

/** Most bridged calls in flight at once, and confirmations waiting, per page. */
export const MAX_IN_FLIGHT = 16
export const MAX_PENDING_CONFIRMATIONS = 4

/** JSON with every `<` escaped, safe to inline in a `<script>`. */
const inlineJson = (value: unknown) => JSON.stringify(value).replace(/</g, '\\u003c')

/**
 * The script that defines `window.legion` inside the page:
 *
 * - `legion.call(method, request)`: run a Manager RPC (`'GetJobInfo'`, with
 *   the request as proto3 JSON) as the viewer; resolves to the reply as
 *   JSON. Only the page's `rpcs`; calls that change something are confirmed
 *   by the viewer first.
 * - `legion.view(name, context)`: render one of the app's own views
 *   (tier 1), resolving to `{ document, actions, title }`.
 * - `legion.action(name, inputs, context)`: run one of the app's own
 *   actions on a job, task, or node context; always confirmed.
 * - `legion.navigate(route)`: move the SPA to an in-app route.
 * - `legion.theme` (`'light'` or `'dark'`) and `legion.onTheme(fn)`.
 *
 * A refusal rejects with an Error whose `code` is `not_allowed`,
 * `cancelled`, `invalid_argument`, `closed`, or a gRPC status in snake case
 * (`permission_denied`, `not_found`, ...).
 */
export function bridgeScript(app: string, page: string, theme: 'light' | 'dark', rpcs: string[]): string {
  return `(function () {
  'use strict';
  var PROTOCOL = ${inlineJson(BRIDGE_PROTOCOL)};
  var pending = {};
  var nextId = 1;
  var themeListeners = [];
  function send(message) {
    return new Promise(function (resolve, reject) {
      var id = nextId++;
      pending[id] = { resolve: resolve, reject: reject };
      message.legion = PROTOCOL;
      message.id = id;
      window.parent.postMessage(message, '*');
    });
  }
  window.addEventListener('message', function (event) {
    if (event.source !== window.parent) return;
    var data = event.data;
    if (!data || typeof data !== 'object' || data.legion !== PROTOCOL) return;
    if (data.event === 'theme') {
      legion.theme = data.theme;
      themeListeners.slice().forEach(function (listener) { try { listener(data.theme); } catch (e) {} });
      return;
    }
    var entry = pending[data.id];
    if (!entry) return;
    delete pending[data.id];
    if (data.ok) {
      entry.resolve(data.result);
    } else {
      var error = new Error((data.error && data.error.message) || 'the bridge refused the call');
      error.code = (data.error && data.error.code) || 'unknown';
      entry.reject(error);
    }
  });
  var legion = {
    app: ${inlineJson(app)},
    page: ${inlineJson(page)},
    rpcs: ${inlineJson(rpcs)},
    theme: ${inlineJson(theme)},
    call: function (method, request) { return send({ op: 'call', method: String(method), request: request === undefined ? {} : request }); },
    view: function (name, context) { return send({ op: 'view', name: String(name), context: context === undefined ? { slot: 'nav' } : context }); },
    action: function (name, inputs, context) { return send({ op: 'action', name: String(name), inputs: inputs === undefined ? {} : inputs, context: context }); },
    navigate: function (route) { return send({ op: 'navigate', route: String(route) }); },
    onTheme: function (listener) { if (typeof listener === 'function') themeListeners.push(listener); }
  };
  window.legion = legion;
})();`
}

/**
 * The `srcdoc` for a page: the CSP and the bridge, then the bundle. A
 * leading doctype stays first, so the page keeps standards mode; the
 * parser puts the CSP `<meta>` in the head either way, before anything the
 * bundle declares.
 */
export function pageSrcdoc(html: string, bridge: string): string {
  const prelude = `<meta http-equiv="Content-Security-Policy" content="${PAGE_CSP}"><script>${bridge}</script>`
  const doctype = /^\s*<!doctype[^>]*>/i.exec(html)
  if (doctype) return doctype[0] + prelude + html.slice(doctype[0].length)
  return prelude + html
}

type ManagerMethod = { name: string; I: MessageType<AnyMessage>; O: MessageType<AnyMessage>; kind: MethodKind; localName: string }

/** Every Manager RPC, by its proto name (`GetJobInfo`). */
const managerMethods: Map<string, ManagerMethod> = new Map(
  Object.entries(Manager.methods).map(([localName, method]) => [
    method.name,
    { ...(method as unknown as Omit<ManagerMethod, 'localName'>), localName },
  ]),
)

export function managerMethod(name: string): ManagerMethod | undefined {
  return managerMethods.get(name)
}

/** A gRPC status as a bridge error code: `permission_denied`. */
export function errorCode(error: unknown): string {
  if (error instanceof ConnectError) {
    return (Code[error.code] ?? 'unknown').replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase()
  }
  return 'unknown'
}

/**
 * What a call is about, for its confirmation: the job, node, key, or
 * object the request names. Best effort from common request fields;
 * the confirmation also shows the whole request.
 */
export function callTarget(request: unknown): string {
  if (!request || typeof request !== 'object') return 'the cluster'
  const fields = request as Record<string, unknown>
  const text = (key: string) => (typeof fields[key] === 'string' && (fields[key] as string).trim() ? (fields[key] as string).trim() : undefined)
  const namespace = text('namespace')
  const job = text('job') ?? text('jobId') ?? text('job_id')
  if (job) return namespace && !job.includes('/') ? `job ${namespace}/${job}` : `job ${job}`
  const name = text('name')
  if (name) return namespace && !name.includes('/') ? `${namespace}/${name}` : name
  const node = text('node') ?? text('nodeId') ?? text('node_id')
  if (node) return `node ${node}`
  const path = text('path') ?? text('key')
  if (path) return `key ${path}`
  const id = text('id') ?? text('instanceId') ?? text('snapshotId')
  if (id) return id
  if (namespace) return `namespace ${namespace}`
  if (fields.all === true) return 'the whole cluster'
  return 'the cluster'
}

/** A context for one of the app's own views or actions, from what a page sent. */
export function contextFrom(value: unknown): { slot: string; namespace: string; job: string; group: string; task: string; node: string } | undefined {
  if (!value || typeof value !== 'object') return undefined
  const fields = value as Record<string, unknown>
  const text = (key: string) => (typeof fields[key] === 'string' ? (fields[key] as string) : '')
  const slot = text('slot')
  if (!slot) return undefined
  return { slot, namespace: text('namespace'), job: text('job'), group: text('group'), task: text('task'), node: text('node') }
}

/** A context's target, for an action's confirmation. */
export function contextTarget(context: { namespace: string; job: string; task: string; node: string }): string {
  if (context.node) return `node ${context.node}`
  const job = context.job ? (context.namespace && context.namespace !== 'default' && !context.job.includes('/') ? `${context.namespace}/${context.job}` : context.job) : ''
  if (context.task) return `task ${context.task} of ${job}`
  if (job) return `job ${job}`
  return 'the cluster'
}

/** Whether the RPC can be called through the bridge at all: unary. */
export const isUnary = (method: ManagerMethod) => method.kind === MethodKind.Unary
