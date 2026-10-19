/*
 * The namespace switcher (docs/namespaces.md, "The UI").
 *
 * List pages whose rows belong to namespaces carry the selection as `?ns=`
 * (names or globs, comma-separated; `*` for every namespace the identity
 * can read), so a filter is a filter and a pasted list link shows what it
 * showed. Detail pages carry the namespace in their path instead; their
 * switcher navigates to the chosen namespace's list rather than stranding
 * you on a page that does not exist there. Cluster-scoped pages have no
 * switcher and say so. An identity that can read a single namespace sees
 * none of this, only that namespace as a badge.
 */
import { useNavigate, useSearchParams } from '@solidjs/router'
import { For, Show } from 'solid-js'
import { isMultiNamespace, parseSelection, readableNamespaces } from '../utils/namespaces'
import { Badge, selectClass } from './ui'

/** The every-namespace selection. */
export const ALL_NAMESPACES = '*'

/** List pages whose rows belong to namespaces: they take `?ns=`. */
export const NAMESPACED_LIST_PAGES = new Set(['/jobs', '/services', '/rollouts', '/volumes', '/events'])

/** Pages about the cluster itself, which no namespace scopes. */
export const CLUSTER_SCOPED_PAGES = new Set([
  '/nodes',
  '/manager',
  '/raft',
  '/observability',
  '/state-explorer',
  '/kv',
])

/**
 * The selection a namespaced list page shows: `?ns=` when given, else every
 * namespace for an identity that can read several (the All view is a real
 * selection and the default), else the one it can read, else `default`.
 */
export const useNamespaceSelection = (): (() => string[]) => {
  const [params] = useSearchParams()
  return () => {
    const raw = parseSelection(typeof params.ns === 'string' ? params.ns : undefined)
    if (raw.length > 0) return raw
    if (isMultiNamespace()) return [ALL_NAMESPACES]
    const readable = readableNamespaces()
    return readable.length === 1 ? [readable[0]] : []
  }
}

/** Whether `namespace` is in a selection (names or `*`-globs). */
export const selectionIncludes = (selection: string[], namespace: string): boolean =>
  selection.length === 0
    ? namespace === 'default'
    : selection.some((pattern) => {
        if (!pattern.includes('*')) return pattern === namespace
        const escaped = pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*')
        return new RegExp(`^${escaped}$`).test(namespace)
      })

function NamespaceSelect(props: { value: string; onChange: (value: string) => void; disabledHint?: string }) {
  const options = () => readableNamespaces()
  const custom = () => props.value !== ALL_NAMESPACES && !options().includes(props.value)
  return (
    <label class="flex items-center gap-2 text-xs text-muted" title={props.disabledHint}>
      <span>Namespace</span>
      <select
        class={selectClass}
        value={props.value}
        aria-label="Namespace"
        onChange={(event) => props.onChange(event.currentTarget.value)}
      >
        <option value={ALL_NAMESPACES}>All namespaces</option>
        <For each={options()}>{(namespace) => <option value={namespace}>{namespace}</option>}</For>
        <Show when={custom()}>
          <option value={props.value}>{props.value}</option>
        </Show>
      </select>
    </label>
  )
}

/** The badge a single-namespace identity sees in place of the switcher. */
function OnlyNamespaceBadge() {
  return (
    <Show when={readableNamespaces().length === 1}>
      <Badge mono title="The only namespace this identity can read">
        {readableNamespaces()[0]}
      </Badge>
    </Show>
  )
}

/** The switcher on a namespaced list page, bound to `?ns=`. */
export function ListNamespaceSwitcher() {
  const [, setParams] = useSearchParams()
  const selection = useNamespaceSelection()
  return (
    <Show when={isMultiNamespace()} fallback={<OnlyNamespaceBadge />}>
      <NamespaceSelect
        value={selection().join(',')}
        onChange={(value) => setParams({ ns: value, page: undefined })}
      />
    </Show>
  )
}

/**
 * The switcher on a detail page: shows the page's namespace, and choosing
 * another opens that namespace's list (`listPath?ns=`), since the page
 * itself names one object in one namespace.
 */
export function DetailNamespaceSwitcher(props: { namespace: string; listPath: string }) {
  const navigate = useNavigate()
  return (
    <Show when={isMultiNamespace()} fallback={<OnlyNamespaceBadge />}>
      <NamespaceSelect
        value={props.namespace}
        disabledHint="This page shows one object; choosing a namespace opens its list"
        onChange={(value) => navigate(`${props.listPath}?ns=${encodeURIComponent(value)}`)}
      />
    </Show>
  )
}

/** The tag a cluster-scoped page shows to a multi-namespace identity. */
export function ClusterScopedTag() {
  return (
    <Show when={isMultiNamespace()}>
      <Badge title="This page is about the cluster itself; no namespace scopes it">cluster-scoped</Badge>
    </Show>
  )
}

/** The header control for a top-level page, by its path. */
export function HeaderNamespaceControl(props: { path: string }) {
  return (
    <Show
      when={NAMESPACED_LIST_PAGES.has(props.path)}
      fallback={
        <Show when={CLUSTER_SCOPED_PAGES.has(props.path)}>
          <ClusterScopedTag />
        </Show>
      }
    >
      <ListNamespaceSwitcher />
    </Show>
  )
}
