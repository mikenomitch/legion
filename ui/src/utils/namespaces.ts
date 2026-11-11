/*
 * Namespaces in the UI (docs/namespaces.md, "The UI").
 *
 * A job's id is canonical: the bare name in `default`, `namespace/name`
 * elsewhere — the same shape the manager stores and every RPC takes. Detail
 * routes carry the namespace as its own path segment
 * (`/jobs/payments-prod/web`, `/jobs/default/web`), so a pasted link always
 * opens the job it names, and navigation cannot drop the namespace the way a
 * query parameter can. List pages carry the selection as `?ns=` instead
 * (`?ns=payments-*`, `?ns=*` for every namespace you can read).
 */
import { managerIdentity } from './connection'

export const DEFAULT_NAMESPACE = 'default'

/** `{ namespace, name }` of a canonical job (or instance) id. */
export const splitJobId = (id: string): { namespace: string; name: string } => {
  const slash = id.indexOf('/')
  if (slash < 0) return { namespace: DEFAULT_NAMESPACE, name: id }
  return { namespace: id.slice(0, slash), name: id.slice(slash + 1) }
}

/** The canonical id of `name` in `namespace`: bare in `default`. */
export const jobIdOf = (namespace: string | undefined, name: string): string =>
  !namespace || namespace === DEFAULT_NAMESPACE ? name : `${namespace}/${name}`

export const jobNamespace = (id: string): string => splitJobId(id).namespace

/** `payments-prod/web`, or `default/web`: always spelled out, for headers. */
export const qualifiedDisplay = (id: string): string => {
  const { namespace, name } = splitJobId(id)
  return `${namespace}/${name}`
}

/** The detail route of a job: `/jobs/<namespace>/<name>`. */
export const jobPath = (id: string): string => {
  const { namespace, name } = splitJobId(id)
  return `/jobs/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`
}

/** The detail route of a service: `/services/<namespace>/<name>`. */
export const servicePath = (namespace: string, name: string): string =>
  `/services/${encodeURIComponent(namespace || DEFAULT_NAMESPACE)}/${encodeURIComponent(name)}`

/** The detail route of a canonical service reference (`ns/name` or `name`). */
export const serviceRefPath = (ref: string): string => {
  const { namespace, name } = splitJobId(ref)
  return servicePath(namespace, name)
}

/** The job id a `/jobs/:namespace/:name/...` route names. */
export const jobIdFromParams = (params: { namespace?: string; name?: string }): string =>
  jobIdOf(params.namespace, params.name ?? '')

/** The instance key: the id without its namespace. */
export const instanceKey = (id: string): string => splitJobId(id).name

/**
 * The namespaces the caller can read, as WhoAmI reports them: the existing
 * namespaces where the identity holds any allow (all of them for a
 * management token or with ACLs off). Empty while the identity is
 * unresolved.
 */
export const readableNamespaces = (): string[] => managerIdentity()?.readableNamespaces ?? []

/**
 * Whether tenancy UI (the switcher, the namespace column, the All view) is
 * worth showing: the identity can read more than one namespace. A
 * single-namespace identity sees a static badge instead.
 */
export const isMultiNamespace = (): boolean => readableNamespaces().length > 1

/** The `?ns=` selection of a list page, parsed: names or globs. */
export const parseSelection = (raw: string | undefined): string[] =>
  (raw ?? '')
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0)

/** Whether a selection spans more than one namespace (shows the column). */
export const selectionIsMulti = (selection: string[]): boolean =>
  selection.length > 1 || selection.some((part) => part.includes('*'))
