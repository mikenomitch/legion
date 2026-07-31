/*
 * Client-side mirror of the manager's ACL evaluation, fed by the effective
 * rules WhoAmI reports for the caller (`rulesJson`). This is COSMETIC ONLY —
 * the manager is the enforcement point; this exists so the UI can hide nav
 * sections and disable controls the identity cannot use instead of letting
 * every click fail with PermissionDenied.
 *
 * The semantics deliberately mirror src/structs/acl.rs:
 * - default deny; a matching deny rule always wins
 * - `*` is the only wildcard, and exact patterns never prefix-match
 * - a rule with a scope selector never matches a request without that
 *   attribute (which is why `templates`-scoped job rules grant nothing to
 *   raw submits)
 * - the wildcard scope (`any`) asks "could any instance be visible?" and is
 *   what nav-level gating uses
 */
import { managerIdentity } from './connection'

export type AclResource =
  | 'job'
  | 'node'
  | 'kv'
  | 'state'
  | 'operator'
  | 'acl'
  | 'exec'
  | 'action'
  | 'template'
  | 'scale'

export type AclActionKind = 'read' | 'write'

export type AclRule = {
  resources: string[]
  actions: string[]
  effect?: string
  namespaces?: string[]
  names?: string[]
  pools?: string[]
  paths?: string[]
  actionNames?: string[]
  templates?: string[]
  host?: boolean
}

export type AclScope = {
  any?: boolean
  namespace?: string
  name?: string
  pool?: string
  path?: string
  actionName?: string
  template?: string
  host?: boolean
}

// Jobs all live in the implicit default namespace today (see acl.rs).
export const DEFAULT_NAMESPACE = 'default'

export const jobScope = (name: string, template?: string): AclScope => ({
  namespace: DEFAULT_NAMESPACE,
  name,
  template,
})

const globMatch = (pattern: string, value: string): boolean => {
  const parts = pattern.split('*')
  if (parts.length === 1) return pattern === value

  let remaining = value
  for (let i = 0; i < parts.length; i += 1) {
    const part = parts[i]
    if (part === '') continue
    if (i === 0) {
      if (!remaining.startsWith(part)) return false
      remaining = remaining.slice(part.length)
    } else if (i === parts.length - 1) {
      return remaining.endsWith(part) && remaining.length >= part.length
    } else {
      const index = remaining.indexOf(part)
      if (index < 0) return false
      remaining = remaining.slice(index + part.length)
    }
  }
  return true
}

const selectorMatches = (selector: string[] | undefined, attr: string | undefined): boolean => {
  if (!selector) return true
  if (attr === undefined) return false
  return selector.some((pattern) => globMatch(pattern, attr))
}

const hasScopeSelectors = (rule: AclRule): boolean =>
  Boolean(
    rule.namespaces || rule.names || rule.pools || rule.paths || rule.actionNames || rule.templates,
  )

const ruleCovers = (rule: AclRule, resource: AclResource, action: AclActionKind): boolean =>
  rule.resources.some((entry) => entry === '*' || entry === resource) &&
  rule.actions.some((entry) => entry === '*' || entry === action)

const ruleMatches = (
  rule: AclRule,
  resource: AclResource,
  action: AclActionKind,
  scope: AclScope,
): boolean => {
  if (!ruleCovers(rule, resource, action)) return false
  if (scope.any) return true
  if (resource === 'exec' && scope.host && rule.host !== true) return false
  return (
    selectorMatches(rule.namespaces, scope.namespace) &&
    selectorMatches(rule.names, scope.name) &&
    selectorMatches(rule.pools, scope.pool) &&
    selectorMatches(rule.paths, scope.path) &&
    selectorMatches(rule.actionNames, scope.actionName) &&
    selectorMatches(rule.templates, scope.template)
  )
}

export const evaluateRules = (
  rules: readonly AclRule[],
  resource: AclResource,
  action: AclActionKind,
  scope: AclScope,
): boolean => {
  let allowed = false
  for (const rule of rules) {
    if (rule.effect === 'deny') {
      if (!ruleCovers(rule, resource, action)) continue
      const denyHits = scope.any ? !hasScopeSelectors(rule) : ruleMatches(rule, resource, action, scope)
      if (denyHits) return false
    } else if (ruleMatches(rule, resource, action, scope)) {
      allowed = true
    }
  }
  return allowed
}

const parseRules = (rulesJson: string | undefined): AclRule[] | null => {
  if (!rulesJson) return null
  try {
    const parsed = JSON.parse(rulesJson)
    return Array.isArray(parsed) ? (parsed as AclRule[]) : null
  } catch {
    return null
  }
}

/**
 * Whether the current identity may perform `action` on `resource` within
 * `scope`. Reactive: reads the `managerIdentity` signal.
 *
 * Fails OPEN when ACLs are disabled, in audit-only mode, or when the
 * identity has not resolved yet (a transient probe failure must not brick
 * the UI). Fails CLOSED on the definite signal: a resolved identity whose
 * rules do not match. The server enforces regardless.
 */
export const allows = (
  resource: AclResource,
  action: AclActionKind,
  scope: AclScope = { any: true },
): boolean => {
  const identity = managerIdentity()
  if (!identity) return true
  if (!identity.aclEnabled || identity.auditOnly) return true
  if (identity.tokenType === 'management') return true

  const rules = parseRules(identity.rulesJson)
  if (!rules) return false
  return evaluateRules(rules, resource, action, scope)
}

/** True when the caller presented no credential (and ACLs are on). */
export const isAnonymous = (): boolean => {
  const identity = managerIdentity()
  return Boolean(identity?.aclEnabled) && identity?.source === 'anonymous'
}

/**
 * The resource a top-level route needs `read` on, used to decide which
 * sidebar destinations to offer. Routes absent from this map need no
 * particular permission.
 */
export const ROUTE_RESOURCE: Record<string, AclResource> = {
  '/jobs': 'job',
  '/services': 'job',
  '/rollouts': 'job',
  // Volumes are job-scoped storage, gated under `job` like their RPCs.
  '/volumes': 'job',
  '/templates': 'template',
  '/nodes': 'node',
  '/manager': 'operator',
  '/raft': 'operator',
  '/events': 'operator',
  '/observability': 'operator',
  '/kv': 'kv',
  '/state-explorer': 'state',
}

/** Nav-level check: could the identity read any instance of `resource`? */
export const canRead = (resource: AclResource): boolean => allows(resource, 'read')

/** Wildcard write check, for controls whose exact scope is not yet known. */
export const canWriteAny = (resource: AclResource): boolean => allows(resource, 'write')
