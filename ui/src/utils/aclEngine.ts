/*
 * The pure half of the UI's ACL mirror: rule types and evaluation, with no
 * imports, so the parity test (`npm run test:unit`, ui/tests/unit) can load
 * it in plain Node. `acl.ts` wraps it with the caller's identity.
 *
 * The semantics mirror `evaluate` in crates/common/src/structs/acl.rs exactly, and the
 * shared cases in tests/fixtures/acl_parity_cases.json are run against both
 * (here by ui/tests/unit/acl-parity.mjs, in Rust by the `parity_fixture`
 * test in crates/common/src/structs/acl.rs). Change the two together, and add a case:
 * - default deny; a matching deny rule always wins
 * - `*` is the only wildcard, and exact patterns never prefix-match
 * - a rule with a scope selector never matches a request without that
 *   attribute (which is why `templates`-scoped job rules grant nothing to
 *   raw submits)
 * - reserved prefixes: an allow rule's `namespaces` and `pools` patterns
 *   reach a `legion-*` value only when the pattern itself starts `legion-`,
 *   and its `paths` patterns reach a `legion/` key only when the pattern
 *   starts `legion/`; deny rules are plain globs
 * - the wildcard scope (`any`) asks "could any instance be visible?" and is
 *   what nav-level gating uses
 * - a `via = "objects"` rule is inert outside an app's object-scoped call
 */

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
  // Boot tickets, scoped by job like `scale`.
  | 'boot'
  | 'net'
  // Snapshots of instances, scoped by job like `job`. `snapshot:write` on a
  // job is as strong as reading its memory (docs/auth-and-acls.md).
  | 'snapshot'
  // Namespace objects themselves; `write` creates and deletes them.
  | 'namespace'
  | 'app'
  // Objects of app-declared kinds, scoped by namespace, name, and `kinds`.
  | 'object'
  | 'audit'
  | 'approval'

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
  bands?: string[]
  host?: boolean
  kinds?: string[]
  via?: string
}

export type AclScope = {
  any?: boolean
  namespace?: string
  name?: string
  pool?: string
  path?: string
  actionName?: string
  template?: string
  band?: string
  host?: boolean
  /** `<app>/<Kind>`, on a request about an app-declared object. */
  kind?: string
}

export const DEFAULT_NAMESPACE = 'default'

/** Namespaces and node pools Legion keeps for itself. */
const RESERVED_NAMESPACE_PREFIX = 'legion-'
const RESERVED_POOL_PREFIX = RESERVED_NAMESPACE_PREFIX
/** The KV prefix Legion keeps for itself. */
const RESERVED_KV_PREFIX = 'legion/'

export const globMatch = (pattern: string, value: string): boolean => {
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

/** acl.rs `reserved_glob_match`: `*` skips a reserved value, like a shell's dotfiles. */
const reservedGlobMatch = (pattern: string, value: string, reserved: string): boolean => {
  if (value.startsWith(reserved) && !pattern.startsWith(reserved)) return false
  return globMatch(pattern, value)
}

const isDeny = (rule: AclRule): boolean => rule.effect === 'deny'

const selectorMatches = (selector: string[] | undefined, attr: string | undefined): boolean => {
  if (!selector) return true
  if (attr === undefined) return false
  return selector.some((pattern) => globMatch(pattern, attr))
}

/*
 * acl.rs `namespace_selector_matches`. Allow patterns follow the reserved
 * rule; deny patterns are plain globs. Without the selector: a deny rule
 * matches every namespace, a `*`-resource allow rule what `*` reaches (not
 * the reserved ones), and an allow rule naming namespaced resources
 * explicitly (stored before namespaces existed) reaches `default` only.
 */
const namespaceMatches = (rule: AclRule, namespace: string | undefined): boolean => {
  if (namespace === undefined) return !rule.namespaces
  if (rule.namespaces) {
    return isDeny(rule)
      ? rule.namespaces.some((pattern) => globMatch(pattern, namespace))
      : rule.namespaces.some((pattern) =>
          reservedGlobMatch(pattern, namespace, RESERVED_NAMESPACE_PREFIX),
        )
  }
  if (isDeny(rule)) return true
  if (rule.resources.includes('*')) {
    return reservedGlobMatch('*', namespace, RESERVED_NAMESPACE_PREFIX)
  }
  return namespace === DEFAULT_NAMESPACE
}

/*
 * acl.rs `pools_selector_matches` / `paths_selector_matches`: with the
 * attribute present, an allow rule follows the reserved rule (and with no
 * selector reaches what `*` reaches); a deny rule is plain globs.
 */
const reservedSelectorMatches = (
  rule: AclRule,
  selector: string[] | undefined,
  attr: string | undefined,
  reserved: string,
): boolean => {
  if (attr === undefined || isDeny(rule)) return selectorMatches(selector, attr)
  if (!selector) return reservedGlobMatch('*', attr, reserved)
  return selector.some((pattern) => reservedGlobMatch(pattern, attr, reserved))
}

const hasScopeSelectors = (rule: AclRule): boolean =>
  Boolean(
    rule.namespaces ||
      rule.names ||
      rule.pools ||
      rule.paths ||
      rule.actionNames ||
      rule.templates ||
      rule.bands ||
      rule.kinds,
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
  // A `via = "objects"` rule is held only through an app's object-scoped
  // call, never by a plain identity.
  if (rule.via !== undefined || !ruleCovers(rule, resource, action)) return false
  if (scope.any) return true
  // A host-level target (an exec into one, or submitting a job with one)
  // needs a rule that opts into `host`.
  if ((resource === 'exec' || resource === 'job') && scope.host && rule.host !== true) return false
  return (
    namespaceMatches(rule, scope.namespace) &&
    selectorMatches(rule.names, scope.name) &&
    reservedSelectorMatches(rule, rule.pools, scope.pool, RESERVED_POOL_PREFIX) &&
    reservedSelectorMatches(rule, rule.paths, scope.path, RESERVED_KV_PREFIX) &&
    selectorMatches(rule.actionNames, scope.actionName) &&
    selectorMatches(rule.templates, scope.template) &&
    selectorMatches(rule.bands, scope.band) &&
    selectorMatches(rule.kinds, scope.kind)
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
    if (isDeny(rule)) {
      if (!ruleCovers(rule, resource, action)) continue
      const denyHits = scope.any ? !hasScopeSelectors(rule) : ruleMatches(rule, resource, action, scope)
      if (denyHits) return false
    } else if (ruleMatches(rule, resource, action, scope)) {
      allowed = true
    }
  }
  return allowed
}
