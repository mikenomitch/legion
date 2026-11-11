/*
 * Client-side mirror of the manager's ACL evaluation, fed by the effective
 * rules WhoAmI reports for the caller (`rulesJson`). This is COSMETIC ONLY —
 * the manager is the enforcement point; this exists so the UI can hide nav
 * sections and disable controls the identity cannot use instead of letting
 * every click fail with PermissionDenied.
 *
 * The rule engine itself is `aclEngine.ts`, which mirrors
 * crates/common/src/structs/acl.rs and is checked against it by shared parity cases
 * (tests/fixtures/acl_parity_cases.json). This file adds the caller.
 */
import { managerIdentity } from './connection'
import { splitJobId } from './namespaces'
import {
  evaluateRules,
  type AclActionKind,
  type AclResource,
  type AclRule,
  type AclScope,
} from './aclEngine'

export type { AclActionKind, AclResource, AclRule, AclScope } from './aclEngine'
export { DEFAULT_NAMESPACE, evaluateRules } from './aclEngine'

/**
 * The scope of a job, from its canonical id (`payments-prod/web`, or the
 * bare `web` in `default`): the namespace and the bare name are separate
 * attributes, matched by a rule's `namespaces` and `names` selectors.
 */
export const jobScope = (id: string, template?: string): AclScope => {
  const { namespace, name } = splitJobId(id)
  return { namespace, name, template }
}

/** The scope of a namespace object itself (`namespace:read|write`). */
export const namespaceScope = (namespace: string): AclScope => ({ namespace, name: namespace })

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
  '/network': 'net',
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
  '/namespaces': 'namespace',
}

/** Nav-level check: could the identity read any instance of `resource`? */
export const canRead = (resource: AclResource): boolean => allows(resource, 'read')

/** Wildcard write check, for controls whose exact scope is not yet known. */
export const canWriteAny = (resource: AclResource): boolean => allows(resource, 'write')
