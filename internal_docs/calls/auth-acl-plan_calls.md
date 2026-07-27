# Judgement Calls — Auth & ACL System

Companion to [`plans/auth-acl-plan.md`](../plans/auth-acl-plan.md).
The decisions behind the ACL model, pulled out of the design prose. Section
references point back at the plan.

Entries are ranked by project impact, highest first.

## 1. Default-deny, and a matching deny always wins (§2)

Once ACLs are enabled, no rule = no access, and deny beats allow regardless of
specificity. Specificity-ordered precedence is the usual alternative and the usual
source of "why did this allow?" incidents.

## 2. A closed vocabulary: 6 resources × 2 actions (§2)

`job | node | kv | state | operator | acl` and `read | write`, deliberately small
enough that every endpoint can be classified by hand and the classification is
testable. The surveyed alternative — a general authz engine (Zanzibar/OpenFGA/
Cedar/OPA style relationship or policy language) — buys expressiveness Legion
does not need at the cost of a second mental model and a policy debugger.

## 3. The `:4748` bootstrap listener and JWT join tokens were removed, not fixed (§2, Phase 3)

The original join tokens were HS512 JWTs signed with a compiled-in constant key,
minted by an unauthenticated listener — forgeable by anyone holding the binary,
so they authenticated nothing. Replaced by manager-minted stored secrets
(`CreateJoinToken`, `node:write`, hashed at rest, revocable, 24h default) plus
per-node tokens issued at registration. The composite token string keeps
`legion join <TOKEN>` a one-command flow; the in-process worker self-issues a
short-lived join token so single-binary dev clusters still work with ACLs on.

## 4. ACLs are off by default, with `auditOnly` as the migration ramp (§2)

Three modes: off (everything allowed, UI shows a dismissable warning),
`enabled + auditOnly` (denials logged under the `acl_audit` target but allowed),
and enforced. Turning on a default-deny system in one step on a live cluster is
how operators get locked out; audit-only lets them find the gaps first. The
`anonymous` policy exists for the same reason.

## 5. One `AuthMethod` trait, shaped like Legion's other plugin entry traits (§2)

Everything that is not an opaque ACL token (userpass, JWT/OIDC, future methods)
goes through a single object-safe `Send + Sync` trait dispatched by name — the
same shape as `TaskPlugin` / `SchedulingPolicy` / `RolloutStrategyPlugin`. This
keeps identity sources a closed, auditable seam today and makes them a plugin
category later without redesign. Hard boundary: a method can only return a
principal plus policy *names*; minting, secret hashing, and policy evaluation
never live inside a method, so no method can grant management access.

## 6. Policies attach to tokens by exact name — globs never apply to attachment (§2)

Glob matching is available *inside* rules (scope selectors), never for deciding
which policies a token carries. This is the CVE-2025-4922 lesson: glob-based
attachment means creating a policy can silently widen existing tokens.

## 7. Minting a management token requires presenting one (§2, escalation control)

`acl:write` is enough to manage policies and mint **client** tokens, but not to
mint a **management** token. Without that asymmetry, `acl:write` is a full
privilege-escalation primitive.

## 8. Opaque token secrets, hashed at rest; accessor IDs are the public handle (§2)

`lgn_<64 hex>` with only the SHA-256 stored. Audit and revocation work off the
accessor ID, so logs and UIs never need the secret. Signed/self-describing tokens
were rejected: they cannot be revoked without a store lookup anyway.

## 9. Check before you fetch (§2)

When a scope requires a lookup (instance → job, rollout → job), enforce a coarse
check first and the precise scope after. Otherwise credential-less callers can
probe resource existence through NotFound-vs-PermissionDenied differences.

## 10. List endpoints gate wide, then filter per item (§2)

Instead of a per-item permission check, list RPCs gate on `AclScope::any()` and
then filter results with `ident.allows(...)`, so a scoped token sees its slice
rather than a blanket denial.

## 11. `write` does not imply `read` (§2)

Explicitly *not* a hierarchy. A token that can submit jobs but not read cluster
state is a legitimate (and common) CI shape, and implication would make that
unexpressible.

## 12. `PermissionDenied` vs `Unauthenticated`, with the required capability in the message (§2)

Missing/insufficient permission returns `PermissionDenied` naming the capability
and a hint (`set LEGION_TOKEN or run legion login`); an unknown, expired, or
invalid credential returns `Unauthenticated`. Collapsing both into one status is
more "secure by obscurity" than useful — Legion's callers include agents that
need to self-correct.

## 13. `state:read` is documented as sensitive rather than split up (§2)

Raw SQL can read every table (hashes included, never plaintext secrets). Rather
than inventing a partial-SQL permission, the capability is named as sensitive and
kept coarse.

## 14. Deliberately deferred (§4)

Dynamic auth-method dylib plugins, worker mTLS, workload-identity API
credentials, and the token-resolution cache were all scoped out after Phases 1–4.
Worker mTLS is the notable one: it is the correct fix for the
`TODO(worker-authn)` gap that
[`workload-identity-oidc-plan_calls.md`](workload-identity-oidc-plan_calls.md)
records, and it stays out of scope precisely so it is done once, for all worker
RPCs, rather than per endpoint.
