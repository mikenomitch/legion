# Judgement Calls — Workload Identity (OIDC)

Companion to
[`plans/workload-identity-oidc-plan.md`](../plans/workload-identity-oidc-plan.md).
Decisions made while implementing the plan where it was ambiguous,
contradictory, or silent. Recorded during the 2026-06-09 implementation pass.

Entries are ranked by project impact, highest first.

## 1. Issuance authz follows the existing worker trust model (and documents the gap)

Worker↔manager RPCs today have no cryptographic worker authentication: heartbeats
carry a self-claimed node id, server TLS identity is not enforced, and join tokens
gate joining only. `IssueWorkloadIdentityToken` therefore enforces what is
enforceable in that model — the allocation must exist and be placed on the
`node_id` the caller claims (PermissionDenied otherwise) — and the gap is
documented in the proto comment plus a `TODO(worker-authn)` referencing the
join-token trust model. Inventing a one-off credential scheme just for this RPC
would have created a second auth system; binding all worker RPCs to per-node
credentials (worker mTLS) is the right fix and is out of scope.

## 2. Rotation runs on the persistence Primary; keys live in SQLite

There is no raft-leader hook for background loops, but `store.persistence_role()`
(Primary/Standby/Candidate) exists — the rotation loop acts only on the Primary
and all managers serve the shared `signing_key` table, so JWKS is identical
cluster-wide. Signing keys (private PEM included) persist in the manager's SQLite,
the same trust domain as the CA material; tokens issued before a manager restart
now verify after it. Retiring keys stay published for `max(rotationAge,
defaultTtl)`; expired rows are retained for audit but never reloaded.

## 3. Red-team hardening (2026-06 adversarial review)

- Token `exp` is clamped to the active key's rotation overlap — chosen over
  admission rejection because task validation can't see `rotationAge`.
- Identity names are charset-validated against path traversal.
- Token temp files are created at their final mode with no umask window.
- The signer refuses `extra_claims` that would override reserved/`legion.io/*`
  claims.
- The cluster-default `sub_template` is validated at startup.

### Known limitation deliberately left documented, not fake-fixed

The WI issuance RPC's authorization checks the allocation's `node_id` against a
**caller-supplied, unauthenticated** node id — because Legion has no per-worker
credential today (heartbeats are unauthenticated too; workers are admitted by
join token). This is the cluster-wide `TODO(worker-authn)` gap, not a regression
in this work. The right fix is worker mTLS binding all worker RPCs to a per-node
credential; faking a one-off scheme for this single RPC would create a second
auth system. Surfaced here and in the proto/`server.rs` comments so it's visible.

## 4. Failed token issuance fails the launch

If a task declares an identity and issuance fails for any reason other than the
feature being disabled, the worker records an allocation failure (so the manager
reschedules) instead of starting the task without its declared identity. Feature
disabled = identities are inert: specs validate, issuance returns
FailedPrecondition, the worker warns once.

## 5. Env-delivered tokens are mirrored to a file with an `<ENV>_FILE` pointer

Env vars cannot change after exec, so a refreshed env-delivered token needs
somewhere to land: every env identity is also written to
`.legion/identity/<alloc>/<name>/token` (mode 0400) and a companion `<ENV>_FILE`
variable points at it. `signal` handlers re-read that file; `restart` updates the
stored task env so the relaunched process gets the fresh token. This mirrors
Nomad's file-first guidance while keeping env delivery ergonomic.

## 6. OIDC endpoints via a hand-rolled tower layer, not axum

The discovery/JWKS routes are served by `OidcHttpLayer` mounted outermost on the
existing main listener (tonic 0.12 with `accept_http1(true)`), intercepting GET on
exactly two paths and passing everything else through. No axum dependency, no
coupling to tonic router internals, pure passthrough when the feature is disabled.
The issuer/jwks URLs in the discovery document come from the configured issuer URL
so they remain correct behind proxies/load balancers.

## 7. Region claim defaults to `auto`

The manager has no region config field today; issuance stamps
`legion.io/region: "auto"` (`DEF_CLUSTER_REGION`), matching every documented `sub`
example. When a real region config lands, it threads through one constant.
