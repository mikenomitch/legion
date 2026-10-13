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

## 2. One cluster-wide issuer: a fixed default for multi-manager clusters, learned by nodes

**The call.** With `workloadIdentity.issuer` unset, a `raft` or `streaming`
cluster defaults to `https://manager.legion` on every manager. A single
`localDisk` manager defaults to its advertised address
(`server.advertiseAddr`). Nodes stop assuming `iss` is their join-token
address. They learn the issuer from `WorkerPreRegisterReply` and every
`HeartbeatReply.workload_identity_issuer`, and fetch the JWKS from any manager
they know, in failover order.

**Why it came up.** The 2026-10-12 AWS spot check. The default was built from
`server.host`, so a `0.0.0.0` bind published `http://127.0.0.1:4747` and
ignored `advertiseAddr`. Worse, any per-manager default gives each raft
manager a different issuer. A session ticket minted on one manager then failed
`InvalidIssuer` on a node that joined through another, and a control-plane
token minted by a peer failed its `aud`/`iss` check. The node compared `iss`
to its join address, which matched only by accident.

**Alternatives.**
- *Replicate the issuer.* The first manager records its advertised address in
  the store and the others adopt it. That honors `advertiseAddr` everywhere,
  but needs a new replicated record, and a follower booting before it catches
  up has nothing to adopt yet. If the recording manager is later removed, the
  issuer names a dead host.
- *Require an explicit issuer in raft mode.* Simple and honest, but it fails
  the out-of-the-box raft cluster on a setting that only federation actually
  needs.
- *Nodes accept any known manager address as `iss`.* This fixes nodes only.
  Control-plane tokens between managers stay broken, and the published
  discovery document still disagrees across managers.

**Trade-off.** `https://manager.legion` isn't fetchable from outside the
cluster, so the multi-manager default discovery document points nowhere
useful. Federation already needed an explicit issuer, and nodes fetch keys from
manager addresses, not the issuer URL. Legion also can't detect managers
configured with different explicit issuers. The docs say so, and the node's
`InvalidIssuer` error names the issuer it expected.

**Why it won.** Simplicity. The value is the same on every manager by
construction, needs no agreement at boot, survives losing any manager, and
reuses the name managers already answer to under the cluster CA. The
single-manager default stays a reachable URL for local development. Surface:
`docs/workload-identity.md` "The issuer", `docs/edge-boot.md` "Enabling it".

## 3. Rotation runs on the persistence Primary; keys live in SQLite

There is no raft-leader hook for background loops, but `store.persistence_role()`
(Primary/Standby/Candidate) exists — the rotation loop acts only on the Primary
and all managers serve the shared `signing_key` table, so JWKS is identical
cluster-wide. Signing keys (private PEM included) persist in the manager's SQLite,
the same trust domain as the CA material; tokens issued before a manager restart
now verify after it. Retiring keys stay published for `max(rotationAge,
defaultTtl)`; expired rows are retained for audit but never reloaded.

## 4. Red-team hardening (2026-06 adversarial review)

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

## 5. Failed token issuance fails the launch

If a task declares an identity and issuance fails for any reason other than the
feature being disabled, the worker records an allocation failure (so the manager
reschedules) instead of starting the task without its declared identity. Feature
disabled = identities are inert: specs validate, issuance returns
FailedPrecondition, the worker warns once.

## 6. Env-delivered tokens are mirrored to a file with an `<ENV>_FILE` pointer

Env vars cannot change after exec, so a refreshed env-delivered token needs
somewhere to land: every env identity is also written to
`.legion/identity/<alloc>/<name>/token` (mode 0400) and a companion `<ENV>_FILE`
variable points at it. `signal` handlers re-read that file; `restart` updates the
stored task env so the relaunched process gets the fresh token. This mirrors
Nomad's file-first guidance while keeping env delivery ergonomic.

## 7. OIDC endpoints via a hand-rolled tower layer, not axum

The discovery/JWKS routes are served by `OidcHttpLayer` mounted outermost on the
existing main listener (tonic 0.12 with `accept_http1(true)`), intercepting GET on
exactly two paths and passing everything else through. No axum dependency, no
coupling to tonic router internals, pure passthrough when the feature is disabled.
The issuer/jwks URLs in the discovery document come from the configured issuer URL
so they remain correct behind proxies/load balancers.

## 8. Region claim defaults to `auto`

The manager has no region config field today; issuance stamps
`legion.io/region: "auto"` (`DEF_CLUSTER_REGION`), matching every documented `sub`
example. When a real region config lands, it threads through one constant.
