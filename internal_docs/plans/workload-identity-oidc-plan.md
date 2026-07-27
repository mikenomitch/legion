# Plan: Workload Identity via OIDC

> **Judgement calls:** [`calls/workload-identity-oidc-plan_calls.md`](../calls/workload-identity-oidc-plan_calls.md)

## Problem Statement

Legion schedules workloads but has no way to give a running task a **verifiable,
short-lived cryptographic identity**. Today, if a task needs to authenticate to an
external system (AWS, GCP, Vault, an internal service), the operator must bake a
long-lived static credential into the job spec, an env var, or a KV value. That is
the single biggest secret-sprawl problem in any scheduler.

Workload identity solves this by having the **manager act as an OIDC identity
provider (IdP)**. Each task instance receives a short-lived, asymmetrically-signed
JWT whose claims describe *what it is* (region, job, group, task, node). External
relying parties are configured to trust Legion's issuer and exchange that JWT for
their own native credentials (e.g. AWS `AssumeRoleWithWebIdentity`, GCP Workload
Identity Federation, Vault JWT auth). No long-lived secrets ever touch the job spec.

This plan covers issuing those tokens, publishing the OIDC discovery + JWKS
documents, delivering tokens into running tasks, and rotating signing keys.

---

## Goals

- **Manager-as-IdP:** Manager publishes a standards-compliant OIDC discovery
  document and JWKS so any third party can verify Legion-issued tokens.
- **Asymmetric signing:** Tokens are signed with a private key the manager never
  exposes; verification uses only the public JWKS. (Replaces the symmetric
  `HS512` + `TMP_SECRET_KEY` placeholder used today for join tokens.)
- **Per-task identity:** Each allocation can be issued a token whose claims are
  derived from real scheduling facts (region, job id, group, task name, node id).
- **Short-lived + auto-renewed:** Tokens have a short TTL (minutes) and are
  refreshed in-place by the worker before expiry, with no task restart.
- **Multiple named identities per task:** A task can declare several independent
  identities, each with its own `name`, audience, TTL, delivery, and change mode —
  so one workload can federate into AWS *and* Vault with correctly-scoped tokens
  (mirrors Nomad's `identity` blocks).
- **Change-mode on refresh:** Each identity declares what happens when its token is
  re-issued — `noop` (just rewrite the file), `signal` (send e.g. SIGHUP so the app
  reloads), or `restart` — reusing the existing injection/reconcile action vocabulary.
- **Control-plane-ready:** The design must support a built-in identity that a
  workload uses to authenticate *back to Legion itself* (KV/secret store, manager
  APIs) in a future phase — so tasks can drop static cluster credentials too.
- **Spec-level opt-in:** Identities are declared in the job spec per task, delivered
  as an env var and/or file, reusing the existing injection plumbing.
- **Key rotation:** Signing keys rotate on a schedule; old public keys remain in
  the JWKS until all tokens signed by them have expired.
- **HA-safe:** In a Raft cluster, all managers serve the same issuer and JWKS so
  verification succeeds regardless of which manager a relying party hits.

## Non-Goals

- **Authorization / policy engine.** This plan issues *identity*; it does not decide
  what that identity may do inside Legion. (ACLs are a separate effort.)
- **Replacing the worker join-token flow.** Join tokens ([src/structs/token.rs](../../src/structs/token.rs))
  keep their own path for now; we *may* later migrate them onto the same asymmetric
  signer, but that is out of scope here.
- **Being a generic secret store.** We don't store provider credentials; we issue a
  token the provider exchanges for its own credential.
- **Client-side SDKs for every cloud.** We deliver a spec-compliant JWT; wiring it
  into AWS/GCP/Vault is the relying party's job (we document the pattern, not a shim).

---

## Background: What Already Exists

Grounding the design in current code so we extend rather than reinvent:

| Concern | Where | Notes |
| --- | --- | --- |
| JWT encode/decode | `jsonwebtoken = "9.1.0"` (Cargo.toml) | Already a dependency; supports RS256/ES256. |
| Existing token + claims | [src/structs/token.rs](../../src/structs/token.rs), [src/manager/join_server.rs](../../src/manager/join_server.rs) | `JoinTokenClaims`, `generate_join_token`. Uses `HS512` + `TMP_SECRET_KEY` — a placeholder, **not** suitable for OIDC. |
| Signing key placeholder | [src/constants.rs:1](../../src/constants.rs:1) | `TMP_SECRET_KEY = b"secret"`. To be superseded by a managed asymmetric keypair. |
| CA / key generation | [src/certs.rs](../../src/certs.rs) | `rcgen`-based CA + leaf signing; a good model for key provenance/storage layout. |
| Task launch + env/file injection | [src/task_plugins/exec.rs](../../src/task_plugins/exec.rs), [container.rs](../../src/task_plugins/container.rs) | Env via `cmd.envs`/`--env`; exec already supports injected files. Token delivery rides this path. |
| Capability gating | `TaskPlugin` trait ([src/task_plugins/mod.rs:77](../../src/task_plugins/mod.rs)) | `supports_kv_env_injection`, `supports_kv_file_injection`, `supports_live_restart`. We add identity capability flags here. |
| KV injection model | `TaskKvInjections` ([src/structs/job.rs:849](../../src/structs/job.rs)) | Proven spec→materialization pattern to mirror for identity. |
| Manager gRPC API | `proto/legion.proto` `service Manager` | Add token-issuance RPC here. |
| State / migrations | [src/storage/migrations.rs](../../src/storage/migrations.rs), SQLite | Signing-key metadata persisted here. |

**Key architectural insight:** OIDC discovery (`/.well-known/openid-configuration`)
and JWKS must be served over **plain HTTPS**, because AWS/GCP/Vault fetch them with
generic HTTP clients — they cannot speak gRPC. The manager therefore needs a small
HTTP listener for these two read-only public endpoints, separate from the gRPC
`Manager` service.

---

## Proposed Design

### 1) The Token (claims)

A workload identity token is an asymmetrically-signed JWT. The standard claims:

- `iss` — issuer URL, e.g. `https://<manager-host>:<port>` (must match discovery doc).
- `sub` — stable subject string identifying the workload (see decision below).
- `aud` — audience(s) requested by the task (provider-specific).
- `exp` / `iat` / `nbf` — short TTL window.
- `jti` — unique token id (for audit/revocation tracing).
- `kid` (header) — id of the signing key, so verifiers pick the right JWKS entry.

Plus Legion-specific claims that expose each scheduling fact discretely, so
providers that can match arbitrary claims don't have to parse `sub`:

- `legion.io/region`, `legion.io/job`, `legion.io/group`, `legion.io/task`,
  `legion.io/node`, `legion.io/alloc`.
- For stateful workloads only: `legion.io/instance` (and `legion.io/slot` in slot
  mode), defined by the
  [Stateful Workloads & Instance Identity plan](stateful-workloads-and-instance-identity-plan.md).

  (Legion has no `namespace` concept today — [src/structs/config.rs:204](../../src/structs/config.rs)
  defines `region` as the top-level scope. If namespaces are introduced later, they
  slot into the `sub` exactly where Nomad puts them; see below.)

Plus optional **operator-defined extra claims** (see "Extra Claims" below).

#### How `sub` is consumed, and why its format is the critical decision

A relying party configures trust on `iss` + `aud`, then writes conditions matching
claims. The catch is *which* claims each provider can actually match:

| Provider | What it can match in a trust/role policy |
| --- | --- |
| **AWS IAM** (`AssumeRoleWithWebIdentity`) | Practically only `sub` and `aud`. Custom claims are **not** exposed as IAM condition keys. |
| **GCP Workload Identity Federation** | Any claim, via attribute mappings (`assertion.<claim>` → attribute) + conditions. |
| **Vault JWT auth** | Any claim — `bound_subject`, `bound_audiences`, `bound_claims` on arbitrary claims. |

> ★ Insight ─────────────────────────────────────
> This asymmetry is why `sub` granularity is the single most important — and
> effectively irreversible — decision here. For the *least* flexible major provider
> (AWS), `sub` is the **only** fine-grained routing key, so its format dictates how
> narrowly an IAM role can be scoped. Too coarse (`job` only) and every task shares
> one role's blast radius, with no other lever. Too fine (`alloc_id`, unique per
> placement) and no stable policy can be written because it changes every reschedule.
> Vault/GCP users barely notice the difference because they can match `legion.io/*`
> claims directly — but AWS users live and die by `sub`.
> ─────────────────────────────────────────────────

**What Nomad does (recommended model):** Nomad emits both — a stable *composed*
subject **and** the discrete facts as separate claims. Its default `sub` is roughly
`<region>:<namespace>:<job>:<group>:<task>`, and critically `allocation_id` is its
**own claim, never part of `sub`**, precisely because it churns. We copy this model,
dropping the segment Legion doesn't have (namespace) and keeping Nomad's colon
delimiter:

```
sub = <region>:<job>:<group>:<task>
```

e.g. `auto:api:web:server`. This is stable across reschedules and gives per-task least
privilege for AWS-style "only `sub` matters" policies, with `alloc`/`node` available
only as `legion.io/*` claims for providers that can match arbitrary claims.

**Stateful / per-instance identity.** For workloads where each *instance* of a group
needs its own identity that persists across allocations — traditional stateful
replicas (slot-based) and sandbox-style instances (keyed) alike — the `sub` is
extended with a stable instance key:
`sub = <region>:<job>:<group>:<task>:<instance-id>`, plus a `legion.io/instance`
(and `legion.io/slot` in slot mode) claim. The instance key is **not** the churning
`alloc_id`; it is a stable handle defined by the
[Stateful Workloads & Instance Identity plan](stateful-workloads-and-instance-identity-plan.md),
which this plan depends on for that case. Because the key is stable, each reschedule
mints a *fresh* short-lived token carrying the *same* `sub` — we persist the identity,
never the token bytes. Keyed ids are guaranteed never to be recycled, so a destroyed
sandbox's `sub` can never be re-minted for a different instance.

**Recommended default + override.** I've sketched the claims type below. Unlike the
first draft, I now recommend a concrete default (matching Nomad's rationale) rather
than leaving it blank — but the format should be operator-overridable via cluster
config, since it's a trust boundary your org owns. Drop this into
`src/structs/workload_identity.rs` in Phase 1:

```rust
/// Claims describing a workload's verifiable identity.
pub struct WorkloadIdentityClaims {
    pub region: String,
    pub job: String,
    pub group: String,
    pub task: String,
    pub node_id: String,
    pub alloc_id: String,
}

impl WorkloadIdentityClaims {
    /// Build the OIDC `sub` claim — the string external IAM policies match on, so
    /// its granularity is a security boundary.
    ///
    /// Mirrors Nomad's composed subject (minus the namespace segment Legion lacks):
    /// stable across reschedules, per-task least privilege. Do NOT include
    /// alloc_id/node_id (they churn → no stable policy); those ride along as
    /// `legion.io/*` claims for providers that can match arbitrary claims. Operators
    /// may override the format via manager config (see config additions).
    pub fn subject(&self) -> String {
        format!("{}:{}:{}:{}", self.region, self.job, self.group, self.task)
    }
}
```

#### Customizing `sub` — the template

The hard-coded `subject()` above is the *default*. Operators can override the format
with a string template so the `sub` matches whatever convention their existing IAM
trust policies expect (different delimiter, ordering, or a prefix). The template is set
as a cluster-config default and may be overridden **per identity** (gated), since
multi-cloud workloads sometimes need different subject shapes per provider.

It is deliberately **dumb `{field}` substitution, not a logic engine** — no
conditionals, no expressions — which avoids an injection surface and keeps the rendered
value predictable. Example:

```jsonnet
// manager config (cluster default)
workload_identity: {
  sub_template: '{region}:{job}:{group}:{task}',   // == the built-in default
}

// per-identity override (e.g. to match an existing AWS policy convention)
identities: [
  { name: 'aws', sub_template: 'legion/{job}/{task}', audience: ['sts.amazonaws.com'], /* ... */ },
]
```

Allowed placeholders are **stable identity fields only**:
`{region}`, `{job}`, `{group}`, `{task}`, and `{instance}` / `{slot}` (stateful only,
per the [stateful plan](stateful-workloads-and-instance-identity-plan.md)).

`sub` is a security boundary, so a template doesn't just format a string — it can
*break* the two invariants the system otherwise guarantees. Admission validation
therefore enforces:

- **Allow-list only.** Unknown placeholder → reject. In particular `{alloc}` and
  `{node}` are **forbidden in `sub`**: they churn, and including them would silently
  destroy the reschedule-stable-identity guarantee. They remain claims-only.
- **Non-empty render.** A template that renders empty → reject.
- **Uniqueness floor.** A template must retain enough fields to keep distinct
  workloads distinct (e.g. omitting everything but `{job}` collapses every group/task
  into one shared identity). Reject (or at minimum hard-warn) templates that drop below
  job+group+task granularity.
- **Stateful coupling.** `{instance}`/`{slot}` are only valid on tasks that declare a
  `stateful` block; using them elsewhere → reject.

> ★ Insight ─────────────────────────────────────
> The subtle trap: a `sub` template turns *stability across reschedules* and
> *uniqueness across workloads* — invariants the system enforces structurally today —
> into things an operator can accidentally break with a typo. Whitelisting only stable
> fields preserves stability by construction; the admission uniqueness check is the
> only thing defending the second invariant. Without both rails, "customizable `sub`"
> is really "a customizable way to merge two workloads' identities."
> ─────────────────────────────────────────────────

#### Extra Claims (operator-defined)

Each identity may declare an `extra_claims` map merged into the JWT. To prevent
forgery of the trust-critical fields, validation **rejects** (via `bail!`, matching
the codebase style) any key in the registered set `{iss, sub, aud, exp, iat, nbf,
jti, kid}` or the reserved `legion.io/*` namespace — a typo'd `sub` should fail loud,
not silently mint an unusable token. Extra claims are **static literals** in v1;
templating/KV-interpolation into claims is a deliberate later step (it adds an
injection surface).

### 2) Manager as OIDC Issuer

- **Signing keypair.** On first boot (or migration), the manager generates an
  asymmetric keypair (recommend **ES256** — small JWKS, fast verify; RS256 is the
  safer default if a relying party rejects EC keys). Reuse the file-layout and
  atomic-write conventions from [src/certs.rs](../../src/certs.rs) (`configs/tls/…`,
  temp-file + rename). Private key stays on the manager; never serialized into a
  token or RPC reply.
- **Key metadata in SQLite.** Persist `{ kid, alg, public_jwk, created_at,
  not_after, state }` via a new migration in [src/storage/migrations.rs](../../src/storage/migrations.rs).
  `state ∈ {active, retiring, expired}`.
- **Discovery + JWKS over HTTP.** These are plain HTTP/1.1 `GET` routes returning
  JSON — external providers (AWS STS, GCP, Vault) use generic HTTP clients and never
  speak gRPC/grpc-web, so a gRPC method won't do. A new module (e.g.
  `src/manager/oidc_http.rs`) mounts an `axum` router serving exactly two
  unauthenticated routes:
  - `GET /.well-known/openid-configuration` → issuer, `jwks_uri`,
    `id_token_signing_alg_values_supported`, `response_types_supported`, etc.
  - `GET /<jwks_path>` → all non-expired public keys as a JWKS.

  **Where to host it — and why not the join server.** The join server
  ([src/manager/daemon.rs:48-57](../../src/manager/daemon.rs)) is the manager's
  existing *unauthenticated* surface, which is conceptually the right kind of place.
  But it is a **non-TLS `tonic` gRPC** listener, and AWS/GCP require the `iss` to be
  reachable over **publicly-trusted HTTPS**. The main manager listener
  ([src/manager/daemon.rs:90-95](../../src/manager/daemon.rs)) already has TLS *and*
  `accept_http1(true)` + CORS, so it is the better home: mount the axum routes there
  as an HTTP fallback alongside the gRPC `Manager` service. (Dev / Vault-with-CA-bundle
  setups can tolerate the non-TLS join port behind a proxy, but the default should be
  the TLS listener.)

  These must be reachable by external providers, so the bind address/issuer URL is
  operator-configured (see config additions).

### 3) Token Issuance Flow

The worker — not the task — requests tokens, so the private signing key and the
issuance RPC stay on the trusted control plane:

1. Worker is launching/managing an allocation that declares one or more identities.
2. For each identity, the worker calls a new gRPC `IssueWorkloadIdentityToken` on the
   `Manager` service, passing `{ alloc_id, identity_name }`. The call is authenticated
   by the existing worker↔manager channel (mTLS / join-token-derived trust).
3. Manager looks up the allocation, finds the named identity in its spec, derives
   `WorkloadIdentityClaims` from the scheduling record (it does **not** trust claims
   sent by the worker), applies that identity's `aud`/`ttl`/`extra_claims`, signs with
   the active key, returns `{ token, expires_at }`.
4. Worker materializes the token (Section 4) and schedules a refresh before `exp`,
   applying the identity's change mode on each refresh (Section 4a).

The manager — not the worker — owns the `aud` and claim derivation, so a compromised
worker can only obtain tokens for allocations placed on it, scoped to the audiences
the spec already declared.

### 4) Job Spec API + Delivery

Add an `identities` list to `TaskConfig` ([src/structs/job.rs:815](../../src/structs/job.rs)),
mirroring the `TaskKvInjections` shape. Each entry is an independent named identity —
this matches Nomad's `identity` blocks and lets one task federate into multiple
providers at once:

```jsonnet
{
  name: 'api',
  kind: 'container',
  identities: [
    {
      name: 'aws',                       // unique per task; used in the issuance RPC
      audience: ['sts.amazonaws.com'],   // one or more; manager-enforced, never empty
      ttl: '15m',                        // token lifetime; worker refreshes before exp
      file: {                            // deliver token as a file (recommended)
        path: '/var/run/legion/identity/aws/token',
        mode: '0400',
      },
      change_mode: 'noop',               // SDK re-reads the file each call → no reload
    },
    {
      name: 'vault',
      audience: ['vault.internal'],
      ttl: '5m',
      env: 'VAULT_WORKLOAD_TOKEN',       // deliver as env var
      change_mode: 'signal',             // env can't change live → signal the app
      change_signal: 'SIGHUP',
      extra_claims: {                    // operator-defined, static, non-reserved
        'app.example.com/tier': 'backend',
      },
    },
  ],
}
```

Rules:
- `name` is required and unique within the task (it's the selector in the issuance RPC
  and in delivery paths).
- At least one of `env` or `file` must be set per identity (otherwise the token is
  unused — fail validation with an actionable error).
- `audience` must be **non-empty** — reject empty at validation. An audience-less
  token is replayable against any trusting provider; Nomad treats `aud` as
  security-critical and so do we.
- Delivery reuses the existing env/file injection machinery in the task plugins; the
  token value is just another rendered input written before/while the task runs.

### 4a) Change Mode (refresh behavior)

Each identity declares what happens when its token is **re-issued before expiry**,
reusing the existing reconcile-action vocabulary rather than inventing a new one
(cf. `KvInjectionAction`):

- `noop` (default): just rewrite the delivered value. For `file` delivery the worker
  atomically rewrites in place (temp + rename, like [src/certs.rs](../../src/certs.rs));
  most cloud SDKs re-read the token file on each call, so no reload is needed.
- `signal`: rewrite, then send `change_signal` (e.g. `SIGHUP`) to the task so a
  long-running process reloads the token without restarting. This is the only way to
  refresh an **env**-delivered token short of a restart, since env is fixed at exec
  time.
- `restart`: rewrite, then restart the allocation/task in place.

Validation: `signal` requires the task kind to support signaling; `env` delivery with
`change_mode: noop` is rejected (it can never actually refresh) — env must use `signal`
or `restart`. **File + `noop` is the documented default.**

### 5) Task-Kind Capability Reporting

Extend the `TaskPlugin` trait ([src/task_plugins/mod.rs:57](../../src/task_plugins/mod.rs))
alongside the existing capability methods:

```rust
fn supports_identity_env(&self) -> bool { false }
fn supports_identity_file(&self) -> bool { false }
```

Validation in `validate_job_config` ([src/task_plugins/mod.rs:185](../../src/task_plugins/mod.rs))
rejects a spec whose delivery mode the task kind can't satisfy — same pattern already
used for KV injections. `exec` and `container` implement both; `noop` neither.

### 6) Key Rotation

- A manager background task (or a Raft-leader-only loop) rotates the signing key when
  the active key reaches `rotation_age`.
- On rotation: generate new key → mark new key `active`, old key `retiring` → keep
  serving the retiring key in the JWKS until `now > retiring.not_after`
  (= last possible `exp` of any token it signed) → then mark `expired` and drop from
  JWKS. This overlap is what prevents valid-but-unverifiable tokens.
- New tokens always sign with the `active` key; the `kid` header lets verifiers
  select correctly during the overlap window.

### 7) Control-Plane Identity (forward-compatible; full impl is a later phase)

Nomad gives every workload a **default identity** it uses to authenticate back to
Nomad's own APIs (reading Variables/secrets, registering services) and out to
Consul/Vault — which is how Nomad eliminates static cluster tokens. We want the same
endgame for Legion: a task should be able to authenticate to the manager's **KV /
secret store and APIs** with a short-lived identity instead of an ambient trust grant.

This plan does **not** implement that authorization, but its design must not preclude
it. Concretely:

- **Reserve a built-in identity name** (e.g. `legion`) that, when present, the manager
  issues with `aud` = the cluster's own issuer/identifier. The same issuance RPC,
  signer, JWKS, rotation, and delivery all apply unchanged — a control-plane identity
  is just an identity whose audience is Legion itself.
- **Verification path:** because the manager already publishes JWKS, the manager (and
  any Legion component) can verify these tokens with the exact same machinery external
  providers use — no second auth system.
- **What's deferred:** mapping a verified control-plane token to *permissions*
  (which KV prefixes / APIs it may touch) is the ACL effort called out in Non-Goals.
  This plan only guarantees the *identity* primitive that effort will build on.

Designing the claim set, issuance RPC, and JWKS to be audience-agnostic now means the
control-plane identity later is a config + authz change, not a re-architecture.

---

## Proto, Struct, and Config Changes

1. **`proto/legion.proto`** — add to `service Manager`:
   ```proto
   rpc IssueWorkloadIdentityToken (IssueWorkloadIdentityTokenRequest)
       returns (IssueWorkloadIdentityTokenReply);
   ```
   with request `{ string alloc_id; string identity_name; }` (audience is **not**
   worker-supplied — the manager reads it from the named identity in the spec) and
   reply `{ string token; int64 expires_at; }`. Regenerate structs via `build.rs`.
2. **`src/structs/`** — new `workload_identity.rs`: `WorkloadIdentityClaims`,
   `WorkloadIdentity` (one named identity: `name`, `audience`, `ttl`, `env`, `file`,
   `change_mode`, `change_signal`, `extra_claims`, optional `sub_template`), and a
   `ChangeMode` enum (`noop`/`signal`/`restart`). Add `identities: Vec<WorkloadIdentity>`
   to `TaskConfig`. The `sub` template renderer (dumb `{field}` substitution over the
   stable-field allow-list, with admission validation) lives here too.
3. **Manager config** ([src/structs/config.rs](../../src/structs/config.rs)) — new block:
   issuer URL, OIDC HTTP route placement (TLS listener by default), JWKS path, signing
   alg, token default TTL, key `rotation_age`, default audience, **and the cluster-default
   `sub_template`** (defaults to `{region}:{job}:{group}:{task}`) that per-identity
   `sub_template` overrides.
4. **Migration** — signing-key table in [src/storage/migrations.rs](../../src/storage/migrations.rs).
5. **Related plan** — stable instance keys for the stateful `sub` variant come from the
   [Stateful Workloads & Instance Identity plan](stateful-workloads-and-instance-identity-plan.md)
   (at least its instance primitive + alloc→instance mapping must land first).
6. **Docs** — new `docs/workload-identity.md`; cross-link from README, plus updates to
   [docs/jobs-tasks-and-scheduling.md](../../docs/jobs-tasks-and-scheduling.md) (spec API),
   [docs/architecture-managers-workers.md](../../docs/architecture-managers-workers.md)
   (manager-as-IdP, HTTP listener), and a provider-integration cookbook
   (AWS / GCP / Vault) in the new doc.

---

## Implementation Phases

### Phase 1 — Keys & Issuer (no task wiring yet)
- Signing keypair generation + SQLite metadata + migration.
- `WorkloadIdentityClaims` + signer with the recommended `subject()` default (and the
  config-driven `sub` format override).
- OIDC discovery + JWKS axum routes on the TLS listener.
- **Exit criteria:** an externally-fetched discovery+JWKS verifies a manually-issued
  token with a standard JWT library.

### Phase 2 — Issuance RPC
- `IssueWorkloadIdentityToken` RPC keyed by `{ alloc_id, identity_name }`; manager
  derives claims + audience from the allocation's spec (never from worker input).
- AuthZ: worker may only request tokens for allocations placed on it.

### Phase 3 — Spec API & Validation
- `identities: [...]` on `TaskConfig`; parse + validate per identity: unique `name`,
  non-empty `audience`, at least one delivery mode, sane `ttl`, `extra_claims`
  reserved-key rejection, change-mode/delivery compatibility.
- `sub_template` renderer + admission validation (allow-listed stable fields only;
  reject `{alloc}`/`{node}`, empty render, sub-uniqueness-floor, and `{instance}`/
  `{slot}` on non-stateful tasks).
- Capability flags on `TaskPlugin` (incl. signaling for `change_mode: signal`); gate
  in `validate_job_config`.

### Phase 4 — Worker Delivery, Refresh & Change Mode
- Worker requests a token per identity at task launch; materialize as env and/or file.
- Refresh loop per identity: re-issue before `exp`, rewrite atomically, then apply
  `change_mode` (`noop` / `signal` with `change_signal` / `restart`).

### Phase 5 — Rotation
- Rotation loop with active/retiring/expired lifecycle and JWKS overlap window.

### Phase 6 — Docs & Provider Cookbook
- `docs/workload-identity.md` with end-to-end AWS `AssumeRoleWithWebIdentity` example,
  GCP WIF, and Vault JWT auth; a multi-identity example; and example jobs in
  `configs/jobs/`.

### Phase 7 (future) — Control-Plane Identity
- Issue the reserved `legion` identity (aud = cluster issuer); manager-side
  verification of these tokens for KV/API access. Authorization/ACL mapping is tracked
  separately (see Non-Goals) — this phase delivers only the verified identity.

---

## Testing Plan

### Unit
- Claims → `sub`/`aud`/custom-claim mapping; default `sub` format; TTL →
  `exp`/`iat`/`nbf`.
- `sub_template` rendering: known fields substitute correctly; unknown field rejected;
  `{alloc}`/`{node}` rejected; empty render rejected; below-uniqueness-floor template
  rejected; `{instance}`/`{slot}` rejected on non-stateful tasks; per-identity override
  beats cluster default.
- `extra_claims` merge: non-reserved keys pass through; reserved (`sub`/`aud`/… and
  `legion.io/*`) keys are rejected.
- JWKS serialization round-trips; `kid` in header matches a JWKS entry.
- Sign with active key, verify with public JWK; verify fails after `exp`.
- Spec validation: duplicate identity `name` rejected; empty `audience` rejected;
  missing delivery mode rejected; capability mismatch rejected; bad ttl rejected;
  `env` + `change_mode: noop` rejected; `signal` on a non-signaling kind rejected.
- Rotation state machine: retiring key stays in JWKS until `not_after`, then dropped.

### Integration
- Manager boots → discovery doc and JWKS fetchable over HTTPS; document validates
  against an OIDC schema/library.
- Worker issuance: a token issued per named identity with the correct `aud`; request
  for a foreign alloc_id or an unknown identity name is denied.
- Multi-identity task: two identities on one task get distinct, correctly-scoped
  tokens delivered to their respective env/file targets.
- Change mode: `noop` rewrites the file with no signal/restart; `signal` rewrites and
  delivers `change_signal` to the task; `restart` restarts the alloc.
- File delivery: token written with correct mode; refresh rewrites in place before
  expiry; old file never left world-readable.
- Rotation under load: tokens signed by the retiring key still verify during overlap;
  fail after the window closes.
- HA: in a multi-manager Raft cluster, JWKS served by a follower verifies a token
  signed by the leader (shared/replicated key set).

### End-to-End (documented, gated)
- A real or mocked relying party (e.g. a local OIDC-verifying stub standing in for
  AWS STS) accepts a Legion token and the trust-policy `sub`/`aud` match.

### Regression
- Jobs without `identities` are unchanged.
- Existing join-token flow ([src/manager/join_server.rs](../../src/manager/join_server.rs)) untouched.

---

## Rollout & Backward Compatibility

- Feature gated behind a manager config flag (`workload_identity.enabled`,
  default `false`).
- No impact on existing jobs: the `identities` list is optional; absent = today's behavior.
- The OIDC HTTP listener only binds when the feature is enabled.
- Join tokens keep their current symmetric path; migrating them onto the asymmetric
  signer is a deliberate follow-up, not a side effect of this work.

---

## Open Questions

1. **`sub` template scope** — the format is now a `{field}` template (default
   `{region}:{job}:{group}:{task}`, see the "Customizing `sub`" subsection). Remaining
   calls: should per-identity `sub_template` overrides be allowed by default or gated
   behind a cluster opt-in? And is the uniqueness floor a hard reject or a warning?
2. **Signing algorithm default** — ES256 (compact) vs RS256 (broadest provider
   compatibility). Lead with which?
3. **Issuer URL & TLS** — relying parties require the issuer to be HTTPS with a
   publicly-trusted cert. How does this interact with the self-signed dev CA in
   [src/certs.rs](../../src/certs.rs)? (Likely: dev = self-signed + insecure flag;
   prod = operator-provided cert.)
4. **Revocation** — short TTL is the primary mitigation, but do we want an explicit
   `jti` denylist for emergency revocation before natural expiry?
5. **Env vs file delivery default** — recommend file (refreshable); confirm whether
   env delivery should be allowed at all given it can't refresh without restart.
6. **HA key distribution** — replicate the private signing key via Raft, or have each
   manager hold its own key and publish all public keys in a shared JWKS? (Trade-off:
   blast radius of a leaked key vs. operational simplicity.)

---

## Acceptance Criteria

- Manager publishes a valid OIDC discovery doc + JWKS that a standard JWT library
  can use to verify Legion-issued tokens.
- A task can declare multiple named identities, each receiving a short-lived,
  asymmetrically-signed token with its own audience, delivered as a file and/or env
  var, with claims derived from real scheduling facts.
- Tokens auto-refresh before expiry; the per-identity change mode (`noop`/`signal`/
  `restart`) governs whether the task is signaled or restarted on refresh.
- The design issues audience-agnostically, so a future control-plane identity
  (workload → manager auth) is a config/authz addition, not a re-architecture.
- Signing keys rotate with a JWKS overlap window; no valid token ever fails
  verification due to rotation.
- Unsupported task kinds / invalid specs are rejected with precise errors.
- Feature is off by default and existing jobs + join-token flow are unaffected.
- `docs/workload-identity.md` demonstrates a full provider-integration example.
