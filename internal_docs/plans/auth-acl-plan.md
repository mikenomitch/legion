# Auth & ACL System Plan

> **Judgement calls:** [`calls/auth-acl-plan_calls.md`](../calls/auth-acl-plan_calls.md)

Status: **Phases 1–4 executed** (see §3 and the per-phase notes in §4 for the
few deliberately deferred items: dynamic auth-method dylib plugins, worker
mTLS, workload-identity API credentials, and the token-resolution cache).

Legion needs a best-in-class auth and ACL system: simple enough to enable in
two commands, flexible enough to scope teams to their own jobs, node pools,
and KV subtrees, and cheap enough to drop into every endpoint as one line of
code. This document records the research that drove the design, the design
itself, what is implemented today, and what comes next.

User-facing docs: [docs/auth-and-acls.md](../../docs/auth-and-acls.md).
Contributor rules: AGENTS.md ("Auth & ACL Checks").

---

## 1. Research: what makes auth/ACL systems good or bad

We studied the systems closest to Legion's shape (Nomad, Consul, Vault,
Kubernetes RBAC) and the modern authorization engines (Zanzibar/SpiceDB/
OpenFGA, Cedar, OPA/Rego, Casbin).

### HashiCorp Nomad (the closest analogue)

Token-based and default-deny: opaque tokens (`management` vs `client`, with a
public **Accessor ID** and secret **Secret ID**) reference named **policies**
(HCL capability lists scoped per namespace/node-pool with glob matching);
**auth methods** (OIDC for humans, JWT for machines) plus **binding rules**
convert external identities into TTL'd tokens; `nomad acl bootstrap` mints the
initial management token once, with a reset-index escape hatch.

- Praised for: a small mental model (token → policies → capabilities),
  intent-revealing capability names (`submit-job`), deny-overrides-allow,
  one-command bootstrap, clean SSO via auth methods.
- Criticized for: instant default-deny at bootstrap (the migration "anonymous
  policy" staging is manual and error-prone), name-glob-only scoping, tokens
  that never expire by default, and CVE-2025-4922 — job→policy lookup used
  *prefix* matching, so `test-job-2` inherited `test-job`'s policies. Lesson:
  **never resolve policies by prefix; exact attachment, explicit globs only in
  rules.**

### Kubernetes RBAC

Roles/ClusterRoles (verbs × resources) bound to subjects. Uniform and
pluggable (OIDC, webhook TokenReview), but: **no deny rules** (purely
additive), verbose (4 object kinds to grant read), instance-level rules barely
exist (`resourceNames` can't cover `create`), well-known escalation footguns
(`escalate`, `bind`, `impersonate`, secrets `list`), and audits repeatedly
find RBAC the most misconfigured part of clusters. Lesson: **keep the object
count low, support deny, and model escalation explicitly.**

### Consul & Vault

Vault: path-based policies with tiny capability vocabulary, strict default
deny, deny wins, and the key insight Legion adopts wholesale: **every auth
method (userpass, LDAP, OIDC, JWT, k8s SA) terminates in the same thing — a
token with policies attached**. Consul adds ergonomic templated policies and
service/node identities, but its `default_policy = "allow"` default is a
documented footgun. Lesson: **N identity sources → 1 token model; secure
defaults once enabled.**

### Modern authz engines (Zanzibar, OpenFGA, SpiceDB, Cedar, OPA, Casbin)

ReBAC engines solve user-facing sharing/hierarchy graphs at the cost of
running a stateful service and syncing data into it; OPA/Rego brings a
learning curve and data-distribution burden. A scheduler's authorization
domain is small, closed, and known at compile time — exactly the case where
**in-process capability lists beat an external policy engine** on latency,
availability, and operator burden. All four comparable systems (Nomad,
Consul, Vault, K8s) made the same call. Lesson: **no policy-engine
dependency; leave an admission-hook seam for later.**

### Distilled requirements

1. Default deny once enabled — with a migration ramp (anonymous policy +
   audit-only mode).
2. Small mental model: token → policies → scoped allow/deny rules.
3. Deny overrides allow, everywhere.
4. One-command bootstrap; recovery path documented.
5. Tokens: opaque, **hashed at rest**, accessor/secret split, TTLs on login
   tokens, revocation by deletion.
6. Auth methods convert any identity (userpass, JWT, OIDC/SSO) into the same
   policy-carrying identity.
7. Exact-match policy attachment; globs only inside rules (CVE lesson).
8. Local, in-process evaluation — no network hop per request.
9. Escalation modeled: only management mints management.
10. Structured, grep-able audit of denials; never log secrets.

---

## 2. Design

### Concepts

```
credential ──authenticate──▶ AclIdentity ──enforce──▶ allow / deny
 (token | JWT | user+pass        │
  | nothing)                     ├─ principal (for audit)
                                 ├─ policies [names]
                                 └─ rules    [flattened allow/deny rules]
```

- **Resources** (closed set of 6): `job` (jobs, specs, plans, rollouts),
  `node` (nodes, pools, ledger), `kv` (entries + locks), `state` (raw state
  store, saved queries), `operator` (raft, manager configs, observability),
  `acl` (tokens/policies/users administration).
- **Actions** (2): `read`, `write`. `write` does not imply `read`.
- **Policies**: named JSON documents, a list of rules:

  ```jsonnet
  [
    { resources: ["job"],  actions: ["read", "write"], names: ["web-*"] },
    { resources: ["kv"],   actions: ["read"],          paths: ["team-a/*"] },
    { resources: ["node"], actions: ["read"],          pools: ["gpu-*"] },
    { resources: ["*"],    actions: ["*"], effect: "deny", paths: ["secret/*"] },
  ]
  ```

  Rule fields: `resources`, `actions`, optional `effect: allow|deny`
  (default allow), optional scope selectors `namespaces` / `names` / `pools` /
  `paths` (glob lists, `*` wildcard only). A rule with a selector matches only
  requests carrying that attribute. **A matching deny always wins.** Default
  deny when no rule allows.
- **Scopes**: each endpoint declares the instance attributes of the request —
  job name (+ implicit namespace `default` until real namespaces land), node
  pool, or KV path. List endpoints gate on a wildcard scope and then filter
  items per-instance, so a scoped token sees only its slice of the cluster.
- **Tokens**: opaque `lgn_<64 hex>` secrets; only the SHA-256 hash is stored.
  Public accessor IDs are the audit/revocation handle. Types: `management`
  (bypasses policy evaluation) and `client` (policy-scoped). Optional expiry;
  login-minted tokens always expire.
- **Identity sources**, all converging on the same identity. Everything that
  is not an opaque ACL token goes through one trait, `AuthMethod`
  (`src/manager/auth_method.rs`): verify a credential, return a principal
  plus policy names. The trait is deliberately shaped like Legion's other
  plugin entry traits (`TaskPlugin`, `SchedulingPolicy`,
  `RolloutStrategyPlugin`) — object-safe, `Send + Sync`, built-ins
  dispatched by name — so identity sources stay a closed, auditable seam
  today and a plugin category later:
  - ACL token in `authorization: Bearer …` (or `x-legion-token`), resolved by
    the `Authenticator` itself.
  - `UserpassMethod` (built-in, always registered): argon2id-hashed users;
    login-only.
  - `JwtAuthMethod` (one instance per configured `acl.authMethods` entry):
    external **JWT/OIDC tokens**, validated via JWKS by URL, inline JWKS,
    OIDC discovery via issuer, or HMAC secret for dev, with bound
    issuer/audiences; **binding rules** map verified claims to policies.
    Works per request (bearer) and at login.
  - **Login exchange** (`legion login`): any method — mints a TTL'd ACL
    token named `<method>:<principal>` carrying the mapped policies.
    Minting, secret hashing, and policy evaluation never live inside a
    method; methods can only yield policy names, never management access.
  - Custom methods: `Authenticator::register_method(Box<dyn AuthMethod>)`
    when embedding Legion as a library.
  - **Anonymous**: no credential ⇒ the `anonymous` policy if it exists
    (migration ramp), else no access.
- **Modes**: `acl.enabled: false` (default — everything allowed, UI shows a
  dismissable warning), `enabled + auditOnly` (denials logged under the
  `acl_audit` tracing target but allowed), `enabled` (enforced).

### Enforcement (the "drop-in" requirement)

Every Manager RPC starts with exactly one line:

```rust
self.check_acl(&request, AclResource::Kv, AclAction::Write,
    AclScope::path(&request.get_ref().key))?;
```

or, when the scope is only known after parsing/fetching:

```rust
let ident = self.authenticate_acl(&request)?;
// ...parse or fetch...
acl::enforce(&ident, AclResource::Job, AclAction::Write, &AclScope::job(&name))?;
```

List endpoints reuse the returned identity to filter:

```rust
jobs.retain(|j| ident.allows(AclResource::Job, AclAction::Read, &AclScope::job(&j.name)));
```

Evaluation is fully in-process (one indexed SQLite lookup per request to
resolve the token, then pure rule matching). Three planes exist:

| Plane | Endpoints | Behavior |
| --- | --- | --- |
| Protected | everything user-facing | default deny once enabled |
| Open | `WhoAmI`, `Login`, `BootstrapAcl`, `GetAuthMethods` | work without credentials (bootstrap is one-shot) |
| Worker | `WorkerPreRegister`, `WorkerRegister`, `Heartbeat`, `HealthCheck` | registration requires a join token; heartbeats/health require the node token minted at registration (audit-only mode admits and logs instead) |

### Worker join (Phase 3 redesign)

The original join tokens were HS512 JWTs signed with a compiled-in constant
key, minted by an unauthenticated listener on `:4748` — forgeable by anyone
with the binary, so they authenticated nothing. Both were removed and
replaced:

- **Join tokens** are now manager-minted stored secrets (`CreateJoinToken`,
  requires `node:write`; hashed at rest; listable/revocable; default 24h
  TTL). The printed token is a self-describing composite (advertised manager
  address + worker type + secret) so `legion join <TOKEN>` stays
  one-command. Worker attributes keep coming from the worker-type template
  plus worker-side fingerprinting; the token pins the template.
- **Node tokens** (`token_type: worker`) are minted per node at registration
  and returned in `WorkerRegisterReply`; workers attach them to heartbeats
  and health checks. Re-registering rotates the credential, and a worker
  that gets `Unauthenticated` on a heartbeat re-registers automatically with
  its join secret. Node tokens grant nothing on the user-facing API.
- The in-process worker (`--run-worker`) self-issues a short-lived join
  token, so single-binary dev clusters keep working with ACLs on.

### Error semantics

- Missing/insufficient permissions → `PermissionDenied` with the required
  capability and a hint (`set LEGION_TOKEN or run legion login`).
- Unknown/expired/invalid credential → `Unauthenticated`.
- Denials emit a structured `acl_audit` event (principal, accessor ID,
  resource, action, scope, enforced flag). Secrets are never logged.

### Escalation control

- `acl:write` lets a token manage policies and mint **client** tokens, but
  minting a **management** token requires presenting a management token.
- Policies attach to tokens by **exact name**; globs never apply to
  attachment (CVE-2025-4922 lesson).
- `state:read` is documented as sensitive: raw SQL can read all tables
  (hashes included, never plaintext secrets).

---

## 3. What is implemented (Phase 1)

- **Policy engine** `src/structs/acl.rs`: rules, globs, scopes, default-deny
  evaluation, deny-wins, list-gating semantics; exhaustively unit-tested.
- **Storage** `src/storage/acl_store.rs` + migrations: `acl_policy`,
  `acl_token` (hashed secrets, unique accessor), `acl_user` (argon2id PHC).
  One-shot transactional bootstrap.
- **Authenticator** `src/manager/acl.rs`: bearer extraction, token
  resolution, login dispatch, token minting, audit-only mode, structured
  denial audit.
- **Auth methods** `src/manager/auth_method.rs`: the `AuthMethod` trait plus
  built-ins — `UserpassMethod` and `JwtAuthMethod` (RS256/384/512, PS*,
  ES256/384 via JWKS URL / inline JWKS / OIDC discovery; HS256 via shared
  secret for dev; binding rules → policies; per-method JWKS cache with
  background refresh). Custom methods registrable via
  `Authenticator::register_method`.
- **API**: 13 new Manager RPCs (`WhoAmI`, `Login`, `BootstrapAcl`, policy
  CRUD, token create/list/delete, user CRUD), fully documented in
  `proto/legion.proto`. All 44 pre-existing RPCs carry checks or explicit
  plane classifications.
- **CLI**: `legion acl bootstrap|whoami|policy|token|user`, `legion login`
  (userpass + JWT exchange; writes `~/.legion/token`, 0600), `legion logout`;
  every outgoing CLI/MCP request attaches `LEGION_TOKEN` env or the token
  file via a client interceptor. Capabilities catalog + `legion_acl_whoami`
  MCP tool added.
- **UI**: dismissable warning banner when ACLs are disabled (and an info
  banner in audit mode); `/access` page (identity introspection, userpass
  login, token paste, logout); bearer token attached via a ConnectRPC
  interceptor from localStorage.
- **Config**: `acl { enabled, auditOnly, loginTtl, jwksRefreshInterval,
  authMethods[] }` following the `Config`/`ConfigFull` pattern; example in
  `configs/manager.jsonnet`.
- **Tests**: `tests/integration/acl_endpoints.rs` — a table classifying **every** Manager
  RPC, a proto-parsing guard that fails when a new RPC lacks coverage, a
  deny/allow matrix over all endpoints (anonymous denied / management passes
  / disabled passes), and end-to-end scenarios (job-name scoping, KV path
  scoping + deny carve-outs, node-pool filtering, anonymous policy, audit
  mode, login flow, escalation guard). Plus unit tests across the three new
  modules.

## 4. Phase status

### Phase 2 — Interactive SSO & richer admin: **executed**
- **Webhook auth method** (`type: "webhook"`, `webhookUrl`,
  `webhookBearer`): POSTs the login credentials to an operator-supplied
  endpoint and maps `{principal, policies}` back. Login-only by design (a
  per-request network call would put the webhook on every RPC's hot path).
  The `AuthMethod::login` trait method became async (`BoxFuture`) to support
  it.
- **OIDC browser login**: `legion login --method <oidc>` runs
  authorization-code + PKCE against the IdP's discovered endpoints with a
  loopback callback (`127.0.0.1:4749`, `--callback-port` to change), then
  exchanges the ID token via `Login`. The UI runs the same flow from the
  Access page ("Continue with <method>", callback route `/oidc/callback`).
  Public method metadata comes from the new open `GetAuthMethods` RPC
  (`oidcClientId`/`oidcScopes` config). Device-code fallback for headless
  hosts remains future work (use `--jwt-file` there today).
- **UI admin**: the Access page grew tabs — Identity & Login, Policies,
  Tokens, Users, Worker Join — full CRUD with one-time secret reveals, plus
  a dismissable "you are browsing anonymously — log in" banner when ACLs are
  enabled and no credential is present.
- **Token lifecycle**: `acl.tokenMaxTtl` bounds client-token lifetimes
  (CreateAclToken rejects, Login clamps; management exempt); expired ACL and
  join tokens are pruned hourly.
- *Deferred*: roles (policy bundles) and policy templating — policy lists on
  tokens have not gotten unwieldy yet; dynamic `plugins/auth/` dylib loading
  stays held back in favor of the webhook method (rationale in
  docs/plugin-system.md; the trait is transport-agnostic so the door stays
  open).

### Phase 3 — Worker-plane hardening: **executed** (see §2 "Worker join")
- Join-token validation on registration, per-node tokens with rotation on
  re-register, automatic worker re-registration on auth failure, audit-mode
  ramp, and removal of the forgeable JWT tokens plus the unauthenticated
  `:4748` bootstrap listener (join tokens now require `node:write`).
- Manager listener TLS is opt-in via `server.tls {enabled, certPath,
  keyPath}` (never inferred from files on disk, so a stray dev cert cannot
  flip the listener to https).
- *Deferred*: worker-plane mTLS client certificates, and per-allocation
  workload-identity tokens as API credentials — the latter belongs to the
  workload-identity plan (its issuer is not yet wired to issue tokens to
  allocations; once it is, a `JwtAuthMethod` against Legion's own JWKS plus
  job-scoped binding rules is the intended shape).

### Phase 4 — Operations at scale: **executed where warranted**
- Bootstrap pre-seeding for IaC: `BootstrapAcl.secret` /
  `legion acl bootstrap --secret` (min 24 chars).
- Expired-credential pruning loop (hourly) on the manager.
- Audit sink: `acl_audit` denial events are tracing events, so they already
  flow through the observability pipeline when `--o11y` is enabled.
- ACL/join state lives in the same SQLite store as everything else, so Raft
  replication falls out of the multi-raft persistence plan; nothing
  ACL-specific to do.
- *Deferred on evidence*: the token→policy resolution cache — per-request
  lookups are indexed point reads; add the cache (with push invalidation on
  token/policy deletion) only if profiles show it. Namespaces/label-selector
  scoping still waits on jobs growing namespaces/labels; the `namespaces`
  rule selector already works against the implicit `default`.

### Non-goals
- External policy engines (OPA/SpiceDB/Cedar) in the authorization fast
  path. If richer admission rules are needed later ("prod jobs must set
  resource limits"), add a separate admission hook above ACLs.
- Signed (JWT) ACL tokens. Opaque + hashed + revocable is the right shape
  for long-lived credentials; JWTs remain inputs (workload identity, SSO),
  not outputs.
