# Auth & ACL System Plan

Status: **Phase 1 implemented** (this PR). Phases 2+ are roadmap.

Legion needs a best-in-class auth and ACL system: simple enough to enable in
two commands, flexible enough to scope teams to their own jobs, node pools,
and KV subtrees, and cheap enough to drop into every endpoint as one line of
code. This document records the research that drove the design, the design
itself, what is implemented today, and what comes next.

User-facing docs: [docs/auth-and-acls.md](../auth-and-acls.md).
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
- **Identity sources**, all converging on the same identity:
  - ACL token in `authorization: Bearer …` (or `x-legion-token`).
  - External **JWT/OIDC token** presented directly as the bearer credential:
    validated against a configured auth method (JWKS by URL, inline JWKS,
    OIDC discovery via issuer, or HMAC secret for dev), with bound
    issuer/audiences; **binding rules** map verified claims to policies.
  - **Login exchange** (`legion login`): `userpass` (argon2id-hashed users)
    or a JWT method — mints a TTL'd ACL token carrying the mapped policies.
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
| Open | `WhoAmI`, `Login`, `BootstrapAcl` | work without credentials (bootstrap is one-shot) |
| Worker | `WorkerPreRegister`, `WorkerRegister`, `Heartbeat`, `HealthCheck` | exempt in v1 so enabling ACLs never severs running workers (hardening: Phase 3) |

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

## 3. What is implemented (Phase 1, this PR)

- **Policy engine** `src/structs/acl.rs`: rules, globs, scopes, default-deny
  evaluation, deny-wins, list-gating semantics; exhaustively unit-tested.
- **Storage** `src/storage/acl_store.rs` + migrations: `acl_policy`,
  `acl_token` (hashed secrets, unique accessor), `acl_user` (argon2id PHC).
  One-shot transactional bootstrap.
- **Runtime** `src/manager/acl.rs`: bearer extraction, token resolution,
  JWT validation (RS256/384/512, PS*, ES256/384 via JWKS URL / inline JWKS /
  OIDC discovery; HS256 via shared secret for dev), binding rules → policies,
  userpass + JWT login minting TTL'd tokens, JWKS background refresh,
  audit-only mode, structured denial audit.
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
- **Tests**: `tests/acl_endpoints.rs` — a table classifying **every** Manager
  RPC, a proto-parsing guard that fails when a new RPC lacks coverage, a
  deny/allow matrix over all endpoints (anonymous denied / management passes
  / disabled passes), and end-to-end scenarios (job-name scoping, KV path
  scoping + deny carve-outs, node-pool filtering, anonymous policy, audit
  mode, login flow, escalation guard). Plus unit tests across the three new
  modules.

## 4. Roadmap

### Phase 2 — Interactive SSO & richer admin
- `legion login --method <oidc>` browser flow: authorization-code + PKCE with
  a loopback callback (Nomad/Vault pattern), and a device-code fallback for
  headless hosts. (Today OIDC works by presenting/exchanging an IdP-issued
  JWT; the UI can run its own PKCE flow against the IdP and paste/exchange
  the ID token.)
- UI: ACL admin pages (policies/tokens/users CRUD) and an optional login
  redirect when anonymous access is denied.
- Roles (named policy bundles) if policy lists on tokens get unwieldy;
  Vault-style policy templating (`${identity.name}`) to avoid per-user
  policies.
- Token lifecycle: server-enforced min/max TTL bounds, expired-token pruning,
  `legion acl token self`-style introspection of other tokens.

### Phase 3 — Worker-plane hardening
- Validate join tokens on `WorkerPreRegister`/`WorkerRegister`; mint a
  per-node credential at registration that workers attach to heartbeats;
  rotate via the existing workload-identity signing infrastructure.
- Re-enable TLS/mTLS on the manager listener (certs.rs groundwork exists);
  consider requiring `operator:write` for `GetJoinToken` on the insecure
  listener when ACLs are enabled.
- Per-allocation workload identity tokens as first-class API credentials
  (bound to the `job` scope of their own job).

### Phase 4 — Operations at scale
- Token→policy resolution cache with explicit TTL + push invalidation on
  token/policy deletion (only needed if per-request SQLite lookups show up in
  profiles; they are indexed point reads today).
- Replicate ACL state through the Raft persistence layer alongside the rest
  of the store (it already lives in the same SQLite file, so this falls out
  of the multi-raft persistence plan).
- Audit sink: ship `acl_audit` events through the observability pipeline;
  optional bootstrap pre-seeding (`--bootstrap-token`) for IaC.
- Namespaces: when jobs grow real namespaces, `AclScope::job` starts carrying
  them; the `namespaces` rule selector already works (everything is
  `default` today). Consider label-selector scoping (the most-requested
  Nomad gap).

### Non-goals
- External policy engines (OPA/SpiceDB/Cedar) in the authorization fast
  path. If richer admission rules are needed later ("prod jobs must set
  resource limits"), add a separate admission hook above ACLs.
- Signed (JWT) ACL tokens. Opaque + hashed + revocable is the right shape
  for long-lived credentials; JWTs remain inputs (workload identity, SSO),
  not outputs.
