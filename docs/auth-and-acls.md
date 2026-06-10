# Auth & ACLs

Legion ships a token-based ACL system in the spirit of Nomad/Consul/Vault:
callers present a bearer credential, the manager resolves it to a set of
named **policies**, and policies contain scoped allow/deny **rules** over a
small set of resources and actions. Evaluation is default-deny once enabled,
and a matching deny rule always overrides an allow.

**ACLs are off by default.** With ACLs disabled every caller has full access
and the UI shows a dismissable warning. Design rationale and roadmap live in
[docs/plans/auth-acl-plan.md](plans/auth-acl-plan.md).

## Quick start

```bash
# 1. Enable ACLs in the manager config and restart the manager.
#    configs/manager.jsonnet:
#      acl: { enabled: true },

# 2. Mint the one-time initial management token.
legion acl bootstrap
export LEGION_TOKEN=lgn_...   # the printed secret (shown exactly once)

# 3. Create a policy and a token for it.
legion acl policy apply dev-team --rules '[
  {"resources": ["job"],  "actions": ["read", "write"], "names": ["dev-*"]},
  {"resources": ["kv"],   "actions": ["read", "write"], "paths": ["dev/*"]},
  {"resources": ["node"], "actions": ["read"]}
]'
legion acl token create --name dev-ci --policy dev-team --ttl 720h

# 4. Check what any credential can do.
legion acl whoami
```

## Concepts

### Resources and actions

| Resource | Covers | `read` | `write` |
| --- | --- | --- | --- |
| `job` | jobs, specs, plans, rollouts | list/inspect/plan | submit/stop/destroy/advance |
| `node` | worker nodes, pools, ledger | list/inspect | restart, drain/attrs |
| `kv` | KV entries and locks | get/list | put/patch/delete/locks |
| `state` | raw state store, saved queries | SQL reads, schema | SQL writes, saved queries |
| `operator` | raft, manager configs, observability | status/history/logs | raft commands, raft join tokens |
| `acl` | tokens, policies, users | list/get | create/update/delete |

Actions are `read` and `write` (`write` does not imply `read`; grant both).
`state:read` is powerful — it can read every table in the state store —
grant it sparingly.

### Policies

A policy is a named JSON array of rules:

```jsonnet
[
  { resources: ["job"],  actions: ["read", "write"], names: ["web-*"] },
  { resources: ["kv"],   actions: ["read"],          paths: ["team-a/*"] },
  { resources: ["node"], actions: ["read"],          pools: ["gpu-*"] },
  // explicit deny always wins, across all attached policies:
  { resources: ["*"],    actions: ["*"], effect: "deny", paths: ["team-a/secrets/*"] },
]
```

- `resources` / `actions` accept `*`.
- `effect` is `allow` (default) or `deny`.
- Scope selectors narrow a rule with `*`-globs: `names` (job names),
  `namespaces` (jobs live in `default` until namespaces land), `pools`
  (node pools), `paths` (KV keys). A rule with a selector only matches
  requests carrying that attribute.
- List endpoints filter to what the caller can read: a token scoped to
  `paths: ["team-a/*"]` sees only `team-a/` keys in `legion kv list`.

Manage with `legion acl policy apply|list|get|delete`. Files can be JSON or
Jsonnet (either a bare rules array or `{description, rules}`).

### Tokens

Opaque secrets (`lgn_…`); the manager stores only a SHA-256 hash. Each token
has a public **accessor ID** (safe to log; used for revocation), a type
(`management` = full access, `client` = policy-scoped), attached policy
names, and an optional expiry. Create/inspect/revoke with
`legion acl token create|list|delete`. Only a management token can create
another management token.

Clients send the secret as a bearer credential. Precedence for the CLI:
`LEGION_TOKEN` env var, then `~/.legion/token` (written by `legion login`,
override path with `LEGION_TOKEN_FILE`). The UI stores it in browser
localStorage via the **Access** page.

### Logging in (userpass, JWT, OIDC / SSO)

Every identity source converges on an ACL token:

- **Username/password**: `legion acl user upsert alice --password ... --policy dev-team`
  (argon2id-hashed), then `legion login -u alice -p ...` mints a token with
  the user's policies (TTL: `acl.loginTtl`, default 24h). The UI Access page
  has the same form.
- **JWT / OIDC tokens**: configure an auth method in the manager config, then
  either present the externally issued JWT directly as the bearer credential
  (validated on every request) or exchange it once:
  `legion login --method corp-oidc --jwt-file token.jwt`.
- **General SSO**: any OIDC IdP works via the `oidc` method type — Legion
  discovers the JWKS from the issuer and validates ID tokens; binding rules
  map claims (for example `groups`) onto policies. The browser-redirect login
  flow is on the roadmap; today the UI/CLI accept the IdP-issued token
  directly.

```jsonnet
acl: {
  enabled: true,
  authMethods: [
    {
      name: "corp-oidc",
      type: "oidc",                      // or "jwt" with jwksUrl/jwksJson/hmacSecret
      issuer: "https://idp.example.com",
      boundAudiences: ["legion"],
      userClaim: "email",
      bindingRules: [
        { claim: "groups", value: "platform", policies: ["ops"] },
        { policies: ["base-read"] },     // unconditional
      ],
      tokenTtl: "8h",
    },
  ],
},
```

### Anonymous policy and audit mode (migration)

Two ramps for enabling ACLs on a live cluster without breaking anyone:

1. **Audit-only**: `acl: { enabled: true, auditOnly: true }` — every denial
   is logged (tracing target `acl_audit`, with principal, accessor ID,
   resource, action, and scope) but allowed. Watch the log, write policies,
   then drop `auditOnly`.
2. **Anonymous policy**: a policy literally named `anonymous` applies to
   requests with no credential. Grant it temporary read access while tokens
   are distributed, then delete it.

### Worker nodes

Worker-plane endpoints (`WorkerPreRegister`, `WorkerRegister`, `Heartbeat`,
`HealthCheck`) are exempt from token checks in v1, so enabling ACLs never
severs running workers. Per-node credentials and mTLS are tracked in the
plan's Phase 3.

### Introspection

`legion acl whoami` (CLI), `WhoAmI` (gRPC), the UI Access page, and the
`legion_acl_whoami` MCP tool all report whether ACLs are enabled, how the
caller was authenticated, and which policies apply — the first stop when
debugging a `PermissionDenied`.

### Recovery

Lost the only management token? Delete the management rows from the
`acl_token` table in the manager's SQLite store (requires filesystem access
to the manager host) and run `legion acl bootstrap` again — bootstrap is
allowed whenever no management token exists.
