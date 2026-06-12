# Auth & ACLs

Legion ships a token-based ACL system: callers present a bearer credential,
the manager resolves it to a set of named **policies**, and policies contain
scoped allow/deny **rules** over a small set of resources and actions.
Evaluation is default-deny once enabled, and a matching deny rule always
overrides an allow.

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
(`management` = full access, `client` = policy-scoped, `worker` = node
credential), attached policy names, and an optional expiry. Create/inspect/
revoke with `legion acl token create|list|delete`. Only a management token
can create another management token.

Lifetime bounds: set `acl: { tokenMaxTtl: "720h" }` to require every client
token (created or login-minted) to expire within the bound; management
tokens are exempt. Expired ACL and join tokens are rejected at
authentication time and pruned hourly.

For infrastructure-as-code, `legion acl bootstrap --secret <value>` (min 24
chars) pre-seeds the initial management token instead of generating a random
one.

Clients send the secret as a bearer credential. Precedence for the CLI:
`LEGION_TOKEN` env var, then `~/.legion/token` (written by `legion login`,
override path with `LEGION_TOKEN_FILE`). The UI stores it in browser
localStorage via the **Access** page.

### Logging in (userpass, JWT, OIDC / SSO)

Every identity source is an **auth method** — one implementation of the same
internal interface (`AuthMethod` in `src/manager/auth_method.rs`): verify a
credential, return a principal plus policy names. The authenticator then
mints an ACL token named `<method>:<principal>` (for example
`userpass:alice`), so audit logs always show how an identity was established.
Built-in methods:

- **Username/password** (`userpass`, always available):
  `legion acl user upsert alice --password ... --policy dev-team`
  (argon2id-hashed), then `legion login -u alice -p ...` mints a token with
  the user's policies (TTL: `acl.loginTtl`, default 24h). The UI Access page
  has the same form.
- **JWT / OIDC tokens**: configure an auth method in the manager config, then
  either present the externally issued JWT directly as the bearer credential
  (validated on every request) or exchange it once:
  `legion login --method corp-oidc --jwt-file token.jwt`.
- **Single sign-on (browser flow)**: for `oidc` methods with an
  `oidcClientId` configured, `legion login --method corp-oidc` opens the
  browser for the standard authorization-code + PKCE flow (loopback callback
  on `127.0.0.1:4749`, override with `--callback-port`; register
  `http://127.0.0.1:4749/oidc/callback` as a redirect URI at the IdP). The UI
  Access page offers the same as a "Continue with <method>" button (redirect
  URI `<ui origin>/oidc/callback`). Binding rules map verified claims (for
  example `groups`) onto policies.
- **Webhook**: `type: "webhook"` delegates verification to your own HTTP
  endpoint — the recommended way to integrate LDAP, SAML bridges, or bespoke
  identity systems in any language. Legion POSTs
  `{"method", "username", "password", "jwt"}` and expects
  `{"principal": "...", "policies": ["..."]}` on HTTP 200 (401 = bad
  credentials, 403 = verified but denied). Login-only; webhooks are never on
  the per-request hot path.

Custom identity sources can also be registered programmatically through the
same `AuthMethod` interface when embedding Legion as a library
(`Authenticator::register_method`) — see [docs/plugin-system.md](plugin-system.md).

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

### Worker nodes: join tokens and node tokens

Introducing a worker is two commands:

```bash
# On the manager (or any host with node:write access):
legion join-token create --type default --ttl 24h
# On the worker host (paste the printed token):
legion join lgnjoin_...
```

The join token is a self-describing composite — manager address, worker type,
and a random secret — so the worker needs no other configuration. The manager
stores only a hash of the secret; list and revoke with
`legion join-token list|delete` or from the UI's Access → Worker Join tab.
Set `server.advertiseAddr` when workers reach the manager at a different
address than it binds.

Workers declare their attributes through their worker type's template
(pool, attributes, resources from `workerConfigPath`) plus worker-side
fingerprinting; the join token pins which template applies.

When ACLs are enforced:

- `WorkerPreRegister`/`WorkerRegister` require a valid join secret.
- Registration mints a per-node **worker token** (returned in the reply) that
  the worker attaches to `Heartbeat`/`HealthCheck`. Node tokens grant nothing
  on the user-facing API.
- Re-registering rotates the node token; on an `Unauthenticated` heartbeat a
  worker automatically re-registers using its join secret.
- In `auditOnly` mode, workers without join tokens are admitted but logged —
  the migration ramp for fleets that predate ACLs: enable audit mode, restart
  workers onto join tokens at your own pace, then enforce.

mTLS for the worker plane remains on the roadmap (the listener-level TLS
below already covers transport encryption).

### TLS

The manager listener serves TLS when configured:

```jsonnet
server: {
  tls: { enabled: true, certPath: "/etc/legion/server.pem", keyPath: "/etc/legion/server.key" },
},
```

Clients use `https://` in `LEGION_ADDR` (CA configurable via
`LEGION_TLS_CA_CERT`). Join tokens minted by a TLS-enabled manager advertise
an `https://` address automatically.

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
