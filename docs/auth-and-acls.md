# Auth & ACLs

Legion uses token-based ACLs to control who can do what in a cluster. Callers
present a bearer credential, and the manager resolves it to a set of named
**policies**. Each policy contains scoped allow/deny **rules** over a small
set of resources and actions. Once ACLs are enabled, anything not allowed is
denied, and a matching deny rule always overrides an allow.

**ACLs are off by default.** With ACLs disabled, every caller has full access
and the UI shows a dismissable warning.

## Quick start

```bash
# 1. Enable ACLs in the manager config and restart the manager.
#    configs/manager.ncl:
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
| `exec` | exec into a running allocation | n/a | open a session (`host = true` for host-level runtimes) |
| `action` | run declared actions on an allocation | n/a | run an action (scope with `action_names`) |
| `template` | job templates | list/inspect | publish versions, delete (scope with `templates`) |
| `scale` | task count overrides ([Scaling Jobs](autoscaling.md)) | scale status | `job scale` / reset (scope by job name). Separate from `job:write`, so a principal can resize without redeploy rights. |
| `boot` | boot tickets ([Edge Boot](edge-boot.md)) | list outstanding tickets | mint a ticket (scope by job name). Separate from `job:write` for the same reason as `scale`: a platform's ticket minter can authorize individual starts without the right to redeploy the fleet. |
| `net` | network policy objects ([Network Handlers](network-handlers.md)) | list policies, show or test a job's effective policy (scope by job name) | apply or delete a policy. Scope with `pools`, which is matched against the policy's own `selector.nodePools`, so a pool's admin can set posture for their pool without cluster-wide write. |
| `snapshot` | instance snapshots ([Suspend and Resume](suspend-and-resume.md)) | list, inspect, warm a published one onto nodes, restore an instance from one, and create instances from one (restore and create also need `job:write`) | create (including `--publish`), delete, and name snapshots, suspend with a memory checkpoint, and fork (fork also needs `job:write`) (scope by job name and namespace) |

Actions are `read` and `write`. `write` does not imply `read`, so grant both.
Grant `state:read` sparingly, since it can read every table in the state
store, including ACL metadata. What it cannot read:

- KV values. Those are stored encrypted
  ([KV Encryption at Rest](kv-encryption.md)), so raw SQL sees ciphertext. A
  `kv` rule scoped by `paths` is the only way to read a value. That includes
  handing a value to the datapath: a job or net policy whose ingress chain
  sets `tls.certFrom = { kv = "<path>" }` needs `kv:read` on that path, on top
  of `job:write` or `net:write`
  ([Certificates from KV](network-handlers.md#certificates-from-kv)).
- Private keys and password hashes. `signing_key.private_key_pem` (the
  workload-identity and boot-ticket signing keys), `net_mesh_ca.key_pem` (the
  mesh CA key), and `acl_user.password_hash` always read as `NULL` through
  raw SQL, however the query reaches them. A `WHERE` clause on them matches
  nothing, and `INSERT ... SELECT` copies `NULL`.

`state:write` is still equivalent to full control of the cluster: it can
rewrite any row, including ACL tokens and policies. State backups
(`GetStateBackup`, `operator:write`) are the whole database, keys included.

Grant `snapshot:write` as carefully as read access to a job's secrets. A memory
snapshot captures everything the workload had in memory, including secrets,
tokens, and decrypted KV values, so `snapshot:write` on a job is equivalent to
reading every secret that job has held in memory. That is why snapshots are a
resource of their own rather than part of `job:write`.

Suspend follows the same line. `legion instance suspend` (`SuspendInstance`)
needs `job:write` on the instance's job, like `instance stop`, plus
`snapshot:write` when the checkpoint is `memory`, because a memory checkpoint
holds the workload's memory. An explicit `--checkpoint memory` is checked
before the instance is looked up. Resuming (`instance start`) needs
`job:write` only: it restores the instance's own checkpoint and widens nothing.

Fork (`legion instance fork`, `ForkInstance`) needs `job:write` and
`snapshot:write` on the origin's job, whatever the checkpoint kind: every
fork starts with what the origin had in memory or on disk. Creating instances
from a snapshot (`instance create --from-snapshot`) needs `job:write` and
`snapshot:read`, since it reads the snapshot. Both grants are checked before
anything is looked up. Naming a snapshot (`legion snapshot alias`) needs
`snapshot:write`.

### Policies

A policy is a named JSON array of rules:

```nickel
[
  { resources = ["job"],  actions = ["read", "write"], names = ["web-*"] },
  { resources = ["kv"],   actions = ["read"],          paths = ["team-a/*"] },
  { resources = ["node"], actions = ["read"],          pools = ["gpu-*"] },
  # explicit deny always wins, across all attached policies:
  { resources = ["*"],    actions = ["*"], effect = "deny", paths = ["team-a/secrets/*"] },
]
```

- `resources` / `actions` accept `*`.
- `effect` is `allow` (default) or `deny`.
- Scope selectors narrow a rule with `*`-globs: `names` (job names),
  `namespaces` (Legion has no user-defined namespaces, so every job is in
  `default`), `pools`
  (node pools), `paths` (KV keys), `actionNames` (declared actions),
  `templates` (job-template names), and `bands` (job priority bands). A rule
  with a selector only matches requests carrying that attribute.
- List endpoints filter to what the caller can read: a token scoped to
  `paths = ["team-a/*"]` sees only `team-a/` keys in `legion kv list`.

**Restricting an account to specific job templates.** A rule with a selector
never matches a request that lacks that attribute. So a `job` rule carrying
`templates` grants submission only through those templates, and raw
`legion job run` stays denied:

```nickel
[
  { resources = ["template"], actions = ["read"], templates = ["render"] },
  { resources = ["job"], actions = ["read", "write"], templates = ["render"] },
]
```

The account can run `render`, and see and stop the jobs that template
created, but cannot submit an arbitrary spec. See
[Job Templates](job-templates.md) for the full model, including the
collision guard and which endpoints carry the template attribute.

**Restricting which priority bands an account may claim.** A job's
`priority` band decides what it can displace
([Preemption and Eviction](preemption-and-eviction.md)). If anyone can use the
top band, everyone will, and it stops meaning anything. Use the `bands`
selector on `job` rules to control this:

```nickel
[
  # owners of prod-* jobs may claim anything, including the never-preempted top band
  { resources = ["job"], actions = ["write"], names = ["prod-*"],
    bands = ["critical", "high", "elevated", "normal", "low", "background"] },
  # everyone else tops out at `high`
  { resources = ["job"], actions = ["read", "write"],
    bands = ["high", "elevated", "normal", "low", "background"] },
]
```

Keep in mind:

- A rule that **omits** `bands` covers every band. Bands are open by default
  and only become a privilege where a policy says so.
- An **unbanded** job is a `normal` job. The submit path fills in the default
  band before enforcing, so a `bands` policy still matches ordinary
  submissions instead of silently granting nothing.
- The check also applies to submitting **through a template** that renders a
  band, so templates are not a way around it.

A refused submit names the band:

```console
$ legion job run api.ncl   # priority: 'critical'
error: permission denied: this token's policy does not allow submitting jobs
in the `critical` priority band (see the `bands` selector in
docs/auth-and-acls.md)
```

Bands are not gated on `operator:write`. `operator` covers raft, manager
configs, and observability. Gating bands on it would make everyone who may
submit a critical job a raft administrator, and it could not express "team A
may use `critical` for their jobs".

Manage policies with `legion acl policy apply|list|get|delete`. A policy file
is Nickel (`.ncl`) or JSON (`.json`), read by the same rules as every other
Legion config (see [Configuration Language](configuration-language.md)). It
can be a bare rules array or `{description, rules}`.

### Tokens

Tokens are opaque secrets (`lgn_…`), and the manager stores only a SHA-256
hash. Each token has:

- a public **accessor ID**, which is safe to log and used for revocation
- a type: `management` (full access), `client` (policy-scoped), or `worker`
  (node credential)
- attached policy names
- an optional expiry

Create, inspect, and revoke tokens with `legion acl token create|list|delete`.
Only a management token can create another management token.

To bound token lifetimes, set `acl = { tokenMaxTtl = "720h" }`. Every client
token (created or login-minted) must then expire within the bound. Management
tokens are exempt. Expired ACL and join tokens are rejected at
authentication time and pruned hourly.

Long-lived streams (`StreamEvents` and exec sessions, including the UI's exec
WebSocket) re-check their credential every 30 seconds. Revoking or expiring a
token, or removing the policy that granted the stream, closes the stream
within that interval with the same `Unauthenticated` or `PermissionDenied`
error a new request would get.

For infrastructure-as-code, `legion acl bootstrap --secret <value>` (min 24
chars) pre-seeds the initial management token instead of generating a random
one.

Clients send the secret as a bearer credential. The CLI checks the
`LEGION_TOKEN` env var first, then `~/.legion/token` (written by
`legion login`, override the path with `LEGION_TOKEN_FILE`). The UI stores it
in browser localStorage via the **Access** page.

### Logging in (userpass, JWT, OIDC / SSO)

Every identity source is an **auth method**. Each one verifies a credential
and returns a principal plus policy names. The manager then mints an ACL
token named `<method>:<principal>` (for example `userpass:alice`), so audit
logs always show how an identity was established. Built-in methods:

- **Username/password** (`userpass`, always available): create a user with
  `legion acl user upsert alice --password ... --policy dev-team`
  (the password is argon2id-hashed). Then `legion login -u alice -p ...`
  mints a token with the user's policies (TTL: `acl.loginTtl`, default 24h).
  The UI Access page has the same form.
- **JWT / OIDC tokens**: configure an auth method in the manager config. Then
  either present the externally issued JWT directly as the bearer credential
  (validated on every request) or exchange it once with
  `legion login --method corp-oidc --jwt-file token.jwt`.
- **Single sign-on (browser flow)**: for `oidc` methods with an
  `oidcClientId` configured, `legion login --method corp-oidc` opens the
  browser for the standard authorization-code + PKCE flow. The loopback
  callback listens on `127.0.0.1:4749` (override with `--callback-port`), so
  register `http://127.0.0.1:4749/oidc/callback` as a redirect URI at the
  IdP. The UI Access page offers the same flow as a "Continue with <method>"
  button (redirect URI `<ui origin>/oidc/callback`). Binding rules map
  verified claims (for example `groups`) onto policies.
- **Webhook**: `type = "webhook"` hands verification to your own HTTP
  endpoint. Use it to integrate LDAP, SAML bridges, or custom identity
  systems in any language. Legion POSTs
  `{"method", "username", "password", "jwt"}` and expects
  `{"principal": "...", "policies": ["..."]}` on HTTP 200 (401 = bad
  credentials, 403 = verified but denied). Webhooks are only called at login,
  never on every request.

When you embed Legion as a library, you can also register custom identity
sources through the same `AuthMethod` interface
(`Authenticator::register_method`). See [docs/plugin-system.md](plugin-system.md).

```nickel
acl = {
  enabled = true,
  authMethods = [
    {
      name = "corp-oidc",
      type = "oidc",                      # or "jwt" with jwksUrl/jwksJson/hmacSecret
      issuer = "https://idp.example.com",
      boundAudiences = ["legion"],
      userClaim = "email",
      bindingRules = [
        { claim = "groups", value = "platform", policies = ["ops"] },
        { policies = ["base-read"] },     # unconditional
      ],
      tokenTtl = "8h",
    },
  ],
},
```

### Anonymous policy and audit mode (migration)

There are two ways to enable ACLs on a live cluster without breaking anyone:

1. **Audit-only**: with `acl = { enabled = true, auditOnly = true }`, every
   denial is logged but allowed. Logs use the tracing target `acl_audit` and
   include principal, accessor ID, resource, action, and scope. Watch the
   log, write policies, then drop `auditOnly`.
2. **Anonymous policy**: a policy named `anonymous` applies to requests with
   no credential. Grant it temporary read access while you distribute
   tokens, then delete it.

### Worker nodes: join tokens and node tokens

Adding a worker takes two commands:

```bash
# On the manager (or any host with node:write access):
legion join-token create --type default --ttl 24h
# On the worker host (paste the printed token):
legion join lgnjoin_...
```

The join token bundles the manager address, the worker type, and a random
secret, so the worker needs no other configuration. The manager stores only
a hash of the secret. List and revoke join tokens with
`legion join-token list|delete` or from the UI's Access → Worker Join tab.
Set `server.advertiseAddr` when workers reach the manager at a different
address than it binds.

Workers get their attributes from their worker type's template (pool,
attributes, and resources from `workerConfigPath`) plus worker-side
fingerprinting. The join token pins which template applies.

When ACLs are enforced:

- `WorkerPreRegister`/`WorkerRegister` require a valid join secret, or the
  node's own node token (see below).
- Registration with a join secret mints a per-node **node token** (returned
  in the reply) that the worker attaches to every worker-plane call. Node
  tokens grant nothing on the user-facing API, and they do not expire.
- The worker persists its node token in its state directory
  (`$LEGION_DATA_DIR/workers/<node>/node_token`, mode `0600`, in a directory
  only the worker's user can enter) and is never logged. A restarted worker
  re-registers with that node token instead of its join secret, so **a join
  token is only needed to introduce a node**: once the node has joined, its
  join token can expire or be revoked and the node still restarts, reboots,
  and upgrades normally.
- A join token only admits its own worker type. Pre-registering as another
  type, or registering into a pool other than that type's pool, fails with
  `PermissionDenied`.
- A node token only speaks for its own node. Every worker-plane RPC that names
  a node (`Heartbeat`, `HealthCheck`, `FetchNetPolicy`,
  `GetArtifactCredential`, `RatifyClaim`, and the identity and certificate
  issuers) refuses a node id that does not match the token with
  `PermissionDenied`. A node token can re-register its own node in its current
  pool, but not any other node. Management tokens act for any node.
- Registering with a join secret rotates the node token (the old one stops
  working). Re-registering with the node token keeps it: the reply carries no
  new token, so a crash during a restart can never leave the worker holding
  a revoked one.
- If the manager refuses the persisted node token (the node was removed, or
  ACL state was reset), or a heartbeat comes back `Unauthenticated`, the
  worker discards it and registers with its join secret. If that is gone
  too, the worker exits with `Unauthenticated: unknown or revoked join
  token`; mint a new join token for that host and run `legion join` again.
- A join token admits any node name, including the name of a node that is
  already registered. Treat join secrets like node credentials: give them
  short TTLs (minutes to a day, long enough to bring the hosts up) and
  revoke them once the fleet has joined. Restarts do not need them. Treat
  the node token file like the node's private key: it lets its holder act as
  that node.
- In `auditOnly` mode, workers without join tokens are admitted but logged,
  and so are the worker-type, pool, and node-id mismatches above. The two
  identity-token issuers refuse a foreign node id even in audit-only mode.
  Use this to migrate fleets that predate ACLs: enable audit mode, restart
  workers onto join tokens at your own pace, then enforce.

Browsers are also limited by origin, with or without ACLs: the gRPC-Web API
and the exec WebSocket only answer loopback origins and those listed in
`server.allowedOrigins` (see
[UI Architecture](ui-architecture.md#allowed-browser-origins)). With ACLs off
this is the only thing that stops a web page from driving a local manager.

### The worker plane

Every worker serves the `Worker` gRPC service (the *worker plane*) on 4848,
and managers dial it for exec, actions, logs, restart, and push dispatch.
It is always mutual TLS, on every platform and in every mode, including
`--run-worker`, whether or not the cluster runs ACLs. There is no plaintext
worker listener and no switch to turn one on. Ports, the bind and advertise
settings, and the firewall rule are in
[architecture-managers-workers.md](architecture-managers-workers.md#ports-and-who-dials-whom).

**The certificates.** One cluster CA (the same CA `legion net ca show`
prints, generated on first use and replicated with the control lane) signs
two kinds of certificate. The role lives in the URI SAN:

| Certificate | Held by | SANs | Issued |
| --- | --- | --- | --- |
| node | each worker | `<node>.node.legion`, `legion://node/<node id>` | the worker generates a key and sends a CSR (`IssueNodeCertificate`, authorized by its node token); the manager chooses the names and ignores what the CSR asks for. 24 hours, re-issued after 8. The key never leaves the worker; it is persisted with the certificate under `<state-dir>/identity/`, mode `0600`. |
| manager | each manager | `legion://manager/<manager id>` (raft node id, or `local`) | the manager signs it for itself from the replicated CA key, on first use, and re-mints before it expires. `ClientAuth` only. |

The node certificate is also the service mesh's node certificate
([network-handlers.md](network-handlers.md#mtls)).

**Caller classes.** The worker serves its node certificate and requests, but
does not require, a client certificate that chains to the cluster CA. A
client certificate from another CA fails the handshake. Each RPC then
admits named caller classes, checked before the request is read:

| RPC | Caller class | What it takes |
| --- | --- | --- |
| `PostWork`, `GetTaskLogs`, `RestartWorker`, `ExecTask`, `StopSessions` | `manager` | a client certificate from the cluster CA carrying `legion://manager/<id>` |
| `StartClaim` | `ticket` | a boot ticket in the request ([edge-boot.md](edge-boot.md)); a client certificate is ignored |

A node certificate is not a manager certificate: the CA signs both, and the
role SAN is what the worker checks. Anything else is refused with
`Unauthenticated`:

```console
$ grpcurl -cacert ca.pem 10.0.1.7:4848 legion.Worker/GetTaskLogs
ERROR:
  Code: Unauthenticated
  Message: GetTaskLogs is a manager-only worker RPC: present a Legion manager certificate
```

So nothing but a manager can open a shell in a task, read a task's logs,
hand a node work, or restart it, and a manager does those only after its own
ACL checks (`exec:write`, `job:read`, `node:write`). Only managers hold
manager certificates: their key never leaves the manager process.

**The manager verifies the worker.** Every manager-to-worker call goes
through one client that trusts only the cluster CA and checks that the
server certificate names the node it means to reach (`<node>.node.legion`).
A worker that registered a stale or stolen address cannot receive another
node's work or exec sessions. The manager never dials an `http://` worker
address: a worker older than protocol 3 keeps running work (heartbeat
replies carry it) but exec, logs, and restart against it fail with an error
naming its protocol version.

**Known limits.**

- The worker-to-manager channel (4747) is plaintext unless the manager
  serves `server.tls`, and workers do not yet configure TLS toward a TLS
  manager. Heartbeat replies (which can carry KV-rendered secrets),
  registration, and the edge-boot JWKS fetch travel over it. Node private
  keys do not: they never leave the worker. Keep 4747 on a network you
  trust until encrypted worker connections ship.
- Join tokens do not pin the cluster CA yet. A worker trusts whatever answers
  at the token's address.
- `legion net ca rotate` swaps the CA in one step; there is no overlap
  window yet. A manager notices within 10 seconds and re-mints its
  certificate. A node re-issues its own on the next heartbeat after a
  manager's dial is refused for the old CA (the refused handshake signals
  it, at most once a minute), and otherwise at its routine refresh within 8
  hours. The first exec, logs, or restart against each node after a
  rotation can fail; retry after a few seconds.
- The `ticket` class on `ExecTask` (direct exec without a manager on the
  path) is not implemented. Every exec goes through a manager.

### TLS

The manager listener serves TLS when configured:

```nickel
server = {
  tls = { enabled = true, certPath = "/etc/legion/server.pem", keyPath = "/etc/legion/server.key" },
},
```

Clients use `https://` in `LEGION_ADDR` (CA configurable via
`LEGION_TLS_CA_CERT`). Join tokens minted by a TLS-enabled manager advertise
an `https://` address automatically.

The exec WebSocket listener (`server.port + 1`, used by the UI terminal and
actions) serves TLS with the same certificate and key. The telemetry
listener (`/metrics` and health probes, `server.port + 2` by default) stays
plaintext whatever `server.tls` says.

### Introspection

`legion acl whoami` (CLI), `WhoAmI` (gRPC), the UI Access page, and the
`legion_acl_whoami` MCP tool all report whether ACLs are enabled, how the
caller was authenticated, and which policies apply. Start here when
debugging a `PermissionDenied`.

### Recovery

If you lose the only management token, delete the management rows from the
`acl_token` table in the manager's SQLite store (this requires filesystem
access to the manager host). Then run `legion acl bootstrap` again.
Bootstrap is allowed whenever no management token exists.
