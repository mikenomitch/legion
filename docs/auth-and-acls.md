# Auth & ACLs

Legion uses token-based ACLs to control who can do what in a cluster. Callers
present a bearer credential, and the manager resolves it to a set of named
**policies**. Each policy contains scoped allow/deny **rules** over a small
set of resources and actions. Once ACLs are enabled, anything not allowed is
denied, and a matching deny rule always overrides an allow.

**ACLs are on by default.** Every API call needs a credential. A fresh
manager mints its own first credential, so nothing is open while you set it
up, and there is no bootstrap endpoint for anything else to race you to.

## Quick start

```bash
# 1. Start a manager. At first start it mints the initial management token
#    and writes the secret, mode 0600, to <dataDir>/bootstrap-token and (when
#    that file does not exist yet) ~/.legion/token:
legion start manager --run-worker
#   INFO ACLs are on. Created the initial management token (accessor 7c1f...).
#        Secret written to .legion/bootstrap-token and /home/me/.legion/token (mode 0600).
#        The CLI on this machine uses it automatically. Treat it like a root password.

# 2. The CLI on the same machine already works: it reads ~/.legion/token.
legion job run configs/jobs/demo.ncl

# 3. Create a policy and a narrower token for everyone and everything else.
legion acl policy apply dev-team --rules '[
  {"resources": ["job"],  "actions": ["read", "write"], "names": ["dev-*"]},
  {"resources": ["kv"],   "actions": ["read", "write"], "paths": ["dev/*"]},
  {"resources": ["node"], "actions": ["read"]}
]'
legion acl token create --name dev-ci --policy dev-team --ttl 720h

# 4. Check what any credential can do.
legion acl whoami
```

On another machine, export the secret (`LEGION_TOKEN=lgn_...`) or run
`legion login`. The UI's login page takes the same secret.

### Where the initial token goes

| File | Written | Mode |
| --- | --- | --- |
| `<dataDir>/bootstrap-token` (`$LEGION_DATA_DIR`, else `./.legion`) | every time the manager mints the initial token | `0600` |
| `~/.legion/token` for the user running the manager (`$LEGION_TOKEN_FILE` overrides the path) | only when the file does not exist yet, so an existing login is never overwritten | `0600` |

The manager logs the accessor ID and the file paths once, and never logs the
secret. When `~/.legion/token` already exists it is left alone and the
manager logs a warning saying how to use the new token instead
(`export LEGION_TOKEN="$(cat <dataDir>/bootstrap-token)"`). This matters on
a laptop where an older cluster's token is still in that file.

The mint is a replicated control-lane write. In a raft cluster only the
manager that leads the control lane mints the token, at first start or when
it first wins leadership, and only that manager's data dir holds the
`bootstrap-token` file. The token itself is replicated and works against
every manager.

A manager writes the secret to `<dataDir>/bootstrap-token.pending` before the
replicated write and renames it into place after. If the manager dies between
the two, the next start finishes the rename, so a cluster can't end up with a
management token whose secret nobody holds.

### Pre-seeding the secret (infrastructure as code)

Point `acl.bootstrapSecretFile` at a file holding the secret (at least 24
characters). The manager reads it once, when it mints the initial token:

```nickel
acl = { bootstrapSecretFile = "/etc/legion/bootstrap-secret" },
```

A secret shorter than 24 characters fails the start, naming the setting.

### Turning ACLs off

`acl = { enabled = false }` is an explicit choice for a throwaway cluster.
Every caller then has full access, including any workload that can reach the
manager: a task on the manager's own host reaches it on loopback, and in a
multi-host cluster every task can reach the address workers use. The manager
logs a warning at every start, the UI shows a banner, and `legion acl whoami`
reports `aclEnabled: false`.

## Concepts

### Resources and actions

| Resource | Covers | `read` | `write` |
| --- | --- | --- | --- |
| `job` | jobs, specs, plans, rollouts | list/inspect/plan | submit/stop/destroy/advance |
| `node` | worker nodes, pools, ledger | list/inspect | restart, drain/attrs |
| `kv` | KV entries and locks | get/list | put/patch/delete/locks |
| `state` | raw state store, saved queries | SQL reads, schema | SQL writes, saved queries |
| `operator` | raft, manager configs, observability | status/history/logs | raft commands, raft join tokens |
| `acl` | tokens, policies, users, delegated exec keys | list/get | create/update/delete |
| `exec` | exec into a running allocation | n/a | open a session, or mint an exec ticket for one (`host = true` for host-level runtimes) |
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

#### Host-level jobs

Some tasks run on the host as root, or close to it. Submitting or updating a
job with one of them needs a `job:write` rule with `host = true`, the same
flag `exec` rules use for exec into host-level runtimes:

| Task | Host-level when |
| --- | --- |
| `exec` | always: it is a process on the host, root unless it sets `user` |
| `container` | `network.mode = "host"`, `security.userNamespace = false`, or `security.seccomp = "unconfined"` |
| `sandbox` | `hypervisor = "dev"` (a container standing in for a VM, not a security boundary) |
| a runtime plugin other than `noop` | always: Legion cannot vouch for its isolation |

```nickel
# Deploy containers and sandboxes, but nothing that runs on the host:
{ resources = ["job"], actions = ["write"], names = ["team-a-*"] },
# Platform admins may deploy host-level jobs too:
{ resources = ["job"], actions = ["write"], names = ["*"], host = true },
```

A refusal names the task and why, and only when the host grant is what is
missing:

```console
$ legion job run backup.ncl
error: permission denied: job "backup" has a host-level task ("dump" uses runtime "exec").
Submitting it needs a job:write rule with host = true (docs/auth-and-acls.md, "Host-level jobs")
```

Management tokens hold every grant, so the first-start token and single-box
flows are unaffected. The check runs on every path that submits a spec
(`RunJob` and `RunJobTemplate`), after the spec parses. Scaling, instances,
and stopping a job that already exists don't change what runs, so they need
only `job:write`. A stored `require` policy that imposes `mode = "host"`
makes every job it matches host-level; writing one needs `net:write`, which
is already an admin grant.

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
error a new request would get. A [direct exec](exec-and-actions.md#direct-exec)
session never touches a manager after its ticket is minted, so its worker
reports it on every heartbeat instead: the manager re-checks the minting
token there, and the session ends within one heartbeat interval of the token
being deleted, expiring, or losing the grant.

For infrastructure-as-code, `acl.bootstrapSecretFile` pre-seeds the initial
management token's secret (see [Pre-seeding the
secret](#pre-seeding-the-secret-infrastructure-as-code)).

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

ACLs are on for a new cluster. For a cluster that ran with
`acl.enabled = false`, there are two ways to turn them on without breaking
anyone (the first start with ACLs on mints the initial management token as
described above):

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

Managers reach workers on the worker plane (port 4848) for dispatch, exec,
logs, and restart. It is authenticated independently of ACL mode, because a
manager's private key never leaves the managers:

- The listener is always mutual TLS with the node's certificate from the
  cluster CA.
- `PostWork`, `GetTaskLogs`, `RestartWorker`, and `StopSessions` admit only a
  caller that presents a manager certificate (`legion://manager/<id>`). `ExecTask` admits
  a manager, or a holder of an exec ticket on a worker that opts in to
  [direct exec](exec-and-actions.md#direct-exec). `StartClaim` admits a holder
  of a valid boot ticket.
- Managers verify the worker they dial against `<node>.node.legion`.
- Join tokens pin the cluster CA (`caHash`), and a joining worker refuses a
  manager whose CA does not match.
- Node keys are generated on the node. `IssueNodeCertificate` takes a CSR
  only and never returns a private key.

See [Managers and Workers](architecture-managers-workers.md#the-worker-plane)
for the caller-class table and the ports, and [Rotating the cluster
CA](#rotating-the-cluster-ca) below.

The worker's own connection to the manager (registration, heartbeats, the
JWKS) is always TLS, verified against the same CA: see
[Workers reach managers over TLS](#workers-reach-managers-over-tls) below.

### Rotating the cluster CA

One CA per cluster signs node, manager, and mesh certificates
(`legion net ca show` prints the trust bundle, its pin, and the rotation
phase). Rotate it in two steps that break no connection:

```console
$ legion net ca rotate
Rotated the cluster CA. The new CA (pin sha256:...) signs every certificate from now on.
The previous CA stays trusted until 2026-11-03T18:05:11Z, so no worker-plane or mesh connection breaks;
nodes re-issue their certificates within one heartbeat. Join tokens minted before
the rotation work until then; mint new ones after.
```

1. The new CA is generated and cross-signed by the old one, and signs every
   certificate from then on. Chains carry the cross-signed certificate, so a
   holder that still trusts only the old CA keeps verifying.
2. The old CA stays in every trust bundle for one certificate lifetime (24
   hours). Every node sees the new CA's pin on its next heartbeat reply and
   re-issues at once.
3. After the overlap, the old CA leaves the bundle. Join tokens that pin it
   stop working; mint new ones. A worker that was offline for the whole
   overlap must re-join.

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

#### Workers reach managers over TLS

Workers always speak TLS to the manager's API port, whether or not the
operator turned `server.tls` on. Registration, heartbeats (whose replies
carry rendered KV secrets and identity tokens), artifact credentials, and
the JWKS that boot and exec tickets are verified against all ride it.

- The worker sends the name `manager.legion`, and the manager answers that
  name with its cluster server certificate (`manager.legion`,
  `legion://manager/<id>`), signed by the cluster CA. Any other name gets
  the operator's `server.tls` certificate when one is configured.
- The worker verifies that certificate against its join token's CA pin from
  the first byte of the first connection, before it sends its join secret,
  and against the cluster CA bundle once it holds one. A manager whose CA
  does not match is refused with the same message as a pin mismatch at
  join.
- Without `server.tls`, the manager tells TLS from plaintext by the
  connection's first byte (a TLS handshake starts with `0x16`, HTTP never
  does), so the CLI and the UI keep using `http://` on the same port while
  every worker uses TLS. With `server.tls` on, a plaintext connection is
  dropped.
- A join token keeps the address the manager advertises, `http://` or
  `https://`. The worker dials TLS to that host and port either way.

Raft traffic between managers is not covered: it stays plaintext on the
managers' network.

### Introspection

`legion acl whoami` (CLI), `WhoAmI` (gRPC), the UI Access page, and the
`legion_acl_whoami` MCP tool all report whether ACLs are enabled, how the
caller was authenticated, and which policies apply. Start here when
debugging a `PermissionDenied`.

### Recovery

If you lose every management token's secret, delete the management rows from
the `acl_token` table in the manager's SQLite store (on the control-lane
leader in a raft cluster; this requires filesystem access to the manager
host), then restart the manager. With no management token in the store, it
mints a fresh one at start and writes it to `<dataDir>/bootstrap-token`
exactly as at first start. Use `acl.bootstrapSecretFile` to choose the
secret.

There is no `legion acl bootstrap` command and no `BootstrapAcl` RPC. An
endpoint that works "whenever no management token exists" can be won by any
workload that reaches the manager after a first start or a recovery, so the
initial token is only ever minted by the manager process itself.
