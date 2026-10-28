# Workload Identity (OIDC)

Workload identity lets your tasks get cloud and service credentials without
putting long-lived secrets in the job spec. Legion acts as an **OIDC identity
provider (IdP)**. It gives each task instance a short-lived, asymmetrically
signed JWT that describes the task (region, job, group, task, node,
allocation). External relying parties such as AWS, GCP, Vault, and internal
services trust Legion's issuer. Your task exchanges the token for that
provider's own credentials.

This page covers the token, the job-spec API, manager configuration, and how
to integrate with common providers.

> **Implementation status.** Task identities work end to end: token issuance,
> the discovery and JWKS endpoints, key rotation, delivery to tasks, and
> refresh. [Node identities](#node-identities) are issued via
> `IssueNodeIdentityToken` and used by the artifact system. The
> [control-plane identity](#control-plane-identity) (`legion`) is verified
> in-process; what it may do is declared on the job
> ([Permissions](#permissions)) and granted at submit, and inside the task
> it reaches the cluster through a per-allocation
> [API socket](#the-api-socket) with no setup. Not yet implemented:
> task-scoped `auth.identity` for artifacts (it needs the fetching
> allocation's id), and the API socket for sandboxes (they use the token
> file). The feature is off by default.

## The token

A workload identity token is an ES256-signed JWT. Standard claims:

| Claim | Meaning |
| --- | --- |
| `iss` | Issuer URL (matches the discovery document). |
| `sub` | Stable composed subject (see below). |
| `aud` | Audience(s) the task requested. Enforced by the manager and never empty. |
| `exp` / `iat` / `nbf` | Short TTL window. |
| `jti` | Unique token id for audit tracing. |
| `kid` (header) | Signing-key id, so verifiers pick the right JWKS entry. |

The token also carries separate Legion claims, so providers that can match
any claim don't have to parse `sub`: `legion.io/region`, `legion.io/namespace`,
`legion.io/job` (the bare name),
`legion.io/group`, `legion.io/task`, `legion.io/node`, `legion.io/alloc` (and
`legion.io/instance` / `legion.io/slot` for stateful workloads). A stateful
workload's token also carries `legion.io/epoch`, the instance's identity
epoch, which Legion uses to retire tokens copied into a fork or snapshot (see
[Forks and snapshots](#forks-and-snapshots)).

### The `sub` claim is a security boundary

The default subject is:

```
sub = <region>:<namespace>:<job>:<group>:<task>      e.g.  auto:default:api:web:server
```

`<job>` is the job's bare name and `<namespace>` its namespace, so two
namespaces' `api` never share a subject ([Namespaces](namespaces.md)). This
matches Nomad's composed subject. The subject stays stable across reschedules
and gives each task its own least-privilege identity. `alloc_id` and `node_id` are not
in `sub`, because they change often and no stable policy could match them.
They are included as `legion.io/*` claims instead.

`sub` matters most for **AWS IAM**, the least flexible major provider. There,
`sub` is effectively the only fine-grained key you can match on, because IAM
doesn't expose custom claims as condition keys. GCP and Vault can match any
`legion.io/*` claim directly.

Operators can override the format with a simple `{field}` template. It does
plain substitution with no logic. Set it at the cluster level
(`workloadIdentity.subTemplate`), and allow other shapes for jobs to choose
per identity with `workloadIdentity.allowedSubTemplates`. A job's
per-identity `subTemplate` must be the cluster template or one of the allowed
ones; anything else is refused at submit, and no token is minted under it:

```console
error: task replica: identity "aws" sets subTemplate "{region}:{group}:{job}:{task}", which is neither the cluster default nor in workloadIdentity.allowedSubTemplates; an operator must allow a subject shape before a job can use it
```

Two templates can render the same subject for two different workloads. With
`job:write` on a job called `web` alone, `{region}:{namespace}:{group}:{job}:{task}`
for group `api` and task `server` renders `auto:default:api:web:server`, which
is the default subject of job `api`'s group `web`. AWS trust policies match nothing
but `sub`, so which subject shapes exist is the operator's decision. Every
template, the allowed ones included, must pass these rules (an invalid entry
stops the manager from starting):

- **Allow-list only.** Allowed: `{region}`, `{namespace}`, `{job}`, `{group}`,
  `{task}`, and `{instance}`/`{slot}` (stateful tasks only). Unknown
  placeholders are rejected.
- **`{alloc}`/`{node}` are forbidden in `sub`.** They change often and would
  break identity that is stable across reschedules. They stay claims-only.
- **Minimum uniqueness.** A template must keep at least `{namespace}`,
  `{job}`, `{group}`, and `{task}`, so distinct workloads stay distinct.
  Templates without them are rejected; dropping `{namespace}` would let two
  namespaces' `web` mint the same subject for different tenants.
- **Non-empty render.** A literal-only or empty-rendering template is rejected.

## Job-spec API

Declare one or more named identities per task. Each one is independent, so one
task can federate into several providers at once.

```nickel
{
  name = "api",
  runtime = "exec",
  identities = [
    {
      name = "aws",                       # unique per task
      audience = ["sts.amazonaws.com"],   # non-empty, manager-enforced
      ttl = "15m",
      file = { path = "/var/run/legion/identity/aws/token", mode = "0400" },
      changeMode = "noop",                # SDK re-reads the file → no reload
    },
    {
      name = "vault",
      audience = ["vault.internal"],
      ttl = "5m",
      env = "VAULT_WORKLOAD_TOKEN",
      changeMode = "signal",              # env can't change live → signal
      changeSignal = "SIGHUP",
      extraClaims = { "app.example.com/tier" = "backend" },
    },
  ],
}
```

A full runnable example is at
[configs/jobs/workload-identity.ncl](../configs/jobs/workload-identity.ncl).

### Validation rules

- `name` is required and unique within the task.
- `audience` must be non-empty, because a token without an audience can be
  replayed against any relying party.
- At least one of `env` or `file` must be set, unless a
  `connect.upstreams` credential names the identity (`from = { identity =
  "<name>" }`). Then the node injects the token into requests to that
  upstream and the identity may have no delivery at all, and no
  `changeMode`. See [Injecting a token into an upstream](#injecting-a-token-into-an-upstream).
  The reserved `legion` identity needs neither: its token always goes to
  `$LEGION_SECRETS_DIR/legion.token` ([Ambient env](#ambient-env)).
- `permissions` and `policies` are accepted on the reserved `legion`
  identity only ([Permissions](#permissions)).
- `extraClaims` are static literals. Keys can't be a reserved JWT claim
  (`iss`/`sub`/`aud`/…) or use the `legion.io/*` namespace.
- **Change mode and delivery must be compatible.** `env` with
  `changeMode = "noop"` is rejected, because env can't refresh in place. The
  exception is a memory-checkpointable sandbox, which keeps the token fresh
  in `/legion/identity/<ENV>` (see [Sandboxes](#sandboxes)). `signal`
  requires `changeSignal`.
- The task runtime must support the requested delivery (see capability gating
  below).

### Change mode (refresh behavior)

When a token is re-issued before expiry, `changeMode` controls what happens:

- `noop` (default): atomically rewrite the file. Most cloud SDKs re-read the
  token file on each call, so no reload is needed. We recommend file delivery
  with `noop`.
- `signal`: rewrite, then send `changeSignal` (for example, `SIGHUP`). This
  is the only way to refresh an env-delivered token without a restart.
- `restart`: rewrite, then restart the task.

### Task-runtime capability gating

| Kind | env delivery | file delivery | signal |
| --- | --- | --- | --- |
| `exec` | ✅ | ✅ | ✅ |
| `container` | ✅ | ✅ (paths under `/legion/task`) | ✅ |
| `sandbox` | ✅ | ✅ (paths under `/legion/identity`) | ❌ |
| `noop` | ❌ | ❌ | ❌ |

Specs that ask for an unsupported delivery are rejected at admission with an
error that says what to change.

**`container` file paths must resolve under `/legion/task`.** The allocation's
task directory is bind-mounted there, and nothing else on the host is
reachable from inside the container. So Legion rejects a destination outside
the mount at submit time, instead of writing the token where the workload
can't read it. Relative paths resolve against the mount:

```nickel
identities = [{
  name = "api",
  audience = ["sts.amazonaws.com"],
  file = { path = "/legion/task/identity/token", mode = "0400" },
  changeMode = "signal",
  changeSignal = "SIGHUP",
}],
```

```
task api identity "api": file.path "/var/run/legion/token" is outside the
container task directory; use a path under /legion/task (or a relative path),
or switch to env delivery
```

This is the one place where a jobspec can't move unchanged between the `exec`
and `container` runtimes. The exec runtime uses the host's filesystem, so its
host-absolute paths stay valid.

Legion mounts the directory, not individual files, so the worker's atomic
rewrite-and-rename on rotation is visible inside the running container. With
`changeMode = "signal"`, the container runtime sends the signal to the
container's init process. A rotation then reaches the workload with no
restart. See [docs/task-runtime-container.md](task-runtime-container.md).

## Manager configuration

```nickel
# manager.ncl
{
  workloadIdentity = {
    enabled = true,                              # default false
    issuer = "https://manager.example.com:4747", # public HTTPS issuer URL
    jwksPath = "/.well-known/jwks.json",            # must start with '/'
    signingAlg = "ES256",                           # only ES256 is supported
    defaultTtl = "15m",
    rotationAge = "24h",
    subTemplate = "{region}:{job}:{group}:{task}",
    # The other subject shapes a job's identity may name in its own
    # `subTemplate`. Empty (the default): only `subTemplate` above.
    allowedSubTemplates = ["{region}:{job}:{group}:{task}:{instance}"],
  },
}
```

`signingAlg` is validated at startup, and only `ES256` is accepted today.
Setting anything else stops the manager from booting, instead of silently
signing with ES256. Malformed `defaultTtl`/`rotationAge`/`jwksPath` values
also fail boot instead of falling back to a default. To federate with a
provider, you only need to set `enabled` and `issuer`. The rest have
defaults.

The feature is off by default. Enabling it doesn't affect existing jobs or the
worker join-token flow.

### Public endpoints (discovery + JWKS)

When enabled, the manager serves two unauthenticated, read-only HTTP/1.1
routes on its main listener (`server.port`):

- `GET /.well-known/openid-configuration`: the OIDC discovery document.
- `GET <jwksPath>` (default `/.well-known/jwks.json`): every non-expired
  public signing key.

These are plain JSON over HTTP because relying parties (AWS STS, GCP, Vault)
fetch them with generic HTTP clients that can't speak gRPC. All other paths on
the listener keep serving gRPC/grpc-web as before.

Legion publishes the `issuer` config value as `iss` in tokens and as
`issuer`/`jwks_uri` in the discovery document. It is not derived from the
bind address, so the published URLs stay correct behind proxies and load
balancers. Some guidelines:

- **AWS / GCP** require the issuer to be reachable over publicly trusted
  HTTPS. The manager currently listens without TLS. Put a TLS-terminating
  proxy (or load balancer) with a valid public certificate in front of the two
  routes, and set `issuer` to that public HTTPS URL.
- **Vault / internal services** can point at the manager directly
  (`oidc_discovery_url` plus a CA bundle, or plain HTTP inside a trusted
  network).
- If `issuer` is unset, Legion picks a default (see
  [The issuer](#the-issuer)). That's fine inside the cluster but not for
  federation.

### The issuer

A cluster has exactly one issuer. Managers share the signing keys, so a token
or ticket any manager mints must carry the same `iss`. Nodes check it on boot,
session, and exec tickets, and managers check it (and the `aud` of
[control-plane tokens](#control-plane-identity)) on tokens their peers
minted.

- **Explicit:** set `workloadIdentity.issuer` to the **same** URL on every
  manager. Legion can't check that the managers agree. A manager with a
  different value mints tokens the rest of the cluster refuses.
- **Default, `raft` or `streaming`:** `https://manager.legion`, the name every
  manager already answers to under the cluster CA. It is the same on every
  manager by construction, whatever each one's address. It is a name, not a
  URL anyone outside the cluster can fetch, so federation still needs an
  explicit issuer.
- **Default, a single `localDisk` manager:** its advertised address,
  `server.advertiseAddr` when set, else `http://<host>:<port>` (`https` with
  `server.tls`), with a wildcard bind (`0.0.0.0`) read as `127.0.0.1`.

Nodes don't derive the issuer from their join token. The manager reports it
when a node pre-registers and on every heartbeat
(`HeartbeatReply.workload_identity_issuer`), so changing it reaches every node
within one heartbeat. Nodes fetch the JWKS from the managers they know, in
failover order, over TLS pinned to the cluster CA, not from the issuer URL.
Changing the issuer invalidates every outstanding token and ticket, because
their `iss` no longer matches.

### Signing-key persistence and restart semantics

Signing keys (including the private key PEM) are stored in the manager's
SQLite store (`signing_key` table). Treat the store like CA material: anyone
who can read it can mint identities, so protect the store file. Storing the
keys is what makes restarts safe:

- On first boot with the feature enabled, the manager generates an ES256
  keypair and stores it.
- On every later boot it restores all non-expired keys. The JWKS keeps
  publishing the same `kid`s, and tokens already held by third parties keep
  verifying. A restart never invalidates outstanding tokens.
- Rotation transitions (`active` → `retiring` → `expired`) are written to the
  store. Expired rows are kept for audit but never served.

### Key rotation

When the feature is enabled, a background loop on the manager checks roughly
every `rotationAge / 10` (minimum 1 minute):

- When the active key is older than `rotationAge` (default 24h), the loop
  rotates it. The old key becomes `retiring`, gets a `not_after` overlap
  window, and stays in the JWKS. A new active key is generated and stored.
  New tokens are signed with the new `kid` right away.
- Retiring keys past `not_after` are marked `expired` and dropped from the
  JWKS.

The overlap window is `max(rotationAge, defaultTtl)`. Any token signed by the
retiring key stays verifiable until it expires. To guarantee this, the manager
caps every token's lifetime at the overlap window when it issues the token.
Admission doesn't check a per-identity `ttl` against `rotationAge`. A longer
`ttl` is accepted, and the issued token's `exp` is silently shortened to
`max(rotationAge, defaultTtl)`. Keep token TTLs in minutes.

A retiring key's `not_after` is computed from the `rotationAge`/`defaultTtl`
in effect at rotation time. If you sharply shrink those values and restart,
tokens already issued under the older, larger TTL can expire after their key
is dropped. Shrink token lifetimes gradually instead of in one large step
across a restart, or accept that in-flight long-TTL tokens may need to be
re-issued.

**HA note:** with raft/streaming persistence, only the primary manager
rotates keys, like other loops that write to the store. Standby managers
don't rotate. On each rotation-check tick, they reload the key set from the
replicated `signing_key` table, so a follower's JWKS and signer pick up keys
the primary rotated in. A follower lags by at most the check interval
(`max(rotationAge / 10, 1m)`). With the default local-disk persistence there
is a single manager, and it is always primary.

Only the control-lane leader generates the first signing key. A manager that
starts without a key in its replica, such as a follower joining before the
leader's key has replicated or any member of a fully restarted cluster
before an election, starts **keyless** rather than failing. It logs `No
workload-identity signing key has replicated to this manager yet`, and it
rechecks the store every 5 seconds until it holds a key. Meanwhile:

- `IssueWorkloadIdentityToken` and `IssueNodeIdentityToken` return
  `UNAVAILABLE` with a "retry shortly" message. Token refreshes retry with
  backoff. A task start that hits it fails like any other issuance error.
- The JWKS route answers `503 Service Unavailable` with `Retry-After: 5`
  instead of an empty key set. The discovery document is still served.
- A keyless manager that wins leadership with no key in the store generates
  one within 5 seconds.

## How tokens reach the task

1. The manager's heartbeat/dispatch payload carries each task's identity
   specs (names, audiences, delivery, change mode). It never carries token
   bytes.
2. Before starting the task, the worker calls `IssueWorkloadIdentityToken` on
   the manager for each identity (`{ alloc_id, identity_name, node_id }`).
   The manager derives all claims from the allocation's scheduling record and
   the named identity in the stored spec. Nothing security-relevant comes from
   the request. The manager rejects the request unless the allocation is
   placed on the calling node. The `node_id` is bound to the caller's
   authenticated per-node worker token (principal `node:<node_id>`, minted at
   registration), so a worker can't forge a peer's node id to mint that peer's
   tokens.
3. Delivery:
   - **file**: written atomically to `file.path` with `file.mode` (default
     `0400`). The worker writes a temp file and renames it, so readers never
     see a partial token. For `exec` tasks the path is used as given, either
     absolute or relative to the worker's working directory (the same as
     `exec` injected files).
   - **env**: injected into the task's env map before start. This happens on
     the worker, so tokens never pass through the scheduler. A live process's
     env can't change, so every env-delivered identity is also mirrored to a
     backing file at `<data dir>/identity/<alloc-id>/<name>/token`, where the
     data dir is `$LEGION_DATA_DIR` or `.legion` (mode
     `0400`). A companion `<ENV>_FILE` variable points at it.
4. A refresh loop on the worker, one per identity, re-issues the token at
   roughly half its remaining TTL. It adds jitter, and retries with backoff if
   the manager is unreachable. It rewrites the file(s) atomically, then applies
   the identity's `changeMode`. Loops stop when the allocation stops.
5. If the manager has the feature disabled, issuance returns
   `FailedPrecondition`. The worker logs one warning, and the task starts
   without tokens. Job specs still validate, so you can declare identities
   before the cluster enables the feature.

### Injecting a token into an upstream

Some origins take a token in a header and nothing else: an internal API
that verifies Legion JWTs, a cloud endpoint fronted by an STS. Instead of
handing the token to the task and having it set the header, name the
identity in an upstream credential and the node sets it:

```nickel
identities = [{ name = "api", audience = ["https://api.internal.example"], ttl = "15m" }],
connect = {
  upstreams = [{
    name = "api",
    to = "https://api.internal.example",
    credentials = [
      { header = "Authorization", prefix = "Bearer ", from = { identity = "api" } },
    ],
  }],
},
```

The task calls `http://$LEGION_UPSTREAM_API/...` with no token. The node's
forwarder fetches the token with `FetchDatapathMaterial` (the manager
mints it from the scheduling record, as above), holds it in memory, and asks
for a new one once less than half the TTL is left. The worker does not run a
refresh loop or write a file for an identity with no `env` or `file`, so the
token is never in the task, its checkpoints, or its snapshots. The reserved
`legion` identity cannot be injected. Everything else about injection
(fail-closed `503`, status, the `https://` requirement) is in
[Credential injection](network-handlers.md#credential-injection).

### Sandboxes

A sandbox guest can't read the worker's files, and it can't take a signal,
so neither the host token file nor `changeMode = "signal"` reaches it. Before
file delivery, the only way to refresh a sandbox's token was `changeMode =
"restart"`, which reboots the guest at about half the TTL and loses
everything it had in memory. That defeats a sandbox kept for its running
state.

So a sandbox gets its tokens through its identity disk, the small raw disk
the guest's resume handler (`legion guest-resume`) already polls for
restores. On every refresh the worker rewrites that disk in place with the
new tokens, and the handler rewrites the token files in the guest. Nothing
restarts. Declare file delivery under `/legion/identity` (a relative path
resolves there):

```nickel
identities = [{
  name = "vault",
  audience = ["vault.example.com"],
  ttl = "15m",
  file = { path = "/legion/identity/vault", mode = "0400" },
  # changeMode defaults to "noop": the guest's file is rewritten in place.
}],
```

On a memory-checkpointable sandbox (one with a `suspend` block that
checkpoints memory, or `snapshots` with `memory` on, and no gated
networking), env delivery works the same way. The env holds the token the
guest booted with, and `/legion/identity/<ENV>` always holds the current one.
There, env with the default `changeMode = "noop"` is accepted:

```nickel
identities = [{
  name = "platform",
  audience = ["api.acme.dev"],
  env = "PLATFORM_TOKEN",   # boot value in env, current value in /legion/identity/PLATFORM_TOKEN
}],
```

A long-lived process re-reads the file rather than its env, as it already
must after a restore.

A memory-checkpointable sandbox that asks for `changeMode = "restart"` is
accepted with a warning on `job run` and `job validate`, since every refresh
would reboot the guest:

```text
warning: task workspace: identity "platform" sets changeMode = "restart", which reboots this
sandbox's guest on every token refresh (about every half TTL) and loses its running state. ...
```

How it works:

- The worker writes a file-delivered token to `<task dir>/identity/<path>` on
  the host, owner-only, and copies it onto the identity disk with its guest
  path and mode. Env tokens ride the same disk by key.
- On a refresh it rewrites the disk *in place*, so the running VMM sees the
  new bytes. The disk keeps the same allocation id and host time, which is
  how the handler tells a token refresh from a restore: it rewrites
  `/legion/identity/`, `/legion/resume.env`, and each file token, and skips
  the reseed, clock step, and `onResume`.
- At boot the handler writes the disk's tokens right away, so the files exist
  from the start.
- A sandbox that is not launched for checkpoints still gets an identity disk
  when it declares a file-delivered identity.
- In dev mode (Apple `container` on macOS) the host's `<task dir>/identity`
  is bind-mounted read-only at `/legion/identity`, so file tokens refresh the
  same way. Env tokens there keep their boot value.

Limits:

- The handler polls the disk every 200 ms, so the guest's file trails the
  worker's refresh by a fraction of a second. A token is refreshed at about
  half its TTL, so that never leaves the guest an expired token.
- A sandbox whose gating comes from a stored network policy launches without
  checkpoint support, so its env tokens are not mirrored. Use file delivery.
- The handler is the worker's own binary, so a guest whose libc cannot run it
  never sees refreshed tokens (init says so on the console). Use `restart`
  there, and accept the reboot.

### Suspended and restored instances

A [suspended](suspend-and-resume.md) sandbox resumes, and a snapshot restores,
as a **new allocation with a new id**: allocation ids are never reused. The
worker issues that allocation's tokens before the restore exactly as it does
before any start, so their `legion.io/alloc` claim names the new allocation.
It writes them to the guest's identity disk, and the guest's resume handler
publishes them under `/legion/identity/<ENV>` and in `/legion/resume.env`
before `suspend.onResume` runs (see [Identity after a
resume](suspend-and-resume.md#identity-after-a-resume)). Processes already
running in the guest keep the env they started with, so re-read the file or
restart them from `onResume`.

A suspend checkpoint holds whatever token the guest had in memory at
capture, and that token stays valid until it expires: up to one TTL (`ttl`,
15 minutes by default) after the capture. A suspend does not retire it,
because only one copy of the instance ever runs, and the resumed guest is the
same instance. Forks and snapshots are different, since they can start a
second instance with the first one's tokens (next section). Keep TTLs short on
tasks you suspend or snapshot, and treat `snapshot:write` on a job as access
to its in-memory secrets.

### Forks and snapshots

A [fork](suspend-and-resume.md#fork) starts every fork from the origin's
memory, so each fork holds the token the origin had at capture, next to the
token issued for the fork itself. A memory snapshot does the same for every
instance created from it (`instance create --from-snapshot`). Without more,
a fork could present the origin's token, with the origin's
`legion.io/instance` and `legion.io/alloc`, for up to one TTL.

Legion closes that window for the verifiers it runs itself:

- Each stateful instance has an **identity epoch**, stamped into its tokens
  as `legion.io/epoch` (a string, like `legion.io/slot`). It starts at `0`.
- When a fork or a manual snapshot of the instance is captured, the manager
  raises the epoch in the same transaction that marks the checkpoint
  `local` (and, for a fork, releases the forks for placement). From then on
  it refuses that instance's tokens from earlier epochs. Every token in the
  checkpoint is one of those.
- The new epoch reaches the instance's node in the heartbeat that reported
  the capture (`TaskAllocation.identity_epoch`), and the worker re-issues
  the allocation's tokens at once instead of at its next half-TTL refresh.
  The running instance holds a current token again within about a heartbeat
  (2 seconds). A sandbox guest's file follows within the resume handler's
  200 ms poll. A refresh triggered this way applies the identity's
  `changeMode` like any other, so `restart` restarts the origin.
- A fork's own tokens name the fork's instance, at the fork's epoch, so they
  are unaffected.

A refused token fails with a message that says why:

```text
control-plane token refused: token was issued to instance user-42 at identity epoch 0,
before a fork or snapshot of it was captured (now epoch 1), so it may be a copy a fork
holds. The instance itself holds a newer token
```

What this covers, and what it does not:

- **Covered:** the [control-plane identity](#control-plane-identity), both as
  an `Authorization: Bearer` credential and for `legion login --method
  legion`. A token naming an instance that no longer exists is refused too.
- **Not covered: relying parties that verify offline.** AWS STS, GCP, Vault,
  and any service that checks the JWT against the published JWKS see a
  validly signed, unexpired token. They cannot know the instance's current
  epoch, so the origin's pre-capture token still works there until it
  expires. Keep `ttl` short on tasks you fork or snapshot, and bind
  per-instance access to `legion.io/instance` only when one TTL of overlap
  is acceptable.
- **ACL tokens minted from an identity token last as long as it does.** An
  ACL token the origin got with `legion login --method legion` before the
  capture is copied into every fork. It expires when the identity token it
  was minted from would have (never later than `acl.loginTtl`), not when the
  epoch moves, so a fork can use it for at most one identity `ttl`. Present
  the identity token as a bearer credential instead of logging in, or delete
  the ACL token after a fork, to close that window too.
- **The window before the refresh.** Between the capture and the origin's
  re-issue (about a heartbeat), the origin's own current token is already
  refused. A process that cached its token in memory gets one failed call,
  then must re-read the token file, as it already must after a restore.

### What each change mode does on refresh

- `noop`: the file is atomically rewritten, and nothing else happens. Cloud
  SDKs re-read web-identity token files on each call, so this is the
  recommended default for file delivery.
- `signal`: the file(s) are rewritten, then `changeSignal` (for example,
  `SIGHUP`) is sent to the task's process. For env delivery, the live env
  still holds the old token, so the handler must re-read the backing file
  (see `<ENV>_FILE` above). An env-only identity with `signal` can't refresh
  unless the app reads that file. That's why the backing file exists.
- `restart`: the stored task config's env is updated with the fresh token,
  then the allocation is restarted in place. The relaunched process sees the
  new value directly.

Tokens are stored on the worker host in these places:

- The delivered token files (mode `0400`).
- The env-delivery backing files under `.legion/identity/`.
- For env-delivered identities, the worker's persisted allocation state. It
  records the task env so tasks can be restored after a worker restart.

All are written owner-only. Token files and their directories are created
with a restrictive mode from the start, so they are never briefly
world-readable. The worker state file and its directory are locked to the
worker's own user. Anyone who can read the worker's state can read them,
which is the same trust boundary as the task itself. Token bytes are never
logged. When an allocation stops, the worker removes its delivered token files
and per-allocation backing directory.

**Known limitations.**

- *`exec` tasks that drop to a different `user`.* The worker's user writes
  token files owner-only. An `exec` task configured with a `user` runs with
  fewer privileges and gets access only to its allocation directory tree. So
  the task user can't read a file-delivered token written outside that tree
  (an absolute `file.path`, or the `.legion/identity/` backing dir). Until
  per-user token ownership is supported, point `file.path` inside the
  allocation directory, or run the task as the worker's user.
- *Apple `container` runtime (macOS).* The Apple container runtime passes env
  vars to `container run` on the command line. That makes an env-delivered
  identity token visible to other local users via `ps` while the launch
  process runs. On that runtime, prefer file delivery or the Linux/OCI runtime
  for secret env. (The Linux runtime writes env into the OCI bundle under the
  owner-only allocation directory, so it isn't exposed this way.)

## Testing this locally in a basic way

1. Enable workload identity in `configs/manager.ncl` (the block is there,
   commented):

   ```nickel
   workloadIdentity = {
     enabled = true,
     # optional; a single local manager defaults to its own address
     # issuer = "https://manager.example.com:4747",
   },
   ```

2. Start a manager (with an in-process worker for a one-machine demo):

   ```sh
   cargo run -- start manager -c configs/manager.ncl --run-worker
   ```

   The log line `Workload identity enabled; serving OIDC discovery and JWKS`
   confirms the issuer is up and the `signing_key` table holds the key.

3. Curl the public endpoints:

   ```sh
   curl -s http://127.0.0.1:4747/.well-known/openid-configuration | jq
   curl -s http://127.0.0.1:4747/.well-known/jwks.json | jq
   ```

4. Run the example job and find the delivered token:

   ```sh
   cargo run -- job run configs/jobs/workload-identity.ncl
   sudo cat /var/run/legion/identity/aws/token   # the 'aws' file identity
   ```

5. Decode the token. Any JWT tool works. This example uses plain shell:

   ```sh
   TOKEN=$(sudo cat /var/run/legion/identity/aws/token)
   echo "$TOKEN" | cut -d. -f2 | tr '_-' '/+' | base64 -d 2>/dev/null | jq
   # → iss, sub ("auto:workload-identity-demo:default:api"), aud, exp,
   #   legion.io/job, legion.io/alloc, ...
   ```

   (`jwt decode "$TOKEN"` from mike-engel/jwt-cli gives the same view.)

6. Watch a refresh happen. The `aws` identity uses `ttl = "15m"`, so the
   worker rewrites the file after about 7 to 8 minutes. For a faster demo, set
   `ttl = "1m"` in the jobspec and watch:

   ```sh
   watch -n5 'sudo stat -c "%y" /var/run/legion/identity/aws/token;      sudo cat /var/run/legion/identity/aws/token | cut -d. -f2 | tr "_-" "/+" | base64 -d 2>/dev/null | jq .exp'
   ```

   Each refresh bumps the mtime and `exp`, with no task restart
   (`changeMode = "noop"`). The `vault` env identity refreshes its backing file
   (`.legion/identity/<alloc>/vault/token`) and SIGHUPs the task instead.

## Node identities

Task identities prove which workload is fetching or calling out. Some
policies need to grant access to the cluster's worker nodes as a whole
instead of to a specific task, for example an ops bucket of tooling every
workload needs. **Node identities** are issued to worker nodes instead of tasks. You
declare them once at the cluster level:

```nickel
# manager.ncl
{
  workloadIdentity = {
    enabled = true,
    issuer = "https://manager.example.com:4747",
    # ... existing fields ...

    nodeIdentities = [
      {
        name = "artifact-pull",            # unique cluster-wide
        audience = ["sts.amazonaws.com"],  # non-empty, like task identities
        ttl = "15m",                       # optional; cluster default TTL otherwise
        subTemplate = "{region}:node:{pool}",  # optional; this is the default
      },
    ],
  },
}
```

Node tokens use the same issuer, signer, JWKS, and rotation as task tokens.
Claims:

| Claim | Value |
| --- | --- |
| `sub` | `prod:node:default` (default template `{region}:node:{pool}`) |
| `legion.io/scope` | `"node"`, which distinguishes node tokens from task tokens |
| `legion.io/region`, `legion.io/pool` | region and pool |
| `legion.io/node` | the node id, for audit only and never in `sub` |

### Sub template rules

Node `subTemplate`s work like task templates. They use simple `{field}`
substitution, and admission validation enforces these rules:

- **Allow-list only:** `{region}` and `{pool}`. Unknown placeholders are
  rejected.
- **`{node}` is forbidden.** Node ids change as nodes are replaced, so no
  stable policy could match them. The node id is claims-only
  (`legion.io/node`), like `{alloc}` for tasks.
- The template must contain at least one placeholder and render non-empty
  (a literal-only template would mint the same subject for every cluster).
- Names must be unique across `nodeIdentities`, and `audience` must be
  non-empty.

### Who consumes node identities

The **artifact system** ([docs/artifacts.md](artifacts.md)) uses node
identities. A task artifact can set `auth = { nodeIdentity = "artifact-pull" }`,
and cluster-level `artifacts.auth` matchers can route source prefixes to a
node identity. A node token is the worker node's own credential, sent to
whatever source the artifact names, so a job that sets `auth.nodeIdentity`
itself is [host-level](auth-and-acls.md#host-level-jobs): submitting it needs
a `job:write` rule with `host = true`. A cluster matcher is how an operator
lends a node identity to every job, for the sources it names. Cluster matchers can use node identities because they are
cluster-scoped. Attaching a per-task identity cluster-wide would weaken the
per-task `sub` security boundary, so matchers accept only `kv` and
`nodeIdentity`.

> **Issuance status.** The `IssueNodeIdentityToken` RPC mints a cluster-level
> token for a registered node (claims `region`, `pool`, and
> `legion.io/scope = "node"`). It checks the caller's per-node worker
> credential the same way task issuance does. The worker resolves
> `auth.nodeIdentity` through it, fetching a fresh token per artifact fetch
> that is used right away and never stored. (Task-scoped `auth.identity` for
> artifacts is not yet supported, because it needs the fetching allocation's
> id passed in the request.)

## The manager's own identity, and why Legion does not issue it

Tasks and worker nodes receive identities that Legion issues. The manager is
the other way round. Where it needs a cloud credential for itself, which today
means shipping its state to object storage under
[streaming persistence](persistence-streaming.md), it presents an identity
issued by the platform it runs on: EKS IRSA, a SPIRE JWT-SVID, Vault, an EC2
instance profile or an ECS task role. On EKS that takes no Legion config at
all; elsewhere it is two environment variables:

```ini
# The manager's service unit
Environment=AWS_ROLE_ARN=arn:aws:iam::123456789012:role/legion-manager-state
Environment=AWS_WEB_IDENTITY_TOKEN_FILE=/run/spire/svid/jwt
```

There is deliberately no `managerIdentities` block, even though the symmetry
with `nodeIdentities` makes one look natural:

- Legion's signing key lives in the SQLite store that streaming persistence
  backs up. A self-issued credential would mean that reading the store is
  enough to overwrite its backups.
- A restore runs before any store exists, so there would be no key and no
  token to restore with.
- AWS validates a token by fetching the issuer's JWKS from the manager, so the
  manager's ability to back itself up would depend on the manager being
  reachable.

The general rule: **an identity may not be issued by the system it is used to
protect.** Node identities satisfy it, because the manager is a separate trust
root from the worker. The manager's own backups cannot satisfy it against the
manager's own issuer.

The resolution order, IAM policy and failure modes are in
[Streaming persistence: Credentials](persistence-streaming.md#credentials).

## Provider integration cookbook

In every case, you configure the relying party to trust Legion's issuer
(`iss`) and validate tokens against the published JWKS. It then matches `aud`
and, where supported, other claims.

### AWS — `AssumeRoleWithWebIdentity`

1. Create an IAM OIDC identity provider for Legion's issuer URL.
2. Write a role trust policy that matches on `sub` and `aud` (the only claims
   AWS can match):

```json
{
  "Effect": "Allow",
  "Principal": { "Federated": "arn:aws:iam::<acct>:oidc-provider/manager.example.com:4747" },
  "Action": "sts:AssumeRoleWithWebIdentity",
  "Condition": {
    "StringEquals": {
      "manager.example.com:4747:aud": "sts.amazonaws.com",
      "manager.example.com:4747:sub": "auto:default:api:web:server"
    }
  }
}
```

3. The task calls `AssumeRoleWithWebIdentity` with the delivered token file.
   Because AWS can only match `sub`, choose your `subTemplate` to match your
   role-scoping convention.

### GCP — Workload Identity Federation

1. Create a workload identity pool and OIDC provider for Legion's issuer.
2. Map attributes from Legion claims and add conditions on them. GCP can match
   any claim:

```
attribute.job   = assertion['legion.io/job']
attribute.group = assertion['legion.io/group']
```

3. Grant the federated principal access. Exchange the token for a GCP access
   token via STS.

### Vault — JWT auth

```hcl
vault write auth/jwt/config \
  oidc_discovery_url="https://manager.example.com:4747"

vault write auth/jwt/role/api \
  role_type="jwt" \
  bound_audiences="vault.internal" \
  bound_claims='{"legion.io/job":"api","legion.io/group":"web"}' \
  user_claim="sub" \
  policies="api"
```

Vault can bind arbitrary claims, so you rarely need a custom `subTemplate`.

## Control-plane identity

A task can authenticate to Legion itself with the reserved identity name
`legion`. Declare it like any other identity, but **omit `audience`**. The
manager owns it and always sets `aud` to the cluster issuer. Say what the job
may do in Legion with `permissions`, and, once workload identity is enabled,
nothing else is needed: no other manager config, no address, and no token
handling in the task.

```nickel
identities = [{
  name = "legion",
  # What this job may do as itself: inline ACL rules, in the shape
  # `legion acl policy apply` takes.
  permissions = [
    { resources = ["kv"], actions = ["read", "write"], paths = ["certs/*"] },
  ],
}],
```

Inside the task, every `legion` command just works, as the job:

```sh
legion kv put certs/app.example.com "$(cat chain.pem key.pem)"
```

Declaring `legion` with an `audience` is rejected at admission, because the
name is reserved and the manager owns its audience.

The manager verifies these tokens with the same in-process signer and JWKS it
uses for everything else. Whenever workload identity is enabled, a built-in
auth method named `legion` handles this, with no OIDC discovery or network
round-trip. A workload presents its `legion` token as an
`Authorization: Bearer` credential on a manager RPC (the
[API socket](#the-api-socket) does this for it), or exchanges it with
`legion login --method legion --jwt <token>`. It authenticates as principal
`legion:<sub>`.

What the token may do comes from two places, added together:

- **The job's grant**: the `permissions` and `policies` its `legion` identity
  declared, granted at submit ([Permissions](#permissions)).
- **`workloadIdentity.controlPlaneBindings`** in manager config: claim-based
  rules for operators who map identities to policies wholesale
  ([Bindings](#bindings)).

A token with neither verifies to a principal and is allowed nothing.

### Permissions

The `legion` identity takes two fields:

| Field | Meaning |
| --- | --- |
| `permissions` | Inline ACL rules, exactly what a policy holds (`resources`, `actions`, `effect`, and the `namespaces`, `names`, `pools`, `paths`, `actionNames`, `templates`, `bands`, `host` selectors; see [Auth and ACLs](auth-and-acls.md#policies)). Validated like `legion acl policy apply` validates a policy. |
| `policies` | Names of existing ACL policies. Each must exist when the job is submitted, and is read by name when a token is verified, so a later edit to the policy applies to the job too. |

**Granted only up to what the submitter holds.** Every path that stores a job
config (`RunJob`, `RunJobTemplate`) checks that the token submitting it holds
every requested permission, and every rule of every named policy. The check
runs on every submit, an unchanged resubmit included, so write access to a
job does not let anyone redeploy access they could not grant themselves. A
submit that asks for more is refused before anything is stored:

```console
$ legion job run configs/jobs/cert-renewer.ncl
Error: failed to run job: cert-renewer-app-example-com: identity 'legion' asks for kv:write on
certs/*, which your token does not hold [code=PermissionDenied]
Hint: have someone with that access submit this job, or ask for it to be granted to you
```

With `--json` the same text is the envelope's `error.message` and
`error.hint`, with `code = "PermissionDenied"`.

How the check decides that the submitter "holds" a rule is in
[Auth and ACLs: Job grants](auth-and-acls.md#job-grants): exact for the
rules people write, conservative in a few cases, and never fooled by a held
`*` into granting a reserved `legion-*` namespace or `legion/` key.

**Recorded per version.** A granted submit records the grant with the job
config version it stored, in the submitter's name: `legion acl grants list`
shows each one and who made it. A token resolves the grant of the version its
allocation runs, so allocations of an old version mid-rollout keep that
version's grant. Grants are keyed by the canonical job id (`team-a/web`), so a
same-named job in another namespace gets nothing.

**Revocation** takes effect at the next request:

```console
$ legion acl grants revoke cert-renewer-app-example-com
revoked 1 grant of job `cert-renewer-app-example-com`; its tokens lose them at their next request
```

The token resolves its grant every time it is verified, and so does an ACL
token minted from it with `legion login --method legion` (it carries a
`grant:alloc/<id>` reference, not a copy). The allocations keep running. A
revoke does not stick to the job: the next submit by someone who holds the
access grants it again, as every submit does.

When Legion itself stores a new version, re-rendering a job after a KV change
([KV store](kv-store.md)), there is no submitter to check: the new version
keeps the previous version's grant when it asks for exactly the same, and gets
none when it asks for anything else.

Like Kubernetes RBAC, a grant outlives the granter's own access: a job
granted `kv:write certs/*` keeps it after the person who submitted it loses
it, until someone revokes the grant or resubmits the job without that access
(which is refused).

[Apps](apps.md) use the same mechanism: an app's manifest lists the
`permissions` its job's `legion` identity asks for, the check runs against
whoever installs or upgrades the app, and the app reconciler renders the
recorded grant into the app's job ([App grants](auth-and-acls.md#app-grants)).

### The API socket

Every allocation whose task declares the `legion` identity gets a unix socket
at `$LEGION_SECRETS_DIR/legion.sock` (the host's `<task dir>/secrets/
legion.sock`; `/legion/task/secrets/legion.sock` inside a container). The
worker listens on it and proxies gRPC to the managers:

- It forwards every call over the worker's own connection to the managers,
  TLS pinned to the cluster CA, and fails over to the next manager when the
  active one stops answering. A failed call through the socket counts toward
  that failover the way a failed heartbeat does.
- A call that carries no `authorization` header gets the allocation's current
  `legion` token, which the worker keeps fresh. A call that carries its own
  credential (a token from `legion login`) keeps it.
- The task needs no route to a manager. That matters where it runs by
  default: the [host guard](network-handlers.md) drops a task's connections
  to Legion's own ports on a manager's node, and a gated task has no route
  out at all. The socket is the allocation's own resource, so neither
  applies.

**Who may connect.** Tasks can share a user, so the file's mode is not enough.
Where every task is guaranteed a cgroup leaf (Linux with cgroup v2, and either
resource enforcement or the kernel host guard on), the worker reads the
connecting process's credentials (`SO_PEERCRED`) and its cgroup, and refuses a
process outside the allocation's own leaf
(`<cgroupParent>/{share,reserve}/legion-<alloc>`), whatever its user. The
refusal is logged on the worker:

```text
WARN Refused an API socket connection: pid 4121 (uid 65534) runs in cgroup
  /legion/share/legion-88, not in allocation 87's
```

Elsewhere (macOS, cgroup v1, enforcement and the host guard both off) only
file modes protect the socket: it sits in the allocation's directory, which
only the allocation's own user can reach, the same boundary that protects its
token file. Two allocations running as the same user, or a process running as
root, can reach each other's sockets there.

### Ambient env

A task that declares the `legion` identity gets, unless its own config sets
them:

| Variable | Value |
| --- | --- |
| `LEGION_ADDR` | `unix://$LEGION_SECRETS_DIR/legion.sock`, when the worker serves the allocation a socket. The CLI and the Rust client accept `unix://` addresses. |
| `LEGION_SECRETS_DIR` | The directory holding the token file and socket, in the task's own view: `<task dir>/secrets` for `exec`, `/legion/task/secrets` in a container, `/legion/identity/secrets` in a sandbox guest. |
| `LEGION_IDENTITY_TOKEN_FILE` | The `legion` token's file: `$LEGION_SECRETS_DIR/legion.token`, or the identity's own `file.path` when it declares one. Refreshed in place. For clients that cannot use a socket. |

`LEGION_IDENTITY_TOKEN_FILE` is deliberately not `LEGION_TOKEN_FILE`: that is
where `legion login` stores the token it mints, and it would overwrite the
worker's. `legion login` inside a task stores its token under
`~/.legion/tokens/` as anywhere else, and refuses to store it over the
identity token file even when `LEGION_TOKEN_FILE` names it.

### Sandboxes and other runtimes

- **Containers** reach the socket through the task-directory mount they
  already have.
- **Sandboxes** cannot reach a socket on the host. They get the token file at
  `/legion/identity/secrets/legion.token` (refreshed through the identity
  disk, see [Sandboxes](#sandboxes)) and `LEGION_IDENTITY_TOKEN_FILE`, but no
  `LEGION_ADDR`. Set `LEGION_ADDR` in the job to a manager the guest can
  reach, and present the token as a bearer credential
  (`LEGION_TOKEN=$(cat $LEGION_IDENTITY_TOKEN_FILE) legion ...`). That
  connection is direct, so it is subject to the guest's network policy and
  has no failover.
- **The Apple `container` runtime** (macOS dev mode) shares the task
  directory over its VM's file sharing, which does not carry unix sockets;
  use the token file there too.

### Bindings

`workloadIdentity.controlPlaneBindings` maps verified claims to policies in
manager config, the same way binding rules do for external JWT auth methods.
It is the claim-based path for operators who map identities wholesale:

```nickel
workloadIdentity = {
  enabled = true,
  controlPlaneBindings = [
    # Grant the `kv-reader` policy to every workload in namespace `ops`.
    { claim = "legion.io/namespace", value = "ops", policies = ["kv-reader"] },
  ],
}
```

Manager config is read at startup, so a change needs a restart, and
`legion.io/job` is the *bare* job name: a binding on it matches a job of that
name in every namespace. Prefer [Permissions](#permissions) for one job's
access.

### Known limitations

- **The coverage check is conservative.** A request covered only by several
  held rules together (a held `a*` and `b*` for a requested `*`) is refused;
  so is a request that leaves out a selector a held rule names. Have someone
  whose policy covers it outright submit the job.
- **A held deny rule** on a resource makes every grant on that resource need
  `acl:write`.
- **A named policy grants what it says at verification time**, not at submit:
  an `acl:write` holder who edits the policy changes every job that names it.
- **Sandboxes have no API socket** (see above).
- **Without cgroup attribution** the socket is protected by file modes only.
