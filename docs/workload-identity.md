# Workload Identity (OIDC)

Legion can act as an **OIDC identity provider (IdP)** and hand each task
instance a short-lived, asymmetrically-signed JWT that describes *what it is*
(region, job, group, task, node, allocation). External relying parties — AWS,
GCP, Vault, internal services — trust Legion's issuer and exchange that token
for their own native credentials. No long-lived secret ever touches the job
spec.

This document describes the feature, the job-spec API, and a provider
integration cookbook. The design rationale lives in
[internal_docs/plans/workload-identity-oidc-plan.md](../internal_docs/plans/workload-identity-oidc-plan.md).

> **Implementation status.** Fully implemented end to end for *task*
> identities: the token/claims model and the security-critical `sub` template
> (`src/structs/workload_identity.rs`); the ES256 signer + JWKS + OIDC
> discovery document + key-rotation state machine with **SQLite-persisted
> signing keys** (`src/manager/workload_identity.rs`,
> `signing_key` table); the public discovery/JWKS HTTP routes on the
> manager's main listener (`src/manager/oidc_http.rs`); the
> `IssueWorkloadIdentityToken` gRPC; worker-side delivery, refresh, and
> change modes (`src/worker/identity.rs`); the background rotation loop
> (`src/manager/daemon.rs`); per-task `identities` spec validation and
> task-kind capability gating; and manager config (off by default).
> [Node identities](#node-identities) are issued via `IssueNodeIdentityToken`
> and consumed by the artifact system. The
> [control-plane identity](#control-plane-identity) (`legion`) is verified
> in-process and mapped to policies via `controlPlaneBindings`. Still pending:
> task-scoped `auth.identity` for artifacts (needs the fetching alloc id), and
> a first-class control-plane authorization model beyond binding rules.

## The token

A workload identity token is an ES256-signed JWT. Standard claims:

| Claim | Meaning |
| --- | --- |
| `iss` | Issuer URL (matches the discovery document). |
| `sub` | Stable composed subject — see below. |
| `aud` | Audience(s) the task requested; manager-enforced, never empty. |
| `exp` / `iat` / `nbf` | Short TTL window. |
| `jti` | Unique token id for audit tracing. |
| `kid` (header) | Signing-key id, so verifiers pick the right JWKS entry. |

Plus discrete Legion facts, so providers that can match arbitrary claims don't
have to parse `sub`: `legion.io/region`, `legion.io/job`, `legion.io/group`,
`legion.io/task`, `legion.io/node`, `legion.io/alloc` (and
`legion.io/instance` / `legion.io/slot` for stateful workloads).

### The `sub` claim is a security boundary

The default subject is:

```
sub = <region>:<job>:<group>:<task>      e.g.  auto:api:web:server
```

This mirrors Nomad: stable across reschedules, per-task least privilege.
`alloc_id`/`node_id` are deliberately **not** in `sub` (they churn, so no
stable policy could be written) — they ride along as `legion.io/*` claims.

For the least-flexible major provider, **AWS IAM**, `sub` is effectively the
*only* fine-grained routing key (custom claims aren't exposed as IAM condition
keys). GCP and Vault can match any `legion.io/*` claim directly.

Operators can override the format with a dumb `{field}` template — no logic,
just substitution — at the cluster level (`workloadIdentity.subTemplate`) or
per identity (`subTemplate`). Admission validation enforces the guardrails:

- **Allow-list only.** Allowed: `{region}`, `{job}`, `{group}`, `{task}`, and
  `{instance}`/`{slot}` (stateful tasks only). Unknown placeholders are rejected.
- **`{alloc}`/`{node}` are forbidden in `sub`** — they churn and would destroy
  reschedule-stable identity. They stay claims-only.
- **Uniqueness floor.** A template must keep at least `{job}`, `{group}`, and
  `{task}`, so distinct workloads stay distinct. (Chosen as a hard reject.)
- **Non-empty render.** A literal-only or empty-rendering template is rejected.

## Job-spec API

Declare one or more named identities per task. Each is independent, so one task
can federate into several providers at once.

```jsonnet
{
  name: 'api',
  kind: 'exec',
  identities: [
    {
      name: 'aws',                       // unique per task
      audience: ['sts.amazonaws.com'],   // non-empty, manager-enforced
      ttl: '15m',
      file: { path: '/var/run/legion/identity/aws/token', mode: '0400' },
      changeMode: 'noop',                // SDK re-reads the file → no reload
    },
    {
      name: 'vault',
      audience: ['vault.internal'],
      ttl: '5m',
      env: 'VAULT_WORKLOAD_TOKEN',
      changeMode: 'signal',              // env can't change live → signal
      changeSignal: 'SIGHUP',
      extraClaims: { 'app.example.com/tier': 'backend' },
    },
  ],
}
```

A full runnable example is at
[configs/jobs/workload-identity.jsonnet](../configs/jobs/workload-identity.jsonnet).

### Validation rules

- `name` is required and unique within the task.
- `audience` must be non-empty (an audience-less token is replayable).
- At least one of `env` or `file` must be set.
- `extraClaims` are static literals; keys may not be a reserved JWT claim
  (`iss`/`sub`/`aud`/…) or use the `legion.io/*` namespace.
- **Change mode / delivery compatibility:** `env` + `changeMode: noop` is
  rejected (env can't refresh in place); `signal` requires `changeSignal`.
- The task kind must support the requested delivery (capability gating below).

### Change mode (refresh behavior)

When a token is re-issued before expiry, `changeMode` governs what happens:

- `noop` (default): atomically rewrite the file. Most cloud SDKs re-read the
  token file per call, so no reload is needed. **File + `noop` is the
  recommended default.**
- `signal`: rewrite, then send `changeSignal` (e.g. `SIGHUP`). The only way to
  refresh an env-delivered token without a restart.
- `restart`: rewrite, then restart the task.

### Task-kind capability gating

| Kind | env delivery | file delivery | signal |
| --- | --- | --- | --- |
| `exec` | ✅ | ✅ | ✅ |
| `container` | ✅ | ❌ | ❌ |
| `noop` | ❌ | ❌ | ❌ |

`container` mirrors its existing injection surface (env yes, file no — file
delivery into a container needs volume plumbing it doesn't yet implement, the
same limitation as KV file injection). An env-delivered identity on a container
must therefore refresh via `changeMode: restart`. Specs that ask for an
unsupported delivery are rejected at admission with an actionable error.

## Manager configuration

```jsonnet
// manager.jsonnet
{
  workloadIdentity: {
    enabled: true,                              // default false
    issuer: 'https://manager.example.com:4747', // public HTTPS issuer URL
    jwksPath: '/.well-known/jwks.json',            // must start with '/'
    signingAlg: 'ES256',                           // only ES256 is supported
    defaultTtl: '15m',
    rotationAge: '24h',
    subTemplate: '{region}:{job}:{group}:{task}',
  },
}
```

`signingAlg` is validated at startup and only `ES256` is accepted today —
setting anything else fails manager boot rather than silently signing ES256.
Malformed `defaultTtl`/`rotationAge`/`jwksPath` values also fail boot instead
of falling back to a default. Only `enabled` and `issuer` are required for
real federation; the rest have sensible defaults.

The feature is off by default; existing jobs and the worker join-token flow are
unaffected.

### Public endpoints (discovery + JWKS)

When enabled, the manager serves two unauthenticated, read-only HTTP/1.1
routes **on its main listener** (`server.port`):

- `GET /.well-known/openid-configuration` — the OIDC discovery document.
- `GET <jwksPath>` (default `/.well-known/jwks.json`) — every non-expired
  public signing key.

These are plain JSON over HTTP because relying parties (AWS STS, GCP, Vault)
fetch them with generic HTTP clients — they cannot speak gRPC. All other
paths on the listener continue to serve gRPC/grpc-web untouched.

The `issuer` config value is what gets published as `iss` in tokens and as
`issuer`/`jwks_uri` in the discovery document — it is **not** derived from
the bind address, so the published URLs stay correct behind proxies and load
balancers. Rules of thumb:

- **AWS / GCP** require the issuer to be reachable over **publicly-trusted
  HTTPS**. The manager itself currently listens without TLS, so put a
  TLS-terminating proxy (or LB) with a real certificate in front of the two
  routes and set `issuer` to that public HTTPS URL.
- **Vault / internal services** can point at the manager directly
  (`oidc_discovery_url` + a CA bundle, or plain HTTP inside a trusted
  network).
- If `issuer` is unset, it defaults to the manager's own
  `http://<host>:<port>` — fine for local development, not for federation.

### Signing-key persistence and restart semantics

Signing keys (including the private key PEM) are persisted in the manager's
SQLite store (`signing_key` table) — the **same trust domain as the CA
material**: anyone who can read the store can mint identities, so protect
the store file accordingly. Persistence is what makes restarts safe:

- On first boot with the feature enabled, the manager generates an ES256
  keypair and persists it.
- On every later boot it restores all non-expired keys, so the JWKS keeps
  publishing the same `kid`s and tokens already held by third parties keep
  verifying. No restart ever invalidates outstanding tokens.
- Rotation transitions (`active` → `retiring` → `expired`) are written
  through to the store; expired rows are retained for audit but never served.

### Key rotation

A background loop on the manager (started only when the feature is enabled)
checks roughly every `rotationAge / 10` (minimum 1 minute):

- When the active key is older than `rotationAge` (default 24h), it rotates:
  the old key becomes `retiring` with a `not_after` overlap window and stays
  in the JWKS; a new active key is generated and persisted. New tokens sign
  with the new `kid` immediately.
- Retiring keys past `not_after` are marked `expired` and dropped from the
  JWKS.

The overlap window is `max(rotationAge, defaultTtl)`, so any token signed by
the retiring key stays verifiable until it expires — provided identity TTLs
stay at or below `rotationAge` (keep token TTLs in minutes; that is the
point of workload identity). Note that a retiring key's `not_after` is
computed from the `rotationAge`/`defaultTtl` in effect **at rotation time**;
if you drastically *shrink* those values and restart, tokens already issued
under the older, larger TTL can expire after the key is dropped. Shrink token
lifetimes gradually (or accept that in-flight long-TTL tokens may need a
re-issue) rather than in one large step across a restart.

**HA note:** with raft/streaming persistence, only the **primary** manager
*rotates* keys (the same leader-only convention as other store-mutating
loops). Standby managers do not rotate, but on each rotation-check tick they
**reload** the key set from the replicated `signing_key` table, so a
follower's JWKS and signer pick up keys the primary rotated in — staleness is
bounded by the check interval (`max(rotationAge / 10, 1m)`), not frozen at
boot. With the default local-disk persistence there is a single manager and
it is always primary.

## How tokens reach the task

1. The manager's heartbeat/dispatch payload carries each task's identity
   *specs* (names, audiences, delivery, change mode) — never token bytes.
2. Before starting the task, the **worker** calls
   `IssueWorkloadIdentityToken` on the manager for each identity
   (`{ alloc_id, identity_name, node_id }`). The manager derives **all**
   claims from the allocation's scheduling record and the named identity in
   the stored spec; nothing security-relevant is read from the request, and
   the request is rejected unless the allocation is placed on the calling
   node. The `node_id` is bound to the caller's **authenticated** per-node
   worker token (principal `node:<node_id>`, minted at registration), so a
   worker cannot forge a peer's node id to mint that peer's tokens.
3. Delivery:
   - **file**: written to `file.path` with `file.mode` (default `0400`),
     atomically (temp file + rename, so readers never see a partial token).
     For `exec` tasks the path is used as given — absolute, or relative to
     the worker's working directory (same rooting as `exec` injected files).
   - **env**: injected into the task's env map before start (worker-side, so
     tokens never transit the scheduler). Because a live process's env can
     never change, every env-delivered identity is **also mirrored to a
     backing file** at `.legion/identity/<alloc-id>/<name>/token` (mode
     `0400`), and a companion `<ENV>_FILE` variable points at it.
4. A per-identity refresh loop on the worker re-issues the token at roughly
   half its remaining TTL (with jitter, and retry/backoff if the manager is
   unreachable), rewrites the file(s) atomically, then applies the
   identity's `changeMode`. Loops stop when the allocation stops.
5. If the manager has the feature disabled, issuance returns
   `FailedPrecondition`; the worker logs one warning and the task starts
   with its identities inert (no tokens delivered). Job specs still
   validate — declaring identities is allowed before the cluster enables
   the feature.

### What each change mode does on refresh

- `noop`: the file is atomically rewritten; nothing else happens. Cloud SDKs
  re-read web-identity token files on each call, so this is the recommended
  default for file delivery.
- `signal`: file(s) rewritten, then `changeSignal` (e.g. `SIGHUP`) is sent
  to the task's process. For env delivery the *live* env still holds the old
  token — the handler must re-read the backing file (see `<ENV>_FILE`
  above). An env-only identity with `signal` therefore can't refresh unless
  the app reads that file; this is exactly why the backing file exists.
- `restart`: the stored task config's env is updated with the fresh token,
  then the allocation is restarted in place — the relaunched process sees
  the new value directly.

Tokens at rest on the worker host: the delivered token files (mode `0400`),
the env-delivery backing files under `.legion/identity/`, and — for
env-delivered identities — the worker's persisted allocation state (which
records the task env so restarts survive worker restarts). All are written
owner-only: token files and their directories are created at a restrictive
mode from the start (never briefly world-readable), and the worker state file
and its directory are locked to the worker's own user. They are readable by
whoever can read the worker's state, the same trust boundary as the task
itself. Token bytes are never logged. When an allocation stops, the worker
removes its delivered token files and per-allocation backing directory.

**Known limitations.**

- *`exec` tasks that drop to a different `user`.* Token files are written
  owner-only by the worker's user. An `exec` task configured with a `user`
  runs de-privileged and hands over only its allocation directory tree, so a
  file-delivered token written outside that tree (an absolute `file.path`, or
  the `.legion/identity/` backing dir) is not readable by the task user. Until
  per-user token ownership lands, point `file.path` inside the allocation
  directory, or run the task as the worker's user, when using file delivery
  with a dropped-privilege `user`.
- *Apple `container` runtime (macOS).* The Apple container runtime passes env
  vars to `container run` on the command line, so an env-delivered identity
  token is visible to other local users via `ps` for the life of the launch
  process. Prefer file delivery, or the Linux/OCI runtime, for secret env on
  that runtime. (The Linux path writes env into the OCI bundle under the
  owner-only allocation directory and is not exposed this way.)

## Testing this locally in a basic way

1. Enable workload identity in `configs/manager.jsonnet` (the block is there,
   commented):

   ```jsonnet
   workloadIdentity: {
     enabled: true,
     // optional; defaults to http://127.0.0.1:4747 for local testing
     // issuer: 'https://manager.example.com:4747',
   },
   ```

2. Start a manager (with an in-process worker for a one-machine demo):

   ```sh
   cargo run -- start manager -c configs/manager.jsonnet --run-worker
   ```

   The log line `Workload identity enabled; serving OIDC discovery and JWKS`
   confirms the issuer is up (and the `signing_key` table now holds the key).

3. Curl the public endpoints:

   ```sh
   curl -s http://127.0.0.1:4747/.well-known/openid-configuration | jq
   curl -s http://127.0.0.1:4747/.well-known/jwks.json | jq
   ```

4. Run the example job and find the delivered token:

   ```sh
   cargo run -- job run configs/jobs/workload-identity.jsonnet
   sudo cat /var/run/legion/identity/aws/token   # the 'aws' file identity
   ```

5. Decode the token (any JWT tool works; here with plain shell):

   ```sh
   TOKEN=$(sudo cat /var/run/legion/identity/aws/token)
   echo "$TOKEN" | cut -d. -f2 | tr '_-' '/+' | base64 -d 2>/dev/null | jq
   # → iss, sub ("auto:workload-identity-demo:default:api"), aud, exp,
   #   legion.io/job, legion.io/alloc, ...
   ```

   (`jwt decode "$TOKEN"` from mike-engel/jwt-cli gives the same view.)

6. Watch a refresh happen: the `aws` identity uses `ttl: '15m'`, so the
   worker rewrites the file after ~7–8 minutes. For a faster demo set
   `ttl: '1m'` in the jobspec and watch:

   ```sh
   watch -n5 'sudo stat -c "%y" /var/run/legion/identity/aws/token;      sudo cat /var/run/legion/identity/aws/token | cut -d. -f2 | tr "_-" "/+" | base64 -d 2>/dev/null | jq .exp'
   ```

   Each refresh bumps the mtime and `exp`, with no task restart
   (`changeMode: noop`). The `vault` env identity refreshes its backing file
   (`.legion/identity/<alloc>/vault/token`) and SIGHUPs the task instead.

## Node identities

Task identities prove *which workload* is fetching or calling out. Some
policies want the inverse grain: not "task X may read" but "**the cluster's
worker nodes** may read" — an ops bucket of tooling every workload needs, or
a pre-warm fetch that happens before any task exists. **Node identities** are
identities issued to worker nodes rather than tasks, declared once at the
cluster level:

```jsonnet
// manager.jsonnet
{
  workloadIdentity: {
    enabled: true,
    issuer: 'https://manager.example.com:4747',
    // ... existing fields ...

    nodeIdentities: [
      {
        name: 'artifact-pull',            // unique cluster-wide
        audience: ['sts.amazonaws.com'],  // non-empty, like task identities
        ttl: '15m',                       // optional; cluster default TTL otherwise
        subTemplate: '{region}:node:{pool}',  // optional; this is the default
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
| `legion.io/scope` | `"node"` — distinguishes node tokens from task tokens |
| `legion.io/region`, `legion.io/pool` | discrete facts |
| `legion.io/node` | the node id — audit only, never in `sub` |

### Sub template rules

Node `subTemplate`s follow the same philosophy as task templates — dumb
`{field}` substitution with admission-validated guardrails:

- **Allow-list only:** `{region}` and `{pool}`. Unknown placeholders are
  rejected.
- **`{node}` is explicitly forbidden** — node ids churn as nodes are
  replaced, so no stable policy could be written against them. The node id
  is claims-only (`legion.io/node`), exactly like `{alloc}` for tasks.
- The template must contain at least one placeholder and render non-empty
  (a literal-only template would mint the same subject for every cluster).
- Names must be unique across `nodeIdentities`; `audience` must be
  non-empty.

### Who consumes node identities

The **artifact system** ([docs/artifacts.md](artifacts.md)) is the first
consumer: a task artifact may set `auth: { nodeIdentity: 'artifact-pull' }`,
and cluster-level `artifacts.auth` matchers may route source prefixes to a
node identity. Cluster matchers may use node identities precisely *because*
they are cluster-scoped — attaching a per-task identity cluster-wide would
blur the per-task `sub` security boundary, so matchers accept `kv` and
`nodeIdentity` only. The planned artifact pre-warm layer authenticates its
warm fetches the same way (a warm pull happens before any task exists).

> **Issuance status.** Node-token issuance is implemented: the
> `IssueNodeIdentityToken` RPC mints a cluster-grain token for a registered
> node (claims `region`, `pool`, and `legion.io/scope = "node"`), authorized
> against the caller's per-node worker credential exactly like task issuance.
> The worker resolves `auth.nodeIdentity` through it — a fresh token per
> artifact fetch, used immediately and never persisted. (Task-scoped
> `auth.identity` for artifacts still needs the fetching allocation's id
> threaded into the request and remains pending.)

## Provider integration cookbook

In every case the relying party is configured to trust Legion's issuer
(`iss`) and validate the token against the published JWKS, then matches `aud`
and (where supported) claims.

### AWS — `AssumeRoleWithWebIdentity`

1. Create an IAM OIDC identity provider for Legion's issuer URL.
2. Write a role trust policy keyed on `sub` and `aud` (the only claims AWS can
   match):

```json
{
  "Effect": "Allow",
  "Principal": { "Federated": "arn:aws:iam::<acct>:oidc-provider/manager.example.com:4747" },
  "Action": "sts:AssumeRoleWithWebIdentity",
  "Condition": {
    "StringEquals": {
      "manager.example.com:4747:aud": "sts.amazonaws.com",
      "manager.example.com:4747:sub": "auto:api:web:server"
    }
  }
}
```

3. The task calls `AssumeRoleWithWebIdentity` with the delivered token file.
   Because AWS can only match `sub`, choose your `subTemplate` to match your
   role-scoping convention.

### GCP — Workload Identity Federation

1. Create a workload identity pool + OIDC provider for Legion's issuer.
2. Map attributes from Legion claims and condition on them — GCP can match any
   claim:

```
attribute.job   = assertion['legion.io/job']
attribute.group = assertion['legion.io/group']
```

3. Grant the federated principal access; exchange the token for a GCP access
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

A task authenticates back to Legion itself with the reserved identity name
`legion`. Declare it like any other identity, but **omit `audience`** — the
manager owns it and always stamps `aud` = the cluster issuer:

```jsonnet
identities: [
  {
    name: 'legion',                 // reserved control-plane identity
    // no `audience` — the manager sets it to the cluster issuer
    file: { path: '/var/run/legion/identity/legion/token', mode: '0400' },
  },
]
```

Declaring `legion` with an `audience` is rejected at admission (the name is
reserved and its audience is manager-owned).

The manager verifies these tokens with the **same in-process signer/JWKS** it
uses for everything else, via a built-in auth method registered under the name
`legion` whenever workload identity is enabled — no self-referential OIDC
discovery, no network round-trip. A workload presents its `legion` token as an
`Authorization: Bearer` credential on a manager RPC (or via `legion login
--method legion --jwt <token>`); it authenticates as principal
`legion:<sub>`.

Mapping that verified identity to *permissions* is done with binding rules —
the same claim→policy mechanism as external JWT auth methods — under
`workloadIdentity.controlPlaneBindings`:

```jsonnet
workloadIdentity: {
  enabled: true,
  controlPlaneBindings: [
    // Grant the `kv-reader` policy to any workload whose job is `api`.
    { claim: 'legion.io/job', value: 'api', policies: ['kv-reader'] },
  ],
}
```

With no binding rules (the default), a control-plane token still verifies to a
principal but carries no policies — the identity primitive only. Broader
control-plane authorization (a first-class policy model for workload → manager
access) remains a separate ACL effort; `controlPlaneBindings` is the seam it
builds on.
