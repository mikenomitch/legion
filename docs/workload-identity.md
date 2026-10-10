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
> in-process and mapped to policies via `controlPlaneBindings`. Not yet
> implemented: task-scoped `auth.identity` for artifacts (it needs the
> fetching allocation's id), and a first-class control-plane authorization
> model beyond binding rules. The feature is off by default.

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
any claim don't have to parse `sub`: `legion.io/region`, `legion.io/job`,
`legion.io/group`, `legion.io/task`, `legion.io/node`, `legion.io/alloc` (and
`legion.io/instance` / `legion.io/slot` for stateful workloads).

### The `sub` claim is a security boundary

The default subject is:

```
sub = <region>:<job>:<group>:<task>      e.g.  auto:api:web:server
```

This matches Nomad. The subject stays stable across reschedules and gives
each task its own least-privilege identity. `alloc_id` and `node_id` are not
in `sub`, because they change often and no stable policy could match them.
They are included as `legion.io/*` claims instead.

`sub` matters most for **AWS IAM**, the least flexible major provider. There,
`sub` is effectively the only fine-grained key you can match on, because IAM
doesn't expose custom claims as condition keys. GCP and Vault can match any
`legion.io/*` claim directly.

Operators can override the format with a simple `{field}` template. It does
plain substitution with no logic. Set it at the cluster level
(`workloadIdentity.subTemplate`) or per identity (`subTemplate`). Admission
validation enforces these rules:

- **Allow-list only.** Allowed: `{region}`, `{job}`, `{group}`, `{task}`, and
  `{instance}`/`{slot}` (stateful tasks only). Unknown placeholders are rejected.
- **`{alloc}`/`{node}` are forbidden in `sub`.** They change often and would
  break identity that is stable across reschedules. They stay claims-only.
- **Minimum uniqueness.** A template must keep at least `{job}`, `{group}`, and
  `{task}`, so distinct workloads stay distinct. Templates without them are
  rejected.
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
- At least one of `env` or `file` must be set.
- `extraClaims` are static literals. Keys can't be a reserved JWT claim
  (`iss`/`sub`/`aud`/…) or use the `legion.io/*` namespace.
- **Change mode and delivery must be compatible.** `env` with
  `changeMode = "noop"` is rejected, because env can't refresh in place.
  `signal` requires `changeSignal`.
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
| `sandbox` | ✅ | ❌ | ❌ |
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
- If `issuer` is unset, it defaults to the manager's own
  `http://<host>:<port>`. That's fine for local development but not for
  federation.

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
     backing file at `.legion/identity/<alloc-id>/<name>/token` (mode
     `0400`). A companion `<ENV>_FILE` variable points at it.
4. A refresh loop on the worker, one per identity, re-issues the token at
   roughly half its remaining TTL. It adds jitter, and retries with backoff if
   the manager is unreachable. It rewrites the file(s) atomically, then applies
   the identity's `changeMode`. Loops stop when the allocation stops.
5. If the manager has the feature disabled, issuance returns
   `FailedPrecondition`. The worker logs one warning, and the task starts
   without tokens. Job specs still validate, so you can declare identities
   before the cluster enables the feature.

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
     # optional; defaults to http://127.0.0.1:4747 for local testing
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
node identity. Cluster matchers can use node identities because they are
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
      "manager.example.com:4747:sub": "auto:api:web:server"
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
manager owns it and always sets `aud` to the cluster issuer:

```nickel
identities = [
  {
    name = "legion",                 # reserved control-plane identity
    # no `audience` — the manager sets it to the cluster issuer
    file = { path = "/var/run/legion/identity/legion/token", mode = "0400" },
  },
]
```

Declaring `legion` with an `audience` is rejected at admission, because the
name is reserved and the manager owns its audience.

The manager verifies these tokens with the same in-process signer and JWKS it
uses for everything else. Whenever workload identity is enabled, a built-in
auth method named `legion` handles this, with no OIDC discovery or network
round-trip. A workload presents its `legion` token as an
`Authorization: Bearer` credential on a manager RPC (or via `legion login
--method legion --jwt <token>`). It authenticates as principal
`legion:<sub>`.

To map that verified identity to permissions, use binding rules under
`workloadIdentity.controlPlaneBindings`. These work the same way as the
claim-to-policy rules for external JWT auth methods:

```nickel
workloadIdentity = {
  enabled = true,
  controlPlaneBindings = [
    # Grant the `kv-reader` policy to any workload whose job is `api`.
    { claim = "legion.io/job", value = "api", policies = ["kv-reader"] },
  ],
}
```

With no binding rules (the default), a control-plane token still verifies to a
principal but carries no policies. Broader control-plane authorization (a
first-class policy model for workload-to-manager access) is not yet
available.
