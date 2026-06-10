# Workload Identity (OIDC)

Legion can act as an **OIDC identity provider (IdP)** and hand each task
instance a short-lived, asymmetrically-signed JWT that describes *what it is*
(region, job, group, task, node, allocation). External relying parties — AWS,
GCP, Vault, internal services — trust Legion's issuer and exchange that token
for their own native credentials. No long-lived secret ever touches the job
spec.

This document describes the feature, the job-spec API, and a provider
integration cookbook. The design rationale lives in
[docs/plans/workload-identity-oidc-plan.md](plans/workload-identity-oidc-plan.md).

> **Implementation status.** This is delivered in phases. The current code
> implements the *foundation*: the token/claims model and the security-critical
> `sub` template (`src/structs/workload_identity.rs`), the ES256 signer + JWKS +
> OIDC discovery document + key-rotation state machine
> (`src/manager/workload_identity.rs`), the per-task `identities` spec API with
> full admission validation, task-kind capability gating, persistence, manager
> config, and the [node identities](#node-identities) config/claims model
> consumed by the artifact system. Still to be wired (tracked as later phases
> in the plan): serving discovery/JWKS over the manager's TLS HTTP listener,
> the `IssueWorkloadIdentityToken` gRPC, worker-side delivery/refresh, and the
> background rotation loop. See the plan's *Implementation Phases* for the full
> breakdown.

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
    jwksPath: '/.well-known/jwks.json',
    signingAlg: 'ES256',
    defaultTtl: '15m',
    rotationAge: '24h',
    subTemplate: '{region}:{job}:{group}:{task}',
  },
}
```

The feature is off by default; existing jobs and the worker join-token flow are
unaffected.

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

> **Issuance status.** The config shape, validation, `sub`/claims model, and
> the artifact-spec surface are implemented. Actual token *issuance* to
> workers is pending the same later phases as task tokens — the
> `IssueWorkloadIdentityToken` RPC (which gains a node mode, gated on node
> registration) and worker-side delivery/refresh. Until those land, an
> artifact fetch authenticating with `nodeIdentity` fails with an actionable
> error suggesting `auth.kv` or anonymous access.

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

## Control-plane identity (forward-compatible)

The reserved identity name `legion` is issued with `aud` set to the cluster's
own issuer, so a workload can authenticate back to Legion itself with the same
signer, JWKS, and verification machinery. Mapping a verified control-plane
token to *permissions* (which KV prefixes / APIs it may touch) is a separate
ACL effort; this feature provides only the verified identity primitive.
