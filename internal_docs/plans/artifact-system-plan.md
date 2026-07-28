# Plan: Artifact Fetching for Tasks

> **Judgement calls:** [`calls/artifact-system-plan_calls.md`](../calls/artifact-system-plan_calls.md)

## Problem Statement

Tasks need *stuff* before they can run: binaries and tarballs from release
URLs, builds from S3-compatible object storage, container images and ORAS
artifacts (WASM modules, ML weights) from OCI registries, repos from git.
Today Legion has no way to express this. The `exec` kind runs whatever binary
already exists on the node, the `container` kind shells out and hopes the
runtime can pull the image anonymously, and the only fetching code in the
tree (`src/getter_rs`) is used solely by the CLI to download jobspecs.

Concretely, today there is:

- no `artifacts` field in the job spec, so every kind would have to invent
  its own download story inside `task.config` (no reuse, divergent auth),
- no authentication for fetches — neither secret-based (registry creds,
  S3 keys) nor identity-based ("this is Legion task `api/web/server`" or
  "this is a Legion worker node in pool `default`" fetching),
- no way for a task to *reference* what was fetched (no task directory, no
  paths or env vars pointing at downloaded content),
- no visibility — nothing in the UI or CLI explains that an allocation is
  stuck downloading, or what a task's inputs even are.

This plan defines one artifact system shared by every task kind: a small
declaration in the job spec, a fetcher registry keyed by URL scheme, staging
into a per-allocation task directory, auth via KV-stored secrets or workload
identity (task-scoped *and* node-scoped), digest pinning at submit, and UI
surfaces for all of it.

**Caching and pre-warming are deliberately out of this plan.** They live in
the companion plan,
[artifact-cache-and-prewarm-plan.md](artifact-cache-and-prewarm-plan.md),
which layers a content-addressed cache, TTLs, disk budgets, pre-pull, and
scheduler locality scoring on top of the interfaces defined here. This plan
is complete and shippable on its own: without the cache, every staging
fetches from origin — correct, just not yet fast. The seams the cache plan
needs (the `ArtifactSink` write abstraction, digests everywhere, per-artifact
status reporting) are part of this plan's contract so the cache slots in
without reshaping anything.

`internal_docs/TODO.md` ("Artifact system"), `internal_docs/differentiators.md`
("Artifacts … Type, auth, how to get them"), and `docs/plugin-system.md`
("Artifact acquisition plugins") all anticipate this work; this plan gives
it a concrete shape.

## User Scenarios

These six scenarios are the acceptance lens for this plan. (Scale-up
warming, cache dedupe, and disk-budget scenarios live in the companion
plan.)

**S1 — Release tarball, referenced by name.** A batch task downloads
`https://github.com/acme/tool/releases/download/v1.4/tool-linux-amd64.tar.gz`,
verifies a sha256, unpacks it into its task directory, and runs it as
`${artifact.tool}/tool` — no hardcoded paths, no guessing where the bytes
landed. One line of spec plus a name.

**S2 — Private build from S3-compatible storage, no secrets in spec.** A
service task fetches `s3://acme-builds/api/v42.tar.gz` from MinIO/R2/AWS.
The task declares a workload identity; the worker presents the identity
token (exchanging it for provider credentials where needed) so the bucket
policy can say "only `prod:api:web:server` may read this prefix." The job
spec contains zero credentials.

**S3 — Pull as the cluster's worker nodes.** An ops bucket holds tooling
every workload needs. Rather than minting a task identity per consumer, the
bucket policy says "any Legion worker node in region `prod`, pool `default`
may read." Tasks (and later, pre-warm policies) fetch under a *node
identity* — a token that proves "I am a Legion worker node of this
cluster," not "I am task X."

**S4 — Private container image with centrally managed creds.** A container
task uses `image: 'ghcr.io/acme/api:v42'`. Registry credentials live in
Legion KV at `secrets/ghcr-pull`; a cluster-level auth matcher maps
`oci://ghcr.io/acme/**` to that KV path. The job spec says nothing about
auth, and rotating the credential is a single `legion kv put`. At submit,
the tag is resolved to a digest so every allocation in the rollout runs
identical bytes.

**S5 — ORAS artifact (model weights / WASM) through the same pipeline.** A
task pulls `oci://registry.acme.io/models/embedder@sha256:abc…`, selecting
layers by media type. Same spec shape, same auth, same staging as every
other artifact — and the future `wasm` kind consumes modules the same way
the container kind consumes images.

**S6 — Operator sees what's happening.** A deploy looks stuck. The UI's
allocation view shows the task is `staging artifacts`, with per-artifact
rows: one staged, one fetching at 37%, one failed with a redacted error.
The job's task view shows the declared artifacts, the digest each tag
resolved to at submit, and which auth (KV path name / identity name — never
values) each fetch uses.

## Goals

- One `artifacts` declaration shared by every task kind — current and
  future — with the fetch/verify/unpack/stage pipeline implemented once in
  the worker core, not per plugin.
- Sources: raw URLs (`https://`), local files (`file://`), git, S3-compatible
  object storage (`s3://` — AWS, MinIO, R2, GCS-interop), and OCI
  (`oci://` — both container images and ORAS artifacts).
- Auth three ways: secrets stored in Legion KV, task-scoped workload
  identity, and **node-scoped workload identity** (fetch as the cluster's
  worker nodes). Cluster-level defaults so most specs carry no auth block.
- A clean way for tasks to reference staged artifacts: names,
  `${artifact.<name>}` interpolation, and `LEGION_ARTIFACT_<NAME>` env vars.
- Digest pinning at submit (`resolve: 'pin'`, the default) so rollouts are
  deterministic, with `resolve: 'always'` to roll with a mutable tag.
- A streamlined recipe for task kinds to adopt artifacts — capability flag,
  config desugaring, typed consumption — modeled on how the container kind
  consumes images.
- Artifacts visible in the UI and CLI: declared inputs on tasks, live
  staging state on allocations, resolved digests, failure reasons.
- Validation at admission: bad schemes, malformed checksums, unknown
  identity references, name collisions, and kind/capability mismatches fail
  at `job run`, not on a node twenty minutes later.

## Non-Goals

- **No caching, no pre-warming, no locality scoring.** That is the
  companion plan ([artifact-cache-and-prewarm-plan.md](artifact-cache-and-prewarm-plan.md)).
  This plan must stand alone without it.
- **Not a build system.** Legion fetches artifacts; producing them (docker
  build, buildpacks) is out of scope.
- **Not an artifact registry.** Legion does not host artifacts; it pulls
  from external stores.
- **Not volumes.** Artifacts are immutable inputs staged before start;
  mutable shared state is the stateful-workloads effort.
- **No KV ACLs.** Which tasks may reference which KV secret paths is a
  separate ACL effort. This plan inherits today's trust model and
  documents that loudly (see Auth and Judgement Calls; tracked in
  `internal_docs/TODO.md`).
- **No live re-fetch of changed artifacts under a running task.** Artifacts
  are resolved at staging time; reacting to a moved tag mid-flight is
  rollout territory (`resolve` controls when tags are re-read).

## Background: What Exists Today

Facts about the current codebase that shape the design:

- **`src/getter_rs`** — a go-getter-style `Getter` trait with `file`,
  `http`, and `git` implementations (blocking, no auth, no checksums).
  Used only by `src/cmd/job.rs::resolve_jobspec_source` to fetch jobspecs
  for `job run/validate/plan`. This becomes the seed of the fetcher
  registry.
- **`object_store = { version = "0.11", features = ["aws"] }`** is already
  a Cargo dependency (used by persistence streaming). The `s3://` fetcher
  is therefore nearly free and covers any S3-compatible endpoint.
- **Task plugins** (`src/task_plugins/mod.rs`) expose lifecycle hooks
  (`prepare` → `start` → `stop` → `destroy`) and capability gates
  (`supports_kv_env_injection()`, `supports_identity_file()`, …). The
  worker's `run_task_allocation` calls `prepare` then `start` synchronously;
  failures route through `record_failure` → heartbeat → manager move logic.
- **No per-allocation directory exists.** `exec` honors an optional `cwd`;
  nothing provisions a private workdir, and worker state is a single JSON
  file under `.legion/workers/<id>/` (`WorkerStateStore`).
- **Heartbeat** (`HeartbeatRequest`) carries allocation IDs, failures, and
  `AllocationHealth { allocation_id, healthy, status }`; the UI polls
  manager gRPC-Web APIs (SolidJS app in `ui/`, clients generated from
  `proto/legion.proto` into `ui/src/gen/`).
- **Workload identity** (`docs/workload-identity.md`): the token model,
  ES256 signer, JWKS, per-task `identities` spec, and admission validation
  exist today. The `IssueWorkloadIdentityToken` RPC and worker-side
  delivery are later phases of that plan — the identity-auth phase here
  depends on them, and this plan *extends* the identity model with
  node-scoped identities (§6).
- **KV** (`docs/kv-store.md`): server-side CRUD with versioning; job submit
  resolves `$path/key` references. There is no separate "secrets" type —
  KV values are the secret store, and there are no KV ACLs yet.

## Proposed Design

### 0) Design at a Glance

```
        job spec                     manager                          worker
  ┌──────────────────┐   ┌────────────────────────────┐   ┌─────────────────────────────────┐
  │ tasks: [{        │   │ admission:                 │   │ stage_artifacts(alloc, task)    │
  │   kind: 'exec',  │──▶│  - parse/validate specs    │──▶│  1. resolve auth                │
  │   artifacts: [{  │   │  - desugar container image │   │     (KV / task WI / node WI)    │
  │     name:'tool', │   │    -> oci:// artifact      │   │  2. fetch -> .staging tmp       │
  │     source:'…',  │   │  - resolve tags -> digests │   │     (hash while writing)        │
  │   }],            │   │    (resolve: 'pin')        │   │  3. verify checksum, unpack     │
  │   config: {      │   │  - persist normalized list │   │  4. move into task dir          │
  │     command:     │   │                            │   │  5. expose ${artifact.tool} +   │
  │   ['${artifact.  │   │ status: per-artifact state │   │     LEGION_ARTIFACT_TOOL        │
  │     tool}/tool'] │◀──│ flows back via heartbeat,  │◀──│  6. report per-artifact states  │
  │   },             │   │ served to CLI + UI         │   │     in heartbeat                │
  │ }]               │   └────────────────────────────┘   └─────────────────────────────────┘
  └──────────────────┘
```

Five principles keep the moving parts honest:

1. **Fetching lives in the worker core, not in task plugins.** Plugins
   *consume* staged artifacts (and may *contribute* implicit ones, like the
   container image); they never download. This is the reuse-across-kinds
   guarantee.
2. **This plan is correct standalone; the cache plan only makes it
   faster.** Fetchers write through an `ArtifactSink` abstraction and
   everything is digest-identified, so the companion plan swaps the sink
   for a content-addressed store without touching fetchers, specs, or
   plugins.
3. **Declarations stay one line.** A bare source string is a full artifact
   spec; auth defaults come from cluster config; `image:` keeps working and
   desugars into the same pipeline.
4. **Auth is resolved late, on the worker, at fetch time.** Credentials
   never ride through allocation rows or the scheduler. Task identities
   prove *which workload* is fetching; node identities prove *the cluster's
   worker nodes* are fetching; KV covers everything else.
5. **Pin by default, roll by choice.** Mutable references (OCI tags, git
   refs) resolve to digests at submit so a rollout ships one artifact;
   `resolve: 'always'` opts a spec back into tracking the tag.

### 1) Job-Spec API

`artifacts` is a new first-class task field, sitting beside `env`, `files`,
`kv`, and `identities` — *not* inside the kind-specific `config`. That is
what makes it shared across kinds and uniformly validatable.

The minimal form:

```jsonnet
{
  name: 'tool',
  kind: 'exec',
  lifecycle: 'batch',
  artifacts: [
    'https://github.com/acme/tool/releases/download/v1.4/tool-linux-amd64.tar.gz',
  ],
  config: {
    // Archives auto-unpack into the task directory; cwd defaults there too.
    command: ['./tool', '--once'],
  },
}
```

The full form:

```jsonnet
artifacts: [
  {
    // Scheme picks the fetcher: https://, http://, file://, git::, s3://, oci://
    source: 's3://acme-builds/api/v42.tar.gz',

    // Optional handle for referencing the staged artifact from task config
    // (§5). Required if you want ${artifact.<name>} / LEGION_ARTIFACT_<NAME>.
    // Lowercase [a-z0-9_-], unique within the task.
    name: 'build',

    // Destination, relative to the task directory. Absolute paths rejected.
    // Default: '.' for archives/trees, source basename for single files.
    dest: 'build/',

    // 'sha256:<hex>'. Presence makes the artifact immutable and verification
    // mandatory.
    checksum: 'sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',

    // 'auto' (default; unpacks recognized archive extensions), 'archive'
    // (force), or 'file' (never unpack).
    unpack: 'auto',

    // For mutable references (OCI tags, git refs): 'pin' (default) resolves
    // to a digest at submit; 'always' re-resolves at every staging. §7.
    resolve: 'pin',

    // Auth override. Omitted => cluster artifactAuth matchers apply.
    // Exactly one of kv / identity / nodeIdentity. §6.
    auth: { kv: 'secrets/build-store' },

    // chmod for single-file artifacts (handy for bare binaries).
    mode: '0755',
  },
]
```

OCI sources support digest pinning and ORAS selection (S5):

```jsonnet
artifacts: [
  {
    name: 'model',
    source: 'oci://registry.acme.io/models/embedder@sha256:abc123...',
    // For ORAS multi-file artifacts: pick layers by media type. Omit to
    // take the whole artifact.
    mediaType: 'application/vnd.acme.model.weights.v1',
    dest: 'models/',
  },
]
```

Container tasks keep their existing surface; `image` is *desugared by the
manager at admission* into an implicit artifact (S4, §8):

```jsonnet
{
  name: 'api',
  kind: 'container',
  config: {
    image: 'ghcr.io/acme/api:v42',   // becomes oci://ghcr.io/acme/api:v42,
    command: ['/srv/api'],            // consumed as an image by the plugin
  },
}
```

Forward compatibility: the companion cache plan adds a `ttl` field (cache
revalidation window) to this same spec; nothing here needs to change for it.

#### Validation rules (admission, in `job validate` / `job plan` too)

- `source` must parse and use a scheme in the cluster's `allowedSchemes`.
- `name` matches `[a-z0-9_-]+` and is unique within the task; any
  `${artifact.<name>}` reference in task config must name a declared
  artifact (catches typos at submit, not at start).
- `checksum` must be `sha256:<64 hex>`; OCI `@digest` sources are treated
  as checksummed.
- `dest` must be relative and may not escape the task directory (`..`
  rejected after normalization).
- `auth` must set exactly one of `kv` / `identity` / `nodeIdentity`;
  `identity` must name an entry in the task's `identities` list;
  `nodeIdentity` must name an entry in cluster
  `workloadIdentity.nodeIdentities` (§6).
- `resolve: 'pin'` on a mutable source requires the manager to resolve it
  at submit (§7); resolution failure fails the submit with the fetch error.
- The task kind must report `supports_artifacts()` (capability gating,
  mirroring KV/identity injection). `noop` rejects artifacts.

### 2) Allocation and Task Directories (prerequisite)

Artifacts need somewhere to land, and today nothing provisions one. Phase 0
introduces:

```
<worker_state_dir>/<worker_id>/
  worker_state.json                  # existing
  allocs/<alloc_id>/<task_name>/     # new: the task directory
```

- The task directory is created before `prepare()` and removed on
  `destroy()` (best-effort, logged).
- Exposed to tasks as `LEGION_ALLOC_DIR` / `LEGION_TASK_DIR` env vars; the
  `exec` kind's `cwd` defaults to the task directory when unset.
- `exec` tasks using landlock get the task directory appended to
  `read_write_paths` automatically — otherwise artifacts would be staged
  somewhere the task cannot read.
- Artifacts stage into `dest` relative to this directory.

This is deliberately the smallest possible alloc-dir feature. The stateful
workloads plan can later bind durable volumes alongside it; nothing here
conflicts.

### 3) Fetchers: One Registry, Five Schemes

`src/getter_rs` grows into `src/artifacts/` (the CLI jobspec path switches
to the same registry — one fetch stack in the tree, which is itself a small
win):

```
src/artifacts/
  mod.rs        # ArtifactSpec, normalized forms, public API
  fetcher.rs    # ArtifactFetcher trait + registry
  fetchers/     # http.rs, file.rs, git.rs, s3.rs, oci.rs
  auth.rs       # credential resolution (§6)
  stage.rs      # staging pipeline: sink, verify, unpack, move (§4)
  status.rs     # per-artifact state reporting (§9)
```

The trait (blocking, like the existing getters — see Judgement Calls):

```rust
/// One fetcher per scheme. Fetchers stream bytes into an ArtifactSink;
/// they never see task directories and never receive raw KV paths — only
/// already-resolved credentials.
pub trait ArtifactFetcher: Send + Sync {
    fn schemes(&self) -> &'static [&'static str];

    /// Resolve a mutable reference to a stable identity without
    /// downloading: OCI tag -> manifest digest, git ref -> commit sha,
    /// HTTP HEAD + ETag where available. Used by the manager for
    /// `resolve: 'pin'` at submit (§7) and by the cache plan for
    /// revalidation. Returns None when the source has no cheap probe.
    fn resolve(&self, req: &FetchRequest<'_>) -> R<Option<ResolvedIdentity>> {
        let _ = req;
        Ok(None)
    }

    /// Download the artifact into `sink`. The sink hashes as it writes, so
    /// checksum verification and digest computation share one pass.
    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> R<FetchedArtifact>;
}

pub struct FetchRequest<'a> {
    pub source: &'a Url,
    pub checksum: Option<&'a Checksum>,
    pub media_type: Option<&'a str>,
    pub credential: Option<&'a Credential>,
    pub limits: &'a FetchLimits, // max bytes, deadline
}
```

`ArtifactSink` is the seam the cache plan needs: in this plan it is a
hashing writer into a per-allocation `.staging` temp directory; the cache
plan substitutes a content-addressed-store writer. Fetchers cannot tell the
difference, which is what keeps the two plans decoupled.

Per-scheme notes:

- **`http` / `https`** — extends the existing `HttpGetter` with streaming
  (no more whole-body buffering), bearer/basic/header credentials, redirect
  and size limits, ETag in `resolve` where servers provide one.
- **`file`** — existing behavior; copies into the sink. Useful for dev and
  node-local pre-provisioned artifacts.
- **`git`** — existing shallow-clone behavior, plus `ref=` query support
  (`git::https://github.com/acme/repo?ref=v1.2.0`) and `resolve` via
  `git ls-remote` so refs pin to commit SHAs at submit.
- **`s3`** — new, thin: `object_store`'s `AmazonS3Builder` (already in the
  dependency tree) speaks any S3-compatible endpoint. Region/endpoint come
  from the credential object or query params
  (`s3://bucket/key?endpoint=https://minio.internal:9000&region=auto`),
  query params winning. ETag conditional support in `resolve`.
- **`oci`** — new, built on the **`oci-client` crate** (decided — see
  Judgement Calls): registry auth dance (anonymous, basic, bearer, OIDC
  token exchange), manifest resolution (index → platform manifest), and two
  materializations: an OCI layout tree for image consumers (container
  plugin) and selected-layer extraction for ORAS consumers (`mediaType`).
  Per-blob dedupe across images is cache-plan territory; here each staging
  materializes what it needs.

Dynamic fetcher plugins (`plugins/artifacts/<scheme>/`, `cdylib` exporting
`get() -> Box<dyn ArtifactFetcher>`, mirroring task-kind plugins) are the
final phase — the trait above is the contract from day one so nothing needs
reshaping later.

### 4) The Staging Pipeline

Staging slots in front of the existing plugin lifecycle in
`run_task_allocation`:

```rust
// worker/task_manager.rs (sketch)
fn run_task_allocation(&mut self, ta: TaskAllocation) -> R<()> {
    let (allocation, task) = self.register(ta)?;
    self.set_allocation_health(allocation.id, true, "staging artifacts");

    // Blocking fetch pipeline on a blocking thread; honors the worker's
    // fetch parallelism. All-or-nothing: any artifact failing (after
    // per-artifact retries) fails the allocation through the existing
    // record_failure path -> heartbeat -> manager on_unhealthy move logic.
    let staged = self
        .artifacts
        .stage_for(&allocation, &task, &mut self.artifact_status)
        .with_context(|| format!("artifact staging for allocation {}", allocation.id))?;

    let mut runtime = TaskRuntime::default();
    runtime.staged_artifacts = staged; // typed payloads + paths + digests (§8)

    let plugin = self.registry.plugin(&task.kind).context("plugin")?;
    plugin.prepare(&allocation, &task, &mut runtime)?;
    plugin.start(&allocation, &task, &mut runtime)?;
    // … existing persistence + health monitor wiring …
}
```

Mechanics:

- Fetchers write into `allocs/<id>/<task>/.staging/`; bytes are hashed
  while streaming; checksum mismatches delete the temp content and fail the
  fetch — corrupt bytes never reach a task directory.
- Unpack happens from the staging dir with path-traversal protection
  (absolute paths, `..`, and symlinks escaping the root are rejected) and
  decompression size limits; then an atomic move into `dest`.
- Per-artifact retries (`attempts`, default 3) with backoff, an overall
  deadline (`timeout`), and a `maxArtifactSize` guard.
- **Failure semantics (decided):** artifact fetch failures reuse the
  existing `on_unhealthy` restart/move budgets — a staging failure is an
  allocation failure, reported via heartbeat, restarted or moved by the
  manager like any other failure. A dedicated `onFetchFailure` policy
  (e.g., treating a registry outage differently from an unhealthy task) is
  a noted follow-up, not in scope. Document this in the user-facing
  artifacts doc so operators aren't surprised that fetch flakiness consumes
  move budget.

Worker fetch configuration (delivered through the existing worker-config
mechanism — `configs/workers/*.jsonnet`; the cache plan later adds a
sibling `cache` block under the same `artifacts` key):

```jsonnet
{
  workerType: 'default',
  pool: 'default',
  artifacts: {
    fetch: {
      parallelism: 4,        // concurrent fetches per worker
      attempts: 3,           // per-artifact retries before alloc failure
      timeout: '10m',
      maxArtifactSize: '50GiB',
    },
  },
}
```

### 5) Referencing Staged Artifacts from Tasks

Named artifacts are addressable two ways, so tasks never hardcode staging
paths (S1):

1. **Env vars (any kind that supports env injection):**
   `LEGION_ARTIFACT_<NAME>` (name uppercased, `-` → `_`) holds the absolute
   staged path — the `dest` directory for trees/archives, the file path for
   single files. `LEGION_TASK_DIR` / `LEGION_ALLOC_DIR` ride along from §2.
2. **`${artifact.<name>}` interpolation:** the worker expands the token in
   the kind config's string fields just before start. For `exec`:
   `command`, `env` values, and `cwd`. For `container`: `command` and `env`
   values (paths only make sense once the kind grows volume plumbing —
   same limitation as its KV file injection).

```jsonnet
{
  name: 'embedder',
  kind: 'exec',
  artifacts: [
    { name: 'tool',  source: 'https://releases.acme.io/embed/v1.4.tar.gz', checksum: 'sha256:9f86…' },
    { name: 'model', source: 'oci://registry.acme.io/models/embedder@sha256:abc…', mediaType: 'application/vnd.acme.model.weights.v1' },
  ],
  config: {
    command: ['${artifact.tool}/bin/embed', '--weights', '${artifact.model}/weights.bin'],
    env: { MODEL_DIR: '${artifact.model}' },
  },
}
```

Unknown `${artifact.…}` references are admission errors (§1), and the
interpolation is worker-side string substitution only — no expression
language, mirroring the workload-identity plan's "dumb `{field}` template"
philosophy.

### 6) Auth

Three doors, one resolution path. Per artifact:

```jsonnet
auth: { kv: 'secrets/ghcr-pull' }              // secret-based
auth: { identity: 'aws' }                      // task workload identity
auth: { identity: 'aws', assumeRole: 'arn:aws:iam::123:role/artifact-pull' }
auth: { nodeIdentity: 'artifact-pull' }        // node workload identity (new)
```

Cluster defaults (manager config) keep specs clean — most jobs should carry
no `auth` at all (S4):

```jsonnet
// manager.jsonnet
{
  artifacts: {
    allowedSchemes: ['https', 's3', 'oci', 'git', 'file'],
    auth: [
      // First match wins. Matchers are scheme + host + path globs.
      // Cluster-level entries may use kv or nodeIdentity — both are
      // cluster-scoped concepts. Task identities must be requested by the
      // task itself (the sub claim is a per-task security boundary).
      { match: 'oci://ghcr.io/acme/**', kv: 'secrets/ghcr-pull' },
      { match: 's3://acme-tooling/**',  nodeIdentity: 'artifact-pull' },
    ],
  },
}
```

Resolution order, evaluated on the worker at fetch time:
**task artifact `auth` → first matching cluster `artifacts.auth` entry →
anonymous.**

#### Secret-based (`kv`)

The worker reads the KV value over its existing authenticated manager
channel immediately before fetching (new `GetArtifactCredential` RPC —
deliberately *not* persisted into allocation rows, never logged, redacted
from traces per the AGENTS.md tracing rules). Credential values are JSON
objects shaped by `type`:

```jsonc
{ "type": "basic",  "username": "ci-pull", "password": "…" }          // http, oci
{ "type": "bearer", "token": "…" }                                     // http, oci
{ "type": "aws",    "accessKeyId": "…", "secretAccessKey": "…",
  "sessionToken": "…", "region": "us-east-1",
  "endpoint": "https://minio.internal:9000" }                          // s3
{ "type": "headers", "headers": { "X-Api-Key": "…" } }                 // http escape hatch
```

Because the read happens at fetch time, rotating a credential is just a KV
put — the next fetch uses the new value, no job touch required.

**KV ACL caveat (accepted for now, loudly documented).** There are no KV
ACLs yet: any job may point `auth.kv` at any KV path. This matches the
existing trust model for `$ref` jobspec resolution and is accepted for now,
with three obligations this plan carries: (1) the user-facing
`docs/artifacts.md` states it in a call-out box, (2) `src/artifacts/auth.rs`
carries the comment below at the credential-read site, (3) it is tracked in
`internal_docs/TODO.md` under the ACL System item so it cannot be missed when
ACLs land.

```rust
// SECURITY BOUNDARY (KV ACLs do not exist yet): any task spec may reference
// any KV path via auth.kv, so possession of job-submit access implies read
// access to every KV credential. Revisit when the ACL system lands — the
// artifact credential read below must become an early enforcement point.
// Tracked in internal_docs/TODO.md (ACL System).
```

#### Task workload identity (`identity`)

`auth.identity` names an entry in the task's `identities` list (the
workload-identity plan's spec API). At staging time the worker obtains that
identity's JWT — this **depends on WI plan Phases 2 and 4**
(`IssueWorkloadIdentityToken` RPC + worker delivery), which are not wired
yet — and uses it per scheme:

- **`oci` / `https`**: present the JWT as a bearer token. Registries and
  artifact stores that trust Legion's issuer can authorize on `sub` /
  `legion.io/*` claims — "it is Legion task X fetching" (S2).
- **`s3` (AWS)**: exchange via `AssumeRoleWithWebIdentity` using
  `auth.assumeRole`, then fetch with the temporary credentials, implemented
  as a custom `object_store` `CredentialProvider`:

```rust
/// object_store CredentialProvider that exchanges a Legion workload
/// identity token for STS credentials. The token comes from the worker's
/// identity manager, scoped to the requesting identity (task or node).
struct WebIdentityProvider {
    token_source: WorkloadTokenHandle, // refreshed by the WI delivery loop
    role_arn: String,
    sts_endpoint: Url,
}
```

#### Node workload identity (`nodeIdentity`) — extending the identity model

S3 needs the inverse grain: not "task X may read" but "**the cluster's
worker nodes** may read." This plan extends workload identity with
**node-scoped identities**, declared once at the cluster level:

```jsonnet
// manager.jsonnet
{
  workloadIdentity: {
    enabled: true,
    issuer: 'https://manager.example.com:4747',
    // ... existing fields ...

    // NEW: identities issued to worker nodes rather than tasks.
    nodeIdentities: [
      {
        name: 'artifact-pull',            // unique cluster-wide
        audience: ['sts.amazonaws.com'],  // non-empty, like task identities
        ttl: '15m',
        // Default sub keeps the same stability rule as task identities:
        // policy-friendly fields only. {node} (the churning node id) is
        // claims-only, exactly like {alloc} for tasks.
        subTemplate: '{region}:node:{pool}',
      },
    ],
  },
}
```

Token shape (same issuer, signer, JWKS, and rotation as task tokens):

| Claim | Value |
| --- | --- |
| `sub` | `prod:node:default` (stable: region + pool by default) |
| `legion.io/scope` | `"node"` (distinguishes from task tokens) |
| `legion.io/region`, `legion.io/pool` | discrete facts |
| `legion.io/node` | the node id — audit only, never in `sub` |

Issuance rides the worker's existing authenticated manager channel (the
same trust that lets it heartbeat and receive work): the WI plan's issuance
RPC gains a node mode, gated on the node's registration, no allocation
required. Workers refresh node tokens with the same delivery loop as task
tokens.

Two properties fall out of node identity that matter beyond this plan:

- **Cluster matchers can use it** (unlike task identities): "everything
  under `s3://acme-tooling/` is fetched as the cluster" is a sane
  cluster-wide default, where attaching a *task's* identity by default
  would blur the per-task security boundary.
- **The pre-warm plan needs it**: a warm fetch happens before any task
  exists, so node identity is the natural (and now already-defined)
  credential for pre-pulling. The companion plan consumes this section
  rather than inventing a prefetch token.

#### Security rules (all phases)

- Credentials and presigned URLs never appear in logs, traces, allocation
  rows, or error strings (errors carry the redacted source).
- Archive extraction rejects absolute paths, `..` traversal, and symlinks
  escaping the staging root; `maxArtifactSize` bounds decompression.
- `allowedSchemes` is enforced at admission *and* on the worker (defense in
  depth).

### 7) Tag and Ref Pinning at Submit (`resolve`)

Decided: **pin by default, with an explicit option to roll.**

- `resolve: 'pin'` (default for mutable sources — OCI tags, git refs): at
  submit, the manager calls the fetcher's `resolve()` to turn the reference
  into a digest/SHA and persists the *pinned* form in the normalized
  artifact list. Every allocation of that job config — including
  reschedules days later — fetches identical bytes, and `job plan` diffs
  show `tag v42: sha256:abc… → sha256:def…` when a tag moved between
  submits. Re-submitting the job re-resolves, which is exactly the "roll
  forward" moment a rollout wants.
- `resolve: 'always'`: the reference is re-resolved at every staging. This
  is the "roll with the tag" option — explicitly chosen, with the
  documented cost that allocations of one job config may run different
  bytes if the tag moves mid-rollout.
- Checksummed/digest sources are already immutable; `resolve` is ignored.

Resolution auth at submit: the manager resolves using the same auth chain
the worker would — it reads KV directly (it owns KV), and for
identity-authed sources it *mints the token itself* (the manager is the
issuer; the task is declared even though not yet placed). Where a flow
genuinely cannot run manager-side, admission fails with an actionable error
suggesting `resolve: 'always'`. Offline `job validate` skips resolution
(validation is static); `job plan` and `job run` perform it.

### 8) Task-Kind Consumption: the Streamlined Recipe

The goal: a new kind adopts artifacts in minutes, the way the container
kind does. Three pieces, all on the existing `TaskPlugin` trait (defaulted,
so existing plugins compile unchanged), plus a typed payload so consumption
is pattern-matching rather than path-guessing:

```rust
/// What staging produced, attached to TaskRuntime before prepare().
pub struct StagedArtifact {
    pub name: Option<String>,
    pub digest: String,
    pub payload: StagedPayload,
}

pub enum StagedPayload {
    /// A single file at `path` (mode applied). The common exec case.
    File { path: PathBuf },
    /// An unpacked tree rooted at `dir`.
    Tree { dir: PathBuf },
    /// An OCI image as an on-disk OCI layout. Image-consuming kinds turn
    /// this into whatever their runtime wants (unpacked rootfs bundle for
    /// youki, `container image load` on macOS).
    Image { oci_layout: PathBuf, reference: String },
    /// mediaType-selected ORAS layers (wasm modules, model files, …).
    Layers { files: Vec<PathBuf>, media_type: String },
}
```

```rust
pub trait TaskPlugin: Send + Sync {
    // … existing methods …

    /// Step 1 — capability gate, mirroring supports_kv_env_injection and
    /// friends. Admission rejects artifact-bearing specs for kinds that
    /// return false. File/Tree payloads then work with ZERO further code:
    /// the worker core stages them into the task dir and exposes
    /// ${artifact.<name>} / LEGION_ARTIFACT_<NAME>.
    fn supports_artifacts(&self) -> bool {
        false
    }

    /// Step 2 (optional) — desugar kind-config fields into artifacts. The
    /// manager calls this at admission; built-in kinds run in the manager
    /// binary. This is how `container.image` joins the shared pipeline
    /// without changing its user-facing spec.
    fn implicit_artifacts(&self, task_config: &Value) -> R<Vec<ArtifactSpec>> {
        let _ = task_config;
        Ok(Vec::new())
    }

    /// Step 3 (optional) — consume non-file payloads during prepare(). The
    /// default handles nothing because File/Tree need no consumption.
    fn consume_artifact(&self, staged: &StagedArtifact, runtime: &mut TaskRuntime) -> R<()> {
        let _ = (staged, runtime);
        Ok(())
    }
}
```

The container kind is the canonical worked example:

```rust
impl TaskPlugin for ContainerTaskPlugin {
    fn supports_artifacts(&self) -> bool { true }

    fn implicit_artifacts(&self, config: &Value) -> R<Vec<ArtifactSpec>> {
        let image = config.get("image").and_then(Value::as_str)
            .context("container task config requires image")?;
        Ok(vec![ArtifactSpec::oci_image(image)]) // name: "image", consume as Image
    }

    fn consume_artifact(&self, staged: &StagedArtifact, runtime: &mut TaskRuntime) -> R<()> {
        match &staged.payload {
            StagedPayload::Image { oci_layout, reference } => {
                // macOS: `container image load`; Linux: unpack rootfs bundle
                // for youki. Runtime-specific, ~30 lines each.
                self.load_image(oci_layout, reference, runtime)
            }
            _ => Ok(()), // file artifacts deferred until volume plumbing exists
        }
    }
}
```

And a future `wasm` kind is the proof of streamlining — full adoption in
roughly fifteen lines:

```rust
impl TaskPlugin for WasmTaskPlugin {
    fn supports_artifacts(&self) -> bool { true }

    fn implicit_artifacts(&self, config: &Value) -> R<Vec<ArtifactSpec>> {
        // wasm config: { module: 'oci://registry/app@sha256:…' }
        let module = config.get("module").and_then(Value::as_str)
            .context("wasm task config requires module")?;
        Ok(vec![ArtifactSpec::oras(module, "application/wasm").named("module")])
    }

    fn consume_artifact(&self, staged: &StagedArtifact, runtime: &mut TaskRuntime) -> R<()> {
        if let StagedPayload::Layers { files, .. } = &staged.payload {
            runtime.set_state("module_path", files[0].display().to_string());
        }
        Ok(())
    }
}
```

Capability matrix at the end of this plan's phases:

| Kind | artifacts | consumption |
| --- | --- | --- |
| `exec` | ✅ | File/Tree in task dir + `${artifact.*}` interpolation |
| `container` | ✅ | implicit `image` → `Image` payload; file artifacts deferred until the kind grows volume plumbing (same limitation as its KV file injection) |
| `noop` | ❌ | rejected at admission |
| future `wasm` | ✅ | ORAS `Layers` via `mediaType` — S5 |

### 9) Artifacts in the UI and Status Surfaces

Artifacts must be visible wherever tasks and allocations are (S6). The UI
is the SolidJS app talking gRPC-Web (`docs/ui-architecture.md`); clients
regenerate from `proto/legion.proto` into `ui/src/gen/`.

**Data plumbing.** The worker reports per-artifact state inside the
existing heartbeat: `AllocationHealth` gains an optional
`artifact_states` JSON field —

```jsonc
[
  { "name": "tool",  "source": "https://releases.acme.io/…(redacted)",
    "state": "staged",   "digest": "sha256:9f86…", "bytes": 8123394,
    "duration_ms": 412 },
  { "name": "model", "source": "oci://registry.acme.io/models/embedder@sha256:abc…",
    "state": "fetching", "bytes_fetched": 1610612736, "bytes_total": 4294967296 },
  { "name": "cfg",   "source": "s3://acme-builds/…",
    "state": "failed",   "attempts": 3, "error": "403 from endpoint (credentials from kv secrets/build-store)" }
]
```

— which the manager persists on the allocation and serves through the same
RPCs the UI already polls. States: `pending` → `fetching` → `verifying` →
`staged` | `failed`. Sources are redacted (no userinfo/query secrets);
errors name the *auth source* (KV path, identity name), never values.

**UI surfaces:**

- **Jobs page — task spec view:** a declared-artifacts table per task:
  name, source, checksum or pinned digest (with the tag it resolved from,
  §7), resolve mode, auth kind (`kv: secrets/ghcr-pull` /
  `identity: aws` / `nodeIdentity: artifact-pull` / `cluster default` /
  `anonymous`), dest.
- **Allocation views (Nodes page summaries + job status drill-down):**
  while staging, the allocation badge shows `staging artifacts` with the
  per-artifact rows above (state, progress bytes, duration, failure
  reason). After start, the rows collapse into a provenance list: what was
  staged, at which digest.
- **CLI parity:** `legion job status -j` and the ledger output include the
  same `artifact_states`, so agents and humans get identical visibility
  (per AGENTS.md structured-output rules).

The companion plan extends these same surfaces with cache hit/miss and
warm-state indicators; nothing here is throwaway.

### 10) Observability and Agent Surfaces

Per AGENTS.md obligations:

- Any new CLI verbs take `--json`, emit the standard envelope, and register
  in `catalog()` (`src/cmd/capabilities.rs`) with correct
  `mutates`/`read_only_safe` flags.
- Tracing: `#[instrument]` on the staging entrypoint and fetcher calls with
  `alloc_id`, `task_name`, redacted `source`, `bytes`, `duration`; no
  credential material in spans, ever.
- Metrics: per-worker fetch counts, failures by scheme, checksum failures,
  retry counts, staging durations.
- Docs: this file, a user-facing `docs/artifacts.md` (including the KV ACL
  call-out and the on_unhealthy fetch-failure semantics), updates to
  `docs/task-kind-*.md` capability tables, `docs/workload-identity.md`
  (node identities), `docs/agent-readiness.md`, and every new jobspec field
  exercised in `configs/jobs/maximal.jsonnet` (repo rule).

## Proto, Struct, and Config Changes

Proto (`proto/legion.proto`, all documented per repo convention):

- `TaskAllocation` += `task_artifacts` (JSON string: normalized + pinned
  `Vec<ArtifactSpec>`, *post* admission desugaring — workers never
  re-derive implicit artifacts or re-resolve pinned tags).
- `AllocationHealth` += optional `artifact_states` (JSON string, §9).
- New manager RPC: `GetArtifactCredential` (worker-auth'd KV credential
  read).
- WI issuance RPC (from the WI plan) gains a node-identity mode (§6).

Structs:

- `src/structs/job.rs`: `TaskConfig.artifacts: Vec<ArtifactSpec>` with
  string-shorthand deserialization + admission validation (names,
  interpolation references, auth one-of, resolve rules).
- `src/structs/config.rs`: `ManagerConfigFull.artifacts`
  (`allowedSchemes`, `auth` matchers);
  `WorkloadIdentityConfigFull.node_identities`; worker config +=
  `artifacts.fetch`.
- `src/task_plugins/mod.rs`: `supports_artifacts` / `implicit_artifacts` /
  `consume_artifact`; `TaskRuntime.staged_artifacts: Vec<StagedArtifact>`.
- `src/artifacts/` module as laid out in §3; `src/getter_rs` folds into it
  (CLI jobspec fetching migrates to the same registry).

New dependencies: `tar` + `flate2` (+ `zstd`, `zip` as warranted) for
unpacking; `oci-client` for the OCI fetcher. `object_store`, `sha2`,
`reqwest`, `rusqlite`, `humantime` are already in tree.

## Implementation Phases

Each phase is independently shippable and states its win. None of them
depend on the cache/pre-warm plan.

### Phase 0 — Allocation/task directories
Create/destroy task dirs, `LEGION_TASK_DIR`/`LEGION_ALLOC_DIR`, exec `cwd`
default, landlock auto-grant.
**Win:** every task gets a private, cleaned-up workdir — a standalone
quality fix (no more tasks dumping files into the worker's cwd) and the
landing zone everything else needs.

### Phase 1 — `artifacts` spec + staging pipeline (https/file/git) + referencing + visibility
Spec + shorthand + admission validation; staging with retries, checksums,
safe unpack; `name` + `${artifact.<name>}` interpolation +
`LEGION_ARTIFACT_*` env; per-artifact `artifact_states` through heartbeat
to CLI/UI (declared-artifacts table + allocation staging rows);
`supports_artifacts` gating; getter_rs unification.
**Win:** S1 and S6 end-to-end — exec tasks become deployable from release
URLs with one spec line, reference their inputs without hardcoded paths,
bad specs fail at `job run`, and a stuck deploy is diagnosable from the UI.
The single biggest day-one UX unlock.

### Phase 2 — Secret auth via KV + cluster matchers
`auth.kv`, credential JSON shapes, `GetArtifactCredential` RPC, cluster
`artifacts.auth` matchers, redaction discipline, KV ACL caveat docs +
code comment + TODO callout.
**Win:** S4's auth model — private artifacts with zero secrets in job
specs and centralized, instant rotation. (Depends on 1.)

### Phase 3 — `s3://` fetcher
`object_store`-backed, endpoint/region via credential or query params.
**Win:** S2's storage path for AWS/MinIO/R2 — small phase because the
dependency already ships in the binary. (Depends on 1; pairs with 2.)

### Phase 4 — `oci://` fetcher + pinning + container desugaring
`oci-client`-based fetcher; `resolve: 'pin'` at submit for OCI tags and git
refs (+ `job plan` digest diffs); ORAS `mediaType` → `Layers` payloads;
container `implicit_artifacts`/`consume_artifact` behind the
`artifacts.containerImageDesugar` cluster flag (see Judgement Calls);
UI provenance (tag → digest).
**Win:** S4 and S5 — private registries through the same auth pipeline,
deterministic rollouts via pinning with an explicit option to roll, and one
fetch path serving container images *and* exec/wasm payloads: the reuse
this whole design exists for. (Depends on 1, 2.)

### Phase 5 — Workload-identity auth (task + node)
`auth.identity` (+ `assumeRole`) and `auth.nodeIdentity`; node-identity
config, token shape, and issuance mode; bearer presentation for oci/https;
STS `CredentialProvider` for s3; cluster matchers accept `nodeIdentity`.
**Win:** S2 and S3 go secretless — store policies keyed on *which task* or
*whose worker nodes* are fetching. Node identity also hands the pre-warm
plan its auth story for free. (Depends on 1; **and on WI plan Phases 2 &
4** — token issuance + worker delivery. If those slip, every other phase
still ships.)

### Phase 6 — Fetcher plugins
`plugins/artifacts/<scheme>/` dylibs via the existing plugin-loading
pattern; registry resolution order: built-ins → dynamic plugins.
**Win:** in-house artifact stores and exotic protocols without forking
core — extensibility parity with task-kind plugins, completing the
`plugin-system.md` "Artifact acquisition plugins" promise. (Depends on 1.)

## Testing Plan

### Unit

- Spec parsing: string shorthand ↔ full form, camelCase fields, rejection
  matrix (absolute/`..` dest, bad checksum format, two-of-three auth keys,
  unknown identity / nodeIdentity name, disallowed scheme, duplicate
  artifact names, `${artifact.x}` referencing an undeclared name).
- Interpolation: token expansion in exec command/env/cwd; container env;
  no expansion inside non-string config values.
- Pinning: `resolve` defaulting (mutable vs checksummed sources), pinned
  form persisted, `resolve: 'always'` passes the raw reference through.
- Auth: matcher precedence (task > cluster > anonymous),
  first-match-wins globs, credential JSON shape validation,
  cluster-matcher rejection of task `identity`, redaction (assert no
  secret bytes in rendered errors/spans).
- Node identity: sub template allowlist (`{region}`/`{pool}` ok, `{node}`
  rejected), claims include `legion.io/scope: node` and `legion.io/node`.
- Unpack: traversal/symlink-escape rejection, size-limit enforcement, mode
  application.

### Integration

- Full lifecycle on a local HTTP server fixture: submit → stage → exec runs
  from task dir referencing `${artifact.tool}` → destroy cleans dir.
- Fetch failure (404 / checksum mismatch) after retries → allocation
  failure heartbeats → manager reschedule path engages (existing
  `on_unhealthy` flow) — pinning the documented failure semantics.
- `artifact_states` flow: heartbeat carries per-artifact states; ledger /
  job status JSON includes them; states progress pending → staged.
- `s3://` against a MinIO container (gated like other env-dependent tests);
  `oci://` against a local registry fixture, including tag → digest
  pinning at submit and a `job plan` diff when the tag moves.
- Node-identity issuance over the worker channel (with WI issuance in
  place), and an artifact fetch authenticating with the node token against
  a fixture that validates `sub`/`legion.io/scope`.

### Regression

- Jobs without `artifacts` are byte-identical through admission and
  dispatch (no proto field set, no staging step entered).
- `noop` + artifacts rejected at admission with an actionable error.
- Container kind behavior unchanged while `containerImageDesugar` is off.
- Existing getter_rs CLI jobspec-fetch behavior preserved through the
  registry migration (`job run https://…` keeps working).
- `configs/jobs/maximal.jsonnet` extended and still validates.

## Rollout & Backward Compatibility

- Everything is additive. No `artifacts` field → no staging step, no new
  RPC traffic, no UI changes beyond empty sections.
- Default scheme allowlist is permissive minus `http` (plain-HTTP requires
  explicit opt-in; `https` everywhere else).
- Container `image` desugaring ships behind a cluster flag
  (`artifacts.containerImageDesugar`, default **off** until the cache plan
  lands) — without a cache, Legion-managed image pulls would re-download
  per allocation and regress runtimes that cache images themselves. The
  explicit-artifacts path has no such regression (there was no prior
  behavior).
- Workers without `artifacts.fetch` config get conservative defaults.
- Node identities are opt-in config under the already-off-by-default
  `workloadIdentity` section.

## Judgement Calls

Split out into
[`calls/artifact-system-plan_calls.md`](../calls/artifact-system-plan_calls.md)
— eleven decisions, ranked by project impact (first-class task field,
pin-by-default, the KV ACL gap, cluster auth grain, cache deferred, container
`image` desugaring, blocking fetcher trait, all-or-nothing staging,
fetch-failure budgets, dumb interpolation, `oci-client`).

## Follow-Ups (noted, not scoped here)

- **Windows staging**: hardlink/copy fallbacks, path semantics, and
  image consumption on Windows need a dedicated pass when Windows workers
  become a priority. Tracked in `internal_docs/TODO.md` (Windows support) so
  it is not missed.
- **`onFetchFailure` policy** distinct from `on_unhealthy` (Judgement Call
  9, *Fetch failures reuse `on_unhealthy` budgets*) if registry/store
  outages prove to deserve different budgets than task unhealthiness.
- **KV ACL enforcement** at `GetArtifactCredential` when the ACL system
  lands (Judgement Call 3, *KV ACL gap accepted for now*; TODO).
- The **cache and pre-warm plan** consumes this plan's seams:
  `ArtifactSink`, digests, `artifact_states`, and node identity.

## Open Questions

1. **Interpolation scope.** `${artifact.<name>}` starts in exec
   (command/env/cwd) and container (command/env). Should it generalize to
   *all* string fields of any kind's config automatically, or stay an
   explicit per-kind capability? Lean: per-kind capability — automatic
   deep-substitution into unknown plugin configs risks corrupting fields
   that legitimately contain `${…}`.
2. **Manager-side resolution edge cases.** `resolve: 'pin'` requires the
   manager to reach registries at submit. For air-gapped managers or
   exchange-only auth flows that genuinely cannot run manager-side, is
   "fail submit, suggest `resolve: 'always'`" acceptable, or is a
   worker-delegated resolution step warranted?
3. **Container file artifacts.** `Image` consumption lands in Phase 4, but
   file/tree artifacts into containers wait on the kind's volume plumbing
   (same gap as its KV file injection). Does that plumbing belong to the
   container kind's roadmap or to a volumes effort?

## Acceptance Criteria

- **Phase 0:** every allocation runs with a private task dir, env vars set,
  dir removed on destroy; exec `cwd` defaults there.
- **Phase 1:** S1 works end-to-end from a jobspec one-liner with
  `${artifact.tool}` referencing; checksum mismatch fails at staging with a
  clear reason visible in `job status` and the UI's per-artifact rows;
  invalid artifact specs (including bad interpolation references) fail
  `job validate`.
- **Phase 2:** S4's credential flow works with specs containing no secret
  material; rotating the KV value affects the next fetch with no job edit;
  no secret bytes appear in logs/traces under test assertion; the KV ACL
  caveat appears in docs, code comment, and TODO.
- **Phase 3:** a MinIO-backed `s3://` artifact stages with endpoint/region
  from credential config.
- **Phase 4:** a private-registry image task (flag on) and an ORAS
  `mediaType` exec task both stage through the same pipeline; submitting a
  job with a moved tag shows a digest diff in `job plan`;
  `resolve: 'always'` tracks the tag.
- **Phase 5:** an `auth.identity` fetch succeeds against a store trusting
  Legion's issuer with policy keyed on task `sub`; an `auth.nodeIdentity`
  fetch succeeds with policy keyed on `<region>:node:<pool>`; cluster
  matchers can route a prefix to a node identity.
- **Phase 6:** a sample out-of-tree fetcher dylib resolves a custom scheme
  end-to-end.
