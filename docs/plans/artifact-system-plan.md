# Plan: Artifact Fetching, Caching, and Pre-Warming

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
  S3 keys) nor identity-based ("this is Legion task `api/web/server`
  fetching"),
- no caching — every allocation start would re-download, which makes
  restarts slow, scale-ups slower, and disk usage unbounded,
- no pre-warming — a 50-node scale-up means 50 simultaneous cold pulls at
  the worst possible moment,
- no per-allocation directory, so there is not even a well-defined place to
  *put* a downloaded artifact.

This plan defines one artifact system shared by every task kind: a small
declaration in the job spec, a fetcher registry keyed by URL scheme, a
content-addressed per-node cache with a size budget and TTLs, auth via
KV-stored secrets or workload identity, and an optional pre-pull layer that
warms nodes ahead of placement and lets the scheduler prefer warm nodes on
the next pass.

`docs/TASK-LIST.md` ("Artifact system"), `docs/differentiators.md`
("Pre-fetching artifacts in deploy process", "Artifact cache generally?"),
and `docs/plugin-system.md` ("Artifact acquisition plugins") all anticipate
this work; this plan gives it a concrete shape.

## User Scenarios

These six scenarios are the acceptance lens for the whole design. Each later
section and phase calls back to them.

**S1 — Release tarball (public URL).** A batch task downloads
`https://github.com/acme/tool/releases/download/v1.4/tool-linux-amd64.tar.gz`,
verifies a sha256, unpacks it into its task directory, and runs the binary.
One line in the spec. No auth.

**S2 — Private build from S3-compatible storage, no secrets in spec.** A
service task fetches `s3://acme-builds/api/v42.tar.gz` from MinIO/R2/AWS.
The task declares a workload identity; the worker presents the identity
token (exchanging it for provider credentials where needed) so the bucket
policy can say "only `prod:api:web:server` may read this prefix." The job
spec contains zero credentials.

**S3 — Private container image with centrally managed creds.** A container
task uses `image: 'ghcr.io/acme/api:v42'`. Registry credentials live in
Legion KV at `secrets/ghcr-pull`; a cluster-level auth matcher maps
`oci://ghcr.io/acme/**` to that KV path. The job spec says nothing about
auth at all, and rotating the credential is a single `legion kv put`.

**S4 — ORAS artifact (model weights / WASM) shared across tasks.** Three
different tasks on the same node reference
`oci://registry.acme.io/models/embedder@sha256:abc...`. The 4 GiB artifact
is downloaded once into the node cache and hardlinked into each task
directory. A digest-pinned artifact never re-downloads.

**S5 — Scale-up without a pull storm.** The autoscaler adds 10 nodes. A
warm-on-join policy pre-pulls the hot artifact set as each node registers;
each worker advertises its cache contents; the next scheduling pass scores
warm nodes higher, so new allocations start in seconds instead of minutes —
and pre-warm failures never block placement, because pre-pull is only ever
an optimization.

**S6 — Edge node with a small disk.** A worker is configured with a 5 GiB
artifact cache. LRU eviction keeps it under budget, per-entry TTLs bound
staleness for mutable references, and artifacts pinned by running
allocations are never evicted. The cache also refuses to fetch when it
would push the node below a configured free-disk floor.

## Goals

- One `artifacts` declaration shared by every task kind — current and
  future — with the fetch/verify/cache/stage pipeline implemented once in
  the worker core, not per plugin.
- Sources: raw URLs (`https://`), local files (`file://`), git, S3-compatible
  object storage (`s3://` — AWS, MinIO, R2, GCS-interop), and OCI
  (`oci://` — both container images and ORAS artifacts).
- Auth two ways: secrets stored in Legion KV and resolved at fetch time on
  the worker, and workload-identity tokens proving *which* task (or that
  Legion itself) is fetching. Cluster-level defaults so most specs carry no
  auth block at all.
- A per-node content-addressed cache with a configurable size budget,
  per-entry TTLs, LRU eviction, pinning for in-use artifacts, and
  single-flight dedup of concurrent fetches.
- Optional pre-pull/warming: an RPC + CLI to push artifacts to selected
  nodes, warm-on-join policies, cache advertisement to the manager, and a
  soft scheduler scoring term so warm nodes win ties on the next pass.
- Keep the simple case simple: `artifacts: ['https://.../app.tar.gz']` is a
  valid, complete declaration.
- Validation at admission: bad schemes, malformed checksums, unknown
  identity references, and kind/capability mismatches fail at `job run`,
  not on a node twenty minutes later.

## Non-Goals

- **Not a build system.** Legion fetches artifacts; producing them (docker
  build, buildpacks) is out of scope (see `differentiators.md` "build &
  push" ideas — separate effort).
- **Not an artifact registry.** Legion does not host artifacts; it pulls
  from external stores. (A first-party registry is a someday idea in
  `old-notes.md`, explicitly not this plan.)
- **Not volumes.** Artifacts are immutable inputs staged before start;
  mutable shared state is the volume/stateful-workloads effort
  (`docs/plans/stateful-workloads-and-instance-identity-plan.md`).
- **No KV ACLs.** Which tasks may reference which KV secret paths is the
  ACL effort flagged in the workload-identity plan; this plan inherits
  today's trust model (any job may reference any KV path) and calls it out.
- **No peer-to-peer distribution.** Workers fetch from origin stores only.
  Node-to-node artifact sharing (torrent-style, registry mirror on
  managers) is future work the cache layout deliberately leaves room for.
- **No live re-fetch of changed artifacts under a running task.** Artifacts
  are resolved at staging time. Reacting to a moved tag mid-flight is rollout
  territory, not artifact territory.

## Background: What Exists Today

Facts about the current codebase that shape the design:

- **`src/getter_rs`** — a go-getter-style `Getter` trait with `file`,
  `http`, and `git` implementations (blocking, no auth, no checksums, no
  cache). Used only by `src/cmd/job.rs::resolve_jobspec_source` to fetch
  jobspecs for `job run/validate/plan`. This becomes the seed of the
  fetcher registry.
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
- **Heartbeat is a poll** (`HeartbeatRequest` carries allocation IDs,
  failures, health; the reply carries desired allocations) and new work is
  *pushed* via the dispatcher's `PostWork` RPC to the worker's gRPC server.
  So the manager already knows how to address an individual worker — the
  pre-pull RPC reuses that path.
- **Node attributes**: constraints/affinities read `config_attrs` →
  `fingerprint_attrs` → `dynamic_attrs`. `UpdateNodeDynamic` shallow-merges
  a diff into `dynamic_attrs` and enqueues `job_update` evals for jobs on
  that node. Useful precedent — but the auto-eval behavior is wrong for
  cache advertisement (see Judgement Calls), so advertisement gets its own
  RPC.
- **Workload identity** (`docs/workload-identity.md`): the token model,
  ES256 signer, JWKS, per-task `identities` spec, and admission validation
  exist today. The `IssueWorkloadIdentityToken` RPC and worker-side
  delivery are later phases of that plan — the identity-auth phase here
  depends on them and says so explicitly.
- **KV** (`docs/kv-store.md`): server-side CRUD with versioning; job submit
  resolves `$path/key` references; per-task `kv_injections` exist in the
  spec but worker-side rendering is not implemented yet. There is no
  separate "secrets" type — KV values are the secret store.
- **Resources**: `resources.disk` is an optional number normalized by
  `ResourcePolicy`; the kitchen-sink fingerprinter reports per-disk
  total/available space; `DiskChecker` flags low-disk nodes.

## Proposed Design

### 0) Design at a Glance

```
        job spec                     manager                          worker
  ┌──────────────────┐   ┌───────────────────────────┐   ┌──────────────────────────────┐
  │ tasks: [{        │   │ admission:                │   │ stage_artifacts(alloc, task) │
  │   kind: 'exec',  │──▶│  - parse/validate specs   │──▶│  1. resolve auth (KV / WI)   │
  │   artifacts: [   │   │  - desugar container      │   │  2. cache.ensure(spec)       │
  │     's3://…',    │   │    image -> oci:// spec   │   │     └─ fetcher by scheme     │
  │   ],             │   │  - normalize + persist    │   │        (single-flight, CAS)  │
  │ }]               │   │                           │   │  3. stage into task dir      │
  └──────────────────┘   │ pre-pull (optional):      │   │     (reflink/hardlink/copy)  │
                         │  warm evals ──PullArtifacts──▶│  4. pin until destroy        │
                         │  scoring: prefer nodes    │   │                              │
                         │  advertising the digests ◀──ReportArtifactCache──┘          │
                         └───────────────────────────┘   └──────────────────────────────┘
```

Five principles keep the moving parts honest:

1. **Fetching lives in the worker core, not in task plugins.** Plugins
   *consume* staged artifacts (and may *contribute* implicit ones, like the
   container image); they never download. This is the reuse-across-kinds
   guarantee.
2. **The content-addressed cache is the only coordination point.** Task
   staging, pre-pull, and scale-up warming all just put bytes in the same
   cache. Pre-pull is *only* a hint: if warming never ran, staging fetches
   cold and everything still works. No new consistency machinery.
3. **Declarations stay one line.** A bare source string is a full artifact
   spec; auth defaults come from cluster config; `image:` keeps working and
   desugars into the same pipeline.
4. **Auth is resolved late, on the worker, at fetch time.** Credentials
   never ride through allocation rows or the scheduler; KV reads and
   identity-token issuance happen over the worker's existing authenticated
   channel just before the fetch.
5. **Checksummed artifacts are immutable; everything else has a TTL.** A
   `sha256:` checksum (or OCI digest) is the cache key — cache forever,
   share freely, pre-pull safely. Mutable references (plain URLs, `:latest`
   tags) get by-source cache entries revalidated after a TTL.

### 1) Job-Spec API

`artifacts` is a new first-class task field, sitting beside `env`, `files`,
`kv`, and `identities` — *not* inside the kind-specific `config`. That is
what makes it shared across kinds and uniformly validatable.

The minimal form (S1):

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

    // Destination, relative to the task directory. Absolute paths rejected.
    // Default: '.' for archives/trees, source basename for single files.
    dest: 'build/',

    // 'sha256:<hex>'. Presence makes the artifact immutable: it becomes the
    // cache key, verification is mandatory, and TTLs no longer apply.
    checksum: 'sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',

    // 'auto' (default; unpacks recognized archive extensions), 'archive'
    // (force), or 'file' (never unpack).
    unpack: 'auto',

    // How long a cached copy of a *mutable* source may be used without
    // revalidation. Ignored when checksum is set. Default: worker cache's
    // defaultTtl.
    ttl: '24h',

    // Auth override. Omitted => cluster artifactAuth matchers apply.
    // Exactly one of kv / identity.
    auth: { kv: 'secrets/build-store' },

    // chmod for single-file artifacts (handy for bare binaries).
    mode: '0755',
  },
]
```

OCI sources support digest pinning and ORAS selection (S4):

```jsonnet
artifacts: [
  {
    source: 'oci://registry.acme.io/models/embedder@sha256:abc123...',
    // For ORAS multi-file artifacts: pick layers by media type. Omit to
    // take the whole artifact.
    mediaType: 'application/vnd.acme.model.weights.v1',
    dest: 'models/',
  },
]
```

Container tasks keep their existing surface; `image` is *desugared by the
manager at admission* into an implicit artifact (S3):

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

#### Validation rules (admission, in `job validate` / `job plan` too)

- `source` must parse and use a scheme in the cluster's `allowedSchemes`.
- `checksum` must be `sha256:<64 hex>`; OCI `@digest` sources are treated
  as checksummed.
- `dest` must be relative and may not escape the task directory (`..`
  rejected after normalization).
- `auth` must set exactly one of `kv` / `identity`; `identity` must name an
  entry in the task's `identities` list.
- The task kind must report `supports_artifacts()` (capability gating,
  mirroring KV/identity injection). `noop` rejects artifacts.
- Per-artifact `ttl` parses via humantime, like every other duration.
- Warm policies (§7) may not reference task-identity auth — see the
  pre-pull/identity interplay note there.

### 2) Allocation and Task Directories (prerequisite)

Artifacts need somewhere to land, and today nothing provisions one. Phase 0
introduces:

```
<worker_state_dir>/<worker_id>/
  worker_state.json                  # existing
  allocs/<alloc_id>/<task_name>/     # new: the task directory
  artifacts/                         # new: the cache (§4)
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
  mod.rs        # ArtifactSpec, resolution, public API
  fetcher.rs    # ArtifactFetcher trait + registry
  fetchers/     # http.rs, file.rs, git.rs, s3.rs, oci.rs
  store.rs      # the cache (§4)
  auth.rs       # credential resolution (§5)
  stage.rs      # link/copy/unpack into task dirs
```

The trait (blocking, like the existing getters — see Judgement Calls):

```rust
/// One fetcher per scheme. Fetchers stream bytes into the cache's staging
/// sink; they never see task directories and never receive raw KV paths —
/// only already-resolved credentials.
pub trait ArtifactFetcher: Send + Sync {
    fn schemes(&self) -> &'static [&'static str];

    /// Cheaply resolve a mutable reference to a stable identity without
    /// downloading: HTTP HEAD + ETag, OCI tag -> manifest digest, git
    /// ls-remote. Returns None when the source has no cheap probe; the
    /// cache then falls back to TTL-only revalidation.
    fn probe(&self, req: &FetchRequest<'_>) -> R<Option<RemoteIdentity>> {
        let _ = req;
        Ok(None)
    }

    /// Download the artifact into `sink`. The sink hashes as it writes, so
    /// checksum verification and content addressing share one pass.
    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut CasSink) -> R<FetchedArtifact>;
}

pub struct FetchRequest<'a> {
    pub source: &'a Url,
    pub checksum: Option<&'a Checksum>,
    pub media_type: Option<&'a str>,
    pub credential: Option<&'a Credential>,
    pub limits: &'a FetchLimits, // max bytes, deadline
}
```

Per-scheme notes:

- **`http` / `https`** — extends the existing `HttpGetter` with streaming
  (no more whole-body buffering), bearer/basic/header credentials,
  ETag/Last-Modified revalidation in `probe`, and redirect + size limits.
- **`file`** — existing behavior; copies (or links, same filesystem) into
  the cache. Useful for dev and for node-local pre-provisioned artifacts.
- **`git`** — existing shallow-clone behavior, plus `ref=` query support
  (`git::https://github.com/acme/repo?ref=v1.2.0`) and `probe` via
  `ls-remote` so a pinned ref caches like an immutable artifact.
- **`s3`** — new, thin: `object_store`'s `AmazonS3Builder` already in the
  dependency tree speaks any S3-compatible endpoint. Region/endpoint come
  from the credential object or query params
  (`s3://bucket/key?endpoint=https://minio.internal:9000&region=auto`),
  query params winning. Conditional GET via ETag for `probe`.
- **`oci`** — new, the largest fetcher: token-dance auth (anonymous,
  basic, bearer, and OIDC token exchange), manifest resolution
  (index → platform manifest), per-*blob* CAS storage so image layers
  dedupe across images and tags, and two materializations: an OCI layout
  tree for image consumers (container plugin) and selected-layer extraction
  for ORAS consumers (`mediaType`). Implemented against the distribution
  spec with `reqwest` (or `oci-client` if review prefers a vetted dep —
  flagged in Open Questions).

Dynamic fetcher plugins (`plugins/artifacts/<scheme>/`, `cdylib` exporting
`get() -> Box<dyn ArtifactFetcher>`, mirroring task-kind plugins) are the
final phase — the trait above is the contract from day one so nothing needs
reshaping later.

### 4) The Artifact Cache

Per-worker, content-addressed, with a sqlite index (rusqlite is already
bundled) for crash-safe refcounts and LRU bookkeeping:

```
<worker_state_dir>/<worker_id>/artifacts/
  index.sqlite          # entries, pins, last_use, sizes, ttl deadlines
  cas/sha256/ab/abcd…/  # immutable blobs and unpacked trees, named by digest
  tmp/                  # in-flight downloads; same fs => atomic rename in
```

Two kinds of cache key:

| Source kind | Key | TTL behavior |
| --- | --- | --- |
| Checksummed / digest-pinned | the digest itself | none — immutable; only LRU/size pressure can remove it |
| Mutable (plain URL, `:latest`, branch ref) | hash of (canonical source, mediaType) → points at a digest | after `ttl`, next use revalidates via `probe` (cheap) or refetches; the old digest stays until evicted |

Worker configuration (delivered through the existing worker-config
mechanism — `configs/workers/*.jsonnet`):

```jsonnet
{
  workerType: 'default',
  pool: 'default',
  artifactCache: {
    maxSize: '20GiB',        // budget for cas/ (S6)
    defaultTtl: '72h',       // mutable-source revalidation window
    minFreeDisk: '5GiB',     // refuse fetches that would cross this floor
    advertise: true,         // §7: report cached digests to the manager
    advertiseLimit: 256,     // cap on advertised digests
    fetch: {
      parallelism: 4,        // concurrent fetches per worker
      attempts: 3,           // per-artifact retries before alloc failure
      timeout: '10m',
      maxArtifactSize: '50GiB',
    },
  },
}
```

Store API (worker-internal):

```rust
impl ArtifactStore {
    /// Fetch-or-hit. Single-flight per cache key: concurrent allocations
    /// wanting the same artifact share one download. Returns a pin held on
    /// behalf of `owner` (an alloc id, or the synthetic prefetch owner).
    pub fn ensure(
        &self,
        artifact: &ResolvedArtifact,
        credential: Option<Credential>,
        owner: PinOwner,
    ) -> R<PinnedArtifact>;

    /// Materialize into a task directory: reflink, else hardlink, else
    /// copy (cross-filesystem / Windows fallback). Applies `mode`, unpacks
    /// per spec with path-traversal protection.
    pub fn stage(&self, pin: &PinnedArtifact, task_dir: &Path, spec: &ArtifactSpec) -> R<()>;

    /// Drop all pins owned by an allocation (called from destroy/failure
    /// paths). Unpinned entries become eviction candidates, not deletions.
    pub fn release(&self, owner: PinOwner);

    /// Enforce budget + TTLs: LRU over unpinned entries until under
    /// maxSize and above minFreeDisk. Runs on an interval and before any
    /// fetch that would exceed budget.
    pub fn gc(&self) -> R<GcReport>;

    /// Most-recently-used digests for advertisement (§7).
    pub fn advertised_digests(&self, cap: usize) -> Vec<String>;
}
```

Rules that make S4 and S6 hold:

- **Pinning**: every allocation that staged an artifact holds a pin until
  `destroy`. GC never touches pinned entries. If the pinned set alone
  exceeds the budget, fetches for *new* allocations fail with an explicit
  "cache budget exhausted by running allocations" error (which routes
  through normal allocation failure → reschedule elsewhere — correct
  behavior on a full node).
- **Single-flight**: a per-key in-process lock; the second requester waits
  on the first download rather than duplicating it.
- **Atomicity**: downloads land in `tmp/` and rename into `cas/` only after
  digest verification. A checksum mismatch deletes the temp file and fails
  the fetch (counted in metrics) — corrupt bytes never enter the cache.
- **Disk accounting**: staged copies are reflinks/hardlinks, so an artifact
  costs disk once per node regardless of how many tasks use it; the cache
  budget is the guardrail. `resources.disk` continues to describe the
  task's own writable footprint (see Judgement Calls for the boundary).

### 5) Auth

Two doors, one resolution path. Per artifact:

```jsonnet
auth: { kv: 'secrets/ghcr-pull' }          // secret-based
auth: { identity: 'aws' }                  // workload-identity-based
auth: { identity: 'aws', assumeRole: 'arn:aws:iam::123:role/artifact-pull' }
```

Cluster defaults (manager config) keep specs clean — most jobs should carry
no `auth` at all (S3):

```jsonnet
// manager.jsonnet
{
  artifacts: {
    allowedSchemes: ['https', 's3', 'oci', 'git', 'file'],
    auth: [
      // First match wins. Matchers are scheme + host + path globs.
      { match: 'oci://ghcr.io/acme/**', kv: 'secrets/ghcr-pull' },
      { match: 's3://acme-builds/**',   kv: 'secrets/build-store' },
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

#### Identity-based (`identity`)

`auth.identity` names an entry in the task's `identities` list (the
workload-identity plan's spec API). At staging time the worker obtains that
identity's JWT — this **depends on WI plan Phases 2 and 4**
(`IssueWorkloadIdentityToken` RPC + worker delivery), which are not wired
yet — and uses it per scheme:

- **`oci` / `https`**: present the JWT as a bearer token. Registries and
  artifact stores that trust Legion's issuer (the JWKS/discovery documents
  the WI plan publishes) can authorize on `sub` / `legion.io/*` claims —
  this is the "it knows it is Legion task X fetching" property (S2).
- **`s3` (AWS)**: exchange via `AssumeRoleWithWebIdentity` using
  `auth.assumeRole`, then fetch with the temporary credentials. Implemented
  as a custom `object_store` `CredentialProvider`:

```rust
/// object_store CredentialProvider that exchanges a Legion workload
/// identity token for STS credentials. The token comes from the worker's
/// identity manager, scoped to this task's identity entry.
struct WebIdentityProvider {
    token_source: WorkloadTokenHandle, // refreshed by the WI delivery loop
    role_arn: String,
    sts_endpoint: Url,
}
```

Sequencing consequence: KV-based auth ships first and unblocks S2's
*storage* path with static MinIO/R2 keys; identity-based auth upgrades it
to secretless once the WI plan's issuance phases land. The artifact spec
shape is identical either way, so jobs migrate by editing one `auth` line.

#### Security rules (all phases)

- Credentials and presigned URLs never appear in logs, traces, allocation
  rows, or error strings (errors carry the redacted source).
- Archive extraction rejects absolute paths, `..` traversal, and symlinks
  escaping the staging root; `maxArtifactSize` bounds decompression.
- `allowedSchemes` (and optional future host allowlist) is enforced at
  admission *and* on the worker (defense in depth — the worker refuses
  sources the manager config wouldn't have admitted).
- KV credential paths are readable by any job today (no KV ACLs yet —
  same boundary the WI plan drew for control-plane identity; revisit with
  the ACL effort).

### 6) Worker Integration and Task-Kind Consumption

Staging slots in front of the existing plugin lifecycle in
`run_task_allocation`:

```rust
// worker/task_manager.rs (sketch)
fn run_task_allocation(&mut self, ta: TaskAllocation) -> R<()> {
    let (allocation, task) = self.register(ta)?;
    self.set_allocation_health(allocation.id, true, "staging artifacts");

    // Blocking fetch pipeline on a blocking thread; honors per-worker
    // parallelism. All-or-nothing: any artifact failing (after retries)
    // fails the allocation through the existing record_failure path,
    // which heartbeats the failure and lets the manager apply
    // on_unhealthy move/backoff logic.
    let staged = self
        .artifacts
        .stage_for(&allocation, &task)
        .with_context(|| format!("artifact staging for allocation {}", allocation.id))?;

    let mut runtime = TaskRuntime::default();
    runtime.staged_artifacts = staged; // paths + digests, available to plugins

    let plugin = self.registry.plugin(&task.kind).context("plugin")?;
    plugin.prepare(&allocation, &task, &mut runtime)?;
    plugin.start(&allocation, &task, &mut runtime)?;
    // … existing persistence + health monitor wiring …
}
```

Visible states: the allocation health status string surfaces
`"staging artifacts"` / `"fetching oci://… (37%)"` so the UI and
`legion job status` explain *why* a task hasn't started — cheap to add,
disproportionately useful during S5-style scale-ups.

`TaskPlugin` grows three members, all defaulted so existing plugins compile
unchanged:

```rust
pub trait TaskPlugin: Send + Sync {
    // … existing methods …

    /// Capability gate, mirroring supports_kv_env_injection and friends.
    /// Admission rejects artifact-bearing specs for kinds that return false.
    fn supports_artifacts(&self) -> bool {
        false
    }

    /// Artifacts implied by the kind's own config. The manager calls this
    /// during admission to desugar e.g. the container `image` field into
    /// an oci:// artifact, so one pipeline fetches, caches, and authorizes
    /// everything. Built-in kinds run in the manager binary, so admission
    /// can call this directly.
    fn implicit_artifacts(&self, task_config: &Value) -> R<Vec<ArtifactSpec>> {
        let _ = task_config;
        Ok(Vec::new())
    }

    /// How the kind consumes a staged artifact that is not a plain file
    /// tree. The container kind consumes `Image` artifacts by preparing a
    /// runtime bundle (OCI layout -> unpacked rootfs for youki; `container
    /// image load` on macOS). Default: artifacts are just files in the
    /// task dir, nothing to do.
    fn consume_artifact(&self, staged: &StagedArtifact, runtime: &mut TaskRuntime) -> R<()> {
        let _ = (staged, runtime);
        Ok(())
    }
}
```

Capability matrix at the end of this plan's phases:

| Kind | artifacts | consumption |
| --- | --- | --- |
| `exec` | ✅ | files/trees in task dir |
| `container` | ✅ | implicit `image` artifact → runtime bundle; file artifacts deferred until the kind grows volume plumbing (same limitation as its KV file injection) |
| `noop` | ❌ | rejected at admission |
| future `wasm` | ✅ | ORAS module via `mediaType` — exactly S4 |

### 7) Pre-Pull and Warming (separate layer, same cache)

Everything above is correct with zero warming. This layer only moves
fetches *earlier* and tells the scheduler about it (S5).

**Operator-driven pull:**

```
legion artifact pull oci://ghcr.io/acme/api@sha256:abc… --constraint pool=default
legion artifact status sha256:abc…       # per-node warm progress
legion artifact ls --node <id>           # what a node's cache holds
```

`WarmArtifacts` (manager RPC, idempotency-keyed like other mutating RPCs)
creates an `artifact_warm` eval. The eval handler selects nodes via the
existing `ConstraintSet` machinery and dispatches `PullArtifacts` to each
worker over the dispatcher's existing push channel. Workers fetch into the
cache under a synthetic `PinOwner::Prefetch` owner (TTL-pinned, e.g. 1h
default, so warmed bytes survive until the rollout that wanted them — but
never pin forever).

**Warm policies (manager config):**

```jsonnet
{
  artifacts: {
    warm: [
      {
        source: 'oci://ghcr.io/acme/api@sha256:abc…',
        constraint: { pool: 'default' },
        onJoin: true,                       // also pull when a node joins
        auth: { kv: 'secrets/ghcr-pull' },  // policy-level auth (see below)
      },
    ],
    // Stretch (flagged in Open Questions): on node join, also warm the
    // checksum-pinned artifacts of running jobs the node could host.
    warmOnJoin: 'policies',  // 'policies' | 'referenced' | 'off'
  },
}
```

`onJoin` hooks the existing `node_join` eval path, which is exactly the
autoscaler scale-up moment.

**Advertisement.** Workers with `advertise: true` report their cached
digest set (capped at `advertiseLimit`, MRU order) via a new
`ReportArtifactCache` RPC whenever it changes materially (debounced). The
manager stores it under `node.dynamic_attrs["legion.artifact_cache"]`.
Deliberately *not* `UpdateNodeDynamic`: that RPC auto-enqueues `job_update`
evals for every job on the node, which a 50-node warm would turn into an
eval storm for no benefit — the next scheduling pass reads attrs fresh
anyway, which is precisely the "signal on the next scheduling run" the
feature wants.

**Scheduler scoring.** A built-in soft term alongside the existing
automatic anti-affinity:

```jsonnet
// manager.jsonnet
{
  schedulerSettings: {
    artifact_locality: 0.3,   // default; 0 disables
  },
}
```

Per candidate node: `score += artifact_locality × (pinned-digest artifacts
of this task present in the node's advertised set ÷ total pinned-digest
artifacts of the task)`. Only checksummed/digest-pinned artifacts
participate — mutable references don't have a stable identity to match, and
that asymmetry is a feature: it nudges users toward pinning, which is also
what makes rollouts deterministic.

**Interplay that needed deciding (the subtle bits):**

- **Pre-pull × task identity.** A warm fetch happens *before any task
  exists*, so it cannot authenticate as a task identity — there is no
  allocation to mint a token for. Warm policies therefore carry their own
  auth (KV-based, or later a manager-minted token with
  `sub = <region>:legion:prefetch`, `legion.io/scope: "prefetch"` under the
  same issuer). Admission warns when a warm policy targets an artifact
  whose only job-side auth is a task identity: warming will fetch with
  policy auth or not at all, and placement-time staging will still
  cache-hit on digest regardless of which credential originally fetched
  the bytes — content addressing makes credential provenance irrelevant
  once integrity is digest-verified.
- **Pre-pull × mutable references.** Warming `:latest` is racy by nature
  (the tag may move between warm and placement). Warm requests for
  un-pinned sources are accepted but flagged in CLI output; the scoring
  term ignores them. Recommendation stays: pin digests for anything you
  warm or roll out.
- **Warming × cache budget.** Prefetch pins are TTL-bounded and rank below
  allocation pins; a warm that would evict another job's *pinned* artifacts
  is refused on-node (reported in `artifact status`) rather than degrading
  running workloads.

### 8) Observability and Agent Surfaces

Per AGENTS.md obligations:

- New CLI verbs (`legion artifact pull|status|ls|gc`) take `--json`, emit
  the standard envelope, and register in `catalog()`
  (`src/cmd/capabilities.rs`) with correct `mutates`/`read_only_safe`.
- MCP: `artifact_status`/`artifact_ls` as read-only tools; `artifact_pull`
  gated behind `--allow-write` with `mutates: true`.
- `WarmArtifacts` accepts an `idempotency_key` and reports `deduplicated`,
  matching the RunJob/StopJob pattern.
- Tracing: `#[instrument]` on the staging entrypoint and fetcher calls with
  `alloc_id`, `task_name`, redacted `source`, `cache_hit`, `bytes`,
  `duration`; per-worker metrics for cache size, hit ratio, evictions,
  checksum failures, fetch retries.
- Docs: this file, plus a user-facing `docs/artifacts.md`, updates to
  `docs/task-kind-*.md` capability tables, `docs/agent-readiness.md`, and
  every new jobspec field exercised in `configs/jobs/maximal.jsonnet`
  (repo rule).

## Proto, Struct, and Config Changes

Proto (`proto/legion.proto`, all documented per repo convention):

- `TaskAllocation` += `task_artifacts` (JSON string: normalized
  `Vec<ArtifactSpec>`, *post* admission desugaring — workers never
  re-derive implicit artifacts).
- New manager RPCs: `GetArtifactCredential` (worker-auth'd KV credential
  read), `ReportArtifactCache`, `WarmArtifacts` (+ status query).
- New worker RPC: `PullArtifacts`.
- New eval kind: `artifact_warm`.

Structs:

- `src/structs/job.rs`: `TaskConfig.artifacts: Vec<ArtifactSpec>` with
  string-shorthand deserialization + admission validation.
- `src/structs/config.rs`: `ManagerConfigFull.artifacts`
  (`allowedSchemes`, `auth` matchers, `warm` policies, `warmOnJoin`);
  worker config += `artifactCache`; `schedulerSettings.artifact_locality`.
- `src/task_plugins/mod.rs`: the three trait additions (§6);
  `TaskRuntime.staged_artifacts`.
- `src/artifacts/` module as laid out in §3; `src/getter_rs` folds into it
  (CLI jobspec fetching migrates to the same registry).

New dependencies: `tar` + `flate2` (+ `zstd`, `zip` as warranted) for
unpacking; possibly `oci-client` (Open Questions). `object_store`, `sha2`,
`reqwest`, `rusqlite`, `humantime` are already in tree.

## Implementation Phases

Each phase is independently shippable and states its win. Dependency edges
are noted; 4 and 5 don't strictly need 3 (anonymous sources work), but 3
makes them useful in private setups.

### Phase 0 — Allocation/task directories
Create/destroy task dirs, `LEGION_TASK_DIR`/`LEGION_ALLOC_DIR`, exec `cwd`
default, landlock auto-grant.
**Win:** every task gets a private, cleaned-up workdir — a standalone
quality fix (no more tasks dumping files into the worker's cwd) and the
landing zone everything else needs.

### Phase 1 — `artifacts` spec + staging pipeline (https/file/git)
Spec + shorthand + admission validation; worker staging step with retries,
checksums, safe unpack; `staging` status visibility; `supports_artifacts`
gating; getter_rs unification.
**Win:** S1 end-to-end — exec tasks become deployable from release URLs
with one spec line, and bad specs fail at `job run` instead of on a node.
The single biggest day-one UX unlock.

### Phase 2 — Content-addressed cache
CAS + sqlite index, TTL/revalidation, size budget + `minFreeDisk`, LRU
eviction, pinning, single-flight, GC loop, `artifact ls/gc` CLI.
**Win:** S4 and S6 — restarts and same-node scale-outs stop re-downloading,
concurrent allocations share one download, disk usage is bounded and
TTL-fresh. (Depends on 1.)

### Phase 3 — Secret auth via KV + cluster matchers
`auth.kv`, credential JSON shapes, `GetArtifactCredential` RPC, cluster
`artifacts.auth` matchers, redaction discipline.
**Win:** S3's auth model — private artifacts with zero secrets in job
specs and centralized, instant rotation. (Depends on 1.)

### Phase 4 — `s3://` fetcher
`object_store`-backed, endpoint/region via credential or query params,
ETag probe.
**Win:** S2's storage path for AWS/MinIO/R2 — small phase because the
dependency already ships in the binary. (Depends on 1; pairs with 3.)

### Phase 5 — `oci://` fetcher + container desugaring
Distribution-spec client, per-blob CAS (layer dedupe), image
materialization for the container plugin (`implicit_artifacts` +
`consume_artifact`), ORAS `mediaType` selection.
**Win:** S3 and S4 in full — private registries for containers through the
same auth/cache pipeline, and one fetch path serving container images *and*
exec/wasm payloads: the reuse this whole design exists for. (Depends on
2, 3.)

### Phase 6 — Workload-identity auth
`auth.identity` (+ `assumeRole`), bearer presentation for oci/https, STS
`CredentialProvider` for s3, prefetch-scope token groundwork.
**Win:** S2 goes secretless — store policies keyed on *which task* is
fetching, the WI plan's promise extended to artifacts. (Depends on 1; **and
on WI plan Phases 2 & 4** — token issuance RPC + worker delivery. If those
slip, every other phase still ships.)

### Phase 7 — Pre-pull, advertisement, locality scoring
`WarmArtifacts`/`PullArtifacts`/`ReportArtifactCache`, `artifact_warm`
eval, warm policies + `onJoin`, `legion.artifact_cache` dynamic attr,
`artifact_locality` scoring term, `artifact pull/status` CLI + MCP.
**Win:** S5 — scale-ups and rollouts without pull storms; warm nodes win
placement on the very next scheduling pass; visible warm progress for
operators and agents. (Depends on 2; uses 3/6 auth for policies.)

### Phase 8 — Fetcher plugins
`plugins/artifacts/<scheme>/` dylibs via the existing plugin-loading
pattern, registry resolution order: task-kind built-ins → dynamic plugins.
**Win:** in-house artifact stores and exotic protocols without forking
core — extensibility parity with task-kind plugins, completing the
`plugin-system.md` "Artifact acquisition plugins" promise. (Depends on 1.)

## Testing Plan

### Unit

- Spec parsing: string shorthand ↔ full form, camelCase fields, rejection
  matrix (absolute/`..` dest, bad checksum format, both `kv`+`identity`,
  unknown identity name, disallowed scheme, `ttl` parse errors).
- Cache: key derivation (checksummed vs mutable), TTL expiry →
  revalidation, LRU eviction order, pins never evicted, budget-exhausted
  error when pinned set exceeds `maxSize`, single-flight (two `ensure`
  calls, one fetch), atomic rename + checksum-mismatch quarantine.
- Auth: matcher precedence (task > cluster > anonymous), first-match-wins
  globs, credential JSON shape validation, redaction (assert no secret
  bytes in rendered errors/spans).
- Unpack: traversal/symlink-escape rejection, size-limit enforcement, mode
  application.
- Scoring: locality fraction math, weight 0 disables, mutable refs ignored.

### Integration

- Full lifecycle on a local HTTP server fixture: submit → stage → exec runs
  from task dir → destroy cleans dir and releases pin; second allocation of
  the same artifact is a cache hit (assert one server hit).
- ETag flip on the fixture server → TTL revalidation refetches; unchanged
  ETag → 304-style reuse.
- Fetch failure (404 / checksum mismatch) after retries → allocation
  failure heartbeats → manager reschedule path engages (existing
  `on_unhealthy` flow).
- `s3://` against a MinIO container (gated like other env-dependent tests);
  `oci://` against a local registry fixture, asserting per-blob dedupe
  across two tags sharing layers.
- Warm: `WarmArtifacts` against two fake workers → both report via
  `ReportArtifactCache` → next `plan_only` run scores the warm node higher;
  also assert *no* `job_update` evals were enqueued by advertisement.

### Regression

- Jobs without `artifacts` are byte-identical through admission and
  dispatch (no proto field set, no staging step entered).
- `noop` + artifacts rejected at admission with an actionable error.
- Existing getter_rs CLI jobspec-fetch behavior preserved through the
  registry migration (`job run https://…` keeps working).
- `configs/jobs/maximal.jsonnet` extended and still validates.

## Rollout & Backward Compatibility

- Everything is additive. No `artifacts` field → no staging step, no cache
  directory created until first use, no new RPC traffic.
- Default scheme allowlist is permissive minus `http` (plain-HTTP requires
  explicit opt-in; `https` everywhere else).
- Container `image` desugaring lands behind the Phase 5 boundary: until
  then the container plugin keeps its current "runtime pulls the image"
  behavior, and afterwards the desugared path is feature-equivalent for
  public images before adding private-registry support.
- `artifact_locality` defaults to a modest 0.3 and can be set to 0; it is a
  soft score, so worst case is the status quo placement.
- Workers without `artifactCache` config get conservative defaults
  (10GiB / 72h / advertise off), so heterogeneous fleets upgrade node by
  node.

## Judgement Calls (decided here, reversible later)

1. **First-class task field, not per-kind config.** The alternative (each
   kind grows its own `download` options) was rejected: it forfeits reuse,
   splinters auth, and makes admission validation per-kind. Kinds keep
   agency through `supports_artifacts` / `implicit_artifacts` /
   `consume_artifact`.
2. **The cache is the only coordination point; pre-pull is a hint.**
   No "artifact placement" bookkeeping in the manager beyond advertised
   attrs. Correctness never depends on warm state, which is what keeps the
   warming layer deletable/optional.
3. **Blocking `ArtifactFetcher` trait run on blocking threads**, matching
   the existing getters and — decisively — the dylib plugin boundary
   (Phase 8), where async traits across `cdylib` are a hazard. Parallelism
   comes from the worker's fetch pool, not async fan-out.
4. **sqlite cache index** (bundled rusqlite) over a JSON sidecar:
   crash-safe pins/refcounts and LRU queries beat hand-rolled file locking,
   and the dependency cost is zero.
5. **Cluster auth defaults support `kv` only; `identity` stays per-task.**
   Identities are task-scoped by design in the WI plan (the `sub` claim is
   the security boundary); a cluster default silently attaching a task's
   identity to fetches would blur that boundary. Revisit if a node-scoped
   prefetch identity (§7) proves out.
6. **Hardlinked stages don't count against `resources.disk`.** The cache
   budget governs shared bytes; `resources.disk` governs the task's own
   writable footprint. Copies (cross-filesystem fallback) do count. Fuzzy
   at the edges, but the alternative (double-counting shared layers per
   task) punishes exactly the dedupe the cache exists to provide.
7. **Container `image` remains the user-facing field** and desugars at
   admission. No spec migration, declarations stay simple, and the worker
   sees only normalized artifact lists.
8. **Dedicated `ReportArtifactCache` RPC** rather than reusing
   `UpdateNodeDynamic`, specifically to *avoid* its job_update eval
   fan-out on mass warming.
9. **TTL means "revalidation window for mutable sources."** Checksummed
   artifacts are immutable and exempt; retention is LRU-under-budget for
   everyone. One word, one meaning.
10. **All-or-nothing staging per task** (any artifact failure fails the
    allocation through the existing failure path). Partial staging plus
    per-artifact retry policies was judged not worth the state-machine
    complexity in v1.

## Open Questions

1. **Tag pinning at submit.** Should the manager resolve mutable OCI tags
   to digests at job submission (`resolve: 'pin'`) so every allocation in a
   rollout runs identical bytes? Strong lean yes as a Phase 5+ follow-up;
   needs a story for `job plan` diffing and explicit `resolve: 'always'`
   opt-out.
2. **OCI client: hand-rolled vs `oci-client` crate.** Hand-rolling keeps
   the dependency tree tight and the token dance transparent; the crate is
   battle-tested. Decide at Phase 5 with a spike.
3. **KV ACLs.** Until the ACL effort lands, any job can point `auth.kv` at
   any path. Acceptable now (matches existing `$ref` trust model) — but the
   artifact system should be an early consumer of ACLs when they arrive.
4. **Advertisement at scale.** Digest list capped at 256 is fine for
   realistic caches; if fleets carry thousands of entries, switch to a
   bloom filter attr (scoring already tolerates false positives — it's a
   soft score).
5. **`warmOnJoin: 'referenced'`** (auto-warm artifacts of running jobs a
   new node could host): high value for S5, but needs a selection bound
   (top-N by reference count? by recent placement frequency?) to avoid
   filling small caches with the long tail.
6. **Per-task-identity pre-pull.** Declined for now (no allocation → no
   token). If demand appears, the manager could mint short-lived
   prefetch-scoped tokens for *declared* (not placed) tasks — security
   review required.
7. **Fetch-failure budgets.** v1 reuses `on_unhealthy` move/restart
   budgets. If artifact flakiness turns out to deserve separate handling
   (e.g., registry outage ≠ unhealthy task), add
   `artifacts.onFetchFailure` later.
8. **Windows staging.** Hardlink fallback exists, but reflink/atime
   semantics and youki-equivalent consumption on Windows need a dedicated
   pass when Windows workers become a priority.
9. **Should `files:` injection and KV file injection also target the task
   dir by default** once Phase 0 lands? Consistency says yes; it's a
   one-line default change but a behavior change for existing specs, so it
   should ride its own small proposal.

## Acceptance Criteria

- **Phase 0:** every allocation runs with a private task dir, env vars set,
  dir removed on destroy; exec `cwd` defaults there.
- **Phase 1:** S1 works end-to-end from a jobspec one-liner; checksum
  mismatch fails at staging with a clear reason visible in `job status`;
  invalid artifact specs fail `job validate`.
- **Phase 2:** second allocation of a pinned artifact performs zero network
  fetches; cache never exceeds `maxSize` with running pins honored; TTL
  expiry triggers revalidation, not blind refetch.
- **Phase 3:** S3's credential flow works with specs containing no secret
  material; rotating the KV value affects the next fetch with no job edit;
  no secret bytes appear in logs/traces under test assertion.
- **Phase 4:** a MinIO-backed `s3://` artifact stages with endpoint/region
  from credential config.
- **Phase 5:** a private-registry container task and an ORAS `mediaType`
  exec task both stage through the same cache, with shared layers stored
  once.
- **Phase 6:** with WI issuance available, an `auth.identity` artifact
  fetch succeeds against a store trusting Legion's issuer, and the bucket
  policy can discriminate by task `sub`.
- **Phase 7:** in a two-node test, warming one node makes it win placement
  on the next pass; advertisement enqueues no job_update evals; warm
  progress is visible via CLI/MCP.
- **Phase 8:** a sample out-of-tree fetcher dylib resolves a custom scheme
  end-to-end.
