# Artifacts

Tasks need *stuff* before they can run: release tarballs, builds from
S3-compatible storage, git repos, container images, ORAS artifacts (model
weights, WASM modules). The artifact system is one declaration and one
fetch/verify/unpack/stage pipeline shared by every task kind: tasks declare
`artifacts`, the worker stages them into a private task directory before the
task starts, and the task references them by name — no hardcoded paths, no
credentials in the job spec.

This document is the user-facing reference. The design rationale lives in
[docs/plans/artifact-system-plan.md](plans/artifact-system-plan.md); the
caching/pre-warming layer is a separate, not-yet-implemented plan
([docs/plans/artifact-cache-and-prewarm-plan.md](plans/artifact-cache-and-prewarm-plan.md)).

> **Implementation status.** The system is implemented end-to-end: spec +
> admission validation, task directories, the staging pipeline with five
> built-in fetchers (`https`/`http`, `file`, `git`, `s3`, `oci`), KV-backed
> auth with cluster matchers, digest pinning at submit, per-artifact status
> through to the CLI and UI, container image desugaring (behind a cluster
> flag), and dynamic fetcher plugins. Two pieces are declared-but-pending:
> **identity/nodeIdentity auth** is validated at admission but fails at fetch
> time until workload-identity token issuance lands (see
> [Auth](#workload-identity-auth-pending)), and there is **no artifact cache
> yet** — every staging fetches from origin (see
> [Limitations](#limitations)).

## Declaring artifacts

`artifacts` is a first-class task field, beside `env`, `kv`, and
`identities` — not inside the kind-specific `config`. A bare source string is
a complete artifact spec:

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

The full object form:

```jsonnet
artifacts: [
  {
    // Scheme picks the fetcher: https://, http://, file://, git:// (or the
    // go-getter style git::https://…), s3://, oci://.
    source: 's3://acme-builds/api/v42.tar.gz',

    // Optional handle for referencing the staged artifact from task config.
    // Required for ${artifact.<name>} / LEGION_ARTIFACT_<NAME>.
    // Lowercase [a-z0-9_-]+, unique within the task.
    name: 'build',

    // Destination, relative to the task directory. Absolute and escaping
    // paths are rejected. Default: '.' for archives/trees, the source
    // basename for single files. A trailing '/' on a single-file dest means
    // "this directory, keep the basename".
    dest: 'build/',

    // 'sha256:<64 hex>'. Presence makes the artifact immutable and
    // verification mandatory (verified while streaming; corrupt bytes never
    // reach the task directory).
    checksum: 'sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',

    // 'auto' (default: unpacks .tar.gz/.tgz/.tar/.gz by extension),
    // 'archive' (force; fails if not a readable archive), or
    // 'file' (never unpack; stage the raw file).
    unpack: 'auto',

    // For mutable references (OCI tags, git branches/tags): 'pin' (default)
    // resolves to a digest at submit; 'always' re-resolves at every staging.
    resolve: 'pin',

    // Auth override. Omitted => cluster artifacts.auth matchers apply.
    // Exactly one of kv / identity / nodeIdentity.
    auth: { kv: 'secrets/build-store' },

    // Octal chmod applied to single-file artifacts (handy for bare binaries;
    // also applied to ORAS layer files).
    mode: '0755',
  },
]
```

OCI sources support digest pinning and ORAS layer selection:

```jsonnet
artifacts: [
  {
    name: 'model',
    source: 'oci://registry.acme.io/models/embedder@sha256:abc…',
    // ORAS multi-file artifacts: pick layers by media type (oci:// only).
    // Omit to materialize the whole artifact as an OCI image layout.
    mediaType: 'application/vnd.acme.model.weights.v1',
    dest: 'models/',
  },
]
```

A runnable example exercising every field is the `batch` task in
[configs/jobs/maximal.jsonnet](../configs/jobs/maximal.jsonnet).

Two read-only annotations appear on stored/normalized specs (the manager
writes them at admission; user specs never set them): `pinnedFrom` (the
original mutable reference when `resolve: pin` rewrote `source` to a digest)
and `authOrigin` (`"task"` or `"cluster"` — where the effective auth came
from).

### Validation rules

Enforced at admission (`job run` / `job plan`) **and** by offline
`legion job validate`:

- `source` must parse; `oci://` sources must include a registry host.
- `name` matches `[a-z0-9_-]+` and is unique within the task.
- Every `${artifact.<name>}` reference anywhere in the task's `config` must
  name a declared artifact (typos fail at submit, not on a node twenty
  minutes later).
- `checksum` must be `sha256:` + 64 hex digits.
- `dest` must be relative and may not escape the task directory (`..` is
  rejected after normalization).
- `auth` must set exactly one of `kv` / `identity` / `nodeIdentity`;
  `assumeRole` may only accompany `identity`; `identity` must name an entry
  in the task's `identities` list.
- `mediaType` is only valid for `oci://` sources.
- `mode` must be an octal file mode.
- The task kind must support artifacts (`supports_artifacts` capability
  gate). `exec` and `container` do; **`noop` rejects artifact-bearing specs**.
- Unknown fields in the object form are rejected.

Two checks need cluster config and therefore run only at admission, not in
offline `job validate`: the scheme must be in the cluster's
`artifacts.allowedSchemes`, and `auth.nodeIdentity` must name an entry in
`workloadIdentity.nodeIdentities`. `job validate` also performs no pin
resolution (it is fully static); `job plan` and `job run` do.

## The task directory

Every allocation gets a private, cleaned-up working directory per task —
artifact-bearing or not:

```
<worker state dir>/allocs/<alloc_id>/<task_name>/
```

(beside the worker's `worker_state.json`, i.e.
`.legion/workers/<id>/allocs/…`).

- Exposed to the task as `LEGION_ALLOC_DIR` and `LEGION_TASK_DIR` env vars
  (for kinds that apply worker-injected env — see the
  [capability matrix](#task-kind-support)).
- The `exec` kind's `cwd` defaults to the task directory when the spec sets
  none.
- `exec` tasks using landlock get the task directory appended to
  `read_write_paths` automatically — otherwise artifacts would stage
  somewhere the sandboxed task cannot read.
- Artifacts stage into `dest` relative to this directory.
- The directory is removed when the allocation is stopped, destroyed, or
  fails (best-effort, logged).

## Referencing staged artifacts

Named artifacts are addressable two ways, so tasks never hardcode staging
paths:

1. **`${artifact.<name>}` interpolation.** The worker substitutes the token
   in every string field of the kind config just before start. It is dumb
   string substitution — no expression language — and unknown references were
   already rejected at admission.
2. **`LEGION_ARTIFACT_<NAME>` env vars** (name uppercased, `-` → `_`),
   holding the absolute staged path. Applied by kinds that inject
   worker-provided env (`exec`).

Both expand to the staged *file path* for single files and the *destination
directory* for everything else (unpacked archives, git trees, ORAS layer
directories, OCI layouts).

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

## Sources

| Scheme | Fetcher notes |
| --- | --- |
| `https://` / `http://` | Streaming download (no whole-body buffering), basic/bearer/headers credentials, redirect limit (5), Content-Length checked against the size cap before streaming. Plain `http` is **not** in the default scheme allowlist. |
| `file://` | Copies a local file or directory tree. Dev workflows and node-local pre-provisioned artifacts. |
| `git` | `git::https://github.com/acme/repo?ref=v1.2.0` (go-getter style) or `git://…`. Shallow clone (depth 1); `ref=` takes a branch, tag, or full 40-hex commit SHA (full SHAs are fetched as exact objects). The `.git` directory is stripped — tasks get a clean worktree. Credentials are injected via `GIT_CONFIG_*` environment variables (an `http.extraHeader` Authorization header), never argv. `resolve()` uses `git ls-remote` to pin refs to commit SHAs. |
| `s3://` | `object_store`-backed; speaks any S3-compatible endpoint (AWS, MinIO, R2, GCS interop). Region/endpoint come from the credential object or query params (`s3://bucket/key?endpoint=https://minio.internal:9000&region=auto`) — query params win. Anonymous fetches skip request signing (public buckets). |
| `oci://` | Built on the `oci-client` crate. Anonymous/basic/bearer registry auth; tags resolve to manifest digests for pinning. Without `mediaType`, the image is materialized as an on-disk **OCI layout** (what image-consuming kinds hand to their runtime). With `mediaType`, only matching ORAS layers are pulled, named by their `org.opencontainers.image.title` annotation (sanitized to a basename; digest hex as fallback). Blob sizes are checked against the size cap before any blob downloads. `?protocol=http` opts into plain-HTTP registries for dev/test. |

Additional schemes can be served by [fetcher plugins](#fetcher-plugins).

The CLI's own jobspec fetching (`legion job run https://…`) goes through the
same fetcher registry — one fetch stack in the tree.

## Auth

Three doors, one resolution path. Per artifact:

```jsonnet
auth: { kv: 'secrets/ghcr-pull' }              // secret-based (works today)
auth: { identity: 'aws' }                      // task workload identity (pending)
auth: { identity: 'aws', assumeRole: 'arn:aws:iam::123:role/artifact-pull' }
auth: { nodeIdentity: 'artifact-pull' }        // node workload identity (pending)
```

Most specs should carry no `auth` at all — cluster defaults route sources to
credentials centrally:

```jsonnet
// manager.jsonnet
{
  artifacts: {
    allowedSchemes: ['https', 's3', 'oci', 'git', 'file'],  // the default; plain http needs opt-in
    auth: [
      // First match wins. Matchers are scheme://host/path globs: `*` matches
      // within one path segment, `**` crosses segments. git::https://…
      // sources match git:// patterns.
      { match: 'oci://ghcr.io/acme/**', kv: 'secrets/ghcr-pull' },
      { match: 's3://acme-tooling/**',  nodeIdentity: 'artifact-pull' },
    ],
  },
}
```

Resolution order: **task artifact `auth` → first matching cluster
`artifacts.auth` entry → anonymous.**

Cluster matchers may use `kv` or `nodeIdentity` — both cluster-scoped
concepts — but never a *task* identity: task identities are a per-task
security boundary, and silently attaching one cluster-wide would blur it.

> **Matchers are baked at admission.** The manager evaluates `artifacts.auth`
> when a job is submitted and persists the *effective* auth on the normalized
> artifact list (annotated `authOrigin: "cluster"`). Changing the matchers
> therefore affects the **next submit**, not jobs already in flight.
> Credential *values* are still resolved late, on the worker, at fetch time —
> so rotating a KV credential needs no re-submit (below).

### Secret-based auth (`kv`)

The credential lives in Legion KV as a JSON object with a `type`
discriminator:

```jsonc
{ "type": "basic",  "username": "ci-pull", "password": "…" }          // http(s), oci, git
{ "type": "bearer", "token": "…" }                                     // http(s), oci, git
{ "type": "aws",    "accessKeyId": "…", "secretAccessKey": "…",
  "sessionToken": "…", "region": "us-east-1",
  "endpoint": "https://minio.internal:9000" }                          // s3 (sessionToken/region/endpoint optional)
{ "type": "headers", "headers": { "X-Api-Key": "…" } }                 // http(s) escape hatch
```

The worker reads the value over its authenticated manager channel via the
`GetArtifactCredential` RPC **immediately before fetching**. The value is
used once and never persisted on allocation rows, logged, or traced; the
manager validates the credential shape server-side so a malformed KV value
fails with an error naming the path, not a mid-staging parse error.

**Rotation story:** because the read happens at fetch time, rotating a
credential is a single `legion kv put` — the next fetch (including restarts
and reschedules of existing jobs) picks up the new value. No job edit, no
re-submit.

> **⚠️ KV ACL caveat — read this.** There are **no KV ACLs yet**: any job may
> point `auth.kv` (or benefit from a cluster matcher pointing) at **any** KV
> path. Possession of job-submit access therefore implies read access to
> every credential stored in KV. This matches the existing trust model for
> `$path/key` jobspec references and is accepted for now, but treat your KV
> namespace accordingly: anyone who can submit jobs can read any KV value by
> staging an artifact with it. When the ACL system lands, the
> `GetArtifactCredential` handler is the enforcement point (tracked in
> [docs/TASK-LIST.md](TASK-LIST.md) under *ACL System*; the SECURITY BOUNDARY
> comment sits at the handler in `src/manager/server.rs` and on the RPC in
> `proto/legion.proto`).

### Workload-identity auth (pending)

`auth.identity` names an entry in the task's `identities` list
([docs/workload-identity.md](workload-identity.md)); `auth.nodeIdentity`
names a cluster-level **node identity** (`workloadIdentity.nodeIdentities` —
see the node-identities section of that doc). The intent: present the JWT as
a bearer token to stores that trust Legion's issuer ("it is Legion task
`prod:api:web:server` fetching" / "it is a Legion worker node in pool
`default`"), with `assumeRole` exchanging it for AWS STS credentials for
`s3://` sources.

> **Status:** the spec surface and all admission validation are implemented,
> but token issuance is not — it depends on the workload-identity plan's
> phases 2 and 4 (`IssueWorkloadIdentityToken` RPC + worker-side delivery).
> Until those land, a fetch using `identity`/`nodeIdentity` fails with an
> actionable error suggesting `auth.kv` or anonymous access, and a
> `resolve: 'pin'` submit on an identity-authed mutable source fails
> suggesting `resolve: 'always'`. Everything else in this document works
> without it.

### Security properties

- Credential values never appear in logs, traces, allocation rows, or error
  strings. Errors and status rows name the auth *source* (`kv:
  secrets/ghcr-pull`, `identity: aws`, `nodeIdentity: artifact-pull`,
  `anonymous`) — never values. The `Credential` type's `Debug` impl is
  redacted.
- Sources are redacted on every status/error surface: userinfo and the whole
  query string are stripped (presigned URLs carry secrets in query values).
  HTTP client errors are stripped of their URLs for the same reason.
- Git credentials travel via environment-scoped git config, never argv.
- `allowedSchemes` is enforced at admission **and** on the worker
  (`artifacts.fetch.allowedSchemes`) — defense in depth.

## Pinning: `resolve: 'pin'` vs `'always'`

Mutable references — OCI **tags** and git **branches/tags** — default to
`resolve: 'pin'`:

- At submit, the manager calls the fetcher's `resolve()` (manifest-digest
  lookup for OCI, `git ls-remote` for git) and persists the **pinned** source
  (`oci://…@sha256:…`, `git::…?ref=<sha>`) plus a `pinnedFrom` annotation
  recording the original reference. Every allocation of that job config —
  including reschedules days later — fetches identical bytes.
- `job plan` and `job run` report what was pinned
  (`task api: pinned oci://ghcr.io/acme/api:v42 -> sha256:def…`), and `job
  plan` additionally diffs against the stored config
  (`task api: artifact oci://ghcr.io/acme/api:v42 moved: sha256:abc… ->
  sha256:def…`) so a moved tag is visible *before* you submit.
- **Re-submitting re-resolves.** That is the deliberate "roll forward"
  moment: push a new image to the tag, re-run `job run`, and the rollout
  ships the new digest.
- Resolution uses the same auth chain a worker would; the manager reads KV
  credentials directly from its own store. If resolution fails (bad
  reference, unreachable registry, identity-authed source), the submit fails
  with the fetch error and the suggestion to set `resolve: 'always'`.

`resolve: 'always'` passes the reference through unchanged; each staging
fetches whatever the tag/branch points at *then*. This is the explicit
"track the tag" option, with the documented cost that allocations of one job
config may run different bytes if the reference moves mid-rollout.

Already-immutable sources — anything with a `checksum`, OCI `@sha256:…`
digests, git full-SHA refs — need no resolution; `resolve` is ignored.

## Staging mechanics

Staging happens on the worker, between receiving the allocation and the
plugin's `prepare()`/`start()`:

1. Tasks without artifacts start exactly as before — no staging step, no new
   RPC traffic.
2. Artifact-bearing tasks are registered with health
   `healthy: false, status: "staging artifacts"` and handed to a **dedicated
   staging thread** — fetches can take minutes, and worker heartbeats keep
   flowing the whole time.
3. Each artifact is fetched into a `.staging/` temp area inside the task
   directory, hashing while streaming. Checksum mismatches discard the bytes
   and fail the attempt — corrupt bytes never reach the task directory.
4. Archives unpack with path-traversal protection (absolute entry paths,
   `..` escapes, and symlink/hardlink targets escaping the extraction root
   are all rejected) and a decompressed-size budget. Recognized formats:
   `.tar.gz`, `.tgz`, `.tar`, and bare `.gz`.
5. Content moves into `dest`. Collisions with existing files **error** rather
   than overwrite, and the temp area is always cleaned up.
6. The task starts with `${artifact.<name>}` interpolated and
   `LEGION_ARTIFACT_<NAME>` set.

Staging is **all-or-nothing per task**: the first artifact that fails (after
its per-artifact retries) fails the whole allocation. Per-artifact fetches
retry with backoff and honor an overall per-artifact deadline and size cap,
all configurable per worker type:

```jsonnet
// configs/workers/default.jsonnet — values shown are the defaults
{
  workerType: 'default',
  pool: 'default',
  artifacts: {
    fetch: {
      parallelism: 4,           // concurrent fetches per worker
      attempts: 3,              // per-artifact tries before the allocation fails
      timeout: '10m',           // overall fetch deadline per artifact
      maxArtifactSize: '50GiB', // per-artifact cap; also bounds decompression
      // allowedSchemes: [...]  // worker-side allowlist (defaults to the cluster default)
    },
  },
}
```

Workers without an `artifacts` block get these defaults. The settings are
delivered through the existing worker pre-registration config flow.

> **Failure semantics — fetch failures spend your `on_unhealthy` budgets.**
> A staging failure (404 after retries, checksum mismatch, timeout, oversize,
> missing credential) is an **allocation failure**, reported through the same
> heartbeat path as a crashed task and handled by the manager's existing
> `on_unhealthy` restart/move logic. That means registry or object-store
> flakiness consumes the task's restart/move budget exactly like task
> unhealthiness — size `on_unhealthy.restart_count`/`move_count` with that in
> mind for jobs that pull from less-reliable origins. A dedicated
> `onFetchFailure` policy (treating a registry outage differently from an
> unhealthy task) is a noted follow-up, not yet implemented.

## Status surfaces

Each artifact progresses `pending` → `fetching` → `verifying` → `staged`, or
ends `failed`. Workers report per-artifact rows inside the existing
heartbeat (`AllocationHealth.artifact_states`, a JSON array); the manager
persists them on allocation health and serves them everywhere allocation
health already flows:

```jsonc
[
  { "name": "tool",  "source": "https://releases.acme.io/embed/v1.4.tar.gz",
    "state": "staged", "digest": "sha256:9f86…", "attempts": 1, "durationMs": 412 },
  { "name": "model", "source": "oci://registry.acme.io/models/embedder@sha256:abc…",
    "state": "fetching", "attempts": 1, "bytesFetched": 1610612736, "bytesTotal": 4294967296 },
  { "name": "cfg",   "source": "s3://acme-builds/cfg.tar.gz",
    "state": "failed", "attempts": 3, "error": "… (credentials from kv secrets/build-store)" }
]
```

Sources are redacted; errors name auth sources, never values.

- **CLI:** `legion job rollout status --id <ID> --json` includes
  `artifact_states` on every allocation, and the ledger (`GetLedger`) carries
  the same field per allocation — agents and humans get identical visibility.
- **UI:** the Jobs page task view shows a declared-artifacts table (name,
  source, checksum or pinned digest with its `pinned from` provenance,
  resolve mode, auth source, dest). Allocation cards on the task, rollout,
  and node views show live staging rows — state badge, progress percentage,
  failure reason — and rollout cards summarize `staging artifacts: 1/3
  staged` while a deploy is mid-fetch.

## Task-kind support

| Kind | artifacts | consumption |
| --- | --- | --- |
| `exec` | ✅ | File/Tree payloads staged into the task dir; `${artifact.*}` interpolation; `LEGION_ARTIFACT_*` / `LEGION_TASK_DIR` env; `cwd` defaults to the task dir; landlock auto-grants it. |
| `container` | ✅ (image, behind a flag) | The implicit `image` artifact is consumed as an OCI layout (below). File/tree artifacts stage to the **host** task dir, which an isolated container filesystem cannot see, and worker-injected env is not applied — both deferred until the kind grows volume plumbing (same limitation as its KV file injection). |
| `noop` | ❌ | Artifact-bearing specs rejected at validation/admission. |

New kinds adopt artifacts through three optional `TaskPlugin` hooks —
`supports_artifacts()` (the capability gate), `implicit_artifacts()`
(desugar kind config into artifacts at admission), and `consume_artifact()`
(handle non-file payloads such as OCI images or ORAS layers). Kinds that
just need files in the task directory only implement the first.

### Container image desugaring (`artifacts.containerImageDesugar`)

With the cluster flag on, the manager desugars a container task's `image`
field at admission into an implicit `oci://` artifact named `image` (Docker
Hub shorthand expands: `alpine:latest` →
`oci://docker.io/library/alpine:latest`), and the worker pulls it through
the shared pipeline — same auth matchers, same pinning, same status rows as
every other artifact. The plugin then loads the staged OCI layout into the
local runtime:

- **macOS (Apple `container`):** the layout is tarred and imported with
  `container image load`, so the subsequent run needs no registry pull.
- **Linux (youki):** not yet supported — consuming a Legion-managed image
  fails with an honest error (OCI layout → runtime bundle conversion is
  pending). Keep the flag off for Linux container workloads.

The flag defaults to **off** until the artifact cache lands: without a
cache, Legion-managed pulls re-download the image for every allocation,
which would regress runtimes that cache images themselves. With the flag
off, container behavior is byte-identical to before the artifact system.

## Fetcher plugins

Schemes beyond the built-ins are served by dynamic fetcher plugins, using
the same dylib pattern as task-kind plugins (see
[docs/plugin-system.md](plugin-system.md)):

```
plugins/artifacts/<scheme>/
  plugin.json                              # {"kind":"artifacts","name":"<scheme>","legionApiVersion":1}
  Cargo.toml                               # cdylib depending on the legion crate
  src/lib.rs                               # exports get()
```

The plugin implements the blocking `ArtifactFetcher` trait
(`src/artifacts/fetcher.rs`) and exports a `get` constructor:

```rust
use legion::artifacts::fetcher::{ArtifactFetcher, FetchRequest, FetchedArtifact, FetchedPayload};
use legion::artifacts::stage::ArtifactSink;

struct MyFetcher;

impl ArtifactFetcher for MyFetcher {
    fn schemes(&self) -> &'static [&'static str] { &["myscheme"] }
    fn fetch(&self, req: &FetchRequest<'_>, sink: &mut ArtifactSink) -> legion::util::R<FetchedArtifact> {
        let mut writer = sink.file_writer("payload.bin")?;
        // … stream bytes into writer (it hashes, counts, and enforces limits) …
        let (path, digest, bytes) = writer.finish()?;
        Ok(FetchedArtifact { bytes, payload: FetchedPayload::File { path, digest } })
    }
}

#[no_mangle]
pub extern "C" fn get() -> Box<dyn ArtifactFetcher> {
    Box::new(MyFetcher)
}
```

Resolution order is **built-ins first, then plugins**: when no built-in
serves a scheme, the registry looks for
`plugins/artifacts/<scheme>/target/release/lib<scheme>.<ext>`, validates the
manifest, loads the dylib, and verifies the fetcher actually reports the
requested scheme. Everything downstream — auth resolution, checksums,
unpack, retries, status reporting — is shared pipeline code; the plugin only
moves bytes. Fetchers are deliberately *blocking* (they run on staging
threads, never the async runtime), which keeps the dylib boundary free of
async-trait hazards. Optionally implement `resolve()` to support
`resolve: 'pin'` for mutable references.

A complete worked example ships at
[plugins/artifacts/demo](../plugins/artifacts/demo/): a `demo://` fetcher
that materializes the URL's `content` query parameter as a staged file —
`artifacts: ['demo://example/hello.txt?content=hi']`.

Remember the allowlists: a custom scheme must be added to the cluster's
`artifacts.allowedSchemes` *and* (if customized) the worker's
`artifacts.fetch.allowedSchemes`.

## Limitations

- **No cache, no pre-warming.** Every staging — including restarts and
  reschedules onto the same node — fetches from origin. The
  content-addressed cache, disk budgets, pre-pull, and scheduler locality
  scoring are the companion plan
  ([artifact-cache-and-prewarm-plan.md](plans/artifact-cache-and-prewarm-plan.md));
  the `ArtifactSink` write seam exists so it slots in without changing
  specs, fetchers, or plugins.
- **Checksums verify single-file payloads** (and, for OCI, the manifest
  digest). Declaring a `checksum` on a tree-producing source (git, `file://`
  directories) is a staging error rather than a silent skip.
- **`identity` / `nodeIdentity` auth fails at fetch time** until
  workload-identity token issuance and delivery land (WI plan phases 2/4).
- **Cluster auth matcher changes apply at the next submit**, not to
  in-flight jobs (the effective auth is baked into the normalized spec at
  admission). KV credential *value* rotation applies immediately.
- **Container kinds consume only the `Image` payload**, image desugaring is
  off by default, and youki image loading is pending; file/tree artifacts
  and env referencing inside containers wait on volume plumbing.
- **Archive formats** are `.tar.gz`/`.tgz`/`.tar`/`.gz`; `zip`/`zstd` are
  not yet recognized (use `unpack: 'file'` to stage such files as-is).
- **Fetch failures consume `on_unhealthy` budgets** (see
  [Failure semantics](#staging-mechanics)); there is no dedicated
  `onFetchFailure` policy yet.
- **Windows staging** has not had a dedicated pass (tracked in
  [docs/TASK-LIST.md](TASK-LIST.md)).
