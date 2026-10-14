# Artifacts

Tasks often need files before they can run: release tarballs, builds from
S3-compatible storage, git repos, container images, or ORAS artifacts (model
weights, WASM modules). Artifacts give every task runtime one way to declare
these and one pipeline to fetch, verify, unpack, and stage them. A task
declares `artifacts`, the worker stages them into a private task directory
before the task starts, and the task refers to them by name. You don't need
hardcoded paths or credentials in the job spec.

This page is the user-facing reference.

> **Implementation status.** The following are implemented: spec and
> admission validation, task directories, the staging pipeline with five
> built-in fetchers (`https`/`http`, `file`, `git`, `s3`, `oci`), KV-backed
> auth with cluster matchers, digest pinning at submit, per-artifact status in
> the CLI and UI, container image desugaring (behind a cluster flag),
> dynamic fetcher plugins, and `nodeIdentity` auth. Two pieces are not
> finished. **Task `identity` auth** (and `assumeRole`) is validated at
> admission but fails at fetch time (see
> [Auth](#workload-identity-auth-pending)). The
> [node content store](#the-node-content-store) caches artifacts that
> declare a `checksum` on an `http(s)`, `s3`, or `file` source, and container
> images (`oci://` images, per layer). Everything else fetches from origin for
> each new allocation (see [Limitations](#limitations)).

## Declaring artifacts

`artifacts` is a top-level task field, next to `env`, `kv`, and
`identities`. It does not go inside the runtime-specific `config`. A bare
source string is a complete artifact spec:

```nickel
{
  name = "tool",
  runtime = "exec",
  lifecycle = "batch",
  artifacts = [
    "https://github.com/acme/tool/releases/download/v1.4/tool-linux-amd64.tar.gz",
  ],
  config = {
    # Archives auto-unpack into the task directory; cwd defaults there too.
    command = ["./tool", "--once"],
  },
}
```

The full object form:

```nickel
artifacts = [
  {
    # Scheme picks the fetcher: https://, http://, file://, git:// (or the
    # go-getter style git::https://…), s3://, oci://.
    source = "s3://acme-builds/api/v42.tar.gz",

    # Optional handle for referencing the staged artifact from task config.
    # Required for ${artifact.<name>} / LEGION_ARTIFACT_<NAME>.
    # Lowercase [a-z0-9_-]+, unique within the task.
    name = "build",

    # Destination, relative to the task directory. Absolute and escaping
    # paths are rejected. Default: "." for archives/trees, the source
    # basename for single files. A trailing "/" on a single-file dest means
    # "this directory, keep the basename".
    dest = "build/",

    # 'sha256:<64 hex>'. Presence makes the artifact immutable and
    # verification mandatory (verified while streaming; corrupt bytes never
    # reach the task directory).
    checksum = "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",

    # "auto" (default: unpacks .tar.gz/.tgz/.tar/.gz by extension),
    # "archive" (force; fails if not a readable archive), or
    # "file" (never unpack; stage the raw file).
    unpack = "auto",

    # For mutable references (OCI tags, git branches/tags): "pin" (default)
    # resolves to a digest at submit; "always" re-resolves at every staging.
    resolve = "pin",

    # Auth override. Omitted => cluster artifacts.auth matchers apply.
    # Exactly one of kv / identity / nodeIdentity.
    auth = { kv = "secrets/build-store" },

    # Octal chmod applied to single-file artifacts (handy for bare binaries;
    # also applied to ORAS layer files).
    mode = "0755",
  },
]
```

OCI sources support digest pinning and ORAS layer selection:

```nickel
artifacts = [
  {
    name = "model",
    source = "oci://registry.acme.io/models/embedder@sha256:abc…",
    # ORAS multi-file artifacts: pick layers by media type (oci:// only).
    # Omit to materialize the whole artifact as an OCI image layout.
    mediaType = "application/vnd.acme.model.weights.v1",
    dest = "models/",
  },
]
```

A runnable example exercising every field is the `batch` task in
[configs/jobs/maximal.ncl](../configs/jobs/maximal.ncl).

Two read-only annotations appear on stored, normalized specs. The manager
writes them at admission, and user specs never set them:

- `pinnedFrom`: the original mutable reference, when `resolve = "pin"`
  rewrote `source` to a digest.
- `authOrigin`: `"task"` or `"cluster"`, depending on where the effective
  auth came from.

### Validation rules

These rules are enforced at admission (`job run` / `job plan`) and by offline
`legion job validate`:

- `source` must parse. `oci://` sources must include a registry host.
- `name` matches `[a-z0-9_-]+` and is unique within the task.
- Every `${artifact.<name>}` reference anywhere in the task's `config` must
  name a declared artifact. Typos fail at submit, not on a node twenty
  minutes later.
- `checksum` must be `sha256:` + 64 hex digits.
- `dest` must be relative and may not escape the task directory (`..` is
  rejected after normalization).
- `auth` must set exactly one of `kv` / `identity` / `nodeIdentity`.
  `assumeRole` is only allowed with `identity`. `identity` must name an entry
  in the task's `identities` list.
- `mediaType` is only valid for `oci://` sources.
- `mode` must be an octal file mode.
- The task runtime must support artifacts (the `supports_artifacts`
  capability). `exec`, `container`, and `sandbox` do. **`noop` rejects specs
  with artifacts**.
- Unknown fields in the object form are rejected.

Two checks need cluster config, so they run only at admission, not in offline
`job validate`. The scheme must be in the cluster's
`artifacts.allowedSchemes`, and `auth.nodeIdentity` must name an entry in
`workloadIdentity.nodeIdentities`. `job validate` also does no pin resolution,
because it is fully static. `job plan` and `job run` do resolve pins.

## The task directory

Every allocation gets a private working directory for each task, whether or
not the task has artifacts. Legion cleans it up afterward:

```
<worker state dir>/allocs/<alloc_id>/<task_name>/
```

This sits next to the worker's `worker_state.json`, for example
`.legion/workers/<id>/allocs/…`.

- It is created owner-only (`0700` on Unix). The allocation directory
  isolates workloads that share a node on disk. No other task or local user
  can reach this allocation's staged artifacts, injected secrets, or files.
  When an `exec` task sets `user`, the allocation tree is chowned to that
  user at start (symlinks are not followed). The process keeps access to its
  own directory after dropping privileges, and to no one else's. A Linux
  container with a user namespace gets traversal of its allocation directory
  only (`0710`, group = its own id range; see
  [container security](task-runtime-container.md#security)).
- The directories above it, the worker state dir and `allocs/`, are
  traverse-only (`0711`). A task's own user can reach its allocation
  directory by path, but nothing can list a node's allocations. Every other
  entry in the state dir guards itself: the state files are `0600`, and
  `logs/`, `content/`, `volumes/`, and `identity/` are owner-only.
- It is exposed to the task as the `LEGION_ALLOC_DIR` and `LEGION_TASK_DIR`
  env vars, for runtimes that apply worker-injected env (see the
  [capability matrix](#task-runtime-support)).
- The `exec` runtime's `cwd` defaults to the task directory when the spec
  sets none.
- `exec` tasks using landlock get the task directory added to
  `read_write_paths` automatically. Otherwise artifacts would stage
  somewhere the sandboxed task cannot read.
- Artifacts stage into `dest` relative to this directory. Paths given to
  tasks (env vars, interpolation) are absolute.
- The directory is removed when the allocation is stopped, destroyed, or
  fails (best-effort, logged). It is also cleared before fresh staging
  begins, so a worker that died mid-staging leaves no stale content behind.

## Referencing staged artifacts

You can refer to named artifacts in two ways, so tasks never hardcode staging
paths:

1. **`${artifact.<name>}` interpolation.** The worker replaces the token in
   every string field of the runtime config just before start. This is plain
   string substitution with no expression language. Unknown references were
   already rejected at admission.
2. **`LEGION_ARTIFACT_<NAME>` env vars** (name uppercased, `-` → `_`), which
   hold the absolute staged path. Runtimes that inject worker-provided env
   (`exec`) set these.

Both expand to the staged file path for single files. For everything else
(unpacked archives, git trees, ORAS layer directories, OCI layouts), they
expand to the destination directory.

```nickel
{
  name = "embedder",
  runtime = "exec",
  artifacts = [
    { name = "tool",  source = "https://releases.acme.io/embed/v1.4.tar.gz", checksum = "sha256:9f86…" },
    { name = "model", source = "oci://registry.acme.io/models/embedder@sha256:abc…", mediaType = "application/vnd.acme.model.weights.v1" },
  ],
  config = {
    command = ["${artifact.tool}/bin/embed", "--weights", "${artifact.model}/weights.bin"],
    env = { MODEL_DIR = "${artifact.model}" },
  },
}
```

## Sources

| Scheme | Fetcher notes |
| --- | --- |
| `https://` / `http://` | Streaming download (no whole-body buffering), basic/bearer/headers credentials, redirect limit (5), Content-Length checked against the size cap before streaming. Plain `http` is **not** in the default scheme allowlist. |
| `file://` | Copies a local file or directory tree. For dev workflows and artifacts pre-provisioned on the node. |
| `git` | `git::https://github.com/acme/repo?ref=v1.2.0` (go-getter style) or `git://…`. Shallow clone (depth 1). `ref=` takes a branch, tag, or full 40-hex commit SHA (full SHAs are fetched as exact objects). The `.git` directory is stripped, so tasks get a clean worktree. Credentials are injected through `GIT_CONFIG_*` environment variables (an `http.extraHeader` Authorization header), never argv. `resolve()` uses `git ls-remote` to pin refs to commit SHAs. |
| `s3://` | Backed by `object_store`. Works with any S3-compatible endpoint (AWS, MinIO, R2, GCS interop). Region and endpoint come from the credential object or from query params (`s3://bucket/key?endpoint=https://minio.internal:9000&region=auto`). Query params win. With neither naming a region, the bucket is addressed in `us-east-1`, whatever the worker's `AWS_REGION` says, so a jobspec artifact resolves the same on every node. Anonymous fetches skip request signing (public buckets). A KV credential signs exactly as written: the worker's own `AWS_*` key variables (an `AWS_SESSION_TOKEN` included) never mix into it. The worker's other `AWS_*` store settings (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, server-side encryption, proxy options) do apply. |
| `oci://` | Built on the `oci-client` crate. Anonymous, basic, or bearer registry auth. Tags resolve to manifest digests for pinning. Without `mediaType`, the image is written to disk as an **OCI layout**, which is what image-consuming runtimes pass to their OCI runtime. It is staged under `.oci/<name>/` unless `dest` is set. With `mediaType`, only matching ORAS layers are pulled. Each is named by its `org.opencontainers.image.title` annotation (sanitized to a basename, with the digest hex as fallback). Blob sizes are checked against the size cap before any blob downloads. `?protocol=http` enables plain-HTTP registries for dev and test. |

Additional schemes can be served by [fetcher plugins](#fetcher-plugins).

The CLI's own jobspec fetching (`legion job run https://…`) uses the same
fetcher registry.

## Auth

There are three kinds of auth, resolved the same way. Per artifact:

```nickel
auth = { kv = "secrets/ghcr-pull" }              # secret-based (works today)
auth = { identity = "aws" }                      # task workload identity (not implemented)
auth = { identity = "aws", assumeRole = "arn:aws:iam::123:role/artifact-pull" }
auth = { nodeIdentity = "artifact-pull" }        # node workload identity (bearer token)
```

Most specs should have no `auth` at all. Cluster defaults map sources to
credentials in one place:

```nickel
# manager.ncl
{
  artifacts = {
    allowedSchemes = ["https", "s3", "oci", "git", "file"],  # the default; plain http needs opt-in
    auth = [
      # First match wins. Matchers are scheme://host/path globs: `*` matches
      # within one path segment, `**` crosses segments. git::https://…
      # sources match git:// patterns.
      { "match" = "oci://ghcr.io/acme/**", kv = "secrets/ghcr-pull" },
      { "match" = "s3://acme-tooling/**",  nodeIdentity = "artifact-pull" },
    ],
  },
}
```

Resolution order: **task artifact `auth` → first matching cluster
`artifacts.auth` entry → anonymous.**

Cluster matchers may use `kv` or `nodeIdentity`, which are both
cluster-scoped. They can never use a task identity. Task identities are a
per-task security boundary, and attaching one cluster-wide would weaken it.

> **Matchers are applied at admission.** The manager evaluates
> `artifacts.auth` when a job is submitted and stores the effective auth on
> the normalized artifact list (annotated `authOrigin = "cluster"`). So
> changing the matchers affects the **next submit**, not jobs already
> running. Credential values are still resolved on the worker at fetch time,
> so rotating a KV credential needs no re-submit (below).

### Secret-based auth (`kv`)

The credential lives in Legion KV as a JSON object with a `type`
discriminator:

```jsonc
{ "type": "basic",  "username": "ci-pull", "password": "…" }             // http(s), oci, git
{ "type": "bearer", "token": "…" }                                       // http(s), oci, git
{ "type": "aws",    "accessKeyId": "…", "secretAccessKey": "…",
  "sessionToken": "…", "region": "us-east-1",
  "endpoint": "https://minio.internal:9000" }                            // s3 (sessionToken/region/endpoint optional)
{ "type": "headers", "headers": { "X-Api-Key": "…" } }                   // http(s) escape hatch
```

The worker reads the value over its authenticated manager channel with the
`GetArtifactCredential` RPC, immediately before fetching. The value is used
once. It is never stored on allocation rows, logged, or traced. The manager
validates the credential shape on the server, so a malformed KV value fails
with an error naming the path instead of a parse error mid-staging.

**Rotation:** the read happens at fetch time, so rotating a credential is a
single `legion kv put`. The next fetch picks up the new value, including
restarts and reschedules of existing jobs. You don't need to edit or
re-submit the job.

> **⚠️ KV ACL caveat.** KV ACLs (`kv` rules scoped by `paths`, see
> [auth-and-acls.md](auth-and-acls.md)) govern the KV RPCs, but they do not
> apply to artifact credentials. Admission does not check the submitter's
> `kv` read rights for `auth.kv`, and `GetArtifactCredential` is a
> worker-plane RPC that serves any path to an authenticated worker. So any
> job may point `auth.kv` at **any** KV path, or use a cluster matcher that
> does. Anyone with job-submit access can read every credential stored in
> KV by staging an artifact with it. This matches the trust model for
> `$path/key` jobspec references. Plan your KV namespace and job-write
> grants with this in mind.

### Workload-identity auth (pending)

`auth.identity` names an entry in the task's `identities` list
([docs/workload-identity.md](workload-identity.md)). `auth.nodeIdentity`
names a cluster-level **node identity** (`workloadIdentity.nodeIdentities`,
see the node-identities section of that doc). The JWT is sent as a bearer
token to stores that trust Legion's issuer. It says either "Legion task
`prod:api:web:server` is fetching" or "a Legion worker node in pool
`default` is fetching".

> **Status:**
>
> - **`nodeIdentity` works.** At fetch time the worker requests a fresh token
>   with the `IssueNodeIdentityToken` RPC and sends it as a bearer
>   credential. This works for sources that accept bearer auth (`https`,
>   `oci`, `git`). `s3://` sources reject it, because the S3 fetcher needs an
>   AWS credential and Legion does not exchange tokens for STS credentials.
> - **Task `identity` does not work yet.** The spec field and admission
>   validation exist, but the worker does not request task tokens for
>   artifact fetches. A fetch using `identity` fails with an error
>   suggesting `auth.kv` or anonymous access. `assumeRole` is accepted with
>   `identity` but is never used.
> - **Pinning needs KV or anonymous auth.** The manager resolves pins only
>   with `kv` or no auth. A `resolve = "pin"` submit on a mutable source that
>   uses `identity` or `nodeIdentity` fails and suggests `resolve = "always"`.

### Security properties

- Credential values never appear in logs, traces, allocation rows, or error
  strings. Errors and status rows name the auth source (`kv:
  secrets/ghcr-pull`, `identity: aws`, `nodeIdentity: artifact-pull`,
  `anonymous`), never the values. The `Credential` type's `Debug` impl is
  redacted.
- Sources are redacted everywhere they appear in status and errors. Userinfo
  and the whole query string are stripped, because presigned URLs carry
  secrets in query values. HTTP client errors have their URLs stripped for
  the same reason.
- Git credentials are passed through environment-scoped git config, never
  argv.
- `allowedSchemes` is enforced at admission **and** on the worker
  (`artifacts.fetch.allowedSchemes`).

## Pinning: `resolve = "pin"` vs `'always'`

Mutable references (OCI tags and git branches or tags) default to
`resolve = "pin"`:

- At submit, the manager calls the fetcher's `resolve()` (manifest-digest
  lookup for OCI, `git ls-remote` for git). It stores the **pinned** source
  (`oci://…@sha256:…`, `git::…?ref=<sha>`) plus a `pinnedFrom` annotation
  with the original reference. Every allocation of that job config fetches
  the same bytes, including reschedules days later.
- `job plan` reports what would be pinned
  (`task api: pinned oci://ghcr.io/acme/api:v42 -> sha256:def…`) and diffs
  it against the stored config
  (`task api: artifact oci://ghcr.io/acme/api:v42 moved: sha256:abc… ->
  sha256:def…`), so you can see a moved tag before you submit. The UI's
  declared-artifacts table shows each pinned digest with the tag it resolved
  from.
- **Re-submitting resolves again.** This is how you roll forward: push a new
  image to the tag, re-run `job run`, and the rollout ships the new digest.
- Resolution uses the same auth chain a worker would. The manager reads KV
  credentials directly from its own store. If resolution fails (bad
  reference, unreachable registry, identity-authed source), the submit fails
  with the fetch error and suggests setting `resolve = "always"`.

`resolve = "always"` passes the reference through unchanged. Each staging
fetches whatever the tag or branch points at at that moment. Use it to track
a tag. The cost is that allocations of one job config may run different bytes
if the reference moves mid-rollout.

Sources that are already immutable need no resolution, and `resolve` is
ignored for them. These include anything with a `checksum`, OCI `@sha256:…`
digests, and git full-SHA refs.

## Staging mechanics

Staging happens on the worker, between receiving the allocation and the
plugin's `prepare()`/`start()`:

1. Tasks without artifacts skip staging entirely. There is no extra step and
   no extra RPC traffic.
2. Tasks with artifacts are registered with health
   `healthy: false, status: "staging artifacts"` and handed to a **dedicated
   staging thread**. Fetches can take minutes, and worker heartbeats keep
   flowing the whole time.
3. Each artifact is fetched into a `.staging/` temp area inside the task
   directory and hashed while it streams. A checksum mismatch discards the
   bytes and fails the attempt, so corrupt bytes never reach the task
   directory.
4. Archives unpack with path-traversal protection and a decompressed-size
   budget. Absolute entry paths, `..` escapes, and symlink or hardlink
   targets that escape the extraction root are all rejected. Recognized
   formats: `.tar.gz`, `.tgz`, `.tar`, and bare `.gz`.
5. Content moves into `dest`. A collision with an existing file is an error,
   not an overwrite. The temp area is always cleaned up.
6. The task starts with `${artifact.<name>}` interpolated and
   `LEGION_ARTIFACT_<NAME>` set.

Staging is **all-or-nothing per task**. The first artifact that fails (after
its retries) fails the whole allocation. Each fetch retries with backoff and
has an overall deadline and size cap. You can configure these per worker
type:

```nickel
# configs/workers/default.ncl — values shown are the defaults
{
  workerType = "default",
  pool = "default",
  artifacts = {
    fetch = {
      parallelism = 4,           # concurrent fetches per worker
      attempts = 3,              # per-artifact tries before the allocation fails
      timeout = "10m",           # overall fetch deadline per artifact
      maxArtifactSize = "50GiB", # per-artifact cap; also bounds decompression
      # allowedSchemes = [...]  # worker-side allowlist (defaults to the cluster default)
    },
  },
}
```

Workers without an `artifacts` block get these defaults. The settings are
delivered through the existing worker pre-registration config flow.

> **Failure semantics: fetch failures use up your `on_unhealthy` budgets.**
> A staging failure (404 after retries, checksum mismatch, timeout, oversize,
> missing credential) is an **allocation failure**. It is reported through
> the same heartbeat path as a crashed task, and the manager handles it with
> the existing `on_unhealthy` restart/move logic. So a flaky registry or
> object store uses up the task's restart and move budget, the same way an
> unhealthy task does. For jobs that pull from less reliable origins, size
> `on_unhealthy.restart_count`/`move_count` with that in mind. There is no
> separate `onFetchFailure` policy yet to treat a registry outage
> differently from an unhealthy task.

### Restart reuse — no refetch when nothing changed

A fully successful staging run writes a manifest (`.staged.json`) into the
task directory. It records the artifact list's fingerprint and every staged
payload. When the same allocation stages again with an unchanged artifact
list, the worker reuses the staged content instead of fetching from origin.
This happens when a worker restart re-dispatches the allocation, or when an
in-place config update didn't touch `artifacts`. Reuse is all-or-nothing. The
worker does a full fresh fetch when:

- The artifact list changed in any way. The fingerprint covers the
  normalized, pinned specs, so a moved pinned digest triggers a refetch.
- Any recorded payload path is missing from the task directory.
- Any artifact sets `resolve = "always"`. That setting asks for a fresh
  resolve at every staging, so it disables reuse for the whole task.
- The previous run never finished. No manifest is written until every
  artifact has staged, and partial content is cleared before the fresh
  fetch.

Reuse applies only on the same node, per allocation. Allocations moved to
another node always fetch fresh, and explicit stops and removals delete the
task directory. Reuse *across* allocations on one node is the job of the
node content store, below.

### The node content store

Each worker keeps one content-addressed store at
`<worker state dir>/content/`. Blobs live at `sha256/<hex>`, named by the
sha256 of their bytes, with a small sqlite index beside them. It is on by
default.

When an artifact declares a `checksum` and comes from an `http(s)`, `s3`, or
`file` source, staging goes through the store:

- **Hit.** The worker copies the blob into the task directory and applies
  `dest`, `mode`, and `unpack` exactly as a fresh fetch would. Nothing is
  fetched. The copy is a clone on filesystems that support it (APFS, btrfs,
  XFS), and the copy is verified against the digest before it is used.
- **Miss.** The worker fetches from origin as usual. After the checksum
  verifies, it copies the bytes into the store, so the next allocation on
  this node is a hit.
- **Concurrent misses share one download.** Two allocations staging the same
  digest at the same moment wait for each other, and the second is a hit.

So a second allocation of a digest-pinned artifact on the same node does not
touch the origin. That includes scale-outs, restarts after a reschedule back
to the node, and new versions of a job that kept the same artifact.

**Container images** go through the store too, one blob per manifest, config,
and layer, whether they were staged as an `oci://` artifact or pulled by the
`container` or `sandbox` runtime itself. A digest reference the node holds
needs no registry request; a tag costs one `HEAD` to check where it points,
and a moved tag downloads only the layers the node lacks. The details, and
how a registry outage is handled, are in
[docs/task-runtime-container.md](task-runtime-container.md#image-cache).
ORAS artifacts (`oci://` with a `mediaType`) are not cached yet.

Tasks always get their own copy. A task that writes to its staged file cannot
change what the next allocation receives. A blob that fails digest
verification (disk damage, someone editing files under the state dir) is
dropped from the store, and the artifact is fetched from origin instead.
Storing is best effort: if the store is full of pinned content or the disk is
low, the allocation still stages from its fetched bytes and the worker logs
`Could not cache artifact in node content store`.

The store keeps a separate byte budget for each **class** of content:

- `artifacts`: cached artifacts.
- `snapshots`: memory snapshot parts (VMM memory and device state, and the
  guest root disk). A local snapshot's parts are pinned to it until it is
  deleted. A published snapshot's parts are unpinned on every node once the
  upload finishes, so they are evicted like any cached entry (see
  [suspend and resume](suspend-and-resume.md#durability)).

A class over budget evicts its own least-recently-used entries and never
touches the other class, so a large memory snapshot cannot push every cached
artifact off the node. Content can also be **pinned** by an owner, such as a
snapshot. Pinned content is never evicted, expired, or removed, and it still
counts toward its class budget. A class whose budget is entirely pinned
refuses new content with an error naming the budget to raise.

Configure the store per worker type:

```nickel
# configs/workers/default.ncl
{
  workerType = "default",
  content = {
    enabled = true,          # false stages every artifact straight from origin
    budgets = {
      artifacts = "10%",     # cached artifacts: a share of the disk or a size
      snapshots = "50GiB",   # checkpoint parts
    },
    ttl = "72h",             # unpinned content unused this long is removed. "0s" disables
    minFreeDisk = "5GiB",    # evict, then refuse, rather than drop below this
  },
}
```

Each budget is either a fixed size (`"50GiB"`) or a share of the filesystem
that holds `<worker state dir>/content/` (`"20%"`, above 0 and at most
100). The worker sizes a share once, when it starts, against that
filesystem's total size, and logs the result (`Sized node content store
budgets`). A disk that grows or shrinks takes effect at the next worker
start.

Without a `budgets` entry, a class takes its default share, clamped so that
very small and very large disks still get a sensible budget:

| Class | Default | Clamp | 8 GiB disk | 100 GiB disk | 1 TiB disk |
| --- | --- | --- | --- | --- | --- |
| `artifacts` | 10% | 1 GiB to 20 GiB | 1 GiB | 10 GiB | 20 GiB |
| `snapshots` | 20% | 2 GiB to 200 GiB | 2 GiB | 20 GiB | 200 GiB |

The artifact cache caps at 20 GiB because a miss costs one refetch, so a
bigger cache buys little. The snapshot budget caps at 200 GiB so a node with
a multi-terabyte data disk keeps most of it for volumes unless you ask for
more. A share you set yourself (`snapshots = "40%"`) is not clamped. On a
small disk the floors can add up to more than 30% of it, and then
`minFreeDisk` is what protects the node: the store evicts unpinned content,
then refuses new content, before free space drops below it, and the
scheduler counts it when it checks a node's
[checkpoint room](suspend-and-resume.md#the-checkpoint-disk-budget).

Every field is optional. A malformed size, percentage, or duration fails
manager startup, not the worker. So do shares that add up past the disk:
`artifacts = "90%"` beside the default 20% for snapshots fails with
`content.budgets.artifacts (90%) and content.budgets.snapshots (20%, the
default) together claim 110% of the disk`. If a worker cannot read the size
of its content filesystem, it cannot size a share, so it logs `Cannot size
the node content store` and runs without a store. Set fixed sizes on such a
platform. The worker runs garbage collection every 10 minutes (TTL
expiry, plus trimming a class whose budget was lowered across a restart).
Puts evict on demand, so the store never grows past a budget between passes.
Deleting `<worker state dir>/content/` while the worker is stopped is always
safe for artifacts. Everything refetches.

Each artifact's status row reports the outcome in `cache`: `"hit"` when it
came from the store, `"miss"` when it was fetched and stored. The field is
absent for artifacts the store cannot serve.

### Content locality

A store only helps when work lands on the node that holds the bytes. So
workers tell the manager what their store holds, and the scheduler prefers
nodes that already hold what a placement needs.

**What workers advertise.** Every heartbeat carries the store's changes
since the last one the manager acknowledged: digests added and removed,
with sizes. Only entries of at least 1 MiB are advertised. Smaller content
downloads in well under a heartbeat, so it is not worth steering placement
for. A worker process sends its full set on its first heartbeat, then
deltas. An unchanged store sends an empty report.

**What the manager keeps.** The manager folds the reports into an index of
`digest -> {node: bytes}`. The index lives in memory, like node liveness,
and is never written to the state store or replicated. Each report carries
an epoch. When a manager cannot apply a report, because it just started, it
just became leader, or a heartbeat reply was lost and the epochs disagree,
it asks the worker for a full report, which arrives on the next heartbeat.
Until a node's report lands, that node scores 0 on locality. Losing the
index costs placement quality for a heartbeat or two, never correctness. A
node silent for two minutes is dropped from the index.

**How placement uses it.** A placement's *content set* is the digests it
needs on its node: the task's artifacts that declare a `checksum`, plus the
memory parts of a snapshot the instance restores from (a resume, a restore,
or a fork). For each candidate node:

```
locality = bytes of the content set the node holds / total bytes of the set
```

The total counts the content the index can size: content no node holds and
no caller sized cannot tell nodes apart, so it is left out. The score joins
the placement score with weight `schedulerSettings.weights.locality`
(default `1.0`, the same scale as `affinities`). A placement whose task has
no digest-pinned artifacts skips the term, and pays nothing for it. Set the
weight to `0` to turn locality off:

```nickel
# manager.ncl
{
  schedulerSettings = {
    weights = { locality = 0.5 },   # default 1.0; 0 disables
  },
}
```

Locality is a preference. It never excludes a node, and it competes with
the other score terms: a node that holds every byte can still lose to a node
that satisfies an affinity, or to an emptier node under `spread`. To see how
often placements land warm, watch
`legion_scheduler_locality_placements_total` (see
[metrics.md](metrics.md#scheduler--eval-loop)).

**Seeing what a node holds.** `legion node content <node>` (the
`GetNodeContent` RPC, the `legion_node_content` MCP tool, and the worker
page's "Content and locality" card) reads the same index the score reads:

```console
$ legion node content w-04
Node w-04  publish: yes  checkpoint: yes
Advertises 3 entries, 574 MiB (epoch 42)

 DIGEST               MiB  SNAPSHOT               ROLE      HOLDERS
 sha256:3f9a0c21d4e5  480  snap-90d1e90d1e90d1e9  memory    3
 sha256:aa00aa00aa00  64   -                      -         5
 sha256:bb11bb11bb11  30   snap-90d1e90d1e90d1e9  vm-state  2

 SNAPSHOT                       JOB         KIND    STATE      CAPTURED HERE  PARTS HERE
 golden (snap-90d1e90d1e90d1e9)  workspaces  memory  published  no             3/3
```

Each entry names the snapshot and part role it belongs to when the manager
knows one, and how many nodes advertise the same digest. The snapshot list
covers what the node captured and every published snapshot it holds parts
of (fetched for a resume or fork, or warmed). It needs `node:read` on the
node's pool, and snapshot rows also need `snapshot:read` on their job.
Because the index is soft state, the reply says `index_reported: false`
when this manager holds no report from the node (it just started, the node
has no store or an older worker, or the node reports to another manager).
It lists entries of 1 MiB and up only, and carries no per-class budgets:
the index records digests and sizes, not classes.

### Publishing content

A worker can also publish content from its store to a shared store. Every
published part is sealed (encrypted) first. Configure it per worker type with
`contentStore`:

```nickel
# configs/workers/default.ncl
{
  workerType = "default",
  contentStore = {
    # s3://bucket/optional/prefix, or file:///shared/dir for a shared mount.
    # Takes the same ?region= and ?endpoint= query parameters as s3 artifacts.
    url = "s3://legion-content-usw2/",
    # Optional. Omit it to use the node's ambient AWS credentials (the AWS_*
    # key variables, web identity, an ECS task role, then the instance
    # profile). Or read an AWS credential object from KV, the same shape
    # artifact `auth.kv` takes:
    auth = { kv = "legion/content-store" },
    # Optional. The size published parts are cut into, and how long chunks
    # are shared between publishes (see "How published parts are stored").
    chunkSize = "1MiB",
    dedupWindow = "24h",
  },
}
```

**Ambient credentials.** With no `auth`, every client of the store (the
publisher, [session manifests](session-mode.md), a restoring node's fetches,
and the manager's [archive](suspend-and-resume.md#archiving-idle-instances) reads)
signs as the node, resolved in the order
[streaming persistence](persistence-streaming.md#credentials) uses: the
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`)
variables, then web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` +
`AWS_ROLE_ARN`, which EKS IRSA injects), then an ECS task role
(`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), then the instance profile. A
configuration that would quietly fall through to the instance profile, a
different and usually broader principal, fails the operation instead and
says why: half a key pair, one of the two web-identity variables without the
other, or EKS Pod Identity (`AWS_CONTAINER_CREDENTIALS_FULL_URI`), which is
not supported:

```text
AWS_WEB_IDENTITY_TOKEN_FILE is set but AWS_ROLE_ARN is not; web identity needs
both, and without them object storage would fall back to instance metadata
```

**Region.** An `s3://` store's region is the URL's `?region=`, then the KV
credential's `region`, then the node's `AWS_REGION` (or `AWS_DEFAULT_REGION`),
then `us-east-1`. Publishing and reading resolve it the same way, so a part is
read in the region it was published to. The node's other `AWS_*` store
settings (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, server-side encryption, proxy
options) apply too. A KV credential's keys are used as written: the node's
own `AWS_*` key variables never mix into them.

A `file://` store is written through `object_store`'s local filesystem: each
object is written beside its final name as `<name>#<n>` and renamed into
place, so a reader never sees a partial object. An interrupted write can
leave a `#<n>` file behind. It is never read or listed as an object.

Published content is cut into chunks (`chunkSize`, 1 MiB by default), and
each distinct, non-zero chunk lands once at `<url>/sealed/<name>`, beside a
sealed index per part that lists them. Every object is encrypted with
AES-256-GCM under keys derived from the cluster's checkpoint key (see
[published checkpoint encryption](suspend-and-resume.md#published-checkpoint-encryption)).
A chunk's key and name are a keyed function of its digest, its job, and
the current dedup window (`dedupWindow`, 24 hours by default), so
publishing is idempotent and incremental: chunks the destination already
holds in the window are not uploaded again, and zero chunks never are (see
[How published parts are stored](suspend-and-resume.md#how-published-parts-are-stored)).
The worker hashes the bytes as it reads them, so corrupt bytes are never
published, and it records the published URL per digest in its store's
index. Chunks upload 16 at a time, so a multi-gigabyte part never sits in
memory. There is no plaintext mode. A `chunkSize` outside 64 KiB to
16 MiB, or not a multiple of 4 KiB, and a `dedupWindow` under a minute, fail
manager startup like any malformed `contentStore`.

Reads never use the publisher. Any worker reads a part's index from its
`s3://` or `file://` URL, opens it with the key the manager sent along with
the work, fetches and opens the chunks it lists, and checks the reassembled
plaintext against the digest before it lands in that worker's own store. Pointing each region's
workers at their own bucket is how region locality works: Legion does not
replicate between buckets.

**Which credential a reading node uses.** A published part is read with the
*reading* node's own `contentStore` credential, never the publisher's, and
never a cluster `artifacts.auth` matcher (those apply to jobspec artifacts
when the job is submitted, and published part URLs never pass through
submission):

- A part URL under the reading node's `contentStore.url` (same bucket, same
  `endpoint`, under its prefix) is fetched with that node's `contentStore.auth`:
  the KV credential is read at fetch time, so a rotated credential is picked up
  on the next fetch, and with no `auth` the node's ambient AWS credentials sign
  the request. A `file://` store needs no credential.
- Any other part URL is fetched anonymously, which a private bucket refuses.
  The error names the store and says the node has no credential for it:

  ```text
  this node has no credential for content store s3://legion-content-use1/:
  a restoring node signs part fetches only for URLs under its own
  contentStore.url (here: s3://legion-content-usw2/), so this fetch was
  anonymous. Give this node the same contentStore url and auth as the node
  that published the snapshot: fetching s3://legion-content-use1/sealed/…:
  fetching part index … after 3 attempt(s): … 403 Forbidden
  ```

So every node that may restore a snapshot needs the same `contentStore` (url
and auth) as the node that published it. A node reads only its own store, so
a snapshot published to one region's bucket cannot be restored on a node
configured for another region's bucket. Constrain the resume to the
publishing region instead.

A malformed `contentStore` (an unsupported scheme, a bucket-less URL, `auth`
on a `file://` store) fails manager startup. A worker whose content store is
disabled logs that it cannot publish and carries on.

**What publishes today: durable checkpoints.** A suspend with
`suspend.durability = "store"`, and `legion snapshot create --publish`, have
the capturing worker publish each memory part of the checkpoint here once it
is captured. Another node then restores it by fetching each part's index and
chunks, verified by digest, into its own content store (see
[suspend and resume](suspend-and-resume.md#durability-surviving-the-node)).
A worker with a usable `contentStore` advertises the `content-publish`
capability when it registers, and `instance suspend` with `durability =
"store"` refuses a node without it. Publishing reads only from the node
content store, so `content.enabled = false` turns publishing off too.

Published parts hold guest memory, secrets included, so they are only ever
stored sealed. Restricting the bucket to your workers is still good practice,
since object sizes are visible to anyone who can list it. Legion never
deletes from the store, because one chunk can belong to many checkpoints.
Expire objects under `sealed/` with a bucket lifecycle rule older than your
longest-lived checkpoint plus `dedupWindow`: a publish reuses only chunks
uploaded in its own window, so that rule never removes a chunk a live
checkpoint needs.

**Warming.** `legion snapshot warm <snapshot> --constraint key=value` asks
matching nodes to fetch a published snapshot's parts into their stores ahead
of time, so the location index (and `locality`) prefers them. It is best
effort and never blocks placement (see
[Warming nodes](suspend-and-resume.md#warming-nodes)). Artifacts have no warm
command yet.

## Status surfaces

Each artifact moves through `pending` → `fetching` → `verifying` → `staged`,
or ends in `failed`. Workers report per-artifact rows inside the existing
heartbeat (`AllocationHealth.artifact_states`, a JSON array). The manager
stores them on allocation health and serves them everywhere allocation health
already appears:

```nickel
[
  { "name" = "tool",  "source" = "https://releases.acme.io/embed/v1.4.tar.gz",
    "state" = "staged", "digest" = "sha256:9f86…", "attempts" = 1, "durationMs" = 412,
    "cache" = "miss" },
  { "name" = "model", "source" = "oci://registry.acme.io/models/embedder@sha256:abc…",
    "state" = "fetching", "attempts" = 1, "bytesFetched" = 1610612736, "bytesTotal" = 4294967296 },
  { "name" = "cfg",   "source" = "s3://acme-builds/cfg.tar.gz",
    "state" = "failed", "attempts" = 3, "error" = "… (credentials from kv secrets/build-store)" }
]
```

Sources are redacted. Errors name auth sources, never values.

- **CLI:** `legion job rollout status --id <ID> --json` includes
  `artifact_states` on every allocation. The ledger (`GetLedger`) has the
  same field per allocation, so agents and humans see the same data.
- **UI:** the Jobs page task view shows a declared-artifacts table (name,
  source, checksum or pinned digest with its `pinned from` origin, resolve
  mode, auth source, dest). Allocation cards on the task, rollout, and node
  views show live staging rows with a state badge, progress percentage, and
  failure reason. Rollout cards show a summary such as `staging artifacts:
  1/3 staged` while a deploy is mid-fetch.
- **Event stream:** artifact lifecycle changes appear in the
  [scheduler event stream](event-stream.md) (`legion events list --kind
  artifact`, `legion events tail`, the UI Events page):
  - `artifact.staging` fires when an allocation enters staging.
  - `artifact.staged` fires once, when every artifact has landed (digests,
    bytes, and durations in metadata).
  - `artifact.failed` fires per failed artifact, with the redacted source,
    error, and attempt count. It is sent with the failure report, so it
    survives the allocation's removal.
  - `artifact.pinned` records submit-time digest pinning.

  The manager derives the staging and staged events from changes between
  heartbeat snapshots, so steady-state heartbeats add nothing to the
  journal.

## Task-runtime support

| Runtime | artifacts | consumption |
| --- | --- | --- |
| `exec` | ✅ | File/Tree payloads staged into the task dir. `${artifact.*}` interpolation. `LEGION_ARTIFACT_*` / `LEGION_TASK_DIR` env. `cwd` defaults to the task dir. Landlock grants access to it automatically. |
| `container` | ✅ (image behind a flag, file/tree always) | The implicit `image` artifact is consumed as an OCI layout (below). File and tree artifacts stage into the host task dir, which is bind-mounted at `/legion/task`, so the container reads them there. `LEGION_ARTIFACT_<NAME>` and `${artifact.<name>}` both hold the in-container path. |
| `sandbox` | ✅ | The implicit `image` artifact becomes the microVM's root filesystem. File/tree/layer artifacts are copied **into the guest** at `/legion/artifacts/<name>` while the root disk is built. `LEGION_ARTIFACT_*` and already-interpolated `${artifact.*}` paths are rewritten from host staging paths to their in-guest locations (see [task-runtime-sandbox.md](task-runtime-sandbox.md)). |
| `noop` | ❌ | Specs with artifacts are rejected at validation and admission. |

New runtimes add artifact support through three optional `TaskPlugin` hooks:

- `supports_artifacts()`: the capability check.
- `implicit_artifacts()`: turn runtime config into artifacts at admission.
- `consume_artifact()`: handle non-file payloads such as OCI images or ORAS
  layers.

Runtimes that only need files in the task directory implement just the
first.

### Container image desugaring (`artifacts.containerImageDesugar`)

With the cluster flag on, the manager turns a container task's `image` field
into an implicit `oci://` artifact named `image` at admission. Docker Hub
shorthand expands: `alpine:latest` → `oci://docker.io/library/alpine:latest`.
The worker pulls it through the shared pipeline, with the same auth matchers,
pinning, and status rows as every other artifact. The plugin then loads the
staged OCI layout into the local runtime:

- **macOS (Apple `container`):** the layout is tarred and imported with
  `container image load`, so the subsequent run needs no registry pull.
- **Linux (runc or crun):** the layout is converted to an OCI
  runtime bundle (rootfs extraction plus a generated `config.json`) and run
  with `<runtime> run --bundle`. See
  [task-runtime-container.md](task-runtime-container.md#linux-image--bundle).

The flag defaults to **off**. On Linux it does not change how often an image
is downloaded: with it on or off, the worker pulls through the
[node content store](#the-node-content-store), so a node downloads each
layer once and a tag costs one `HEAD` request per start (Apple `container`
pulls for itself when the flag is off). What the flag adds is the artifact
pipeline's auth matchers, digest pinning, and status rows.

## Fetcher plugins

Dynamic fetcher plugins add schemes beyond the built-ins. They use the same
dylib pattern as task-runtime plugins (see
[docs/plugin-system.md](plugin-system.md)):

```
plugins/artifacts/<scheme>/
  plugin.json                              # {"kind":"artifacts","name":"<scheme>","legionApiVersion":2}
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

// Required: lets the host refuse a mismatched build (docs/plugin-system.md).
legion::declare_plugin_abi!();

#[no_mangle]
pub extern "C" fn get() -> Box<dyn ArtifactFetcher> {
    Box::new(MyFetcher)
}
```

Resolution order is **built-ins first, then plugins**. When no built-in
serves a scheme, the registry looks for
`plugins/artifacts/<scheme>/target/release/lib<scheme>.<ext>`. It validates
the manifest, loads the dylib, and checks that the fetcher reports the
requested scheme. Everything after the fetch (auth resolution, checksums,
unpack, retries, status reporting) is shared pipeline code. The plugin only
moves bytes. Fetchers are blocking. They run on staging threads, never the
async runtime, which keeps async-trait problems away from the dylib
boundary. Optionally implement `resolve()` to support `resolve = "pin"` for
mutable references.

A complete example ships at [plugins/artifacts/demo](../plugins/artifacts/demo/).
It is a `demo://` fetcher that writes the URL's `content` query parameter as
a staged file: `artifacts = ["demo://example/hello.txt?content=hi"]`.

Remember the allowlists. Add a custom scheme to the cluster's
`artifacts.allowedSchemes`, and also to the worker's
`artifacts.fetch.allowedSchemes` if you have customized it.

## Limitations

- **The node content store caches digest-pinned single files and container
  images.** A file artifact needs a `checksum` and an `http(s)`, `s3`, or
  `file` source; an `oci://` image is cached per blob. Git trees, `file://`
  directories, ORAS layers, and artifacts without a `checksum` fetch from
  origin for every new allocation. There is
  no TTL-based revalidation of mutable URLs, so a plain URL without a
  checksum is never served from the store. Add a `checksum` to opt in.
- **No pre-warming for artifacts.** The store fills only when an allocation
  stages, or when `snapshot warm` pulls a published snapshot's parts. There
  is no artifact pre-pull and no way to list or clear the store from the CLI.
  Each node's store is its own: a new node always fetches from origin.
- **Locality covers digest-pinned artifacts only.** Container image layers
  are not in the content store, so images do not count toward a placement's
  content set.
- **The location index is per manager.** Each manager indexes the nodes
  whose heartbeats it serves. In a multi-manager cluster, nodes that
  heartbeat a manager other than the one scheduling score 0 on locality.
  Placement stays correct and only loses the preference.
- **`contentStore.auth.nodeIdentity` is rejected.** S3 uploads need an AWS
  credential, and node identity tokens are not exchanged for one. Use
  `auth.kv` or the node's ambient credentials.
- **EKS Pod Identity, `~/.aws/credentials` profiles and SSO are not
  ambient sources.** `object_store`'s AWS chain stops at the four sources
  above. Pod Identity is refused rather than downgraded to the node's
  instance role. On EKS use IRSA, or `auth.kv`.
- **`s3://` is the only object-store backend.** There is no GCS-native or
  Azure path; S3-compatible endpoints (R2, MinIO, GCS interop) work.
- **Checksums verify single-file payloads** (and, for OCI, the manifest
  digest). Declaring a `checksum` on a source that produces a tree (git,
  `file://` directories) is a staging error, not a silent skip.
- **Task `identity` auth fails at fetch time**, and `assumeRole` is never
  used. `nodeIdentity` works for bearer-auth sources but not `s3://`.
  Neither can be used with `resolve = "pin"`.
- **Cluster auth matcher changes apply at the next submit**, not to running
  jobs, because the effective auth is stored in the normalized spec at
  admission. Rotating a KV credential value applies immediately.
- **The `sandbox` runtime consumes every payload.** The `Image` becomes the
  guest root filesystem, and File/Tree/Layers are copied into the guest root
  disk. It builds its own filesystem, so it has somewhere to put them.
- **Container runtimes consume only the `Image` payload** (macOS imports it
  into Apple `container`, Linux converts it to an OCI runtime bundle), and
  image desugaring is off by default. File and tree artifacts need no extra
  handling. They stage into the task directory, which the runtime mounts at
  `/legion/task` (see
  [task-runtime-container.md](task-runtime-container.md#the-task-directory-legiontask)).
  An artifact staged outside the task directory would not be visible inside
  the container, so its `LEGION_ARTIFACT_*` var is left out instead of
  pointing at a host path the container can't see.
- **Archive formats** are `.tar.gz`/`.tgz`/`.tar`/`.gz`. `zip` and `zstd`
  are not recognized yet. Use `unpack = "file"` to stage such files as-is.
- **Fetch failures use up `on_unhealthy` budgets** (see
  [Failure semantics](#staging-mechanics)). There is no dedicated
  `onFetchFailure` policy yet.
- **Windows staging** has not been specifically tested.
