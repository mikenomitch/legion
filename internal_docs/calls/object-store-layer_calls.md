# Judgement calls: one object-storage layer

No plan: this is a refactor of how Legion opens object stores, with no new
user-facing surface. The layer is `src/storage/objects/` (`mod.rs`: schemes,
`StoreUrl`, `ObjectCredential`, `OpenOptions`, `open`/`open_s3`; `aws.rs`:
principal resolution and the one `AmazonS3` builder). Its consumers are the
`contentStore` publisher (`src/content_publish.rs`), session manifests
(`src/session_manifest.rs`), the `s3://` artifact fetcher
(`src/artifacts/fetchers/s3.rs`), the manager's archive reads
(`src/manager/archive.rs`), and streaming persistence's replica, restore and
lease (`src/storage/persistence.rs`).

The failure class it removes: three S3 client builders that each resolved
credentials and options their own way. Two bugs came from it before this
change (`8f1078a`: the lease store had no conditional put, so the lease could
never be acquired on real S3; `bb86470`: restore fetches did not sign with the
node's `contentStore` credential), and the unification found three more,
recorded below (#3, #4, #8).

Entries are ranked by project impact, highest first; the number is the rank.

## 1. Every object store resolves its principal through streaming persistence's strict chain

**The call.** There is one resolver, `objects::resolve_principal`, and every
consumer calls it. An `ObjectCredential` is `Anonymous`, `Ambient` or explicit
`Aws` keys; `Ambient` resolves in the order streaming persistence already
used (`AWS_*` keys, web identity, an ECS task role, instance metadata), with
its refusals: half a key pair, one web-identity variable without the other,
and EKS Pod Identity fail instead of falling through to the instance profile.
That now applies to a worker's `contentStore` (publishing, session manifests,
restore fetches signed with `Credential::AwsAmbient`) and to the manager's
archive reads, which used `AmazonS3Builder::from_env` and fell through
silently. A worker whose pod has `AWS_ROLE_ARN` but no token file now fails
its publish with:

```text
AWS_ROLE_ARN is set but AWS_WEB_IDENTITY_TOKEN_FILE is not; web identity needs
both, and without them object storage would fall back to instance metadata
```

where it used to publish as the node's instance role.

**The alternatives.** (a) Keep strictness per consumer: persistence refuses,
`contentStore` stays lenient (maybe with a warning). That is the
zero-behavior-change version. (b) Make the lenient `from_env` chain the one
path and drop persistence's refusals.

**The trade-off.** A worker or manager that works today only because of the
fall-through (EKS Pod Identity set, the node role happening to have bucket
access) now fails until it is given IRSA or `contentStore.auth.kv`. It fails
loudly, at the first publish or archive read, naming the variable; no
credential *source* was removed, since `object_store` 0.11 never read
profiles or Pod Identity for any consumer. Workers still do not resolve their
principal at boot the way persistence does (that needs a daemon change), so the
failure surfaces on first use, not at startup.

**Why (simplicity, and security).** The point of the layer is that a store
authenticates the same way whoever opens it. (a) would leave exactly the split
this work exists to remove: the same environment would sign as one principal
for the WAL replica and another for checkpoint parts. (b) reopens the hole
persistence closed on purpose (manager-workload-identity calls #5): a silent
switch to a broader principal. The worker's `contentStore` holds guest memory,
so it deserves the stricter rule more, not less. Documented in
`docs/artifacts.md` ("Ambient credentials") and
`docs/persistence-streaming.md` ("Credentials").

## 2. Consumers keep two documented options instead of one merged rule

**The call.** `OpenOptions` carries the two things that legitimately differ
between consumers, and nothing else:

- `region_default`: `Environment` (`AWS_REGION`, then `AWS_DEFAULT_REGION`,
  then `us-east-1`) for a node's own stores, `Fixed("us-east-1")` for the
  `s3://` fetcher's jobspec artifacts (anonymous or KV-signed). The fetcher's
  ambient reads (published parts) take `Environment`, as `bb86470` set up.
- `aws_env_settings`: whether the process's other `AWS_*` store settings
  (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, SSE, proxy, IMDS endpoint) apply, as
  `from_env` applied them. On for `contentStore`, the fetcher and the archive;
  off for streaming persistence, which never read them.

Plus `conditional_put`, which the lease and session manifests set.

**The alternatives.** One rule for everyone: either the AWS-SDK convention
(the environment region applies to every store, explicit keys or not), or
the fetcher's (a fixed `us-east-1`); and either every consumer imports
`AWS_*` settings or none does.

**The trade-off.** Two knobs a reader of the layer has to understand, and a
`contentStore` store and a jobspec artifact in the same bucket can resolve to
different regions when neither names one. Each consumer's existing behavior
is kept exactly.

**Why (simplicity for the operator).** Each merged rule breaks someone
silently. Applying `AWS_REGION` to jobspec artifacts breaks an EKS worker
(where `AWS_REGION` is injected) fetching a public `us-east-1` artifact with
no `?region=`, a case that works today. Importing `AWS_ENDPOINT` into
persistence would redirect the cluster's backups to whatever endpoint the
manager's environment names for something else. Dropping the import for
`contentStore` breaks a MinIO setup configured through `AWS_ENDPOINT` /
`AWS_ALLOW_HTTP`. A named option per difference is the honest record of a real
difference. The fetcher's rule is now stated in `docs/artifacts.md`, Sources
table.

## 3. A restore read signed with a KV credential takes the region the publisher signs for

**The call.** `ContentStoreTarget::read_credential` fills a KV credential's
missing `region` with the node's environment region before handing it to the
fetcher. Before, the publisher (`from_env`) signed a KV-credentialed
`contentStore` in `AWS_REGION`, while the restore fetch of the same object
signed in `us-east-1`. On a node with `AWS_REGION=us-west-2`, a bucket in
`us-west-2`, a KV credential without `region` and a URL without `?region=`,
every publish worked and every restore from the store failed.

**The alternatives.** (a) Leave it and file a TODO, keeping this refactor
behavior-neutral. (b) Change the fetcher's region default for every KV
credential. (c) Carry the store's open options through `FetchRequest`.

**The trade-off.** The fix lives in `read_credential`, so the region rule for
`contentStore` reads is stated in two places (`OpenOptions::default()` for the
writers, the fill for the fetcher). It changes behavior only where publish and
restore disagreed, which was broken.

**Why.** It is the same failure class as `bb86470` (a restore read
authenticated differently from the publish), found by putting the two
builders side by side. (b) breaks jobspec artifacts (#2). (c) changes the
fetcher plugin ABI (`FetchRequest` crosses into dylib fetchers) for one field.

## 4. The manager's archive picks a credential with the worker's rule

**The call.** `archive::credential_for` uses `StoreUrl::covers`, the predicate
`ContentStoreTarget::contains_url` uses on the worker: same scheme, bucket and
`endpoint`, path under the root's at a `/` boundary. The manager used a looser
test of its own (no endpoint check, a string prefix with no boundary), so
`s3://b/content` matched URLs under `s3://b/content-old`, and with roots
sorted by URL, a manifest in `content-old` was read with `content`'s
credential.

**The alternatives.** Keep the manager's own matcher unchanged.

**The trade-off.** A manifest URL that only the looser rule matched (another
endpoint, a sibling prefix) is now read with the manager's ambient
credentials. Manifests are always written at `document_url(root, …)`, which
keeps the root's endpoint and prefix, so no correctly written manifest
changes credential.

**Why (simplicity).** One answer to "is this URL in that store" for workers
and the manager, so a credential is never sent somewhere one side would not
send it. Documented in `docs/suspend-and-resume.md` ("Archiving idle
instances").

## 5. `file://` stays on filesystem code in three places

**The call.** `file://` stores are `object_store`'s `LocalFileSystem` for the
`contentStore` publisher (sealed parts and archive manifests) and the
manager's archive reads, listings and deletes. Three consumers keep their own
filesystem code, each at the boundary of the layer, which still parses and
classifies the URL for them:

- Session manifests keep `FileManifestBackend`, because their
  compare-and-swap needs `PutMode::Update`, which `LocalFileSystem` 0.11 does
  not implement (`local.rs`: `NotImplemented`). Its ETag is a content hash;
  `LocalFileSystem`'s is inode/mtime/size, so the two cannot be mixed in one
  backend either.
- Streaming persistence keeps rustyriver's `FileReplicaClient`, whose
  on-disk layout (`<root>/ltx/<level>/…`) differs from what
  `ObjectStoreClient` writes (`<root>/<level:04x>/…`). Swapping it would move
  existing replicas.
- The `file://` artifact fetcher keeps `std::fs`: an artifact source is any
  local path, directory trees included, and `object_store`'s `Path` refuses
  names a filesystem accepts.

**The alternatives.** (a) Wrap `LocalFileSystem` in an `ObjectStore` that adds
an `flock` compare-and-swap, and run manifests through one backend. (b) Point
rustyriver at `ObjectStoreClient` over `LocalFileSystem`. (c) Fetch
single-file artifacts through the layer and keep `std::fs` only for trees.

**The trade-off.** Five `file://` implementations became three, not one.
Session-manifest CAS on `file://` remains its own code to maintain.

**Why (simplicity).** (a) forks `object_store` behavior inside Legion, the
same kind of hand-mirrored code this work removes elsewhere. (b) changes an
on-disk format, which needs a migration. (c) saves a few lines for a second
code path in a fetcher that has to keep the first.

## 6. A `file://` key is the name as written, and the store is rooted at `/`

**The call.** `StoreUrl::key` builds an `s3://` key with `ObjectPath::from`
(which percent-encodes characters such as `~`, `#` and `%` in each segment,
as every S3 consumer already did) and a `file://` key with `ObjectPath::parse`
under the root, so the object `manifests/a~b/x.json` is that file exactly.
`open` returns `LocalFileSystem::new()`, rooted at `/`, with the directory in
the key.

**The alternatives.** Use `ObjectPath::from` for both, and
`LocalFileSystem::new_with_prefix(root)`.

**The trade-off.** The two backends encode keys differently, which `StoreUrl`
hides but a reader has to know. `parse` refuses a name with an empty, `.` or
`..` segment, which `root.join(name)` accepted (and which no Legion object
name has).

**Why.** The old `file://` code wrote `root.join(name)`; encoding would have
moved any object whose name has one of those characters, and is not
something a shared mount's other readers expect. `new_with_prefix`
canonicalizes the root, so it fails when the directory does not exist yet,
which a fresh store, and the manager reading a mount it does not have, both
hit.

## 7. rustyriver still parses the replica URL; the layer only classifies it

**The call.** `objects::url_or_path` decides whether `replicaUrl` is `s3://`
(handed over raw), `file://` or a bare path (a directory, with `file://`
stripped as text so a relative path stays relative, as before), or an
unsupported `scheme://`. For `s3://`, rustyriver's `ObjectStoreConfig` still
reads bucket, region, endpoint, path style and provider defaults, and the
layer's `open_s3` builds the store from those. An unsupported scheme now fails
the boot:

```text
invalid persistence.streaming.replicaUrl "gs://b/db": scheme "gs" is not supported; use s3:// or file://
```

It used to be taken as a relative directory named `gs:`.

**The alternatives.** Parse the replica URL with `StoreUrl` like every other
store URL.

**The trade-off.** Two parsers still read `s3://` URLs: `StoreUrl` and
rustyriver's dialect. They agree on the common form; rustyriver also accepts
access-point ARNs, `bucket.host:port` hosts, `forcePathStyle`, `skipVerify`
and `LITESTREAM_S3_ENDPOINT`, none of which `StoreUrl` does.

**Why (extensibility at the boundary).** Folding rustyriver's dialect into
`StoreUrl` would fork its behavior inside Legion, and dropping it would break
replica URLs that work today. The scheme decision, which is what the call
sites used to repeat, is in one place.

## 8. The builder starts from `AmazonS3Builder::new()`, not `from_env()`

**The call.** `aws::build` starts from `new()`, imports the non-credential
`AWS_*` settings itself when `aws_env_settings` is on, and sets the principal
explicitly. Under `from_env`, a KV credential without a `sessionToken` was
signed with the process's `AWS_SESSION_TOKEN`, since `from_env` loaded it and
nothing cleared it: on a node with temporary environment keys, every
KV-signed fetch or publish sent a token that does not belong to its keys.

**The alternatives.** Keep `from_env()` and override what each principal
needs.

**The trade-off.** The layer keeps a list of the config keys that belong to
the principal and region rather than to the store (keys, token, ECS URI,
region, bucket), and a key `object_store` adds later is imported by default.

**Why.** `object_store` has no way to unset a builder field, so starting from
`from_env` makes "these keys and nothing else" impossible to express. The
list is small and tested.

## 9. `open` returns `Arc<dyn ObjectStore>`, and the layout lives on `StoreUrl`

**The call.** `open(&StoreUrl, &ObjectCredential, &OpenOptions)` returns only
the client. The layout under the URL (`key`, `name_of`, `object_url`,
`covers`, `lists_in_order`) is on `StoreUrl`, so consumers that open per call
(to pick up a rotated KV credential) and code that only builds URLs share it.
No consumer names `AmazonS3`; the module is `objects`, not `object_store`,
which would shadow the crate. Adding a backend (`gs://`, `az://`) is an
`object_store` Cargo feature, a `Scheme` arm, a `Backend` variant built in
`open`, and an `ObjectCredential` variant for its key shape. No backend was
added.

**The alternatives.** Return `(store, prefix)` as a tuple or an "opened store"
struct; or wrap `ObjectStore` in a Legion trait.

**The trade-off.** A caller has to keep the `StoreUrl` beside the client to
build keys.

**Why (extensibility).** The key layout is a property of the URL, not of a
connection: session manifests and the publisher build keys and URLs before or
without opening anything. A Legion trait over `ObjectStore` would be one more
interface to keep in step with the crate.

## 10. One publisher type, with `file://` uploads going through multipart

**The call.** `FileContentPublisher` and `S3ContentPublisher` became one
`StorePublisher` over `dyn ObjectStore`. A `file://` publish now streams
through `LocalFileSystem`'s multipart upload (16 MiB parts written into a
staging file `<name>#<n>`, renamed on completion) instead of a direct
`std::fs` write to `.<name>.<pid>.tmp`. The idempotence check is a `head`, so
an error other than not-found fails the publish instead of rewriting the
object.

**The alternatives.** Keep a `std::fs` publisher for `file://`.

**The trade-off.** The `file://` path now buffers 16 MiB parts and runs a
private runtime, and its throughput against the old direct write is not
measured (`content_publish::tests::publish_throughput` exists for that). The
staging file's name changed for anyone watching a shared mount.

**Why (simplicity).** Two publishers had drifted already (different
idempotence checks, different temp files). One code path means the sealing,
verification and abort logic that guards against publishing corrupt bytes is
the same for both schemes, and a `file://` test now exercises the code real
S3 publishes run.
