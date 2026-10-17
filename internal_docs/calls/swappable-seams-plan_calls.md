# Judgement calls: swappable seams

Plan: [`plans/swappable-seams-plan.md`](../plans/swappable-seams-plan.md).
The cluster CA half records its own calls in
[`cluster-ca-provider-plan_calls.md`](cluster-ca-provider-plan_calls.md).

Entries are ranked by project impact, highest first; the number is the rank.

---

The object-storage entries came from that layer's own record and are ranked
in with the rest. The layer is `src/storage/objects/` (`mod.rs`: schemes,
`StoreUrl`, `ObjectCredential`, `OpenOptions`, `open`/`open_s3`; `aws.rs`:
principal resolution and the one `AmazonS3` builder). Its consumers are the
`contentStore` publisher (`src/content_publish.rs`), session manifests
(`src/session_manifest.rs`), the `s3://` artifact fetcher
(`src/artifacts/fetchers/s3.rs`), the manager's archive reads
(`src/manager/archive.rs`), and streaming persistence's replica, restore and
lease (`src/storage/persistence.rs`). Putting the three old S3 builders side
by side found three more bugs than the two that motivated it (entries
7, 8, 12).

---

## 1. The attachment is a closed enum, not a trait or a plugin kind

**Call.** How an allocation reaches the datapath is
`netd::attach::Attachment` — `Host`, `Netns`, `GuestVsock`, `GuestTap` —
decided once per allocation and carried by every listener spec. Adding a
capture mode is a new variant and its arms in `listen` and `task_dial`, in
this crate.

**Alternatives.** (a) A `Capture` trait with the four modes as
implementations, so an out-of-tree crate could add one. (b) A datapath
plugin kind (`plugins/datapath/`), the CNI-shaped answer the service-discovery
plan once sketched.

**Trade-off.** Nobody outside the repository can add a capture mode. A CNI
bridge, TPROXY, or a TUN device is still a Legion change, reviewed here.

**Why (extensibility, kept deliberately closed).** The audit's problem was
never that modes could not be added from outside; it was that adding one
inside meant finding ~40 sites. A closed enum turns that into a compile
error at every `match` that misses the new variant, which a trait object
does not. Service-mesh calls entry 5 keeps the datapath plugin deferred, and
nothing here reopens it; the enum is the seam that makes that plugin cheap
when it is wanted, because `Netns { path }` is already the CNI contract.

---

## 2. The worker ↔ datapath boundary is an in-process enum, with no wire encoding yet

**Call.** The worker talks to the datapath through `Netd` and three types:
`NetdInput` (an enum of everything it tells it), `NetdWants` (what to fetch
from the manager), and `NetdReport` (what goes on the next heartbeat).
`NetdInput` derives no `Serialize`; the persisted state is `DesiredNetwork`
plus policy bodies, as before. `ForwarderContext` stays one internal type
behind `Netd`.

**Alternatives.** (a) Ship the `legion netd` process split now, with a Unix
socket protocol. (b) Make `NetdInput` serializable today so the split is
only a transport. (c) A `Datapath` trait with an in-process implementation
and a future client implementation.

**Trade-off.** The split is still work: a transport, framing, supervision,
fd hand-off. Secrets (credentials, KV-held certificates, the mesh key) ride
in `NetdInput` variants, and whoever adds the wire encoding must keep them
off disk — `netd_state.json` must keep holding only `DesiredNetwork` and
bodies. `ForwarderContext` is still large; splitting it is open in TODO.

**Why (simplicity now, extensibility kept).** The split's hard part was
knowing what crosses the boundary; that is now a type the compiler checks.
Serializing it before there is a second process would add derives on
secret-bearing types with nobody to read them, and a trait with one
implementation is ceremony. Service-mesh calls entry 17 deferred the split
for good reasons that still hold.

---

## 3. Every object store resolves its principal through one chain; only streaming persistence refuses an ambiguous one

**Call.** There is one resolver, `objects::resolve_principal_with`, and every
consumer calls it. An `ObjectCredential` is `Anonymous`, `Ambient` or explicit
`Aws` keys; `Ambient` resolves in the order streaming persistence already
used (`AWS_*` keys, web identity, an ECS task role, instance metadata). What
an *ambiguous* environment does — one web-identity variable without the
other, or EKS Pod Identity, which `object_store` cannot use — is the one
per-consumer choice, `OpenOptions::ambient_fallthrough`: streaming
persistence refuses (as it did), and a worker's `contentStore`, artifact
fetches, and the manager's archive reads continue down the chain, as
`AmazonS3Builder::from_env` did, and now log a warning once that names the
variable:

```text
WARN Object storage continues down the AWS credential chain problem="AWS_ROLE_ARN is set but AWS_WEB_IDENTITY_TOKEN_FILE is not; ..."
```

Half a key pair is refused everywhere (`object_store` refused it already).

**Alternatives.** (a) Strict everywhere: the same environment refuses for the
WAL replica and for checkpoint parts. This is what the layer first shipped
with on its branch. (b) Lenient everywhere, dropping persistence's refusals.

**Trade-off.** The same environment can still sign as two principals: a
node whose `AWS_ROLE_ARN` lacks its token file stores checkpoint parts as
its instance role while persistence refuses to start. The warning makes that
visible; it does not prevent it.

**Why (the request, then security).** The work was asked to keep current
functionality intact, and (a) breaks working clusters: an EKS node on Pod
Identity whose node role can reach the bucket publishes checkpoints today
and would stop. (b) reopens the hole persistence closed on purpose
(manager-workload-identity calls #5): a cluster's backups silently signing as
a broader principal. One resolver with one named switch keeps both
behaviors and removes the three hand-written chains. Tightening
`contentStore` later is a one-line default change, recorded in TODO.

---

## 4. A node whose `resolv.conf` names no nameserver answers `SERVFAIL`, not `1.1.1.1`

**Call.** The per-allocation resolvers forward only to the `nameserver`
lines of `/etc/resolv.conf`. With none, every name outside `svc.legion` gets
`SERVFAIL` at once, and the worker warns at start:

```text
WARN /etc/resolv.conf names no nameserver; allocation resolvers will answer SERVFAIL for every name outside svc.legion
```

Before, the resolver silently forwarded to `1.1.1.1:53`.

**Alternatives.** (a) Keep the fallback and document it. (b) A worker config
field for upstream resolvers, with `1.1.1.1` as its default.

**Trade-off.** A node with an empty `resolv.conf` that "worked" before (by
leaking queries to Cloudflare) now resolves nothing outside the zone until
the operator names a resolver.

**Why (simplicity, and not surprising the operator).** Sending a tenant's
queries to a third party nobody configured is a privacy and egress surprise
on exactly the nodes (air-gapped, locked-down) where it matters most.
`SERVFAIL` fails fast and the warning names the cause. A config field can
come when someone asks for resolvers different from the host's; there is
no installed base to protect (AGENTS.md).

---

## 5. The egress proxy keeps connections alive and judges every request

**Call.** HTTP through the gated egress proxy is served by hyper. Each
request on a client connection — kept alive or pipelined — is judged on its
own and sent upstream on a connection of its own, in origin form.

**Alternatives.** Keep one request per client connection: answer the first,
send `Connection: close`, and refuse anything pipelined behind it. That is
what the hand-rolled parser intended, and it is simpler to reason about.

**Trade-off.** A client connection now lives as long as the client wants,
so the activity ledger counts it open longer (an idle keep-alive connection
to the proxy keeps a `connections`-signal idle policy awake). The upstream
side still opens a connection per request, so there is no connection reuse
to the origin.

**Why (security, then ease).** The old parser judged the first request and
then spliced the rest of the connection, so a pipelined second request
reached the origin unjudged; only the origin honouring `Connection: close`
stood in the way. Judging per request removes the dependence on the origin.
Keeping the client connection is what every HTTP client expects of a
forward proxy, and refusing it would make pip and npm slower for nothing.

---

## 6. Consumers keep three documented options instead of one merged rule

**The call.** `OpenOptions` carries the things that legitimately differ
between consumers, and nothing else:

- `region_default`: `Environment` (`AWS_REGION`, then `AWS_DEFAULT_REGION`,
  then `us-east-1`) for a node's own stores, `Fixed("us-east-1")` for the
  `s3://` fetcher's jobspec artifacts (anonymous or KV-signed). The fetcher's
  ambient reads (published parts) take `Environment`, as `bb86470` set up.
- `aws_env_settings`: whether the process's other `AWS_*` store settings
  (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, SSE, proxy, IMDS endpoint) apply, as
  `from_env` applied them. On for `contentStore`, the fetcher and the archive;
  off for streaming persistence, which never read them.

Plus `conditional_put`, which the lease and session manifests set, and
`ambient_fallthrough` (entry 3).

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

---

## 7. A restore read signed with a KV credential takes the region the publisher signs for

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
builders side by side. (b) breaks jobspec artifacts (#6). (c) changes the
fetcher plugin ABI (`FetchRequest` crosses into dylib fetchers) for one field.

---

## 8. The manager's archive picks a credential with the worker's rule

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

---

## 9. Policy assignments land with the desired set, before the listeners bind

**Call.** `NetdInput::Desired` applies the attribution identities and the
policy assignments first, then the namespaces, the listeners, and the
endpoint feed. Before, the heartbeat bound the listeners first and applied
assignments later in the tick, after templates and live KV files were
written.

**Alternatives.** Keep assignments a separate input sent where the old code
applied them.

**Trade-off.** One heartbeat tick does the policy work earlier. Nothing in
templates or KV files reads policy, so no behavior depends on the old order.

**Why (simplicity).** A listener bound in the same step as its policy
assignment sees the assignment immediately, so a gated listener spends less
time failing closed on `policy-pending`. One input for "what the manager
said this beat" is easier to carry over a socket later than two.

---

## 10. `file://` stays on filesystem code in three places

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

---

## 11. rustyriver still parses the replica URL; the layer only classifies it

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

---

## 12. The builder starts from `AmazonS3Builder::new()`, not `from_env()`

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

---

## 13. A truncated answer is refetched over TCP only for clients that asked over TCP

**Call.** When a forwarded answer comes back truncated, the resolver refetches
it over TCP only if the client reached it on its TCP listener. A UDP client,
and a gated sandbox guest (whose agent relays UDP queries over a vsock
stream), gets the truncated answer.

**Alternatives.** Always refetch over TCP and return the full answer.

**Trade-off.** A guest's large answer still costs it a retry over TCP to its
own agent, which relays that as a stream too.

**Why (correctness).** A full answer handed back for a UDP client can exceed
the buffer it advertised and be dropped. Answering each transport the way
its client expects is what every resolver does; the guest case is a UDP
client in disguise.

---

## 14. A node that reported no `os` gets no VIP block

**Call.** Placement assigns a VIP block only when the node's `os`
fingerprint is `linux`. A node with no `os` attribute gets none, and its
upstreams keep their loopback ports.

**Alternatives.** Fall back to the manager's own OS when the node reported
none (the old behavior, everywhere).

**Trade-off.** A Linux node whose fingerprint lacks `os` (a simulated node,
or a custom fingerprinter) loses name-is-config until it reports one.

**Why (correctness).** The bug was answering a node question with a manager
fact; keeping that as a fallback keeps the bug for the case it is hardest to
spot. Every real worker reports `os` (`std::env::consts::OS`).

---

## 15. A `file://` key is the name as written, and the store is rooted at `/`

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

---

## 16. `open` returns `Arc<dyn ObjectStore>`, and the layout lives on `StoreUrl`

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

---

## 17. One publisher type, with `file://` uploads going through multipart

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

---

## 18. `hickory-proto` 0.25 with only `std`, for parsing and building

**Call.** The resolver uses `hickory-proto` 0.25.2, default features off,
`std` on, to parse queries and build answers. Forwarding stays our own code
(UDP, and TCP for refetches).

**Alternatives.** 0.26 (the latest); or `hickory-resolver` to do the
forwarding as well.

**Trade-off.** One minor version behind. Forwarding, timeouts and resolver
order stay ours to maintain.

**Why (simplicity).** The parser was the part worth replacing: compressed
names, EDNS, and response building are where hand-rolled DNS goes wrong.
The forwarding is thirty lines whose semantics (first answer wins, policy
before forwarding) we want explicit. 0.25's builder API is the one the code
was written and tested against.

---

## 19. `dial_tcp` sets no socket options

**Call.** `netd::listen::dial_tcp` is connect-with-timeout and nothing else.
Callers keep the options they set before (`set_nodelay` on HTTP-mode and
ingress dials, nothing on L4 splices).

**Alternatives.** Set `TCP_NODELAY` on every datapath dial in one place.

**Trade-off.** The seam does less than it could.

**Why (scope).** A behavior change on every spliced connection belongs with
the datapath performance work in flight
([#42](https://github.com/mikenomitch/legion/pull/42)), not in a refactor.
The seam is where such a change, or a future capture mode's `SO_MARK`, will
go.

---

## 20. Restore seeds the namespace set; templates add their own problems

**Call.** `Netd::restore` runs the same `Desired` path as a heartbeat, so the
gated namespaces of surviving allocations enter the set the datapath tears
down from; before, a restarted worker started with an empty set and never
removed those namespaces when their allocations left. Template-render
problems, which the datapath does not know about, are appended to each
status by the worker after `report()`.

**Alternatives.** Keep restore separate from capture; make `Netd` call into
the templates module.

**Trade-off.** Namespaces of allocations that did *not* survive a restart are
still not torn down (the restore only knows survivors); that leak predates
this work and is an open item. Template problems now come last in an
allocation's problem list instead of before credential problems.

**Why (simplicity).** One path for "here is what should exist" is easier to
keep correct than two, and the datapath should not depend on the worker's
template renderer.
