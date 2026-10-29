# Judgement Calls: app-object-kinds-plan

Decisions made while designing and building
[`plans/app-object-kinds-plan.md`](../plans/app-object-kinds-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.**

**Revised after adversarial review (2026-10-29).** Three reviews (of this
plan, and of the apps substrate's security and correctness) changed O1
before it shipped. Entries 1-43 are the decisions as they stand (28-31 were made while
building O1); entries 44-50 are the original entries they supersede, kept as written at the end
so the reasoning that lost is not silently dropped. The guiding principle
the review added: **an object can never make its app do more than its
writer could do directly, and the write path never depends on the app being
up.** Most of the new entries follow from it.

## 1. An object's contents are authorized against its writer: `requires` with spec pointers, fail-closed, in core, recorded as `authority`

*Adversarial review.* **Call.** A kind declares `requires`: permissions
writing an object needs, whose scope may come from the object's contents
through JSON pointers. The manager resolves them against the effective spec
and checks them as the writer, with the scope the corresponding core RPC
builds, on apply and on delete, fail-closed, with no app call. The resolved
list is stored on the object as `authority`, with who authorized it and
when.

```nickel
requires = [
  { resource = "job", actions = ["write"], namespace = "/spec/targetNamespace" },
  { resource = "kv",  actions = ["read"],  path = "/spec/credentials/kv" },
  { resource = "job", actions = ["write"], host = true, when = "/spec/allowHost" },
  { resource = "operator", actions = ["write"] },
]
```

```text
Error: writing gitops/Repo team-a/storefront needs job:write in team-b (from spec.targetNamespace), which your token does not hold
```

**Alternatives.** (a) The original `writeRequires`, checked on the object's
own namespace and name (entry 44): a team lead with `job:write` on
`names = ["storefront"]` could write a Repo named `storefront` that deploys
`billing`, or into `team-b`; a tenant without `host = true` could have the
app deploy host-level jobs; a `credentials.kv` field could make the app read
another team's secret. (b) Leave cross-references to the app's validate
call, which fails open and which a writer can make time out. (c) Reconcile
as the writer's current rights through delegation (entry 26).

**Trade-off.** A kind's author must describe, in pointers, what its objects
make the app do; a requirement the author forgets is not checked until O3
bounds the app by `authority`. Pointers are a small language of their own
(string or list, fallbacks, `when`). An upgrade that tightens `requires`
does not re-authorize stored objects.

**Why (security, simplicity).** It is the only place the check can be both
binding and independent of the app: core already does exactly this for
jobspec credentials (`enforce_kv_credential_read`: "job submit access would
be a way to exfiltrate any secret in KV"). Recording `authority` gives O3 a
fixed, auditable bound for the app's object-scoped calls rather than the
kind's current declaration.

## 2. Status is its own row, on the Status lane; the CAS token is `specVersion`, which status never moves

*Adversarial review.* **Call.** An object's status lives in
`app_object_status`, keyed by the object's `uid`, on the Status lane beside
`node_health` and `task_status`. Status writes never touch the Control lane,
never move `specVersion` (the apply and delete CAS token), and never wake
watches. Identical writes are dropped without a raft proposal. Status CAS is
the uid plus a generation that never goes backwards and never exceeds what
the replica sees. Only the owning app writes it (app-plane), and a person's
apply refuses a file that sets `status`. Status is for facts that change
when the world changes; per-tick numbers stay in memory.

**Alternatives.** The original plan (entry 46): status in the spec row on
the Control lane, every write a new `version`. Or Kubernetes' status
subresource with RBAC (anyone granted it writes).

**Trade-off.** Cross-lane lag: the Status leader may see an older
generation than the Control leader, so a status write can be refused as
"ahead of the spec" until the lane catches up, and a reader must use
`observedGeneration` to know which spec a status describes. Two tables to
keep consistent: status rows for deleted objects are swept, not deleted in
the same entry.

**Why (performance).** After O2, 10,000 policies reporting every 15 seconds
would have been about 670 Control-lane entries a second, every one waking
every app's watch, echoing back to the autoscaler, flooding the event
journal, and making a person's `--expected-version` stale within a tick.
Legion already learned this with node liveness (phase C-1); this follows
its own precedent.

## 3. A kind's name is `<app>/<Kind>`; files and the API require it, short names are a CLI convenience

*Adversarial review.* **Call.** Two apps may both declare `Repo`; they are
`gitops/Repo` and `reposync/Repo`. Install refuses only a duplicate within
one app or a reserved noun. Object files and `ApplyObject` require the
qualified kind; `get`, `list`, `delete`, and `explain` take a short name in
any case, plural optional, when exactly one kind matches.

```text
Error: kind "Repo" must be qualified with its app: kind = "gitops/Repo"
Error: "repo" names 2 kinds: gitops/Repo, reposync/Repo
Hint: name one: legion object get gitops/Repo storefront
```

**Alternatives.** The original (entry 47): flat names unique cluster-wide,
refused at install. A `--kind Repo=SyncRepo` rename at install, mirroring
`--key`.

**Trade-off.** Files are wordier (`kind = "gitops/Repo"`), and an
interactive short name can stop working the day a second app with the same
noun is installed (the error says what to type instead).

**Why (simplicity, extensibility).** A file has to mean the same thing
forever: with short names in files, installing an unrelated app would break
every GitOps repository that used the noun. That is why Kubernetes requires
`apiVersion` and `kind` in every manifest. And common nouns (`Repo`,
`Certificate`, `Route`) would have made third-party apps mutually exclusive
in a way only their authors could fix.

## 4. Objects are one generic table; state is derived on read

*Revised by the adversarial review.* The table, keyed by (app, kind, namespace, name) with a never-reused `uid`, stands. The spec is no longer stored exactly as written: defaults are persisted at write (entry 6), and status moved to its own table (entry 2). What is still derived on read is the state (`valid`, `invalid`, `orphaned`) and the defaults of fields an upgrade added. The original text follows; its defaults-on-read argument is superseded by entry 6.

**Call.** Every app's objects live in one Control-lane table, `app_object`,
keyed by (app, kind, namespace, name). The spec is stored exactly as
written; the effective spec (schema defaults filled) and the state
(`valid`, `invalid`, `orphaned`) are computed on every read against the
kind as installed *now*.

```console
$ legion object get repo storefront -n team-a
Repo team-a/storefront (gitops)  valid  generation 4  version 118
spec:      {"url":"https://github.com/acme/deploy"}
effective: {"branch":"main","path":".","url":"https://github.com/acme/deploy"}
```

**Alternatives.** (a) A table per kind, created at install: what a
database-minded design does, with typed columns and indexes per kind.
(b) Kubernetes' behavior: defaults are applied and *stored* on write
(`apiserver` defaulting), and a validity flag is written when an upgrade
re-validates.

**Trade-off.** Every read re-checks the spec against the compiled schema
(cached by digest, so one validation per object per read, no compile), and
a list of 10,000 objects validates 10,000 specs. Nothing can index into a
spec's fields; `-l` filters by labels only. Accepted until a kind has that
many objects; the autoscaler's 10,000-policy load test in O2 is the first
place it will show.

**Why (simplicity, extensibility).** One table means installing an app runs
no DDL on a replicated store, raft ownership is one line, and an uninstall
leaves rows rather than orphaned tables. Deriving state on read is the
jobspec-extension rule (apps calls 85-86): an upgrade that adds a default or
tightens a schema reaches every object at once with no rewrite, which an
upgrade over a raft log would otherwise have to batch and could leave half
done.

## 5. The access mapping is pinned to `object`, `job`, `scale`, `node`, checked namespace-wide, and `*` keeps its meaning

*Adversarial review.* **Call.** `access.resource` is `object` (the default),
`job`, `scale`, or `node`; install refuses anything else. A mapped
namespaced kind is checked on the object's namespace with name `*`, so
job-name selectors never decide who writes objects; a `job` mapping carries
`band = "normal"` as a submit does. A mapped cluster kind is checked on
`AclScope::none()`. Per-name control is the `object` resource's `names`.
A `resources = ["*"]` rule reaches objects of every kind in its
namespaces, as `*` reaches everything else.

```nickel
{ resources = ["scale"], actions = ["write"], namespaces = ["media"] }                    # writes ScalingPolicies in media
{ resources = ["scale"], actions = ["write"], namespaces = ["media"], names = ["web-*"] }  # does not: it is about jobs
```

**Alternatives.** The original (entry 44): any resource, scoped as "a job
named like the object", which let `names = ["web-*"]` grant writes to a
policy named `web-proxy`, failed band-scoped rules silently, and could map
onto `acl` or `state`. Kubernetes' choice: no mapping, every kind its own
resource. Making `*` skip `object` unless the rule names `kinds`.

**Trade-off.** A mapped kind cannot be granted per name. `*` rules reach
kinds installed later, so §10's original "default-deny" claim was false and
is corrected: the bound is `requires` (entry 1), not the mapping.

**Why (simplicity).** A few documented translations are explainable; an
arbitrary one is a coincidence trap. Keeping `*` meaning everything avoids
a special case every policy author would trip over.

## 6. Defaults are persisted at write

*Adversarial review.* **Call.** The stored spec is the written spec plus the
defaults of the schema current at write. On read, defaults fill only fields
the stored spec lacks (fields an upgrade added). The upgrade prompt reports
changed defaults:

```text
gitops/Repo.branch default "main" → "trunk": 14 existing Repos keep "main"; re-apply to adopt
```

**Alternatives.** The original (entry 4's superseded half): store as
written, fill every default on read, so an upgrade's new default reaches
every object that did not set it.

**Trade-off.** Changing a default no longer reaches existing objects; their
owners re-apply. The stored spec is not exactly what the person wrote
(`legion object get` shows the stored spec; the dry-run diff compares what
is stored).

**Why (correctness).** Defaults filled on read let an upgrade silently
redeploy every Repo from another branch with no generation bump, no event,
no actor, and an `observedGeneration` that claims the app reconciled a spec
it never saw. Kubernetes persists defaults for the same reason.

## 7. Kind definitions persist in `object_kind`; orphans keep their schema and access mapping

*Adversarial review.* **Call.** One Control-lane row per (app, kind) holds
the declaration, manifest digest, and state (`active` or `orphaned`).
Uninstalling with `--force`, or dropping a kind with `--force`, marks it
`orphaned` and keeps the declaration, so its objects are read and deleted
with the kind's own access mapping, schema, and columns. The rows are
derived from the app records by an idempotent sync after every app write
and on the reconciler's full pass; an orphaned row with no objects left is
dropped.

**Alternatives.** The original (entry 45): orphans fall back to the `object`
resource, which locked `scale:write` holders out of the ScalingPolicies
they wrote. Storing the access resource on every object row.

**Trade-off.** A second source of truth beside the app record, kept in step
by a sync rather than in the same raft entry: between an app write and its
sync (normally the same request), kinds can lag the record.

**Why (simplicity).** One row per kind is the natural home for "what this
kind was", and the orphan rule stays one sentence: an orphan answers to the
rules its kind had.

## 8. The validate call fails open, and is advice, never a control

*Revised by the adversarial review.* The call stands, but its role is narrowed: because a writer can make it time out (a tarpit URL in a field the app probes), it is user experience only. Everything a writer must be refused is in core: the schema, `immutable`, and `requires` (entry 1). The public doc says so.

**Call.** A kind with `validate = true` is checked by its app on every apply,
with two seconds to answer; an app that is disabled, unreachable, slow, or
answers anything but `200` lets the write through with a warning.

```text
warning: Repo team-a/storefront was not checked by the gitops app (did not answer within 2s); the schema check passed
```

**Alternatives.** Fail closed: Kubernetes admission webhooks with
`failurePolicy: Fail`, which is what makes an app's checks binding.

**Trade-off.** A write can store an object the app would have refused.
The app must check again when the object reaches it on its watch, and say so
in the object's status. O2 makes this concrete: an unknown scaling plugin
surfaces in status, not at apply, while the autoscaler is down.

**Why (simplicity).** The same rule as jobspec extensions (apps calls 87),
for the same reason: a broken add-on must never block writes, and the
Kubernetes failure mode (a webhook down blocks every write it covers) is
the one lesson the prior art teaches loudest.

## 9. Bounded cleanup instead of finalizers; objects are namespace contents

*Adversarial review.* **Call.** A kind may declare `cleanup = "10m"` (at
most `1h`). Deleting such an object marks it deleting (its spec freezes),
the app sees it on its watch and calls `ReleaseObject` when done, and at the
deadline (or at once when its kind is orphaned) the manager removes it
anyway, emitting `object.cleanup_abandoned`. Objects count as namespace
contents: a namespace delete is refused while non-deleting objects remain,
and `--cascade` deletes them with the caller's per-kind access and
`requires` checked for each.

**Alternatives.** Kubernetes finalizers (an object stays until its
controller removes the finalizer). The original plan (entry 49): deleting a
namespace leaves its objects.

**Trade-off.** An app that needs longer than an hour to release external
resources must do the rest from its own records, keyed by `uid`. A cascade
can be refused because the caller lacks one kind's access.

**Why (simplicity).** Nothing can stay stuck: the stuck `Terminating`
namespace is Kubernetes' most familiar failure. And leaving objects behind
let a recreated namespace resurrect another team's Repos for GitOps to
deploy with its own grant.

## 10. Schemas are closed by default, with `immutable` and `writeOnly` fields; CEL `rules` wait for O4

*Adversarial review.* **Call.** A schema with `properties` and neither
`additionalProperties` nor `patternProperties` is treated as
`additionalProperties = false` (`allOf`/`anyOf`/`oneOf` branches excepted),
so an unknown field is an error naming its path, as an unknown jobspec key
is. `immutable` lists pointers an update may not change, checked in core.
`writeOnly: true` marks secrets, redacted on every read but the owning
app's watch and in diffs, events, and errors. `rules` (CEL) is reserved and
refused until O4.

```text
Error: gitops/Repo team-a/storefront: spec.brnach: unknown field (did you mean branch?)
Error: gitops/Repo team-a/storefront: spec.url is immutable (was "https://github.com/acme/deploy"); delete and recreate the object to change it
```

**Alternatives.** Open schemas (the original: only where the author wrote
`additionalProperties = false`). CEL now (`cel-interpreter` 0.10, MIT).

**Trade-off.** Composed schemas (`allOf` of property sets) must opt out
explicitly; cross-field checks (`min <= max`) wait for O4 and sit with the
app's `Accepted` condition meanwhile, which is the regression O2 records.

**Why (simplicity).** "Told at apply, with the path" is the job the job
author asked for, and Legion already made jobspecs strict for it. CEL is a
new language and dependency whose cost bounds need their own design; draft-7
conditionals, `immutable`, and `Accepted` cover the interim.

## 11. Status has a standard envelope, and the manager derives a summary anyone can wait on

*Adversarial review.* **Call.** Status is `{ observedGeneration, conditions,
fields }`, with standard condition types `Accepted` and `Ready`; the
manager derives `Pending`, `Ready`, `NotReady: <reason>`, `Rejected:
<reason>`, `Stale`, `Invalid`, `Orphaned`, or `Deleting` for every object,
and `legion object apply --wait` waits on it for every kind.

**Alternatives.** Free-form status JSON (the original), leaving readiness
per kind.

**Trade-off.** Apps must speak conditions; a kind whose app sets neither
`Accepted` nor `Ready` reads `Pending` forever and `--wait` times out.

**Why (simplicity, agents).** A generic wait is what lets GitOps and agents
treat every kind the same, the lesson of Kubernetes' kstatus.

*Built (O1).* The summary gained `Accepted`: an app that reports only
`Accepted=true` (a kind with nothing further to reconcile) reads `Accepted`,
and `--wait` succeeds on it, rather than `Pending` forever. Precedence is
`Orphaned`, `Deleting`, `Invalid`, `Stale`, `Pending`, `Rejected`,
`NotReady`, `Ready`, `Accepted` (`summarize` in
`src/structs/app_object.rs`; docs/app-objects.md, "Status").

## 12. Object events reach only callers who may read the kind there, and carry no values

*Adversarial review.* **Call.** `object.*` events carry the kind's access
resource and the object's scope in metadata, and a tenant's feed shows one
only when it may read that kind on that scope (operators see all). Events
carry the ref, uid, generation, actor, and condition transition, never spec
or status values.

**Alternatives.** The original: namespaced object events reach whoever
reaches the namespace, as `namespace.*` events do.

**Trade-off.** An event records the access the kind had when it was
emitted; an upgrade that changes the mapping does not re-filter history.

**Why (security).** Reaching a namespace is any allow rule there
(`exec:read` will do), so the original leaked the names, kinds, and writers
that `GetObject` hides behind check-before-fetch.

## 13. Objects have a never-reused `uid`

*Adversarial review.* **Call.** Every object gets a `uid` at create, a value
of a never-repeating counter. Status, the watch's tombstones, and app-side
state (O2's pauses and intents) key on it.

**Alternatives.** Key everything by (app, kind, namespace, name), as the
original did.

**Trade-off.** One more identifier in every API object.

**Why (correctness).** Without it, a delete and a recreate under the same
name inherits stale status, a pause, or a half-done drain.

## 14. `object` is a namespaced resource; a cluster kind's objects carry namespace `*`

**Call.** The new `object` resource is namespaced, so its rules carry
`namespaces`. A cluster kind's object is checked with the namespace `*` on
its scope, which only a rule reaching every namespace (`namespaces =
["*"]`) matches.

```nickel
{ resources = ["object"], actions = ["write"], namespaces = ["*"], kinds = ["backup/Schedule"] }   # cluster kind
{ resources = ["object"], actions = ["write"], namespaces = ["team-a"], kinds = ["gitops/Repo"] }  # namespaced kind
```

**Alternatives.** (a) Two resources, `object` (namespaced) and
`clusterobject`. (b) A cluster-scoped `object` resource with namespaces
matched as an ordinary selector.

**Trade-off.** "Write cluster objects" reads as "write in every namespace",
which over-reaches for a rule that also covers namespaced kinds: `kinds =
["gitops/*"]` with `namespaces = ["*"]` covers both gitops's cluster kinds
and its namespaced ones everywhere. Narrow with `kinds`.

**Why (simplicity).** One resource, and the namespace rules
(`namespaces` required, the reserved `legion-*` prefix) work unchanged. The
`*` namespace is already how Legion scopes an object no namespace owns
(`AclScope::unowned`, unowned volumes), so it is not a new idea to explain.

## 15. Objects an app writes are `managedBy` it, and a person needs `force` to change them

*Adversarial review.* **Call.** An apply by an app's own identity stamps the
object `managedBy = <app>`, as the jobs an app submits are stamped. A
person's apply or delete of a managed object needs `--force`. A delegated
viewer identity (`alice via app gitops`) counts as the person.

**Alternatives.** Only the owning app may write spec (Kubernetes has no
such rule either), or no distinction.

**Trade-off.** One more flag a person may need.

**Why (simplicity).** Apps create objects too (cert-manager creates its own
children); a person editing one should know an app will likely overwrite
it, as with an app's job.

## 16. Objects have size limits and a per-kind `maxObjects`

*Adversarial review.* **Call.** A spec is at most 64 KiB after defaults,
status `fields` 16 KiB, 32 labels; a kind's `maxObjects` (default 10,000, at
most 100,000) is enforced inside the replicated write that creates an
object. Per-namespace quotas are deferred to namespace policy.

**Alternatives.** No limits (the original); per-namespace quotas now.

**Trade-off.** One tenant can still use a kind's whole `maxObjects`.

**Why (performance).** Anyone with `object:write` in one namespace could
otherwise grow the Control lane's snapshot and every app's relist.

## 17. The watch chunks its snapshot, carries uids and tombstones, and polls a per-app fingerprint; no resume yet

*Adversarial review.* **Call.** `WatchObjects` sends its snapshot in
messages of at most 1,000 objects, the last marked `snapshotDone`; deletes
are tombstones with the uid; it polls, every second, a fingerprint of the
calling app's rows and kinds only, so other apps' writes and every status
write leave it asleep; the 64-shard value is a stored column the query
filters on. There is no `since_version` resume: a reconnect relists.

**Alternatives.** The original (entry 48): one snapshot message and a
table-wide fingerprint. A change log with tombstone retention and resume.

**Trade-off.** Every reconnect is a full relist, fine to about 50,000
objects per app; past that, resume needs a change log.

**Why (performance, simplicity).** It takes what `WatchScalingPolicies`
already learned (chunking) and stops one app's churn costing every other
app, without a new table.

The original entry's claim that the poll's cost "was measured acceptable"
(citing apps calls 90) was unsupported: that entry records no measurement.
Nothing has been measured for this watch either; the bound above is an
estimate from the relist cost, and resume is the follow-up if it is wrong.

## 18. Watch shards use the autoscaler's FNV-1a over a per-object key, not the extensions' SHA-256 of the job id

**Call.** `WatchObjects` shards by FNV-1a 64 of `<app>/<Kind>:<id>` modulo
`shard_count`, the hash and 64-shard count the autoscaler's Phase 7 leases
use.

**Alternatives.** The `WatchAppExtensions` hash (SHA-256 of the canonical
job id), so the two app-plane watches agree with each other.

**Trade-off.** The two watches shard differently. An app that shards both
its extension values and its objects holds two sets of leases, or one set
interpreted two ways.

**Why (extensibility).** The only sharded app is the autoscaler, and O2 moves
its policies onto objects: matching its hash means its lease code is
untouched and a replica's shard set means the same thing on both of its
streams (`WatchScalingPolicies` for tasks, `WatchObjects` for standalone
policies).

## 19. No schema versions or conversion in v1

**Call.** A kind has one schema. An upgrade that changes it re-checks every
object and flags failures `invalid`; nothing converts them.

**Alternatives.** Kubernetes' versioned CRDs with a storage version and
conversion webhooks.

**Trade-off.** An app that renames a field breaks its users' objects until
they re-apply. Apps must evolve schemas additively or accept that.

**Why (simplicity).** Legion has no installed base (AGENTS.md); conversion
webhooks are the most complex part of CRDs and the most often broken. The
upgrade prompt makes the cost visible before anyone pays it.

## 20. The reserved-noun list is every CLI noun, every ACL resource, and the core object nouns

*Revised by the adversarial review*: unchanged, except that the error now names the qualified kind and the list also reserves `spec` and `status`.

**Call.** A kind may not be named, in any case, singular or plural, after a
top-level CLI noun (`job`, `node`, `scaling`, `events`, …), an ACL resource
(`object`, `scale`, …), or a core object noun that is not a top-level
command (`task`, `group`, `allocation`, `policy`, `token`, `grant`,
`rollout`, `eval`, `pool`, `secret`, `user`, `kind`, …). The list is
`RESERVED_KIND_NOUNS`, and a test checks it covers every top-level CLI
command.

```text
Error: app gitops: kinds (Policy): `Policy` is a Legion noun (policy) and is reserved; pick a name that says what the app means (RolloutPolicy, ScalingPolicy)
```

**Alternatives.** Reserve only what collides with a CLI path today.

**Trade-off.** `Policy`, `Token`, and `Secret` are natural names apps lose.
`ScalingPolicy` is fine: only exact nouns are reserved.

**Why (simplicity).** An object called `Job` that is not a job is the kind
of ambiguity that costs an incident; the test keeps the list honest as
commands are added.

## 21. A cluster kind ignores the request's default namespace but refuses a namespace written in the object

**Call.** `ApplyObject` takes the object (which may say `namespace`) and a
default namespace (the CLI's `-n` or `LEGION_NAMESPACE`). For a cluster
kind, the default is ignored with a warning; a namespace written in the
object is an error. `get` and `delete` ignore the namespace for a cluster
kind.

**Alternatives.** (a) Refuse any namespace on a cluster kind (then an
exported `LEGION_NAMESPACE` breaks every cluster apply). (b) Ignore both
silently, as kubectl does (kubectl#1413).

**Trade-off.** Two rules where one would be simpler to state.

**Why (simplicity).** Each matches what the user meant: an environment
default is not a statement about this object, and a file that says
`namespace = "x"` for a cluster kind is a mistake worth stopping.

## 22. A cleanup ends with a `ReleaseObject` RPC, not a flag on the status write

*Adversarial review.* **Call.** The app ends a deleting object's cleanup
with `ReleaseObject`, an app-plane Control-lane write that removes the row
when the uid still matches.

**Alternatives.** `UpdateObjectStatus` with `release = true`.

**Trade-off.** One more RPC.

**Why (simplicity).** Status writes are Status-lane and releasing deletes a
Control-lane row; one RPC doing both would be two raft entries on two lanes
pretending to be one.

## 23. O2 keeps scaling defaults in core

**Call.** When the autoscaler's standalone policies become objects, the
cluster's scaling defaults (`legion scaling defaults apply`) stay a core
table and RPC.

**Alternatives.** Make them a `ScalingDefaults` kind the autoscaler owns.

**Trade-off.** One autoscaler concept stays outside the generic system.

**Why (correctness).** The manager resolves a task's `autoscale` against the
defaults at submit and in `job plan`, synchronously, and must not depend on
an app being up to admit a job.

## 24. Applying an object of a kind not installed yet fails retryably; pending objects are deferred

*Adversarial review.* **Call.** `ApplyObject` of a kind no app declares
fails with `KIND_NOT_INSTALLED` on a retryable status.

**Alternatives.** `--allow-pending`: store it and adopt it at install if its
writer still passes `access` and `requires`, as pending jobspec values are.

**Trade-off.** A GitOps sync that applies an app and its objects in one
pass needs a retry.

**Why (simplicity).** Retrying converges with no new state; pending objects
can come when GitOps needs them.

## 25. Object versions come from the `app_object` counter in `scaling_counter`

*Revised by the adversarial review*: the counter now also mints `uid`s and `specVersion`s; status has no version.

**Call.** `version` is the next value of the `app_object` counter in the
existing `scaling_counter` table.

**Alternatives.** A new counter table, or `MAX(version) + 1`.

**Trade-off.** A table named for scaling holds a counter for every app's
objects.

**Why (simplicity).** The table is a generic name-to-counter map on the
Control lane already, and `MAX + 1` would repeat a version after the newest
object is deleted and recreated, which breaks CAS.

## 26. O3 bounds an app by the authority recorded at write, not by its writer's current rights

*Adversarial review.* **Call.** When an app acts for an object (O3), the
manager evaluates the call as the app's grant intersected with the object's
recorded `authority`. Authority is not revoked when the writer later loses
rights; `legion object list --authority-lapsed` finds such objects, and a
re-apply re-authorizes.

**Alternatives.** Reconcile as the writer's current rights (the plan
review's option C), through the delegation that shipped for actions.

**Trade-off.** A writer who loses a permission keeps their past objects'
power until someone re-applies them.

**Why (correctness).** A departing engineer must not break production
deploys (Kubernetes and Flux semantics); the lapsed list makes the gap
visible instead.

## 27. Every pointer in a kind's declaration points into the object document

**Call.** `requires`, `immutable`, and `columns` all take JSON pointers into
the object as `legion object get -j` shows it (`/spec/url`,
`/status/fields/commit`, `/name`).

**Alternatives.** Pointers into the spec for `immutable` (`/url`), as the
review's sketch wrote it.

**Trade-off.** Every spec pointer starts with `/spec`.

**Why (simplicity).** One convention for three fields; a pointer means the
same thing wherever it is written.

## 28. The owning app has no implicit rights over its kind's objects

*Built (O1).* **Call.** An app's own identity reads and writes objects,
including its own kind's, only as its grant allows: a `gitops` that writes
Repos lists `{ resources = ["object"], kinds = ["gitops/Repo"], ... }` in its
manifest's `permissions`, and its writes are checked against `requires` like
anyone's. Only the app-plane calls (`WatchObjects`, `UpdateObjectStatus`,
`ReleaseObject`) are implied by owning the kind.

**Alternatives.** Implicit `object:read`/`write` on the app's own kinds for
its identity, as owning a kind might suggest.

**Trade-off.** One more permission line in a manifest that writes its own
objects, and the installer must hold it.

**Why (security, simplicity).** The guiding principle: the grant shown at
install is everything the app can do. An implicit right would be a second,
unprompted grant, and an app writing objects without `requires` would be the
confused deputy entry 1 exists to stop.

## 29. A caller with no object rights at all cannot tell which kinds exist

*Built (O1).* **Call.** `ApplyObject`, `GetObject`, `ListObjects` with a
kind, and `DeleteObject` first check that the caller may read (or write)
some kind's objects somewhere, or `object` anywhere, before resolving the
typed kind; only then does an unknown kind read `unknown kind "x" (did you
mean ...)`.

**Alternatives.** The plan's order: resolve the kind (public metadata)
first, then check the object's scope.

**Trade-off.** A caller with no object rights gets `PermissionDenied`
instead of the kind lookup's hint.

**Why (security).** `ListObjectKinds` already shows a caller only the kinds
it may read; the other RPCs answering `unknown kind` vs `PermissionDenied`
would have leaked the list anyway (AGENTS.md, "Check before you fetch").

## 30. The framework's reconcile loop is a builder over an `ObjectPlane` trait, and outcomes are constructors

*Built (O1).* **Call.** `ctx.reconcile("Repo", handler)`, or
`ctx.start_reconciler(ctx.reconciler("Repo").resync(..).shards(..)
.on_cleanup(..), handler)`; the handler returns `ObjectOutcome::ready()`,
`not_ready(reason, msg)`, `rejected(reason, msg)`, or `accepted()`, refined
with `because`, `with_fields`, `with_condition`, `requeue_after`. A handler
`Err` is reported as `Ready=false` (`ReconcileFailed`) keeping the last
`Accepted`, and retried with backoff (1 s doubling to 5 min); at most 4
objects reconcile at once. The loop runs over an `ObjectPlane` trait
(watch, status, release), implemented for the app's client.

**Alternatives.** The plan's sketch, `Reconciler::new(handler)` with
`Outcome::ready(reason, message)`; a loop hard-wired to the gRPC client.

**Trade-off.** A slightly larger surface than one constructor; a trait
apps never implement themselves.

**Why (simplicity, testability).** Most outcomes need no reason (`ready()`),
the rest read as sentences (`ready().because("Synced", ..)`). The trait
lets the loop be tested against a scripted fake and against the real
manager service in process (`tests/integration/app_objects.rs`), without a
network.

## 31. Watch messages are bounded by bytes as well as by count

*Built (O1).* **Call.** A `WatchObjects` message holds at most 1,000
objects and about 2 MiB of them (always at least one); the framework
decodes messages up to 16 MiB.

**Alternatives.** Count-only chunks (the plan), with a decode limit large
enough for 1,000 maximal objects (about 150 MiB).

**Trade-off.** More messages for a snapshot of large objects.

**Why (performance).** A count bound alone lets one message reach the
worst case of every object at its 64 KiB spec, twice (stored and
effective), which no client should have to buffer.

## 32. The autoscaler keys its objects' scaling state by uid, `object:<uid>`

*Implementation, Phase O2 (`object_target_key`,
`Store::sweep_object_scaling_state`).* **Call.** A scaling policy's or
node group's history, pause, action row, and status-board entry live in
core's scaling tables under `object:<uid>`, never under its name. The API
names the object (`ScalingObjectRef { kind, namespace, name, uid }`); a
person's reference (`legion scaling history workers --kind nodegroup`) is
resolved to the current object's uid. The scaling state of deleted objects
is swept on the app reconciler's full pass.

```console
$ legion scaling pause workers --kind nodegroup --for 1h     # the node group's pause
$ legion scaling explain workers --kind scalingpolicy         # ScalingPolicy default/workers: not paused
```

**Alternatives.** (a) Key by kind and reference (`autoscaler/NodeGroup:workers`),
so history outlives a delete and a recreate, and sweep the pause and the
action row on delete (or stamp them with the uid). (b) Keep today's
`policy:<id>` with a `nodes/` prefix for node groups (the plan review's
suggestion).

**Trade-off.** An object's history goes with it: a policy deleted and
created again under the same name starts with no history (the events stay
in the journal). Ids break once: `policy:<id>` keys, `--standalone`, and the
bare-name lookup are gone (no installed base; the PR says so).

**Why (correctness, simplicity).** A uid cannot collide (`NodeGroup
workers` and `ScalingPolicy default/workers` are two objects) and cannot be
inherited: a recreated object is never paused by a pause meant for the old
one, and never resumes a drain the old one's intent recorded. The sweep
only tidies; nothing depends on it for correctness, so object deletes stay
generic and know nothing of scaling.

## 33. Cross-field checks move to the app: a fail-open regression until O4

*Implementation, Phase O2.* **Call.** The manager checks a ScalingPolicy
or NodeGroup against its kind's schema at apply; what a schema cannot
express (`min` above `max`, a check's strategy settings, a plugin the
namespace may not use) is refused at apply only by the autoscaler's
validate call, and otherwise by the autoscaler at evaluation, which holds
the object `Accepted=false`:

```text
$ legion object apply -f inverted.ncl     # the autoscaler is down
autoscaler/ScalingPolicy media/render: created (generation 1, spec version 52)
warning: autoscaler/ScalingPolicy media/render was not checked by the autoscaler app (...)
$ legion object get scalingpolicy render -n media
autoscaler/ScalingPolicy media/render (valid)  Rejected: Invalid (render: min = 9 is above max = 3)
```

**Alternatives.** (a) Run the policy parser in `ApplyObject` for the
autoscaler's kinds (an autoscaler-specific hook in the generic write
path). (b) Wait for O4's CEL `rules` before moving the policies.

**Trade-off.** Today's `legion scaling apply` refused such a policy
fail-closed, in the manager. Now, with the app down, it is stored and
reads `Rejected` once the app runs. It is never acted on, so the
regression is in feedback, not in safety.

**Why (extensibility).** The generic write path stays generic (§1: it
never depends on the app), and O4's `rules` restore the fail-closed check
for every kind, not just this one. Recorded as a known limitation in
docs/autoscaling.md.

## 34. `WatchScalingPolicies` keeps serving the autoscaler what only core holds; the app resolves its objects itself

*Implementation, Phase O2.* **Call.** `WatchScalingPolicies` streams task
policies only. To the autoscaler's own identity (or a management token) it
also carries `context` (the cluster's scaling defaults, every namespace
policy's `autoscale`, the cluster-wide pause) and `objects` (each object
target's own pause and action row, keyed `object:<uid>`, unsharded). The
app reads the objects over `WatchObjects` and resolves each ScalingPolicy
against `context` with the same code core uses.

**Alternatives.** (a) Keep the manager resolving standalone policies and
streaming them (`StandaloneScalingPolicy`), reading `app_object` instead of
`scaling_policy`. (b) The app polls `GetScalingDefaults` per namespace and
the pauses through `GetScalingInputs`. (c) A new app-plane RPC.

**Trade-off.** The layered resolution runs in two places (the app, to
evaluate; the manager, for status and the defaults check), from one
function in one binary. Object targets' pauses and action rows go to every
replica, not just the one that owns the object.

**Why (simplicity).** The app reads its objects through the generic
system, as any app would (the point of O2), and core serves only what core
owns. (a) would keep a second, autoscaler-shaped object feed in core; (b)
replaces a push with polling and loses change notification for the
defaults; the existing stream already wakes on defaults, namespace-policy,
pause, and action-row changes.

## 35. The policy parser stays linked into the manager

*Implementation, Phase O2 (`manager/server/scaling_objects.rs`).*
**Call.** Core reads the autoscaler's objects, through the generic
`Store::app_objects` by kind, in four places, and parses them with the
autoscaler's parser: `ApplyScalingDefaults`' impact check (ScalingPolicies
inherit the same layers as tasks), `GetScalingStatus` (mode, bounds, and
the effective policy with provenance), `RecordScalingAction`'s existence
check (by uid), and history and pause lookups (name to uid). No storage and
no ACL in core are autoscaler-specific.

**Alternatives.** `GetScalingStatus` shows only what the app reported, and
the defaults check ignores ScalingPolicies.

**Trade-off.** Core still knows the ScalingPolicy spec format; a change to
it must keep both readers in step (they share the code, so a build does).

**Why (correctness).** A defaults change that would invalidate a
ScalingPolicy must be refused as it is for a task (fail-closed, before the
write), and `legion scaling status` must say what a policy is, and why it
is invalid, while the app is down.

## 36. A deleted node group retires through the scale-in path

*Implementation, Phase O2 (`NodeGroupObservation::retiring`,
`NodeDecision::retired`).* **Call.** A NodeGroup has `cleanup = "15m"`.
While it is Deleting, the engine adds nothing, makes every member a victim
at once (ignoring `down.when`, `down.for`, the step budget, and `min`),
keeps the never-victim protections, scales the provider to 0 once no member
is left (an instance that never joined strands nothing), and the app
releases the object when no member, drain, or provider instance is left. A
pause holds the cleanup; a dry-run group is released at once.

```text
autoscaler/NodeGroup workers-general (valid)  Deleting
  fields: {"cleanup":"1 member(s) left, 0 draining; 1 protected (they stay: w-3 (runs legion-system job ...))"}
```

**Alternatives.** (a) `target/scale` to 0 at once (the provider stops
instances undrained). (b) Keep `down`'s budget (a 10-node group at one
drain per 5 minutes cannot finish in 15). (c) Drain protected members too.

**Trade-off.** A protected member, or work that cannot move within the
drain deadline, outlives the window: the manager removes the object at the
deadline and the machine is left to the operator, still labeled with the
group's name.

**Why (correctness).** The scale-in path is the one that never strands work
or stops the wrong machine (provider-id checks, drains before terminate,
forget after); deletion reuses it rather than inventing a second way to
remove machines.

## 37. The autoscaler runs its own loop over the object cache; the dynamic shard hook goes in both

*Implementation, Phase O2 (`run_object_watch`, `ShardSelection`,
`Reconciler::shards_from`, `StatusBook`).* **Call.** The autoscaler keeps
two `ObjectCache`s current with `run_object_watch` (ScalingPolicies of the
shards it holds; every NodeGroup on the replica holding the `nodegroups`
shard), evaluates them on its tick, and writes status through a
`StatusBook`. The selection a replica watches is a `tokio::sync::watch`
channel the lease controller's ownership feeds; `Reconciler::shards_from`
gives the reconcile loop the same hook.

**Alternatives.** The framework's reconcile loop, one handler per object.

**Trade-off.** The bundled app does not exercise the reconcile loop; the
framework has two ways to own objects.

**Why (performance).** An evaluation tick reads every target's inputs in
one batched call and runs the expander across all node groups at once; a
per-object handler would split both. The shard hook belongs in the
framework, so any sharded app gets it.

## 38. Object status holds verdicts, written on change; the last action comes from the action row

*Implementation, Phase O2 (`objects::verdict`).* **Call.** Status says
`Accepted` (the spec parses, resolves, and its plugins are open to it),
`Ready` (the last evaluation worked; reason `Paused` while paused,
`TargetError`, `MissingData`, `Deleting`), and fields `target`, `policy`,
`digest`, `error`, `lastAction` (the last closed action intent, from the
replicated action row), `cleanup`. The current and wanted counts stay on
the scaling status board.

**Alternatives.** Report every evaluation in status; keep the last action
in the replica's memory.

**Trade-off.** `legion object get` does not show the current count; that
is `legion scaling status`.

**Why (performance, correctness).** Ten thousand policies reporting every
15 seconds is the Status-lane churn entry 2 exists to avoid. The action row
is replicated, so a replica that restarts or takes a shard over reports
the same last action instead of dropping it.

## 39. The kinds' schemas are generated from the parser's types and filled in when the bundled manifest loads

*Implementation, Phase O2 (`BundledApp::generated`,
`policy::scaling_policy_schema`).* **Call.** `autoscaler.ncl` declares the
kinds without `schema`; the app's Rust types (schemars over
`AutoscaleRecord` and `NodeGroupPolicy`, with every `$ref` inlined and
`null` alternatives dropped) fill `schema` and `status.schema` before the
manifest is parsed.

**Alternatives.** Hand-write the schemas in Nickel, with a test that they
match the types.

**Trade-off.** The manifest file alone does not show the schemas
(`legion object explain` and `legion object kinds -j` do).

**Why (simplicity).** One source of truth: a field added to the policy
record reaches the kind's schema, its `explain` text (the doc comments),
and the parser at once.

## 40. Pausing and naming an object follow its kind; a node group and a policy may share a name

*Implementation, Phase O2.* **Call.** Reading an object's status or
history needs its kind's read access; pausing one needs what writing it
needs (its access and `requires`), checked before the lookup. The scaling
verbs name an object with `--kind` (`legion scaling explain workers --kind
nodegroup`). The manager-side node-group special case
(`enforce_policy_write`, `gate_policy_ref`, the shared bare-name space) is
gone.

**Alternatives.** Keep `scale:write` for any pause, and `operator:write`
plus `node:write` for a node group's, hard-coded.

**Trade-off.** A `scale:write` holder who could pause a policy by its job-
like name before now needs `scale:write` namespace-wide in its namespace.

**Why (simplicity).** One rule: the kind's declaration decides. A third-
party app's kinds get the same treatment with no core change.

## 41. Both kinds' target plugin is immutable

*Implementation, Phase O2.* **Call.** `immutable = ["/spec/target/plugin"]`
on ScalingPolicy, as the plan says, and `["/spec/target/nodes/plugin"]` on
NodeGroup, which the plan did not ask for.

**Alternatives.** Let a node group change provider in place.

**Trade-off.** Moving a group to another provider is a delete (which
drains it) and a create.

**Why (correctness).** A group's members belong to the provider that
started them; a new provider would not list them, so every member would
read "the provider does not list its instance" and could never be scaled
in.

## 42. Pages read objects, never write them; the Autoscaling page lists policies, not node groups

*Implementation, Phase O2 (`PAGE_RPCS`, `render_policies`).* **Call.**
`ListObjectKinds`, `GetObject`, and `ListObjects` are page-callable reads
(installing a page that lists one needs `object`, `job`, `scale`, and
`node` read everywhere). `ApplyObject` and `DeleteObject` are refused for
pages. The removed scaling-policy RPCs leave the list; `PauseScaling` now
names `node:write` too. The Autoscaling page shows ScalingPolicies in the
viewer's namespaces, and no node groups.

**Alternatives.** Classify object writes with a permission list.

**Trade-off.** A page cannot write objects; an app that needs it uses an
action.

**Why (security).** What writing an object needs is whatever its kind
`requires`, which no fixed list bounds, so no installer check could bound a
page that writes them. The page's `scale:read` says nothing about the
viewer's `node:read`, which a node group needs.

## 43. `legion scaling simulate` resolves a ScalingPolicy file on the client

*Implementation, Phase O2.* **Call.** `GetScalingDefaultsReply` gains
`governing` (the namespace's governing policies' `autoscale` records), and
`simulate` resolves an `autoscaler/ScalingPolicy` object file against it
and the cluster record locally.

**Alternatives.** An RPC that resolves a spec without storing it (today's
`ApplyScalingPolicy --dry-run`).

**Trade-off.** Plugins are not checked in a simulation (it runs none).

**Why (simplicity).** `GetScalingDefaults` already shows the same values
with provenance to the same callers; no new RPC.

# Superseded entries

Kept as written when they were superseded; they no longer describe the design.

## 44. *Superseded.* A kind's ACL is one resource: the generic `object`, or an existing one of the same scope

**Superseded by entries 1 (`requires`) and 5 (the access mapping)** (adversarial review, 2026-10-29). The original reasoning, kept as written (entry numbers in it refer to the original ranking):

**Call.** `access = { resource = … }` names the ACL resource that governs a
kind's objects, default `object`. A namespaced kind may map to a namespaced
resource, a cluster kind to a cluster-scoped one; install refuses a
mismatch. `writeRequires` adds permissions every writer must also hold, on
the object's scope.

```nickel
# O2's ScalingPolicy: governed by exactly the scale:write that governs scaling
{ name = "ScalingPolicy", scope = "namespaced", access = { resource = "scale" }, ... }
# O2's NodeGroup: creating machines needs what creating machines needs
{ name = "NodeGroup", scope = "cluster", access = { resource = "node" },
  writeRequires = [{ resource = "operator", action = "write" }], ... }
```

**Alternatives.** (a) Kubernetes': every kind is its own resource, always
(`object` with `kinds` only). (b) A per-kind permission table the app ships
(aggregated ClusterRoles).

**Trade-off.** A mapped kind's objects are reachable by every holder of that
resource on that scope: anyone with `scale:write` in `media` can write
`ScalingPolicy` objects there, whether or not an admin meant "scaling
policies" when they granted it. The install prompt shows the mapping so the
installer decides. A mapped kind also cannot be granted separately from the
resource (no "may write policies but not resize jobs").

**Why (simplicity).** Grants that mean the same thing should be one grant.
With (a), O2 would have needed every `scale:write` holder to also be given
`object:write` on `autoscaler/ScalingPolicy`, and the two would drift. The
default stays K8s-like (`object` with `kinds`), so a kind that maps onto
nothing is still governed separately.

## 45. *Superseded.* An orphaned object is governed by the `object` resource

**Superseded by entry 7 (orphans keep their kind's mapping)** (adversarial review, 2026-10-29). The original reasoning, kept as written (entry numbers in it refer to the original ranking):

**Call.** Once a kind's declaration is gone (uninstall, or an upgrade that
drops it, both with `--force`), its objects are read and deleted with
`object:read`/`object:write` on `<app>/<Kind>`, whatever the kind mapped to.

**Alternatives.** Store the access resource on every row, so an orphan
keeps its last mapping.

**Trade-off.** A team that managed its `ScalingPolicy` orphans with
`scale:write` needs `object:write` (or an operator) to clean them up.

**Why (simplicity).** A stored per-row mapping goes stale the moment an
upgrade changes it, and then two rows of one kind answer to different
resources. Orphans are an operator's clean-up after a forced removal, and
the rule fits in one sentence.

## 46. *Superseded.* Status is the owning app's alone, with CAS on version or generation

**Superseded by entry 2 (status on the Status lane, `specVersion` CAS)** (adversarial review, 2026-10-29). The original reasoning, kept as written (entry numbers in it refer to the original ranking):

**Call.** `UpdateObjectStatus` is app-plane: only the app that owns the
kind (its workload identity) writes status, never the spec. `ApplyObject`
refuses a file that sets `status`. The app passes `expected_generation`
("this status is for the spec I saw") or `expected_version`.

**Alternatives.** Kubernetes' `/status` subresource with RBAC: any principal
granted `update` on `repos/status` writes it, including people.

**Trade-off.** A person cannot correct a status by hand, and an app cannot
delegate status writes to a helper with another identity.

**Why (simplicity).** Status is the app's report; letting anyone else write
it makes it a claim nobody can trust at 3am. App identity is already the
app plane's key (`WatchAppExtensions`, `ReportScalingStatus`).

## 47. *Superseded.* Kind names are flat, unique cluster-wide case-insensitively, with `<app>/<Kind>` as the qualified spelling

**Superseded by entry 3 (`<app>/<Kind>` is the name)** (adversarial review, 2026-10-29). The original reasoning, kept as written (entry numbers in it refer to the original ranking):

**Call.** No API groups. `Repo` is unique across installed apps and bundled
manifests, ignoring case; the CLI, MCP, and API accept any case and an
optional plural; `gitops/Repo` always names gitops's kind.

```console
$ legion object list repos -n team-a
$ legion object get gitops/Repo storefront -n team-a
```

**Alternatives.** Kubernetes' group-qualified kinds (`repos.gitops.legion.io`)
with declared plural and short names.

**Trade-off.** Two apps cannot both ship a `Repo`; the second must rename
its kind. Install says so, naming the owner.

**Why (simplicity).** Kind names are typed at a terminal. The jobspec-key
rule already established "the short name may collide, the qualified one
never does" (apps calls on `apps.<app>`), so it is one idea, not two.

## 48. *Superseded.* `WatchObjects` polls a fingerprint each second

**Superseded by entry 17 (chunked, per-app watch)** (adversarial review, 2026-10-29). The original reasoning, kept as written (entry numbers in it refer to the original ranking):

**Call.** The watch re-reads when the apps' fingerprint or the table's
(count, highest version) moves, checked every second, with a full re-read
every 30 seconds.

**Alternatives.** Event-driven delivery from the store's change feed.

**Trade-off.** Up to a second of latency, and one cheap query per stream
per second.

**Why (simplicity).** The same shape as `WatchAppExtensions` (apps calls
90), whose cost was measured acceptable; both move to events together when
it is not.

## 49. *Superseded.* Deleting a namespace leaves its objects

**Superseded by entry 9 (objects are namespace contents)** (adversarial review, 2026-10-29). The original reasoning, kept as written (entry numbers in it refer to the original ranking):

**Call.** A namespace's emptiness check and `--cascade` ignore objects.
Objects in a deleted namespace stay readable and deletable; applying one is
refused until the namespace exists again.

**Alternatives.** Count objects as contents (refusing the delete), and have
`--cascade` delete them, as Kubernetes does.

**Trade-off.** An app can be left acting on objects in a namespace that no
longer exists (GitOps would fail to deploy into it, and report that in
status).

**Why (scope).** Cascading needs a per-kind ACL check inside the cascade
and a proto change to the namespace contents reply; the standalone scaling
policies O2 replaces behave the same way today. Recorded as a TODO with the
plan's lean (refuse, and cascade).

## 50. *Superseded.* Defaults filled on read (the defaults half of the original entry 1)

**Superseded by entry 6** (adversarial review, 2026-10-29). The original
argued that deriving defaults on read lets an upgrade that adds or changes
a default reach every object with no rewrite, which a raft-replicated
upgrade would otherwise have to batch and could leave half done. The review
showed the cost: a changed default silently changes what the app does with
no generation bump, event, or actor. The original entry's full text is kept
as entry 4's body.
