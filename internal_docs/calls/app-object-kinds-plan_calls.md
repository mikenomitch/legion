# Judgement Calls: app-object-kinds-plan

Decisions made while designing and building
[`plans/app-object-kinds-plan.md`](../plans/app-object-kinds-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.**

## 1. Objects are one generic table, spec stored as written, state derived on read

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

## 2. A kind's ACL is one resource: the generic `object`, or an existing one of the same scope

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

## 3. The validate call fails open

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

## 4. `object` is a namespaced resource; a cluster kind's objects carry namespace `*`

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

## 5. An orphaned object is governed by the `object` resource

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

## 6. Status is the owning app's alone, with CAS on version or generation

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

## 7. No schema versions or conversion in v1

**Call.** A kind has one schema. An upgrade that changes it re-checks every
object and flags failures `invalid`; nothing converts them.

**Alternatives.** Kubernetes' versioned CRDs with a storage version and
conversion webhooks.

**Trade-off.** An app that renames a field breaks its users' objects until
they re-apply. Apps must evolve schemas additively or accept that.

**Why (simplicity).** Legion has no installed base (AGENTS.md); conversion
webhooks are the most complex part of CRDs and the most often broken. The
upgrade prompt makes the cost visible before anyone pays it.

## 8. Kind names are flat, unique cluster-wide case-insensitively, with `<app>/<Kind>` as the qualified spelling

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

## 9. The reserved-noun list is every CLI noun, every ACL resource, and the core object nouns

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

## 10. Watch shards use the autoscaler's FNV-1a over a per-object key, not the extensions' SHA-256 of the job id

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

## 11. A cluster kind ignores the request's default namespace but refuses a namespace written in the object

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

## 12. `WatchObjects` polls a fingerprint each second

**Call.** The watch re-reads when the apps' fingerprint or the table's
(count, highest version) moves, checked every second, with a full re-read
every 30 seconds.

**Alternatives.** Event-driven delivery from the store's change feed.

**Trade-off.** Up to a second of latency, and one cheap query per stream
per second.

**Why (simplicity).** The same shape as `WatchAppExtensions` (apps calls
90), whose cost was measured acceptable; both move to events together when
it is not.

## 13. Deleting a namespace leaves its objects

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

## 14. O2 keeps scaling defaults in core

**Call.** When the autoscaler's standalone policies become objects, the
cluster's scaling defaults (`legion scaling defaults apply`) stay a core
table and RPC.

**Alternatives.** Make them a `ScalingDefaults` kind the autoscaler owns.

**Trade-off.** One autoscaler concept stays outside the generic system.

**Why (correctness).** The manager resolves a task's `autoscale` against the
defaults at submit and in `job plan`, synchronously, and must not depend on
an app being up to admit a job.

## 15. Object versions come from the `app_object` counter in `scaling_counter`

**Call.** `version` is the next value of the `app_object` counter in the
existing `scaling_counter` table.

**Alternatives.** A new counter table, or `MAX(version) + 1`.

**Trade-off.** A table named for scaling holds a counter for every app's
objects.

**Why (simplicity).** The table is a generic name-to-counter map on the
Control lane already, and `MAX + 1` would repeat a version after the newest
object is deleted and recreated, which breaks CAS.
