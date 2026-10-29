# App Objects

An [app](apps.md) can declare **object kinds**: new kinds of thing people
write to Legion, the way Kubernetes CRDs add resources to its API. The
`gitops` app declares `Repo`; people apply Repos; gitops watches them,
deploys what each names, and reports back on each one.

```console
$ legion object kinds
KIND         SCOPE       ACCESS                                     STATE   DESCRIPTION
gitops/Repo  namespaced  object (+ job:write on what spec.jobs names)  active  A git repository gitops deploys jobs from into this namespace.

$ legion object apply -f repo.ncl --wait
gitops/Repo team-a/storefront: created (generation 1, spec version 41)
gitops/Repo team-a/storefront: Ready (Synced: at 3f9a1c2)

$ legion object list repos -n team-a
NAME        STATUS  AGE  BRANCH  COMMIT
storefront  Ready   2m   main    3f9a1c2
```

Legion keeps the objects: it checks each one against its kind's schema and
against what writing it requires of the writer, stores it on the managers
(replicated with the rest of the control state), versions it, and serves it
to the CLI, the API, the MCP server, and the app. The app only reconciles.

Two rules shape everything below:

- **An object can never make its app do more than its writer could do
  directly.** A kind declares what writing one of its objects requires
  (`requires`), Legion checks it against the writer on every write, and
  records what it checked on the object.
- **Writing never depends on the app being up.** Every check that can
  refuse a write runs in the manager. The app's own check is advice and
  fails open.

## Declaring a kind

A manifest's `kinds` lists them. The shipped example,
[`configs/apps/example-app.ncl`](../configs/apps/example-app.ncl):

```nickel
kinds = [
  {
    name = "Repo",
    description = "A git repository gitops deploys jobs from into this namespace.",
    scope = "namespaced",
    schema = {
      type = "object",
      required = ["url"],
      properties = {
        url = { type = "string", description = "The repository URL." },
        branch = { type = "string", default = "main" },
        path = { type = "string", default = "jobs/" },
        jobs = { type = "array", items = { type = "string" } },
        deployKey = { type = "string", writeOnly = true },
      },
    },
    status = { schema = { type = "object", properties = { commit = { type = "string" }, jobs = { type = "integer" } } } },
    examples = [{ url = "https://github.com/acme/deploy", path = "jobs/storefront" }],
    validate = true,
    access = { resource = "object" },
    requires = [{ resource = "job", actions = ["write"], name = "/spec/jobs" }],
    immutable = ["/spec/url"],
    cleanup = "10m",
    maxObjects = 500,
    columns = ["/spec/branch", { name = "COMMIT", pointer = "/status/fields/commit" }],
  },
],
```

| Field | Meaning |
| --- | --- |
| `name` | PascalCase and singular: a capital letter, then letters and digits, at most 63. The kind's full name is `<app>/<name>` (`gitops/Repo`). |
| `description` | One line, shown by `legion object kinds` and `explain`. |
| `scope` | `namespaced` (the default) or `cluster`. A cluster-scoped object has no namespace. |
| `schema` | JSON Schema (draft 7) for the spec: an object schema, at most 64 KiB, local `$ref`s only. **Closed by default**: a subschema with `properties` and neither `additionalProperties` nor `patternProperties` refuses fields it does not name, so a typo is an error, not a silently ignored field. Set `additionalProperties` to open one. `allOf`, `anyOf`, `oneOf`, `not`, and `if`/`then`/`else` branches are left open. |
| `status.schema` | The schema of the status `fields` the app reports. Same rules. |
| `examples` | Specs, each checked against the schema at install. |
| `validate` | Also ask the app on each apply that changes the spec ([The validate call](#the-validate-call)). |
| `access.resource` | What governs the kind's objects: `object` (the default), `job` or `scale` for a namespaced kind, `node` for a cluster kind ([Access](#access)). |
| `requires` | What writing or deleting an object also needs of its writer, scoped from the object ([Requires](#requires)). |
| `immutable` | JSON pointers (`/spec/url`) an update may not change. |
| `writeOnly: true` | On any property: the value is a secret, shown as `"<redacted>"` on every read but the owning app's watch. |
| `cleanup` | How long a delete waits for the app to release the object: a duration, at most `1h`. Default: none, deletes are immediate ([Deleting](#deleting)). |
| `maxObjects` | At most this many objects of the kind, cluster-wide. Default 10,000, at most 100,000. |
| `columns` | Up to 8 extra columns for `legion object list`: a pointer (its last segment, upper-cased, is the header) or `{ name, pointer }`. |
| `rules` | Reserved for cross-field validation rules; refused today. |

Every pointer in a declaration (`requires`, `immutable`, `columns`) points
into the same document, the object as `legion object get -j` shows it:
`{ name, namespace, labels, spec, status }`.

Install refuses a kind name that is one of Legion's own nouns (`Job`,
`Node`, `Namespace`, `Policy`, `Object`, `Kind`, every top-level CLI command,
every ACL resource, in any case, singular or plural), two kinds of one app
that differ only by case or plural, a schema that does not compile, an
example that fails its schema, an access mapping that does not fit the
scope, and `rules`. The install prompt lists each kind with what writing it
takes:

```text
Kinds:
  gitops/Repo            namespaced; written with object:write on gitops/Repo, and job:write on what spec.jobs names; checked by the app (advisory); deletes wait up to 10m for the app
```

Two apps may both declare `Repo`: they are different kinds, `gitops/Repo`
and `reposync/Repo`.

## Objects

An object file holds one object or a list of them
([`configs/apps/example-repo.ncl`](../configs/apps/example-repo.ncl)):

```nickel
{
  kind = "gitops/Repo",           # always qualified: <app>/<Kind>
  name = "storefront",            # a DNS label: lowercase, digits, '-'; at most 63
  namespace = "team-a",           # namespaced kinds; default -n, then $LEGION_NAMESPACE, then `default`
  labels = { team = "storefront" },
  spec = {
    url = "https://github.com/acme/deploy",
    path = "jobs/storefront",
    jobs = ["storefront", "storefront-worker"],
  },
}
```

`kind` must be qualified in a file and in the API, so a file means the same
thing on every cluster, whatever else is installed. `status` belongs to the
app: a file that sets it is refused. Files are Nickel or JSON, evaluated by
the CLI like jobspecs.

Each stored object has:

- a **uid**, assigned at create and never reused. A delete and a recreate
  under the same name are different objects; status, and whatever the app
  keys to the object, never carries over;
- its **spec, with the schema's defaults filled at write**. A later upgrade
  that changes a default does not move existing objects; one that adds a
  defaulted field fills it on read until the object is next written;
- a **generation**, from 1, bumped when the spec changes;
- a **spec version**, the compare-and-set token (`--expected-version`): new
  on every write of the spec or labels, never on a status report;
- labels, who created and last wrote it and when, and its **authority**
  (what its last write was checked against, [Requires](#requires));
- **managed by**: the app whose own identity wrote it, if one did;
- a **state**: `valid`; `invalid` when the stored spec fails the kind's
  current schema (an upgrade tightened it; the object stays readable and
  deletable, and the next apply must conform); `orphaned` when no installed
  app declares the kind any more.

Limits: a spec is at most 64 KiB with its defaults; at most 32 labels, keys
at most 63 characters, values at most 256; the kind's `maxObjects`, counted
in the same replicated write that creates an object.

### Applying

```console
$ legion object apply -f repo.ncl --dry-run
gitops/Repo team-a/storefront: would change (generation 3 → 4); dry run, nothing written
  spec.branch: "main" → "release"
  spec.deployKey: changed (redacted)

$ legion object apply -f repo.ncl --expected-version 117
Error: gitops/Repo team-a/storefront is at spec version 118, not 117: it changed since you read it
Hint: read it again (legion object get gitops/Repo team-a/storefront) and apply on top of it
```

An apply runs, in order: the kind lookup; `write` on the kind's access
resource ([Access](#access)); the schema (unknown fields fail with a
did-you-mean: `spec.brnch: unknown field (did you mean branch?)`);
`immutable`; `requires` ([Requires](#requires)); the app's validate call
when the kind asks for one; then the write. `--dry-run` runs all of it and
prints the field diff instead of writing. `--expected-version N` writes only
if the object is at spec version `N` (`0`: only if it does not exist yet).
`--idempotency-key` makes a retry safe. Applying an unchanged spec and
labels writes nothing and keeps the generation.

`--wait` then waits (default 2 minutes, `--timeout`) until the app has
reported on the new generation: `Ready` or `Accepted` succeeds; `Rejected`,
`NotReady`, `Invalid`, or `Orphaned` fails with the reason. The object is
stored either way; `--wait` only says whether the app has caught up.

### Reading

```console
$ legion object get repo storefront -n team-a
gitops/Repo team-a/storefront (valid)  Ready (Synced: at 3f9a1c2)
uid 812  generation 4 (observed 4)  spec version 118
labels: team=storefront
created by alice 3d ago; updated by bob 2h ago
authority (checked against bob): job:write in team-a on storefront (from spec.jobs); job:write in team-a on storefront-worker (from spec.jobs)
spec:
  { "branch": "release", "deployKey": "<redacted>", ... }
status (reported by gitops 40s ago):
  Accepted=true
  Ready=true (Synced): at 3f9a1c2
  fields: {"commit":"3f9a1c2","jobs":2}

$ legion object list -A                # every kind, every namespace you can read
$ legion object list repos -l team=storefront
$ legion object explain gitops/Repo
$ legion object explain repo.spec.branch
gitops/Repo.spec.branch: string, default "main"
  The branch to deploy.
```

`get`, `list`, `delete`, and `explain` take a **short name** for the kind
(any case, plural or not: `repo`, `Repos`) when exactly one installed kind
has it, and otherwise list the candidates:

```text
Error: "repo" names 2 kinds: gitops/Repo, reposync/Repo; name one
```

An object's name may be written `namespace/name`. `writeOnly` values read
as `"<redacted>"` everywhere a person reads them: get, list, the diff, and
the validate call's errors.

### Managed objects

An object written by an app's own identity (its `legion` token, not a
viewer the app acts for) records **managed by** that app, as the jobs an
app submits do. A person's apply or delete of a managed object is refused
without `--force`, since the app would likely overwrite the change.

## Access

Every read and write of an object checks the kind's **access resource** on
the object's scope:

| `access.resource` | Namespaced kind | Cluster kind |
| --- | --- | --- |
| `object` (default) | the object's namespace and name, and the kind | namespace `*` (only a rule with `namespaces = ["*"]` reaches it), name, and kind |
| `job`, `scale` | the object's namespace, namespace-wide (name `*`) | refused at install |
| `node` | refused at install | cluster-wide, plus the kind |

The **`object`** ACL resource is namespaced, with a **`kinds`** selector of
globs over `<app>/<Kind>`:

```nickel
[
  { resources = ["object"], actions = ["read", "write"], namespaces = ["team-a"], kinds = ["gitops/Repo"] },
  { resources = ["object"], actions = ["read"], namespaces = ["*"], kinds = ["gitops/*"] },
]
```

`names` narrows by object name. A kind mapped to `job` or `scale` is
checked namespace-wide, so only a `job` (or `scale`) rule without a `names`
selector, or with `names = ["*"]`, reaches it: selectors written about job
names never decide who writes objects. A `job` mapping carries band
`normal`, as a plain submit does, so band-scoped `job` rules still match.

A `resources = ["*"]` rule reaches objects of every kind in its namespaces,
including kinds installed later, as it does for every other resource. What
it lets a writer do is still bounded by `requires`.

**Check before fetch.** A caller who may not read the kind on the object's
scope gets `PermissionDenied` whether or not the object exists, and a
caller with no object rights at all learns nothing about which kinds exist.
`legion object kinds` lists only the kinds you may read somewhere; list
calls return only the objects you may read.

### Requires

`requires` says what an object makes its app do, so Legion can demand it of
whoever writes the object:

```nickel
requires = [
  { resource = "job", actions = ["write"] },                                     # in the object's namespace, every job
  { resource = "job", actions = ["write"], name = "/spec/jobs" },                # the jobs it names (a string or a list)
  { resource = "job", actions = ["write"], namespace = "/spec/targetNamespace" },
  { resource = "kv",  actions = ["read"],  path = "/spec/credentials" },          # a key it names
  { resource = "job", actions = ["write"], host = true, when = "/spec/allowHost" },
  { resource = "operator", actions = ["write"] },
]
```

Each entry is resolved against the object with its defaults, and checked
against the writer with the scope the matching core call would check, on
every apply and delete, fail-closed:

| Key | For | Default when absent, or when the pointer names nothing |
| --- | --- | --- |
| `namespace` | namespaced resources | the object's namespace; `*` for a cluster kind's object |
| `name` | namespaced resources (job names for `job`) | `*`: namespace-wide |
| `path` | `kv` | the entry does not apply |
| `pool` | `node` | none |
| `band` | `job` | `normal` |
| `template` | `job` | none |
| `host` | `job` | `false`; `true` checks host-level work, which only a `host = true` rule covers |
| `when` | any | the entry always applies; with a pointer, only when it names `true`, a non-empty string, list, or record, or a non-zero number |

A pointer naming a list resolves to one permission per value (at most 64
per object). The refusal names what is missing:

```text
Error: writing gitops/Repo team-a/api needs job:write in team-a on api (from spec.jobs), which your token does not hold
Hint: the kind requires it of whoever writes its objects (`legion object explain gitops/Repo`); have someone who holds it make the change
```

What was checked is stored on the object as its **authority**: the
resolved permissions, who held them (principal and token accessor), and
when. Re-applying an unchanged object as someone else records them as the
authorizer (`--dry-run` says `would record you as its authorizer`). An
upgrade that changes `requires` applies to later writes only; the upgrade
prompt counts the objects authorized under the previous requirements.

The app's own writes are checked the same way, against its grant: an app
writes objects (its own kind's or another's) only as far as its manifest's
`permissions` reach.

## The validate call

A kind with `validate = true` is also checked by its app, after every core
check passes, on each apply that changes the spec (dry runs too): the
manager sends `POST /legion/v1/objects/validate` to one of the app's
allocations, with the writer as the viewer, the way the
[extension validate call](apps.md#the-validate-call) works. The body:

```json
{
  "kind": "gitops/Repo", "namespace": "team-a", "name": "storefront",
  "operation": "update",
  "spec": { "url": "https://github.com/acme/deploy", "deployKey": "..." },
  "effective": { "url": "https://github.com/acme/deploy", "branch": "main", "deployKey": "..." },
  "previous": { "...": "the stored spec, or null on create" },
  "labels": { "team": "storefront" }
}
```

The app answers `{ "errors": [...], "warnings": [...] }`: errors refuse the
apply (`the gitops app refused it: ...`), warnings come back with the reply.

The call **fails open**: an app that is disabled, unreachable, slower than 2
seconds, or answers anything but `200` lets the write through with a
warning (`gitops/Repo team-a/storefront was not checked by the gitops app
(...)`). It is a convenience, never a control: a writer can make it time
out. Anything that must refuse a write belongs in the schema, `immutable`,
or `requires`. Apps answer it with `UiRegistry::object_validate`
([Writing an app that owns a kind](#writing-an-app-that-owns-a-kind)).

## Status

The app reports each object's status with `UpdateObjectStatus`:

```json
{
  "observedGeneration": 4,
  "conditions": [
    { "type": "Accepted", "status": "true" },
    { "type": "Ready", "status": "true", "reason": "Synced", "message": "at 3f9a1c2", "since": "2026-10-29T10:02:11Z" }
  ],
  "fields": { "commit": "3f9a1c2", "jobs": 2 }
}
```

- **Conditions** have a PascalCase `type`, a status of `true`, `false`, or
  `unknown`, a `reason` (letters, digits, `_`, `-`), and a message of at
  most 1 KiB; at most 16. `Accepted` (the app understood and accepted the
  spec) and `Ready` (reconciled and working) are standard; apps may add
  their own. The manager stamps `since` when a condition's status changes.
- **Fields** are checked against the kind's `status.schema`, at most 16
  KiB. Status is for facts that change when the world does (the deployed
  commit), not for per-tick telemetry.
- `observedGeneration` is the generation the app reconciled. The manager
  refuses one above the object's current generation and one below the
  stored status's, so a slow writer never overwrites newer status, and a
  write for a uid that is not the current object's (deleted and recreated
  in between) is refused.
- Status is stored apart from the object, on the managers' status lane: a
  report never changes the spec version and never wakes the app's watch.
  A report identical to the stored one writes nothing.

Every object has a **summary**, whatever its kind, in this order of
precedence:

| Summary | When |
| --- | --- |
| `Orphaned` | no installed app declares the kind |
| `Deleting` | a delete is waiting for the app's cleanup |
| `Invalid` | the stored spec fails the kind's current schema |
| `Stale` | the app is disabled or has no healthy allocation; the status shown is as old as it says |
| `Pending` | no status yet, or `observedGeneration` is below the generation |
| `Rejected: <reason>` | `Accepted` is `false` |
| `NotReady: <reason>` | `Ready` is `false` |
| `Ready` | `Ready` is `true` |
| `Accepted` | `Accepted` is `true` and the app reports no `Ready` |

The summary's detail is the condition's reason and message.

## Deleting

```console
$ legion object delete repo storefront -n team-a
gitops/Repo team-a/storefront: deleting; the gitops app has until its cleanup deadline (10m) to release it (`--now` skips the cleanup)
$ legion object delete repo storefront -n team-a --wait     # until it is gone
$ legion object delete repo storefront -n team-a --now      # skip the cleanup
```

A delete needs what writing needs: `write` on the access resource and the
kind's `requires`. Without a `cleanup` window, the object goes at once. With
one, it turns **Deleting**: its spec is frozen (applies are refused), the
app sees it on its watch with `deleting = true`, cleans up whatever the
object made, and calls `ReleaseObject`, which removes it. When the window
ends first, or the kind is orphaned, the manager removes it anyway and
emits `object.cleanup_abandoned`. `--now` skips the window (same rights).
Nothing can stay stuck in Deleting, which is the difference from Kubernetes
finalizers. `--expected-version` and `--force` work as for apply.

Legion keeps no owner references and does no garbage collection: an app
labels what it creates with the object's uid and removes it when the
object is gone.

## Lifecycle

- **Install** registers the app's kinds. Objects an earlier install of the
  same app left orphaned are adopted: the prompt says how many, they are
  checked against the new schema (failures read `Invalid`), and they keep
  their recorded authority.
- **Upgrade** may add kinds and change schemas, access, `requires`,
  `immutable`, `cleanup`, columns, and limits. The prompt reports existing
  objects that fail the new schema, defaults that changed (and how many
  objects keep the old value), and objects authorized under different
  `requires`. Dropping a kind that still has objects needs `--force` and
  orphans them. Changing a kind's `scope` while it has objects is refused.
- **Disable** changes nothing about objects: they stay writable, the
  validate call fails open, and their summary reads `Stale`.
- **Uninstall** with objects needs `--force` and leaves them, orphaned:
  readable and deletable with their kind's recorded access, never written.
  An app's lifecycle never deletes objects.
- An **orphaned kind** stays listed by `legion object kinds` (state
  `orphaned`) while it has objects, and is dropped on the app reconciler's
  next pass after its last object goes.
- **Namespaces.** Objects are namespace contents: deleting a namespace that
  holds objects is refused (`namespace 'team-a' is not empty: 1 object`), and
  `legion namespace delete --cascade` deletes them first, checking the
  caller's access and `requires` for each, and honoring each kind's
  cleanup. A Deleting object does not hold its namespace.

## Writing an app that owns a kind

An app reads its objects with **`WatchObjects`** and reports with
**`UpdateObjectStatus`** and **`ReleaseObject`**. All three are app-plane:
only the owning app's own identity (or a management token naming the app)
may call them, and an app reaches only its own kinds.

`WatchObjects` streams the app's objects of the kinds it names (all of
them by default): a snapshot first, split into messages of at most 1,000
objects and about 2 MiB (`snapshot` on the first, `snapshotDone` on the
last), then a message whenever objects change (`upserts`) or go
(`deletes`, tombstones carrying the uid). Each object carries its uid,
generation, spec with `writeOnly` values revealed, its status as of that
read, `deleting`, and its `shard`. A status report wakes nothing; another
app's writes wake nothing. After a reconnect, the app gets a fresh snapshot
and treats a uid it no longer sees as deleted. To split the work, a replica
asks for `shards` of `shard_count`: an object's shard is the FNV-1a hash of
`<app>/<Kind>:<namespace>/<name>` modulo the count.

The Rust app framework (`legion::apps::objects`) wraps this in a reconcile
loop:

```rust
use legion::apps::objects::ObjectOutcome;

context.start_reconciler(
    context
        .reconciler("Repo")
        .resync(Duration::from_secs(300))
        .on_cleanup(|request| async move { undeploy(&request.object.id).await }),
    |request| async move {
        let spec: RepoSpec = request.spec_as()?;
        if !spec.url.starts_with("https://") {
            return Ok(ObjectOutcome::rejected("UnsupportedUrl", "only https repositories"));
        }
        match sync(&spec).await {
            Ok(commit) => Ok(ObjectOutcome::ready()
                .because("Synced", format!("at {commit}"))
                .with_fields(json!({ "commit": commit }))),
            Err(err) => Ok(ObjectOutcome::not_ready("SyncFailed", err.to_string())),
        }
    },
)?;
```

- It is **level-triggered**: the handler runs for every object at start,
  whenever an object's spec or state changes, and on a periodic resync
  (default 5 minutes). It must be idempotent.
- Calls for **one object never overlap**; at most 4 objects reconcile at
  once (`concurrency`).
- An `Err`, or an object whose status write fails, retries with
  exponential backoff (1 second, doubling, at most 5 minutes); an `Err` is
  reported as `Ready=false` with reason `ReconcileFailed`.
- The outcome becomes the `Accepted` and `Ready` conditions and the status
  `fields` (`ready`, `not_ready`, `rejected`, `accepted`, plus
  `with_condition`, `because`, `with_fields`, `requeue_after`), written
  with the generation the handler saw, **only when they differ** from what
  is stored.
- A **deleting** object goes to the `on_cleanup` handler instead, and is
  released when it returns `Ok`; without a handler it is released at once.
- `shards(count, mine)` reconciles a static subset of the objects.

`ObjectCache`, `watch_objects`, and `write_object_status` are there for an
app that wants its own loop; `UiRegistry::object_validate(kind, handler)`
answers the validate call (without a handler, every object passes). An app
not written in Rust calls the three RPCs and serves
`POST /legion/v1/objects/validate` itself; `proto/legion.proto` documents
them.

## Events and metrics

| Event | When |
| --- | --- |
| `object.applied` | an object was created or its spec or labels changed (generation, who) |
| `object.deleted` | an object was removed: deleted, released, or cascaded |
| `object.deleting` | a delete started the kind's cleanup window (the deadline) |
| `object.cleanup_abandoned` | the manager removed a Deleting object the app did not release (window over, or kind orphaned) |
| `object.status_changed` | an object's `Ready` or `Accepted` condition changed (from, to, reason), not every report |

Events carry the object's kind, namespace, name, uid, and the actor, never
a spec, status fields, or a redacted value. An `object.*` event reaches only
a caller who may read that kind on that object's scope (or holds
`operator:read`): reaching the namespace is not enough.
`legion_object_status_writes_total{app}` counts status writes on
`/metrics`.

## API, CLI, and MCP reference

| CLI | RPC |
| --- | --- |
| `legion object kinds [--app APP]` | `ListObjectKinds` |
| `legion object explain <KIND>[.<field path>]` | `ListObjectKinds` |
| `legion object apply -f FILE [-n NS] [--dry-run] [--expected-version N] [--force] [--wait [--timeout D]] [--idempotency-key K]` | `ApplyObject` |
| `legion object get <KIND> <NAME> [-n NS]` | `GetObject` |
| `legion object list [KIND] [-n NS \| -A] [-l k=v]` | `ListObjects` |
| `legion object delete <KIND> <NAME> [-n NS] [--expected-version N] [--now] [--force] [--wait [--timeout D]] [--idempotency-key K]` | `DeleteObject` |
| (the owning app) | `WatchObjects`, `UpdateObjectStatus`, `ReleaseObject` |

Every command takes `--json`. The MCP server lists `legion_object_kinds`
(with each kind's schemas and examples, so an agent can write a valid
object), `legion_object_list`, and `legion_object_get`; with
`--allow-write`, `legion_object_apply` (with `wait`) and
`legion_object_delete` ([Agent Readiness](agent-readiness.md)). An apply of
a kind no installed app declares fails with code `KIND_NOT_INSTALLED` on a
retryable `UNAVAILABLE`, so a sync that installs an app and applies its
objects in one pass converges on retry.

## Failure modes

| You see | Why | What to do |
| --- | --- | --- |
| `KIND_NOT_INSTALLED` (retryable) | no installed app declares the kind | install the app, or retry once it is |
| `kind "Repo" must be qualified with its app: kind = "gitops/Repo"` | files and the API take `<app>/<Kind>` | qualify it |
| `"repo" names 2 kinds` | a short name more than one app declares | name the kind in full |
| `spec.x: unknown field` | the schema is closed | fix the field; `legion object explain` lists them |
| `spec.url is immutable` | an update changed an `immutable` pointer | delete and recreate |
| `PermissionDenied ... needs job:write ...` | the kind's `requires` | have someone who holds it write the object |
| `... is managed by the gitops app` | the app wrote it | `--force` to change it anyway |
| `... is being deleted` | its cleanup window is running | wait (`delete --wait`), then apply |
| `ABORTED ... changed since you read it` | `--expected-version` is stale | read it again and re-apply |
| `RESOURCE_EXHAUSTED ... maxObjects` | the kind is full | delete objects, or raise `maxObjects` in the app |
| `... was not checked by the gitops app (...)` (warning) | the validate call failed open | nothing: the write went through; check the app |
| summary `Stale` | the app is disabled or not healthy | `legion app status <app>` |
| summary `Pending` that stays | the app has not reported on this generation | `legion app logs <app>` |

## Limitations

- **No per-namespace quotas for objects.** `maxObjects` caps a kind
  cluster-wide; namespace policies do not count objects yet.
- **No cross-field validation in the manager.** Rules such as `min <= max`
  are the app's (`Rejected`) until CEL `rules` land; `rules` is refused
  today.
- **The validate call is advice**, and a dry run does not prove the app
  would accept the object later.
- **No resume for the watch.** A reconnect relists the app's objects; that
  is comfortable to about 50,000 objects per app.
- **One schema version per kind.** There is no conversion between schema
  versions; an upgrade that tightens a schema leaves non-conforming objects
  `Invalid` until rewritten.
- **Authority is recorded, not enforced on the app.** The record says what
  an object's writer was checked for; the app still acts with its own
  grant. Bounding an app's calls by an object's authority is planned, not
  shipped.
- **`writeOnly` values are stored like any spec**, in the replicated state
  store, and readable by the owning app and anyone with `state:read`.
  Keep real credentials in KV (encrypted at rest) and name them by path
  with a `requires` on `kv:read`.
- **The status lane may lag the spec**: on a follower, a status can
  briefly describe an older generation; `observedGeneration` says which.
- **No web UI for objects yet**; the CLI, API, and MCP cover them.
- **The framework is Rust only.** Other apps use the RPCs directly.
