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
validation rules and against what writing it requires of the writer,
stores it on the managers
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

And the app is held to the first rule too: when it acts on an object, it
can [act for the object](#acting-for-an-object), with exactly what the
object's writer was checked for, rather than with its own grant.

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
    rules = [
      {
        rule = "!has(self.deployKey) || self.url.startsWith('ssh://') || self.url.startsWith('git@')",
        message = "a deploy key is an SSH key: use an ssh:// or git@ URL with it",
        path = "/spec/deployKey",
      },
    ],
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
| `rules` | CEL expressions every apply's spec must pass, each with a `message` and an optional `path` ([Validation rules](#validation-rules)). At most 32. |
| `writeOnly: true` | On any property: the value is a secret, shown as `"<redacted>"` on every read but the owning app's watch. |
| `cleanup` | How long a delete waits for the app to release the object: a duration, at most `1h`. Default: none, deletes are immediate ([Deleting](#deleting)). |
| `maxObjects` | At most this many objects of the kind, cluster-wide. Default 10,000, at most 100,000. |
| `columns` | Up to 8 extra columns for `legion object list`: a pointer (its last segment, upper-cased, is the header) or `{ name, pointer }`. |

Every pointer in a declaration (`requires`, `immutable`, a rule's `path`,
`columns`) points into the same document, the object as `legion object get -j` shows it:
`{ name, namespace, labels, spec, status }`.

Install refuses a kind name that is one of Legion's own nouns (`Job`,
`Node`, `Namespace`, `Policy`, `Object`, `Kind`, every top-level CLI command,
every ACL resource, in any case, singular or plural), two kinds of one app
that differ only by case or plural, a schema that does not compile, an
example that fails its schema or rules, a rule that does not compile or
breaks a cost bound, and an access mapping that does not fit the scope. The
install prompt lists each kind with what writing it takes:

```text
Kinds:
  gitops/Repo            namespaced; written with object:write on gitops/Repo, and job:write on what spec.jobs names; 1 validation rule; checked by the app (advisory); deletes wait up to 10m for the app
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
  current schema or rules (an upgrade tightened them; the object stays
  readable and deletable, and the next apply must conform); `orphaned`
  when no installed app declares the kind any more.

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
`immutable`; the kind's rules ([Validation rules](#validation-rules));
`requires` ([Requires](#requires)); the app's validate call when the kind
asks for one; then the write. `--dry-run` runs all of it and
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
app submits do. So does one the app writes while [acting for another
object](#acting-for-an-object) (cert-manager creating a child for its
parent). A person's apply or delete of a managed object is refused
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

## Validation rules

A schema checks each field on its own. A kind's **`rules`** check what a
schema cannot say, usually across fields: [CEL](https://cel.dev)
expressions that must be true, evaluated by the manager on every apply,
fail-closed, whether or not the app is up. They are Legion's answer to
validating webhooks, as `x-kubernetes-validations` is Kubernetes':

```nickel
rules = [
  { rule = "self.min <= self.max", message = "min must not exceed max", path = "/spec/min" },
  {
    rule = "self.checks.all(c, self.checks.exists_one(d, d.name == c.name))",
    message = "every check needs its own name",
    path = "/spec/checks",
  },
  # A transition rule: it uses oldSelf, so it checks updates only.
  { rule = "self.replicas >= oldSelf.replicas", message = "replicas may only grow" },
],
```

| Key | Meaning |
| --- | --- |
| `rule` | A CEL expression that must evaluate to `true`. At most 1 KiB. |
| `message` | What a failure says. Required, at most 512 bytes, fixed text (`messageExpression` is not supported). |
| `path` | A JSON pointer into the spec (`/spec/min`), like every pointer in the declaration: where the failure is reported. Default: the spec as a whole. |

What a rule sees:

- **`self`** is the spec as it will be stored: the writer's spec with the
  schema's defaults filled. A JSON object is a CEL map: `self.min` reads a
  field (and fails, `no such key`, when it is absent), `has(self.min)`
  tests for one, `'team' in self.labels` for a key. Integers are `int`,
  other numbers `double`, and the two compare (`self.ratio < 1`). Lists
  are lists.
- **`oldSelf`** is the stored spec, defaults filled, when an apply updates
  an object. A rule that uses it is a **transition rule**: checked on
  updates only, never on create, and never when a stored object is judged
  on read. `immutable` is still the plain way to freeze a field; a
  transition rule says how one may change.
- Nothing else: a rule that names another variable is refused at install.

**When they run.** An apply evaluates every rule after the schema and
`immutable` and before `requires` ([Applying](#applying)); dry runs too.
Every failing rule is reported, each at its path:

```text
$ legion object apply -f inverted.ncl
Error: autoscaler/ScalingPolicy media/render: spec.min: min must not exceed max; spec.checks: every check needs its own name
Hint: `legion object explain autoscaler/ScalingPolicy` lists the kind's rules
```

A rule also fails, and refuses the write, when it errors (it read a field
the spec lacks: guard optional fields with `has()`), evaluates to something
other than a bool, or runs past its budget (below); the message says which,
without the values involved. Install and upgrade compile every rule and
check every example against the non-transition ones (an example is a
create). An upgrade that adds or tightens rules re-checks the stored
objects: its prompt lists those that fail, and they read `invalid`
(summary `Invalid`) until their next apply conforms. `legion object
explain` prints a kind's rules, and `ListObjectKinds` (`legion object kinds
-j`, the MCP `legion_object_kinds`) returns them.

**Cost.** A rule over a large list can be expensive, so the manager bounds
every rule, by construction:

| Bound | Limit |
| --- | --- |
| Rules per kind | 32 |
| A rule's source | 1 KiB |
| Bracket nesting (`(`, `[`, `{`, counted on the raw text before any parsing) and conditionals (`? :`) | 32 each |
| Expression depth, once parsed (evaluation recurses once per level) | 24 |
| Comprehensions (`all`, `exists`, `exists_one`, `map`, `filter`) nested in one another | 2 |
| Time | 10 ms per rule, 50 ms for all of a kind's rules on one write |
| The spec | 64 KiB with its defaults; 128 levels deep |

Every comprehension checks its rule's deadline on each iteration, so even a
nested `all` over a 20,000-item list stops after 10 ms. A rule that runs
past its budget fails and refuses the write (`... (not checked: rules[2]
ran past its 10 ms budget, so the write is refused)`), whatever it would
have returned. An apply evaluates its rules on a separate thread and waits
at most 100 ms for them: work between two deadline checks (a long chain of
list concatenations over a large list) cannot be interrupted, so it
finishes on that thread, but the write is refused at the timeout either
way.

Typical rules are cheap: one comparison like `self.min <= self.max` costs
about 10 to 20 microseconds, including turning a policy-sized spec into CEL
values, and compiling one at install about 1 to 50 microseconds (release
build). The 64 KiB spec limit bounds the conversion: about 3 ms for a
30,000-item list.

**The CEL subset.** Rules run on the [`cel`](https://crates.io/crates/cel)
crate (cel-rust) 0.15.0 with its standard library and none of its optional
features: the operators, `size`, `has`, `in`, `startsWith`, `endsWith`,
`contains`, `type`, the conversions (`int`, `uint`, `double`, `string`),
and the macros `all`, `exists`, `exists_one`, `map`, and `filter`. Not
available: `matches` (regular expressions), durations and timestamps, the
string, list, and math extensions, optional syntax (`self.?x`), and
message construction. There is no type checker: a rule that compiles can
still fail at evaluation, which refuses the write, so try a new kind's
rules against a few objects with `--dry-run`.

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
`rules`, or `requires`. Apps answer it with `UiRegistry::object_validate`
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
| `Invalid` | the stored spec fails the kind's current schema or rules |
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
  checked against the new schema and rules (failures read `Invalid`), and
  they keep
  their recorded authority.
- **Upgrade** may add kinds and change schemas, rules, access, `requires`,
  `immutable`, `cleanup`, columns, and limits. The prompt reports existing
  objects that fail the new schema or rules (they read `Invalid`), defaults
  that changed (and how many
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
- `shards(count, mine)` reconciles a static subset of the objects;
  `shards_from(selection)` follows a subset that moves (a
  `tokio::sync::watch` channel of `ShardSelection`, as lease-based shards
  publish it), opening a new watch whenever it changes.

`ObjectCache`, `watch_objects`, `run_object_watch` (a cache kept current,
following a `ShardSelection`), `write_object_status`, and `StatusBook`
(write a status only when it changed) are there for an app that wants its
own loop, as the autoscaler does; `UiRegistry::object_validate(kind, handler)`
answers the validate call (without a handler, every object passes). An app
not written in Rust calls the three RPCs and serves
`POST /legion/v1/objects/validate` itself; `proto/legion.proto` documents
them.

## Acting for an object

`requires` checks a Repo's author when the Repo is written. Without more,
gitops would then deploy it with its own grant, which has to reach every
namespace any Repo could target: the grant, not the author, would bound
what gitops does. Acting for the object closes that gap. The app names the
object it is working on, and the manager holds the call to what that
object's writer was checked for.

### The header

An app's call names one of its objects in `legion-for-object`, beside its
own credential:

```text
legion-for-object: gitops/Repo/team-a/storefront#812@4
```

The value is `<app>/<Kind>/<namespace>/<name>#<uid>@<generation>`; a
cluster kind's object has an empty namespace
(`autoscaler/NodeGroup//workers#17@3`). The call runs as **`gitops for Repo
team-a/storefront`**:

- its allow rules are the object's **recorded authority**, the concrete
  permissions its `requires` resolved to and its writer was checked for at
  its last apply, each as an exact rule (the namespace, the job name or
  every job, the band, the KV key, host-level or not, as checked);
- **intersected with the app's grant**: a permission the app's grant does
  not cover whole is dropped, so the identity never holds more than the
  app could do, nor more than the writer could;
- plus the app's deny rules. It is not a management identity, carries no
  policies, and expires with the app's credential.

```console
$ legion object get repo storefront -n team-a
...
authority (checked against alice): job:write in team-a on storefront (from spec.jobs); job:write in team-a on storefront-worker (from spec.jobs)
```

So gitops, acting for `team-a/storefront`, may write `team-a/storefront`
and `team-a/storefront-worker` at band `normal`, and nothing else: not
`team-a/billing`, nothing in `team-b`, not a host-level job, not a read of
the job (unless the kind requires one), not KV. Anything else is:

```text
PermissionDenied: permission denied: job:write is outside what gitops for Repo team-a/storefront holds
Hint: an app acting for an object holds only the permissions the object's writer was checked for (its recorded authority, `legion object get`), within the app's own grant; re-applying the object as someone who holds more records more
```

The manager refuses the header, `Unauthenticated` or `PermissionDenied`
naming `legion-for-object`, when the caller is not **the owning app's own
identity** (a management token, a person, another app, an app acting for
a viewer), when **no object has the uid** or it is not the object the
reference names, or when the kind is orphaned. It never falls back to the
app's own identity, and logs each refusal under `acl_audit`. A **deleting**
object may be named, so the app's cleanup can act for it. Streams
(`WatchObjects`, `StreamEvents`, ...) and the app plane (`UpdateObjectStatus`,
`ReleaseObject`, the autoscaler's RPCs) take the app's own credential and
refuse the header. A call may act for an object or for a viewer
(`legion-on-behalf-of`), never both (`InvalidArgument`). With ACLs off the
header is ignored, like every credential, and nothing is stamped.

The **generation is advisory.** The object's current authority applies,
whatever generation the app read: authority changes only when someone
applies the object, so a stale generation can do no more than the current
one. The manager logs a mismatch (at debug level) and acts with the
current authority.

### `via = "objects"`

A manifest permission may say `via = "objects"`:

```nickel
permissions = [
  { resources = ["job"], actions = ["read"], namespaces = ["team-*"] },
  { resources = ["job"], actions = ["write"], namespaces = ["*"], via = "objects" },
  { resources = ["kv"], actions = ["read", "write"], paths = ["legion/apps/gitops/*"] },
],
```

Such a rule is part of the app's grant **only when it acts for one of its
objects**, where it is intersected with that object's authority. The app's
own credential cannot use it at all: gitops above writes no job on its
own, anywhere. The install prompt says so:

```text
gitops 0.2.0 runs its own code (1 artifact) on your managers, as its own unprivileged user,
in legion-system, and asks for:
  job       read          namespaces: team-*
  job       write         namespaces: *; only for its objects (bounded by each object's recorded authority)
  kv        read, write   paths: legion/apps/gitops/*
  (rules marked "only for its objects" hold only while gitops acts for one of its gitops/Repo objects, and only as far as that object's writer was checked for: gitops writes them only where an object's author could)
```

The installer must hold a `via` rule like any other permission, and
`legion acl grants list` shows it with `(via objects)`. An app with no
kinds may not ask for one, and a stored ACL policy may not carry one (no
token ever acts for an object). Acting as a viewer
(`legion-on-behalf-of`) is bounded by the app's plain grant: a `via` rule
plays no part there, so an action that deploys acts for the object
instead.

### In the app

The framework hands each reconcile handler a client that acts for the
object, and the app's own client beside it:

```rust
context.reconcile("Repo", |request| async move {
    let spec: RepoSpec = request.spec_as()?;
    let mut legion = request.client()?;          // legion-for-object: this Repo
    for job in render(&spec).await? {
        legion.run_job(job).await?;              // held to the Repo's authority, stamped
    }
    let mut own = request.app_client()?;         // the app's own grant (its KV state)
    own.put_kv(state_entry(&request)).await?;
    Ok(ObjectOutcome::ready())
})?;
```

Outside the loop, `context.client_for(&object)` makes the same client for
any object the app owns, and `legion::apps::objects::acting_for(&object)`
gives the header's value for an app that calls Legion its own way. Not for
streams.

### Stamps

A job submitted for an object (`RunJob`, `RunJobTemplate`) is stamped with
it: its version's `managedBy` is `{ app, object }`, and its grant, if the
job asks for one, is recorded in the object-scoped principal's name.

```console
$ legion job status storefront -n team-a
...
managed by gitops for Repo team-a/storefront; gitops will likely write back a version you submit
```

`GetJobInfo.managed_by.object` carries the reference
(`gitops/Repo/team-a/storefront#812`), and `legion job status -j` shows it
as `managedBy.object` with its `label`. A person may still submit over
such a job, with no `--force`: the namespace's owner can always take a job
over, as in an incident. The submit replaces the version and its stamp,
and says who will likely write it back:

```text
warning: team-a/storefront was managed by gitops for Repo team-a/storefront; this version replaces it, and gitops will likely write the job back on its next reconcile
```

Other writes made for an object are not stamped; their audit records and
events carry the principal (`gitops for Repo team-a/storefront`). An
object the app writes while acting for another object is `managedBy` the
app.

### Authority is recorded, not revoked

The authority an object carries is what its writer held **when they
applied it**. If the writer later loses a right, or leaves and their token
is deleted, the object keeps it, and the app keeps deploying within it.
This is the Kubernetes and Flux model: a departing engineer does not break
production deploys, and nobody has to find every object a person ever
wrote before revoking their access. The cost is that power outlives the
person who held it, so Legion makes it findable:

```console
$ legion object list repos -A --authority-lapsed
NAME        NAMESPACE  STATUS  AGE  AUTHORITY                                                   AUTHORIZED BY
storefront  team-a     Ready   3d   lapsed: alice no longer holds job:write in team-a on storefront  alice
billing     team-a     Ready   9d   lapsed: bob's token (accessor 0f3a…) was deleted            bob
reports     team-b     Ready   2d   unknown: sso:carol authorized it without an ACL token ...    sso:carol
```

The manager re-resolves each object's authorizer through its token's
accessor, as a request with that token would authenticate now:

- **lapsed**: the token was deleted or has expired, or no longer holds a
  permission the object records;
- **unknown**: the writer had no accessor to re-resolve (a JWT or SSO
  login, or an app's workload identity).

`--authority-lapsed` needs `acl:read` besides read on the objects, since
it reveals what other principals hold now. With ACLs off nothing lapses.
**Re-applying an object re-authorizes it** as whoever applies it (who must
hold what it requires); deleting it ends the app's authority over it once
the object is gone (a deleting object keeps it for its cleanup).
`ListObjects.authority_lapsed`, and `authorityLapsed` on the MCP
`legion_object_list` tool, do the same.

An upgrade that changes a kind's `requires` changes later writes only:
objects keep the authority they were written with until re-applied (the
upgrade prompt counts them).

## A worked example: the autoscaler

The bundled [autoscaler](autoscaling.md) owns two kinds, and is the
model for an app that owns objects:

```console
$ legion object kinds --app autoscaler
KIND                      SCOPE       ACCESS                                            STATE   DESCRIPTION
autoscaler/NodeGroup      cluster     node:write, and operator:write                    active  A pool of workers a target plugin starts and stops: ...
autoscaler/ScalingPolicy  namespaced  scale:write in the object's namespace             active  A scaling policy for a target outside Legion, run through a target plugin.
```

- **`autoscaler/ScalingPolicy`** ([Standalone
  policies](autoscaling.md#standalone-policies)) is namespaced and mapped
  onto `scale`, so it is written by whoever may scale in its namespace,
  namespace-wide: the grant that governs scaling governs scaling policies,
  with no second grant to keep in step. `target.plugin` is `immutable`.
- **`autoscaler/NodeGroup`** ([Node groups](autoscaling.md#node-groups)) is
  cluster-scoped, mapped onto `node`, and `requires` `operator:write`:
  creating one buys machines. Its `cleanup = "15m"` turns a delete into
  "drain, terminate, and forget every member, then release": the app's
  cleanup runs through the same scale-in path a quiet group shrinks by,
  and at the deadline the manager removes the object anyway.
- **Schemas from the app's types.** The manifest leaves both kinds'
  `schema` and `status.schema` to the app's Rust types, filled in when the
  bundled manifest loads, so the manager's check and the app's parser
  cannot drift.
- **Cross-field checks as rules.** Both kinds declare the parser's
  cross-field checks as CEL `rules` (`self.min <= self.max`, unique check
  names, [The policy rules](autoscaling.md#the-policy-rules)), so the
  manager refuses an inverted policy at apply even with the autoscaler
  stopped. What needs the cluster (a strategy's settings, a plugin's
  config) the validate call reports at apply, as advice, and the app
  reports as `Accepted=false` when it evaluates the object.
- **A security control lives in the app, fail-closed.** Which namespaces
  may use a scaling plugin is the app's config (`namespaces` on a plugin).
  The app checks it on every evaluation and holds a policy that names a
  closed plugin `Rejected`; the validate call only mirrors it.
- **Its own loop, not the reconcile loop.** The autoscaler evaluates every
  policy together each interval (one batched read of the cluster's
  inputs, one expander across node groups), so it keeps two
  `ObjectCache`s current with `run_object_watch` and writes status itself
  through a `StatusBook`, which writes only what changed: `Accepted`,
  `Ready`, the resolved target, the last action. The per-tick numbers stay
  in its own status report, never in object status.
- **Lease-based shards.** Its replicas split policies by the shard each
  object carries; the share each replica holds moves as replicas come and
  go, published as a `ShardSelection` the watch follows (a new watch, and
  a fresh snapshot, on every change). Every node group goes to the one
  replica holding the `nodegroups` shard, since the expander must see all
  of them. `Reconciler::shards_from` gives the reconcile loop the same
  hook.
- **State keyed by uid.** Its history, pauses, and action intents live in
  core's scaling tables under `object:<uid>`, so a `NodeGroup workers` and
  a `ScalingPolicy default/workers` never share state, and a deleted and
  recreated object never inherits a pause or a half-done drain. The
  managers sweep the scaling state of deleted objects.
- **Its own grant, not its objects'.** The autoscaler does not [act for
  its objects](#acting-for-an-object). What its objects make it do happens
  mostly outside Legion's ACLs (a target plugin scales an external
  service; a node group's provider starts and stops machines), where an
  object-scoped credential bounds nothing; its Legion calls for them are
  the app plane (`RecordScalingAction`, `ReportScalingStatus`), which take
  its own identity; and draining a node group's members is governed by the
  kind's access (`node`), which a NodeGroup's writer is checked for at
  write but which its authority does not record. Writing a NodeGroup
  already needs `node:write` and `operator:write`.

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
| `legion object list [KIND] [-n NS \| -A] [-l k=v] [--authority-lapsed]` | `ListObjects` |
| `legion object delete <KIND> <NAME> [-n NS] [--expected-version N] [--now] [--force] [--wait [--timeout D]] [--idempotency-key K]` | `DeleteObject` |
| (the owning app) | `WatchObjects`, `UpdateObjectStatus`, `ReleaseObject` |
| (the owning app, acting for an object) | any RPC, with `legion-for-object` |

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
| `spec.min: min must not exceed max` | the spec fails one of the kind's rules (the message is the kind's; every failing rule is listed) | fix the spec; `legion object explain` lists the rules |
| `... (rules[2] could not be evaluated: no such key: max)` | a rule read a field the spec does not have | set the field, or ask the app's author to guard it with `has()` |
| `... ran past its 10 ms budget, so the write is refused` | a rule is too expensive for this spec (usually nested comprehensions over a large list) | make the list shorter, or ask the app's author for a cheaper rule |
| `PermissionDenied ... needs job:write ...` | the kind's `requires` | have someone who holds it write the object |
| `... is managed by the gitops app` | the app wrote it | `--force` to change it anyway |
| `... is being deleted` | its cleanup window is running | wait (`delete --wait`), then apply |
| `ABORTED ... changed since you read it` | `--expected-version` is stale | read it again and re-apply |
| `RESOURCE_EXHAUSTED ... maxObjects` | the kind is full | delete objects, or raise `maxObjects` in the app |
| `... was not checked by the gitops app (...)` (warning) | the validate call failed open | nothing: the write went through; check the app |
| summary `Stale` | the app is disabled or not healthy | `legion app status <app>` |
| summary `Pending` that stays | the app has not reported on this generation | `legion app logs <app>` |
| `... is outside what gitops for Repo team-a/x holds` | the app acted for an object beyond its recorded authority (or the app's grant) | re-apply the object as someone who holds what it needs, or widen the kind's `requires` |
| `legion-for-object: ... is not an app's` / `acts only for its own objects` | the header came from a person, a management token, or another app | only the owning app's own credential names its objects |
| `legion-for-object: ... no object has uid N` | the object was deleted (or recreated with a new uid) | act for the current object; the watch carries its uid |
| `--authority-lapsed` lists an object | its writer lost a right or their token | re-apply it as someone who holds it, or delete it |

## Limitations

- **No per-namespace quotas for objects.** `maxObjects` caps a kind
  cluster-wide; namespace policies do not count objects yet.
- **Rules are per kind, not per field.** A kind's `rules` see the whole
  spec; there are no rules attached to a subschema (Kubernetes'
  `x-kubernetes-validations` on any property), and a rule over every item
  of a list is written with `all`.
- **No message expressions or type checking.** A rule's `message` is fixed
  text (`messageExpression` is refused). Rules are not type-checked against
  the schema: a rule that reads a field the spec lacks fails at apply, so
  try a new kind's rules with `--dry-run`.
- **A rule's cost is bounded by a clock, not estimated.** There is no
  static cost estimate as Kubernetes has; the budget is wall-clock time,
  so a rule close to its 10 ms can pass on an idle manager and fail on a
  busy one, and work the budget check cannot interrupt (between two
  comprehension iterations) runs to its end on the manager's thread after
  the write is refused. The source, depth, and nesting limits keep that
  work small.
- **The validate call is advice**, and a dry run does not prove the app
  would accept the object later.
- **No resume for the watch.** A reconnect relists the app's objects; that
  is comfortable to about 50,000 objects per app.
- **One schema version per kind.** There is no conversion between schema
  versions; an upgrade that tightens a schema leaves non-conforming objects
  `Invalid` until rewritten.
- **Acting for an object is the app's choice.** An app bounded by its
  objects' authority is one that uses `legion-for-object` (the framework's
  default client in a reconcile handler) and keeps its own grant small,
  with `via = "objects"` on what only its objects should make it do. Its
  plain grant is still everything its own credential can do.
- **Authority is not revoked** when its writer loses rights;
  `--authority-lapsed` finds such objects, and a re-apply re-authorizes.
  An authorizer without a token accessor (JWT, SSO, workload identity)
  reads `unknown`.
- **The intersection is per permission.** A recorded permission the app's
  grant covers only in part (the grant names some jobs, the authority every
  job in a namespace) is dropped, not narrowed.
- **Only job submits are stamped** with the object; other writes made for
  an object name it only in their actor.
- **`writeOnly` values are stored like any spec**, in the replicated state
  store, and readable by the owning app and anyone with `state:read`.
  Keep real credentials in KV (encrypted at rest) and name them by path
  with a `requires` on `kv:read`.
- **The status lane may lag the spec**: on a follower, a status can
  briefly describe an older generation; `observedGeneration` says which.
- **No web UI for objects yet**; the CLI, API, and MCP cover them.
- **The framework is Rust only.** Other apps use the RPCs directly.
