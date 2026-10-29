# App-owned object kinds: Legion's CRDs

> **Judgement calls:** [app-object-kinds-plan_calls.md](../calls/app-object-kinds-plan_calls.md)

**Status: IN EXECUTION** (planned 2026-10-29, on top of PR #60). **Phase O1**
(the generic system) is being built with this plan. **Phase O2** (the
autoscaler's standalone scaling policies and node groups become object kinds
the autoscaler owns) is planned here and not started. Deliberately deferred:
schema versions and conversion between them, owner references, garbage
collection and finalizers, a generic object browser in the SPA, objects that
block a namespace's deletion, and event-driven delivery of `WatchObjects`
(it polls, as `WatchAppExtensions` does).

This plan resolves the "app-defined object kinds" deferral in
[legion-apps-plan.md](legion-apps-plan.md) (§4 non-goals, §11 open
questions). That plan said to wait for a second example; there are two now:
the GitOps app needs a `Repo`, and the autoscaler's standalone policies are
already an app's objects living in core.

---

## 1. Problem statement

An app is a program that manages the cluster ([apps.md](../../docs/apps.md)).
Most apps need **their own objects**: things a person declares and the app
acts on, which are not a property of any one job.

- GitOps needs a **`Repo`**: a URL, a path, a branch, a target namespace.
  It is not a job (it *produces* jobs), so a jobspec extension key cannot
  hold it.
- The autoscaler needs **standalone scaling policies** and **node groups**.
  They exist today only because they were built into core: a
  `scaling_policy` table, three RPCs (`ApplyScalingPolicy`,
  `DeleteScalingPolicy`, `ListScalingPolicies`), `legion scaling
  apply|list|delete`, two MCP tools, ACL code that special-cases node
  groups, and a policy parser the manager links in
  (`src/manager/server/scaling_policy_rpcs.rs`,
  `src/storage/store/scaling_policy_ops.rs`, `src/autoscaler/policy.rs`).
  Every one of those lines is something a third-party app cannot have.
- A cert manager needs a `Certificate`; an ingress app needs a `Route`; a
  backup app needs a `Schedule` that names several volumes.

What an app author has to do instead today:

1. **Put the objects in KV** under `legion/apps/<app>/…`. Nothing validates
   them, the ACL is the KV path (so "team A may edit its own repos" means
   one KV path per team), the CLI is `legion kv put` of raw JSON, and
   nothing tells the app what changed except polling.
2. **Put them in the app's config** (`legion app config set`). One blob,
   `app:write` to change any of it, and every edit rolls the app's job.
3. **Get the object built into core**, as the autoscaler did.

None of these give a person what Kubernetes gives a CRD author for free: a
typed object, validated on write, scoped by namespace, governed by the same
ACLs as everything else, with a CLI, an API, an audit trail, and a watch the
controller reacts to.

## 2. Prior art

- **Kubernetes CustomResourceDefinitions.** A CRD declares a group, a kind,
  plural and short names, a scope (`Namespaced` or `Cluster`), versions with
  an OpenAPI v3 schema each, a `status` subresource, and
  `additionalPrinterColumns`. The API server validates on write, keeps
  `metadata.generation` (bumped on spec changes only) and
  `resourceVersion` (bumped on every write, the CAS token), and serves list
  and watch. Controllers write `status` through the status subresource with
  `observedGeneration`. RBAC addresses `resources` in an API group.
  *Take:* the object shape (spec as written, status from the controller),
  scope, generation versus version, the status subresource, printer
  columns, the controller watch. *Reject:* API groups and the
  plural/singular/shortNames triple (one name, matched case-insensitively,
  plural optional, is enough), schema versions and conversion webhooks
  (deferred: no installed base), and admission webhooks with
  `failurePolicy: Fail` (a broken controller must not block writes; we
  take the fail-open validate call jobspec extensions already use).
- **Kubernetes RBAC for CRDs.** A ClusterRole names the CRD's resource;
  aggregated roles let a CRD author ship "admin/edit/view" roles. Mapping a
  CRD onto an existing permission is impossible: a `HorizontalPodAutoscaler`
  is its own resource, so "may scale" and "may write HPAs" are separate
  grants that drift. *Take:* objects are ACL-governed per kind. *Change:*
  a kind may map onto an existing Legion resource (`scale`, `node`), so a
  `ScalingPolicy` is governed by exactly the `scale:write` that governs
  scaling today.
- **Nomad.** Has no user-defined object kinds. Nomad's autoscaler keeps its
  policies in job specs or files on the autoscaler's disk; Nomad Variables
  (KV with ACLs by path) is what people use instead. *Reject:* the
  KV-by-convention model is what §1 lists as the status quo.
- **Consul config entries.** A closed set of kinds (`service-defaults`,
  `ingress-gateway`), each with a schema, written with `consul config
  write <file>`, with CAS (`-cas -modify-index`). *Take:* one generic
  `write` verb over a file that names its kind, and the CAS index.
- **Crossplane / Terraform providers.** The provider declares the schema,
  core enforces it; the provider reconciles and reports status.
  *Take:* the app declares, the manager enforces, the app reports.
- **Legion itself.** Jobspec extensions
  ([apps.md, "Jobspec extensions"](../../docs/apps.md#jobspec-extensions))
  already have everything an object kind needs except the storage: an
  app-declared JSON Schema (draft 7, local refs only, at most 64 KiB),
  examples that must validate, defaults filled on read, install-time
  collision checks, a qualified spelling that never collides, `requires`,
  the fail-open validate call, `WatchAppExtensions` with shards, and
  pending/orphaned states. The apps plan predicted that this "generalizes
  to stored objects with little new design" (§11). This plan holds it to
  that.

## 3. Jobs to be done

**App author** (writes GitOps, a cert manager, the autoscaler)
- When my app needs settings that are not about one job, I want to declare
  a typed object in my manifest, so people can write them with the same
  tools they use for everything else and I don't build an API.
- When someone writes one, I want to hear about it at once and know which
  version I'm acting on, so I can reconcile and say how it went.
- When I can check something a schema can't ("is this repository
  reachable?"), I want to be asked before it's stored, without becoming a
  single point of failure for every write.

**Job author / team member**
- When I want my team's repository deployed, I want to write a small file
  and apply it, so I don't ask an operator to edit an app's config.
- When I mistype a field, I want to be told at apply, with the path, so I
  don't find out from a silent app.
- When the app has done something with my object, I want to see its status
  next to what I wrote, so I know whether it worked.

**Security / platform admin**
- When I install an app with its own kinds, I want to see who will be able
  to write them, so I don't hand a tenant power by accident.
- When I already decided who may scale things, I want scaling objects to
  follow that decision, so I don't maintain two grants that drift.
- When a kind can create machines, I want writing it to need the rights
  that creating machines needs.

**Cluster operator**
- When I upgrade or remove an app, I want to know what happens to the
  objects people wrote, so an upgrade doesn't silently break them.

**Autonomous agent**
- When I'm asked to set something up through an app, I want to discover
  the kinds and their schemas, dry-run a change and see the diff, and
  apply it idempotently, so I can self-correct before writing.

*Not in scope:* the on-call responder. Objects are configuration; incident
work reads their status, which `legion object get` and the app's own views
already show.

### User scenarios

1. **GitOps.** The platform team installs GitOps; the prompt says `Repo`
   objects are namespaced and written with `object:write` on `gitops/Repo`
   plus `job:write`. A team lead holding both in `team-a` writes
   `repo.ncl` and runs `legion object apply -f repo.ncl`. The manager checks
   it against the schema, asks GitOps (which answers within two seconds, or
   not at all and the write goes through with a warning), stores it at
   generation 1, and emits `object.applied`. GitOps sees it on its watch,
   syncs, and writes `status = { commit = "3f9a1c2", synced = true }` with
   `observedGeneration = 1`. `legion object list repos -n team-a` shows the
   URL and the commit.
2. **The autoscaler, after O2.** `legion object apply -f render-farm.ncl`
   creates a `ScalingPolicy` in `media`. It needs `scale:write` there,
   exactly as `legion scaling apply` did. A `NodeGroup` needs `node:write`
   and `operator:write`. The autoscaler watches both kinds and writes their
   status; `legion scaling apply` is gone.
3. **An upgrade that tightens a schema.** GitOps 0.3.0 makes `branch`
   required. `legion app upgrade` reports `2 Repo objects fail this
   version's schema (flagged invalid)` and lists them. They stay stored and
   read `invalid`; GitOps sees the flag on its watch; their next apply must
   conform.

## 4. Goals and non-goals

**Goals**

- An app declares object kinds in its manifest; installing it registers
  them, with collision and reserved-name checks.
- One generic API, CLI (`legion object …`), and MCP surface for every kind:
  apply (with dry-run diff, CAS, idempotency), get, list, delete.
- Objects are validated on write against the kind's schema, with defaults
  filled on read, and optionally by the app (fail-open).
- Every read and write is ACL-checked: a kind maps to the generic `object`
  resource or to an existing resource of the same scope, plus extra
  `writeRequires`.
- The owning app watches its objects (snapshot, then deltas, shardable) and
  writes their status; nobody else writes status, and the app never writes
  spec through that path.
- Upgrades and uninstalls say what happens to existing objects and never
  delete them.
- O2 fits without new generic machinery.

**Non-goals**

- **Schema versions and conversion** (`v1alpha1` → `v1`). An app that
  changes its schema incompatibly flags old objects invalid; their owners
  fix them. Legion has no installed base to convert.
- **Owner references, garbage collection, finalizers.** Deleting an object
  deletes the row; the app learns from its watch and cleans up.
- **A generic object browser in the SPA.** Apps already draw their objects
  in their own views; a generic page is a UI plan of its own.
- **Objects of a kind declared by core.** Core nouns stay core; kinds are
  how apps extend Legion.
- **Cross-object references and validation** (a `Repo` naming a namespace
  that exists). That is the app's validate call's job.
- **Event-driven watch delivery.** `WatchObjects` polls a fingerprint each
  second, like `WatchAppExtensions` (calls 90 in the apps plan).

## 5. Background: what exists today

- **App manifests and records.** `AppManifest`
  (`src/structs/app.rs:178`) keeps `extensions` as JSON so a record
  round-trips; `AppManifest::validate` (`:292`) parses and checks them.
  Records live in the Control-lane `app` table
  (`src/storage/app_store.rs`); `Store::app_fingerprint` changes on every
  app write.
- **Schemas.** `compile_schema` (`src/structs/extensions.rs:409`) compiles
  draft 7 with a resolver that refuses every non-local `$ref`
  (`LocalRefsOnly`, `:376`); `compiled_extension_schema` (`:441`) adds the
  "describes an object" rule and a digest-keyed cache; `fill_defaults`
  (`:472`) fills defaults the validator does not. `CompiledSchema::check`
  fills then validates, returning dotted error paths.
- **Extension lifecycle.** `install_app_rpc`, `upgrade_app_rpc`, and
  `uninstall_app_rpc` (`src/manager/server/app_rpcs.rs:497`, `:658`,
  `:848`) run the extension collision checks, extend the prompt with
  `lifecycle_report` (`extension_rpcs.rs:116`), and refuse uninstall with
  values unless `force`.
- **App plane.** `calling_app` (`extension_rpcs.rs:595`) recognizes an app
  by its `legion` bearer token's claims or by a grant reference
  (`app_of_identity`, `delegation.rs:35`); delegated identities are
  refused. `stream_credential` re-authenticates a stream every 30 seconds.
  `watch_app_extensions_rpc` (`:624`) polls a fingerprint each second and
  diffs.
- **The validate call.** `validate_call` (`extension_rpcs.rs:510`) signs a
  viewer assertion (`AssertionKind::Validate`, inputs digest of the body),
  forwards `POST` to one allocation of the app through `forward_to_app`,
  and fails open on anything but a `200` within `VALIDATE_TIMEOUT` (2 s).
  The app framework answers it in `UiServer::handle_at`
  (`src/apps/ui.rs:759`).
- **ACL.** `AclResource` (`src/structs/acl.rs:27`) is a closed list;
  `namespaced()` decides whether a rule must carry `namespaces`. Selectors
  are `Option<Vec<String>>` on `AclRule` matched against `AclScope`
  attributes; `reserved_glob_match` keeps `*` out of `legion-*`.
  `AclScope::unowned()` (namespace `*`) is matched only by rules reaching
  every namespace. `app` added no new selector: it reuses `names` for app
  names (`AclScope::app`). Grant coverage (`src/structs/grant.rs:336`) and
  the install prompt's rule wording (`src/structs/app.rs:1463`) enumerate
  selectors.
- **Standalone scaling policies** (O2's input). One `scaling_policy` row
  per policy (`namespace` empty for a node group), versions from the
  never-repeating `scaling_counter` (`scaling_policy_ops.rs`), ACL in
  `enforce_policy_write` (`scaling_policy_rpcs.rs:309`): `scale:write` on
  `AclScope::job(id)`, or `operator:write` plus `node:write` for a node
  group. `WatchScalingPolicies` streams them with task policies; the
  autoscaler shards them by FNV-1a of `policy:<id>`, every node group in
  the one `nodegroups` shard (`src/autoscaler/shard.rs`).
- **Raft.** Every table has one writing lane (`TABLE_OWNERS`,
  `src/storage/raft/ownership.rs:139`); a coverage test fails on an
  unclassified table.
- **Events.** `EventAudience::sees` (`src/manager/server.rs:1263`) shows a
  tenant the events of jobs it may read and `namespace.*` events of
  namespaces it reaches; everything else is operator-only.

## 6. Proposed design

### 6.1 Declaring a kind

A manifest gains `kinds`, a list:

```nickel
kinds = [
  {
    name = "Repo",                       # PascalCase, singular; unique cluster-wide
    description = "A git repository gitops deploys jobs from.",
    scope = "namespaced",                # or "cluster"; default "namespaced"
    schema = {                           # the spec's JSON Schema (draft 7)
      type = "object",
      additionalProperties = false,
      required = ["url"],
      properties = {
        url    = { type = "string" },
        path   = { type = "string", default = "." },
        branch = { type = "string", default = "main" },
      },
    },
    status = { schema = { type = "object", properties = { commit = { type = "string" } } } },
    examples = [{ url = "https://github.com/acme/deploy", path = "jobs" }],
    validate = true,                     # ask the app on every write (fails open)
    access = { resource = "object" },    # the default
    writeRequires = [{ resource = "job", action = "write" }],
    columns = ["/spec/url", { name = "COMMIT", pointer = "/status/commit" }],
  },
],
```

| Field | Rule |
| --- | --- |
| `name` | `^[A-Z][A-Za-z0-9]*$`, at most 63. Singular by convention. |
| `scope` | `namespaced` (default) or `cluster`. A cluster object has no namespace. |
| `schema` | Draft 7, describes an object, at most 64 KiB, local `$ref`s only: the extension rules, by the same code. Defaults are filled on read. |
| `status.schema` | Optional; the same rules. Checked on every status write. Without one, status is any object. |
| `examples` | Each must pass `schema` (with defaults filled). |
| `validate` | Ask the app on every apply (§6.6). |
| `access.resource` | The ACL resource that governs the kind's objects (§6.4). Default `object`. |
| `writeRequires` | Extra `{ resource, action }` permissions every writer (apply and delete) must also hold on the object's scope. |
| `columns` | At most 8 JSON pointers into `{ name, namespace, spec, status }` for `legion object list`. A string's header is its last segment upper-cased; a record names it. |

### 6.2 Names and collisions

- Kind names are **unique cluster-wide, case-insensitively**. Install (and
  upgrade, for a kind it adds) refuses a kind another installed app
  declares, or any bundled manifest in this binary declares, naming the
  owner:

  ```text
  Error: gitops declares kind Repo, which the reposync app already owns
  Hint: the qualified spelling (reposync/Repo) always names that app's kind; rename this app's kind to install it
  ```

- A **reserved core noun** is refused, singular or plural, in any case:
  every top-level CLI noun and ACL resource and the core object nouns
  (`job`, `task`, `group`, `node`, `pool`, `namespace`, `template`, `app`,
  `policy`, `token`, `allocation`, `instance`, `volume`, `snapshot`,
  `service`, `eval`, `rollout`, `kv`, `secret`, `acl`, `user`, `grant`,
  `event`, `action`, `exec`, `session`, `sandbox`, `artifact`, `plugin`,
  `object`, `kind`, …; the full list is `RESERVED_KIND_NOUNS` in
  `src/structs/app_object.rs`, and calls entry 9 records how it was
  picked).
- **The qualified spelling `<app>/<Kind>` always works**, and is the only
  way to name an orphaned kind's objects once another app has taken the
  short name.
- **Matching is forgiving**: the CLI, MCP, and API take the kind in any
  case and optionally plural (`Repo`, `repo`, `repos`, `gitops/repos`;
  `Policy` takes `policies`).

### 6.3 Objects

```nickel
# repo.ncl  —  legion object apply -f repo.ncl
{
  kind = "Repo",
  name = "storefront",              # a DNS label: lowercase, digits, '-'; at most 63
  namespace = "team-a",             # namespaced kinds; default -n, else `default`
  labels = { team = "a" },          # optional, string to string
  spec = { url = "https://github.com/acme/deploy", path = "jobs/storefront" },
}
```

A file may hold one object or a list. `status` is never written by a
person: a file that sets it is refused.

The manager stores, per object (one `app_object` row, keyed by app, kind,
namespace, name):

- `spec` **as written**; the effective spec (schema defaults filled from
  the kind's *current* schema) is computed on read, so an upgrade that adds
  a default reaches every object that did not set it;
- `status` (JSON the app wrote, or none) and `observedGeneration`;
- `generation`: starts at 1 and bumps when the spec changes (not on label
  or status writes);
- `version`: the CAS token, a value of a never-repeating counter, new on
  every write of the row (spec, labels, or status);
- labels; who created and last updated it, and when; when status was last
  written; `validatedBy = <app>@<manifest digest>`.

Its **state** is derived on read:

| State | When |
| --- | --- |
| `valid` | its app is installed, declares the kind, and the spec passes the current schema |
| `invalid` | the spec fails the current schema (an upgrade tightened it): readable, deletable, and the next apply must conform |
| `orphaned` | its app was uninstalled, or no longer declares the kind (both need `--force`): readable and deletable, not writable |

### 6.4 ACL

Every read and write is checked against the kind's **access resource** on
the object's **scope**, then (writes only) each `writeRequires` on the same
scope:

| | Namespaced kind | Cluster kind |
| --- | --- | --- |
| scope | the object's namespace and name | the object's name (and namespace `*` for the `object` resource) |
| `object` (default) | a rule with `namespaces` and optionally `names` and `kinds` | a rule with `namespaces = ["*"]` |
| mapped resource | a namespaced one (`job`, `scale`, `snapshot`, …), as a job named like the object | a cluster-scoped one (`node`, `operator`, `net`, …), unscoped but for `names` |

The new **`object`** resource is namespaced (rules carry `namespaces`) and
has a new **`kinds`** selector, globs over `<app>/<Kind>`:

```nickel
{ resources = ["object"], actions = ["read", "write"], namespaces = ["team-a"], kinds = ["gitops/Repo"] },
{ resources = ["object"], actions = ["read"], namespaces = ["*"], kinds = ["gitops/*"] },
```

A cluster kind's objects carry the namespace `*` on an `object` scope, which
only `namespaces = ["*"]` reaches: the same rule unowned volumes follow. The
reserved-prefix rule applies unchanged through `namespaces`: an object in
`legion-system` is reached only by a `legion-*` pattern.

**Mapping a kind onto an existing resource** (`access = { resource =
"scale" }`) means whoever holds that resource's action on the object's
scope manages the objects: `scale:write` in `media` writes `ScalingPolicy`
objects in `media`, exactly as it writes `legion scaling apply` policies
today. The scope must fit: a namespaced kind maps only to a namespaced
resource, a cluster kind only to a cluster-scoped one; install refuses
anything else. The install prompt shows the mapping, because it is a
decision about who may feed the app instructions:

```text
gitops 0.2.0 runs its own code (1 artifact) on your managers, ...
Kinds:
  Repo  namespaced; written with object:write on gitops/Repo, plus job:write; checked by the app
```

**Check before fetch.** The kind is resolved from installed manifests
(public metadata), the ACL is enforced on the requested scope, and only
then is the object read, so a caller without access cannot tell a missing
object from a forbidden one. List endpoints gate on "may read this kind
anywhere" and filter items.

**Orphaned objects** are governed by the `object` resource (their kind,
and so its mapping, is gone; calls entry 5).

### 6.5 The API

| RPC | ACL | What |
| --- | --- | --- |
| `ListObjectKinds` | per kind: read on its access resource anywhere | installed kinds, their schemas, scope, access, columns, owner |
| `ApplyObject` | write on access + `writeRequires`, on the object's scope | create or update; `dry_run` returns a field diff; `expected_version` CAS (`0` = create only); `idempotency_key`; `warnings` |
| `GetObject` | read on access | one object, with its effective spec, status, and state |
| `ListObjects` | read on access, per item | by kind (or every kind), namespaces (`-n`/`-A`), labels |
| `DeleteObject` | write on access + `writeRequires` | CAS and idempotency |
| `WatchObjects` | app-plane: the owning app | snapshot, then upserts and deletes, filtered to the app's kinds, optionally sharded |
| `UpdateObjectStatus` | app-plane: the owning app | writes `status` and `observedGeneration`, CAS on `version` or `generation`; never touches the spec |

`ApplyObject` takes the object as JSON (`object_json`, the file format) and
a default `namespace` used when the object names none. For a cluster kind
that default is ignored with a warning, while a `namespace` written in the
object itself is an error, so an inherited `LEGION_NAMESPACE` never breaks
a cluster apply and a wrong file never silently loses its namespace.

```console
$ legion object apply -f repo.ncl --dry-run
Repo team-a/storefront: would change (generation 3 → 4; dry run, nothing written)
  spec.branch: "main" → "release"
$ legion object apply -f repo.ncl
Repo team-a/storefront: updated (generation 4, version 118)
$ legion object apply -f repo.ncl --expected-version 117
Error: Repo team-a/storefront is at version 118, not 117: it changed since you read it
Hint: read it again (legion object get repo storefront -n team-a) and apply on top of it
```

### 6.6 The validate call

A kind with `validate = true` is checked by its app on every apply that
changes the spec, after the schema passes: `POST /legion/v1/objects/validate`
to one allocation of the app, through the same worker-plane forward and
viewer assertion as the extension validate call (kind `validate`, name
`object`, the writer as the viewer, the body's digest as its inputs):

```json
{ "app": "gitops", "kind": "Repo", "namespace": "team-a", "name": "storefront",
  "operation": "update",
  "spec": { "url": "https://github.com/acme/deploy" },
  "effective": { "url": "https://github.com/acme/deploy", "path": ".", "branch": "main" },
  "previous": { "url": "https://github.com/acme/old" } }
```

It answers `{ "errors": [...], "warnings": [...] }`. Errors refuse the
apply; warnings come back in the reply. Two seconds, and **fail open**: an
app that is disabled, unreachable, slow, or answers anything but `200`
passes the write with a warning. A dry run asks too (it is the point of a
dry run). The app framework answers it with
`UiRegistry::validate_objects(handler)`; without a handler every object
passes.

### 6.7 Watching and status

`WatchObjects` is the app's only feed. The first message has `snapshot`
set and carries every object of the app's kinds (optionally only `kinds`);
later messages carry upserts (any change of version, or of the derived
state after an upgrade) and deletes; every message has a stream
`generation`, and every object its own `generation` and `version`.
`invalid` objects are included and flagged; orphaned ones are not (they are
no longer the app's). It polls a fingerprint of the apps and the table
about once a second, like `WatchAppExtensions`.

**Shards.** `shard_count` and `shards` filter by FNV-1a 64 of the object's
**shard key**, `<app>/<Kind>:<namespace>/<name>` (`<app>/<Kind>:<name>` for
a cluster kind), modulo the count: the hash and the 64-shard count the
autoscaler's Phase 7 leases already use (`src/autoscaler/shard.rs`), so an
app replica holding shards `{3, 17}` of 64 asks for exactly those. Each
object carries its `shard`. A kind whose objects must be evaluated together
(node groups) is watched unsharded by whichever replica holds the shard of
a fixed key, in a second watch filtered to that kind.

`UpdateObjectStatus` writes `status` (checked against `status.schema`) and
`observedGeneration`, with `expected_version` or `expected_generation` as
CAS (the second is the usual one: "this status is for the spec I saw"). It
bumps `version`, not `generation`, emits `object.status_changed` when the
status changed, and is refused for an orphaned or delegated caller or an
app that does not own the kind.

On the app framework (`src/apps/objects.rs`): `ObjectCache` applies watch
messages; `AppContext::watch_objects(kinds, shards)` keeps one current
(reconnecting with backoff, restartable when the shards change);
`AppContext::write_object_status(...)` writes status.

### 6.8 Lifecycle

- **Install** registers the kinds after the checks above. Objects left
  orphaned by an earlier install of the same app are adopted again, and
  the prompt says how many and how they fare against the new schemas.
- **Upgrade** may add kinds, change schemas, access, `writeRequires`, and
  columns. Every existing object of the app's kinds is checked against the
  new schemas and reported in the prompt (`2 fail this version's schema
  (flagged invalid)`). Removing a kind that still has objects needs
  `--force`, which leaves them orphaned. Changing a kind's `scope` while it
  has objects is refused (an object cannot change scope; there is no
  conversion).
- **Disable** changes nothing about objects: they stay writable, the
  validate call fails open, and nothing acts on them.
- **Uninstall** with objects needs `--force`; they stay, orphaned.
- **Deleting a namespace** leaves its objects (deferred: calls entry 13).

### 6.9 Events

`object.applied` (created or updated: who, kind, generation, version),
`object.deleted`, and `object.status_changed` (by the app, with
`observedGeneration`). A namespaced object's events carry its namespace in
metadata and reach whoever reaches that namespace, as `namespace.*` events
do; a cluster object's reach operators only.

### 6.10 CLI and MCP

```console
$ legion object kinds
KIND               APP         SCOPE       ACCESS                  DESCRIPTION
Repo               gitops      namespaced  object (+ job:write)    A git repository gitops deploys jobs from.
$ legion object apply -f repo.ncl [--dry-run] [-n NS] [--expected-version N] [--idempotency-key K]
$ legion object get repo storefront -n team-a
$ legion object list repos -n team-a          # or -A; -l team=a; no KIND lists every kind
NAME        URL                              COMMIT    STATE  GEN
storefront  https://github.com/acme/deploy   3f9a1c2   valid  4
$ legion object delete repo storefront -n team-a [--expected-version N]
```

Every command takes `-j` and emits the standard envelope. MCP:
`legion_object_kinds`, `legion_object_list`, `legion_object_get` (read);
`legion_object_apply`, `legion_object_delete` (mutating, behind
`--allow-write`). The capability catalog gains `object.kinds`,
`object.apply`, `object.get`, `object.list`, `object.delete`.

## 7. Proto, struct, config, and persistence changes

- **Proto.** Seven Manager RPCs (§6.5) and their messages: `ObjectKind`,
  `AppObject`, `ObjectRef`, `ObjectEvent`, and request/reply pairs.
  `UpgradeAppRequest.force` (remove a kind that has objects);
  `UninstallAppReply.orphaned_objects`; `App.kinds`. Every field commented.
- **Manifest.** `AppManifest.kinds` (JSON, parsed to `KindDecl` in
  `src/structs/app_object.rs`, like `extensions`).
- **ACL.** `AclResource::Object` (`object`, namespaced); `AclRule.kinds`;
  `AclScope.kind`; grant coverage and rule wording learn the selector.
- **Persistence.** One migration: table `app_object (app, kind, namespace,
  name, spec, status, observed_generation, generation, version, labels,
  created_by, created_ms, updated_by, updated_ms, status_updated_by,
  status_updated_ms, validated_by)`, primary key (app, kind, namespace,
  name), Control lane. Versions come from the `app_object` counter in
  `scaling_counter`. No job-config field changes, so `maximal.ncl` is
  untouched.
- **Examples.** `configs/apps/example-app.ncl` declares `Repo`;
  `configs/apps/example-repo.ncl` is an object file.

## 8. Implementation phases

**O1: the generic system** (this change; independently valuable: GitOps
and every third-party app can have objects).

1. Manifest `kinds`, validation, collisions, the reserved list, kind
   resolution, the registry (cached on the apps' fingerprint).
2. The `app_object` table, its store ops, raft ownership.
3. The `object` ACL resource and `kinds` selector.
4. The seven RPCs, the validate call, events, and the lifecycle in
   install/upgrade/uninstall.
5. `legion object …`, MCP tools, capability entries.
6. The app framework helpers (watch, status, validate handler).
7. Docs (`docs/app-objects.md`, `apps.md`, `auth-and-acls.md`,
   `agent-readiness.md`), the example kind and object, TODO.

**O2: the autoscaler's policies become its objects** (a later change;
valuable alone: it deletes a core subsystem and proves the generic one).

- The bundled manifest declares `ScalingPolicy` (namespaced, `access =
  scale`) and `NodeGroup` (cluster, `access = node`, `writeRequires =
  [operator:write]`), their schemas generated from the existing
  `StandaloneSpec` structs (`schemars`), `validate = true`.
- Removed, with no aliases (AGENTS.md): the `scaling_policy` table (a
  migration drops it; existing rows are not converted, there being no
  installed base), `ApplyScalingPolicy`, `DeleteScalingPolicy`,
  `ListScalingPolicies`, `legion scaling apply|list|delete`, the
  `legion_scaling_apply` MCP tool, and the manager-side node-group ACL
  special case. `legion object apply|list|delete` replace them.
- `WatchScalingPolicies` serves task policies only; the autoscaler reads
  standalone policies and node groups through `WatchObjects` (external
  policies sharded; node groups in one unsharded watch, opened by the
  replica holding the `nodegroups` shard) and writes their status
  (resolved policy, last evaluation, errors) with `UpdateObjectStatus`.
- Plugin allowlist and schema checks move to the autoscaler's validate
  call (fail-open at write), and stay enforced at evaluation, where an
  invalid policy is reported in its status.
- `configs/scaling/*.ncl` take the object file format (`kind`, `name`,
  `namespace`, `spec`).
- **Scaling defaults stay core**: the manager resolves `autoscale` at
  submit and in `job plan` against cluster and namespace defaults, so they
  cannot live behind an app (calls entry 14).
- `legion scaling explain|history|pause --standalone` keep working by
  policy id; `scaling_action`, pauses, and history keep keying standalone
  policies by id (`<namespace>/<name>`), which an object's id preserves.

O2's risks: the policy ids that key history, pauses, and action intents
must stay byte-identical (`media/render`, bare `workers` for a node
group); the shard key changes from `policy:<id>` to the object shard key,
moving every external policy to another shard once (its windows restart);
`legion scaling list` loses standalone policies to `legion object list`,
so the UI's autoscaling page and the `scaling status` table must read
objects too; and the validate call is fail-open where today's apply-time
plugin check is not (an invalid plugin name now surfaces in status, not at
apply, when the autoscaler is down).

## 9. Testing plan

- **Unit** (`src/structs/app_object.rs`, `acl.rs`): kind declaration rules
  (name shape, reserved nouns singular and plural, schema rules shared with
  extensions, examples, scope/access fit, `writeRequires`, columns); kind
  resolution (case, plural, qualified, did-you-mean); object parsing
  (unknown fields, `status` refused, names, labels, namespace rules); diff
  lines; shard keys and stability; the `kinds` selector and the `object`
  resource's namespace rules (cluster objects need `*`).
- **Store** (`app_object_ops.rs`): create, unchanged apply writes nothing,
  generation versus version, CAS (create-only, mismatch), status CAS on
  version and generation, delete with CAS, fingerprint, counter never
  repeats across delete and recreate.
- **Integration** (`tests/integration/app_objects.rs`): install the
  example app, apply/get/list/delete through the service with ACLs on
  (object-resource scoping by namespace and kind; a kind mapped to `scale`;
  `writeRequires`; check-before-fetch: a forbidden caller gets
  PermissionDenied for a missing object too); dry-run diff; CAS;
  idempotency; the validate call (refusal, warning, fail-open with no app);
  `WatchObjects` snapshot, upsert, delete, shards, refused to another app;
  `UpdateObjectStatus` CAS and ownership; upgrade report and `invalid`;
  remove-kind and uninstall needing `--force`, then `orphaned` reads.
- **ACL coverage**: seven rows in `tests/integration/acl_endpoints.rs`
  with a fixture kind; `every_manager_rpc_is_classified`.
- **Raft**: `raft_seam::` and the ownership coverage test with the new
  table.
- **CLI/MCP**: every command takes `--json`; the MCP read tools are listed
  without `--allow-write`, the mutating ones only with it; catalog
  consistency.

## 10. Rollout and backward compatibility

Additive in O1: a new table, new RPCs, a new manifest field, a new ACL
resource and selector. Existing manifests (no `kinds`) are unaffected.
Policies written before have no `object` rules, so nobody reaches objects of
`object`-access kinds until an admin grants it (default-deny); management
tokens do. A mixed-version cluster: an older manager cannot apply the
`app_object` changeset, which the raft schema-version guard already refuses
to propose to it (docs/upgrades.md), so upgrade managers before installing
apps with kinds.

O2 is a breaking change by design (AGENTS.md): `legion scaling apply` and
its RPCs disappear in the same release that teaches the autoscaler
`WatchObjects`; the PR says so.

## 11. Open questions

- **Should the validate call run on delete?** Lean: no. A broken app must
  never pin an object in place; the app learns of the delete on its watch.
- **Should objects block a namespace's deletion?** Lean: yes, with
  `--cascade` deleting them, once a second kind exists in tenant
  namespaces. Deferred (calls entry 13).
- **Event-driven watch delivery.** Lean: do it for both watches at once
  when the poll cost shows up.

## 12. Acceptance criteria

- **O1.**
  - Installing the example app shows `Repo` and its access in the prompt;
    `legion object kinds` lists it.
  - A token with `object:write` on `gitops/Repo` in `team-a` (and
    `job:write` there) applies `configs/apps/example-repo.ncl`; the same
    token is denied in `team-b`, and gets PermissionDenied (not NotFound)
    for `legion object get repo missing -n team-b`.
  - `--dry-run` prints the field diff; `--expected-version` refuses a stale
    write; a repeated `--idempotency-key` is deduplicated.
  - A spec failing the schema is refused with its path; a schema default
    shows in `legion object get` without being stored.
  - The owning app's `WatchObjects` gets a snapshot, then the apply; another
    app is refused; `UpdateObjectStatus` shows in `get` and emits
    `object.status_changed`.
  - Upgrading to a manifest that tightens the schema reports the failing
    objects and they read `invalid`; removing the kind or uninstalling
    needs `--force` and leaves them `orphaned`, readable and deletable.
- **O2.** `legion object apply -f configs/scaling/render-farm.ncl` creates
  a `ScalingPolicy` with `scale:write` and nothing else; a `NodeGroup`
  needs `node:write` and `operator:write`; the autoscaler evaluates both
  and their status shows its verdict; `legion scaling apply` no longer
  exists.
