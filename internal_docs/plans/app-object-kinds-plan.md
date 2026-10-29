# App-owned object kinds: Legion's CRDs

> **Judgement calls:** [app-object-kinds-plan_calls.md](../calls/app-object-kinds-plan_calls.md)

**Status: IN EXECUTION** (planned 2026-10-29, on top of PR #60). **Phase O1**
(the generic system) is built (2026-10-29, branch `claude/app-objects`;
public page [docs/app-objects.md](../../docs/app-objects.md)); what the
build decided beyond this plan is calls entries 28-31 and the note under
entry 11 (the `Accepted` summary). **Phases O2-O4** are
planned here and not started: O2 moves the autoscaler's standalone scaling
policies and node groups onto kinds it owns, O3 lets an app act for an
object with the authority its writer recorded, O4 adds CEL `rules`.
Deliberately deferred: schema versions and conversion, owner references and
core garbage collection (bounded `cleanup` replaces finalizers), a generic
object browser in the SPA, `since_version` watch resume, per-namespace
object quotas, and `--allow-pending` for objects of a kind not installed
yet.

**Revised after adversarial review (2026-10-29).** Three reviews of this
plan and of the apps substrate changed O1 before it shipped; calls entries
1-31 are the decisions as they stand (28-31 from the build), and the superseded originals are kept
at the end of the calls doc. What changed:
- a guiding principle (§1): an object can never make its app do more than
  its writer could, and the write path never depends on the app being up;
- **kind identity** is `<app>/<Kind>`; files and the API require it, short
  names are a CLI convenience, and two apps may share one (§6.2);
- **kind definitions persist** in an `object_kind` table, so orphaned
  objects keep their schema and access mapping (§6.2, §6.9);
- objects get a never-reused **`uid`**, and **defaults are persisted at
  write** (§6.3);
- **status** moved to its own Status-lane table, `app_object_status`, with
  conditions, a generic summary, and `--wait`; the CAS token is
  `spec_version`, which status never moves (§6.7);
- `writeRequires` became **`requires` with spec pointers**, enforced as the
  writer, fail-closed, in core, and recorded on the object as `authority`
  (§6.4);
- the **access mapping** is pinned to `object`, `job`, `scale`, `node`,
  checked namespace-wide (§6.4);
- **schemas are closed by default**, with `immutable` and `writeOnly`
  fields; CEL `rules` are reserved for O4 (§6.1);
- objects written by an app are **`managedBy`** it (§6.5);
- **events are filtered by the kind's ACL** (§6.10);
- **limits**, including `maxObjects` per kind (§6.3);
- objects are **namespace contents**, and deletion gets **bounded
  cleanup** instead of finalizers (§6.9);
- the watch **chunks** its snapshot, carries uids and tombstones, and uses
  a per-app fingerprint (§6.8);
- the app framework gets a **reconcile loop** (§6.8), and the CLI
  `legion object explain` (§6.11).

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

**The guiding principle.** An object can never make its app do more than
its writer could do directly, and the write path never depends on the app
being up. Kubernetes broke both: custom resources became
privilege-escalation vectors for cluster-admin operators (an Argo CD
`Application` or a Flux `Kustomization` deploys whatever its author names,
with the controller's rights), and admission webhooks put controllers in the
write path. Most of §6 follows from these two rules: the requirements an
object's contents imply are enforced in core, as the writer, fail-closed
(§6.4); the app's own validation is advisory (§6.6); and nothing an app
does or fails to do can pin an object in place (§6.9).

## 2. Prior art

- **Kubernetes CustomResourceDefinitions.** A CRD declares a group, a kind,
  plural and short names, a scope (`Namespaced` or `Cluster`), versions with
  an OpenAPI v3 schema each, a `status` subresource, and
  `additionalPrinterColumns`. The API server validates on write, keeps
  `metadata.generation` (bumped on spec changes only) and
  `resourceVersion` (bumped on every write, the CAS token), and serves list
  and watch. Controllers write `status` through the status subresource with
  `observedGeneration`. RBAC addresses `resources` in an API group.
  *Take:* the object shape (spec, status from the controller), a `uid`,
  scope, generation versus version, defaults persisted at write, a status
  kept apart from the spec, printer columns, `explain`, the controller
  watch. *Reject:* API groups and the
  plural/singular/shortNames triple (one name, matched case-insensitively,
  plural optional, is enough), schema versions and conversion webhooks
  (deferred: no installed base), and admission webhooks with
  `failurePolicy: Fail` (a broken controller must not block writes; we
  take the fail-open validate call jobspec extensions already use).
- **Kubernetes RBAC for CRDs.** A ClusterRole names the CRD's resource.
  A CRD author *can* map a kind onto the built-in roles, through aggregated
  ClusterRoles (`rbac.authorization.k8s.io/aggregate-to-edit`), and
  Kubernetes learned what that costs: every `edit` holder silently gets the
  operator's power, because a custom resource is an instruction to a
  controller that holds far more than `edit`. *Take:* objects are
  ACL-governed per kind, by default through their own resource. *Change:*
  a kind may map onto a pinned few existing resources (`job`, `scale`,
  `node`), checked namespace-wide, so a `ScalingPolicy` is governed by
  exactly the `scale:write` that governs scaling today, and the real bound
  on what an object can make its app do is `requires` (§6.4), not the
  mapping.
- **Escalation through custom resources.** Argo CD `Application`s and Flux
  `Kustomization`s deploy what their author names, with the controller's
  rights; Flux added service-account impersonation and a lockdown flag to
  stop tenants deploying cluster-wide. *Take:* the permissions an object's
  contents imply are checked against its writer at write (§6.4), and
  recorded, so a later phase can bound the app by them (O3).
- **Conditions, `observedGeneration`, kstatus.** Kubernetes converged on
  `Ready`-style conditions and a generic readiness computation so tools can
  wait on any kind. *Take:* a status envelope with standard conditions and
  a manager-derived summary (§6.7).
- **Finalizers and stuck `Terminating` namespaces.** *Reject:* finalizers.
  Take a bounded cleanup window instead (§6.9).
- **Structural schemas.** Kubernetes `v1` CRDs prune unknown fields.
  *Take:* schemas are closed by default (§6.1), as Legion's strict
  jobspecs are.
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
- When someone writes an object that makes an app deploy into a namespace,
  read a secret, or run something on the host, I want that checked against
  *them*, so an app with a broad grant is never a way around their
  permissions.

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

1. **GitOps.** The platform team installs GitOps; the prompt says
   `gitops/Repo` objects are namespaced, written with `object:write` on
   `gitops/Repo`, and that writing one also needs `job:write` in the
   namespace its `spec.targetNamespace` names. A team lead holding both in
   `team-a` writes `repo.ncl` (`kind = "gitops/Repo"`) and runs `legion
   object apply -f repo.ncl --wait`. The manager checks it against the
   schema (an unknown field is an error, with its path), checks the
   requirements as the team lead, asks GitOps (advice only: it answers
   within two seconds or the write goes through with a warning), stores it
   at generation 1 with its defaults filled, and emits `object.applied`.
   GitOps's reconcile loop sees it, syncs, and reports `Ready = true,
   reason Synced`; `--wait` returns. `legion object list repos -n team-a`
   shows `Ready` and the commit.
2. **The autoscaler, after O2.** `legion object apply -f render-farm.ncl`
   creates an `autoscaler/ScalingPolicy` in `media`. It needs `scale:write`
   in `media`, as `legion scaling apply` did. A `NodeGroup` needs
   `node:write` and `operator:write`; deleting one drains and terminates its
   nodes within its `cleanup` window. The autoscaler reconciles both kinds
   and reports their durable verdicts in status; `legion scaling apply` is
   gone.
3. **An upgrade that tightens a schema.** GitOps 0.3.0 makes `branch`
   required and changes `interval`'s default. `legion app upgrade` reports
   `2 Repo objects fail this version's schema (flagged invalid)` and lists
   them, and `Repo.interval default "1m" → "5m": 14 existing Repos keep
   "1m"; re-apply to adopt`. The failing objects stay stored and read
   `Invalid`; their next apply must conform.

## 4. Goals and non-goals

**Goals**

- An app declares object kinds in its manifest; installing it registers
  them. A kind's name is `<app>/<Kind>`.
- One generic API, CLI (`legion object …`), and MCP surface for every kind:
  apply (dry-run diff, CAS, idempotency, `--wait`), get, list, delete,
  explain.
- Objects are validated on write against a closed schema, with
  `immutable` and `writeOnly` fields and defaults persisted at write; the
  app may advise (fail-open).
- Every read and write is ACL-checked, on the kind's `object` resource or a
  pinned existing one; the permissions an object's contents imply
  (`requires`) are enforced on its writer, fail-closed, and recorded.
- The owning app reconciles its objects (a chunked, shardable watch and a
  framework loop) and reports status on the Status lane; the manager turns
  status into a generic summary anyone can wait on.
- Upgrades and uninstalls say what happens to existing objects and never
  delete them; deletion has a bounded cleanup window and nothing can stay
  stuck.
- O2 fits.

**Non-goals**

- **Schema versions and conversion** (`v1alpha1` → `v1`). An app that
  changes its schema incompatibly flags old objects invalid; their owners
  fix them. Legion has no installed base to convert.
- **Owner references, core garbage collection, finalizers.** An app stamps
  what it creates with the object's `uid` and collects it level-triggered;
  the bounded `cleanup` window covers external resources it must release
  while it can still read the spec.
- **CEL rules in O1.** Reserved (`rules`), refused until O4.
- **A generic object browser in the SPA.** Apps draw their objects in their
  own views; a generic page is a UI plan of its own.
- **Objects of a kind declared by core.** Core nouns stay core.
- **Watch resume** (`since_version`). It needs a change log; full relists
  are fine to about 50,000 objects per app.
- **Per-namespace object quotas** (namespace policy) and storing objects of
  a kind that is not installed yet (`--allow-pending`).

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
- **What already lives off the Control lane.** Node liveness moved out of
  `node` into `node_health` on the Status lane (phase C-1), and the
  autoscaler's per-tick status is held in memory on the Status-lane leader,
  never replicated (`src/manager/scaling_status.rs`); both because churn on
  the Control lane slows everything else on it.
- **Spec-aware checks already in core.** `enforce_kv_credential_read`
  (`src/manager/server.rs:14644`) checks `kv:read` on every KV path a
  jobspec's credentials name, because "job submit access would be a way to
  exfiltrate any secret in KV". Extension `requires` checks cluster-scoped
  permissions with `AclScope::none()` (`extension_rpcs.rs:296`).
- **Chunked watches.** `WatchScalingPolicies` sends at most 1,000 policies
  per message (`autoscale_rpcs.rs:2437`); app clients use tonic's default
  4 MiB decode limit (`cmd/helpers.rs:407`).
- **Namespace contents.** A namespace delete counts jobs, volumes, and
  snapshots (`NamespaceContents`, `src/storage/namespace_store.rs`), and
  `--cascade` removes them.
- **Strict jobspecs.** Unknown jobspec keys are errors since A6a
  (`UnknownFieldMode::Reject`, `spec_fields.rs:95`).
- **Events.** `EventAudience::sees` (`src/manager/server.rs:1263`) shows a
  tenant the events of jobs it may read and `namespace.*` events of
  namespaces it reaches; everything else is operator-only.

## 6. Proposed design

### 6.1 Declaring a kind

A manifest gains `kinds`, a list:

```nickel
kinds = [
  {
    name = "Repo",                       # the kind is gitops/Repo; PascalCase, singular
    description = "A git repository gitops deploys jobs from.",
    scope = "namespaced",                # or "cluster"; default "namespaced"
    schema = {                           # the spec's JSON Schema (draft 7); closed by default
      type = "object",
      required = ["url", "targetNamespace"],
      properties = {
        url             = { type = "string" },
        path            = { type = "string", default = "." },
        branch          = { type = "string", default = "main" },
        targetNamespace = { type = "string" },
        token           = { type = "string", writeOnly = true },   # redacted on every read but the app's
      },
    },
    status = { schema = { type = "object", properties = { commit = { type = "string" } } } },
    examples = [{ url = "https://github.com/acme/deploy", targetNamespace = "team-a" }],
    validate = true,                     # also ask the app (advisory, fails open)
    access = { resource = "object" },    # the default; or job, scale, node
    requires = [                         # what writing one needs, from its contents
      { resource = "job", actions = ["write"], namespace = "/spec/targetNamespace" },
    ],
    immutable = ["/spec/url"],           # an update may not change these
    cleanup = "10m",                     # deleting waits up to this for the app to release it
    maxObjects = 1000,                   # default 10,000, at most 100,000
    columns = ["/spec/url", { name = "COMMIT", pointer = "/status/fields/commit" }],
  },
],
```

| Field | Rule |
| --- | --- |
| `name` | `^[A-Z][A-Za-z0-9]*$`, at most 63, not a reserved noun (§6.2). The kind's name is `<app>/<name>`. |
| `scope` | `namespaced` (default) or `cluster`. A cluster object has no namespace. |
| `schema` | Draft 7, describes an object, at most 64 KiB, local `$ref`s only (the extension rules, by the same compiler). **Closed by default**: any schema with `properties` and neither `additionalProperties` nor `patternProperties` is treated as `additionalProperties = false`, so an unknown field is an error naming its path, as an unknown jobspec key is. Set `additionalProperties` to opt a subschema out. `allOf`/`anyOf`/`oneOf` branches are not closed (each would refuse the others' fields). |
| `status.schema` | Optional; the same rules. Checks the status `fields` the app writes (§6.7). |
| `examples` | Each must pass `schema` (with defaults filled). |
| `validate` | Ask the app on every apply that changes the spec (§6.6). Advisory. |
| `access.resource` | `object` (default), `job`, `scale`, or `node` (§6.4). |
| `requires` | What writing (apply or delete) an object needs, resolved from its contents (§6.4). |
| `immutable` | JSON pointers into the object (`/spec/...`) an update may not change; changing one means delete and recreate. |
| `writeOnly: true` | The draft-7 keyword, on any property: the value is a secret, shown as `"<redacted>"` on every read but the owning app's watch. |
| `cleanup` | How long a delete waits for the app to release the object (§6.9): a duration, at most `1h`; default none (deletes at once). |
| `maxObjects` | At most this many objects of the kind, cluster-wide. Default 10,000, at most 100,000. |
| `columns` | At most 8 JSON pointers into `{ name, namespace, labels, spec, status }` for `legion object list`. A string's header is its last segment upper-cased; a record names it. |
| `rules` | Reserved for CEL validation rules (O4); setting it is refused in O1. |

Every pointer in a kind's declaration points into the same document, the
object as `legion object get -j` shows it, so `/spec/url` means one thing
in `requires`, `immutable`, and `columns`.

### 6.2 Identity

- **A kind's name is `<app>/<Kind>`**: `gitops/Repo`. Two apps may both
  declare `Repo`; they are different kinds. Install refuses only a
  duplicate within one app (ignoring case and plurals) or a **reserved core
  noun**, singular or plural, in any case: every top-level CLI noun, every
  ACL resource, and the core object nouns (`job`, `task`, `group`, `node`,
  `pool`, `namespace`, `template`, `app`, `policy`, `token`, `allocation`,
  `instance`, `volume`, `snapshot`, `service`, `eval`, `rollout`, `kv`,
  `secret`, `acl`, `user`, `grant`, `event`, `action`, `exec`, `session`,
  `sandbox`, `artifact`, `plugin`, `object`, `kind`, `spec`, `status`, …;
  `RESERVED_KIND_NOUNS` in `src/structs/app_object.rs`, calls entry 20).
- **Files and the API require the qualified kind.** A file has to mean the
  same thing forever; if it said `kind = "Repo"`, it would break
  cluster-wide the day a second app installed a `Repo`. `ApplyObject`
  refuses an unqualified kind, naming the qualified one when exactly one
  matches:

  ```text
  Error: kind "Repo" must be qualified with its app: kind = "gitops/Repo"
  ```

- **Interactive commands take short names** (`get`, `list`, `delete`,
  `explain`; the API's `GetObject`, `ListObjects`, `DeleteObject`): any
  case, plural optional (`repos`, `Policy` takes `policies`), when exactly
  one kind matches. Otherwise the error lists them:

  ```text
  Error: "repo" names 2 kinds: gitops/Repo, reposync/Repo
  Hint: name one: legion object get gitops/Repo storefront
  ```

- **Kind definitions persist** in an `object_kind` table on the Control
  lane: one row per (app, kind) with the declaration as installed, the
  manifest digest, its state (`active` or `orphaned`), and timestamps.
  Install and upgrade upsert the rows; uninstalling with `--force`, or an
  upgrade that drops a kind with `--force`, marks them `orphaned` and keeps
  the declaration. The manager's kind registry reads this table (cached on
  its fingerprint), so an orphaned kind's objects keep their schema,
  columns, and access mapping. `legion object kinds` lists orphaned kinds,
  marked. A row is derived from the app records by an idempotent sync that
  runs after every app write and on the app reconciler's full pass; an
  orphaned row whose objects are all gone is dropped.

### 6.3 Objects

```nickel
# repo.ncl  —  legion object apply -f repo.ncl
{
  kind = "gitops/Repo",             # qualified: <app>/<Kind>
  name = "storefront",              # a DNS label: lowercase, digits, '-'; at most 63
  namespace = "team-a",             # namespaced kinds; default -n, else `default`
  labels = { team = "a" },          # optional, string to string
  spec = { url = "https://github.com/acme/deploy", targetNamespace = "team-a" },
}
```

A file may hold one object or a list. `status` is never applied by a
person: a file that sets it is refused.

An object (one `app_object` row on the Control lane, keyed by app, kind,
namespace, name) has:

- a **`uid`**, never reused (a value of a counter), assigned at create.
  Everything keyed to "this object" uses it: its status row, the watch, and
  app-side state such as O2's pauses and intents, so a delete and a
  recreate never inherit stale status, pauses, or drains;
- its **spec with defaults persisted at write**: the written spec plus the
  defaults of the schema current at that write. On read, defaults fill only
  fields the stored spec lacks (a field an upgrade added). So an upgrade
  that *changes* a default never silently moves an existing object, and
  one that *adds* a defaulted field reaches them all (Kubernetes'
  semantics). The upgrade prompt reports default changes and how many
  objects keep the old value;
- **`generation`**, from 1, bumped when the spec changes;
- **`specVersion`**, the compare-and-set token for apply and delete, a
  value of a never-repeating counter, new on every write of the spec or
  labels (and on a delete's move to `Deleting`) and **never** on a status
  write, so a person's `--expected-version` does not go stale because the
  app reported;
- **labels**; who created it and last wrote it, and when; the schema
  digest it was checked against;
- **`authority`**: the permissions its `requires` resolved to at its last
  write, as verified against the writer, with who and when (§6.4);
- **`managedBy`**: the app whose own identity last wrote it, if any (§6.5);
- **`deletingSince`**, while a bounded cleanup runs (§6.9).

Its **state**, derived on read: `valid`; `invalid` (the stored spec fails
the kind's current schema: an upgrade tightened it; readable, deletable,
and the next apply must conform); `orphaned` (its kind is orphaned:
readable and deletable with the kind's recorded access, not writable).

**Limits.** A spec is at most 64 KiB after defaults; at most 32 labels, keys
at most 63 characters, values at most 256; a kind's `maxObjects`, enforced
inside the replicated write that creates an object. Per-namespace quotas
belong in namespace policy (deferred).

### 6.4 ACL and authority

**Access.** Reads and writes check the kind's access resource on the
object's scope:

| `access.resource` | Namespaced kind | Cluster kind |
| --- | --- | --- |
| `object` (default) | the object's namespace and name, and `kinds = <app>/<Kind>` | namespace `*` (only `namespaces = ["*"]` reaches it), name, kind |
| `job`, `scale` | the object's namespace, **name `*`**: namespace-wide | refused at install |
| `node` | refused at install | `AclScope::none()` (plus the kind) |

The new **`object`** resource is namespaced (rules carry `namespaces`) with
a new **`kinds`** selector, globs over `<app>/<Kind>`:

```nickel
{ resources = ["object"], actions = ["read", "write"], namespaces = ["team-a"], kinds = ["gitops/Repo"] },
{ resources = ["object"], actions = ["read"], namespaces = ["*"], kinds = ["gitops/*"] },
```

A mapped namespaced kind is checked **namespace-wide**: the scope's name
is `*`, which only a name-unrestricted rule (or a `names = ["*"]` one)
matches, so selectors written about job names never decide who writes
objects. Per-name control is the `object` resource's `names`. A `job`
mapping carries `band = "normal"` on its scope, as a plain submit does, so
band-scoped `job` rules still match. The mapping is limited to these four
resources because each has a documented translation; install refuses
anything else. The install prompt shows the mapping:

```text
Kinds:
  gitops/Repo   namespaced; written with object:write on gitops/Repo, and
                job:write in the namespace spec.targetNamespace names; checked by the app
```

**`*` keeps its meaning.** A `resources = ["*"]` rule in `team-a` reaches
`team-a`'s objects of every kind, including kinds installed later, as `*`
means for every other resource. Its blast radius is bounded by
`requires`: writing an object still needs the rights its contents imply.

**Requires.** What an object makes its app do is checked against its
writer, in core, fail-closed, on apply and on delete:

```nickel
requires = [
  { resource = "job", actions = ["write"] },                                     # the object's namespace, namespace-wide
  { resource = "job", actions = ["write"], namespace = "/spec/targetNamespace" },  # a pointer: a string or a list of strings
  { resource = "kv",  actions = ["read"],  path = "/spec/credentials/kv" },       # generalizes enforce_kv_credential_read
  { resource = "job", actions = ["write"], host = true, when = "/spec/allowHost" },  # host-level, only when the spec asks
  { resource = "operator", actions = ["write"] },                                  # cluster-scoped: AclScope::none()
]
```

- Each entry is resolved against the **effective spec** (with defaults)
  and checked with the scope **the corresponding core RPC builds**: a
  namespaced resource on the namespace (the pointer's value, or the
  object's own namespace; `*` for a cluster kind's object) and name `*`
  unless a `name` pointer gives one; `kv` on the `path`; `node` on the
  `pool` pointer if any; a cluster-scoped resource on `AclScope::none()`.
  `job` carries `band` (a pointer, default `normal`), `template` (a
  pointer), and `host` the way a submit does.
- A pointer that resolves to nothing: `namespace` falls back to the
  object's namespace; `path` and `when` make the entry not apply; `name`,
  `pool`, `band`, `template` take their defaults.
- **The resolved list is recorded on the object as `authority`**: the
  concrete permissions verified, and who authorized them (principal,
  accessor) and when. O3 bounds the app's object-scoped calls by it, never
  by the kind's current declaration. An upgrade that changes `requires`
  affects only later writes; the prompt says `N Repos were authorized under
  the previous requirements; they keep that authority until re-applied`.

**Check before fetch.** The kind is resolved from the `object_kind` table
(public metadata), the ACL is enforced on the requested scope, and only
then is the object read, so a caller without access cannot tell a missing
object from a forbidden one. List endpoints gate on "may read this kind
anywhere" and filter items. Orphaned objects are read and deleted with
their kind's recorded access mapping.

### 6.5 The API

| RPC | ACL | What |
| --- | --- | --- |
| `ListObjectKinds` | per kind: read on its access resource somewhere | kinds (active and orphaned), schemas, scope, access, requires, immutable, cleanup, columns |
| `ApplyObject` | write on access + `requires`, on the object's scope | create or update; qualified kind; `dry_run` field diff; `expected_version` (`specVersion`) CAS, `0` = create only; `force` over a managed object; `idempotency_key`; `warnings` |
| `GetObject` | read on access | one object: spec, status, summary, state, uid, authority |
| `ListObjects` | read on access, per item | by kind (or every kind), namespaces, labels |
| `DeleteObject` | write on access + `requires` | CAS on `specVersion`; `now` skips cleanup; `force` over a managed object; idempotency |
| `WatchObjects` | app-plane: the owning app | chunked snapshot, upserts, tombstones; filtered to the app's kinds; shardable |
| `UpdateObjectStatus` | app-plane: the owning app | conditions and fields with `observedGeneration`; uid-checked; Status lane |
| `ReleaseObject` | app-plane: the owning app | ends a deleting object's cleanup: the row goes |

- **Not installed yet.** `ApplyObject` of a kind no installed app declares
  fails with error code `KIND_NOT_INSTALLED` on a retryable status, so a
  GitOps sync that applies an app and its objects in one pass converges on
  retry.
- **Managed objects.** An object written by an app's own identity (its
  `legion` token, not a delegated viewer) is stamped `managedBy = <app>`,
  as the jobs an app submits are. A person's apply or delete of a managed
  object needs `force`. Delegated identities (`alice via app gitops`)
  count as the person.
- **Namespaces** (calls entry 21): a namespaced object's namespace is the
  one it names, else the request's default namespace, else `default`. For
  a cluster kind, a namespace in the object is an error and the request's
  default is ignored with a warning.

```console
$ legion object apply -f repo.ncl --dry-run
gitops/Repo team-a/storefront: would change (generation 3 → 4); dry run, nothing written
  spec.branch: "main" → "release"
  spec.token: changed (redacted)
$ legion object apply -f repo.ncl --wait
gitops/Repo team-a/storefront: updated (generation 4, spec version 118)
gitops/Repo team-a/storefront: Ready (Synced: at 3f9a1c2)
$ legion object apply -f repo.ncl --expected-version 117
Error: gitops/Repo team-a/storefront is at spec version 118, not 117: it changed since you read it
Hint: read it again (legion object get gitops/Repo storefront -n team-a) and apply on top of it
```

### 6.6 The validate call: advice, never a control

A kind with `validate = true` is asked by its app on every apply that
changes the spec, after the schema, `immutable`, and `requires` checks
pass: `POST /legion/v1/objects/validate` to one allocation, through the
worker-plane forward and viewer assertion the extension validate call uses
(kind `validate`, name `object`, the writer as the viewer, the body's
digest as its inputs). The body carries the kind, the ref, `create` or
`update`, the spec as written and effective, and the stored spec it
replaces. The app answers `{ "errors": [...], "warnings": [...] }`: errors
refuse the apply, warnings come back in the reply.

It **fails open**: an app that is disabled, unreachable, slow (two
seconds), or answers anything but `200` lets the write through with a
warning. That makes it user experience, never a security check: a writer
can make it time out (a tarpit URL in a field the app probes). Everything a
writer must be refused lives in core: the schema, `immutable`, and
`requires`. A dry run asks too.

### 6.7 Status

```nickel
status = {
  observedGeneration = 4,
  conditions = [{ type = "Ready", status = "true", reason = "Synced", message = "at 3f9a1c2", since = "2026-10-29T10:02:11Z" }],
  fields = { commit = "3f9a1c2" },     # checked against the kind's status.schema
}
```

- Status is its **own row** in `app_object_status`, keyed by the object's
  `uid`, on the **Status lane**, beside `node_health`, `task_status`, and
  `allocation_health`: `observedGeneration`, `conditions`, `fields`, who
  wrote it and when. Status churn never touches the Control lane, never
  moves `specVersion`, and never wakes a watch.
- **Identical writes are dropped** without proposing to raft.
- **Status CAS**: the `uid` must be the current object's; the
  `observedGeneration` must not exceed the generation this replica sees,
  and must not be below the stored one (it never goes backwards, so a slow
  replica cannot overwrite newer status). Because the Status lane may lag
  the Control lane, `observedGeneration` is how a reader tells which spec a
  status describes.
- **Limits**: `fields` at most 16 KiB; at most 16 conditions; a message at
  most 1 KiB.
- **Standard conditions**: `Accepted` (the app understood and accepted the
  spec) and `Ready` (reconciled and working). Apps may add their own types.
- **Status is for facts that change when the world changes**, not per-tick
  telemetry: per-evaluation numbers stay in memory, as
  `ReportScalingStatus` already does.
- Status rows whose `uid` no longer has an object are swept on the Status
  leader. `legion_object_status_writes_total{app}` counts writes.

The manager derives a **summary** for every object, whatever its kind:

| Summary | When |
| --- | --- |
| `Pending` | no status yet, or `observedGeneration < generation` |
| `Ready` | `Ready` is true |
| `NotReady: <reason>` | `Ready` is false |
| `Rejected: <reason>` | `Accepted` is false |
| `Stale` | the owning app is disabled or not running; the status is shown with its age |
| `Invalid` | the spec fails the current schema |
| `Orphaned` | the owning app or kind is gone |
| `Deleting` | a bounded cleanup is running (§6.9) |

`legion object apply --wait [--timeout 2m]` waits until `observedGeneration`
reaches the applied generation and `Ready` (or, without one, `Accepted`)
is settled, and exits non-zero on `Rejected` or `NotReady`, for every
kind. The MCP apply tool takes `wait`.

### 6.8 Watching, and the app framework

`WatchObjects` is the app's feed:

- the snapshot is **chunked**: at most 1,000 objects per message, the last
  with `snapshotDone`; the framework raises its client's decode limit;
- later messages carry upserts (any change of the spec row, or of an
  object's state after a kind changes) and **tombstones** (deleted
  objects, with their `uid`); every message has a stream `sequence`;
- each object carries its `uid`, spec (with `writeOnly` values, which only
  the owning app sees), the stored status at the time it was read,
  `deleting` and its deadline, and its `shard`;
- after a reconnect the app gets a fresh snapshot and treats a `uid` it no
  longer sees as deleted; the framework's cache does this;
- it polls a **per-app fingerprint** of the spec table and the app's kinds,
  so another app's writes and every status write leave it asleep;
- **shards**: FNV-1a 64 of `<app>/<Kind>:<namespace>/<name>`
  (`<app>/<Kind>:<name>` for a cluster kind) modulo `shard_count`, the
  autoscaler's hash (calls entry 18). The 64-shard value is a stored,
  indexed column, so the default count filters in the query;
- no `since_version` resume: a relist is fine to about 50,000 objects per
  app (calls entry 17).

The framework (`src/apps/objects.rs`) gives an app a **reconcile loop**:

```rust
ctx.reconcile("gitops/Repo", Reconciler::new(|object| async move {
    sync(&object).await?;                          // Err → Ready=false, backoff
    Ok(Outcome::ready("Synced", format!("at {commit}")).with_fields(json!({ "commit": commit })))
}).on_cleanup(|object| async move { release_external(&object).await }));
```

- level-triggered: on change, and a periodic resync (default 5 minutes);
- serialized per object, with exponential backoff on errors;
- the outcome becomes conditions and `fields`, written **only when
  changed**, with `observedGeneration` handled for the handler;
- deleting objects go to the cleanup handler, and are released when it
  returns `Ok`;
- an optional static shard set (O2 lifts the autoscaler's lease-based
  shards into it).

`watch_objects`, `write_object_status`, and the validate handler stay
available for apps that want their own loop.

### 6.9 Lifecycle and deletion

- **Install** registers the kinds (`object_kind` rows). Objects an earlier
  install of the same app left orphaned are **adopted** again: the prompt
  says how many, they are re-checked against the new schemas (failures
  read `Invalid`), and they keep their recorded `authority`.
- **Upgrade** may add kinds and change schemas, access, `requires`,
  `immutable`, `cleanup`, and columns. The prompt reports every existing
  object checked against the new schema, every default that changed and
  how many objects keep the old value, and how many objects were authorized
  under different `requires`. Dropping a kind that still has objects needs
  `--force` and orphans them. Changing a kind's `scope` while it has
  objects is refused.
- **Disable** changes nothing about objects: they stay writable, the
  validate call fails open, nothing acts on them, and their summary reads
  `Stale`.
- **Uninstall** with objects needs `--force`; they stay, orphaned. Objects
  are never deleted by an app's lifecycle (Helm's CRD lesson).
- **Namespaces.** Objects are namespace contents: a namespace delete is
  refused while any non-deleting objects are in it, and `--cascade` deletes
  them, checking the caller's per-kind access and `requires` for each. A
  recreated namespace can never resurrect objects.
- **Bounded cleanup, no finalizers.** A kind with `cleanup = "10m"` turns a
  delete into a two-step: the object gets `deletingSince` (its spec
  freezes: applies are refused), and the app sees `deleting` on its watch,
  does its cleanup, and calls `ReleaseObject`, which removes the row. At
  the deadline, or at once when the kind is orphaned, the manager removes
  it anyway and emits `object.cleanup_abandoned`. `legion object delete
  --wait` waits for the row to go; `--now` skips the cleanup (same
  rights). Nothing can stay stuck, which is the difference from Kubernetes
  finalizers. A deleting object does not hold its namespace.
- **No owner references and no core GC.** An app stamps what it creates
  with the object's `uid` and garbage-collects level-triggered when the
  object is gone; the cleanup hook covers external resources it must
  release while it can still read the spec.

### 6.10 Events

`object.applied` (created or updated: generation, actor), `object.deleted`,
`object.deleting` (a cleanup started), `object.cleanup_abandoned`, and
`object.status_changed` (a `Ready` or `Accepted` transition only, not every
status write). They carry only the ref, `uid`, generation, actor, and the
condition transition: never a spec, status fields, or redacted value. An
`object.*` event reaches only callers who may **read that kind on that
object's scope** (or hold `operator:read`); reaching the namespace is not
enough, since that would defeat check-before-fetch.

### 6.11 CLI and MCP

```console
$ legion object kinds
KIND           SCOPE       ACCESS                         STATE   DESCRIPTION
gitops/Repo    namespaced  object (+ job:write in spec)   active  A git repository gitops deploys jobs from.
$ legion object explain gitops/Repo.spec.branch
gitops/Repo.spec.branch: string, default "main"
...
$ legion object apply -f repo.ncl [--dry-run] [-n NS] [--expected-version N] [--force] [--wait [--timeout D]]
$ legion object get repo storefront -n team-a
$ legion object list repos -n team-a          # -A; -l team=a; no KIND lists every kind
NAME        STATUS                AGE   URL                              COMMIT
storefront  Ready                 3d    https://github.com/acme/deploy   3f9a1c2
$ legion object delete repo storefront -n team-a [--expected-version N] [--wait | --now] [--force]
```

Every command takes `-j` and emits the standard envelope. `explain` reads
the kind's schema (description, type, default, required, immutable,
write-only, examples) for the kind or a field path. MCP:
`legion_object_kinds`, `legion_object_list`, `legion_object_get` (read);
`legion_object_apply` (with `wait`) and `legion_object_delete` (mutating,
behind `--allow-write`). Capability entries: `object.kinds`,
`object.explain`, `object.apply`, `object.get`, `object.list`,
`object.delete`.

## 7. Proto, struct, config, and persistence changes

- **Proto.** Eight Manager RPCs (§6.5) and their messages: `ObjectKind`,
  `AppObject`, `ObjectStatus`, `ObjectCondition`, `ObjectAuthority`,
  `ObjectRef`, `ObjectEvent`, and request/reply pairs.
  `UpgradeAppRequest.force` (drop a kind with objects);
  `UninstallAppReply.orphaned_objects`; `App.kinds`;
  `NamespaceCounts.objects` and `NamespaceContents.objects`. Every field
  commented.
- **Manifest.** `AppManifest.kinds` (JSON, parsed to `KindDecl` in
  `src/structs/app_object.rs`, like `extensions`).
- **ACL.** `AclResource::Object` (`object`, namespaced); `AclRule.kinds`;
  `AclScope.kind`; grant coverage and rule wording learn the selector.
- **Persistence.** One migration, three tables:
  - `object_kind (app, kind, decl, digest, state, created_ms, updated_ms)`,
    Control lane;
  - `app_object (app, kind, namespace, name, uid, spec, labels, generation,
    spec_version, authority, managed_by, deleting_since, cleanup_deadline,
    shard, schema_digest, created_by, created_ms, updated_by, updated_ms)`,
    primary key (app, kind, namespace, name), unique `uid`, Control lane;
  - `app_object_status (uid, observed_generation, conditions, fields,
    updated_by, updated_ms)`, Status lane.

  `uid`s and spec versions come from the `app_object` counter in
  `scaling_counter` (calls entry 25). No job-config field changes, so
  `maximal.ncl` is untouched.
- **Metrics.** `legion_object_status_writes_total{app}`.
- **Examples.** `configs/apps/example-app.ncl` declares `Repo`;
  `configs/apps/example-repo.ncl` is an object file.

## 8. Implementation phases

**O1: the generic system** (this change; independently valuable: GitOps and
every third-party app can have objects). It may ship as two commit series:
core (§6.1-§6.7, §6.9-§6.11), then the framework (§6.8's reconcile loop),
the example, and the docs.

**O2: the autoscaler's policies become its objects** (a later change;
valuable alone: it deletes a core subsystem and proves the generic one).

- The bundled manifest declares `autoscaler/ScalingPolicy` (namespaced,
  `access = scale`, namespace-wide; `immutable` on the target plugin) and
  `autoscaler/NodeGroup` (cluster, `access = node`, `requires`
  `operator:write`, `cleanup = "15m"` so deleting a group drains and
  terminates its nodes). Schemas are generated from the existing
  `StandaloneSpec` structs (`schemars`), `validate = true`.
- Removed, with no aliases (AGENTS.md): the `scaling_policy` table (a
  migration drops it; rows are not converted, there being no installed
  base), `ApplyScalingPolicy`, `DeleteScalingPolicy`,
  `ListScalingPolicies`, `legion scaling apply|list|delete`, the
  `legion_scaling_apply` MCP tool, and the manager-side node-group ACL
  special case. `legion object apply|list|delete` replace them.
- Autoscaler state (history, pauses, intents) is keyed so that `NodeGroup
  workers` and `ScalingPolicy default/workers` cannot collide: by the
  object ref with its kind, or by `uid`. Ids may break.
- `WatchScalingPolicies` serves task policies only; the autoscaler
  reconciles its kinds through the framework (lifting its lease-based
  shards into it). Durable verdicts (`Accepted`, `Ready`, the resolved
  target, the last action) go in object status; per-tick numbers stay on
  the `ReportScalingStatus` board.
- The namespace plugin allowlist is enforced by the app at evaluation,
  fail-closed: it is a security control. The validate call mirrors it for
  the user.
- Every place core reads standalone policies is listed and reworked:
  `ApplyScalingDefaults`' invalidation check, `GetScalingStatus`,
  `RecordScalingAction`'s existence check, and pause and history lookups.
  `PAGE_RPCS` is updated. The policy parser stays linked into the manager
  for the defaults check, and the plan says so.
- Cross-field checks (`min <= max`) move to the app (`Accepted = false`)
  until O4 adds manager rules: a regression from today's fail-closed apply,
  recorded.
- `configs/scaling/*.ncl` take the object file format. **Scaling defaults
  stay core**: the manager resolves `autoscale` at submit and in `job
  plan` against them (calls entry 23).

O2's risks: history, pauses, and intents rekeyed (ids break once); the
shard key changes from `policy:<id>` to the object shard key, so every
external policy moves shard once and its windows restart; `legion scaling
list` loses standalone policies to `legion object list`, so the UI's
autoscaling page and `scaling status` must read objects; the cross-field
regression above.

**O3: objects carry their authority.** The app calls with
`legion-for-object: gitops/Repo/team-a/storefront#<uid>@<generation>`; the
manager evaluates the call as "gitops for Repo team-a/storefront": the
app's grant intersected with the object's recorded `authority`, reusing
the delegation intersection. Writes made this way are stamped `managedBy =
{ app, object uid }`, so `legion job status` shows "managed by gitops for
Repo team-a/storefront". Manifest permissions may say `via = "objects"`:
usable only through object-scoped calls, so the install prompt can say
"gitops writes jobs only where a Repo's author could". Authority is
recorded at write and not revoked when the writer later loses rights (a
departing engineer does not break production deploys, as with Kubernetes
and Flux); `legion object list --authority-lapsed` finds objects whose
authorizer no longer holds the requirements, and a re-apply re-authorizes.
Rejected: reconciling as the writer's current rights (calls entry 26).

**O4: CEL `rules`** on a kind, cost-bounded and fail-closed in the
manager: `rules = [{ rule = "self.min <= self.max", message = "min must not
exceed max" }]` (the `cel-interpreter` crate, MIT). Until then a manifest
that sets `rules` is refused.

## 9. Testing plan

- **Unit** (`src/structs/app_object.rs`, `acl.rs`): kind declaration rules
  (names, reserved nouns singular and plural, within-app duplicates,
  schema rules, closing, examples, access pinning, `requires` pointers,
  `immutable`, `cleanup`, `maxObjects`, `rules` refused, columns); kind
  resolution (qualified required for files, short names, ambiguity);
  object parsing; defaults persisted at write and filled for added fields;
  unknown fields with their paths; `writeOnly` redaction in reads and
  diffs; requirement resolution (pointers, lists, fallbacks, `when`,
  `host`, `band`); the summary table; shard keys; the `object` resource,
  `kinds`, namespace-wide scopes (`*` names), and `AclScope::none()`.
- **Store** (`app_object_ops.rs`): uid never reused, generation versus
  `specVersion`, CAS (create-only, mismatch), `maxObjects` in the write,
  status rows on their own lane (uid check, generation bounds, identical
  writes dropped), deleting and release, cleanup expiry, kind sync and
  orphaning, fingerprints.
- **Integration** (`tests/integration/app_objects.rs`): install the example
  app; apply/get/list/delete with ACLs on (object resource by namespace and
  kind; a kind mapped to `scale` checked namespace-wide; `requires` with a
  namespace pointer and a KV path; band-scoped rules; check-before-fetch:
  PermissionDenied for a missing object too); dry-run diff; CAS;
  idempotency; the validate call (refusal, warning, fail-open); managed
  objects and `force`; `WatchObjects` chunked snapshot, upsert, tombstone,
  shards, refused to another app; `UpdateObjectStatus` and the summary;
  `ReleaseObject` and cleanup expiry; events filtered by kind ACL; upgrade
  report, `invalid`, orphaning with `--force` and readable orphans with
  their mapping; namespace delete refused and `--cascade`.
- **ACL coverage**: eight rows in `tests/integration/acl_endpoints.rs`;
  `every_manager_rpc_is_classified`.
- **Raft**: `raft_seam::` and the ownership coverage test with the three
  tables on their lanes.
- **CLI/MCP**: every command takes `--json`; read tools listed without
  `--allow-write`, mutating ones only with it; catalog consistency.

## 10. Rollout and backward compatibility

Additive in O1: new tables, RPCs, a manifest field, an ACL resource and
selector. Existing manifests (no `kinds`) are unaffected.

**Who can reach objects on day one.** Not nobody: a `resources = ["*"]`
rule reaches the `object` resource like every other, so a token holding
`*` in `team-a` can read and write `team-a`'s objects of every kind,
including kinds installed later. That is deliberate (`*` keeps one
meaning; calls entry 5), and bounded: writing an object still needs what
its `requires` resolve to. Rules naming specific resources reach no
objects until an admin adds `object` rules; management tokens reach all.

A mixed-version cluster: an older manager cannot apply the new tables'
changesets, which the raft schema-version guard refuses to propose to it
(docs/upgrades.md), so upgrade managers before installing apps with kinds.

O2 is a breaking change by design (AGENTS.md): `legion scaling apply` and
its RPCs disappear in the release that moves the autoscaler onto objects;
the PR says so.

## 11. Open questions

- **Should the validate call run on delete?** Lean: no. Deleting must never
  depend on the app; the cleanup window is the app's chance to act.
- **Per-namespace object quotas.** Lean: a namespace-policy limit
  (`quota.objects = { "gitops/Repo" = 50 }`) enforced inside the write, as
  `maxJobs` is. Deferred.
- **Pending objects for install ordering** (`--allow-pending`, adopted at
  install only if the stored writer still passes `access` and `requires`).
  Lean: wait for GitOps to need it; `KIND_NOT_INSTALLED` retries cover it.
- **Event-driven watch delivery and resume.** Lean: do both watches
  together once a change log exists.

## 12. Acceptance criteria

- **O1.**
  - Installing the example app shows `gitops/Repo`, its access, and its
    `requires` in the prompt; `legion object kinds` lists it.
  - A token with `object:write` on `gitops/Repo` and `job:write` in
    `team-a` applies `configs/apps/example-repo.ncl`; the same token is
    refused a Repo whose `spec.targetNamespace` is `team-b`, is denied in
    `team-b`, and gets PermissionDenied (not NotFound) for `legion object
    get gitops/Repo missing -n team-b`.
  - A file with `kind = "Repo"` is refused, naming `gitops/Repo`; `legion
    object get repo storefront` works while the name is unambiguous.
  - `--dry-run` prints the field diff (a `writeOnly` field as `changed
    (redacted)`); `--expected-version` refuses a stale write and is not
    made stale by a status write; a repeated `--idempotency-key` is
    deduplicated; an unknown field and an `immutable` change are refused
    with their paths.
  - The owning app's watch gets a chunked snapshot and the apply; another
    app is refused; its `Ready` status shows in `get`, `list` shows
    `Ready`, and `apply --wait` returns.
  - An upgrade that tightens the schema and changes a default reports
    both; failing objects read `Invalid`; removing the kind or uninstalling
    needs `--force` and leaves them `Orphaned`, readable and deletable with
    the kind's mapping.
  - A namespace holding objects is not deleted without `--cascade`; a kind
    with `cleanup` deletes in two steps and never longer than its window.
- **O2.** `legion object apply -f configs/scaling/render-farm.ncl` creates
  an `autoscaler/ScalingPolicy` with `scale:write` in its namespace and
  nothing else; a `NodeGroup` needs `node:write` and `operator:write`; the
  autoscaler reports both in status; `legion scaling apply` no longer
  exists.
- **O3.** A Repo authored by someone holding `job:write` only in `team-a`
  cannot make GitOps write a job in `team-b`, whatever the app's grant.
- **O4.** A `ScalingPolicy` with `min > max` is refused at apply.
