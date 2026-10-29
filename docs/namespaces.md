# Namespaces

A namespace is a **name scope**, a **permission scope**, and a **policy
anchor**. Two teams can each run a job called `web`; a token can be limited
to `payments-*`; and a namespace policy can cap what every `payments-*`
namespace holds, together.

A namespace is **not an isolation boundary.** Creating one moves no
workload onto other machines, networks, or kernels. Node pools,
`network.mode = "gated"`, net policies, and the `sandbox` runtime do that.
The table below says what a namespace does, plane by plane, so nobody has to
infer it.

| Plane | A namespace… | It does **not**… | Real isolation comes from |
| --- | --- | --- | --- |
| Names | separates every job-keyed name: jobs, versions, instances, volume claims, snapshot aliases, tickets, content-store paths | — | — |
| API and ACLs | scopes every namespaced RPC and list (`job`, `exec`, `action`, `scale`, `boot`, `snapshot`, `namespace`) | hide cluster-scoped views: `node:read` sees every allocation's job id in the ledger, the state explorer is raw SQL over everything, `operator:read` sees every event | not granting `node`, `state` or `operator` to tenants |
| Service discovery and mesh | resolves service names inside the namespace; reaching another namespace is written on both sides ([Services](#services-and-the-mesh)) | stop a process from dialing an address it already knows | `network.mode = "gated"` plus net policies that select namespaces |
| Network | nothing by itself | filter packets | `gated` networking plus net policies |
| Compute | nothing, unless a [namespace policy](#namespace-policies) restricts its pools or a worker type [accepts only some namespaces](#nodes-that-accept-only-some-namespaces) | separate kernels or caches | node pools; node-side `namespaces` lists; the `sandbox` runtime |
| KV | nothing | KV is path-scoped and cluster-wide | ACL `paths` selectors (the `legion/` prefix is [reserved](#reserved-namespaces)) |
| Workload identity | makes subjects distinct and adds a `legion.io/namespace` claim | — | IdP trust policies that match the namespace |
| Capacity | nothing, unless a namespace policy sets a quota | — | [namespace policies](#namespace-policies) |
| Observability | stamps events and metrics; tenants read their own events | — | — |

`default` always exists and cannot be deleted. A cluster that never creates
another namespace behaves exactly like one without the feature: every job
lives in `default` and keeps its bare name. Legion creates one namespace of
its own, `legion-system`, for the jobs it runs on the managers
([Reserved namespaces](#reserved-namespaces)).

## Quick start

```console
$ legion namespace create payments-prod -d "Payments, production"
Created namespace payments-prod.

$ legion job run web.ncl -n payments-prod
$ legion job list -n payments-prod
$ legion job status web -n payments-prod          # or: legion job status payments-prod/web
$ export LEGION_NAMESPACE=payments-prod          # the sticky form, for this shell
$ legion job list -A                             # every namespace you can read
```

A jobspec may name its own namespace:

```nickel
{
  name = "web",
  namespace = "payments-prod",
  tasks = [ ... ],
}
```

## Job identity

A job's id is its **canonical id**: the bare name in `default`
(`web`), and `namespace/name` everywhere else (`payments-prod/web`). It is
the storage key and the display form, and every RPC field that names a job
takes it. Everything derived from a job id inherits the namespace: job
config ids (`payments-prod/web-4`), slot instance ids
(`payments-prod/web-frontend-0`), volume claim ids, seal-key inputs, and
content-store paths. No job-keyed table has a separate namespace column, so
none can forget it.

`Job` replies carry `namespace` and the bare `name` beside the canonical
`id`.

### Which namespace a submit lands in

The manager resolves it, from the jobspec and the request:

| Jobspec `namespace` (or a qualified `name`) | Request `namespace` (`-n`) | Result |
| --- | --- | --- |
| unset | unset | `default` |
| unset | `dev` | `dev` |
| `prod` | unset | `prod` |
| `prod` | `prod` | `prod` |
| `prod` | `dev` | `InvalidArgument`: `jobspec declares namespace 'prod' but the request names 'dev'; remove one` |

The same rule covers every request that carries a spec: `RunJob`,
`PlanJob`, `RunJobTemplate` and `RenderJobTemplate`. Authorization runs
against the resolved namespace, and only then does the manager check that
the namespace exists, so a caller with no grant there cannot learn whether
it does.

On the CLI, `-n` is always sent. `LEGION_NAMESPACE` is a default, like a
kubectl context namespace: it applies only when the jobspec names no
namespace, and never conflicts with one.

### References on the CLI

| You type | Meaning |
| --- | --- |
| `web` | `web` in the **active namespace** |
| `payments-prod/web` | `web` in `payments-prod`, whatever the active namespace is |
| `-n payments-prod web` | the same |

The active namespace is `-n/--namespace`, else `LEGION_NAMESPACE`, else
`default`. There is no persisted context: `export LEGION_NAMESPACE=…` lives
as long as the shell and shows up in `env`.

- `-n` exists only on commands that act on something namespaced. Every
  command with a job or instance argument has it (as `--namespace` alone
  where `-n` was taken, as on `instance fork --count`). `legion node list
  -n prod` is a usage error, not a silently ignored flag.
- A qualified name that disagrees with `-n` is an error, not a precedence
  rule: `"checkout-prod/web" names namespace 'checkout-prod' but the request
  names 'payments-prod'; drop one`.
- **List commands** (`job list`, `job rollout list`, `service list`, `volume
  list`, `snapshot list`) take names or globs, comma-separated
  (`-n 'payments-*,shared'`), and `-A/--all-namespaces` means every namespace
  you can read. For a selection spanning several, `job list` adds a
  `NAMESPACE` column and `service list` qualifies each name
  (`shared/auth`); the others show the canonical job id, which carries it.
- **Not found names the namespace, and helps without leaking.** When a job
  of that name exists in other namespaces you can read, the error says so;
  namespaces you cannot read are never named:

  ```console
  $ legion job status api
  error: job 'api' not found in namespace 'default'
  hint: a job named 'api' exists in namespace 'payments-prod'; retry with -n payments-prod
  ```

- **An empty list says what it hid:** `No jobs in namespace 'default'. 14
  job(s) in 2 other namespace(s) you can read; use -A.`
- **A denial names where you can act**, read off your own token's rules:

  ```console
  $ legion job stop checkout-prod/web
  error: permission denied: job:write requires a policy granting it in this namespace
  hint: this token can write job in payments-*
  ```

Tasks get `LEGION_NAMESPACE` and `LEGION_JOB_NAME` (the bare name) in their
environment, beside `LEGION_JOB_ID` (the canonical id). A `legion` command
run inside a task therefore defaults to the task's own namespace.

MCP tools that name a job or an instance take an optional `namespace` and
accept qualified names; list tools take `namespaces` (an array of names or
globs, `["*"]` for every namespace). Replies carry canonical ids.

## Managing namespaces

```console
$ legion namespace list
$ legion namespace get payments-prod
$ legion namespace create payments-loadtest --meta owner=payments --dry-run
$ legion namespace apply -f payments-prod.ncl       # { name, description, meta }
$ legion namespace delete payments-old              # refused while it holds anything
$ legion namespace delete payments-old --dry-run    # list what it holds
$ legion namespace delete payments-old --cascade    # stop and destroy it all, then delete
```

- **Names are DNS labels**: lowercase letters, digits and `-`, starting and
  ending alphanumeric, at most 63 characters. The `legion-` prefix is
  [reserved](#reserved-namespaces): nobody can create a `legion-*`
  namespace. Namespaces cannot be renamed.
- **`description` and `meta`** are free-form; Legion never reads `meta`.
  The namespace object carries nothing policy-bearing, so whoever may write
  a namespace cannot loosen anything by editing it.
- **Creating a namespace reports its blast radius.** `create` and `apply`
  (and `--dry-run`) list every ACL rule that reaches the new namespace,
  including rules written long ago with globs like `payments-*` or `*`, so
  the grants that take effect the moment it exists are never a surprise.
  The list needs `acl:read`.
- **`get` shows** the namespace's contents by kind, its [charged
  usage](#how-quota-is-charged), and every namespace policy governing it
  with the aggregate that policy is checked against.

### Deleting a namespace

A delete is refused while the namespace holds anything, naming what is in
the way:

```text
namespace 'payments-old' is not empty: 3 jobs, 1 volume
```

`--dry-run` lists the contents. `--cascade` stops every job, waits up to 90
seconds for their allocations to drain, destroys the jobs, deletes their
volumes and snapshots, then deletes the namespace, synchronously and in
that order, printing each step. There is no `Terminating` state: a cascade
that fails halfway leaves the namespace present and the command re-runnable.
The final delete is one conditional write, so a submit that races in makes
it refuse rather than succeed falsely.

A cascade needs `job:write` in the namespace (and `snapshot:write` when it
holds snapshots) on top of `namespace:write`, so the right to delete an
environment is never a back door to destroying workloads you could not
otherwise touch. `default` cannot be deleted, and neither can a reserved
`legion-*` namespace.

## Reserved namespaces

Names starting `legion-` belong to Legion. Today there is one:
**`legion-system`**, where Legion's own operational jobs run, on the
managers' [system workers](architecture-managers-workers.md#the-system-worker).

- **Created on an empty store.** The first manager to lead the control lane
  of a new cluster creates `legion-system`, together with a namespace policy
  of the same name:

  ```nickel
  { name = "legion-system", namespaces = ["legion-system"],
    pools = { default = ["legion-system", "default"] } }
  ```

  A job in `legion-system` whose permit names no pool therefore may run in
  the `legion-system` pool, which is where the managers' system workers are,
  or in `default`, where a `--run-worker` manager's worker is (a dev
  cluster). It does not reach ordinary workers in `default`: their
  `namespaces` (by default `["*"]`) refuse `legion-system`, and that, not
  the pool, is the boundary ([Nodes that accept only some namespaces](#nodes-that-accept-only-some-namespaces)).
  The policy is an ordinary one: an operator may edit or delete it, and
  Legion never writes it again. A cluster created before this release keeps
  the single-pool default it was created with.
- **Nobody can create, rename, or delete one.** `legion namespace create
  legion-x` is refused, and so is deleting `legion-system`:

  ```text
  namespace 'legion-system' is reserved for Legion and cannot be deleted
  ```
- **`*` does not reach them.** In an allow rule's `namespaces` selector, a
  glob matches a `legion-*` namespace only when the pattern itself starts
  with `legion-`, the way a shell's `*` skips dotfiles. `["*"]` covers every
  tenant namespace, today's and tomorrow's, but not `legion-system`;
  `["legion-*"]` or `["legion-system"]` reach it, and "every namespace,
  reserved ones included" is `["*", "legion-*"]`. The same holds for an
  allow rule with `resources = ["*"]` and no selector: it reaches what `*`
  reaches. A tenant admin holding `namespaces = ["*"]` therefore cannot
  submit to, scale, or exec into a job in `legion-system` and borrow the
  credential it runs with:

  ```text
  $ legion job run agent.ncl -n legion-system
  error: permission denied: job:write requires a policy granting it in this namespace
    Hint: this token can write job in *
    Hint: `*` does not reach reserved `legion-*` namespaces; a rule must name them,
    as `namespaces = ["legion-system"]` or `["*", "legion-*"]`
  ```

  Deny rules keep plain globs, because the rule exists to keep grants from
  reaching too far and a deny that reached less would grant more: a deny on
  `["*"]` still denies inside `legion-system`. Management tokens bypass
  rules, as always.
- **The `legion/` KV prefix is reserved the same way.** An allow rule's
  `paths` pattern reaches a key under `legion/` only when it starts with
  `legion/`; `paths = ["*"]`, and a `kv` rule with no `paths`, stop short of
  it. It is set aside for the leases and state of Legion's own jobs.
- **Namespace policies skip them too.** A policy whose selector is `["*"]`
  does not govern `legion-system`, so a tenant-wide quota, runtime list, or
  default pool never lands on what Legion runs for itself. A selector
  starting `legion-` governs it.
- **Tasks there run unprivileged.** They run on the managers, where root
  could read the manager's data directory and raft state, delete the host
  guard's rules, or leave its cgroup. Submit refuses an `exec` task with no
  `user`, or `user = "root"` / `"0"`, and a `container` that opts out of the
  privilege floor (`security.userNamespace = false` or
  `security.seccomp = "unconfined"`):

  ```text
  task "agent" runs as root, and tasks in namespace legion-system may not: they
  run on the managers' system workers, where root could read the manager's state
  and leave its limits.
  Hint: set user = "nobody" (or another unprivileged user) in the task's config
  ```

  Nor may any task there have the worker act for it on the node, whatever
  its runtime and user, and whoever submits it: an `exec` health check (the
  worker runs its command), a `file://` or `git::file://` artifact or image
  (the worker copies node files in), or an artifact with `auth.nodeIdentity`
  (the worker presents the node's credential). On a manager the worker is
  root, so each would make the task root there:

  ```text
  task "agent" declares exec health check "probe", which the worker runs on
  the host, and tasks in namespace legion-system may not: they run on the
  managers' system workers, where the worker that would do it is root.
  Hint: use an `http` or `tcp` health check, and fetch artifacts from a URL
  ```

  `legion job validate` applies the same check offline when the jobspec
  names its namespace (`namespace = "legion-system"`); the manager applies
  it on every submit. Apps' tasks also run as each app's own uid
  ([Apps: What Legion renders](apps.md#what-legion-renders)).

## Nodes that accept only some namespaces

A worker type can reserve its nodes for some namespaces:

```nickel
# configs/workers/ci.ncl
{ workerType = "ci", pool = "ci", namespaces = ["ci-*"] }
```

A node whose worker type sets `namespaces` runs only jobs from a namespace
one of its patterns matches. It is a restriction of the node, not a
toleration a job writes: a job in `payments` stays off a `ci` node even with
`permit = { pool = "ci" }`, and nothing in a jobspec can change that. Pools
still apply as well, so a `ci-main` job lands on these nodes only if its
permit names the `ci` pool. Without `namespaces` (the default), a node
accepts every namespace, reserved ones included.

- **Names or `*`-globs**, following the [reserved rule](#reserved-namespaces):
  `["*"]` accepts every tenant namespace but not `legion-system`. An empty
  list is refused when the manager loads the worker type; leave the field out
  to accept everything.
- **Where it is checked.** In the scheduler's candidate filter, beside the
  permit, so a plan never offers a refused node; again in the apply
  transaction that commits the plan; and in the blocked-eval wake pass, so a
  node that accepts only other namespaces joining a job's pool never wakes
  that job ([scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)).
- **Diagnostics.** When the restriction is why a task cannot place, the
  failure (in `legion job plan`, and in `legion job info`'s blocked reason)
  names it:

  ```text
  Failed to place task web: no available nodes matching constraints, permit, or
  resources; 3 nodes accept only namespaces [legion-system]. ...
  ```
- **On the node.** The worker records the list as its `namespaces` config
  attribute, which `legion node info` shows. `namespaces` (and the
  `capacity` and `legion.role` attributes Legion sets on system workers) may
  not be used as a worker type `attributes` key.

## ACLs

A namespace is one selector on an ACL rule; there is no second policy
language. Resources split into two kinds:

| Namespaced (a scope carries a namespace) | Cluster-scoped (a scope never does) |
| --- | --- |
| `job`, `exec`, `action`, `scale`, `boot`, `snapshot`, `namespace` | `node`, `kv`, `state`, `operator`, `acl`, `template`, `net` |

**An allow rule over a namespaced resource must say which namespaces it
covers, and a rule over only cluster-scoped resources must not.** Both are
checked when the policy is written:

```text
rule 0 (job) must say which namespaces it covers: add `namespaces = ["default"]`
for one namespace, or `["*"]` for every namespace, including ones created later

rule 2 (node) has a `namespaces` selector, but it is cluster-scoped, so the rule
could never match; remove the selector

rule 1 mixes namespaced resources (job) with cluster-scoped ones (node); split it
into two rules, one with a `namespaces` selector and one without
```

`resources = ["*"]` rules and deny rules are exempt from the first check:
a `*` rule is explicit about its reach already, and a deny can only narrow.
"Every namespace, including future ones" is always something an admin
wrote (`["*"]`), never something they forgot. It does not include the
reserved `legion-*` namespaces, which only a pattern starting `legion-`
reaches ([Reserved namespaces](#reserved-namespaces)).

A rule naming an exact namespace that does not exist yet is accepted with a
warning (`rule 0 names namespace 'payments-prdo', which does not exist`),
because policies are often written before the namespaces they name. Globs
never warn. Include the delimiter in a glob (`payments-*`, not `payments*`):
a prefix without one also matches `paymentsx`.

```nickel
# The payments team's whole policy.
[
  { resources = ["job", "exec", "action", "scale", "snapshot"], actions = ["read", "write"],
    namespaces = ["payments-*"] },
  # Delegated creation: payments may create and delete payments-* namespaces.
  { resources = ["namespace"], actions = ["read", "write"], namespaces = ["payments-*"] },
]
```

Deny rules compose with globs, so "everything except the platform's
namespaces" can be written (`*` already leaves out `legion-*`):

```nickel
[
  { resources = ["job"], actions = ["read"], namespaces = ["*"] },
  { resources = ["job"], actions = ["read"], namespaces = ["platform-*"],
    effect = "deny" },
]
```

The `namespace` resource governs namespace objects themselves: `read` gets
one, `write` creates, updates and deletes those whose names match. It is
its own resource so that "may create a namespace" never implies "may
reconfigure the cluster".

**Seeing namespaces.** `ListNamespaces` returns a namespace when the caller
holds any allow in it: `namespace:read`, or any read or write on a
namespaced resource there. `WhoAmI` reports the same set as
`readable_namespaces`, which is how the CLI and UI choose defaults and
hints without probing. Every list RPC filters per item, and `*` only widens
the candidate set: it never skips the filter.

The `bands` selector composes with `namespaces`, so "only payments may
claim `high`" is one rule. A volume no job owns (a legacy row) belongs to no
namespace: it is visible and deletable only through a job rule with
`namespaces = ["*"]`.

## Services and the mesh

A service lives in its job's namespace, and the catalog's identity is
`(namespace, service)`. Two namespaces may each advertise `web`; neither
sees the other's endpoints. `ListServices`, `ResolveService` and
`ListServiceEdges` filter per item by `job:read` in the service's namespace.
`ListServices` takes `namespaces` (empty means `default`, `["*"]` every
namespace you can read), and every `ServiceSummary` and `ServiceEndpoint`
carries its `namespace`. A service reference on the API is canonical like a
job id: `api` in `default`, `payments-prod/api` elsewhere.

**Upstreams resolve in the consumer's own namespace.** `{ to = "redis" }`
means the `redis` of the namespace the task runs in, and the short DNS
form `redis.svc.legion` keeps meaning that. A bare name never reaches
across.

**Crossing a namespace is written on both sides.** The consumer names the
namespace it reaches into, and the target admits the consumer:

```nickel
# in checkout-prod: the consumer names where it is reaching
connect = { upstreams = [
  { to = "redis" },                                    # own namespace
  { to = "auth", namespace = "shared" },               # explicit crossing, L4
  { to = "http://billing", namespace = "shared" },     # explicit crossing, HTTP
] },

# in shared: the target admits the consumer
service = { name = "auth", port = 8443, allow = ["gateway", "checkout-prod/api", "payments-*/api"] },
```

`allow` entries are job globs:

| `allow` entry | Admits |
| --- | --- |
| absent, `[]`, or `"*"` | every job in the service's **own** namespace |
| `"gateway"` | the job `gateway` in the service's own namespace |
| `"checkout-prod/api"` | `api` in `checkout-prod` |
| `"payments-*/api"` | `api` in every `payments-*` namespace |
| `"*/*"` | every job in every namespace |

A crossing upstream is keyed `<service>.<namespace>`. That key names its
environment variables (`LEGION_UPSTREAM_AUTH_SHARED`,
`LEGION_UPSTREAM_PORT_AUTH_SHARED`, `LEGION_UPSTREAM_HOST_AUTH_SHARED`) and
its DNS name, **`auth.shared.svc.legion`**, so the crossing is visible in
the config that uses it. The node resolver also answers the SRV form
`_auth._tcp.shared.svc.legion`. An upstream with a `name` alias keeps its
alias as the key. A namespace name is a DNS label, and Legion answers only
under `svc.legion`, so a namespace called `com` shadows nothing.

Validation refuses an upstream whose `namespace` is not a namespace name, or
that sets `namespace` on an external destination (`https://…`, `tcp://…`).
A crossing is always the `namespace` field: `to` is a service name or a URL,
so a `shared/auth` form would read as a URL path under `http://`.

Without the mesh, `allow` controls what the catalog offers a consumer; on a
`mesh = true` service the receiving node also applies it to the verified
caller's job ([The mesh](network-handlers.md#the-mesh)). Beyond that,
enforcement on the wire is `network.mode = "gated"` plus net policies. Net policies are cluster objects whose selector can name
`namespaces`; their selector facts carry each task's real namespace, and a
`jobs` glob matches the job's bare name.

`ListServiceEdges` edges carry `target`, the canonical service the caller's
upstream key resolves to; its `service` filter matches `target`, the key,
or the calling job.

## Instances

A caller-chosen instance key is stored qualified by its job's namespace,
exactly as job ids are: `user-42` in `default`, `payments-prod/user-42` in
`payments-prod`. Keys, and the tombstones that keep a destroyed key from
being reused, are therefore **unique per namespace**: two tenants may each
hold `user-42`, and one cannot squat on or probe for the other's.

- `CreateInstance.instance_id` (and `CreateInstances.instance_ids`,
  `stateful.initialInstances`, a promotion's `new_id`) are keys; the reply
  returns the canonical id, which every other instance RPC takes.
- **Server-minted ids** (`<task>-<26 base32>`, and node-tagged session ids)
  are cluster-unique by construction and never qualified.
- Slot instance ids embed the job id (`payments-prod/web-frontend-0`).
- The CLI and MCP resolve instance arguments like job arguments: `legion
  instance get user-42 -n payments-prod`, or `payments-prod/user-42`. A
  minted id is used as written. Beside a qualified `--job payments-prod/web`,
  a bare key resolves in that job's namespace.
- A boot ticket's `instance_id` or `instance_prefix` may be written as a key;
  the manager qualifies it in the job's namespace, and the ticket carries the
  canonical form a node compares `StartClaim` against.
- Tasks see `LEGION_INSTANCE_ID` (canonical) and `LEGION_INSTANCE_KEY` (the
  key alone).

## Workload identity

The default subject is `{region}:{namespace}:{job}:{group}:{task}`, which
renders as `global:payments-prod:web:frontend:nginx`; `{job}` is the bare
name. `{namespace}` is part of the uniqueness floor: a `subTemplate`, an
`allowedSubTemplates` entry, or a per-identity `subTemplate` that drops it
is rejected at config load or submit, as a dropped `{job}` is, because two
namespaces' `web` would otherwise mint identical subjects for different
tenants. Tokens carry a `legion.io/namespace` claim, which control-plane
binding rules can match. See [Workload identity](workload-identity.md).

## Events

Every event about a job carries `namespace`; cluster events (nodes, raft,
the scheduler) carry none. Lifecycle events `namespace.applied` and
`namespace.deleted` record namespace writes.

- **Operators** (`operator:read`) see every event, as before.
- **Tenants** with `job:read` in some namespaces read those namespaces'
  events through the same `ListEvents` and `StreamEvents` RPCs, filtered per
  item. A stream re-checks the caller's grants on its interval, so a revoked
  grant stops it.
- Both RPCs take `namespaces` (names or globs). Unlike job lists, an empty
  selection means every event the caller may see, cluster events included;
  any selection keeps only events about those namespaces' jobs.

```console
$ legion events follow -n 'payments-*'
```

## Metrics

| Family | Labels | Meaning |
| --- | --- | --- |
| `legion_namespace_jobs` | `namespace` | registered jobs |
| `legion_namespace_allocations` | `namespace` | live allocations (not `complete` or `failed`) |
| `legion_namespace_reserved_cpu_mhz` | `namespace` | CPU reserved by live allocations |
| `legion_namespace_reserved_memory_bytes` | `namespace` | memory reserved by live allocations |
| `legion_job_submits_total` | `namespace` | submissions accepted (deduplicated retries excluded) |
| `legion_preemptions_total` | `pool`, `band`, `namespace` | allocations evicted by preemption |

The gauges refresh on the node-status tick and list every existing
namespace, so an emptied namespace reads 0. Like every family, each caps at
256 label combinations and folds the rest into `"other"`, so past 256
namespaces the per-namespace gauges stop being exact. See
[Metrics](metrics.md#namespaces).

## Exec keys

A delegated exec key's `jobs` pattern matches canonical job ids:
`agents-*` matches jobs in `default` only; `agents/*` or
`agents-*/web*` is `namespace-glob/name-glob`. `*` alone never crosses a
namespace. A pattern with more than one `/` is refused.

## Namespace policies

A namespace policy is a cluster object that selects namespaces by glob and
governs the jobs in them. It is apart from the namespace object on
purpose:

1. **Delegated creation would dodge a per-namespace quota.** If the
   payments lead may create `payments-*`, a quota on `payments-prod` is
   escaped by creating `payments-prod2`. A policy's quota is an aggregate
   over every namespace it matches, so `payments-loadtest` lands inside
   payments' totals the moment it exists.
2. **The namespace writer would be the quota writer.** Policies are written
   with `acl:write` and read with `acl:read`. Whoever may write ACL policy
   could grant anything anyway, so governance at that privilege adds no
   escalation path. A tenant sees the limits that bind it through `legion
   namespace get`, which needs only `namespace:read`.

```nickel
# legion namespace policy apply -f payments.ncl
{
  name = "payments",
  description = "Payments: every environment, together",
  namespaces = ["payments", "payments-*"],
  # Aggregate over EVERY matching namespace, per priority band.
  # A band not listed is uncapped.
  quota = {
    high   = { cpu = 64000,  memory = "128Gi" },
    normal = { cpu = 200000, memory = "512Gi", cores = 32 },
  },
  maxJobs = 300,
  maxNamespaces = 8,                                    # caps delegated creation
  pools = { allowed = ["shared-*", "payments-gpu"], default = "shared-general" },
  runtimes = ["container", "sandbox"],
  hostVolumes = ["payments-*"],
}
```

| Field | Meaning |
| --- | --- |
| `name` | Unique: letters, digits, `-`, `_`, `.` |
| `namespaces` | Required, non-empty: namespace names or `*`-globs. A glob reaches a reserved `legion-*` namespace only when it starts with `legion-` |
| `quota` | Band (`critical` … `background`) → `{ cpu (MHz), memory (MiB, or a size like "64Gi"), cores }`; each dimension optional |
| `maxJobs` | Jobs (any state) across every matching namespace |
| `maxNamespaces` | Namespaces matching the policy |
| `pools.allowed` | Pool names or globs every task must run in |
| `pools.default` | The pool a task whose permit names none lands in, or a list of pools it may land in (`["legion-system", "default"]`, rendered as a `pool matches ^(…)$` permit). Each must be within `allowed`, and a pool name, not a glob |
| `runtimes` | Allowed task runtimes, names or globs |
| `hostVolumes` | Allowed host volumes (`driver = "host"`, by `params.name`), names or globs |
| `autoscale` | Autoscaling defaults for every task in the matching namespaces: the jobspec `autoscale` record without `checks`, overriding the cluster's scaling defaults field by field, and live ([autoscaling.md](autoscaling.md#overriding-the-defaults)). Its `min` must be a multiple of `count` of at least `"1x"` |

For `pools.allowed`, `runtimes` and `hostVolumes`, **unset allows
everything and an empty list allows nothing.**

**Several policies may match one namespace, and all of them apply.** Each
quota and count is checked independently against its own aggregate;
allowlists intersect, so a second policy can only narrow. There is no
precedence to learn. Two policies that set different `pools.default` for
the same namespace make a task without a pool unadmittable, and the error
says so.

### When the checks run

Checks run at admission, never in placement, and never evict or re-judge
work that was already admitted:

| Moment | Checks |
| --- | --- |
| `job run`, `job plan`, `RunJobTemplate` | runtimes, pools, host volumes, quota, `maxJobs` |
| `job scale` out | quota (scale-in is never refused) |
| `instance create` (keyed, named sessions) | quota, for the new instances |
| `namespace create` | `maxNamespaces` |

Runtimes, pools and host volumes need only the jobspec. A policy's default
pool is written into the stored config (a `pool = "…"` permit), so the
stored spec says where the task runs. A task whose permit uses another
operator for `pool` (`!=`, `matches`) under a policy that restricts pools is
refused, since admission cannot prove where it lands: name the pool with
`pool = "…"`.

Quota and `maxJobs` run inside the replicated write that stores the job, so
two racing submits serialize, and the second sees the first.

```console
$ legion job run load.ncl -n payments-loadtest
error: namespace policy 'payments' caps band 'high' at 64000 MHz CPU across payments, payments-*;
       in use 61500 MHz CPU, this job adds 4000 MHz CPU
hint: `legion namespace get payments-loadtest` shows usage; lower the band or the count
```

**A job never loses ground it holds.** A resubmit whose charge does not
grow in a dimension is admitted in that dimension even when the namespace is
over the cap (say, after the cap was lowered), so turning a quota on or
down never breaks a job that was already admitted. An unchanged resubmit
is not re-checked at all.

### How quota is charged

Per band, a job is charged the larger of what it **declares** and what it
**runs**, dimension by dimension:

- **Declared**: for each count-driven task of its current config, the
  effective count (a `job scale` override included) times its reservation
  (`resources`, after `taskResourceDefaults`, so every task has one). A
  stopped job declares nothing.
- **Live**: the reservations of its allocations not yet `complete` or
  `failed`, the same set the scheduler counts against node capacity.

Declared demand is what closes the race: a job is charged from the moment
it is admitted, not from the moment it places. Live allocations charge what
the spec cannot say: keyed instances, per-node (`count.per`) tasks, and the
old version still running during a rollout. Quota never forces a jobspec
change.

`legion namespace get` shows the namespace's charged usage per band and, for
each governing policy, the aggregate over every namespace it matches (which
may include namespaces you cannot read; only totals are shown):

```console
$ legion namespace get payments-loadtest
payments-loadtest
  2 job(s), 0 volume(s), 0 snapshot(s); created 2026-10-19T10:02:11Z

Charged usage:
BAND    CPU (MHz)  MEMORY (MiB)  CORES
high    4000       2048          0

Namespace policies governing it (usage across every namespace they match):
  payments (namespaces payments, payments-*)
    quota high cpu: 61500 / 64000 MHz
    quota high memory: 90112 / 131072 MiB
    jobs: 211 / 300
    namespaces: 5 / 8
    runtimes: [container, sandbox]
```

The `legion_namespace_reserved_*` gauges show live reservations only, so
they can read lower than the charged usage while admitted work is still
placing.

### Previewing a policy

`--dry-run` validates the policy and lists what it would have refused among
existing jobs and counts, had it been in force: jobs on a disallowed
runtime, pool or host volume, bands over a cap, and counts over `maxJobs` or
`maxNamespaces`. Nothing is written, and a stored policy never evicts these
either; it refuses what comes next.

```console
$ legion namespace policy apply -f payments.ncl --dry-run
Would update namespace policy payments (namespaces payments, payments-*).
Had it been in force, it would have refused (admitted work keeps running):
  [runtimes] payments-prod/batch: namespace policy 'payments' allows runtimes [container, sandbox] in payments, payments-*; task main uses exec
  [maxJobs] payments, payments-*: 312 jobs across payments, payments-*, over maxJobs 300
```

An `autoscale` record is checked against every autoscaled task it would
govern: a change that would leave some task's effective policy invalid (two
policies setting one field differently, a floor above a task's ceiling) is
listed under `[autoscale]` by `--dry-run`, and refused by a plain apply.
`--force` applies it anyway, and those tasks hold with state `invalid` until
fixed ([autoscaling.md](autoscaling.md#overriding-the-defaults)).

```console
$ legion namespace policy list
$ legion namespace policy get payments      # limits, aggregate and per-namespace usage
$ legion namespace policy delete payments
```

## The UI

- **Detail routes carry the namespace as a path segment**:
  `/jobs/payments-prod/web`, `/jobs/default/web/tasks/main`,
  `/services/payments-prod/api`. A pasted link always opens the object it
  names, and navigation cannot drop the namespace.
- **List pages carry the selection as `?ns=`** (`/jobs?ns=payments-*`,
  `ns=*` for every namespace you can read). Jobs, Services, Rollouts,
  Volumes and Events follow it, and a `Namespace` column appears when the
  selection spans several.
- **The switcher sits in the page header.** "All namespaces" is a real
  selection and the default for an identity that reads several. On a job
  page the switcher shows the job's namespace, and choosing another opens
  that namespace's list. Cluster-scoped pages (Workers, Managers, Raft, KV,
  State Explorer, Observability) have no switcher and say they are
  cluster-scoped. **An identity that can read one namespace sees none of
  this**, only that namespace as a badge.
- **Job headers show the namespace**, linking to its list.
- **Governance → Namespaces** lists namespaces with what each holds; a
  namespace's page shows its contents, charged usage, governing policies
  with each limit against its aggregate, and (with `acl:read`) the ACL rules
  that reach it.
- Launching a template offers a target namespace to identities that read
  several.

## Content-store layout

Objects a node writes to a `contentStore` spell the namespace out as a path
segment, `default` included, so one job's prefix never covers another
namespace's objects:

```text
<contentStore.url>/manifests/<namespace>/<job>/<group>/<task>/<instance key>.json
<contentStore.url>/sessions/<namespace>/<job>/<xx>/<session id>.json
<contentStore.url>/sessions/_fence/<node>/<session id>
```

The fence segment starts with `_`, which no namespace name can, so no
namespace's manifests can land under a fence prefix.

## Failure modes and diagnostics

| Symptom | Cause | Fix |
| --- | --- | --- |
| `job 'web' not found in namespace 'default'` | the job is in another namespace | the hint names it when you can read it; use `-n` or `ns/web` |
| `jobspec declares namespace 'prod' but the request names 'dev'` | `-n` disagrees with the spec | drop one |
| `namespace 'x' does not exist` | submitted into a namespace nobody created | `legion namespace create x` |
| `rule 0 (job) must say which namespaces it covers` | an ACL rule over a namespaced resource without `namespaces` | add `namespaces = ["default"]` or `["*"]` |
| a `ns/web` upstream never gets endpoints | the target's `allow` does not admit the consumer | add `"<consumer-ns>/<job>"` to the service's `allow` |
| `auth.shared.svc.legion` does not resolve | the upstream does not name `namespace = "shared"`, or uses an alias | the DNS name is the upstream's key |
| `namespace policy 'p' caps band …` | over an aggregate quota | `legion namespace get <ns>`; lower the count or band, or raise the quota |
| `… must name its pool exactly` | a policy restricts pools and the permit uses `!=`/`matches` | write `pool = "<name>"` |
| `namespace 'x' is not empty: 3 jobs` | delete with contents | `--dry-run` to see them, `--cascade` to remove them |
| `namespace "legion-x" uses the reserved prefix "legion-"` | creating a `legion-*` namespace | pick another name; only Legion creates them |
| `namespace 'legion-system' is reserved for Legion and cannot be deleted` | deleting a reserved namespace | expected |
| permission denied in `legion-system` with a `namespaces = ["*"]` token | `*` does not reach reserved namespaces | a rule with `namespaces = ["legion-system"]` (or `["*", "legion-*"]`) |
| `task "x" runs as root, and tasks in namespace legion-system may not` | an exec task with no `user` (or root) in a reserved namespace | `config.user = "nobody"` |
| `task "x" sets security.userNamespace = false, … may not run privileged` | a container opting out of the privilege floor in a reserved namespace | remove the setting |
| `N nodes accept only namespaces [ci-*]` in a placement failure | nodes the job's permit allows reserve themselves for other namespaces | run the job in an accepted namespace, or widen the worker type's `namespaces` |
| a tenant's event stream stops | its grant was revoked | expected: streams re-check grants |

## Known limitations

- **Not an isolation boundary**, as the table at the top says. Cluster
  views (`node:read`'s ledger, the state explorer, `operator:read` events)
  show every namespace; do not grant them to tenants.
- **Cluster-scoped objects stay cluster-scoped**: KV, saved queries, net
  policies, job templates, exec keys and node pools. A template can be run
  into any namespace the caller may submit to; its collision guard is
  namespace-local.
- **Tenant-authored net policies are not supported.** Writing a net policy
  needs a cluster-wide `net:write`; proving that a selector's namespace globs
  are covered by the writer's would be glob subsumption.
- **Unnamed sessions are not charged against quota** and are not checked at
  creation: they write nothing to the replicated store until promoted.
  Named sessions and every other instance are.
- **Quota admission scans** the cluster's jobs and live allocations for each
  submit into a governed namespace, inside the control-lane write. That is
  cheap at hundreds of jobs and grows linearly; namespaces no policy governs
  pay nothing.
- **The `job scale` and instance-create checks are pre-checks**, not inside
  the write, so two racing scale-outs can both pass. Submits are exact.
- **Per-namespace metric gauges fold past 256 namespaces** into `"other"`.
- **Rollout, volume and snapshot replies** carry the canonical `job_id`
  rather than a separate `namespace` field; split it at the first `/`.
- Namespaces cannot be renamed or nested.
- **Node `namespaces` lists are pinned at registration.** The manager takes
  a node's `namespaces` (and its `legion.role`) from what admits the
  registration, whatever the worker reports: the join token's worker type,
  the settings a manager pinned on its own system worker's token, or, when a
  node re-registers with its node token, what it first registered with. A
  worker that forges its registration can neither drop its restriction nor
  claim to be a manager's. With ACLs off, or a management token, nothing
  about the caller is proven, so the reported values stand.
- **There is no soft form** of the node restriction (no "prefer not"); a
  node accepts a namespace or it does not.
- **List selection is not ACL.** `-n '*'` on a list command selects every
  namespace, `legion-system` included; what the caller may read still
  filters the items.
- **`legion-system` and its policy are created once**, on an empty store. A
  deleted `legion-system` policy is not recreated.

## See also

- [Auth and ACLs](auth-and-acls.md) — the rule model and the `namespaces`
  selector
- [Service discovery](service-discovery.md) and [Network
  handlers](network-handlers.md) — upstreams, `allow`, and net policies
- [Workload identity](workload-identity.md)
- [Event stream](event-stream.md) and [Metrics](metrics.md)
- [Agent readiness](agent-readiness.md) — namespaces for agents
