# Legion Apps: system jobs on managers, granted identities, and UI extensions

> **Judgement calls:** [legion-apps-plan_calls.md](../calls/legion-apps-plan_calls.md)

**Status: PLANNED** (written 2026-10-19; jobspec extensions and strict
unknown fields added 2026-10-27). Nothing is built. It has six phases (§8),
and each ships alone. The first consumer is the autoscaler
([autoscaler-v2-plan.md](autoscaler-v2-plan.md)). GitOps is the second, and
this plan checks the substrate against it (§6.8). Deliberately deferred:
installing apps from a registry (it waits on the registry plan), apps owned
by tenants in their own namespaces, and in-page JavaScript extensions that
share the operator UI's origin. That last one is rejected, not deferred
(calls entry 8).

This plan replaces the ecosystem-app approach in the superseded
[autoscaler-plan.md](autoscaler-plan.md) §6.6 and §6.8: the in-core loop
with a job as the escape hatch, and the link-only Apps sidebar.

---

## 1. Problem statement

`internal_docs/differentiators.md` and the TODO's "Ecosystem apps" section
promise an omakase cluster: an autoscaler, GitOps, a cert manager, and an
ingress, all simple on day one and splittable on day N. Each is a program
that **manages the cluster** rather than serving users. Today every one of
them hits the same six walls:

1. **No reliable place to run.**
   - Jobs land on workers. A workerless cluster has none, a full cluster has
     no room, and a node the autoscaler is draining could be the one hosting
     the autoscaler.
   - `legion start manager --run-worker` exists (`src/cmd/start.rs:18-23`),
     but it turns the manager into an ordinary `default`-type worker in the
     `default` pool (worker type hard-coded at
     `src/manager/daemon.rs:783-802`). Nothing marks the node as a manager,
     and tenant work lands on it freely.
2. **No isolation for that place.**
   - Pools act as taints (`src/constraints.rs:114-175`), but the toleration
     is the job's own `permit`, so any job author can opt in.
   - Namespace policies are allowlists that only intersect
     (`src/structs/namespace_policy.rs:342-380`), so "only `legion-system`
     may use pool X" means restricting every other namespace glob.
   - The `legion-` namespace prefix is reserved
     (`src/structs/namespace.rs:34-36`), but nothing ever creates a
     `legion-*` namespace.
3. **Talking to Legion takes two-sided wiring.**
   - The worked example, `configs/jobs/cert-renewer.ncl`, needs four
     things: an `identities` entry, a `controlPlaneBindings` rule in
     *manager config* (cloned at startup, `daemon.rs:344`, so changing it
     needs a restart), a separately applied ACL policy, and a hard-coded
     `LEGION_ADDR`.
   - The binding matches `legion.io/job`, which is the *bare* job name, so
     a job of that name in any namespace gets the policies
     (`docs/workload-identity.md:39`).
   - The kernel host guard drops a task's connections to Legion's ports on
     the node's *local* addresses (`docs/network-handlers.md:1297`). A job on
     a manager's own node therefore can't reach that manager. It can reach
     other managers only if it has a route to them, and a single-manager
     cluster has no other manager.
4. **No lifecycle for first-party add-ons.** Nothing in `src/` submits a
   job on the manager's behalf, upgrades it with the binary, or reports its
   health as part of the cluster's.
5. **No way to appear in the UI.**
   - Routes and nav are hard-coded in `ui/src/App.tsx:265-336,1165-1198`.
   - The manager doesn't serve the SPA. The operator's token lives in
     `localStorage['legion-acl-token']` (`ui/src/grpcClient.ts:8-30`).
   - The planned reverse proxy deliberately uses a separate origin and
     passes no user identity
     ([manager-reverse-proxy-plan.md](manager-reverse-proxy-plan.md) §4,
     §6.4).
   - Every add-on would therefore need hand-written SPA pages in Legion's
     own repo, or a link out to a port the browser usually can't reach.
6. **No way to give an app per-job settings.**
   - A backup app needs to know which task to back up and how often. A DNS
     app needs the hostname a job wants. The jobspec is where per-job intent
     lives, but its schema belongs to core, so there is nowhere to put an
     app's settings except a field core already knows.
   - Worse, the parsers ignore keys they don't know. `TaskConfigInput`
     (`src/structs/job.rs:2404-2406`) has no `deny_unknown_fields`. So a
     typo like `autoscael = true`, or `autoscale = true` sent to a manager
     that predates it, is accepted and silently dropped.

## 2. Prior art

### Where cluster add-ons run

- **Kubernetes.** Control-plane nodes carry the
  `node-role.kubernetes.io/control-plane:NoSchedule` taint, and add-ons
  tolerate it. `kube-system` holds them. The addon-manager reconciles
  manifests from disk and reverts drift. The cluster-autoscaler commonly runs
  on control-plane nodes precisely so that it never runs on a node it might
  remove. *Take:* a reserved namespace, managers that accept only it, and a
  reconciler that owns bundled apps.
- **Nomad.** `system` jobs run one per node. Node pools let a namespace set
  its default pool, and Enterprise can restrict which pools a namespace may
  use. The Nomad Autoscaler is just a job on clients. *Take:* "it's just a
  job". *Avoid:* placing it where its own decisions can evict it.

### How add-ons get credentials

- **Kubernetes ServiceAccounts.** The token, `ca.crt`, and `namespace` are
  mounted automatically, and `KUBERNETES_SERVICE_HOST` is injected, so every
  client library's in-cluster config needs zero setup. RBAC **escalation
  prevention**: you can only create a role binding granting permissions you
  hold yourself. *Take* both.
- **Nomad Task API** (1.5+). Each task gets a unix socket in its secrets
  directory, proxied by the client agent and authenticated as the task's
  workload identity. Tasks reach the API with no token handling and no
  network path to servers. *Take*: it is the only design that also clears
  Legion's host guard (§6.4).
- **Nomad workload-associated ACL policies.**
  `nomad acl policy apply -namespace ns -job web` attaches a policy to a
  job by name. That is one-sided, but it is operator-applied and outside
  the jobspec. *Improve:* the job requests permissions, and the submitter
  is checked.
- **GitHub Apps and OLM.** A manifest declares the permissions, and the
  installer reviews and approves them at install time; an upgrade that asks
  for more needs re-approval. *Take:* the install prompt and the permission
  diff on upgrade.

### How add-ons attach per-job settings

- **Kubernetes** gives apps two tools.
  - *Annotations* are a free-form string map with a prefix convention
    (`cluster-autoscaler.kubernetes.io/safe-to-evict`). There is no
    ceremony, nothing is validated, and a typo is silently inert.
  - *CustomResourceDefinitions* carry an OpenAPI schema that the API server
    enforces, plus admission webhooks for semantic checks. A webhook with
    `failurePolicy: Fail` that goes down blocks every write it covers, a
    lesson most operator teams learn once. CRDs are separate objects, so
    per-workload settings live apart from the workload (an HPA next to its
    Deployment).
  - *Take:* a schema enforced by the core server, and a semantic hook that
    fails open. *Reject:* free-form annotations as the main mechanism.
- **Nomad** has `meta` string maps on job, group, and task. Tools read
  conventions from them, and nothing validates them. **Docker labels** and
  **Consul tags** (Traefik's `traefik.http.routers…`) are the same idea with
  the same failure: stringly typed and unvalidated.
- **Terraform providers** publish a schema per resource, and core validates
  every block against it before the provider sees it. *Take:* the plugin
  declares, and core enforces.
- **Legion itself.** Task runtimes publish a JSON Schema for their `config`
  block. Submit validates it with the `jsonschema` crate
  (`src/task_plugins/mod.rs:2134`), and `legion job schema` merges the live
  schemas so agents author against them (`docs/agent-readiness.md` §4).
  *Take:* the same shape for apps.

### How add-ons get into the UI

- **Slack Block Kit.** Apps return JSON blocks that Slack renders natively:
  no app code runs in the client, the theme is consistent, and it works on
  every client. *Take*: this is tier 1 (§6.5). It also renders in a
  terminal and for an agent.
- **Shopify App Bridge, Docker Desktop extensions.** The app UI runs in an
  iframe and talks to the host through a postMessage bridge. Shopify mints a
  per-user **session token** (a short JWT) that the app's backend verifies.
  *Take*: this is tier 2, with the bridge calling the API *as the viewer*.
- **Argo CD extensions.** UI extensions are trusted JavaScript bundles.
  *Proxy extensions* forward requests to an extension backend with
  `Argocd-Username` and `Argocd-User-Groups` headers, after RBAC checks per
  extension. *Take:* the manager authorizes first, then forwards with a
  verifiable viewer assertion. *Reject:* trusted in-origin JavaScript.
- **Headlamp, OpenShift console dynamic plugins, Grafana app plugins.**
  These are JavaScript plugins loaded into the console's own origin.
  Admins install them, and they are fully trusted. Grafana added plugin
  signing because that trust is the problem. *Reject* for Legion: the
  operator's bearer token sits in `localStorage`, so any in-origin plugin
  holds it.

## 3. Jobs to be done

**Cluster operator**
- When I start a cluster, I want the built-in add-ons (the autoscaler first)
  running somewhere safe without doing anything, so the omakase promise
  holds on day one.
- When I run dedicated managers, I want add-ons to use a small, capped share
  of them and nothing more, so a misbehaving add-on can't starve raft.
- When I'd rather keep managers clean, I want to move an add-on to workers
  with one command, so placement is my call and not a rebuild.
- When I install a third-party add-on, I want to see exactly what it may do
  before it runs, so I'm not handing out admin by accident.

**Job author** (anyone writing a job that talks to Legion)
- When my job needs to read KV or scale itself, I want to say what it needs
  in the jobspec and have it just work, so I don't file a ticket for a
  manager-config change and a restart.
- When my code calls Legion, I want the CLI and client libraries inside the
  task to already be logged in and pointed at the cluster, so there is no
  token plumbing in my code.
- When I mistype a field, I want the deploy to fail and say so, so a setting
  I thought I'd turned on isn't silently ignored.
- When I deploy a job before the add-on it configures is installed, or to a
  cluster that's mid-upgrade, I want to push it anyway on purpose, so being
  strict doesn't block a migration.

**App author** (someone writing an add-on: the autoscaler, GitOps, or a
third party)
- When I build an add-on, I want to ship one manifest that says how it runs,
  what it may do, and what it shows in the UI, so installing it is one
  command.
- When my add-on has state worth seeing, I want it to show up on the job and
  node pages where operators already look, without learning the SPA's
  framework or getting a PR into Legion.
- When my add-on needs settings per job, I want job authors to write them in
  the jobspec next to the job, checked at submit against a schema I publish,
  so a mistake fails at deploy time instead of at 3am.

**Security / platform admin**
- When someone grants a job permissions, I want that to be impossible beyond
  what they hold themselves, so permission to deploy never quietly becomes
  permission to hand out permissions.
- When an add-on shows UI to a viewer, I want it to act with *that viewer's*
  rights, so a read-only user can't press a button that runs with the
  add-on's power.
- When an add-on is installed, I want tenants unable to modify or exec into
  it, so its credential can't be borrowed.

**On-call responder**
- When something add-on-driven goes wrong, I want its status, logs, and
  recent actions one command away, so the add-on is not a black box.

**Autonomous agent**
- When I operate the cluster, I want every add-on view available as
  structured data through the CLI and MCP, so whatever a human sees on a
  panel, I can read too.
- When I write a jobspec, I want one schema that already includes the
  installed add-ons' settings, so I can write them correctly the first time.

### User scenarios

**A. Day one.** `legion start manager` on a laptop or on three dedicated
hosts. With nothing configured, each manager runs a **system worker** that
accepts only `legion-system` jobs, capped at 1 CPU and 1 GiB. The autoscaler
app comes up there. `legion app list` shows it running, and the UI sidebar
has an Autoscaling entry.

**B. A job that talks to Legion.** The cert renewer's 20 lines of setup
become one block:

```nickel
identities = [{
  name = "legion",
  permissions = [{ resources = ["kv"], actions = ["read", "write"], paths = ["certs/*"] }],
}],
```

Inside the task, `legion kv put certs/app.example.com …` just works, through
a local socket the worker provides. Nobody touched manager config.

**C. A third-party add-on.**

```console
$ legion app install https://example.com/gitops/0.2.0/app.ncl
gitops 0.2.0 runs its own code (1 artifact) on your managers, as user nobody,
in legion-system, and asks for:
  job   read, write   namespaces: team-*
  kv    read          paths: legion/apps/gitops/*
UI: a GitOps page, a Sync panel on job pages, one action (Sync now, needs job:write)
Your token holds all of these. Grant them and install? [y/N] y
installed gitops 0.2.0; job legion-system/gitops starting
```

A "Sync" panel appears on every job page that GitOps manages, rendered
natively by the SPA from JSON the app returns.

## 4. Goals and non-goals

**Goals**

1. An **app** is an ordinary job plus a manifest. There is no new job type
   and no new runtime.
2. Managers host apps by default, through a capped, namespace-restricted
   in-process worker. Moving an app to workers takes one command.
3. A job's permissions are declared in its jobspec and granted at submit
   only up to what the submitter holds. Apps use the same mechanism.
4. Talking to Legion from inside a task needs zero configuration, and works
   through the host guard and gated networking.
5. Apps register UI that the SPA renders natively (tier 1), or that runs
   sandboxed (tier 2). Either way it acts as the viewer, and the same views
   are available to the CLI and MCP.
6. Bundled apps upgrade with the binary and report health as part of the
   cluster's.
7. Apps add per-job settings to the jobspec as namespaced, schema-validated
   blocks. Unknown fields stop being silently dropped, and a flag allows
   them deliberately.

**Non-goals**

- **A registry.** `legion app install` takes a file or a URL. The registry
  plan adds names.
- **Tenant-owned apps** (apps living in a team's namespace, with UI scoped to
  it). Apps are cluster add-ons in `legion-system`. The manifest has a
  `namespace` field reserved for this.
- **In-origin JavaScript plugins** (Headlamp style). Rejected, not deferred
  (calls entry 8).
- **Serving the SPA from the manager.** Tier 1 and tier 2 both work with the
  SPA wherever it is served today.
- **A general taints-and-tolerations system.** The node-side `namespaces`
  restriction is one targeted rule. Pools already act as taints.
- **App-defined object kinds** (a CRD equivalent: standalone objects an app
  defines, the way the autoscaler defines `ScalingPolicy` in core).
  Per-job blocks cover per-job settings. An app that needs its own kind of
  object waits for a second example (§11).

## 5. Background: what exists today

- **Manager as worker**: `--run-worker` mints a 15-minute join token and runs
  `run_worker_local` with `worker_type: "default"` and `in_process: true`
  (`src/manager/daemon.rs:496-530,783-802`). `in_process` only changes
  shutdown behavior. No attribute marks the node as a manager.
- **Pools and permits.** `PermitSet::new` adds an implicit `pool=default`
  (`src/constraints.rs:114-121`). `is_permitted` requires every node
  restriction to match some permit rule (`:123-175`). This is applied in
  `scan_base_eligible` (`src/manager/scheduler.rs:3165-3182`). Blocked evals
  are keyed by pool, and their wake pass re-runs the permit checks
  (`docs/scheduler-and-eval-loop.md` § Keys).
- **Namespaces** (shipped 2026-10-19).
  - `default` always exists. The `legion-` prefix is reserved at creation
    only (`namespace.rs:108-116`).
  - Namespaced ACL resources are `job, exec, action, scale, boot,
    snapshot, namespace`; their rules must name `namespaces`, and globs are
    allowed (`docs/namespaces.md:196-245`).
  - Tokens carry `legion.io/namespace`.
- **Bands.** `critical` is "for control-plane jobs" and is never preempted
  (`src/structs/band.rs:41-61`, `docs/preemption-and-eviction.md`).
- **Workload identity.** The reserved `legion` identity mints a JWT with
  the cluster issuer as `aud` (`src/structs/workload_identity.rs:28,238`).
  - The manager's built-in `legion` auth method maps it to principal
    `legion:<sub>` and resolves policies from `controlPlaneBindings` with
    `bound_policies` (`src/manager/auth_method.rs:555-660,820-846`).
  - The default TTL is 15m, refreshed at about half life
    (`src/worker/identity.rs:715`).
  - Env injected into tasks: `LEGION_ALLOC_ID`, `LEGION_JOB_ID`,
    `LEGION_JOB_NAME`, `LEGION_NAMESPACE`, `LEGION_TASK_NAME`, ports and
    upstreams (`src/task_plugins/mod.rs:127-151`). Not injected:
    `LEGION_ADDR`, CA, or token.
  - The CLI reads `LEGION_TOKEN`, then `LEGION_TOKEN_FILE`, then
    `~/.legion/tokens/…`, and the address from `LEGION_ADDR`
    (`src/cmd/helpers.rs:125,165-213`).
- **Host guard.** An nftables table drops allocation sockets to the
  metadata service, Legion's own ports when the manager shares the node,
  `127.42.0.0/16`, and other allocations' listeners. `hostGuard.allow` is
  the operator's escape hatch (`docs/network-handlers.md` § host guard).
- **ACL.** There are 14 resources (`src/structs/acl.rs:21-78`). Policies
  are applied with `legion acl policy apply`. No built-in policy documents
  ship, and management tokens bypass rules.
- **Job templates** are stored, versioned, and rendered server-side
  (`docs/job-templates.md`). Apps could, but need not, use them.
- **UI.**
  - A SolidJS SPA over gRPC-Web, using the capability-probe nav pattern for
    Observability (`ui/src/App.tsx:315-324`).
  - The manager answers browsers only from loopback plus
    `server.allowedOrigins` (`docs/ui-architecture.md:754-783`).
  - There is no extension mechanism of any kind.
- **Jobspec parsing.** `TaskConfigInput` and the job-level input
  deserialize without `deny_unknown_fields` (`src/structs/job.rs:2404-2406`).
  Unknown keys are dropped silently, except `kind`, which has a hand-written
  error (`:2507-2521`). Task-runtime `config` blocks are validated against
  the runtime's JSON Schema (`src/task_plugins/mod.rs:2134`), and
  `legion job schema` serves the merged schema.
- **In-place version moves.** A resubmit whose task configs are
  schedule-equivalent (`task_schedule_equivalent`, `src/storage/store.rs:21251`:
  count, gang, schedule, resources, constraints, permit, affinities,
  connect) can move running allocations onto the new version without a
  rollout (`migrate_allocations_to_job_config`). KV re-renders already use
  that path (`src/manager/server.rs:4150-4180`).
- **Worker-plane tunnel.** It is planned, not built:
  `Worker.ProxyTunnel` in the reverse-proxy plan (§6.2-6.6), which is the
  manager-to-allocation path a UI needs to reach an app's backend.

## 6. Proposed design

### 6.1 What an app is

An **app** is three things:

1. a **job** in the reserved `legion-system` namespace;
2. a **grant**: the permissions the job's `legion` identity holds;
3. **registrations**: UI slots, views, and actions (§6.5), and jobspec
   extensions (§6.6).

A **manifest** declares all three, and an **app record** on the Control lane
holds the installed manifest, the operator's settings, and the grant. The
manager's **app reconciler** keeps the job matching the record.

There is no special job type (calls entry 1). An app's allocations are
scheduled, restarted, rolled out, logged, exec'd into (by those allowed),
and metered exactly like any other job's. Everything special about apps is
added around the job, not inside the scheduler.

```nickel
# The bundled autoscaler's manifest (shipped inside the binary).
{
  name = "autoscaler",
  description = "Scales tasks and node groups from scaling policies.",
  version = "%{legion.version}",          # bundled apps track the binary
  placement = "managers",                 # managers (default) | workers | any
  replicas = 1,
  job = {
    priority = "critical",
    task = {
      name = "agent",
      runtime = "exec",
      config = { command = "legion:self", args = ["app", "run", "autoscaler"] },
      resources = { cpu = 200, memory = 256 },
      network = { ports = [{ name = "api" }] },    # not a catalog service (§6.5)
    },
  },
  permissions = [
    { resources = ["scale"], actions = ["read", "write"], namespaces = ["*"] },
    { resources = ["job"],   actions = ["read"],          namespaces = ["*"] },
    { resources = ["node"],  actions = ["read", "write"] },
    { resources = ["kv"],    actions = ["read", "write"], paths = ["legion/apps/autoscaler/*"] },
  ],
  ui = { port = "api", ... },             # §6.5
  config = { schema = ..., default = { ... } },   # a Nickel contract plus defaults
}
```

### 6.2 Where apps run

**The `legion-system` namespace.**
- The first manager to lead the Control lane on an empty store creates it,
  bypassing the reserved-prefix check. It can't be deleted.
- ACL namespace globs **do not match reserved `legion-*` namespaces unless
  the pattern itself starts with `legion-`** (`*` matches `payments-prod`,
  not `legion-system`; `legion-*` or `legion-system` match it). This works
  like a shell's `*` skipping dotfiles. A tenant-admin policy written as
  `namespaces = ["*"]` therefore can't edit or exec into the autoscaler and
  borrow its identity. Management tokens bypass rules as always (calls
  entry 5).
- The same rule reserves the **`legion/` KV prefix**, where apps keep
  leases and state. A `paths = ["*"]` rule doesn't reach `legion/…`, only a
  pattern that starts with `legion/` does. Node-side `namespaces` lists
  (below) follow the rule too: `["*"]` on a node does not accept
  `legion-system`.

**Apps run unprivileged** (calls entry 4).
- A task in a reserved namespace may not run as root. The reconciler sets
  `user` on bundled apps' exec tasks to `systemWorker.appUser` (default
  `nobody`). Submit refuses an exec task in `legion-system` without a
  non-root `user`, and refuses `privileged` containers.
- The reason is the host. Root on a manager can read the manager's data
  directory and raft state, and can delete the host-guard table. A root exec
  task can also leave its cgroup. Any of those would turn "an app" into
  "the cluster".
- Even unprivileged, an app's code runs on the managers with its grant. So
  **anything that changes what code an app runs** (`app install`, `app
  upgrade`, and an `app config set` that adds or changes plugin sources or
  artifacts) re-runs the grant check against the caller, the same as
  install. The install prompt says so plainly (§6.3).

**Node-side namespace restriction** (calls entry 10). Worker types gain
`namespaces`:

```nickel
# configs/workers/ci.ncl
{ pool = "ci", namespaces = ["ci-*"] }    # this node only runs jobs from ci-* namespaces
```

The scheduler's candidate filter checks it beside `is_permitted`: a node
with a non-empty `namespaces` list is ineligible for any job whose namespace
it doesn't match. It is a node restriction, not a permit, so a job cannot
opt itself in. The blocked-eval wake pass re-runs the same check, so a
restricted node waking a blocked job only happens when the job could
actually land there.

```
$ legion job plan web.ncl -n payments-prod
... unplaced: no available nodes: 3 nodes accept only namespaces [legion-system]
```

**The system worker** (calls entry 2). Every manager runs one in-process
worker by default:

```nickel
# configs/manager.ncl  (these are the defaults; write nothing to get them)
systemWorker = {
  enabled = true,
  namespaces = ["legion-system"],
  resources = { cpu = 1000, memory = 1024 },   # what the manager lends to apps
},
```

- It registers as worker type `legion-system` in pool `legion-system`, with
  attribute `legion.role = "manager"`. Nothing else lands there: tenants
  are stopped by the namespace restriction, and by the pool taint.
- Capacity is the `resources` lend, not the host, and the lend is enforced,
  not just advertised. The system worker's cgroup parent
  (`legion-system/`) gets `cpu.max` and `memory.max` equal to the lend.
  Per-allocation `cpu.weight` lets apps burst *within* it, but never past
  it. raft and the manager keep the rest of the box. macOS has no cgroups,
  so there the lend is advertised capacity only, and the public doc says
  so.
- One per manager, so apps fail over across managers. A single-manager
  cluster has one.
- `legion start manager --run-worker` keeps its meaning ("this manager also
  runs everything"). It now sets `systemWorker.namespaces = ["*",
  "legion-*"]`, removes the lend cap, and joins the `default` pool and
  worker type, as before. It keeps the `legion.role = "manager"` attribute.
  Placement (below) keys on that attribute, so apps still land there on a
  single-node dev cluster.
- `systemWorker.enabled = false` is the "keep managers clean" option.
  Bundled apps then run on workers (`placement = "any"` fallback), and
  `legion app list` notes the trade-off:

  ```
  autoscaler  running on wkr-3   note: no system worker; node scale-in will never choose wkr-3
  ```

**Placement as a setting, not a rebuild.** The reconciler renders placement
into the app job's `permit` and constraints:

| `placement` | Permit | Constraint / affinity |
| --- | --- | --- |
| `managers` (default) | pools `legion-system` and `default` | constraint `legion.role = "manager"` |
| `workers` | pool `default` (or `settings.pool`) | constraint `legion.role is_not_set` |
| `any` | both | affinity toward `legion.role = "manager"` |

`legion app set autoscaler --placement workers` changes the record, and the
reconciler resubmits (a normal rollout). Keying `managers` on the attribute
rather than the pool is what makes it work both on a dedicated system
worker (pool `legion-system`) and on a `--run-worker` dev manager (pool
`default`). The `legion-system` namespace also gets a stored namespace
policy with `pools.default = "legion-system"`, so a hand-written
`legion-system` job with no permit lands on managers too.

### 6.3 App lifecycle

**Records and the reconciler.**
- An `app` table on the Control lane holds: name, source
  (`bundled | file | url`), manifest JSON and digest, enabled, settings
  (`replicas`, `placement`, `resources`), config, grant, and installed-by.
- The reconciler runs on the Control-lane leader (the
  idempotency-prune loop pattern, `daemon.rs:426`). For each enabled app it
  renders the job from manifest, settings, and config, and compares it with
  the stored job config.
- On a difference it submits through the normal `RunJob` path as the
  principal `legion:app-reconciler`, carrying the grant recorded on the app
  record (§6.4). Edits made to an app's job directly
  are therefore reverted; `legion app` is the interface. For each disabled
  app it stops the job.
- The reconciler records a status: `running`, `degraded`, `pending (why)`,
  or `stopped`.

**Bundled apps run the node's own binary** (calls entry 12).
`command = "legion:self"` is resolved by the exec runtime to the worker's
own executable (`std::env::current_exe()`). The app is always the same
build as the node that runs it: no artifact, no image, no registry, and it
works offline. A manager upgrade changes `legion.version` in the rendered
manifest, so the reconciler rolls the app forward on its own. Third-party
apps use artifacts or images like any job.

**Seeding.**
- On an empty store, the first Control leader seeds a record for each
  bundled app from its manifest defaults. The autoscaler is seeded enabled.
- Manager config may set seed-time overrides (`apps = { autoscaler = {
  enabled = false } }`). These are read **only when seeding**, because
  config can differ between managers and the record is the single source
  afterwards. Startup logs that, and `legion app` is the tool from then on.

**The surface:**

```console
$ legion app list
NAME        VERSION  SOURCE   STATUS    PLACEMENT  REPLICAS  UI
autoscaler  0.14.0   bundled  running   managers   1/1       nav, job, task
gitops      0.2.0    url      degraded  managers   0/1       job      (pending: no room in pool legion-system)

$ legion app status autoscaler        # record, grant, job status, last reconcile, app health
$ legion app logs autoscaler -f       # = legion job logs legion-system/autoscaler
$ legion app config get autoscaler    # current config (Nickel, -j for JSON)
$ legion app config set autoscaler -f autoscaler.ncl   # validated against the manifest's contract
$ legion app config schema autoscaler
$ legion app set autoscaler --replicas 3 --placement any
$ legion app disable autoscaler       # stops the job, keeps record and config
$ legion app enable autoscaler
$ legion app install ./app.ncl | <url>   [--yes]   # the prompt: what runs where, as whom, with what grant and UI rpcs
$ legion app upgrade gitops <url>     # shows the permission diff; new permissions need approval
$ legion app uninstall gitops         # stops the job, revokes the grant, drops UI registrations
```

**Config.**
- Config lives on the record. It is validated at `config set` against the
  manifest's Nickel contract, and rendered into the job as a file
  (`$LEGION_APP_CONFIG`, JSON).
- A config change resubmits the job. That is a normal rollout, so the job's
  version history is the config's history (calls entry 13).
- A config change that alters the code the app runs (manifest-declared
  `codeFields`: for the autoscaler, `plugins.*.source` and `plugins.*.url`)
  needs the caller to hold the app's whole grant, as `install` does. Other
  config changes need only `app:write`. Without this rule, `app:write` on
  the autoscaler would be a path to `node:write` (calls entry 4).

**RPCs.** `ListApps`, `GetApp`, `InstallApp`, `UpgradeApp`, `UninstallApp`,
`SetAppEnabled`, `UpdateAppSettings`, and `SetAppConfig`. They are mutating
where appropriate, and every mutating one takes `idempotency_key`. ACL
resource **`app`** is cluster-scoped, with a `names` selector: `app:read`
lists and shows, `app:write` changes. MCP: `legion_app_list` and
`legion_app_status` (read-only), plus `legion_app_view` (§6.5).

### 6.4 Identity: declared permissions, granted at submit, ambient inside the task

**Declared on the job** (calls entry 3). The reserved `legion` identity
gains `permissions` (inline ACL rules, in the same shape `legion acl policy
apply` takes) and `policies` (names of existing policies):

```nickel
identities = [{
  name = "legion",
  permissions = [{ resources = ["kv"], actions = ["read", "write"], paths = ["certs/*"] }],
}],
```

**Granted only up to what the submitter holds.** At `RunJob`,
`RunJobTemplate`, and every other path that stores a job config, the manager
checks that the submitting token is allowed every requested
(resource, action, scope).

- A requested glob is covered when the literal pattern string matches one
  of the submitter's allow globs. This test is sound for Legion's `*`-only
  globs: if `payments-*` as a string matches `pay*`, every namespace
  matching `payments-*` also matches `pay*`.
- **Reserved names first.** A requested pattern that can match a reserved
  name (it starts with `legion-` for namespaces, or `legion/` for KV paths)
  is covered only by a held pattern that also starts with that prefix.
  Otherwise a held `*` would string-match a requested `legion-*`, and a
  tenant admin could grant a job `exec:write` into `legion-system`.
- When the submitter holds any deny rule on that resource, the check
  conservatively requires `acl:write`.
- Management tokens pass.
- The check runs on **every** submit of the job, so re-submitting an app
  job (even unchanged) requires holding what it holds, and nobody can
  launder a grant by editing a job they can write.

```
error: cert-renewer: identity 'legion' asks for kv:write on certs/*, which your token
  does not hold.
  hint: have someone with that access submit this job, or ask for it to be granted to you
```

**Resolved per version.**
- The grant is stored with the job config version that requested it,
  together with the granting principal.
- The `legion` auth method resolves a token's policies from the grant of
  the allocation's config version, then adds `controlPlaneBindings` as
  before. Allocations of an old version mid-rollout keep that version's
  grant.
- Keying is by the canonical job id, which includes the namespace. That
  closes the bare-name binding gotcha (§1.3).
- `legion acl grants list [--job …]` shows every grant and who made it.
  `legion acl grants revoke <job>` drops it, effective on the next token
  verification.
- `controlPlaneBindings` stays, as the claim-based path for operators who
  map identities wholesale.

For **apps**, `legion app install` and `upgrade` show the manifest's
permissions and run the same check against the installer, then record the
grant on the app record. The reconciler renders them into the app job's
identity. Its own resubmits are checked against that recorded grant, not
against the reconciler's principal. A permission the installer approved
stays approved until an `upgrade` changes it. Any *other* principal
resubmitting an app job directly still faces the full check. Bundled apps'
grants are recorded at seed time as granted by `legion:bootstrap`, the only
grant that skips the check. It is limited to manifests compiled into the
binary (calls entry 15).

**Ambient: a local API socket** (calls entry 9).
- Every allocation whose task declares the `legion` identity, which every
  app task does, gets a unix socket at `$LEGION_SECRETS_DIR/legion.sock`.
  The worker listens on it and proxies gRPC to the managers.
- The worker attaches the allocation's current identity token to each
  forwarded call, and handles manager failover, TLS, and the cluster CA.
- The task gets `LEGION_ADDR=unix://$LEGION_SECRETS_DIR/legion.sock`. The
  CLI and the Rust client learn `unix://` addresses. Every
  `legion …` command inside the task therefore just works, with no token
  in the task's environment.
- This is the only route that works everywhere. The host guard drops
  allocation sockets to the manager's ports on a manager's own node, gated
  allocations would otherwise need a policy hole to the managers, and the
  socket needs neither.
- The worker knows which allocation owns which socket, and it checks every
  connection. File mode alone isn't enough, because tasks can share a uid.
  So the worker reads the peer's credentials (`SO_PEERCRED`), and accepts
  the connection only if that pid's cgroup is inside the allocation's own
  leaf (`legion/{share,reserve}/legion-<alloc>`). A process in another
  allocation is refused even if it can open the path. Attribution is
  therefore exact (compare the datapath's caveat in
  `src/worker/netd/dns.rs:4-10`).
- The token file is still written (`$LEGION_SECRETS_DIR/legion.token`) for
  clients that can't use a socket, and its path is in
  `LEGION_IDENTITY_TOKEN_FILE`. It is deliberately **not**
  `LEGION_TOKEN_FILE`: the CLI's `legion login` writes its own token to
  `LEGION_TOKEN_FILE` (`src/cmd/helpers.rs:165-175`), and would overwrite
  the worker's.
- Containers get the socket through the task-directory mount they already
  have. **Sandboxes** get it later, over the guest agent's vsock; until
  then they fall back to the token file plus `LEGION_ADDR` set to a
  reachable manager.

### 6.5 UI extensions

**Tier 1: declarative views** (calls entry 8). An app declares **slots** in
its manifest. The SPA renders the JSON **views** the app returns, with its
own components:

```nickel
ui = {
  nav  = [{ title = "Autoscaling", icon = "gauge", view = "overview" }],
  job  = [{ title = "Autoscaling", view = "job",
            permission = { resource = "scale", action = "read" } }],     # checked on the job in context
  task = [{ badge = "task-badge", permission = { resource = "scale", action = "read" } }],
  node = [{ title = "Node group", view = "node", permission = { resource = "node", action = "read" } }],
  actions = [
    { name = "pause", title = "Pause autoscaling", confirm = "Pause autoscaling for %{job}?",
      inputs = { for = { type = "duration", default = "1h" } },
      permission = { resource = "scale", action = "write" } },
  ],
  refresh = "10s",
}
```

The slots are `nav` (a page in the sidebar's Apps section), `overview` (a
card on the cluster overview), `job` (a panel on the job page), `task` (a
badge on a task row), and `node` (a panel on the node page). Each slot gets
its context (`namespace`, `job`, `group`, `task`, `node`).

A view is a small, versioned JSON document (`legion-view/1`) built from a
closed set of blocks:

- `stats`: label, value, tone;
- `series`: time series, at most 4 lines and 2 axes;
- `table`: typed columns, links to Legion routes;
- `timeline`: dated entries with detail;
- `keyValue`;
- `notice`: a tone and text;
- `markdown`: CommonMark, sanitized, no raw HTML, links to `http(s)` or
  in-app routes only;
- `actions`: buttons bound to manifest-declared actions.

An app may return `{ "hidden": true }` to say "nothing here for this job".

```json
{ "view": "legion-view/1", "blocks": [
  { "type": "stats", "items": [
    { "label": "Now", "value": "6" }, { "label": "Bounds", "value": "2–8" },
    { "label": "CPU", "value": "91%", "tone": "warn" } ] },
  { "type": "series", "title": "Replicas vs CPU (1h)", "series": [
    { "name": "replicas", "points": [[1792412400, 4], [1792412460, 6]] },
    { "name": "cpu", "axis": "right", "unit": "%", "points": [[1792412400, 91]] } ] },
  { "type": "timeline", "items": [
    { "at": 1792412545, "title": "4 → 6", "detail": "cpu 0.91 vs 0.70 (4 ready)" } ] },
  { "type": "actions", "items": [{ "action": "pause", "label": "Pause 1h" }] }
] }
```

**One data path for the UI, CLI, and MCP** (calls entry 11).
- `ListUiExtensions` returns the slots the viewer may see, filtered by
  `app:read`. `GetAppView {app, view, context}` and
  `InvokeAppAction {app, action, context, inputs, idempotency_key}` are
  gRPC methods on the manager. The SPA calls them over gRPC-Web like any
  other RPC. The browser never talks to an app directly.
- **The manager authorizes before forwarding.** It checks `app:read` (or
  `app:write`, for actions), then evaluates the view's or action's declared
  `permission` *as the viewer* against the context's scope (`scale:write`
  on `payments-prod/api`, for example). Only then does it forward.
- **Actions only where there is a scope.** Actions may be declared only in
  slots with a context (`job`, `task`, `node`), so the declared permission
  always has something to be checked against. `nav` and `overview` pages
  hold views only.
- **The forward** goes over the worker-plane tunnel to the app allocation's
  `ui.port`: `GET /legion/v1/views/<view>?…` or
  `POST /legion/v1/actions/<action>`. This is the reverse-proxy plan's
  `Worker.ProxyTunnel`; a unary subset is built here if that plan hasn't
  landed.
- **The UI port is not a service.** It is never registered in the catalog,
  and the worker delivers tunnel traffic to it locally.
- **The viewer assertion is mandatory.** Each request carries a header
  `Legion-Viewer: <JWT>`, signed with the cluster's workload-identity key.
  Its `aud` is `app:<name>`. It carries the viewer's principal and accessor
  id, the checked permission, the **context** that permission was checked
  on, and, for actions, a hash of the inputs. It lives 60 seconds.
  - Apps must reject any request whose assertion is missing, expired, for
    another audience, or whose context and inputs don't match the request.
  - The app framework (`legion app run …` and the Rust SDK) does this
    before the handler runs. `legion app test` checks it for third-party
    apps.
  - A workload that can reach the port directly can therefore do nothing
    with it.
- **Apps don't authorize viewers.** The manager already did, against the
  context in the assertion. The app's one obligation is to act only on that
  context: inputs are typed values (string, number, duration, enum,
  boolean), and the manifest can't declare an input that names a different
  target.
- **Actions are audited.** Every action is recorded as an `app.action` event
  (app, action, viewer, context, outcome).
- **The terminal renders the same views**:
  `legion app view autoscaler job web` prints stats, tables, and timelines
  as text, and `-j` returns the JSON. The MCP tool `legion_app_view` returns
  the JSON. Whatever a human sees on a panel, an agent can read.

**Tier 2: sandboxed pages** (Phase A5), for apps whose UI doesn't fit the
blocks: an editor, a graph, or a log viewer, for example.
- The manifest declares `ui.pages = [{ title, bundle = "/legion/v1/page" }]`.
  The app serves a **single-file HTML bundle**.
- The SPA fetches it through `GetAppView`, so it is authorized the same
  way, and mounts it in an `<iframe sandbox="allow-scripts" srcdoc=…>`.
  There is no `allow-same-origin`, no `allow-forms`, no `allow-popups`, and
  no `allow-top-navigation`.
- The opaque origin means the page cannot read the SPA's storage or
  cookies, or make credentialed requests to the manager.
- The SPA prepends a CSP: `default-src 'none'; script-src 'unsafe-inline';
  style-src 'unsafe-inline'; img-src data:; connect-src 'none';
  form-action 'none'; base-uri 'none'`. The page cannot load remote code or
  open network connections.
- **What this does not stop.** A page can still navigate its own frame to
  an external URL carrying data in the query string, and no CSP directive
  reliably prevents that today. A tier-2 page can therefore leak whatever
  its viewer can read through its allowlisted RPCs. The plan treats tier-2
  code as **trusted with the viewer's read access**:
  - the install prompt lists every page's `rpcs`;
  - every mutating bridge call shows an SPA-owned confirmation ("GitOps
    wants to run `RunJob` on team-a/web as you");
  - the public doc states the limit.
- Everything else goes through a `postMessage` bridge:
  - `legion.call(method, request)` runs a Legion RPC **as the viewer**,
    limited to the methods the manifest lists in `ui.pages[].rpcs`;
  - `legion.view(name, ctx)` and `legion.action(name, inputs)` reuse the
    tier-1 path;
  - `legion.navigate(route)` moves the SPA;
  - `legion.theme` is the current theme.

  The page never sees a token. The only power it has is what the viewer
  has, narrowed again by its manifest.

### 6.6 Jobspec extensions: apps add their own blocks

An app that acts on jobs usually needs settings per job: what to back up and
how often, which hostname to route, which repo a job came from. The natural
home is the jobspec, next to the job it describes, versioned and reviewed
with it. Each app gets **one namespaced block**, `apps.<app name>`, at the
job level, the task level, or both. The app declares the block's schema in
its manifest, and the manager enforces it (calls entry 6).

```nickel
{
  name = "orders-db",
  apps = {
    uptime = { url = "https://orders.acme.dev/healthz", alert = "#payments-oncall" },  # job level
  },
  task = {
    name = "postgres",
    runtime = "container",
    config = { image = "postgres:17" },
    volumes = [ ... ],
    apps = {
      backup = { schedule = "0 3 * * *", keep = 14 },                                  # task level
    },
  },
}
```

**Declaring an extension point.** In the manifest:

```nickel
extensions = {
  task = {
    description = "Back up this task's volume on a schedule.",
    # JSON Schema (2020-12), written as a Nickel record. The same format task
    # runtimes use for their `config` block.
    schema = {
      type = "object",
      additionalProperties = false,
      required = ["schedule"],
      properties = {
        schedule = { type = "string", description = "Cron expression, in UTC." },
        keep     = { type = "integer", minimum = 1, default = 7, description = "Backups to retain." },
        volume   = { type = "string", description = "Volume to back up. Defaults to the task's only volume." },
      },
    },
    examples = [{ schedule = "0 3 * * *", keep = 14 }],
    validate = true,      # also ask the app at submit, for checks a schema can't express (optional)
    # requires = { resource = "net", action = "write" },   # extra permission to set this block (optional)
  },
  # job = { ... },        # a job-level block, declared the same way
},
```

- The schema must describe an object, be at most 64 KiB, and use only local
  `$ref`s (no remote fetches).
- It is compiled once, at install or upgrade, with the `jsonschema` crate
  that already validates runtime config. A schema that doesn't compile
  fails the install, and so does an `example` that doesn't validate, so the
  examples an app ships can't drift from its schema.
- The block's key is the app's name, which is already unique in the
  cluster, so two apps can't collide. Neither can an app and a future core
  field, because core fields never live under `apps`.

**At submit**, for each `apps.<name>` block, the manager:

1. **Finds the owner.** It looks up the installed app `<name>` with an
   extension at that level. If there is none, the submit fails, or with
   `--allow-unknown-fields` the block is stored unvalidated (below).
2. **Validates the shape, and fills defaults.** It applies the schema and
   fills its `default`s, so the stored block is the expanded one, and
   `legion job plan` prints it.
3. **Checks `requires`**, if the app declared it, as the submitter, on the
   job's namespace.
4. **Asks the app**, if `validate = true`. It sends the expanded block and
   its context to `POST /legion/v1/extensions/validate`, using the same
   worker-plane forward and assertion as UI views (§6.5), with the
   submitter as the viewer, and a 2-second budget. The app returns errors,
   which reject the submit, or warnings, which pass through to the reply.
   **If the app can't be reached, the submit succeeds with a warning.**
   That is Kubernetes' failing-webhook lesson: a broken add-on must not
   block deploys. The app checks the block again when it arrives through
   its watch, and reports problems on its own status and views.
5. **Stores the block** in the task or job config, stamped
   `validated_by = <app>@<manifest digest>`.

```
error: orders-db: task postgres: apps.backup.keep: 0 is less than the minimum of 1
error: orders-db: task postgres: apps.backup (backup app): volume 'logs' is not one of
  this task's volumes (data)
warning: orders-db: task postgres: apps.backup was not checked by the backup app
  (unreachable); the schema check passed
```

**Changing only a block restarts nothing.** A resubmit whose only
differences are in `apps` blocks still creates a job version, and it moves
the running allocations onto it in place, through the path KV re-renders
already use (`src/manager/server.rs:4150-4180`). There is no rollout. The
app sees the new block through its watch.

**How blocks reach the app.** `WatchAppExtensions { shards, shard_count }`
is app-plane: it returns only the calling app's own blocks. It sends the
current blocks first, then upserts and deletes of (job id, config version,
level, group/task, namespace, block, validity). It shards by the same key
hash as `WatchScalingPolicies` ([autoscaler-v2-plan.md](autoscaler-v2-plan.md)
§6.9), so a sharded app needs only one lease scheme. Humans and agents see
blocks in `legion job inspect` and `legion job info`, like any other field.

**Discoverability.**
- `legion job schema` (and `legion_job_schema` over MCP) merges every
  installed app's extension schemas under `apps.<name>` at the right level,
  the same way it already merges the live task-runtime config schemas. An
  agent authoring a jobspec sees exactly what this cluster accepts.
- `legion app extensions [app]` lists the extension points, with their
  descriptions and examples.

**Lifecycle.**
- **Install and upgrade** register or replace the schema. The manager then
  checks every stored block for that app against the new schema, including
  blocks stored unvalidated under the flag, and reports the result in the
  prompt:

  ```
  14 jobs carry apps.backup blocks; 2 fail this version's schema:
    orders-db/postgres: apps.backup.keep: required property missing
  ```

  Failing blocks stay stored, are flagged `invalid` in `job info` and on the
  app's watch (the app decides whether to act on them), and the next
  resubmit of those jobs must conform.
- **Disable** keeps the schema registered, so submits are still validated.
  Nothing acts on the blocks while the app is off, and `job info` says
  `app disabled`.
- **Uninstall** lists the jobs carrying the app's blocks, and needs
  `--force` if there are any. The blocks stay stored, flagged `orphaned`.
  Resubmitting such a job fails unless the block is removed or
  `--allow-unknown-fields` is passed.

**Trust.** A block is an instruction, from whoever can submit the job, to an
app that holds its own grant.
- The app acts only on the job (and task) that carries the block. This is
  the same context rule as for actions (§6.5): the app framework hands the
  handler the job, not a free-form target, and schemas can't declare a
  field that names a different job.
- `requires` lets an app demand more than `job:write` from whoever sets its
  block. A DNS app could require `net:write`, so a team that can deploy
  can't also claim hostnames. It is checked on every submit that adds or
  changes the block.
- Blocks are visible to anyone with `job:read`, so they must never hold
  secrets. Schemas should take KV paths instead, and the public doc says
  so.
- Schemas are data, compiled once. No app code runs in the manager on
  submit; the only app code involved is the optional, time-boxed validate
  call, which runs in the app.

**Unknown fields are errors, unless you ask** (calls entry 7). Every key in
a jobspec that neither core nor an installed app's extension knows becomes
a submit error, with a suggestion, at the job level, the task level, and in
core nested blocks:

```
error: web: task web: unknown field `autoscael` (did you mean `autoscale`?)
error: orders-db: task postgres: apps.backup: no installed app named 'backup' declares a
  task extension
  hint: `legion app list` shows installed apps. Install backup, remove the block, or pass
  --allow-unknown-fields to store it unvalidated
```

`--allow-unknown-fields` accepts the spec anyway. It is
`allow_unknown_fields` on `RunJob`, `PlanJob`, `ValidateJob`, and
`RunJobTemplate`, and a per-repo setting for GitOps.
- **An unknown core field is dropped.** The manager has nowhere to put a
  field it doesn't model. Each dropped field is listed in the reply's
  `warnings` and recorded on the job version.
- **An `apps.<name>` block for an app that isn't installed**, or doesn't
  declare that level, **is stored unvalidated** and flagged `pending`. When
  an app with that name and extension point is installed, it validates the
  pending blocks (and its install prompt reports them), and from then on
  they reach the app like any other block.
- **A block for an installed app that fails its schema is still an error.**
  The flag is about fields Legion doesn't know, not values it knows are
  wrong.

```console
$ legion job run orders-db.ncl --allow-unknown-fields
warning: task postgres: dropped unknown field `memoryMax2`
warning: task postgres: stored apps.backup unvalidated (no app named 'backup' is installed)
orders-db: submitted version 3
$ legion job info orders-db
...
submitted with --allow-unknown-fields: dropped `memoryMax2`; apps.backup pending (no app 'backup')
```

The cases the flag exists for:
- a jobspec written for a newer Legion, submitted mid-upgrade;
- GitOps syncing a job before the app it configures is installed;
- moving jobs between clusters that have different apps installed.

Recording what was dropped or left pending on the version, and showing it in
`job info`, keeps the escape hatch visible instead of habitual. A manager
that predates this phase still drops unknown keys silently. That can't be
fixed retroactively, and `docs/upgrades.md` will say so.

### 6.7 Health and visibility

- `legion app status` and the UI's Apps page show:
  - the record, the grant, and the job's derived status;
  - the reconciler's last action and error;
  - the app's own health, from a manifest-declared `health` view or the
    job's checks.
- Events: `app.installed`, `app.upgraded`, `app.uninstalled`,
  `app.enabled`, `app.disabled`, `app.config_changed`, `app.reconciled`,
  `app.reconcile_failed`, and `app.action`.
- `legion_apps{status}` on `/metrics`.
- A stopped bundled app is a cluster-health finding (`legion doctor`, when
  it exists, and the UI's overview), not a silent absence.

### 6.8 Checking the substrate against GitOps

Before committing to this design, we checked it against the second app it
must serve:

```nickel
{
  name = "gitops",
  placement = "managers",
  job = { task = { name = "sync", runtime = "exec",
    artifacts = ["https://example.com/gitops/0.2.0/legion-gitops-linux-amd64.tar.gz"],
    config = { command = "legion-gitops" } } },
  permissions = [
    { resources = ["job"],      actions = ["read", "write"], namespaces = ["team-*"] },
    { resources = ["template"], actions = ["read"] },
    { resources = ["kv"],       actions = ["read"], paths = ["legion/apps/gitops/*"] },
  ],
  ui = {
    nav = [{ title = "GitOps", view = "repos" }],
    job = [{ title = "Sync", view = "job-sync", permission = { resource = "job", action = "read" } }],
    actions = [{ name = "sync-now", title = "Sync now",
                 permission = { resource = "job", action = "write" } }],
  },
  config = { schema = { repos | Array { url | String, path | String, namespace | String } } },
}
```

| GitOps needs | Provided by |
| --- | --- |
| Run reliably, survive full workers | §6.2 system worker |
| Deploy jobs without being admin | §6.4 grant, limited to `team-*` and checked against the installer |
| Call `legion job run` from inside | §6.4 local socket; `legion` is `legion:self` or ships in its artifact |
| Show sync state where operators look | §6.5 `job` slot plus the `repos` page |
| "Sync now" pressed by a viewer who may not deploy | §6.5 action permission `job:write` checked as the viewer, on that job |
| Its repo credentials | config refers to KV paths; KV reads through its grant |
| Upgrades with new permissions | §6.3 permission diff at `app upgrade` |
| Mark the jobs it deployed, and where they came from | §6.6: GitOps adds a job-level `apps.gitops = { repo, path, commit }` block when it submits, and its Sync panel reads it |

Nothing GitOps needs is autoscaler-specific, and nothing here is
GitOps-specific. That is the test the substrate had to pass.

**What it surfaces: GitOps is an escalation hub.** Every job GitOps submits
goes through the grant check (§6.4) as GitOps. GitOps must therefore hold
every permission any job it deploys asks for. An operator who wants GitOps
to deploy jobs with `kv:write` grants GitOps `kv:write`, and anyone who can
merge to its repos can then spend that. This is honest, and it is the same
property Argo CD's service account has. Mitigations: the grant is visible
and scoped (`namespaces = ["team-*"]`), and the GitOps plan should map repos
to namespaces one to one. The open question (§11) is whether grants could be
pre-approved per namespace instead, so GitOps doesn't need the union.

## 7. Proto, struct, config, and persistence changes

- **Proto**, every new RPC and field with public comments:
  - `ListApps`, `GetApp`, `InstallApp`, `UpgradeApp`, `UninstallApp`,
    `SetAppEnabled`, `UpdateAppSettings`, `SetAppConfig`;
  - `ListUiExtensions`, `GetAppView`, `InvokeAppAction`;
  - `WatchAppExtensions` (app-plane: only the caller's own blocks);
  - `bool allow_unknown_fields` on `RunJob`, `PlanJob`, `ValidateJob`, and
    `RunJobTemplate`, with dropped and pending fields reported in the
    reply's `warnings`;
  - the job and task messages gain the `apps` object; `GetJobSchema` merges
    installed apps' extension schemas;
  - `ListGrants`, `RevokeGrant`;
  - `WorkloadIdentity` gains `repeated AclRule permissions` and
    `repeated string policies`;
  - `Node` gains `repeated string namespaces`.
- **ACL.** A new resource `app` (cluster-scoped, `names` selector). Every
  RPC above goes in `tests/integration/acl_endpoints.rs`. Glob matching
  gains the reserved-prefix rule for `legion-*` namespaces and `legion/` KV
  paths, in rule evaluation and in grant coverage, unit-tested in
  `src/structs/acl.rs`. The grant check runs on `RunJob`, `RunJobTemplate`,
  and every other path that stores a job config.
- **Store**, on the Control lane:
  - an `app` table;
  - `job_grant` (job id, config version, rules JSON, granted by, at,
    revoked);
  - `node.namespaces` (JSON array).
  - Bootstrap creates `legion-system` and its namespace policy
    (`pools.default = "legion-system"`).
- **Scheduler.** A namespace check in `scan_base_eligible` and in the
  blocked-eval wake match.
- **Worker.**
  - The `legion:self` resolution in the exec runtime.
  - The per-allocation API socket proxy: a tonic service over a
    `UnixListener`, with an `SO_PEERCRED` plus cgroup check, that forwards
    with the allocation's token.
  - `LEGION_ADDR`, `LEGION_IDENTITY_TOKEN_FILE`, `LEGION_SECRETS_DIR`, and
    `LEGION_APP_CONFIG` injection.
  - The system worker's capped cgroup parent (`cpu.max`, `memory.max`).
- **Submit validation.** No root exec tasks and no privileged containers in
  reserved namespaces.
- **Jobspec parsing (A6).**
  - `deny_unknown_fields` on the job input, the task input, and core nested
    blocks, with a did-you-mean error (closest known key by edit distance).
  - Under `allow_unknown_fields`, unknown keys are collected rather than
    failing the parse.
  - A new `apps` object at job and task level. Each key is an app name, and
    each value is a JSON object stored with `validated_by` and a state
    (`valid`, `pending`, `invalid`, or `orphaned`).
  - The job version records what the flag dropped or left pending.
  - The in-place comparator treats a resubmit that differs only in `apps`
    blocks as schedule-equivalent, so it takes the
    `migrate_allocations_to_job_config` path.
- **CLI and client.** `unix://` addresses; `legion app …`;
  `legion acl grants …`.
- **Config.**
  - `ManagerConfig.systemWorker` (`enabled`, `namespaces`, `resources`,
    `appUser`) and `ManagerConfig.apps` (seed-only).
  - Worker type `namespaces`.
  - `configs/jobs/maximal.ncl`: `identities[].permissions` and
    `identities[].policies`.
  - `configs/apps/example-app.ncl`: a complete manifest with tier-1 UI and
    an `extensions` block.
  - `configs/jobs/maximal.ncl`: an `apps` block at job and task level,
    commented out, because it needs the named app installed and maximal
    must submit on a bare cluster.
- **UI.** An extension registry fetched at login, slot outlets on the job,
  task-row, node, overview, and sidebar surfaces, the block renderer, and
  (A5) the sandboxed page host with its bridge.

## 8. Implementation phases

- **A1: places to run.**
  - The `legion-system` namespace and the reserved-glob ACL rule.
  - Node `namespaces` restriction (scheduler and wake pass).
  - The system worker, on by default, with `--run-worker` reshaped.

  *Alone:* dedicated nodes for a namespace, and managers that can host
  operational jobs safely. Scheduler-touching, so it ships with
  `scheduler_bench` numbers.
- **A2: identity that just works.**
  - `identities[].permissions` and `policies`, the submit-time escalation
    check, grants per version, `legion acl grants`.
  - The per-allocation API socket and ambient env, with `unix://` in the CLI
    and client.

  *Alone:* the cert renewer drops its manager-config binding and its
  hard-coded address; `configs/jobs/cert-renewer.ncl` is rewritten as the
  example.
- **A3: apps.**
  - Manifests, records, the reconciler, `legion:self`, seeding.
  - `legion app` (install from a file or URL, upgrade with a permission
    diff, config, settings, enable/disable), app events and metrics, MCP
    read tools.

  *Alone:* bundled apps exist. The autoscaler's Phase 2 is the first.
- **A4: UI tier 1.**
  - `ListUiExtensions`, `GetAppView`, `InvokeAppAction`, the viewer
    assertion, and the worker-plane forward (a unary tunnel subset if
    reverse-proxy Phase 1 hasn't shipped).
  - SPA slot outlets and the block renderer; `legion app view`;
    `legion_app_view`.
- **A5: UI tier 2.** Sandboxed single-file pages, the bridge with an RPC
  allowlist and mutation confirmations, and the CSP wrapper.

- **A6: jobspec extensions and strict fields.** Two halves:
  - **Strict fields.** Unknown fields become submit errors with
    suggestions, and `--allow-unknown-fields` is the escape hatch. This half
    depends on nothing, and can ship first, even before A1.
  - **Extensions.** Manifest `extensions`, the `apps` block, schema
    validation and defaults, `requires`, the validate call (it needs A4's
    forward; until then `validate = true` is accepted and skipped with a
    warning), `WatchAppExtensions`, `job schema` merging, the
    install/upgrade/uninstall lifecycle, and in-place version moves. Needs
    A3.

  *Alone:* typos and version skew stop being silent on day one, and third
  parties can build apps that take per-job settings without changing
  Legion.

Deferred, not phased: installing from a registry
(`legion app install @legion/gitops`). It waits on a registry plan that
doesn't exist yet, and `install <url>` covers distribution until then.

## 9. Testing plan

**A1: places to run**
- Namespace-restricted nodes reject other namespaces even when the job
  permits their pool.
- The wake pass does not wake a job onto a node that rejects its
  namespace.
- A node with `namespaces = ["*"]` does not accept `legion-system`.
- The reserved-prefix rule: `*` matches neither `legion-system` nor
  `legion/…` KV paths, and `legion-*` and `legion/*` do.
- The system worker's cgroup parent caps CPU at the lend under a busy loop
  (Linux), and `--run-worker` still hosts a `placement = managers` app on
  one node.
- Submit refuses a root exec task and a privileged container in
  `legion-system`.
- Benchmark: before and after numbers for `candidate_construction`.

**A2: identity**
- The glob coverage table: sound cases, conservative refusals, deny rules,
  and the reserved prefix (a held `*` does not cover a requested
  `legion-*`).
- Escalation: a token with `job:write` and no `kv:write` cannot submit,
  through `RunJob` or `RunJobTemplate`, a job requesting `kv:write`.
- Revocation takes effect on the next verification.
- The socket:
  - a host-mode task on a manager's system worker reaches the API through
    the socket while the host guard is active, and is refused by the host
    guard on the direct port;
  - a process from another allocation, under the same uid, is refused by
    the peer-cgroup check;
  - failover across managers;
  - `legion login` inside a task doesn't touch the identity token file.

**A3: apps**
- The reconciler:
  - seeding happens only once;
  - drift is reverted;
  - a manager upgrade rolls the bundled app;
  - disable stops the job, and uninstall revokes the grant;
  - a placement change re-renders permits and constraints.
- An `app config set` that changes a code field is refused for an `app:write`
  token lacking the app's grant, and accepted otherwise.
- Integration (`tests/integration/apps.rs`): scenario A on one manager with
  zero workers, and the §6.8 GitOps-shaped manifest installed (prompt,
  grant, reconciled job).

**A4: UI tier 1**
- The block-schema validator rejects unknown blocks and raw HTML.
- A viewer without the declared permission gets no forward: the app sees
  zero requests (asserted on a fake app).
- The app framework rejects a request with a missing, expired,
  wrong-audience, or context-mismatched assertion, including a direct call
  to the UI port.
- Actions are audited.
- Scenario C with a fake app declaring one view and one action, rendered by
  the SPA, `legion app view`, and `legion_app_view`.

**A5: UI tier 2** (Playwright with the pre-installed Chromium)
- The bridge refuses an RPC outside the allowlist.
- A mutating call shows the confirmation.
- The page cannot read `localStorage` or submit a form.

**A6: jobspec extensions and strict fields**
- Unknown fields:
  - a job, task, or nested-block typo fails with the right suggestion;
  - the `kind` error still reads as before;
  - with the flag, core unknowns are dropped, listed in `warnings`, and
    recorded on the version;
  - an unknown app block is stored `pending`;
  - a schema-invalid block for an installed app still fails.
- Extensions:
  - schema validation errors carry the JSON path, and defaults are filled
    into the stored block;
  - `requires` is enforced as the submitter;
  - the validate call: an error rejects, a warning passes, and a timeout or
    unreachable app accepts with a warning;
  - a resubmit that changes only an `apps` block moves allocations in place
    (same allocation ids, no rollout);
  - `WatchAppExtensions` returns only the caller's own blocks, and refuses
    other identities;
  - `legion job schema` contains each installed extension;
  - an install with a remote `$ref`, or with an example failing its own
    schema, is refused;
  - upgrade re-validation reports and flags failing blocks;
  - installing an app picks up its `pending` blocks;
  - uninstall needs `--force` and leaves blocks `orphaned`.

## 10. Rollout and compatibility

- **New default:** managers run a system worker. A cluster that wants none
  sets `systemWorker.enabled = false`. The PR states this as a deliberate
  behavior change: managers now consume up to the lend.
- **ACL glob semantics change** for reserved namespaces. No `legion-*`
  namespace exists in any cluster today, so this removes nothing anyone
  has.
- **`--run-worker`** keeps its meaning, implemented as the system worker
  with `namespaces = ["*", "legion-*"]` and no lend cap.
- **Reserved namespaces are unprivileged.** No existing job lives in one, so
  nothing breaks. The rule is stated in `docs/namespaces.md`.
- **Unknown jobspec fields become errors (A6).** This is the
  widest-reaching break in the plan. Any spec carrying a stray or misspelled
  key that is silently ignored today will fail to submit. The PR says so,
  the error names the key and suggests a fix, and `--allow-unknown-fields`
  restores today's behavior for anyone who needs time.
- **`controlPlaneBindings`** keeps working. The cert-renewer example and
  `docs/workload-identity.md` move to `permissions`.
- **Version skew.** An old worker doesn't provide the socket, so tasks
  scheduled on it fall back to the token file. Apps tolerate both, because
  the client tries `LEGION_ADDR`, and the worker sets it only when it
  provides the socket.
- **Docs, in the change that ships each phase:**
  - `docs/apps.md` (new): what an app is, the manifest reference, the
    lifecycle, the trust model, writing an app, the view-block reference,
    and the jobspec-extension reference;
  - `docs/workload-identity.md` (permissions, grants, the socket);
  - `docs/namespaces.md` (`legion-system`, node restrictions, the glob
    rule);
  - `docs/architecture-managers-workers.md` (the system worker);
  - `docs/ui-architecture.md` (extensions);
  - `docs/auth-and-acls.md` (the `app` resource, grants, the viewer
    assertion);
  - `docs/agent-readiness.md` (`legion_app_*` tools, extension schemas in
    `job schema`, `allow_unknown_fields`);
  - `docs/jobs-tasks-and-scheduling.md` (unknown fields, the flag, and the
    `apps` block);
  - the `README.md` and `docs/README.md` indexes.

## 11. Open questions

- **Should the system worker's lend scale with the host** (say 10% of the
  manager's CPU and memory) instead of a fixed 1 CPU / 1 GiB? Lean: fixed.
  It is predictable, and raft's needs don't scale with core count.
- **Tenant apps** (a team's app in its own namespace, UI visible only
  there). Lean: yes eventually, as the manifest's reserved `namespace`. The
  hard part is UI scoping, not the mechanism.
- **Should views be able to stream** (server-sent updates) instead of
  `refresh` polling? Lean: polling at ≥5s in tier 1. Tier 2 pages can poll
  through the bridge too.
- **Should `legion:self` be allowed for third-party apps** (a Legion-CLI
  script app)? Lean: yes. The CLI is a reasonable runtime for small apps,
  and it is the same binary the node already trusts.
- **Pre-approved grants per namespace**, so an app like GitOps doesn't have
  to hold the union of what its jobs request (§6.8). For example, a
  namespace policy listing the permissions jobs in it may request without
  the submitter holding them. Lean: worth designing in the GitOps plan,
  since GitOps is the app that makes it necessary. The grant check here is
  deliberately the simple version.
- **Can tier-2 pages be made unable to leak data?** Self-navigation can't
  be blocked reliably today. Lean: accept and document the limit (§6.5).
  Revisit if browsers ship navigation-limiting CSP (the withdrawn
  `navigate-to`), or if fenced frames become usable for this.
- **App-defined object kinds** (standalone, schema-validated objects an
  app owns, like Kubernetes CRDs). Lean: wait. Per-job settings belong in
  the job, and the one standalone kind needed so far (`ScalingPolicy`) is
  core. If a second app needs its own kind, the extension machinery
  (schemas in the manifest, validation in the manager, a watch stream)
  generalizes to stored objects with little new design.
- **Should GitOps set `--allow-unknown-fields` by default?** Lean: no. It
  should be per repo and off by default, because GitOps is exactly where a
  silently dropped field would go unnoticed longest.
- **Does the app reconciler belong to a broader "managed jobs" concept**
  (jobs owned by a controller, edits reverted)? Lean: not until a second
  owner besides apps exists.

## 12. Acceptance criteria

- **A1.**
  - On a fresh single manager with no workers, a `legion-system` job is
    placed on the manager.
  - A `default` job with `permit = pool=legion-system` is not placed, and
    `job plan` names the namespace restriction.
  - A `namespaces = ["*"]` tenant-admin token cannot `job run` or `exec`
    into `legion-system`, or write under `legion/` in KV.
  - A root exec task submitted to `legion-system` is refused.
- **A2.**
  - The rewritten cert-renewer runs with no manager-config change.
  - Submitting it with a token lacking `kv:write certs/*` fails with the
    documented error.
  - Inside the task, `legion kv get` works through the socket with the
    host guard active.
- **A3.**
  - `legion app list` on a fresh cluster shows the autoscaler running.
  - Upgrading the manager binary rolls it.
  - Installing the §6.8 manifest shows the permission prompt and records
    the grant.
  - Editing `legion-system/gitops` directly is reverted.
- **A4.**
  - The job page shows an app's panel only to viewers holding the declared
    permission.
  - An action by a viewer without it is refused before the app sees a
    request.
  - The same view renders with `legion app view` and `legion_app_view`.
- **A6.**
  - `autoscael = true` fails to submit with `did you mean autoscale?`.
  - The same spec with `--allow-unknown-fields` submits and reports the
    drop in the reply and in `job info`.
  - With a fake `backup` app installed, `apps.backup = { keep = 0 }` is
    rejected with the JSON path, and `{ schedule = "0 3 * * *" }` is stored
    with `keep = 7` filled in.
  - Changing `keep` on a running job doesn't restart it.
  - `legion job schema` shows `apps.backup` at task level.
- **A5.** A tier-2 page can call an allowlisted RPC as the viewer, is
  refused a non-allowlisted one, and cannot read the SPA's token
  (Playwright).
