# Legion Apps: system jobs on managers, granted identities, and UI extensions

> **Judgement calls:** [legion-apps-plan_calls.md](../calls/legion-apps-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-19; reconciler and upgrade
details, jobspec extensions, and strict unknown fields added 2026-10-27).
**A1 shipped 2026-10-28** (the `legion-system` namespace, the reserved-glob
rule, node-side `namespaces`, and the system worker; docs:
[namespaces.md](../../docs/namespaces.md#reserved-namespaces),
[architecture-managers-workers.md](../../docs/architecture-managers-workers.md#the-system-worker)).
Where A1 differs from this text, calls entries 18-28 say how and why; the
largest differences are that a node's `namespaces` is a config attribute,
not a column (entry 19), and that the system worker has its own port and
state directory (entry 20). **A2 shipped 2026-10-28** (`permissions` and
`policies` on the `legion` identity, granted at submit up to what the
submitter holds; per-version grants with `legion acl grants list|revoke`;
the per-allocation API socket with ambient `LEGION_ADDR=unix://…`; and
registration-pinned node `namespaces` and `legion.role`; docs:
[workload-identity.md](../../docs/workload-identity.md#control-plane-identity),
[auth-and-acls.md](../../docs/auth-and-acls.md#job-grants)). Calls entries
29-41 record where A2 differs: a token resolves its grant through a
`grant:alloc/<id>` reference (entry 29), grants are per task (entry 33),
registration pins from the token for in-process workers (entry 32), and
sandboxes get the token file but no socket yet (entry 37). **A3 shipped
2026-10-28** (app manifests and Control-lane records; the app reconciler
with the `managedBy` digest stamp, per-transition idempotency keys, revert
of direct edits, restart of stopped jobs, backoff and parked refusals;
`legion:self`; the build marker from a new worker-reported `buildId`;
seeding with the `apps` block; the `app` ACL resource, eight RPCs,
`legion app`, app events, `legion_apps{status}`, and the MCP read tools;
docs: [apps.md](../../docs/apps.md)). Calls entries 42-57 record where A3
differs: the reconciler submits as an identity holding exactly the recorded
grant (entry 42), bundled apps cannot be uninstalled (entry 43), only
`artifact = true` code fields become artifacts (entry 46), and workload
identity stays off by default, so apps hold no credentials until it is on
(entry 56). A4-A6 are not built. It has six phases (§8),
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
  (`src/task_plugins/mod.rs:2134`), and `legion job schema` prints the
  merged schema so agents author against it (`docs/agent-readiness.md` §4).
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
7. Apps add per-job settings to the jobspec as natural, schema-validated
   keys on the job, a group, a task, a volume, a port, or a service. Unknown
   fields stop being silently dropped, and a flag allows them deliberately.

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
  Extension keys cover per-job settings. An app that needs its own kind of
  object waits for a second example (§11).

## 5. Background: what exists today

Line numbers refer to commit `0f5672a` (2026-10-19), where the recon was
done. `main` has moved since, so some have drifted, but the symbols are
named and the facts were re-checked after merging `main` on 2026-10-28.

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
- **Jobspec parsing.**
  - The job input and `TaskConfigInput` deserialize without
    `deny_unknown_fields` (`src/structs/job.rs:2404-2406`), so unknown keys
    there are dropped silently. The exception is `kind`, which has a
    hand-written error (`:2507-2521`).
  - Several nested blocks already refuse unknown keys: `VolumeSpec`
    (`:1777`), `StatefulConfig`, `StatefulStorageConfig`, `StopConfig`, and
    `DisruptionConfig`.
  - Tasks are parsed through an `#[serde(untagged)]` `TaskConfigList`
    (`:2397`), which replaces a task's own parse error with serde's generic
    untagged-enum message.
  - Task-runtime `config` blocks are validated against the runtime's JSON
    Schema with the `jsonschema` crate, version 0.17.1, which recompiles the
    schema on every call (`src/task_plugins/mod.rs:2132-2134`).
  - `legion job validate` and `legion job schema` run in the CLI, from
    schemas compiled into the binary (`src/cmd/job.rs:550,666`). There is
    no manager RPC for either.
- **In-place version moves exist only for KV re-renders.** `run_job`'s
  `Updated` branch always creates a rollout (`create_rollout_for_update`,
  `src/manager/server.rs:2451-2462`). The KV re-render path is the one place
  that moves running allocations onto a new version without a rollout. It
  checks `job_configs_schedule_equivalent` (built on
  `task_schedule_equivalent`, `src/storage/store.rs:21251`), then calls
  `migrate_allocations_to_job_config`, which is a separate Scheduler-lane
  write (`server.rs:4150-4180`).
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
  # No `version`: a bundled app's version is the build marker its job was
  # rendered for (§6.3). Third-party manifests set one.
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
  config = { schema = { ... }, default = { ... } },   # JSON Schema plus defaults, as for extensions (§6.6)
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
  entry 6).
- The same rule reserves the **`legion/` KV prefix**, where apps keep
  leases and state. A `paths = ["*"]` rule doesn't reach `legion/…`, only a
  pattern that starts with `legion/` does. Node-side `namespaces` lists
  (below) follow the rule too: `["*"]` on a node does not accept
  `legion-system`.

**Apps run unprivileged** (calls entry 5).
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

**Records.** An `app` table on the Control lane holds: name, source
(`bundled | file | url`), manifest JSON and digest, enabled, settings
(`replicas`, `placement`, `resources`), config, grant, installed-by, and
the reconciler's status (last digest, last action, last error).

**The reconciler** (calls entry 13) keeps each app's job matching its
record. It makes no decisions about what the app does. It runs only on the
Control-lane leader (the idempotency-prune loop pattern, `daemon.rs:426`),
and handles one app at a time, with at most one submit in flight per app.

- **Render.** For each enabled app, it renders the job, a pure function of
  the record and the version marker (below):
  - the manifest's `job`, with its name and namespace forced
    (`legion-system/<app>`);
  - `replicas` becomes `count`;
  - placement becomes permits and constraints (§6.2);
  - exec tasks run as `systemWorker.appUser`;
  - the `legion` identity carries the recorded grant;
  - the config is rendered to a file (`$LEGION_APP_CONFIG`);
  - plugin sources become artifacts;
  - for bundled apps, the version marker and its constraint.
- **Compare a digest, not the stored job.** The render's digest is stamped
  on the version the reconciler submits, as `managedBy = { app, digest }`.
  That is an internal field only the reconciler's own submits set; any
  other submit of the job clears it. A pass compares the fresh digest with
  the latest version's stamp:
  - equal means nothing to do;
  - different means the record changed, or someone submitted the job
    directly. Either way, the reconciler resubmits.

  It doesn't diff the rendered job against the stored one, because the
  stored config has defaults filled in and fields normalized, so a
  structural diff would find differences that aren't there.

  Which writes keep the stamp:
  - the reconciler's own submits set it;
  - a KV re-render of the app's job keeps it, because it re-renders the
    same spec (`server.rs:4108`);
  - `RunJob` and `RunJobTemplate` from any other principal clear it, and so
    does any other path that stores a new job version.

  The pass also checks the job's state, not just its spec. An enabled app
  whose job was stopped (someone ran `legion job stop`) is resubmitted, and
  a disabled app's running job is stopped.
- **Submit exactly once per transition.** The reconciler submits through
  the normal `RunJob` path as `legion:app-reconciler`, carrying the grant
  recorded on the app record (§6.4), with
  `idempotency_key = "app:<name>:<digest>:<from>"`.
  - `<from>` is the state the transition starts from: the job's latest
    version id, `none`, or `stopped-<version>`.
  - If the leader dies between the write and noticing it succeeded, the
    next leader's pass starts from the same state, sends the same key, and
    gets `deduplicated`, not a second version.
  - A *new* transition (reverting a direct edit, re-enabling within a day of
    a disable) starts from a different state, so it gets a new key.
  - The starting state has to be in the key because idempotency keys are
    global and kept for 24 hours (`IDEMPOTENCY_KEY_TTL`,
    `src/manager/daemon.rs:60`). A digest-only key would make the next
    revert of an identical edit a no-op.
  - Stopping a disabled app's job uses `app:<name>:stop:<from>`.
- **Triggers.** It is event-driven, with a backstop timer:
  - a write to an app record (install, upgrade, config, settings, enable,
    disable);
  - any change to an app's job (a new version or a stop), which is what
    catches direct edits within seconds;
  - acquiring Control-lane leadership, after which the new leader runs a
    full pass;
  - a node joining, leaving, or changing `buildVersion` or `buildId` among
    an app's eligible nodes (the build marker below);
  - a full pass every 60 seconds, in case an event was missed.

  Bursts coalesce: one pass per app at a time, and an event that arrives
  during a pass queues exactly one more.
- **Failures.**
  - A failed submit retries with backoff (5 seconds, doubling to 5
    minutes). The status becomes `degraded: reconcile failing: <error>`,
    and `app.reconcile_failed` is emitted on the first failure and then at
    most every 10 minutes.
  - A submit refused by validation (a schema, grant, or quota problem) does
    not retry on the timer. It waits for the next change to the app's record
    or to the cluster's version state, so a permanent error doesn't spin.
- **Drift.** A direct submit to an app's job clears the stamp, and its job
  change triggers a pass, so the edit is reverted within seconds:

  ```console
  $ legion job info legion-system/autoscaler
  ...
  managed by app autoscaler (digest 3f9a…); direct edits are reverted. Use `legion app`.
  ```

  The `app.reconciled` event names what happened: `config changed`,
  `version 0.14.0 → 0.15.0`, or `reverted a direct edit by alice`.
- **Status.** Each app is `running`, `degraded`, `pending (why)`, or
  `stopped`. `legion app status` shows the last action, the last error, and
  the digest.

**Bundled apps run the node's own binary** (calls entry 12).
`command = "legion:self"` is resolved by the exec runtime to the worker's
own executable (`std::env::current_exe()`). The app is always the same
build as the node that runs it: no artifact, no image, no registry, and it
works offline. Third-party apps use artifacts or images like any job.

**Upgrades roll a bundled app once its nodes agree on a new build** (calls
entry 14). A bundled app's code is the binary of the node it runs on, so the
reconciler restarts it only when there is one new build to move to:

- **The build.** Workers report `buildVersion` at registration
  (`server.rs:8679-8686`). This plan adds `buildId`, a hash of the worker's
  own executable computed at startup, because `buildVersion` is
  `CARGO_PKG_VERSION` (`src/worker/daemon.rs:3598`, `0.1.0` today), which
  doesn't change on a rebuild without a version bump.
- **The build marker** is the (`buildVersion`, `buildId`) pair, defined
  only when every ready node the app's placement allows reports the same
  pair.
  - For `placement = managers`, those nodes are the managers' own system
    workers.
  - Nodes reporting `unknown` or an unparsable version are left out of the
    marker, and out of placement by the constraint below.
- **While nodes disagree**, during an upgrade, the marker keeps its last
  value. So a straggler holds it, and an older node joining later doesn't
  move it backwards.
- **When the eligible set is on one build again**, the marker moves to it,
  and the reconciler rolls the app once. That covers a deliberate downgrade
  of the whole set too.
- **With no ready eligible node** (a fresh cluster, before its first
  system worker registers), there is no marker yet. The app is pending
  anyway.
- **The render** carries
  `{ key = "buildVersion", operator = "version_matches", value = ">=<marker version>" }`,
  so a reschedule never lands the app on a node older than its marker.
- **New bundled manifests.** A new build can change a bundled app's job,
  permissions, keys, or config schema, because the manifest is compiled
  into the binary.
  - The reconciler adopts the new manifest only when the leader itself runs
    the marker's build, since it can read only manifests compiled into its
    own binary. Until then it renders from the manifest stored on the
    record. For `placement = managers` the leader is one of the eligible
    nodes, so this holds as soon as the marker moves. A `workers` app waits
    for the leader to match.
  - Permission changes in the new manifest are recorded as granted by
    `legion:bootstrap`, like the seed (calls entry 17), and reported in the
    `app.upgraded` event and in `legion app status`.
  - Schema changes re-check stored values, as an install-time upgrade does
    (§6.6).

```
autoscaler  running  0.14.0  upgrade waiting: 2 of 5 eligible nodes on 0.14.0 (wkr-7, wkr-9)
```

Limits, stated in the public doc:
- **"Once" counts the reconciler's own restarts.** A manager restarting for
  its upgrade still kills the allocations on its system worker, and the
  scheduler places them again, possibly on a manager that hasn't upgraded
  yet.
- **Pre-releases.** `version_matches` uses semver requirements
  (`src/constraints.rs`), under which a pre-release of a newer version
  (`0.16.0-rc.1`) doesn't satisfy `>=0.15.0`. During a mixed window, such
  nodes can't host the app. Once every eligible node is on the pre-release,
  the marker moves to it and the constraint matches again.

Third-party apps carry their version in their artifacts and get none of
this, unless they use `legion:self` (§11).

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
$ legion app config set autoscaler -f autoscaler.ncl   # validated against the manifest's config schema
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
  manifest's config schema (JSON Schema, the same format and validator as
  extension keys, §6.6), and rendered into the job as a file
  (`$LEGION_APP_CONFIG`, JSON).
- A config change resubmits the job. That is a normal rollout, so the job's
  version history is the config's history (calls entry 15).
- A config change that alters the code the app runs (manifest-declared
  `codeFields`: for the autoscaler, `plugins.*.source` and `plugins.*.url`)
  needs the caller to hold the app's whole grant, as `install` does. Other
  config changes need only `app:write`. Without this rule, `app:write` on
  the autoscaler would be a path to `node:write` (calls entry 5).

**RPCs.** `ListApps`, `GetApp`, `InstallApp`, `UpgradeApp`, `UninstallApp`,
`SetAppEnabled`, `UpdateAppSettings`, and `SetAppConfig`. They are mutating
where appropriate, and every mutating one takes `idempotency_key`. ACL
resource **`app`** is cluster-scoped, with a `names` selector: `app:read`
lists and shows, `app:write` changes. MCP: `legion_app_list` and
`legion_app_status` (read-only), plus `legion_app_view` (§6.5).

### 6.4 Identity: declared permissions, granted at submit, ambient inside the task

**Declared on the job** (calls entry 4). The reserved `legion` identity
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
binary (calls entry 17).

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

### 6.6 Jobspec extensions: apps add their own keys

An app that acts on jobs usually needs settings per job: what to back up and
how often, which hostname to route, which repo a job came from. The natural
home is the jobspec, next to the thing it describes, versioned and reviewed
with it. So apps declare **keys** at **extension points** in the jobspec,
and job authors write them where they belong, like any other field (calls
entry 7):

```nickel
{
  name = "orders-db",
  uptime = { url = "https://orders.acme.dev/healthz", alert = "#payments-oncall" },   # job: the uptime app
  task = {
    name = "postgres",
    runtime = "container",
    config = { image = "postgres:17" },
    volumes = [
      { name = "data", path = "/var/lib/postgresql",
        backup = { schedule = "0 3 * * *", keep = 14 } },                              # volume: the backup app
    ],
    network = { ports = [{ name = "http", dns = { hostname = "orders.acme.dev" } }] },  # port: the dns app
  },
}
```

Values are written in Nickel, like the rest of the jobspec. `let` bindings,
functions, imports, and merging all work, because the CLI evaluates the
whole spec to JSON before submitting (`render_jobspec`,
`src/cmd/job.rs:1160-1170`), and stores the Nickel sources with the job for
`legion job spec`. The schema checks the evaluated value, not the Nickel
that produced it.

**Extension points.** Core decides *where* apps may add keys, and apps
decide *what* goes there:

| Point | Where the key is written | Context the app receives |
| --- | --- | --- |
| `job` | the top level of the jobspec | the job |
| `group` | on any one task of the group, applying to the whole group (the jobspec has no group objects; this is the same rule `autoscale` uses) | the job, the group, and its tasks |
| `task` | a task | the job, group, and task |
| `volume` | an entry in a task's `volumes` | the job, task, and volume name |
| `port` | an entry in a task's `network.ports` | the job, task, and port name |
| `service` | a task's `service` block | the job, task, and service name |

A key at `group` written on two tasks of the same group is a submit error.
Adding a point (say, artifacts) is a Legion change, not an app's: points are
where core promises to keep its own new fields from colliding (below).

**Declaring keys.** In the manifest:

```nickel
extensions = [
  {
    point = "volume",
    key = "backup",            # what job authors write; defaults to the app's name
    description = "Back up this volume on a schedule.",
    # JSON Schema (draft 7, the dialect schemars emits for task-runtime
    # `config` schemas), written as a Nickel record.
    schema = {
      type = "object",
      additionalProperties = false,
      required = ["schedule"],
      properties = {
        schedule = { type = "string", description = "Cron expression, in UTC." },
        keep     = { type = "integer", minimum = 1, default = 7, description = "Backups to retain." },
      },
    },
    examples = [{ schedule = "0 3 * * *", keep = 14 }],
    validate = true,           # also ask the app at submit, for checks a schema can't express (optional)
    # requires = { resource = "net", action = "write" },   # extra permission to set this key (optional)
  },
],
```

- An app declares at most one key per point. An app with several settings
  nests them in that key's record. `task` and `group` keys are both
  written on a task, so they count as one point: an app declares at most
  one of the two.
- The schema must describe an object, be at most 64 KiB, and use only local
  `$ref`s.
- It is compiled once, at install or upgrade, with the `jsonschema` crate
  that already validates runtime config, and cached by digest. The
  compiler is configured with a resolver that refuses remote refs, because
  the crate resolves them over HTTP by default.
- A schema that doesn't compile fails the install, and so does an `example`
  that doesn't validate, so the examples an app ships can't drift from its
  schema.
- The crate validates but doesn't fill `default`s. Legion fills them itself
  (below).

**Keys can't collide.** There are three rules, checked at install and at
Legion upgrades:

1. **Against today's core fields.** Installing an app whose key equals a
   core field at that point (`name`, `resources`, or `config` at `task`, for
   example) fails.
2. **Against other apps.** Installing an app whose key another installed app
   already declared at the same point fails, naming the owner. `task` and
   `group` count as one point here. The operator can install it under
   another key instead:

   ```console
   $ legion app install ./pgbackup.ncl
   error: pgbackup declares `backup` on volumes, which the backup app already owns
     hint: install it under another key: --key volume.backup=pgbackup
   ```

   Job authors then write `pgbackup = { … }`.
3. **Against core fields Legion adds later.** Every key also has a
   **qualified spelling** that can never collide: `apps.<app> = { … }` at the
   same point. On a volume, that is
   `{ name = "data", apps = { backup = { … } } }`. It always works.
   - If a Legion release adds a core field named like an app's key at that
     point, the core field wins. The manager logs the conflict at startup,
     `legion app status` lists it, and the release notes name it.
   - The manager can't tell, from one submit, whether the author means the
     new core field or the app's key. So a submit gets an error pointing at
     the qualified spelling only when the job's previous version stored that
     app's value under the short key. Every other submit takes the key as
     the new core field.
   - Stored values are untouched, because they are stored by app, not by
     key.
   - To make this rare, Legion checks new field names against the keys of
     first-party apps (and, once it exists, the registry's).

**Parsing.** Every jobspec input captures the keys it doesn't know in a
`#[serde(flatten)]` map, and validation decides what to do with them.
- **Why capture.** It replaces `deny_unknown_fields` where that exists
  today. `VolumeSpec` refuses unknown keys now, so a volume `backup` key
  needs the change. Serde can't combine `flatten` with
  `deny_unknown_fields`, and reject-or-keep has to be decided per request
  (`allow_unknown_fields`), not at compile time.
- **At an extension point**, a captured key that matches an installed key
  is validated, then stored **by app** (`extensions[<app>]`), so a rename
  (rule 2) or a later collision (rule 3) never orphans data. Any other
  captured key is an error, or pending under the flag.
- **In other blocks**, every captured key is an error, or dropped under the
  flag, so each unknown key has exactly one error path.
- **Tasks are parsed by shape** (a list or a map) instead of through the
  untagged `TaskConfigList`. That way a task's own error, with its
  did-you-mean, reaches the user, not serde's generic untagged-enum
  message.
- `legion job spec` shows the submitted Nickel. `legion job info` shows
  each value under the key the author wrote.

**At submit**, for each extension value, the manager:

1. **Finds the owner**: the installed app that declares this key, or this
   qualified `apps.<app>`, at this point. If there is none, the submit
   fails, or with `--allow-unknown-fields` the value is stored as pending
   (below).
2. **Validates the shape.** It applies the schema with the schema's
   defaults filled in. Values are **stored as written**. Defaults are filled
   on read, from the app's current schema, so `legion job plan`,
   `legion job info`, and the watch all show the effective value. An app
   upgrade that changes a default then reaches every value that didn't set
   it: the same live rule as `autoscale` defaults
   ([autoscaler-v2-plan.md](autoscaler-v2-plan.md) §6.3).
3. **Checks `requires`**, if the app declared it, as the submitter, on the
   job's namespace, when the value is new or changed.
4. **Asks the app**, if `validate = true`. It sends the expanded value and
   its context to `POST /legion/v1/extensions/validate`, using the same
   worker-plane forward and assertion as UI views (§6.5), with the
   submitter as the viewer, and a 2-second budget. The app returns errors,
   which reject the submit, or warnings, which pass through to the reply.
   **If the app can't be reached, the submit succeeds with a warning.**
   That is Kubernetes' failing-webhook lesson: a broken add-on must not
   block deploys. The app checks the value again when it arrives through
   its watch, and reports problems on its own status and views.
5. **Stores the value**, stamped `validated_by = <app>@<manifest digest>`.

```
error: orders-db: task postgres: volume data: backup.keep: 0 is less than the minimum of 1
error: orders-db: task postgres: volume data: backup (backup app): this volume is
  node-local, and backups need a volume that can be snapshotted
warning: orders-db: task postgres: volume data: backup was not checked by the backup app
  (unreachable); the schema check passed
```

**Changing only extension values restarts nothing.** A resubmit whose only
differences are extension values still creates a job version. A new check
in `run_job`'s `Updated` branch, which today always creates a rollout
(`src/manager/server.rs:2451-2462`), sees that only extension values
changed. It then moves the running allocations onto the new version
synchronously, the way KV re-renders do (`server.rs:4150-4180`), instead of
creating a rollout. The app sees the new value through its watch.

**How values reach the app.** `WatchAppExtensions { shards, shard_count }`
is app-plane: it returns only the calling app's own values. It sends the
current values first, then upserts and deletes of (job id, config version,
point, path, namespace, value as written, effective value, validity). The path names the group, task,
volume, port, or service. It shards by the same key hash as
`WatchScalingPolicies` ([autoscaler-v2-plan.md](autoscaler-v2-plan.md)
§6.9), so a sharded app needs only one lease scheme. Humans and agents see
the values in `legion job spec` and `legion job info`, like any other
field.

**Discoverability.**
- A new `GetJobSchema` RPC (`job:read`) returns the cluster's jobspec
  schema: the compiled-in core schema plus each installed key as a property
  at its point, described as, for example, "from the backup app, 0.3.1".
  - `legion job schema`, `legion job validate`, and `legion_job_schema`
    use it when a manager is reachable. An agent authoring a jobspec then
    sees exactly what this cluster accepts.
  - Offline they fall back to the compiled-in schema, as today, and say
    that installed keys weren't checked. That keeps
    `job_schema_tool_runs_without_manager` true.
- Did-you-mean suggestions include installed keys.
- Nickel users get the same keys at evaluation time once the planned
  importable contract library for the jobspec (`legion/jobspec.ncl`,
  `internal_docs/TODO.md` § Configuration language) exists. It is meant to
  be generated from the same source as `legion job schema`, so a cluster's
  installed keys belong in it too. Until then, a wrong value is caught at
  submit, not at evaluation.
- `legion app extensions [app]` lists every key, with its point,
  description, and examples.

**Lifecycle.**
- **Install and upgrade** register or replace an app's keys and schemas.
  The manager then checks every stored value for that app against the new
  schema, including values stored as pending under the flag, and reports
  the result in the prompt:

  ```
  14 jobs set backup on volumes; 2 fail this version's schema:
    orders-db/postgres/data: backup.schedule: required property missing
  ```

  Failing values stay stored, are flagged `invalid` in `job info` and on the
  app's watch (the app decides whether to act on them), and the next
  resubmit of those jobs must conform.
- **Disable** keeps the keys registered, so submits are still validated.
  Nothing acts on the values while the app is off, and `job info` says
  `app disabled`.
- **Uninstall** lists the jobs using the app's keys, and needs `--force` if
  there are any. The values stay stored, flagged `orphaned`. Resubmitting
  such a job fails unless the key is removed or `--allow-unknown-fields` is
  passed.

**Trust.** An extension value is an instruction, from whoever can submit
the job, to an app that holds its own grant.
- The app acts only on what carries the value: the job, task, volume, port,
  or service in its context. This is the same context rule as for actions
  (§6.5): the app framework hands the handler that object, not a free-form
  target, and schemas can't declare a field that names a different job.
- `requires` lets an app demand more than `job:write` from whoever sets its
  key. A DNS app could require `net:write`, so a team that can deploy can't
  also claim hostnames. It is checked on every submit that adds or changes
  the value.
- Values are visible to anyone with `job:read`, so they must never hold
  secrets. Schemas should take KV paths instead, and the public doc says
  so.
- Schemas are data, compiled once. No app code runs in the manager on
  submit; the only app code involved is the optional, time-boxed validate
  call, which runs in the app.

**Unknown fields are errors, unless you ask** (calls entry 3). Every key
that is neither a core field nor an installed app's key at that point
becomes a submit error, with a suggestion drawn from both. This applies at
the job level, the task level, and in every core nested block:

```
error: web: task web: unknown field `autoscael` (did you mean `autoscale`?)
error: orders-db: task postgres: volume data: unknown field `backup`: not a volume field,
  and no installed app declares a `backup` key on volumes
  hint: `legion app extensions` lists installed keys. Install the app, remove the key, or
  pass --allow-unknown-fields
```

`--allow-unknown-fields` accepts the spec anyway. It is
`allow_unknown_fields` on `RunJob`, `PlanJob`, and `RunJobTemplate`, a flag
on the CLI's `job validate`, and a per-repo setting for GitOps.
- **An unknown key at an extension point is kept**, stored unvalidated as
  `pending`. Legion can't tell a typo there from the key of an app that
  isn't installed yet, so it loses neither. When an app declaring that key
  at that point is installed, it validates and adopts the pending values
  (and its install prompt reports them). A pending value no app ever claims
  stays inert, and `job info` shows it.
- **An unknown key anywhere else is dropped**: inside `resources`, a
  `rollout` block, and so on. Nothing could ever own it there. Each dropped
  key is listed in the reply's `warnings`.
- **A value for an installed app's key that fails its schema is still an
  error.** The flag is about keys Legion doesn't know, not values it knows
  are wrong.
- What the flag dropped or left pending is recorded on the job version.

```console
$ legion job run orders-db.ncl --allow-unknown-fields
warning: task postgres: dropped unknown field `resources.memoryMax2`
warning: task postgres: volume data: kept `backup` as pending (no installed app declares it)
orders-db: submitted version 3
$ legion job info orders-db
...
submitted with --allow-unknown-fields: dropped resources.memoryMax2; backup on volume data
pending (no installed app declares it)
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
| Mark the jobs it deployed, and where they came from | §6.6: GitOps declares a job-level `gitops` key and sets `gitops = { repo, path, commit }` when it submits; its Sync panel reads it |

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
  - `WatchAppExtensions` (app-plane: only the caller's own values);
  - `bool allow_unknown_fields` on `RunJob`, `PlanJob`, and
    `RunJobTemplate`, with dropped and pending fields reported in the
    reply's `warnings`;
  - the job, task, volume, port, and service messages carry extension
    values;
  - a new `GetJobSchema` RPC (`job:read`): the core schema plus installed
    keys at their points;
  - `WorkerRegisterRequest` gains `build_id`, stored as the `buildId`
    dynamic attribute next to `buildVersion`;
  - `InstallApp` gains `key_overrides` (the `--key` rename);
  - `ListGrants`, `RevokeGrant`;
  - `WorkloadIdentity` gains `repeated AclRule permissions` and
    `repeated string policies`;
  - `Node` gains `repeated string namespaces`. *A1 adds
    `WorkerConfig.namespaces` instead; the node carries the list as a config
    attribute (calls entry 19).*
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
  - a `managedBy { app, digest }` field on job versions. It is set by the
    reconciler's submits, kept by KV re-renders, and cleared by any other
    write that stores a version;
  - reconciler status columns on `app` (last digest, last action, last
    error, the held version marker and its reason);
  - `node.namespaces` (JSON array). *A1 carries it as the node's
    `namespaces` config attribute instead (calls entry 19).*
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
  - Unknown keys are submit errors with a did-you-mean (the closest known
    key by edit distance, over core fields and installed keys).
  - Every input captures unknown keys with `#[serde(flatten)]`, replacing
    `deny_unknown_fields` on `VolumeSpec` and the other nested blocks that
    have it. At extension points (job, task, volume and port entries,
    service) captured keys resolve against the installed extensions, and
    the qualified `apps` object is accepted. Elsewhere they are errors, or
    dropped under the flag.
  - Tasks parse by shape instead of through the untagged `TaskConfigList`,
    so their errors reach the user.
  - Extension schemas are compiled once and cached by digest, with remote
    refs refused. Default-filling on read is Legion's own code.
  - Values are stored by app (`extensions[<app>]`), each with
    `validated_by` and a state (`valid`, `pending`, `invalid`, or
    `orphaned`).
  - The job version records what the flag dropped or left pending.
  - A new predicate in `run_job`'s `Updated` branch
    (`src/manager/server.rs:2451`), checked before
    `create_rollout_for_update`. If every difference from the previous
    version is in extension values, it moves the allocations synchronously
    with `migrate_allocations_to_job_config`, as the KV path does, instead
    of creating a rollout. `task_schedule_equivalent` is unchanged; it
    keeps serving the `job_submit` eval and stale-config repair.
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
  - `configs/jobs/maximal.ncl`: extension keys at the job, task, volume,
    and port points, plus one qualified `apps.<name>` example, all commented
    out, because they need the named apps installed and maximal must submit
    on a bare cluster.
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
  - Manifests, records, the reconciler, `legion:self`, seeding, and the
    build marker (which needs workers to report `buildId`).
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
  - **Extensions.**
    - Manifest `extensions` with keys at the job, group, task, volume,
      port, and service points.
    - The collision rules (core fields, other apps with the `--key` rename,
      and the qualified `apps.<app>` spelling).
    - Schema validation and defaults, and `requires`.
    - The validate call. It needs A4's forward; until then,
      `validate = true` is accepted and skipped with a warning.
    - `WatchAppExtensions`; the `GetJobSchema` RPC behind `job schema` and
      `job validate`; did-you-mean over installed keys.
    - The install, upgrade, and uninstall lifecycle, and in-place version
      moves.

    Needs A3.

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
  - an unchanged record makes no submit (the digests match), even though
    the stored config was normalized;
  - a direct submit is reverted within one pass, and the event names who
    made it;
  - the same direct edit made twice in a row is reverted both times (the
    second revert starts from a different version, so it isn't
    deduplicated);
  - disable, then enable within an hour: the job runs again;
  - a direct `legion job stop` of an enabled app's job is restarted;
  - a KV re-render of an app's job keeps the `managedBy` stamp, and a
    `RunJobTemplate` by another principal clears it;
  - a leader killed between the submit and recording it: the next leader's
    pass is `deduplicated`, and no second version appears;
  - a burst of record changes produces one submit per settled state;
  - a failing submit backs off, and a validation-refused submit doesn't
    retry until something changes;
  - disable stops the job, and uninstall revokes the grant;
  - a placement change re-renders permits and constraints.
- Upgrade ordering, with simulated nodes reporting `buildVersion` and
  `buildId`:
  - the marker is defined only when the eligible ready nodes agree;
  - a rolling upgrade of three managers produces exactly one reconciler
    restart, after the third;
  - a rebuild with an unchanged `buildVersion` and a new `buildId` rolls the
    app;
  - an older node joining later doesn't move the marker backwards;
  - a node reporting `unknown` is left out of the marker and of placement;
  - the constraint keeps a rescheduled app off an older node;
  - a straggler holds the marker, and status names it;
  - a new bundled manifest is adopted only when the leader runs the
    marker's build, and its new permissions are recorded as bootstrap
    grants.
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
  - a job, task, or nested-block typo fails with the right suggestion,
    including an installed app key as the suggestion, and a task-level typo
    shows the task's own error, not serde's untagged-enum message;
  - a volume still rejects an unknown key that no app declares, after
    `VolumeSpec` moves from `deny_unknown_fields` to capture;
  - `legion job validate` offline checks core fields and says installed
    keys weren't checked;
  - the `kind` error still reads as before;
  - with the flag, an unknown key at an extension point is kept `pending`,
    and an unknown key elsewhere is dropped and listed in `warnings`, both
    recorded on the version;
  - a schema-invalid value for an installed app's key still fails.
- Extensions:
  - each point (job, group via a task, task, volume, port, and service)
    resolves its keys, and a group key on two tasks of one group is
    refused;
  - the collision rules:
    - install is refused over a core field or another app's key, and over
      another app's `task` key for a `group` key of the same name;
    - `--key` renames, and the stored values follow the app, not the key;
    - the qualified `apps.<app>` spelling works at every point;
    - a simulated new core field: a job whose previous version used the
      short key for the app gets the qualified-spelling error, and a new job
      using the key gets the core field;
  - schema validation errors carry the full path (task, volume, field);
  - values are stored as written, defaults are filled on read, and an app
    upgrade that changes a default changes the effective value of every
    value that didn't set it;
  - an install whose schema uses a remote `$ref` is refused without any
    network request;
  - `requires` is enforced as the submitter;
  - the validate call: an error rejects, a warning passes, and a timeout or
    unreachable app accepts with a warning;
  - a resubmit that changes only extension values moves allocations in
    place (same allocation ids, no rollout);
  - `WatchAppExtensions` returns only the caller's own values, with their
    point and path, and refuses other identities;
  - `legion job schema` contains each installed key at its point;
  - an install with a remote `$ref`, or with an example failing its own
    schema, is refused;
  - upgrade re-validation reports and flags failing values;
  - installing an app adopts the matching `pending` values;
  - uninstall needs `--force` and leaves values `orphaned`.

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
  - `docs/jobs-tasks-and-scheduling.md` (unknown fields, the flag, and
    extension keys at each point);
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
- **More extension points.** Artifacts, rollout blocks, and namespaces
  themselves are candidates. Lean: add points only when an app needs one,
  since each is a promise about where core won't reuse names.
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
  - A rolling upgrade of three managers restarts it exactly once, after the
    last manager, on the new build.
  - Killing the Control leader in the middle of a reconcile leaves exactly
    one new job version.
  - Installing the §6.8 manifest shows the permission prompt and records
    the grant.
  - Editing `legion-system/gitops` directly is reverted.
- **A4.**
  - The job page shows an app's panel only to viewers holding the declared
    permission.
  - An action by a viewer without it is refused before the app sees a
    request.
  - The same view renders with `legion app view` and `legion_app_view`.
- **A5.** A tier-2 page can call an allowlisted RPC as the viewer, is
  refused a non-allowlisted one, and cannot read the SPA's token
  (Playwright).
- **A6.**
  - `autoscael = true` fails to submit with `did you mean autoscale?`.
  - The same spec with `--allow-unknown-fields` submits. Because a task is
    an extension point, `autoscael` is kept as `pending`, and both the reply
    and `job info` say so.
  - With a fake `backup` app declaring a volume key, a volume with
    `backup = { keep = 0 }` is rejected with the full path.
    `backup = { schedule = "0 3 * * *" }` is stored as written, and
    `job info` shows `keep = 7` filled in.
  - The same value written as `apps = { backup = { … } }` is accepted.
  - Changing `keep` on a running job doesn't restart it.
  - `legion job schema`, against the cluster, shows `backup` on volumes.
  - Installing a second app that declares `backup` on volumes is refused,
    and succeeds with `--key volume.backup=pgbackup`.
