# Legion Apps: system jobs on managers, granted identities, and UI extensions

> **Judgement calls:** [legion-apps-plan_calls.md](../calls/legion-apps-plan_calls.md)

**Status: PLANNED** (written 2026-10-19). Nothing is built. It has six phases
(§8), and each ships alone. The first consumer is the autoscaler
([autoscaler-v2-plan.md](autoscaler-v2-plan.md)). GitOps is the second, and
this plan checks the substrate against it (§6.7). Deliberately deferred:
installing apps from a registry (it waits on the registry plan), apps owned
by tenants in their own namespaces, and in-page JavaScript extensions that
share the operator UI's origin. That last one is rejected, not deferred
(calls entry 4).

This plan replaces the ecosystem-app approach in the superseded
[autoscaler-plan.md](autoscaler-plan.md) §6.6 and §6.8: the in-core loop
with a job as the escape hatch, and the link-only Apps sidebar.

---

## 1. Problem statement

`internal_docs/differentiators.md` and the TODO's "Ecosystem apps" section
promise an omakase cluster: an autoscaler, GitOps, a cert manager, and an
ingress, all simple on day one and splittable on day N. Each is a program
that **manages the cluster** rather than serving users. Today every one of
them hits the same five walls:

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
   - The kernel host guard drops a task's connections to the manager's API
     whenever the manager shares the node (`docs/network-handlers.md`
     § The host guard in the kernel). So the cert renewer's approach cannot
     work at all for a job running on a manager.
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

**App author** (someone writing an add-on: the autoscaler, GitOps, or a
third party)
- When I build an add-on, I want to ship one manifest that says how it runs,
  what it may do, and what it shows in the UI, so installing it is one
  command.
- When my add-on has state worth seeing, I want it to show up on the job and
  node pages where operators already look, without learning the SPA's
  framework or getting a PR into Legion.

**Security / platform admin**
- When someone grants a job permissions, I want that to be impossible beyond
  what they hold themselves, so `job:write` never quietly becomes
  `acl:write`.
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
gitops 0.2.0 runs in legion-system (on managers) and asks for:
  job   read, write   namespaces: team-*
  kv    read          paths: legion/apps/gitops/*
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

**Non-goals**

- **A registry.** `legion app install` takes a file or a URL. The registry
  plan adds names.
- **Tenant-owned apps** (apps living in a team's namespace, with UI scoped to
  it). Apps are cluster add-ons in `legion-system`. The manifest has a
  `namespace` field reserved for this.
- **In-origin JavaScript plugins** (Headlamp style). Rejected, not deferred
  (calls entry 4).
- **Serving the SPA from the manager.** Tier 1 and tier 2 both work with the
  SPA wherever it is served today.
- **A general taints-and-tolerations system.** The node-side `namespaces`
  restriction is one targeted rule. Pools already act as taints.

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
- **Worker-plane tunnel.** It is planned, not built:
  `Worker.ProxyTunnel` in the reverse-proxy plan (§6.2-6.6), which is the
  manager-to-allocation path a UI needs to reach an app's backend.

## 6. Proposed design

### 6.1 What an app is

An **app** is three things:

1. a **job** in the reserved `legion-system` namespace;
2. a **grant**: the permissions the job's `legion` identity holds;
3. **registrations**: UI slots, views, and actions.

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
      network = { ports = [{ name = "api" }] },
      service = { name = "autoscaler", port = "api", meta = { protocol = "http" } },
    },
  },
  permissions = [
    { resources = ["scale"], actions = ["read", "write"], namespaces = ["*"] },
    { resources = ["job"],   actions = ["read"],          namespaces = ["*"] },
    { resources = ["node"],  actions = ["read", "write"] },
    { resources = ["kv"],    actions = ["read", "write"], paths = ["legion/apps/autoscaler/*"] },
  ],
  ui = { ... },                           # §6.5
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
  entry 8).

**Node-side namespace restriction** (calls entry 6). Worker types gain
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
  attribute `legion.role = "manager"`. The `legion-system` namespace's
  stored policy sets `pools.default = "legion-system"`. Apps therefore land
  on managers with no permit written. Nothing else lands there: tenants are
  stopped by the namespace restriction and by the pool taint.
- Capacity is the `resources` lend, not the host. Resource enforcement caps
  each allocation, so all the apps together cannot exceed the lend. raft and
  the manager keep the rest of the box.
- One per manager, so apps fail over across managers. A single-manager
  cluster has one.
- `legion start manager --run-worker` keeps its meaning ("this manager also
  runs everything"). It now sets `systemWorker.namespaces = ["*"]` and
  joins the `default` pool and worker type, as before.
- `systemWorker.enabled = false` is the "keep managers clean" option.
  Bundled apps then run on workers (`placement = "any"` fallback), and
  `legion app list` notes the trade-off:

  ```
  autoscaler  running on wkr-3   note: no system worker; node scale-in will never choose wkr-3
  ```

**Placement as a setting, not a rebuild.** `legion app set autoscaler
--placement workers` re-renders the job's permit to `pool = default`, and
the reconciler resubmits it (a normal rollout). `managers` permits only
`legion-system`. `any` permits both, preferring managers by affinity.

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
  principal `legion:app-reconciler`. Edits made to an app's job directly
  are therefore reverted; `legion app` is the interface. For each disabled
  app it stops the job.
- The reconciler records a status: `running`, `degraded`, `pending (why)`,
  or `stopped`.

**Bundled apps run the node's own binary** (calls entry 9).
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
$ legion app install ./app.ncl | <url>   [--yes]
$ legion app upgrade gitops <url>     # shows the permission diff; new permissions need approval
$ legion app uninstall gitops         # stops the job, revokes the grant, drops UI registrations
```

**Config.**
- Config lives on the record. It is validated at `config set` against the
  manifest's Nickel contract, and rendered into the job as a file
  (`$LEGION_APP_CONFIG`, JSON).
- A config change resubmits the job. That is a normal rollout, so the job's
  version history is the config's history (calls entry 10).

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

**Granted only up to what the submitter holds.** At `RunJob`, the manager
checks that the submitting token is allowed every requested
(resource, action, scope).

- A requested glob is covered when the literal pattern string matches one
  of the submitter's allow globs. This test is sound for Legion's `*`-only
  globs: if `payments-*` as a string matches `pay*`, every namespace
  matching `payments-*` also matches `pay*`.
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
grants are recorded at seed time as granted by `legion:bootstrap`.

**Ambient: a local API socket** (calls entry 5).
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
- The worker knows exactly which allocation owns which socket, so
  attribution is exact (compare the datapath's caveat in
  `src/worker/netd/dns.rs:4-10`).
- The token file is still written (`$LEGION_SECRETS_DIR/legion.token`, with
  `LEGION_TOKEN_FILE` set) for clients that can't use a socket, such as
  HTTP clients talking to a manager the task can reach.
- Containers get the socket through the task-directory mount they already
  have. **Sandboxes** get it later, over the guest agent's vsock; until
  then they fall back to the token file plus `LEGION_ADDR` set to a
  reachable manager.

### 6.5 UI extensions

**Tier 1: declarative views** (calls entry 4). An app declares **slots** in
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

**One data path for the UI, CLI, and MCP** (calls entry 7).
- `ListUiExtensions` returns the slots the viewer may see, filtered by
  `app:read`. `GetAppView {app, view, context}` and
  `InvokeAppAction {app, action, context, inputs, idempotency_key}` are
  gRPC methods on the manager. The SPA calls them over gRPC-Web like any
  other RPC. The browser never talks to an app directly.
- **The manager authorizes before forwarding.** It checks `app:read` (or
  `app:write`, for actions), then evaluates the view's or action's declared
  `permission` *as the viewer* against the context's scope (`scale:write`
  on `payments-prod/api`, for example). Only then does it forward.
- **The forward** goes over the worker-plane tunnel to the app allocation's
  `api` port: `GET /legion/v1/views/<view>?…` or
  `POST /legion/v1/actions/<action>`. This is the reverse-proxy plan's
  `Worker.ProxyTunnel`; a unary subset is built here if that plan hasn't
  landed.
- **The viewer assertion.** The request carries a header
  `Legion-Viewer: <JWT>`, signed with the cluster's workload-identity key.
  Its `aud` is `app:<name>`, its `sub` is the viewer's principal, it
  carries the accessor id and the checked permission, and it lives 60
  seconds. Apps can verify it against the published JWKS for their own
  audit. They do **not** need to make authorization decisions, because the
  manager already did.
- **Actions.** Every action is recorded as an `app.action` event (app,
  action, viewer, context, outcome).
- **The terminal renders the same views**:
  `legion app view autoscaler job web` prints stats, tables, and timelines
  as text, and `-j` returns the JSON. The MCP tool `legion_app_view` returns
  the JSON. Whatever a human sees on a panel, an agent can read.

**Tier 2: sandboxed pages** (Phase A5), for apps whose UI doesn't fit the
blocks: an editor, a graph, or a log viewer, for example.
- The manifest declares `ui.pages = [{ title, bundle = "/legion/v1/page" }]`.
  The app serves a **single-file HTML bundle**.
- The SPA fetches it through `GetAppView`, so it is authorized the same
  way, and mounts it in an
  `<iframe sandbox="allow-scripts allow-forms" srcdoc=…>`.
- There is no `allow-same-origin`, so the page has an opaque origin. It
  cannot read the SPA's storage or cookies, or make credentialed requests
  to the manager.
- The SPA prepends a CSP (`default-src 'none'; script-src 'unsafe-inline';
  style-src 'unsafe-inline'; img-src data:; connect-src 'none'`), so the
  page cannot exfiltrate anything or load remote code.
- Everything else goes through a `postMessage` bridge:
  - `legion.call(method, request)` runs a Legion RPC **as the viewer**,
    limited to the methods the manifest lists in `ui.pages[].rpcs`;
  - `legion.view(name, ctx)` and `legion.action(name, inputs)` reuse the
    tier-1 path;
  - `legion.navigate(route)` moves the SPA;
  - `legion.theme` is the current theme.

  The page never sees a token. The only power it has is what the viewer
  has, narrowed again by its manifest.

### 6.6 Health and visibility

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

### 6.7 Checking the substrate against GitOps

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

Nothing GitOps needs is autoscaler-specific, and nothing here is
GitOps-specific. That is the test the substrate had to pass.

## 7. Proto, struct, config, and persistence changes

- **Proto**, every new RPC and field with public comments:
  - `ListApps`, `GetApp`, `InstallApp`, `UpgradeApp`, `UninstallApp`,
    `SetAppEnabled`, `UpdateAppSettings`, `SetAppConfig`;
  - `ListUiExtensions`, `GetAppView`, `InvokeAppAction`;
  - `ListGrants`, `RevokeGrant`;
  - `WorkloadIdentity` gains `repeated AclRule permissions` and
    `repeated string policies`;
  - `Node` gains `repeated string namespaces`.
- **ACL.** A new resource `app` (cluster-scoped, `names` selector). Every
  RPC above goes in `tests/integration/acl_endpoints.rs`. Glob matching
  gains the reserved-namespace rule, unit-tested in `src/structs/acl.rs`.
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
  - The per-allocation API socket proxy, a tonic service over a
    `UnixListener` that forwards with the allocation's token.
  - `LEGION_ADDR`, `LEGION_TOKEN_FILE`, `LEGION_SECRETS_DIR`, and
    `LEGION_APP_CONFIG` injection.
- **CLI and client.** `unix://` addresses; `legion app …`;
  `legion acl grants …`.
- **Config.**
  - `ManagerConfig.systemWorker` and `ManagerConfig.apps` (seed-only).
  - Worker type `namespaces`.
  - `configs/jobs/maximal.ncl`: `identities[].permissions` and
    `identities[].policies`.
  - `configs/apps/example-app.ncl`: a complete manifest with tier-1 UI.
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
  allowlist, and the CSP wrapper.
- **A6: from a registry**, once the registry plan exists:
  `legion app install @legion/gitops`.

## 9. Testing plan

- **Scheduler.**
  - Namespace-restricted nodes reject other namespaces even when the job
    permits their pool.
  - The wake pass does not wake a job onto a node that rejects its
    namespace.
  - `--run-worker` keeps today's behavior.
  - Benchmark: before and after numbers for `candidate_construction`.
- **ACL.**
  - Glob coverage table: sound cases, conservative refusals, deny rules.
  - The reserved-namespace rule: `*` does not match `legion-system`, and
    `legion-*` does.
  - Escalation: a token with `job:write` and no `kv:write` cannot submit a
    job requesting `kv:write`, and cannot resubmit the autoscaler's job.
  - Revocation takes effect on the next verification.
- **Socket.**
  - A host-mode task on a manager's system worker reaches the API through
    the socket while the host guard is active, and is refused by the host
    guard on the direct port.
  - The forwarded identity is the allocation's own, and two allocations on
    one node can't use each other's sockets (file mode, path).
  - Failover across managers.
- **Reconciler.**
  - Seeding happens only once.
  - Drift is reverted.
  - A manager upgrade rolls the bundled app.
  - Disable stops the job, and uninstall revokes the grant.
  - A placement change re-renders the permits.
- **UI.**
  - The block-schema validator rejects unknown blocks and raw HTML.
  - A viewer without the declared permission gets no forward: the app sees
    zero requests (asserted on a fake app).
  - Actions are audited.
  - Tier 2: the bridge refuses an RPC outside the allowlist, and the page
    cannot read `localStorage` (Playwright with the pre-installed
    Chromium).
- **Integration** (`tests/integration/apps.rs`):
  - scenario A on one manager with zero workers;
  - scenario C with a fake app that declares one view and one action;
  - the GitOps-shaped manifest from §6.7, installed and rendered.

## 10. Rollout and compatibility

- **New default:** managers run a system worker. A cluster that wants none
  sets `systemWorker.enabled = false`. The PR states this as a deliberate
  behavior change: managers now consume up to the lend.
- **ACL glob semantics change** for reserved namespaces. No `legion-*`
  namespace exists in any cluster today, so this removes nothing anyone
  has.
- **`--run-worker`** keeps its meaning, implemented as the system worker
  with `namespaces = ["*"]`.
- **`controlPlaneBindings`** keeps working. The cert-renewer example and
  `docs/workload-identity.md` move to `permissions`.
- **Version skew.** An old worker doesn't provide the socket, so tasks
  scheduled on it fall back to the token file. Apps tolerate both, because
  the client tries `LEGION_ADDR`, and the worker sets it only when it
  provides the socket.
- **Docs, in the change that ships each phase:**
  - `docs/apps.md` (new): what an app is, the manifest reference, the
    lifecycle, the trust model, writing an app, and the view-block
    reference;
  - `docs/workload-identity.md` (permissions, grants, the socket);
  - `docs/namespaces.md` (`legion-system`, node restrictions, the glob
    rule);
  - `docs/architecture-managers-workers.md` (the system worker);
  - `docs/ui-architecture.md` (extensions);
  - `docs/auth-and-acls.md` (the `app` resource, grants, the viewer
    assertion);
  - `docs/agent-readiness.md` (`legion_app_*` tools);
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
- **Does the app reconciler belong to a broader "managed jobs" concept**
  (jobs owned by a controller, edits reverted)? Lean: not until a second
  owner besides apps exists.

## 12. Acceptance criteria

- **A1.**
  - On a fresh single manager with no workers, a `legion-system` job is
    placed on the manager.
  - A `default` job with `permit = pool=legion-system` is not placed, and
    `job plan` names the namespace restriction.
  - A `namespaces = ["*"]` tenant-admin token cannot `job run` into
    `legion-system`.
- **A2.**
  - The rewritten cert-renewer runs with no manager-config change.
  - Submitting it with a token lacking `kv:write certs/*` fails with the
    documented error.
  - Inside the task, `legion kv get` works through the socket with the
    host guard active.
- **A3.**
  - `legion app list` on a fresh cluster shows the autoscaler running.
  - Upgrading the manager binary rolls it.
  - Installing the §6.7 manifest shows the permission prompt and records
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
