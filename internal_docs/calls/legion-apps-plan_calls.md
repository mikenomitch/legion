# Judgement Calls: legion-apps-plan

Decisions made while designing
[`plans/legion-apps-plan.md`](../plans/legion-apps-plan.md). **Entries are
ordered by project impact, highest first, and an entry's number is its
rank.** These are design-time calls. Implementation inserts its own at their
rank. Autoscaler-specific calls are in
[autoscaler-v2-plan_calls.md](autoscaler-v2-plan_calls.md).

## 1. An app is an ordinary job plus a manifest, not a special job type

**Call.** Apps are jobs in `legion-system`. What makes them apps is around
the job:
- a manifest;
- an app record;
- a grant;
- UI registrations;
- a reconciler that owns the job.

The scheduler, runtimes, rollouts, logs, and exec treat them like any
other job.

**Alternatives.**
- (a) **The user's first framing.** A special job type that the manager
  runs itself, on manager nodes. Close to Kubernetes static pods, which the
  kubelet runs from disk without the scheduler.
- (b) **The superseded plan's shape.** Add-ons compiled into the manager as
  loops, with "escape to a job" as a second mode.

**Trade-off.**
- An app depends on the scheduler and a worker being able to place it. A
  scheduler bug that stops placement also stops the autoscaler. Static
  pods would survive that.
- Changes go through the normal rollout path, with its latency.
- The reconciler is a second writer of job specs, and its "edits are
  reverted" behavior has to be documented and visible.

**Why (simplicity and extensibility).**
- **Against (a).** A new job type touches every consumer of job state
  (scheduler, UI, ACL, status), permanently, to buy what two small,
  general rules already provide: a node-side namespace restriction (entry
  6) plus a pool. Everything apps need from "special" is a placement rule
  and a credential, not a lifecycle. The user's second message proposed
  exactly that ("always runs on a worker, and the manager can be a worker
  that only accepts certain namespaces"), and the investigation agrees.
- **Against (b).** It forks every add-on into two deployment modes and
  runs add-on logic, including user plugins, inside the manager process.
- **GitOps (plan §6.7).** It works with no app-specific code, which is the
  evidence that the cut is in the right place.

## 2. Managers host apps by default, through a capped, namespace-restricted system worker

**Call.**
- Every manager runs an in-process worker by default: worker type and pool
  `legion-system`, attribute `legion.role = "manager"`.
- It accepts only the `legion-system` namespace.
- Its capacity is a fixed lend (1 CPU / 1 GiB by default), not the host,
  enforced by `cpu.max` and `memory.max` on the system worker's cgroup
  parent.
- `systemWorker.enabled = false` turns it off. `--run-worker` keeps its old
  meaning by widening `namespaces` to `["*", "legion-*"]` and dropping the
  cap. App placement keys on `legion.role = "manager"`, not the pool, so
  apps still land on a `--run-worker` manager.

**Alternatives.**
- (a) Off by default: apps run on workers, and managers stay pure.
- (b) Run apps on managers only when there are zero workers.

**Trade-off.**
- Managers spend real CPU and memory on apps by default.
- An app bug (a hot loop, a leak up to its limit) shares a host with raft.
  The cgroup parent's `cpu.max` and `memory.max` bound it to the lend on
  Linux, even though per-allocation `cpu.weight` lets apps burst. On macOS
  there are no cgroups, so only the advertised capacity bounds it.
- It changes the default for every new cluster.

**Why (simplicity and correctness).** Three properties hold only on
managers:
- the app runs on a cluster with zero workers, so node autoscaling can start
  from nothing;
- it runs when every worker is full, which is the moment scale-up matters;
- it is never on a node its own scale-in might drain.

(a) breaks all three by default, and fixing them takes configuration on day
one, which the omakase goal forbids. (b) makes placement depend on cluster
shape, so the app moves at the worst moment (when the first worker joins
mid-scale-up). Kubernetes runs the cluster-autoscaler on control-plane
nodes for these same reasons.

## 3. A job's permissions are declared in its jobspec and granted only up to what the submitter holds

**Call.**
- The reserved `legion` identity gains `permissions` (inline rules) and
  `policies` (names).
- On every path that stores a job config (`RunJob`, `RunJobTemplate`), the
  manager checks that the submitting token holds each requested rule. Glob
  coverage is decided by string-matching the requested pattern against the
  held globs. Reserved prefixes are covered only by held patterns with the
  same prefix (entry 5), and deny rules force `acl:write`.
- Grants are stored per config version, with the granting principal, and
  listed and revoked with `legion acl grants`.
- Apps use the same check at `app install` and `upgrade`.

```nickel
identities = [{
  name = "legion",
  permissions = [{ resources = ["kv"], actions = ["read", "write"], paths = ["certs/*"] }],
}],
```

**Alternatives.**
- (a) Today's mechanism: `controlPlaneBindings` in manager config.
- (b) Nomad's: an operator attaches a policy to a job by name with a CLI
  command, outside the jobspec.
- (c) A request-and-approve queue: the job asks, an admin approves.

**Trade-off.**
- Permission grows with the person, not the role. Anyone with `job:write`
  plus `kv:write certs/*` can mint a job holding `kv:write certs/*`, and
  it keeps that grant after the person's own access is revoked (the same
  as Kubernetes RBAC), until someone runs `acl grants revoke`.
- The coverage check is conservative. Some legitimate grants need an
  admin to submit them.
- Every submit of an app job re-checks the grant, so only principals who
  hold the app's permissions can redeploy it. That is a feature, but it
  surprises someone with plain `job:write` on `legion-system`.

**Why (simplicity, inside a hard security boundary).**
- **Against (a).** Two-sided wiring that needs a manager restart, and its
  `legion.io/job` claim is the bare job name, which matches the same name
  in every namespace (`docs/workload-identity.md:39`).
- **Against (b).** One-sided, but the job's needs live outside the job.
- **Against (c).** The safest, but it puts a human in the loop of every
  deploy that touches Legion.

"You can grant what you hold" is Kubernetes' escalation-prevention rule.
It lets the jobspec be the whole story ("this job needs X") with no
privilege gained. Re-checking on every submit is what stops someone with
`job:write` on an app's namespace from laundering its grant by editing it.

## 4. Apps run unprivileged, and changing an app's code needs its whole grant

**Call.**
- Tasks in reserved namespaces may not run as root: exec tasks need a
  non-root `user` (bundled apps get `systemWorker.appUser`, default
  `nobody`), and privileged containers are refused.
- `app install`, `app upgrade`, and any `app config set` that changes a
  manifest-declared code field (the autoscaler's plugin sources) re-run the
  grant check against the caller.
- The install prompt states that the app runs its own code on the managers.

```
gitops 0.2.0 runs its own code (1 artifact) on your managers, as user nobody,
in legion-system, and asks for: ...
```

**Alternatives.**
- (a) Let apps run as root, like any exec task today
  (`docs/task-runtime-exec.md`: no `user` means root), and trust the
  installer.
- (b) Require containers or sandboxes for every app.
- (c) Treat `app:write` as plain configuration access, with no grant check
  on config.

**Trade-off.**
- Bundled apps need a usable unprivileged user on every manager host
  (`nobody` almost always exists, but the binary must be executable by
  it).
- An app that genuinely needs root, such as a future network controller,
  can't be an app in `legion-system` as designed.
- Operators with `app:write` but not the app's grant can still change
  settings, but can't add plugins.

**Why (security boundary).**
- **Against (a).** Root on a manager reads raft state and the data
  directory, and can delete the host-guard table and escape its cgroup.
  "Install an app" would silently mean "hand out cluster root".
- **Against (b).** It needs a container runtime on every manager, and would
  stop `legion:self` (entry 10) from working.
- **Against (c).** It makes `app:write` on the autoscaler a path to
  `node:write`, by adding a plugin that runs with the app's grant.

Running unprivileged keeps the blast radius of an app at its grant. The
code-field rule keeps the grant at what the person changing the code
already holds.

## 5. Reserved `legion-*` namespaces and the `legion/` KV prefix are not matched by `*`

**Call.** In ACL namespace selectors, a glob matches a reserved namespace
only if the pattern itself starts with `legion-`. `namespaces = ["*"]` covers
every tenant namespace, but not `legion-system`. The same rule reserves the
`legion/` KV prefix (app leases and state). It applies three ways:
- in rule evaluation;
- in node-side `namespaces` lists;
- in grant coverage, where a held `*` does not cover a requested `legion-*`.

**Alternatives.**
- (a) Plain glob semantics, and document "don't give tenants `*`".
- (b) A separate ACL resource for system jobs.

**Trade-off.** A special case in glob matching. An operator who really means
"everything" writes `["*", "legion-*"]`, and the policy doc must say so
prominently.

**Why (security boundary).** Tenant-admin policies written as
`namespaces = ["*"]` are common and reasonable. Under plain globs, they
would grant `exec:write` into the autoscaler's task, which holds a socket
authenticated as the autoscaler. That is `node:write` and cluster-wide
`scale:write` by the side door. Broad `kv:write` would likewise let a tenant
take the autoscaler's shard leases. And without the rule in grant coverage,
the same tenant admin could grant a job of their own `exec:write` on
`legion-*` and get the side door back. (b) duplicates every namespaced resource.
The "dotfiles skip `*`" rule is familiar from every shell, and it removes
nothing anyone has today, because no `legion-*` namespace exists yet.

## 6. UI extensions are declarative views first, sandboxed pages second, and never in-origin JavaScript (reverses old autoscaler-plan entry 13)

**Call.**
- **Tier 1** (Phase A4). Apps return JSON views built from a closed set of
  blocks (`stats`, `series`, `table`, `timeline`, `keyValue`, `notice`,
  sanitized `markdown`, `actions`). The SPA renders them in declared slots:
  `nav`, `overview`, `job`, `task`, and `node`.
- **Tier 2** (Phase A5). A single-file HTML page in a sandboxed `srcdoc`
  iframe with an opaque origin and a strict CSP. Its only power is a
  postMessage bridge that calls allowlisted RPCs as the viewer.
- No app code ever runs in the SPA's origin.

**Alternatives.**
- (a) **The superseded plan's choice.** Link-only "Apps" sidebar entries,
  with panels deferred to a future plan.
- (b) **Headlamp, OpenShift, Grafana.** JavaScript plugins loaded into the
  console's origin. Unlimited UI, admin-installed, fully trusted.
- (c) **Iframes only**, served from the reverse proxy's origin with cookie
  auth.

**Trade-off.**
- Tier-1 apps can't draw anything the block set doesn't have, and growing
  the block set is a Legion release.
- Tier-2 pages are single-file, can't load remote resources, and must route
  every byte of data through the bridge.
- An app author who wants a rich UI gets a worse developer experience than
  a Grafana plugin author.

**Why (security, and agent-friendliness as a bonus).**
- **Against (b).** The operator's bearer token is in `localStorage`, so any
  in-origin plugin is a full credential theft by design. Grafana had to
  invent plugin signing to live with that.
- **Against (a).** It fails the user's ask ("integrated with the main UI")
  outright. A link to a port the browser usually can't reach is not
  integration.
- **Against (c).** It breaks under third-party-cookie blocking, and needs
  the reverse proxy shipped first.

Declarative views are rendered by Legion's own components, so they match the
theme. Being data, they also render in the terminal and over MCP (entry 9),
which no JavaScript plugin model gives. The autoscaler and GitOps both fit
in tier 1, which suggests most operational apps do. Tier 2 is the safety
valve for the rest.

## 7. Inside a task, Legion is reached through a per-allocation API socket the worker provides

**Call.** An allocation with the `legion` identity gets
`$LEGION_SECRETS_DIR/legion.sock`, and `LEGION_ADDR` is set to its
`unix://` path. The worker proxies gRPC from it to the managers, attaching
the allocation's current identity token, and handles failover and TLS. It
accepts a connection only if the peer's cgroup (`SO_PEERCRED`) is inside
the allocation's own leaf. The token file is still written for clients that
can't use a socket, with its path in `LEGION_IDENTITY_TOKEN_FILE`. That is
not `LEGION_TOKEN_FILE`, which `legion login` writes to.

**Alternatives.**
- (a) **Kubernetes-style ambient env.** Inject a reachable manager address,
  the token file, and the CA, and let clients dial managers directly.
- (b) **The status quo.** The job author wires `LEGION_ADDR` and token
  delivery by hand, as `configs/jobs/cert-renewer.ncl` does.

**Trade-off.**
- A new proxy on the worker, on every API call from every such task.
- A failure mode of its own: a worker that can't reach managers fails the
  task's calls even if the task itself could.
- Sandboxes can't use it until a vsock path exists, so they fall back to
  (a).
- `unix://` support has to be added to the CLI and the client.

**Why (simplicity, forced by a real constraint).** Investigation found that
(a) does not work where apps run by default. The host guard drops allocation
sockets to the manager's ports whenever the manager shares the node
(`docs/network-handlers.md` § host guard). Gated allocations would also need
a policy hole to every manager.

The socket is the allocation's own resource, so it clears both. The worker
checks which allocation the connecting process belongs to, not just who
can open the path (tasks can share a uid), so attribution is exact. No
token ever has to sit in the task's environment.

This is Nomad's Task API, which exists for the same reasons. Choosing
(a) would have meant documenting `hostGuard.allow` holes for the default
deployment.

## 8. Nodes restrict namespaces from the node side, not through permits or namespace policies

**Call.** Worker types gain `namespaces = [...]`. The scheduler's candidate
filter, and the blocked-eval wake match, refuse a job whose namespace the
node doesn't accept, regardless of the job's permit.

**Alternatives.**
- (a) Use what exists: put managers in pool `legion-system` and rely on the
  pool taint. Every job needs a permit to land there.
- (b) (a) plus namespace policies restricting `pools.allowed` everywhere
  else.
- (c) A general taints-and-tolerations system with effects.

**Trade-off.**
- A new scheduler predicate on the hot path, which needs benchmark numbers.
- A second way to say "who may land here" alongside pools.
- No `PreferNoSchedule`-style soft form.

**Why (security boundary, then simplicity).**
- **(a)** Self-service: any job author can write the permit.
- **(b)** Namespace policies only intersect and have no negation, so it
  means restricting every glob except one, forever, and the next namespace
  created is unrestricted.
- **(c)** It is the right general system and 10× the surface, for one
  rule we need now.

A node-side namespace list can't be opted into by the job, is one line in a
worker type, and also gives operators dedicated nodes for any namespace
(`ci-*` on CI hosts). That is independently useful, which is how a phase
ships alone.

## 9. App views and actions go through manager RPCs, authorized as the viewer before anything reaches the app

**Call.** The SPA, the CLI, and MCP call `GetAppView` and `InvokeAppAction`
on the manager. The manager:
1. checks `app:read` (or `app:write`);
2. evaluates the manifest-declared `permission` *as the viewer* on the
   context's scope;
3. forwards over the worker-plane tunnel with a 60-second signed
   `Legion-Viewer` JWT;
4. records actions as `app.action` events.

Apps never authorize viewers themselves. They do have to **verify** the
assertion on every request (the app framework does it, and
`legion app test` checks it), and act only on the context it names. The UI
port is never a catalog service. Actions may only sit in slots that have a
context to check (`job`, `task`, `node`).

**Alternatives.**
- (a) Argo CD's model: the browser calls the app backend via a proxy, and
  the app authorizes using forwarded identity headers.
- (b) The app authorizes, and calls a `CheckPermission` RPC with the
  viewer's assertion.

**Trade-off.**
- Every panel load costs a manager hop plus a tunnel hop.
- A third-party app that skips assertion verification can be driven
  directly by any workload that can reach its port. Not advertising the
  port narrows that, `legion app test` catches it, and the app framework
  makes skipping it take effort, but it remains the app author's
  obligation.
- The manager carries app traffic.
- Permissions are only as fine-grained as one resource/action per view or
  action. An app that needs per-row authorization must still check the
  assertion itself.

**Why (security boundary and simplicity).** The confused-deputy risk is the
whole game here: an app holds a powerful identity, and a button pressed by a
read-only viewer must not run with it. Centralizing the check means every
app gets it right by construction, including third-party ones whose
authors never thought about it. The same RPC serving the SPA, the CLI
(`legion app view`), and MCP (`legion_app_view`) gives agents parity with
humans for free (AGENTS.md: agent-friendly surfaces).

## 10. Bundled apps run the node's own binary (`command = "legion:self"`)

**Call.** The exec runtime resolves `legion:self` to the worker's own
executable, so the autoscaler is `legion app run autoscaler` from the same
build as the node that runs it. A manager upgrade bumps the rendered
manifest's version, and the reconciler rolls the app.

**Alternatives.**
- (a) Download a versioned release binary as an artifact.
- (b) Ship a container image.

**Trade-off.**
- During a rolling upgrade, an app on an old worker runs old code against a
  newer manager. This is the same version skew any worker has, and it is
  covered by `docs/upgrades.md`.
- Bundled apps can't be upgraded independently of Legion.

**Why (simplicity).** There is nothing to download, pin, mirror, or keep in
sync, it works air-gapped, and the app is exactly as trusted as the binary
already running there. (a) needs a release host and per-arch checksums in the
manifest. (b) needs a container runtime on every manager. Third-party apps
use (a) or (b) like any job.

## 11. App config lives on the app record, and any change is a rollout

**Call.** `legion app config set` validates against the manifest's Nickel
contract, stores the config on the record, and the reconciler resubmits the
job with the config rendered as a file. There is no live reload. A change to
a manifest-declared code field (plugin sources) re-runs the grant check
against the caller (entry 4).

**Alternatives.**
- (a) Store config in KV and render it with the shipped KV-reactive file
  updates (`onChange = "update"`), giving live reload without a restart.
- (b) Apps read config with their own RPC, and watch it.

**Trade-off.** Every config change restarts the app. For the autoscaler that
means a cold-start window that pauses scale-in for one stabilization
period.

**Why (simplicity).**
- One path: settings, placement, plugins (which become artifacts and need a
  resubmit anyway), and config all change the same way.
- The job's version history *is* the config history, with rollback through
  the normal rollout tools.
- (a) splits the config's truth between KV and the job, with two kinds of
  change behaving differently.
- (b) asks every app author to write watch logic.

## 12. Manager-config `apps` settings apply only when the cluster is first seeded

**Call.** `apps = { autoscaler = { enabled = false } }` in manager config is
read once, when the first Control leader seeds app records on an empty
store. After that, `legion app` is the interface, and startup logs that the
block is ignored.

**Alternatives.**
- (a) Read it continuously, with the leader's config winning.
- (b) Have no manager-config surface at all.

**Trade-off.** It surprises config-as-code operators, because editing
`manager.ncl` later does nothing. Mitigated by the startup log, and by
GitOps managing apps eventually.

**Why (simplicity).**
- **Against (a).** Manager config can differ per manager, so "the leader's
  config wins" makes app state flip on leader election. That is the same
  per-manager drift `controlPlaneBindings` suffers from.
- **Against (b).** It leaves no way to bootstrap a cluster without the
  autoscaler, short of disabling it after it has already started.

Seed-only gives a deterministic day-one choice and a single source of truth
afterwards.
## 13. Bundled apps' grants are recorded by bootstrap, and skip the escalation check

**Call.** When the first Control leader seeds records for the apps compiled
into the binary, their manifests' permissions are recorded as granted by
`legion:bootstrap`. No principal is checked, because no human is
submitting. This is the only grant that bypasses the check, and only
manifests embedded in the binary can use it.

**Alternatives.**
- (a) Require the first operator to approve the autoscaler's grant
  (`legion app approve autoscaler`) before it runs.
- (b) Seed bundled apps disabled.

**Trade-off.** A fresh cluster runs a process holding `node:write` and
cluster-wide `scale:write` that nobody explicitly approved. Its trust rests
on being part of the same binary the operator chose to run.

**Why (simplicity).** (a) and (b) both break "it's just there" on day one,
which is the omakase promise. And the binary is already trusted with far
more than this grant: it *is* the manager. An operator who doesn't want the
autoscaler seeds it disabled (`apps = { autoscaler = { enabled = false } }`)
or runs `legion app disable`.

