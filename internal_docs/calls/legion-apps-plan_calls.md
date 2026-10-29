# Judgement Calls: legion-apps-plan

Decisions made while designing
[`plans/legion-apps-plan.md`](../plans/legion-apps-plan.md). **Entries are
ordered by project impact, highest first, and an entry's number is its
rank.** Entries 1-17 are design-time calls. Implementation calls start at
18 (phase A1, entries 18-28) and continue at 29 (phase A2, entries 29-41),
42 (phase A3, entries 42-57), 58 (phase A6a, strict fields, entries
58-67), 68 (phase A4, UI tier 1, entries 68-79), 80 (phase A5, sandboxed
pages, entries 80-84), 85 (phase A6b, jobspec extensions, entries 85-94),
and 95 (A1 follow-ups, entries 95-97), appended after the design calls, since none outranks them; each
phase's entries are ranked among themselves. Entry 98 records the A4
follow-up that made actions run as the viewer
([app-actions-as-viewer-plan.md](../plans/app-actions-as-viewer-plan.md)),
whose own calls are in
[app-actions-as-viewer-plan_calls.md](app-actions-as-viewer-plan_calls.md).
Entries 99-110 are the hardening after an adversarial security review of
the apps subsystem, ranked among themselves (the two critical holes first,
the accepted design-level risks at 105-106); they revise entries 5, 42, 80,
81, and 93, and supersede 55, each of which says so. Entries 111-129 are
the hardening after an adversarial correctness and operability review,
ranked among themselves; entry 111 supersedes entry 13's reverting of
direct edits, and 114 revises 51.
Autoscaler-specific calls are in
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
  10) plus a pool. Everything apps need from "special" is a placement rule
  and a credential, not a lifecycle. The user's second message proposed
  exactly that ("always runs on a worker, and the manager can be a worker
  that only accepts certain namespaces"), and the investigation agrees.
- **Against (b).** It forks every add-on into two deployment modes and
  runs add-on logic, including user plugins, inside the manager process.
- **GitOps (plan §6.8).** It works with no app-specific code, which is the
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

## 3. Unknown jobspec fields are errors by default, and `--allow-unknown-fields` opts out

**Call.**
- Every unknown key in a jobspec fails the submit, with a did-you-mean
  suggestion.
- `--allow-unknown-fields` (`allow_unknown_fields` on the job RPCs) accepts
  the spec anyway:
  - an unknown key at an extension point is kept, stored unvalidated as
    `pending`, and adopted when an app declaring it is installed;
  - an unknown key anywhere else is dropped and reported;
  - a value that fails an installed app's schema is still an error.
- What the flag dropped or left pending is recorded on the job version and
  shown in `job info`.

```
error: web: task web: unknown field `autoscael` (did you mean `autoscale`?)
$ legion job run web.ncl --allow-unknown-fields
warning: task web: kept `autoscael` as pending (no installed app declares it)
```

**Alternatives.**
- (a) Keep today's behavior: unknown keys are ignored
  (`TaskConfigInput` has no `deny_unknown_fields`).
- (b) Strict, with no escape hatch.
- (c) Accept everything, with warnings.

**Trade-off.**
- A breaking change. Every spec carrying a stray key fails until it is
  fixed or the flag is passed.
- Implementation churn in the parser. `VolumeSpec` and four other nested
  blocks trade `deny_unknown_fields` for a capture map (serde can't combine
  it with `flatten`, and the choice has to be made per request). Tasks stop
  parsing through an untagged enum, which today hides their errors.
- Under the flag, a key dropped from a non-extension block still loses
  intent, because Legion can't store a field it doesn't model there. Keys
  at extension points are kept, so a typo there lingers as an inert pending
  value until someone notices it in `job info`.
- A flag used habitually (a GitOps repo that always sets it) gives back
  most of what strictness buys. Recording drops on the version, and
  showing them in `job info`, makes that visible, but doesn't prevent it.

**Why (simplicity: what you wrote is what runs).**
- **Against (a).** The silent drop is the worst failure in the system: it
  reports success and does nothing. `autoscale = true` sent to a manager
  that predates it would deploy without autoscaling, and nobody would know
  until the traffic spike.
- **Against (c).** Warnings in a reply nobody reads in CI are (a) with
  extra steps.
- **Against (b), the user's call.** There are real cases where submitting
  anyway is right: a spec written for a newer Legion during an upgrade;
  GitOps syncing a job before the app it configures is installed; moving
  jobs between clusters with different apps. A deliberate, recorded flag
  serves them without making the default unsafe.
- **Keeping unknown keys at extension points.** They are stored rather
  than dropped because they may have a future owner. Installing the app
  later picks them up with no resubmit. Storing a typo is the cheaper
  mistake.

## 4. A job's permissions are declared in its jobspec and granted only up to what the submitter holds

**Call.**
- The reserved `legion` identity gains `permissions` (inline rules) and
  `policies` (names).
- On every path that stores a job config (`RunJob`, `RunJobTemplate`), the
  manager checks that the submitting token holds each requested rule. Glob
  coverage is decided by string-matching the requested pattern against the
  held globs. Reserved prefixes are covered only by held patterns with the
  same prefix (entry 6), and deny rules force `acl:write`.
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

## 5. Apps run unprivileged, and changing an app's code needs its whole grant

> **Revised by entries 100 and 101** (hardening): apps also may not hold
> anything host-level, and each runs as its own uid; `systemWorker.appUser`
> is gone.

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
  stop `legion:self` (entry 12) from working.
- **Against (c).** It makes `app:write` on the autoscaler a path to
  `node:write`, by adding a plugin that runs with the app's grant.

Running unprivileged keeps the blast radius of an app at its grant. The
code-field rule keeps the grant at what the person changing the code
already holds.

## 6. Reserved `legion-*` namespaces and the `legion/` KV prefix are not matched by `*`

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

## 7. Apps declare natural keys at fixed extension points, with `apps.<app>` as a qualified spelling that never collides (revised; the first draft was namespaced-only)

**Call.**
- An app's manifest declares keys at extension points that core defines:
  the job, a group (written on one of its tasks), a task, a volume entry, a
  port entry, or a service.
- Each key has a JSON Schema, optional `examples`, an optional `validate`
  call that fails open, and an optional `requires` permission.
- Job authors write the key where it belongs, and the manager validates it
  at submit, fills its defaults, and stores the value by app.
- Collisions are refused at install, against core fields and against other
  apps (`task` and `group` keys count as one point, since both are written
  on a task); an operator can rename with `--key`.
- `apps.<app> = { … }` at the same point is a qualified spelling that
  always works, including after a later Legion release adds a core field
  with the same name. Only a job whose previous version used the short key
  for the app is told to switch; any other use of the name is the new core
  field.
- Values are stored as written, and schema defaults are filled on read
  from the app's current schema. This is the same live rule as `autoscale`
  defaults (autoscaler calls entry 3).
- The app receives its values through `WatchAppExtensions`. A change to
  only extension values moves allocations in place, with no rollout.

```nickel
volumes = [{ name = "data", path = "/var/lib/postgresql", backup = { schedule = "0 3 * * *" } }]
```
```
error: orders-db: task postgres: volume data: backup.keep: 0 is less than the minimum of 1
```

**Revision.** The first draft of this entry (written the same day, never
built) required every app setting to live in one namespaced block,
`apps.<name>`, at job or task level only. It rejected top-level keys,
alternative (d) below, because of collisions. The user asked for settings
that "feel more natural", including keys on groups and elsewhere. The
collision argument turned out to be answerable: install-time checks, the
`--key` rename, and the qualified spelling as the permanent fallback. So
this entry now records the reversal.

**Alternatives.**
- (a) **Free-form `meta`.** Nomad's `meta`, Kubernetes annotations, Docker
  labels: a string map apps read by convention.
- (b) **App-defined object kinds** that reference jobs. This is Kubernetes'
  CRD shape (an HPA next to its Deployment).
- (c) **Nickel contracts evaluated by the manager.** They are more
  expressive (cross-field checks, computed defaults), and Nickel is already
  Legion's config language.
- (d) **Namespaced blocks only** (`apps.backup = { … }`), the first draft.
  It is predictable and collision-free by construction.
- (e) **Keys anywhere.** Let apps choose any path in the jobspec, not just
  core-defined points.

**Trade-off.**
- **Collisions are now possible.** Install refuses them, but two clusters
  with different apps owning `backup` read the same file differently. The
  qualified spelling pins the owner when that matters.
- **A future core field can shadow a key.** Specs using the short key then
  fail with a pointer to the qualified spelling: a migration, though not a
  silent one.
- **Unknown keys are ambiguous.** Under the flag, an unknown key at an
  extension point might be a typo or an uninstalled app's key, so it is
  kept as pending rather than dropped (entry 3).
- **JSON Schema** can't express most cross-field or cluster-aware checks.
  Those go to the validate call, which is best-effort by design.
- **Validity depends on the cluster.** Whether a jobspec is valid now
  depends on which apps are installed (mitigated by
  `--allow-unknown-fields`, and by `job schema` showing the cluster's own
  schema).
- **Values share their job's lifecycle and visibility.** Settings that need
  their own lifecycle don't fit.

**Why (simplicity for the job author, extensibility for the app author).**
- **For natural keys.** A setting that reads like part of the job, sitting
  next to what it configures (a backup schedule on the volume it backs up,
  a hostname on the port it routes), is easier to write and review than the
  same setting parked under `apps` at task level.
- **Against (d).** It reads as plumbing, and can't attach to a volume or a
  port.
- **Against (e).** It would make every core block a potential collision
  site. Fixed points keep the promise small: core avoids new field names
  only at those points, and checks them against known app keys.
- **Against (a).** Stringly typed, unvalidated, and a typo is inert: the
  exact problem entry 3 exists to fix.
- **Against (b).** It splits one service's settings across files, and
  needs object storage, ACLs, and lifecycle per kind.
- **Against (c).** It puts a Nickel evaluation, on the bounded 5-second
  template threads (`docs/job-templates.md` § Rendering is sandboxed), in
  the submit path of every job carrying an extension. Its contracts can't
  be exported to the JSON Schema that `legion job schema`, editors, and
  agents consume.
- **For JSON Schema.** It is exactly what task runtimes already do for
  their `config` block (`src/task_plugins/mod.rs:2134`), in the draft 7
  dialect schemars emits. Values are still written in Nickel; only the
  evaluated result is checked. Two things the existing use doesn't do must
  be built: refusing remote `$ref`s (the crate resolves them over HTTP by
  default), and filling `default`s (the crate only validates).

## 8. UI extensions are declarative views first, sandboxed pages second, and never in-origin JavaScript (reverses old autoscaler-plan entry 13)

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
theme. Being data, they also render in the terminal and over MCP (entry 11),
which no JavaScript plugin model gives. The autoscaler and GitOps both fit
in tier 1, which suggests most operational apps do. Tier 2 is the safety
valve for the rest.

## 9. Inside a task, Legion is reached through a per-allocation API socket the worker provides

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

## 10. Nodes restrict namespaces from the node side, not through permits or namespace policies

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

## 11. App views and actions go through manager RPCs, authorized as the viewer before anything reaches the app

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

## 12. Bundled apps run the node's own binary (`command = "legion:self"`)

**Call.** The exec runtime resolves `legion:self` to the worker's own
executable, so the autoscaler is `legion app run autoscaler` from the same
build as the node that runs it. The reconciler restarts it when the nodes
it can run on move to a new build (entry 14).

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

## 13. The reconciler is event-driven, compares a digest stamped on the job, and submits with an idempotency key

> **Superseded in part by entry 111** (hardening): direct writes to an
> installed app's job are refused, not reverted (alternative (c) below);
> a version that reaches the job some other way is still reverted.

**Call.**
- The reconciler renders each app's job as a pure function of the app
  record and the build marker, then compares the render's digest with a
  `managedBy = { app, digest }` stamp on the job's latest version. The
  stamp is set by the reconciler's submits and kept by KV re-renders. Any
  other write that stores a version clears it. The reconciler also checks
  the job's state: a stopped job for an enabled app is restarted.
- It submits with `idempotency_key = "app:<name>:<digest>:<from>"`, where
  `<from>` is the job state it starts from.
- It runs on events (record writes, changes to app jobs, leadership
  acquisition, node version changes), plus a 60-second full pass, and
  coalesces bursts.
- A direct edit to an app's job is reverted within seconds, and the event
  names who made it:

  ```
  $ legion job info legion-system/autoscaler
  managed by app autoscaler (digest 3f9a…); direct edits are reverted. Use `legion app`.
  app.reconciled  autoscaler  reverted a direct edit by alice (version 7 -> 8)
  ```

**Alternatives.**
- (a) Diff the rendered job against the stored job config, which is what
  the first draft said.
- (b) A fixed timer only (the addon-manager style).
- (c) Refuse direct submits to app jobs instead of reverting them.

**Trade-off.**
- A new internal field on job versions, and its "cleared by any other
  submit" rule must hold on every path that stores a job config.
- Event plumbing to maintain.
- Reverting means someone's direct edit appears to work for a few seconds
  before it vanishes. The `job info` banner and the `app.reconciled` event
  are the only signals.

**Why (correctness and simplicity).**
- **Against (a).** The stored config is normalized (defaults filled, fields
  canonicalized), so a structural diff sees phantom changes and resubmits
  forever.
- **Against (b).** A timer alone either reacts slowly to `legion app config
  set` or burns passes. Events make the common case immediate, and the
  timer stays as the backstop.
- **The idempotency key.** It is what makes a leader failover between
  "submit" and "noticed it succeeded" harmless: the new leader's pass
  starts from the same state, and is `deduplicated`.
- **Why the starting state is in the key.** Keys are global and kept for
  24 hours. A digest-only key would make the second revert of an identical
  direct edit, or a re-enable within a day of a disable, a silent no-op.
- **Against (c), for now.** Refusing is arguably clearer feedback, and
  remains a reasonable later change. Reverting matches what operators of
  Kubernetes' addon-manager expect, and keeps an admin's emergency hand
  edit possible for the seconds it takes to notice. We'd switch to (c) if
  reverted edits cause confusion in practice.

## 14. Bundled apps roll once the nodes they can run on agree on a new build, not when the leader upgrades

**Call.**
- A bundled app's build marker is the (`buildVersion`, `buildId`) pair that
  every ready node its placement allows agrees on. `buildId` is a new hash
  of the worker's executable, because `buildVersion` is `CARGO_PKG_VERSION`
  and doesn't change on a rebuild without a version bump.
- While the nodes disagree, the marker keeps its last value. It never moves
  backwards because an old node joined.
- The render carries a `buildVersion version_matches ">=<marker>"`
  constraint, so a reschedule never lands the app on a node older than its
  marker.
- The app rolls when the marker moves. The new bundled manifest is adopted
  then, once the leader runs that build.

```
autoscaler  running  0.14.0  upgrade waiting: 2 of 5 eligible nodes on 0.14.0 (wkr-7, wkr-9)
```

**Alternatives.**
- (a) Roll when the leader's own version changes (the first draft).
- (b) Make it a manual step: `legion app upgrade autoscaler` after a
  cluster upgrade.
- (c) Ship bundled apps as versioned artifacts, decoupled from node builds.
- (d) Use the oldest `buildVersion` among eligible nodes as the marker
  (this entry's previous revision, superseded the same day). It rolled the
  app *backwards* when an older node joined, never rolled on a rebuild
  without a version bump, and had no answer when no eligible node was
  ready.

**Trade-off.**
- One straggler node holds every bundled app on the older build, possibly
  for a long time. That is within the version-skew policy, and status names
  the straggler, but the app's version is no longer "the manager's
  version", which is less obvious.
- A new trigger (node version changes) for the reconciler, and a new
  registration field (`build_id`).
- "Once" means once per upgrade *from the reconciler*. A manager restarting
  for its own upgrade still kills the allocations on its system worker, and
  the scheduler may place them on a manager that hasn't upgraded yet.
- Semver's pre-release rule keeps nodes on a newer pre-release from
  satisfying the constraint during a mixed window.

**Why (simplicity and correctness).**
- **Against (a).** The app's code is whatever binary its node runs
  (`legion:self`), so rolling when the leader upgrades restarts the app once
  per upgraded manager. It can even land the "new" version on a manager
  that hasn't upgraded yet, running old code under a new marker.
- **Against (b).** It adds an operator step to every upgrade, and the
  omakase goal forbids that.
- **Against (c).** It gives up `legion:self`'s nothing-to-download property
  (entry 12).

Requiring agreement, rather than taking the oldest version, is what keeps a
late-joining older node from rolling the app backwards. `buildId` is what
makes development rebuilds roll at all.

## 15. App config lives on the app record, and any change is a rollout

**Call.** `legion app config set` validates against the manifest's config
schema (JSON Schema, like extension keys), stores the config on the record,
and the reconciler resubmits the job with the config rendered as a file. There is no live reload. A change to
a manifest-declared code field (plugin sources) re-runs the grant check
against the caller (entry 5).

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

## 16. Manager-config `apps` settings apply only when the cluster is first seeded

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
## 17. Bundled apps' grants are recorded by bootstrap, and skip the escalation check

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


## 18. The reserved-prefix rule only ever narrows: deny rules keep plain globs, a missing selector reaches what `*` reaches, and namespace policies follow it too (A1)

**Call.** Implementing entry 6:
- Only **allow** rules use the reserved match. A deny rule's `namespaces`
  and `paths` globs stay plain, so a deny on `*` still denies inside
  `legion-system`.
- An allow rule with **no** selector where one could apply reaches what `*`
  reaches: a `resources = ["*"]` rule with no `namespaces`, and a `kv` rule
  with no `paths`, stop short of `legion-*` and `legion/` too.
- **Namespace-policy selectors** follow the rule: a `*` policy does not
  govern `legion-system`.
- A denial inside a reserved name adds a hint that says why:

```text
permission denied: job:write requires a policy granting it in this namespace
  Hint: this token can write job in *
  Hint: `*` does not reach reserved `legion-*` namespaces; a rule must name them,
  as `namespaces = ["legion-system"]` or `["*", "legion-*"]`
```

**Alternatives.**
- (a) Apply the reserved match to every glob, deny rules included.
- (b) Treat a missing selector as "everything", reserved names included
  (the literal reading of "the rule is about patterns").
- (c) Leave namespace policies on plain globs, as the plan's text says
  nothing about them.

**Trade-off.**
- A `resources = ["*"], actions = ["*"]` policy with no selectors is no
  longer quite "everything": it misses `legion-system` and `legion/`. Such a
  policy holds `acl:write`, so its holder can widen it, but it is a surprise
  to explain.
- One more rule to learn for policy authors: "`*` skips `legion-` in
  policies too".

**Why (security boundary, then simplicity).**
- **Against (a).** A deny that matches less grants more. The rule exists to
  stop broad grants reaching Legion's own jobs; letting it shrink a deny
  would turn it into an escalation path.
- **Against (b).** The broad KV grant people actually write is a `kv` rule
  with no `paths`. Under (b) it would reach the autoscaler's leases, which
  is the exact hole entry 6 closes.
- **Against (c).** A tenant-wide policy with `pools.default` (common) would
  govern `legion-system` as well and disagree with its own `pools.default =
  "legion-system"`, making every pool-less `legion-system` job
  unadmittable. A tenant quota would also start charging Legion's jobs.

## 19. A node's `namespaces` rides as a config attribute the worker reports, not a node column and a `Node` proto field (deviates from plan §7)

**Call.** Worker types store `namespaces` (a new `worker_type` column and
`WorkerConfig.namespaces`, so pre-registration hands it to the worker). The
worker writes it into its registration's config attributes as
`namespaces = [...]`, next to `resources`, `reservedResources` and the
existing `restriction` block, and the scheduler reads it from there. The
system worker's `capacity` and `legion.role` ride the same way.

```console
$ legion node info legion-system-x8f2k1qz -j | jq .data.configAttrs.namespaces
["legion-system"]
```

**Alternatives.**
- (a) The plan's shape: a `node.namespaces` column, `Node`/`NodeMsg` gaining
  `repeated string namespaces`, set by the manager at registration from the
  join token's worker type.
- (b) (a) without the manager-side pinning: the worker reports it in a
  dedicated `WorkerRegisterRequest` field.

**Trade-off.**
- The restriction is as trustworthy as any worker-reported attribute: a
  worker that forges its registration can drop its own restriction, and
  claim `legion.role = "manager"`. The restriction keeps tenants' jobs off
  nodes; it does not authenticate nodes. That matters once A3 places apps
  by `legion.role`, and is a TODO item before then.
- `namespaces` becomes a reserved attribute key: a worker type's
  `attributes` may not use it (nor `capacity` or `legion.role`).

**Why (simplicity and performance).**
- A column means a migration on `node`, a field on two structs with about
  150 literals between them, and a read-model change. The attribute is read
  with one map lookup per node in the candidate scan, exactly as the
  existing `restriction` block is.
- Pinning (a) cannot work for the in-process worker anyway: its namespaces
  come from each manager's own `systemWorker` block (entry 24), not from the
  cluster-wide stored type.
- Moving to a column with manager-side pinning later is a contained change
  (the predicate reads one helper, `constraints::node_namespaces`).

## 20. The system worker has its own port (4849) and state directory, so a standalone worker can share its host

**Call.** `systemWorker.port` (default 4849) is where the system worker's
worker plane listens, and it keeps its state under
`<data dir>/system-worker/` instead of `<data dir>/workers/`. `--run-worker`
keeps `LEGION_WORKER_PORT` (4848) and `workers/`, as before.

**Alternatives.**
- (a) What the plan implies: the in-process worker as it was, on
  `LEGION_WORKER_PORT` and `<data dir>/workers/`.
- (b) An ephemeral port.

**Trade-off.**
- One more port to open between managers in a multi-manager cluster, and one
  more config field.
- A manager that switches between the system worker and `--run-worker`
  registers a new node each way; the old one goes lost.

**Why (simplicity).** With the system worker on by default, (a) breaks the
most common local setup, `legion start manager` then `legion join` from the
same checkout: the standalone worker collides on 4848, and finds the system
worker's id in `workers/` and adopts it (or refuses: "Multiple worker IDs
found"). (b) would make the host guard and firewall rules unpredictable.
The host guard's `nftables` table is still one per host; that is recorded as
a limitation and a TODO item, not fixed here.

## 21. A follower's system worker forwards its join token to the control leader, and waits for a leader rather than failing the start

**Call.** The token a manager mints for its own in-process worker is written
with `insert_join_token_forwarding`, which forwards the row to the control
lane's leader on `NotRaftLeader` (a new `ForwardableLaneWrite::InsertJoinToken`
variant). The CA pin and the mint are retried with backoff (1s to 30s) in the
worker's own task, so a manager that starts before its cluster has a leader
starts its worker once one is elected.

**Alternatives.**
- (a) Keep the old behavior: mint locally and fail the manager's start on
  error. `--run-worker` on a raft follower has always failed this way.
- (b) Have the in-process worker skip join tokens (an in-memory credential
  only its own manager recognizes).

**Trade-off.** A new manager-plane wire message, which a leader that
predates it cannot decode (the follower retries until the leader is
upgraded). Client `CreateJoinToken` calls keep the non-forwarding write, so
the client plane's "follow the leader hint" rule (raft plan D3) holds.

**Why (correctness of the default).** "One system worker per manager, so apps
fail over across managers" (entry 2) is false if followers cannot start one,
and with the system worker on by default (a) would make every follower of a
fresh raft cluster fail to start. (b) breaks the moment the worker
re-registers through another manager (it learns them all and fails over),
which is the case the system worker exists for.

## 22. The lend is a `capacity` ceiling on the node's fingerprint, and offers no exclusive cores

**Call.** The system worker registers `capacity = { cpu, memory, cores = 0 }`
as a config attribute, and `ResourcePool::from_node` caps the fingerprinted
CPU, memory and cores at it before `reservedResources` and oversubscription.

**Alternatives.**
- (a) Compute `reservedResources` on the worker as fingerprint minus lend.
- (b) Rewrite the system worker's fingerprint to the lend.

**Trade-off.**
- A new scheduler input, read once per node when its free pool is built
  (not per candidate).
- Apps on the system worker cannot use `cores`: a `cores` task never places
  there.

**Why (correctness).** (a) breaks on hosts whose CPU clock is unknown: the
fingerprint's CPU is then infinite, and infinity minus the lend is still
infinity, so the lend would cap nothing. (b) makes `legion node info` lie
about the host. Zero `cores` keeps the system worker's governor from pinning
host CPUs that a standalone worker's governor on the same host also thinks it
owns.

## 23. `legion-system` is created whenever a control leader finds it missing, and its policy only together with it

**Call.** Every control-lane leader, at startup and on taking leadership,
creates `legion-system` if it does not exist, and writes its namespace
policy in the same entry. A leader that finds the namespace does nothing,
policy or not.

**Alternatives.**
- (a) Detect "empty store" explicitly (no jobs, no namespaces) and create
  them only then.
- (b) Ensure both every time, recreating a deleted policy.
- (c) Create the namespace in a schema migration, like `default`.

**Trade-off.** A store from before this change gets the namespace on its
next leader start, which is more than "empty store" promises; there is no
installed base, so it does not matter.

**Why (simplicity).** The namespace cannot be deleted, so "missing" only
ever means "never created": that is the empty-store condition without a
heuristic for (a). (b) would fight an operator who deliberately edited or
deleted the policy. (c) runs on every manager independently and would also
need the policy as raw JSON in SQL; a leader-only write keeps one creation
path that later phases (seeding bundled apps) extend.

## 24. The `legion-system` worker type is built in and always registered; each manager's own settings apply in process

**Call.** Every control leader stores a built-in worker type `legion-system`
(pool `legion-system`, `namespaces = ["legion-system"]`, `namePrefix =
"legion-system"`, `legion.role = "manager"`), whatever its own
`systemWorker` says. A worker config file may not declare that name. Each
manager hands its in-process worker its own lend, namespaces, cgroup parent
and port (`LocalWorkerSettings`), which override the stored type.

**Alternatives.**
- (a) Store each manager's `systemWorker` settings in the type, last writer
  wins.
- (b) Register the type only when the leader's own system worker is on.

**Trade-off.** The stored type says `["legion-system"]` even on a cluster
whose managers configure other namespaces; a join token minted for
`legion-system` by hand gives a standalone worker the stored settings, not
any manager's.

**Why (simplicity).** Manager configs can differ, and the worker type is
cluster-wide, so (a) makes the lend flip with leadership (the same drift
entry 16 rejects for app settings). (b) leaves a follower with the system
worker on unable to pre-register when the leader runs with it off.

## 25. The node-namespace check rides inside `PermitSet`, which now carries the job's namespace

**Call.** `PermitSet::new(permit, namespace)`; `is_permitted` checks the
permit and then the node's `namespaces`. Every caller (the scan, the index
walk, the group filter, preemption, the apply recheck, the wake match) gets
the check without a second call site to forget. `permits_restrictions` is
the permit alone, for the diagnostic.

**Alternatives.** A separate `accepts_namespace` call next to every
`is_permitted`, as the plan sketches ("beside `is_permitted`").

**Trade-off.** "Permit" now means slightly more than the jobspec's `permit`
block.

**Why (correctness, then performance).** Eight places decide whether a task
may use a node, and missing one (the apply recheck, say)
would let a stale plan commit onto a refused node. One predicate cannot
drift. The cost is one attribute lookup per node per task; the
`candidate_construction` numbers are in the A1 change.

## 26. "Privileged containers" means the container privilege floor's opt-outs

**Call.** In a reserved namespace, submit refuses a container with
`security.userNamespace = false` or `security.seccomp = "unconfined"`, the
two opt-outs that make container root host root or remove the syscall
filter. Host networking, capability adds, and the sandbox runtime's `dev`
hypervisor are allowed.

**Alternatives.** Refuse every host-level task (`exec` with any user,
host-network containers, `dev` sandboxes, unknown plugin runtimes), the
classification `job:write` with `host` uses.

**Trade-off.** An app container on the host network can reach what the
host guard allows; a `dev` sandbox is a container, not a VM.

**Why (simplicity).** Apps are expected to be `exec` tasks
(`legion:self`), which are host-level by definition, so "host-level" cannot
be the line. The line the plan draws is root: these two settings are what
make a container's root the host's.

## 27. The lend's CPU becomes `cpu.max` over the host's own clock

**Call.** `cpu.max` quota = lend MHz / the host's average MHz per core x the
100ms period, so 1000 MHz is 40% of a CPU on a 2.5 GHz host. A host that
reports no plausible clock counts 1000 MHz as one CPU.

**Alternatives.** Read the plan's "1 CPU" literally: 1000 MHz is always one
CPU's quota.

**Trade-off.** The default lend is less than a whole CPU on most servers.

**Why (consistency).** The scheduler counts the lend in MHz against tasks
declared in MHz; a kernel ceiling in different units would let the apps it
placed use more (or less) than it believes it gave them.

## 28. `--run-worker` wins over `systemWorker.enabled = false`

**Call.** The flag always starts its worker; the block is ignored for that
run.

**Alternatives.** Treat `enabled = false` as "no in-process worker at all",
the flag included.

**Trade-off.** A config that says "no worker" can be overridden by a flag.

**Why (simplicity).** A command-line flag is the more specific, more recent
statement of intent, the same precedence `--no-acl` has over `acl.enabled`.

## 29. A token carries a grant reference, `grant:alloc/<id>`, resolved at every verification, not the granted rules (A2)

**Call.** The `legion` auth method adds `grant:alloc/<id>` to a verified
token's policy names when its allocation's version has a live grant, and
`resolve_acl_rules` expands that name, on every request, into the grant's
inline permissions plus the rules of its named policies. A token minted from
the identity with `legion login --method legion` stores the same name. A
stored policy may not be named `grant:…`, and `CreateAclToken` and
`UpsertAclUser` already refuse names that are not stored policies, so only
the auth method can hand one out.

```console
$ legion acl whoami            # inside the task, through its API socket
Principal:  legion:auto:default:cert-renewer-app-example-com:… (via jwt)
Policies:   grant:alloc/412
```

**Alternatives.**
- (a) `VerifiedIdentity` gains inline `rules`, resolved once when the token
  is verified, and Login copies the grant's policy names into the ACL token
  it mints.
- (b) Snapshot the grant into a synthetic stored policy per version.

**Trade-off.** One more indexed read per request on a workload token
(allocation → version → grant), and a policy-name prefix (`grant:`) that
users cannot use. The name is visible in `whoami` and `acl token list`.

**Why (security boundary, then simplicity).** The plan promises revocation
"effective on the next token verification". Under (a) a login-minted token
keeps its copy until it expires, so a revoke would not reach it. (b) leaves
rows to clean up and a second copy of the grant to drift. A name resolved
where every other policy name is resolved needs no new path through the
authenticator, and every credential type gets the same revocation.

## 30. Coverage is decided per (resource, action, namespace pattern, path pattern), and a request's missing selector needs the held rule's to be missing too (A2)

**Call.** Implementing calls entry 4's coverage test
(`crate::structs::grant::first_uncovered`):
- Each requested allow rule is split into atoms: every resource (a `*`
  resource asks for all fourteen), every action (`write` first, so a miss
  names the more privileged one), every `namespaces` pattern and every
  `paths` pattern. One held allow rule must cover the whole atom; the other
  selectors (`names`, `pools`, `actionNames`, `templates`, `bands`) are
  checked whole on that rule.
- A selector the request leaves out is covered only by a held rule that
  leaves it out too, with the existing default reach for `namespaces` (a
  `*`-resource rule reaches what `*` reaches, a legacy explicit one
  `default`) and `paths` (what `*` reaches).
- A held rule that names `namespaces` never covers a cluster-scoped request
  (its scopes carry no namespace, so that rule never matches them).
- A held deny on the resource requires `acl:write` **in addition** to allow
  coverage. `host = true` must be held as `host = true`.

```text
side-door: identity 'legion' asks for exec:write in legion-system, which your token does not
hold (`*` does not reach reserved `legion-*` namespaces or `legion/` keys; only a rule that
names them covers a request for them)
```

**Alternatives.**
- (a) Evaluate the request with `evaluate()` against synthesized sample
  scopes (one per requested pattern).
- (b) Treat a missing selector as `*` on both sides.
- (c) With a deny held, require `acl:write` instead of coverage.

**Trade-off.** Conservative refusals: a request covered only by a union of
held rules (`a*` plus `b*` for `*`), or one that leaves out a selector a held
rule names (`names = ["*"]`), needs an admin to submit it.

**Why (security boundary).** (a) is unsound: a sample scope stands for one
name, not every name a glob reaches. (b) lets a held
`resources = ["*"], namespaces = ["*"]` grant `kv:write`, which that rule
itself never matches, an escalation found while writing the coverage table.
(c) lets an `acl:write` holder skip the check; requiring both costs an ACL
admin nothing they could not do anyway, and keeps the error about the actual
missing rule.

## 31. The socket checks the peer's cgroup only where every task is guaranteed a leaf, and otherwise relies on file modes (A2)

**Call.** `PeerCheck::Cgroup` when the node runs cgroup v2 (unified at
`/sys/fs/cgroup`) and either the resource governor enforces or the kernel
host guard runs (both give every task a leaf). Then a connection from outside
the allocation's leaf is refused, and so is any connection while the leaf is
missing. Elsewhere `PeerCheck::FileMode`: the socket is `0666` inside the
allocation's owner-only directory.

```text
WARN Refused an API socket connection: pid 4121 (uid 65534) runs in cgroup
  /legion/share/legion-88, not in allocation 87's
```

**Alternatives.**
- (a) Refuse every connection without a leaf (no socket on macOS or cgroup
  v1).
- (b) Decide per connection: check the cgroup when the leaf exists, admit
  otherwise.

**Trade-off.** Without cgroup attribution, two allocations running as the
same user, or any root process, can use each other's sockets (documented).
The dev container and CI (cgroup v1) exercise only the file-mode path end to
end; the cgroup refusal is unit-tested on the parser, and an `#[ignore]`d
test needs root and delegated cgroup v2.

**Why (simplicity, inside the boundary the kernel can enforce).** (a) breaks
the one-command local cluster on a Mac and on most CI hosts, where the token
file next to the socket is equally reachable anyway. (b) opens a window
before a task's first launch, when its leaf does not exist yet but its socket
does, in which any process the directory admits could connect as the
allocation. Deciding once per node keeps the rule explainable.

## 32. Registration pins `namespaces` and `legion.role` from the token, the worker type, or the node record, by grant (A2, A1 follow-up)

**Call.** `worker_register` overwrites the two keys in the reported config
attributes:
- a join token a manager minted for its own in-process worker carries them
  (`join_token.pinned_attrs`, a new column, from the manager's
  `systemWorker` or `--run-worker` settings);
- any other join token takes them from its worker type (`namespaces`, and
  `legion.role` from the type's `attributes`);
- a node token keeps what the node registered with;
- ACLs off, a management token, or an audit-only pass keep what the worker
  reported.

**Alternatives.**
- (a) Pin from the worker type alone (the text of entry 19's TODO).
- (b) Move `namespaces` to a node column set from the token (plan §7).

**Trade-off.** A migration on `join_token`, and the forwarded join-token
write carries the new field. A registration with no proven caller still
reports what it likes, which with ACLs off is everything anyway.

**Why (correctness).** (a) is wrong for exactly the workers that matter:
every manager's system worker registers as the built-in `legion-system`
type, whose stored `namespaces` is `["legion-system"]` whatever a manager's
own `systemWorker.namespaces` says (entry 24), and `--run-worker` registers
as `default`, whose type has no `legion.role`. The minting manager is the
only party that knows its own worker's settings, so it writes them where
registration reads them. (b) is the larger change entry 19 declined.

## 33. A grant is per task: the version's row holds each task's request, and a token gets its own task's (A2)

**Call.** `job_grant.tasks` is a JSON array of `{group, task, permissions,
policies}`, and a token resolves the entry for its allocation's task.

**Alternatives.** One rule set per version, the union of every task's
request (the plan's "rules JSON").

**Trade-off.** A token's resolution joins `task_config` for the task's group
and name.

**Why (security).** The jobspec declares permissions on each task's identity;
a union would hand a sidecar the access its main task asked for, which no one
reviewing the spec would expect.

## 34. A KV re-render keeps the previous version's grant only when it asks for exactly the same (A2)

**Call.** Versions Legion stores itself (`Store::handle_job_submit`, the KV
re-render path) carry the previous version's unrevoked grant forward, with
its granter, when the new version's requests are identical, and get no grant
otherwise (logged).

**Alternatives.** (a) Always carry forward. (b) Never: every re-render drops
the grant until someone resubmits.

**Trade-off.** A re-render that changes the requested permissions (through a
legacy `$` KV reference inside them) leaves the job without a grant until a
holder resubmits it.

**Why (security, then simplicity).** (a) lets whoever writes a KV key the
spec references widen a job's grant with no check. (b) breaks every KV-driven
job with permissions on its first re-render, the common case.

## 35. Every principal's submit records the grant, so a resubmit restores a revoked grant; revocation is not sticky (A2)

**Call.** A submit that passes the check upserts the version's grant in the
submitter's name, an unchanged resubmit included; a revoked row is granted
again. An unchanged live row is not rewritten, so a steady resubmit adds
nothing to the raft log.

**Alternatives.** Keep a revoked grant revoked until an admin clears it (a
"blocked" flag on the job).

**Trade-off.** "Revoke" means "take it back from what runs now", not "forbid
it": a holder can redeploy the job with the access.

**Why (simplicity).** A grant only records that someone who held the access
submitted the job. A sticky revoke would be a second authorization system on
top of the ACL (who may clear it?), and the way to stop someone regranting is
to take the access away from them.

## 36. The socket keeps a caller's own credential and attaches the identity only when there is none (A2)

**Call.** The proxy adds `authorization: Bearer <identity token>` only to
requests that carry no `authorization` header.

**Alternatives.** Always overwrite it with the allocation's identity.

**Trade-off.** A process that holds some other token can use the socket as a
route to the managers for it.

**Why (simplicity).** It grants nothing: the caller already holds the token,
and could present it to any manager it can reach. It makes `legion login`
inside a task useful (a person exec'd into a task can act as themselves);
an overwrite would silently turn their token into the job's.

## 37. The `legion` identity always delivers a token file, which the worker writes at `secrets/legion.token` when the identity names no file (A2)

**Call.** `WorkloadIdentity::is_delivered` is true for `legion`, and the
worker's parser fills in `file = "secrets/legion.token"` (relative to the
delivery root) when it declares none, so the existing delivery, refresh,
sandbox identity disk and cleanup apply unchanged. The task directory is
created owner-only when a token is delivered, before the first launch.
Sandboxes get the token file and `LEGION_IDENTITY_TOKEN_FILE`, but no socket
and no `LEGION_ADDR`.

**Alternatives.** A separate writer for this token file outside the
declared-delivery machinery, and a socket over vsock for sandboxes now.

**Trade-off.** A sandbox task sets `LEGION_ADDR` itself and presents the
token as a bearer credential, over a direct connection with no failover.

**Why (simplicity).** One delivery path keeps refresh, the sandbox disk and
cleanup consistent; the vsock path is its own project (plan §6.4 defers it).
Creating the task directory early also fixed a pre-existing gap the
end-to-end test found: a relative token path could not be written before the
task's first launch.

## 38. A call that fails to reach a manager is not retried on the next one; it counts toward the worker's failover (A2)

**Call.** On a transport error the proxy answers `Unavailable` ("the worker
could not reach a manager for this allocation's API socket") and records the
failure on `ManagerEndpoints`; two in a row move the worker, and with it the
socket, to the next manager.

**Alternatives.** Buffer each request body and replay it on the next manager.

**Trade-off.** The first call after a manager dies fails, and the client
retries.

**Why (simplicity and correctness).** gRPC bodies are streams (exec is
bidirectional), and replaying a mutating call after a transport error can
apply it twice; `Unavailable` is retryable, and mutating RPCs take
idempotency keys for exactly this.

## 39. The `sub` charset check reads the job's bare name, so a namespaced job can declare identities (A2)

**Call.** Admission checks `[A-Za-z0-9._-]` against the bare job name, not
the canonical id.

```text
before: Error validating task configs: job name "team-a/web" contains '/', which is not allowed
        for a task that declares workload identities
```

**Alternatives.** Keep checking the canonical id.

**Trade-off.** None found: the namespace is its own `sub` component, validated
as a namespace name.

**Why (correctness).** Without it no job outside `default` could declare any
identity, `legion-system` apps included (found by the canonical-id grant
test).

## 40. A job's own env wins over the ambient env (A2)

**Call.** `LEGION_ADDR`, `LEGION_SECRETS_DIR` and `LEGION_IDENTITY_TOKEN_FILE`
are injected only when the task's config does not set them.

**Alternatives.** Always set them.

**Trade-off.** A job that sets `LEGION_ADDR` by hand (the old cert renewer)
keeps its direct connection, with the host-guard and failover caveats, until
it drops the variable.

**Why (simplicity).** A job that sets the address does so on purpose (a
manager in another cluster, a test fixture); overriding it would be a
surprise with no way out.

## 41. `permissions` and `policies` ride in the identity JSON, not a `WorkloadIdentity` proto message (A2, deviates from plan §7)

**Call.** The fields are on the `WorkloadIdentity` struct and travel inside
the task's identities JSON (`TaskAllocation.task_identities`); `permissions`
uses the ACL rule struct.

**Alternatives.** The plan's `repeated AclRule permissions` on a proto
message.

**Trade-off.** None: no proto message for identities exists, and one for two
fields would fork the representation.

**Why (simplicity).** Identities have always been JSON on the wire; the new
fields follow.

## 42. The reconciler submits as an identity that holds exactly the app's recorded grant, plus `job:write` on the app's job (A3)

> **Revised by entry 100** (hardening): the `job:write` rule no longer has
> `host = true`; an app's own `exec` task is not host-level for the
> reconciler, and nothing else host-level may be in an app's job.

**Call.** "Reconciler submits are checked against the recorded grant"
(plan §6.4) is implemented by what the reconciler *is*, not by a bypass: its
submits go through the ordinary submit path as an `AclIdentity` named
`legion:app-reconciler` whose rules are the app record's granted allow rules
plus `{ job, read+write, namespaces = ["legion-system"], names = [<app>],
host = true }`. The unchanged grant check then compares the render's
`legion` identity with those rules; the version's job grant is recorded as
`<granter> (app <name>)`.

```console
$ legion acl grants list --job legion-system/gitops
 legion-system/gitops | 3 | job:read,write in team-* ... | userpass:alice (app gitops) | ...
```

**Alternatives.**
- (a) Submit with a management identity and skip `enforce_identity_grants`
  for app jobs.
- (b) A `GrantSource` that records the app's grant without any check.

**Trade-off.** A manifest whose job reads KV (a template, a `$` reference)
or KV-authenticated artifacts needs `kv:read` on those keys in its own
`permissions`, because every other admission check also runs against this
identity; such an app shows `reconcile refused` until its manifest asks for
them (documented).

**Why (security boundary, then simplicity).** (a) and (b) each add a path
that skips a check, and every later admission check would have to remember
the exemption. Holding exactly the grant means the reconciler can submit
only what the app was granted, on only its own job, through the one path
everything else uses; a render that asked for more than the recorded grant
(a bug in adoption, say) is refused like anyone's.

## 43. Bundled apps cannot be uninstalled, so seeding is "insert each bundled app that has no record" (A3)

**Call.** `UninstallApp` refuses a bundled app (`FailedPrecondition`:
"disable it instead"). Seeding inserts a record for every bundled app that
has none, on every Control leadership acquisition, with `ON CONFLICT DO
NOTHING`; the `apps` seed block applies only to that insert.

**Alternatives.**
- (a) Seed only on an empty store (entry 23's "the namespace was missing"
  condition), and let bundled apps be uninstalled.
- (b) Allow uninstall, and remember seeded names in a tombstone table.

**Trade-off.** An operator who never wants the autoscaler keeps a disabled
record of it. A bundled app added in a later release is seeded on an
existing cluster the first time a leader runs that release (with its
permissions granted by `legion:bootstrap` then, not at cluster birth).

**Why (simplicity).** With no uninstall, "missing" only ever means "never
seeded", so no marker is needed and seeding stays idempotent across leaders.
(a) would never seed a bundled app added after the cluster's first day.
(b) adds a table to remember a decision `disable` already expresses.

## 44. `buildId` hashes the whole executable once per process, and `sha2` is optimized in dev builds (A3)

**Call.** `util::build_id()` is the first 16 bytes of the SHA-256 of
`current_exe()`, computed once on a blocking thread before the worker's
first registration (and by the manager for its own build). `Cargo.toml`
builds `sha2` at `opt-level = 3` in the dev profile.

**Alternatives.**
- (a) A build id baked in at compile time by `build.rs`.
- (b) The executable's size and mtime.
- (c) A hash of a sample of the file.

**Trade-off.** Startup reads the whole binary: well under a second for the
26 MB release build, about a second for a 400 MB debug build (tens of
seconds at `-O0`, hence the profile override). Two nodes running the same
bytes from different paths agree, which is the point.

**Why (correctness).** (a) is stale whenever cargo skips the build script,
which it does on most rebuilds, so a rebuild would not roll the app. (b)
makes identical binaries copied at different times disagree, so the marker
would never settle across nodes. (c) can miss a small change of equal
length.

## 45. The build marker is in the bundled app's environment, so a new marker is a new job version (A3)

**Call.** A bundled app's render carries `LEGION_APP_BUILD =
<buildVersion>+<buildId>` in every task's env, besides the `version_matches
>=<version>` constraint.

**Alternatives.** The constraint alone, as the plan writes it.

**Trade-off.** One more variable in the task's environment.

**Why (correctness).** A rebuild without a version bump changes `buildId`
but not the constraint, so the render would be byte-identical, the submit
would be `Unchanged`, and the app would keep running the old binary: exactly
the case `buildId` exists for (entry 14). The env makes the rolled job
differ, and gives the app its own build for logs.

## 46. Only `codeFields` entries marked `artifact = true` become artifacts (A3, deviates from plan §6.3)

**Call.** A `codeFields` entry is a JSON pointer (`"/plugins/*/url"`), or `{
pointer, artifact = true }`. Every code field needs the app's grant to
change; only artifact entries' string values are rendered as artifacts
(into `code/<path>/`, with a sibling `checksum` as the checksum).

```nickel
codeFields = [{ pointer = "/plugins/*/source", artifact = true }, "/plugins/*/url"],
```

**Alternatives.** Render every URL-valued code field as an artifact (the
plan's "plugin sources become artifacts", read literally).

**Trade-off.** One more shape in the manifest.

**Why (correctness).** The autoscaler's two code fields are different
kinds: `source` is a binary to fetch, `url` is an HTTP endpoint the app
calls. Fetching the endpoint as an artifact would fail staging, or fetch
something that is not code.

## 47. `managedBy` and the submitter live in the version's `meta`, and an unchanged reconciler submit stamps the version in place (A3)

**Call.** The stamp is `job_config.meta.managedBy` (beside
`jobTemplate`), and every principal's submit records `meta.submittedBy`,
which names the editor in `reverted a direct edit by <who>`. A KV re-render
copies the previous version's stamp; any other submit leaves it out. When a
reconciler submit is `Unchanged` (a direct edit left the job byte-identical
to the render) it writes the stamp onto the latest version.

**Alternatives.** A `managed_by` column on `job_config` with a migration;
leaving an unchanged version unstamped.

**Trade-off.** The stamp is not queryable by SQL index (one JSON read per
app per pass).

**Why (simplicity, then correctness).** Provenance already lives in `meta`,
which every version write builds; a column would touch every
`INSERT_JOB_CONFIG` caller. Without the in-place stamp, a job a direct edit
left equal to the render compares unequal forever, and the reconciler would
resubmit on every pass.

## 48. Status is derived when read, from the record, the job, and its allocations (A3)

**Call.** `running`, `degraded`, `pending (why)`, and `stopped` are computed
by `GetApp`/`ListApps` (and the metric) from the record's enabled flag and
last error, the job's latest version and state, its running allocations
against replicas times tasks, and the scheduler's blocked message. The
record stores only the reconciler's last action, error, digest, and marker.

**Alternatives.** Store a status column the reconciler maintains.

**Trade-off.** Each read costs a few store reads per app.

**Why (correctness).** Allocation state changes without a reconcile pass
(a crash, a placement); a stored status would lag it by up to a minute and
need a write per change.

## 49. A reconciler idempotency key is `deduplicated` only when the job shows its submit took (A3)

**Call.** The reconciler's keys are stored unscoped (`app-reconciler/app:<name>:<digest>:<from>`).
A hit counts as deduplicated only when the job's latest version carries the
digest and is not stopped; otherwise the reconciler submits anyway.

**Alternatives.** Trust the key alone, as `RunJob` does.

**Trade-off.** None found: a resubmit of the same render is `Unchanged`, so
it never adds a version.

**Why (correctness).** `<from>` is `none` for every lifetime of a job that
was destroyed, and version ids repeat after a destroy, so within the key's
24 hours a stale key could otherwise silence a needed resubmit. The stamp
check is what makes the key safe; the key only reports what happened.

## 50. An upgrade checks the whole new manifest against the caller, and needs approval only when it adds permissions (A3)

**Call.** `UpgradeApp` runs the grant check for every permission of the new
manifest (not just the added ones), refuses without `approve` only when the
diff adds rules, and takes an optional `config_json` for a schema change the
stored config no longer matches.

**Alternatives.** Check only the added rules; always require approval.

**Trade-off.** Upgrading needs as much access as installing did.

**Why (security boundary).** A new manifest is new code holding the whole
grant (entry 5), so the upgrader must hold all of it. An upgrade that only
narrows the grant asks nothing new, so a prompt would be noise.

## 51. Uninstall disables, stops, revokes, then deletes, and leaves the stopped job (A3)

> **Revised by entry 114** (hardening): "every interleaving stops it" did
> not hold for a submit already past its read of the record; the submit is
> now compare-and-set on the record, and jobs stamped for no record are
> swept.

**Call.** `UninstallApp` first writes the record disabled, then stops the
job, revokes its grants, and deletes the record; the stopped job stays in
`legion-system` until someone destroys it.

**Alternatives.** Delete the record first; destroy the job too.

**Trade-off.** A stopped job left behind for the operator to remove.

**Why (correctness).** Deleting first races the reconciler (a pass between
the delete and the stop finds nothing to do, and one between a stop and the
delete restarts the job); disabling first makes every interleaving stop it,
and a retry after a failure finds the record and finishes. Destroying would
erase the job's version history, which is the app's config history
(entry 15).

## 52. The config file reaches the task through the jobspec's `files`, and the worker resolves `LEGION_APP_CONFIG` (A3)

**Call.** The render adds `{ path = "legion-app-config.json", content, mode
= "0400" }` to each `exec` and `container` task's `config.files`, and
`LEGION_APP_CONFIG = "legion-app-config.json"`; the worker rewrites that
relative value to the task's own view of the task directory before start,
whether or not workload identity is on. Other runtimes get no config file.

**Alternatives.** A new task-level field the worker materializes; the
config inline in an env var.

**Trade-off.** Sandbox apps have no config file (documented); the file is
part of the stored job, so the config is readable by whoever can read the
job.

**Why (simplicity).** `files` already writes into the task directory
atomically and is chowned with it; one small rewrite makes the path right
for both runtimes. App config is settings, not secrets (secrets go in KV
under the app's grant), so riding in the job is fine and gives config
history for free.

## 53. A refused submit parks on (record `updated_at`, render digest) (A3)

**Call.** `InvalidArgument`, `PermissionDenied`, `ResourceExhausted`,
`AlreadyExists`, `OutOfRange`, and any `FailedPrecondition` but
not-the-leader are refusals: the app shows `reconcile refused: …` and passes
are skipped until the record's `updated_at` or the render's digest (which
includes the build marker) changes. Everything else backs off 5s→5m.

**Alternatives.** Retry refusals on the backoff timer too.

**Trade-off.** A refusal caused by something outside the app (a namespace
quota, a deleted policy) needs a write to the app (`legion app enable`) to
retry once fixed (documented).

**Why (performance).** A permanent error retried on a timer spins raft
writes and events forever; the plan asks it to wait for a relevant change.

## 54. The straggler status names the nodes behind the newest build (A3, rewords plan §6.3)

**Call.** While eligible nodes disagree: `upgrade waiting: 2 of 3 eligible
nodes on 0.15.0 (waiting for mgr-3)`, where the version is the newest build
present and the names are the nodes not on it (five at most, then "and N
more").

**Alternatives.** The plan's example, `2 of 5 eligible nodes on 0.14.0
(wkr-7, wkr-9)`, which reads as either the old or the new set.

**Trade-off.** None found.

**Why (simplicity).** The operator's question is "which nodes do I still
have to upgrade?"; naming them after "waiting for" answers it without
knowing which build the app runs.

## 55. Exec tasks get `systemWorker.appUser` only when the manifest names no user, from the leading manager's config (A3)

> **Superseded by entry 101** (hardening): each app runs as its own uid,
> recorded on the app; manifests may not name `user`, and
> `systemWorker.appUser` is removed.

**Call.** The render sets `user` on an `exec` task that has none, to the
leader's `systemWorker.appUser`; a manifest that names a (non-root) user
keeps it.

**Alternatives.** Force `appUser` on every exec task; record `appUser` on
the app record at install.

**Trade-off.** Managers that disagree on `appUser` render different jobs,
so a leadership change resubmits the app (documented: keep it the same on
every manager).

**Why (simplicity).** `appUser` is a fact about hosts (the user must exist
there), so it belongs to manager config, not the record; forcing it would
break an app that needs its own unprivileged user.

## 56. Workload identity stays off by default, so apps hold no credentials until it is turned on (A3, defers a default)

**Call.** A3 does not change `workloadIdentity.enabled` (default false).
Apps run and report healthy without it, but their `legion` identity gets no
token or socket; `legion app status` notes it, and the docs say so.

**Alternatives.** Turn workload identity on by default in A3.

**Trade-off.** The autoscaler's Phase 2 needs it on to call Legion; until
the default changes, a fresh cluster's autoscaler cannot act.

**Why (scope).** Turning it on is a cluster-wide default change (an issuer
URL, a signing key, JWKS endpoints) with its own plan-level questions; A3's
apps work either way. Recorded as a TODO item for the autoscaler's Phase 2.

## 57. `legion app config get` prints JSON, and the bundled manifest is evaluated Nickel with a list `command` (A3, deviates from plan §6.1, §6.3)

**Call.** `config get` prints the config as JSON (valid input to `config
set`), not Nickel. The bundled manifest is a Nickel file compiled into the
binary and evaluated once per process (imports refused); its exec task is
`command = ["legion:self", "app", "run", "autoscaler"]`, the exec runtime's
shape, not the plan's `command`/`args` pair.

**Alternatives.** A JSON-to-Nickel printer; a JSON bundled manifest.

**Trade-off.** `config get` output is not idiomatic Nickel.

**Why (simplicity).** No JSON-to-Nickel printer exists, and JSON is valid
input for every Nickel-reading command. Keeping the bundled manifest in
Nickel keeps it reviewable next to the docs, and a unit test evaluates and
validates it.

## 58. Every `JobConfig` parse is strict, not only the submit paths (A6a)

**Call.** No jobspec block uses `deny_unknown_fields` any more. Each one
captures unknown keys in a `#[serde(flatten)] unknown_fields` map, and
`JobConfig::parse_submitted(value, mode)` turns them into errors (or keeps
or drops them under `allow_unknown_fields`). The plain `Deserialize` for
`JobConfig` is strict as well, and accepts only the stored
`pendingExtensions` maps, so a config read back from the store, a KV
re-render, or a template render fails loudly on a key it doesn't know.

**Alternatives.** A lenient default `Deserialize`, with strictness applied
only on the `RunJob`, `PlanJob`, and `RunJobTemplate` paths.

**Trade-off.** Internal round-trips must never carry an unknown key: a
renamed field in a stored config is an error on read rather than a silent
drop, so a rename needs its stored rows rewritten. Two shipped bugs fell out
of this at once (below).

**Why (simplicity).** The point of the phase is that no path accepts a key
silently. With a lenient default, every new parse site would have to opt
in, and the first one that forgot would reintroduce the failure. Going
strict found that `JobConfig` serialized `rollout_by_group` in snake_case,
so KV re-renders silently dropped per-group rollout overrides (now
`rolloutByGroup`), and that seven example jobspecs (`fullstack`,
`connect-demo`, `mac-container-service`, `mac-dag`, `maximal`, `postgres`,
`service-demo`) used snake_case keys (`on_unhealthy`,
`unhealthy_after_count`, `boot_grace_time`, `restart_*`) that had never
taken effect.

## 59. Some blocks stay strict under `--allow-unknown-fields` (A6a, deviates from plan §6.6)

**Call.** The plan says the flag keeps unknown keys at extension points and
drops them everywhere else. A few blocks refuse an unknown key even with
the flag:
- network policy blocks (`egress`, `ingress`, `handlers`), which are shared
  with `legion net policy` objects;
- value sources (`{ kv | identity | file }`);
- the runtime's `config` (checked against the runtime's own schema);
- the hand-parsed `count`, `gang`, and `schedule` shapes.

**Alternatives.** Drop unknown keys in these blocks too, as the plan says.

**Trade-off.** The flag doesn't get every spec through: a typo in one of
these blocks still fails the submit.

**Why (safety).** Dropping a misspelled key in a network policy weakens the
policy that gets applied, and dropping one in a value source or a count
shape changes what the task runs with. The flag exists for version skew
and uninstalled apps, and neither ever adds keys to these blocks.

## 60. Free-form maps are not checked, so a typo inside `resources` is not caught (A6a)

**Call.** Maps whose keys are data rather than fields take any key:
`resources` (its keys are resource dimensions, including a cluster's custom
ones), `constraints`, `permit`, `affinities`, `network.labels`,
`service.meta`, volume `params`, and identity `extraClaims`. The plan's own
example, `resources.memoryMax2`, therefore can't be caught, and the plan's
example now uses `stop.signall`.

**Alternatives.** Check `resources` against the cluster's known dimensions
at submit.

**Trade-off.** `resources = { memroy = 512 }` is still accepted. It asks
for a dimension no node offers, so the job blocks rather than running with
the wrong size, and the blocked reason names it.

**Why (simplicity).** The parser can't know a cluster's custom dimensions,
and checking them at submit would make a jobspec's validity depend on which
nodes are registered. The list is in the public doc
(docs/jobs-tasks-and-scheduling.md, "Unknown Fields").

## 61. Pending keys are stored under an explicit `pendingExtensions` map, not inline (A6a)

**Call.** A key kept by the flag is stored in its block's
`pendingExtensions` map: in `job_config.meta` for the job, in a new
`task_config.pending_extensions` column for a task, and inside the existing
JSON for volumes, ports, and services. A strict submit that carries a
hand-written `pendingExtensions` is refused; with the flag it is accepted.

**Alternatives.** Store pending keys inline, next to the core fields, as
they were written.

**Trade-off.** One more name in the stored and `job info -j` shapes, and a
column migration on `task_config`.

**Why (extensibility).** Inline, a pending `backup` would be reinterpreted
by any future core field named `backup`, and nothing could tell a pending
value from a validated one. The explicit map keeps them apart until an app
adopts the key (A6b).

## 62. A change to pending keys alone creates a version and a rollout (A6a, until A6b)

**Call.** Resubmitting a spec whose only difference is a pending key makes
a new version and rolls it out, like any other change.

**Alternatives.** Move allocations to the new version in place, since no
runtime reads a pending value.

**Trade-off.** A pending-only change restarts the job's allocations.

**Why (scope).** In-place version moves for extension values are A6b's
lifecycle work (plan §8), and A6b needs the same predicate for an
installed app's keys. Until then, a restart is correct, only slower.

## 63. Suggestion lists are hand-written per block, and a test checks them (A6a)

**Call.** Each block's known keys (`FIELDS`) are listed by hand next to the
struct with `spec_block!` or `extension_point!`. Detection itself is
serde's: a key no field consumes lands in the flatten map whatever the
list says. A unit test serializes every `configs/jobs` example and fails
when a serialized key is missing from its block's list.

**Alternatives.** Derive the list from serde (it exposes none with
`flatten`), or a proc macro.

**Trade-off.** A new field needs a second edit. Forgetting it only loses
the did-you-mean suggestion, and only if no example uses the field. The
merge with A2 had to add `permissions` and `policies` to the identity list.

**Why (simplicity).** No proc-macro crate for one list per struct, and the
failure mode is a missing hint, never a missed key.

## 64. The unknown-fields record describes the submit that created the version (A6a)

**Call.** What the flag kept and dropped is recorded on the version
(`job_config.meta.unknownFields`) and shown by `job info`. It is not part
of `is_same_as`, so resubmitting a spec that renders to the same config is
`unchanged` and keeps the old record, even when the resubmit no longer
carries the dropped keys. KV re-renders carry pending keys forward but
write no record.

**Alternatives.** Make the record part of the version's identity, so a
resubmit without the dropped keys makes a new version.

**Trade-off.** `job info` can show a dropped key that the latest submit no
longer sent.

**Why (simplicity).** A new version, and a rollout, for a change that
alters nothing the job runs would be worse than a stale note. The proto
comment on `GetJobInfoReply.unknown_fields` says so.

## 65. `warnings` carries every warning, and the message still has them (A6a)

**Call.** `RunJobReply.warnings` and `RunJobTemplateReply.warnings` list
every submit warning, not only unknown-key ones, and the reply's `message`
still has them appended for older clients. A deduplicated replay rebuilds
`warnings` from the stored message.

**Alternatives.** A field for unknown-key warnings alone.

**Trade-off.** Clients that print both see warnings twice; the CLI prints
`warnings` and not the appended text.

**Why (agent-friendliness).** One structured list is what an agent checks,
and a replay answers like the original.

## 66. `template render` takes no flag, and `job info -j` adds `unknownFields` (A6a)

**Call.** `legion template render` prints a rendered spec without
submitting it, so it has no `--allow-unknown-fields`; the submit that
follows takes it. `legion job info -j` adds an `unknownFields` key to the
job object only when the latest version has a record.

**Alternatives.** The flag on `render` too, as a no-op or a filter.

**Trade-off.** None worth noting; the JSON change is additive.

**Why (simplicity).** A flag that changes nothing would only mislead.

## 67. The app reconciler's submits are always strict (merge of A3 and A6a)

**Call.** The reconciler submits an app's render with
`UnknownFieldMode::Reject`; an app cannot ask for `allow_unknown_fields`.

**Alternatives.** Let a manifest mark its job as lenient.

**Trade-off.** A manifest written for a newer Legion fails its reconcile
instead of running with keys dropped. The failure is in `legion app
status` and the reconcile event.

**Why (safety).** The render is Legion's own output from a manifest that
was validated at install. An unknown key in it is a manifest bug or real
version skew, and the build marker (calls entry 45) already keeps a new
manifest from running on an older manager.

## 68. Running an app action needs `app:read` plus the action's declared permission, not `app:write` (A4, deviates from plan §6.5 and entry 11)

> **Partly superseded (2026-10-29) by entry 98.** The gate below stands. What
> an action then does is no longer "the app, with its own grant": the app
> acts as the viewer, bounded by the declared permission on the context and
> by its grant ([app-actions-as-viewer-plan.md](../plans/app-actions-as-viewer-plan.md)).

**Call.** `InvokeAppAction` is gated on `app:read` on the app, then the
permission the action declares in the manifest, checked on the context it
is pressed in (the job, task, or node).

```nickel
actions = [{ name = "pause", permission = { resource = "scale", action = "write" } }]
```

**Alternatives.** `app:write` on the app, plus the declared permission.

**Trade-off.** The manifest's declaration alone decides who may press a
button; an installer must read it, and the install prompt shows it.

**Why (simplicity, least privilege).** `app:write` reconfigures, upgrades,
or disables an app cluster-wide. Requiring it would push operators to hand
that to everyone who should be able to press Pause.

## 69. Views are authorized as the viewer and forwarded with a signed viewer assertion bound to the context (A4)

**Call.** The manager checks `app:read` and the entry's declared
permission as the viewer, then forwards with a 60-second ES256 JWT signed
with the cluster's workload-identity key: `aud = app:<name>`, `jti`, and a
`legion.io/viewer` claim naming the principal, the permission checked, its
scope, the viewer's namespaces for `nav`/`overview`, the exact context, and
for actions the SHA-256 of the inputs' canonical JSON. The app framework
refuses a missing, expired, wrong-audience, foreign-key or claimless
assertion (401) and a kind, name, context or inputs mismatch (403), and
uses each action assertion once.

**Alternatives.** A per-app HMAC secret; reserving `app:*` audiences on
job identity tokens.

**Trade-off.** Views need workload identity (on by default since the
autoscaler's Phase 2) and an API socket to fetch the keys, so sandbox apps
cannot serve views yet.

**Why (security).** Jobs cannot set `legion.io/` claims, so a job identity
token minted with audience `app:x` is still refused, and a captured action
request cannot be replayed against the app's port.

## 70. The forward is a narrow unary worker RPC, not a general proxy (A4)

**Call.** `Worker.ForwardAppRequest` (manager caller class only) carries
one HTTP/1.1 request to an allocation's declared port: `legion-system`
jobs only, paths under `/legion/v1/`, three headers, a 256 KiB body, a
1 MiB answer within 10 seconds, dialed the way ingress dials a task. The
manager tries the next allocation (up to three, healthy first) only when
the request never reached one, and never retries after it may have.

**Alternatives.** Build the reverse-proxy plan's `ProxyTunnel` now.

**Trade-off.** One more worker RPC, to fold into `ProxyTunnel` when that
lands; a failure mid-exchange is not retried.

**Why (security, simplicity).** The smallest surface that keeps the
worker-plane-auth model, and no action can run twice.

## 71. The manager validates every view, and the framework validates before sending (A4)

**Call.** A view must match the closed `legion-view/1` block set; the
manager refuses an answer outside it (unknown blocks or fields, raw HTML,
non-http(s) links, images) with FailedPrecondition, even though the
framework already checked it. Markdown is a CommonMark subset rendered as
DOM nodes, never `innerHTML`; `<` followed by a letter, `/`, `!` or `?` is
refused, so `a<b` and autolinks are refused too.

**Alternatives.** Trust the framework's check.

**Trade-off.** Validation runs twice, and some harmless text is refused.

**Why (security).** Third-party apps need not use Legion's framework; the
manager is the trust boundary for the SPA, the CLI, and MCP.

## 72. Permission scopes depend on the slot (A4)

**Call.** On `job` and `task` slots a namespaced permission is checked on
that job; on `node` slots a `node` permission is checked on the node's
pool (after a coarse check) and a namespaced one needs cluster-wide
authority; on `nav` and `overview` any grant somewhere passes, and the
assertion carries the viewer's matching namespaces for the app to filter.

**Alternatives.** Require cluster-wide authority on `nav` and `overview`.

**Trade-off.** An app must filter its sidebar and overview rows itself.

**Why (ease).** Team-scoped viewers still get a useful sidebar page, with
no extra RPC for the app.

## 73. App errors never reach the caller as an auth error (A4)

**Call.** The app's 401/403 become FailedPrecondition ("refused the viewer
assertion"), 404 NotFound, other 4xx FailedPrecondition with the app's
message, 5xx Unavailable.

**Alternatives.** Pass the app's status through.

**Trade-off.** A caller can't tell an app's refusal from other
preconditions without reading the message.

**Why (correctness).** A PermissionDenied would mislead the caller about
their own access, which the manager already decided.

## 74. The manifest's `ui` block is typed and strict at install (A4, deviates from A3's "stored as written")

**Call.** Unknown `ui` fields are install errors, `ui.port` (default
`api`) must be a port the app's job declares, and every action needs a
`permission`. Confirmation placeholders are `{job}`, `{task}`, `{node}`,
not the plan's `%{job}`, which Nickel interpolates.

**Alternatives.** Store as written and fail at view time; `%%{job}`.

**Trade-off.** A3's opaque storage of `ui` changes shape.

**Why (ease).** Errors surface at install, where the author is.

## 75. A task badge is a top-level `badge` in the view document (A4)

**Call.** A `task` slot entry names a `badge` view that answers
`{ badge = { text, tone } }` (or nothing); a task entry may also carry a
panel `view`.

**Alternatives.** A badge block in the block set.

**Trade-off.** One special field outside the blocks.

**Why (simplicity).** A badge is not drawable in a panel, and the block
set stays the plan's eight.

## 76. `app.action` events go on the app's job, and views are not audited (A4)

**Call.** Every action, including a denied one (after `app:read` passes),
is an `app.action` event on the app's job with the context in `meta`.
Viewing is not recorded.

**Alternatives.** Scope events to the context's job; audit views.

**Trade-off.** A job's own event stream doesn't show actions pressed on
its panel.

**Why (consistency).** All app events live on the app's job.

## 77. Views are polled at `ui.refresh`, never more often than every 5 seconds (A4)

**Call.** Unset `refresh` means no polling.

**Alternatives.** Streamed views.

**Trade-off.** Panels lag by up to one refresh.

**Why (simplicity).** Unary forwards only; streaming waits on the reverse
proxy.

## 78. No action command or MCP tool; `legion app test` deferred (A4)

**Call.** `legion app view` and `legion_app_view` read views and list
actions, but nothing outside the UI runs an action (`InvokeAppAction` is
public). The plan's `legion app test` (checking that a third-party app
refuses bad assertions) is not built.

**Alternatives.** `legion app action`; an MCP write tool.

**Trade-off.** Terminal users and agents can read but not press buttons.

**Why (scope).** The plan names neither; the RPC is enough for scripts.

## 79. An in-process forwarder seam carries the integration tests (A4)

**Call.** The `AppForwarder` trait lets tests deliver to a framework-built
fake app in process; the worker's lookup of a real allocation is covered
by its NotFound case.

**Alternatives.** A real-binary test with a runnable fake app.

**Trade-off.** Less end-to-end coverage of `app_forward_target`.

**Why (speed).** The scenarios run in seconds without a worker.

## 80. Page-callable RPCs are a closed allowlist classified by effect (A5)

> **Revised by entry 102** (hardening): each entry also names its
> permissions, which install checks against the installer, and ACL,
> token, grant, app writes and key rotations were removed.

**Call.** A sandboxed page may name only RPCs in `PAGE_RPCS`, each marked
read or write by what it does (`RestoreInstance` is a write). Refused
outright: RPCs that return credentials (`Login`, `CreateAclToken`,
`CreateJoinToken`, `GetRaftJoinToken`, `MintExecTicket`, `CreateExecKey`,
`IssueBootTicket`, `GetStateBackup`), break-glass, streaming, UI plumbing,
and worker- or app-plane RPCs.

**Alternatives.** Derive read/write from the ACL action or the method
name.

**Trade-off.** A new RPC is unavailable to pages until someone classifies
it.

**Why (security).** Safe by default.

## 81. The manager re-checks a bridged call against the page's current manifest (A5)

> **Revised by entry 108** (hardening): every refusal now reads the same,
> since the check runs before authentication.

**Call.** The SPA stamps bridged calls with `legion-app-page: <app>/<page>`;
a manager layer refuses a stamped call the page's current `rpcs` don't
list, or whose app is disabled. The viewer's ACL still decides every call.

**Alternatives.** Enforce the allowlist in the SPA alone.

**Trade-off.** One store read per bridged call; the header is the SPA's
own claim, so this is not an authorization boundary.

**Why (defense in depth).** An SPA bug can't widen a page's reach, an
upgrade that narrows `rpcs` applies to pages already open, and bridged
calls appear in the manager's logs.

## 82. Pages are a sibling RPC, live only in the sidebar, and confirm every action (A5)

**Call.** `GetAppPage` fetches a page's single-file HTML (UTF-8, at most
1 MiB) with the same authorization as a view and its own assertion kind.
Pages appear as `nav` entries with an empty context; job, task, and node
pages are deferred. `legion.action` therefore takes an explicit context,
and every page action shows the SPA's confirmation.

**Alternatives.** Extend `GetAppView`; pages in every slot.

**Trade-off.** One more RPC to classify; read-only actions are confirmed
too.

**Why (simplicity, safety).** A page is HTML, not a view document, and the
bridge can't see an action's declared effect.

## 83. The bridge closes on the frame's second load (A5)

**Call.** The SPA answers only messages whose `source` is the page's frame
and whose origin is `"null"`, and only until the frame loads a second
time; after that the bridge is closed for good. The CSP is the plan's,
inserted after a leading doctype (standards mode).

**Alternatives.** Keep the bridge open across navigations.

**Trade-off.** A page that reloads itself loses the bridge.

**Why (security).** Content the frame navigates to never gets answers.

## 84. No MCP tool for pages, and the smoke tests stay out of CI (A5)

**Call.** Agents get views (`legion_app_view`), not pages. The Playwright
smoke tests (`npm run test:smoke`, 52 checks) are committed but not run in
CI, and Playwright is not a UI dependency.

**Alternatives.** Install a browser in CI.

**Trade-off.** UI regressions are caught only when someone runs them.

**Why (cost).** CI has no browser, and an agent can't run HTML anyway.

## 85. Extension values are one job-level list, and their state and defaults are worked out on read (A6b)

**Call.** A job version's extension values are stored as written in one
list in `job_config.meta.extensions`, each with its point and path (task,
volume, port, ...). Nothing derived is stored: whether a value is valid,
invalid, orphaned (its app uninstalled) or pending, and its defaults, are
computed against the current registry on every read, with compiled
schemas cached by digest. `legion job info` lists them under
`jobspec extensions:` with their state now.

**Alternatives.** Fields on each block's config (`task_config`, volume,
port, service JSON); persisting the state and the filled-in value.

**Trade-off.** Readers map a value's path back to its block, and every
read pays a (cached) schema check.

**Why (simplicity).** No `task_config` migration, one place for `job info`
and `WatchAppExtensions` to read, and install, upgrade and uninstall need
no rewrite pass over every job.

## 86. Pending values are adopted on read, except for keys with `requires` (A6b)

**Call.** A value kept as `pending` by `--allow-unknown-fields` reads as
adopted once an app declaring that key is installed. It is not rewritten
into the stored list until the job's next submit, and a key whose
declaration has `requires` is not adopted at all: someone holding the
permission has to submit it.

**Alternatives.** Rewrite every pending value into an adopted one at
install.

**Trade-off.** Adoption is not persisted until the next submit.

**Why (security, simplicity).** No bulk write at install, and a `requires`
key must be set by a submitter who holds what it requires.

## 87. The validate call fails open, per app, on run only (A6b)

**Call.** For apps declaring `validate = true`, `RunJob` and
`RunJobTemplate` send one POST per app to `/legion/v1/extensions/validate`
over the A4 forward, with a 2-second timeout and a viewer assertion of
kind `validate`, after checking `job:write`. A rejection fails the submit
(`InvalidArgument`); an unreachable, disabled, non-200 or slow app is
skipped with a warning. Plans, `job validate`, and idempotent replays don't
call it.

**Alternatives.** Fail closed; also call it from `job plan` and
`job validate`.

**Trade-off.** A down app's check is skipped, and a plan can pass what the
run then refuses.

**Why (availability).** A down app must not block deploys, and the submit
path stays synchronous.

## 88. `requires` is checked as the submitter, on the job or cluster-wide (A6b)

**Call.** A key's `requires` permission is enforced on the submitter for
new or changed values only: on the job for a namespaced resource, and
cluster-wide for a cluster-scoped one (`net`, `node`).

**Alternatives.** Always scope it to the job.

**Trade-off.** Some asks need a cluster grant.

**Why (consistency).** It matches how those resources are scoped
everywhere else.

## 89. One owner per key; renames happen at install (A6b)

**Call.** A key colliding with a core field is refused at install (an
upgrade keeps keys the app already declares); a key another app owns is
refused with a hint to install it under another key
(`--key <point>.<key>=<new key>`). Renames live in the app's settings, survive upgrades, and can only be set
at install. A key a newer core field now shadows is detected (a raw JSON
check), logged at manager start, and its users get the qualified-spelling
error.

**Alternatives.** Allow overlap and pick a winner; renames after install.

**Trade-off.** No `app set --key` to rename later.

**Why (simplicity).** One owner per key keeps the jobspec unambiguous.

## 90. `WatchAppExtensions` polls the store and diffs (A6b)

**Call.** The stream sends a snapshot, then upserts and deletes with a
generation, found by a 1-second fingerprint check and a 30-second full
read, sharded by SHA-256 of the job (first 8 bytes, mod the shard count).
Only the calling app's own values are returned, identified by its workload
identity or grant reference; a management token must name the app.

**Alternatives.** Feed it from the event stream.

**Trade-off.** Up to a second of latency and a periodic read per stream.

**Why (simplicity).** No new event plumbing, and it covers every job
status.

## 91. Extension-only resubmits move in place through the autoscaler's predicate (A6b)

**Call.** `classify_in_place` extends Phase 2's predicate: it strips
extension values and every `pendingExtensions` map, plus autoscale-owned
counts, and compares the rest. When only extension values differ,
allocations move to the new version with the same ids and no rollout:

```console
Job updated in place: only jobspec extension values changed, so nothing restarts (the apps that own them see the new values)
```

A value's `validated_by` is ignored when comparing.

**Alternatives.** A second predicate for extensions.

**Trade-off.** One predicate carries two features' rules.

**Why (simplicity).** One place decides what restarts.

## 92. `GetJobSchema` needs `job:read`, and installed schemas are rebased into `$defs` (A6b)

**Call.** The schema behind `legion job schema` and `job validate` is the
core schema plus every installed key at its point under
`$defs/ext.<app>.<point>`, readable with `job:read` anywhere. Offline,
`job validate` checks core fields and reports installed keys `unchecked`.

**Alternatives.** Open to every caller.

**Trade-off.** Callers without `job:read` get no schema.

**Why (security).** It reveals which apps are installed.

## 93. KV references in extension values resolve once, at submit (A6b)

> **Revised by entry 99** (hardening): their `kv:read` is checked like any
> reference's, before resolution; only the dependency edge is dropped.

**Call.** A `{ kv = … }` reference inside an extension value is resolved
when the job is submitted, like the rest of the spec; a reference whose
pointer does not resolve in the stored config is dropped.

**Alternatives.** Live re-resolution.

**Trade-off.** A KV change reaches an app's value only on the next submit
or KV re-render.

**Why (consistency).** The same rule as every other jobspec value.

## 94. `maximal.ncl` shows extension keys as comments, and `job plan` shows values (A6b)

**Call.** The maximal example shows each point's key (and the
`apps.<app>` spelling) commented out, since no app is installed when its
test parses it. `PlanJobReply` gains `extensions`, so `job plan` prints
the values a submit would store.

**Alternatives.** Real keys under `pendingExtensions`.

**Trade-off.** The examples are not parsed.

**Why (simplicity).** No fake install in the examples test.

## 95. The host guard's nftables table and BPF pins are named after the worker's cgroup parent (A1 follow-up)

**Call.** The default parent keeps `legion_host_guard` and
`/sys/fs/bpf/legion`; any other parent gets a sanitized suffix (the system
worker uses `legion_host_guard_legion-system`), so a manager's system
worker and a standalone worker on one host stop overwriting each other.

**Alternatives.** Name them after the worker id.

**Trade-off.** Two workers sharing one cgroup parent still collide
(documented).

**Why (operability).** A name stable across restarts lets a worker clean
up its own objects, and the default is unchanged.

## 96. `*` pool globs don't reach reserved `legion-` pools (A1 follow-up)

**Call.** An allow rule's `pools` glob reaches a `legion-*` pool only when
the pattern itself starts with `legion-`, for every node action including
read; a rule with no `pools` selector reaches what `*` reaches. Deny rules
keep plain globs, and grant coverage follows the same rule.

**Alternatives.** Protect only writes.

**Trade-off.** A tenant with `node:read` on `*` no longer lists the
managers' system workers.

**Why (consistency).** The same reserved-name model as namespaces and KV
paths (entry 18).

## 97. `pools.default` can be a list, and `legion-system` defaults to both pools (A1 follow-up)

**Call.** A namespace policy's `pools.default` takes one pool or a list;
the bootstrap `legion-system` policy defaults to
`["legion-system", "default"]`, so a hand-written `legion-system` job also
places on a `--run-worker` manager's worker.

**Alternatives.** Put `--run-worker` nodes in the `legion-system` pool.

**Trade-off.** A new policy shape; clusters bootstrapped earlier keep the
single-pool policy.

**Why (ease).** Ordinary workers still refuse `legion-system` through
their `namespaces`, and the list form is reusable.

## 98. Actions run as the viewer, through a delegation on the viewer assertion (A4 follow-up, supersedes the "runs as the app" half of entry 68)

**Call.** An action's assertion carries a `delegation` (the declared
permission, `read` on the same resource when the viewer holds it, the exact
scope checked); the app presents it back in `legion-on-behalf-of`, and the
manager runs those calls as `alice via app <app>`, bounded by the
delegation and by the app's grant. The gate of entry 68 is unchanged. The
design and its own calls are in
[app-actions-as-viewer-plan.md](../plans/app-actions-as-viewer-plan.md) and
[app-actions-as-viewer-plan_calls.md](app-actions-as-viewer-plan_calls.md).

**Alternatives.** Keep actions running with the app's grant, with the
viewer in the reason (entry 68 as built, autoscaler calls entry 83).

**Trade-off.** Apps that want the bound must use the viewer client (or send
the header); an app's own client still works in a handler.

**Why (security).** The manifest's declared permission becomes a bound the
cluster enforces, and history names the person.


## 99. Every KV reference needs `kv:read`, checked before any is resolved (revises entry 93)

*Hardening after adversarial review.*

**Call.** `resolve_jobspec_kv_references` takes the caller and enforces
`kv:read` on every `$path/key` string in the jobspec, and on every key a
task's `kvInjections` read (the `templateKv` pointer first), **before** it
resolves any of them. The dependency-edge `retain` of entry 93 still drops
the edges of references inside extension values and pending keys, but no
longer their read check. A miss is `PermissionDenied` naming the key:

```text
error: permission denied: ... (the jobspec reads kv team-b/db/password; the submitter needs kv read on it)
```

RunJob, RunJobTemplate, RenderJobTemplate, PlanJob, and the apps' validate
call all parse through it, so the order holds on every path.

**Alternatives.**
- (a) Enforce read on the dropped references after the parse, as the
  reviewer's smallest fix said.
- (b) Refuse `$` references inside extension values and pending keys.
- (c) Never echo resolved values in errors (redact parse and schema
  messages).

**Trade-off.** A submit that names a key the caller cannot read now fails
with `PermissionDenied` even when the key is missing (it used to be
`InvalidArgument: kv entry ... not found`), and a template stored in KV is
read only after its `templateKv` key's check.

**Why (security boundary).** Checking before resolving is the one order in
which nothing the caller may not read exists in memory to leak: not in a
plan's extension values, a stored version, a serde error ("unknown priority
band `hunter2`"), a schema error, or the body posted to an app's validate
endpoint, and a missing key cannot be told from an unreadable one. (a)
leaves every echo that happens between resolution and the check; (b) takes
away a working feature and leaves core fields; (c) has to find every
message that can quote a value, now and later.

## 100. No host-level construct in an app's job, whoever installs it; the reconciler holds no `host` grant (revises entries 5 and 42)

*Hardening after adversarial review.*

**Call.**
- `validate_reserved_namespace_task` refuses, in `legion-system`, for
  every submitter, the constructs that make the worker act for a task on
  the node: an `exec` health check, a `file://` or `git::file://` artifact
  or image, an artifact with `auth.nodeIdentity`, and, for an `exec` task,
  a file the worker writes at an absolute host path (`config.files`,
  `kvInjections.files`, an identity's `file`, a `connect.templates`
  destination), a `config.cgroup` it joins, or a negative OOM score. The
  review named the first three; the rest came up while scoping the `exec`
  exemption below, since each is the root worker acting before it drops
  to the task's user.
- Install, upgrade, and `SetAppConfig` render the app's job and run that
  check plus the host-level classification, so the person making the
  change hears the refusal (`InvalidArgument`), not the reconciler later.
- `reconciler_identity` drops `host = true`. For a submit with
  `SubmitOrigin::App`, `app_task_host_level_reason` does not count the
  `exec` runtime itself (its task runs as the app's own uid, entry 101);
  everything else host-level still needs a host grant the reconciler never
  holds.

```text
Error: app checker: task "main" declares exec health check "probe", which the worker runs on the host, and tasks in namespace legion-system may not: ...
```

**Alternatives.**
- (a) Let an installer who holds host-level `job:write` in `legion-system`
  put host-level constructs in an app (record `host` in the grant, keep it
  on the reconciler identity only then).
- (b) Keep `host = true` on the reconciler and rely on the namespace check
  alone.
- (c) Check only at install.

**Trade-off.** An app that would need an `exec` health check, a node file,
or the node's identity cannot be an app, even for an admin; it has to be an
ordinary job. Container `network.mode = "host"`, `dev` sandboxes, and
plugin runtimes are also refused for apps (they are host-level and the
reconciler has no grant), though a human with the host grant may still run
them in `legion-system`.

**Why (security boundary, simplicity).** On a manager the worker is root,
so each construct was root on the managers for anyone with `app:write`.
Apps don't need them (health is `http`, code comes from a URL), so
"never" is simpler to state and to check than (a)'s "unless the installer
held host", which would also have to track the grant through upgrades and
config changes. (b) leaves whatever host-level construct the namespace
check does not name (a host-network container, a plugin runtime) open to
`app:write`. (c) misses `SetAppConfig`, whose artifact code fields render
new artifacts.

## 101. Each app runs as its own uid from a fixed range, assigned by the store; `systemWorker.appUser` is removed (supersedes entry 55)

*Hardening after adversarial review.*

**Call.** The app record gains `uid`. `insert_app` assigns the lowest free
uid from `APP_UID_BASE` (1900000000; 65,536 of them) inside the replicated
write, so two installs never pick the same one, and a unique index holds
it; a migration numbers existing apps in name order. The render always sets
an `exec` task's `user` to that uid (gid the same number), a manifest may
not name `user`, and the exec runtime runs a bare numeric uid with no
passwd entry (`HOME=/nonexistent`). `systemWorker.appUser` is gone.

```text
gitops 0.2.0 runs its own code (1 artifact) on your managers, as its own unprivileged user,
```

**Alternatives.**
- (a) Per-allocation uids picked by the worker, like the container
  user-namespace pool.
- (b) A uid hashed from the app's name.
- (c) Named users (`legion-app-<name>`) the operator creates.
- (d) Keep one `appUser` and protect `secrets/` with per-allocation groups.

**Trade-off.** The range is fixed and undocumented hosts could already use
it (documented: keep it free). A freed uid is reused by the next install,
so files a removed app left outside its allocation directory (none by
default) would be readable by the next. Breaking: `systemWorker.appUser`
and manifests' `user` on exec tasks are refused.

**Why (security boundary, simplicity).** One shared `nobody` let any app
read the autoscaler's token from its `secrets/` (owner-only files are
nothing between processes of one uid). A uid per app in the record is one
number the render, the docs, and an operator's `ps` agree on; it survives
restarts and moves between nodes. (a) gives two replicas of one app
different users and needs worker-side bookkeeping for no gain between
apps; (b) collides; (c) needs host provisioning before an install works;
(d) still shares the uid for everything else (the config file, `/proc`).
Forcing the user closes the remaining hole: a manifest that could name a
user could name another app's uid.

## 102. A page's `rpcs` are bounded by its installer's rights, checked at install as a grant; ACL, token, app writes and key rotations leave `PAGE_RPCS` (revises entries 80 and 81)

*Hardening after adversarial review.*

**Call.** `PAGE_RPCS` names the permissions behind each RPC (its gate, plus
the extra checks a few make: `RestoreInstance` is `snapshot:read` and
`job:write`). Install and upgrade turn each listed RPC into requested rules
covering everywhere it reaches, reserved scopes included (`namespaces = ["*",
"legion-*"]`, `paths = ["*", "legion/*"]`, `pools = ["*", "legion-*"]`), and
run the grant check against the caller. ACL policy, user, token, exec-key,
grant and namespace-policy writes, `DeleteJoinToken`, every app write, and
`RotateDataKey`, `RewrapKv`, `RotateMeshCa` are no longer page-callable. An
integration test checks each entry's first permission against the
`acl_endpoints` gate table.

```text
Error: page "console" lists GetKv, which runs as whoever views the page, so it asks for kv:read on legion/*, which your token does not hold (...)
```

**Alternatives.**
- (a) Bound every bridged call at runtime by viewer ∩ a page grant the
  installer recorded, in `authenticate_acl` (the reviewer's second fix).
- (b) Require the permissions only on non-reserved scopes (`*`).

**Trade-off.** Installing a page with broad RPCs takes a near-management
token, and a page whose installer later loses rights keeps working
("grants outlive their granter", a TODO). The bound is per RPC's own
checks, not per request: a viewer still decides what a write targets.

**Why (security boundary, simplicity).** A static check is the grant check
everyone already knows, with its messages and dry run; (a) needs an
intersection of glob rule sets the ACL engine does not have, applied on
every call's identity, including the management viewers who bypass rules.
Asking for reserved scopes too is what makes "a page can never make a
viewer do what its installer couldn't" true without (a): a viewer's call
can target `legion-system` and `legion/` keys. Writes that change who may
do what are refused outright, because a "Run as me" from an administrator
would be a full takeover.

## 103. Default filling is bounded, and a schema that applies itself to the same value is refused at install

*Hardening after adversarial review.*

**Call.** `fill_defaults` does not follow a `$ref` back into a schema it is
already inside of without descending into the value, nor at all inside a
value a default just supplied, and fails past 10,000 added values or
100,000 schema applications. `compile_schema` (extension and config
schemas) refuses a schema whose `$ref`/`allOf`/`anyOf`/`oneOf`/`not`/`if`
chain comes back to itself on the same value, which the validator follows
until its stack overflows and aborts the process (verified: `{"allOf":
[{"$ref": "#"}]}` crashed the test binary).

**Alternatives.** Only the reviewer's insertion cap; recursing into no
inserted default at all.

**Trade-off.** A recursive schema fills defaults one level into a value it
supplied (it still fills everything the value spells out); a schema with a
same-value loop, which no validator could finish, is refused at install.

**Why (performance, robustness).** Other code is about to reuse the fill
for app object kinds, so it has to be safe on any schema, not just the
reviewer's; the cycle guard keeps legitimate recursion (trees, linked
lists) working where a blanket "no recursion into defaults" would lose a
default's own nested defaults.

## 104. Placement takes `operator:write` (not the app's grant)

*Hardening after adversarial review.*

**Call.** `UpdateAppSettings` that changes placement or pool, and
`InstallApp` of a manifest whose `placement` is not `managers`, need
`operator:write` beside `app:write`. Replicas and resources do not.

```text
Error: changing the placement or pool decides which nodes hold app autoscaler's token and run its code with its grant, so it needs operator:write as well as app:write
```

**Alternatives.** Require the app's whole grant, as a code field does (the
reviewer's first fix); refuse pools whose worker types do not list
`legion-system`.

**Trade-off.** A person who may manage an app but not operate the cluster
can no longer move it; seeding (`apps.<name>.placement` in the manager
config) is the operator's and unchanged.

**Why (security boundary).** Placement decides where the token lands: a
statement about the cluster's nodes, which is what `operator` means. The
app's grant says nothing about who has root on which pool, and refusing
pools by worker type would break `any` and `workers` placement, which are
features.

## 105. Delegations stay coarse: a permission on a scope, for 60 seconds (accepted)

*Hardening after adversarial review.*

**Call.** Not changed. A delegation is `resource:action` on the pressed
context, usable for its 60 seconds on any RPC that permission gates; a
"Pause" press lets the app call `ScaleTask(count = 0)` on that job as the
viewer.

**Alternatives.** Bind a delegation to named RPCs, or to one call.

**Trade-off.** The app can do more as the viewer than the button says,
within the permission the viewer approved on that one context; an app can
also skip delegation and use its own grant.

**Why (simplicity).** The bound that matters is the scope and the
permission, which the manifest declares and the viewer holds; RPC-level
binding needs a per-RPC permission model the ACLs do not have (entry 102
is the first table of one). Recorded in TODO.md as a follow-up.

## 106. A view entry without a permission answers with the app's grant (accepted for now)

*Hardening after adversarial review.*

**Call.** Not changed. `job`, `task`, and `node` entries may omit
`permission`; then any `app:read` holder can ask for the view on any
context, and the app answers from its own grant.

**Alternatives.** Default the permission to `job:read` (or `node:read`) on
the context.

**Trade-off.** An app whose view shows more than names leaks it to every
`app:read` holder; the bundled autoscaler declares a permission on every
entry.

**Why (scope).** Changing a default is its own decision (it changes what
existing manifests show to whom); the hardening pass leaves it as a TODO,
with the default the reviewer suggests.

## 107. A bridged write is confirmed for the message it will send, and the dialog arms

*Hardening after adversarial review.*

**Call.** The SPA refuses a request that spells a field both ways (`nodeId`
and `node_id`), derives the confirmation's target and detail from the
decoded message's JSON, keeps "Run as me" disabled for 500 ms after each
confirmation appears (re-armed for each queued one), and focuses Cancel.

**Alternatives.** Pick a spelling and drop the other; a longer delay; a
typed confirmation.

**Trade-off.** A page that sends both spellings gets `invalid_argument`
rather than a guess; every approval waits half a second.

**Why (safety).** The decoder keeps the last spelling, so showing the raw
request let a page show one node and forget another; refusing is clearer
than silently choosing. The delay stops a double-click from approving the
next queued call.

## 108. The page guard answers every refusal alike, before authentication

*Hardening after adversarial review.*

**Call.** `check_page_call` returns one `PermissionDenied` for an
uninstalled or disabled app, an undeclared page, and an unlisted RPC, and
logs the reason instead.

**Alternatives.** Run the check inside `authenticate_acl`, after
authentication, with detailed messages for authenticated callers.

**Trade-off.** A page author debugging a refusal reads the manager's log
(the SPA already refuses most cases itself, with details).

**Why (simplicity).** The tower layer sees every route, Open ones
included; moving the check into `authenticate_acl` would skip RPCs that
never authenticate (`Login`), and the detailed message helps no one the
SPA's own check does not already help.

## 109. An app is its stamped job: install takes over no job, and identity needs `managedBy`

*Hardening after adversarial review.*

**Call.** Install refuses (`FailedPrecondition`) when `legion-system/<name>`
exists and its latest version is not stamped `managedBy` this app.
`app_of_identity` counts an allocation as the app's only when the version
it runs carries the stamp for that app, and views, actions, pages, and
validate calls are forwarded only to such allocations.

**Alternatives.** Check the stamp at install only.

**Trade-off.** An operator's own job named like a future app blocks that
app's install until it is destroyed.

**Why (security boundary).** The job name alone is not the app: allocations
of a version someone submitted straight to the job would otherwise speak
for the app (app-plane RPCs, delegations) and receive its viewers'
assertions.

## 110. Idempotency scopes include how the caller authenticated

*Hardening after adversarial review.*

**Call.** A scope is `<source>:accessor:<id>` or `<source>:principal:<name>`.

**Alternatives.** Prefix with the source only for callers without an
accessor.

**Trade-off.** Retries recorded before the change are not recognized
(nothing to keep: no installed base).

**Why (simplicity).** One shape for every caller; a JWT subject named like a
delegated identity or an accessor can never share another's scope.

## 111. Direct writes to an installed app's job are refused, for everyone, with no `--force` (supersedes entry 13's reverting)

*Hardening after adversarial review.*

**Call.** While an app has a record, `RunJob`, `RunJobTemplate` (and
`PlanJob`, which predicts them), `ScaleTask`, `StopJob`, `DestroyJob`, and
`StopRollout` on `legion-system/<app>` are `FAILED_PRECONDITION`, for every
caller, management tokens included, naming the `legion app` verb that does
the job instead. `AdvanceRollout` stays allowed. Once the app is
uninstalled, its stopped job is an ordinary job. The reconciler still
reverts a version that reached the job some other way (one an older
release let through), and the `job info` banner says edits are refused.

```text
$ legion job scale legion-system/autoscaler --count 0
Error: job legion-system/autoscaler is app autoscaler's, and the app reconciler owns it, so scaling it directly is refused (docs/apps.md, "Editing an app's job")
Hint: `legion app set autoscaler` changes its replicas, placement and resources, `legion app config set autoscaler` its config, `legion app disable autoscaler` stops it, and `legion app uninstall autoscaler` removes the app, after which the job is an ordinary job
```

**Alternatives.**
- (a) Keep reverting (entry 13), and also clear foreign scale overrides in
  the reconcile pass (the review's second fix for M3).
- (b) Refuse, but let a management token override with `--force` (a flag
  on each job verb).
- (c) Refuse `AdvanceRollout` too, as the brief's list of rollouts said.

**Trade-off.** An admin's emergency hand edit of an app's job is gone; the
paths are `legion app disable` (which relies on the reconciler to stop the
job) and `legion app uninstall` (which stops it in the RPC). An app's job
can no longer be autoscaled (its count is `replicas`; the autoscaler's own
`ScaleTask` is refused too). A GitOps tool that syncs `legion-system` must
switch to `legion app`. Breaking (no installed base).

**Why (simplicity, correctness).** Reverting was a fight: a GitOps or CI
tool that syncs `legion-system` and the reconciler each wrote a version per
round, forever, with two restarts each, and versions are never pruned; and
a scale override is not a version, so it survived every revert and
silently disabled the app (M3). Refusing gives the writer an error it can
act on, and makes "the job is the record" true by construction. (a) keeps
the fight and adds a second reconcile rule. (b) adds a flag to five verbs
for a case `disable` and `uninstall` already cover, and an escape hatch is
exactly what a sync tool would learn to pass. (c) would deadlock an app
whose manifest asks for manual rollouts: advancing moves toward the
reconciler's version, not away from it.

## 112. Stored app records read leniently, and one that still does not decode is that record's problem only (H3; design concern 5)

*Hardening after adversarial review.*

**Call.** The store decodes app rows one at a time. A manifest or grant
first decodes strictly; on failure, the fields this build's typed parts do
not know (`AppManifest`, its `config` and `codeFields` entries, `AclRule`)
are dropped and it decodes again (`AppManifest::from_stored`). A row that
still fails (a changed type) is `CorruptApp`: logged once per (name,
`updated_at`, error), counted as `legion_apps{status="corrupt"}`, listed by
`ListApps`/`GetApp` with status `corrupt` and `corrupt: <error>`, skipped by
the reconciler (`PassOutcome::Corrupt`) and by the registry of jobspec keys,
and removed by `UninstallApp` by name (a bundled one included; the
reconciler's full pass now seeds every time, so it comes back from the
running build). Installs and upgrades keep parsing strictly.

```text
NAME  VERSION  SOURCE  STATUS   ...  DETAIL
lite  -        file    corrupt  ...  corrupt: app.manifest: invalid type: string "three", expected u32 (`legion app uninstall lite` removes the record)
```

**Alternatives.**
- (a) Keep `deny_unknown_fields` on stored records and only isolate rows
  (any rename makes the row corrupt).
- (b) Drop `deny_unknown_fields` everywhere and check unknown fields at
  install another way.
- (c) A migration per manifest-format change.

**Trade-off.** A field renamed between releases silently loses its value
on read (it gets its default), instead of failing loudly; the next
`upgrade` (or, for a bundled app, adoption) rewrites the record whole. The
known-field lists are hand-kept beside the structs; a unit test holds them
to the schema and serialization.

**Why (simplicity, robustness).** AGENTS.md says to rename freely, and the
bundled autoscaler's manifest is a stored record every release rewrites, so
the strict read was a cluster-wide outage waiting for the first rename:
every submit reads the installed keys, and one bad row failed them all.
Lenient reads make the common case (a field added, renamed, removed) a
non-event, and per-row isolation makes the rare one (a changed type) one
app's problem with a one-command fix. (a) turns every rename into that
fix. (b) loses serde's "unknown field `x`, expected one of …" at install,
which is the message authors need. (c) is the ceremony the policy exists
to avoid.

## 113. Keyed app actions run on the Control leader and hold their key while they run (H1)

*Hardening after adversarial review.*

**Call.** `InvokeAppAction` calls `require_lane_leader(Control)` right after
its ACL check: a follower answers the typed not-the-leader rejection before
anything reaches the app, so the leader-forward layer's replay is the only
run. With an idempotency key, the leader holds the key in memory
(`InFlightKeys`) from before the lookup until the reply is recorded; a
duplicate waits up to 45 seconds and then finds the reply
(`deduplicated`), or gets `ABORTED`. Recording the key after the app ran is
best effort: a failure is logged, never returned, because a returned
not-the-leader would be replayed.

The other RPCs with an external side effect before a replicated write were
checked for the same replay hazard:
- `ask_apps_to_validate` (RunJob, RunJobTemplate): same fix (entry 127).
- `GetAppView`, `GetAppPage` (`ForwardAppRequest`): a read, and nothing
  replicated after it; a follower answers them itself.
- `ForwardAppRequest` users are only those three and the validate call.
- The autoscaler's plugin calls are made by the app, not inside a manager
  RPC: its intents are compare-and-set rows written *before* the calls
  (`RecordScalingAction`), so a replayed write cannot repeat a call.
- `IssueBootTicket`, `CreateJoinToken` and the other keyed writes have no
  external effect before their replicated write.

**Alternatives.**
- (a) Write a "pending" idempotency row before forwarding, and update it
  after.
- (b) Check leadership only when a key is present.

**Trade-off.** An action on a follower costs a forward to the leader even
without a key; an action fails while the leader is unreachable. A
leadership change while an action runs can still let a retry with the same
key run it again on the new leader (the in-flight set is per manager), as
can a retry after the 24-hour key expiry. Documented.

**Why (correctness, simplicity).** The bug was a side effect before the
write that the forward layer assumes nothing precedes; failing first is the
one ordering that makes the replay safe. (a) needs UPDATE and DELETE on
`idempotency_record`, which is a commutative insert-only table (the raft
capture guard refuses anything else), and a pending row left by a crash
would block the key for a day. (b) leaves unkeyed actions running on
followers, where their audit events are forwarded best-effort.

## 114. The reconciler's submit is compare-and-set on the app record, and jobs stamped for no record are swept (H2)

*Hardening after adversarial review.*

**Call.** `GrantSource::App` carries the record's `updated_at`; inside the
submit's own transaction (`handle_job_submit_tx`, before the version or its
grant is written), the store refuses with `StorageError::Aborted` (gRPC
`ABORTED`) unless the app's row exists, is enabled, and is at that
`updated_at`. The reconciler treats `ABORTED` as "render again" (no
backoff, no event). Uninstall's first step (disable) therefore defeats any
submit in flight. Each full pass also finds jobs in `legion-system` whose
latest version is stamped for an app with no row and that are not stopped,
and stops them and revokes their grants (`stopped the job of an app that
is not installed`); a pass for a missing app does the same.

**Alternatives.**
- (a) Only the sweep (the review's first fix).
- (b) Compare the manifest digest instead of `updated_at`.

**Trade-off.** Any record write during a submit (a settings change, a
second `config set`) aborts it and costs one more render. The window
between the submit's commit and its separate `clear_job_stopped_status`
write remains; the sweep closes it within a pass.

**Why (correctness).** Entry 51's "disable first" argument assumed every
submit reads the record after the disable; one already past its read was
the counterexample. Checking in the same entry as the version is the only
check that cannot be raced. (a) lets the job run with a fresh grant until
the next full pass. `updated_at` moves on every operator write, including
disable, which a digest would not show.

## 115. Status counts only the latest version's healthy allocations; a record write retries an unhealthy job; rollback is deferred (M1; design concern 2)

*Hardening after adversarial review.*

**Call.** `app_job_allocations` splits healthy allocations into `running`
(the job's latest version) and `previous`. `derive_status` reads `running`
only at the latest version; `upgrading` when it falls short and
`previous > 0`; `degraded: upgrade failing: the rollout to job version N
failed (…)` when the newest rollout to that version is `failed` (rolled
back or stopped at a deadline). A pass that finds the job current but not
healthy, after a write that left `pending_change` set (`legion app enable`,
`legion app upgrade` with the same manifest), resubmits it with
`force_reschedule` under the key `…:<version>@<updated_at>`. No `previous
manifest` column and no `legion app rollback`.

**Alternatives.**
- (a) Keep `previous_manifest` on the record and add `legion app rollback`
  (a proto field or RPC, a migration, a CLI verb, a capability, docs).
- (b) Retry an unhealthy job on every pass.

**Trade-off.** Going back means keeping the old manifest file and
upgrading to it. A config change's rollout shows `upgrading` too (any new
version is an upgrade of the job).

**Why (simplicity).** The review's harm was that a failed upgrade looked
like success and could not be retried; status and the retry fix both, and
rollback is a new surface the brief allowed only if small. It is not: a
migration, a reply shape, and a decision about which config goes back with
the manifest. (b) would restart a crash-looping job every minute. Recorded
in TODO.md.

## 116. Install, upgrade, config and settings changes dry-run the submit's admission chain (M8)

*Hardening after adversarial review.*

**Call.** `submit_rendered_job_from` is split into `admit_rendered_job`
(everything before the store write) and the write. The app RPCs call
`admit_app_job`: the render, as the reconciler's identity (the record's
grant), through `admit_rendered_job` with `SubmitOrigin::App`, then the
namespace quota check `PlanJob` runs. A refusal keeps its code and reads
`app <name>'s job would be refused when the reconciler submits it: …`; on
`install --dry-run` it is the error, not `uncovered`.

**Alternatives.** Run `PlanJob` itself; check only at install.

**Trade-off.** Install and upgrade resolve pins (a registry round trip) and
hold the RPC for it; a registry blip refuses the install (`UNAVAILABLE`,
retryable) rather than parking the app.

**Why (correctness).** The checks the reconciler's submit makes are the
contract; running the same function as the same identity cannot drift from
it, which a plan-shaped copy could. Settings and config changes can trip a
quota or a pool policy just as an install can.

## 117. A failed app job is restarted with backoff (M2)

*Hardening after adversarial review.*

**Call.** A latest version whose job status is `failed` is never current.
The reconciler resubmits it (the submit's `clear_job_failed_status` starts
the same version again), with key `…:failed-<version>`, and backs off
5s→5m between restarts while it keeps failing (`restarts`, `restart_at` in
the leader's memory; the loop wakes for it). Status reads `degraded: its job
failed; …`, and while waiting, `its job failed; restarting it in Ns
(restart N)`. The count resets once the job runs healthy.

**Alternatives.** Leave a failed job to the operator (`legion app
disable`/`enable`); restart without backoff.

**Trade-off.** A job whose own policy gave up is overruled; an app that
fails for good restarts every five minutes.

**Why (correctness).** An app is the platform's own add-on: "keep it
running" is the reconciler's whole job, and the job's `moveCount` or a KV
`onDelete = "fail"` describe one placement's failure, not the app's wish
to stop. The backoff keeps a hard failure from spinning.

## 118. A pin that could not be reached is `UNAVAILABLE`, classified by the error's type (M4)

*Hardening after adversarial review.*

**Call.** The submit puts a `PinResolveFailure { transient }` context on a
pin resolution's error. Transient: an OCI `RequestError`/`IoError`, a
`ServerError` 5xx or 429, a `reqwest` connect or timeout error, an I/O
error of a connection kind, or git's stderr naming DNS, a refused or reset
connection, a timeout, or an HTTP 5xx/429. Those make the submit
`UNAVAILABLE` ("the source could not be reached; the submit can be retried
as it is"), which the reconciler backs off on; everything else stays
`INVALID_ARGUMENT`, a refusal.

**Alternatives.** Map every resolution error to `UNAVAILABLE`; retry
refusals on a slow timer too.

**Trade-off.** Git's classification reads text, and a new failure wording
falls back to "refusal" (parked until a write, as before).

**Why (correctness).** A typo'd tag should stop and say so; a registry
blip should not park an app until someone touches it. Classifying by type
keeps both, and RunJob's callers get a `retryable` envelope that is true.

## 119. Bundled apps agree on the version; mixed builds of it get a set id, and adoption needs the version only (M5)

*Hardening after adversarial review.*

**Call.** `decide_build_marker` groups ready eligible nodes by
`buildVersion`. One version: the marker is that version with the single
build id, or `mixed-<hash of the sorted distinct ids>` when several
binaries of it run. Several versions: hold, naming the nodes behind the
newest. Adoption compares the leader's `buildVersion` with the marker's,
not the pair.

**Alternatives.** Key the marker on the version alone (no rebuild rolls);
key `buildId` per architecture.

**Trade-off.** A dev rebuild rolled out one node at a time rolls the app at
each step (the set changes each time), instead of once; two builds of one
version that embed different manifests could flap adoption between leaders
(only in development).

**Why (correctness).** amd64 and arm64 builds, and distro and tarball
packages, always differ in `buildId`, so a mixed fleet never agreed: a fresh
cluster's autoscaler never started. The set id keeps entry 14's point (a
rebuild without a version bump rolls) while letting a mixed fleet settle.

## 120. A bundled upgrade whose new schema refuses the stored config is held, and `config set` accepts a config for the held schema (M6)

*Hardening after adversarial review.*

**Call.** When the embedded manifest's config schema refuses the stored
config, adoption returns `Held`: the manifest is not adopted, the job stays
rendered for the record's previous marker, `marker_reason` starts
`upgrade held: ` (status `degraded` with it), and `app.reconcile_failed` is
emitted when the reason first appears. `SetAppConfig` on a bundled app in
that state accepts a config that fails the stored schema if it matches the
embedded one; the next pass adopts.

**Alternatives.** Adopt anyway and report (as before); migrate the config
(drop what the new schema refuses).

**Trade-off.** Holding the job spec does not hold the binary: `legion:self`
runs whatever its node has, so an allocation that restarts on an upgraded
node runs the new build with the old config and fails until the config is
set (documented). The rest of the upgrade (new permissions, a new job
shape) waits on the operator.

**Why (correctness).** Adopting guaranteed a crash loop with the failure
buried in `last_action`; holding keeps whatever still runs running and puts
the reason in status. Migrating would guess at the operator's intent.

## 121. An upgrade that drops an extension point jobs use needs `--force`, and the prompt reports orphaned values apart (M7)

*Hardening after adversarial review.*

**Call.** `UpgradeApp` computes the jobs whose values the upgraded registry
reads as `orphaned` (`orphaned_jobs`); without `force` the upgrade is
`FAILED_PRECONDITION` naming them, as uninstall is, and the dry run lists
them and says so. `lifecycle_report` lists orphaned values on their own
lines instead of counting them as passing.

**Alternatives.** Report only, as before.

**Trade-off.** One more flag on `upgrade`.

**Why (consistency).** Dropping a point is uninstall's effect on those
values; every such job's next submit fails. The gate already existed for
uninstall.

## 122. Uninstall keeps the app's KV unless `--purge`, and the next app of that name is told (L3)

*Hardening after adversarial review.*

**Call.** Keys under `legion/apps/<name>/` stay. `--purge` (needs `kv:write`
on the prefix) deletes them and their locks in the same replicated entry as
the record's deletion. The reply carries `kv_keys` and `purged`; an install
whose prefix holds keys adds a prompt line before approval.

**Alternatives.** Delete on every uninstall; refuse an install while keys
are left.

**Trade-off.** Leftover state by default, and a same-named app from
another vendor can still take it over after the prompt.

**Why (safety).** Uninstall-and-reinstall to fix something is common, and
losing an app's state to it is not recoverable; telling the next installer
is enough for "not silently".

## 123. Every `legion app` write has a dry run

*Hardening after adversarial review.*

**Call.** `dry_run` on `SetAppEnabled`, `UpdateAppSettings`,
`SetAppConfig`, `UninstallApp` (with `--dry-run` on the CLI); settings and
config dry runs run the admission chain. `UpgradeAppReply` gains
`uncovered` and `orphaned_jobs`, and the CLI refuses before asking when
either says it must.

**Alternatives.** Dry runs on install and upgrade only (as before).

**Trade-off.** Four proto fields.

**Why (agent surface).** AGENTS.md asks for a preview path wherever one
can exist; the upgrade's refusal was only in message text.

## 124. A node-group intent is acted on for at most 120 seconds (L6)

*Hardening after adversarial review.*

**Call.** `act_node_group` runs every step under one deadline
(`ACT_BUDGET`, `RECONCILE_AFTER_MS` − 30 s) from winning the intent: a step
not started by then fails with "not started, the 120s act budget was
spent", one still running is abandoned at it; both close the intent as
`failed`, and the next tick decides again.

**Alternatives.** Renew the intent while acting; raise
`RECONCILE_AFTER_MS`.

**Trade-off.** A decision with several slow target calls finishes over
several ticks.

**Why (correctness).** Sequential `target/terminate` calls of up to 120 s
each, behind a plugin's single turn, could outlast the 150 s after which
another replica reconciles the intent and acts on the group: two replicas
acting at once. A deadline needs no new protocol; renewal would need
another compare-and-set write per step.

## 125. Install and uninstall write their idempotency key in the record's own entry, and uninstall's retry returns the first reply (L5)

*Hardening after adversarial review.*

**Call.** `insert_app_recorded` and `delete_app_recorded` call
`idempotency_record` inside the same `replicated` closure (it composes into
the capture), with the uninstall's whole reply as JSON.

**Alternatives.** Document the gap.

**Trade-off.** None found.

**Why (correctness).** A crash between the two entries made a retry read
"already installed" or `NotFound`; one entry cannot split.

## 126. The reconciler forgets everything on acquiring leadership, and checks "already current" before a parked refusal (L1)

*Hardening after adversarial review.*

**Call.** `reset_runtime` on every acquisition; the refused-skip moved after
the current check.

**Alternatives.** Clear only refusals.

**Trade-off.** A failing app is retried once more after a leadership flip.

**Why (correctness).** Another leader may have fixed what this one's
earlier term parked; entry 53's "a new leader retries once" is now true of
the same manager too.

## 127. Apps' validate calls are asked on the Control leader, and a short answer warns (L2)

*Hardening after adversarial review.*

**Call.** `ask_apps_to_validate` requires Control leadership before calling
any app (when any app is to be asked), so a follower's submit is replayed
before the calls. A value past the end of an app's `results` gets the
fail-open warning instead of passing silently.

**Alternatives.** Correct the docs only.

**Trade-off.** A submit with validated keys on a follower fails while the
leader is unreachable (it would have failed at the write anyway).

**Why (correctness).** Same as entry 113, for a side effect that is only a
cost, and the docs' "one call per submit" becomes true.

## 128. An upgrade keeps the operator's settings, and says so where the new defaults differ

*Hardening after adversarial review.*

**Call.** Unchanged behavior, now stated: replicas, placement, pool and
resources stay; the prompt adds `Keeps replicas 1 (this version's default
is 3; …)` when the new manifest's default differs from both the old
default and the current setting.

**Alternatives.** Adopt the new defaults when the setting still equals the
old default.

**Trade-off.** A vendor's new default reaches only new installs.

**Why (simplicity).** Settings are the operator's; guessing whether an
equal value was chosen or inherited would move apps under people.

## 129. Design concerns 3, 4 and 7 of the review are accepted for now

*Hardening after adversarial review.*

**Call.** Not changed: the reconciler is one serial loop that blocks on
admission (3); a render depends on the leader's build, so a mixed-version
manager set re-rolls apps when leadership moves (4); validation fails open,
and an app has no health of its own beyond its allocations' (7). Concern 5
is entry 112, concern 2 entries 115 and 117, concern 1 entry 111, and
concern 6 (placement as a path to the grant) was closed by entry 104. Each
open one is a TODO.md item.

**Alternatives.** A worker pool per app; pinning renders to the record;
app-reported health in status.

**Trade-off.** One slow registry stalls every app's pass for its timeout;
an upgrade window can roll apps twice.

**Why (scope).** Each is a design change with its own plan, not a fix.
