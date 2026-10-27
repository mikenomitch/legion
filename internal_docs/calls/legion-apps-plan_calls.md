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

