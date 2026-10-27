# Judgement Calls: autoscaler-v2-plan

Decisions made while designing
[`plans/autoscaler-v2-plan.md`](../plans/autoscaler-v2-plan.md). **Entries are
ordered by project impact, highest first, and an entry's number is its
rank.** These are design-time calls. Implementation inserts its own at
their rank. The superseded plan's calls
([autoscaler-plan_calls.md](autoscaler-plan_calls.md)) still govern the
shipped Phase 0 surface. The [ledger](#ledger-what-stands-from-the-superseded-plans-calls)
at the end maps which of them stand.

The substrate calls (where apps run, how they authenticate, how they reach
the UI) are in [legion-apps-plan_calls.md](legion-apps-plan_calls.md).

## 1. The autoscaler is a Legion app, a job, not a manager loop (reverses old entry 2)

**Call.** The autoscaler runs only as the bundled app
`legion-system/autoscaler`: an ordinary job, hosted by default on the
managers' system workers, reading and writing through the public API. No
in-core evaluator exists, so there is no "same loop, two deployment modes".

```console
$ legion app list
NAME        VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS
autoscaler  0.14.0   bundled  running  managers   1/1
```

**Alternatives.**
- (a) The old plan: an in-core loop on the scheduler-lane leader, default
  on, with `legion autoscaler agent` as an escape-to-job mode running the
  same code. You get zero extra processes on day one, and direct store
  reads.
- (b) The user's first framing: a **special job type** the manager runs
  itself, on managers.

**Trade-off.**
- Day one has more moving parts: a process, a placement, a gRPC hop, and a
  dependency on the scheduler being able to place `legion-system` jobs.
- If the app's allocation dies, scaling pauses until the reschedule (tens of
  seconds). The manager has to report that absence loudly (plan §6.6), or
  it would be a silent failure.
- Every input must exist as an RPC, so `GetScalingInputs`,
  `GetCapacityDemand`, and `WatchScalingPolicies` are new API surface we
  now maintain.

**Why (extensibility and performance, at a bounded simplicity cost).**
Running out of process is what makes the three plugin systems safe.
User-supplied plugins crash or hang the app, never the manager, which
dissolves the security argument the old plan used to keep logic out
(entry 4).

It also makes the autoscaler scale. A leader-only in-core loop can't be
sharded, and an app can (entry 13).

It upgrades and restarts independently, it dogfoods the public API (so
agents get the same surface), and it is the first user of a substrate GitOps
needs anyway. A special job type (b) would add a scheduler concept to buy
what a namespace restriction plus a pool already give (apps calls entry 1).

The simplicity cost is paid by the bundled-app reconciler. A fresh cluster
still needs zero configuration (apps scenario A), which was the property the
in-core loop existed to protect.

## 2. `autoscale = true` means: min = count, max = 4 × count, CPU at 70% of the reservation, fast up, slow down

**Call.** The one-word form expands to:
- bounds of `count..4×count`;
- a single CPU target-value check at 0.7 of the reservation over a 1m
  window;
- up: no stabilization, at most max(+100%, +4) per minute;
- down: 5m stabilization, at most max(−10%, −1) per minute;
- 30s warmup and a 10% tolerance.

`legion job plan` prints the expansion. `count` becomes the floor.

```console
autoscale web/default/web:
  hold cpu at 70% of 500MHz per replica, 1m average over ready replicas
  bounds 2..8 (min = count, max = 4 × count)
```

**Alternatives.**
- (a) **HPA's `min = 1`**, with `max` required. This is the mainstream
  expectation, saves the most money overnight, and forces the user to
  think about the ceiling.
- (b) **The old plan's rule**: `count` is the initial value only, and `min`
  defaults to 1.
- (c) **CPU plus memory** (max of both), which covers memory-bound services.
- (d) **HPA's rate defaults**: 100% per 15s down after the 5-minute window.

**Trade-off.**
- `min = count` never saves money below the written count unless the
  author lowers `min` explicitly.
- `4×` is an arbitrary ceiling. A real 10× launch spike hits it, and
  `scale.limited` is the only warning.
- CPU-only misses services that run out of memory before CPU.
- The gentle scale-in (10% per minute) leaves a 30-replica service paying
  for about 20 minutes of excess after load drops.

**Why (simplicity: "trust it to be roughly right").**
- **The floor.** A user who wrote `count = 3` for HA would be harmed by an
  autoscaler that quietly takes them to 1. The safe reading of a number
  they wrote is "at least this many". It also keeps editing `count` in the
  spec meaningful: it moves the floor, where the old plan had to warn
  "spec count ignored".
- **The ceiling.** A required `max` would make the word `true` insufficient,
  and an unbounded default lets a broken signal spend the cluster.
- **Memory.** It usually doesn't fall with load. A memory check merged with
  max therefore blocks scale-in indefinitely, and one restricted to
  scale-out oscillates against CPU (CPU says in, the per-replica memory
  share rises, memory says out). We'd rather ship a default that is
  sometimes conservative than one that flaps.
- **The rates.** They trade cost for not being the cause of an incident.

The evidence that would revisit this: dry-run histories showing a large
share of policies pinned at `max`, or memory-driven OOMs on autoscaled
tasks.

## 3. Defaults layer field by field (built-in, cluster, namespace policy, task) and resolve live

**Call.**
- Every `autoscale` default can be overridden at three levels:
  - the task (`autoscale = { … }`);
  - the namespace (an `autoscale` record in a namespace policy);
  - the cluster (`legion scaling defaults apply`).
- A field set at one level wins over the levels below it, field by field,
  including nested records (`step` replaces as a whole).
- `min` and `max` take numbers or `"Nx"` multiples of `count`.
- Signals are all or nothing.
- The jobspec stores what the author wrote. The manager resolves the
  effective policy live, whenever any level changes.
- `scaling explain --policy` shows where every field came from.

```console
$ legion scaling defaults apply defaults.ncl      # { cpu = 0.6, max = "6x" }
cluster scaling defaults: v4; 212 policies inherit a changed field, 0 invalid
```

**Alternatives.**
- (a) **Per-task overrides only.** This is what the plan had before:
  `autoscale = { max = 20 }`, with fixed built-ins.
- (b) **Expand and freeze at submit.** Store the expanded record, so what a
  job version says is exactly what runs.
- (c) **Cluster defaults in the autoscaler's app config**
  (`legion app config set autoscaler`).
- (d) **Most-specific namespace policy wins** when several match.

**Trade-off.**
- A job's scaling behavior can change without its spec changing. That is
  mitigated by the policy digest on every `scale_history` row, and by
  provenance in `explain`, but `job inspect` alone no longer tells the whole
  story.
- A defaults change hits many policies at once. It is refused if it would
  invalidate any, and `--dry-run` shows the blast radius, but a valid
  change that is simply a bad idea lands everywhere.
- Four levels to reason about.

**Why (simplicity: "easily overridable" means override once, for many).**
- **Against (a).** It makes an operator who disagrees with a built-in edit
  every jobspec.
- **Against (b).** It turns a cluster-wide retune ("scale-in is too eager")
  into a mass resubmit.
- **Against (c).** The defaults shape how *core* expands a core field.
  Putting them in an app's config couples the two, restarts the app on
  every change (apps calls entry 13), and gates defaults behind `app:write`
  instead of the operator permission they deserve.
- **Against (d).** It breaks namespace policies' deliberate "no precedence
  to learn" rule. Conflicting fields fail the same way two `pools.default`
  values already do.

Live resolution, plus the digest, plus provenance, keeps one override in one
place, and always answers "why is it 0.6?" in one command.

## 4. Plugins run inside the app and speak JSON-RPC 2.0 over stdio or HTTP (reverses old entries 3 and 9)

**Call.** All three kinds (sources, strategies, targets) share one protocol:
newline-delimited JSON-RPC 2.0, the framing `legion mcp serve` already uses.
A `command` plugin is spawned once and kept alive. A `url` plugin receives
the same messages over HTTP POST, so it can be a Legion service of its own.
A built-in `exec` target wraps one-shot shell scripts. Built-ins implement
the same traits in-process.

**Alternatives.**
- (a) **Dylib kinds**, like five of Legion's plugin kinds. This is the
  old plan's step 2 (`plugins/metrics/`, `plugins/strategies/`).
- (b) **go-plugin-style gRPC** over a local socket (Nomad), or KEDA's gRPC
  external scaler.
- (c) **WASM for strategies** (the old plan's step 3).
- (d) **One-shot exec per call**, the volume and cluster-CA precedent
  (`util::run_json_command`).

**Trade-off.**
- A persistent process protocol is new in the codebase: supervision,
  restart with backoff, per-call timeouts, and a conformance suite to own.
- JSON is slower than gRPC.
- There is no compile-time contract with plugin authors, only a versioned
  protocol name (`legion-scaling/1`) and a JSON schema per method.

**Why (extensibility).**
- **(a)** Dylibs pass `Box<dyn Trait>` across an unstable ABI (audit M6,
  still open), are Rust-only, and crash their host.
- **(d)** One-shot exec costs a process per call. That is fine at a node
  group's minutes-scale cadence, and unworkable at 10,000 policies × 15s
  against Prometheus.
- **(b)** gRPC needs codegen in the plugin's language for no benefit at
  these call rates.
- **(c)** WASM has no runtime in the tree, and can't do the network I/O that
  sources and targets exist for.

JSON-RPC over stdio is the smallest thing any language can implement in an
afternoon (persona: integration author). The HTTP transport makes
"a plugin is a service" free.

Old entry 3's security reasoning (no user logic in the manager) still
holds. It is satisfied by placement now, because plugins run in the app's
process tree, not by refusing plugins.

## 5. Tenant policies name operator-installed plugins; they never carry a command or a URL

**Call.** Plugins are installed in the autoscaler's app config, an
operator-only surface. Each may carry a `namespaces` allowlist. Policies
refer to plugins by name and supply only data: a query string, a target
value, a config object validated against the plugin's schema.

```
error: payments-prod/api/default/api: check 'lag' uses source 'team-prom', which is not
  available to namespace payments-prod (allowed: analytics-*)
```

**Alternatives.**
- (a) Let a policy specify `source = { prometheus = { address = … } }` or
  `target = { exec = { command = … } }` inline. That is the most
  self-service option, and it's what Nomad's policy files allow (because
  they are operator files).
- (b) Allow inline sources only for namespace owners holding a new
  permission.

**Trade-off.**
- A team with its own Prometheus must ask the operator to register it once.
- Within a shared source, a tenant's query can read any series the source
  can see. Its only output is a scalar that moves their own count, but it
  is a side channel, documented publicly with the advice to give each
  tenant group its own source.

**Why (security boundary).** Plugins run with the app's identity, which can
resize every job and drain nodes. An inline command is remote code
execution as that identity. An inline URL turns the app's network position
into an SSRF primitive aimed at internal endpoints (the host-guard plan
spent a phase closing exactly these doors for tasks). (b) only moves the
problem to whoever gets that permission. By-name references keep the
policy surface pure data, which is also what lets `scaling apply --dry-run`
and `scaling simulate` stay safe to run.

## 6. Safeguards are one fixed pipeline whose stages report what they changed

**Call.** Every decision passes the same ordered stages:

```
merge → headroom → stabilize → step → bounds → quota/capacity/rollout →
pin/pause → mode
```

Each stage records a note when it changes the number. The notes *are* the
explanation (`scaling explain`), the history row's summary, and the event
detail. There is no separate "reason" code path.

**Alternatives.**
- (a) HPA's style: independent behaviors, with conditions computed
  separately (`ScalingLimited`, `AbleToScale`) that can disagree with what
  happened.
- (b) A user-orderable pipeline, or safeguard plugins.

**Trade-off.** The order is fixed and opinionated. For example, bounds apply
*after* the step limit. A raised floor (a schedule's 08:00 `min = 10`, a
resubmit with a higher `count`) or a lowered `max` therefore takes effect at
once, as a jump the step limit can't pace. Some combinations can't be
expressed: you can't rate-limit a schedule's arrival.

**Why (simplicity).** The user's two asks, "flexible safeguards" and "know
what scaled and why", turn out to be one mechanism. A fixed order means an
operator can predict the result by reading the stage list once. Making
explanations a by-product of the pipeline means they can never drift from
what actually happened, which is the failure mode of most autoscalers'
"why" surfaces. Every safeguard the user named (rate up, floor down) is a
stage. New ones insert as stages, which is the extension point.

## 7. Node scale-up demand is computed by the manager, simulating the scheduler's own rules

**Call.** `GetCapacityDemand` runs on a manager. It takes the jobs short of
their effective count, builds a synthetic node per node-group template, and
packs the pending replicas onto them with the scheduler's feasibility
functions (`is_permitted`, constraints, `distinct`, ports, oversubscription
ratios, gang quorum). The autoscaler receives a number per group
(`nodegroup.pending`) and treats it like any other metric.

**Alternatives.**
- (a) The autoscaler reimplements placement from raw state (Nomad
  Autoscaler, Cluster Autoscaler's in-process scheduler framework).
- (b) The scheduler records structured shortfall on its hot path (the old
  plan's entry 8 mechanism).
- (c) Scale up on utilization only.

**Trade-off.**
- The demand is approximate. First-fit-decreasing packing is not the
  scheduler's scoring, so the simulation can over-ask by a node.
- It is a point-in-time read with no memory of trends.
- A manager does CPU work proportional to (short jobs × groups) on each
  call.

**Why (simplicity and performance).**
- **(a)** A second copy of the placement rules, outside the repo's
  scheduler tests, *will* disagree with the scheduler: pools-as-taints
  alone would be a classic drift.
- **(b)** It puts bookkeeping on the critical path, which the AGENTS.md
  scheduler rule makes expensive to change.
- **(c)** It can't scale from zero nodes.

Read-side simulation keeps one source of truth for "what fits", costs
nothing when unused, and keeps the plugin protocol scalar. Old entry 8's
*signal* (pending work, not utilization) stands. Only *where it is
computed* moves.

## 8. Node-group and external-target policies are stored objects, not manager config or app-local files

**Call.** `legion scaling apply nodes.ncl` stores a versioned
`ScalingPolicy` on the Control lane, gated by ACL. Jobspec `autoscale` blocks
compile to the same internal type. One `scaling list` shows both.

**Alternatives.**
- (a) The old plan's choice: node groups in manager config.
- (b) Put them in the autoscaler's app config.
- (c) Nomad's choice: policy files on the autoscaler's disk.

**Trade-off.** A new object kind with CRUD RPCs, ACL rules, and history,
which is the most API surface of the options.

**Why (simplicity at operation time).**
- **(a)** Manager config differs per manager, and changing it needs a
  restart (the same flaw `controlPlaneBindings` has).
- **(b)** It makes policies opaque to ACLs, the UI, and `scaling list`, and
  ties a capacity owner's edits to whoever may configure the app.
- **(c)** It keeps policies out of cluster state entirely, Nomad's
  long-standing complaint.

A stored object is validated at apply, versioned, listable, and
permissioned by namespace, the same as every other thing users
`apply` in Legion.

## 9. `autoscale` stays a first-class core field, not an `apps.autoscaler` extension block

**Call.** The autoscaler's per-task policy is the core jobspec field
`autoscale`, validated and resolved by the manager. It does not use the
generic jobspec-extension mechanism that third-party apps use
(apps calls entry 6).

**Alternatives.** Express it as `apps.autoscaler = { … }`: dogfood the
extension mechanism with the first app, keep autoscaling semantics out of
core, and let a third-party autoscaler replace the bundled one with no core
change.

**Trade-off.**
- Core carries the autoscale schema, its defaults, and its validation.
- The first app doesn't exercise the extension mechanism, so GitOps or a
  third-party app will be its first real user.
- A replacement autoscaler would use its own `apps.<name>` block, not
  `autoscale`.

**Why (correctness).** Core behavior depends on this field:
- submit arbitration over who owns the live count (plan §6.3);
- the `--for` rule on manual scales;
- in-place resubmits for `autoscale` and `count`;
- the layered defaults (entry 3).

Its submit errors need the task's runtime and count, and must be hard
errors. Extension validation is best-effort by design (an unreachable app
lets the submit through). The extension mechanism's main benefit is an app
that versions apart from Legion, and that buys nothing for a bundled app
that *is* the Legion binary.

## 10. A manual scale on an autoscaled task is a time-boxed pin, and `--for` is required

**Call.** `legion job scale web --count 12 --for 2h` pins the count, and
`--min` / `--max` with `--for` override a single bound. The pipeline treats
a pin as final until it expires or `--release`. Without `--for`, the command
is refused on autoscaled tasks:

```
error: web/default/web is autoscaled; a manual count needs --for (e.g. --for 1h),
  or remove autoscale from the jobspec
```

**Alternatives.**
- (a) HPA: accept the write and let the autoscaler overwrite it on its next
  tick.
- (b) Reject manual scaling of autoscaled tasks entirely.
- (c) A pin with no expiry by default, with a loud status.

**Trade-off.**
- One more flag to remember in an incident (the error says exactly what to
  type).
- An expiring pin can start a gentle scale-in while the incident is still
  open, if the responder underestimated `--for`.

**Why (simplicity for the on-call persona).**
- **(a)** It is the most-cited HPA surprise: the responder's action silently
  undone.
- **(b)** It takes away the one lever an incident needs.
- **(c)** It produces forgotten pins that disable autoscaling forever: cost
  drift no dashboard explains.

A required expiry forces the one decision that matters and guarantees
autoscaling resumes on its own. The step limits make the resumption gentle.

## 11. Usage reaches one place by rolling up on the liveness digest; nothing goes through raft (reverses old entry 11's storage)

**Call.** Each manager aggregates the usage and edge reports it received
into per-target rollups. Every 10s it sends the changed rollups to the
status-lane leader on the existing digest RPC. The leader merges them, keeps
one hour in memory, and serves `GetScalingInputs`.

**Alternatives.**
- (a) The old plan's choice: widen `allocation_health` with usage fields,
  replicated on the Status lane, so every manager has everything.
- (b) The autoscaler fans out to every manager and merges.
- (c) Workers push usage to the autoscaler directly.

**Trade-off.**
- After a status-lane leader change, inputs are empty until every peer's
  full resync arrives, about one interval. The missing-data hold covers
  that: nothing scales in, and the app's own stabilization windows survive
  because they live in the app. The one-hour simulation buffer is lost, so
  `scaling simulate` has less history.
- Rollups are keyed per (node, target), not per target, so the payload grows
  with how widely targets spread across nodes. That is the price of never
  double-counting a node that switches managers.
- The data is only as fresh as the 10s report interval.
- One more payload rides the digest.

**Why (performance).**
- **(a)** It turns a deliberately off-raft, 10-second, per-allocation
  signal into replicated writes. That is exactly what usage was designed to
  avoid (`src/manager/usage.rs` header: "worthless after a restart … never
  touches the state store").
- **(b)** It leaks manager topology into a client and multiplies reads by
  the manager count.
- **(c)** It couples workers to an app's address.

The digest already exists for the same "who heard what" problem (liveness),
already costs managers × 1 RPC per interval regardless of fleet size, and
rollups are bounded by the number of targets, not allocations.

## 12. Every autoscaler write is compare-and-set; overlapping leases are tolerated, not prevented

**Call.**
- `ScaleTask.expected_version` must match the `task_scale` row version the
  decision was computed from, or the write is refused with
  `FAILED_PRECONDITION`, and the app re-reads and re-evaluates.
- Node and external targets, whose effects happen outside Legion, first
  record an action intent (`RecordScalingAction`) with CAS on the policy's
  action version. Only the winner calls the plugin.
- Versions are raft log indexes, so they never repeat.
- Shard leases stay KV locks with no fencing.

**Alternatives.**
- (a) Add fencing tokens (a lock generation) to KV locks and check them on
  `ScaleTask`.
- (b) Trust leases, and accept a rare double write.

**Trade-off.** Each scale costs a version read (it is already in
`GetScalingInputs`), and in a hot overlap a human's write can make the
autoscaler waste an evaluation.

**Why (simplicity).**
- **(a)** It is a general KV feature, and it fences only autoscaler against
  autoscaler.
- **(b)** It is wrong exactly when it matters: during failover. For node
  targets, a double write means two ASG resizes or two drains of
  different victims.

CAS on the thing being written also fences autoscaler against human. It
closes the race where an incident responder's `job scale` is overwritten
by an evaluation that started a second earlier, with one integer column. The
intent row gives external side effects the same guarantee: the replicated
write is the fence, and the plugin call only follows it. Using the raft
index instead of a per-row counter closes the ABA case. Phase 0's
arbitration deletes and recreates override rows, which would otherwise
reset a counter to a value a stale writer still holds.

## 13. The app scales out by sharding policies across replicas

**Call.** Policies hash to 64 shards. Replicas hold membership and shard
leases in KV and balance to `ceil(64 / members)`. `legion app set
autoscaler --replicas 3` is the whole procedure. The default is one replica.

**Alternatives.**
- (a) Active-standby only (Nomad Autoscaler HA, cluster-autoscaler leader
  election, KEDA's operator).
- (b) Shard by namespace.

**Trade-off.**
- Rebalancing logic and lease churn to test.
- A policy's evaluations can briefly move between replicas, losing that
  replica's in-memory stabilization window. The cold-start guard covers it,
  at the cost of a scale-in pause for moved policies.

**Why (performance).** The user asked that this "scale very easily". With
the inputs already batched server-side, the remaining cost is external
queries per policy per tick, which active-standby can't spread. Namespace
sharding skews badly on one big tenant. Hash shards with a fixed count keep
rebalancing proportional and make "add replicas" the only scaling knob.

## 14. Actions are durable in `scale_history`; holds live only in the app

**Call.** Every applied change (autoscaler or human) writes a
`scale_history` row with the decision, in the override's transaction,
capped at 200 per target. So does every "would scale" decision of a
`dry-run` policy, with `applied = false`. An evaluation that held is kept
only in the app's memory, plus the per-target state in the manager's cached
`ReportScalingStatus`.

**Alternatives.**
- (a) Store every evaluation (a full audit trail of non-actions).
- (b) Rely on events: they are capped at 10,000 cluster-wide and
  documented as "not an audit log".

**Trade-off.** "Why didn't it scale at 03:12 last night?" can't be answered
after the fact. Only the current hold reason and the app's short ring are
available.

**Why (performance).** Evaluations happen every 15s per policy, and storing
them is raft write amplification for data that is almost always "within
tolerance". Actions are rare and are what audits ask about. Dry-run "would"
decisions are exactly as rare, and they are the record a team reads before
trusting a policy (scenario S2), so they get the same treatment. The events
journal is best-effort by design, so it can't be the record.

## 15. `autoscale` on a task in a multi-task group scales the whole group

**Call.** If the task's group has other tasks, the target is the group:
every member moves to the same count in one write, and utilization is the
sum of usage over the sum of reservations. Only one task in the group may
carry `autoscale`, and members must share a count.

**Alternatives.**
- (a) Scale only the declaring task. That is Phase 0's per-task model,
  with no new rule.
- (b) Add group objects to the jobspec, with `autoscale` on the group.

**Trade-off.** A new validation rule, and "set on one task, applies to its
siblings" is mildly surprising, so the `job plan` expansion says
`autoscale group api (tasks: api, sidecar)`.

**Why (simplicity).** The user asked to scale "task group counts", and in
Legion the group is the co-placed unit. Scaling one member of an
all-or-nothing group alone (a) produces groups that can't place. Group
objects (b) are a jobspec redesign this plan has no business doing.

## 16. Legion's core drains nodes; target plugins only start and stop instances

**Call.** For node targets, the app selects victims, cordons and drains
through `SetNodeAvailability`, waits, and only then calls
`target/terminate(ids)`, followed by `ForgetNode`. Plugins never touch
Legion's node lifecycle.

**Alternatives.** Nomad's shape: the target plugin owns drain and
termination, so a provider can do provider-specific things such as
lifecycle hooks or ASG detach semantics.

**Trade-off.** A provider whose native termination path drains differently
(an ASG lifecycle hook calling back into Legion, for example) has to opt
out of its own feature.

**Why (simplicity and correctness).** Drain correctness (stateful `onLost`,
`disruption.minAvailable`, the never-victim rules, abort and uncordon on
deadline) is Legion's to get right once, with one test suite. Repeating it
per plugin is how the old plan's "never strand a draining node" acceptance
criterion would fail in one plugin out of five.

## 17. The jobspec field is `autoscale`, a boolean or a record

**Call.** `autoscale = true` or `autoscale = { … }`.

**Alternatives.**
- (a) The old plan's `scaling` block, always a record.
- (b) Nomad's `scaling { enabled = true … }`.

**Trade-off.** A field that is either a boolean or a record is a slightly
unusual schema (serde untagged; the JSON schema uses `oneOf`).

**Why (simplicity).** The user's own phrasing ("set autoscaling to true on a
group") is the product, and `scaling = true` reads like a statement about
the job rather than a switch. The field never shipped, so renaming costs
nothing.

## 18. The bundled autoscaler holds `node:write` and cluster-wide `scale:write` in one grant

**Call.** The autoscaler's manifest requests one fixed permission set,
including `node:write` (drain, forget, join tokens), whether or not the
cluster has node-group policies.

**Alternatives.**
- (a) **Optional permission groups** in the manifest, granted only when
  enabled. For example a `nodes` group the operator turns on when applying
  the first node-group policy.
- (b) **Two apps**, `autoscaler` and `node-autoscaler`, each with its own
  grant.

**Trade-off.** A cluster that never autoscales nodes still runs a process
that holds `node:write`. Its compromise (through a plugin bug, say) could
drain nodes.

**Why (simplicity).**
- **Against (a).** It adds a grant state machine to every app, and a second
  approval step exactly when an operator is applying their first node
  policy.
- **Against (b).** It doubles the processes, and splits one pipeline and
  one status surface in two.
- **The real mitigations are elsewhere.** Plugins are operator-installed
  (entry 5), and node-group policies need `operator:write` to create. App
  config changes that add plugin code re-check the grant against the
  caller (apps calls entry 4). We'd revisit if a third-party app needs
  optional groups for its own reasons.

## 19. A `step` limit is the larger of its percent and replica halves, and writing `step` replaces both

**Call.** `step = { percent, replicas }` allows the larger of the two per
`every`. Writing `step` replaces the default record, and an omitted half is
0.

**Alternatives.**
- (a) The smaller of the two.
- (b) HPA's explicit `selectPolicy: Max|Min` over a list of policies.
- (c) Merge a partial `step` with the defaults.

**Trade-off.** "Larger" lets small services move quickly (+4 on a
2-replica service is +200%). A user who writes only `replicas = 2` might
expect the percent default to survive, and it doesn't.

**Why (simplicity).**
- **"Larger".** Small services need an absolute floor on movement, and big
  ones need a proportional one. One fixed rule covers both, without HPA's
  list-plus-selector.
- **Replacing the record.** Merging (c) makes `step = { replicas = 2 }` mean
  "2 or 100%", which silently does nothing. Replacing makes what you wrote
  the whole limit.

## Ledger: what stands from the superseded plan's calls

The superseded plan's calls are not rewritten (`plans/README.md`). This is
a map, not a judgement call, so it is unnumbered. Here is where each one
lands now:

| Old entry | Status | Note |
| --- | --- | --- |
| 1. Desired count is a separate owned record | **Stands** | Phase 0, the foundation of everything here |
| 2. Manager subsystem first, job second | **Reversed** | entry 1 |
| 3. Declarative policies; logic waits for WASM | **Reversed in mechanism, kept in spirit** | entry 4 (logic now runs out of process in the app, not in the manager) and entry 5 |
| 4. Resubmit ownership via `base_count` | **Split** | rule 2 (no policy: `base_count` arbitrates) stands. Rule 1 changes: `count` is the default `min`, not an initial value only, and submit arbitration keeps and clamps the override for autoscaled tasks (plan §6.3, entry 2) |
| 5. `task_scale` on Control | **Stands** | the new tables join it there |
| 6. Scale-out applies, scale-in defers during rollouts | **Stands** | enforced by `ScaleTask`, so the app can't bypass it |
| 7. `pending_count` promotion | **Stands** | |
| 8. Node scale-up keys on unplaceable work | **Stands, moved** | entry 7 (computed by the manager) |
| 9. Node providers speak the exec ABI | **Superseded** | entry 4 (JSON-RPC protocol; one-shot exec survives as the built-in `exec` target) |
| 10. Own `scale` ACL resource | **Stands** | the app's grant uses it |
| 11. Usage on the heartbeat's status write, windows in evaluator memory | **Reversed (storage), kept (windows)** | entry 11 |
| 12. `min` defaults to 1, `min = 0` needs a wake signal | **Split** | the default changed (entry 2); the wake rule stands |
| 13. First-party UI pages, link-only extensions | **Reversed** | apps calls entry 8 |
| 14. Refuse keyed and `count.per` | **Stands** | |
| 15. Scaling doesn't move `updated_at` | **Stands** | |
| 16. `scale_in` stop cause | **Stands** | autoscaler scale-ins report it too |
