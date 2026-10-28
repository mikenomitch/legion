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

**Call.** The one-word form means the built-in defaults, which are below.
Entry 3 lets a cluster or a namespace override them, except that an
inherited `min` can never go below `count`:
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
  mitigated by the policy digest on every `scale_history` row (each digest
  resolves to a stored snapshot of the effective policy), and by provenance
  in `explain`. But `job spec` alone no longer tells the whole story, and
  rolling a job back to an older version doesn't restore the scaling
  behavior it had then, because the defaults are today's.
- **A departure for namespace policies.** Until now, they act only at
  admission and write their results into the stored config: a policy's
  `pools.default` becomes a `pool = "…"` permit, and is never re-judged
  (`docs/namespaces.md` § When the checks run). `autoscale` defaults are the
  first policy field resolved live. The difference is deliberate. A pool
  decides placement, which must not shift under running work. A scaling
  default tunes behavior that the autoscaler re-reads every tick anyway.
  The public doc has to state it, because it breaks the rule readers will
  expect.
- **Inherited `min` is restricted.** A cluster or namespace can set `min`
  only as a multiple of `count` of at least `"1x"`, so defaults can never
  lower an HA floor that entry 2 protects.
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
  every change (apps calls entry 15), and gates defaults behind `app:write`
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
- **(c)** Legion now embeds WASM (`wasmi`, `src/wasm`, for network plugins),
  but that layer deliberately offers no sockets. It can't do the network
  I/O that sources and targets exist for, so it could only ever cover
  strategies. One protocol for all three kinds beats two half-measures. WASM
  strategies remain a good later addition on that layer (plan §4).

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

## 9. `autoscale` stays a first-class core field, not a key the autoscaler app declares

**Call.** The autoscaler's per-task policy is the core jobspec field
`autoscale`, validated and resolved by the manager. It does not use the
generic jobspec-extension mechanism that third-party apps use
(apps calls entry 7). Users see it in core places:

```
$ legion app extensions autoscaler
autoscaler declares no jobspec keys (autoscale is a core field: docs/autoscaling.md)
error: replacement-autoscaler declares `autoscale` on tasks, which is a core field
  hint: install it under another key: --key task.autoscale=ras
```

**Alternatives.** Have the bundled app declare `autoscale` as a task-level
extension key. Since extension keys are written as natural top-level keys,
the jobspec would look identical to users. This would dogfood the extension
mechanism with the first app, keep autoscaling semantics out of core, and
let a third-party autoscaler replace the bundled one with no core change.

**Trade-off.**
- Core carries the autoscale schema, its defaults, and its validation.
- The first app doesn't exercise the extension mechanism, so GitOps or a
  third-party app will be its first real user.
- A replacement autoscaler can't claim `autoscale`, because it is a core
  field and installs are refused on collision. It declares its own key
  instead.

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
  caller (apps calls entry 5). We'd revisit if a third-party app needs
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

## 20. Signals are all or nothing at every defaults level, not only the task's

*Implementation, engine core (`src/autoscaler/defaults.rs`).*

**Call.** If a level sets any signal (`cpu`, `memory`), its signals
replace, rather than add to, the signals inherited from the levels below it.
The plan stated this only for the task's own record; the engine applies it at
the cluster and namespace levels too.

```nickel
# cluster defaults
{ memory = 0.8 }   # effective default signal is now memory only, not cpu + memory
```

**Alternatives.** Merge `cpu` and `memory` field by field across levels, so
a cluster default of `{ memory = 0.8 }` adds a memory target to the built-in
CPU target.

**Trade-off.** An operator who wants "CPU and memory" everywhere has to write
both. A cluster-level `{ memory = 0.8 }` silently drops CPU scaling for every
inheriting task, which `scaling explain --policy` shows but nothing warns
about.

**Why (simplicity).** One rule at every level is easier to hold in your head
than "replace at the task, merge elsewhere", and it matches entry 3's
all-or-nothing rule for the task.

## 21. A namespace-policy conflict on a field is waived when the task sets that field

*Implementation, engine core.*

**Call.** Two namespace policies that set the same `autoscale` field to
different values make that field unresolvable, but only for tasks that don't
set the field themselves. A task that writes the field gets no error.

**Alternatives.** Refuse every task in the overlap, whether or not it
overrides the field.

**Trade-off.** A conflicting pair of policies can sit unnoticed until the
first task that relies on the default.

**Why (simplicity).** It mirrors how two `pools.default` values behave today
(only a task without a pool is unadmittable), and the error appears exactly
where it can be fixed, with a hint to set the field on the task.

## 22. `min ≤ count` is checked only for a jobspec `min`; `count ≤ max` always

*Implementation, engine core.*

**Call.** Validation rejects a task's own `min` above its `count`, but an
inherited `min` (a multiple of at least `"1x"`) may resolve above `count`,
and then raises the floor. `count ≤ max` is checked for every level.

**Alternatives.** Apply `min ≤ count` at every level.

**Trade-off.** None found. The rule is asymmetric, which the public doc must
state.

**Why (correctness).** Entry 3 promises that defaults can *raise* floors
(`min = "2x"` cluster-wide). Checking `min ≤ count` for inherited values would
make that impossible.

## 23. Schedules use 5-field crontab with Legion's own time-zone and DST handling

*Implementation, engine core (`src/autoscaler/schedule.rs`).*

**Call.** `schedule[].cron` is standard 5-field crontab (`0 8 * * 1-5` means
Monday to Friday). The engine translates it for the `cron` crate, which is
seconds-first and numbers Sunday as 1, so the plan's `1-5` would otherwise
have meant Sunday to Thursday. The engine evaluates windows in the
schedule's IANA `timezone` (`chrono-tz`) with its own DST rules: a start that
falls in a spring-forward gap uses the pre-gap offset, and a repeated
fall-back hour opens the window once.

**Alternatives.** Use the crate's syntax directly, the same as batch
`schedule` does.

**Trade-off.** Two cron dialects in one jobspec: a batch task's `schedule`
is seconds-first; an autoscale `schedule` is crontab. There is also a new
dependency (`chrono-tz`, 0.10.4, with `phf` 0.12.1).

**Why (simplicity).** Operators write crontab from memory, and a schedule
that silently ran on Sundays instead of Fridays is the worst kind of
surprise.

## 24. Warmup counts not-ready replicas at 0% for scale-out and at the target for scale-in

*Implementation, engine core (`src/autoscaler/strategies.rs`).*

**Call.** Not-ready replicas (`current − ready`) count as 0% busy when the
strategy is deciding to scale out, and as exactly at target when it is
deciding to scale in. That is the Kubernetes HPA rule.

**Alternatives.** Count them as fully busy (utilization 1.0) for scale-in.

**Trade-off.** Counting at target makes a warming fleet slightly more
willing to scale in than counting at 100% would.

**Why.** The plan names the HPA rule, and at-target avoids treating a
warming replica as an overload signal.

## 25. The step budget is a percent of the count at the window's start, with no refunds

*Implementation, engine core (`src/autoscaler/pipeline.rs`).*

**Call.** The step limit's percent half is taken of the count at the start
of the rolling `every` window, rounded up for scale-out and down for
scale-in. A change in the opposite direction does not give budget back.

**Alternatives.** Percent of the current count, recomputed every evaluation.

**Trade-off.** Slightly slower than the plan's prose suggests: in S1 the
first scale-in lands at 9:50, not 10:00.

**Why (safety).** A base that moves with the count lets a run of small
evaluations compound past the intended rate. A fixed base per window makes
"at most 10% per minute" literally true.

## 26. Pause beats pin, and a schedule collision keeps the ceiling

*Implementation, engine core.*

**Call.** When a pause and a pin are both active, the pause wins and nothing
is written. Schedules only ever raise `min` or lower `max`. When two
overlapping schedule entries make `min > max`, the ceiling wins.

**Alternatives.** Pin beats pause (the human's explicit count wins). Or the
floor wins on a collision.

**Trade-off.** An operator who pins during a cluster-wide pause sees the pin
held until the pause ends.

**Why (safety).** A pause is the kill switch, and a kill switch that a pin
can override isn't one. Keeping the ceiling on a collision errs toward
spending less.

## 27. One `scale.limited` event per episode

*Implementation, engine core.*

**Call.** `scale.limited` is emitted once per episode, keyed by the stage
that limited the count. A tick held for warmup or stabilization neither ends
nor restarts the episode.

**Alternatives.** Emit on every limited evaluation, or rate-limit by time.

**Trade-off.** A long episode is one event, so its duration has to come from
status, not the event stream.

**Why (simplicity).** The event stream is capped and best-effort. One event
per state change is the rule the plan sets (§6.6).

## 28. Scale to zero follows KEDA: only activation checks vote at zero

*Implementation, engine core.*

**Call.**
- At zero replicas, only checks with `activation` vote.
- A target is idle (scaled to zero) only when every check reads below
  activation.
- Missing data is never idle.

**Alternatives.** Let normal checks vote at zero, with their own thresholds.

**Trade-off.** A policy must name an activation check to ever wake. That is
already enforced by the `min = 0` validation (plan §6.12).

**Why (correctness).** It is the 0↔1 versus 1↔N split the plan takes from
KEDA. Treating missing data as idle would put a service to sleep because its
metrics source had a blip.

## 29. Rollups ride their own `ReportNodeLiveness` call every 10 seconds, not the 1-second digest

*Implementation, Phase 1 (`src/manager/scaling_inputs.rs`).*

**Call.** Each non-leader manager sends its rollups to the status-lane leader
every 10 seconds on a separate `ReportNodeLiveness` call that carries no node
ids. It does not ride every tenth 1-second liveness digest. The leader skips
liveness bookkeeping for a digest with no node ids. The rollups are wrapped
in a `RollupSync { epoch, seq, base_seq, full }` rather than the plan's bare
`repeated TargetRollup`, so the leader can apply deltas and detect gaps.

**Alternatives.** Piggyback the rollups on the existing digest, as the plan
sketched (§6.7).

**Trade-off.** One more manager-plane RPC per manager every 10 seconds, and a
special case in the digest handler.

**Why (performance and simplicity).** The liveness digest is on the path
that decides whether nodes are alive. Keeping rollups off it means a large
rollup payload can never delay a liveness report.

## 30. Rollup deltas compare content, and unchanged rollups are re-confirmed every 60 seconds

*Implementation, Phase 1.*

**Call.**
- A rollup is re-sent only when its content changes; the sample time alone
  doesn't count. Unchanged rollups are re-sent every 60 seconds.
- Any sync, even an empty one, confirms all of that manager's rollups.
- The leader drops a manager's rollups after 35 seconds without a sync.

**Alternatives.** Re-send every rollup on every tick.

**Trade-off.**
- A node that moves back to a manager it reported through before can show
  stale numbers for up to 60 seconds.
- `age_seconds` in `GetScalingInputs` means time since confirmation, not the
  sample's own age.

**Why (performance).** The steady-state payload shrinks to almost nothing.
The debug-profile benchmark measured a 54ms steady delta against 136ms for a
full send at 33,330 replicas.

## 31. `resyncing` is a 10-second window after takeover, not per-peer tracking

*Implementation, Phase 1.*

**Call.** After a manager takes the status lane, `GetScalingInputs` reports
`freshness.state = resyncing` for 10 seconds. It doesn't track which peers
have completed their full resync. A single-manager cluster never shows it.

**Alternatives.** Track each peer's resync, and report `resyncing` until all
of them have arrived.

**Trade-off.** A slow peer can leave a target's numbers incomplete for a moment
after the window closes. The missing-data hold in the pipeline still guards
against acting on that.

**Why (simplicity).** A fixed window is easy to explain, and peers send their
full set at once (entry 32), so the window is almost always longer than
needed.

## 32. A resync request is answered in the same tick

*Implementation, Phase 1.*

**Call.** When a new leader asks a peer for a full resync, the peer sends its
full set in the same tick instead of waiting for its next 10-second send.

**Alternatives.** Send the full set on the next regular tick.

**Trade-off.** A brief burst of full payloads right after an election.

**Why (correctness).** It shrinks the window where targets read as missing
data from about 10 seconds to about zero.

## 33. Request rates are attributed through the caller's declared upstream

*Implementation, Phase 1.*

**Call.**
- A service's request, error, and latency figures come from edge samples,
  attributed through the caller's declared upstream key.
- An undeclared key is treated as a service in the caller's own namespace,
  the same rule `ListServiceEdges` uses. External hosts are dropped.
- A service backed by several tasks reports its full totals on each of them.

**Alternatives.** Attribute by resolving each edge to the specific allocation
it reached.

**Trade-off.**
- The figures cover internal callers only, which the public doc states.
- A task that shares a service with another task sees the combined traffic.

**Why (simplicity).** Edges are recorded on the caller's side, by the name it
asked for, so that is the only attribution the data supports today.

## 34. A job outside the caller's grant fails the whole `GetScalingInputs` batch

*Implementation, Phase 1.*

**Call.** If a request names a job the caller may not read, the whole batch
fails with `PERMISSION_DENIED`. An unknown job or task gets a per-entry error
instead.

**Alternatives.** Return per-entry `PERMISSION_DENIED` and answer the rest.

**Trade-off.** A caller has to split mixed-permission batches itself.

**Why (security).** Per-entry answers would let a caller without access tell
an existing job from a missing one (AGENTS.md: check before you fetch).

## 35. `unplaced` counts across versions, and is 0 for keyed, `count.per`, batch, and cron tasks

*Implementation, Phase 1.*

**Call.** `unplaced` is the effective count minus live placements across all
config versions. It is always 0 for keyed tasks, `count.per` tasks, batch and
cron tasks, which have no autoscalable count. A stopped or failed job has a
desired count of 0.

**Alternatives.** Count only placements of the latest version.

**Trade-off.** Mid-rollout, old-version allocations count as placed, so
`unplaced` can read low while a rollout is still placing new ones.

**Why (correctness).** Capacity is what the capacity guard and node
autoscaling care about, and old-version allocations really do occupy it.

## 36. `job info -j` adds a `scalingInputs` key rather than reshaping the job

*Implementation, Phase 1.*

**Call.** `legion job info -j` gains a `scalingInputs` key on the `Job`
object. The human output adds a request-vs-usage block.

**Alternatives.** A separate `legion job usage` command, as an earlier plan
draft had.

**Trade-off.** `job info` does one more RPC.

**Why (simplicity).** One place to look, and the change is additive to an
output that agents already parse.

## 37. Workload identity is on by default, so the autoscaler holds credentials on a fresh cluster

*Implementation, Phase 2 (`WorkloadIdentityConfigFull::default`; closes apps
calls entry 56's deferral).*

**Call.** `workloadIdentity.enabled` now defaults to `true`. A fresh cluster
runs the in-process issuer: its first signing key is generated and
replicated at first boot, the `legion` control-plane auth method is
registered, the discovery and JWKS routes are served, and every task that
declares the `legion` identity (every app) gets a token and an API socket.
`enabled = false` remains the opt-out.

```nickel
# manager.ncl — nothing to set; to opt out:
workloadIdentity = { enabled = false }
```

**Alternatives.**
- (b) Split the flag: the reserved `legion` identity (Legion talking to
  itself) always on, while identities with an outside `audience`, and the
  public discovery and JWKS routes, stay behind `enabled`.
- Leave it off, and have the apps reconciler turn it on per app (impossible:
  it is process-wide state built at startup).

**Trade-off.**
- Every new manager serves two unauthenticated JSON routes (public keys
  only) and runs a key-rotation loop (one Control write a day).
- A job can now declare an identity for an outside audience on a fresh
  cluster and get a token whose `iss` is the cluster-internal default
  (`https://manager.legion`, or a single manager's advertised address),
  which a cloud provider will not trust. Federation still needs an explicit
  public `issuer`, as before.
- Direct exec, edge boot tickets, and material identity work out of the box
  too, which they did not before.

**Why (simplicity).** Enabling needs no operator input: the issuer URL has a
correct in-cluster default, the key is generated and replicated by the
Control leader, and the JWKS only publishes public keys. (b) would give one
flag two meanings and two code paths for one issuer, to protect a surface
that is already harmless. AGENTS.md asks for the default that is right for a
new cluster, and the plan's promise is that `autoscale = true` works on one.

## 38. Phase 2 runs everything the engine implements on the `legion` source

*Implementation, Phase 2 (`crate::manager::autoscale::validate_static`;
deviates from the plan's phase split, §8).*

**Call.** A Phase 2 jobspec may use every field the engine core runs:
`checks` (on the `legion` source, with its fixed query vocabulary), all five
strategies, `schedule`, `headroom`, `scaleIn = false`, and `onMissing`.
Refused at submit: any other source, a `legion` query outside the
vocabulary, a `service.*` query on a task without a `service` block,
`min = 0`, and `activation`.

```
error: web/default/web: autoscale.checks[0] (lag): unknown source 'team-prom'. Hint:
  the only source in this version is `legion`, which reads task.cpu.utilization, ...
```

**Alternatives.** Accept only the Phase 2 surface the plan names (`cpu`,
`memory`, bounds, `up`/`down`, `warmup`, `tolerance`, `window`, `mode`), and
refuse `checks`, `schedule`, `headroom`, `scaleIn`, and `onMissing` until
Phase 3.

**Trade-off.** More surface ships before its Phase 3 acceptance tests (S2's
business-hours floor, S3); those fields rest on the engine's unit tests and
the app's mapping of the `legion` vocabulary.

**Why (simplicity).** The engine already evaluates them, and the only new
code is the vocabulary mapping. Refusing them would have added validation
code to remove later and an error users would hit for no reason. Phase 3
reduces to the external sources, scale to zero, and `scaling simulate`.

## 39. Window averages are kept by the app, by its own clock

*Implementation, Phase 2 (`src/apps/autoscaler.rs`, `TargetState::windowed`).*

**Call.** A check's `window` (default `1m`) is the mean of the readings the
app took inside it, timestamped by the app's clock. `GetScalingInputs` is
read with `history_seconds = 0`.

**Alternatives.** Average the manager's 10-second rollup history for the
window (`history_seconds = window`), which the inputs RPC already serves.

**Trade-off.** At a 15s interval a 1m window holds four readings rather than
six, and an app restart starts the window empty (the cold-start hold covers
scale-in).

**Why (correctness).** The history is stamped by the status-lane leader's
clock and restarts empty on a takeover; the stabilization and step windows
live in the app (plan §6.9). One clock for every window keeps them
consistent, and makes the engine's decisions reproducible from the
readings it saw (the simulator and the tests drive that clock).

## 40. The app-plane gate recognizes the autoscaler by its grant reference, and admits management tokens

*Implementation, Phase 2 (`ManagerDaemonService::is_autoscaler_identity`).*

**Call.** `ReportScalingStatus` and `RecordScalingAction` accept a caller
whose policies include a job-grant reference (`grant:alloc/<id>`) to an
allocation of `legion-system/autoscaler`, which both a bearer `legion` token
and an ACL token minted from one with `legion login --method legion`
carry; or a management token. `ScaleTask`'s `source = "autoscaler"` is
honored on the same test, and from anyone else the write is recorded as
`operator` rather than refused.

**Alternatives.**
- Match the principal `legion:<sub>` against the subject the cluster's
  `subTemplate` would render for the app.
- Refuse management tokens too, and refuse a forged `source` outright.

**Trade-off.** An operator with a management token can post status as the
app (they can also disable it). A person who claims `source = autoscaler`
gets an `operator` write, which on an autoscaled task is then refused for
lacking `--for`, rather than a dedicated error.

**Why (security, simplicity).** The grant reference is signed by the
manager and names the allocation, so it cannot be forged and survives a
custom `subTemplate`. Admitting management mirrors the worker plane and
keeps the ACL test matrix uniform.

## 41. Versions are a Control-lane counter, not the raft log index

*Implementation, Phase 2 (`scaling_counter`, `Store::next_scaling_counter_tx`;
deviates from plan §6.9 and §7).*

**Call.** `task_scale.version` is the next value of the `task_scale` counter
in the `scaling_counter` table, taken in the same transaction as the write.

**Alternatives.** The raft log index of the row's last write, as the plan
says.

**Trade-off.** One extra row update per write.

**Why (correctness).** A replicated op's body runs on the leader before its
entry is appended, so the index is not known while the row is written, and a
single local-disk manager has no log at all. A counter in the same
transaction keeps the property the plan wanted: versions never repeat across
a delete and recreate, on every persistence backend.

## 42. A pin is a floor and a ceiling on the override row, applied when it is written

*Implementation, Phase 2 (`task_scale.pin_until_ms/pin_min/pin_max`).*

**Call.** `legion job scale web --count 12 --for 2h` writes the count 12 and
pins floor and ceiling at 12; `--min`/`--max` pin one side and move the
count into range at once. `effective_task_count` is unchanged: the count is
already where the pin wants it. The server refuses autoscaler writes to a
count pin and outside a pinned range; when the pin expires the count stays
and the autoscaler takes over at its normal rate.

**Alternatives.** Store the pinned count separately and have
`effective_task_count` (and the SQL that reads `task_scale.count` directly)
prefer it while the pin lasts.

**Trade-off.** An expired pin leaves the count where the pin put it, rather
than snapping back to what the autoscaler last chose.

**Why (simplicity, safety).** The scheduler's reads, quota, and slot
retirement stay untouched, and "a forgotten pin comes down gently" (plan
§6.5) is exactly the behavior the scale-in rate gives.

## 43. A dry-run policy records each distinct "would" once, through `RecordScalingAction`

*Implementation, Phase 2.*

**Call.** `RecordScalingAction` ships with one kind, `would`: a dry-run
decision's history row (`applied = false`). The app records a would-change
only when its `from -> to` differs from the last one it recorded for the
target.

**Alternatives.** Write a row on every evaluation that would act; or carry
would-rows on `ReportScalingStatus`.

**Trade-off.** A dry-run policy that keeps wanting the same change shows
one row, not one per tick, so a week of "would" history reads as the
changes the policy would have made, not how long it wanted them.

**Why (simplicity).** A dry-run count never moves, so per-tick rows would
fill the 200-row cap in under an hour. The RPC the plan reserves for action
intents (§6.9) is the natural home for an action that is not a count write.

## 44. The autoscaler's grant is exactly what Phase 2 calls: `scale:read` and `scale:write`

*Implementation, Phase 2 (`src/apps/bundled/autoscaler.ncl`; defers calls
entry 18 until node groups ship).*

**Call.** The bundled manifest asks for `scale` read and write in every
namespace and nothing else. The app-plane RPCs need no rule.

**Alternatives.** Keep the plan's full grant (`job:read`, `node:read/write`,
`kv` on `legion/apps/autoscaler/*`) from day one, as entry 18 argues.

**Trade-off.** Phases 5 and 7 change the manifest's permissions, which a
bundled app's upgrade grants through `legion:bootstrap` (apps calls).

**Why (security).** Entry 18 is about not splitting the grant by *feature
use* on one build; a build that never calls node or KV APIs has no reason
to hold them.

## 45. Status lives on the status-lane leader; NOT RUNNING after three of the app's intervals

*Implementation, Phase 2 (`crate::manager::scaling_status`).*

**Call.** `ReportScalingStatus` and `GetScalingStatus` are answered by the
status-lane leader (others forward), which keeps the newest report per
target in memory. The app counts as running while a report arrived within
three of the intervals it reports; a manager that has held the board for
less than three default intervals with no report says `starting`.

**Alternatives.** Replicate the app's heartbeat (a Control write per tick),
or let any manager hold what it received.

**Trade-off.** A status-lane takeover shows `starting` and then
`not-evaluated` targets until the app's next report.

**Why (performance).** A tick's report is the app's heartbeat; replicating
it would put a raft write on every interval for state that is stale an
interval later, which is the rule usage rollups already follow.

## 46. `WatchScalingPolicies` sends the full set on change, checking a fingerprint each second

*Implementation, Phase 2.*

**Call.** The stream checks a cheap fingerprint (the autoscaled jobs' latest
config ids, the defaults' version, the namespace policies' count and newest
update) once a second; on a change it resolves every policy and sends the
whole set if it differs from the last one sent.

**Alternatives.** Deltas; event-driven recompute from the store's event
bus; shard filters (plan §6.9).

**Trade-off.** A full set per change, and up to a second of delay. At
thousands of policies a change re-sends all of them.

**Why (simplicity).** One replacement message is trivially correct for one
replica; deltas and shard filters arrive with sharding (Phase 7), which is
where their cost pays.

## 47. An in-place resubmit needs no rollout in flight and every allocation on the previous version

*Implementation, Phase 2 (`update_autoscale_in_place`).*

**Call.** The in-place path (§6.3) runs only when the job has no active
rollout and every allocation belongs to the previous config; otherwise the
autoscale-only change rolls out as before.

**Alternatives.** Migrate the previous version's allocations even mid-rollout.

**Trade-off.** Editing `autoscale` during a rollout restarts the rollout.

**Why (correctness).** `migrate_allocations_to_job_config` moves one
version's allocations; mid-rollout the older version's would be stranded
under a rollout whose target version was just superseded.

## 48. The quota stage is the write's admission check

*Implementation, Phase 2.*

**Call.** The app passes no quota headroom to the engine. A scale-out the
namespace quota cannot admit is refused by `ScaleTask` (its Phase 0
admission check), and the target reports `held` with the quota's message.

**Alternatives.** Compute each target's quota headroom per tick and feed the
engine's quota stage, so it scales to exactly what fits.

**Trade-off.** Near the quota a target holds instead of filling the last
few replicas, and the state reads `held` rather than `limited`.

**Why (simplicity).** Headroom per band needs the namespace's whole charge,
which `GetScalingInputs` does not carry; the refusal is already correct and
explained. Listed as a follow-up.

## 49. A pause without `--for` holds until resumed, and pauses have three scopes

*Implementation, Phase 2.*

**Call.** `legion scaling pause` takes a task, a job (every autoscaled task
in it), or `--all`. `--for` is optional; without it the pause holds until
`legion scaling resume` at the same scope.

**Alternatives.** Require `--for`, as pins do.

**Trade-off.** A forgotten pause holds forever (but shows on every status
surface).

**Why (simplicity).** A pause is the kill switch: an operator in an incident
should not have to guess a duration. Pins require `--for` because a pin
changes the count, and a pause does not.

## 50. Warmup comes from rises in the ready count

*Implementation, Phase 2.*

**Call.** The app counts a replica as warming for `warmup` after the task's
ready count rose, newest rises dropping first when replicas leave.
Utilization is still the mean over every replica that reports usage.

**Alternatives.** Per-replica ready times in `GetScalingInputs`.

**Trade-off.** A replica that turns healthy at the same moment another
leaves is not seen as warming, and a warming replica's usage still enters
the mean.

**Why (simplicity).** The inputs carry counts, not per-replica times, and
the warmup rule only needs to damp a fresh scale-out.

## 51. Join attributes: the token beats `--attr`, which beats the worker type; hosts may set only `legion.provider-id` under `legion.*`

*Implementation, Phase 5a.*

**Call.** A node's attributes merge the join token's `attributes` over the
host's `--attr`, over its worker type's. Keys under `legion.` are reserved:
a host may set only the per-host `legion.provider-id`; a token may set
`legion.nodegroup` and other `legion.*` keys; nobody may set
`legion.role`, `legion.join`, or keys Legion reads as node structure
(`namespaces`, `resources`, `capacity`, ...). A bad `--attr` fails the
join with InvalidArgument.

```console
$ legion join-token create --max-uses 5 --attr legion.nodegroup=workers-general
$ legion join --token-file token --attr legion.provider-id=i-0abc --attr rack=r7
```

`--attr legion.nodegroup=…` on the host is refused with InvalidArgument.

**Alternatives.** Let the host's `--attr` win over the token; a fixed list
of reserved keys instead of a reserved prefix.

**Trade-off.** Worker operators can't override a token's attributes, and a
new per-host `legion.*` key needs a code change to allow.

**Why (security).** A node-group token has to vouch for
`legion.nodegroup`, and node autoscaling decides what to drain and
terminate by it. A booting host must not be able to join a group it
wasn't minted for.

## 52. `max_uses` counts distinct new nodes, consumed atomically as the join's last step

*Implementation, Phase 5a.*

**Call.** A use is spent only when a token introduces a node that didn't
exist: re-registering the same node (matched through its recorded
`legion.join.token`) is free, and a forgotten node coming back counts as
new. The counter is consumed with a conditional
`UPDATE … WHERE uses < max_uses` immediately before the node row is
inserted; if it can't be consumed, the join is refused.

```console
$ legion join-token list
ID        USES  ATTRIBUTES
jt-7f3a   3/5   legion.nodegroup=workers-general
```

**Alternatives.** Count every registration; consume the use when the token
is first presented.

**Trade-off.** Two brand-new hosts racing for the last use under the same
name can see one refused although both started.

**Why (simplicity).** Worker restarts must not burn a group's budget, and
failing closed at the last use is the safe direction for a token that
creates capacity someone pays for.

## 53. ForgetNode refuses a node that holds anything; `--force` takes the lost-node path

*Implementation, Phase 5a.*

**Call.** `legion node forget` refuses a node with any allocation
(including held stateful ones), one that is live and schedulable, or one
holding node-local volumes. A cordoned, empty node can be forgotten even
while it still heartbeats, so an autoscaler can forget an instance it just
terminated. `--force` skips the guard and removes the node the way a lost
node is handled: allocations become `NodeLost`, gang members lost,
snapshots lost, then the node row and its worker token go in one Control
write.

**Alternatives.** Forget only dead nodes; a soft delete that hides the
record.

**Trade-off.** `--force` on a live node strands its running processes until
the worker notices its token is revoked.

**Why (safety).** The default can't lose work; the forced path reuses the
one tested lost-node path rather than inventing a second.

## 54. Pending work is only what the scheduler already failed to place

*Implementation, Phase 5a.*

**Call.** `GetCapacityDemand` counts unplaced replicas of active jobs'
latest configs (across versions) and keyed instances with no allocation.
It leaves out jobs whose eval is still pending or in flight (listed as
`awaiting_scheduler`), `count.per`, cron, and finished batch tasks, and it
does not subtract free room on existing nodes.

**Alternatives.** Simulate the whole cluster: pending work against existing
nodes first, then templates.

**Trade-off.** A rollout waiting for surge room shows no demand, and the
answer can over-ask by a node because existing free room isn't modelled.

**Why (simplicity).** Work the scheduler hasn't tried yet may fit on what
exists; counting it would provision nodes the next eval doesn't need.
Over-asking by one node is bounded by `step` and corrected by scale-in.

## 55. The synthetic node is the worker type plus the shape, through the existing resource model

*Implementation, Phase 5a.*

**Call.** Each template becomes a synthetic node built from its worker type
(pool, namespaces, attributes, resources), then the shape applied through
the existing `capacity` ceiling attribute, plus `legion.nodegroup`.
`like_node` copies a real member instead; a shape can only lower its
limits. Feasibility runs through `ResourcePool::from_node` and the
scheduler's own checks; two scheduler functions (`task_static_ports`,
`node_provides_volume_drivers`) became `pub(crate)` for it.

**Alternatives.** A separate resource model for templates; duplicating the
two functions.

**Trade-off.** A shape can't describe a node larger than its worker type
allows, only smaller.

**Why (correctness).** The demand answer can't disagree with the scheduler
when it uses the scheduler's code. `scheduler_bench` before/after
(`candidate_construction`, ms, indexes off/on): 1000 4.12→4.02 / 3.54→3.76;
5000 17.61→17.60 / 8.94→8.97; 10000 36.50→35.77 / 13.07→12.82 — noise.

## 56. Unreadable pending entries are redacted, not dropped

*Implementation, Phase 5a.*

**Call.** `GetCapacityDemand` needs `node:read` (per template pool). A
pending or unfittable entry for a job the caller can't read keeps its
counts and failing rule, with empty names and `redacted = true`.

**Alternatives.** Drop entries the caller can't read; require `job:read`
on every job.

**Trade-off.** A `node:read` caller learns that some unreadable work exists
and how big it is.

**Why (correctness).** Node totals must cover all pending work, or a
narrowly scoped autoscaler would under-provision.

## 57. Templates are answered independently, and a task group packs as one unit

*Implementation, Phase 5a.*

**Call.** Each template is answered as "if only this group grew"; only
`upcoming` nodes absorb pending work across all templates first. The
engine's expander then picks one group per tick (least-waste, the only
expander built). A task group's tasks pack as one unit sized by their sum;
a gang packs atomically.

**Alternatives.** A joint optimisation across templates.

**Trade-off.** Mixed shapes can need one more tick to converge than an
optimiser would.

**Why (simplicity).** Predictable answers that match the Cluster
Autoscaler's expander model, and how the scheduler places groups.

## 58. Scale-in never lowers the provider's desired count; it terminates chosen nodes

*Implementation, Phase 5a.*

**Call.** The node-group engine scales in only by draining and terminating
specific victims; it never sets a smaller desired count and lets the
provider pick. Above `max` it holds. Scale-in holds while the group is
growing, has demand, or has a scale-up pending; pause aborts drains still
in the draining stage (uncordoning them). A 60s cordon grace and a 2m
terminate retry are built in.

**Alternatives.** Lower the desired count and let the provider choose.

**Trade-off.** Every provider plugin must support terminating a named
instance.

**Why (safety).** Only Legion knows which nodes are empty and safe to
remove; a provider choosing would kill a busy node.

## 59. `legion-system` allocations protect a node, except per-node system ones

*Implementation, Phase 5a.*

**Call.** A node running a `legion-system` allocation is never a scale-in
victim, unless that allocation is a per-node (`count.per`) system task,
which every node has.

**Alternatives.** Protect every node with any system allocation.

**Trade-off.** A per-node system task is stopped when its node goes.

**Why (correctness).** Otherwise a per-node system app would pin every node
and scale-in could never happen.

## 60. Node-group engine defaults

*Implementation, Phase 5a.*

**Call.** `max` is required and `min` defaults to 0. Up: step
`{ percent = 100, replicas = 5 }` every 2m, `joinTimeout = "10m"`. Down:
`when = "empty"` for 10m, step `{ percent = 10, replicas = 1 }` every 5m,
`drainDeadline = "10m"`; `underutilized` means under 50% reserved. A
`mode` field gives node groups a dry run.

**Alternatives.** No default `max` (unbounded growth); scale-in on by
`underutilized`.

**Trade-off.** Groups grow by at most 5 nodes per 2 minutes until tuned.

**Why (safety).** A node group spends money; the defaults bound the spend
and remove only nodes that are provably empty.

## 61. Capacity-demand simulation is capped at 20,000 replicas

*Implementation, Phase 5a.*

**Call.** The simulation packs at most 20,000 pending replicas and sets
`truncated` past that; packing keeps a start cursor per template so
first-fit stays near-linear.

**Alternatives.** No cap; paging.

**Trade-off.** A larger backlog is under-counted until it drains below the
cap; `truncated` says so.

**Why (performance).** The RPC runs on whichever manager answers, and must
stay cheap enough to call every tick.

## 62. Tokens are enforced even with ACLs off, and provider ids are not yet unique

*Implementation, Phase 5a.*

**Call.** `max_uses` and token attributes apply whenever a presented token
resolves, ACLs on or off. Uniqueness of `legion.provider-id` across nodes is
not enforced; Phase 5b cross-checks it against the provider's status.

**Alternatives.** Skip token limits with ACLs off; refuse a duplicate
provider id at join.

**Trade-off.** Two nodes can claim one instance id until Phase 5b.

**Why (simplicity).** Node groups must not depend on the ACL mode, and only
the provider knows which instance ids are real.

## 63. The manager checks that a plugin is installed and open; only the autoscaler checks its kind and schema

*Implementation, Phase 4 (`autoscaler::plugins::PluginCatalog::check_use`,
`manager::autoscale::check_plugin_use`, `apps::autoscaler::host::PluginHost::check`).*

**Call.** Submit (`job run`, `job plan`) and `scaling apply` refuse a policy
that names a plugin the autoscaler's app config does not install, or one
whose `namespaces` allowlist does not open it to the policy's namespace.
Whether the plugin implements the kind the policy uses, and whether the
policy's config matches its `policyConfigSchema`, is known only from the
plugin's `initialize`, so the autoscaler checks those at evaluation and
reports the policy `invalid`. `scaling apply --wait` waits for that verdict.

```
$ legion scaling apply render-farm.ncl --wait
scaling policy media/render-farm: created (v1)
Error: the autoscaler found scaling policy media/render-farm invalid: the config
  for plugin render does not match its policyConfigSchema: "pool" is a required property
```

**Alternatives.**
- (a) The manager starts every plugin itself to read its handshake, and
  checks everything at submit.
- (b) Check nothing at submit; let the autoscaler report every problem.

**Trade-off.** A policy can be stored and still be invalid; the error
arrives one evaluation later (`--wait`, `scaling explain`, a
`scale.check_failed` event), not as the submit's error.

**Why (security boundary, simplicity).** (a) would run operator-installed
code inside the manager, the thing entry 4 keeps plugins out of, and it
would need the artifacts the app's job fetches. (b) would let a tenant name
a plugin closed to their namespace and learn about it only from status.
The install and allowlist checks need only the stored app config, so they
stay synchronous where the security boundary is.

## 64. A standalone policy is scoped like a job of the same name in its namespace

*Implementation, Phase 4 (`scaling_policy_rpcs.rs`).*

**Call.** `ApplyScalingPolicy`, `DeleteScalingPolicy`, and the policy forms
of `PauseScaling`, `GetScalingStatus`, and `GetScaleHistory` check `scale`
on `AclScope::job("<namespace>/<name>")`. A node-group policy needs
`operator:write` and `node:write` instead, and no `scale` grant.

```json
[{ "resources": ["scale"], "namespaces": ["media"], "names": ["web*"], "actions": ["write"] }]
```

covers the job `media/web` and the policy `media/web-batch` alike.

**Alternatives.**
- (a) A new ACL resource, `scaling-policy`.
- (b) A new scope attribute (`policy`) on the `scale` resource.

**Trade-off.** A `names` rule cannot tell a job from a policy of the same
name: granting `scale` on `render` grants both.

**Why (simplicity).** The `scale` resource already means "resize rights
without redeploy rights", which is what applying a policy is. Reusing the
job scope keeps the ACL vocabulary and every existing grant meaningful;
names collide only when an operator chooses colliding names.

## 65. Simulation replays the counts the manager recorded, and the live engine spends its step budget on changes it did not make

*Implementation, Phase 3 (`manager::scaling_inputs::autoscaled_replica_counts`,
`autoscaler::simulate`, `autoscaler::signals`; acceptance test
`simulate_over_history_matches_the_live_decisions_test`).*

**Call.**
- Each 10-second history point of an autoscaled task carries its desired,
  healthy, and unplaced counts, read on the status-lane leader once per
  rollup tick.
- `scaling simulate JOB` defaults to **replay**: the count is the recorded
  one, readiness comes from the recorded healthy count through the warmup
  rule, and the policy's own decisions spend the step budget when made.
- The live engine and the replay treat every other count change (a
  person's scale, a pin) the same way: it spends the step budget when it is
  first observed. The readiness and window code is one module both use.

```console
$ legion scaling simulate web
web/default/web: replay 240 evaluation(s) from 13:02 UTC to 14:02 UTC (history), ...
```

**Alternatives.**
- (a) Simulate from the usage history alone, with the policy's own count
  (follow) as the only mode.
- (b) Reconstruct the counts from `scale_history` rows.

**Trade-off.** One store read per autoscaled job per 10 seconds on the
leader, and three more numbers per history point. The live engine now
counts a person's scale-out against its own step budget for that window.

**Why (correctness).** The acceptance criterion is that the live policy,
replayed, makes the decisions the autoscaler made. That is only possible
when the simulator sees what the engine saw. (a) diverges as soon as the
real count differs from the policy's (a pin, a rollout); (b) misses changes
that never wrote history (an operator's resubmit). Counting others' changes
against the budget matches what `PolicyState` already documented, and is
what lets replay agree with live.

## 66. A target's change is recorded before it is made

*Implementation, Phase 4 (`apps::autoscaler::Engine::evaluate_standalone`,
`RecordScalingAction` kinds `scale` and `failed`).*

**Call.** For a standalone policy the autoscaler first calls
`RecordScalingAction { kind = "scale", policy }`, which the manager refuses
while a pause covers the policy, then calls the plugin's `target/scale`. A
failed call adds a `failed` history row and reports `error`.

**Alternatives.** Call the plugin, then record what happened.

**Trade-off.** A change can be recorded and not made (the call fails after
the row is written; the `failed` row follows it). Plugins must make
`target/scale` idempotent, which the protocol already requires.

**Why (security, correctness).** It puts the pause on the server for
targets Legion does not own, as `ScaleTask` does for tasks: a misbehaving
or stale autoscaler cannot scale a paused target. And the durable record
never misses a change that happened.

## 67. One request at a time per plugin program, and an error reply is a healthy plugin

*Implementation, Phase 4 (`apps::autoscaler::rpc::Transport`).*

**Call.**
- A program plugin gets one request at a time; the next waits for the
  answer.
- A call that times out (10 seconds; 120 for targets) kills the process
  group, and the next call restarts it: at once the first time, then after
  1, 2, 4, … up to 60 seconds while failures continue.
- A JSON-RPC error answer is not a failure: the plugin is healthy, the check
  reads missing (or the target reports `error`).

**Alternatives.** Pipeline concurrent requests by `id`; restart on every
error answer.

**Trade-off.** A slow program plugin serializes every policy that uses it:
1,000 policies on one 100 ms strategy take 100 seconds a pass. (Each
distinct source query is asked once per evaluation, which bounds this for
sources.)

**Why (extensibility).** A plugin author can write a single-threaded loop
in any language and be correct; a late answer can never be read as the
reply to the next request, because the process that owed it is gone.
Concurrency is a follow-up (a per-plugin `concurrency` setting) once a real
plugin needs it.

## 68. A plugin's code fields are what it runs: `source`, `command`, `url`, and an `exec` target's `config.command`

*Implementation, Phase 4 (`src/apps/bundled/autoscaler.ncl` `codeFields`;
extends the apps plan's `source` and `url`, §6.3).*

**Call.** Changing a plugin's `source` (an artifact), `command`, `url`, or
an `exec` target's `config.command` needs the app's whole grant. A built-in
source's `config` (a Prometheus `address`, an `http` `url`), `builtin`, and
`namespaces` need only `app:write` on the autoscaler.

```text
Error: changing app autoscaler's code (/plugins/render/config/command) runs code with its grant, ...
```

**Alternatives.**
- (a) The apps plan's two fields only (`source`, `url`).
- (b) Every plugin field but `namespaces`, `config` included.

**Trade-off.** An `app:write` holder can point a built-in source at any
address the autoscaler can reach, and read the number it returns in
`scaling explain`: a narrow SSRF from the app's network position, open to
operators only (tenants' policies never carry an address, entry 5).

**Why (security boundary, simplicity).** (a) would let `app:write` run any
program with the app's grant, through `command` or an `exec` target. (b)
makes routine operator edits (a new Prometheus address) need the app's
whole grant, which no operator role holds but the bootstrap one.

## 69. `scaling apply` refuses a node-group policy until node groups run

*Implementation, Phase 4 (deviates from plan §8's Phase 4, which stores
node-group policies).*

**Call.** A policy with `target = { nodes = … }` parses and passes its ACL
check, then is refused with `failed_precondition`, naming `legion scaling
demand -f` as what sizes it today.

**Alternatives.** Store it inert, listed with a `not run` state.

**Trade-off.** An operator cannot stage node-group policies ahead of
Phase 5b.

**Why (simplicity).** A stored policy that does nothing reads as working;
the refusal says what exists instead. Phase 5b removes the refusal, and
the parser, ACL rule, and table already take the shape.

*Superseded by Phase 5b:* the refusal is gone, and `scaling apply` stores
and runs node groups (entries 85-94).

## 70. Removing a plugin from the app config is not refused; the policies that use it go `invalid`

*Implementation, Phase 4.*

**Call.** `legion app config set autoscaler` validates the plugins it
installs, but does not check the policies that name them. A policy whose
plugin is removed or closed to its namespace reads `invalid` from the next
policy push (the watch fingerprint includes the app's config).

**Alternatives.** Refuse a config change that would invalidate a stored
policy or a running job's, as `scaling defaults apply` does (`--force` to
override).

**Trade-off.** An operator can break tenants' policies in one command; the
damage shows in `scaling status` rather than at the change.

**Why (simplicity).** The app config is the generic apps surface, which
has no hook for an app to refuse its own config change on cluster state.
The failure is safe: an invalid policy holds its count. Listing affected
policies on `app config set` is a follow-up.

## 71. Standalone policies reuse the task tables, keyed `policy:<id>`

*Implementation, Phase 4 (`storage::store::scaling_policy_ops`,
`manager::autoscale::policy_target_key`).*

**Call.** The new `scaling_policy` table (Control lane) holds only the
stored spec and its version. History, pauses, and status reuse the task
tables with the target key `policy:<namespace>/<name>`, an empty group and
task. Their RPCs take a `policy` field next to `job`.

**Alternatives.** Separate history, pause, and status tables per kind.

**Trade-off.** The task tables' `job_id` column holds a non-job key for
these rows, which every reader of them must recognize.

**Why (simplicity).** History caps, pause precedence, status staleness and
events all work unchanged; `pause --all` covers policies for free.

## 72. Prometheus averages the window itself, with a range query

*Implementation, Phase 3 (`apps::autoscaler::builtins::PrometheusSource`).*

**Call.** A `prometheus` check with a `window` reads
`/api/v1/query_range` over it with a step of a twelfth of the window (at
least 1s) and averages the points; the reading is marked as already
windowed, so the engine does not average it again. A zero window reads an
instant query. An `http` source returns one number per read, which the
engine averages like a `legion` reading.

**Alternatives.** Instant queries only, averaged by the engine.

**Trade-off.** A range query is heavier on Prometheus than an instant one.

**Why (correctness).** The window is full from the first evaluation, after
an autoscaler restart too, and it is the window the user's Prometheus
dashboards show; engine-side averaging would start empty and see only one
point per 15 seconds.

## 73. Scale to zero keeps `count >= 1`, and a service does not wake on a request

*Implementation, Phase 3.*

**Call.** With `min = 0`, `count` is still the starting size and must be at
least 1; the task sleeps once its activation checks go quiet. Waking on a
first request (`wake = "on-request"`) is not built: a service at zero
refuses connections until a check wakes it.

```
error: web/default/web: count = 0 with autoscale; count is the starting size (with min = 0 the
  task sleeps once its activation checks go quiet). Hint: set count >= 1
```

**Alternatives.** Accept `count = 0`; hold requests at the gateway for a
service at zero.

**Trade-off.** A queue worker deployed before its queue fills runs one
replica until the down window passes. Request-driven services cannot scale
to zero yet.

**Why (simplicity).** A submit that starts nothing reads as broken. Holding
requests needs a gateway hook that today exists only for keyed instances;
it is deferred with the rest of `wake` (TODO).

## 74. Following a policy over history conserves the load

*Implementation, Phase 3 (`SimOptions::conserve_load`).*

**Call.** In follow mode over history, a `legion` utilization reading is
rescaled from the recorded count to the simulated count: twice the
replicas at half the utilization. Other readings (queue depth, request
rate) are used as recorded.

**Alternatives.** Use the recorded utilization unchanged.

**Trade-off.** It assumes load is shared evenly and does not change with
the replica count, which is wrong for a latency check.

**Why (correctness).** Without it, a policy that would have scaled out
keeps seeing the recorded (overloaded) utilization and scales out forever.

## 75. Any strategy name that is not built in names a strategy plugin

*Implementation, Phase 4 (`autoscaler::strategies::StrategyConfig::Plugin`).*

**Call.** `strategy = { name = "calendar", replicas = 6 }` names the
installed plugin `calendar`; the rest of the record is its config, checked
against its schema by the autoscaler. A plugin strategy with no `query`
gets `reading = null`.

**Alternatives.** A separate field: `strategy = { plugin = "calendar",
config = { … } }`.

**Trade-off.** A misspelled built-in (`per_replica`) reads as an
uninstalled plugin; the submit error names the built-ins to catch that.

**Why (simplicity).** One shape for every strategy, so swapping a built-in
for a plugin is a one-word change.

## 76. The `exec` target runs one command per operation

*Implementation, Phase 4 (`apps::autoscaler::builtins::ExecTarget`).*

**Call.** The `exec` builtin runs its command with the operation (`status`,
`scale`) appended as the last argument (a script's `$1`) and `{ apiVersion = 1, op, policy, config,
desired, decision }` on stdin, reading JSON from stdout, the volume-driver
convention (`util::run_json_command`).

**Alternatives.** A persistent process speaking the full protocol.

**Trade-off.** A process per call, fine at a target's cadence (one status
call per evaluation) and the reason sources are not offered this way.

**Why (simplicity).** A shell script is the operator's existing tool for
"start more of these"; it needs no JSON-RPC loop.

## 77. The `noop` target keeps its count in the autoscaler's memory

*Implementation, Phase 4.*

**Call.** `target = { plugin = "noop" }` records decisions and events and
holds its count in the app, starting at the policy's `min`.

**Alternatives.** Persist the count in the store.

**Trade-off.** An app restart resets a noop target to `min`.

**Why (simplicity).** `noop` exists to try a policy's signals without a
target; its count is the policy's own state, and the history keeps every
decision.

## 78. `ApplyScalingPolicy` reads its scope from the body; nothing is looked up first

*Implementation, Phase 4.*

**Call.** The handler authenticates, parses the policy, then checks the
precise scope (entry 64). `DeleteScalingPolicy` gates coarsely, then checks
the precise scope before the lookup.

**Alternatives.** A coarse `scale:write` check before parsing.

**Trade-off.** An anonymous caller with a malformed body gets the parse
error, not `Unauthenticated` (a well-formed one gets `Unauthenticated`).

**Why (correctness).** A coarse `scale:write` gate would refuse an operator
holding `operator:write` and `node:write` but no `scale` grant, the exact
principal a node-group policy is for. Nothing is fetched before the check,
so there is nothing to probe.

## 79. `--standalone NAME` names a policy on `explain`, `history`, `pause`, and `resume`

*Implementation, Phase 4 (`src/cmd/scaling.rs`).*

**Call.** The existing commands take `--standalone` to read their argument
as a standalone policy's name (`-n` for its namespace).

**Alternatives.** A `policy/` prefix on the argument; separate `scaling
policy explain|history|…` commands.

**Trade-off.** One more flag to know, and a policy and a job with the same
name need it to be told apart.

**Why (simplicity).** The commands mean the same thing for both kinds; a
flag is discoverable in `--help`, a prefix is not.

## 80. `scaling plugin test` takes its options before the command

*Implementation, Phase 4 (`src/cmd/scaling_plugin.rs`).*

**Call.** Everything from the first positional argument on is the plugin's
own command line:
`legion scaling plugin test --config '{…}' python3 strategy.py --verbose`.

**Alternatives.** Require `--` before the command.

**Trade-off.** An option written after the command goes to the plugin.

**Why (simplicity).** Plugins take flags of their own (`--http`); this
passes them through untouched without a separator to remember.

## 81. The Rust SDK is a workspace crate that does not depend on Legion; the example tests skip without their toolchains

*Implementation, Phase 4 (`examples/scaling-plugins/`).*

**Call.** `legion-scaling-plugin` is a workspace member depending only on
`serde`/`serde_json`, so `cargo test --all` tests it and builds its example.
The Python example's conformance test needs `python3`; the Go example's
needs `go` (standard library only, no network); the Rust example's needs the
example binary `cargo test --all` builds. Each skips with a message when its
toolchain or binary is absent.

**Alternatives.** Publish the SDK separately; require every toolchain in CI.

**Trade-off.** A run without Go skips the Go example silently apart from a
line on stderr.

**Why (extensibility).** A plugin should compile without Legion's build, and
CI must not need the network.

## 82. The autoscaler's UI is app views in their own module, reading the app's evaluation log

*Implementation, Phase 6.*

**Call.** The job panel, task badge, Autoscaling sidebar page and
node-group panel are views registered by `src/apps/autoscaler_ui.rs`, with
small hooks in the engine. The job panel's hour of replicas against the
signal comes from the app's in-memory evaluation log, not from
`GetScalingInputs` history.

```console
$ legion app view autoscaler job web
```

**Alternatives.** Draw the series from the manager's rollup history.

**Trade-off.** The series is empty after an app restart, and per replica
until sharding shares it.

**Why (correctness).** It shows the exact reading the policy decided on,
for any source, plugins included.

## 83. Pause, Resume and Release buttons run with the app's grant

*Implementation, Phase 6.*

**Call.** The panel offers Pause (with a duration), Resume, and Release.
The action runs as the app, with `by <viewer>` in the reason; the
`app.action` event names the viewer. Resume is an addition to the plan's
sketch.

**Alternatives.** Pause and Release only; act as the viewer.

**Trade-off.** The pause's recorded principal is the app's; the viewer is
in the reason and the event.

**Why (ease).** Without Resume, a pause made from the UI could only be
lifted from the CLI; app actions run as the app by design (apps calls
entry 68).

## 84. The sidebar page declares `scale:read`, and the grant gains `node:read`

*Implementation, Phase 6.*

**Call.** The Autoscaling page needs `scale:read` somewhere and lists only
the viewer's namespaces; the node-group panel needs `node:read` on the
node's pool, so the app's grant gains `node:read`, adopted on upgrade by
`legion:bootstrap`. The node panel shows what exists: the join-token
group, the provider id, pool members, and capacity demand for nodes like
it (cached 30 seconds); a node in no group shows nothing.

**Alternatives.** No permission on the page (every `app:read` viewer sees
every policy).

**Trade-off.** The app can read every node.

**Why (security).** Without the permission the assertion carries no
namespaces to filter by.

## 85. A node group's action intents are one compare-and-set row per policy, carrying its drains

*Implementation, Phase 5b (`scaling_action`, `Store::open_scaling_action`,
`apps::autoscaler::nodegroups`).*

**Call.** Each standalone policy has one `scaling_action` row: a version
(from the `scaling_action` counter), the latest intent, its state (`open`,
`succeeded`, `failed`, `reconciled`), and its owner. One acting evaluation
opens **one** intent covering everything it does that tick (the new
count, the victims to drain, the members to terminate, forget, or
uncordon) and the drains left in flight afterwards (`drains_json`), then
closes it with the outcome. Both writes are compare-and-set on the version.

```console
$ legion scaling history workers-general --standalone
 TIME           | TASK            | CHANGE | BY         | SUMMARY
----------------+-----------------+--------+------------+-----------------------------------------
 10-28 14:02:15 | workers-general | -      | autoscaler | forget w-i-2: its instance is gone
 10-28 14:00:15 | workers-general | -      | autoscaler | terminate w-i-2 (instance i-2)
 10-28 13:58:00 | workers-general | -      | autoscaler | drain w-i-2: scale-in: w-i-2 empty for 1m
 10-28 13:41:30 | workers-general | 0 → 3  | autoscaler | pending work needs 3 nodes (6 replicas)
```

**Alternatives.**
- (a) One row per action (a drain, a terminate), each with its own CAS.
- (b) A separate `node_group_drain` table, plus intents for the provider
  calls only.

**Trade-off.** A tick's actions win or lose together: a replica that loses
the race for one drain loses the whole tick, and acts again next tick from
the winner's state. Two writes per acting tick per group, even for a tick
that only forgets a node.

**Why (simplicity, correctness).** Everything a new owner needs to resume a
group (which nodes are draining, since when, at which step) is in one row
it can read from the policy stream; there is no second table to keep in
step with the first, and no order in which partial writes could leave a
drain recorded but its terminate not. (a) would let two replicas each win
a different victim's drain in the same overlap, which is the double action
calls entry 12 exists to prevent.

## 86. `RecordScalingAction` takes `intent` and `outcome`; external targets use them too

*Implementation, Phase 5b (reshapes Phase 4's `scale` and `failed`, calls
entry 66).*

**Call.** The action kinds are `would` (unchanged), `intent` (CAS-open an
intent: `policy`, `expected_action_version`, `intent`), and `outcome`
(CAS-close it: `succeeded`, `failed`, `reconciled`). The reply says whether
the write `won` and carries the action row either way. An external
target's scale is an intent with `desired` and nothing else; `scale` and
`failed` are gone.

**Alternatives.** Keep `scale`/`failed` for external targets and add the
intents for node groups only.

**Trade-off.** A breaking change to an app-plane RPC (no installed base;
AGENTS.md says break it). An external policy's scale now costs two writes
(intent, outcome) instead of one or two.

**Why (correctness).** Two replicas overlapping on an external target would
otherwise both call `target/scale`; the plan's no-duplicate rule is for
"node and external targets" alike. One mechanism, one set of history rows,
one place a pause is enforced (an intent that changes the count or starts a
drain is refused while paused).

## 87. A replica that loses the race restores its state and follows the winner; an open intent is reconciled after 150 seconds of its own clock

*Implementation, Phase 5b (`nodegroups::GroupRuntime::adopt`,
`RECONCILE_AFTER_MS`).*

**Call.** The engine decides on a copy of its per-group state's
predecessor: a lost CAS (or a pause refusal) puts the state back as it was
before the decision, then adopts the winner's version and drains. An intent
another replica left `open` is waited for (the group reports `held`,
"waiting for alloc-41's action d-… to finish") until this replica has
seen it open for 150 seconds by its own clock (the longest target call,
120 seconds, plus a tick); then it is closed `reconciled` with what
`target/status` shows, and the group goes on from the provider's count and
the recorded drains.

**Alternatives.** Reconcile at once; compare the intent's `opened_at` with
the local clock; check whether the owner's membership lease is live.

**Trade-off.** A replica that died mid-call delays its group by up to 150
seconds. A replica that is alive but slower than 150 seconds per call can
have its intent reconciled under it; its outcome write then loses, which is
harmless (the call was idempotent and the next owner re-reads the
provider).

**Why (correctness).** Reconciling at once would let a second replica act
on a group while the first is still inside `target/scale` with stale
status. A replica's own clock measures how long *it* has seen the intent
open, which is immune to clock skew between hosts (and to the virtual
clocks the tests run).

## 88. The provider-id cross-check protects a node instead of failing the group

*Implementation, Phase 5b (`NodeView.provider_problem`,
`Protection::ProviderId`; resolves calls entry 62's open half).*

**Call.** A member is never a scale-in victim when it has no
`legion.provider-id`, shares one with another member, or names an instance
the provider's `target/status` does not list. A provider that lists no
members (and holds a count above zero) has its members' ids taken on trust.
The rule shows in `explain` like the other protections: `never a victim:
w-7 (its provider id i-0abc is not an instance the provider lists)`.

**Alternatives.** Terminate by the id anyway; refuse registration of a
duplicate id; hold the whole group invalid.

**Trade-off.** A misconfigured member is never reclaimed by the autoscaler
(an operator forgets it by hand); a provider that cannot list instances
gets no protection from a host that lies about its id.

**Why (security).** `--attr legion.provider-id` is the one reserved
attribute a host sets for itself, so it is the one a compromised or
misconfigured host can forge; terminating by it unchecked would let one
host aim scale-in at another machine. Refusing registration would need the
provider's view inside the join path.

## 89. Members, protections, and the fits-elsewhere simulation come from `GetCapacityDemand`

*Implementation, Phase 5b (`NodeGroupTemplate.members`, `fit_check`;
`manager::node_group_members`, `capacity_demand::fit_on`).*

**Call.** The autoscaler asks one `GetCapacityDemand` per evaluation with
every group's template and `members = true`: the manager answers, per
group, each member's status, provider id, `legion.scale-in`, reserved
share, and allocations with their kind (work, session envelope with its
live sessions, per-node), node-local storage, and whether draining the node
would break the task's `disruption.minAvailable` (or a gang's quorum). With
`fit_check` (`down.when = "underutilized"`), each live member under half
reserved is packed onto the other live members' free capacity with the
demand's own rules.

**Alternatives.** The app assembles members from `GetNodes`,
`ListNodeUsage` and job configs, and simulates fits itself.

**Trade-off.** `GetCapacityDemand` grows a second job; a member's view
costs the manager a read of its allocations and their task configs per
evaluation (one query per group, one per distinct job config).

**Why (performance, correctness).** One read per evaluation, computed where
the data is, by the same packer the demand uses: the app cannot disagree
with the manager about what fits, and needs no reach into job configs. The
minAvailable floor is the one preemption already honors (the stricter of
`minAvailable` against the effective count and a gang's quorum).

## 90. The fits-elsewhere simulation does not see static ports already in use, and minAvailable is per node

*Implementation, Phase 5b.*

**Call.** `fit_on` packs a member's allocations onto the others' free
CPU, memory and custom resources with the permit, namespace, constraint,
volume-driver and `distinct` rules, but does not know which static ports
the targets' allocations already hold. The minAvailable check asks, per
node, whether the task's live allocations less this node's fall under the
floor; two victims of one task in one evaluation are not summed.

**Alternatives.** Load every target's allocations' ports; sum victims.

**Trade-off.** An underutilized node whose work needs a static port held
elsewhere can be chosen; its drain then leaves that replica pending until
the scheduler finds room (and the group's scale-in holds while it is
pending). The default scale-in budget is one node per five minutes, so a
second victim of the same task in one evaluation is rare.

**Why (simplicity).** The scheduler's own placement still enforces ports
and floors on the evicted work; the simulation only has to be right about
capacity, which is what makes a node underutilized.

## 91. A node group shares the bare-name space with `default`'s policies, and is read with `node:read`

*Implementation, Phase 5b (`ManagerDaemonService::find_policy`,
`gate_policy_ref`).*

**Call.** A node group's id is its bare name, as is an external policy's in
`default`, so `scaling apply` refuses a name the other kind holds
(`ALREADY_EXISTS`). A bare reference finds the `default` policy, then the
node group. Node groups are listed, watched, and read (status, history)
with `node:read`, and written, deleted, and paused with `operator:write`
and `node:write`. Before a lookup, the gate admits a caller who could act
on either kind; node groups are looked up only for a caller who may see
them.

**Alternatives.** A `nodes/` prefix on node-group ids; `--nodes` on every
command.

**Trade-off.** One name cannot be both; a caller with `scale:write` on
`default` and none on nodes cannot learn a node group exists (it reads
not found).

**Why (simplicity).** `legion scaling explain workers-general
--standalone` reads the way the policy was written, with nothing new to
type, and the history and pause keys (`policy:<id>`) stay as Phase 4 made
them.

## 92. One join token per scale-up, minted after the intent wins

*Implementation, Phase 5b (`Engine::mint_join_token`).*

**Call.** Each scale-up mints a token with `max_uses` = the nodes it adds,
`legion.nodegroup` and the shape's attributes stamped, a TTL of twice
`up.joinTimeout`, and the decision in its description; it is minted only
after the intent wins, and passed to `target/scale` as `join`.

**Alternatives.** One long-lived token per group, rotated.

**Trade-off.** A token row per scale-up (pruned when expired); a machine
that boots after the TTL cannot join (it has missed the join timeout
anyway).

**Why (security).** A leaked token admits at most the nodes of one
scale-up, for twenty minutes, and a lost race mints nothing.

## 93. The `aws-asg` plugin is a crate of its own with SigV4 written out, and hands the token over through SSM

*Implementation, Phase 5b (`plugins/scaling/aws-asg/`).*

**Call.** The plugin lives with the first-party plugins
(`plugins/scaling/aws-asg/`), outside Legion's workspace (as `plugins/` is),
with its own lockfile and no AWS SDK: SigV4, the Query API, STS
`AssumeRoleWithWebIdentity` and IMDSv2 are written against `reqwest` and
`sha2`, which Legion already builds. The join token reaches new instances
through an SSM `SecureString` parameter the policy names
(`joinTokenParameter`), written before `SetDesiredCapacity`. Its tests
(SigV4 against AWS's published examples, recorded responses, the whole
plugin against a local mock AWS) run in its own CI job.

```nickel
target = { nodes = { plugin = "aws-asg",
  config = { asg = "legion-workers", joinTokenParameter = "/legion/workers-general/join" } } }
```

**Alternatives.**
- (a) The AWS SDK crates (not in `Cargo.lock`; well over a hundred new
  crates).
- (b) A workspace member.
- (c) Deliver the token in a new launch-template version's user data, or in
  instance tags.

**Trade-off.** Our own SigV4 to maintain (about 200 lines, tested against
the published vectors); a second dependency (SSM) for the token; its tests
are not part of `cargo test --all`.

**Why (simplicity, security).** (a) would more than double the build for
one plugin. (b) would put a plugin's dependency choices in Legion's lock.
A launch-template version per scale-up (c) is slow and rate-limited, and
tags are readable by anything that can describe instances; an SSM
`SecureString` is encrypted and readable only by the instance role the
operator grants.

## 94. The app's grant gains `node:write` and `job:read`

*Implementation, Phase 5b (`src/apps/bundled/autoscaler.ncl`; carries out
calls entry 18 as entry 44 deferred it).*

**Call.** The manifest's permissions are now `scale` read and write in
every namespace, `node` read and write, and `job` read in every namespace:
`node:write` to mint group join tokens, drain, and forget; `job:read` so
`GetCapacityDemand` names the jobs on a member (a `legion-system`
allocation's job in a protection), not just counts them.

**Alternatives.** Run node groups with redacted job names (namespaces
still decide protections).

**Trade-off.** The app can read every job's spec; it still cannot change
one.

**Why (simplicity).** `explain` and the events say *which* job holds a node
(`runs legion-system job autoscaler`), which is what an operator needs to
act on a protection.

## 95. A lone replica holds one lease for every shard

*Implementation, Phase 7 (`apps::autoscaler::shards`; deviates from plan
§6.9).*

**Call.** The plan's shard leases are 64 KV locks. A replica that is the
only live member holds one lock instead, `legion/apps/autoscaler/shards/all`,
which covers every shard. With two or more members, each holds individual
`shards/<n>` locks up to `ceil(64 / members)`; a replica holding `all` when
another joins takes its share one by one and then releases `all`, and the
newcomer waits for that before it takes any.

**Alternatives.** 64 shard locks always (the plan); no shard locks at all,
each replica deriving its shards from the member list (rendezvous hashing).

**Trade-off.** One more state in the lease logic (and a hand-over round
when the second replica joins).

**Why (performance).** Every lease renewal is a Control-lane raft write.
With 64 locks renewed every 5 seconds, the default single replica would
cost every cluster 13 writes a second forever, for nothing; with `all` it
costs two writes per renewal. Derived ownership without shard locks would
let two replicas with different views of the member list both take a shard
for as long as their views differ; the locks make the hand-over explicit.

## 96. Leases are listed with `ListKv`'s `include_locks`

*Implementation, Phase 7 (`ListKvRequest.include_locks`, `Store::list_kv_locks`).*

**Call.** A replica reads every lease in one `ListKv { prefix =
"legion/apps/autoscaler/", include_locks = true }`: the unexpired locks
under the prefix with their owner and expiry, filtered by `kv:read` like
entries. A listed lock's `lock_id` is left empty: only the holder needs it.

**Alternatives.** A new `ListKvLocks` RPC; membership as KV entries with a
heartbeat timestamp (no locks).

**Trade-off.** `ListKv` answers two things.

**Why (simplicity).** One flag on an existing read, already ACL-filtered
per key, and no new RPC to classify. Timestamps written by each replica
would compare clocks across hosts; a lock's expiry is the manager's.

## 97. Shards are FNV-1a of the target's key, and every node group is in one shard

*Implementation, Phase 7 (`autoscaler::shard`).*

**Call.** `shard = fnv1a64(key) % 64`, where the key is
`<job>/<group>/<task>` for a task target and `policy:<id>` for an external
target; every node group uses the key `nodegroups`.

**Alternatives.** Hash node groups like other policies; shard by namespace
(calls entry 13 rejected it).

**Trade-off.** All node groups load one replica.

**Why (correctness).** The expander chooses *one* group per evaluation
among every group that could host the same pending work; groups on
different replicas would each grow for the same replicas. A cluster has
few node groups.

## 98. The policy watch sends deltas, filtered by shard, and restarts when the shards change

*Implementation, Phase 7 (`WatchScalingPoliciesRequest.shard_count`,
`shards`, `deltas`; `ScalingPolicySet.full`, `removed`; closes calls entry
46's follow-up).*

**Call.** With `deltas`, the first message is the full set (`full = true`)
and each later one carries only what changed: new or changed policies, and
the keys of removed ones (`removed`). A change of more than 1,000 policies
is split into messages of at most 1,000, all but the last marked `more`,
and the app applies them together. With `shard_count` and `shards`, only
policies in those shards are sent. A replica whose shards change restarts
its watch with the new filter (and so gets a full set).

**Alternatives.** Change the filter in-stream (a bidirectional stream); keep
full sets and filter in the app.

**Trade-off.** A rebalance costs a full set per changed replica; the
manager still re-resolves every policy when its fingerprint moves (the
delta saves the wire and the app, not the resolution).

**Why (performance).** At 10,000 policies a full set is megabytes per
change, and past gRPC's 4 MiB default message limit; a pause or one job's
resubmit now sends one entry, and the first set arrives in parts that each
fit. Restarting on a rebalance is rare (a replica joining or dying) and
keeps the stream a plain server stream.

## 99. The status board keeps one report per replica; a target belongs to the replica that last reported it

*Implementation, Phase 7 (`manager::scaling_status`;
`ScalingAppStatus.replicas`, `ScalingTargetStatus.instance`).*

**Call.** The board keeps the newest report per instance and per target.
The app is `running` while any replica reported within three of its
intervals, and NOT RUNNING when none has; the app line names every replica
and its shard count, and each target's status names the replica that sent
it.

**Alternatives.** Merge replicas into one app report (the newest).

**Trade-off.** A replica that died lingers in `replicas` (marked stale)
for up to an hour.

**Why (simplicity).** An operator sees which replica owns a target and when
each last reported, which is what diagnosing a stuck shard needs.

## 100. Plugin concurrency is a cap on in-flight calls, and a program plugin stays at one

*Implementation, Phase 7 (`plugins.<name>.concurrency`; follows up calls
entry 67).*

**Call.** Every plugin gets a semaphore: `concurrency` in its app-config
entry (1 to 64) caps its calls in flight; the default is 8 for URL plugins
and the HTTP and `exec` builtins, and 1 for a program plugin, which may not
set more (`concurrency > 1 on a program plugin: it answers one request at a
time; serve it over url to take several`). Identical source queries were
already asked once per evaluation; that stays.

**Alternatives.** Multiplex requests over one program's stdio by id; start a
pool of program processes.

**Trade-off.** A slow program plugin still serializes its policies (the
Phase 4 limitation stands, with the `url` form as the way out).

**Why (simplicity, safety).** The cap is what protects a Prometheus or a
cloud API from 10,000 policies' worth of queries in one burst; a pool of
program processes would break plugins that keep state, and multiplexing
would break single-threaded plugins the protocol promises to support.

## 101. The 10,000-policy number is measured in process, on one machine

*Implementation, Phase 7 (`tests/integration/autoscaler_load.rs`, ignored;
docs/benchmarks.md).*

**Call.** Acceptance P7's load run submits 10,000 autoscaled jobs to an
in-process manager, heartbeats usage for every allocation, and times one
engine's evaluation ticks (inputs, decisions, report) with the built-in
`legion` source. It is an opt-in test (`--ignored`), and the published
numbers say which build and machine produced them.

**Alternatives.** A real cluster run on the AWS rig.

**Trade-off.** No network between the app and the manager, and the
manager's other load (placement, heartbeats from real workers) is absent;
the numbers bound the autoscaler's own cost, not a cluster's.

**Why (simplicity).** The question P7 asks is whether one replica keeps up
with its interval; that is the app's CPU and one batched read per 1,000
targets, which an in-process run measures exactly and repeatably.

## 102. A replica that cannot reach its leases keeps evaluating what it owned

*Implementation, Phase 7 (`Shards::step`).*

**Call.** When a replica cannot take or renew its membership lease (the
manager is unreachable, or refuses the KV call), it keeps the ownership it
last published, which for a replica that never held a lease is every
shard, and tries again next round.

**Alternatives.** Evaluate nothing until the leases answer.

**Trade-off.** While another replica holds the leases, two replicas may
evaluate the same shards (their writes stay compare-and-set, so nothing
doubles, but both pay for the evaluation).

**Why (simplicity).** The default is one replica, and a lone replica that
stopped scaling because a KV call failed would turn a lease problem into
an autoscaling outage. The leases decide who does the work, never whether
a write is safe.

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
