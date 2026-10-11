# Judgement Calls: Suspend, Resume, Fork, and Oversubscription

Decisions made while writing
[`plans/suspend-resume-and-oversubscription-plan.md`](../plans/suspend-resume-and-oversubscription-plan.md),
before any code exists. **Entries are ranked by project impact, highest
first. The number is the rank.** Implementation-time calls get inserted at
their rank as the phases execute. If a citation elsewhere names an entry by
number, fix it when you renumber.

## 1. Generic primitives in core, not an agent actor runtime

**Call.** Build seven independent primitives (usage, burstable reservations,
pressure ladder, checkpoints, suspended instances, idle policy, content
locality) and express the agent use case as a jobspec that combines them.
There is no "actor", "sandbox pool", or agent-specific object.

**Alternatives.** (a) A Substrate-style split: a new actor object
multiplexed by a node-local runtime onto pre-started worker allocations,
with its own lifecycle and API. This is what Google shipped and it reaches
agent density in fewer steps. (b) The old snapshot plan's stance: Legion
offers fast `create --from-snapshot` and platforms run idle timers and pools
themselves.

**Trade-off.** The agent story arrives only after Phases 3–5 and spans
the scheduler, worker, runtimes, and storage. (a) would be demo-able sooner
and contained in one subsystem. Revisit if Phase 4 resume latency cannot get
under ~500 ms without a pre-started worker, because that is the one thing
an actor runtime gets that restore-into-a-fresh-VMM may not.

**Why (simplicity).** An instance already has a stable id, a storage handle,
identity, and at-most-one-live-allocation. An actor would be a second copy
of all four, with a second scheduler the first cannot see. Each primitive
also earns its keep for non-agent users (`memoryMax` for bursty services,
the content store for every artifact), which (a) does not. (b) loses
because a scheduler that cannot see a suspended instance cannot release its
capacity, prefer its node on resume, or use suspend as a pressure valve.

## 2. Overcommit is a reservation/limit split plus a per-node ratio, with the guarantee stated honestly

**Call.** `resources.memoryMax` makes `memory` the reservation and
`memoryMax` the hard limit (cgroup `memory.low` / `memory.max`). Separately,
a worker can declare `oversubscription = { memory = 1.5 }`. At ratio 1.0 a
task within its reservation is never evicted for pressure. Above 1.0 that
guarantee is void, and the docs say so.

```nickel
resources = { memory = 400, memoryMax = 2048 }   # job
oversubscription = { memory = 1.5 }               # worker
```

**Alternatives.** (a) Kubernetes QoS classes (Guaranteed / Burstable /
BestEffort) as a named concept. (b) Borg-style usage-based scheduling:
place on measured usage plus headroom, no static ratio. (c) Nomad's shape
only: `memory_max` with no node ratio and no pressure response.

**Trade-off.** A static ratio is crude: it is either too conservative for
an idle-heavy pool or too aggressive for a busy one, and operators must
pick it. (b) adapts automatically and packs denser. We accept the
crudeness until Phase 1 has collected enough real usage history to tune a
usage-aware scheduler.

**Why (simplicity).** Two numbers on the task and one per node are
explainable at 3am, and the scheduler's fit check doesn't change shape.
QoS classes would be a third axis next to priority bands, and "above its
reservation right now" is a measured fact that the ladder can use directly.
(c) alone leaves overcommitted nodes to the kernel OOM killer, which is
the failure mode this plan exists to replace.

## 3. `suspended` is a new instance state, not a flavor of `stopped`

**Call.** `InstanceState` gains `suspended`, with a `checkpoint_id`. It sits
beside `stopped` instead of extending it.

```console
ID        STATE      CHECKPOINT
user-17   suspended  snap-77e0 (memory, local w-03)
user-31   stopped    -
```

**Alternatives.** Keep one `stopped` state and add an optional checkpoint
pointer. That means one fewer state for every consumer (UI, filters,
events, the scheduler's release hook) and was the simpler choice on its
face.

**Trade-off.** Every consumer of instance state learns a new value, and a
downgraded manager sees an unknown state (the upgrade doc covers it).

**Why (simplicity, for the operator).** The *system* puts instances into
`suspended` (idle policy, pressure ladder), while a human puts them into
`stopped`. Auto-wake must never wake something an operator deliberately
stopped, and "how many sandboxes are parked" is the number an operator
watches. Folding both into `stopped` would hide that distinction in a
nullable column that every filter has to remember.

## 4. Suspend releases the reservation by default

**Call.** A suspended instance holds no resources. `suspend.reserve = true`
keeps its reservation on its node for the "must resume instantly, always"
tier.

**Alternatives.** Hold the reservation by default (guaranteed resume
capacity, like a paused VM), and make release opt-in.

**Trade-off.** A resume can wait for capacity, and a local-durability
checkpoint cannot move, so a full node delays resume until something frees
up or `resumeTimeout` triggers a cold boot elsewhere.

**Why (performance and scale).** Releasing capacity is the entire density
win. A default that holds reservations would make suspend a slower `stop`
with no benefit, and the plan's headline scenario (2,000 sandboxes on 20
nodes) would not fit.

## 5. A resume on a full node makes room by suspending idle suspendable work there (resume guards)

*Resume guards, implementation.*

**Call.** When a resume from a `local` checkpoint cannot fit on its node, the
manager suspends instances on that node until it fits, in the `suspend`
pressure step's order (band ascending, then quiet longest). Eligible: a
running keyed instance whose task has a `suspend` block and that either sets
`onPressure = true` or is quiet right now by its own idle policy (the node
reports an idle-since), with a band that does not outrank the resume's. On by
default, behind manager `suspend.makeRoom`.

```nickel
suspend = { makeRoom = true },   # manager config, the default
```

**Alternatives.** (a) Only the reservation knobs (`reserve`,
`suspendedReserve`), so a full node simply waits. (b) Eligibility from
`onPressure` alone. (c) Any instance with a `suspend` block, idle or not.
(d) Use preemption: evict lower-band work for the resume.

**Trade-off.** A returning user can push a quiet user's sandbox into
`suspended`, which costs that user one restore later. An instance with an idle
policy is eligible the moment it is quiet, well before its own `after`, so
make-room suspends earlier than the idle sweep would. Same-band work is fair
game.

**Why (ease, performance).** Suspend is reversible and keeps state, so it is
the cheapest way to turn "node full" into "room", and it keeps the density
win of releasing reservations (entry 4) without making resumes wait out
`resumeTimeout`. (b) would leave the agent guide's sandboxes, which all set
`idle`, ineligible unless authors also opt into pressure. (c) would suspend
an active user mid-command. (d) destroys state and needs a `stop` the user
did not ask for. Excluding more important bands mirrors preemption, so an
operator reasons about one rule.

## 6. `suspend.reserve` takes a flag, a ratio, or fixed amounts, and only a full hold keeps cores (resume guards)

*Resume guards, implementation.*

**Call.** `reserve` accepts `false`/`true`, a ratio in `0..=1`, or
`{ memory = "1GiB", cpu = 100 }` (MiB or a size, and MHz). A ratio scales
`memory` and `cpu` MHz. Exclusive `cores`, devices, custom resources, and the
`allocations` slot are held only at exactly 1.0. An amount must fit inside the
task's declared `resources` for that dimension, and naming an undeclared
dimension is refused at submit.

```text
Error: task "workspace": suspend.reserve.memory (4096 MiB) exceeds the task's resources.memory (2048 MiB). A suspended instance cannot hold more than it reserves while running
Hint: lower suspend.reserve.memory, or use reserve = true to hold the whole reservation
```

**Alternatives.** (a) A separate `reserveRatio` field next to the boolean.
(b) Scale cores too (round down). (c) Check amounts against the manager's
`taskResourceDefaults` when a dimension is undeclared.

**Trade-off.** One field with three shapes is harder to describe in a schema,
and an untagged parse gives worse errors, so the field is parsed by shape and
range-checked in `resolve` to name the field and the fix. Requiring the
dimension to be declared refuses some specs that would have worked.

**Why (simplicity).** The common answers are "all", "none", and "about a
quarter", and the existing boolean stays valid with no migration. A pinned
core is exclusive by definition (entry 66), so a quarter of
one cannot be held. Submit-time validation is offline and does not know the
manager's defaults, so (c) could pass a spec that means something else on the
node.

## 7. Checkpoints are node-local by default, publishing is opt-in

**Call.** `suspend.durability = "local"` (default) keeps the checkpoint only
on the suspending node and pins resume there. `"store"` publishes it to the
worker's `contentStore` so resume can land anywhere and survives node loss.

**Alternatives.** Always publish (every suspend uploads its memory to object
storage), so suspended instances are never tied to a node.

**Trade-off.** By default, a node loss loses every suspended memory state
on it. With `onRestoreFailure = "cold"` those instances reboot on their
storage, which is also node-local by default, so the honest summary is
"node loss loses parked work, just like it loses running work today."

**Why (performance).** Uploading gigabytes on every idle suspend would make
object-store bandwidth and cost the bottleneck for a feature whose common
case is "resume on the same node twenty minutes later." Fly made the same
call. Platforms that need portability turn on `store` per job.

## 8. A published checkpoint needs instance data that can move, so node-local volumes are refused rather than copied (Phase 7b)

*Phase 7b, implementation. Answers the plan's §6.8 "the node-local volume is
a separate question".*

**Call.** `durability = "store"` is refused when the instance volume lives on
one node: at submit for the built-in `local-dir`, `host`, and `tmpfs`, at
admission for a plugin driver that advertises `nodeLocal`, and again by
`SuspendInstance` (and `snapshot create --publish`) on the node. A published
checkpoint works with no `stateful.storage` (the guest's root disk is a
checkpoint part) or with a network driver such as `nfs`.

```text
Error: task "workspace" sets suspend.durability = "store", but its stateful.storage uses volume driver `local-dir`, which keeps the data on one node. A published checkpoint could resume on another node without its volume
Hint: use a network volume driver (such as `nfs`) for stateful.storage, drop stateful.storage, or keep durability = "local"
```

**Alternatives.** Publish the `local-dir` volume as a content part too
(archive the directory or the block image, upload it, and recreate the
volume from it on the new node). Or accept the combination and let the
resume land elsewhere on an empty volume.

**Trade-off.** The plan's flagship agent jobspec (a `local-dir` block volume
plus a memory checkpoint) cannot use `store` as written. It has to drop its
volume (keeping data in the guest's root disk, which travels with the
checkpoint) or move it to a network driver. The acceptance criterion holds
for those shapes, not for `local-dir`.

**Why (simplicity, correctness).** Moving a volume means re-homing a volume
row the scheduler, the binder, and the node's driver all treat as pinned:
the volume pin query, the binder's ordering, provisioning on the new node,
and reclaiming the old copy, for every driver. That is its own feature with
its own failure modes (a half-moved volume), and doing it quietly inside
publish would make a 10 GiB volume upload on every suspend. Accepting the
combination would resume a guest whose page cache describes a disk that is
not there, which is the silent corruption the plan forbids. Refusing at
submit is honest, and volume migration can arrive later as a driver op
without changing this surface.

## 9. The pressure ladder runs on the worker and reports afterward

**Call.** Under memory pressure the worker reclaims, suspends opted-in idle
instances, and evicts, in a fixed order, without asking the manager. It
reports actions in the heartbeat and the manager reconciles state.

**Alternatives.** Manager-driven pressure response: workers report
pressure, the manager picks victims with full cluster knowledge and issues
stops.

**Trade-off.** The worker acts with node-local knowledge only and can pick
a victim the manager would not have. The manager has to reconcile
worker-initiated suspensions, which adds a second entry into the suspend
transaction.

**Why (performance).** Memory pressure escalates in seconds and the kernel
OOM killer doesn't wait for a heartbeat round trip, let alone a manager
that may be unreachable. Kubernetes' kubelet eviction is node-local for the
same reason. Idle suspend, which is not urgent, stays manager-driven (see 18).

## 10. Make-room avoids thrash with an age rule, one round at a time, and all-or-nothing rounds (resume guards)

*Resume guards, implementation.*

**Call.** Three rules: an instance whose row changed state within the last
two minutes (`MAKE_ROOM_COOLDOWN`, read from `instance.updated_at`) is never
chosen. While any suspend is in flight on the node, make-room waits, and each
resume gets at most one round per two minutes. A round starts suspends only
when the chosen set is enough for the resume to fit, counting what each victim
keeps held. Otherwise nothing is suspended and `no_room` is counted.

**Alternatives.** (a) A per-node rate limit only. (b) Mark make-room victims
and forbid them from triggering make-room themselves. (c) Suspend
incrementally, one per pass, until it fits. (d) Make the cooldown a manager
setting.

**Trade-off.** A legitimately busy node can wait up to two minutes for a
second round. `updated_at` moves on any state or binding change, so an
instance can look younger than it is, which only ever makes it safer. The
memory of rounds is in-process, so a manager restart grants one early round,
which the age rule still bounds.

**Why (simplicity).** The age rule alone breaks the A-suspends-B,
B-suspends-A cycle, because the instance a round resumed is always younger
than the cooldown when it could be chosen again. All-or-nothing keeps a node
that can never fit the resume from losing anything. (b) needs new state on
the instance. (c) would suspend work for a resume that may never fit. A
constant keeps the surface to one toggle until someone needs to tune it.

## 11. Memory pressure is configured as named steps under `resourceEnforcement`, with a previewable order

> **Revised (PR #15 review).** The triggers were renamed `minFree` and
> `maxStall`, from `freeBelow` and `stallAbove`, to match
> `content.minFreeDisk`: memory and disk floors use the same words, and each
> reads as a limit the node defends. No alias for the old names. The worker
> config rejects unknown fields, so an old name fails at load.

*Execution-time revision of the plan's §6.4.*

**Call.** The pressure response is described by what happens to a task, not
by kernel mechanics. Steps have names (`reclaim`, `suspend`,
`evict-bursting`, `evict-reserved`) instead of rung numbers. Triggers read
as plain thresholds (`freeBelow`, `stallAbove`) instead of PSI field names.
Both `oversubscribe` and `memoryPressure` nest under `resourceEnforcement`,
the response is on by default whenever enforcement is, and an operator can
drop a step from `steps`. `legion node usage` prints the current "who goes
first" order.

```nickel
resourceEnforcement = {
    enabled = true,
    oversubscribe = { memory = 1.5 },
    memoryPressure = { freeBelow = "2GiB", steps = ["reclaim", "suspend", "evict-bursting"] },
}
```

```console
Detail:  evicted by evict-bursting on w-03: using 1.9 GiB of a 400 MiB reservation ...
```

**Alternatives.** (a) The plan's first draft: a top-level `pressure {
psiSomeAvg10, minAvailable, settle }` block, a sibling top-level
`oversubscription` block, and a five-rung table. (b) A per-task
`onMemoryPressure = "reclaim" | "suspend" | "evict"` knob so each job picks
its own fate. (c) Kubernetes-style named QoS classes.

**Trade-off.** Nesting makes the worker config one level deeper, and a
fixed step order means an operator cannot, say, evict before suspending.
Dropping a step is the only customization. (b) would give job authors more
control but lets every job opt out of eviction, which defeats the point on a
shared node.

**Why (simplicity/ease).** The first draft made users learn the kernel's
PSI vocabulary and a numbered table to answer one question: "what happens to
my task when the node runs short?" The answer now fits in three sentences
in the public doc, the stop reason names the step that acted, and the
preview shows the order before it matters. Nesting under
`resourceEnforcement` turns "oversubscription without cgroups" from a
warning into a config that cannot be written. Bands plus
`suspend.onPressure` already give job authors the two levers that matter
(how important, and can it be parked), so (b) adds surface without adding
a decision.

## 12. The content location index is in-memory soft state, off raft

**Call.** Workers send content deltas in the heartbeat. The manager keeps
`digest → nodes` in memory only, rebuilt from full reports after failover.
Only entries of at least 1 MiB are advertised. This amends the artifact cache
plan, which used a `legion.artifact_cache` dynamic attribute.

**Alternatives.** (a) The cache plan's dynamic attribute. (b) A replicated
`node_content` table.

**Trade-off.** After a leader change, locality scores are zero until each
node's next full report, so a few seconds of placements ignore warmth. The
index is invisible to `legion state query`.

**Why (performance and scale).** At snapshot scale a node's content set
churns on every suspend. Dynamic attributes are replicated and every change
enqueues evals for the node's jobs (`src/manager/server.rs:6209-6217`).
A table puts that churn on raft. Losing locality briefly costs placement
quality, never correctness, which is the same argument that took liveness
off raft.

## 13. Locality is a score, except for local checkpoints

**Call.** The `locality` term is a weighted preference in the existing sum.
The only hard placement rule is that a resume from a `durability = "local"`
checkpoint pins to its node.

**Alternatives.** Make locality a constraint whenever the content is large
(say, over 1 GiB) to avoid slow cross-node transfers.

**Trade-off.** A placement can land on a cold node and pay the fetch,
which for a multi-GiB snapshot is seconds without lazy memory.

**Why (simplicity).** A size threshold that flips placement from "prefer"
to "require" is a cliff nobody will remember at 3am, and it would turn a
slow placement into a stuck one when the warm nodes are full. The one hard
case is hard because the bytes exist nowhere else.

## 14. Suspend and snapshots ride the allocation payload, and the worker knows its instance

**Call.** `TaskAllocation` carries the task's `suspend` and `snapshots`
blocks as JSON (`task_suspend`, `task_snapshots`, proto fields 23 and 24 in
a `// Track 1` block), and the worker sets `Allocation.instance_id` from the
`LEGION_INSTANCE_ID` the manager already injects into the task env.

**Alternatives.** (a) Leave the payload alone and have the worker ask the
manager for the job spec. (b) Send one derived summary (idle policy, on
pressure, checkpoint launch) instead of the blocks.

**Trade-off.** Two more strings on every payload that declares the blocks,
and a manager that predates them leaves them empty, so a mixed-version node
still reports no idle time and launches no sandbox ready for checkpoints.

**Why (correctness, simplicity).** The worker rebuilt its `TaskConfig` from
the payload and never saw either block. So on a real cluster no node ever
reported idle time, `onPressure` was always off, and no sandbox launched
ready for memory checkpoints. Unit tests built the `TaskConfig` directly and
never noticed. The macOS end-to-end test found it on its first run. Sending
the declarations keeps the worker's single predicate
(`memory_checkpoint_ask`, `IdlePolicy::for_task`) as the one decision,
rather than a second copy of it in a summary. (a) would put a manager round
trip on every launch.

## 15. A published restore is unpinned and checked per candidate, for any instance with a restore source (Phase 7b)

*Phase 7b, implementation. Lifts what 3c deferred: `compat` was only checked
on the pinned node.*

**Call.** The scheduler's restore rule is keyed on the snapshot's state, not
on why the instance restores. A `local` snapshot pins the instance to its
node and checks `compat` there, as before. A `published` one gets no pin: its
`compat` filters every eligible candidate, and its parts join the placement's
content set so `locality` prefers nodes holding them. The binder accepts a
restoring instance with a published snapshot on any node and prefers the
node that captured it. The rule reads `instance.restore_from`, so a resume, a
RestoreInstance rollback, and Phase 6's fork and `create --from-snapshot`
all get cross-node placement once their snapshot is published.

**Alternatives.** Pin a published restore to its capturing node while that
node is up, and float it only once the node is gone. Or keep checking
`compat` against one chosen node and fail the placement there.

**Trade-off.** A published resume can move off a healthy node that holds its
parts when another node scores higher overall (locality is one weighted
term), and then pays the fetch. When several restoring instances of one task
have published snapshots with different `compat` and are placed in the same
plan, the binder pairs allocations to instances in its own order, which can
put an instance on a node checked against another instance's manifest. The
node's restore then fails with a named reason and `onRestoreFailure` applies.

**Why (simplicity, extensibility).** "Prefer, never require" is the plan's
locality rule (call 13), and a soft pin that flips on node health is a second
mechanism for the same preference. Filtering per candidate is what makes the
placement error useful ("no eligible node can run it", with the attribute
per node) instead of failing on whichever node was picked. Keying on
`restore_from` means fork needs no placement code of its own.

## 16. A restore's placement names its instance, and the apply binds exactly that instance (integration)

*Phase 6 and 7b integration. A bug fix: the binder could cross restores.*

**Call.** The planner records, on each placement it made for a restoring
instance (a hard pin or a floating published restore), that instance's id
(`PlannedAllocation.instance_id`). The id rides the format-2
`plan_submission` placements JSON (`instance_id`, omitted when absent), and
the apply passes it to the keyed binder as the instance to bind, the way an
edge-boot ratification does. `tests/published_resume_integration.rs`
(`restores_with_different_compat_in_one_plan_bind_their_own_nodes`) shows
the crossing it fixes.

**Alternatives.** (a) Teach the binder's SQL to check `compat`. (b) Order
the planner's restores the way the binder orders instances.

**Trade-off.** With `plan_candidates.count = 0` (the kill switch that emits
format-1 placements for mixed-version managers) the instance id is dropped,
and binding falls back to the old order. A target that is no longer
bindable fails the plan's apply rather than binding another instance.

**Why (correctness, simplicity).** The planner already chose the node for
one instance: the node passed that instance's pin, `compat`, and spread
rule. (a) would duplicate the compat check in SQL, and (b) breaks as soon
as preemption, fallback candidates, or a snapshot-node preference reorders
anything. Naming the instance makes the binder's job a lookup.

## 17. On macOS, `resourceEnforcement.enabled` is the measuring switch, read through one `container stats` call

**Call.** A macOS worker with `resourceEnforcement.enabled` confines
nothing, but reports usage for Apple-container sandboxes from one
`container stats --format json --no-stream` call per report (every running
container, filtered by the name each sandbox recorded).

**Alternatives.** (a) Measure Apple containers whenever they run, with no
switch. (b) Call `container stats <name>` per sandbox.

**Trade-off.** "Enforcement" on a Mac enforces nothing, which the worker's
startup log now says. The call is a subprocess on the heartbeat path every
10 seconds while any dev sandbox runs.

**Why (simplicity).** One switch means "this node measures", on every OS, so
`idle-unmeasured` keeps its one fix. (b) fails the whole call when any
named container has stopped, which happens in every teardown window.

## 18. Idle is detected on the worker and acted on by the manager

**Call.** The worker computes `idle_since` per allocation from signals it
already sees. The manager runs a 15s sweep and issues suspends.

**Alternatives.** Let the worker suspend idle allocations on its own, like
the pressure ladder does.

**Trade-off.** Up to one sweep interval plus a heartbeat of extra idle time
before suspend, and a manager-side loop.

**Why (simplicity).** Suspend is an instance state transition, and the
manager owns instance rows. Keeping non-urgent transitions manager-driven
means exactly one component normally decides, and the worker-initiated path
(entry 9) stays an exception reserved for emergencies.

## 19. The `suspend` step acts first, reports a local suspension, and the desired set is its acknowledgment (Phase 5)

*Phase 5, implementation.*

**Call.** Under memory pressure the worker suspends opted-in instances on its
own, as entry 9 planned. It reuses the SuspendInstance capture (`capture_for_suspend`) with a
snapshot id it mints, holds the allocation out of relaunch, and reports a
`SnapshotReport` with `local_suspension = true`. The manager has no row for
that id, so it inserts one (`purpose = suspend`) and runs the same suspend
transaction with `suspend_reason = pressure`. The worker re-sends the report
on every heartbeat until the manager drops the allocation from the desired
set. If the instance moved on meanwhile (stopped, destroyed, replaced), the
row is inserted as `deleting` and the node reclaims the checkpoint. A
SuspendInstance capture still outstanding for the same instance is dropped:
the node already stopped the allocation, so it could never finish.

The step's candidates are every allocation whose task sets
`suspend.onPressure = true`, in any band (`critical` included) and with
node-pinned storage, ordered band ascending then quiet longest first. It
does not require the instance to be idle: a busy opted-in instance is
suspended last, but before any eviction. `legion node usage` shows the row as
`reclaim, then suspend` and lists no step after `suspend`.

```console
 #  ALLOCATION      BAND    RES/USED MiB  STEP
 2  1042 workspace  normal  512/1830      reclaim, then suspend
```

**Alternatives.** (a) The worker asks the manager to suspend and waits for a
`suspend_allocations` entry, like SuspendInstance. (b) A dedicated
`local_suspensions` heartbeat field and an explicit ack field in the reply,
as the plan's §7 sketched. (c) Only idle opted-in instances are candidates.
(d) Persist pending local-suspension reports in the worker's state store.

**Trade-off.** A worker restart between the capture and the manager applying
the report loses the record: the checkpoint stays pinned on the node with no
row, and the allocation is relaunched fresh on its retained volume. Listed in
the public limitations and TODO.md. The desired set as an ack means a report
can be re-sent a few times, so the apply is idempotent by snapshot id.

**Why (simplicity, the user's job).** (a) waits a heartbeat or more while the
node runs out of memory, which is the case the step exists for. (b) adds two
wire fields that duplicate what the snapshot channel and the desired set
already say. (c) would let an opted-in instance be *evicted* (state lost) when
it happened to be busy, which is worse for its author than a suspend. The
author opted in, so the band and pinning rules that protect against losing
work do not apply to a step that loses none. (d) is a new persistence path for
a narrow window, deferred until it shows up in practice.

## 20. The idle policy needs resource enforcement, and says so on the instance rather than at submit (Phase 5)

*Phase 5, implementation.*

**Call.** Idle detection rides the usage sampler, which runs only on workers
with `resourceEnforcement.enabled` (it measures CPU from the allocation's
cgroup and reports with usage). On a node without it, a task's `suspend.idle`
is accepted and never fires, and `instance get` says so:

```text
detail: idle-unmeasured: the task's suspend.idle policy (after 180s) cannot act because node w-03 sends no usage reports (set resourceEnforcement.enabled on its worker type), so the instance will not suspend when idle
```

**Alternatives.** (a) Refuse `suspend.idle` at submit unless every eligible
node enforces. (b) Run the sampler without enforcement, with only the network
and exec signals. (c) Make `suspend.idle` a placement constraint (only nodes
that enforce).

**Trade-off.** The requirement is a runtime fact, not a submit error, so a job
can sit on a non-enforcing node with an idle policy that does nothing, visible
only in `instance get`, the docs, and the missing `idle since` line.

**Why (simplicity).** Whether a node enforces is node state, and submit cannot
know where an instance will land. (a) would reject specs that are fine on most
of the fleet. (b) gives an idle policy that silently ignores CPU, so a busy
compile with no traffic would suspend mid-build. (c) changes placement for a
field the author thought was about suspend. Telling the operator where it
cannot act, in the place they look, keeps the failure loud without inventing
a new placement rule. macOS workers (no cgroups) fall here too, which is why
the real-`exec` idle test could not run on the dev machine.

## 21. Disk-only suspend for `exec` and `container`, no CRIU

**Call.** `exec` and `container` support `checkpoint = "disk"` only. They
suspend (storage retained, resources released, auto-suspend and wake apply)
and always cold-boot on resume.

**Alternatives.** CRIU-based memory checkpoints for both runtimes.

**Trade-off.** Non-VM workloads lose in-memory state on suspend, so the
feature is weaker for them, and users will ask why containers can't do what
sandboxes can.

**Why (simplicity and security).** CRIU couples to kernel versions, TCP
state, and namespaces, and Kubernetes is disabling its implicit restore
path after CVEs where restored processes kept the source's credentials. VM
snapshots have a small, well-defined state surface. The `CheckpointSupport`
seam leaves room for a CRIU runtime plugin later.

## 22. Fork charges every fork its full reservation

**Call.** Forks on one node share unmodified memory pages through
`MAP_PRIVATE`, but each is still charged its full `memory` reservation.

**Alternatives.** Charge forks a fraction (or zero) for shared pages,
which would make fork fan-out much denser on paper.

**Trade-off.** Real density from sharing goes unclaimed by the scheduler.
It shows up only as slack that the oversubscription ratio and the ladder
can use.

**Why (simplicity).** Shared pages decay as forks diverge, at a rate
nobody can predict. Placement that depends on a number that shrinks under
it would produce surprise pressure later.

*Phase 6, as built.* The sharing is Firecracker's: every fork's restore
hard-links the content-store memory part into its own directory and loads it
`MAP_PRIVATE`. cloud-hypervisor copies memory into the guest at restore, so
its forks share nothing. The public doc says so.

## 23. Forks wait as `pending` instances with `pending_op = fork`, not in a new state (Phase 6)

*Phase 6, implementation.*

**Call.** ForkInstance creates the fork instances and the fork checkpoint in
one transaction, before the node has captured anything. Each fork is
`pending` with `restore_from` naming the checkpoint and `pending_op = fork`.
The keyed desired count, the binder, and the restore pins all skip
`pending_op = fork`, so nothing is placed until the capture report clears it
(and enqueues the placement) in the transaction that makes the checkpoint
`local`. A failed capture terminates the forks with the reason in `detail`.

```console
$ legion instance list agents
ID          STATE    ...  DETAIL
run-17-f1   pending  ...  (forking)
```

**Alternatives.** (a) Create the instances only when the capture lands, and
reserve the ids some other way. (b) A new `forking` instance state. (c) Park
forks as `suspended` with the fork checkpoint as their `checkpoint_id`.

**Trade-off.** Three placement queries grow a predicate, and a failed fork
leaves terminated tombstones behind (ids are never recycled, so a retry gets
the next numbers).

**Why (simplicity).** The reply has to return the ids, and an id must exist
to be owned (a concurrent `instance create --id run-17-f1` must collide), so
(a) needs a second id-reservation mechanism. (b) touches every match on
`InstanceState` in the codebase for a state that lasts one heartbeat. (c)
would let `instance start` resume a fork from a checkpoint that does not
exist yet, and would show forks as suspended. `pending_op` already exists for
exactly this ("an operation is in flight"), and `instance get` shows it as
`forking`.

## 24. `--spread` publishes the fork checkpoint, and each fork goes to a node no sibling runs on (Phase 6, finished at integration)

*Phase 6 shipped the flag refused with a hint. The Phase 6 and 7b integration
made it work.*

**Call.** `legion instance fork --spread` takes a memory checkpoint with
`durability = store`. The forks wait for the publish, not the capture
(`instance.forked` fires when the snapshot is `published`). Each fork is then
a floating published restore with one more hard rule: no node that runs, or
in this plan was given, a fork of the same checkpoint. The origin's node may
take one. A fork with no such node left is skipped for this plan and waits,
and the task's other placements go ahead. Spread placements carry no
fallback candidates. There is no separate `--publish` flag on fork, and the
task's `suspend.durability` does not apply to forks.

```console
$ legion instance fork run-17 --count 3 --spread --wait -j
{"ok":true,"data":{..., "durability":"store","checkpoint_state":"published", ...}}
```

**Alternatives.** (a) A `--publish` flag on fork, independent of
`--spread`. (b) Fork checkpoints follow the task's `suspend.durability`.
(c) Spread as a soft preference (anti-affinity score). (d) Fail the whole
plan when a spread fork cannot be placed, as a compat failure does.

**Trade-off.** Spreading needs a `contentStore`, a memory checkpoint, and
an instance with no `stateful.storage`, and it adds an upload before any
fork starts. The distinct rule holds for a fork's first placement only: a
fork rescheduled later is placed like any instance. Preemption can still
run for a spread placement that then finds every node taken (it books
victims before the spread filter), which wastes an eviction in a rare case.

**Why (simplicity).** A fork checkpoint is spent as soon as its forks
restore, so publishing one is only worth its upload when the forks need to
leave the origin's node, which is exactly `--spread`. (a) and (b) would
upload gigabytes nobody reads. (c) is the silent degrade the user ruled
out: "spread" that sometimes stacks is not spread. (d) would let one fork
too many block every other placement of the task, when the natural outcome
is that the extra fork waits like a `distinct` placement. No fallback
candidates keeps two siblings in one plan from both falling back to the
same node.

## 25. `instance start` resumes, with `--cold` to discard

**Call.** No `resume` verb. `legion instance start` on a suspended instance
restores from its checkpoint, and `--cold` discards it and boots fresh.

**Alternatives.** A separate `legion instance resume`, with `start`
rejecting suspended instances.

**Trade-off.** "Start" now means two different things depending on state,
and an agent reading the CLI has to know that.

**Why (simplicity).** Callers want "make it run." A separate verb forces
every platform and agent to check state first and branch, and gets a
needless error when it guesses wrong. The response says what happened
(`resumed_from`), so the difference is visible without being required.

## 26. Sandbox guest size follows `resources`

**Call.** Guest memory is `memoryMax` if set, else `memory`, and a sandbox
task that declares no `memory` gets the runtime's own 512 MiB default, ahead
of `taskResourceDefaults.memory` even when an operator set one explicitly.
The derivation runs once, at submit (`TaskPlugin::normalize_task`, built-in
runtimes only), and pins the result into the stored config as `memory_mib`.
The worker boots `memory_mib`, falling back to 512, exactly as before. An
explicit `config.memory_mib` that disagrees is a submit warning in the
Phase 2 release, and still sizes the guest there, then an error in a later
release. `enforced_memory_limit_bytes` reads the same `memory_mib`, so the
cgroup ceiling is the guest plus `sandbox.vmmOverheadMiB`.

```console
warning: task workspace sets config.memory_mib = 1024, but its resources size the guest at
512 MiB (resources.memoryMax, else resources.memory). This release still boots 1024 MiB, ...
```

**Alternatives.** (a) Keep `memory_mib` independent, as before. (b) Reject
the mismatch at once, as the plan's §6.3 first said. (c) During the warning
release, boot the derived size anyway and only warn. (d) Derive on the
worker from the stored `resources`, the first Phase 2 cut. Stored configs
are normalized, so the worker could not tell a declared `memory` from a
defaulted 256 MiB, and every undeclared sandbox, including ones already
stored, would have shrunk silently from 512 to 256 MiB. (e) Let an explicit
`taskResourceDefaults.memory` win for sandboxes.

**Trade-off.** An undeclared sandbox now reserves 512 MiB where it reserved
256 MiB, so it packs less densely, which is the honest number since it
always booted 512. Resubmitting a job stored by an older manager pins the
size and shows as a new version. An operator who wants smaller guests
cluster-wide has to say so per job (`resources.memory`). Third-party
runtimes cannot hook normalization, because it runs inside the submit
transaction and must not load a dylib. A derived guest under 64 MiB fails at
submit. Apple `container` (macOS) sizes its VM from `memoryMax`, else
`memory`.

**Why (simplicity).** Two unrelated memory numbers on one task are the
reason a sandbox could reserve 128 MiB and run a 4 GiB guest. With burst and
pressure handling in play, the scheduler, cgroup, and guest must agree on one
model. (b) would break a resubmit of every existing spec that sets
`memory_mib`, which §10 rules out. (c) and (d) would silently resize running
workloads on their next restart, which the project rules out: a change must
fail loudly or preserve behavior. (e) loses because `taskResourceDefaults`
predates any tie between reservation and guest size and is tuned for host
processes. Honoring it would turn a scheduling default into a guest resize
nobody asked for, the same silent change. Pinning at submit also keeps old
and new workers in agreement, since both read `memory_mib`. The warning text comes from a new `TaskPlugin::check_resources` hook, so
any runtime that sizes its own workload can do the same.

## 27. `cpus` stays an alias for `cpu` in MHz, with a submit warning under 10 (integration)

*Found while writing the agent sandboxes guide.*

**Call.** `resources.cpus` keeps meaning `cpu`, in MHz. Submit (`job run`)
warns when `cpu` or `cpus` is above 0 and under 10, naming the likely
intent (`did you mean cpu = 1000?`), and every example that implied a CPU
count is now `cpu = <MHz>`. The fractional-`cores` hint suggests `cpu = 500`,
not `cpus = 0.5`.

```text
task workspace sets resources.cpus = 1, which is 1 MHz, not 1 CPU(s): `cpu`
and its alias `cpus` are in MHz. Did you mean cpu = 1000?
```

**Alternatives.** (a) Make `cpus` mean a CPU count. (b) Reject `cpus` at
submit. (c) Deprecate the alias now.

**Trade-off.** The footgun stays in the language. A deliberate tiny share
(`cpu = 5`) warns on every submit.

**Why (simplicity).** (a) silently changes the reservation of every job in
the wild that uses `cpus`, and (b) breaks them. A warning catches the
mistake at the moment it is made and changes nothing that works. Deprecating
the alias is a TODO.

## 28. Allocation ids are never reused (Phase 4)

*Phase 4, implementation. Reverses what Phase 3c accepted.*

**Call.** The `allocation` table is rebuilt with `INTEGER PRIMARY KEY
AUTOINCREMENT`, so every placement gets a new id, including the allocation a
resume or a snapshot restore places. A restored guest's workload-identity
token is issued for that new id, so its `legion.io/alloc` claim names the new
allocation, which the plan's §6.12 requires.

```console
$ legion instance get user-42 -j | jq .data.instance.current_alloc_id   # 41 before suspend
$ legion instance start user-42 --wait && legion instance get user-42 -j | jq .data.instance.current_alloc_id
42
```

**Alternatives.** (a) Keep id reuse (`INTEGER PRIMARY KEY` hands out
`MAX(id) + 1`) and let the worker tell a replacement from the original, as
3c did for RestoreInstance. (b) A side counter table read and bumped on every
insert. (c) Mint fresh ids only on the resume and restore paths.

**Trade-off.** A one-time table rebuild on upgrade (every row copied, indexes
recreated), and an id deleted before the migration can still be reused once.
`sqlite_sequence` is now written by allocation inserts too.

**Why (simplicity, and the security requirement).** With reuse, a suspend of
the newest allocation and a resume hands the new allocation the old id, so a
token naming "the new allocation" would name the old one too, and the
at-most-one reasoning everywhere has to ask whether an id means the same
task. (a) left that ambiguity in every consumer. (b) adds a write per
placement for what SQLite does natively, and the eval and rollout tables
already use AUTOINCREMENT the same way. (c) still lets an ordinary stop then
start collide. 3c's worker guard (`reconcile_allocations` stopping a reused
id) stays as a defense for stores migrated mid-flight.

## 29. `memory.low` protects every governed reservation, not only bursting ones (Phase 2)

**Call.** Every governed allocation that declares `memory` gets
`memory.low = memory`, whether or not it sets `memoryMax`. The worker writes
`memory.low = max` on the cgroups it owns (`legion/`, `share/`, `reserve/`) so
leaf protection passes through. `container` gets the same as OCI
`memory.reservation`.

**Alternatives.** Set `memory.low` only on tasks with `memoryMax`, the
literal reading of "`memory.max = memoryMax` and `memory.low = memory`".

**Trade-off.** A cluster with enforcement on and no `memoryMax` anywhere
still sees a new cgroup file written on every task. The kernel only consults
it under reclaim, and its effect there is to protect Legion reservations from
reclaim ahead of unprotected memory, which is the point. Ancestors above
`cgroupParent` can still cap protection when cgroup v2 is mounted without
`memory_recursiveprot` (systemd's default has it). The doc's failure table
says so.

**Why (simplicity).** The plan's ordering promise is "the kernel reclaims
from tasks above their reservation before it touches tasks within it." That
only holds if tasks within their reservation are protected, including the
ones that never burst. Protecting only bursting tasks would make a
non-bursting neighbour the kernel's first target, exactly backwards.

## 30. Sandbox tokens refresh through the identity disk, in place, and env with `noop` is allowed where that works

**Call.** A sandbox supports file delivery under `/legion/identity`. The
worker writes the token to `<task dir>/identity/` on the host and copies it
onto the guest's identity disk, rewriting the disk in place on each refresh
(same allocation and host time, new tokens). The guest's handler tells a
token refresh from a restore by that and rewrites only the token files. On a
memory-checkpointable, non-gated sandbox, env delivery also mirrors to
`/legion/identity/<ENV>`, so env with `changeMode = "noop"` is accepted
there. `changeMode = "restart"` on such a sandbox is a submit warning.

**Alternatives.** (a) A vsock agent that writes files into the guest. (b) A
virtio-fs or 9p share of the host token directory. (c) Keep env-only and
document the reboot. (d) A new `changeMode = "guest"`.

**Trade-off.** The env var still holds the boot token, so a process that
read it once keeps a stale one. A guest whose libc cannot run the handler
never sees a refresh. In dev mode, env tokens are not mirrored.

**Why (simplicity).** The identity disk and its poller already exist on
every memory-checkpointable sandbox for restores, so a refresh is one more
writer of the same format. (a) and (b) are new guest plumbing on both VMMs.
(c) reboots the guest every half TTL, which defeats a sandbox kept for its
running state. (d) is a mode name for what `noop` already means for files.
The rename-based writer could not be reused, because a running VMM keeps the
old inode open.

## 31. Sandboxes get a balloon for `reclaim`, plus free page reporting on cloud-hypervisor (Phase 5)

*Phase 5, implementation. Answers the plan's open question 3.*

**Call.** A sandbox that can run above a reservation (`memoryMax` above
`memory`, or launched for memory checkpoints) boots with a virtio-balloon
device, starting deflated, with `deflate_on_oom`. The `reclaim` step inflates
it by the bytes above the reservation, never past `guest - reservation`, and
deflates it when the pressure episode ends. cloud-hypervisor also gets
`free_page_reporting=on`. Firecracker gets the balloon without it.

```text
--balloon size=0,deflate_on_oom=on,free_page_reporting=on          # cloud-hypervisor
"balloon": { "amount_mib": 0, "deflate_on_oom": true, ... }       # Firecracker config
```

**Alternatives.** (a) Free page reporting only, everywhere. (b) Balloon only.
(c) A balloon on every sandbox. (d) Leave the balloon inflated after the
episode and let `deflate_on_oom` give memory back on demand.

**Trade-off.** A ballooned Firecracker sandbox now boots with `--api-sock`
(the balloon is driven over the API), where before only checkpointable ones
did. Firecracker guests get no free page reporting until we confirm the
pinned release accepts the field (verification item 5.5). The balloon is a
request: `freed_bytes` reports its growth, not a measurement.

**Why (simplicity, performance).** Free page reporting is automatic but
depends on the guest kernel (`CONFIG_PAGE_REPORTING`) and gives no explicit
lever when the node is short, which (a) needs. A balloon alone (b) leaves
freed guest pages stranded between episodes. Together they are the plan's
lean. A balloon on a sandbox that can never exceed its reservation (c) only
adds a device and an API socket for nothing to reclaim. Firecracker rejects
unknown config fields and fails the boot, so enabling page reporting there
blind would break older supported releases. Deflating at episode end (not d)
gives the guest its burst room back as soon as the node can afford it, which
is what `memoryMax` promises.

## 32. A memory suspend kills the guest only after its checkpoint is stored (Phase 4)

*Phase 4, implementation.*

**Call.** The worker runs a suspend's capture as a live checkpoint that ends
in a kill: the runtime pauses the guest and writes its memory
(`AfterCheckpoint::Pause`), the parts go into the node content store while
the guest stays paused, and only then is the paused VMM killed and the
allocation torn down with cause `suspended`. Any failure before the kill
resumes the guest and reports the error, so the instance stays `active`.

**Alternatives.** (a) The runtime's `AfterCheckpoint::Stop`, which kills the
VMM right after it writes memory, then ingest. (b) Resume the guest after
the capture and stop it gracefully.

**Trade-off.** The guest stays paused through the content-store ingest, which
hashes every part, so a memory suspend takes seconds longer than the VMM's
write alone. The plan's 1.5 s suspend target for 1 GiB may not hold on slow
disk.

**Why (the user requirement).** "Suspend never kills a workload it failed to
capture." With (a), a full or disabled content store after the kill loses a
workload whose memory was never stored. (b) lets the guest run past the
captured instant, so its disk moves on from the memory image and the restore
resumes a page cache that no longer matches its disk. Hashing on ingest only
once is a tracked follow-up that removes most of the cost.

## 33. The resume handler is the worker's binary polling the identity disk (Phase 4)

*Phase 4, implementation.*

**Call.** A sandbox launched for memory checkpoints gets the worker's binary
at `/legion/legion`, and its init starts `legion guest-resume`, which polls
the identity disk every 200 ms (dropping the block cache first) and refreshes
the guest when it names a new allocation: RNG, clock, hostname, MAC,
identity files, then `onResume`.

**Alternatives.** (a) A vsock control channel the worker uses to tell the
guest it was restored. (b) The handler in POSIX shell inside the generated
init. (c) Rely on VMGenID alone.

**Trade-off.** A guest whose libc cannot run the worker's binary (a musl
image with a glibc worker) restores without the refresh, which the public
doc states. A 200 ms poll means up to that long before the refresh, and a
small idle cost per checkpointable sandbox.

**Why (simplicity, extensibility).** The identity disk already exists for
exactly this (3b reserved it so forks work without hotplug), so polling it
needs no new device and works on both VMMs. (a) is the gated sandbox's
datapath problem again: vsock is part of the saved state and is the reason
gated sandboxes cannot be checkpointed yet. (b) cannot reseed with credit
(`RNDADDENTROPY` is an ioctl) or set the clock precisely, and parsing the
identity JSON in shell is fragile. (c) only covers the RNG, and only on VMMs
that expose it. The handler is a pure function over a `GuestHost` trait, so
it is tested with a fake on any OS.

## 34. The `snapshots` block opts a task in, with a `memory` switch

*Phase 3c, implementation.*

**Call.** Declaring `snapshots { ttl, keep, memory }` on a task opts it into
snapshots. On a `sandbox` with a real VMM it also launches the sandbox ready
for memory checkpoints, unless `memory = false`. Disk snapshots work without
the block. One predicate, `memory_checkpoint_ask`, decides "asks for memory
checkpoints" for both the worker's launch and submit validation.

```nickel
snapshots = { ttl = "7d", keep = 5, memory = false },
```

**Alternatives.** (a) The plan's `{ ttl, keep }` only, with every sandbox on
a real VMM launched checkpointable. (b) A separate `checkpoint = "memory"`
field, like the plan's `suspend.checkpoint`.

**Trade-off.** A field the plan did not list, and a task must be restarted
after adding the block before its first memory snapshot.

**Why (simplicity and security).** A checkpointable Firecracker sandbox has
an API socket into its VMM, and a sandbox that never needs one should not
have one. Gated sandboxes need a way to keep disk snapshots without asking
for memory ones (entry 48).
Phase 4 extends the same predicate with `suspend.checkpoint = "memory"`.

## 35. `SuspendInstance` works without a `suspend` block (Phase 4)

*Phase 4, implementation.*

**Call.** Any keyed instance can be suspended. The block only sets defaults.
Without a `--checkpoint`, the kind is the block's `checkpoint`, else memory
when the sandbox was launched for memory checkpoints and its node can take
one, else disk.

**Alternatives.** Require the block, the way memory snapshots require the
`snapshots` block.

**Trade-off.** A suspend of a task that never declared one can be a disk
suspend where the caller expected memory. The reply and `instance list`
name the kind.

**Why (ease).** A disk suspend of a keyed `exec` instance is useful with no
jobspec change, and a platform driving suspend over the API should not need
a spec edit and a restart of every instance first. Memory still needs the
launch preparation, which is what the block (or `snapshots`) asks for.

## 36. Published checkpoint parts are uploaded unencrypted (Phase 7b)

*Phase 7b, implementation. Plan open question 4 leaned toward encrypting
with the KV keyring as a follow-up.*

**Call.** A publish uploads each memory part as a plain object. The public
doc says so in its Security section and tells operators to restrict the
bucket and turn on server-side encryption.

**Alternatives.** Encrypt each part with a data key from the KV keyring
before upload, and decrypt on fetch.

**Trade-off.** Anyone who can read the bucket can read the guest's memory,
secrets and live tokens included. Bucket policy and provider encryption are
the only protection.

**Why (simplicity).** Envelope encryption of multi-GiB blobs needs a
streaming cipher in both the publisher and the fetcher, a key id in the
manifest, and a rotation and rewrap story for objects that outlive keys,
which is the plan's open question 4 and its own plan. Doing it halfway (no
rewrap) would be worse than saying plainly that it is not done.

## 37. The `suspendedReserve` floor lives under `resourceEnforcement`, needs no cgroups, and applies when a suspend lands (resume guards)

> **Superseded: the floor was removed (PR #15 review).** A worker-level
> floor put a job owner's decision (is a slow resume acceptable?) in operator
> config, gave two knobs for one thing, and sat in a cgroup block it didn't
> need. `suspend.reserve` on the job, with make-room as the safety net, is
> the only control now. If operators need a floor later, it belongs on the
> node pool, not per-worker config. The original call follows.

*Resume guards, implementation.*

**Call.** `resourceEnforcement.suspendedReserve` (a ratio, default 0.0) rides
registration as `config_attrs.suspendedReserve`, unlike the oversubscription
ratio it is advertised whether or not the node can enforce, and the suspend
transaction reads it from the node row once, when the suspend lands. Each
instance is charged `max(job reserve, floor)` per dimension.

```nickel
resourceEnforcement = { suspendedReserve = 0.25 },   # worker config
```

**Alternatives.** (a) A top-level worker field, since it needs no cgroups.
(b) Require `enabled = true` like the oversubscription ratio. (c) Recompute
every held row when a node re-registers with a different floor.

**Trade-off.** Changing the floor does not re-charge instances already
suspended, so a node can run under an old floor until they resume. The field
sits in a block named for cgroups even though it needs none, which the docs
call out.

**Why (simplicity).** It is a capacity knob next to the other capacity knob
(`oversubscribe`), where an operator tuning density looks. Gating it on
enforcement would refuse it on macOS and dev workers for no reason, since it
only changes what the scheduler counts. Re-charging on registration would
put a fleet-wide rewrite on a heartbeat-adjacent path for a rare change.

## 38. The node content store is on by default

*Phase 3a, implementation.*

**Call.** Workers open the store with defaults (20 GiB artifacts, 50 GiB
snapshots, 72h idle TTL, 5 GiB free-disk floor) unless the worker config
sets `content.enabled = false`.

**Alternatives.** Off until configured, as the cache plan's rollout section
suggested.

**Trade-off.** Upgraded workers start using up to 20 GiB of state-dir disk
for artifacts nobody asked to cache.

**Why (ease).** Only checksummed artifacts are stored (call 72), so a hit is
always the right bytes. The budget and free-disk floor bound the cost, and
the snapshot class needs a store to exist on every node before checkpoints
can write to it.

## 39. `weights.locality` defaults to 1.0 the day Phase 7a ships

*Phase 7a, implementation.*

**Call.** `schedulerSettings.weights.locality` defaults to `1.0`, the same
scale as `affinities`, and the term is live for any task with a
digest-pinned artifact.

```nickel
schedulerSettings = { weights = { locality = 0 } }   # opt out
```

**Alternatives.** (a) The plan's "0 until Phase 7 ships", read as "0 until
all of Phase 7 (including 7b) ships". (b) The cache plan's
`artifact_locality: 0.3`.

**Trade-off.** Upgraded clusters change placement for jobs with pinned
artifacts: a warm node now beats an equally scored cold one, and at weight
1.0 locality can outweigh a full spread or bin-pack preference. Operators
who tuned placement around the old score have to set the weight to `0`.

**Why (ease, performance).** Phase 7 is shipping. Holding the default at 0
until 7b would ship the index and the term with nothing using them. The
term is bounded to `0..=1`, so 1.0 means "all the bytes are here is worth
one satisfied affinity", which is easy to reason about, and it is skipped
entirely for tasks with no content set, so ordinary jobs see no change.

## 40. Restore replaces the allocation through the scheduler

*Phase 3c, implementation.*

**Call.** `RestoreInstance` stops the instance's allocation (cause
`restore`), moves it to `pending` with `instance.restore_from` set, and
enqueues placement. The planner pins a restoring instance to its snapshot's
node and checks the manifest's `compat` there, the binder pairs it only with
an allocation on that node, and the dispatch payload carries the manifest.

```text
Failed to place task workspace: instance user-42 restores memory snapshot
snap-3f9a…, which node w-03 cannot run: cpu.model is "6:143:8", the checkpoint
needs "6:106:6"…
```

**Alternatives.** Restore inside the running allocation: the manager sends
the node a `restore` op, and the worker stops the task, restores the volume,
and restarts it under the same allocation id.

**Trade-off.** A restore costs a stop, a placement, and a start instead of a
local restart. The replacement was first handed the old allocation's id
(ids were reused), which the worker had to recognize
(`reconcile_allocations` stops the old task first). Phase 4 made ids
monotonic (see entry 28).

**Why (extensibility and correctness).** Phase 4's resume is exactly this
path from `suspended`, so it inherits the pinning, the compat check, and the
payload. Compat belongs in placement, where it can fail with a diagnostic
before anything runs, rather than inside a worker that has already stopped
the task.

## 41. A failed restore boots cold through the normal restart path

*Phase 3c, implementation.*

**Call.** The worker reports a failed restore and fails the allocation with
the same reason. The manager clears the instance's restore source before it
processes the allocation failure, so the replacement boots cold on the
retained storage.

**Alternatives.** Leave the instance `pending` with the error, or retry the
same restore.

**Trade-off.** The instance comes back, but not in the state the user asked
for, and the volume may or may not have been rolled back depending on which
step failed.

**Why (ease).** It is the plan's `onRestoreFailure = "cold"` default without
the knob, which Phase 4 adds. Retrying would loop forever on a deterministic
failure such as a CPU mismatch.

## 42. A failed publish is retried twice by the manager, then stays local (Phase 7b, revised at integration)

*Phase 7b shipped no manager retry. The integration added a bounded one.*

**Call.** The worker retries each part three times. A failed report then
counts one attempt (`snapshot.publish_attempts`) and schedules the next for
one minute later, then five (`snapshot.publish_retry_at`, unix seconds). The
work query re-sends `op = publish` once the retry is due. After the third
failed attempt the retry time is cleared and the manager stops asking. The
snapshot stays `local` throughout, with the state in `error`:

```text
publish attempt 1 of 3 failed, retrying in 60s: s3: access denied
publish failed after 3 attempts: s3: access denied
```

A failure report that arrives before its retry is due is a replay of one
already counted, and is ignored.

**Alternatives.** (a) No manager retry, as Phase 7b shipped. (b) Re-send on
every heartbeat until it succeeds. (c) A `snapshot publish <id>` command.

**Trade-off.** Two columns and a migration. Three attempts over about six
minutes still leave a longer outage's checkpoint local for good, and there
is still no manual retry. Each attempt emits `snapshot.publish_failed`, and
`legion_checkpoint_publish_total` counts `retrying` apart from `failed`.

**Why (ease).** A store blip or an expiring credential is common and
recovers by itself, and a spread fork waits on its publish, so (a) turned a
short outage into terminated forks. (b) is an endless warning for the
misconfigurations that never recover. A short, durable backoff covers the
first without the second, and the error text tells an agent which case it is
in. The minute gap is longer than a heartbeat, so the worker has dropped the
failed work and runs it fresh. (c) is a reasonable follow-up.

## 43. An auto TAP is `tap = "auto"` plus a required bridge, named like a fork's TAP

**Call.** `config.network = { mode = "tap", tap = "auto", bridge = "br0" }`
has the node create a TAP per instance, named `lgf` plus 12 hex of the
instance id's SHA-256 (`fork::own_tap_name`, the fork naming), attach it to
`bridge`, record it for destroy, and recreate it on the same bridge on
restore. `bridge` is required with `auto` and refused with a fixed TAP. A
free leftover `lgf` device is reused and re-attached. The guest MAC defaults
to `fork_mac(allocation_id)`.

**Alternatives.** (a) A new `mode = "bridge"` with `bridge` alone. (b)
Make gated networking checkpointable instead. (c) Allow `auto` without a
bridge (a standalone TAP the operator routes). (d) Name the device per
allocation.

**Trade-off.** Egress for these sandboxes is whatever the operator puts on
the bridge, not Legion's per-sandbox policy. A cold boot gets a new MAC, so
a guest's DHCP lease changes across cold boots.

**Why (simplicity, extensibility).** The surface reads as "a TAP, but
Legion makes it", next to the existing `tap = "tap0"`, and keeps
`mode`'s two values. (a) would be a third mode meaning the same thing. (b)
needs the vsock datapath to survive a restore, which is a project of its
own. (c) gives a guest a NIC on nothing, which is a silent failure. (d)
would give a resumed instance a different device than its checkpoint
records, and a fork a different name than its later boots. One name per
instance unifies boot, resume, and fork on one code path
(`create_owned_tap`).

```nickel
config = { network = { mode = "tap", tap = "auto", bridge = "br0" } },
```

## 44. A pressure suspend stays local whatever `durability` says, and an idle suspend that cannot publish is skipped (integration)

*Phase 5 and 7b integration.*

**Call.** The `suspend` pressure step's checkpoint is inserted as a `local`
snapshot even when the task sets `durability = "store"`, and is not
published. The idle sweep computes the task's durability like
SuspendInstance does, and when the checkpoint could not be published (a
node-local volume, a worker without a `contentStore`) it skips the instance
with the reason in its debug log instead of suspending.

**Alternatives.** (a) Publish pressure checkpoints too. (b) Idle-suspend to
a local checkpoint when it cannot publish.

**Trade-off.** A sandbox suspended under pressure is node-bound until its
next manual or idle suspend, even with `durability = "store"`. A task
misconfigured for publishing never idle-suspends, and the only signal is
the sweep's skip counter and debug log.

**Why (correctness).** A node short of memory should not also start
gigabyte uploads, and the pressure step acts before the manager hears of
it. (b) would silently hand a `store` task a checkpoint that dies with its
node, which is the degrade the user ruled out.

## 45. A resume that cannot restore is settled by the report or a manager sweep (Phase 4)

*Phase 4, implementation.*

**Call.** Three paths apply `onRestoreFailure`. A failed restore report
settles in the heartbeat that carries it. `StartInstance` on a checkpoint
already known to be unusable (lost with its node, failed) settles at once,
returning `FailedPrecondition` under `fail`. A resume stuck waiting is
settled by a sweep on the manager's maintenance tick: a lost checkpoint
always, and a resume older than `resumeTimeout` under `cold` only.

**Alternatives.** (a) Mark the snapshot `lost` on node *loss* (missed
heartbeats) rather than node removal. (b) Apply `resumeTimeout` under
`fail` too, putting the instance back to `suspended`. (c) A per-resume timer.

**Trade-off.** A checkpoint on a node that is down but not removed is not
`lost`. Under `cold`, `resumeTimeout` covers it. Under `fail`, the resume
waits for the node like any pending placement. With a node-local volume, the
cold boot is still pinned to the volume's node.

**Why (simplicity).** A lost node often comes back, and its checkpoint with
it, so declaring the bytes gone on a missed heartbeat would throw away
resumable state. 3c already marks snapshots `lost` on removal, and the sweep
reuses its tick. (b) would make a capacity wait look like a restore failure,
which the plan's `fail` mode does not describe. (c) adds state that a
failover loses, while the sweep reads it from the rows.

## 46. A fork that cannot restore follows `onRestoreFailure`, and `fail` stops it (Phase 6)

*Phase 6, implementation.*

**Call.** A restore into a new instance (a fork or a `from_snapshot`
create) that fails on the node follows the task's
`suspend.onRestoreFailure`. `cold` (the default) lets the replacement boot
fresh on the instance's own volume, with the reason in `detail`. `fail`
removes the allocation and stops the instance. `instance.restore_failed`
says which (`outcome: cold` or `stopped`).

**Alternatives.** (a) Always boot cold, like a failed in-place rollback
(entry 41). (b) Terminate the fork.

**Trade-off.** `fail` here means `stopped`, not `suspended` as it does for a
resume, so the knob reads slightly differently for forks. And a fork whose
volume clone failed boots cold on an empty volume under `cold`, with only
`detail` and the event to say so.

**Why (simplicity).** Users asked for unsupported or failed paths to be
loud, and a platform that set `onRestoreFailure = "fail"` said it would
rather surface a lost restore than boot something unexpected. A fork has no
checkpoint of its own to stay suspended on, so `stopped` is the nearest
state that keeps the id and storage for a retry. (b) would throw the id
away for what may be a transient TAP or disk problem.

## 47. The checkpoint budget is a placement filter on soft state, for fresh placements of memory-suspend tasks only (resume guards)

*Resume guards, implementation.*

**Call.** Workers report the content store's `snapshots` class (budget, used,
pinned) on every content report, the manager keeps it in the in-memory
content location index, and the scheduler skips a node for a new instance
when `budget - pinned` is smaller than one checkpoint of the task (guest
memory: `config.memory_mib`, else `memoryMax`, else `memory`). Only tasks
whose `suspend` block checkpoints memory are filtered, and only placements
with no restore source and no volume pin. A node with no report has room.
The set of short nodes is read once per task, so a fleet with no full budget
pays nothing per candidate.

```text
Failed to place task workspace: 4 eligible node(s), but every one's snapshots content budget is full: less than 1024 MiB free for this task's memory checkpoint (suspend). ...
```

**Alternatives.** (a) Persist the budget on the node row (replicated). (b) A
score term instead of a filter. (c) Count checkpoints the plan itself will
create (every new instance eventually suspends). (d) Filter on `used` rather
than `pinned`.

**Trade-off.** After a manager restart or failover the filter is open until
nodes report again, and a plan placing many instances can overfill a budget
that had room for one. The estimate is the guest's full memory, which
overstates a sparse checkpoint.

**Why (performance, simplicity).** Budget headroom changes with every
checkpoint, so (a) would be a raft write per suspend per node for a
best-effort guard. This is the same posture as the location index (entry 12). A filter, unlike a score, gives the explicit
diagnostic the "fail loudly" rule asks for. Unpinned content is evictable LRU,
so `used` would refuse nodes that have room.

## 48. Memory checkpoints on gated sandboxes fail loudly, at submit where possible

*Phase 3c, implementation.*

**Call.** Job validation (`job run`, `job validate`, `job plan`) refuses a
`sandbox` task that declares gated networking and asks for memory checkpoints.
When gating comes from a stored policy instead, `CreateSnapshot --memory`
fails with `FailedPrecondition` and the same hint.

```text
Error: task "workspace" asks for memory checkpoints, but sandbox memory checkpoints don't support gated networking yet
Hint: set snapshots.memory = false for disk-only snapshots, or drop network gating for this task
```

**Alternatives.** 3b's behavior alone: launch without checkpoint support and
log a warning, so the failure appears only when someone asks for a memory
snapshot (entry 55).

**Trade-off.** A jobspec that validated before (a gated sandbox with a
`snapshots` block) is now refused until it sets `memory = false`. The
block is new in this phase, so no shipped spec breaks.

**Why (ease).** A limitation discovered at submit costs one edit. Discovered
at snapshot time, it costs the snapshot the user needed. Hints now ride the
status message on a `Hint:` line, which the CLI lifts into the envelope's
`hint` field.

## 49. A store suspend on a worker without a `contentStore` is refused at call time, through a capability (Phase 7b)

*Phase 7b, implementation. The brief asked to reject at submit where
possible.*

**Call.** A worker with a usable `contentStore` advertises a second
registration capability, `content-publish`. `SuspendInstance` with
`durability = "store"`, and `snapshot create --publish`, refuse a node
without it with `FailedPrecondition`, before anything is captured:

```text
instance user-42 runs on node w-03, whose worker has no contentStore to publish to (it does not advertise the `content-publish` capability)
Hint: add a contentStore { url } block to that worker's config (docs/artifacts.md, "Publishing content"), or keep suspend.durability = "local"
```

**Alternatives.** Refuse at submit when no node in the cluster can publish.
Or add an implicit placement constraint so `store` tasks only land on
publishing nodes.

**Trade-off.** A job is accepted, and instances run, on nodes that could
never publish. The refusal comes at the first suspend, not at submit.

**Why (simplicity).** Submit cannot know where an instance will run, and
"some node can publish" says nothing about the node it lands on. An implicit
constraint would make a store task unschedulable on a fleet that simply has
not configured publishing yet, which is a worse surprise than a clear error
on suspend. The capability check is also what keeps a mixed-version fleet
safe: an old worker never advertises it.

## 50. Published restores go only to workers that advertise `checkpoint-fetch` (integration)

*Phase 7b follow-up, done at integration.*

**Call.** Workers now advertise a third capability, `checkpoint-fetch`,
meaning "I can restore a published snapshot I never held". Placement of a
floating published restore (and spread forks, and creates from a published
snapshot) keeps only candidates that advertise it, and `snapshot warm` warms
only those nodes. A fleet with none fails the placement naming the
capability.

**Alternatives.** (a) Compare `buildVersion` against the release that
shipped the fetch. (b) Reuse `checkpoint`, which every worker since Phase 3c
advertises.

**Trade-off.** One more capability string on registration, and a fleet
must re-register (restart its workers) before published restores can
place on it after an upgrade.

**Why (simplicity).** (b) is the bug: a Phase 3c-6 worker advertises
`checkpoint` but cannot fetch parts, so its restore fails after placement.
(a) ties placement to version arithmetic. A capability says exactly what the
manager needs to know, and matches how `content-publish` already works.

## 51. Later-phase `suspend` fields are refused at submit (Phase 4)

*Phase 4, implementation. Phase 5 shipped `idle` and `onPressure` and kept
the rule for what is still later: the `hint` idle signal (Phase 8).*

**Call.** `idle`, `onPressure`, and `durability = "store"` parse (so the
error can be specific) and then fail submit with a hint naming the phase that
ships them. `onResume` on a task that resumes cold (any runtime but
`sandbox`, or `checkpoint = "disk"`) fails too. Since Phase 5, `idle.signals
= ["hint"]` fails the same way, and its hint points at `connections`:

```text
Error: task "workspace": suspend.idle.signals "hint" is not implemented yet: the workload's own busy/idle signal ships in suspend plan Phase 8
Hint: remove "hint". To keep an instance awake through a quiet call, add "connections" (any open ingress connection counts as activity)
```

```text
Error: task "workspace": suspend.durability = "store" is not available yet: publishing checkpoints to a content store ships in suspend plan Phase 7b
Hint: remove durability, or set durability = "local" (the checkpoint stays on the node that took it)
```

**Alternatives.** Accept and ignore them, or let `deny_unknown_fields` reject
them as unknown.

**Trade-off.** Specs written for a later release fail on this one until the
fields are removed.

**Why (the user requirement).** Unsupported combinations fail loudly. An
unknown-field error would tell the author the field does not exist, which is
wrong. It is planned.

## 52. On a guest's own NIC, only bytes the guest sends count

**Call.** The `network` signal on a TAP sandbox reads the TAP's `rx_bytes`
(what the guest sent), and on an Apple container `networkTxBytes`. A rise
between two samples is activity. What the guest receives does not count.

**Alternatives.** Count both directions, as the brief first suggested, or
both with a noise floor.

**Trade-off.** A guest that only receives (a stream it never acknowledges)
looks idle. Every real TCP exchange sends acknowledgements, so that is a
non-TCP edge case.

**Why (correctness).** Measured on this Mac, an idle Apple container's
receive counter rose by about 1 KB a minute from broadcast noise on the
vmnet bridge while its transmit counter held still. A Linux bridge carries
the same ARP and multicast noise. Counting it would keep every sandbox awake
forever, which is the failure the idle policy exists to avoid, and a floor
would be a tuning knob nobody can pick well.

## 53. `connections` on a sandbox that is not gated is refused, not implemented with conntrack

**Call.** Submit refuses `suspend.idle.signals = [..., "connections"]` on a
sandbox without gated networking, with a hint (raise `after`, or suspend
from the platform).

**Alternatives.** Count conntrack entries for the guest's IP.

**Trade-off.** No signal keeps an auto-TAP sandbox awake through a quiet
model call. Platforms raise `after` or suspend themselves.

**Why (simplicity).** Legion does not know a TAP guest's IP (DHCP on the
operator's bridge assigns it), conntrack only sees bridged traffic with
`br_netfilter` loaded, and an outbound connection the guest holds open is
the case that matters, which conntrack would count only while the entry
lives. Accepting the signal and having it never fire is the silent failure
the "fail loudly" rule forbids.

## 54. `exec` on a sandbox is refused when pinned to a VMM, and a warning otherwise

**Call.** An explicitly listed `exec` on a sandbox with `hypervisor =
"firecracker"` or `"cloud-hypervisor"` is refused at submit. With `auto` (or
from the default list) it is a warning on `job run` and `job validate`.

**Alternatives.** Refuse it on every sandbox, or say nothing and document
it.

**Trade-off.** The default signal list on a sandbox warns on every submit
until the author lists signals explicitly.

**Why (simplicity).** With `auto`, the same spec runs on a Mac in dev mode,
where `exec` works, so refusing it would break the macOS dev loop. Only a
pinned VMM is certain never to fire it.

## 55. Gated sandboxes are not memory-checkpointable yet

*Phase 3b, implementation.*

**Call.** A sandbox with `network.mode = "gated"` launches without
checkpoint support even when asked, with a warning, and a checkpoint of one
fails with an error naming the reason. (Phase 3c also refuses the combination
at submit, entry 48.)

**Alternatives.** Route the vsock socket through the device-link directory
too.

**Trade-off.** Agent sandboxes on gated networking cannot suspend with
memory until this is lifted.

**Why.** The vsock UDS path is baked into the saved state, and the host
datapath listens on per-port sockets derived from it. Relinking those is
possible but deserves its own tests against a real guest agent, which the
Phase 3b slice could not run.

## 56. The response escalates one step at a time, each step once per allocation per episode (Phase 2)

**Call.** The worker checks the node every second. Under pressure, and once
`settle` has passed since its last action, it takes the first step in run
order that has a candidate it has not already acted on in this episode. It
acts on that step's candidates in order until their expected freed bytes
cover the shortfall below `freeBelow` (at least one when only the stall
trigger fired), then waits `settle` again. An episode ends when neither
trigger fires, which resets the record.

**Alternatives.** (a) A level counter that advances after every round,
whether or not the current step had more to do. (b) Act on every candidate
of a step at once. (c) Re-run `reclaim` on every round.

**Trade-off.** On a kernel before 5.19, `memory.reclaim` fails, so the
response skips straight to `evict-bursting` with nothing reclaimed. A spike
faster than one second can still reach the OOM killer first. Both are in the
public doc's limits.

**Why (simplicity, predictability).** "Each step runs only until pressure
clears" needs a stopping rule, and "cover the shortfall" is the one an
operator can check against the numbers in the event. (a) would stop a
bursting task while another bursting task in a lower band was still
untouched. (b) would stop five tasks to free memory one would have freed.
(c) loops forever on anonymous memory the kernel can never reclaim, so the
response would never escalate.

## 57. `evict-bursting` can stop a bursting `critical` task, `evict-reserved` never touches `critical` (Phase 2)

**Call.** Read the §6.4 table literally: `evict-bursting` acts on any
allocation above its reservation, `critical` included (last, by band order),
and `evict-reserved` acts on anyone below `critical`. Node-pinned storage is
never evicted by either step, but still gives memory back in `reclaim`.

**Alternatives.** Exempt `critical` from both eviction steps.

**Trade-off.** A `critical` task that bursts can be stopped for pressure.

**Why (predictability).** The guarantee the plan offers is about the
reservation: within it you are left alone (at ratio 1.0, always). Exempting
`critical` from `evict-bursting` would let a bursting critical task push the
node to the OOM killer, which then picks a victim with no regard for bands.
A critical task that needs more memory should reserve it.

## 58. Evicted allocations are held out of relaunch, and their replacement may land on the same node (Phase 2)

**Call.** The worker stops an evicted task through the ordinary graceful stop
with `cause = node_pressure` written into its stop notice, and holds the
allocation id out of relaunch until the manager drops it from the desired
set. The manager handles `pressure_actions` before it builds the desired set,
removes the allocation with `StopCause::NodePressure`, and queues a
`job_reschedule` eval. Nothing steers the replacement away from the node.

**Alternatives.** (a) A transient anti-affinity for the evicting node. (b)
Let the manager do the eviction from the report.

**Trade-off.** A replacement can be placed back on the node that just
evicted it if that node is still the best fit by reservations. The public doc
names it.

**Why (simplicity).** (a) needs the scheduler to remember pressure, which is
new state with its own expiry rules, and the plan's §4 keeps measured usage
out of placement for now. (b) would wait on a round trip while the node runs
out of memory (entry 9). Holding the id prevents the obvious race, where a
heartbeat reply sent before the report arrived would restart the task the
worker just stopped.

## 59. Idle `network` and `connections` count what the datapath fronts (Phase 5)

*Phase 5, implementation.*

**Call.** `network` is any bytes through the node datapath: the egress
edges Phase 1 already folded into activity, plus every ingress connection the
datapath fronts, wrapped in a counting stream that stamps a per-allocation
last-bytes time. `connections` is the count of those ingress connections that
are open. A port the task binds directly on the host, with no ingress chain,
is invisible to both.

**Alternatives.** (a) Read per-allocation socket tables (`/proc/<pid>/net/tcp`
in the task's network namespace, or conntrack) for open connections and byte
counters. (b) eBPF counters per cgroup.

**Trade-off.** A task that serves on a raw host port gets no help from
`network` or `connections`: only CPU and egress through the datapath count
for it. The public limitations say so.

**Why (simplicity, performance).** The datapath already owns the bytes for
every runtime, sandbox guests included, which `/proc` scanning (a) does not
see inside a VM. The counter is one relaxed atomic store per read or write on
a slot fetched once per connection, so the hot path takes no lock. (b) is a
new kernel dependency for a signal the datapath gives for free.

## 60. A partial hold is the suspended allocation row with its resources cut to the share (resume guards)

*Resume guards, implementation. Generalizes the next entry.*

**Call.** On suspend, the store computes `max(job reserve, node floor)` and
either deletes the row (nothing held), keeps it as is with status `suspended`
(a full hold, the Phase 4 behavior), or keeps it with status `suspended`, its
`resources` rewritten to the held share, and `network` cleared. Every other
dimension is written as an explicit zero (`allocations = 0`, `cores = 0`,
device lists empty) so `taskResourceDefaults` cannot refill it. The node and
job-config epochs are bumped because the row's usage changed under the same
id.

**Alternatives.** (a) A separate per-node suspended-charge aggregate in the
usage cache. (b) A second "hold" row beside the allocation.

**Trade-off.** The row no longer describes what the task asked for, so a
reader of the allocation table sees reduced resources on a `suspended` row
(the CLI marks it `(suspended, held)`). The held share is computed once and
never re-derived, so later changes to the job's `reserve` or the node floor
do not apply to it.

**Why (simplicity, performance).** The fit check, the read model's per-node
pools, the `node_alloc_usage` cache, and the plan-apply recheck all derive
capacity from allocation rows, so a row with smaller numbers is honored
everywhere with no new code on the scheduler hot path. (a) would need a
second input threaded through every one of those paths and its own cache
invalidation. Resume, job stop, and job update already delete held rows.

## 61. `reserve = true` keeps the allocation row with status `suspended` (Phase 4)

*Phase 4, implementation. Generalized to partial holds by entry 60.*

**Call.** With `suspend.reserve`, the suspend transaction keeps the
allocation row and sets its status to `suspended`. The heartbeat never
dispatches a `suspended` row, the scheduler counts suspended instances that
hold a row as desired (so it neither culls nor re-places the hold), and a
resume deletes the row and places the restore in one transaction.

**Alternatives.** (a) A reservation ledger the scheduler adds to node usage.
(b) Keep the row `placed` and filter on the instance state everywhere.

**Trade-off.** A job update or job stop removes held rows like any other
allocation. The instance stays suspended, now holding nothing, and the
public doc says so. Rollouts count live instances only, so a hold on an old
config is replaced rather than carried over.

**Why (simplicity, performance).** Node usage is derived from allocation
rows in several places (the per-node memo, the plan-apply recheck, `node
usage`), so a row is the one reservation every path already honors, at no
per-candidate cost. Completed batch allocations already use a
never-dispatched status the same way. (a) would touch the scheduler hot path
and every capacity reader. (b) puts an instance lookup on the heartbeat path
for every allocation.

## 62. Legion never deletes published parts from the store (Phase 7b)

*Phase 7b, implementation.*

**Call.** Deleting a published snapshot removes its row and its node's local
copy. The objects under `<contentStore.url>/sha256/` stay. The public doc
tells operators to expire them with bucket lifecycle rules.

**Alternatives.** Delete each part from the store when the snapshot goes.
Or keep a reference count of snapshots per digest in the state store and
delete at zero.

**Trade-off.** Storage grows until a lifecycle rule trims it, and a rule
that is too aggressive can remove a part a published snapshot still needs,
which then fails its restore with a fetch error.

**Why (simplicity, correctness).** Parts are keyed by digest, so two
snapshots (or two forks of one) share objects. Deleting on snapshot delete
would break the other snapshot. A reference count would need to be exact
across regions, buckets, and manager failovers to be safe to delete on,
which is a garbage collector, not a publish feature.

## 63. `snapshot` is its own ACL resource, and restore needs `snapshot:read` plus `job:write`

*Phase 3c, implementation.*

**Call.** `snapshot:write` covers create and delete, `snapshot:read` covers
list, get, and restore. Restore also needs `job:write` on the job, since it
replaces an allocation.

**Alternatives.** Put snapshots under `job`, or make restore `snapshot:write`.

**Trade-off.** One more resource in the vocabulary, and a restore checks two.

**Why (security).** A memory snapshot is a copy of the workload's secrets, so
`job:write` (redeploy rights) must not imply it. Restoring reads a snapshot
into a workload the caller can already redeploy, so it needs no more than
read on the snapshot.

## 64. Fork needs `snapshot:write` for either kind, and `from_snapshot` needs `snapshot:read`, both checked before any lookup (Phase 6)

*Phase 6, implementation.*

**Call.** ForkInstance needs `job:write` and `snapshot:write` on the origin's
job whatever the checkpoint kind. `CreateInstance(s)` with `from_snapshot`
needs `snapshot:read` on the job on top of `job:write`. The coarse grants are
checked before the instance or snapshot is looked up.

**Alternatives.** (a) Require `snapshot:write` only for a memory fork, as
suspend does. (b) Check `snapshot:read` only once the snapshot is resolved.

**Trade-off.** A platform that only ever takes disk forks must still grant
`snapshot:write`.

**Why (simplicity).** A suspend's disk checkpoint never leaves the
instance, but every fork, disk or memory, hands a copy of the origin's data
to a new instance, so it is a read of the origin in the sense the
`snapshot` resource exists to gate. (b) would let a caller without the
grant probe which snapshot names exist through NotFound versus
PermissionDenied, which AGENTS.md's "check before you fetch" rule forbids.

## 65. Sandbox VMMs join the worker cgroup, with a ceiling of guest plus overhead (Phase 1)

**Call.** `sandbox` returns `ResourceEnforcement::WorkerCgroup`. The VMM
process joins the allocation's cgroup before it execs, and a new trait
method, `TaskPlugin::enforced_memory_limit_bytes`, lets the runtime replace
the ceiling with `config.memory_mib + sandbox.vmmOverheadMiB`. Dev-mode
backends keep the declared `resources.memory` and hand the same cgroup to
their OCI runtime.

```nickel
sandbox = { vmmOverheadMiB = 64 }   # worker config
```

**Alternatives.** (a) `RuntimeApplied`, the shape the old TODO item
proposed: size the guest from the assignment and pin vCPU threads, with no
cgroup. (b) Use `resources.memory` as the cgroup ceiling now, ahead of
Phase 2's guest-size derivation.

**Trade-off.** Until Phase 2 (entry 26), the ceiling follows the guest size
and not the reservation, so a spec that reserves less than its guest still
overcommits the node, now visibly in `node usage`. Sandbox tasks also pick
up the governed-runtime submit checks (a fractional `cores`, or `cores` with
`cpu`, is now rejected).

**Why (simplicity).** The VMM is a host process, so the cgroup is the only
place guest RAM can be both limited and measured, which every later phase
needs. (a) would limit nothing the host kernel can see. (b) would OOM-kill
every existing sandbox whose guest is bigger than its reservation, which is
most of them today.

## 66. Oversubscription scales capacity at its source, and the aggregate cache keeps a format version instead of the ratio (Phase 2)

**Call.** The worker reports `oversubscribe` in its registration
`config_attrs`, and `ResourcePool::from_node` multiplies memory and CPU MHz
capacity after `reservedResources`. The cached `NodeUsage` aggregate's
policy fingerprint does **not** gain the ratio, which deviates from §6.3 and
the Phase 2 brief. It gains `"aggregate": 2` instead, because `memoryMax` is
now excluded from the aggregate.

**Alternatives.** Put the ratio in the policy fingerprint as the plan says.

**Trade-off.** One cache rebuild per node after the upgrade (the version
bump), and none when a ratio changes.

**Why (correctness, performance).** The cached aggregate holds allocation
demand only. Capacity, ratio included, is read from the node row on every
pool build, both in the read model's reprojection (which runs on every node
upsert) and in the apply transaction. So a ratio change already reaches every
pool, and a test proves it
(`a_ratio_change_reaches_a_pool_built_from_a_cached_aggregate`). The policy
fingerprint is also manager-wide, while ratios are per node, so the plan's
version could not have keyed it anyway. The real staleness risk was
`memoryMax` itself: before this phase it would have been summed as a custom
resource dimension, so any aggregate cached with one must not be served.

## 67. Only a node that can enforce advertises a ratio, and the manager rejects one without enforcement (Phase 2)

**Call.** `oversubscribe` and `memoryPressure` above defaults with
`resourceEnforcement.enabled = false` fail when the manager loads the worker
config, as does a ratio below 1.0 or an unknown step name. A worker with the
ratio but no cgroup v2 registers at physical capacity and logs a warning.

**Alternatives.** Accept and warn, as `resources` does on ungoverned
runtimes.

**Trade-off.** A config error stops the manager at startup instead of
degrading one node.

**Why (simplicity).** The plan nests the ratio under enforcement so that the
misconfiguration is "impossible to write rather than a warning to ignore."
The worker-side check covers the one case the manager cannot see: a node
whose kernel cannot do what its worker type promises.

## 68. The checkpoint channel re-sends work until applied, and the worker replays its reports

*Phase 3c, implementation.*

**Call.** Outstanding snapshot work is the `snapshot` row itself: every
heartbeat reply carries a `CheckpointRequest` for each of the node's rows in
`capturing` or `deleting`, until the manager has applied the node's
`SnapshotReport`. The worker runs each `(snapshot, op)` once, keeps its last
report, and re-sends that report whenever the request comes back. A restore's
outcome rides the same reports, re-sent while the manager still attaches the
restore payload. Report application forwards to the scheduler-lane leader
like volume reports do.

**Alternatives.** (a) A manager-to-worker push RPC (`PostWork`) with an ack.
(b) One-shot delivery in the heartbeat reply plus a worker-side queue the
manager acks explicitly.

**Trade-off.** One indexed lookup per heartbeat on every node, and a
worker-side map of finished work that is pruned only when the manager stops
asking.

**Why (simplicity and extensibility).** It reuses the reclaim pattern volume
destroys already prove: no new transport, no ack protocol, and every failure
(a lost heartbeat, a worker restart, a report that landed on a follower) is
the same retry. Phase 4's suspend is the same request with `then = stop` and
`purpose = suspend`, so it needs no second channel.

## 69. Publishing is a checkpoint-channel op sent after the capture lands (Phase 7b)

*Phase 7b, implementation. Follows the plan's §6.8 "enters `suspended` when
the local write finishes".*

**Call.** The capture report applies the suspend as it always did (snapshot
`local`, instance `suspended`, reservation freed). A `local` row with
`durability = 'store'` is then publish work for its node, so the next
heartbeat carries `CheckpointRequest { op = "publish", manifest_json }`.
The node uploads and reports the manifest with each part's `url`, which
moves the row to `published`.

**Alternatives.** Have the worker publish on its own right after the
capture, in the same thread, and report both in one go.

**Trade-off.** One extra heartbeat (2 seconds by default) between the
suspend and the upload's start, during which the checkpoint is local and a
resume is pinned to its node.

**Why (simplicity).** The channel already gives re-sends, worker-side
deduplication, report replay, and the delete-waits-for-in-flight rule. A
worker-initiated upload would need its own durable "I owe a publish" record
to survive a worker restart between capture and upload. Here, a restart
simply gets the request again.

## 70. Memory-checkpoint compat is exact match

*Phase 3b, implementation.*

**Call.** A node can restore a memory checkpoint only when `cpu.vendor`,
`cpu.model`, the `cpu.features` digest, and the VMM version all equal the
recorded values. `compat_matches` names the first attribute that differs.

**Alternatives.** Firecracker CPU templates or a feature-subset check, which
let a checkpoint resume on any CPU with at least the recorded features.

**Trade-off.** A mixed-generation fleet sees fewer restore candidates, and a
VMM point upgrade strands every local checkpoint on that node.

**Why (correctness first).** A guest resumed on a CPU missing a feature it
probed at boot crashes or corrupts data, and both VMMs change their snapshot
format between releases. Exact match is the safe floor, and widening it is
an additive change to one function.

## 71. A live memory snapshot keeps the guest paused across the volume snapshot

*Phase 3c, implementation.*

**Call.** `AfterCheckpoint` gains `Pause`, and `TaskPlugin` gains
`resume_after_checkpoint`. For a live memory snapshot the worker asks the
runtime to capture and stay paused, snapshots the instance volume, then
resumes the guest, on success and on failure alike.

**Alternatives.** 3b's shape: the runtime resumes the guest right after the
memory capture, and the volume half is crash-consistent.

**Trade-off.** The guest's pause grows by the volume snapshot (a reflink is
instant, a sparse copy of a large image is not), and the trait grows a
method.

**Why (correctness).** A restored guest's page cache and journal describe the
disk at capture. A volume copied after the guest moved on is silent
corruption on a block volume, the same reason 3b copies the root disk while
paused (entry 98).

## 72. The node content store caches only digest-pinned single files

*Phase 3a, implementation.*

**Call.** The stager consults the node content store only for an artifact
with a `checksum` on an `http(s)`, `s3`, or `file` source. Everything else
(git trees, `file://` directories, OCI images, ORAS layers, unchecksummed
URLs) stages from origin exactly as before. The store's `ttl` is idle
expiry of unpinned content, not revalidation of mutable URLs, and there is
no jobspec `ttl` field yet.

```nickel
artifacts = [
  { source = "https://releases.acme.io/tool-1.4.tar.gz",
    checksum = "sha256:9f86…" },   # served from the node store after the first fetch
  "https://releases.acme.io/nightly/tool.tar.gz",  # always fetched
]
```

**Alternatives.** (a) The cache plan's full Phase 1: a mutable-source key
(hash of source and media type) with TTL revalidation through
`ArtifactFetcher::resolve()`, plus a per-artifact `ttl` field. (b) Cache
trees and OCI layouts as directories keyed by their manifest digest.

**Trade-off.** Unchecksummed artifacts, container images, and git sources
still refetch per allocation, so image desugaring stays off by default and
the cache plan's Phase 1 is only partly done (TODO.md lists the rest).

**Why (simplicity and correctness).** A checksum is the one case where a
hit is provably the right bytes. Caching a plain URL for 72 hours by default
would silently change what an existing job receives after a re-publish, and
`resolve()` returns a manifest digest or commit SHA that cannot be compared
with a file's sha256. Trees have no byte digest to verify a hit against.
This slice gives the acceptance case (a digest-pinned artifact on the second
allocation) and the snapshot API, and leaves the semantics questions to a
change that can settle them.

## 73. The cross-node restore fetch rides the staging thread, through the artifact stager (Phase 7b)

*Phase 7b, implementation.*

**Call.** When an allocation restores from a manifest with parts its node
lacks, `begin_task_allocation` routes it through the staging path even if
the task has no artifacts. The staging thread calls
`ArtifactStager::fetch_into_store` for each missing part
(`published_artifact(url, digest)`), which uses the stager's fetchers,
allowed schemes, credentials, parallelism limit, retries, deadline, and
single-flight, verifies the digest, and imports the part into the node
content store pinned to the allocation. The pin is released once the start
(and so the restore) has run. The allocation's health reads `fetching
snapshot` meanwhile.

**Alternatives.** Fetch inside `restore_from_snapshot`, under the task
manager lock. Or stage the parts as ordinary artifacts into the task
directory and import them from there.

**Trade-off.** Fetches obey `artifacts.fetch` settings meant for artifacts:
a worker whose `allowedSchemes` leaves out `s3` cannot restore a published
snapshot, and `timeout` (600 s by default) bounds a multi-GiB fetch.

**Why (performance, simplicity).** A fetch under the lock would stall the
node's heartbeats for the whole download. Staging into the task directory
would copy every byte twice. Reusing the stager is what "through the normal
stager" asked for, and keeps one fetch path to secure and tune.

## 74. Suspend is keyed-only in v1

**Call.** The `suspend` block is rejected on anything but keyed stateful
tasks.

**Alternatives.** Allow it on slot instances, treating a suspended slot as a
temporarily missing replica.

**Trade-off.** Stateful slot pools (a sharded cache, say) can't park
replicas, and the feature reads narrower than "generic."

**Why (simplicity).** Slot counts, ordered rollouts, and
`disruption.minAvailable` all assume a slot is either live or being
replaced. Teaching them a third answer is real work with no concrete user
yet. Keyed instances are where sandboxes live.

## 75. Preemption does not suspend, even for tasks that set `onPressure` (Phase 5)

*Phase 5, implementation.*

**Call.** `suspend.onPressure` covers the node's memory pressure response
only. A placement that preempts an opted-in instance still stops it with
grace and requeues it, as before.

**Alternatives.** Suspend preemption victims that opted in.

**Trade-off.** A preempted opted-in sandbox loses its in-memory state where a
suspend would have kept it.

**Why (simplicity).** Preemption frees capacity for another placement in the
same plan, and a suspend's capture runs on the node over seconds, after the
plan commits. Waiting on it would hold the preemptor's placement on a worker
round trip, and a failed capture would have to undo a committed plan. The
pressure response has no such coupling: the worker acts and reports.

## 76. Restore is keyed-only, in place, from the instance's own snapshots

*Phase 3c, implementation.*

**Call.** `RestoreInstance` takes a keyed instance and one of its own
snapshots. Slot instances can be snapshotted but not restored.

**Alternatives.** Restore any instance, or restore one instance's snapshot
into another.

**Trade-off.** No slot rollback, and no clone-by-restore.

**Why (simplicity).** Slot lifecycles belong to the scheduler (a rollout could
replace the restored allocation at any moment), and restoring another
instance's snapshot is fork, which needs Phase 6's identity work.

*Phase 6.* Restoring another instance's snapshot shipped as a new instance,
not a rollback: `instance fork` and `instance create --from-snapshot`.
`RestoreInstance` stays in place and points at both in its error.

## 77. Fork ids are `<origin>-f<n>` with the lowest never-used `n`, even under `idSource = "caller"` (Phase 6)

*Phase 6, implementation.*

**Call.** The manager names forks `<origin>-f1`, `-f2`, ..., skipping every
id that has ever existed in any job (tombstones included), and mints an id
when the suffixed one would pass the 128-character limit. It derives ids
even when the task declares `stateful.idSource = "caller"`, and there, an
origin id too long to suffix is refused (`InvalidArgument`, with a hint to
create from a snapshot) instead of given a minted id. Verified at
integration (`forks_of_a_caller_named_task_get_derived_ids`).

```console
$ legion instance fork run-17 --count 2 -j
{"ok":true,"data":{"instances":["run-17-f4","run-17-f5"], ...}}
```

**Alternatives.** (a) Let the caller pass fork ids. (b) Server-minted random
ids, as `CreateInstances --count` does. (c) Refuse fork on `idSource =
"caller"` tasks.

**Trade-off.** Fork numbering is global per origin, not per call, so a
second fork of `run-17` does not start at `-f1`. A platform that wants its
own ids has to create from a snapshot instead.

**Why (simplicity).** The plan's surface (§6.9) shows derived ids, and they
make lineage readable in `instance list` without a lineage column. The
"lowest never-used" rule keeps the never-recycle guarantee. `idSource =
"caller"` exists so a platform's ids map to its own users, and a fork's id
is derived from such an id, so it stays in the platform's namespace rather
than inventing a random one. (a) is a reasonable follow-up if a platform
asks.

## 78. `snapshot create --publish` is only for snapshots made of content parts (Phase 7b)

*Phase 7b, implementation. The brief asked for manual durability "if
cheap".*

**Call.** `CreateSnapshot` takes `publish`, which gives the row `durability
= store`. It is refused for a disk snapshot and for an instance with a
node-local volume, so in practice it publishes memory snapshots of sandboxes
without `stateful.storage`. The worker refuses again if a manifest it is
asked to publish carries volume refs.

**Alternatives.** Publish only the memory parts of any memory snapshot and
call it published while its volume half stays on the node.

**Trade-off.** Manual publishing covers a narrow shape today.

**Why (correctness).** "Published" has to mean "restorable anywhere".
Publishing half a snapshot would let placement float a restore whose volume
snapshot exists on one node only.

## 79. A job stop leaves suspended instances suspended (Phase 4)

*Phase 4, implementation.*

**Call.** A job stop moves live keyed instances to `stopped` and skips
`suspended` ones, which keep their checkpoint. `instance stop` on a suspended
instance discards the checkpoint, and `instance destroy` does too.

**Alternatives.** Treat suspended like live and stop it with the job,
discarding the checkpoint.

**Trade-off.** A stopped job can still hold suspend checkpoints on its nodes,
and `start` on one fails while the job is stopped ("run the job first").

**Why (simplicity).** A suspended instance holds nothing a job stop needs
to release, and a stop, then a run, must not silently cost a user their
parked workspace. An explicit `instance stop` is how to say "I don't want
the checkpoint".

## 80. A snapshot captures the instance volume only

*Phase 3c, implementation.*

**Call.** Disk and memory snapshots copy the instance's `stateful.storage`
volume and nothing else. Shared `volumes = [...]` claims are left alone.

**Alternatives.** Snapshot every mounted volume whose driver supports it.

**Trade-off.** A sandbox that attaches a block claim resumes from a memory
snapshot with that claim's current contents (TODO.md tracks refusing it).

**Why (simplicity).** Claims belong to the task, not the instance, and are
often shared or read-only (`host` model directories). Rolling one back for one
instance would roll it back under every other instance using it.

## 81. Apple dev containers are named after their node, and run with no `--` before the command

**Call.** A dev-mode sandbox on Apple `container` is named
`legion-sbx-<node>-<allocation>`, and its launch passes the command right
after the image.

**Alternatives.** Keep `legion-sbx-<allocation>`, and keep the `--`
separator.

**Trade-off.** Names are longer, and a command whose first token looks like
a flag is passed through as Apple `container` 1.1 parses it.

**Why (correctness).** Two local clusters both number allocations from 1,
and a launch removes any stale container with its name, so one cluster
removed the other's running sandbox. Apple `container` 1.1 runs `--` as the
executable ("failed to find target executable --"), so no dev sandbox could
start at all. The container runtime's Apple launch already dropped it.

## 82. A suspend leaves the instance volume in place (Phase 4)

*Phase 4, implementation. Applies the plan's §6.5 note.*

**Call.** A suspend never copies or snapshots the instance volume, for
either kind. Its manifest has no volume refs, the volume stays where it is on
the node, and the resume restores onto it. So the volume driver needs no
`snapshot` op to suspend, unlike a snapshot.

**Alternatives.** Snapshot the volume on suspend like a memory snapshot does.

**Trade-off.** Anything that changes the volume while the instance is
suspended (an operator, a `volume delete --force`) is what the resume sees,
and a memory resume then has a page cache that disagrees with it. Nothing in
Legion writes to a suspended instance's volume.

**Why (performance, ease).** A copy costs a full volume's write on every
suspend for a state nobody else touches, and requiring the `snapshot` op
would keep `exec` and `container` tasks on drivers without it from
suspending at all.

## 83. A fork checkpoint is spent once no instance restores from it (Phase 6)

*Phase 6, implementation. Extended to published fork checkpoints at
integration.*

**Call.** A `purpose = fork` snapshot is discarded as soon as the last
instance restoring from it reports (success or failure), or by the
maintenance sweep once every fork was destroyed first. A published fork
checkpoint (a spread fork) is spent the same way: its node's copy is
reclaimed, and its parts stay in the content store, which Legion never
deletes from. It is never subject to `snapshots.ttl` or `keep`, and it
cannot be named or used to seed `from_snapshot` creates.

**Alternatives.** (a) Keep fork checkpoints like manual snapshots, under
`ttl` and `keep`. (b) Keep them until the origin is destroyed.

**Trade-off.** A fork cannot be repeated from the same instant. A platform
that wants that takes a manual snapshot and creates from it. A spread fork
leaves its published parts in the bucket until a lifecycle rule removes
them.

**Why (performance, simplicity).** A memory fork checkpoint is the guest's
whole RAM pinned on the node, and nobody asked to keep it. (a) would leave
gigabytes per fork call behind until a `ttl` the task may not set. Once a
Firecracker fork is running, it maps its own hard link to the memory part,
so reclaiming the content-store entry does not disturb it. Manual snapshots
already cover "keep a starting point".

## 84. A new instance stopped before it restored keeps its restore source (integration)

*Phase 6 follow-up, done at integration.*

**Call.** Stopping an instance whose `restore_from` names another
instance's snapshot (a fork, or a create from a snapshot, that has not
restored yet) keeps `restore_from`, and a fork still waiting for its
capture keeps `pending_op = fork`. A later `start` restores it. Its own
snapshots (a resume, a rollback) are dropped on stop as before.

**Alternatives.** (a) Terminate a waiting fork on stop. (b) Refuse `start`
with a hint. (c) Leave it detached, so `start` boots it on an empty volume
(Phase 6's behavior).

**Trade-off.** A stopped fork holds its checkpoint (gigabytes of guest
memory on the node) until it is started or destroyed.

**Why (ease).** Stop means "pause this instance", and a fork's state is its
checkpoint, so keeping the source is what stop already means for every other
instance. (a) turns a pause into a delete, (b) leaves an instance that can
never start, and (c) silently boots an empty workspace, which the user ruled
out.

## 85. A restore recreates a missing TAP device and refuses a busy or different one

*Phase 3c, implementation.*

**Call.** Before a sandbox restore loads saved state, the worker checks the
TAP device the checkpoint recorded: the task must still name it, a free one
is used, a missing one is created and brought up (and deleted on destroy),
and a busy one, a different one, or a failed create fails the restore with
`restore needs TAP device <name>: ...`. Busy is detected with a
`TUNSETIFF` attach probe.

**Alternatives.** (a) Require the operator to pre-create the device, as boot
does. (b) Firecracker's network overrides on snapshot load, which
cloud-hypervisor does not have.

**Trade-off.** Legion now creates TAP devices in one path (restore) while boot
still expects them pre-created, and the probe needs root or
`CAP_NET_ADMIN`.

**Why (correctness).** A restore that silently came up without its network,
or attached to another VM's device, is worse than one that fails with the
device's name. Recreating a missing device covers the common case of a node
reboot between snapshot and restore.

## 86. A fork gets its own TAP device, named from its instance id, on the origin TAP's bridge (Phase 6)

*Phase 6, implementation. The remote bridge rule was added at integration.*

**Call.** A memory restore into a new instance whose task wires a TAP device
moves the guest NIC to a TAP of its own: `lgf` plus 12 hex characters of the
SHA-256 of the instance id. The worker creates it (or uses a free one an
operator pre-created), enslaves it to the origin TAP's bridge when there is
one, and removes it on destroy. On a node where the origin's TAP does not
exist (a spread fork), it joins the bridge of the task's configured TAP
there, and with neither on this node it fails by name (`restore needs TAP
device lgf...: ... would have no network`). Firecracker gets the device
through `network_overrides` on snapshot load (1.12 and later, refused at
the API before that), cloud-hypervisor through the restore's own rewritten
copy of `config.json`. The fork's later checkpoints record that TAP, so its
own suspend and resume keep it. The guest's MAC is `02:4c` plus the
allocation id, set by the resume handler.

**Alternatives.** (a) Refuse to fork a TAP-wired sandbox. (b) A jobspec
template for per-instance TAP names. (c) Name the TAP from the allocation id.

**Trade-off.** Legion now creates and bridges TAP devices, which it
otherwise leaves to the operator. Addresses configured inside the guest are
copied, so a static IP collides on the bridge, which the docs and guide say
up front.

**Why (ease).** The user asked for a distinct MAC and TAP per fork, and (a)
would make memory fork useless for the most common sandbox networking. (b)
adds jobspec surface for one feature. (c) would change the TAP on every
resume of the fork. Joining the origin's bridge puts the fork where the
operator already wired the origin, which is the least surprising segment.

## 87. A disk fork of a TAP-wired sandbox is refused, and a fresh boot on a held TAP fails by name (integration)

*Phase 6 follow-up, done at integration.*

**Call.** ForkInstance refuses `--checkpoint disk` for a sandbox whose
config wires a TAP, with `Hint: use --checkpoint memory`. Every fresh
microVM boot with a TAP checks the device first, and fails with `sandbox
needs TAP device tap0 ..., but another process holds it` when another VMM
holds it. That covers a fork that falls back to a cold boot and a disk
snapshot seed placed next to its origin.

**Alternatives.** (a) Give cold boots of fork-lineage instances their own
TAP too. (b) A per-instance TAP template in the jobspec.

**Trade-off.** A fixed-TAP task still runs one instance per node, and a
fork whose memory restore fails under `onRestoreFailure = "cold"` fails to
start instead of booting without the origin's network.

**Why (simplicity).** Both alternatives make Legion pick network devices
for fresh boots, which is jobspec surface and host plumbing for an edge
case. The bug was the silent collision, and naming it at the API (where the
origin is certainly on the node) and at boot (where the VMM's own error is
opaque) fixes that without new surface.

## 88. Stateful tasks never truncate index candidates (Phase 7b)

*Phase 7b, implementation. A bug fix found by the new `restore` benchmark
group.*

**Call.** The scheduling index may cut a task's candidate list to the
nearest-fit nodes. That cut is now off for every task with a `stateful`
block, beside the existing exclusions (`count.per`, `distinct`, task
volumes, multi-task groups). Before, a restore from a `local` snapshot (and
a node-local `stateful.storage` pin) failed placement on any fleet large
enough to arm the index whenever its pinned node fell outside the cut,
with the misleading "node ..., which is unavailable".
`tests/restore_placement_indexed.rs` covers it.

**Alternatives.** Keep truncating and add the pinned nodes back into the
cut list. Or disable truncation only when the task has pins right now (a
store lookup during candidate construction).

**Trade-off.** Keyed and slot stateful tasks pay the full candidate scan on
large fleets even when none of their instances is pinned. The `restore`
group in `scheduler_bench` measures the pinned and floating cases.

**Why (correctness, simplicity).** A pin is a hard rule and truncation is an
optimization. Re-adding pinned nodes would need the capacity and scoring
state that is built from the truncated list, and a per-plan pin lookup moves
store reads into candidate construction. Stateful tasks are a minority of
placements, and their placements are already dominated by the pin and
volume lookups.

## 89. Usage is held per manager, in memory, and wrapped in one `UsageReport` (Phase 1)

**Call.** The heartbeat carries `optional UsageReport usage` (sample time,
per-allocation usage, node memory), sent every 10 seconds. The receiving
manager keeps the latest report per node in a `UsageTable` beside its other
heartbeat-derived soft state, drops reports older than 2 minutes, and never
writes usage to the store.

**Alternatives.** (a) The plan's flat `repeated AllocationUsage
allocation_usage` on `HeartbeatRequest`. (b) Ride liveness digests to the
status-lane leader so every manager can answer.

**Trade-off.** With several managers, only the one a node reports to can
answer `GetNodeUsage` for it, and another one says `usage_reported: false`.
The `net_status` view has the same limitation, so operators meet one rule
and not two.

**Why (performance, simplicity).** One optional message makes "no report on
this beat" and "old worker" the same, cheap case, and carries the sample time
the CPU rate and idle math need. Usage is high-churn and worthless after a
restart, the same reasoning that keeps heartbeat timestamps off raft. (b) is
recorded in TODO.md if the limitation bites.

## 90. The idle sweep runs wherever usage lands, and `begin_suspend` forwards to the lane leader (Phase 5)

*Phase 5, implementation.*

**Call.** The idle sweep runs in the manager maintenance loop on every
manager, not only the scheduler-lane leader, on its own cadence (`suspend.idleSweep`,
default 15s). It scans the in-memory usage table and reads the store only for
allocations past their window. `Store::begin_suspend` became a forwardable
lane write (`ForwardableLaneWrite::BeginSuspend`), so a follower's sweep
starts suspends through the leader. Each allocation is tried at most once a
minute.

**Alternatives.** (a) Gate the sweep on scheduler leadership, like the
resume sweep. (b) Replicate usage to the leader.

**Trade-off.** One more forwardable op on the manager-plane wire. On a cluster
whose nodes heartbeat to several managers, each sweeps only its own nodes.

**Why (simplicity).** Usage lives on the manager a node heartbeats to (entry
89). (a) would never see the usage of nodes that
report to a follower, so their instances would never suspend. (b) puts
high-churn data on raft, which Phase 1 refused. Forwarding the one write
that matters keeps the sweep where its data is.

## 91. Snapshot names point only at manual snapshots, local or published, and the `name` column follows them (Phase 6)

*Phase 6, implementation.*

**Call.** `legion snapshot alias set <job> <name> <snapshot>` creates or
moves a name in the existing `snapshot_alias` table. Only a `manual`
snapshot of the same job can be named, `local` or (since the Phase 7b
integration) `published`. A published name seeds `from_snapshot` creates on
any compatible node. The snapshot row's `name`
column is kept equal to one of the names pointing at it (cleared when the
last one moves away), and `snapshot get` lists all of them as `aliases`.

**Alternatives.** (a) Names on any snapshot, including suspend and fork
checkpoints. (b) Leave `name` as the creation-time name forever, and show
aliases only in a separate listing.

**Trade-off.** A snapshot can carry several names but the list view shows
one. `--name` at create and `alias set` share one namespace, so a name
cannot be reused for a new snapshot without `alias delete` or `alias set`.

**Why (simplicity).** A suspend checkpoint is spent when its instance
resumes and a fork checkpoint when its forks restore, so a name on either
would dangle without warning. (b) would show `agent-golden` on last week's
snapshot after the name moved, which is exactly the confusion a golden
image workflow cannot afford.

## 92. The location index is per manager, like usage

*Phase 7a, implementation.*

**Call.** The index lives on `Store` and is written by whichever manager
serves a node's heartbeat. The scheduler reads its own manager's index.

**Alternatives.** Forward content reports to the scheduler-lane leader
through the liveness digest channel, or replicate deltas to every manager.

**Trade-off.** In a multi-manager cluster, nodes that heartbeat a peer
score 0 on locality at the scheduling manager. Placement stays correct and
loses only the preference. The public doc states it, and TODO.md tracks the
forwarding follow-up.

**Why (simplicity).** It is the same rule usage and `net_status` follow
(entry 89), so operators meet one rule. Forwarding is a protocol change on a
channel Phase 3c is also extending, and the index is soft state whose loss
is already designed to be harmless.

## 93. The worker learns bands from the desired set and pinning from its own drivers (Phase 2)

**Call.** `TaskAllocation` gains `priority_band`, filled in the heartbeat's
desired-set builder from a read-model cache keyed by job config id (bands
are immutable per config, so it is never invalidated). Node-pinned storage
is derived on the worker: an instance volume on a driver this node reports as
node-local. Anything the worker cannot describe reads as untouchable
(node-pinned, never evicted).

**Alternatives.** (a) Compute the order on the manager, which already has
bands, pinning, and usage, and send the worker a ranked list. (b) A store
read per job config per heartbeat for the band.

**Trade-off.** The band rides every heartbeat reply (a few bytes per
allocation). A worker talking to an older manager sees every band as
`normal`, so its order falls back to "most memory first".

**Why (simplicity, performance).** The worker must act without the manager
(entry 9), so it needs the inputs, not a list computed elsewhere that could
be a heartbeat stale. The cache keeps the heartbeat path free of store reads,
which is the property `cached_task_configs_for_job_config` was built for.
Treating unknown as untouchable is the safe direction: the OOM killer is
still the backstop.

## 94. `contentStore.auth` is `kv` or ambient, and `nodeIdentity` is refused

*Phase 7a, implementation.*

**Call.** The worker's `contentStore.auth` accepts `{ kv = "<path>" }` (an
AWS credential object, the same shape artifact `auth.kv` reads). Omitted, the
S3 client uses the node's ambient credentials (AWS environment variables,
then the instance profile). `auth = { nodeIdentity = … }` parses and then
fails manager startup with a hint.

**Alternatives.** Accept the plan's `auth = { nodeIdentity = true }` and
present a node identity token, as `nodeIdentity` artifact auth does for
bearer sources.

**Trade-off.** The plan's example config does not work as written.
Operators on AWS use instance profiles, which is the common case anyway.

**Why (simplicity).** S3 needs SigV4 credentials, and exchanging an OIDC
token for them (STS `AssumeRoleWithWebIdentity`) is not wired anywhere in
Legion yet: the `s3://` fetcher rejects bearer credentials for the same
reason. Accepting the field and failing at the first upload would violate
the rule that unsupported combinations fail loudly at submit.

## 95. Content stores are configured per worker, not as a manager-side region map

**Call.** Each worker config names its `contentStore` URL. Region locality
falls out of giving each region's workers their own bucket. This replaces
the old plan's manager-side `snapshots.stores` map.

**Alternatives.** The old plan's manager config:
`snapshots.stores = { "us-west" = { url = ... } }` with a `defaultStore`.

**Trade-off.** Store config is spread across worker types instead of in one
place, and a misconfigured worker publishes to the wrong region silently
(the manifest records the URL, so it is visible after the fact).

**Why (simplicity and extensibility).** The worker is the one uploading,
with its own credentials. Worker types already differ by region, and
artifact fetch settings already live in worker config. A manager-side map
would need a region-to-worker resolution step that the worker config
already encodes.

## 96. Warms are an in-memory queue on the heartbeat, matched by node attributes (Phase 7b)

*Phase 7b, implementation. The cache plan's `WarmArtifacts` design had a
warm eval and a warm-state table.*

**Call.** `WarmSnapshot` picks every available node that matches the
`key=value` constraints (the jobspec `constraint` matcher over node
attributes) and passes the manifest's `compat`, skips nodes the location
index says already hold every part of at least 1 MiB, and queues a warm per
node in a leader-local queue beside the location index. The node's next
heartbeat carries `op = warm`, the node fetches through the stager into its
content store, unpinned, and its report clears the entry and emits
`snapshot.warmed` or `snapshot.warm_failed`. Entries expire after 15
minutes. The RPC needs `snapshot:read`, and the MCP tool and capability
entry are marked mutating.

**Alternatives.** A persisted warm table and an `artifact_warm` eval, as the
cache plan proposed. Pinning warmed parts with a TTL.

**Trade-off.** A manager restart, or a node that heartbeats a different
manager, drops queued warms silently (the operator warms again). Warmed
parts can be evicted by LRU before they are used. `snapshot:read` lets a
caller start downloads on many nodes.

**Why (simplicity).** Warming is best effort by definition, so the queue can
be soft state like the location index it feeds. A table plus an eval adds a
replicated write per node per warm for a hint. The ACL follows what warming
does to the snapshot (reads it), and the MCP gate follows what it does to
the fleet (downloads), so a read-only agent cannot trigger it.

## 97. Workers advertise a `checkpoint` capability at registration

*Phase 3c, implementation.*

**Call.** `WorkerRegisterRequest.capabilities` lists optional features, and
the manager records them in the node's `dynamic_attrs`. Snapshot work goes
only to nodes that list `checkpoint`, and a request for an instance on an older
worker fails naming its version.

**Alternatives.** Bump the worker protocol version.

**Trade-off.** One more field to keep in sync between registration and the
features that read it.

**Why (extensibility).** A protocol bump gates every feature behind every
other one. Capabilities let a fleet mix versions and let later phases (fork,
locality) add their own names without coordinating.

## 98. The guest root disk is a memory-checkpoint part

*Phase 3b, implementation.*

**Call.** A memory checkpoint copies the sandbox's ephemeral `rootfs.img`
(reflink where possible) while the VM is paused and records it as part
`rootfs`. Restore gives the new allocation its own copy.

**Alternatives.** Rebuild the root disk from the image on restore, as a cold
start does.

**Trade-off.** A checkpoint grows by the root disk's written bytes, and on
filesystems without reflinks the copy costs time while the guest is paused.

**Why (correctness).** The restored guest's page cache, ext4 journal, and
open files all describe the disk as it was at capture. A rebuilt disk is a
different filesystem under a running kernel, which is silent corruption.

## 99. A checkpointable sandbox sees its disks through stable device links

*Phase 3b, implementation.*

**Call.** When a sandbox is launched for memory checkpoints, the VMM gets
symlinks under `<allocs>/.vmdev/<link_key>/` (`rootfs.img`, `vol0.img`, …,
`identity.img`) instead of the real disk paths. A restore repoints the links
at the new allocation's files before the VMM opens them, under a node-wide
lock.

**Alternatives.** (a) Keep disks at a path that does not include the
allocation id, so the same path is valid after restore. (b) Run the VMM in a
jailer chroot with relative paths, like Firecracker's production setup.
(c) Rely on Firecracker's `PATCH /drives` after load alone.

**Trade-off.** Concurrent restores of one checkpoint (forks) are serialized
for the repoint-and-load window, and a crash in that window can leave links
pointing at a dead allocation (harmless, the next restore repoints them).
(b) is the stronger isolation story and is still worth doing on its own.

**Why (simplicity and extensibility).** Both VMMs reopen the recorded disk
paths at load, and cloud-hypervisor has no way to override them. (a) breaks
the "allocation dir is the scratch" model every runtime shares. (c) does not
exist for cloud-hypervisor, and Firecracker opens the recorded paths during
the load, before a PATCH can run (to confirm on real hardware, see the
Linux verification doc). Links work on both VMMs, need no
privileges, and survive fork unchanged.

## 100. The pressure response runs on its own one-second tick (Phase 2)

**Call.** A dedicated task on the worker checks `/proc/meminfo` and PSI every
second and acts in a blocking thread. Actions queue for the next heartbeat.
The usage report, every 10 seconds, carries the preview order and the
response's status.

**Alternatives.** Run the response in the heartbeat tick (every 2 seconds by
default, up to 30 on large fleets).

**Trade-off.** One more timer per worker, and per-allocation cgroup reads
every second while the node is under pressure (none otherwise).

**Why (performance).** The heartbeat cadence is a fleet-scale knob that
operators raise to protect the control plane. Tying memory reflexes to it
would make a large fleet slower to defend its nodes for no reason.
`memory.reclaim` reclaims synchronously, so it also must not block the
heartbeat.

## 101. A fixed 10-second report interval, and activity from CPU and traffic (Phase 1)

**Call.** No knob for the report interval. An allocation is active in an
interval if it used at least 1% of a core or moved traffic through the node
datapath. Exec sessions don't count yet.

**Alternatives.** A worker-config interval. Counting exec sessions now.

**Trade-off.** An interactive exec session that is nearly idle on CPU reads
as idle. Phase 5's idle policy must add it before it suspends anything.

**Why (simplicity).** Usage is a display in Phase 1, and nothing acts on
idle time yet. A knob nobody needs is one more thing to document, and
datapath counters were already on the heartbeat path at no extra cost.

## 102. `cpuBelow` scales with the task's cpu, never below 1% of a core, and `after` is at least 30s (Phase 5)

*Phase 5, implementation.*

**Call.** The `cpu` signal fires at `cpuBelow` x the task's cpu, in cores:
declared `cores`, else `cpu` MHz over the worker's own per-core clock, else one
core when the platform reports no clock. The threshold never drops below 10
millicores, the Phase 1 activity floor. `idle.after` below 30s is refused at
submit.

```text
Error: task "workspace": suspend.idle.after "5s" is shorter than 30s: workers report activity every 10s and the manager checks every 15s, so a shorter window cannot be honored
Hint: set after = "30s" or longer
```

**Alternatives.** (a) `cpuBelow` as a fraction of one core. (b) No floor.
(c) Accept any `after` and suspend as soon as the sweep sees it.

**Trade-off.** A small task's real threshold can be higher than its
`cpuBelow` says (a 100 MHz task's 2% is below the floor). The MHz conversion
depends on the worker's clock reading, which is coarse on some platforms.

**Why (the user's job).** "2% of my task" is what an author can reason about
(a is not). Without a floor (b), a kernel thread waking would keep a tiny
sandbox awake forever. A window shorter than the system can measure (c) is a
promise it would break silently.

## 103. A worker's first heartbeat carries the full set, and every heartbeat carries a report

*Phase 7a, implementation.*

**Call.** No change to `RegisterWorker`. A worker process starts with
"needs full" set, so its first heartbeat after registration sends the full
set. Every later heartbeat sends a report, an empty `epoch == base_epoch`
probe when nothing changed. The worker diffs the store's current set (one
indexed sqlite query) against the set the manager last acknowledged, and
advances nothing until the reply arrives.

**Alternatives.** (a) Send the full set in `WorkerRegisterRequest`. (b) Send
nothing when unchanged. (c) Track a change journal in the store instead of
diffing.

**Trade-off.** Every heartbeat costs one store query on the worker and a
few bytes on the wire, even when nothing changed. A store with tens of
thousands of large entries would make the diff noticeable.

**Why (simplicity).** The probe is how a freshly elected manager discovers
it holds nothing and asks for a resync, so no leadership signal has to reach
workers. Diffing against the acknowledged set means a failed heartbeat needs
no restore step, unlike every other report on the request. (a) would put the
same logic in two RPCs and still need the resync path for failover.

## 104. Locality is computed once per task and folded into the cached per-candidate state

*Phase 7a, implementation.*

**Call.** The planner asks the index for one task's scores once (one probe
per digest plus one write per holder), then writes each candidate's score
into `NodeScoringState`, so the per-placement loop reads a field. Plugin
policies get the same scores on `SchedulingContext::locality`.

**Alternatives.** Look the score up by node id in the per-placement loop.

**Trade-off.** One more `f64` per candidate state, and the refill path must
re-apply the scores (it does).

**Why (performance).** The cached path exists to avoid hashing node ids per
placement. Locality adds no per-placement hashing, so tasks with a content
set pay a per-task cost only.

## 105. Content sizes come from the index, and unsizeable content is left out

*Phase 7a, implementation.*

**Call.** A content set is a list of digests with optional sizes. Pinned
artifacts carry no size, so the size comes from any node that advertises
the digest. A digest no node holds and no caller sized is left out of both
sides of `bytes present / total bytes`. A set with nothing sizeable skips
the term.

**Alternatives.** (a) Count digests instead of bytes. (b) Add a size field
to `ArtifactSpec` so admission knows it. (c) Charge unknown digests a
nominal size.

**Trade-off.** A task with one cached 10 MiB file and one never-seen 5 GiB
file scores 1.0 on the node holding the small one, because the big one has
no size anywhere. Content nobody holds cannot tell nodes apart, so the
ranking is still right, but the absolute score overstates warmth.

**Why (simplicity).** Bytes are what the plan promised, and the index
already knows every size that matters for ranking. (b) is a new jobspec
field for a soft score, and (c) invents a number. Restore manifests (Phase
7b) know their part sizes and pass them, so the overstatement only affects
artifacts.

## 106. Tasks get a copy of a cached artifact, never a hardlink

*Phase 3a, implementation.*

**Call.** A hit copies the blob into the task directory with
`std::fs::copy`, which clones on APFS, btrfs, and XFS, and verifies the
copy's digest before placing it. Blobs are mode 0444 in the store.

**Alternatives.** The cache plan's reflink, then hardlink, then copy, so
an artifact costs disk once per node.

**Trade-off.** On ext4 (no reflink) every allocation pays a full copy and
the disk for it. `resources.disk` accounting is unchanged either way.

**Why (correctness).** A hardlink shares an inode, so a task that writes to
or `chmod`s its staged file changes the store's blob for every later
allocation. A per-allocation `mode` could not be applied without changing it
for everyone. Copy-on-write clones give the disk saving where the
filesystem supports it, without the shared-mutation hazard.

## 107. Caching an artifact is best effort and holds no allocation pin

*Phase 3a, implementation.*

**Call.** A put that fails (class budget full of pinned content, disk below
`minFreeDisk`) logs a warning and the allocation stages from its fetched
bytes. Artifacts are not pinned by the allocations that use them. Only
explicit owners, such as snapshots, pin.

**Alternatives.** The cache plan's rule: every staging allocation pins its
artifacts until destroy, and a fetch that cannot fit fails the allocation
with "cache budget exhausted by running allocations."

**Trade-off.** A busy node can evict an artifact that a running allocation
staged, so that allocation's next restart on a *new* task dir refetches. Its
current task dir is unaffected because it holds a copy (call 106).

**Why (simplicity).** With copies instead of hardlinks, nothing a running
task uses lives in the store, so an allocation pin protects nothing. Making
the cache able to fail an allocation that would have staged fine without it
would be a regression. The no-new-release-hook design also means destroy
paths need no new wiring.

## 108. A store snapshot still waiting to publish is lost with its node (Phase 7b)

*Phase 7b, implementation.*

**Call.** Node removal marks `capturing` and `local` snapshots `lost`, as
before, including a `local` row with `durability = store` whose upload had
not finished. A `published` row keeps its `node_id` (where it was captured)
and is not touched.

**Alternatives.** Keep a pending-publish row `local` in the hope the node
comes back to finish the upload.

**Trade-off.** The window between capture and upload is a window of local
durability, and the doc says so.

**Why (simplicity).** A removed node is gone by definition, and nothing else
holds those bytes.

## 109. Make-room queues the resume's evaluation once it fits (resume guards)

*Resume guards, implementation.*

**Call.** The make-room pass (every 5 seconds) re-checks each waiting
resume, and when it now fits it enqueues a `job_reschedule` eval for the
resume's job, at most once per 10 seconds per resume.

**Alternatives.** (a) Have the suspend transaction enqueue evals for every
job with a resume waiting on that node. (b) Rely on the next unrelated eval.

**Trade-off.** A resume can wait up to one pass after the room appears. Any
room counts, so a resume also gets placed when capacity frees for other
reasons, which changes behavior with `makeRoom = false` not at all.

**Why (simplicity).** Nothing else re-evaluates a resume that failed to
place. (a) puts a cross-job lookup inside the suspend transaction. The pass
already computes the fit, so the nudge is free.

## 110. Deleting waits for the node, except when there is nothing to reclaim

*Phase 3c, implementation.*

**Call.** A delete moves the snapshot to `deleting` and removes the row only
once its node confirms the pins and volume snapshot are gone. A failed or lost
snapshot, or one whose node no longer exists, is removed at once. A worker
defers a delete while a capture of the same snapshot is still running.

**Alternatives.** Remove the row immediately and let the node garbage-collect
unreferenced bytes.

**Trade-off.** A snapshot on an unreachable node sits in `deleting` until the
node returns or is removed.

**Why (correctness).** Pinned bytes are never evicted, so a row removed before
the node released them would leak disk the store could never reclaim.

## 111. Suspended checkpoint bytes come from the manager's snapshot rows, the budget from the worker (resume guards)

*Resume guards, implementation. Deviates from the brief.*

**Call.** `GetNodeUsage.suspended.checkpoint_bytes` sums `total_bytes` of the
checkpoints of suspended instances on the node from the manager's snapshot
records. The worker reports only the `snapshots` class totals (budget, used,
pinned).

**Alternatives.** Have the worker report its suspended checkpoint bytes, as
the brief proposed.

**Trade-off.** Disk checkpoints report 0 bytes (their volume stays in place),
and the figure is the capture size, not on-disk bytes after deduplication.

**Why (simplicity).** The worker's content store pins by snapshot id and does
not know which snapshots are suspends after a restart. The manager's rows are
authoritative and survive worker restarts.

## 112. The worker decides "launch for checkpoints", through `TaskInstance`

*Phase 3b, implementation.*

**Call.** `TaskInstance.checkpoint_launch: bool` tells the runtime to boot a
sandbox with device links, an identity disk, and (Firecracker) an API
socket. Worker core sets it. The pure predicate
`firecracker_needs_api_socket(support, enabled)` is exported for the track
that wires the jobspec.

**Alternatives.** Read a jobspec field (`suspend`, `snapshots`) inside the
runtime now.

**Trade-off.** Until Phase 4 wires `suspend`, no jobspec turns it on, so the
feature is reachable only from tests. (Phase 3c wired it to the `snapshots`
block, entry 34.)

**Why (simplicity).** The jobspec fields belong to the manager-side track,
which was running in parallel. A bool on the instance keeps the runtime
ignorant of job config shape and gives that track one line to wire.

## 113. `restore` takes the checkpoint directory as an argument

*Phase 3b, implementation.*

**Call.** `TaskPlugin::restore(alloc, task, inst, manifest, source:
&CheckpointDir, identity)`. The worker materializes the manifest's parts
into `source` under their recorded file names before the call.

**Alternatives.** The plan's `restore(.., manifest, identity)`, with the
runtime resolving parts by digest itself.

**Trade-off.** One more argument on a public trait, and every runtime gets
it whether or not it reads memory.

**Why (extensibility).** The node content store (Phase 3a) is being built in
parallel. Resolving digests inside the runtime would couple every runtime to
the store's API. With a directory in hand, the merge only changes the worker
code that fills `source` (hardlink or reflink by digest), and runtimes never
change.

## 114. The restore payload says it is a fork, rather than the worker inferring it (Phase 6)

*Phase 6, implementation.*

**Call.** `SnapshotRestore` gains `fork` and `instance_id`. The manager sets
`fork` when the snapshot was taken from another instance. The worker clones
volumes, assigns a MAC, and moves the TAP only when `fork` is set, and uses
`instance_id` for `LEGION_INSTANCE_ID` and the TAP name.

**Alternatives.** Infer a fork on the worker by comparing the manifest's
`origin_instance` with the allocation's instance.

**Trade-off.** Two more proto fields on a payload every restoring
allocation carries.

**Why (simplicity).** The worker's allocations carry no instance binding
(the instance id reaches the task only through injected env), and the
manifest's `origin_instance` is empty for the same reason. Inference would
need both plumbed anyway, and an explicit flag is easier to read in a
heartbeat dump.

## 115. `sandbox.vmmOverheadMiB` rides inside the enforcement payload (Phase 1)

**Call.** Operators write a top-level `sandbox { vmmOverheadMiB }` block in
the worker config, as the plan names it. The manager folds it into the
`resourceEnforcement` JSON it already stores per worker type, and the worker
exports it to the runtime as `LEGION_SANDBOX_VMM_OVERHEAD_MIB`.

**Alternatives.** A new `sandbox` column on `worker_type`, plus a proto field
on `WorkerConfig`.

**Trade-off.** `resourceEnforcement.sandbox` is also accepted as a
spelling, since it is the wire field. The public docs show only the
top-level block.

**Why (simplicity).** The overhead only matters when enforcement is on, so
it belongs with enforcement on the wire. Folding it avoids a migration and a
wire field for a single integer.

## 116. Fork and `from_snapshot` creates are capped at 64 per call (Phase 6)

*Phase 6, implementation.*

**Call.** `count` above 64 is InvalidArgument, for ForkInstance and for
`CreateInstance(s)` with `from_snapshot`. `count = 0` on a fork means 1.

**Alternatives.** No cap (as plain `CreateInstances` has none), or a cap
derived from the node's free memory.

**Trade-off.** A platform seeding hundreds of sandboxes from one golden
snapshot makes several calls.

**Why (simplicity).** Every instance lands on the checkpoint's one node, so
a large fan-out mostly queues pending placements there, and a typo
(`--count 500`) would create hundreds of tombstone-forever ids. A capacity
derived cap would change between calls. 64 is far above what a node runs at
once today.

## 117. Store settings ride in the `artifacts` wire payload

*Phase 3a, implementation.*

**Call.** The user writes a top-level `content { … }` block in worker
config, as the plan specifies. On the wire it travels inside the existing
`WorkerConfig.artifacts` JSON string under a `content` key
(`WorkerArtifactsPayload`).

**Alternatives.** A new `string content = 14` on `WorkerConfig`, a
`worker_type.content` column, and a migration, the pattern
`resource_enforcement` and `edge_boot` followed.

**Trade-off.** One wire field now carries two config blocks, which a reader
of the proto has to be told (the field comment says so).

**Why (simplicity).** Several phases are being built in parallel. A new
proto field number and a migration are the two edits most likely to collide
on merge, and the artifacts payload is already the worker's artifact-staging
config. Older workers ignore the extra key, so skew is free.

## 118. The identity disk is a raw header plus JSON

*Phase 3b, implementation.*

**Call.** 1 MiB raw image: `LEGIONID`, a format version, a payload length,
then the `ResumeIdentity` as JSON, zero-padded. Mode 0600 on the host,
read-only in the guest.

**Alternatives.** An ext4 image with files, or Firecracker MMDS.

**Trade-off.** The guest agent needs a few lines of parsing instead of
`cat /mnt/identity/hostname`.

**Why (simplicity).** The host writes one per restore, so `mkfs` and a mount
on that path would be slow and need privileges. MMDS is Firecracker-only and
rides in the snapshot, so it would carry the old identity.

## 119. `local-dir` snapshots live beside volumes, and clone is idempotent by existence

*Phase 3b, implementation.*

**Call.** Snapshots go to `<volume dir>/local-dir-snapshots/<id>/`, built
under a temporary name and renamed into place. A `clone` into a volume id
that already exists returns success without checking contents.

**Alternatives.** Snapshots inside the volume directory, or a marker file
recording which snapshot a clone came from.

**Trade-off.** A caller that reuses a volume id for a different clone gets
the old one back silently.

**Why (simplicity).** Snapshots must survive volume destroy, so they cannot
live inside it. Volume ids are unique per instance and minted by the
manager, and publication is by rename, so an existing directory can only be
a finished clone of this request. A marker would have to live inside a
filesystem-mode volume, where the workload can see and delete it.

## 120. A small blocking HTTP client for the VMM APIs

*Phase 3b, implementation.*

**Call.** The Firecracker and cloud-hypervisor clients use a ~100-line
HTTP/1.1 client over `std::os::unix::net::UnixStream`.

**Alternatives.** `hyper` (already a dependency) with a Unix connector, or
shelling out to `ch-remote` and `curl`.

**Trade-off.** No keep-alive, no chunked responses (neither VMM sends them).

**Why (simplicity).** The task-runtime trait is synchronous and runs under
the worker's task-manager lock, so an async client would need a runtime
bridge. Shelling out adds binaries to install. The fake-VMM tests exercise
the exact bytes either way.

## 121. The locality metric is a warm/partial/cold counter at plan time

*Phase 7a, implementation.*

**Call.** `legion_scheduler_locality_placements_total{outcome}` counts
placements with a content set by how warm the planned node was. The hit
ratio is `warm / total`.

**Alternatives.** A histogram of the locality fraction, or counting at
apply time against the bound candidate.

**Trade-off.** A placement the apply binds to a fallback candidate counts
the planned node, and `partial` hides how partial.

**Why (simplicity).** The question operators ask is "how often do we land
warm", which one ratio answers. Counting at plan time keeps the metric next
to the score that produced it, and fallbacks are rare outside contention.

## 122. Published URLs are recorded in the content store index, apart from entries

*Phase 7a, implementation.*

**Call.** `ContentPublishing::publish` records `digest -> url` in a
`published` table in the node content store's sqlite index, independent of
the `entry` table.

**Alternatives.** Keep the record in memory, or as a column on `entry`.

**Trade-off.** A record can outlive the remote object if someone deletes
it from the bucket. A re-publish then trusts the record and uploads
nothing, and the reader's fetch fails on a missing object.

**Why (simplicity).** A published copy outlives local eviction, so tying the
record to `entry` would forget it the moment LRU runs. Phase 7b records URLs
in the snapshot manifest anyway, which is the authoritative copy, so this
table is a cache that only saves a HEAD request.

## 123. Suspend columns are read apart from the generic instance row (Phase 4)

*Phase 4, implementation.*

**Call.** The §7 columns (`checkpoint_id`, `pending_op`, `suspended_at`,
the last suspend and restore times, `lifecycle_error`) live on `instance`
but are read by their own query (`instance_suspend_state`,
`instance_suspend_states_for_job`), not added to `INSTANCE_ATTRS`.

**Alternatives.** Add them to `Instance` and every instance query.

**Trade-off.** `instance list` runs one more query per job, and code that
needs both must ask twice.

**Why (simplicity).** Instance rows are selected positionally in many
places, and the generic upsert must never clear suspend state (the same
reason 3c kept `restore_from` out of it). Keeping them apart means the
existing paths cannot drop them by accident.

## 124. `suspend_reason` is written when a suspend starts (Phase 5)

*Phase 5, implementation.*

**Call.** `begin_suspend` writes the reason (`manual`, `idle`, `pressure`) to
`instance.suspend_reason` with `pending_op = suspend`, and the capture's
transaction reads it back. `NewSnapshot` carries it. A failed suspend leaves
the column naming that attempt.

**Alternatives.** A `suspend_reason` column on the `snapshot` row, read when
the capture lands.

**Trade-off.** After a failed idle suspend, `suspend_reason` reads `idle`
while the instance is `active`, until the next suspend. `instance get`
documents it as the reason of the current or last suspend.

**Why (simplicity).** No migration, and parallel phases are adding their own
to the same tables. The value is what `instance get` shows while a suspend is
in flight, which is useful in its own right.

## 125. `instance fork --wait` is opt-in, like `suspend --wait` (Phase 6)

*Phase 6, implementation.*

**Call.** `legion instance fork` returns the fork ids at once, with
`pause_ms` null and `checkpoint_state: capturing`. `--wait` blocks until the
capture lands and fills in `pause_ms`.

**Alternatives.** Wait by default, as the plan's §6.9 example implies by
showing `pause_ms` without a flag.

**Trade-off.** The plan's example envelope needs `--wait` to reproduce
exactly.

**Why (simplicity).** Every other checkpoint verb (`suspend`, `snapshot
create`, `snapshot restore`) returns at once and waits only on `--wait`.
One rule for all of them is easier for an agent than one exception, and the
ids, which are what a caller needs first, are in the immediate reply.

## 126. The real cross-node test carries its part on a disk-kind manifest (Phase 7b)

*Phase 7b, testing.*

**Call.** `a_published_snapshot_restores_on_a_node_that_never_held_it`
publishes a part from one real worker task manager to a `file://` store and
restores on a second one, through the stager, with an `exec` task. The
manifest is `kind = "disk"` with one memory part, because `exec` refuses to
restore a memory manifest.

**Alternatives.** A fake runtime that accepts memory restores, registered
only in tests.

**Trade-off.** No macOS test loads a guest's memory on a node that did not
capture it. That is item 7b.3 of the Linux verification plan.

**Why (ease).** The transport (publish, fetch by digest, store, materialize,
restore report) is the new code, and it is runtime-independent. A fake
runtime would test the fake.

## 127. New proto fields take the next free number, renumbered at merge

*Phase 3c, implementation; revised when Phases 2, 3, and 7a merged.*

**Call.** Every field this plan adds to an existing message takes the next
free number on that message, with no gaps. Phase 3c first gave its fields on
shared messages number 30 so it could run alongside Phase 2 and 7a, which
also add heartbeat fields. The merge renumbered them:
`HeartbeatRequest.snapshot_reports = 13`, `content_locations = 14`,
`HeartbeatReply.checkpoint_requests = 9`, `content_resync = 10`,
`TaskAllocation.restore = 22`, `WorkerRegisterRequest.capabilities = 9`, and
`InstanceInfo.restore_snapshot_id = 14`.

**Alternatives.** (a) Keep the 30s and accept the gaps. (b) Reserve a range
per phase in the plan up front.

**Trade-off.** Renumbering is a wire break between builds of the branch.
Nothing on the branch has been released, so no cluster mixes them.

**Why (simplicity).** The parallel-branch range was a merge aid, not a design.
Sequential numbers are what a reader of `legion.proto` expects, and a gap
invites the next contributor to wonder what was deleted. After release the
numbers are frozen like any other.

## 128. Resume-guard proto fields start at 200 (resume guards)

*Resume guards, implementation.*

**Call.** Fields this track adds to existing messages (`InstanceInfo`,
`GetNodeUsageReply`, `NodeAllocationUsage`, `ContentLocationReport`) are
numbered from 200 in a block marked `Track 2`. New messages
(`SuspendedCapacity`, `SnapshotBudget`) number from 1.

**Alternatives.** The next free number, as entry 127 prefers.

**Trade-off.** Gaps in the numbering until the merge renumbers them, as
entry 127 describes. The merge did: every field from the parallel work now
takes the next free number on its message, and the track labels are gone
from the proto.

**Why (simplicity).** Two other tracks add fields to `InstanceInfo` and the
node usage messages in parallel. A distinct range keeps their branches from
colliding on a number, and the merge renumbers once.

## 129. This plan supersedes the snapshot plan's phasing, not its mechanics

**Call.** `sandbox-snapshots-and-fast-start-plan.md` stays as the reference
for capture mechanics, the manifest, quiesce hooks, identity collision, and
UFFD. It gets a header pointing here for scope, phasing, warm-pool stance,
and blob transport.

**Alternatives.** Rewrite the old plan in place, or fold all of its content
into this one.

**Trade-off.** A reader needs two documents to see the whole design.

**Why (simplicity).** The repo's rule is never to silently rewrite a
superseded plan. The old plan's mechanics are still right, and restating
them here would create two copies to drift apart.

## 130. Snapshot restores keep the locked launch path (rebase onto main)

*Rebase onto main, after the audit's W3 fix moved launches off the
task-manager lock.*

**Call.** A dispatch that restores from a snapshot (a resume, a fork, or
`RestoreInstance`) starts through the locked path, `start_staged_allocation`
or `run_allocation_gated`, rather than main's new unlocked launch. Every
other launch still runs its runtime calls with the lock released. The
unlocked path also gained this plan's two launch inputs: the runtime's
enforced memory limit and `checkpoint_launch`.

**Alternatives.** (a) Teach `UnlockedLaunch::run` to restore: roll the
instance volumes back, call the runtime's `restore`, and carry the outcome
back to `finish_unlocked_launch` so it lands on the checkpoint channel. (b)
Restore unlocked but record the outcome from the launch thread.

**Trade-off.** A memory restore holds the task-manager lock for as long as
the VMM takes to load its snapshot, so heartbeats from that node wait behind
it. That is the stall W3 removed for ordinary launches.

**Why (simplicity).** Restore records its outcome on the checkpoint channel
from inside the start, and a restore failure follows `onRestoreFailure`. Both
need the manager state the lock guards. Moving that across the unlocked
handoff is a real change to restore's failure handling, not a merge fix, so
it is a TODO item under the audit's W3 follow-up instead.
