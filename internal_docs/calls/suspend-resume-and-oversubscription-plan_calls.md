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

## 5. Checkpoints are node-local by default, publishing is opt-in

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

## 6. The pressure ladder runs on the worker and reports afterward

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
same reason. Idle suspend, which is not urgent, stays manager-driven (see 9).

## 7. The content location index is in-memory soft state, off raft

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

## 8. Locality is a score, except for local checkpoints

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

## 9. Idle is detected on the worker and acted on by the manager

**Call.** The worker computes `idle_since` per allocation from signals it
already sees. The manager runs a 15s sweep and issues suspends.

**Alternatives.** Let the worker suspend idle allocations on its own, like
the pressure ladder does.

**Trade-off.** Up to one sweep interval plus a heartbeat of extra idle time
before suspend, and a manager-side loop.

**Why (simplicity).** Suspend is an instance state transition, and the
manager owns instance rows. Keeping non-urgent transitions manager-driven
means exactly one component normally decides, and the worker-initiated path
(entry 6) stays an exception reserved for emergencies.

## 10. Disk-only suspend for `exec` and `container`, no CRIU

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

## 11. Fork charges every fork its full reservation

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

## 12. `instance start` resumes, with `--cold` to discard

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

## 13. Sandbox guest size follows `resources`

**Call.** Guest memory is `memoryMax` if set, else `memory`. An explicit
`config.memory_mib` that disagrees is a submit error, after one release of
warnings.

**Alternatives.** Keep `memory_mib` independent, as today.

**Trade-off.** A breaking change for specs where the two numbers differ,
mitigated by the warning release.

**Why (simplicity).** Two unrelated memory numbers on one task are the
reason a sandbox can reserve 128 MiB and run a 4 GiB guest today. With
burst and pressure handling in play, the scheduler, cgroup, and guest must
agree on one model.

## 14. Suspend is keyed-only in v1

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

## 15. Content stores are configured per worker, not as a manager-side region map

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

## 16. This plan supersedes the snapshot plan's phasing, not its mechanics

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
