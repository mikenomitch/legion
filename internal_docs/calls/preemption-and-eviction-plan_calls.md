# Judgement Calls: Preemption and Eviction

Decisions made while writing and building
[`plans/preemption-and-eviction-plan.md`](../plans/preemption-and-eviction-plan.md)
(status SHIPPED — phases 1a, 1b, 2, 3 and the Phase 4 UI item are built).
**Entries are ordered most-impactful first; the number is the rank.**
Implementation-time calls are inserted at their rank, not appended: entries 3,
5, 6 and 10 came out of building Phase 1a, and entries 5, 8, 13, 15, 16, 17
and 18 out of building 1b/2/3. The design-time entries they displaced kept
their relative order.

## 1. Priority is a closed set of named bands, and preemption is strictly cross-band

**Call.** `priority: 'critical' | 'high' | 'elevated' | 'normal' | 'low' |
'background'` on the job (originally four bands; see entry 5 for the two
added during implementation).
An allocation is only ever a victim of a job in a *strictly higher* band —
never its own band. No integer priorities, no delta rules.

```jsonnet
{ name: 'api', priority: 'high' }
```

**Alternatives.** (a) Nomad's 1–100 integer (plus its hidden "victims must
be ≥10 points lower" rule) — the obvious comparator, and what a Nomad user
would expect to find. (b) K8s PriorityClass: an int32 space with named
classes layered on top.

**Trade-off.** No fine-grained ordering *within* a band: two `high` jobs
contending for one slot resolve by timing, and a user who wants "slightly
more important than my other batch job" has no dial. If real fleets need
intra-band ordering we would have to add a secondary key, which weakens the
"bands are the whole story" pitch.

**Why (simplicity).** Every mature system converged here after pain: Borg
collapsed fine-grained priorities into bands *because* of preemption
cascades and banned preemption inside the production band; Nomad's 10-point
delta is a band rule wearing an integer costume; K8s's 2^31 space produced
the 1000000000 arms race that platforms patch with quota and webhooks. Bands
make the anti-cascade guarantee structural (cascade depth ≤ 2, strictly
downward), make governance a vocabulary question ("who may use critical")
instead of a value audit, and are explainable in one sentence. This is the
headline simplicity claim applied to the scariest scheduler feature.

## 2. Evictions ride the placement plan and commit under the epoch CAS

**Call.** The planner emits victims *in* the plan (`PlanOutcome.evictions`);
`apply_plan_submission_apply` deletes victims, inserts the preemptor's
allocations, enqueues the victims' `job_reschedule` evals, and stages the
events — one transaction, fenced by the same epoch compare-and-set as every
other plan.

**Alternatives.** (a) Two-phase: an eviction eval frees capacity, a
follow-up eval places the preemptor — K8s's shape (`nominatedNodeName`).
(b) Nomad's shape: same-plan eviction but with the client racing stop and
start unordered.

**Trade-off.** Widens the plan format (`plan_submission.evictions` column,
migration) and the apply path, and makes the read-model fencing window
(TODO § Scheduling) release-blocking for Phase 3 instead of a background
nit. Two-phase would have touched less of the store.

**Why (performance/correctness).** Two-phase is the documented disaster: the
freed capacity is not reserved, so anything can steal it — K8s needed three
KEPs (1923/4832/5278) to partially fix it and still shipped an 8,000-eviction
preemption loop (kubernetes#130429); Volcano #5022 applied evictions whose
preemption then failed. Legion's store shape — allocations are rows, plans
are CAS'd — makes "reservation" free: the instant capacity is freed it is
occupied by the preemptor's rows in the same commit. The strongest
correctness property in the plan costs one column.

## 3. Phase 1 split: the stop *cause* ships without the stop *ladder*

**Call.** What the plan called Phase 1 became 1a (cause recorded, delivered,
logged, in event metadata) and 1b (signal, `killAfter`, notice file, reaper).
1a shipped; termination is still immediate.

```console
$ legion events list --kind allocation.removed -j | jq '.data.events[0].metadata'
{ "count": 3, "cause": "drain" }
```

**Alternatives.** (a) Ship Phase 1 whole, as planned. (b) Ship the ladder
first and the cause later — the plan's own framing, since `stop { signal,
killAfter }` is the user-visible surface.

**Trade-off.** Two releases instead of one, and 1a ships a `StopContext` whose
only field is a cause no in-tree runtime acts on — vocabulary slightly ahead
of behavior. A reviewer reading `stop_with` today will reasonably ask what it
is for. The public doc has to lead with a scope note saying tasks still get no
grace period, which is a weaker headline than "every stop is now clean".

**Why (correctness first, then simplicity).** Building it revealed that (a) is
not merely bigger, it is *wrong as specified*: `reconcile_allocations` runs
under the task-manager mutex inside the async heartbeat loop, so a `stop()`
that waits for a deadline holds the lock heartbeats, the self-fence watchdog
and health reporting all need. At a 2s cadence, a 30s grace would let the
manager declare the node lost and reschedule everything on it — graceful stop
turning into node flapping, strictly worse than the hard kill it replaces.
Grace needs a terminating set and a reaper, which is a small state machine,
not an argument change. Option (b) inverts the dependency: a notice file
written immediately before SIGKILL is a file no task can read, so the ladder
has to come with, or before, the file. Splitting on the seam that actually
exists let the debuggability win — "why did my task go away?" — ship in days
while the state machine gets designed properly.

## 4. Preemption ships enabled; choosing a band below `normal` is the opt-in

**Call.** `schedulerSettings.preemption.mode` defaults to `enforce`. Nothing
is ever a victim unless its author set `priority: 'low'` (or a `high`+
preemptor meets `normal` work), so the workload-level consent is the band
choice itself. Operators keep `observe` / `off` / `disabledPools` as brakes.

**Alternatives.** (a) Nomad: off by default for service/batch, enabled via
an operator API — the cautious default. (b) K8s: always on for everyone
with no off switch at all.

**Trade-off.** A user who sets `priority: 'low'` casually — as a label, not
a consent — gets evicted and is surprised. We accept that risk and spend
documentation on it (the band descriptions say "may be displaced") rather
than adding a switch. The `observe` mode and submit-time warnings are the
mitigations.

**Why (simplicity/ease).** Nomad's off-by-default is its single biggest
preemption ergonomics failure: priority exists in the jobspec, silently does
nothing, and "preemption not working" has its own troubleshooting genre. A
field that always means what it says beats a safer field that usually means
nothing. K8s proves always-on is livable even with a far sharper model.

## 5. Six bands, not four: `elevated` and `background` slot in without moving the originals

**Call.** The band vocabulary is `critical` / `high` / **`elevated`** /
`normal` / `low` / **`background`**. The two additions were inserted *between
and below* the plan's original four rather than replacing them, and the
original four kept their eval priorities exactly (90 / 70 / 50 / 30); the new
bands took the gaps (`elevated` 60, `background` 15).

```jsonnet
{ name: 'ingest-workers', priority: 'elevated' }   // above normal, off the pager
{ name: 'cache-warmer',   priority: 'background' } // first to go
```

**Alternatives.** (a) Keep four bands — the plan as written, and the Borg
precedent (monitoring / production / batch / best-effort) exactly. (b) Add the
two but re-space every band evenly (95/80/65/50/35/20), which is what you
would pick if designing the six from scratch. (c) Add two *above* the top,
mirroring Kubernetes' `system-cluster-critical` tier.

**Trade-off.** Six bands is more vocabulary to teach than four, and it dilutes
the "there are only a handful of them" pitch slightly. The uneven eval-priority
spacing (a 20-point gap between `critical` and `high`, a 10-point gap between
`high` and `elevated`) is an artifact of preserving the original numbers rather
than a designed curve; anyone reading the table cold will notice. And the
middle band is the one most likely to be misused as "normal but I care" — the
`priority: 1000000000` failure mode in miniature, one notch down.

**Why (simplicity).** (b) would have been cleaner on paper and worse in
practice: `normal` has been the manager's `defaultPriority` of 50 since before
bands existed, so moving it would silently re-order the eval queue on every
existing cluster for no user-visible gain. Preserving the four means an
un-banded cluster's queue behaves *identically* after the upgrade, and the
only thing that changes is that two new opt-in values exist. (c) is the arms
race the closed vocabulary exists to prevent — a tier above `critical` invites
a tier above that. The two bands earn their place from the shape of the gap
they fill: `normal`→`low` is a cliff from "expected to run" to "expected to be
displaced", and real fleets have work on both sides of it that is neither
(async workers behind a user-visible queue; opportunistic re-encodes). Six is
still small enough to hold in your head and to render as a table.

## 6. Band governance is an ACL selector, not `operator:write`

**Call.** Which priority bands a caller may submit is a `bands` selector on
`job` ACL rules, alongside the existing `namespaces` / `names` / `pools` /
`templates` selectors — not a coarse `operator:write` check on `critical`.

```json
{"resources": ["job"], "actions": ["write"], "namespaces": ["prod"], "bands": ["critical", "high"]}
```

**Alternatives.** (a) The plan's original proposal: `critical` requires
`operator:write`, one line in the RunJob handler. (b) A boolean
`privileged_bands: true` on job rules, mirroring the `host` flag on exec
rules. (c) A `priority.maxBandByNamespace` config map (the plan's Open
Question 5 lean).

**Trade-off.** Materially more work than (a): a new selector, its
`has_scope_selectors` wiring, band normalization before `enforce`, coverage on
the update and template-render paths, and docs. (a) is one line and would
have unblocked Phase 2 immediately.

**Why (extensibility).** `operator` is documented as "raft, manager configs,
observability" — using it here makes everyone who may submit a critical job a
raft administrator, and every raft administrator a critical-job submitter.
That is not a scope, it is a coincidence of vocabulary. It is also
all-or-nothing, so "team A may use `critical` in their namespace" is
inexpressible, which is the first thing any multi-tenant cluster asks for.
(b) cannot express "high yes, critical no" — exactly the split the plan wants.
(c) would have shipped a *second* policy vocabulary beside ACLs, then needed
migrating onto them the moment anyone wanted per-team rather than
per-namespace scoping. The selector costs more up front and is the same work
as Phase 4's per-namespace band caps, done once in the right place — so
Phase 4 loses an item rather than gaining one.

## 7. `stop_with` is added alongside `stop`, so the plugin ABI does not break

**Call.** `TaskPlugin` gains `stop_with(allocation, task, instance, ctx)` with
a default implementation that delegates to `stop()`. The plan called for
threading `StopContext` through `stop` itself — an ABI break with in-tree
plugins "updated in the same commit".

```rust
fn stop_with(&self, allocation: &Allocation, task: &TaskConfig,
             instance: &mut TaskInstance, _ctx: &StopContext) -> R<()> {
    self.stop(allocation, task, instance)   // runtimes that ignore the cause
}
```

**Alternatives.** (a) The planned break: change `stop`'s signature, update the
in-tree runtimes, done. (b) Break it *and* add a plugin ABI version symbol, so
a stale dylib fails loudly instead of mismatching.

**Trade-off.** Two methods where one would do, and a trait that now needs a
sentence explaining which one the worker calls. A future reader could
implement `stop` and wonder why their cause never arrives — mitigated by doc
comments on both, but it is real surface area. (b) remains unbuilt, so a
genuinely stale third-party dylib still has no version guard.

**Why (extensibility).** `plugins/tasks/` is a public extension point loaded
through a bare `extern "C" fn() -> Box<dyn TaskPlugin>` with no version
symbol, so changing the vtable layout means a third-party plugin either fails
to load or mismatches *silently*. Phase 1a delivers no behavior a runtime must
participate in — the cause is informational until the ladder lands — so
breaking every out-of-tree plugin to deliver a field none of them would read
is pure cost. When 1b gives runtimes something to *do* with the context, they
override one method rather than being forced to. The version symbol from (b)
is worth adding on its own merits and is now noted in the plan's 1b list
rather than smuggled in here.

## 8. Phase 1b breaks the `stop_with` ABI on purpose, and adds a version symbol so it is loud

**Call.** `stop_with` changed shape in 1b — from `R<()>` to `R<bool>` ("a stop
notice was delivered; the process is still running") — and `StopContext`
gained `signal`, `deadline` and `grace`. Both are vtable changes. To make a
stale dylib fail at load instead of mismatching silently, every task plugin
must now export an ABI generation:

```rust
legion::declare_task_plugin_abi!();   // beside the `get` constructor
```

A plugin that declares nothing, or a different generation, is refused with a
message naming the rebuild.

**Alternatives.** (a) Keep `stop_with -> R<()>` and add a *third* method
(`begin_stop`) with the new signature, preserving entry 7's no-break promise
literally. (b) Change the signature and skip the version symbol, as Phase 1a's
`stop_with` addition did — the ABI has never had one.

**Trade-off.** Every out-of-tree task plugin must be rebuilt against this
release, and any that isn't now fails to load rather than running degraded.
That is a hard break in a public extension point, taken deliberately. It also
adds one line of required boilerplate to every plugin forever, and a new
failure mode ("plugin declares no ABI version") that a plugin author meeting
Legion for the first time will hit.

**Why (correctness over the letter of entry 7).** Entry 7's actual argument was
that breaking every plugin to deliver a field none of them would read is pure
cost — and it said explicitly that when 1b gives runtimes something to *do*,
they should override one method. That is exactly what happened: `stop_with`
now carries real semantics a runtime must participate in, and (a) would leave
three near-identical stop methods on the trait with a paragraph explaining
which one the worker calls — the knob-count failure this plan spends its
whole research section arguing against. The version symbol is what makes the
break defensible: (b) is the genuinely dangerous option, because the loader
is a bare `extern "C" fn() -> Box<dyn TaskPlugin>` with no type information,
so a stale dylib would load happily and then call the wrong slots. Entry 7
itself flagged the missing version guard as worth adding on its own merits;
this is where it was.

## 9. One eviction pipeline: graceful stop lands first and serves every cause

**Call.** Phase 1 gives *every* platform stop — job stop, rollout
replacement, drain, reschedule, preemption — the same ladder: notice file →
`stop.signal` (default SIGTERM) → `killAfter` (default 30s, cap 5m) →
SIGKILL. Preemption adds a cause, not a mechanism.

```jsonnet
stop: { signal: 'SIGTERM', killAfter: '30s' }
```

**Alternatives.** (a) A preemption-specific grace path, leaving existing
stops hard kills (smallest diff for this plan). (b) K8s's status quo:
per-path semantics (~9 eviction paths, inconsistent grace/budget behavior).

**Trade-off.** A worker behavior change for *all* jobs (stops that used to be
instant now take up to `killAfter`), and Phase 3 taking a dependency on
Phase 1. Also stops-before-starts reconcile ordering couples a new
allocation's start latency to co-located victims' grace.

*Amended during implementation:* this entry originally also listed a
`TaskPlugin` ABI break as a cost. Phase 1a avoided it (entry 7); Phase 1b then
took it deliberately, with a version symbol to make it loud (entry 8). And the pipeline lands in two releases
rather than one, per entry 3.

**Why (simplicity).** The K8s taxonomy — ~9 kill paths, each with different
budget/grace semantics — is the most-cited operational failure in this
problem space, and it is unfixable there precisely because it was allowed to
diverge early. Legion currently has *zero* graceful paths, which is the one
moment unifying them costs nothing. The rollout/mesh roadmap
(drain-before-SIGTERM) needs this exact contract anyway.

## 10. `disruption.minAvailable` binds only voluntary disruption, and is a hard rule

**Call.** The floor is enforced in victim selection (the scheduler cannot
cross it), and it governs *only* voluntary disruption — preemption now, a
future rebalancer/scale-in later. Drain, node loss, and `job stop` ignore
it.

```jsonnet
disruption: { minAvailable: '50%' }   // default 0
```

**Alternatives.** (a) K8s PDB: one floor gating maintenance too — the
app-owner-friendly reading. (b) Nomad: no floor; `migrate.max_parallel` as
a scoring penalty during preemption.

**Trade-off.** An app's floor genuinely does not protect it from a drain —
an operator draining half the cluster can take a service below
`minAvailable`. That is a real gap versus the PDB promise, and the public
doc must say it plainly.

**Why (simplicity/ops).** A floor that gates mandatory work is the
documented cluster-wedge: `maxUnavailable: 0` blocking upgrades is so
common that K8s grew `unhealthyPodEvictionPolicy: AlwaysAllow` as an escape
hatch, and every managed-K8s vendor has a KB page for it. A floor that only
shrinks preemption's options can never wedge anything — worst case the
preemptor stays pending, which is today's behavior. And a *hard* rule beats
Nomad's penalty because a budget that can be outbid is not a budget
(Nomad's is violated even by its own drainer).

## 11. Victims requeue via the existing `job_reschedule` machinery, in the apply transaction

**Call.** No new eval kind, no persisted "preempted" intent, no blocked-eval
queue. The apply transaction enqueues `job_reschedule` for each victim job;
reschedule is driven by desired count, and `regenerate_scheduling_evals`
already converges under-count jobs after failover.

**Alternatives.** (a) A dedicated `job_preempt_replan` eval kind (Nomad's
`EvalTriggerPreemption`) for attribution. (b) A blocked-eval concept so a
displaced job waits for capacity explicitly.

**Trade-off.** A preempted job with nowhere to go simply sits under-count
until some later trigger reconciles it — same as today's lost-node case.
Attribution lives in events, not in the eval row. If Legion later grows a
blocked/pending-eval concept, victim requeue should adopt it.

**Why (simplicity/extensibility).** Requeue-by-desired-count is why the
Nomad #9890 class of bug (preempted dispatch allocs never replaced) cannot
exist here — nothing has to *remember* the eviction for the victim to come
back. One less eval kind, one less persisted state, and the recovery story
is the one that already exists.

## 12. Stop causes are advisory: a missing cause never delays a stop

**Call.** Cause delivery is best-effort. A worker that reconciles before the
notice arrives, talks to an older manager, was partitioned past the 10-minute
retention, or receives a cause it does not recognize stops the allocation
anyway, recording `unknown`. `unknown` is a normal value, not an error.

**Alternatives.** (a) Guaranteed delivery: hold the stop until the cause
arrives, or have the worker acknowledge each notice like the volume-destroy
feed does. (b) Treat an unrecognized cause as a protocol error.

**Trade-off.** Observability has a hole that will occasionally be visible: a
stop genuinely caused by a drain can be journalled as `unknown`, and someone
debugging cannot tell "no cause recorded" from "cause lost". The `preempted`
cause inherits this, so Phase 3's victim→preemptor link is best-effort in the
same way and the public doc has to say so.

**Why (correctness).** The alternative is a metadata lookup on the path that
stops workloads. A notice that cannot be read — busy database, lagging read
model, manager mid-failover — would then wedge every stop on the node, turning
an observability feature into an availability dependency. Nothing downstream
of a stop needs the cause to be correct; it is for humans and for the event
journal. (b) fails the same way one level up: it would make adding a cause a
coordinated fleet upgrade, when the whole point of a string on the wire is
that Phase 3 can start emitting `preempted` to workers deployed today.

## 13. Grace is a terminating set plus a reaper on the supervisor loop, not a waiting `stop()`

**Call.** `stop_allocation_inner` signals, records a deadline, moves the
allocation into a `terminating` map, and **returns**. A reaper on the existing
task-supervisor loop (one tick per second) finishes each entry when its process
exits or its deadline passes — running `destroy`, unmounting volumes, and
removing the allocation directory.

**Alternatives.** (a) Have `stop()` wait for the deadline, which is what the
plan originally specified. (b) A dedicated reaper task with its own timer,
rather than reusing the supervisor's tick. (c) Spawn a detached thread per
stopping allocation.

**Trade-off.** Teardown is now up to one supervisor tick (1s) later than the
process exit, and the terminating set is worker-local state that a worker
restart drops — a worker that dies mid-grace leaves the alloc dir and its
mounts behind until the next start's cleanup. Two maps now describe "this
allocation exists", which a reader has to hold in their head.

**Why (correctness).** (a) is not merely bigger, it is wrong: the stop runs
under the task-manager mutex inside the async heartbeat loop, so a `stop()`
that waits holds the lock heartbeats, the self-fence watchdog, health reporting
and log maintenance all need. At a 2s heartbeat cadence a 30s grace would let
the manager declare the node lost and reschedule everything on it — graceful
stop turning into node flapping, strictly worse than the hard kill it replaces.
(c) reintroduces the same hazard one level down: a detached thread still has to
take the mutex to finish teardown, and now N of them contend for it. (b) is
defensible but pays a second timer for work the supervisor's existing tick
already sequences correctly — the supervisor is *already* the loop that knows
about process exits, and reaping before polling exits is what keeps a reaped
allocation from being mistaken for a supervised one to restart.

The reaper placement also fixes a latent bug in the original design. The
existing comment in `stop_allocation_inner` — "release storage after the
process is gone, never before" — was only true because SIGKILL was
synchronous. Grace decouples "stop returned" from "process is gone", and
unmounting under a live task is how a database gets corrupted. Teardown had to
move behind the actual exit, which is precisely what the terminating set makes
possible.

## 14. The band also sets eval-queue priority — one field, both meanings

**Call.** `critical/high/normal/low` map to eval priorities 90/70/50/30;
`legion job run --priority N` and `--asap` stay as explicit per-submission
overrides of the queue position only.

**Alternatives.** Separate knobs: keep eval priority fully independent of
the preemption band (Nomad's factual state, and what its #11434 asks to
formalize).

**Trade-off.** The pre-existing `--priority` flag now means "override the
band's queue position", which is subtler than before; and queue starvation
of `low` evals under sustained `critical` load becomes possible (accepted;
revisit with aging if a real queue shows it).

**Why (simplicity).** Users of every prior system expect "higher priority =
scheduled first *and* wins fights" and file issues when it isn't true.
Two fields tracking one intuition is a knob tax with no payer.

## 15. Node-pinned stateful allocations are never victims

**Call.** An allocation whose instance storage is node-local (`local-dir`)
is ineligible for preemption, categorically. Stateful allocations on
cluster-reachable storage (`nfs`, future drivers) are eligible and move
through the normal instance-reschedule path.

**Alternatives.** (a) Allow eviction with same-node recreation (frees
nothing — the capacity is needed *on that node*). (b) Force-move and
re-mint storage elsewhere (silent data abandonment). (c) Make all stateful
allocations ineligible (simpler rule, but overbroad — NFS-backed instances
survive rollout replacements today and preemption is the same clean stop).

**Trade-off.** A node hoarded by low-band node-pinned stateful work is
un-preemptible, and the "no eligible victims" error must say so or the
operator will not understand why. Also victim eligibility now consults
volume placement, a small scheduler→volume coupling.

**Why (simplicity/correctness).** Preempting a pinned instance either does
not free capacity or destroys data — both indefensible. The rule follows
the same principle as drain's held-stateful behavior: the platform never
force-moves protected state; an operator who wants it moved acts explicitly.

## 16. The worker orders stops before starts, and delays co-located starts until victims exit

**Call.** `reconcile_allocations` applies removals first; a new allocation
on a node with in-grace victims starts only after they exit (bounded by the
5m `killAfter` cap).

**Alternatives.** (a) Start immediately and let victim + preemptor overlap
for the grace window (Nomad's effective behavior) — fine for exec (no hard
isolation), briefly oversubscribing for container/sandbox. (b) Manager-side
sequencing (place the preemptor only after victims confirm exit) — a
two-phase design by the back door, reopening call 2.

**Trade-off.** Any new allocation landing on a node with slow-stopping
victims inherits their grace as start latency, even if unrelated to the
preemption. Accepted: the cap bounds it, and "the node is never running
victim + preemptor beyond grace" is an invariant an operator can rely on.

**Why (correctness within simplicity).** The plan is atomic logically
(call 2); this is the cheapest way to make the physical handover match it
without new protocol between manager and worker.

## 17. Observe mode journals at the point of decision, not on the way out of the plan

**Call.** In `observe` mode the `allocation.preempt_planned` event is emitted
from inside the victim-selection pass, the moment the decision is made — not
from the plan's return path. Dry runs (`job plan`, `plan_for_config`) run the
identical pass with journalling **off**.

**Alternatives.** (a) Collect observed victims on the plan outcome and emit
once when `plan_and_apply` returns — the obvious shape, and what the first
implementation did. (b) Emit from both, and deduplicate.

**Trade-off.** The event is emitted from inside a function that is otherwise
pure planning, so preemption's one non-transactional write sits in an odd
place; and the emit needs a flag threaded through the pass to keep dry runs
silent, which is a parameter that exists only to say "do not write".

**Why (correctness).** (a) does not work, and the way it fails is instructive:
in observe mode the placement then **fails**, so `build_plan` returns `Err`
and the plan never reaches the code that would have emitted the event. The
whole mode would have been silent — the one thing it exists to not be. The
dry-run flag is not incidental either: without it, repeatedly previewing a job
would look, in the journal, like repeatedly nearly-preempting one.

## 18. Victim metadata is loaded lazily, from the store, only after placement has already failed

**Call.** The preemption pass builds its victim index by querying the store
(one joined statement over `allocation`, `task_config` and `job_config` for
the candidate nodes) the first time a placement in that plan actually fails
for capacity. A plan that never needs preemption never issues the query.

**Alternatives.** (a) Carry band, disruption floor, age and stop ladder on the
`Allocation` rows the read model already holds in memory, so the pass needs no
query at all. (b) Build the index eagerly at the top of every `build_plan`.

**Trade-off.** The rare path pays a database round trip inside planning, and
the query's freshness is not fenced by the plan's epoch sample the way the
capacity snapshot is — a band changed between the query and the apply is not
detected. (It cannot cause an over-commit: the apply re-reads the victim rows
and the epoch CAS still covers the nodes. The worst case is evicting on a
band that changed a moment ago.) It also means the preemption path reads state
the rest of planning does not, which is a coupling a future refactor has to
respect.

**Why (performance).** (b) would put a fleet-wide join on every plan in the
cluster to serve a feature most plans never use — precisely the cost the plan's
testing section makes release-blocking ("the preemption pass must cost nothing
when it doesn't trigger"). (a) is the *right* long-term answer and was
rejected for scope: `Allocation` is constructed in dozens of places and
carried through the read model, the caches, the raft changesets and the wire,
so widening it for four fields used by one code path would touch far more than
preemption. The lazy query buys the same behavior with a change confined to
the scheduler. If preemption becomes common enough for the query to show up in
the plan-phase histogram, (a) is the fix, and `legion_scheduler_plan_phase_seconds`
is already the instrument that would say so.

## 19. No preemption plugin seam in v1

**Call.** Preemption is built into the planner. `SchedulingPolicy` keeps
its ABI; there is no `PreemptionPolicy` plugin category yet.

**Alternatives.** Extend `SchedulingContext` and the policy return type so
plugins can propose victim sets — extensibility is Legion's #1
differentiator and schedulers are literally the first plugin type listed in
`internal_docs/differentiators.md`.

**Trade-off.** Off-brand, and a plugin author who wants custom victim logic
(e.g. cost-aware eviction) is blocked until the seam opens. The victim
selection is deliberately factored as a pure function
(eligible-set → ordered victims) so the seam can open later without a
redesign.

**Why (simplicity, deliberately over extensibility).** Preemption is the
highest-blast-radius feature the scheduler has; the research is a catalog
of systems whose *first-party* victim logic shipped bugs (Nomad's device
segfault, Volcano's non-atomic evictions, YuniKorn's victim-count cap).
Freezing the semantics while they prove out — then opening a seam around a
known-good contract — is the same "keep a seam closed for now" posture the
calls README explicitly blesses.
