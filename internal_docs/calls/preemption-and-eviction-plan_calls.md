# Judgement Calls: Preemption and Eviction

Decisions made while writing
[`plans/preemption-and-eviction-plan.md`](../plans/preemption-and-eviction-plan.md)
(status PLANNED — these are design-time calls; implementation-time calls get
appended here at their rank). **Entries are ordered most-impactful first; the
number is the rank.**

## 1. Priority is four named bands, and preemption is strictly cross-band

**Call.** `priority: 'critical' | 'high' | 'normal' | 'low'` on the job.
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

## 3. Preemption ships enabled; choosing a band below `normal` is the opt-in

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

## 4. One eviction pipeline: graceful stop lands first and serves every cause

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

**Trade-off.** A `TaskPlugin` ABI break (`StopContext` threaded through
`stop`), a worker behavior change for *all* jobs (stops that used to be
instant now take up to `killAfter`), and Phase 3 taking a dependency on
Phase 1. Also stops-before-starts reconcile ordering couples a new
allocation's start latency to co-located victims' grace.

**Why (simplicity).** The K8s taxonomy — ~9 kill paths, each with different
budget/grace semantics — is the most-cited operational failure in this
problem space, and it is unfixable there precisely because it was allowed to
diverge early. Legion currently has *zero* graceful paths, which is the one
moment unifying them costs nothing. The rollout/mesh roadmap
(drain-before-SIGTERM) needs this exact contract anyway.

## 5. `disruption.minAvailable` binds only voluntary disruption, and is a hard rule

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

## 6. Victims requeue via the existing `job_reschedule` machinery, in the apply transaction

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

## 7. The band also sets eval-queue priority — one field, both meanings

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

## 8. Node-pinned stateful allocations are never victims

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

## 9. The worker orders stops before starts, and delays co-located starts until victims exit

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

## 10. No preemption plugin seam in v1

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
