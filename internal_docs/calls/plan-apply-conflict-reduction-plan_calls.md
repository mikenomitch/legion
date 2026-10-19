# Judgement Calls — Plan-Apply Conflict Reduction

Plan: [`../plans/plan-apply-conflict-reduction-plan.md`](../plans/plan-apply-conflict-reduction-plan.md)

Ordered by project impact, highest first. An entry's number is its rank.

## 1. The node epoch became a ranking hint, not a fit gate

**Call.** `apply_plan_submission_on` binds in two passes. Pass 1 takes
candidates whose node epoch is unchanged, in rank order — the old walk
exactly. Pass 2 revisits the candidates pass 1 deferred and admits them on
the in-transaction revalidation alone. A plan is rejected only when both
passes fail, and the message names what actually blocked it:

```
before: no candidate for task 41 could bind: all 1 candidate(s) were stale
        or invalid since the plan snapshot (node n-0421 epoch moved
        (expected 7, latest 9))

after:  no candidate for task 41 could bind: none of its 8 candidate(s)
        still fits (node n-0421 no longer has capacity for task 41 …)
```

**Alternatives.** (a) Keep the epoch gate and widen candidate lists
instead — more candidates means more chances one is clean. That is the
option the late-binding plan already took, and the AWS numbers are what it
achieves: at 16 eval threads through 5000 nodes, *every* candidate is
dirty, so a longer list only costs more scoring. (b) Nomad-style partial
plan application: commit the placements that fit, drop the rest. Strictly
more throughput, and genuinely tempting. (c) Serialize planning per pool.

**Trade-off.** The epoch no longer proves anything about capacity, so
correctness now rests entirely on the in-transaction rechecks being
complete. If a future input to placement is added and *not* re-derived at
apply, the old design would have caught it by accident and this one will
not. That is a real, permanent maintenance obligation, and it is why the
revalidation list is enumerated in the walk's comment and in the plan.
Mitigation: the four rechecks (node row/availability, constraints/permit,
capacity via a rebuilt pool, ports) plus the in-tx `distinct` tracker are
each covered by a test that predates this change.

**Why (performance/scale, then simplicity).** 73.6% of plans rejected, and
throughput *falling* as eval threads are added, is a scaling failure, not
a tuning problem — conflict was being detected at "node touched" instead
of "node full" (Omega's exact warning). Rejecting (b) was the simplicity
call: partial application makes "did my plan happen?" a per-placement
question, which counts, groups, and rollouts all have to answer, and it
would have made this change large enough to need its own migration story.

## 2. `target_instance` keeps the hard epoch gate

**Call.** When a submission carries `target_instance` — an edge-boot
ratification, always exactly one placement — pass 2 never runs. A moved
node epoch still rejects, with a conflict.

**Alternative.** Treat it like any other placement and let the
revalidation decide. The instance binding is unique-constrained in the
store anyway, so a double-bind would fail loudly rather than silently.

**Trade-off.** Ratifications keep the old conflict rate. They are rare
(only `speculativeStart` tasks produce them) and already have a retry
path, so this costs almost nothing measurable — but it *is* an
inconsistency in the model: two placements, two different apply
semantics, and a reader has to know why.

**Why (simplicity of the guarantee).** Ratification asks a different
question from placement. "Does this still fit?" is answerable from
committed capacity; "does somebody else already own this sandbox?" is not
— the sandbox claim lives outside the resource pool. Turning a conflict
into a bind because the node happens to have room would be answering the
wrong question. A failed unique constraint is a 500, not a conflict, and
an operator debugging a double-boot at 3am should see "somebody beat you
to it".

## 3. The overlap coupling edge went; the `distinct` and group edges stayed

**Call.** `decouple_plan_components` no longer unions placements whose
candidate lists overlap. It still couples placements of a task with a
`distinct` constraint, and placements in a multi-task co-placement group.

**Alternative.** Keep the overlap edge and rely on longer lists. Or keep
it only for placements whose lists overlap *at rank 0*.

**Trade-off.** Two placements of one plan can now both target one node.
That is safe — each bind is debited from the node's in-transaction free
pool before the next placement is considered — but the safety argument
moved from "structurally impossible" to "checked at apply". A test
(`concurrent_plans_cannot_oversubscribe_a_node`, unmodified) is now what
holds it up, rather than the shape of the data.

**Why (performance/scale).** The edge fired on the most common job shape
in existence: a count-N service into one pool overlaps by construction, so
*every* placement collapsed to a one-element list and late-binding turned
itself off exactly where it was needed. The two surviving edges are
different in kind — `distinct` and co-placement constrain which nodes are
*jointly* acceptable, which a per-node capacity debit cannot express, so
their all-or-nothing apply is still the honest implementation.

## 4. The eviction savepoint became a whole-body savepoint

**Call.** The savepoint a rejection rolls back now covers the placement
walk as well as the evictions, and is named `legion_plan_body`. It is
opened when the plan has evictions *or* more than one placement.

**Alternatives.** (a) Leave it: the bug is pre-existing and unrelated to
this change, so fix it in its own commit. (b) Commit the partial
placements deliberately — that is Nomad-style partial application, and
the rows would at least be doing useful work.

**Trade-off.** One extra `SAVEPOINT`/`RELEASE` pair per multi-placement
plan, and a change to a hot, heavily-commented path that this work did not
set out to touch. Single-placement plans with no evictions skip it, which
is the common case.

**Why (correctness).** A rejection commits, by design, to persist the
`rejected` row and its event. Before this, a plan that bound its first
placement and then exhausted its second committed that allocation row
alongside a submission that says the plan did not happen — a row no
`allocation.placed` event announced and no read-model refresh covered.
`plan_walk_debit_stops_two_placements_of_one_plan_oversubscribing` found
it while proving the deleted overlap edge was safe, which is exactly the
kind of thing that should block, not defer: deleting the edge makes
multi-placement plans with real candidate lists the normal case, so the
window this bug lives in got much wider.

## 5. Rank 0 is exempt from the replica anti-affinity rule

**Call.** The in-walk rule that keeps a task's replicas off a node this
plan already bound them to applies only at rank > 0.

**Alternative.** Apply it at every rank, as originally specified — a plan
never stacks replicas in pass 1, full stop.

**Trade-off.** Under contention two replicas can still share a node when
both name it at rank 0. Accepted: that is the planner's decision, not an
accident.

**Why (correctness of the stated contract, and `bin_pack`).** Applying it
at rank 0 broke the pass-1 promise that uncontended placements bind the
planner's argmax byte-for-byte, and it silently overrode `bin_pack`: a
`reschedule_only_places_missing_allocations` regression caught the
scheduler spreading two replicas across two nodes where the ledger had
deliberately packed them onto one. The planner's ledger has already
consumed capacity for earlier replicas by the time it picks rank 0 for a
later one, so rank 0 *is* the policy's answer. The rule earns its keep
only where the planner has no opinion: the fallback tail.

## 6. Pass 2 improves `bin_pack` and mildly violates `spread`

**Call.** Ship pass 2 for both built-in policies, with no policy-specific
behavior.

**Alternative.** Run pass 2 only under `bin_pack`, and keep `spread`
conservative — a spread cluster arguably *wants* the skip, since landing
elsewhere is the policy's whole intent.

**Trade-off, stated honestly.** Under `bin_pack`, pass 2 is a quality
*improvement*: the old walk skipped a touched-but-fitting node and spread
the placement across the near-tie band, which is precisely what bin-packing
exists to avoid (the old public doc listed this as a known limitation,
"conservative skips spread bursts"). Under `spread`, pass 2 is a mild
policy violation: a node that just received an allocation is now bindable
again this round, where the epoch skip would have pushed the placement to
an emptier node. The magnitude is bounded — pass 2 only runs for
candidates pass 1 could not bind, and it walks the same score-ordered
list, so it never reaches below the planner's own ranking.

**Why (simplicity).** A policy-conditional apply walk means the store
knows about scheduling policy, and the same submission applies differently
depending on manager config — which is a raft determinism hazard the
moment two managers disagree about the policy string. `spread`'s
divergence is a fallback-ranking nuance; a policy-aware store is a
structural problem. If the AWS rerun shows spread quality suffering, the
fix belongs at plan time (widen the band), not in the apply walk.

## 7. Six existing tests were rewritten to assert the new contract

**Call.** Tests asserting "a moved node epoch rejects the plan" were
rewritten to assert what now happens, and renamed where the name had
become false. Every rewrite keeps a negative case: an epoch-moved
candidate that genuinely does not fit still rejects, with a capacity
reason.

| Test | What it asserted | Now |
| --- | --- | --- |
| `plan_walk_rejects_when_all_candidates_are_stale` | both candidates dirty → reject | split: `..._rebinds_an_epoch_dirty_candidate_that_still_fits` and `..._rejects_when_every_epoch_dirty_candidate_is_full` |
| `plan_submission_rejects_stale_epoch_for_shared_node_scope` | second job on a shared node → reject | `..._binds_when_it_fits_and_rejects_when_full`, both halves, via `maxAllocations` |
| `plan_batch_keeps_per_plan_cas_verdicts` | stale *node* epoch rejects inside a batch | stale *job-config* epoch rejects inside a batch, and its batch-mate still applies |
| `lazy_version_sample_is_the_default_and_fences_a_second_job` | rejection used as proof the epoch was bumped | `..._and_bumps_node_epochs`, asserting the epoch directly |
| `decouple_components_shrinks_overlapping_lists_only` / `..._transitive_overlap_is_one_component` | overlap couples | overlap does not couple |
| `count_two_placements_on_a_shared_pool_are_coupled_singletons` | same-pool replicas are singletons | they keep their lists, rank 0 unchanged |

**Alternative.** Keep every test and gate the new behavior behind a
setting, so both contracts hold at once.

**Trade-off.** Six tests changed in the same commit as the behavior they
covered, which is exactly the shape that can hide a real regression. The
mitigation is the line drawn below, and the fact that not one *safety*
test needed touching.

**Why.** The distinction applied to every one of them: a test asserting
**old intended behavior we deliberately changed** gets rewritten; a test
asserting a **safety invariant** must keep passing as-is, and if it fails
the change is wrong. Every failing test fell in the first bucket, and the
three that define the second bucket —
`concurrent_plans_cannot_oversubscribe_a_node`,
`plan_walk_job_config_epoch_mismatch_rejects_despite_fresh_candidates`,
and `tests/integration/raft_apply_roundtrip.rs` — passed unmodified throughout. A
kill-switch setting was rejected because two live apply semantics in one
binary is a raft determinism hazard for a change with no rollback need:
the old behavior is strictly a subset of the new one.

## 8. The fallback tail is sampled from a fixed-width pool, not a measured one

**Call.** `ranked_candidate_list` samples the below-band tail from the top
`W` nodes by score, where `W = plan_apply_batch_max × plan_candidates.count`
(128 at defaults), clamped to the eligible set.

**Alternative.** Derive `W` from an actual measurement of placements in
flight (queue depth, or the last batch's size), so a quiet cluster keeps a
tight tail and a busy one widens automatically.

**Trade-off.** `W` is a heuristic wearing a formula's clothes: the second
factor is "how many placements race for the same scores at once", for
which the candidate-list length is a stand-in, not a measurement. On a
cluster with 8 nodes it is harmlessly clamped; on one with 50,000 it may
be too narrow.

**Why (simplicity).** A measured `W` makes candidate construction depend
on live queue state, which means two managers planning the same job
produce different lists for reasons nobody can reconstruct from the logs.
A constant derived from two settings an operator already sets is
explainable, and the plan names the evidence that would make us revisit it
(fallback collisions persisting at 5000 nodes in the rerun).

## 9. The pool-rebuild cap is a fixed budget, not a time budget

**Call.** A plan may rebuild at most `max(4 × placements, 256)` distinct
node free pools; beyond that, candidates needing a fresh pool are skipped
with a reason that says so.

**Alternative.** Bound the walk by elapsed time inside the transaction.

**Trade-off.** A pathological plan can be rejected for exhausting its
budget rather than for a real capacity reason, and the number is arbitrary
(a plan binding rank 0 everywhere needs exactly one rebuild per distinct
node, so the cap is ~4× slack).

**Why (raft determinism).** A time-based bound makes the apply walk depend
on wall clock, so a slow follower would reject a plan its leader applied —
which corrupts replicated state. A count is a pure function of the
submission, and replays identically everywhere.
