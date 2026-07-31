# Judgement Calls: Late-Binding Plan Apply

Decisions behind
[`plans/late-binding-plan-apply-plan.md`](../plans/late-binding-plan-apply-plan.md).
**Entries are ranked by project impact, highest first; the number is the
rank.** Entries 1–6 were seeded at design time, before execution; entries
7 and below were added during implementation (2026-07-29).

## 1. "Free at apply time" is defined by the epoch, not by capacity math

**Call.** The applier binds a placement to the first candidate whose
*node epoch* is unchanged since the planner sampled it. An unchanged
epoch proves no allocation changed on that node, so the planner's
plan-time fit verification is still valid — the epoch is the fit proof,
carried forward. The applier never reconstructs a `ResourceLedger`.

**Alternatives.** (a) An applier-side ledger: recompute free capacity
inside the write transaction and bind to the first candidate that
*actually* fits. Strictly better placements (a touched-but-still-fitting
node is not skipped) and the shape a scheduler purist would build.
(b) No walk at all — keep single-node plans and rely on plan-time
decorrelation (jitter) alone.

**Merged since.** The pre-benchmark hardening work added an
apply-transaction recheck of the node's *own* totals; entry 17 records how
the two compose. The claim above is unchanged for competing allocations —
the applier still never re-derives free capacity from other jobs' rows.

**Trade-off.** Conservative skips: an unrelated small allocation on
candidate A bumps its epoch and pushes the plan to B even though A still
fits, so bursts spread across the near-tie band where bin_pack would
have stacked. Accepted, and bounded by the band construction — every
candidate is a near-equivalent by score. Evidence to revisit: fallback-
rank metrics showing deep-rank binds while pool utilization is low
(capacity-aware skipping is the named Phase 4 follow-up).

**Why (simplicity, performance).** (a) moves scheduling logic into the
store's replicated apply path — the `ResourceLedger`, resource policy
normalization, and reservation handling would all grow a second home
inside a raft state machine, permanently. The epoch walk adds only
per-candidate comparisons to a transaction that already does epoch
comparisons, keeps the applier explainable in one sentence ("first
candidate nobody touched"), and is trivially deterministic for raft
replay. (b) loses the headline property: with commit-time choice, a race
costs a rank; with jitter alone it still costs a full replan.

## 2. Multi-placement plans partition into coupling components; v1 walks only singletons

**Call.** Placements in one plan are grouped by coupling edges —
overlapping candidate lists (capacity/static-port sharing), shared
`distinct` constraint keys, same co-placement group. Singleton
components get independent candidate walks; components of size >1 keep
today's all-or-nothing CAS on their exact nodes. Colocation affinities
are deliberately not an edge (score coupling, not validity coupling).

**Alternatives.** (a) Candidate lists for every placement with
applier-side stacking checks — maximal fallback coverage, requires the
applier ledger rejected in call 1. (b) The blunt scope: candidate lists
only for plans with exactly one placement, everything else unchanged —
simpler, but a GPU task + CPU sidecar in disjoint pools would forfeit
late binding for no correctness reason.

**Trade-off.** A count-N service on one pool is one coupled component
and gains nothing from this plan until Phase 4 (striped disjoint lists,
partial commit). Accepted: those plans are exactly where intra-plan
stacking can oversubscribe a node through epoch checks that cannot see
it, and the motivating workload (independent single-task jobs) is all
singletons anyway.

**Why (simplicity).** The component rule preserves the invariant that
makes optimistic apply trustworthy — the epoch CAS never admits a
placement whose fit proof might be invalidated *by the same plan*. One
graph with three edge kinds is explainable and unit-testable; every
loosening (striping, partial commit) is an explicit later decision
rather than a hidden assumption.

## 3. Conflict-exhausted evals requeue with backoff; they never terminally fail

**Call.** When `plan_with_conflict_retries` exhausts its budget, the
eval is released back to pending with a delay (reusing
`release_eval_for_retry`, built for foreign-lane `NotRaftLeader`
rejections) instead of being marked `failed` — a state that is never
retried (`src/storage/store.rs:9525`). Terminal failure remains for
non-conflict errors.

**Alternatives.** (a) Keep terminal failure but raise the default retry
budget — cheaper, but any finite in-handler budget still drops work
under a large enough burst, just later. (b) A capped requeue count with
a terminal state after M rounds — reintroduces the silent drop under a
different name.

**Trade-off.** A plan that can never place (saturated pool) now retries
forever at capped backoff instead of parking in a terminal state; the
queue carries standing work and operators watching for `failed` evals
must watch pending-age instead. Accepted: conflict is transient by
definition, and "placed" or "visibly waiting" are the only honest
states for acknowledged work. Evidence to revisit: requeue storms
dominating the eval queue in practice.

**Why (simplicity/ease).** "A submit either places or is visibly
pending" is a guarantee an operator — or an agent driving `job run` —
can rely on without building their own poll-and-resubmit loop. The
current behavior (three lost races ⇒ job silently never starts) is the
sharpest edge this whole plan exists to remove; candidate lists make it
rare, this makes it impossible.

## 4. Candidate lists have a deterministic argmax head and a jittered tail

**Call.** Rank 0 is today's pick — the true argmax with exact-tie
shuffle. Ranks 1..k−1 are sampled without replacement, score-weighted,
from the near-tie band (reusing the 1% `RESCHEDULE_SCORE_JITTER_THRESHOLD`
band). All randomness runs at plan time on the leader; the apply walk is
deterministic.

**Alternatives.** (a) Arm the existing Reschedule-mode score jitter in
Fresh mode — five lines, but taxes *every* placement's quality to fix a
problem that only exists under contention, and a race still costs a
full replan. (b) Pure top-k by score, no jitter — self-resolves (racers
slide down the shared prefix against committed state) but needs
k ≥ burst size, since each racer consumes one shared prefix slot, and
concentrates fallback traffic on the same next-best nodes.

**Trade-off.** Fallback placements are randomized within the band rather
than strictly score-ordered — under contention you may get the 4th-best
near-tie instead of the 2nd. Accepted: the band bounds the quality loss
by construction, and fallbacks only occur when a race actually
happened.

**Why (performance).** Sparrow's power-of-k result: decorrelated tails
let a short list (k=8) survive bursts far larger than k, keeping the
submission payload and walk cost O(k) while conflict probability under
a burst drops to ~P(all k candidates touched in the snapshot-to-apply
window). Better quality than (a) when uncontended, better burst
survival than (b) at equal k.

## 5. Per-component partial commit is deferred to the backlog, not v1

**Call.** A rejected component (or all-candidates-stale plan) rejects
and re-enters the retry/requeue path whole; the apply transaction does
not commit surviving components and report the rest. Omega-style
incremental commit is Phase 4, riding the existing Reschedule refill
machinery if metrics justify it.

**Alternatives.** Partial commit in v1: one transaction inserts the
placements that resolved and returns the conflicted remainder for
refill — strictly less wasted work, and the refill shape
(`count.per` group refill, excluded nodes) already exists.

**Trade-off.** A multi-component plan that loses one component replans
everything, including placements that were never in dispute. Accepted
for v1: with candidate walks, component-level rejection is already the
rare path, and partial commit changes plan-level semantics (a "half
applied" submission state) that every consumer of plan outcomes —
events, rollouts, `job plan` explain, idempotency — would need to
learn.

**Why (simplicity).** Plan apply currently has two outcomes; keeping it
that way while the walk absorbs the common races keeps the mental model
("a plan applies or it retries") intact. Adding a third outcome is
justified only by evidence the two-outcome model still wastes real
work.

## 6. Physically-full pools stay out of scope — no serialized lane in this plan

**Call.** When only one node fits, candidate lists degenerate to length
1 and behavior is today's plus the requeue backstop. The "second lane"
(per-pool serialized or batched apply) is explicitly not built here; the
plan names the metric (`plan_apply_total{rejected}` sustained nonzero on
saturated pools after Phase 2) that would trigger a separate plan.

**Alternatives.** Build batching now: coalesce pending single-task
submits against one pool into a single plan pass — one snapshot, one
CAS, zero mutual conflicts; it is what Nomad's single-scheduler shape
gives implicitly.

**Trade-off.** Saturated-pool bursts still pay retry/requeue rounds
rather than queueing cleanly. Accepted: at saturation the binding
constraint is capacity, not scheduler throughput — the work waits either
way, and the requeue backstop makes the waiting visible and safe.

**Why (simplicity, extensibility).** Batching introduces a second
scheduling mode with its own fairness, ordering, and failure questions.
Making the optimistic lane cheap first (this plan) and measuring what
remains keeps Legion at one scheduling concept unless data proves two
are needed — the differentiator claim is "simplest scheduler to run,"
not "most lanes."

## 7. Phases 2 and 3 ship as one mechanism: the coupling partition is the only gate

**Call.** Implementation builds candidate lists for *every* placement and
then runs the coupling partition (`decouple_plan_components`) at the end
of `build_plan`; non-singleton components shrink to singleton lists. There
is no separate "Phase 2" rule ("multi-placement plans get singleton
lists") — a plan with one placement is simply a singleton component, so
the partition subsumes it.

**Alternatives.** Ship Phase 2 literally (special-case plans with exactly
one placement), then replace that gate with the partition in a second PR.

**Trade-off.** One larger change instead of two independently shippable
ones; the fallback-stacking safety argument rests entirely on the
partition's correctness from day one.

**Why (simplicity).** The phase split existed to stage risk, not because
the shapes differ — the partition *is* the Phase 2 rule generalized, and
implementing the special case first would have meant writing throwaway
gating logic plus tests for a shape that was scheduled for deletion. The
partition is small (union–find over three edge kinds) and unit-tested
per edge.

## 8. Every per-candidate validation failure rejects as a conflict, even for singleton lists

**Call.** In the apply walk, a candidate that fails *any* re-validation —
stale epoch, node row missing, unavailable, constraints/permit, static
port collision — is skipped; exhausting the list rejects the plan with
today's conflict-shaped rejection, which the eval retries/requeues. This
applies to singleton (including format-1) submissions too, so a node row
deleted between snapshot and apply now yields a retried rejection where it
used to be a hard eval error ("Node X not found while applying plan").

**Alternatives.** Preserve the old hard-error surface for singleton
lists; classify node-gone as a distinct terminal error.

**Trade-off.** A truly exotic permanent condition (a placement whose only
candidate never becomes valid but keeps parsing) retries instead of
failing fast. Bounded: replans compute fresh plans against fresh state,
so a permanently-invalid *candidate* cannot recur unless the planner
keeps choosing it, and planner-side errors still fail evals terminally.

**Why (simplicity).** One skip rule for the whole walk ("a candidate the
committed state no longer supports is skipped") keeps the applier
explainable; a node vanishing mid-plan is exactly the kind of transient
race the retry path exists for.

## 9. Static-port collisions get a typed error so ONLY collisions skip candidates

**Call.** `assign_allocation_ports` returns a typed `StaticPortInUse`
error for a taken static port; the walk downcasts for it and skips the
candidate. Every other error out of network assignment (stored-config
parse failures, SQL errors) propagates and fails the apply.

**Alternatives.** Treat any network-assignment error as a skip — simpler,
no new type.

**Trade-off.** A new public error type in `structs::network`.

**Why (correctness).** A malformed stored network config fails on *every*
candidate of *every* replan; treating it as a skip would convert a
permanent config error into an infinite conflict-requeue loop. The typed
marker keeps the "skip = somebody got there first" invariant true.

## 10. `plan_apply_total{outcome}` outcomes are mutually exclusive: `fallback` replaces `applied`

**Call.** An apply where at least one placement bound below rank 0
increments `outcome="fallback"`; `outcome="applied"` now means every
placement got its first choice. The plan's wording ("gains a `fallback`
outcome label value") left double-counting open.

**Alternatives.** Count `applied` for every success and `fallback`
additionally — keeps `applied` meaning "successful applies" but makes
the operator subtract series to answer "how many races am I absorbing?".

**Trade-off.** Dashboards summing successful applies must now use
`applied + fallback`. Accepted: the on-call question this metric exists
for ("contention vs capacity at a glance", plan §Jobs) is a ratio of
exclusive outcomes.

**Why (simplicity/ease).** Three disjoint outcomes read directly:
`applied` = no race, `fallback` = race absorbed, `rejected` = race lost.

## 11. The requeue emits a NEW `eval.requeued` event kind

**Call.** The backstop records a dedicated `eval.requeued` (warn) event in
the same scheduler-lane op as the release, rather than reusing an
existing kind. The plan said "the requeue emits the existing eval
lifecycle events", but no existing kind fires on a release — reusing
`eval.enqueued` would misstate what happened and `eval.failed` would be
false.

**Alternatives.** Stay silent (like the `NotRaftLeader` release path);
overload `eval.enqueued` with metadata.

**Trade-off.** One more event kind in the catalog and docs table.

**Why (simplicity/ease).** The plan's own acceptance criterion is
"observably requeued (event + pending row)"; a distinct kind makes
`legion events list --kind eval.requeued` the diagnosis command, and the
event-stream catalog is designed to grow by kind.

## 12. `job plan` candidate preview required one additive proto field, despite "Proto: none"

**Call.** The explain surface (`PlanJobReply.PlannedAllocation`) gained
`repeated string candidate_nodes = 4`. The plan declared "Proto: none",
assuming the explain payload was a JSON envelope; it is a real proto
message, so the promised candidate preview (§6.6) could not ship without
a field.

**Alternatives.** (a) Drop the preview from v1 — leaves the plan's
observability story half-shipped. (b) Encode the preview into the
human-readable `message` string — unparseable for agents, breaking the
structured-output rule.

**Trade-off.** A wire-visible (though additive and
backward/forward-compatible) proto change, plus regenerated UI stubs.

**Why (extensibility).** Agents self-correct from `job plan` before
mutating; a preview they cannot parse is not a preview. Additive proto3
fields are the cheapest honest way to keep that surface truthful.

## 13. Format-2 emission keys off the setting alone, not the plan's shape

**Call.** `plan_candidates >= 1` emits format-2 rows even when every list
is a singleton (fully coupled plans); only `plan_candidates: 0` emits the
pre-candidate format-1 shape.

**Alternatives.** Auto-emit format 1 whenever all lists are singletons —
maximizes mixed-version compatibility without operator action.

**Trade-off.** A mixed-version fleet running defaults can replicate
format-2 rows an old peer cannot parse even for plans that carry no real
fallbacks. Covered by the documented kill switch + upgrade-order policy.

**Why (simplicity).** One deterministic mapping from setting to wire
format; shape-dependent emission would make "which format will this
cluster write?" unanswerable without inspecting each plan, and the
compatibility problem it half-solves is already solved fully by the
switch.

## 14. Tail-sampling weights are scores shifted to the band floor (plus epsilon)

**Call.** Sampling weight for a band member is
`score − (best − band) + ε` with `ε = max(band, 1e-9)/100` — proportional
to score within the band, exactly uniform when the whole band ties, and
strictly positive so floor-ties stay sampleable. This settles the open
question's lean (score-weighted over uniform).

**Alternatives.** Uniform weights; softmax over raw scores.

**Trade-off.** Near-uniform in practice for a 1% band — the extra
mechanism buys little until someone widens `band`.

**Why (performance, cheap to change).** Keeps the quality ordering
meaningful at any band width, costs three arithmetic ops per candidate,
and is isolated in one function with seeded-RNG tests if benchmarks ever
argue for a different curve.

## 15. Reschedule mode builds candidate lists too; policy plugins get singletons

**Call.** Both cached selection paths (Fresh argmax and Reschedule's
jittered pick) produce full ranked lists — the head keeps each mode's
existing selection rule byte-for-byte. Third-party scheduling-policy
plugins return one node and expose no scored ranking, so their placements
carry singleton lists (exact-CAS semantics, documented).

**Alternatives.** Fresh-only lists (the plan's motivating scenarios are
all submits); extend the plugin ABI with a ranked-selection hook.

**Trade-off.** Plugin users get none of the fallback win until the ABI
grows a ranked hook; reschedule storms (node loss) gain decorrelation the
plan never promised.

**Why (extensibility, simplicity).** Reschedule bursts after a node loss
race exactly like submit bursts, and the machinery is shared; changing
the plugin ABI is a compatibility decision that deserves its own plan.

## 16. The candidate walk rides the fused single-op apply (merge with the scheduler-performance work)

**Call.** The scheduler-performance PR fused plan-submission insert +
apply into one replicated op (`Store::submit_and_apply_plan`) while this
plan was in flight. The merge composes them: the production path calls
`submit_and_apply_plan_with_candidates` (format-2 insert + candidate walk
in one op; format-1 fused op when `plan_candidates: 0`), so the walk's
per-candidate epoch checks run inside the single quorum round-trip the
fusion left. The split two-step candidates API
(`insert_plan_submission_with_candidates` + `apply_plan_submission`)
stays for callers that genuinely stage submissions, and for tests.

**Alternatives.** Keep candidates on the two-op path and let only the
kill-switch path use the fusion — simpler merge, but the headline
workload (burst submits on raft) would pay the extra quorum round-trip
exactly where contention throughput matters most.

**Trade-off.** One more replicated op name (`submit_and_apply_plan_with_
candidates`) in the seam classification.

**Why (performance, simplicity).** Both changes exist to make the same
hot path cheaper; composing them is the only shape where neither undoes
the other's win.

## 17. Node-total capacity re-validation joins the walk as a skip class, not a rejection

**Call.** `main` grew an apply-transaction recheck that a placement still
fits the node's *current* totals — the fence against a worker
re-registration that shrinks capacity (or changes `reservedResources` /
`maxAllocations`), which bumps no allocation epoch and so is invisible to
the epoch compare-and-set. On `main` a failed recheck rejected the whole
plan. Merged into the walk, it is one more per-candidate skip: the
placement slides to the next candidate, and only an exhausted list
rejects. The free pool is rebuilt lazily, per candidate actually
attempted, like the node rows.

**Alternatives.** (a) Keep it a hard rejection even with candidates —
one shrunken node kills a plan whose other candidates were fine.
(b) Drop the recheck on the candidate path and keep it only under
`plan_candidates: 0` — cheaper, but reopens the oversubscription hole
the hardening work closed, exactly on the default path.
(c) Rebuild every candidate's pool up front — simple, but pays k× the
allocation scan per placement for rows the walk never looks at (the same
regression lazy node rows were introduced to avoid).

**Trade-off.** A capacity skip reads as a conflict to the eval loop, so
a fleet-wide shrink shows up as requeues rather than an immediate
terminal error. That is the correct posture — a replan against the new
totals is what the operator wants — but it costs one requeue delay
before the failure is legible.

**Why (simplicity, performance).** Every other "the world moved since
the snapshot" condition in the walk is a skip; making the newest one a
rejection would leave the walk with two rules for one idea. Laziness
keeps the recheck's cost proportional to the skips actually taken.

## 18. Candidate submissions ride the plan-apply group commit

**Call.** `submit_and_apply_plan_with_candidates` goes through the same
coalescer as `submit_and_apply_plan`: `PlanApplyRequest` carries a
`PlanPlacements` enum (format-1 nodes or format-2 candidate lists), and
the batch body dispatches per format. Each format keeps its own
replicated-op name, so the raft log and `tests/raft_seam.rs` still say
which walk ran.

**Alternatives.** Leave the candidate path on the single-plan replicated
op (what the two branches would have produced left alone). Since
candidate lists are on by default, that quietly retires the group commit
for every production plan — the batching would only ever engage under the
`plan_candidates: 0` kill switch.

**Trade-off.** One enum in the coalescer's request type, and a
delegating `submit_and_apply_plan_with_candidates_apply` body kept for
the seam classification.

**Why (performance).** The two changes optimize the same hot path for
the same workload — concurrent submits. A merge that keeps only one of
them under the default config wastes the other outright.

## 19. The lazy epoch sample covers the candidate union

**Call.** `main`'s lazy sample (plan first, then sample epochs for the
scopes the plan touched, accepted only when the read-model generation
proves nothing changed) samples the union of every placement's candidate
list, not just the placed-on nodes. Anything less and the walk would
compare a candidate against a default-0 epoch and skip it.

**Alternatives.** Force the eager whole-fleet sample whenever candidate
lists are on — correct, but throws away the O(affected) win on exactly
the default path.

**Trade-off.** The lazy sample is now O(placements x k) instead of
O(placements); still far below O(fleet) for any real fleet, and the
generation check that makes the late sample sound is unchanged.

**Why (performance).** Keeps both wins composable: the sample stays
proportional to what the plan touched, and "what the plan touched" is
exactly the set the walk may bind.

## 20. The fused scoring loop stays on the one-candidate path

**Call.** `main` replaced score-all/sort/shuffle with a fused
argmax-with-reservoir-tie-break loop that never materializes a scored
vector. Candidate lists need the scores to rank a band, so the ranked
path keeps the scored vector; the fused loop is kept and used whenever
the effective list length is 1 (`plan_candidates: 0`).

**Alternatives.** (a) Delete the fused loop — one code path, but the
kill switch would then be *slower* than the feature it exists to
disable. (b) Build a top-k fused loop so the ranked path never
materializes scores either — the right end state, but a rewrite of the
tail sampling, not a merge resolution.

**Trade-off.** Two selection paths in `select_node_candidates`,
distinguished by one branch on the setting.

**Why (performance, simplicity).** The kill switch must stay the cheap
path — it is what an operator falls back to under mixed versions. (b)
stays a backlog item, tracked with the candidate-construction benchmark
numbers that would justify it.
