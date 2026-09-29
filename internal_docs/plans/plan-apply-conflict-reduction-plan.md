# Plan-Apply Conflict Reduction

> **Judgement calls:** [`../calls/plan-apply-conflict-reduction-plan_calls.md`](../calls/plan-apply-conflict-reduction-plan_calls.md)

**Status: IN EXECUTION** — phases 1–3 landed together (two-pass bind,
candidate-list restoration, decorrelated fallback sampling). Phase 4
(re-measurement on AWS) is not run; the numbers below are still the
pre-change baseline. Deliberately deferred: per-component partial commit,
striping disjoint candidate lists across identical replicas, and any
adaptive tuning of eval-thread concurrency (that stays in
[adaptive-scheduler-concurrency-plan.md](adaptive-scheduler-concurrency-plan.md)).

## 1. Problem statement

The AWS phase-2 campaign (2026-08-04, see
[../aws-phase2-findings.md](../aws-phase2-findings.md)) found the
scheduler's ceiling, and it is not CPU, fsync, or raft. It is that the
scheduler rejects most of the work it plans.

`legion_plan_apply_total{outcome="rejected"}`:

| Configuration | Rejected |
| --- | --- |
| 4 eval threads, 1000 nodes | 15.9% |
| 4 eval threads, through 5000 nodes | 31.9% |
| 16 eval threads, through 5000 nodes | 73.6% |

Cumulative across the campaign: **4974 applied vs 14382 rejected**. Three
of every four plans built at the widest configuration were thrown away and
re-planned.

Two consequences follow, and both were measured:

- **Throughput inverts with concurrency.** Going from 4 to 16 eval threads
  *cost* 12% throughput at 1000 nodes, while CPU went from 29% to 85%.
  Adding schedulers makes the cluster slower and hotter, which is the
  opposite of the property the eval loop is supposed to have.
- **Throughput collapses with fleet size.** 354.6 alloc/sec at 1000 nodes
  falls to 86.6 alloc/sec at 5000 nodes, and p99 placement latency goes
  from 3.8s to 177s.

A rejected plan is not free: it re-runs the whole eval — node snapshot,
scoring, candidate construction — and re-enters the same contention it
just lost. At a 73.6% rejection rate the eval loop spends most of its CPU
on plans it will discard.

## 2. Root cause

Two independent mechanisms, both in code that was correct when written.

### 2.1 The overlap coupling edge collapses candidate lists

`Scheduler::decouple_plan_components` (`src/manager/scheduler.rs`) unions
any two placements whose candidate lists share a node, then shrinks every
non-singleton component to a **one-element** list. The intent was safety:
two independent fallbacks onto a shared node might oversubscribe it.

The effect at scale is that late-binding turns itself off. Any
multi-placement job into a constrained pool — a count-N service, i.e. the
most common job shape there is, and exactly what `web_fleet` submits —
has overlapping lists *by construction*, so every placement collapses to
its single chosen node and the plan degenerates to the pre-candidate
all-or-nothing CAS. The benchmark's rejection message says so directly:
"all 1 candidate(s) were stale or invalid".

### 2.2 The node epoch rejects placements that would have fit

`Store::apply_plan_submission_on` walks the candidate list and rejects a
candidate whose node epoch moved since the plan snapshot — *first*, before
any other check. But everything the epoch stands in for is already
re-derived inside that same serialized apply transaction, against
committed rows:

- the node row and its availability (a cordon bumps no allocation epoch,
  so this check had to exist anyway);
- constraints and permit, on the fresh row;
- capacity, from a free pool rebuilt in-transaction by
  `free_pool_for_node_tx` — `reservedResources` and `maxAllocations`
  included — with this plan's own binds debited as it goes;
- static ports, re-read by `assign_allocation_network`;
- `distinct`, from a tracker rebuilt in-transaction in
  `load_allocation_task_state`.

Job-level count double-spend is fenced separately and independently, by
the job-config epoch, which is a hard all-or-nothing check at the top of
the same function.

So a moved node epoch means "somebody else placed something here", not
"this no longer fits". Under contention those are very different
statements, and the walk was treating the first as the second.

## 3. Prior art

- **Nomad** plans through a single leader-side plan applier
  ([`plan_apply.go`](https://github.com/hashicorp/nomad/blob/main/nomad/plan_apply.go)):
  the leader *re-evaluates each allocation's fit* against the current
  state snapshot at apply time and commits the subset that still fits
  (`evaluatePlanPlacements`), rather than rejecting the whole plan on a
  version mismatch. Partial application is the norm there. We take the
  re-evaluation and keep our all-or-nothing per-plan commit.
- **Kubernetes** binds one pod at a time and lets the kubelet's admission
  and the API server's `resourceVersion` conflict handling sort out
  races; the scheduler's optimistic assume-cache is a *hint*, corrected by
  the authoritative check at bind. The parallel is exact: our node epoch
  is the hint, the in-transaction recheck is the bind-time authority.
- **Omega** (Google, [EuroSys '13](https://research.google/pubs/pub41684/))
  is the canonical statement of the trade-off: shared-state optimistic
  concurrency scales only if conflict detection is at the granularity of
  the *resource actually contended*. Detecting at "node touched" rather
  than "node full" is exactly the over-broad conflict granularity the
  paper warns produces livelock as parallelism grows — which is the
  inverted-throughput curve above.

What we reject: Nomad's partial plan application. A plan that cannot place
every placement still rejects wholesale, because partial commit interacts
with counts, groups, and rollouts in ways that need their own plan.

## 4. Jobs to be done

- **Operator running a busy cluster.** "I added scheduler capacity and it
  got slower. I need adding eval threads to add throughput, or at worst do
  nothing." Today they must know that eval-thread concurrency is
  self-defeating above ~4.
- **Operator diagnosing a stuck queue.** "Tell me whether my pool is full
  or my scheduler is fighting itself." Today both look identical: a rising
  `rejected` counter and a growing eval queue. After this change,
  `rejected` means *the cluster is out of room*, and the new
  `legion_scheduler_plan_bind_pass{pass="revalidated"}` counter shows
  contention being absorbed.
- **Developer of a count-N service.** "My 20-replica web service should
  schedule as fast as 20 one-replica jobs." Today it is far slower,
  because it is the exact shape that disables late-binding.

## 5. Goals and non-goals

**Goals**

1. A plan is rejected only when its placements genuinely do not fit.
2. Uncontended placement quality is unchanged, byte for byte.
3. Concurrent planners stop picking identical fallback nodes.
4. No safety property weakens: no oversubscription, no count double-spend,
   no ratification of a sandbox somebody else owns, deterministic raft
   replay.

**Non-goals**

- Partial plan application (Nomad-style). Out of scope; needs its own plan.
- Changing eval-thread concurrency, or making it adaptive.
- Gang scheduling / blocked-eval semantics for genuinely full pools.
- Any job-spec surface. This plan adds no user-facing configuration.

## 6. Background — what exists today

- `src/manager/scheduler.rs` — `build_plan` → `decouple_plan_components`
  (coupling components), `select_node_candidates` →
  `ranked_candidate_list` (rank 0 argmax, band-sampled tail, deterministic
  below-band fill), `candidate_budget` (index truncation budget).
- `src/storage/store.rs` — `apply_plan_submission_on` (job-config epoch
  gate, eviction savepoint, per-candidate walk), `free_pool_for_node_tx`,
  `run_plan_batch_txn` (group commit, per-plan savepoints).
- `docs/late-binding-plan-apply.md` — the public contract, which said in
  as many words that "the epoch *is* the fit proof, carried forward".

## 7. Proposed design

### 7.1 Two-pass candidate bind (`apply_plan_submission_on`)

The walk becomes two passes over the same ranked list:

- **Pass 1 — `clean`.** In rank order, candidates whose node epoch is
  unchanged. Byte-for-byte the old behavior, so an uncontended placement
  binds the planner's argmax exactly as before.
- **Pass 2 — `revalidated`.** In rank order, only the candidates pass 1
  deferred (epoch moved, or held by the replica rule below), admitted
  purely on the in-transaction revalidation that already ran.

A placement rejects only if both passes fail, and the rejection names the
real cause ("no longer has capacity", "does not satisfy constraints")
rather than staleness.

**`target_instance` keeps the hard epoch gate.** An edge-boot ratification
carries exactly one placement and asks "bind this sandbox on this node, or
tell me who beat me to it". There, a moved epoch may mean another plan
already owns the sandbox, which no capacity recheck can disprove, so pass
2 never runs.

**Replica anti-affinity, pass 1 only.** A `HashSet<(task_id, node_id)>` of
the plan's own binds keeps a task's replicas off a node this plan already
bound them to — but only at rank > 0. Rank 0 is exempt because the
planner's ledger already decided whether replicas share a node, and under
`bin_pack` stacking them is the intent.

**A rejected plan leaves nothing behind.** Found while writing the tests
for §7.2, and pre-existing: the walk inserts allocations as it binds, and
a rejection *commits* (it has to — it persists the `rejected` submission
row and its event). A plan that bound placement 1 and then found no
candidate for placement 2 therefore committed placement 1's allocation
row: a row no `allocation.placed` event announced, no read-model refresh
covered, and whose own submission says the plan did not happen. The
eviction savepoint is now a **body** savepoint (`legion_plan_body`)
covering the evictions *and* the placement walk, rolled back on rejection
before the rejection row is written. Opened only when the plan can
actually leave something behind — it has evictions, or more than one
placement.

**Pool-rebuild cap.** A plan may rebuild at most
`max(4 × placements, 256)` distinct node pools; past that, candidates
needing a fresh pool are skipped. This bounds the group commit's single
transaction against a pathological plan.

Metric: `legion_scheduler_plan_bind_pass{pass="clean"|"revalidated"}`.

### 7.2 Restore the candidate lists (`decouple_plan_components`)

Delete the overlapping-lists coupling edge. The `distinct`-key edge and
the co-placement-group edge stay: those constrain *which* nodes are
jointly acceptable, which the per-node capacity debit cannot express.

Safety for the deleted edge comes from the apply walk: each bind is
debited from that node's in-transaction free pool before the next
placement is considered, and static ports are re-read per bind. Two
placements landing on one node is therefore either fine (it fits) or
caught (it does not).

### 7.3 Decorrelate the fallback tail (`ranked_candidate_list`)

The below-band fill took the top `need` nodes with a deterministic
`select_nth_unstable_by`, so every concurrent planner produced the *same*
fallback list in the same order — the tail collided exactly as the head
did. It now takes the top `W` by score and samples `need` from them
without replacement, using the plan-time RNG, then restores score order so
a lower rank is never a better node.

`W = clamp(k, plan_apply_batch_max × placements-in-flight, |eligible|)`,
with the candidate-list length as the available stand-in for the second
term: **128 at the defaults** (batch max 16 × count 8).
`candidate_budget` widens to `max(needed × 8, 64, W)` so index truncation
cannot hand every planner the same narrow slice back.

All randomness stays at plan time on the planning manager. The apply walk
remains a pure function of committed state plus the submission, so raft
replicas replay identically.

## 8. Persistence, proto, and config changes

None. No new submission format, no schema change, no new
`schedulerSettings` key, no job-config field. The change is entirely in
how an existing submission is interpreted at apply.

## 9. Implementation phases

1. **Two-pass bind** (`store.rs`) + the `bind_pass` metric. Ships alone:
   it reduces rejections even with lists still collapsed to length 1,
   because a length-1 list with a dirty epoch now revalidates.
2. **Candidate-list restoration** (`scheduler.rs`). Depends on phase 1 for
   its safety argument (the pool debit is what replaces the overlap edge).
3. **Fallback decorrelation + budget widening** (`scheduler.rs`). Ships
   alone; it only changes which nodes ranks 1.. name.
4. **Re-measure on AWS.** Rerun the phase-2 campaign matrix and compare
   against the table in §1. *Not run.*

## 10. Testing plan

- `concurrent_plans_cannot_oversubscribe_a_node` and
  `tests/raft_apply_roundtrip.rs` pass **unmodified** — the load-bearing
  safety regression.
- New: an epoch-dirty candidate that still fits binds via pass 2; an
  epoch-dirty candidate that is full rejects *with a capacity reason*; a
  batch of overlapping plans in one `run_plan_batch_txn` all apply while
  capacity allows; two placements of one plan cannot oversubscribe a
  one-slot node, nor both claim one static port (the pair that replaces
  the deleted coupling edge); a rejected plan commits no allocations.
- Rewritten (they asserted the old contract, and are listed in the
  judgement calls): the shared-node-scope epoch test, the
  all-candidates-stale test, the batch CAS-verdict test, the two
  overlap-coupling unit tests, the shared-pool coupling test, and the
  lazy-sample test.
- Unchanged and still passing: the job-config epoch fences, the
  capacity-shrink recheck, the cordon and static-port skips, the
  candidate-rank reporting.

## 11. Rollout and backward compatibility

Default-on, no flag. Old submissions (format 1 and format 2) parse and
apply unchanged; `plan_candidates = 0` still restores single-node
submissions, which now also get the pass-2 revalidation. Mixed-version
manager fleets are unaffected: the change is apply-side interpretation of
a format that did not change. Under raft, all managers must run the same
version as usual — a follower on the old code would reject where the
leader applied, which the standard same-version upgrade order already
prevents.

## 12. Open questions

- **Is the replica anti-affinity rule worth keeping?** It fires only on
  fallbacks, but it is a policy opinion inside the store. Lean: keep it
  until the AWS rerun shows the fallback distribution, then delete it if
  the sampled tail already spreads replicas.
- **Should `W` come from a real placements-in-flight estimate** rather
  than the candidate count? Lean: only if the rerun shows fallback
  collisions persisting at 5000 nodes.

## 13. Acceptance criteria

- Phase 1: `bind_pass{pass="revalidated"}` is non-zero under contention,
  and no rejection message contains "stale" unless `target_instance` is
  set.
- Phase 2: a count-N job's placements carry candidate lists of length > 1
  in `legion job plan`.
- Phase 3: two planners racing the same pool produce different rank-1
  nodes.
- Phase 4: rejection rate at 16 eval threads / 5000 nodes materially below
  73.6%, and 4 → 16 eval threads no longer *costs* throughput.
