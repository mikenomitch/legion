# Judgement Calls — Pre-Benchmark Hardening (2026-07-30)

The batch of scheduler-correctness, eval-path, and observability work that
prepares the AWS comparative benchmark
([plans/aws-comparative-benchmark-plan.md](../plans/aws-comparative-benchmark-plan.md)):
apply-time resource re-validation, eval-path scheduling fixes, the raft
engine metrics, worker telemetry, and the OTLP metrics push. No single plan
owns this batch — each item was an open TODO — so the calls live here, named
after the branch's theme. **Entries are ranked by project impact; the number
is the rank.**

## 1. Apply-time capacity re-validation reconstructs the ledger in the apply transaction, instead of bumping epochs on node updates

**The call.** The plan-apply transaction rebuilds each placement node's free
pool from the fresh node row plus the allocation rows the transaction can
see (`ResourcePool::free_from_rows`), and rejects placements that no longer
fit. Node-update paths still bump no allocation epoch.

**The alternative.** Bump an allocation epoch whenever a node update changes
capacity (`resources`, `reservedResources`, `maxAllocations`), letting the
existing CAS catch the race. This is the structurally "pure" fix — one fence
for every input.

**The trade-off.** The recheck costs one `SELECT` per distinct placement
node plus pool arithmetic inside every apply — measured well under the
existing per-placement work, but nonzero on the hottest path. And the fix is
a backstop, not a fence: a capacity *increase* between snapshot and apply is
still not seen (harmless — plans only get more room).

**Why.** The epoch-bump alternative is a cross-lane write under `lanes`
(node updates are control-lane, allocation epochs scheduler-lane), which the
TODO itself flagged as "wants a plan" — a coupling this batch should not
smuggle in. The in-tx reconstruction is cheap now that reads are local to
the apply, closes the actual oversubscription window, and follows the
precedent ports set (also re-read in the apply tx). Simplicity: one
correctness mechanism (apply-time re-validation on the fresh row) now covers
availability, constraints, and resources uniformly.

## 2. The apply-time recheck mirrors the planner's ledger by excluding the replaced JOB's rows on Fresh plans

**The call.** The in-tx pool reconstruction excludes allocation rows
belonging to the planned config's *job* (via the `job_config` join) when the
submission's mode is `fresh` — exactly the planner's `drop_replaced` filter.

**The alternative.** Exclude only rows with the submission's own
`job_config_id` (no join), or exclude nothing and accept double-counting.

**The trade-off.** The mode-conditional join makes the recheck's SQL depend
on planner semantics; if the planner's Fresh-drop rule ever changes, this
query must change with it (the paired tests
`apply_rejects_when_node_capacity_shrank_since_the_plan_snapshot` /
`apply_capacity_recheck_does_not_double_count_a_fresh_plans_own_config` and
the pre-existing `fresh_replan_ignores_the_jobs_own_outgoing_allocations`
pin the coupling).

**Why.** Anything less than the planner's exact view makes the recheck
reject legal plans: a job resubmit whose task uses more than half a node
double-counts old + new and fails a plain redeploy — the same failure the
planner's own comment warns about. Config-id-only exclusion was tried first
and `fresh_replan_ignores_the_jobs_own_outgoing_allocations` caught it
(v1 → v2 resubmits carry a different config id, same job). Correctness of
the backstop is worth the coupling; the tests make it visible.

## 3. Eval handling moved to the blocking pool wholesale, instead of async-ifying the eval handlers

**The call.** `handle_claimed_eval` runs under `tokio::task::spawn_blocking`
(awaited, preserving per-worker ordering). `Eval::handle` and everything
under it stays synchronous.

**The alternative.** Make `Eval::handle` and `plan_with_conflict_retries`
async and replace `thread::sleep` with `tokio::time::sleep` — the
"idiomatic" fix.

**The trade-off.** Blocking-pool threads are a bounded shared resource
(default 512); eval handling now competes with other `spawn_blocking` users.
The retry sleeps still *hold a thread* — they just hold a blocking-pool
thread instead of a runtime worker.

**Why.** The handlers are blocking end to end — synchronous SQLite work and
raft proposals that block on quorum — so an async signature would be
cosmetic: every await point would still sit on blocking calls. Async-ifying
would also ripple through every eval call site (tests, server, store
seeding). Simplicity: one line at the dispatch boundary puts the whole
blocking pipeline where blocking work belongs.

## 4. NotRaftLeader retry attempts are tracked in process memory, not in the eval table

**The call.** The exponential backoff for foreign-lane NotRaftLeader
releases (5s → 60s cap, 1s jitter) counts attempts in an in-process map,
cleared on completion/terminal failure and reset by restart or failover. The
replicated eval schema is untouched.

**The alternative.** An `attempts` column on the eval table, bumped in
`release_eval_for_retry` — durable, exact across failovers.

**The trade-off.** The counter lies after a failover or restart (resets to
the 5s base), and entries stranded by leadership moves rely on a size guard
(clear at 10k) rather than precise cleanup — at the cost of one backoff
reset per stranded eval.

**Why.** Resetting on failover is *correct* here, not a compromise: new
leadership is exactly the condition the backoff waits for, so probing fast
again is the desired behavior. A schema change is an on-disk format change
(migration, replicated apply semantics, upgrade-skew questions) purchased
for a counter whose only job is log hygiene. Simplicity wins.

## 5. Raft latency metrics are labeled by the raft GROUP, not the logical lane, under `single` topology

**The call.** `legion_raft_capture_seconds`, `legion_raft_propose_apply_seconds`
and `legion_raft_ops_total` carry `lane="control"` for every op under
`single` topology (all lanes ride the one group), and the real lane name
under `lanes`.

**The alternative.** Label by the op's logical lane in both topologies, so
`single` clusters still see scheduler-vs-status write mix.

**The trade-off.** Under `single` topology the write-mix breakdown is lost;
you cannot tell scheduler writes from status writes in the histograms.

**Why.** The histograms measure the *replication pipeline* — commit latency
is a property of the raft group, and pretending a `single` cluster has three
pipelines would make the series lie exactly where they are meant to
attribute bottlenecks (comparing `lanes` vs `single` A/B runs). The gauge
series (term, indexes) already have this shape: one `control` series under
`single`. Consistency beats granularity here; the debug trace still carries
the op name for mix questions.

## 6. Worker telemetry is enabled by env var, not worker config

**The call.** The worker's Prometheus listener is opt-in via
`LEGION_WORKER_TELEMETRY_PORT`, binding `0.0.0.0:<port>`; there is no config
field and no default port.

**The alternative.** A `telemetry { port }` block in the worker-type config,
mirroring the manager's `telemetry.port`, defaulted on.

**The trade-off.** Two configuration idioms for the same concept (manager:
config block; worker: env var), and fleet-wide enablement needs an env var
in the provisioning template rather than one config line on the manager.

**Why.** Workers joined with a bare token have no config file — the
worker-type config arrives from the manager, and a *manager-pushed* port
would be wrong for exactly the fleets that need this most: benchmark hosts
packing many worker processes, where every process needs its own port (or
port 0). An env var set per process by whatever launched it matches the
deployment reality. Revisit if worker-type config grows per-process
overrides.

## 7. OTLP metrics push serializes the hand-rolled registry to OTLP protobuf directly, instead of adopting the OpenTelemetry metrics SDK

**The call.** The push walks a structured snapshot of the existing registry
(`FamilySnapshot`) and builds `ExportMetricsServiceRequest` protobufs by
hand (via `opentelemetry-proto` message types), POSTed with the existing
reqwest client.

**The alternative.** Register everything as OpenTelemetry instruments and
let the SDK's `PeriodicReader` + OTLP exporter own aggregation and export.

**The trade-off.** We own the OTLP mapping (temporality, bucket shapes,
resource attributes) and must track proto conventions ourselves; none of
the SDK's niceties (views, exemplars, delta temporality) come along.

**Why.** The registry is deliberately dependency-free and Prometheus-first
(docs/metrics.md tenet), and the OTel SDK cannot ingest pre-aggregated
histograms — bridging would mean either double bookkeeping on hot paths or
losing bucket fidelity (observable instruments can't carry histograms).
Hand-building the protobuf keeps one source of truth, full fidelity
(cumulative sums, real bucket counts), and zero new hot-path cost; the push
is ~150 lines against stable proto types.

## 8. The raft-mode concurrency test accepts either winner, asserting only the invariant

**The call.** `concurrent_planners_cannot_oversubscribe_under_raft[_lanes]`
treats a plan-conflict loss as success for either racer and asserts exactly
one allocation lands — it does not assert which job wins or that a conflict
even occurs (a serialized interleaving where both plans fit... cannot
happen here since the node fits one; but timing may serialize the race).

**The alternative.** Force the interleaving deterministically (pause points
/ fail-points inside `plan_and_apply`) so the epoch-mirror window is
provably entered every run.

**The trade-off.** On a quiet machine the two planners may serialize and the
test passes without exercising the contested window — it is regression
insurance in expectation, not a proof per run.

**Why.** Legion has no fail-point infrastructure, and adding one for a
regression test is disproportionate (and itself a scheduling perturbation).
The barrier release plus raft's commit-then-project asymmetry makes the
window overwhelmingly likely across CI runs, and the invariant asserted is
the one that matters. Matches the posture of the existing local_disk twin.
