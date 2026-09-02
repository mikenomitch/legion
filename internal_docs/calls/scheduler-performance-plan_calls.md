# Judgement calls: scheduler-performance-plan

Plan: [`../plans/scheduler-performance-plan.md`](../plans/scheduler-performance-plan.md)

Entries are ranked by project impact, highest first; the number is the rank.

## 1. The read model keeps a per-job allocation index, so a plan never walks the cluster's allocations

- **The call:** `SchedulingReadModel` carries a secondary index,
  `allocations_by_job` (job → node → `[AllocationRef]`), maintained by the
  same operations that mutate `allocations_by_node` (every splice indexes
  or unindexes its row, every whole-list replacement and node removal goes
  through one helper, the full load rebuilds it). `plan_capacity_snapshot`
  hands the planned job's entry to the scheduler, which derives everything
  it used to walk the whole cluster for — the rows a Fresh plan drops, the
  counts a Reschedule needs, group pinning, `distinct` seeding, and the
  scoring cache's per-node task presence — from that one small map.
- **Alternatives:** (a) keep walking `allocations_by_node` but fuse the
  five-to-six walks into one (this was the first step, and it took the
  fill slope from ~4.5 ms to ~2.1 ms per 200k allocations); (b) ask the
  database for the job's rows (`idx_allocation_job_config_id`) instead of
  the read model, which is O(job) but reads a source that can be ahead of
  or behind the snapshot the plan is built against; (c) a persistent
  (structurally shared) map for the snapshot itself, a new dependency.
- **Trade-off:** one more derived structure the read model has to keep
  right on every publish path, on every replica, plus ~40 bytes per
  allocation. A stale entry can only cost a needless re-test of one
  node's list on the drop side (the drop re-checks the actual rows), but
  on the count side it would let a Reschedule miscount its own rows — so
  the existing read-model reload-equivalence tests now assert the index
  equals a rebuild after every publish path they exercise (splice, drop,
  scrub, wholesale reload, node removal, full rebuild).
- **Why it won:** it was the last per-plan cost that grew with cluster
  fill rather than with the job being placed: `plan_under_fill` measured
  `plan_only` at 2.55 ms with an empty fleet and 7.07 ms at 40
  allocations per node on the same 5,000 nodes, with the walks the whole
  difference. (b) trades coherence for speed and the plan fence is built
  on snapshot coherence; (c) buys a dependency for a cost the index
  removes outright. Performance goal, with the invariant kept
  mechanical rather than remembered (simplicity).

## 2. A locally committed plan publishes its rows through the read model's delta path

- **The call:** `emit_plan_effects` builds an `AllocationDelta` from the
  rows the plan itself inserted and deleted and calls
  `cache_apply_allocation_delta`, instead of `cache_refresh_allocation_scopes`
  reloading every touched node's whole allocation list from the database.
- **Alternatives:** keep the full per-node reload (self-healing by
  construction); or splice the plan's own `Allocation` values straight
  into the model without re-reading them.
- **Trade-off:** the delta path is not self-healing for rows it does not
  name — it detects a contradiction and reloads that node, and it scrubs
  one node per application, which is the same bound raft replicas already
  live with. Re-reading the placed rows by id (one indexed read each) was
  kept over splicing the in-memory values so the published row is exactly
  the committed one, network assignment and all.
- **Why it won:** under `local_disk` the post-commit refresh re-read and
  re-normalized O(allocations on each touched node) — invisible to the
  plan-phase timers because it runs after the commit, and growing with
  fill. The raft apply path already projected exactly this delta from the
  committed changeset, so this unifies the two backends on one
  read-model maintenance path rather than adding a second (simplicity;
  performance).

## 3. Per-placement passes stop re-answering questions the candidate list already answered

- **The call:** `eligible_nodes` no longer re-runs the task's permit over
  every candidate on every placement (the candidate list was built by a
  pass that applied constraints, permit, and volume-driver support once,
  and none of the three change during a plan), and `PlanScoringCache`
  keys its per-node state by the candidate's address (`NodeKey`, a
  multiplicative hash of the `&Node` pointer into the plan's shared
  snapshot) in a `Vec` parallel to the candidate list, instead of by id
  string in a `HashMap<String, _>`. `PermitSet::is_permitted` itself takes
  an allocation-free path for nodes without a `restriction` block.
- **Alternatives:** keep the per-placement permit check as a cheap safety
  net; carry candidate indices through `eligible`/`scored` lists instead
  of an address-keyed map; a faster string hasher for the id-keyed map.
- **Trade-off:** the invariant "every candidate list handed to a placement
  was permit-filtered" is now load-bearing and documented on
  `eligible_nodes` rather than enforced per placement; the address key is
  an unusual idiom (never dereferenced, only compared — and the snapshot's
  `Arc<Node>`s cannot move while the plan holds them). Index-carrying
  lists would remove even the pointer hash but touch every scoring entry
  point; the address map removes the string hashing with no API churn.
- **Why it won:** the permit check materialized a restriction map per
  candidate per placement — at 1,000 nodes and 100 placements per plan
  that was ~100k map allocations per plan, and the `scheduler_speed`
  1,000-allocation cells halved (254 → 121 ms; 365 → 200 ms at 10k
  nodes). Performance goal; no user-visible surface.

## 4. Every per-node and per-plan statement on the apply and eval paths goes through the statement cache, and scope epochs read before they seed

- **The call:** `bump_scope_epoch_tx`, the node-usage fingerprint and cache
  reads, the usage upsert, the plan-submission insert/select/update, the
  task-state load, the eval claim/complete/count statements, and the
  scheduler's small per-plan reads (`job_band`, `task_statuses`, `job_id`,
  pinned placements, scale/instance counts) use `prepare_cached`;
  `scope_epoch_tx` reads first and only seeds a row on a miss.
- **Alternatives:** leave the raw `execute`/`query_row` calls (they are
  correct) and rely on the earlier partial cache pass; or hoist prepared
  statements into long-lived struct fields.
- **Trade-off:** a cached statement holds a slot in the per-connection
  cache (sized to 128, so no eviction pressure) and the code reads
  slightly noisier (`prepare_cached(..)?.query_row(..)`); the read-first
  epoch changes the statement order on a miss (SELECT then INSERT instead
  of INSERT then SELECT) but not the rows written.
- **Why it won:** each uncached call re-parsed SQL — several parses per
  node per plan inside the serialized commit, and one per eval on the
  claim/complete path. The eval critical path bench dropped 23-28% at 1/4/8
  workers from this alone. Performance goal; mechanical change.

## 5. The fill axis gets its own benchmark, filled through the store rather than the scheduler

- **The call:** `plan_under_fill` pre-fills a 5,000-node fleet with 0/10/40
  allocations per node from filler jobs via `Store::apply_allocation_plan`
  (chunked by node range so every node's list reloads once), then measures
  a six-placement job as `plan_only` (planning phases only, no state
  drift) and as `plan_and_apply` (a fresh job per iteration, per-phase
  means printed after the cell).
- **Alternatives:** fill through `plan_and_apply` (what `scheduler_speed`
  does, at ~35 s per 100k allocations); extend the scenario runner with a
  fill knob; measure only on the AWS rig where fill was first observed.
- **Trade-off:** the store-side fill bypasses the planner (so the fill
  itself is not a scheduling measurement, by design) and `plan_and_apply`
  under fill still pays two local commits per plan (apply plus the inline
  status projection the daemon defers to a drain task), and swings more
  run to run on a VM than the read-only cell — the `plan_only` cell is
  the one to read for planning cost, the phase means for where apply time
  goes.
- **Why it won:** neither existing micro group starts from a filled
  cluster, so the slope the AWS `dense_fleet` ladder measured was
  invisible locally; a 40-second local cell now reproduces it and is the
  regression test for this whole change set (performance goal;
  extensibility of the benchmark suite).

## 6. Fuse plan-submission insert + apply into one replicated op

- **The call:** `Store::submit_and_apply_plan` runs the `plan_submission`
  insert and the plan apply inside a single `replicated()` op; the
  scheduler's `plan_and_apply` path uses it. The two-step
  `insert_plan_submission` / `apply_plan_submission` API remains for
  callers that genuinely stage submissions (tests, the eval critical-path
  bench's staged form).
- **Alternatives:** keep the two-step path and instead batch applies
  *across* evals (group commit); or stop persisting the submission row on
  the critical path entirely.
- **Trade-off:** the fused op couples two writes in one changeset (two
  SQLite transactions inside one raft capture); the `PlanTimings`
  `insert_submission` phase disappears into `apply`. Group commit would
  win more under concurrency but is an architecture-level change
  (review plan D5); dropping the row would lose the plan audit trail.
- **Why it won:** the backend matrix measured the separate insert as a
  full quorum round-trip costing 0.3-2.1 ms per plan on raft — pure
  overhead for a row consumed microseconds later. Fusing is the largest
  structural win available without changing architecture or semantics
  (performance goal; epoch CAS semantics untouched). Measured result: a
  controlled same-state Criterion A/B shows local_disk neutral-to-
  slightly-positive (−1.9% at 1k nodes, p=0.04); the raft win is
  structural (one quorum round-trip instead of two) but host variance
  kept scenario runs from quantifying it — see the 2026-07-29 section of
  `benchmarks/scheduler/README.md` and call #13.

## 7. Backend matrix lives in an integration-test runner, not Criterion

- **The call:** the scenario × backend matrix is
  `tests/scheduler_scale_benchmark.rs` (`#[ignore]`, env-driven, prints a
  table + writes JSON), not a Criterion bench.
- **Alternatives:** parameterize the existing Criterion benches over
  backends.
- **Trade-off:** no Criterion statistics (outlier detection, baselines);
  we trade that for scenario realism, correctness gating inside the run,
  raft-cluster lifecycle control, and machine-diffable JSON output.
  Criterion still owns the micro benches (`candidate_construction`).
- **Why it won:** a 3-node raft cluster per (scenario × sample) inside
  Criterion's iteration model is wall-clock prohibitive and measures
  cluster bring-up, not scheduling. Simplicity of one runner shared with
  the CI gate also keeps the two from drifting.

## 8. One scenario schema, v2 fields optional, v1 files keep parsing

- **The call:** `node_profiles`/`job_types`/`scheduler_settings`/`slo` are
  optional fields on the same struct; a v1 file is one uniform profile +
  one job type. Mutual-exclusion (`node_count` xor `node_profiles`) is
  asserted at load.
- **Alternatives:** a `version` field with distinct schemas; a separate
  scenarios-v2 directory.
- **Trade-off:** an Option-heavy struct and two ways to say the same
  thing, versus migration churn and a broken benchmark history.
- **Why it won:** the three legacy scenarios keep gating CI unchanged
  (continuity of recorded numbers), and the runner stays one code path
  (simplicity).

## 9. Benchmark correctness now checks constraint conformance

- **The call:** a scenario cell only counts if every job has its full
  allocation count on active nodes **and** every constrained task's
  allocations sit on nodes whose attrs satisfy the constraints; the scale
  runner hard-asserts correctness = 1.0.
- **Alternatives:** keep the old count-only check.
- **Trade-off:** a scheduler bug now fails benchmark runs (by design);
  equality-only constraint semantics in the checker (what the scenarios
  use), not the scheduler's full expression language.
- **Why it won:** "fastest" is only a claim if the work is right — a fast
  misplacing scheduler must fail the benchmark, not set a record.

## 10. `PlanTimings` phases are disjoint wall-clock slices, observed on every plan

- **The call:** eight timestamps in `plan_and_apply_internal`, a
  `PlanTimings` struct on `PlanOutcome`, one debug log line, and a
  `legion_scheduler_plan_phase_seconds{phase}` histogram observed on every
  plan (no sampling).
- **Alternatives:** tracing-span timing extracted by a subscriber
  (no struct); sampling the metric.
- **Trade-off:** eight `Instant::now()` calls + eight histogram observes
  per plan (~atomic ops, measured noise-floor); the struct must be kept in
  sync when the plan path changes shape (the fusion in call #6 already
  removed a field — the runner and metric phase lists updated with it).
- **Why it won:** the struct is what lets benchmark runners and the AWS
  test aggregate identical phase names without a tracing pipeline;
  always-on means production clusters carry the same breakdown the
  benchmarks do (extensibility + the plan's "same names everywhere" goal).

## 11. raft3 benchmark cluster uses relaxed election timing and drives the leader

- **The call:** the in-process 3-node cluster uses 2-3 s election
  timeouts (vs 250/600-1200 ms production defaults) and the runner sends
  all writes to the current leader's store handle.
- **Alternatives:** production timings; scheduling through a follower with
  forwarding; measuring failover mid-benchmark.
- **Trade-off:** the cell measures steady-state quorum-commit cost only —
  leadership flaps and failover gaps are excluded on purpose (they'd
  poison latency samples with election waits; the AWS plan owns failover
  measurement).
- **Why it won:** same reasoning as `tests/raft_lanes.rs` — an in-process
  cluster on a loaded machine flaps at production timings, and a flapping
  benchmark measures the flap, not the scheduler.

## 12. Per-scenario SLO overrides instead of tightening the global gate

- **The call:** `slo.json` stays as the (loose) global default; each new
  realistic scenario carries its own tighter-or-looser bounds sized to its
  scale.
- **Alternatives:** tighten the global thresholds toward measured
  baselines (the earlier review of this gate called them 7-100x loose).
- **Trade-off:** the legacy scenarios keep their loose gate, so the gate
  still won't catch a moderate regression on them; CI stability across
  heterogeneous runner hardware is preserved.
- **Why it won:** CI runs debug builds on variable runners — a
  baseline-ratcheted gate flakes, and a flaky gate gets deleted. The
  honest regression instrument is the recorded release-build matrix in
  `benchmarks/scheduler/README.md`, not the CI smoke bound. Revisit after
  a few weeks of recorded numbers exist.

## 13. Benchmark numbers are recorded with host + state caveats, and cross-run deltas need same-state A/B

- **The call:** the recorded tables in `benchmarks/scheduler/README.md`
  state host, build profile, and the observed run-to-run variance; the
  fused-apply before/after claim rests on a stash/unstash same-state
  Criterion A/B rather than on scenario-matrix deltas.
- **Alternatives:** publish the most favorable cross-run comparison
  (early runs suggested raft throughput roughly doubling — the numbers
  were real, the comparison was not: one side ran while a test suite
  finished, and this laptop drifts 30-40% on unrelated phases between
  sustained runs, with no OS thermal event recorded).
- **Trade-off:** the recorded win reads "structural + measured-neutral"
  instead of "2x", which is less quotable.
- **Why it won:** the entire point of this plan is a *defensible*
  "fastest at scale" claim; one over-claimed table would taint every
  future one. The AWS rig exists to produce authoritative steady-state
  numbers on dedicated hardware.

## 14. Scenario resources are pool units (MiB / MHz), not bytes

- **The call:** `job_types[].resources` uses the scheduler's pool units:
  `memory` in MiB, `cpu` in MHz (fingerprint frequencies summed).
- **Alternatives:** accept bytes and convert in the runner.
- **Trade-off:** unit mistakes in a scenario fail placement loudly (the
  first draft of these scenarios requested "1 GiB" as bytes and asked for
  a TiB — caught because placement failed, which is the failure mode
  working as intended).
- **Why it won:** matching `TaskConfig.resources` semantics exactly means
  scenario files are copy-pastable to real jobspecs and vice versa
  (simplicity; no hidden conversion layer).
