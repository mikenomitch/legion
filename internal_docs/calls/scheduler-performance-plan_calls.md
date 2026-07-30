# Judgement calls: scheduler-performance-plan

Plan: [`../plans/scheduler-performance-plan.md`](../plans/scheduler-performance-plan.md)

## 1. Fuse plan-submission insert + apply into one replicated op

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
  `benchmarks/scheduler/README.md` and call #8.

## 2. Backend matrix lives in an integration-test runner, not Criterion

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

## 3. One scenario schema, v2 fields optional, v1 files keep parsing

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

## 4. Benchmark correctness now checks constraint conformance

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

## 5. `PlanTimings` phases are disjoint wall-clock slices, observed on every plan

- **The call:** eight timestamps in `plan_and_apply_internal`, a
  `PlanTimings` struct on `PlanOutcome`, one debug log line, and a
  `legion_scheduler_plan_phase_seconds{phase}` histogram observed on every
  plan (no sampling).
- **Alternatives:** tracing-span timing extracted by a subscriber
  (no struct); sampling the metric.
- **Trade-off:** eight `Instant::now()` calls + eight histogram observes
  per plan (~atomic ops, measured noise-floor); the struct must be kept in
  sync when the plan path changes shape (the fusion in call #1 already
  removed a field — the runner and metric phase lists updated with it).
- **Why it won:** the struct is what lets benchmark runners and the AWS
  test aggregate identical phase names without a tracing pipeline;
  always-on means production clusters carry the same breakdown the
  benchmarks do (extensibility + the plan's "same names everywhere" goal).

## 6. raft3 benchmark cluster uses relaxed election timing and drives the leader

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

## 7. Per-scenario SLO overrides instead of tightening the global gate

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

## 8. Benchmark numbers are recorded with host + state caveats, and cross-run deltas need same-state A/B

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

## 9. Scenario resources are pool units (MiB / MHz), not bytes

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
