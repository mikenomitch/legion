# Next Up: Current Focus

The pre-benchmark hardening batch (2026-07-30,
[calls](calls/pre-benchmark-hardening_calls.md)) is **done**: everything the
AWS comparative benchmark
([plans/aws-comparative-benchmark-plan.md](plans/aws-comparative-benchmark-plan.md))
wanted fixed or observable before a campaign, minus the deliberately
skipped items.

- **Correctness under benchmark-shaped load**: apply-time re-validation of
  node resource totals (the last unfenced plan input), raft-mode
  concurrent-planning oversubscription tests (both topologies), eval
  handling on the blocking pool, NotRaftLeader release backoff.
- **Measurement**: the full `legion_raft_*` engine series with
  capture-vs-propose bottleneck attribution, the opt-in worker `/metrics`
  listener, OTLP metrics push for both registries
  ([docs/metrics.md](../docs/metrics.md)).
- **Scheduler plan-path cleanups** with before/after numbers
  ([benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md)) —
  the TODO's "slowest bits" list is now empty; further wins want profiling.
- **Autoscaler phase 1** — `legion job scale`
  ([docs/autoscaling.md](../docs/autoscaling.md)): count overrides without
  a new version or rollout, the `scale` ACL resource, the `job_scale`
  eval. Gives the benchmark driver version-churn-free load control.

**Landed in parallel on main** (merged in 2026-07-30): `legion start worker
--simulate N` ([plan](plans/worker-simulation-plan.md)) and the AWS
benchmark rig's phase 1 — so the campaign's phase-0 blocker is *gone*, and
the remaining gate is an AWS account, not code. Jobs also gained a real
`updated_at`.

## The follow-up queue (in priority order)

1. ~~Feed simulated fleets into the `legion_worker_*` families~~ — **done**
   (2026-08-02): sim fleets start the shared worker telemetry listener and
   report into the process-global registry, and the AWS rig scrapes them
   (`LEGION_WORKER_TELEMETRY_PORT=9101`, a `legion-sim-workers` Prometheus
   job, a sim-fleet dashboard row). See TODO.md § Done → Observability.
2. Late-binding plan apply
   ([plan](plans/late-binding-plan-apply-plan.md)) — skipped in this batch
   by explicit decision; the contention benchmark (its phase 0) is the
   entry point.
3. Node pools consumed scheduling-side, gang scheduling, sysbatch — the
   feature-parity set the comparative campaign's fairness record will be
   read against (all planned, tracked in TODO.md § Scheduling).
4. Per-lane tuning knobs + a Raft health UI page (the rest of the old
   "knobs, tracing, UI" item).
5. Worker mTLS / per-node credentials (also upgrades raft transport auth).
6. Idempotency records folded into their mutating op's capture (retry
   atomicity — TODO.md, Persistence).
7. Autoscaler phase 2 (usage collection) — shares the worker-metrics
   surface started here; build with per-task runtime metrics.

Higher-level backlog: [TODO.md](TODO.md).
