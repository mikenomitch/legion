# Next Up: Current Focus

The two workstreams of
[docs/plans/durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md)
are **done** (2026-07-23):

- **Workstream A — Real Raft persistence** shipped: `persistence.type =
  "raft"` now quorum-replicates every store mutation via changeset capture
  through openraft, with real membership/join tokens, snapshots +
  compaction + crash recovery, leader-only scheduling, non-leader write
  rejection with a leader hint, and `legion state backup` /
  `--restore-from`. Operator docs: [docs/multi-raft.md](multi-raft.md),
  [docs/raft-operations.md](raft-operations.md),
  [docs/upgrades.md](upgrades.md).
- **Workstream B — Operational table stakes** shipped: node
  cordon/drain/uncordon, the worker↔manager version handshake, and the
  manager `/metrics` + `/healthz` + `/readyz` telemetry listener
  ([docs/metrics.md](metrics.md)).

## The follow-up queue (from the plan's §6, in priority order)

1. `node` liveness split (`node_health` table) → make `topology=lanes`
   runnable (it is currently refused at engine start) → per-lane
   snapshots → flip the default.
2. Lane-isolation benchmark + tuning profiles + diagnostics + UI verdict
   layer — including finally emitting the reserved `legion_raft_*`
   Prometheus series.
3. Worker mTLS / per-node credentials (also upgrades raft transport auth
   beyond the shared cluster secret).
4. Auto-forward writes to the raft leader (today: reject + hint).
5. OTLP metrics push + worker-side `/metrics`.
6. Raft autopilot — speculative, default-off.

Higher-level backlog: [docs/TASK-LIST.md](TASK-LIST.md).
