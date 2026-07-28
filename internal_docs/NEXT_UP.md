# Next Up: Current Focus

The durability plan's workstreams
([plans/durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md))
are **done**, including the 2026-07-28 round:

- **Workstreams A + B** (2026-07-23): real Raft persistence (changeset
  replication, membership, snapshots, backup/restore) and operational
  table stakes (cordon/drain, version handshake, telemetry listener).
- **Workstream C** (2026-07-24): `topology: lanes` unblocked —
  single-writer table ownership, per-lane snapshots, voter-parity checks.
- **Workstream D + adversarial verification** (2026-07-28): standing-split
  resilience (worker-plane lane-write forwarding, accepted submits with
  per-lane reconcilers, honest `/readyz`), the lane-isolation A/B numbers,
  and the fix set from four skeptic sweeps (governor catch-up cap,
  remove-member hardening, seed-snapshot purge gate, orphan-sweep grace,
  plan-fence epoch mirror). Calls:
  [calls/durability-and-ops-execution-plan_calls.md](calls/durability-and-ops-execution-plan_calls.md).

## The follow-up queue (in priority order)

1. Per-lane tuning knobs, bottleneck tracing, Raft health UI — and finally
   emitting the reserved `legion_raft_*` Prometheus series.
2. Apply-time re-validation of node resource totals (the one plan-fence
   input still unfenced — see TODO.md, Scheduling).
3. Worker mTLS / per-node credentials (also upgrades raft transport auth
   beyond the shared cluster secret).
4. Idempotency records folded into their mutating op's capture (retry
   atomicity — TODO.md, Persistence).
5. OTLP metrics push + worker-side `/metrics`.
6. Raft autopilot — speculative, default-off.

Higher-level backlog: [TODO.md](TODO.md).
