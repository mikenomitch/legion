# Judgement Calls — Multi-Raft Persistence

Companion to
[`plans/multi-raft-persistence-plan.md`](../plans/multi-raft-persistence-plan.md).
The decisions the plan makes where a real alternative existed, pulled out of the
design prose so they can be reviewed (and reversed) without re-reading the plan.
Section references point back at the plan.

Entries are ranked by project impact, highest first.

## 1. Lanes partition by write *class*, not by feature (§1)

Three groups — `control`, `scheduler`, `status` — chosen so each lane's writes are
homogeneous: operator/user-driven and bursty, placement critical-path, and
high-volume worker→manager status. The rejected alternative is a lane per
subsystem ("SD gets one, then secrets, then autoscaling…"), which slides straight
back into the operational complexity multi-raft exists to remove: every lane is
another leader, election timeout, snapshot pipeline, and failure mode. A new lane
is justified only by a genuinely new write class.

## 2. Option A — one SQLite file, lane-scoped apply (§4)

Recommended over one file per lane. Cross-lane reads stay ordinary SQL joins and
`Store` needs minimal surgery; the cost is that snapshot logic must scope to a
table subset and one file is a shared fsync surface (mitigated by WAL + per-lane
batching). The isolation that matters — write-path isolation — comes from
independent logs and apply loops, not separate data files. Option B (file per
lane) stays documented as the fallback if a single WAL fsync becomes the
bottleneck; adopting it later does not change the lane model.

## 3. Multi-raft is default-on, with a collapse switch (§7)

`persistence.type = "raft"` means the three default lanes with no extra config;
`raft.topology = "single"` collapses everything into one group for tiny clusters
and debugging. A node joins once and is enrolled into *all* lanes — operators
never hand-assign lanes. The alternative (opt-in lanes) would leave the good
configuration unused by default.

## 4. Cross-lane staleness is surfaced, not eliminated (§5)

Lanes commit independently, so a placement may briefly run against a slightly
stale job_config. Rather than adding cross-lane transactions, the plan leans on
what already exists: the `SchedulingReadModel` projects all three lanes into one
view, and the `allocation_epoch` CAS rejects placements computed against stale
state. Cross-lane lag shows up as an occasional epoch conflict + retry — not a
correctness bug — and is exposed as a `lane_apply_lag` metric. Read-your-writes is
guaranteed within a lane; across lanes it is read-after-commit with bounded
staleness.

## 5. Service discovery does **not** get its own lane (§1a)

SD fragments cleanly across the existing three: definitions and intentions on
**control**, health on **status**, registration **derived**, resolution served
from the read model. The tempting case (health-check volume, data-path
criticality) fails on inspection — the critical SD path is a *read*, already
isolated from every write log, and SD health writes *are* the status write class.
Escape hatch for SD-heavy fleets: `raft.lanes.serviceDiscovery = true`, **default
off**. If the health firehose ever dominates, the right levers are general
status-lane ones (replicate debounced state transitions à la Consul; shard the
status lane), not an SD-shaped lane.

## 6. Node liveness splits out of the `node` table (§1)

The `node` table mixes identity (registration, attributes — control-lane work)
with liveness (last-heartbeat, status — extremely high frequency). Leaving
heartbeats in the control lane would reintroduce exactly the contention being
eliminated, so heartbeat-driven liveness moves to a new `node_health` table on the
**status** lane while registration stays on **control**. The scheduler reads a
join of the two through the read model. Smallest change that keeps each lane
homogeneous.

## 7. Endpoint registration is a derived projection, not a replicated write (§1a)

A live endpoint is `allocation ⋈ node(address) ⋈ service definition`, all of which
are already replicated, so the read model can project the endpoint set with no new
write and no cross-lane atomicity problem. Only if registration ever needs state
the allocation lacks (app-reported readiness) does it materialize — and then it
co-commits on the **scheduler** lane so an allocation and its endpoint land
atomically, never an orphan endpoint on a dead alloc.

## 8. Lane assignment is centralized and test-enforced (§1)

One `Lane` enum plus a `lane_for(command)` classifier, with a `#[test]` asserting
the table↔lane mapping is total and disjoint. The alternative — scattering lane
choice across call sites — makes the partition unverifiable.

## 9. Scheduler-lane key-range sharding is optional and flagged (Phase 8)

Sharding the scheduler lane is real headroom but a genuine complexity step, so it
is a separate, flag-gated phase rather than part of the default topology.

## 10. Autopilot is speculative and default-off (Phase 12)

Auto-tuning Raft parameters is kept explicitly speculative: shipped off, and only
after the tuning knobs, diagnostics, and docs of Phases 9–11 exist to explain what
it would be changing.
