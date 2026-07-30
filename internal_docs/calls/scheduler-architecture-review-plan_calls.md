# Judgement calls: scheduler-architecture-review-plan

Plan: [`../plans/scheduler-architecture-review-plan.md`](../plans/scheduler-architecture-review-plan.md)

These are planning-stage calls (the plan proposes; nothing ships from it).

## 1. Keep SQLite as the authoritative store; refuse the in-memory rewrite

- **The call:** the review explicitly rejects (for now) the
  Nomad/etcd-style "authoritative in-memory state, log as truth" shape
  (plan D3), despite it being the largest possible apply-phase win.
- **Alternatives:** commit to the rewrite; or leave it un-discussed.
- **Trade-off:** Legion accepts a permanently higher per-apply floor
  (µs-vs-ms class) and must win the same ground via fusion, batching, and
  fewer applies.
- **Why:** queryable-SQLite state is a load-bearing operability
  differentiator (simplicity goal outranks a benchmark cell); and the
  measured data shows the cheaper attacks (fused apply already landed,
  group commit proposed) recover most of the gap. Recording the refusal
  explicitly is the point — the AWS report can cite it honestly.

## 2. Rank the O(fleet) version sample and group commit above candidate sampling

- **The call:** D4 (lazy epoch sample) and D5 (apply group commit) rank
  above D1 (bounded candidate scoring), though D1 is the
  industry-standard scale lever.
- **Alternatives:** lead with D1 (what Nomad/K8s would do first).
- **Trade-off:** the top two items don't flatten `build_plan`'s fleet
  scaling; D1 eventually still has to happen for 50k+ fleets.
- **Why:** the 2026-07-29 matrix says apply + version-sample are where
  the milliseconds actually are at ≤10k nodes, and neither costs
  placement quality; D1 trades quality for speed and so needs its
  packing-quality gate built first (performance with honesty).

## 3. Single active scheduler stays; the parallel path is designed but deferred

- **The call:** keep one scheduling manager; document the
  follower-plans/leader-applies escalation (D2) rather than start it.
- **Alternatives:** begin the plan-queue architecture now, before the
  AWS data exists.
- **Trade-off:** aggregate-throughput headroom stays untapped; if the AWS
  test shows single-manager saturation, this becomes the critical path.
- **Why:** the partitioned-epoch CAS already is the multi-scheduler
  admission rule, so deferral costs no rework later; and the operational
  complexity of distributed planning is only worth paying on evidence
  (simplicity until performance demands otherwise).
