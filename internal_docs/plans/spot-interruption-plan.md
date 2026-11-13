# Plan: Spot Interruption Notices

> **Judgement calls:** [`calls/spot-interruption-plan_calls.md`](../calls/spot-interruption-plan_calls.md)

**Status: SHIPPED (2026-11-12).** Built as designed below. Deliberately not
built: providers other than AWS (an operator or handler runs `legion node
interrupt`), acting on the deadline (draining non-gang work, timing the move),
and checkpoint-before-move. Public doc:
[`docs/spot-interruption.md`](../../docs/spot-interruption.md). Origin:
architecture review AI7, deferred in
[placement-and-commit-cost-plan.md §6.6](placement-and-commit-cost-plan.md);
parent plan [gang-scheduling-plan.md](gang-scheduling-plan.md) (Revision 3).

## 1. Problem statement

A spot node's reclaim reaches Legion today only as a lost node, after the
fact. A gang member on it is then lost, its peers stall in a collective until
the node is declared lost, and the gang restarts — minutes late. Worse, a
fresh gang can be placed onto a node that already has a notice. EC2 gives two
minutes' warning through the instance metadata service; nothing reads it.

## 2. Prior art

- **AWS Node Termination Handler** (Kubernetes): polls IMDS
  `spot/instance-action`, cordons and drains the node.
- **Karpenter**: consumes interruption events from SQS/EventBridge, taints and
  drains.
- **Nomad**: no built-in handling; users run a handler that drains.
- **Ray**: autoscaler marks the node and restarts actors elsewhere.

Taken: the IMDS poll (no AWS credentials or queues needed), and a node
condition the scheduler honors. Rejected: draining everything by default (a
drain of non-gang work is a policy decision an operator can already make, and
a two-minute drain of a stateful task can do more harm than the reclaim).

## 3. Jobs to be done

- **Platform operator:** "Run training on spot without every reclaim costing a
  stalled gang and a late restart." / "Let my own termination handler tell
  Legion a node is going."
- **ML engineer:** "When a spot node is reclaimed, my gang should restart on
  healthy nodes, not wait to notice."
- **Agent:** "Record that a node is going, through the tools I already have."

## 4. Goals and non-goals

Goals: read EC2's notice on the node; record it on the node; keep gangs off
interrupted nodes; move gang members off them at once through the gang's own
`onMemberLoss`; an operator path for every other source.

Non-goals: other cloud providers in the worker; draining non-gang work;
waiting for a checkpoint; scheduling around predicted interruptions.

## 5. Background — what exists today

- Node dynamic attributes and `UpdateNodeDynamic`
  (`crates/manager/src/manager/server.rs`), with `legion node update`.
- Gang member loss on lost/drained nodes:
  `Store::gang_members_lost_on_node` (`store/gang_losses.rs`).
- Worker-type config rides to workers in `WorkerArtifactsPayload`.

## 6. Proposed design

### 6.1 The notice

A node dynamic attribute, `legion.interruption = { deadline, source, detail
}`, `null` when withdrawn. An attribute rather than a new node column: it is
visible in `node info`, usable in constraints, settable through the existing
RPC and MCP tool, and needs no schema change.

### 6.2 Sources

- Worker: `spotInterruption = { provider = "aws", endpoint?, pollEvery? }` in
  the worker config. The worker polls IMDSv2 (v1 fallback) and reports
  `HeartbeatRequest.interruption` while a notice stands; the manager writes
  the attribute when it changes and withdraws it (unless an operator set it)
  when the worker stops reporting one.
- Operator: `legion node interrupt <id> [--in 2m] [--detail ...] | --clear`
  (writes the attribute through `UpdateNodeDynamic`).

### 6.3 Effects

- The planner drops interrupted nodes from a gang task's candidates.
- When a notice lands (either source), every gang member on the node is
  removed with the new stop cause `interruption`, and its gang's
  `onMemberLoss` applies, as for a lost node (no move budget). Non-gang work
  is untouched.

## 7. Proto, struct, config, persistence

`HeartbeatRequest.interruption` (`NodeInterruptionNotice`), `StopCause::
Interruption`, `WorkerSpotInterruptionConfig` in `WorkerConfigAlt` and
`WorkerArtifactsPayload`, `INTERRUPTION_ATTR`, the CLI verb and capability
`node.interrupt`. No migration.

## 8. Implementation phases

One phase; the watcher alone is visible but useless without the effects, and
the effects alone are reachable by the operator verb.

## 9. Testing plan

Unit: notice parsing; the watcher against a fake metadata server (notice,
404, error); config validation; planner skips interrupted nodes for gangs
only; the move restarts a gang and leaves other work.

## 10. Rollout

Opt-in per worker type for the watcher; the attribute's effects are on for
every cluster (a node only has one if someone set it).

## 11. Open questions

- Should a notice also cordon the node for all work? Lean: no; the operator's
  handler can drain, and Legion should not take that call for stateful work.

## 12. Acceptance criteria

A node with a notice takes no gang; its gang members move at once with cause
`interruption`; the worker watcher reports and withdraws EC2's notice;
`legion node interrupt` sets and clears it.
