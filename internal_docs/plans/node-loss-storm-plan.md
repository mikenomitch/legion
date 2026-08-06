# Node-loss storms: stop recovery from starving the cluster

> **Judgement calls:** [node-loss-storm-plan_calls.md](../calls/node-loss-storm-plan_calls.md)

**Status: SHIPPED** (2026-08-06, written with the change). Phases 1–4 all
landed together; what was deliberately deferred is in §8.

## 1. The problem

When many nodes go lost at once, the recovery machinery itself becomes the
cluster's bottleneck. Measured on the AWS rig 2026-08-04 (~3,000 simulated
nodes vanishing at once): job submission slowed to ~6 s per job,
`evals_processed_total{kind="node_lost_reschedule"}` hit 2,752 against
`{kind="job_submit"}` at 66, with a 764-deep eval queue. Four separate
causes, none of them the reschedule work itself:

1. **Equal priority means submissions queue behind recovery.** The eval
   queue is strict `priority DESC, id ASC`, and `node_lost_reschedule`
   evals enqueued at the same default priority as submissions — so at equal
   priority the older recovery backlog runs strictly ahead of every newer
   submission, forever.
2. **Lost nodes with nothing on them still cost a full eval.** The
   "anything to reschedule?" check lived inside the eval handler, *after*
   the enqueue, the claim, and the terminal write had each been through a
   scheduler-lane quorum round.
3. **The fan-out was one quorum round per node, per 2 s tick, inline in
   the status checker.** `mark_nodes_as_lost` looped one `enqueue_eval`
   (and one EXISTS probe) per node, and `recheck_lost_nodes` re-drove the
   loop every tick for as long as nodes stayed lost — all on a tokio
   runtime thread, stalling the checker's own gauge refresh and
   checkpoint.
4. **Recovery was invisible.** A node coming back cleared its `lost` row
   with no counter and (on the re-registration path) no log line, so
   `nodes_marked_lost_total` could only ever count up and a flapping fleet
   read as a shrinking one.

## 2. Prior art

- **Nomad** creates `node-update` evals per affected *job* with the job's
  own priority, so recovery work inherits workload priority rather than
  competing at a flat system priority; its scheduler also batches
  node-down processing per evaluation. Legion's reschedule evals are
  per-*node* (the handler owns the stateful-loss gates), so job priority
  cannot be inherited cleanly — but the "recovery must not outrank new
  work at the same priority" property is the same one Nomad gets from
  job-priority inheritance.
- **Kubernetes** rate-limits node-lifecycle eviction explicitly
  (`--node-eviction-rate`, plus the zone-outage secondary rate), and its
  node controller marks nodes NotReady in bulk. Legion's equivalent
  backpressure is the mass-disruption breaker plus, with this plan, a
  fan-out that costs O(1) quorum rounds instead of O(nodes).
- **Consul** separates liveness (gossip) from acting on it; Legion's
  liveness is already off the raft path
  ([liveness-off-the-raft-path-plan.md](liveness-off-the-raft-path-plan.md)) —
  this plan is about the *acting* half.

## 3. Jobs to be done

- **As an operator running jobs**, when a rack or AZ drops, I need new job
  submissions to still land in seconds — recovery of dead workloads matters,
  but it must not freeze the cluster for everyone else.
- **As an operator watching a fleet**, I need to see churn: how many nodes
  went lost *and how many came back*, so a flapping network reads as
  flapping rather than as steady loss.
- **As a capacity planner**, I need a mass-loss event's control-plane cost
  to be bounded (a few quorum rounds per tick), not proportional to the
  fleet.

## 4. Goals and non-goals

**Goals**

- New submissions overtake the recovery backlog during a storm.
- A storm's per-tick control-plane cost is O(1) quorum rounds, off the
  runtime threads.
- Lost-but-empty nodes cost nothing until they have something to recover.
- `lost − recovered` is computable from metrics.

**Non-goals**

- Cross-kind fairness in `claim_eval` (a scheduler change; the priority
  band gets the same outcome for this case).
- K8s-style eviction *rate limiting* — the mass-disruption breaker already
  guards the pathological end, and a budgeted drain was evaluated and
  rejected once before
  ([preemption calls #10](../calls/preemption-and-eviction-plan_calls.md)).
- Changing what the reschedule handler does per node (stateful-loss gates
  are untouched).

## 5. What exists today (pre-change)

`Store::mark_nodes_as_lost`: one batched status-lane op, one batched
control-lane op, then one `enqueue_lost_node_reschedule` per node — an
EXISTS probe plus a scheduler-lane round each. `Store::recheck_lost_nodes`:
batched dedup probe, then the same per-node enqueues, every 2 s tick, from
the async status checker. The handler
(`reschedule_allocations_for_lost_node`) early-returns on a node with no
allocations. `Store::enqueue_evals` (one round for N evals) already exists
but is unused here.

## 6. Design

No new user-facing surface. The observable changes:

- `node_lost_reschedule` evals enqueue at `default_priority − 10`
  (`NODE_LOST_RESCHEDULE_PRIORITY_DROP`), so a default-priority submission
  (50) overtakes a storm's backlog (40). An operator's explicit
  `--priority`/band settings behave as before.
- One scheduler-lane round enqueues the whole batch
  (`Store::enqueue_lost_node_reschedules`): dedup against queued evals
  (one query), filter to nodes that actually hold allocations (chunked
  indexed query), then `enqueue_evals`. Under split lane leadership the
  batched op is rejected and the path falls back to the per-node
  forwarding-aware enqueue — exactly the pre-change behavior.
- The status checker runs `mark_nodes_as_lost` and `recheck_lost_nodes`
  on the blocking pool, so a storm never parks the checker's runtime
  thread.
- `legion_nodes_recovered_total{path="heartbeat"|"reregistration"}`
  counts recoveries; the re-registration path also logs.

Safety of the empty-node filter: allocations cannot be *planned* onto an
unavailable node, and the recheck re-runs the filter every tick — so an
allocation that lands via an in-flight plan racing the loss is picked up
one tick later. The handler's own `allocs.is_empty()` early-return stays
as the backstop.

## 7. Testing

- `mark_nodes_as_lost` unit tests (reschedule, empty input, availability
  sync) continue to pass unchanged — the batch path preserves per-node
  semantics.
- The daemon's node-loss end-to-end test pumps the queue through the
  batched enqueue.
- Storm-scale behavior (submission latency during a 3,000-node loss) is
  an AWS-rig measurement; see §8.

## 8. Deferred

- **Re-measuring the storm on the rig** (the 2026-08-04 numbers are the
  before; there is no after yet).
- **Cross-kind fairness in `claim_eval`** if the priority band proves
  insufficient (e.g. a storm of *high-band* jobs' reschedules).
- **`mark_node_ready` status normalization** and **drain-intent
  preservation across loss** — adjacent defects recorded in
  [TODO.md](../TODO.md), untouched here.
- A batched wire variant for forwarded enqueues (new cross-version
  message; the fallback keeps the old cost only in the split-leadership
  corner).
