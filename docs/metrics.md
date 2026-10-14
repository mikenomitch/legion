# Metrics & Health Probes

The manager exposes Prometheus metrics and Kubernetes-style health probes on
a dedicated HTTP listener (implemented in `src/manager/telemetry.rs`). Use them
to alert on and graph cluster health. The registry is hand-rolled on atomics
(no prometheus crate) and renders the standard text exposition format, so any
Prometheus-compatible scraper works unchanged.

## Endpoints

| Path       | Method | Response |
|------------|--------|----------|
| `/metrics` | GET    | `200` with `Content-Type: text/plain; version=0.0.4`. Prometheus text exposition of every metric family below. |
| `/healthz` | GET    | Liveness. `200 ok` whenever the listener is serving (no dependencies checked). Use for restart decisions. |
| `/readyz`  | GET    | Readiness. `200 ok` only when a store read probe (`SELECT 1` on a read connection) succeeds **and**, in raft mode, `raft_status()` reports a known leader. Under `topology = "lanes"`, every lane must have a leader and all lanes must be led by the **same** manager. Otherwise `503` with the reason in the body. Use for load-balancer and traffic decisions. See the details below. |

Example `503` bodies from `/readyz`:

- `not ready: raft leader unknown`
- `not ready: raft lane leadership has not converged for 14s: lanes are led by different managers (control=mgr-a,scheduler=mgr-b,status=mgr-a) …`

A lane split only breaks readiness after about 10s of continuous divergence.
Each manager's own probe measures this from the moment it first sees the
split, so brief post-election convergence and rolling restarts don't flap load
balancers.

A standing split leaves the cluster degraded, but still up. Worker liveness
forwards to the lane leaders, the control leader accepts job submits and lane
reconcilers complete them, and reads and single-lane operations are unaffected.
Scheduling latency suffers, though, and `legion state backup` is unavailable.
The `503` tells you to investigate per-lane leadership.

Convergence is best effort (see [multi-raft.md](multi-raft.md) § "Lane
leadership"), so a split can persist. If the probe stays at `503`, treat it as
a signal that the split has persisted, and don't wait it out indefinitely.

Under a multi-lane topology, the body carries one
`lane <name>: leader <node> (this node: <role>)` line per lane on both the
`200` and `503` paths. In raft mode, a `200` body may also carry advisory
`note:` lines, such as the even-voter-parity advisory ("2 voters need both
alive to commit …"). These don't affect the status code. An even-sized
cluster serves fine. It just survives fewer failures than it could, so voter
parity doesn't break readiness.

Anything else is `404`, and non-GET methods are `405`. The listener serves only
read-only operational data. It never exposes secrets, tokens, or job payloads.

## Configuration

Optional `telemetry` block in the manager config:

```nickel
{
  server = { port = 4747, host = "127.0.0.1" },
  # ...
  telemetry = {
    enabled = true,   # default: true
    port = 4749,      # default: server.port + 2
  },
}
```

- `enabled` (default `true`): master switch for the listener.
- `port` (default `server.port + 2`): the listener binds this port on
  `server.host`. The `+ 2` convention leaves `server.port + 1` for the exec
  WebSocket listener.

## Metric families

All families are prefixed `legion_`. Labels are low-cardinality by design.
Each family caps its distinct label combinations at 256 and folds any overflow
into a single series with every label set to `"other"`.

### gRPC server

- **`legion_grpc_requests_total{method, code}`** (counter): gRPC requests
  handled by the manager. `method` is the `Service/Method` pair from the URI
  path (e.g. `manager.Manager/StopJob`). `code` is the gRPC status name
  (`ok`, `not_found`, `permission_denied`, ...) when tonic surfaces
  `grpc-status` in the response headers. It does this for every error
  (trailers-only) response. Successful responses carry `grpc-status` in HTTP
  trailers, which the middleware does not buffer, so they are labeled by HTTP
  status class instead (`http_2xx` = success). A transport-level failure is
  labeled `transport_error`.
- **`legion_grpc_request_duration_seconds{method}`** (histogram): request
  duration, measured until response headers are produced (not the end of a
  streaming body).
- **`legion_grpc_in_flight{method}`** (gauge): requests currently executing,
  by method. The count starts when the service accepts the request, so it
  includes time spent waiting for a runtime worker to poll it. It is
  decremented even when a client hangs up mid-call. Read it against
  `legion_runtime_worker_threads`. If the sum sits at or above the
  worker-thread count, handlers are occupying the runtime, which is what a
  blocking call inside an async handler looks like. A busy cluster looks
  different. Duration alone can't tell those apart, but concurrency can.

### Scheduler / eval loop

For symptom-first guidance on these families (which one to read, which
neighbor rules a cause in or out, and which knob to turn), see
[diagnosing-scheduler-performance.md](diagnosing-scheduler-performance.md).

- **`legion_evals_processed_total{kind, status}`** (counter): evals
  completed by the eval workers. `kind` is the eval kind (`job_submit`,
  `node-left`, ...). `status` is `ok` or `error`.
- **`legion_eval_queue_depth`** (gauge): pending (unclaimed) evals in the
  queue, including delayed evals not yet ready. Updated transactionally on
  enqueue, claim, and startup requeue.
- **`legion_plan_apply_total{outcome}`** (counter): scheduler plan
  submissions applied to the store. `outcome` is one of:
  - `applied`: every placement bound its first-choice candidate.
  - `fallback`: applied, but at least one placement bound a lower-ranked
    candidate because of a race.
  - `rejected`: no candidate still fits. Causes are capacity, a late
    constraint or cordon, or (for an edge-boot ratification only) an epoch
    conflict.

  The outcomes are mutually exclusive. See
  [late-binding-plan-apply.md](late-binding-plan-apply.md).
- **`legion_eval_claim_total{outcome}`** (counter): eval claim attempts by
  an eval worker. `claimed` found work and `empty` did not. A claim is a
  mark in the leader's memory and writes nothing, so an empty attempt costs
  one read of the queue and the wake itself: the `empty` share is the cost
  of waking a worker for work that does not exist. A large share means the
  pool is being over-woken. Claims carried over by the fused
  complete-and-claim round are not counted.

  A claim takes a *batch* (see
  [scheduler-and-eval-loop.md](scheduler-and-eval-loop.md#batching)), so
  `claimed` counts rounds that found work, not evals. Divide
  `legion_evals_processed_total` by it to get evals per claim round, which
  shows how much batching is engaging. A ratio pinned at 1 on a busy cluster
  means the queue never gets deeper than `evalThreads`, so the bottleneck is
  upstream of the eval loop.
- **`legion_scheduler_plan_candidate_rank`** (histogram): the candidate rank
  each applied placement bound at. 0 is the planner's first choice. Higher
  ranks mean the apply walk absorbed a race by falling back.
- **`legion_scheduler_plan_bind_pass{pass}`** (counter): placements bound
  by the apply walk, by pass. `clean` means the candidate's node was
  untouched since the plan snapshot. `revalidated` means its allocation
  epoch had moved and the in-transaction recheck (capacity, constraints,
  ports, availability) admitted it anyway. A `revalidated` share that rises
  with load means contention is being absorbed instead of rejected. Read it
  next to `legion_plan_apply_total{outcome="rejected"}`. The healthy shape is
  `revalidated` climbing while `rejected` stays flat.
- **`legion_eval_conflict_requeues_total`** (counter): evals released back
  to the pending queue after exhausting their plan-conflict retry budget,
  instead of failing terminally (the requeue backstop).
- **`legion_evals_blocked{key_kind}`** (gauge): evals waiting blocked on
  capacity, by the kind of key they wait under: `class`, `node`, or
  `escaped` (see
  [Blocked evals](scheduler-and-eval-loop.md#blocked-evals)). Set by the
  scheduler-lane leader's wake pass, so it reads 0 on other managers and is
  refreshed when a capacity event arrives. A rising `class` count is a
  capacity signal for that class; `escaped` means no node was schedulable.
- **`legion_evals_unblocked_total{event}`** (counter): blocked evals returned
  to the pending queue, by what woke them: `register`, `node` (pool,
  availability, or attributes changed), `ready`, `freed` (allocations left a
  node), `missed` (capacity appeared while the eval ran, so it never
  blocked), or `startup`.
- **`legion_blocked_wait_seconds`** (histogram): how long a blocked eval
  waited, from the entry that blocked it to the one that woke it. Buckets run
  from 100 ms to an hour.

#### Adaptive scheduler concurrency

All scheduler-lane-leader-only: a manager that does no planning reports zeros.
See [adaptive-scheduler-concurrency.md](adaptive-scheduler-concurrency.md).

- **`legion_scheduler_concurrency_limit`** (gauge): planners currently
  admitted. In `observe` mode this is the *shadow* limit — what the
  controller would enforce — which is what makes observe mode useful on its
  own.
- **`legion_scheduler_concurrency_in_flight`** (gauge): admission permits
  held by workers inside a planning eval. 0 when the controller is `off`,
  which never consults the gate.
- **`legion_scheduler_concurrency_state{state}`** (gauge): 1 on the current
  state, 0 on the others: `steady`, `throttled`, `probing`, `saturated`.
  **`saturated` is the one to alert on**: it means backing off did not
  improve goodput, so the conflicts are capacity-driven and the remedy is
  nodes rather than patience.
- **`legion_scheduler_concurrency_conflict_rate`** (gauge): the smoothed
  rejection share the controller is deciding on. Fallbacks count as progress,
  so this is not the same number as a raw ratio of `legion_plan_apply_total`
  outcomes taken over a different window.
- **`legion_scheduler_concurrency_fallback_rate`** (gauge): the smoothed
  share of plans that bound at a lower-ranked candidate: races *absorbed*.
  Reported for diagnosis; never fed into the decision.
- **`legion_scheduler_concurrency_goodput`** (gauge): smoothed plans that
  actually placed, per second. The objective every decrease is verified
  against.
- **`legion_scheduler_concurrency_adjustments_total{direction}`** (counter) —
  `increase`, `decrease`, or `revert` (a decrease undone by the goodput
  guard). A sustained two-value sawtooth is hunting — a bug, not a cosmetic
  issue.
- **`legion_scheduler_admission_skips_total`** (counter): times a throttled
  worker claimed a non-planning eval (today `job_stop`) instead of the queue
  head. This counts the one cross-class priority inversion the admission gate
  introduces.
- **`legion_preemptions_total{pool, band}`** (counter): allocations evicted
  by scheduler preemption, labeled by the victim's node pool and its
  priority band. Alert on the rate. Preemption of production-adjacent bands
  should be rare. A rising rate in a `low` band is a capacity signal, and
  usually not an incident ([preemption-and-eviction.md](preemption-and-eviction.md)).
- **`legion_preemption_plans_total{outcome}`** (counter): preemption passes
  that reached a decision. `placed` means victims were evicted and the
  preemptor placed. `observe` means the pass ran under
  `preemption.mode = "observe"` and committed nothing. `no_victims` means
  preemption was enabled, ran, and still found nothing it was allowed to take.
  Compare `observe` against zero to size the blast radius before switching to
  `enforce`. A rising `no_victims` is a capacity signal: work is failing to
  place and the scheduler has already run out of displacement options.
- **`legion_scheduler_plan_phase_seconds{phase}`** (histogram): per-phase
  wall-clock time of every `plan_and_apply`. Phases, in execution order:
  - `job_lookup`: the plan's job and priority-band lookups.
  - `available_nodes`: the cached node set, or its rebuild after a node
    change.
  - `task_fetch`: task-config load.
  - `capacity_snapshot`: allocations and free pools.
  - `build_plan`: candidate construction, scoring, and placement.
  - `version_sample`: the optimistic-concurrency epoch sample.
  - `speculative_dispatch`: early payloads for tasks that opt into
    `speculativeStart`. Near zero otherwise.
  - `apply`: plan-submission insert, epoch compare-and-set, and the candidate
    walk's allocation writes, as one replicated op. On raft this includes
    propose → quorum-commit → apply.
  - `post_apply`: ratification dispatch and the ledger deltas.
  - `total`: the whole call.

  The phases are disjoint, so `total` minus their sum is glue and should stay
  small. Rejected plans are observed too, up to `apply`. Look here first when
  scheduling latency moves. Compare the phase that grew against the others
  before profiling. The benchmark runners aggregate the same phases
  ([testing-and-benchmarks.md](testing-and-benchmarks.md)).
- **`legion_eval_handle_seconds{kind}`** (histogram): wall time to handle one
  claimed eval, from handing it to the blocking pool to its outcome.
  Throughput is roughly `evalThreads` divided by this. The difference from
  the plan's `total` is everything an eval does outside `plan_and_apply`,
  which `legion_eval_phase_seconds` breaks down.
- **`legion_eval_phase_seconds{kind, phase}`** (histogram): where one eval's
  handler time goes.
  - `queue_wait`: the wait for a blocking-pool thread, recorded for every
    kind. Heartbeats share the pool, so a saturated pool shows here and not
    as a slow handler.

  For the kinds that place work (`job_submit`, `job_update`,
  `job_reschedule`, `job_scale`), the remaining phases partition the handler:
  - `scheduler_init`: scheduler construction.
  - `prepare`: the kind's own pre-plan work, such as a submit's
    previous-config lookup.
  - `plan`: every `plan_and_apply` attempt, rejected ones included.
  - `conflict_backoff`: sleeps between attempts after a fence rejection.
    Absent when nothing conflicted.
  - `dispatch`: building and handing off the workers' payloads. The sends
    are detached and show in `legion_dispatch_send_seconds`.
  - `finish`: post-plan bookkeeping, such as removing a replaced config's
    allocations and scheduling batch follow-ups.
- **`legion_eval_queue_round_seconds{op}`** (histogram): one eval-queue round
  on the scheduler lane. `claim` is a standalone claim. `settle` writes a
  batch's outcomes and claims the next batch in one replicated op. Both run
  between handlers, outside `legion_eval_handle_seconds`.
- **`legion_scheduler_node_list_rebuilds_total`** (counter): rebuilds of the
  scheduler's cached available-node set (the sorted list and its by-ID
  lookup). A plan rebuilds it only when a node changed since the last plan,
  so it sits near zero while the fleet is stable. A rate that tracks the plan
  rate means nodes are changing under every plan, and each plan pays an
  O(fleet) rebuild.
- **`legion_scheduler_lazy_sample_fallback_total`** (counter):
  **retired, always 0.** It counted plans that re-ran with the eager
  whole-fleet epoch sample because the read model changed mid-plan.
  Plans now take their fence epochs from their own capacity snapshot and
  never re-run (see "The epoch sample comes from the plan's own snapshot"
  in [scheduler-and-eval-loop.md](scheduler-and-eval-loop.md)). Nothing
  increments it anymore. It is still exported so dashboards that graph it
  don't break.

### Content locality

- **`legion_scheduler_locality_placements_total{outcome}`** (counter):
  placements whose task had a content set
  ([artifacts.md](artifacts.md#content-locality)), by how much of the set the
  chosen node already held. `outcome` is `warm` (all of it), `partial`
  (some), or `cold` (none). Counted when the plan picks the node, so a
  placement the apply later binds to a fallback candidate counts the planned
  node. Tasks without digest-pinned artifacts are not counted.

  The locality hit ratio at placement:

  ```promql
  sum(rate(legion_scheduler_locality_placements_total{outcome="warm"}[5m]))
    / sum(rate(legion_scheduler_locality_placements_total[5m]))
  ```

  A low ratio while nodes hold the content usually means other score terms
  outweigh locality (raise `schedulerSettings.weights.locality`), or the
  warm nodes are full.

### Scheduling indexes

See [scheduling-indexes.md](scheduling-indexes.md) for the feature these
instrument.

- **`legion_scheduler_index_queries_total{index, outcome}`** (counter):
  candidate queries against the declared scheduling indexes, per planned
  task. `outcome` is one of:
  - `used`: the index generated the candidates.
  - `shadow`: it ran alongside the authoritative scan.
  - `fell_back`: it returned no eligible candidates and the scan served
    instead.
  - `refilled`: a truncated candidate list ran dry mid-placement and was
    widened to the scan.
  - `unusable`: no index bound the task's constraints. `index` is `none`.
- **`legion_scheduler_index_candidates{index}`** (histogram): candidate
  count handed to placement per index-served task, after re-verification
  and any truncation.
- **`legion_scheduler_index_shadow_total{index, result}`** (counter):
  shadow-mode comparisons between the index's candidate set and the scan's.
  `result` is `agree` or `disagree`. Any `disagree` is a bug worth filing.
  The scan stayed authoritative, but the index missed nodes.
- **`legion_scheduler_index_maintenance_seconds`** (histogram): time spent
  re-projecting nodes into the indexes on read-model mutations (node
  upserts, availability changes, allocation changes).

### Cluster shape

- **`legion_nodes{status}`** (gauge): registered worker nodes by status
  (`active`, `lost`, ...). Refreshed on the node-status-checker tick
  (`workerCheckSeconds`), so it lags by at most one tick. A status that
  empties out drops to `0` instead of disappearing.
- **`legion_read_model_nodes{set}`** (gauge): nodes in the manager's
  in-memory scheduling read model, refreshed on the same tick.
  `set="all"` is every cached node. `set="available"` is the planner's
  candidate universe. In steady state, `set="all"` equals `legion_nodes`
  summed over statuses. A persistent gap means the scheduler is planning
  over a truncated view of the fleet.
- **`legion_allocations`** (gauge): live allocations tracked by the
  manager. Refreshed on the same tick.
- **`legion_heartbeats_total`** (counter): worker heartbeat RPCs accepted
  (past authentication) by the manager.
- **`legion_nodes_marked_lost_total`** (counter): nodes marked lost after
  missed heartbeats. This tells you nodes went away, while
  `legion_nodes{status="lost"}` tells you nodes are currently away.
- **`legion_nodes_recovered_total{path}`** (counter): lost nodes that came
  back. `path="heartbeat"` means the node resumed beating.
  `path="reregistration"` means the worker re-registered.
  `path="reconcile"` means the liveness owner found a node it is hearing
  from still stored `unavailable` and returned it to the scheduling pool
  (a half-applied recovery; see docs/node-liveness.md). `marked_lost` minus
  this is the fleet's net loss. A flapping fleet moves both.

### Node usage

Per-node gauges from the usage reports workers with
`resourceEnforcement.enabled` send every 10 seconds
([resource-enforcement.md](resource-enforcement.md#measuring-usage)). Updated
when a report arrives. A node whose report is older than 2 minutes drops to
`0`, so a node that stopped reporting doesn't look current. Usage lives on the
manager that received the heartbeat, so with several managers sum or max
across them by `node`.

- **`legion_node_memory_physical_bytes{node}`** (gauge): `MemTotal`.
- **`legion_node_memory_available_bytes{node}`** (gauge): `MemAvailable`, the
  kernel's estimate of memory new work can use without swapping. The one to
  alert on for a node running short.
- **`legion_node_memory_used_bytes{node}`** (gauge): sum of `memory.current`
  over the node's governed allocations.
- **`legion_node_cpu_used_millicores{node}`** (gauge): CPU used by governed
  allocations over the last interval, in thousandths of a core.

One series per reporting node. Reservations aren't exported as gauges. Read
them with `legion node usage`.

- **`legion_node_pressure_actions_total{node,step}`** (counter): actions a
  node's memory pressure response reported, by `step` (`reclaim`, `suspend`,
  `evict-bursting`, `evict-reserved`). Counted when the heartbeat carrying
  them reaches this manager, so with several managers sum across them. A
  rising `evict-*` rate means reservations are too low for real usage, or a
  ratio is too aggressive. See
  [resource-enforcement.md](resource-enforcement.md#memory-pressure).

### Suspend and resume

Observed when a node's report reaches this manager, so with several managers
sum across them. See [suspend-and-resume.md](suspend-and-resume.md#metrics).

- **`legion_instance_suspend_seconds{kind}`** (histogram): how long each
  successful suspend's capture took on its node, by checkpoint `kind`
  (`memory`, `disk`). For a memory suspend this is the time the guest spent
  paused plus storing its memory. Buckets run from 25 ms to 60 s.
- **`legion_instance_resume_seconds{kind}`** (histogram): how long each
  successful resume's restore took on its node, by `kind`. Placement time is
  not included. The memory target is under 300 ms for a 1 GiB guest on local
  NVMe.
- **`legion_instance_suspends_total{reason}`** (counter): suspends applied,
  by `reason`: `manual` (SuspendInstance), `idle` (a task's idle policy),
  `pressure` (a node's `suspend` pressure step), or `make_room` (to make room
  for a resume, see
  [Capacity for resumes](suspend-and-resume.md#capacity-for-resumes)).
- **`legion_idle_sweep_suspends_total{outcome}`** (counter): suspends the
  idle sweep tried to start, by `outcome`: `started`, or `skipped` (the
  instance changed under it, an operation was already in flight, or its node
  cannot checkpoint). Counted on the manager that ran the sweep, which is the
  one holding the node's usage reports.
- **`legion_make_room_suspends_total{outcome}`** (counter): suspends the
  manager started to make room for a resume waiting on its checkpoint's
  node, by `outcome`: `started`, `failed` (the suspend could not start), or
  `no_room` (suspending every eligible instance would not have been enough,
  so none was suspended, counted once per round). A steady `no_room` means
  resumes wait on nodes whose work cannot be suspended for them.
- **`legion_checkpoint_bytes{kind}`** (histogram): size of each captured
  checkpoint, suspends and snapshots alike, from 1 MiB to 64 GiB. A disk
  suspend reports 0, because its volume stays in place uncopied.
- **`legion_checkpoint_publish_bytes{kind}`** (histogram): bytes a node
  uploaded to its `contentStore` to publish one checkpoint
  (`durability = "store"`). 0 when the store already held every part, since
  parts are keyed by digest.
- **`legion_checkpoint_publish_seconds{kind}`** (histogram): how long each
  successful publish took on its node, from 50 ms to 10 minutes.
- **`legion_checkpoint_publish_total{outcome}`** (counter): publishes
  reported, by `outcome`: `published`, `retrying` (the attempt failed and the
  manager asks the node again after a backoff), or `failed` (the last of three
  attempts failed). A failed publish leaves the checkpoint local to its node,
  so alert on a rising `failed` rate, and watch `retrying` for a store going
  bad.

- **`legion_instance_archived_total{mode}`** (counter): suspended instances
  [archived](suspend-and-resume.md#archiving-idle-instances) once their
  checkpoint published, by `mode`: `slim` (a slim instance row stays) or
  `manifestOnly` (no row stays, the bucket holds the instance). Counted on
  the manager that recorded the publish.

### Sessions

Counted on the manager that served the request, so with several managers sum
across them. See [session-mode.md](session-mode.md).

- **`legion_session_starts_total{outcome}`** (counter): session creates the
  session router answered, by `outcome`: `started`, `deduplicated` (an
  idempotent retry found the session running), `no_envelope` (no node holds
  an envelope for the task yet), `no_room` (every envelope was full by the
  router's view), or `refused` (the nodes tried all refused). None of them
  writes the replicated store. A rising `no_room` or `refused` rate means
  the envelopes are too small for the arrival rate.
- **`legion_session_stops_total{reason}`** (counter): sessions this manager
  ended on their nodes, by `reason`: `stop`, `destroy`, or `duplicate` (the
  later copy of a session id running on two nodes).
- **`legion_session_promotions_total`** (counter): running sessions promoted
  to keyed instances, one scheduler-lane raft entry each.

Two more are counted on the worker that holds the sessions, on its own
listener ([Worker](#worker)):

- **`legion_session_evictions_total{reason}`** (counter): suspended
  sessions whose local checkpoint the node evicted, by `reason`:
  `keep_for` (`suspend.keepFor` passed), `envelope_gone` (the job stopped
  or the node was drained), `disk_pressure` (below `content.minFreeDisk`),
  or `other`. A later `instance start` on one answers `NOT_FOUND`
  "evicted". A rising `disk_pressure` count means the node's disk is too
  small for its suspended sessions and their `keepFor`.
- **`legion_session_checkpoints_skipped_total`** (counter): periodic
  session checkpoints (`suspend.checkpointEvery`) skipped because the
  node's capture backlog exceeded one interval. Any steady rate means the
  node cannot capture its sessions that often: lengthen the interval or
  run fewer sessions per node.
- **`legion_session_superseded_total{via}`** (counter): copies of
  `suspend.durability = "store"` sessions this node ended because a newer
  generation of the session runs elsewhere, by how it learned: `write` (its
  own conditional manifest write failed), `fence_list` (a fence marker a
  move left), `resume` (a resume found the manifest moved on), or
  `reconnect` (a self-fenced session the content store no longer names
  this node for). Each is a split brain that converged
  ([No split brain](session-mode.md#no-split-brain)); a steady rate means
  sessions are being moved while their old node still runs them, usually
  forced moves or a partition.
- **`legion_session_moves_total`** (counter): `store` sessions this node
  resumed from their manifest after they ran or slept on another node.
- **`legion_session_self_fenced_total`** (counter): running `store`
  sessions this node suspended because it reached neither a manager nor
  its content store for `stateful.fenceAfter`.

### Heartbeat cadence

Reported on the status-lane leader (the manager that owns node liveness), and
`0` everywhere else. See
[heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md).

- **`legion_heartbeat_freshness_ratio`** (gauge): alert on this one. It is the
  fraction of active nodes that checked in within one interval. `1.0` means
  the fleet is keeping up. False node loss begins at
  `served_interval / lost_window`, which is `0.20` at the defaults. Reads
  `0` on a fleet under 20 nodes, where the ratio is noise. Check
  `legion_heartbeat_sustainable_nodes` to see whether it is measurable at all.
- **`legion_heartbeat_served_interval_seconds`** (gauge): the interval this
  manager is telling workers to use. Adaptive cadence is on by default, so
  this grows with fleet size: 2 up to 2,500 nodes, 21 at 20,000 at the
  default `targetRatePerSecond`. With `heartbeat.adaptive = false` it reads
  `minInterval`, the interval freshness is measured against. Record it with
  every benchmark result.
- **`legion_heartbeat_lost_window_seconds`** (gauge): the window derived
  from it (`missedBeatsBeforeLost x interval`).
- **`legion_heartbeat_sustainable_nodes`** (gauge): nodes this control plane
  is observed to sustain at the current interval. While the fleet is keeping
  up, this is a lower bound. An unsaturated control plane never reveals its
  ceiling, so on a healthy cluster this reads as roughly the fleet size.

### Node liveness

Heartbeats live in the memory of the manager that owns node status. Only
status transitions are replicated. See
[node-liveness.md](node-liveness.md).

- **`legion_liveness_tracked_nodes`** (gauge): nodes in this manager's
  liveness table. Should equal `legion_nodes{status="active"}` on the owner,
  and `0` elsewhere.
- **`legion_liveness_responsible_seconds`** (gauge): seconds since this
  manager became responsible for liveness (process start or status-lane
  leadership acquisition, whichever is later). Resets at every acquisition,
  so a dashboard shows when a manager just took over.
- **`legion_liveness_digest_rpcs_total{outcome}`** (counter): liveness
  digests sent to the status-lane leader: `ok`, `not_leader`, `error`,
  `unimplemented`. A non-zero `unimplemented` means a manager that predates
  digests leads the lane, and heartbeats have fallen back to replicated
  writes. Finish the rolling upgrade.
- **`legion_liveness_digest_nodes`** (histogram): node IDs per digest.
- **`legion_liveness_peer_stale{peer}`** (gauge): `1` while a peer manager
  has missed several digests. Alert on this for manager-to-manager problems.
  Without it, one quiet manager looks like its share of the fleet dying.
- **`legion_liveness_loss_suppressed_total{reason}`** (counter): would-be
  node losses the manager declined to act on:
  `failover_grace`, `peer_digest_gap`, `mass_disruption`. This lets you tell
  "nothing was marked lost" apart from "nothing was wrong".
- **`legion_liveness_checkpoint_seconds`** (histogram): wall-clock time of one
  replicated liveness checkpoint (the display-only batched write).

### Async runtime

The manager's handlers run on a multi-threaded tokio runtime, and several of
them make blocking store calls. When every worker thread is occupied by one,
latency appears on every handler, including ones that do no work. Meanwhile
CPU sits idle and no single operation is slow. These two families name that
condition directly. Without them, everything just looks slow for no clear
reason.

- **`legion_runtime_worker_threads`** (gauge): worker threads in the
  runtime. Set once at startup. This is the ceiling on concurrent blocking
  handlers and the denominator for `legion_grpc_in_flight`.
- **`legion_runtime_spawn_delay_seconds`** (histogram): a probe task is
  spawned once a second and records how long it waited before its first
  poll. Tens of microseconds is a healthy runtime. Milliseconds means no
  worker thread was free to pick up trivial work. That points to blocking
  handlers, not load: the delay lands on unrelated RPCs, so a do-nothing
  endpoint's p99 rises along with an expensive one's. Buckets start at 50µs,
  finer than the other duration histograms, so you can tell the healthy case
  from the saturated one.

### Store

- **`legion_store_write_lock_wait_seconds`** (histogram): time spent
  waiting to acquire the store's SQLite write-connection lock. Sustained high
  buckets indicate write contention on the state store.
- **`legion_store_read_model_lock_wait_seconds{mode}`** (histogram): time
  spent acquiring the scheduler read model's lock, by `mode` (`read` or
  `write`). Planners and heartbeats take it to read. Every allocation publish
  and node change takes it to write. An uncontended acquisition is recorded
  as 0, so the mean is the true average wait and `_count` is the acquisition
  rate. A rising `read` tail means writers hold the lock long or often. A
  rising `write` tail means readers do. Either one lands inside whichever
  plan phase was running, so read it next to
  `legion_scheduler_plan_phase_seconds`.
- **`legion_store_read_lock_wait_seconds`** (histogram): time spent waiting
  for one of the store's read-only SQLite connections. A reader takes any free
  connection before queueing on a busy one, so a read that found one
  immediately is recorded as 0. Read the ratio of the `le="0"` bucket to
  `_count`: it shows how often the pool was saturated, not how long a query
  took. Demand here is heartbeat-driven, so it scales with fleet size, not
  eval throughput. A rising tail lands inside whichever scheduler plan phase
  happened to be running (usually `task_fetch`), so read it alongside
  `legion_scheduler_plan_phase_seconds`. If it stays high on a well-sized
  pool, fix it by issuing fewer reads, not by adding connections.

  **How the pool is sized.** `evalThreads + 4`, capped at 16 connections. The
  eval workers are the manager's concurrent readers. The `+ 4` covers readers
  outside that pool: worker heartbeats, the read-only gRPC and UI surface, the
  metrics scrape, and the background reconcilers. There is no separate knob,
  so raising `evalThreads` also widens the pool. The cap is a memory budget,
  not a contention limit. Each connection has its own SQLite page cache capped
  at 64 MiB, and those caches are not shared, so 16 connections is a 1 GiB
  ceiling. SQLite fills the cache lazily, so only a cluster with a working set
  that large approaches the cap. The pool is not sized from the host's core
  count. These reads are latency-bound, not CPU-bound, and a page-cache miss
  holds a connection through an I/O wait that uses no CPU.

- **`legion_read_model_drift_repaired_total{source}`** (counter): nodes
  whose cached scheduling state disagreed with the state database and was
  reloaded. `source="scrub"` is the 5-second background sweep.
  `source="delta"` is a committed change that contradicted the cache. Both
  should stay at zero. See
  [state-store.md § Read model coherence](state-store.md#read-model-coherence).
- **`legion_read_model_external_writes_total`** (counter): times another
  process or connection committed to the state database while the manager
  was running, forcing a full rebuild of the cache.

#### Group commit (the coalescer)

Four write paths batch concurrent submitters into one commit:

- Heartbeats (`path="heartbeat"`).
- Scheduler plan applies (`path="plan_apply"`).
- The two lane-halves of job intake: the placement eval
  (`path="eval_enqueue"`) and the `task_status` seed
  (`path="task_status_seed"`).

See [scheduler-and-eval-loop.md](scheduler-and-eval-loop.md). Batching is
opportunistic. A batch forms only from requests that arrive while a commit is
already in flight. Configuring batching doesn't mean it is happening, and only
these families can tell you which is the case.

- **`legion_store_coalesce_batch_size{path}`** (histogram): requests drained
  into one group commit. Watch the `le="1"` bucket. Compared against
  `_count`, it shows how often batching did not engage at all. If the
  distribution is pinned at 1 while the path is under load, submitters are
  not overlapping the way the design assumes, and raising the batch cap will
  do nothing.
- **`legion_store_coalesce_commit_seconds{path}`** (histogram): wall-clock
  time of the batch runner itself, i.e. the actual commit (on raft, including
  propose → quorum → apply).
- **`legion_store_coalesce_wait_seconds{path, role}`** (histogram): how long
  a submitter waited before being served. `role="leader"` is the time from
  enqueue to winning the commit slot, measured before its own commit
  starts, so it is pure contention. `role="follower"` is the time until another
  submitter's batch handed back a result, which includes that batch's commit.
  Compare it against `..._commit_seconds` to tell "the commit is slow" from
  "submitters are queued behind a commit". A large leader wait with a small
  commit time means the path is lock-bound, not storage-bound.
- **`legion_store_coalesce_wait_rounds_total{path}`** (counter): times a
  waiting submitter was woken by a finished commit that was neither its own
  result nor its turn to lead. In other words, it sat through an extra commit.
  Divide by `..._wait_seconds_count` to get the mean number of extra commits a
  request waits out. The healthy value is zero: every submitter was served by
  the first commit it queued behind. A ratio near or above 1 means requests
  keep missing the batch they arrived behind. Either `plan_apply_batch_max` is
  capping the drain, or a commit is so slow that a second one starts before
  the first cohort clears.

### Raft

- **`legion_raft_voters{lane}`** (gauge): voting members of each raft
  lane's group, as this manager knows them (`single` topology: one
  `control` series, `lanes`: three). Refreshed on the node-status-checker
  tick, on leaders and followers alike. Alert when it is even or differs
  from `persistence.raft.expectedVoters`. An even voter count adds a
  manager without adding fault tolerance (see [multi-raft.md](multi-raft.md),
  "How many managers?").
- **`legion_raft_quorum{lane}`** (gauge): majority size required to commit
  on each lane (`voters / 2 + 1`). `voters - quorum` is the number of
  manager failures the lane survives while still committing.
- **`legion_raft_term{lane}`** (gauge): the lane's current raft term as
  this manager sees it. A climbing term means elections. A term that keeps
  climbing without settling is leadership churn (see [multi-raft.md](multi-raft.md) on the
  election governor).
- **`legion_raft_commit_index{lane}`** (gauge): highest quorum-committed
  log index of the lane, as this manager sees it.
- **`legion_raft_applied_index{lane}`** (gauge): highest log index this
  manager's state machine has applied on the lane. Compare across managers
  to spot a lagging follower.
- **`legion_raft_apply_lag{lane}`** (gauge): `commit_index -
  applied_index` on this manager. Sustained growth means the local state
  machine (SQLite apply plus read-model projection) can't keep up with the
  commit rate.
- **`legion_raft_is_leader{lane}`** (gauge): `1` when this manager leads
  the lane, else `0`. Summed across managers, each lane should be exactly 1.
  A flapping series shows leadership churn.
- **`legion_raft_capture_seconds{lane}`** (histogram): the local capture
  phase of a replicated op: run the mutation, collect its changeset, roll
  back. This is purely local store work.
- **`legion_raft_propose_apply_seconds{lane}`** (histogram): the
  propose → quorum-commit → local-apply round-trip of a replicated op.
  Use it to find the bottleneck by comparing against
  `legion_raft_capture_seconds`. When write latency moves, whichever
  histogram moved names the cause: local SQLite and capture, or the
  replication pipeline (network, fsync, follower lag).
- **`legion_raft_ops_total{lane,op,outcome}`** (counter): replicated store
  ops by outcome:
  - `applied`.
  - `empty`: the mutation touched no rows and skipped the raft entry.
  - `not_leader`: proposal rejected before capture. This is normal on
    followers under divergent lane leadership.
  - `error`.

  `op` is the operation name, so you can see which ops keep a lane busy. A
  lane's `applied` count is its quorum-round count, so
  `topk(10, sum by (lane, op) (rate(legion_raft_ops_total{outcome="applied"}[1m])))`
  is the list to shorten when a lane is the limit.

  **Split by `outcome` before drawing conclusions.** A high op rate does not
  mean a high replication cost, because an op whose changeset came out empty
  never reaches quorum. On a 5,000-node cluster, `update_node_health_checks`
  is the busiest op on the status lane at ~134/s, and every one of them is
  `empty`. Workers re-report the same health check and the row does not
  change. Without the outcome split, that op looks like the dominant cost in
  the system, when it costs no rounds at all.
- **`legion_raft_pipeline_wait_seconds{lane}`** (histogram): time a
  replicated op sat in the lane's pipeline queue before the lane's pipeline
  thread began executing it. Each lane is served by one OS thread
  (capture → propose → apply are serialized per lane, see
  [multi-raft.md](multi-raft.md)), so queueing is a significant part of
  latency. The capture and propose histograms don't show it, because they
  only start once a job is picked up. This is the third place to look for a
  bottleneck. When this grows while `legion_raft_propose_apply_seconds` does
  not, replication is fine and the lane's serialized pipeline is the
  constraint.
- **`legion_raft_pipeline_depth{lane}`** (gauge): ops queued to a lane's
  pipeline thread and not yet started. Sustained depth above 1 means writes
  on that lane are waiting in line behind each other.

The gauge series refresh on the node-status-checker tick, on leaders and
followers alike. The histograms and the op counter are recorded at the
replication call site, on leaders only, since followers reject before capture.
The pipeline queue families are the exception. They are recorded on every
node, since a job queues before anyone knows whether this node leads.

Together the three latency families break one replicated write down end to
end: `pipeline_wait` (queued behind this lane's other writes) → `capture`
(local SQLite) → `propose_apply` (quorum plus local apply). Whichever grew is
the answer. If none did, the time is outside the store, and
`legion_runtime_spawn_delay_seconds` is the next place to look.
Under `single` topology every lane rides the one `control` raft group, so
the latency histograms and op counters carry `lane="control"`.

Anything else under the `legion_raft_*` prefix (snapshot timings,
per-peer replication lag, ...) is **reserved** for the raft engine. No other
subsystem may claim the prefix. The `pipeline` families are part of that
engine (the queue comes from its one-thread-per-lane design), which is why
they carry the prefix despite being recorded off the leader path.

### KV encryption keyring

All of these report from the answering manager's point of view, because the
root key is per-manager state ([kv-encryption.md](kv-encryption.md)).

- **`legion_keyring_unlocked`** (gauge): `1` when this manager can encrypt
  and decrypt every KV value (every data key unwrapped). `0` when the
  keyring is locked, disabled, or not initialized yet. Alert on `0` while
  `encryption.enabled` is true, since it means a root key is missing here.
- **`legion_keyring_keys{state}`** (gauge): keyring keys by state
  (`active` | `retired` data keys, and the `checkpoint` key that seals
  published checkpoints). If the `retired` count stays above zero longer
  than a rewrap takes, values still sit under an old key. `checkpoint` is 1
  once the keyring is initialized.
- **`legion_keyring_active_key_age_seconds`** (gauge): age of the active
  data key. It rotates automatically at `encryption.dataKeyRotationAge`
  (default 30 days).
- **`legion_kv_rows{encryption}`** (gauge): KV entries by storage form
  (`encrypted` | `plaintext`). Plaintext rows after enabling encryption are
  pre-upgrade values a rewrap has not converted yet.
- **`legion_kv_decrypt_failures_total{reason}`** (counter): reads that
  could not be decrypted. `locked` means the data key is not unwrapped on this
  manager. `corrupt` means the ciphertext failed authentication: it was
  modified on disk, moved between keys, or written under a different root key.
- **`legion_keyring_provider_calls_total{op,outcome}`** (counter): root
  key provider calls (`describe` | `wrap` | `unwrap`) by outcome (`ok` |
  `error`). These never happen on a KV read or write path. A rising `error`
  series with an `exec` provider points to a KMS problem.

### Worker

Workers can expose their own Prometheus listener. Opt in with the
`LEGION_WORKER_TELEMETRY_PORT` environment variable (a port number, where `0`
binds an ephemeral port that is logged at startup). It is an env var instead of
a config field because workers joined with a bare token have no config
file, and hosts packing many worker processes need a distinct port per
process anyway. The listener binds `0.0.0.0:<port>` and serves only `/metrics`
and `/healthz`. A worker has no store or quorum to probe, so there is no
`/readyz`. The manager's node health and heartbeat state is the cluster's view
of worker readiness. Default: disabled.

- **`legion_worker_heartbeats_total{outcome}`** (counter): heartbeat
  round-trips attempted, by outcome (`ok` | `error` | `unauthenticated`).
  A rising `error` rate is the worker-side view of manager unreachability.
  `unauthenticated` means the node credential was revoked or rotated and the
  worker is re-registering.
- **`legion_net_conns_total{direction,protocol,verdict}`** (counter):
  connections the node datapath decided ([network handlers](network-handlers.md)),
  by direction (`egress` | `ingress`), protocol (`tcp` | `http` | `dns`), and
  verdict (`allow` | `deny` | `error`). Labels are bounded. Details such as
  which rule, which destination, and for whom are on the `net.denied` event,
  never here.
- **`legion_net_denied_total{direction}`** (counter): connections and
  requests denied by network policy.
- **`legion_net_bytes_total{direction}`** (counter): bytes forwarded through
  the datapath (`egress` = task to destination, `ingress` = destination to
  task).
- **`legion_net_handler_total{handler,outcome}`** (counter): external
  handler calls (`authz`, `proxy`) by outcome: `allow`, `deny`, `proxy` (the
  flow was handed off), `error` (timeout, bad reply, no endpoint).
- **`legion_net_handler_seconds`** (histogram): wall-clock time of one `authz`
  webhook call. Cache hits are not observed.
- **`legion_worker_heartbeat_duration_seconds`** (histogram): wall-clock
  time of one heartbeat round-trip (status report plus reconcile against the
  manager's reply). In a scale test, use it to show the workers are not the
  bottleneck.
- **`legion_worker_pressure_actions_total{step}`** (counter): actions this
  node's memory pressure response took, by `step` (`reclaim`,
  `evict-bursting`, `evict-reserved`). Counted when the worker acts, before
  the manager hears about it.
- **`legion_worker_allocations_running`** (gauge): allocations this worker
  currently runs, as reported on its last heartbeat.
- **`legion_worker_allocation_failures_reported_total`** (counter):
  allocation failures this worker has reported to the manager.

Edge boot ([edge-boot.md](edge-boot.md)) adds five more. All are zero on a node
that has never taken a provisional start.

- **`legion_provisional_starts_total{origin,gate}`** (counter): provisional
  allocations admitted, by `origin` (`manager` | `edge`) and gate.
- **`legion_provisional_refused_total{reason}`** (counter): ratifications
  refused. A rising `instance_claimed` rate points to a routing problem (two
  edges racing for one sandbox) rather than a cluster problem. `no_capacity`
  and `ineligible` point the other way.
- **`legion_provisional_killed_total{cause}`** (counter): provisional work
  killed, by `refused`, `expired`, or `failed`. A rising `expired` count means
  verdicts are not arriving inside the lease. Check manager reachability
  before raising `ratifyTimeout`.
- **`legion_provisional_ratify_seconds`** (histogram): time from admission to
  verdict. Edge boot exists to take this latency off the start path, and its
  p99 tells you what `ratifyTimeout` should be.
- **`legion_provisional_active`** (gauge): unratified claims held right now.
  It makes started-but-unconfirmed capacity visible. A value that does not
  return to zero means claims are leaking.

Session starts on the node ([session-mode.md](session-mode.md)) add two:

- **`legion_worker_launch_refusals_total`** (counter): provisional starts
  (sessions and claims) refused because no launch slot freed within
  `edgeBoot.queueTimeout`. Each one answered `RESOURCE_EXHAUSTED` and the
  caller tried another node. A steady rise means starts arrive faster than
  `edgeBoot.launchConcurrency` launches finish.
- **`legion_worker_edge_session_starts_total`** (counter): sessions this
  node started from a session ticket (`Worker.StartSession`), not counting
  deduplicated retries.

Every task start on the node adds one histogram:

- **`legion_task_start_stage_seconds{runtime,stage}`** (histogram): wall-clock
  time of each stage of a task start, from the moment the node has the work
  to the moment the workload is healthy. It is the place to look when starts
  are slow, because it says which stage is slow. Buckets run from 1 ms to
  2 minutes.

  | `stage` | What it times | Recorded for |
  | --- | --- | --- |
  | `staging` | Artifact staging, including a restore's snapshot fetch, on its own thread | Tasks with artifacts (every container image when `containerImageDesugar` is on) |
  | `volumes` | Volume mounts, including a restore's volume rollback | Tasks with volumes |
  | `prepare` | Artifact consumption and the runtime's `prepare` | Every start |
  | `start` | The runtime's `start`. The process or VMM is spawned when it returns | Every start that is not a restore |
  | `restore` | A restore from a checkpoint, in place of `start` | Restores |
  | `register` | From `start` returning to the start being recorded: the wait for the worker's task lock, and the state-file write | Every start |
  | `total` | From the node having the work to the start being recorded. Every stage above falls inside it | Every start |
  | `health` | From the start being recorded to the first passing check | Tasks with `boot` or `health` checks |

  The sandbox runtime also reports its own sub-stages of `start`, in order:
  `network` (the guest's TAP device and address lease, TAP sandboxes only),
  `image` (the kernel and the image's OCI layout and config), `root_disk`
  (the root disk's fingerprint, then a reuse or a build), `vmm_config` (a
  checkpoint launch's device links and identity disk, the balloon, and the
  VMM's config), and `vmm_spawn` (spawning the VMM). The guest boots after
  `vmm_spawn`, so the guest's own boot falls inside `health`.

  Stages nest, so don't sum them across `stage`. A restart in place (a
  failed health check under `onUnhealthy`) is timed as a start of its own,
  with `start`, `register`, and `total`. For a claim held at the `stage` gate
  ([edge-boot.md](edge-boot.md)), `total` includes the wait for its
  ratification. The worker also logs one `Task allocation started` line per
  start with the same breakdown in milliseconds, for a single slow start:

  ```text
  INFO Task allocation started allocation_id=4312 task_name=box task_runtime=sandbox
    total_ms=2611 stages="staging=1204ms prepare=1ms image=2ms root_disk=1380ms
    vmm_config=1ms vmm_spawn=3ms start=1389ms register=2ms"
  ```

#### Simulated fleets

`legion start worker --simulate N`
([worker-simulation.md](worker-simulation.md)) is a worker process that
carries N nodes, so it exports the same registry from the same opt-in
listener. Set `LEGION_WORKER_TELEMETRY_PORT` on the sim host and scrape it
like any other worker.

Everything above is per-process, so on a sim host the families aggregate
over every node in the fleet. `legion_worker_heartbeats_total` counts the
fleet's heartbeats, not one node's. `legion_worker_allocations_running`
sums what every node reported on its last heartbeat (a simulated node acks
allocations instead of running them). Three families describe the fleet
itself and stay at zero in a real worker:

- **`legion_worker_simulated_nodes`** (gauge): simulated nodes in this
  process that have completed registration.
- **`legion_worker_simulated_nodes_target`** (gauge): the fleet size the
  process was asked for. A persistent gap below it is a stalled ramp.
  Compare against the registration counter to tell "still ramping" from
  "erroring".
- **`legion_worker_simulated_registrations_total{outcome}`** (counter):
  registration attempts, `ok` or `error`. Re-registrations (after an
  `unauthenticated` heartbeat) count too, so over a long run it outgrows
  the node count.

Per-node series are not exported. At the ~1,250 nodes per host a scale
campaign packs, per-node labels would explode cardinality, and the manager
already holds the authoritative per-node view. To learn about one simulated
node, ask the manager, not the sim host.

Keep two limitations in mind when reading a campaign:

- The fleet-wide allocation gauge is maintained by per-node deltas and
  repaired from the authoritative per-node state every 30s. Under heavy
  churn, a scrape can briefly disagree with the manager by a few allocations.
  Counters and the histogram are exact.
- Aborting a fleet (or killing sim nodes for a storm scenario) stops the
  process without walking the gauges back down first. Read a torn-down
  fleet's last sample as the state at teardown, not as a live value.

## Diagnosing a manager throughput ceiling

This section gives the manager-wide read order for when RPC throughput has
plateaued and you don't yet know which layer owns the time. If the symptom is
specifically about scheduling (placements are slow, throughput fell when you
added eval threads or nodes, or work is not running and nothing reports an
error), start at
[Diagnosing scheduler performance](diagnosing-scheduler-performance.md). That
page is organized by symptom and names the config knob for each one.

When RPC throughput plateaus below what the workload should produce, the
handler-duration histogram tells you that it is slow, but not why. Read
these in order. Each step rules out a layer.

1. **Is anything actually busy?** Compare `legion_grpc_in_flight` with
   `legion_runtime_worker_threads`, and check
   `legion_runtime_spawn_delay_seconds`. In-flight pinned at the worker count,
   with spawn delay in the milliseconds, means blocking handlers have
   saturated the runtime. To tell saturation from load, check whether a
   trivial endpoint (`HealthCheck`) shows the same latency as an expensive one.
2. **Is the group commit batching?** Compare the `le="1"` bucket of
   `legion_store_coalesce_batch_size` with its `_count`. If nearly all
   batches are size 1, batching never engages, and the path pays a full
   commit per request.
3. **Waiting or committing?** Compare `legion_store_coalesce_wait_seconds`
   (by `role`) with `legion_store_coalesce_commit_seconds`. Check
   `legion_store_coalesce_wait_rounds_total` for the share of the wait spent
   on extra commits instead of the one being waited on.
4. **Is it replication or the queue in front of it?** Compare
   `legion_raft_pipeline_wait_seconds` and `legion_raft_pipeline_depth`
   with `legion_raft_capture_seconds` and
   `legion_raft_propose_apply_seconds`.

   Heartbeats no longer commit, so on a steady-state fleet
   `legion_raft_pipeline_depth{lane="status"}` should sit at about zero
   between display checkpoints. If it doesn't, heartbeats are still reaching
   raft. Check `legion_liveness_digest_rpcs_total{outcome="unimplemented"}`
   for a mixed-version manager fleet.
5. **Is it the fleet, or the control plane?** Before concluding that nodes
   died, read `legion_heartbeat_freshness_ratio`. A low ratio means the
   control plane is behind, while the machines may be fine.
   `legion node cadence` prints the same answer with the remedy attached.

Here is an example of the pattern this catches. A raft control plane received
hundreds of heartbeats a second, with `Heartbeat` p99 in the tens of
milliseconds, `AppendEntries` p99 at 3ms, and manager CPU under 25%.
Replication was fast, so the time went to queueing and waiting.

## OTLP metrics push

Both the manager's and the worker's registries can also be pushed over
OTLP/HTTP (protobuf) to any OTLP-capable backend. It uses the same opt-in
convention as trace export ([observability.md](observability.md)):

- `OTEL_EXPORTER_OTLP_ENDPOINT`: base collector URL. `/v1/metrics` is
  appended for the metrics signal.
- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`: exact metrics endpoint, used
  verbatim.
- `OTEL_METRIC_EXPORT_INTERVAL`: export interval in milliseconds
  (default 60000).
- `LEGION_OTLP=1`: force-enable at the default endpoint.

Each tick exports every registry the process actually initialized (a
worker-only process pushes no manager families and vice versa), with
cumulative temporality and full histogram buckets. Failed exports are
logged at debug and retried on the next tick, so the push never
back-pressures the daemon. The pull `/metrics` listeners are unaffected
and can run alongside.

## Scrape configuration example

```yaml
scrape_configs:
  - job_name: legion-manager
    metrics_path: /metrics
    static_configs:
      - targets: ["10.0.0.5:4749"] # server.port + 2 (or telemetry.port)
  - job_name: legion-workers
    metrics_path: /metrics
    static_configs:
      - targets: ["10.0.1.7:9091"] # LEGION_WORKER_TELEMETRY_PORT, if enabled
```

Probe wiring (e.g. under systemd or Kubernetes):

- Liveness: `GET http://<host>:4749/healthz`. Restart the manager on
  sustained failure.
- Readiness: `GET http://<host>:4749/readyz`. Remove the manager from
  rotation while it returns `503`. In raft mode, a follower without a known
  leader reports not-ready even though the process is healthy.

## Relationship to other observability surfaces

Tracing and spans (OTLP export, see [observability.md](observability.md)) and the event stream
([event-stream.md](event-stream.md)) are unchanged. This endpoint adds cheap aggregate
counters for alerting and dashboards where traces are too heavy.
