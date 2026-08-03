# Metrics & Health Probes

The manager exposes Prometheus metrics and Kubernetes-style health probes on
a dedicated HTTP listener (implemented in `src/manager/telemetry.rs`). The
registry is dependency-free (hand-rolled on atomics — no prometheus crate)
and renders the standard text exposition format, so any Prometheus-compatible
scraper works unchanged.

## Endpoints

| Path       | Method | Response |
|------------|--------|----------|
| `/metrics` | GET    | `200` with `Content-Type: text/plain; version=0.0.4` — Prometheus text exposition of every metric family below. |
| `/healthz` | GET    | Liveness. `200 ok` whenever the listener is serving (no dependencies checked). Use for restart decisions. |
| `/readyz`  | GET    | Readiness. `200 ok` only when a store read probe (`SELECT 1` on a read connection) succeeds **and**, when persistence runs in raft mode, `raft_status()` reports a known leader — under `topology = "lanes"` a leader on **every** lane, and those leaders must be the **same** manager (JC-40 co-location). Otherwise `503` with the reason in the body (e.g. `not ready: raft leader unknown`, or `not ready: raft lane leadership has not converged for 14s: lanes are led by different managers (control=mgr-a,scheduler=mgr-b,status=mgr-a) …`). A lane split is tolerated for ~10s of continuously-observed divergence (measured by each manager's own probe from the moment it first sees the split, so brief post-election convergence and rolling restarts must not flap load balancers) and only then breaks readiness. A standing split is degraded, not down — worker liveness forwards to the lane leaders, job submits are accepted by the control leader and completed by lane reconcilers, reads and single-lane operations are unaffected — but scheduling latency suffers and `legion state backup` is unavailable, so the `503` is the operator signal to investigate per-lane leadership. Convergence is **best effort** (see [multi-raft.md](multi-raft.md) § "Lane leadership"), so a split can persist: this probe staying `503` is the intended signal that it has, not a transient to wait out indefinitely. Under a multi-lane topology the body carries one `lane <name>: leader <node> (this node: <role>)` line per lane on both the `200` and `503` paths. In raft mode a `200` body may additionally carry advisory `note:` lines — e.g. the even-voter-parity advisory ("2 voters need both alive to commit …") — which do **not** affect the status code: an even cluster serves fine today, it just survives fewer failures than it could, so readiness is deliberately not broken by voter parity. Use for load-balancer / traffic decisions. |

Anything else is `404`; non-GET methods are `405`. The listener serves only
read-only operational data — no secrets, tokens, or job payloads.

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

All families are prefixed `legion_`. Labels are low-cardinality by design:
each family caps its distinct label combinations (256) and folds any overflow
into a single series with every label set to `"other"`.

### gRPC server

- **`legion_grpc_requests_total{method, code}`** (counter) — gRPC requests
  handled by the manager. `method` is the `Service/Method` pair from the URI
  path (e.g. `manager.Manager/StopJob`). `code` is the gRPC status name
  (`ok`, `not_found`, `permission_denied`, ...) when tonic surfaces
  `grpc-status` in the response headers — which it does for every error
  (trailers-only) response; successful responses carry `grpc-status` in HTTP
  trailers, which the middleware does not buffer, so they are labeled by HTTP
  status class instead (`http_2xx` = success). A transport-level failure is
  labeled `transport_error`.
- **`legion_grpc_request_duration_seconds{method}`** (histogram) — request
  duration, measured until response headers are produced (not the end of a
  streaming body).
- **`legion_grpc_in_flight{method}`** (gauge) — requests currently executing,
  by method. Counted from the moment the service accepts the request, so it
  includes time spent waiting for a runtime worker to poll it, and it is
  decremented even when a client hangs up mid-call. Read it against
  `legion_runtime_worker_threads`: a sum that sits at or above the
  worker-thread count means handlers are *occupying* the runtime rather than
  the cluster being busy — the shape a blocking call inside an async handler
  produces. Duration alone cannot tell those apart; concurrency can.

### Scheduler / eval loop

- **`legion_evals_processed_total{kind, status}`** (counter) — evals
  completed by the eval workers. `kind` is the eval kind (`job_submit`,
  `node-left`, ...); `status` is `ok` or `error`.
- **`legion_eval_queue_depth`** (gauge) — pending (unclaimed) evals in the
  queue, including delayed evals not yet ready. Updated transactionally on
  enqueue, claim, and startup requeue.
- **`legion_plan_apply_total{outcome}`** (counter) — scheduler plan
  submissions applied to the store; `outcome` is `applied` (every placement
  bound its first-choice candidate), `fallback` (applied, but at least one
  placement bound a lower-ranked candidate because of a race), or
  `rejected` (optimistic-concurrency epoch conflicts or late constraint
  failures). The outcomes are mutually exclusive — see
  [late-binding-plan-apply.md](late-binding-plan-apply.md).
- **`legion_scheduler_plan_candidate_rank`** (histogram) — candidate rank
  each applied placement bound at (0 = the planner's first choice; higher
  ranks mean the apply walk absorbed a race by falling back).
- **`legion_eval_conflict_requeues_total`** (counter) — evals released back
  to the pending queue after exhausting their plan-conflict retry budget,
  instead of failing terminally (the requeue backstop).
- **`legion_preemptions_total{pool, band}`** (counter) — allocations evicted
  by scheduler preemption, labelled by the victim's node pool and its
  priority band. Rate on this is the number to alert on: preemption of
  production-adjacent bands should be rare, and a rising rate in a `low` band
  is a capacity signal rather than an incident
  (docs/preemption-and-eviction.md).
- **`legion_preemption_plans_total{outcome}`** (counter) — preemption passes
  that reached a decision. `placed` means victims were evicted and the
  preemptor placed; `observe` means the pass ran under
  `preemption.mode = "observe"` and committed nothing; `no_victims` means
  preemption was enabled, ran, and still found nothing it was allowed to take.
  Comparing `observe` against zero is how an operator sizes the blast radius
  before switching to `enforce`; a rising `no_victims` is a capacity signal —
  work is failing to place and the scheduler has already exhausted its
  displacement options.
- **`legion_scheduler_plan_phase_seconds{phase}`** (histogram) — per-phase
  wall-clock of every `plan_and_apply`, phases in execution order:
  `available_nodes` (node snapshot fetch), `version_sample` (the
  optimistic-concurrency epoch sample), `task_fetch` (task-config load),
  `capacity_snapshot` (allocations + free pools), `build_plan` (candidate
  construction, scoring, placement), `apply` (plan-submission insert +
  epoch compare-and-set + the candidate walk's allocation writes, one
  replicated op — on raft this includes propose→quorum-commit→apply), and
  `total` (the whole call). This is the first place to look when
  scheduling latency moves: compare the phase that grew against the
  others before profiling. The benchmark runners aggregate the same
  phases (docs/testing-and-benchmarks.md).
- **`legion_scheduler_lazy_sample_fallback_total`** (counter) — plans that
  re-ran with the eager (whole-fleet) optimistic-concurrency sample
  because the read model changed while the plan was being built (see the
  lazy epoch sample in docs/scheduler-and-eval-loop.md). A steadily high
  rate relative to plan volume means heavy concurrent churn; correctness
  is unaffected either way.

### Scheduling indexes

See [scheduling-indexes.md](scheduling-indexes.md) for the feature these
instrument.

- **`legion_scheduler_index_queries_total{index, outcome}`** (counter) —
  candidate queries against the declared scheduling indexes, per planned
  task. `outcome` is `used` (the index generated the candidates), `shadow`
  (it ran alongside the authoritative scan), `fell_back` (it returned no
  eligible candidates and the scan served instead), `refilled` (a truncated
  candidate list ran dry mid-placement and was widened to the scan), or
  `unusable` (no index bound the task's constraints; `index` is `none`).
- **`legion_scheduler_index_candidates{index}`** (histogram) — candidate
  count handed to placement per index-served task, after re-verification
  and any truncation.
- **`legion_scheduler_index_shadow_total{index, result}`** (counter) —
  shadow-mode comparisons between the index's candidate set and the scan's;
  `result` is `agree` or `disagree`. Any `disagree` is a bug report waiting
  to be filed — the scan stayed authoritative, but the index missed nodes.
- **`legion_scheduler_index_maintenance_seconds`** (histogram) — time spent
  re-projecting nodes into the indexes on read-model mutations (node
  upserts, availability changes, allocation changes).

### Cluster shape

- **`legion_nodes{status}`** (gauge) — registered worker nodes by status
  (`active`, `lost`, ...). Refreshed on the node-status-checker tick
  (`workerCheckSeconds`), so it lags by at most one tick; a status that
  empties out drops to `0` rather than disappearing.
- **`legion_allocations`** (gauge) — live allocations tracked by the
  manager. Refreshed on the same tick.
- **`legion_heartbeats_total`** (counter) — worker heartbeat RPCs accepted
  (past authentication) by the manager.

### Async runtime

The manager's handlers run on a multi-threaded tokio runtime, and several of
them make **blocking** store calls. When every worker thread is occupied by
one, latency appears on *every* handler — including ones that do no work —
while CPU sits idle and no individual operation is slow. These two families
name that condition directly; without them it looks like "everything is
mysteriously slow".

- **`legion_runtime_worker_threads`** (gauge) — worker threads in the
  runtime. Set once at startup. This is the ceiling on concurrent blocking
  handlers and the denominator for `legion_grpc_in_flight`.
- **`legion_runtime_spawn_delay_seconds`** (histogram) — a probe task is
  spawned once a second and records how long it waited before its first
  poll. **Tens of microseconds** is a healthy runtime. **Milliseconds** means
  no worker thread was free to pick up trivial work, which is the signature
  of blocking handlers rather than of load: the delay lands on unrelated
  RPCs, so a do-nothing endpoint's p99 rises alongside a genuinely expensive
  one's. Buckets are finer than the other duration histograms (50µs up)
  precisely so the healthy case is distinguishable from the saturated one.

### Store

- **`legion_store_write_lock_wait_seconds`** (histogram) — time spent
  waiting to acquire the store's SQLite write-connection lock. Sustained high
  buckets indicate write contention on the state store.

#### Group commit (the coalescer)

Two write paths batch concurrent submitters into one commit: heartbeats
(`path="heartbeat"`) and scheduler plan applies (`path="plan_apply"`). See
[scheduler-and-eval-loop.md](scheduler-and-eval-loop.md). Batching is
*opportunistic* — a batch forms only from requests that arrive while a commit
is already in flight — so "batching is configured" and "batching is
happening" are different claims, and only these families can tell them apart.

- **`legion_store_coalesce_batch_size{path}`** (histogram) — requests drained
  into one group commit. **The `le="1"` bucket is the number that matters**:
  compared against `_count`, it is how often batching did not engage at all.
  A distribution pinned at 1 while the path is under load means submitters
  are not overlapping the way the design assumes, and raising the batch cap
  will do nothing.
- **`legion_store_coalesce_commit_seconds{path}`** (histogram) — wall-clock
  of the batch runner itself, i.e. the actual commit (on raft, including
  propose → quorum → apply).
- **`legion_store_coalesce_wait_seconds{path, role}`** (histogram) — how long
  a submitter waited before being served. `role="leader"` is the time from
  enqueue to winning the commit lock, measured *before* its own commit
  starts: pure contention. `role="follower"` is the time until another
  submitter's batch handed back a result, which includes that batch's commit.
  **Attribution**: against `..._commit_seconds`, this splits "the commit is
  slow" from "submitters are queued behind a commit". A large leader wait
  with a small commit time means the path is lock-bound, not storage-bound.
- **`legion_store_coalesce_wait_rounds_total{path}`** (counter) — times a
  waiting submitter's 1ms reply poll timed out and it re-contended for the
  commit lock. Divided by `..._wait_seconds_count` it is the mean number of
  poll rounds per request: latency that belongs to the coalescer's own
  polling rather than to any commit. A ratio far above 1 means requests are
  routinely sitting through several poll intervals.

### Raft

- **`legion_raft_voters{lane}`** (gauge) — voting members of each raft
  lane's group, as this manager knows them (`single` topology: one
  `control` series; `lanes`: three). Refreshed on the node-status-checker
  tick, on leaders and followers alike. Alert when it is even or differs
  from `persistence.raft.expectedVoters` — an even voter count adds a
  manager without adding fault tolerance (see docs/multi-raft.md,
  "How many managers?").
- **`legion_raft_quorum{lane}`** (gauge) — majority size required to commit
  on each lane (`voters / 2 + 1`). `voters - quorum` is the number of
  manager failures the lane survives while still committing.
- **`legion_raft_term{lane}`** (gauge) — the lane's current raft term as
  this manager sees it. A climbing term means elections; a term that climbs
  without settling is leadership churn (see docs/multi-raft.md on the
  election governor).
- **`legion_raft_commit_index{lane}`** (gauge) — highest quorum-committed
  log index of the lane, as this manager sees it.
- **`legion_raft_applied_index{lane}`** (gauge) — highest log index this
  manager's state machine has applied on the lane. Compare across managers
  to spot a lagging follower.
- **`legion_raft_apply_lag{lane}`** (gauge) — `commit_index -
  applied_index` on this manager. Sustained growth means the local state
  machine (SQLite apply + read-model projection) cannot keep up with the
  commit rate.
- **`legion_raft_is_leader{lane}`** (gauge) — `1` when this manager leads
  the lane, else `0`. Summed across managers each lane should be exactly 1;
  a flapping series is leadership churn made visible.
- **`legion_raft_capture_seconds{lane}`** (histogram) — the local capture
  phase of a replicated op: run the mutation, collect its changeset, roll
  back. This is pure local store work.
- **`legion_raft_propose_apply_seconds{lane}`** (histogram) — the
  propose → quorum-commit → local-apply round-trip of a replicated op.
  **Bottleneck attribution**: compare against `legion_raft_capture_seconds`
  — when write latency moves, whichever histogram moved names the culprit
  (local SQLite/capture vs the replication pipeline: network, fsync,
  follower lag).
- **`legion_raft_ops_total{lane,outcome}`** (counter) — replicated store
  ops by outcome: `applied`, `empty` (the mutation touched no rows and
  skipped the raft entry), `not_leader` (proposal rejected before capture —
  normal on followers under divergent lane leadership), `error`.
- **`legion_raft_pipeline_wait_seconds{lane}`** (histogram) — time a
  replicated op sat in the lane's pipeline queue before the lane's pipeline
  thread began executing it. Each lane is served by exactly **one** OS thread
  (capture → propose → apply are serialized per lane, see
  [multi-raft.md](multi-raft.md)), so queueing is a first-class latency
  component — and it is invisible in the capture/propose histograms, which
  only start once a job is picked up. **This is the third bottleneck
  candidate**: when this grows while `legion_raft_propose_apply_seconds` does
  not, replication is fine and the lane's serialized pipeline is the
  constraint.
- **`legion_raft_pipeline_depth{lane}`** (gauge) — ops queued to a lane's
  pipeline thread and not yet started. Sustained depth above 1 means writes
  on that lane are standing in line behind each other.

The gauge series refresh on the node-status-checker tick, on leaders and
followers alike; the histograms and the op counter are recorded at the
replication call site (leaders only — followers reject before capture). The
pipeline queue families are the exception: they are recorded on *every*
node, since a job queues before anyone knows whether this node leads.

Together the four latency families decompose one replicated write end to
end: `pipeline_wait` (queued behind this lane's other writes) → `capture`
(local SQLite) → `propose_apply` (quorum + local apply). Whichever grew is
the answer; if none did, the time is outside the store and
`legion_runtime_spawn_delay_seconds` is the next place to look.
Under `single` topology every lane rides the one `control` raft group, so
the latency histograms and op counters carry `lane="control"`.

Anything else under the `legion_raft_*` prefix (snapshot timings,
per-peer replication lag, ...) remains **reserved** for the raft engine —
no other subsystem may claim the prefix. The `pipeline` families are part
of that engine (the queue is a consequence of its one-thread-per-lane
design), which is why they carry the prefix despite being recorded off the
leader path.

### Worker

Workers can expose their own Prometheus listener, opt-in via the
`LEGION_WORKER_TELEMETRY_PORT` environment variable (a port number; `0`
binds an ephemeral port, logged at startup). It is an env var rather than a
config field deliberately: workers joined with a bare token have no config
file, and hosts packing many worker processes need a distinct port per
process anyway. The listener binds `0.0.0.0:<port>` and serves `/metrics`
and `/healthz` only — a worker has no store or quorum to probe, so there is
no `/readyz`; the manager's node health/heartbeat state is the cluster's
view of worker readiness. Default: disabled.

- **`legion_worker_heartbeats_total{outcome}`** (counter) — heartbeat
  round-trips attempted, by outcome (`ok` | `error` | `unauthenticated`).
  A rising `error` rate is the worker-side view of manager unreachability;
  `unauthenticated` means the node credential was revoked/rotated and the
  worker is re-registering.
- **`legion_worker_heartbeat_duration_seconds`** (histogram) — wall-clock
  of one heartbeat round-trip (status report + reconcile against the
  manager's reply). In a scale test this is how you prove the workers are
  not the bottleneck.
- **`legion_worker_allocations_running`** (gauge) — allocations this worker
  currently runs, as reported on its last heartbeat.
- **`legion_worker_allocation_failures_reported_total`** (counter) —
  allocation failures this worker has reported to the manager.

Edge boot ([edge-boot.md](edge-boot.md)) adds five more, all zero on a node
that has never taken a provisional start:

- **`legion_provisional_starts_total{origin,gate}`** (counter) — provisional
  allocations admitted, by `origin` (`manager` | `edge`) and gate.
- **`legion_provisional_refused_total{reason}`** (counter) — ratifications
  refused. A rising `instance_claimed` rate is a routing problem (two edges
  racing for one sandbox), not a cluster problem; `no_capacity` and
  `ineligible` are the reverse.
- **`legion_provisional_killed_total{cause}`** (counter) — provisional work
  killed, by `refused`, `expired`, or `failed`. A rising `expired` count means
  verdicts are not arriving inside the lease — look at manager reachability
  before raising `ratifyTimeout`.
- **`legion_provisional_ratify_seconds`** (histogram) — admission to verdict.
  This is the latency the whole feature exists to remove from the start path;
  its p99 is what sets a sensible `ratifyTimeout`.
- **`legion_provisional_active`** (gauge) — unratified claims held right now.
  This is how started-but-unconfirmed capacity becomes visible instead of
  invisible; a value that does not return to zero means claims are leaking.

#### Simulated fleets

`legion start worker --simulate N`
([worker-simulation.md](worker-simulation.md)) is a worker process that
happens to carry N nodes, so it exports this same registry from this same
opt-in listener — set `LEGION_WORKER_TELEMETRY_PORT` on the sim host and
scrape it like any other worker.

Everything above is **per-process**: on a sim host the families aggregate
over every node in the fleet. `legion_worker_heartbeats_total` counts the
fleet's heartbeats, not one node's, and `legion_worker_allocations_running`
sums what every node reported on its last heartbeat (a simulated node acks
allocations rather than running them). Three families describe the fleet
itself, and stay at zero in a real worker:

- **`legion_worker_simulated_nodes`** (gauge) — simulated nodes in this
  process that have completed registration.
- **`legion_worker_simulated_nodes_target`** (gauge) — the fleet size the
  process was asked for. A persistent gap below it is a stalled ramp:
  compare against the registration counter to tell "still ramping" from
  "erroring".
- **`legion_worker_simulated_registrations_total{outcome}`** (counter) —
  registration attempts, `ok` or `error`. Re-registrations (after an
  `unauthenticated` heartbeat) count too, so over a long run it outruns
  the node count.

**Per-node series are deliberately not exported.** At the ~1,250 nodes per
host a scale campaign packs, per-node labels are a cardinality bomb, and
the manager already holds the authoritative per-node view. If you need to
know about one simulated node, ask the manager, not the sim host.

Two limitations worth knowing when reading a campaign:

- The fleet-wide allocation gauge is maintained by per-node deltas and
  repaired from the authoritative per-node state every 30s, so a scrape can
  briefly disagree with the manager by a few allocations under heavy churn.
  Counters and the histogram are exact.
- Aborting a fleet (or killing sim nodes for a storm scenario) stops the
  process; it does not walk the gauges back down first. Read a torn-down
  fleet's last sample as "state at teardown", not as a live value.

## Diagnosing a manager throughput ceiling

When RPC throughput plateaus below what the workload should produce, the
handler-duration histogram tells you *that* it is slow, never *why*. Read
these in order — each step rules out a layer:

1. **Is anything actually busy?** `legion_grpc_in_flight` versus
   `legion_runtime_worker_threads`, and `legion_runtime_spawn_delay_seconds`.
   In-flight pinned at the worker count with spawn delay in the
   milliseconds means the runtime is saturated by blocking handlers. The
   tell that it is saturation and not load: a trivial endpoint
   (`HealthCheck`) shows the same latency as an expensive one.
2. **Is the group commit batching?** `legion_store_coalesce_batch_size`'s
   `le="1"` bucket against its `_count`. Nearly all size-1 means batching
   never engages, and the path is paying a full commit per request.
3. **Waiting or committing?** `legion_store_coalesce_wait_seconds` (by
   `role`) against `legion_store_coalesce_commit_seconds`, plus
   `legion_store_coalesce_wait_rounds_total` for the polling share.
4. **Is it replication or the queue in front of it?**
   `legion_raft_pipeline_wait_seconds` and `legion_raft_pipeline_depth`
   against `legion_raft_capture_seconds` and
   `legion_raft_propose_apply_seconds`.

A worked example of the shape this catches: hundreds of heartbeats a second
against a raft control plane, `Heartbeat` p99 in the tens of milliseconds,
`AppendEntries` p99 at 3ms, manager CPU under 25%. Replication was fast, so
the time was queueing and waiting — which none of the pre-existing families
could show.

## OTLP metrics push

Both the manager's and the worker's registries can additionally be
**pushed** over OTLP/HTTP (protobuf) to any OTLP-capable backend — the
same opt-in convention as trace export (docs/observability.md):

- `OTEL_EXPORTER_OTLP_ENDPOINT` — base collector URL; `/v1/metrics` is
  appended for the metrics signal.
- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` — exact metrics endpoint, used
  verbatim.
- `OTEL_METRIC_EXPORT_INTERVAL` — export interval in milliseconds
  (default 60000).
- `LEGION_OTLP=1` — force-enable at the default endpoint.

Each tick exports every registry the process actually initialized (a
worker-only process pushes no manager families and vice versa) with
cumulative temporality and full histogram buckets. Failed exports are
logged at debug and retried on the next tick — the push is never allowed
to back-pressure the daemon. The pull `/metrics` listeners are unaffected
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

- liveness: `GET http://<host>:4749/healthz` — restart the manager on
  sustained failure.
- readiness: `GET http://<host>:4749/readyz` — remove the manager from
  rotation while `503`; in raft mode a follower without a known leader
  reports not-ready even though the process is healthy.

## Relationship to other observability surfaces

Tracing/spans (OTLP export, see docs/observability.md) and the event stream
(docs/event-stream.md) are unchanged; this endpoint adds cheap aggregate
counters for alerting and dashboards where traces are too heavy.
