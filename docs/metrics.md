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

```jsonnet
{
  server: { port: 4747, host: "127.0.0.1" },
  // ...
  telemetry: {
    enabled: true,   // default: true
    port: 4749,      // default: server.port + 2
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

### Scheduler / eval loop

- **`legion_evals_processed_total{kind, status}`** (counter) — evals
  completed by the eval workers. `kind` is the eval kind (`job_submit`,
  `node-left`, ...); `status` is `ok` or `error`.
- **`legion_eval_queue_depth`** (gauge) — pending (unclaimed) evals in the
  queue, including delayed evals not yet ready. Updated transactionally on
  enqueue, claim, and startup requeue.
- **`legion_plan_apply_total{outcome}`** (counter) — scheduler plan
  submissions applied to the store; `outcome` is `applied` or `rejected`
  (rejections are optimistic-concurrency epoch conflicts or late constraint
  failures — see docs/scheduler-and-eval-loop.md).

### Cluster shape

- **`legion_nodes{status}`** (gauge) — registered worker nodes by status
  (`active`, `lost`, ...). Refreshed on the node-status-checker tick
  (`workerCheckSeconds`), so it lags by at most one tick; a status that
  empties out drops to `0` rather than disappearing.
- **`legion_allocations`** (gauge) — live allocations tracked by the
  manager. Refreshed on the same tick.
- **`legion_heartbeats_total`** (counter) — worker heartbeat RPCs accepted
  (past authentication) by the manager.

### Store

- **`legion_store_write_lock_wait_seconds`** (histogram) — time spent
  waiting to acquire the store's SQLite write-connection lock. Sustained high
  buckets indicate write contention on the state store.

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

The rest of the `legion_raft_*` prefix (term, commit/applied index,
leadership changes, replication lag, ...) remains **reserved** for the raft
persistence engine work — nothing else emits it yet, and no other subsystem
may claim the prefix.

## Scrape configuration example

```yaml
scrape_configs:
  - job_name: legion-manager
    metrics_path: /metrics
    static_configs:
      - targets: ["10.0.0.5:4749"] # server.port + 2 (or telemetry.port)
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
