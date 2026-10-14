# Observability: Current State

This page lists the observability Legion has today: logs, traces, metrics,
events, and health signals. Each section links to the page that covers it in
depth.

## Logging and Tracing Setup

`init_tracing()` / `init_tracing_with_options()` (`src/tracing_util.rs`)
configure the `tracing_subscriber` stack:

- Pretty console logs (stdout, or stderr for stdio protocol servers like
  `legion mcp serve`).
- JSON file logs, daily rolling under `./logs/events.log.<date>`.
- `EnvFilter` from the environment (default `info`). You control log verbosity
  with standard `RUST_LOG`-style tracing filters.
- With `--o11y`, an extra JSON span layer (span new/close events) writes
  daily-rolling files under `./traces/spans.json.<date>`.

Old files are pruned at startup: Legion keeps 7 days of `./logs` and 3 days of
`./traces`.

## OTLP Trace Export (OpenTelemetry)

Legion can export its own control-plane spans over **OTLP/HTTP** to any
OpenTelemetry backend (Tempo, Jaeger, Honeycomb, an OTel Collector, and so on).
The exporter lives in `src/otel.rs` and is wired into the subscriber stack in
`src/tracing_util.rs`.

Export is opt-in. Turn it on with the standard OpenTelemetry env vars:

- `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4318`)
- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
- `OTEL_SERVICE_NAME`
- `LEGION_OTLP=1` to force the default endpoint

Only long-running servers (`legion start manager` / `legion start worker`)
export. Short CLI invocations never start an exporter.

For the full configuration reference, see the "OpenTelemetry traces for Legion
itself" section of [docs/logging.md](logging.md).

## Metrics and Health Probes

The manager serves a separate telemetry HTTP listener
(`src/manager/telemetry.rs`, default port `server.port + 2`):

- `GET /metrics`: Prometheus text exposition (`legion_*` families such as gRPC
  request counts and latency, evals, plan applies, heartbeats, nodes, and
  events).
- `GET /healthz`: liveness probe.
- `GET /readyz`: readiness probe (a store read probe, plus a known raft leader
  when in raft mode).

Full reference: [docs/metrics.md](metrics.md).

## Event Stream

Legion journals cluster lifecycle events (jobs, nodes, rollouts, drains, and
more). You can stream them with `ListEvents`/`StreamEvents`, the CLI, MCP, and
the UI. See [docs/event-stream.md](event-stream.md).

## Task Logs

Task stdout/stderr capture, rotation, size caps, disk sweep, and the CLI/UI
log surfaces are covered in [docs/logging.md](logging.md).

## Runtime Health Signals

The system records operational health in these places:

- Node heartbeat and node health check payloads on the manager.
- Allocation health snapshots (`allocation_health` table) reported from workers.
- Rollout event stream (`rollout_event` table) with structured metadata.

The UI rollout and node views read from these.

### Slow task starts

When a start is slow, the worker says where the time went. Each start logs one
`Task allocation started` line with the time each stage took (artifact
staging, volume mounts, `prepare`, the runtime's `start` and its own steps,
registration), and the worker's opt-in `/metrics` listener exports the same
stages as `legion_task_start_stage_seconds{runtime,stage}`, plus `health`, the
time to the first passing check. See [metrics.md](metrics.md) for the stage
table.

## What Exists in the UI

- Node health summaries inferred from health check payloads.
- Rollout event log and allocation health overlays.
- Relative timestamps and statuses for jobs/nodes/rollouts.

## Raft persistence observability

When manager persistence is set to `raft` (quorum-replicated writes, see
`docs/multi-raft.md`), the engine's live metrics stream backs these surfaces:

- `GetRaftStatus` / `legion raft status` / the UI Raft page: role, current
  leader, term, commit/applied indexes, voters/learners, and per-lane
  detail when more than one raft group is active.
- `ListRaftHistory` / `legion raft history`: recorded transitions (elections,
  role changes, membership changes, join-token issuance) plus operator
  commands attributed to the actor who ran them.
- `/readyz`: in raft mode, readiness also requires a known leader. Under
  `topology = "lanes"`, every lane needs a known leader. Once a ~10s
  post-election grace window has passed, it must be the same manager on all
  lanes (lane leadership is co-located on a best-effort basis; see
  docs/metrics.md and docs/multi-raft.md § "Lane leadership").

The `legion_raft_*` Prometheus prefix carries the engine series:

- Quorum shape (`voters`/`quorum`).
- Per-lane progress (`term`, `commit_index`, `applied_index`, `apply_lag`,
  `is_leader`).
- Histograms that show where time goes (`capture_seconds` vs
  `propose_apply_seconds`, plus `ops_total{lane,outcome}`).

See `docs/metrics.md` for details. Snapshot timings and per-peer replication
lag are reserved and not yet emitted.

## OTLP metrics push

Besides the pull-model `/metrics` listeners, the manager and the worker can
both push their metric registries over OTLP/HTTP to any OTLP backend. This
uses the same `OTEL_*` env-var conventions as trace export
(`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, `OTEL_METRIC_EXPORT_INTERVAL`,
`LEGION_OTLP=1`). Pushes use cumulative temporality and full histogram buckets.
Details are in `docs/metrics.md` § "OTLP metrics push".

## Gaps

What does **not** exist yet:

- No built-in trace viewer in the UI. For now, use the `--o11y` span files or
  OTLP export to an external backend.
- The worker `/metrics` listener is opt-in (`LEGION_WORKER_TELEMETRY_PORT`).
  It exposes a small set of families (heartbeats, running allocations, reported
  failures) and no per-task runtime metrics yet.
- Raft snapshot timings and per-peer replication lag are reserved and not yet
  emitted.
