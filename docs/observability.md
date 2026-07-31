# Observability: Current State

This document describes what observability exists today and where each piece is
documented in depth.

## Logging and Tracing Setup

`init_tracing()` / `init_tracing_with_options()` (`src/tracing_util.rs`)
configure the `tracing_subscriber` stack:

- Pretty console logs (stdout, or stderr for stdio protocol servers like
  `legion mcp serve`).
- JSON file logs, daily rolling under `./logs/events.log.<date>`.
- `EnvFilter` from the environment (default `info`), so log verbosity is
  controlled with standard `RUST_LOG`-style tracing filters.
- With `--o11y`, an additional JSON span layer (span new/close events) writes
  daily-rolling files under `./traces/spans.json.<date>`.

Retention is pruned at startup: 7 days for `./logs`, 3 days for `./traces`.

## OTLP Trace Export (OpenTelemetry)

Legion's own control-plane spans can be exported over **OTLP/HTTP** to any
OpenTelemetry backend (Tempo, Jaeger, Honeycomb, an OTel Collector, …). This
is implemented in `src/otel.rs` and wired into the subscriber stack in
`src/tracing_util.rs`. It is opt-in via the standard OpenTelemetry env vars
(`OTEL_EXPORTER_OTLP_ENDPOINT`, default `http://localhost:4318`;
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`; `OTEL_SERVICE_NAME`; or `LEGION_OTLP=1`
to force the default endpoint) and is limited to long-running servers
(`legion start manager` / `legion start worker`) — short CLI invocations never
spin up an exporter.

Full configuration reference: the "OpenTelemetry traces for Legion itself"
section of [docs/logging.md](logging.md).

## Metrics and Health Probes

The manager serves a dedicated telemetry HTTP listener
(`src/manager/telemetry.rs`, default port `server.port + 2`):

- `GET /metrics` — Prometheus text exposition (`legion_*` families: gRPC
  request counts/latency, evals, plan applies, heartbeats, nodes, events, …).
- `GET /healthz` — liveness probe.
- `GET /readyz` — readiness probe (store read probe; raft leader known when in
  raft mode).

Full reference: [docs/metrics.md](metrics.md).

## Event Stream

Cluster lifecycle events (jobs, nodes, rollouts, drains, …) are journaled and
streamable via `ListEvents`/`StreamEvents`, the CLI, MCP, and the UI. See
[docs/event-stream.md](event-stream.md).

## Task Logs

Task stdout/stderr capture, rotation, size caps, disk sweep, and the CLI/UI
log surfaces are documented in [docs/logging.md](logging.md).

## Runtime Health Signals

The system records operational health in these places:

- Node heartbeat and node health check payloads on manager.
- Allocation health snapshots (`allocation_health` table) reported from workers.
- Rollout event stream (`rollout_event` table) with structured metadata.

These are used by the UI rollout and node views.

## What Exists in the UI

- Node health summaries inferred from health check payloads.
- Rollout event log and allocation health overlays.
- Relative timestamps and statuses for jobs/nodes/rollouts.

## Raft persistence observability

When manager persistence is configured as `raft` (real quorum-replicated
writes — `docs/multi-raft.md`), the engine's live metrics stream backs:

- `GetRaftStatus` / `legion raft status` / the UI Raft page: role, current
  leader, term, commit/applied indexes, voters/learners, and per-lane
  detail when more than one raft group is active.
- `ListRaftHistory` / `legion raft history`: real transitions (elections,
  role changes, membership changes, join-token issuance) plus
  actor-attributed operator commands.
- `/readyz`: in raft mode readiness additionally requires a known leader —
  under `topology = "lanes"`, on every lane, and the same manager on all of
  them once a ~10s post-election grace window has passed (lane leadership is
  co-located, best effort; see docs/metrics.md and
  docs/multi-raft.md § "Lane leadership").

The `legion_raft_*` Prometheus prefix carries the engine series: quorum
shape (`voters`/`quorum`), per-lane progress (`term`, `commit_index`,
`applied_index`, `apply_lag`, `is_leader`), and the bottleneck-attribution
histograms (`capture_seconds` vs `propose_apply_seconds`, plus
`ops_total{lane,outcome}`) — see `docs/metrics.md`. Snapshot timings and
per-peer replication lag remain reserved.

## OTLP metrics push

Beyond the pull-model `/metrics` listeners, both the manager's and the
worker's metric registries can be pushed over OTLP/HTTP to any OTLP
backend, gated by the same `OTEL_*` env-var conventions as trace export
(`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, `OTEL_METRIC_EXPORT_INTERVAL`,
`LEGION_OTLP=1`). Cumulative temporality, full histogram buckets; details
in `docs/metrics.md` § "OTLP metrics push".

## Gaps

Honest list of what does **not** exist yet:

- No first-class trace viewer in the UI (`--o11y` span files and OTLP export
  to an external backend are the current options).
- The worker `/metrics` listener is opt-in (`LEGION_WORKER_TELEMETRY_PORT`)
  and its family set is small (heartbeats, running allocations, reported
  failures) — no per-task runtime metrics yet.
- Raft snapshot timings and per-peer replication lag are reserved, not yet
  emitted.
