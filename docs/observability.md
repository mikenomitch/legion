# Observability: Current State

This document describes what observability exists today.

## Logging and Tracing Setup

`init_tracing()` configures two `tracing_subscriber` layers:

- pretty stdout logs
- JSON file logs (daily rolling under `./logs/events.log.<date>`)

`EnvFilter::from_default_env()` is enabled, so log verbosity can be controlled with standard tracing env filters.

## OTEL Status

There is an explicit note in code that the current file JSON layer is not yet a real OTEL export pipeline.

Current state:

- OTEL-compatible/standardized export pipeline is not complete.
- Traces are not shipped to a backend by default.

## Runtime Health Signals

The system currently records operational health in these places:

- Node heartbeat and node health check payloads on manager.
- Allocation health snapshots (`allocation_health` table) reported from workers.
- Rollout event stream (`rollout_event` table) with structured metadata.

These are used by the UI rollout and node views.

## What Exists in the UI

- Node health summaries inferred from health check payloads.
- Rollout event log and allocation health overlays.
- Relative timestamps and statuses for jobs/nodes/rollouts.

## Gaps (From Task List)

Planned but not yet complete:

- Scheduler-focused tracing depth improvements.
- First-class trace viewer in UI.
- Broader metrics/perf instrumentation.
- Event-stream and log UX improvements.



## Raft control-plane observability

When manager persistence is configured as `raft`, observability includes raft role/leader/term and operator event history (elections, transfers, membership changes, join-token issuance). These signals are available via gRPC (`GetRaftStatus`, `ListRaftHistory`) and the UI Raft page.
