# State Store

This document describes persistence used by manager and worker processes.

## Manager Store (SQLite)

Manager state lives in SQLite under manager `store_path`.
Default resulting file path: `.legion/store.sqlite/store.sqlite`.

Startup (`Store::start_storage`):

- opens writable connection
- runs migrations (`src/storage/migrations.rs`)
- creates read-only connection pool
- enables WAL mode and foreign keys on write connection
- initializes in-memory scheduling read model cache

## Connection Model

`Store` maintains:

- one write connection (`write_conn`)
- a small read-only pool (`read_conns`, query-only)
- round-robin read selection

This reduces lock contention for read-heavy paths.

## Read Model Cache

In-memory `SchedulingReadModel` cache stores:

- nodes
- allocations by node
- task configs by job config

Scheduler hot paths consume this cache (`cached_nodes`, `cached_allocations_by_node`, `cached_task_configs_for_job_config`).
Write operations update both DB and cache.

## Key Tables

Main tables (see migrations):

- `node`, `worker_type`
- `job`, `job_config`, `task_config`, `task_status`, `job_update`
- `allocation`
- `eval`
- `manager_config`
- `kv_entry`, `kv_lock`, `job_kv_reference`
- `allocation_epoch`, `plan_submission`
- `rollout`, `allocation_health`, `rollout_event`

## Allocation Epoch and Plan Submission Safety

Scheduler apply path uses compare-and-swap semantics:

1. Read `allocation_epoch`.
2. Record plan with `expected_epoch`.
3. Apply only when epoch still matches.
4. Reject stale plans (`PlanApplyOutcome::Rejected`) and retry at caller level.

This avoids writing placements against stale cluster state.

## Error Model

Storage maps SQLite errors into `StorageError` categories:

- `NotFound`
- `Conflict`
- `InvalidInput`
- lock/busy/read-only/corrupt/io/sqlite

Manager API maps these to gRPC status codes.

## Worker Local State Store

Workers have a separate local JSON state file (`WorkerStateStore`):

- default path root: `.legion/workers/<worker-id>/worker_state.json`
- stores allocation + task + PID records
- used for restart recovery if tracked PID is still running

This store is local worker runtime state, not shared cluster state.

## What Is Not Implemented Yet

Based on task list, durable replicated manager state (Raft/litestream-like replication) is planned but not implemented.

