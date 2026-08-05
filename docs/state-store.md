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
- a read-only pool (`read_conns`, query-only)
- round-robin read selection, with steal-on-contention

This reduces lock contention for read-heavy paths.

The pool holds `evalThreads + 4` connections, capped at 16. Eval workers are
the manager's concurrent readers, and the `+ 4` covers the readers that are
not eval workers: worker heartbeats, the read-only gRPC and UI surface, the
metrics scrape, and the background reconcilers. A reader takes any free
connection before it queues on a busy one, so one slow query does not stall
the readers behind it.

The cap exists for memory, not contention. Every connection carries its own
SQLite page cache capped at 64 MiB (`cache_size = -65536`), and the caches do
not share — 16 connections is a 1 GiB ceiling for a cluster whose working set
is large enough to reach it. `cache_size` is a cap that SQLite fills lazily,
so a small deployment holds only what it reads.

There is no separate configuration field for the pool size; raising
`evalThreads` widens it. `legion_store_read_lock_wait_seconds`
([metrics.md](metrics.md)) is how you tell whether it is short — its `le="0"`
bucket against `_count` is how often a reader found a free connection at once.

## Read Model Cache

In-memory `SchedulingReadModel` cache stores:

- nodes (`Arc`'d, so a plan's node snapshot is refcount bumps rather than
  deep clones of every node's JSON attributes)
- allocations by node (each node's vector `Arc`'d the same way; the few
  writers copy-on-write)
- task configs by job config
- per-node free resource pools (fingerprinted capacity minus current
  allocation usage, for available nodes)
- the scheduling indexes ([scheduling-indexes.md](scheduling-indexes.md))

Scheduler hot paths consume this cache (`available_node_arcs`,
`plan_capacity_snapshot`, `cached_task_configs_for_job_config`; other
callers use the deep-clone `cached_nodes`). Write operations update both DB
and cache; the same write-locked pass re-derives the affected node's free
pool and index projections, so derived state can never be observed out of
step with the nodes and allocations it came from.

## Key Tables

Main tables (see migrations):

- `node` (identity/pool/attrs/availability), `node_health` (liveness:
  status, last heartbeat, lost-at marker, this node's own lost window
  `lost_after_seconds`, health checks — split out so the raft status lane is
  the table's single writer; readers hydrate `Node` via a
  `node ⟕ node_health` join, and a node that has not heartbeated yet
  hydrates registration defaults). Note that `last_heartbeat` /
  `last_heartbeat_unix` are **display state**: a steady-state heartbeat
  writes nothing here, and the columns are refreshed by a periodic
  checkpoint (see [node-liveness.md](node-liveness.md))
- `worker_type`
- `job`, `job_config`, `task_config`, `task_status`, `job_update`
- `allocation`
- `eval`
- `manager_config`
- `kv_entry`, `kv_lock`, `job_kv_reference`
- `allocation_epoch`, `plan_submission`
- `rollout`, `allocation_health`, `rollout_event`
- `instance` (stateful identity), `volume` (the storage its handle points at —
  see [volumes.md](volumes.md))

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

## Persistence Modes

Everything above describes the store in every mode; `persistence.type`
selects how durable the writes are:

- **`local_disk`** (default): local SQLite only, exactly as described above.
- **`streaming`**: Litestream-style WAL shipping to object storage with
  lease-fenced failover — see `docs/persistence-streaming.md`.
- **`raft`**: **real quorum-replicated writes.** Every mutating store
  method routes through `Store::replicated()`: its row-level effects are
  captured as a SQLite session changeset on the leader (and rolled back
  locally), committed through openraft, and applied byte-identically on
  every manager — replicas cannot diverge. The read-model cache and event
  stream are projected from the committed changeset at apply time, so
  followers get both for free and nothing is visible before quorum commit.
  In raft mode, `write_connection()` outside the capture/apply paths fails
  loudly (a runtime bypass guard), so no code path can mutate state
  without replicating it. Snapshots, log compaction, membership, and
  backup/restore are all implemented — see `docs/multi-raft.md` and
  `docs/raft-operations.md`.

Server-side backups exist in every mode: `legion state backup` (a
`VACUUM INTO` copy plus the raft applied index) and
`legion start manager --restore-from <backup>` — see `docs/upgrades.md`.

## What Is Not Implemented Yet

- Raft `topology = "lanes"` (independent per-domain raft groups) is
  runnable and measured but stays **opt-in** — `single` remains the
  recommended default. See `docs/multi-raft.md` § "Topology: `single` vs
  `lanes`".
- Non-leader **client** writes are rejected with a leader hint, not
  auto-forwarded; worker-plane writes (heartbeats, reports) forward to the
  owning lane's leader (`docs/multi-raft.md` § "Non-leader write redirects
  and forwarding").
- Raft transport auth is a shared cluster secret (mTLS pending); of the
  `legion_raft_*` metrics series only the voters/quorum gauges are emitted,
  the rest is reserved but not yet emitted.
- Key-range sharding, cross-region topologies, and parallel scheduler
  planning across managers remain future work.
