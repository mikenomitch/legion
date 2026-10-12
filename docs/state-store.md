# State Store

This page describes how manager and worker processes persist state.

## Manager Store (SQLite)

Manager state lives in SQLite under the manager's `store_path`.
The default file path is `<data dir>/store.sqlite`, where the data dir is
`$LEGION_DATA_DIR` or `.legion` (see [data-directory.md](data-directory.md)).

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
the manager's main concurrent readers. The extra 4 cover the other readers:
worker heartbeats, the read-only gRPC and UI surface, the metrics scrape, and
the background reconcilers. A reader takes any free connection before it
queues on a busy one, so one slow query does not stall the readers behind it.

The cap of 16 limits memory use. Every connection has its own SQLite page
cache capped at 64 MiB (`cache_size = -65536`), and the caches are not
shared. That makes 16 connections a 1 GiB ceiling for a cluster whose working
set is large enough to reach it. SQLite fills the cache lazily, so a small
deployment holds only what it reads.

There is no separate configuration field for the pool size. Raise
`evalThreads` to widen it. Use `legion_store_read_lock_wait_seconds`
([metrics.md](metrics.md)) to tell whether the pool is too small. Its `le="0"`
bucket compared to `_count` shows how often a reader found a free connection
right away.

## Read Model Cache

The in-memory `SchedulingReadModel` cache stores:

- nodes (`Arc`'d, so a plan's node snapshot costs refcount bumps instead of
  deep clones of every node's JSON attributes)
- allocations by node (each node's vector is `Arc`'d the same way, and the
  few writers copy on write)
- task configs by job config
- per-node free resource pools (fingerprinted capacity minus current
  allocation usage, for available nodes)
- the scheduling indexes ([scheduling-indexes.md](scheduling-indexes.md))

Scheduler hot paths read this cache through `available_node_arcs`,
`plan_capacity_snapshot`, and `cached_task_configs_for_job_config`. Other
callers use the deep-clone `cached_nodes`. The same write-locked pass that
updates a node or its allocations re-derives that node's free pool and index
projections, so derived state is never visible out of step with the nodes and
allocations it came from.

### Read model coherence

SQLite is the source of truth and the cache follows it. In every persistence
mode, the cache is updated from the committed changeset of each write, not by
the write's own code:

- **`raft`**: each manager projects the cache from the changeset it applies
  (see [Persistence Modes](#persistence-modes)).
- **`local_disk` and `streaming`**: each mutation runs with a SQLite session
  attached to the cache's tables (`node`, `node_health`, `allocation`,
  `task_config`, `allocation_epoch_partition`). When the mutation returns, the
  cache is projected from what it committed. That includes rows removed by
  `ON DELETE CASCADE` and rows a mutation committed before it failed.
  Mutations run one at a time on a process-wide lock, so projections apply in
  commit order.

A stale cache cannot oversubscribe a node, because plan-apply rechecks free
capacity against SQLite inside its write transaction. It can still make the
scheduler skip a node that has room, or keep picking one that does not. Two
background checks run every 5 seconds on every manager to bound that:

- **Outside writes.** If another process commits to the state database (for
  example `sqlite3` against the live file), the manager notices through
  `PRAGMA data_version`, rebuilds the whole cache from disk, logs a warning,
  and increments `legion_read_model_external_writes_total`. Writing the state
  database while the manager runs is still unsupported. This check only
  limits the damage. A `TRUNCATE` or `RESTART` checkpoint run by another
  process also counts as an outside write and costs one rebuild. The
  manager's own checkpoints are `PASSIVE` and do not count.
- **Scrub.** The manager reloads 128 nodes per tick from SQLite (the node row
  and its allocation list), compares them with the cache, and repairs any
  that differ. The rotation covers every node, cached or committed, so a
  10,000-node fleet is swept in about 6.5 minutes. Each repair logs a warning
  and increments `legion_read_model_drift_repaired_total{source="scrub"}`.
  Task configs are not scrubbed.

Either counter rising in a healthy cluster is a bug worth reporting.

For tests and soak runs, `LEGION_READ_MODEL_SHADOW_CHECK=1` compares the whole
cache with a fresh load after every `local_disk` mutation and panics on any
difference, naming the operation. It is always on in Legion's unit tests. It
reloads the whole cache on every write, so never set it in production. It does
not cover raft replicas, which apply lanes concurrently and have no quiet point
to compare at.

## Key Tables

Main tables (see migrations):

- `node` (identity, pool, attributes, availability) and `node_health`
  (liveness: status, last heartbeat, lost-at marker, the node's own lost
  window `lost_after_seconds`, and health checks). `node_health` is a
  separate table so the raft status lane is its only writer. Readers build
  `Node` with a `node ⟕ node_health` join, and a node that has not sent a
  heartbeat yet gets registration defaults. `last_heartbeat` and
  `last_heartbeat_unix` are display state only. A steady-state heartbeat
  writes nothing here, and a periodic checkpoint refreshes the columns (see
  [node-liveness.md](node-liveness.md)).
- `worker_type`
- `job`, `job_config`, `task_config`, `task_status`, `job_update`
- `allocation`
- `eval`
- `manager_config`
- `kv_entry`, `kv_lock`, `job_kv_reference`
- `allocation_epoch`, `plan_submission`
- `node_alloc_usage`, a cache rather than a source of truth. It holds one
  row per node with the aggregated resource usage of that node's
  allocations. Each row also stores a fingerprint of the rows it was derived
  from and of the resource policy that normalized them. Only the plan-apply
  capacity recheck reads it, and only when both fingerprints still match. A
  row written under a different `taskResourceDefaults`/`customResources`
  configuration is rebuilt instead of served
  (see [scheduler-and-eval-loop.md](scheduler-and-eval-loop.md#the-per-node-usage-cache)).
- `rollout`, `allocation_health`, `rollout_event`
- `instance` (stateful identity), `volume` (the storage its handle points at,
  see [volumes.md](volumes.md))

### Column Affinity

JSON payload columns (`allocation.resources`, `plan_submission.placements`,
`task_config.*`, node attributes, and the rest) are declared `TEXT`. Older
releases declared them `JSONB`, which SQLite's affinity rules map to NUMERIC
affinity. That made every insert try to parse the whole JSON blob as a
number before storing it as text, which wasted CPU on the plan-apply and
job-intake write paths. A migration rebuilds the affected tables in place.
It preserves data, column order, indexes, and rowids, so a rolling manager
upgrade across it is safe.

`STRING` and `TIMESTAMP` columns keep NUMERIC affinity on purpose. For those
columns SQLite's numeric conversion can succeed (numeric-looking ids,
unix-seconds timestamps), so changing their type would change what is stored
and break comparisons against existing rows.

## Allocation Epoch and Plan Submission Safety

The scheduler's apply path uses compare-and-swap semantics:

1. Read `allocation_epoch`.
2. Record plan with `expected_epoch`.
3. Apply only when epoch still matches.
4. Reject stale plans (`PlanApplyOutcome::Rejected`) and retry at the caller.

This prevents writing placements against stale cluster state.

## Error Model

Storage maps SQLite errors into `StorageError` categories:

- `NotFound`
- `Conflict`
- `InvalidInput`
- lock/busy/read-only/corrupt/io/sqlite

The manager API maps these to gRPC status codes.

## Worker Local State Store

Workers have a separate local JSON state file (`WorkerStateStore`):

- default path root: `<data dir>/workers/<worker-id>/worker_state.json`
- stores allocation, task, and PID records
- used for restart recovery when a tracked PID is still running

This store holds local worker runtime state. It is not shared cluster state.

## Persistence Modes

Everything above applies in every mode. `persistence.type` selects how
durable the writes are:

- **`local_disk`** (default): local SQLite only, as described above.
- **`streaming`**: Litestream-style WAL shipping to object storage with
  lease-fenced failover. See `docs/persistence-streaming.md`.
- **`raft`**: quorum-replicated writes. Every mutating store method routes
  through `Store::replicated()`. Its row-level effects are captured as a
  SQLite session changeset on the leader (and rolled back locally),
  committed through openraft, and applied byte-identically on every
  manager, so replicas cannot diverge. The read-model cache and event
  stream are built from the committed changeset at apply time. Followers
  get both without extra work, and nothing is visible before quorum commit.
  The cache update is a delta. The changeset names which allocation rows
  changed, so the cache patches those rows instead of reloading every
  touched node's allocation list. The cost of a commit follows the size of
  the change, not how full the cluster is.
  A delta cannot repair a cache entry that is already wrong, so three
  safeguards apply. Any node whose delta contradicts the cache is reloaded
  in full. Any changeset whose allocation records cannot be classified
  falls back to a full reload. And one node per changeset is reloaded in
  full on a round-robin sweep of the fleet. The fallbacks log at `warn`
  with the allocation ids or nodes involved, and a contradicted delta
  increments `legion_read_model_drift_repaired_total{source="delta"}`. They
  should not happen in a healthy cluster. Repeated occurrences mean the cache
  and committed state disagree.
  In raft mode, calling `write_connection()` outside the capture and apply
  paths fails with an error (a runtime bypass guard), so no code path can
  change state without replicating it. Snapshots, log compaction,
  membership, and backup/restore are all implemented. See
  `docs/multi-raft.md` and `docs/raft-operations.md`.

Server-side backups work in every mode: `legion state backup` (a
`VACUUM INTO` copy plus the raft applied index) and
`legion start manager --restore-from <backup>`. See `docs/upgrades.md`.

## What Is Not Implemented Yet

- Raft `topology = "lanes"` (independent per-domain raft groups) works but
  is opt-in. `single` is the recommended default. See `docs/multi-raft.md`
  § "Topology: `single` vs `lanes`".
- Write forwarding is one hop. A non-leader replays a client write to the
  leader (`docs/raft-operations.md` § "Writes on any manager") and forwards
  worker-plane writes per operation (`docs/multi-raft.md` § "Non-leader
  write redirects and forwarding"); when the leader is unreachable from the
  manager that received the write, it fails with the leader hint rather than
  being retried elsewhere.
- Managers dial each other's raft transport over plaintext HTTP/2 and
  authenticate with a shared cluster secret. There is no mTLS between
  managers.
- Key-range sharding, cross-region topologies, and parallel scheduler
  planning across managers are not implemented.
