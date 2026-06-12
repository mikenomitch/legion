# Managers and Workers

This document describes the current manager/worker design in Legion.

## Current Cluster Shape

Legion currently runs as a single-manager control plane plus one or more workers:

- Manager:
  - Owns the scheduler and eval loop.
  - Persists cluster state in local SQLite (`.legion/store.sqlite/store.sqlite` by default).
  - Exposes the main gRPC API on `:4747` (join tokens are minted here via the
    authenticated `CreateJoinToken` RPC; the former insecure `:4748` listener
    was removed).
- Worker:
  - Registers itself with the manager (with a join token when ACLs are
    enforced) and receives a per-node credential for subsequent calls.
  - Runs task allocations via task-kind plugins.
  - Sends periodic heartbeat and health-check updates.

Important: Legion now includes a Raft control-plane mode for manager leader election/failover and operator workflows. Full SQLite state-machine replication through Raft is still in progress; today, managers still persist local SQLite data.

## Manager Startup

`run_manager` starts these loops/services:

- Main manager gRPC server (`Manager` service) on `server.port`.
- Node status checker loop (`start_node_status_checker`).
- Eval worker pool (`start_eval_loop`) with `eval_threads` workers.
- With ACLs enabled: JWKS refresh and expired-credential pruning loops.
- Optional in-process worker (`--run-worker`; the manager self-issues its
  join token when ACLs are enforced).

Note on TLS: opt-in via `server.tls { enabled, certPath, keyPath }` in the
manager config; off by default.

## Worker Startup and Join Flow

Join tokens are minted by `legion join-token create` (the `CreateJoinToken`
RPC, requires `node:write` when ACLs are enabled). The printed token is a
self-describing composite (manager address + secret + worker type); the
manager stores only a hash of the secret. See docs/auth-and-acls.md.

Worker startup (`run_worker`) flow:

1. Decode the join token locally (address, worker type, secret).
2. Connect to manager from token `addr`.
3. Call `WorkerPreRegister` (carrying the join secret when present) to fetch
   worker-type config (`pool`, attributes, resource limits, additional checks).
4. Resolve worker ID:
   - Use `--name` if provided.
   - Else reuse existing single worker state dir.
   - Else generate random ID (optionally with configured prefix).
5. Restore persisted allocations from local worker state file if task PIDs are still alive.
6. Start worker gRPC server (`Worker` service) on `127.0.0.1:4848` by default.
7. Fingerprint node + run node/task readiness checks.
8. Register node via `WorkerRegister`; when ACLs are enforced the reply
   carries a per-node token that the worker attaches to all subsequent
   worker-plane calls (re-registering rotates it; on an Unauthenticated
   heartbeat the worker re-registers with its join secret automatically).
9. Enter steady-state loops:
   - Heartbeats every `WORKER_HEARTBEAT_DURATION_SEC` (default 2s).
   - Node checks every `WORKER_NODE_CHECK_DURATION_SEC` (default 30s).

## Manager <-> Worker Communication

### Manager APIs used by worker

- `WorkerPreRegister`: worker fetches typed config.
- `WorkerRegister`: worker sends initial config/fingerprint/health payload.
- `Heartbeat`: worker sends:
  - active allocation IDs,
  - failed allocations,
  - allocation health snapshots.
  Manager responds with desired allocations.
- `HealthCheck`: worker sends node health check JSON.

### Worker APIs used by manager

- `PostWork`: manager dispatches task allocations to workers.
- `RestartWorker`: manager can instruct a worker process restart.

### Allocation reconciliation model

- Manager is source of truth for desired allocations (`allocation` table).
- Worker reports current allocations in heartbeat.
- Manager returns desired allocations for that node.
- Worker reconciles by stopping undesired tasks and starting missing ones.

## Node Loss and Recovery Behavior

Two code paths exist:

- Operational path (used in runtime):
  - Manager node checker marks stale-heartbeat nodes as lost.
  - Lost-node allocations are removed and reconciled immediately via store functions.
- Eval path (used in tests/manual eval insertion):
  - `node_drop` / `node_join` eval kinds call equivalent store reconciliation logic.

## What Is Not Implemented Yet

From current code and task list:

- Manager-to-manager sync/consensus.
- Robust TLS join/rotation flow.
- Automatic cross-manager failover behavior.

