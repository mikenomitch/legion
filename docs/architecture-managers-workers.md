# Managers and Workers

This document describes the current manager/worker design in Legion.

## Current Cluster Shape

Legion currently runs as a single-manager control plane plus one or more workers:

- Manager:
  - Owns the scheduler and eval loop.
  - Persists cluster state in local SQLite (`.legion/store.sqlite/store.sqlite` by default).
  - Exposes the main gRPC API on `:4747`.
  - Exposes an insecure join-token API on `:4748`.
- Worker:
  - Registers itself with the manager.
  - Runs task allocations via task-kind plugins.
  - Sends periodic heartbeat and health-check updates.

Important: Legion now includes a Raft control-plane mode for manager leader election/failover and operator workflows. Full SQLite state-machine replication through Raft is still in progress; today, managers still persist local SQLite data.

## Manager Startup

`run_manager` starts these loops/services:

- Join server (`InsecureManager` service) on `server.insecure_port`.
- Main manager gRPC server (`Manager` service) on `server.port`. When
  `workloadIdentity.enabled` is set, this same listener also answers two
  plain HTTP/1.1 GET routes for the manager-as-OIDC-IdP role —
  `/.well-known/openid-configuration` and the JWKS path — via a tower layer
  mounted in front of the gRPC stack (`src/manager/oidc_http.rs`); external
  relying parties (AWS/GCP/Vault) fetch these with generic HTTP clients.
- Node status checker loop (`start_node_status_checker`).
- Eval worker pool (`start_eval_loop`) with `eval_threads` workers.
- Workload-identity signing-key rotation loop (only when
  `workloadIdentity.enabled`; primary manager only in HA setups). See
  [docs/workload-identity.md](workload-identity.md).
- Optional in-process worker (`--run-worker`).

Note on TLS: server TLS discovery exists, but `load_server_tls_identity()` currently returns `Ok(None)`, so manager traffic runs without TLS today.

## Worker Startup and Join Flow

Worker startup (`run_worker`) flow:

1. Decode join token (JWT), validate expiry unless `skip_expiry_validation`.
2. Connect to manager from token `address`.
3. Call `WorkerPreRegister` to fetch worker-type config (`pool`, attributes, resource limits, additional checks).
4. Resolve worker ID:
   - Use `--name` if provided.
   - Else reuse existing single worker state dir.
   - Else generate random ID (optionally with configured prefix).
5. Restore persisted allocations from local worker state file if task PIDs are still alive.
6. Start worker gRPC server (`Worker` service) on `127.0.0.1:4848` by default.
7. Fingerprint node + run node/task readiness checks.
8. Register node via `WorkerRegister`.
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
- `IssueWorkloadIdentityToken`: worker fetches short-lived OIDC tokens for
  identities declared on its allocations' tasks (at launch and on refresh);
  the manager derives all claims server-side and only serves allocations
  placed on the calling node. See [docs/workload-identity.md](workload-identity.md).

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

