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
  - Runs task allocations via task-runtime plugins.
  - Sends periodic heartbeat and health-check updates.

Important: Legion now includes a Raft control-plane mode for manager leader election/failover and operator workflows. Full SQLite state-machine replication through Raft is still in progress; today, managers still persist local SQLite data.

## Manager Startup

`run_manager` starts these loops/services:

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
   - Heartbeats every `heartbeatSeconds` (worker-type config; default 2s,
     clamped to 1–300s), with ±10% jitter per beat so a fleet that
     (re)started together does not stay phase-locked. A manager running
     adaptive cadence can serve a different interval on the heartbeat reply
     ([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)).
   - Node checks every `WORKER_NODE_CHECK_DURATION_SEC` (default 30s).

### Heartbeat cadence and scale

The heartbeat interval is the manager-load knob for large fleets: 10k
workers at the default 2s cadence is 5,000 heartbeat RPCs/second.

- **`heartbeatSeconds`** in a worker-type config sets that worker type's
  cadence (served to workers at pre-registration). The lost-node window
  MOVES WITH IT: a node is lost after `missedBeatsBeforeLost` (5) missed
  beats, so `5 x 2s` is the historical fixed 10s and a 60s batch pool gets a
  300s window without touching the 2s pool's. That is what makes the knob
  usable — under the old fixed 10s window, anything above ~3s traded
  detection margin for load.
- **A steady-state heartbeat costs no raft commit and no fsync.** It is one
  write into the in-memory liveness table on the manager that owns node
  status; only status TRANSITIONS are replicated. A manager that does not
  own liveness reports what it heard in one digest RPC per second, whatever
  the fleet size. See [node-liveness.md](node-liveness.md).
- **Group commit still exists** for the paths that do commit — the display
  checkpoint, and the mixed-version fallback during a manager upgrade:
  concurrent writes coalesce into one status-lane commit (one raft round for
  up to 128 nodes).
- **Cheap liveness ticks**: the mark-lost scan compares monotonic ages held
  in memory against each node's own window — no per-node timestamp parsing
  and no database read of the heartbeat clock at all.
- **One number says whether the fleet is keeping up**:
  `legion_heartbeat_freshness_ratio`, or `legion node cadence`.

Measured throughput per backend and fleet size lives in
`benchmarks/scheduler/README.md`
([testing-and-benchmarks.md](testing-and-benchmarks.md) has the runner).
Sizing a control plane for a fleet — the `R x I` formula, the table, and the
measurement recipe — is in
[heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md).

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
  - Failover grace: liveness lives in the memory of the manager that owns
    node status, so a manager that has just BECOME responsible for it — at
    process start, or by acquiring status-lane leadership mid-life — inherits
    nothing. It seeds every active node at `now` and marks nothing lost for
    `liveness.failoverGrace` (60s), giving reconnecting workers time to
    check in. A manager restart, an election, or a rolling upgrade therefore
    never deschedules (and thus never kills) tasks on healthy workers.
    Keying this on responsibility rather than on process uptime is what makes
    the election case safe; see [node-liveness.md](node-liveness.md).
  - Mass-disruption circuit breaker: if more than half the active fleet would
    be marked lost in a single tick, none of them are, and the manager logs at
    `error`. A control plane does not lose half its fleet in one tick.
- Eval path (used in tests/manual eval insertion):
  - `node_drop` / `node_join` eval kinds call equivalent store reconciliation logic.

## What Is Not Implemented Yet

From current code and task list:

- Manager-to-manager sync/consensus.
- Robust TLS join/rotation flow.
- Automatic cross-manager failover behavior.

