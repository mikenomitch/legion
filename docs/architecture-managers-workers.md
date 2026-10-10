# Managers and Workers

This page describes how Legion's managers and workers fit together: what each one runs, how a worker joins, and how they keep allocations in sync.

## Current Cluster Shape

A Legion cluster has one or more managers and one or more workers. By default it runs a single manager. The persistence modes below add more managers for high availability.

- Manager:
  - Owns the scheduler and eval loop.
  - Persists cluster state in local SQLite (`.legion/store.sqlite` by default, movable with `LEGION_DATA_DIR`. See [data-directory.md](data-directory.md)).
  - Exposes the main gRPC API on `:4747`. Join tokens are minted here through
    the authenticated `CreateJoinToken` RPC.
- Worker:
  - Registers itself with the manager (with a join token when ACLs are
    enforced) and receives a per-node credential for subsequent calls.
  - Runs task allocations via task-runtime plugins.
  - Sends periodic heartbeat and health-check updates.

The manager's `persistence.type` decides how its state survives a manager failure:

- `local_disk` (default): one manager with local SQLite.
- `raft`: several managers replicate every store write through Raft. A quorum
  commits each write before it is visible, one manager leads and runs the
  scheduler, and a new leader takes over automatically after a failure. See
  [raft-operations.md](raft-operations.md).
- `streaming`: one primary ships its SQLite WAL to object storage. An
  optional object-storage lease handles automatic failover. See
  [persistence-streaming.md](persistence-streaming.md).

## Manager Startup

`run_manager` starts these loops/services:

- Main manager gRPC server (`Manager` service) on `server.port`. When
  `workloadIdentity.enabled` is set, this listener also serves two plain
  HTTP/1.1 GET routes so the manager can act as an OIDC identity provider:
  `/.well-known/openid-configuration` and the JWKS path. External relying
  parties (AWS, GCP, Vault) fetch these with ordinary HTTP clients.
- Node status checker loop (`start_node_status_checker`).
- Eval worker pool (`start_eval_loop`) with `eval_threads` workers.
- Workload-identity signing-key rotation loop (only when
  `workloadIdentity.enabled`, and only on the primary manager in HA setups). See
  [docs/workload-identity.md](workload-identity.md).
- With ACLs enabled: JWKS refresh and expired-credential pruning loops.
- Optional in-process worker (`--run-worker`). When ACLs are enforced, the
  manager issues its own join token for it.

TLS is off by default. Turn it on with `server.tls { enabled, certPath, keyPath }`
in the manager config.

## Worker Startup and Join Flow

Create a join token with `legion join-token create` (the `CreateJoinToken`
RPC). It requires `node:write` when ACLs are enabled. The printed token bundles
the manager address, a secret, and the worker type. The manager stores only a
hash of the secret. See docs/auth-and-acls.md.

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
8. Register node via `WorkerRegister`. When ACLs are enforced, the reply
   carries a per-node token that the worker attaches to all later
   worker-plane calls. Re-registering rotates the token. If a heartbeat comes
   back Unauthenticated, the worker re-registers with its join secret
   automatically.
9. Enter steady-state loops:
   - Heartbeats every `heartbeatSeconds` (worker-type config, default 2s,
     clamped to 1–300s). Each beat gets ±10% jitter so a fleet that started
     together does not stay in lockstep. With adaptive cadence (on by
     default), the manager serves an interval sized from fleet size on the
     heartbeat reply, and the worker beats at the larger of that and its
     own `heartbeatSeconds`. Up to 2,500 nodes the served interval is the
     same 2s ([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)).
   - Node checks every `WORKER_NODE_CHECK_DURATION_SEC` (default 30s).

### Heartbeat cadence and scale

The heartbeat interval is the main way to control manager load on large
fleets. For example, 10k workers at a fixed 2s cadence would send 5,000
heartbeat RPCs per second. Adaptive cadence, on by default, sizes the
interval from fleet size so the steady load stays at or below 1,250 a second
(8s at 10k nodes), and `heartbeat.adaptive = false` turns it off.

- `heartbeatSeconds` in a worker-type config sets that worker type's
  cadence. Workers receive it at pre-registration. The lost-node window
  scales with it: a node is lost after `missedBeatsBeforeLost` (5) missed
  beats. A 2s pool gets a 10s window, and a 60s batch pool gets a 300s
  window, without affecting each other.
- A steady-state heartbeat costs no raft commit and no fsync. It is one
  write into the in-memory liveness table on the manager that owns node
  status. Only status changes are replicated. A manager that does not own
  liveness reports what it heard in one digest RPC per second, regardless of
  fleet size. See [node-liveness.md](node-liveness.md).
- Group commit still applies to the paths that do commit: the display
  checkpoint, and the mixed-version fallback during a manager upgrade.
  Concurrent writes coalesce into one status-lane commit (one raft round for
  up to 128 nodes).
- The mark-lost scan is cheap. It compares monotonic ages held in memory
  against each node's own window, with no timestamp parsing and no database
  read.
- To check whether the fleet is keeping up, watch
  `legion_heartbeat_freshness_ratio` or run `legion node cadence`.

Measured throughput per backend and fleet size lives in
`benchmarks/scheduler/README.md`
([testing-and-benchmarks.md](testing-and-benchmarks.md) has the runner).
For sizing a control plane to a fleet (the `R x I` formula, the table, and
how to measure), see
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
  identities declared on its allocations' tasks, at launch and on refresh.
  The manager derives all claims itself and only serves allocations placed
  on the calling node. See [docs/workload-identity.md](workload-identity.md).

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
    node status. A manager that has just become responsible for it (at
    process start, or by winning status-lane leadership later) starts with
    no liveness data. It seeds every active node at `now` and marks nothing
    lost for `liveness.failoverGrace` (60s), so reconnecting workers have
    time to check in. A manager restart, an election, or a rolling upgrade
    never deschedules or kills tasks on healthy workers. The grace period
    starts when the manager takes over responsibility, not when the process
    starts, which keeps the election case safe. See
    [node-liveness.md](node-liveness.md).
  - Mass-disruption circuit breaker: if more than half the active fleet would
    be marked lost in a single tick, none of them are, and the manager logs at
    `error`. Losing half the fleet at once almost always points at a
    control-plane problem rather than at the workers.
- Eval path (used in tests/manual eval insertion):
  - `node_drop` / `node_join` eval kinds call equivalent store reconciliation logic.

## What Is Not Implemented Yet

- Worker-to-manager TLS: the worker's connection to the manager does not
  configure TLS, even when the manager has `server.tls` enabled.
- TLS certificate rotation: the manager reads `certPath` and `keyPath` once
  at startup. To rotate the certificate, restart the manager.

