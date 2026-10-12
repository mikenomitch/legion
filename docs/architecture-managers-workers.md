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
  - Serves the worker plane (the `Worker` gRPC service) on `:4848`, always
    over mutual TLS. Managers dial it for exec, logs, restart, and push
    dispatch. See [Ports and who dials whom](#ports-and-who-dials-whom).
  - Runs task allocations via task-runtime plugins.
  - Sends periodic heartbeat and health-check updates.

## Ports and who dials whom

| Port | Served by | Dialed by | Protocol |
| --- | --- | --- | --- |
| `4747` (`server.port`) | each manager | workers, the CLI, the UI, other managers (raft) | gRPC; TLS only with `server.tls` |
| `4748`, `4749` | each manager | browsers (exec WebSocket), Prometheus (`/metrics`) | HTTP |
| `4848` (`LEGION_WORKER_PORT`) | each worker | managers; edge routers (`StartClaim`) | gRPC over mutual TLS, always |
| task ports | each worker | other nodes (service mesh, `connect` upstreams), clients | the task's own |

Both directions must be open for a multi-host cluster: workers dial managers
on 4747, and **managers dial workers on 4848**. Allow 4848 from every
manager (and from edge routers if you use [edge boot](edge-boot.md)). Nothing
else needs it.

The worker plane is reachable by default and safe to expose because it is
mutual TLS with per-RPC caller classes: only a manager certificate can call
`PostWork`, `GetTaskLogs`, `ExecTask`, or `RestartWorker`, and `StartClaim`
needs a signed boot ticket. See [auth-and-acls.md](auth-and-acls.md#the-worker-plane).

Where a worker listens and what it advertises:

| Setting | Default | Meaning |
| --- | --- | --- |
| `LEGION_WORKER_HOST` | `0.0.0.0` | IP the worker plane binds |
| `LEGION_WORKER_PORT` | `4848` | Port the worker plane binds and advertises |
| `LEGION_WORKER_ADVERTISE_ADDR` | detected | Host, or `host:port`, that managers and routers dial |
| `--advertise-addr` on `legion join` / `legion start worker` | — | Same as `LEGION_WORKER_ADVERTISE_ADDR`, and wins over it |

When neither is set, the worker advertises the local address of its route to
the manager: it connects a UDP socket toward the manager's address and reads
back the local address (no packet is sent). On a VPC that is the private IP
managers on the same network dial. A worker that reaches its manager over
loopback (`legion join` on the manager's box) advertises `127.0.0.1`. The
in-process worker of `legion start manager --run-worker` is the exception:
when the manager has a routable `server.advertiseAddr` (or, failing that, a
routable raft `advertiseAddr`), the worker advertises that host instead, so
the other managers of a multi-manager cluster can reach it.
Behind NAT, or with asymmetric routing, the guess is wrong; set the override.
The worker logs what it chose:

```text
INFO Worker plane listening on 0.0.0.0:4848 (mTLS); advertised as https://10.0.1.7:4848
```

The worker registers that as its `workerAddress` (`https://10.0.1.7:4848`).
Everything that needs the node's address takes the host from it: the
manager's dials, service-discovery endpoints (`legion service resolve`), the
mesh's cross-node `connect` hops, and the address edge routers are given.

If a manager cannot reach a worker, the worker keeps running work (heartbeat
replies carry its allocations), but exec, logs, and restart fail with a
message that names the node, the address, and the port:

```text
cannot reach node wkr-1 at https://10.0.1.7:4848 (Connection refused (os error 111)).
Managers dial workers on port 4848: allow it from every manager, or set
LEGION_WORKER_ADVERTISE_ADDR on the worker if this address is wrong
```

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
2. Build the worker's manager set: the token's `addr`, plus every manager
   address a previous run of this worker persisted (`managers.json` in its
   state directory). The token's manager is tried first; one that cannot be
   reached moves the worker to the next address, so a restarted worker comes
   up even when the manager its token names is down. See
   [Managers a worker can reach](#managers-a-worker-can-reach).
3. Call `WorkerPreRegister` to fetch worker-type config (`pool`, attributes,
   resource limits, additional checks). A restarted worker presents the node
   token it persisted; a new one presents its join secret.
4. Resolve worker ID:
   - Use `--name` if provided.
   - Else reuse existing single worker state dir.
   - Else generate random ID (optionally with configured prefix).
5. Restore persisted allocations from local worker state file if task PIDs are still alive.
   Reload the node certificate persisted under `<state-dir>/identity/`
   (mode `0600`), if it is still valid, so the worker plane can serve before
   a manager answers.
6. Resolve the worker plane's bind address (`LEGION_WORKER_HOST`,
   `LEGION_WORKER_PORT`) and advertised address (see
   [Ports and who dials whom](#ports-and-who-dials-whom)).
7. Fingerprint node + run node/task readiness checks.
8. Register node via `WorkerRegister`, with `workerAddress =
   https://<advertised host>:<port>`. When ACLs are enforced, the reply
   carries a per-node token that the worker attaches to all later
   worker-plane calls, and persists with mode `0600` in its state directory
   (`node_token`). A restarted worker re-registers with that node token
   rather than its join token, so it does not depend on the join token still
   being valid; the manager keeps the token it presented. A join-token
   registration mints and rotates the token. If the manager refuses the
   persisted token, or a heartbeat comes back Unauthenticated, the worker
   drops it and re-registers with its join secret. Re-registration keeps an
   operator's `drained`, so a cordoned node stays cordoned across a worker
   restart; it clears the `unavailable` that node loss sets, because a
   registering worker is alive. Only a new node starts `available`. The
   reply also carries every manager's address.
9. Issue the node certificate: generate a key locally, send a CSR
   (`IssueNodeCertificate`), persist the key and certificate, and start the
   worker plane listener with them (mutual TLS). The listener does not
   listen until the node holds a certificate.
10. Enter steady-state loops:
   - Heartbeats every `heartbeatSeconds` (worker-type config, default 2s,
     clamped to 1–300s). Each beat gets ±10% jitter so a fleet that started
     together does not stay in lockstep. With adaptive cadence (on by
     default), the manager serves an interval sized from fleet size on the
     heartbeat reply, and the worker beats at the larger of that and its
     own `heartbeatSeconds`. Up to 2,500 nodes the served interval is the
     same 2s ([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)).
   - Node checks every `WORKER_NODE_CHECK_DURATION_SEC` (default 30s).
   - The node certificate is re-issued on the heartbeat loop after a third
     of its 24-hour lifetime, and early when a worker-plane handshake shows
     a peer that trusts a different cluster CA (after
     `legion net ca rotate`).

### Managers a worker can reach

In a raft cluster any manager accepts any worker-plane call: a manager that
does not lead the owning raft lane forwards the write to the leader. So a
worker is not tied to the manager in its join token.

- Every `WorkerRegister` and `Heartbeat` reply carries `manager_addresses`:
  every raft member's advertised address (its `persistence.raft.advertiseAddr`,
  which is the same listener as its API), with the answering manager's API
  scheme. The worker keeps the set, always including the address from its
  token, and persists it to `managers.json` in its state directory whenever
  it changes.
- The worker talks to one manager at a time. After two consecutive
  heartbeats that fail to reach it (connection refused, a 5s connect
  timeout, a 30s request timeout, or a dead HTTP/2 keepalive), it moves to
  the next known manager, logging `Manager unreachable; failing over to the
  next known manager`. It wraps around the list, so it returns to a
  recovered manager the same way it left. A manager that answers with any
  error, including `Unauthenticated` or a not-the-leader rejection, is
  reachable and is not failed away from.
- Every client the worker holds (heartbeats, health checks, workload
  identity, artifact credentials, edge-boot ratification) follows the switch.
- A restarted worker starts from the persisted set: if the token's manager
  does not answer, it tries the others in turn, forever, with backoff.

Limits: the addresses are the raft advertise addresses, so they must be
reachable from workers. When they are not (managers that peer over a private
network the workers cannot see), the worker tries each, keeps the ones that
answer, and always keeps its token's address. Outside raft mode the list is
empty and the worker keeps its one manager.

### Running a worker under a service manager

Tasks outlive the worker process on purpose: a restarted worker re-adopts
the tasks it persisted (by pid, verified against the process start time) and
then reconciles them against the manager's desired set on its first
heartbeat, stopping any the manager no longer wants (for example because the
node was marked lost while the worker was down and the work was placed
elsewhere). Two settings decide whether that works under systemd:

- **A stable state directory.** The worker's identity and its record of
  running tasks live in `$LEGION_DATA_DIR/workers/<node>/` (default
  `./.legion`, relative to the working directory). Run the worker with a
  fixed `LEGION_DATA_DIR` (or `WorkingDirectory=`) and a fixed `--name`. A
  worker started from another directory finds no state, registers as a
  different node, and cannot see the tasks the previous process left
  running; the worker logs the state directory it uses at startup.
- **`KillMode=process`.** systemd's default (`control-group`) kills every
  task with the worker on `systemctl stop` or `restart`, so nothing is left
  to re-adopt and every restart restarts every task. `KillMode=process`
  stops only the worker; systemd then reports `Unit process N remains
  running after unit stopped` for each task, which is expected. Choose
  `control-group` (or `mixed`) instead when you prefer a worker restart to
  restart its tasks too.

```ini
[Service]
ExecStart=/usr/local/bin/legion join --name %H lgnjoin_...
Environment=LEGION_DATA_DIR=/var/lib/legion
KillMode=process
Restart=always
```

When a task's tracked process is gone at restart but its children are not
(a `sh -c` wrapper that exited while its server kept running), the worker
kills the rest of that task's process group instead of leaving it
unmanaged, and logs `killed_orphans = true` on the restore line.

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
- `IssueNodeCertificate`: worker sends a CSR for a key it generated and gets
  back its node certificate and the cluster CA. The certificate serves the
  worker plane and the service mesh.

### Worker APIs used by manager

All of these go over the worker plane: mutual TLS to the node's
`workerAddress`, with the manager's own certificate as the client identity
and the node's name verified on the server certificate.

- `PostWork`: manager dispatches task allocations to workers. Best-effort:
  a failed push is healed by the next heartbeat reply.
- `GetTaskLogs`: `legion job logs` and the UI read captured output.
- `ExecTask`: `legion exec`, actions, and the UI terminal, after the manager
  has applied ACLs.
- `RestartWorker`: manager can instruct a worker process restart.
- `StartClaim` is the one RPC managers never call: edge routers do, with a
  boot ticket ([edge-boot.md](edge-boot.md)).

### Allocation reconciliation model

- Manager is source of truth for desired allocations (`allocation` table).
- Worker reports current allocations in heartbeat.
- Manager returns desired allocations for that node.
- Worker reconciles by stopping undesired tasks and starting missing ones.
- An empty desired set is authoritative. It means "run nothing here," so the
  worker stops everything it has, including the last allocation on a node that
  was drained or had its work stopped or preempted. A failed heartbeat is never
  read this way: the worker reconciles only against a reply the manager
  actually sent.

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
- Availability reconcile: node loss and recovery are each two replicated
  writes (the status lane's `lost`/`active`, and the control lane's
  `node.availability`), and the scheduler places only on `available`. Every
  tick, the manager that owns liveness returns to `available` any node that
  is `active`, stored `unavailable`, and that it has actually heard from
  since taking responsibility; and withdraws any `lost` node still stored
  `available`. It never touches `drained` (an operator's cordon), and loss
  and recovery never overwrite it either, so a cordoned node stays cordoned
  through an outage. Repairs count in
  `legion_nodes_recovered_total{path="reconcile"}`. See
  [node-liveness.md](node-liveness.md).
- Capacity pass: a worker registration or an uncordon queues one `node_join`
  eval with no node. It re-evaluates jobs short of their count and jobs with
  `count.per` tasks (see
  [jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md#node-added)).
- Eval path (used in tests/manual eval insertion):
  - `node_drop` / `node_join` eval kinds with a node call equivalent store reconciliation logic.

## What Is Not Implemented Yet

- Worker-to-manager TLS: the worker's connection to the manager (4747) does
  not configure TLS, even when the manager has `server.tls` enabled.
  Heartbeats, registration, and node certificate issuance travel in
  plaintext on that channel (the node's private key does not: it never
  leaves the worker). The manager-to-worker direction (4848) is always
  mutual TLS.
- Join tokens do not pin the cluster CA yet, so a joining worker cannot tell
  a real manager from an impostor at the token's address.
- No reachability probe: a worker whose 4848 is firewalled looks healthy in
  `legion node info` until exec or logs against it fail with the message
  above.
- `legion net ca rotate` replaces the CA in one step, with no overlap
  window. Each node re-issues its certificate on the next heartbeat after a
  manager's dial to it is refused for the old CA, so the first exec, logs,
  or restart against each node after a rotation can fail.
- Workers behind NAT that managers cannot dial are not supported. The
  advertise override covers port-forwarded setups.
- TLS certificate rotation: the manager reads `certPath` and `keyPath` once
  at startup. To rotate the certificate, restart the manager.

