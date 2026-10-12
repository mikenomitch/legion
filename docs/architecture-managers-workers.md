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

Operator TLS is off by default. Turn it on with `server.tls { enabled,
certPath, keyPath }` in the manager config. It covers the gRPC listener and
the exec WebSocket listener (`server.port + 1`). The telemetry listener stays
plaintext. Workers speak TLS to the gRPC listener either way: the manager
answers a TLS handshake for `manager.legion` with its cluster server
certificate, and, without `server.tls`, still answers plaintext for the CLI
and UI on the same port ([auth-and-acls.md](auth-and-acls.md#workers-reach-managers-over-tls)).

## Worker Startup and Join Flow

Create a join token with `legion join-token create` (the `CreateJoinToken`
RPC). It requires `node:write` when ACLs are enabled. The printed token bundles
the manager address, a secret, the worker type, and the pin of the cluster CA
(`caHash`). The manager stores only a hash of the secret. See
docs/auth-and-acls.md.

Worker startup (`run_worker`) flow:

1. Decode the join token locally (address, worker type, secret, CA pin). A
   token without a CA pin (format v1) is refused with a hint to mint a new
   one.
2. Connect to manager from token `addr`.
3. Call `WorkerPreRegister` (carrying the join secret when present) to fetch
   worker-type config (`pool`, attributes, resource limits, additional checks).
4. Resolve worker ID:
   - Use `--name` if provided.
   - Else reuse existing single worker state dir.
   - Else generate random ID (optionally with configured prefix).
5. Restore persisted allocations from local worker state file if task PIDs are still alive.
6. Load (or, on first start, generate) the node's private key from
   `<worker state dir>/identity/node.key` (mode `0600`). The key never leaves
   the host.
7. Fingerprint node + run node/task readiness checks, and work out the
   advertised address (see [Ports and addresses](#ports-and-addresses)).
8. Register node via `WorkerRegister`. When ACLs are enforced, the reply
   carries a per-node token that the worker attaches to all later
   worker-plane calls. Re-registering rotates the token. If a heartbeat comes
   back Unauthenticated, the worker re-registers with its join secret
   automatically. Re-registration keeps the node's availability, so a
   drained node stays drained across a worker restart. Only a new node
   starts `available`.
9. Send a CSR to `IssueNodeCertificate` and check that the CA bundle in the
   reply contains the join token's pin. A mismatch stops the join:

   ```text
   error: the manager at https://10.0.0.5:4747 presented cluster CA sha256:51aa07...,
   but this join token pins sha256:9c1e4f...
   Hint: the address may not be your manager, or the CA was rotated after this token
   was minted. Mint a new token with `legion join-token create`.
   ```

   The certificate and bundle persist next to the key, so a worker restarted
   while every manager is down still serves its plane (edge boot needs that).
10. Start the worker plane (`Worker` service): mutual TLS with the node
    certificate on `0.0.0.0:4848` by default (see [The worker
    plane](#the-worker-plane)).
11. Enter steady-state loops:
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

- `PostWork`: manager dispatches task allocations to workers. Best effort: a
  push that fails (an unreachable worker, a worker on an older protocol) costs
  one heartbeat interval, because the heartbeat reply carries the full desired
  set.
- `GetTaskLogs`: `legion job logs`.
- `ExecTask`: `legion exec`, actions, the UI terminal, and the MCP exec tools,
  proxied by the manager.
- `RestartWorker`: `legion node restart`. The worker refuses a request naming
  another node, so a stale address can't restart the wrong machine.
- `StartClaim`: not called by managers. Edge routers call it with a boot
  ticket ([edge-boot.md](edge-boot.md)).

### The worker plane

The `Worker` service is always served over mutual TLS, on every platform and
in every mode, including `--run-worker`. There is no plaintext worker
listener and no setting that turns one on.

- **Server identity.** The worker presents its node certificate,
  `<node>.node.legion` and `legion://node/<id>`, signed by the cluster CA
  from a CSR. The worker generated the key; the manager chose the names.
- **Client identity.** Client certificates are requested but not required,
  because edge routers and direct-exec clients authenticate with tickets.
  A certificate that is presented must chain to the cluster CA, or the
  handshake fails.
- **Caller classes.** Each RPC admits named classes of caller:

  | RPC | Admits | Check |
  | --- | --- | --- |
  | `PostWork`, `GetTaskLogs`, `RestartWorker` | manager | a client certificate from the cluster CA carrying `legion://manager/<id>` |
  | `ExecTask` | manager | as above |
  | `StartClaim` | ticket | the boot ticket in the request; a client certificate is ignored |

  A node's certificate is signed by the same CA, but it is not a manager
  certificate: the role in the URI SAN is what the worker checks. A request
  that presents a non-manager certificate is refused outright, never treated
  as a ticket holder. `tests/worker_plane_classes.rs` parses the proto and
  fails the build when a `Worker` RPC has no class, the same way
  `tests/acl_endpoints.rs` guards the `Manager` service.

  ```console
  $ grpcurl -cacert ca.pem 10.0.1.7:4848 legion.Worker/GetTaskLogs
  ERROR:
    Code: Unauthenticated
    Message: GetTaskLogs is a manager-only worker RPC: present a Legion manager certificate
  ```

- **Manager certificates.** Each manager mints its own client certificate
  (`legion://manager/<id>`, where the id is the raft `nodeId`, or `local`)
  from the CA key it already holds through the store, and re-mints it every 8
  hours. Nothing is distributed.
- **The manager verifies the worker it dials.** Every manager-to-worker call
  (dispatch, exec, actions, logs, restart) dials the node's advertised
  address with the cluster CA as the only root and checks the server
  certificate against `<node>.node.legion`, so a worker that registered a
  stolen or stale address can't receive another node's work. Channels are
  cached per node.
- **No plaintext, no older protocol.** A manager dials only workers that
  report protocol version 3 and an `https://` address. A protocol-2 worker
  keeps heartbeating and runs the work its heartbeat replies carry, but exec,
  logs, and restart against it fail with an error naming its version.
- **Certificate lifetime.** Node certificates are valid for 24 hours and are
  re-issued after 8, on the heartbeat loop, with no listener restart.

### Ports and addresses

| Port | Who listens | Who dials it |
| --- | --- | --- |
| `4747` (`server.port`) | manager: gRPC API, raft transport, OIDC discovery. TLS for workers always; plaintext too unless `server.tls` is on | CLI, UI, MCP, workers (TLS), peer managers |
| `4748` (`server.port + 1`) | manager: exec WebSocket | the UI terminal |
| `4749` (`server.port + 2`) | manager: `/metrics`, health probes | Prometheus, load balancers |
| `4848` (`LEGION_WORKER_PORT`) | worker plane (mTLS) | managers; edge routers (`StartClaim`) |

Operators must allow 4848 from managers to workers (and from routers, with
edge boot), and 4747 from workers to managers. The mesh also dials node to
node on task ports.

The worker binds `LEGION_WORKER_HOST` (default `0.0.0.0`) and advertises one
address, recorded as the node's `workerAddress` (`https://<host>:<port>`):

| Setting | Default | Meaning |
| --- | --- | --- |
| `LEGION_WORKER_HOST` | `0.0.0.0` | Address the worker plane binds |
| `LEGION_WORKER_PORT` | `4848` | Port it binds and advertises |
| `LEGION_WORKER_ADVERTISE_ADDR`, `--advertise-addr` on `legion join` and `legion start worker` | auto | Host, or `host:port`, that managers and routers dial |

Auto-detection picks the local address of the worker's route to its manager
(a connected UDP socket's local address; no packet is sent). On a VPC that is
the private IP managers on the same network dial. A worker co-located with
its manager gets `127.0.0.1`. Behind NAT, or with asymmetric routing, the
guess is wrong; set the override. Several workers on one host share a route,
so give each its own port and, if they must be told apart by address,
`--advertise-addr`. The worker logs what it chose:

```text
INFO Worker plane listening on 0.0.0.0:4848 (mTLS); advertised as https://10.0.1.7:4848
```

Service discovery endpoints, the mesh, and edge boot all take the host from
`workerAddress`, so a correct address is what makes them work across hosts.

The worker simulator (`legion start worker --simulate N`) serves its whole
fleet from one listener with one fleet certificate
(`<prefix>.fleet.legion`), issued through the first simulated node to
register. Its nodes carry the `workerFleet` attribute, and managers verify
the fleet's name for them.

### Reachability

Dispatch survives a worker the managers can't dial (heartbeat replies carry
the work), so a firewalled worker looks healthy until someone needs its logs.
To surface that at join time, the manager that accepts a registration probes
the worker plane back (`Worker/Probe`, manager class) once the worker's
listener is up, retrying for about half a minute, and records the outcome on
the node (`workerPlane` in its dynamic attributes):

```console
$ legion node info wkr-1
Node ID: wkr-1
Worker plane   https://10.0.1.7:4848   unreachable from mgr-b: connection timed out
               Managers dial workers on this address. Allow it from the managers, or set
               LEGION_WORKER_ADVERTISE_ADDR if this address is wrong.
Host guard     active (nftables)
```

The UI node page shows the same line. A reply that names another node
counts as unreachable: the advertised address is stale. The probe is
informational and does not feed node readiness, because the node still runs
work. It runs once per registration, from one manager; a worker that becomes
unreachable later is reported when a call to it fails.

Exec, logs, and restart fail the same way when a manager can't reach a
worker:

```console
$ legion exec 412 -it -- sh
error: exec failed: cannot reach node wkr-1 at https://10.0.1.7:4848 (connection timed out)
hint: managers dial workers on port 4848; see `legion node info wkr-1`
```

In `--json` output and MCP tool errors this is the standard envelope with
`code: "WORKER_UNREACHABLE"` and `retryable: true`.

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
- Capacity pass: a worker registration or an uncordon queues one `node_join`
  eval with no node. It re-evaluates jobs short of their count and jobs with
  `count.per` tasks (see
  [jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md#node-added)).
- Eval path (used in tests/manual eval insertion):
  - `node_drop` / `node_join` eval kinds with a node call equivalent store reconciliation logic.

## What Is Not Implemented Yet

- Manager-to-manager (raft) TLS: raft peers dial each other in plaintext,
  so keep the managers on a network you trust. With `server.tls` on, the
  managers' API port drops plaintext connections, which a multi-manager raft
  cluster still needs.
- Workers behind NAT that managers can't dial: exec, logs, restart, and the
  mesh need a reachable `workerAddress`. Work still reaches such a worker
  through heartbeat replies.
- TLS certificate rotation: the manager reads `certPath` and `keyPath` once
  at startup. To rotate the certificate, restart the manager.

