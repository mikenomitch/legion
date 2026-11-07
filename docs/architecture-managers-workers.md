# Managers and Workers

This page describes how Legion's managers and workers fit together: what each one runs, how a worker joins, and how they keep allocations in sync.

## Current Cluster Shape

A Legion cluster has one or more managers and one or more workers. By default it runs a single manager. The persistence modes below add more managers for high availability.

- Manager:
  - Owns the scheduler and eval loop.
  - Persists cluster state in local SQLite (`.legion/store.sqlite` by default, movable with `LEGION_DATA_DIR`. See [data-directory.md](data-directory.md)).
  - Exposes the main gRPC API on `:4747`. Join tokens are minted here through
    the authenticated `CreateJoinToken` RPC.
  - Runs a [system worker](#the-system-worker) in its own process by
    default: a small, capped worker that accepts only Legion's own jobs (the
    `legion-system` namespace), so a cluster with no workers joined still has
    somewhere to run them.
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
| `4747` (`server.port`) | each manager | workers, the CLI, the UI, other managers (raft) | gRPC; always TLS for workers, TLS for everyone with `server.tls` |
| `4748`, `4749` | each manager | browsers (exec WebSocket), Prometheus (`/metrics`) | HTTP |
| `4848` (`LEGION_WORKER_PORT`) | each worker | managers; edge routers (`StartClaim`) | gRPC over mutual TLS, always |
| `4849` (`systemWorker.port`) | each manager's [system worker](#the-system-worker) | managers | gRPC over mutual TLS, always |
| task ports | each worker | other nodes (service mesh, `connect` upstreams), clients | the task's own |

Both directions must be open for a multi-host cluster: workers dial managers
on 4747, and **managers dial workers on 4848**. Allow 4848 from every
manager (and from edge routers if you use [edge boot](edge-boot.md)). Nothing
else needs it. With several managers, allow 4849 between managers too: each
dials the others' system workers.

The worker plane is reachable by default and safe to expose because it is
mutual TLS with per-RPC caller classes: only a manager certificate can call
`PostWork`, `GetTaskLogs`, `ExecTask`, `RestartWorker`, `StopSessions`, or
`ForwardAppRequest`, and
`StartClaim` needs a signed boot ticket. See [auth-and-acls.md](auth-and-acls.md#the-worker-plane).

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

The worker simulator (`legion start worker --simulate N`) serves its whole
fleet from one listener with one fleet certificate
(`<prefix>.fleet.legion`), issued through the first simulated node to
register. Its nodes carry the `workerFleet` attribute, and managers verify
the fleet's name for them. The worker type a fleet joins with must grant
the fleet ([worker-simulation.md](worker-simulation.md)), so a real node
cannot claim one.

If a manager cannot reach a worker, the worker keeps running work (heartbeat
replies carry its allocations), but exec, logs, and restart fail with a
message that names the node, the address, and the port. The manager also
probes each worker back when it registers and records the result; see
[Reachability](#reachability).

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
- The in-process worker: the [system worker](#the-system-worker) by default,
  `--run-worker`'s worker instead when the flag is given, or none with
  `systemWorker.enabled = false`. When ACLs are enforced, the manager issues
  its own join token for it.
- `--no-acl` turns ACLs off for that run, overriding `acl.enabled`. It is for
  throwaway local clusters only. See
  [docs/auth-and-acls.md](auth-and-acls.md#turning-acls-off).

Operator TLS is off by default. Turn it on with `server.tls { enabled,
certPath, keyPath }` in the manager config. It covers the gRPC listener and
the exec WebSocket listener (`server.port + 1`). The telemetry listener stays
plaintext. Workers speak TLS to the gRPC listener either way: the manager
answers a TLS handshake for `manager.legion` with its cluster server
certificate, and, without `server.tls`, still answers plaintext for the CLI
and UI on the same port ([auth-and-acls.md](auth-and-acls.md#workers-reach-managers-over-tls)).
Managers speak TLS to each other on that port too, under the name
`raft.legion` and a key derived from the cluster secret, with or without
`server.tls` ([auth-and-acls.md](auth-and-acls.md#managers-reach-each-other-over-tls)).

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
2. Build the worker's manager set: the token's `addr`, plus every manager
   address a previous run of this worker persisted (`managers.json` in its
   state directory). The token's manager is tried first; one that cannot be
   reached moves the worker to the next address, so a restarted worker comes
   up even when the manager its token names is down. Every one of them is
   dialed over TLS and verified against the pinned cluster CA. See
   [Managers a worker can reach](#managers-a-worker-can-reach).
3. Call `WorkerPreRegister` to fetch worker-type config (`pool`, attributes,
   resource limits, additional checks, and runtime settings such as
   `resourceEnforcement` and the `container.allowCaps` capability allowlist,
   see [docs/task-runtime-container.md](task-runtime-container.md#which-capabilities-a-node-allows)).
   A restarted worker presents the node token it persisted; a new one
   presents its join secret.
4. Resolve worker ID:
   - Use `--name` if provided.
   - Else reuse existing single worker state dir.
   - Else generate random ID (optionally with configured prefix).
5. Restore persisted allocations from local worker state file if task PIDs are still alive.
6. Load (or, on first start, generate) the node's private key from
   `<worker state dir>/identity/node.key` (mode `0600`). The key never leaves
   the host. Reload the node certificate persisted next to it, if it is still
   valid, so the worker plane can serve before a manager answers.
7. Fingerprint node + run node/task readiness checks, and work out the
   advertised address (see [Ports and who dials whom](#ports-and-who-dials-whom)).
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
   reply also carries every manager's address. The registration also carries
   the host's `--attr k=v` values, which the manager merges with the join
   token's attributes and the worker type's (the token wins, then `--attr`;
   see [Node attributes from the join](auth-and-acls.md#node-attributes-from-the-join)),
   and counts against the token's `--max-uses` when this is a new node.
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
  identity, KV templates, artifact credentials, node identity tokens,
  edge-boot ratification) follows the switch. So does the JWKS fetch that
  verifies boot and exec tickets: it asks the active manager, not the
  token's.
- Trust is per cluster, not per address. The managers share the cluster CA,
  so every address a manager reports is dialed with the same verifier as the
  token's manager: the token's CA pin, or the cluster CA bundle once the
  worker holds one. When the bundle replaces the pin, it replaces it for
  every manager at once. An address only says where to dial. The manager
  there must still present a `manager.legion` certificate chained to that CA,
  and an address no manager of the worker's cluster reported is never
  trusted.
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
- **The join token in a file.** A token passed as an argument stays in the
  worker's command line for the life of the process, where any local user
  can read it with `ps` or `/proc/<pid>/cmdline`, and every restart needs it
  again. Write it to a root-owned `0600` file and pass `--token-file`. The
  worker warns when the token arrives on the command line, and when the file
  is readable by other users. There is no environment variable for the
  token on purpose: exec tasks inherit the worker's environment.

```ini
[Service]
ExecStart=/usr/local/bin/legion join --name %H --token-file /etc/legion/join-token
Environment=LEGION_DATA_DIR=/var/lib/legion
KillMode=process
Restart=always
```

```sh
sudo install -d -m 0700 /etc/legion
legion join-token create --ttl 720h -j | jq -r .data.token \
  | sudo install -m 0600 /dev/stdin /etc/legion/join-token
```

systemd's `LoadCredential=join-token:/etc/legion/join-token` with
`--token-file ${CREDENTIALS_DIRECTORY}/join-token` works too, and keeps the
file out of the unit's own filesystem view.

The in-process worker (the system worker, or `--run-worker`'s) shares the
manager's process, so it stops when the manager does and the manager waits
for it (session checkpoints first) before exiting. On Linux its tasks
outlive it and the next start re-adopts them, the same as a standalone
worker. On macOS it stops every task before the process exits, so its Apple
containers do not keep running after the cluster has gone. The next start
runs them again under the same allocations. See
[Stopping a single-process cluster](task-runtime-container.md#stopping-a-single-process-cluster-macos).

A restarted worker re-registers with the node token it persisted, not the
join token's secret, so revoking the join token after the first
registration does not strand the node. The worker still reads the join
token at every start for the manager address and the cluster CA pin it
carries, so keep the file in place.

Re-adoption keeps a task's processes, not its mesh connections. The node's
datapath (`connect` upstream listeners, ingress listeners, TLS termination,
handlers) runs inside the worker process, so a worker restart severs every
connection in flight through it, and new dials fail until the restarted
worker's first heartbeat brings back the endpoint feed. Apps should reconnect
on a dropped upstream connection. See
[Restarting a worker in place](upgrades.md#restarting-a-worker-in-place).

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

## The system worker

Every manager runs one in-process worker for Legion's own operational jobs
by default: the [apps](apps.md) with `placement = "managers"`, the bundled
autoscaler among them. A single manager with no workers joined can therefore run them,
they keep running when every worker is full, and they are never on a node a
cluster's own scale-in might drain. One per manager, so they fail over
across managers.

```nickel
# configs/manager.ncl: these are the defaults; write nothing to get them
systemWorker = {
  enabled = true,
  namespaces = ["legion-system"],
  resources = { cpu = 1000, memory = 1024 },   # MHz and MiB the manager lends
  port = 4849,
},
```

| Field | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Run it. `false` keeps the manager free of allocations; jobs in `legion-system` then need a worker whose type accepts that namespace |
| `namespaces` | `["legion-system"]` | The namespaces it accepts, as names or globs ([node restrictions](namespaces.md#nodes-that-accept-only-some-namespaces)). `*` skips reserved names, so "everything" is `["*", "legion-*"]` |
| `resources` | `{ cpu = 1000, memory = 1024 }` | The lend, in MHz and MiB: the node's whole capacity as the scheduler sees it, and its kernel ceiling where the node can enforce one |
| `port` | `4849` | Its worker plane port |

There is no setting for the user apps run as: every app's `exec` tasks run
as the app's own uid, from `1900000000` up, assigned when it is installed
([Apps: What Legion renders](apps.md#what-legion-renders)).

What the manager registers:

- **Worker type and pool `legion-system`.** The type is built in: every
  manager's store has it, whatever any one manager's config says, and a
  worker config file may not declare a type of that name. Nodes get ids
  like `legion-system-x8f2k1qz`.
- **`namespaces = ["legion-system"]`** (the configured list), recorded on
  the node, so no job from another namespace lands there whatever its
  permit says. The pool keeps away every job that does not permit it, too.
  A job in `legion-system` whose permit names no pool goes there through
  the `legion-system` namespace policy (`pools.default = ["legion-system",
  "default"]`, so it also places on a `--run-worker` manager's worker;
  [Reserved namespaces](namespaces.md#reserved-namespaces)).
- **Attribute `legion.role = "manager"`**, so a job can be constrained to
  managers' workers (`constraints = { "legion.role" = "manager" }`).
  The manager pins both the namespace list and the role on the join token
  it mints for its own worker, and registration takes them from the token,
  not from what the worker reports: a forged registration cannot drop the
  restriction or claim the role ([namespaces.md](namespaces.md#known-limitations)).
- **Capacity: the lend, not the host.** The node's `capacity` attribute caps
  its schedulable CPU and memory at `resources`, and offers no exclusive
  `cores` (the lend is a share of the host's CPUs, not whole ones to pin).
  `legion node usage` shows the lend as the node's capacity.
- **A kernel ceiling, where the node can carry one.** On Linux with cgroup
  v2 and resource enforcement, its allocations live under their own cgroup
  parent, `legion-system/`, whose `cpu.max` and `memory.max` are the lend.
  Each allocation's own `cpu.weight` lets it burst within the lend, never
  past it, so raft and the manager keep the rest of the host. CPU MHz become
  a quota over the host's clock (1000 MHz on a 2.5 GHz host is 40% of one
  CPU); on a host that reports no plausible clock, 1000 MHz counts as one
  CPU. The worker logs the ceiling it wrote:

  ```text
  INFO System worker capped at the manager's lend: its allocations together use
  at most this much of the host cgroup_parent=legion-system cpu_max=40000 100000
  memory_max=1073741824
  ```

  Where it cannot (macOS, no cgroup v2, enforcement off, or the kernel
  refuses), the lend is advertised capacity only, and it says so:

  ```text
  WARN System worker's lend is advertised capacity only: the scheduler places no
  more than it here, but nothing in the kernel caps what its allocations use
  ```
- **Its own port and state.** It listens on `systemWorker.port` (4849) and
  keeps its state in `<data dir>/system-worker/`, apart from a standalone
  worker's `<data dir>/workers/`, so `legion join` on a manager's own host
  (a laptop, a small cluster) gets a node of its own instead of the system
  worker's id.
- **On every manager, leader or not.** A follower forwards the join token it
  mints for its system worker to the control lane's leader; while the
  cluster has no leader it retries, and the system worker starts once one is
  elected. During a rolling upgrade, a follower on the new build whose
  leader predates the system worker retries the same way until the leader
  is upgraded, logging `In-process worker cannot join yet; retrying`.

**`--run-worker` keeps its meaning**, "this manager also runs everything":
its worker joins worker type and pool `default` with
`namespaces = ["*", "legion-*"]` (every namespace, reserved ones included),
no lend cap, `LEGION_WORKER_PORT` (4848), and `<data dir>/workers/`, as
before. It still carries `legion.role = "manager"`. It replaces the system
worker for that run, and runs even with `systemWorker.enabled = false`.

Known limitations:

- **Two workers on one host each guard only their own allocations.** On
  Linux the kernel host guard (`nftables` table) and the eBPF classifier's
  pin directory are named after the worker's cgroup parent
  (`legion_host_guard_legion-system` and `/sys/fs/bpf/legion_legion-system`
  for the system worker; `legion_host_guard` and `/sys/fs/bpf/legion` for a
  worker with the default parent), so a manager's system worker and a
  standalone worker on one host keep separate guards instead of overwriting
  each other's. Each guard covers the sockets of its own worker's
  allocations; give two workers on one host different cgroup parents (the
  system worker always uses `legion-system`).
- **The lend is fixed**, not a share of the host.
- **Bundled apps are not built yet.** The system worker runs whatever is
  submitted to `legion-system`; nothing is submitted there automatically.

## Manager <-> Worker Communication

### Manager APIs used by worker

- `WorkerPreRegister`: worker fetches typed config.
- `WorkerRegister`: worker sends initial config/fingerprint/health payload.
- `Heartbeat`: worker sends failed and terminated allocations, and either
  (a worker without the work stream) its active allocation IDs and every
  allocation's health, or (a worker on the work stream) its stream position,
  a digest of what it holds, and only the health that changed. The manager
  responds with the desired allocations, or, for a worker on the stream,
  with `desired_via_stream` and no desired set (see [Delivery](#delivery)).
- `StreamWork`: one long-lived stream per worker that carries its work as
  sequenced deltas (see [Delivery](#delivery)).
- `HealthCheck`: worker sends node health check JSON.
- `IssueWorkloadIdentityToken`: worker fetches short-lived OIDC tokens for
  identities declared on its allocations' tasks, at launch and on refresh.
  The manager derives all claims itself and only serves allocations placed
  on the calling node. See [docs/workload-identity.md](workload-identity.md).
- `IssueNodeCertificate`: worker sends a CSR for a key it generated and gets
  back its node certificate and the cluster CA. The certificate serves the
  worker plane and the service mesh.

### Worker APIs used by manager

- `PostWork`: manager dispatches task allocations to workers that do not
  stream their work (see [Delivery](#delivery)), and edge-boot provisional
  starts and ratifications to any worker. Best effort: a push that fails (an
  unreachable worker, a worker on an older protocol) costs one heartbeat
  interval, because the heartbeat reply carries the full desired set.
- `GetTaskLogs`: `legion job logs`.
- `ExecTask`: `legion exec`, actions, the UI terminal, and the MCP exec tools,
  proxied by the manager. Direct-exec clients call it too, with an exec
  ticket ([Direct exec](exec-and-actions.md#direct-exec)).
- `RestartWorker`: `legion node restart`. The worker refuses a request naming
  another node, so a stale address can't restart the wrong machine.
- `StopSessions`: a manager ends running [sessions](session-mode.md) on the
  node (`legion instance stop` or `destroy` on a session, or the later copy
  of a duplicated one). Session starts arrive as `PostWork`.
- `ForwardAppRequest`: an [app](apps.md#views-and-actions)'s view or action,
  after the manager authorized the viewer and signed a viewer assertion. The
  worker delivers one HTTP request to the app allocation's UI port, dialed
  the way the ingress datapath dials a task, and only for a running
  allocation of a `legion-system` job that declares the port, under
  `/legion/v1/`, with a bounded body and answer.
- `StartClaim`: not called by managers. Edge routers call it with a boot
  ticket ([edge-boot.md](edge-boot.md)).

### The worker plane

The `Worker` service is always served over mutual TLS, on every platform and
in every mode, including `--run-worker`. There is no plaintext worker
listener and no setting that turns one on.

- **Server identity.** The worker presents its node certificate,
  `<node>.node.legion` and `legion://node/<id>`, signed by the cluster CA
  from a CSR. The worker generated the key; the manager chose the names.
  Which CA signs is the managers' `clusterCa` provider
  ([The Cluster CA](cluster-ca.md)).
- **Client identity.** Client certificates are requested but not required,
  because edge routers and direct-exec clients authenticate with tickets.
  A certificate that is presented must chain to the cluster CA, or the
  handshake fails.
- **Caller classes.** Each RPC admits named classes of caller:

  | RPC | Admits | Check |
  | --- | --- | --- |
  | `PostWork`, `GetTaskLogs`, `RestartWorker`, `Probe`, `StopSessions`, `ForwardAppRequest` | manager | a client certificate from the cluster CA carrying `legion://manager/<id>` |
  | `ExecTask` | manager, ticket | a manager certificate as above, or (where the worker sets `exec = { direct = true }`) an exec ticket in the first frame, verified offline against the cluster JWKS |
  | `StartClaim` | ticket | the boot ticket in the request; a client certificate is ignored |

  A node's certificate is signed by the same CA, but it is not a manager
  certificate: the role in the URI SAN is what the worker checks. A request
  that presents a non-manager certificate is refused outright, never treated
  as a ticket holder. `tests/integration/worker_plane_classes.rs` parses the proto and
  fails the build when a `Worker` RPC has no class, the same way
  `tests/integration/acl_endpoints.rs` guards the `Manager` service.

  ```console
  $ grpcurl -cacert ca.pem 10.0.1.7:4848 legion.Worker/GetTaskLogs
  ERROR:
    Code: Unauthenticated
    Message: GetTaskLogs is a manager-only worker RPC: present a Legion manager certificate
  ```

- **Manager certificates.** Each manager mints its own client certificate
  (`legion://manager/<id>`, where the id is the raft `nodeId`, or `local`):
  it generates the key and has a CSR signed by the cluster CA provider (the
  built-in CA in the store by default, or a `file`/`exec` CA,
  [The Cluster CA](cluster-ca.md)), and re-mints at a third of the
  certificate's lifetime (8 hours for the built-in CA). No key is
  distributed.
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

### Delivery

A worker gets its work over `Manager.StreamWork`: it opens one stream to
its manager at startup and keeps it open. The manager sends a **sequenced
delta** for each committed change to an allocation on the node, so a
placement reaches the worker one network round trip after it commits,
whatever the heartbeat cadence:

- **Deltas.** Each message carries the node's next sequence number, the
  allocations added or changed (the full payload, exactly what the desired
  set would carry), and the allocations that left with why (their stop
  notices). The worker starts a new allocation as soon as its delta lands,
  through the same launch path a push takes (the stream was authenticated
  when the worker opened it, so no per-delta certificate check applies),
  and runs its reconcile at once for a
  stop or a changed payload. It acks each message it applied.
- **Full resync.** A worker the manager cannot continue gets its whole
  desired set once, in place of what it holds, then deltas. That happens
  when the position the worker reconnects with is from another manager
  process (a failover, a manager restart, a different manager), is ahead of
  the manager's, or is older than the messages the manager still holds. A
  manager also resyncs every worker streaming from it after it installs a
  raft snapshot, or when it cannot tell which allocations a committed change
  touched. Such a failure never fails the change itself, which has already
  committed.
- **Replay.** A worker that reconnects to the same manager process with a
  position it still holds gets the messages after it, in order. Nothing is
  lost and nothing is applied twice.
- **Digest check.** Every heartbeat reports the stream position and a digest
  of the allocation ids the worker holds. A manager whose own digest for the
  node disagrees, with nothing in flight, answers `work_resync` and sends a
  full resync on the stream, so a lost delta is caught within one beat.
- **Any manager serves it.** The delivery log is memory on the manager the
  worker streams from, fed by the changeset projection every manager runs on
  its own replica. A leader failover disturbs only the streams attached to
  the manager that died: their workers reconnect and resync once; workers
  streaming from a surviving manager keep their position.
- **Checkpoint work.** When a snapshot row of the node commits in a state
  that is work for it (a capture for a suspend, fork, or snapshot, a
  publish, a reclaim), or a warm is queued for it, the next message pushes
  that `CheckpointRequest`, and the worker starts it at once. A push is a
  hint, not the record: it goes out once per stream, is never replayed, and
  does not count toward the digest. The heartbeat reply still lists the
  node's outstanding checkpoint work on every beat, so a lost push costs one
  heartbeat interval, and a new stream is pushed everything still
  outstanding. See [How it works](suspend-and-resume.md#how-it-works).

A worker on the stream sends a heartbeat of a few dozen bytes and gets a
reply of a few bytes, however many allocations it runs; the manager builds
no desired set for it. Measured on a 300-allocation node: 42-byte request,
7-byte reply, and 0.5 ms of manager time per beat, against 5,154 bytes,
102,670 bytes, and 26 ms for the full set (debug build). The connect and
network-policy feeds still ride the reply, computed from the payloads the
stream sent, and only when a task on the node declares one.

The worker also sends a heartbeat early, without waiting for its interval,
when it has something the manager is waiting on: after a stream message that
stopped or changed an allocation (so the reconcile runs now), and when a
checkpoint report is ready (a finished capture, restore, publish, or warm),
so a suspend, resume, or fork settles one round trip after the node finishes
it. An early beat is an ordinary heartbeat. It carries everything else the
worker has to report, and wakes that arrive while one is in flight fold
into the next.

While its stream is down (the manager restarted, the network blipped), a
worker reports no position, and the manager answers with today's full
desired set, which the worker reconciles as before; a worker that does not
stream at all, or a simulated fleet started with `--sim-no-work-stream`,
always works that way. A worker advertises the `work-stream` capability at
registration (informational: the manager recognizes a streaming worker by
the position its heartbeat reports).

Known limitations: edge-boot provisional dispatches and their ratification
payloads still travel as `PostWork` pushes and the `RatifyClaim` reply, not
as stream messages. A worker that has an unratified provisional start for a
task does not start a streamed allocation of that task by itself; the
ratification or its next reconcile settles it.

### Allocation reconciliation model

- Manager is source of truth for desired allocations (`allocation` table).
- A worker's desired set is the stream's mirror (or, off the stream, the
  set in the heartbeat reply).
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
  `count.per` tasks that have no pending or blocked eval (see
  [jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md#node-added)).
  A job whose placement found no room has a blocked eval, which the
  registration, the uncordon, or capacity freed on a node wakes when the node
  could host it ([Blocked evals](scheduler-and-eval-loop.md#blocked-evals)).
- Eval path (used in tests/manual eval insertion):
  - `node_drop` / `node_join` eval kinds with a node call equivalent store reconciliation logic.

### Forgetting a node

A node that is gone keeps its record: it reads `lost` in `legion node list`
until something removes it. `legion node forget` (the `ForgetNode` RPC,
`node:write` on the node's pool) is that something. It removes the record and
revokes the node's token in one write, so a worker still running on that
machine can no longer heartbeat or re-register under its old identity:

```bash
legion node forget ip-10-0-1-7            # a dead machine
legion node drain web-3 && legion node forget web-3   # once web-3 is empty
legion node forget old-host --force       # the machine is gone; drop it whatever its state
```

Without `--force` the manager refuses, with `FailedPrecondition` and what to
do instead, a node that:

- still has allocations, including stateful ones its `stateful.onLost`
  policy is holding: drain it and wait until it is empty;
- is live and schedulable (status `active`, not cordoned): cordon or drain it,
  or wait until it is marked lost. A cordoned, empty node can be forgotten
  while it still heartbeats; that is the scale-in order (drain, stop the
  machine, forget it);
- holds node-local volumes, whose data lives on that machine.

`--force` asserts the machine and its data are gone for good. The record goes
whatever its state; its allocations are removed and replaced elsewhere at
once (stateful instances too, without waiting for `onLost.replaceAfter`,
which is only safe because the machine is gone); and node-local volumes stay
pinned to the missing node, so their instances do not place until you
replace them (`legion instance replace`). Snapshots stored on the node are
marked lost either way, and its liveness rows are swept by the next orphan
pass. The reply counts what went: allocations, node tokens, snapshots, and
orphaned volumes.

Forgetting is retry-safe: with `--idempotency-key`, a repeat of a forget that
succeeded replays its result (`deduplicated = true`) instead of answering
`NotFound`. The MCP tool is `legion_node_forget` (behind `--allow-write`).

A forgotten machine that comes back needs a join token, and joins as a new
node. A node group's `--max-uses` token is already spent by then, so it cannot
rejoin with that one ([Limiting a token's uses](auth-and-acls.md#limiting-a-tokens-uses)).
Nothing forgets nodes automatically yet: a lost node stays listed until an
operator forgets it.

## What Is Not Implemented Yet

- Workers behind NAT that managers can't dial: exec, logs, restart, and the
  mesh need a reachable `workerAddress`. Work still reaches such a worker
  through heartbeat replies.
- TLS certificate rotation: the manager reads `certPath` and `keyPath` once
  at startup. To rotate the certificate, restart the manager.

