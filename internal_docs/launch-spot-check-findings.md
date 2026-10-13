# Pre-launch spot check: findings

**Date:** 2026-10-11. **Branch:** `spot-check/launch` (off `main` @ `af0b57a`).
**Rig:** [`tests/spot-check/`](../tests/spot-check/README.md), with 3 raft
managers (t3.small, ACLs on) and 2 spot m7i.large workers on AWS, plus a
local macOS cluster. **How to rerun:** `tests/spot-check/smoke.sh`, and the
human pass in [RUNBOOK.md](../tests/spot-check/RUNBOOK.md).

The goal was to spot check every headline feature on real machines before
sharing Legion. The short version: **single-node demos mostly work, and the
multi-node story does not yet.** Several failures only show up once a
cluster has two workers, a restart, or a leader change, which is exactly
what a first external user will do.

## Re-run: 2026-10-12

**Branch:** `spot-check/fixes-2026-10-12` (off `main` @ `063cb96`). Same rig,
plus the section 12 sandbox and session scenarios, the macOS pass, the UI,
and MCP. Smoke went from 68 to 89 passing checks of 100 on the first fixed
build. The run found a new layer of bugs under the ones fixed since
2026-10-11. All of these are fixed on the branch and were re-verified on the
rig unless marked otherwise. The old `shims.patch` is gone: its one hunk had
landed, and this run needed no shims at the end.

**P0, fixed**

- **Linux containers never started with the defaults.** youki, the default
  runtime, refuses the idmapped mounts the default `userNamespace = true`
  needs. Under runc the shifted container root could not traverse the 0700
  worker state and allocation directories. runc is now the default, youki
  is dropped (runc or crun only), and the layout is 0711 / 0710 with the
  range's gid.
- **One SQL typo killed raft.** `note_work_changes` queried
  `allocation.job_id`. It ran inside the raft apply, so the error stopped
  openraft's core on the leader and left the cluster leaderless for 20
  minutes. The query is fixed, and post-commit projection errors can no
  longer fail an apply.
- **Exec tasks never started with resource enforcement on.** Controllers
  were never delegated to the `share` and `reserve` slices. Every failure
  also leaked a leaf cgroup (467 on one node).
- **DAG steps never advanced.** `complete_allocation` wrote `task_status`
  from a scheduler-lane op, so the lane ownership guard rejected every batch
  completion on a raft cluster. Unrecorded terminal reports are now retried.
- **A joining manager with workload identity crashed at boot.** It tried to
  persist a signing key into a store that had not replicated yet.
- **Edge boot was unreachable.** `edgeBoot.enabled` never reached the
  server, and tickets then failed `InvalidIssuer` because the default issuer
  was `http://127.0.0.1:4747` and differed per manager. Raft clusters now
  share one issuer (`https://manager.legion`) that workers learn from the
  managers.
- **Every memory resume cold-booted.** The node reported cloud-hypervisor
  `51.2` and the checkpoint `51.2.0`, compared as strings.
- **microVM volumes lost recent writes on stop.** The VMM was killed without
  a guest sync. The guest now shuts down from inside and every kill or pause
  flushes first. *Unit-tested; not yet re-run on the rig.*
- **Image pulls failed after a leader failover.** Worker trust was keyed by
  the join manager's address, so TLS calls to the new active manager had no
  CA pin. Trust is now per cluster. *Unit-tested; not yet re-run on the rig.*
- **A `durability = "store"` checkpoint never resumed on another node.**
  sc-wkr-2 published the parts with its `contentStore.auth.kv` credential,
  but sc-wkr-1 fetched them through the artifact fetchers unsigned, and S3
  answered `AccessDenied`. A part under the node's own `contentStore.url` is
  now read with that store's credential. *Unit-tested; not yet re-run on the
  rig.*

**P1, fixed**

- Failing tasks were re-placed every 30 seconds forever (about 580
  allocations in 25 minutes). Moves now back off exponentially up to 30
  minutes, and pressure evictions spend the same budget.
- Streamed work starts called the worker's own `PostWork` handler, which
  refused it, so starts waited for the next heartbeat and logged an ERROR
  each time.
- `node.pressure` events raised on a follower were dropped. Events now
  forward to the leader.
- Preemption took an older normal-priority victim over young low-priority
  ones, because of the 60-second victim age guard.
- The join token sat in the worker's argv, readable by every task. There is
  now `--token-file`.
- `legion exec` into an exec task ran in the worker's cgroup, outside the
  task's limits.
- macOS: Apple container names were not node-scoped, so two local clusters
  replaced each other's containers. A hung `container` CLI call wedged the
  whole single-process cluster. CLI calls now have deadlines.
- The UI Managers page showed only managers that had led (old P2-12).
- `legion session nodes` reported live nodes as `down`.

**P2, fixed:** destroyed volumes listed by default, the CLI hanging on a
leaderless cluster, misleading gating warning on macOS, and leader-only
ERROR noise on followers. Later: one `~/.legion/token` for every cluster
(now one per manager address), the CLI panicking on a closed pipe,
`state query` user errors coming back `Internal`, server errors naming proto
fields instead of CLI flags, and node attributes as JSON strings in MCP.

**Still open:** listed in [TODO.md](TODO.md) under the 2026-10-12 groups.
The notable ones:

- A worker restart severs in-flight mesh connections, since the datapath
  lives in the worker. Apps without reconnect logic break until rescheduled.
  Documented in `docs/upgrades.md` now.
- Stopping the macOS manager leaves its Apple containers running.
- `job plan` errors where `job run` would preempt.
- Instance JSON shows `nodeId: null` for an active instance.
- Memory restore took 5.9 s on nested virtualization, against the 300 ms
  bare-metal target. Treat it as a data point.

## How to read this

- **Severity.** P0 breaks a headline feature on a normal multi-node cluster.
  P1 breaks a common path or loses work. P2 is a papercut, a misleading
  message, or a docs gap.
- **Status.** *Open* means untouched. *Shimmed* means a not-for-merge patch let
  testing continue on 2026-10-11 (the patch file is gone now that every shim
  has a real fix). Every root cause below was read in the source, unless the
  entry says otherwise.

## What worked

- **Cluster formation.** Three raft managers bootstrapped with a join token,
  ACLs bootstrapped with a pre-seeded secret, and two workers joined. Leader
  election worked when the leader was stopped.
- **Private images.** ECR pulls through a KV-held credential and an
  `artifacts.auth` matcher, with `containerImageDesugar` on.
- **Exec, logs, and actions across hosts**, once the worker-address shim was
  in. Logs and exec also kept working through a follower after a failover.
- **microVM sandboxes on spot instances.** AWS nested virtualization on m7i
  gives `/dev/kvm`, and cloud-hypervisor booted the image. The guest console
  reached `legion job logs`, and HTTP reached the guest through a hand-built
  TAP device.
- **KV, both ways.** A file template with `onChange = "update"` changed under a
  running task with no restart (also on macOS through Apple `container`,
  which settles an open question in the container-runtime plan). An env
  template restarted the task.
- **The mesh across workers.** Two nginx frontends on different nodes reached
  two API replicas and Postgres over `connect` upstreams, a note written
  through worker 1 read back through worker 2, and HTTP-mode balancing hit
  both API replicas. This needed shims and image changes (P0-3, P0-7).
- **Gated agent sandboxes.** Exec into a gated container worked, egress to an
  allowlisted host passed (200), a denied host was refused at the proxy
  (403), and `/workspace` survived `instance stop` and `start`.
- **Stateful Redis on EBS.** AOF on a `host` volume on the EBS data disk
  survived `--force-reschedule`.
- **macOS local development.** Exec, Apple `container` with a locally built
  image, exec into it, KV live updates, and a dev-mode sandbox with exec
  through the launcher's browser terminal (with the P1-6 fix).

## P0

### P0-1. Workers on other hosts lose exec, logs, discovery, and the mesh

*Shimmed; planned.* The worker binds and advertises `127.0.0.1:4848`
(`src/worker/daemon.rs:137-138`, `:315-318`), and its API is
unauthenticated. Fix designed in
[worker-plane-auth-plan.md](plans/worker-plane-auth-plan.md). The shim adds
`LEGION_WORKER_HOST` and `LEGION_WORKER_ADVERTISE_ADDR`, and the rig limits
4848 to the managers' security group.

### P0-2. Batch tasks never finish, and `restart` policies are ignored

*Open.* The worker rebuilds each task's `TaskConfig` from the dispatch
payload with no `lifecycle` or `restart` (`src/worker/task_manager.rs`,
`task_from_allocation`), because `TaskAllocation` carries neither field.
Every task therefore runs as a `service` with `restart = always`.

- A batch task that exits 0 restarts every 30 seconds forever. The DAG's
  `fetch` ran 11 times in one run. Worker log: `Task process exited;
  restarting ... status: exited with status 0`.
- A jobspec's `restart = { policy = ... }` has no effect on the worker.
- Batch status never reaches success, so anything waiting on it waits forever.

**Fix:** carry lifecycle and restart in `TaskAllocation`, or serialize the
whole task config, and add an end-to-end test where a batch task exits 0.

### P0-3. Health checks never start for tasks with artifacts, including every container image when `containerImageDesugar` is on

*Shimmed.* Artifact staging runs on a plain OS thread
(`spawn_staging_thread`, `src/worker/task_manager.rs`), which then starts the
health monitor. That calls `tokio::spawn` outside a runtime and panics:
`there is no reactor running` at `src/worker/task_health.rs:73`. The task
runs, but it sits at "health checks starting" forever. It is never healthy,
so service discovery returns no endpoints, the mesh routes nothing, and
health-gated rollouts never advance. The shim enters the caller's runtime
handle inside the thread, and is probably the real fix.

### P0-4. DAG dependencies are not enforced at runtime

*Open.* The scheduler counts a batch dependency as satisfied when it is
merely planned in the same pass (`dependency_satisfied`,
`src/manager/scheduler.rs`, the `planned_dep && Batch` arms), and the worker
has no dependency gating at all (`src/worker` never reads `depends_on`). A
whole chain of batch steps starts at once.

- On macOS, `validate` crashed with `FileNotFoundError` before `fetch` had
  written its input, and `serve` started although `report` had failed.
- On AWS, the DAG "finished" only because P0-2 kept restarting crashed steps
  until their inputs happened to exist.

The shipped DAG examples only `sleep`, so nothing ever noticed. **Fix:**
satisfy `success`/`complete` only from real task status (and re-evaluate the
job when a batch task completes), or gate starts on the worker.

### P0-5. Losing one manager can take every worker offline

*Open.* Two bugs compound:

- **Workers know one manager.** A worker only dials the address in its join
  token, so every worker joined through `sc-mgr-1` depends on it.
- **Followers cannot forward heartbeats in the `single` topology.** When
  `sc-mgr-1` came back as a follower, every forwarded heartbeat failed:
  `ForwardLaneWrite lane mismatch: envelope says control, op
  update_node_heartbeat belongs to status`
  (`src/storage/raft/server.rs:240`). The sender tags the physical lane, and
  the receiver checks the logical one. `single` is the topology the docs
  recommend.

Stopping `sc-mgr-1` (a follower at the time) marked both workers `lost`.
After restarting it, the workers stayed `lost` for as long as it was not the
leader. Workloads kept running throughout. Recovery was
`legion raft command trigger-election` on `sc-mgr-1` (see P2-5 for why not
`transfer-leader`).

### P0-6. After nodes come back, the scheduler still sees them as unavailable

*Open, root cause inferred.* After P0-5, `legion node list` showed both
workers `active`/`available`, but the stored rows said
`availability = unavailable` (`legion state query "select id, availability
from node"`). Every placement failed with `No nodes available to place
allocations`, and the volume validator rejected stateful jobs with
`local-dir ... installed on sc-wkr-1, sc-wkr-2 but unusable everywhere`.
`legion node uncordon` on each node fixed it. Our reading: liveness lives in
the leader's memory and only transitions are replicated, so a new leader that
first sees a node healthy never writes the `unavailable` → `available`
transition.

### P0-7. Stock images fail in Linux containers

*Open.* Three behaviors of the Linux container runtime, each of which breaks
common images:

- **Minimal capabilities.** Containers get only `CAP_AUDIT_WRITE`,
  `CAP_KILL`, and `CAP_NET_BIND_SERVICE`
  (`src/task_plugins/oci_bundle.rs:639-642`), where Docker also grants
  `CHOWN`, `SETUID`, `SETGID`, `DAC_OVERRIDE`, `FOWNER`, and more, and there is
  no jobspec knob. Stock nginx fails with `chown("/var/cache/nginx/client_temp",
  101) failed (1: Operation not permitted)`. The Postgres entrypoint's chown
  and `gosu` fail the same way. **The repo's own `container-service.ncl`,
  `fullstack.ncl`, and `postgres.ncl` cannot run on Linux.**
- **Ownership is dropped on layer extraction.** A `chown nginx` in the image
  shows up as `0:0` in the extracted rootfs, so images that run as a non-root
  `USER` cannot write their own directories.
- **Non-root processes cannot reopen `/dev/stdout` or `/dev/stderr`.** The
  task's stdout and stderr are root-owned log files, so opening them by path
  fails with EACCES, which breaks nginx's default log symlinks.

Also, `RLIMIT_NOFILE` is a hard 1024 (Redis warns at start). The spot-check
images work around all of this (see `images/frontend` and `images/db`).
**Fix:** default to Docker's capability set (with a jobspec override), keep
ownership when extracting layers, and hand containers pipes or fds that
non-root users can reopen.

### P0-8. A restarted worker cannot rejoin once its join token expires

*Open.* With ACLs on, a worker re-registers with its original join secret
every time the process starts. After the token's TTL it dies at startup with
`Unauthenticated: unknown or revoked join token`, and systemd gives up. The
node's per-node token is not persisted for re-registration.
`docs/auth-and-acls.md` tells operators to give join tokens short TTLs and
revoke them once the fleet has joined, which guarantees this on the first
upgrade or reboot. The rig now mints 720h tokens as a workaround.

## P1

### P1-1. Resource enforcement never turns on

*Open.* In `run_worker_with_manager` (`src/worker/daemon.rs`), the worker
creates its task manager, whose `NodeResourceGovernor::from_env()` reads the
enforcement env, about fifteen lines before `apply_resource_enforcement_env`
sets that env. The governor is
always disabled, but the worker logs `Resource enforcement enabled`. Tasks
run in the worker's own cgroup with `memory.max = max`, and
`/sys/fs/cgroup/legion` is never created.

### P1-2. Static ports deadlock scheduling, reschedules, and rollouts

*Open.*

- **Too many replicas.** `count = 2` with a static port on one node: the
  planner puts both replicas on the node, the second cannot bind, and the
  whole plan is rejected. The eval requeues about twice a second forever (60
  rejections in 30 seconds), nothing places (not even replica 1), and the job
  reports `done`.
- **Reschedules** of a failed static-port allocation loop on `requeued after
  plan conflicts` indefinitely.
- **Rollouts.** Updating a single-replica static-port service (`kv-demo`)
  left the rollout at `waiting for placement capacity` with 0 allocations.
  The service was down, and 500+ `rollout_tick` events piled up.

**Fix:** let a plan place what fits and report the rest as unplaced, account
for in-plan static ports when picking candidates, and stop rather than
replace when a static-port service cannot surge.

### P1-3. A reschedule excludes the node the task failed on, even when it is the only option

*Open.* After a failure, the reschedule eval fails with `No nodes available
to place allocations` on a one- or two-node cluster, and the job is dead for
good, even though the node is healthy. For a node-local stateful instance
the message is worse: `instance ... holds node-local storage on node
sc-wkr-1, which is unavailable ... use legion volume delete --force <id>`.
That node was healthy, so the advice tells a user to delete data they
haven't lost.

### P1-4. Gated containers race their network namespace

*Open, intermittent.* An allocation failed with `gated network namespace
/var/run/netns/legion-60 is not ready; the worker creates it before start
and retries on its next heartbeat`, but it was marked failed rather than
retried, which then hit P1-3. The race is between artifact staging finishing
and the heartbeat that creates the namespace.

### P1-5. Tasks are orphaned when a worker re-joins

*Open, observed; root cause not confirmed.* After the P0-8 re-join, processes
from the previous worker run kept running unmanaged (systemd: `Unit process
8793 (python3) remains running after unit stopped`, still bound to :8082),
while the manager placed replacements that collided with them on ports and
the TAP device.

### P1-6. macOS sandboxes do not start

*Fixed on main* after the run, so `shims.patch` no longer carries it. Dev
mode passed a `--` after the image (`src/task_plugins/sandbox.rs:563` at the
time), which Apple `container` runs as the
command: `failed to find target executable --`. The container runtime avoids
exactly this, and its test asserts no separator. With the fix, exec into a
macOS sandbox works end to end, including from the launcher's browser
terminal.

### P1-7. Allocation ids are reused

*Open.* Ids restart from low numbers once older allocations are gone. A
worker's `logs/<id>/` directory then mixes the old allocation's output into
the new one's (a sandbox's stdout started with the DAG's `fetch` lines), and
a stale id held by a client or script can point at a different job's task.
That is how the smoke test once exec'd into the wrong workload.

### P1-8. Job status is misleading

*Open.* A running service job shows `done`. A job with zero allocations after
a failed placement also shows `done`. Re-running an unchanged spec for a dead
job is a silent no-op (`Job config unchanged; no update needed`), so there is
no obvious way to retry.

### P1-9. The real failure is hard to find

*Open.*

- **Misattributed failures.** An image pull failure (a 429 from the
  registry) shows up in events only as `health check failed: tcp check
  failed: Connection refused`, and the pull error is in task stderr alone.
- **Logs vanish.** Once an allocation is removed, `legion job logs` returns
  `No allocations found`, although the files are still on the worker. On
  macOS the Apple `container` 200 MiB minimum-memory error was only findable
  that way.

## P2

1. **The README quickstart fails.** `legion start manager` with no `-c`
   evaluates `./configs/manager.ncl` and exits outside a repo checkout
   (`failed to read ./configs/manager.ncl`), and `legion job status` does
   not exist (it is `legion job info`).
2. **Errors hide their cause.** `job destroy` on a running job and `raft
   command transfer-leader` both return `Internal error` while the server
   logs a clear reason (`Job must be stopped before it can be destroyed`,
   `not supported by openraft 0.9`). Worker errors reach the CLI as raw tonic
   `Status` debug output, response headers included, coded `Internal` instead
   of `FailedPrecondition`.
3. **`legion kv get` on a missing key** exits 0 with `{ "ok": true, "data":
   null }`, not a `NOT_FOUND` error, which breaks the obvious
   `kv get || kv put` pattern.
4. **Agent-readiness gaps.** `legion raft join-token` has no `--json`.
   Instance JSON is snake_case (`current_alloc_id`) while hand-built
   envelopes are camelCase (`instanceIds`).
5. **Writes to a follower fail with an unreachable hint.** The CLI does not
   follow the leader, and the hint names the leader's private advertise
   address (`172.31.90.175:4747`), which a laptop cannot reach.
   `transfer-leader` is unsupported, so moving leadership means
   `trigger-election` on the target manager.
6. **macOS.** Apple `container` rejects VMs under 200 MiB, and Legion does not
   check `resources.memory` at submit. A fresh start logs `ERROR ... node not
   found`.
7. **Worker noise.** Every worker logs `ERROR missing plugin manifest at
   /src/plugins/tasks/noop/plugin.json`, because the plugin dir defaults to
   the build machine's path, plus `Log sink maintenance failed ... No such
   file or directory` every 10 seconds.
8. **`legion exec` into an exec task** does not get the task's environment
   (`LEGION_PORT_HTTP` is empty), while actions do.
9. **No image cache.** Every start pulls again. Anonymous `public.ecr.aws`
   rate-limited a laptop with 429s after a few rebuilds and restarts.
10. **Raft terms climb fast.** Term 10 after two leader stops, 16 after one
    manual election, which suggests several failed rounds per election.
    Observation only.
11. **Install docs gaps.** No Linux instructions for installing `youki` (the
    default runtime), cloud-hypervisor, or a guest kernel. Existing TODO
    items cover the kernel and `redis.ncl`.
12. **The UI's Managers page shows only managers that have led.** A manager
    writes its row (`upsert_manager_config` in `src/manager/daemon.rs`) only
    while it holds control-lane leadership, so followers never appear. The
    three-voter rig showed one manager. `legion raft status` is the accurate
    view. The page should list raft members and their roles.

## Not covered

The web UI, the MCP server, edge boot, mesh mTLS (`ingress.tls`), workload
identity, node drain, and the streaming persistence mode. The runbook lists
the UI and MCP as optional steps.
