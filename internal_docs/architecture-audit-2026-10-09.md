# Architecture audit (2026-10-09)

> **Judgement calls:** [architecture-audit-2026-10-09_calls.md](calls/architecture-audit-2026-10-09_calls.md)

A whole-codebase review for design-level errors, not style. Five reviewers each
took one area (state store and raft, scheduler, manager API and security,
worker, cross-cutting) and every finding was confirmed in code with a concrete
failure scenario. Findings already tracked in [TODO.md](TODO.md) were left out.

The core design held up. Plan/apply fencing, leader-only loops in raft mode,
ACL coverage, and capture/apply mutual exclusion all checked out. The problems
cluster at the edges: retries across failover, upgrades, empty or stuck nodes,
and trust between workers.

Each finding has an ID used by the fix PR, its commits, and the calls doc. The
**Status** line records what the fix PR did. Line numbers are from the review
and may have drifted.

## Critical

### W1. An empty desired set never stops anything
- `src/worker/daemon.rs` wraps the whole reconcile in
  `if !reply.desired_allocations.is_empty()`.
- The manager sends an empty list when a node has nothing left, which the
  proto documents as valid. So stopping, draining, or preempting the last
  allocation on a node leaves it running forever while the manager thinks it
  stopped.
- Unit tests call `reconcile_allocations(&[])` directly and the simulator
  reconciles unconditionally, so neither covers the daemon's guard.

### S1. Worker registration lets any node take over any other node's identity
- `verify_join_for_registration` (`src/manager/server.rs`) returns `Ok` for any
  caller that is already a worker and discards the verified join token record.
- `worker_register` then trusts `request.name` and `request.pool`, upserts the
  node, and mints a token for it, which revokes the victim's token.
- A compromised node A registers as B, locks B out, and passes the principal
  checks on `IssueWorkloadIdentityToken`, `IssueNodeCertificate`, and
  `IssueNodeIdentityToken` (cloud federation tokens, mesh certs).
- The join token's pinned worker type is never enforced, contradicting
  `docs/auth-and-acls.md`.
- `Heartbeat`, `HealthCheck`, `RatifyClaim`, and `FetchNetPolicy` never bind
  the request's node id to the caller's principal.

### R1. A new raft leader captures writes before it has applied the log
- `execute_replicated` (`src/storage/raft/driver.rs`) only checks
  `state == Leader` before `capture_mutation`. Nothing waits for `last_applied`
  to reach the leader's first entry of the term.
- Old leader A logs `put_kv x` at entry 100, dies before the commit is known.
  B wins with applied=99, the client retries on B, and B captures an INSERT at
  entry 102. Entry 100 applies first, entry 102 conflicts, and
  `resolve_apply_conflict` aborts. That is deterministic, so every replica
  including the leader halts the lane on the same entry.

### R2. Rolling manager upgrades across an `ADD COLUMN` silently split replicas
- Raft entries are SQLite session changesets. SQLite skips every change for a
  table whose target has fewer columns than the changeset, returns OK, and only
  logs a warning.
- An upgraded leader's `allocation`, `task_config`, or `node_health` writes are
  dropped by old voters, which still advance their applied index. Upgrading
  them later never replays the lost writes.
- `docs/upgrades.md` recommends rolling upgrades. `raft/types.rs` says mixed
  versions are unsupported. Only the keyring has a schema-version gate.

### R4. Streaming-mode lease fencing does not fence
- `Store::is_raft_leader` returns `true` for every non-raft backend, so a
  standby runs eval workers and the node checker and accepts writes.
- `StreamingPersistenceDriver::start` starts WAL shipping whatever the role, so
  two managers ship to one replica prefix.
- Nothing restores a standby from the replica when it takes the lease.
  `docs/persistence-streaming.md` promises "automatic primary election/fencing".

### SC1. A rollback is undone by the next leader failover
- `rollback_rollout` re-places the old config but the job's latest config is
  still the bad one.
- `regenerate_scheduling_evals` sees "latest has 0 allocs" and enqueues
  `job_submit`, bringing the bad version back at full count with no canary or
  health gating. Rollback is the default `onHealthDeadline`.
- A stopped rollout without revert leaves a mixed state that the same pass
  force-completes.

## High

### SC2. The claim sweep deletes unrelated or newer evals
- `claim_eval_apply` (`src/storage/store.rs`) deletes pending evals with the
  same kind and the same job **or the same node**, ignoring `job_config_id`.
- Reschedule evals for jobs A, B, and C on one node collapse into A's, leaving
  B and C under-replicated. A stale v2 `rollout_tick` sweeps the v3 tick and
  stalls the new rollout.

### SC3. Lost-node and drain rescheduling can strand jobs
- `reschedule_allocations_for_lost_node` and the drain variant remove
  allocations, then reconcile each config with `?` and no retry. One failing
  config aborts the rest.
- The re-enqueue that the code comment relies on only fires for nodes that
  still have allocations, so nothing heals it.

### SC4. Nothing retries placement when capacity appears
- "No capacity" fails the eval terminally. Production never enqueues
  `node_join`, and worker registration triggers nothing.
- Under-capacity jobs never converge as nodes join, and per-node
  (`count.per`) tasks never reach new nodes. `docs/autoscaling.md` says
  otherwise.

### SC5. Plans don't check the job or config is still wanted
- `plan_and_apply_internal` never checks job status or that `job_config_id` is
  still the latest. A delayed reschedule after `job stop`, or after a v2
  rollout completes, places the full count for a stopped or superseded config.

### S2. With ACLs off, any website can drive the cluster
- gRPC-Web is served with `CorsLayer::allow_origin(Any)`, and the exec
  WebSocket sends `access-control-allow-origin: *` with no Origin or Host check.
- The default `configs/manager.ncl` has ACLs off, which makes every caller
  management. A malicious page can call `RunJob` against `127.0.0.1`, and DNS
  rebinding works the same way.

### S3. `state:read` exposes plaintext private keys
- `QueryStateStore` runs arbitrary SELECTs. `signing_key.private_key_pem` and
  `net_mesh_ca.key_pem` are stored in plaintext.
- A `state:read` holder can forge workload-identity JWTs and boot tickets and
  mint mesh certs. `docs/auth-and-acls.md` implies only KV values are hidden.

### S4. Idempotency keys are one global namespace and never expire
- `idempotency_lookup` selects by `key` only and never compares `operation`.
- Reusing a `job run` key on `job destroy` silently skips the destroy and
  returns `deduplicated: true`. A second user can replay another's key and read
  their recorded reply.
- No code path ever deletes from `idempotency_record`.

### R3. Clusters formed before KV encryption get the root key only on the leader
- The root key reaches a manager only through the raft join reply. Existing
  members never re-join, and followers never mint.
- After a rolling upgrade, follower KV reads fail. After the first failover,
  KV writes fail cluster-wide. Losing the original leader's disk loses every
  KV value. `docs/kv-encryption.md` says the rolling upgrade turns encryption
  on by itself.

### W2. Tasks are re-adopted by bare pid
- `restore_allocations` accepts any pid that `pid_is_running`. After a host
  reboot the pids belong to unrelated processes, which the worker adopts,
  reports as running, and later kills on stop.
- `pid_is_running` rescans the process table per call, once per allocation per
  second, under the task-manager mutex.

### W3. Blocking work under the task-manager mutex starves heartbeats
- Volume mounts, `plugin.prepare`/`start`, and OCI registry pulls run while
  holding the global `Mutex<WorkerTaskManager>`, which `heartbeat_tick` and the
  self-fence watchdog also need.
- A slow pull or attach past the lost window gets the node marked lost and its
  stateless work replaced, so it runs twice.

### M1. One template upload can hang a manager core forever
- `UpsertJobTemplate` evaluates Nickel with no timeout through
  `block_in_place`. A non-terminating program pins a thread per call, and a few
  calls starve raft heartbeats.

### M2. A failed gRPC bind leaves a headless manager running
- The serve future's result is discarded inside a `join!`. A bind or TLS
  failure leaves eval workers and the node checker running with no API and no
  raft transport.

## Medium

### W4. Allocations in their stop grace window are dropped from durable state
- `stop_allocation_inner` removes the allocation before draining, and
  `persist_state` only serializes `self.allocations`. A worker restart orphans a
  task that ignores SIGTERM, and its volume loses its in-use guard.

### W5. The hard kill hits only the leader pid
- SIGTERM goes to the process group, but the deadline SIGKILL goes to the pid
  alone. `sh -c server` orphans the server.

### W6. The self-fence clock resets on every worker restart
- `last_successful_heartbeat` starts at `Instant::now()` on boot and is never
  persisted, so a crash-looping partitioned worker pushes its fence out each
  restart while the manager's `replaceAfter` keeps counting.

### SC6. `job_update` and rollback delete everything, then place with no recovery
- If the plan fails, the job sits at zero allocations with a terminal failed
  eval, and nothing re-derives it in steady state.

### SC7. Fresh plans spend capacity the old config still holds
- Apply counts the replaced job's allocations as free, and the old config is
  removed only after apply, so nodes briefly overcommit.

### S5. `RotateMeshCa` is cluster-wide behind a pool-scoped check
- It uses `check_acl(Net, Write, AclScope::any())`, which any pool-scoped net
  admin passes.

### S6. Re-registration resets node availability
- `worker_register` hardcodes `availability: "available"`, so a worker restart
  silently un-drains a node.

### S7. Fetch-before-check, unscoped volume delete, and streams that never re-auth
- `GetNode` and `RestartWorker` fetch before enforcing ACLs. `DeleteVolume`
  enforces no scope on volumes without a job. `StreamEvents` and exec sessions
  keep running after their token is revoked.

### M3. The manager has no shutdown path
- No SIGTERM handler, so raft leaders die without transferring leadership and
  streaming mode never does its final sync.

### M4. KV-driven reconciliation is best-effort, though the docs say "never dropped"
- KV events are queued in memory and discarded on error, crash, or
  `NotRaftLeader`. No sweep recovers them.

### M5. Nothing guards the wire contract
- No `buf breaking`-style check in CI, and proto history already shows a
  renumbered field and a reused number.

### M6. The dylib plugin ABI is Rust-to-Rust and unchecked
- Plugins export `Box<dyn Trait>` across a dylib boundary, guarded only by
  hand-bumped integers. A different rustc or serde feature set passes the check
  and is undefined behavior. Rollout plugins run inside the manager.

### R5. Snapshot builds stall every write and load the database into memory
- `VACUUM INTO` runs under the process-global capture mutex, and the image is
  read fully into memory on build and install.
