# Judgement calls: the TODO bug and security sweep (2026-11-08)

This work had no plan: it fixed the open bug and security items in
[`TODO.md`](../TODO.md), and the fixes are listed in its Done section under "TODO bug
and security sweep (2026-11-08)". Entries are ranked by project impact, so an
entry's number is its rank. Limitations a user needs to know are also stated in
the public docs that each entry links.

## 1. A remote proxy that isn't on the mesh is refused, not dialed in plaintext

An HTTP-mode upstream that hands its request to a `service =` proxy now goes over
the mesh (mTLS plus a PROXY v2 header naming the workload) when the proxy's
endpoint is on the mesh. A plain endpoint on this node is still dialed in
plaintext over loopback. A plain endpoint on another node is not dialed at all:

```console
502: proxy endpoint 10.0.1.7:3128 is on another node and not on the mesh; set `mesh = true` on the proxy's service, or use `local = true`
```

- **Alternatives:** refuse only when a credential is injected or the target is
  `https`; or drop remote plain endpoints when the hop resolves.
- **Trade-off:** this breaks anyone running a remote proxy that isn't on the mesh
  in front of an HTTP-mode upstream. TCP, ingress, and gated-proxy `http://`
  hand-offs are still plaintext (open in TODO). The proxy's mesh port has to stay
  in L4 mode, because an HTTP-mode ingress chain would re-issue the request in
  origin form.
- **Why:** the hand-off can carry an injected credential or a downgraded `https`
  request, so the rule fails closed, and the fix is one field. Simplicity and the
  security boundary won over a narrower rule that users would have to reason
  about per request. [docs](../../docs/network-handlers.md)

## 2. `local-dir` volumes moved beside `volumes/`, each behind its own guard

A volume's payload is now at `<state dir>/local-volumes/<id>/data`, where
`local-volumes/` is `0711` and `<id>/` is a `0700` guard. An `exec` task with a
`user` is handed the guard and the data by `lchown`, but only when the top
directory isn't already the user's (Kubernetes `OnRootMismatch`).

- **Alternatives:** make `volumes/` itself traverse-only, as the TODO entry
  suggested; or hand the data over with POSIX ACLs, or a group and `0750`.
- **Trade-off:** the on-disk path changed (there is no installed base). A volume
  used by `exec` tasks with different `user`s belongs to whichever started last,
  and a read-only mount is chowned too. The user owns its guard, so it could swap
  `data` for a symlink, which is the same class of risk as the allocation-dir
  chown.
- **Why:** `volumes/` also holds nfs and tmpfs mountpoints (tmpfs defaults to
  `1777`), snapshots, and plugin `volumeDir` data, all created at the umask, so
  making it traversable would expose them to anyone who can guess a name. The
  guard wraps the data rather than the data dir being `0700`, so a container's
  non-root image user (nginx, postgres) keeps the read access it has today.
  [docs](../../docs/volumes.md)

## 3. What the loser of an idempotency-key race is handed depends on the operation

`deduplicated: true` now reaches the loser of a concurrent race on every keyed
RPC (`src/manager/server/idempotency.rs`). What the loser is told depends on the
kind of operation:

- **The write commits with its record** (instance verbs, fork, suspend and
  resume, snapshot create, boot-ticket issue): the claim sits in the same write,
  so the loser's write rolls back and it is handed the winner's reply. The
  boot-ticket and snapshot rows moved into the same group commit as their key to
  make this possible.
- **A repeat converges** (stop, destroy, scale, deletes, settings): the loser
  gets the winner's recorded message.
- **Exec key create:** the loser's key is deleted and the winner's is returned.
- **The operation can't be undone** (`rotate_data_key`, app actions): both ran.
  The loser gets its own reply with `deduplicated: true` and a warning is logged.

- **Alternative:** one rule for every RPC, such as always handing back the
  winner's message.
- **Trade-off:** four behaviors to document. `install_app`/`uninstall_app` and a
  losing `delete_namespace_policy` are still not covered (open in TODO).
- **Why:** an agent retrying with a key needs the truth about what ran, and that
  differs by operation. [docs](../../docs/agent-readiness.md)

## 4. `onChange = "none"` moves allocations in place

A watched KV put with `none` (or a `watch = false` edge) now moves the running
allocations to the new config in place. It doesn't rewrite their files or
replace anything. A failed job is not moved.

- **Alternatives:** a marker column that `reconcile_missing_rollouts` skips (a
  schema change to replicated state); or not storing the new config, which would
  make a later node-failure reschedule use stale secrets.
- **Trade-off:** if a rollout is in flight, only the allocations on the immediately
  previous config move (shared with `update`, open in TODO).
- **Why:** in-place moves already exist and are tested for `update`, so this was
  the simplest option. [docs](../../docs/kv-store.md)

## 5. The manager's pin guard has no allow list

`resolve = "pin"` lookups use the built-in host-guard classes plus the manager's
own interface addresses. Private ranges are not refused, matching the worker's
defaults.

- **Alternative:** a manager `hostGuard.allow` setting.
- **Trade-off:** a dev registry on the manager's own host needs an explicit
  digest or `resolve = "always"`. A guard refusal is recognized by its message
  text, not by a typed error.
- **Why:** an allow list is a new config surface and needs a plan. The refusal
  names the two workarounds. [docs](../../docs/artifacts.md#what-the-host-guard-checks)

## 6. Container ownership is proven by the bundle path, and the cgroup leaf stays `legion-<alloc>`

Linux OCI ids are now `legion-<node>-<alloc>`, capped at 63 bytes because the id
is also the hostname; only the node part is cut. Stale cleanup deletes a
container only when `runc state` reports this allocation's own bundle.

- **Alternatives:** OCI annotations read back from `state`, mirroring the macOS
  labels; and node-scoping the cgroup leaf too.
- **Trade-off:** two workers sharing a `cgroupParent` can still collide on
  cgroups, and gated netns names aren't node-scoped either (open in TODO). When
  `state` fails, the container is treated as absent and not deleted.
- **Why:** runc and crun both report `bundle`. Annotations would have needed a
  new field at 12 bundle construction sites, and the leaf name is parsed by the
  API socket, the kernel guard, and the orphan sweep. Simplicity won.
  [docs](../../docs/task-runtime-container.md)

## 7. A fail-open body hop streams an oversized body through

When every body hop on a route has `onError = "allow"`, a body over the largest
`maxBytes` is sent through unread: the part already read, then the rest. If any
hop fails closed, the request is denied with `403`. A client that disconnects
mid-body now gets `400`, not `413`.

- **Alternative:** keep `413`, treating an oversized body as a client error
  rather than a hop error.
- **Trade-off:** a fail-open `dlp` hop never inspects bodies over its cap. The docs
  say so and advise `deny` for a hop that must see everything.
- **Why:** "allow" on error lets the request through for every other hop error,
  so it should mean the same here. [docs](../../docs/network-handlers.md)

## 8. A revert whose re-placement fails still returns success

`StopRollout` with `revert` now cancels the rollout and removes the new version
in one write, then re-places the old version. If the re-placement fails, the stop
returns OK with the rollout's own message and queues a `job_reschedule` retry:

```console
Rollout stopped and reverted; the new version is removed, but re-placing the previous version failed (…), so a job_reschedule retry is queued
```

- **Alternative:** return the error after cancelling.
- **Trade-off:** a caller that checks only the status code doesn't see the failed
  re-placement. The `StopRollout` reply now returns the rollout's own message.
- **Why:** once the rollout is terminal, a retried stop does nothing, so an error
  would invite a pointless retry while the real one is already queued.
  [docs](../../docs/rollouts.md)

## 9. A collision a caller can't read is answered `PERMISSION_DENIED`

A template submit that collides with an existing job gets `FAILED_PRECONDITION …
already exists` only if the caller can read that job. Anyone else gets a
`PERMISSION_DENIED` built only from the job name and template.

- **Alternative:** a neutral `FAILED_PRECONDITION` for everyone.
- **Trade-off:** the status code differs by who asks. A refused run still differs
  from a successful create, and that can't be closed without breaking creating
  jobs through templates.
- **Why:** the denial looks like any other ACL refusal, so it tells the caller
  nothing new. [docs](../../docs/job-templates.md)

## 10. Stale settles are fenced by a claim epoch, and there is no periodic re-sweep

Claims live in the leader's memory, and eval rows stay `pending` while they are
claimed, so the TODO's `status = 'in_flight'` guard would check nothing. Instead,
`EvalClaims` has an epoch that goes up on every clear, and a settle writes only
the outcomes still claimed in its own epoch.

- **Alternative:** a SQL status guard, plus a periodic `requeue_in_flight_evals`.
- **Trade-off:** a plan's own settle isn't fenced (harmless), and a handler that
  never returns keeps its claim until leadership changes (open in TODO).
- **Why:** the epoch fence is one mutex read per settle. A re-sweep needs a
  stated bound on how long a handler can run, and none can be stated today.
  [docs](../../docs/scheduler-and-eval-loop.md)

## 11. A plugin write scans running jobs

`UpsertNetPlugin` now checks the latest config of every job that isn't stopped,
for each hop that uses the plugin, and refuses with `FAILED_PRECONDITION`. Dry
runs are checked too. The error names the job only when the caller has `job:read`.

- **Alternative:** keep checking policies only. That was calls entry 43 of the
  mesh-interfaces plan, chosen for performance.
- **Trade-off:** each plugin write costs a full scan of running jobs.
  `DeleteNetPlugin` still checks only policies (open in TODO).
- **Why:** plugin writes are rare operator actions, and a running job left with an
  invalid config is worse than a slow write. Correctness won over performance on
  a cold path. [docs](../../docs/network-plugins.md)

## 12. Smaller calls

- **A no-op `rollout_tick` re-runs its own eval quietly** instead of writing a
  completion with no event. That costs one write instead of two, and it touches
  no wire op. A tick counts as a no-op when its status, message, and runtime
  state are unchanged and it released no dependents. The first tick of each new
  wait still emits events.
- **A rollout's backoff wait doesn't reset the step's health clock**, so a
  crash-looping new version still hits `healthyDeadline`. Backoff chains and
  `moveCount` stay per version.
- **Startup recovery runs whenever this process is the scheduler's writer**, in
  every mode. That costs one idempotent re-plan eval per active job at boot, and
  it also covers a raft node that is already leader when it starts.
- **A failed projection records a `recovered` capacity event per available node**
  rather than waking evals directly, which a replicated apply can't do. On fleets
  over 8,192 available nodes, the log cap trims the oldest of these events.
- **The task-config read-through is guarded by an epoch counter, not `Arc`
  pointer equality.** Pointer equality can't see absent → cached → removed, which
  is exactly the destroy race.
- **`node_alloc_usage` orphans are swept without the two-sweep grace
  `node_health` uses.** The row is a validated cache, so a wrong delete costs one
  rebuild.
- **Whether rustyriver owns WAL checkpointing is decided once from config**
  (streaming with a replica URL). The `wal_autocheckpoint=4000` backstop stays.
- **The orphan-netns sweep skips any namespace a process is inside**, because
  netns names carry no node id. It runs once per worker start.
- **Boot-fence kills are recorded in `boot_fenced.json`**, not in a new
  `PersistedAllocation` field. A crash before the next heartbeat loses the report.
- **A pushed gated task's launch-time namespace isn't added to netd's set**, so a
  heartbeat reply computed before the push can't tear it down.
- **A follower's audit batch whose forward fails is logged, not re-queued**, which
  keeps the writer simple and the queue bounded.
- **The UI ACL parity test is a dependency-free Node script reading a shared JSON
  fixture**, not vitest. The old mirror fails 10 of its 50 cases.
