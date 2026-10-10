# Judgement calls: architecture audit fixes (2026-10-09)

Audit: [architecture-audit-2026-10-09.md](../architecture-audit-2026-10-09.md).
Finding IDs (W1, S2, R4, ...) refer to that doc. Entries are ranked by project
impact, highest first.

Every fix started by re-checking the finding against the code, its history,
and the plans and calls docs, to make sure the current behavior wasn't
deliberate. One finding (SC7) was deliberate and was left alone, see entry 17.

## 1. Browser origins are allow-listed in every ACL mode, loopback allowed by default (S2)

**Call.** The manager refuses any gRPC-Web or exec WebSocket request whose
`Origin` is not loopback or listed in the new `server.allowedOrigins`.
Requests without an `Origin` header (CLI, workers, SDKs) are unaffected. CORS
echoes only allowed origins instead of `*`.

```nickel
server = { allowedOrigins = ["https://ui.example"] },  # "*" allows any origin
```

```text
HTTP 403
WARN Refused a browser request from a disallowed origin
```

**Alternatives.** (a) Same-origin only. (b) Restrict origins only when ACLs
are off, since that's the mode where every caller is management.

**Trade-off.** A UI served from a non-loopback host, a tunnel, or a forwarded
port must add its origin to `allowedOrigins`. The Vite dev proxy forwards the
browser's origin, so local dev on `:5173` keeps working. We did not add a
`Host` check, because the Origin check already stops DNS rebinding and real
clients reach the manager by many names.

**Why (simplicity).** Same-origin can't work: the UI isn't served by the
manager, and the exec WebSocket lives on port+1, which is always a different
origin. One rule in every mode is easier to explain than a rule that changes
with `acl.enabled`, and an ACL-on cluster is not hurt by refusing origins it
never intended to serve.

## 2. Mixed-schema raft clusters refuse only the writes an older member can't apply (R2)

**Call.** SQLite silently skips changeset rows for a table with fewer
columns, so two checks now stand in front of that. On apply, each table in a
changeset is checked against the local schema, and a mismatch is an error
that halts the entry instead of a silent skip. The lane resumes from it after
the upgrade. On propose, the leader checks each captured changeset against
the schema of every member advertising an older version, rebuilt from
`migrations::table_shapes_at(v)`, and refuses only writes that member can't
apply.

```text
raft op put_allocation needs a schema that manager mgr-b does not have yet (...)
```

`docs/upgrades.md` now says: upgrade followers first, the leader last.

**Alternatives.** (a) Refuse every write while any voter reports a lower
schema version, as the keyring gate does. (b) Apply-side check only.

**Trade-off.** The leader keeps a per-version schema cache built by replaying
migrations in memory, which is more code than (a). An upgrade done leader-first
still refuses writes on changed tables until the followers catch up. That is
loud, and better than divergence.

**Why (simplicity for operators).** With (a), any upgrade that touches the
schema freezes all writes until every manager is done. With this call, writes
to unchanged tables keep working mid-upgrade. The apply-side check stays,
because it also catches members the leader hasn't heard a version from.

## 3. Join tokens pin the worker type, but a join secret can still claim an existing node name (S1)

**Call.** Registration now returns what the credential allows. A join token
pins its worker type and pool. A node token with no join secret may only
re-register its own node, in its current pool. Worker-plane RPCs that take a
node id (Heartbeat, HealthCheck, FetchNetPolicy, GetArtifactCredential,
RatifyClaim) bind it to the caller's node token. Audit-only mode logs these
mismatches and allows them, like the rest of the worker plane. The two
identity-token issuers refuse a foreign node even in audit-only mode.

**Alternatives.** (a) Per-node join tokens, or workers persisting their node
token so a join secret is only needed once. (b) Refuse registration of an
existing node name from a join secret.

**Trade-off.** A compromised node still holds its join secret in memory and
can take over a peer's name with it. This is documented, with the mitigation:
short join-token TTLs, revoked once the fleet has joined. (b) breaks the
normal "worker lost its disk and rejoins" path.

**Why (simplicity).** The node-token path was the wide-open hole, and closing
it needs no new concept. (a) is a change to how workers bootstrap and belongs
in its own plan. It's in TODO.md.

## 4. Streaming standbys restore from the replica when they take the lease (R4)

**Call.** With the object-storage lease, a manager starts as a standby: its
writer loops idle and its store refuses writes. On acquiring the lease it
restores the replica, migrates it, swaps it in through the existing
`install_state_db` path, then starts shipping, and only then reports
`primary`. A restore failure gives the lease back. A primary that loses the
lease, or can't renew it within one TTL, is fenced until the process
restarts.

**Alternatives.** (a) Fail closed: a standby stays read-only, and promotion
needs a manual restart plus restore. (b) Mark streaming mode experimental and
leave it.

**Trade-off.** About 200 lines and a live database swap. A single restarted
primary also restores from the replica, so it can lose its last unshipped sync
interval, within the documented RPO. A fenced ex-primary never competes again
in the same process, because its local rustyriver state may conflict with the
new primary's.

**Why (simplicity).** The docs promised automatic failover. rustyriver
already supports restore and the store already supports a live swap, so
building the promised behavior was cheaper than walking the promise back.

## 5. A rollback stays a terminal rollout state, not a new job version (SC1)

**Call.** When the latest config's rollout is `failed` or `cancelled`,
regeneration converges whatever the rollout left behind. If it rolled back
(the latest config has no allocations), it reschedules the source config. If
only the latest config runs, the normal path applies. A mix of old and new is
left alone instead of being force-completed.

**Alternatives.** Nomad's model: a rollback re-submits the old spec as a new
job version, so everything keyed on "latest" is right by construction.

**Trade-off.** A forced reschedule, a KV-driven restart, or `job scale` after
a rollback still targets the rolled-back config. This is documented in
`docs/rollouts.md` and listed in TODO.md.

**Why (simplicity of the fix).** The Nomad model changes job history and
unchanged-resubmit detection, which users can see, so it needs a plan. This
call closes the failover hole without changing any surface.

## 6. Idempotency scope goes into the stored key, with a 24-hour TTL (S4)

**Call.** The stored key is `"{operation}/{len}:{caller}/{key}"`, where the
caller is the token's accessor id, or the principal for JWT and anonymous
callers. Records older than 24 hours are pruned hourly by the control-lane
leader, so records live 24 to 25 hours.

**Alternatives.** New `operation` and `caller` columns on
`idempotency_record`.

**Trade-off.** Records written before the upgrade no longer match, so a retry
that spans the upgrade runs again. The key still doesn't cover the request
body: the same key with different arguments replays the first reply, which is
documented.

**Why (simplicity).** Adding columns is exactly the rolling-upgrade hazard of
R2. Encoding the scope into the key needs no schema change.

## 7. A plan for a config nobody wants is an empty success, not an error (SC5)

**Call.** A config is wanted when its job exists and isn't stopped, and the
config is either the latest, an older one that still has its own allocations,
or the source of a rollout to the latest that hasn't completed. The planner
checks this before planning and the apply transaction re-checks it. An
unwanted plan places nothing and succeeds, visible as an
`allocation.plan_rejected` event and an info log.

**Alternatives.** (a) Treat it as a conflict. (b) Treat it as a hard error.
(c) "Wanted" means latest only.

**Trade-off.** An older config mid-rollout can still be topped up by a late
reschedule, which matches today's lost-node behavior.

**Why (simplicity).** (a) requeues forever and (b) marks correct no-ops as
failed. (c) breaks staged-rollout floors and stopped mixes, which need to
replace a lost allocation in kind.

## 8. Plugins are fingerprinted at load, not moved to a stable ABI (M6)

**Call.** `build.rs` records the rustc version and commit, crate name and
version, target and panic strategy. A const fingerprint hashes that plus the
size and alignment of every type that crosses the boundary. Plugins export it
through C-ABI symbols from `legion::declare_plugin_abi!()`, and all five
loaders refuse a mismatch before calling `get`.

**Alternatives.** A stable ABI (`abi_stable`, a C ABI, or out-of-process
plugins). Hashing `Cargo.lock`.

**Trade-off.** Rollout plugins still run inside the manager. A dependency
change that keeps every probed type's size and alignment but reorders fields
still gets through. The build profile is left out, so a debug host loads
release plugins in development.

**Why (extensibility).** Refusing a mismatched build turns undefined behavior
into a clear load error today without redesigning the plugin surface. A build
script can't see a plugin's own lockfile, so a lockfile hash was not
available. The stable ABI is in TODO.md.

## 9. Existing raft members fetch the KV root key from the cluster, and the fetch never mints (R3)

**Call.** A new `RaftTransport.FetchRootKey` RPC, authenticated by the cluster
secret and limited to control-lane members, exports an existing root key,
sealed like the join bundle. `keyring_tick` calls it when the `file`
provider has no key, trying the leader first.

**Alternatives.** (a) Block enabling encryption until every voter holds the
key. (b) Reuse the join path's mint-on-demand.

**Trade-off.** One new transport RPC. Until the others fetch, the key exists
only on the minting leader, for about as long as it takes the keyring row to
replicate. That window is documented.

**Why (simplicity).** The KV encryption calls (#18, #22) already accept that
a cluster-secret holder gets the root key, and #22 weighed this exact RPC. It
also covers the lost-key and failover cases, which (a) doesn't. A freshly
minted key wraps none of the data keys, so (b) would hand back a useless key.

## 10. Capacity wakeups are one nodeless `node_join` eval (SC4)

**Call.** Worker registration and uncordon call
`Store::request_capacity_reconcile`, which queues one `node_join` eval with no
node unless one is already pending. It reconciles configs that are short of
their count or have `count.per` tasks, plus incomplete-rollout targets.
Queueing failure doesn't fail the registration or uncordon.

**Alternatives.** (a) A new eval kind. (b) Full Nomad-style blocked evals.

**Trade-off.** The "short of count" check is loose, so a false positive costs
a no-op plan. Batch tasks are excluded, because a finished batch task can't be
told from one that never placed. Allocations freed by a stop or scale-in, and
a lost node recovering through heartbeats, do not trigger the pass yet.

**Why (simplicity).** A nodeless `node_join` reuses an existing kind without
the legacy handler's fake-node insertion and its `mark_node_ready` bug. Blocked
evals belong to the gang-scheduling plan.

## 11. Only launches moved off the worker's task-manager lock (W3)

**Call.** A manager-dispatched launch registers a pid-less placeholder under
the lock, does volume mounts, artifacts, `prepare` and `start` with no lock
held, then retakes the lock to install, record a failure, or tear down a launch
cancelled meanwhile. A duplicate dispatch gets `BeginOutcome::InFlight`.

**Alternatives.** Also move stop, destroy and unmount off the lock.

**Trade-off.** Stop-path shell-outs can still stall heartbeats. Edge-boot
provisional starts and `ratify_claim` keep the locked path, because their
failures settle a claim instead of being reported. A launch cancelled
mid-start is hard-destroyed with no grace period, because it never reached the
running set. That is stated in the public doc. It holds back only its own
allocation id, and its CPU and memory stay reserved until teardown.

**Why (performance).** Image pulls and volume attaches are the slow,
heartbeat-starving cases. Moving teardown needs a new "tearing down" state to
keep the rule that starts wait behind stops, which is a bigger change. The rest
is in TODO.md.

## 12. Worker self-fencing survives restarts through a boot fence (W6)

**Call.** The wall-clock time of the last manager contact is persisted beside
`worker_state.json` on every heartbeat and registration. A boot fence runs from
process start until pre-registration succeeds, and destroys restored stateful
tasks whose fence window the persisted silence already exceeds.

**Alternatives.** Reorder boot so restore and the in-process watchdog run
before contacting the manager.

**Trade-off.** The boot fence reports nothing. The manager learns about a
fenced task only by its absence. Registration still resets the clock, leaving
a few seconds unfenced between pre-registration and registration. The record
isn't fsynced, so a lost write only makes it older and fences earlier, which is
the safe direction. A backward clock step delays the fence by the size of the
step.

**Why (simplicity).** Restore needs config the manager serves (volumes,
resource enforcement, artifacts), so it can't move before contact. The audit
said the in-process clock reset was the bug. The real gap was that a worker
that can't reach the manager never got as far as restore, so its surviving
stateful tasks were never fenced at all.

## 13. Private keys are redacted from state queries, not encrypted at rest (S3)

**Call.** `QueryStateStore` installs a SQLite authorizer for the statement
that reads `signing_key.private_key_pem`, `net_mesh_ca.key_pem` and
`acl_user.password_hash` as NULL, however the query reaches them.

**Alternatives.** Encrypt those columns with the KV keyring.

**Trade-off.** State backups (`operator:write`) still contain the keys. That's
documented. The password hash is redacted too, against offline cracking.

**Why (simplicity).** No format change, nothing new for raft or restore to
handle, and no dependency on the root key, which R3 showed can be missing on
followers.

## 14. Template evaluation runs on a capped thread pool with a timeout (M1)

**Call.** Each Nickel evaluation runs on its own OS thread under a 5-second
budget that includes waiting for a slot. A global semaphore caps evaluations at
half the cores, between 1 and 4. The evaluation thread holds its permit, so an
abandoned runaway keeps its slot until it really ends.

```text
RESOURCE_EXHAUSTED: the manager is already evaluating N templates; retry shortly
```

**Alternatives.** Evaluate in a killable child process.

**Trade-off.** A runaway still pins a core until it finishes or the manager
restarts, and a stream of runaways blocks all template evaluation. The 5-second
budget now applies at publish time too.

**Why (simplicity).** nickel-lang-core 0.18 has no step limit or cancel hook.
A subprocess needs re-exec plumbing the test binary can't do. The cap is what
protects raft and the API.

## 15. Missed KV reactions are recovered by a stateless leader sweep (M4)

**Call.** Every 30 seconds, the control-lane leader re-renders each non-stopped
job with KV references and compares it to what's running. On a difference it
reconciles the job's present keys as puts. A missing `onDelete=fail` key is
reconciled as a delete.

```text
INFO Job missed a KV-driven reconciliation; reconciling now
```

**Alternatives.** A durable per-job marker column.

**Trade-off.** A missed delete with `onDelete=restart|rollout` can't be
detected, because the deleted key keeps its last rendered value. A recovered
put applies the strongest `onChange` among the job's present keys. The sweep is
O(jobs) reads every 30 seconds. `docs/kv-store.md` no longer says events are
"never dropped".

**Why (simplicity).** No schema change (see R2), and it compares replicated
state, so it covers crashes and leader changes alike.

## 16. Manager shutdown stops raft lanes but doesn't transfer leadership (M3)

**Call.** SIGTERM or SIGINT drains RPCs and streams for 10 seconds, stops
background loops, and gives persistence 15 seconds to shut down. Streaming
does a final replica sync before releasing the lease. Raft shuts down each
lane's core. A second signal skips the rest.

**Alternatives.** Emulate a leadership step-down, which the codebase has
already rejected once.

**Trade-off.** Stopping a leader pauses writes on its lanes for one election
timeout. The budgets fit inside Kubernetes' default 30-second grace period.

**Why (simplicity).** openraft 0.9 has no leadership-transfer API. When
merging this with R4, a fenced manager skips the final sync, because the
replica belongs to the new primary by then.

## 17. Fresh plans still count the replaced job's allocations as free (SC7, not changed)

**Call.** No change. Counting the old config's allocations as free during a
Fresh plan is deliberate and documented in `plan_and_apply_internal`.

**Alternatives.** Remove the replaced allocations inside the same apply
transaction.

**Trade-off.** Old and new tasks briefly overcommit the node, and a crash
between apply and the old config's removal leaves the overcommit until the
next failover.

**Why.** Without it, resubmitting any job that uses more than half a node
fails. The atomic swap needs the node-epoch self-conflict handling that
evictions use, plus careful ordering of stateful release and rebind. It's in
TODO.md.

## 18. Stream re-auth is a 30-second recheck, and jobless volumes need cluster-wide `job:write` (S7)

**Call.** `StreamEvents` and exec sessions (gRPC and the WebSocket) re-check
their credential every 30 seconds (`acl::STREAM_REAUTH_INTERVAL`). Exec re-runs
the same authorization it opened with. Deleting a volume no job owns needs
cluster-wide `job:write`.

**Alternatives.** Push-based revocation into open streams. `node:write` in
the volume's pool for jobless volumes.

**Trade-off.** A revoked token keeps streaming for up to 30 seconds. An exec
session also ends if its allocation leaves a runnable state mid-session.

**Why (simplicity).** A timer needs no revocation fan-out. Keeping jobless
volumes under `job` matches the "volumes are job storage" model.

## 19. A raft peer that sends no schema version is treated as version 0 (R2)

**Call.** A transport reply with no `x-legion-schema-version` header records
the peer as version 0.

**Alternatives.** Treat the peer as unknown and skip the check.

**Trade-off.** A newer leader refuses every write while a pre-header member is
running. Upgrading followers first avoids it.

**Why.** A pre-header build has no apply-side check either, so skipping the
check means silent divergence.

## 20. Restored task records without a start time are trusted once (W2)

**Call.** A task is re-adopted only when its pid's start time matches the
recorded one, at one-second resolution. Records written before this change
have no start time, so they are adopted on liveness alone, once, and
backfilled.

**Alternatives.** Refuse records without a start time.

**Trade-off.** The reused-pid risk remains for that one restart.

**Why.** Refusing them would orphan or duplicate every live task on the first
upgrade. One-second resolution is what `sysinfo` gives across platforms, and a
pid reused within the same second after a reboot is not a realistic case.

## 21. Job-scoped evals must also match on node to collapse (SC2)

**Call.** The claim sweep collapses only evals with the same kind, job, config
and node.

**Alternatives.** Ignore the node for job-scoped kinds.

**Trade-off.** Reschedules for one job from two different nodes stay separate
evals.

**Why.** A reschedule's node is the node the replacement must avoid, so the
two aren't duplicates.

## 22. Failed reconciles retry after a fixed 5 seconds (SC3, SC6)

**Call.** A config that fails during a lost-node, drain or capacity pass, or a
`job_update` or rollback that can't place, queues a `job_reschedule` delayed by
`RECONCILE_RETRY_DELAY` (5 seconds).

**Alternatives.** Exponential backoff.

**Trade-off.** A config that can never place retries every 5 seconds until
something else changes.

**Why (simplicity).** The SC4 capacity pass and leader regeneration also pick
these jobs up, so backoff would add state for little gain.

## 23. A drain interrupted by a worker restart resumes by signalling at once (W4)

**Call.** Draining and terminating allocations are persisted with a `stopping`
block. On restore, ones in their grace window rejoin the terminating set with
their original deadline, and ones that were draining are signalled right away.

**Alternatives.** Restart the drain period.

**Trade-off.** The grace deadline is stored as wall-clock time, so a host clock
step shifts it.

**Why.** The restart already spent the time the drain buys.

## 24. Re-registration reads availability in the handler (S6)

**Call.** The handler reads the node's existing availability and sends it
explicitly to the store, instead of the apply step reading it.

**Alternatives.** Read it inside the replicated apply.

**Trade-off.** A drain that lands between the read and the insert can be
overwritten, which is a small window.

**Why.** Reading in apply would make the result depend on each replica's
state, and replicas must apply deterministically.

## 25. The proto breaking check uses a pinned buf binary and `WIRE` rules (M5)

**Call.** CI downloads buf 1.50.0, checks its published sha256, and runs `buf
breaking` against the base branch's `legion.proto` extracted with `git show`.
A `proto-breaking-ok` label skips it.

**Alternatives.** `bufbuild/buf-action`. `WIRE_JSON` rules.

**Trade-off.** Version bumps are manual edits. Field renames pass.

**Why.** An exact, checksummed version with no need for buf to clone the base
ref. Renames don't break the wire format, and nothing depends on proto JSON
names yet.

## 26. Snapshot builds stopped stalling writes, but stay in memory (R5)

**Call.** Snapshots are built on a dedicated read-only connection outside the
capture mutex. Whole-database snapshots read the applied marker back out of the
`VACUUM INTO` image, and per-lane snapshots dump inside one read transaction.

**Alternatives.** Also stream snapshot data as a file
(`SnapshotData = tokio::fs::File`).

**Trade-off.** A snapshot is still held fully in memory on build and install.
That is documented in `docs/multi-raft.md` and listed in TODO.md.

**Why (performance).** The global write stall was the outage-shaped half. The
file-backed snapshot type touches install and every snapshot test.

## 27. The fix branch is based on the rewritten `origin/main`

**Call.** The audit ran against local `main`, which had diverged from a
rewritten, newer `origin/main` (830 commits ahead and 841 behind). The fixes
are based on `origin/main`, and each fix re-verified its finding there.

**Alternatives.** Base on local `main`.

**Trade-off.** Some audit line numbers point at the old history.

**Why.** The PR has to land on `origin/main`.
