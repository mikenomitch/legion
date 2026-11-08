# Judgement calls: launch blockers and security gaps (2026-11-08)

Plan: [launch-blockers-and-security-gaps-plan.md](../plans/launch-blockers-and-security-gaps-plan.md).
Entries are ranked by project impact, highest first; an entry's number is its
rank.

## 1. A cached image is served per repository, and only to a credential the registry accepts

**Call.** The node content store records, for every blob of an image, the
repository (`registry/repo`) it was pulled and verified from and whether the
registry served it anonymously (`image_source`). A pull may read the cache
only when the image came from the same repository and is public there or the
registry accepts this credential (a grant from the last 10 minutes, or one
manifest `HEAD` by digest). An auth refusal fails the pull. A pull that is not
authorized fetches its manifests from the registry and reuses only blobs
verified from the same repository.

```text
staging failed: oci://ghcr.io/acme/api@sha256:4f…: the registry refused this job's credentials for an image this node holds; a cached private image is served only to jobs its registry lets pull it
```

**Alternatives.** (a) The TODO's own sketch: one `HEAD` with the job's
credential before serving any cached image, no provenance. (b) Kubernetes'
`AlwaysPullImages`: always pull. (c) Per-credential cache partitions.

**Trade-off.** One `HEAD` per private-image start after the 10-minute grant
lapses, a one-time re-pull of every image cached before provenance existed,
and a private image whose registry is down cannot start after a worker
restart (grants are in memory). (a) alone was spoofable: a job naming
`oci://attacker/x@sha256:<victim>` gets a `200` from the attacker's own
registry. (b) costs every start a full manifest round trip and the
rate-limited GETs the cache exists to avoid. (c) multiplies disk use by
tenants.

**Why (simplicity, with security winning over performance at the margin).**
Provenance plus a credential check closes the cross-tenant leak and the
cross-repository spoof without changing the public-image fast path (zero
requests). Grants stay in memory because the key is an unsalted hash of the
credential.

## 2. `restart.policy = "never"` ends a failed task instead of moving it

**Call.** A `never` task whose exit is not a success completes as `failed`
(`TerminatedAllocation.success = false`), and is neither restarted nor
rescheduled. `success` dependents stay held; `complete` dependents start.

**Alternatives.** Drop the `success` field and keep `never` meaning "no local
restart, but reschedule", as the docs' table said.

**Trade-off.** A behavior change: a `never` task that used to retry on another
node now stops. Users who wanted that should use `onFailure` (the batch
default).

**Why (simplicity).** "Never" that still retries elsewhere surprises everyone,
the proto already promised the field, and without it a `complete` dependency
on a failing batch step was unreachable.

## 3. A keyed instance spends its own move budget, and spending it stops only that instance

**Call.** For `stateful.mode = "keyed"`, `moveCount` counts the instance's own
moves inside `movePeriod`; when spent, the instance is stopped (as `legion
instance stop` would) with a `last_stop` hint, the event carries
`instanceStopped: true`, and no `job.failed` follows. `legion instance start`
clears the instance's move history. Slot instances and stateless tasks keep
the task-wide budget that fails the job.

**Alternatives.** (a) Per-instance counting that still fails the job. (b)
Per-instance counting for slot instances too.

**Trade-off.** A keyed job with `moveCount` set never fails as a whole from
failures; an operator watching only `job.failed` misses a stopped instance.
(b) would change what a fixed replica set's budget means.

**Why (simplicity).** Keyed instances are independent units (a sandbox per
user); one user's broken workspace failing everyone's is the bug. Slot
instances are a replica set whose health is the job's.

## 4. Dependency releases bypass rollout gates, but a service dependent stays inside its step

**Call.** Every `rollout_tick` first places held dependents whose dependency
exited: batch dependents whole, service and sidecar dependents only up to
their group's active step target, and not while `destroyFirst` still has old
allocations of them. A paused rollout releases and then returns without
running its steps. A batch task with exit-dependents is placed whole at every
step.

**Alternatives.** (a) Keep releases inside the step path (the old shape). (b)
Release everything regardless of step targets.

**Trade-off.** A pause no longer freezes the new version's batch DAG, which
someone may have relied on to stop the DAG. (b) would let a paused rollout
grow the new version's services past the step size.

**Why (simplicity).** A DAG step is work inside the new version, not part of
replacing the old one; gating it on replacement made a stuck rollout look like
a stuck DAG. Keeping services to the step size keeps the rollout's promise.

## 5. The scheduler reads finished batch replicas from its own lane

**Call.** `tasks_for_plan_from_store` overlays the outcome implied by
`complete`/`failed` allocation rows (scheduler lane) on the status-lane
`task_status` read, upgrading only `pending`/`running`.

**Alternatives.** (a) Queue the dependency eval from the status-lane apply on
the scheduler leader. (b) Stamp the eval with the status-lane index it must
wait for.

**Trade-off.** One more indexed read per plan, only for configs with
`dependsOn`, and two sources of the same fact. Scheduler benchmarks were not
re-run (the benches have no `dependsOn`, so the path is not exercised).

**Why (simplicity).** Raft log order already guarantees the rows are applied
before the eval; (a) and (b) add an apply hook or a wire field to get the
same guarantee.

## 6. An unreadable worker state record is stopped, not adopted or ignored

**Call.** The state file is parsed per record. A record this build cannot
read is stopped (cgroup leaf, then its process group when the start time
matches), and the file is kept as `worker_state.json.unreadable-<ts>`.

**Alternatives.** (a) Fail worker start until an operator fixes the file. (b)
Parse leniently and adopt what can be read.

**Trade-off.** An upgrade that changes the record format restarts those
tasks.

**Why (simplicity).** (a) turns every format change into an outage; (b) adopts
tasks with fields the worker does not understand. Stopping is what the manager
expects anyway: it places them again.

## 7. A worker under a new name warns about the old identity's tasks; it does not stop them

**Call.** At startup, live tasks recorded by another identity in the same data
dir are reported in a warning naming `--name <old>`.

**Alternatives.** Stop them, after taking an exclusive lock on each identity's
directory to prove no worker runs it.

**Trade-off.** Those tasks keep running unmanaged until an operator acts or
the manager's replacements collide with them.

**Why (simplicity).** Two workers on one data dir under different names is
legal, and a worker briefly restarting would lose its tasks to a sibling. A
lock is a new surface for a case the spot check did not hit (it used a fixed
name).

## 8. Guarded git is pinned; OCI and S3 are judged but not pinned

**Call.** Git (a subprocess) is pinned with `http.curloptResolve`, refuses
redirects, pins ssh by `HostName`, refuses `git://`, and skips LFS smudge.
OCI's registry and S3's effective endpoint are judged before the fetch only.

**Alternatives.** A worker-local guarded forward proxy for every artifact
client (CONNECT plus absolute-form HTTP), which would also cover redirects and
rebinding.

**Trade-off.** OCI and S3 clients still resolve again and follow redirects
unjudged; an S3 endpoint that passes the check can redirect to a guarded
address. Stated in docs/artifacts.md and open in TODO.

**Why (simplicity vs security).** The proxy is a new subsystem; the judged
check closes the direct cases now, and OCI's digest verification already
keeps a redirect's bytes out of the task directory.

## 9. A published restore prefers nodes whose `contentStore` holds its parts, falling back to all

**Call.** Workers advertise their store URL (`contentStore` attribute, never
`auth`); a published restore narrows to nodes whose store covers every part,
or keeps every candidate when none does.

**Alternatives.** Require such a node (a hard filter).

**Trade-off.** With no matching node eligible, the restore can still land on
a node that fetches anonymously and fails there.

**Why (simplicity).** A public store or a `file://` store shared by path would
be refused by a hard filter; the soft rule never makes a working restore
impossible.

## 10. The checkpoint's `network` is the allocation's mode; the vsock is recorded separately

**Call.** `SandboxCheckpointState.network` is now `gated` only for a gated
allocation; a new `vsock` field records the device, and the Firecracker
vsock-move refusal keys off it.

**Alternatives.** Rename the field.

**Trade-off.** Checkpoints taken before the change recorded every guest as
gated and are refused when restored into a task that is not gated.

**Why (simplicity).** The two meanings had different consumers; splitting them
keeps each check honest.

## 11. Old runc is avoided by an implicit constraint, not a readiness check

**Call.** A worker on runc < 1.2 advertises `container.idmappedMounts =
"unsupported"`, and user-namespaced container tasks gain
`container.idmappedMounts is_not_set` at submit.

**Alternatives.** A failing readiness key.

**Trade-off.** The constraint is visible in `job inspect`, and a node that
registered before the upgrade is not covered until it re-registers.

**Why (simplicity).** Readiness keys make up node health, so a failing key
would mark the node unhealthy for every other workload. The constraint uses
the mechanism `sandbox.bridges` already uses.
