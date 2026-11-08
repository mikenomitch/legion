# Launch blockers and security gaps (2026-11-08)

> **Judgement calls:** [launch-blockers-and-security-gaps-plan_calls.md](../calls/launch-blockers-and-security-gaps-plan_calls.md)

**Status: SHIPPED** in [#67](https://github.com/mikenomitch/legion/pull/67), except what §9
names as deferred. Nothing here has run on the AWS rig yet; every fix is
covered by unit or integration tests only.

This plan is written after the fact, for one batch of fixes picked from
[TODO.md](../TODO.md): the correctness bugs still open from the 2026-10-11/12
spot checks ("Tier 1") and four security gaps ("Tier 3"). Each fix is small on
its own, but several change a default, a persisted format, or a security
boundary, which is why they get a plan and a calls record.

## 1. Problem

Tier 1: things that were broken on a running cluster.

1. **A DAG stalls in the `lanes` topology.** The dependency eval a batch exit
   queues can run on the scheduler leader before it applies the status-lane
   write that says the task finished. It reads `running`, holds the
   dependents, and nothing evaluates the job again.
2. **A rollout freezes the new version's DAG.** A dependency release under a
   rollout is a `rollout_tick`, and the tick runs the step gates (manual
   advance, step stagger, a paused group, the batch retire window) before it
   places anything. With fractional steps a batch task with dependents was
   placed a fraction at a time and could never succeed, so the step waited
   forever.
3. **A sidecar is retired out from under a running batch step.** The batch
   retire window kept a running old batch step and retired the sidecar beside
   it (the spot check's missing `progress`).
4. **Tasks are orphaned after a worker restart.** The worker state file was
   parsed in one piece. One record a new build could not read failed the whole
   restore; the worker forgot every running task, overwrote the file, and the
   manager started a second copy of each (P1-5's most likely root cause).
5. **A published restore lands on a node that cannot read it**, and **the
   gated-vs-TAP restore refusal never fires** because every microVM guest has
   a vsock and the checkpoint recorded the device, not the network mode.
6. **A finished batch step runs twice** when the worker restarts between the
   exit and the heartbeat that reports it: the outcome lived only in memory.
   And `restart.policy = "never"` still rescheduled a failed task.
7. **`moveCount` fails a whole keyed job** when a few unrelated sandboxes
   fail.

Tier 3: places where a job could reach what the host guard or a credential
should have kept from it.

8. **Git, OCI, and S3 artifact fetches** were not judged by the host guard
   (S3 only for a query `endpoint`, not one from the credential).
9. **`legion exec` into a gated `exec` task** ran in the host's network
   namespace.
10. **The node image cache served private images** to any job on the node,
    by digest with no request, or by tag after the registry refused the job's
    credential.
11. **A runc older than 1.2** passed `container: pass` readiness, so the
    scheduler placed user-namespaced containers on it, and every start failed.

## 2. Prior art

- **Kubernetes `AlwaysPullImages`** forces a pull (and so a credential check)
  for every pod; containerd's CRI checks pull credentials per image but shares
  layers by digest. We take the credential check without the pull: one
  manifest `HEAD` by digest, remembered for 10 minutes per credential.
- **Nomad** counts `reschedule.attempts` per allocation lineage, not per job;
  K8s Jobs count `backoffLimit` per Job but StatefulSet/sandbox-style
  workloads are per pod. Keyed instances are per-user units, closer to a pod
  than a Job.
- **Kubernetes Jobs** have `restartPolicy: Never`: a failed pod stays failed
  (the Job controller may create another up to `backoffLimit`). Our `never`
  is per task, so it now means what the name says.
- **curl `CURLOPT_RESOLVE`** (git `http.curloptResolve`, git 2.37+) is the
  standard way to pin a hostname to addresses without editing `/etc/hosts`.

## 3. Jobs to be done

- **Operator:** "After I upgrade a worker, its running tasks keep running and
  are not started twice." "A worker with an old runc does not swallow every
  container placement." "A job cannot read my cloud metadata or my node's
  services through an artifact URL."
- **Tenant / job author:** "My DAG keeps moving during a rollout I paused."
  "One user's broken sandbox does not fail every other user's." "Another
  tenant cannot run my private image because it is cached on a shared node."
- **Agent operating the cluster:** "A failed step with `restart = never`
  ends the job's DAG branch predictably instead of looping."

## 4. Goals and non-goals

Goals: fix each item above, with a regression test that fails without the
fix; keep the cache and placement fast paths fast.

Non-goals: a guarded forward proxy for every artifact client (§9); a
per-identity worker lock; re-running the AWS spot check (tracked in TODO).

## 5. Design, per item

1. **Lanes stall.** `Scheduler::tasks_for_plan_from_store` overlays
   `Store::batch_outcomes_for_job_config` (finished replicas' scheduler-lane
   rows) onto the status-lane read, upgrading only `pending`/`running`.
2. **Rollout freeze.** `release_dependency_holds` runs first in every
   `rollout_tick`: a batch dependent is placed whole, a service dependent up
   to its group's active step target (never while `destroyFirst` still has
   old allocations of it). A paused rollout releases and returns without
   running its steps. `build_step_targets` places a batch task with
   exit-dependents whole at every step.
3. **Sidecar.** `retire_old_allocations_for_group` keeps a sidecar on any node
   where the window holds a running old batch step of its group.
4. **Orphans.** `WorkerStateStore::load_records` parses record by record; an
   unreadable record yields its pid, start time, and resource assignment,
   which `restore_allocations` uses to stop it (cgroup leaf, then the process
   group when the start time matches). The file is kept as
   `worker_state.json.unreadable-<ts>`. Dead and recycled records also kill
   their cgroup leaf. A worker under a new `--name` warns about another
   identity's live tasks.
5. **Restore placement.** Workers advertise `contentStore` (URL only). A
   published restore narrows to nodes whose store covers every part URL,
   falling back to all candidates. The checkpoint records `network` from the
   allocation's mode and the vsock as a separate `vsock` field.
6. **Batch outcomes.** `PersistedAllocation.terminal` holds the outcome and
   whether a manager recorded it; restore keeps the allocation terminated and
   re-sends an unrecorded outcome. `ExitDecision::TerminalFailure` routes a
   `never` task's failed exit through `TerminatedAllocation.success = false`.
7. **Move budget.** `remove_failed_allocation_apply` counts a keyed instance's
   own holds; spending them stops just that instance
   (`MoveVerdict::InstanceStopped`). `instance start` clears its move history.
8. **Artifact fetches.** Git: judged and pinned (`http.curloptResolve`,
   `http.followRedirects=false`, ssh `HostName`/`HostKeyAlias`, `git://`
   refused, LFS smudge off). OCI: the registry is judged before any request.
   S3: the credential's endpoint is judged too, and regions are validated.
9. **Exec netns.** `ExecOptions.netns`; sessions spawn on a thread that
   `setns`es first. A missing namespace fails the session.
10. **Image cache.** `image_source(digest, repository, anonymous)` in the node
    content store, written for every blob of an image pulled and verified
    from a repository. A pull is served from the cache only for that
    repository and only when public or the credential is accepted (grant or
    `HEAD`); unauthorized pulls take manifests from the registry and reuse
    only blobs verified from the same repository.
11. **Old runc.** Workers advertise `container.idmappedMounts = "unsupported"`;
    the container runtime's `normalize_task` adds
    `container.idmappedMounts is_not_set` to user-namespaced tasks.

## 6. Format and surface changes

- Worker state file: `terminal` on a record; load is per record.
- Node content store: new `image_source` table.
- Checkpoint `runtime_state`: new `vsock`; `network` now means the mode.
- Node attributes: `contentStore`, `container.idmappedMounts` (structural).
- Event `allocation.reschedule_exhausted` gains `instanceStopped`.
- `TerminatedAllocation.success = false` is now sent.
- No proto shape changes; the `success` field's comment now says when it is
  false.

## 7. Testing

Each fix has a regression test, listed in
[#67](https://github.com/mikenomitch/legion/pull/67). The lanes overlay's
test was confirmed to fail with the fix reverted; the others were written
against the behavior before the fix, but not re-checked by reverting.

## 8. Rollout

No installed base, so no shims. What breaks, on purpose:

- `restart.policy = "never"` no longer reschedules a failed task.
- A keyed job with `moveCount` no longer fails when one instance spends its
  budget.
- Memory checkpoints taken before this change recorded every microVM guest
  as gated; one cannot be restored into a task that is not gated.
- Cached images without provenance are pulled from their registry once more.
- A guarded worker refuses `git::git://` sources and fetches LFS pointers
  only; guarded `http(s)` git needs git 2.37+.

## 9. Deferred

- OCI and S3 clients still resolve hosts again and follow redirects
  unjudged; closing that needs a guarded forward proxy (CONNECT plus
  absolute-form HTTP) or object_store 0.12's connector hook, which is tied to
  rustyriver's object_store version.
- Manager-side `resolve = "pin"` lookups are unjudged.
- A worker re-joining under a new name only warns about the old identity's
  tasks.
- A pushed gated task's datapath listeners still bind at the next heartbeat.
- Everything above needs a re-run on the AWS rig.
