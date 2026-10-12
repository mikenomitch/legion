# Job status derived from the workload

> **Judgement calls:** [`../calls/job-status-plan_calls.md`](../calls/job-status-plan_calls.md)

**Status: SHIPPED.** Phases 1 and 2 landed together in
[mikenomitch/legion#21](https://github.com/mikenomitch/legion/pull/21).
Deliberately deferred: per-replica outcomes (a task's outcome is still the
last exit any of its allocations reported), and an event when a job's
derived status changes (the status is computed on read, so there is no
transition to emit).

## 1. Problem statement

`legion job list` showed `done` for every job from the moment it was
submitted. A long-running service read `done` while serving traffic. A batch
job read `done` while it was still running, and it read `done` when every
task had failed to place. The UI's job page papered over this by deriving
`running` client-side, but the Jobs index, the CLI, MCP, and `GetJobInfo`
all showed the stored word.

The cause: `job.status` is a lifecycle flag. It is written on submit
(`done`), stop (`stopped`), resubmit, and KV dependency failure/recovery
(`failed`), and never on allocation changes. No single stored word can
describe a workload whose state lives in allocations and task outcomes.

## 2. Prior art

- **Nomad** reports a job as `pending`, `running`, or `dead`, computed from
  its allocations. A finished batch job and a stopped service both read
  `dead`. Periodic (cron) parents stay `running`. We take "computed from
  allocations" and reject `dead`, which merges success, failure, and stop
  into one word an on-call responder then has to unpick.
- **Kubernetes** has no single job status: Deployments carry conditions
  (`Available`, `Progressing`), Jobs carry `succeeded`/`failed` counts and a
  `Complete`/`Failed` condition, CronJobs carry `lastScheduleTime`. We take
  the distinction between complete and failed and the idea that a cron job
  between runs is its own state, and reject the per-kind surfaces: Legion
  has one job type, so it gets one status field.

## 3. Jobs to be done

- **Job author** — "I ran my service; tell me it is up. I ran my batch job;
  tell me it finished, and whether it worked."
- **On-call responder** — "Show me which jobs are not doing their work
  (`failed`, `pending`) without opening each one."
- **Autonomous agent** — "After `job run`, poll one field until it is
  `running` or `complete`, and stop on `failed`."
- **Cluster operator** — "A job I scaled to zero or parked should not look
  broken."

Scenarios: a service goes `pending` → `running`; a nightly batch goes
`pending` → `running` → `complete`; a cron job reads `scheduled` between runs;
a migration that exits non-zero leaves its job `failed` even though the API
task that depends on it is still waiting.

## 4. Goals and non-goals

Goals: one status field on `Job` that says what the workload is doing, the
same on every surface; a precedence order that surfaces failures before
waits; no new write path.

Non-goals: per-replica outcomes; health (a running but unhealthy service
reads `running` — health has its own surfaces); status-change events; a
persisted derived column.

## 5. What exists today

- `job.status` column, written by `handle_job_submit_tx`, `stop_job_apply`,
  `mark_job_failed_for_kv_apply`, `clear_job_failed_status_apply`,
  `clear_job_stopped_status_apply` (`src/storage/store.rs`).
- Allocation statuses `placed` / `complete` / `failed` / `failed:<reason>` /
  `suspended` (`src/constants.rs`). Terminal allocations keep their row.
- `task_status`: one row per task of each job config, `pending` → `running`
  → `success`/`complete`/`failed`, maintained on placement and on worker exit
  reports (`complete_allocation_apply`). DAG `dependsOn` conditions read it.
- Keyed instances in `instance` (`pending`, `active`, `rescheduling`,
  `stopped`, `suspended`, `terminated`).
- Internal gates read the column: stopped jobs reject instance RPCs
  (`server.rs`, `fork_rpcs.rs`, `snapshot_rpcs.rs`); the reconcilers skip
  `stopped`/`failed` jobs.

## 6. Design

### 6.1 Two values, one on the wire

The column stays the lifecycle flag, renamed `done` → `active`. Every
internal gate keeps reading it. `GetJobInfo` and `ListJobs` replace
`Job.status` with a derived value before replying.

### 6.2 The derivation

Inputs per job: live allocations (status not `complete`, `failed`, or
`suspended`) across every config version; for each task of the latest
version, whether it wants work (`count.num > 0`, or for a keyed task at least
one pending/active/rescheduling instance), whether it has a `schedule`, and
its `task_status`; the number of suspended instances. First match wins:

| Status | When |
| --- | --- |
| `stopped` | lifecycle `stopped` |
| `failed` | lifecycle `failed` (KV) |
| `running` | any live allocation |
| `failed` | a wanted task's outcome is `failed` |
| `pending` | a wanted task has no successful outcome yet |
| `scheduled` | a wanted cron task finished its run |
| `complete` | every wanted task finished successfully |
| `suspended` | nothing wanted; a keyed instance is suspended |
| `idle` | nothing wanted |

### 6.3 Surface

```console
$ legion job list
NAME      STATUS     PRIORITY
web       running    normal
nightly   complete   normal
$ legion job list --status failed
```

`ListJobsRequest.status`, `status_counts`, the free-text query, and the
`status` sort all apply to the derived value. The UI drops its client-side
`running` workaround, its chips list the new values, and its Stop button
stays available on `failed`/`complete` jobs.

## 7. Persistence and proto changes

No migration and no new column. `Job.status`'s proto comment documents the
derived values. The stored `done` becomes `active`; existing rows keep `done`
until their next lifecycle write, which the derivation treats like `active`.

## 8. Phases

1. Rename the stored `done` to `active` (ships alone: no surface reads
   `done` as "finished" any more).
2. Derive `Job.status` in `GetJobInfo`/`ListJobs`; update CLI help, MCP,
   UI, docs.

## 9. Testing

Unit tests for every row of the precedence table
(`src/storage/store/job_status_ops.rs`); a store test that drives a service
and a batch job through placement and exit and checks both the single and
the list read; the existing `ListJobs` filter test asserts unplaced jobs
count as `pending`.

## 10. Rollout

Breaking on the wire: clients that filtered on `done` or `active` must use
the derived values. No installed base, so no compatibility path.

## 11. Acceptance criteria

`legion job list` shows `running` for a placed service and `complete` for a
batch job whose task exited 0; `--status pending` lists unplaced jobs; the UI
Jobs index and job page agree.
