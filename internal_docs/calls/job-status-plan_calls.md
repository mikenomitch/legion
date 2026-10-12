# Job status plan — judgement calls

Calls made for [job-status-plan.md](../plans/job-status-plan.md): renaming
the stored `done` and deriving `Job.status` from the workload. Entries are
ranked by project impact, highest first; the number is the rank.

## 1. `Job.status` on the wire is derived on read; the column stays a lifecycle flag

**The call.** `GetJobInfo` and `ListJobs` compute the status from live
allocations, latest-version task outcomes, and instance states each time
they answer. The `job.status` column keeps the lifecycle flag (`active`,
`stopped`, `failed`) and every internal gate keeps reading it.

```console
$ legion job list                       $ legion state query "SELECT id, status FROM job"
web      running                        web      active
nightly  complete                       nightly  active
```

**Alternatives.** Persist a derived column and update it on every allocation
and task-status write. Or keep `status` as the flag and add a separate
`state` field beside it.

**Trade-off.** `ListJobs` does three aggregate reads per call, over
allocations, latest-version tasks, and instances, instead of one read of the
job table. And `legion state query` on `job.status` shows a different word
than `legion job list`, which the docs have to explain.

**Why.** A persisted column would put a write on the allocation and
task-status paths, the hottest writes in the scheduler (performance), and
would need to stay correct across lanes and reschedules. A second field
gives users two status words to choose between (simplicity). Deriving on
read touches no write path and gives one field that means what it says.
The aggregate reads are one pass each, which is the same order of work the
UI's Jobs page already did client-side by fetching the whole ledger.

## 2. A failed task beats a waiting one

**The call.** With nothing live, any wanted task whose outcome is `failed`
makes the job `failed`, even if other tasks are still `pending`.

**Alternatives.** `pending` first, on the grounds that work may still run.

**Trade-off.** A job whose service task is briefly `failed` between a crash
and its reschedule reads `failed` for that window rather than `pending`.

**Why.** The common shape is a DAG: `migrate` failed and `api` waits on
`migrate` succeeding. That job will never make progress, and `pending`
would hide the one fact the on-call responder needs. A transient `failed`
during a reschedule is honest about what just happened.

## 3. Eight words, not Nomad's three

**The call.** The derived values are `running`, `pending`, `failed`,
`scheduled`, `complete`, `suspended`, `idle`, `stopped`.

**Alternatives.** Nomad's `pending` / `running` / `dead`, or fold `scheduled`
into `complete` and `idle`/`suspended` into `pending`.

**Trade-off.** More values for clients, filters, and the UI's chips to know.

**Why.** Each merge makes one case lie: `dead` hides whether a batch job
worked; `complete` on a cron job says it will not run again; `pending` on a
job scaled to zero says something is wrong when the operator asked for
nothing. The words are self-explanatory, so the extra values cost little to
read (simplicity for the reader over brevity of the enum).

## 4. A task's outcome is its `task_status` row, not a per-replica count

**The call.** Terminal outcomes come from `task_status`, the same per-task
row DAG `dependsOn` conditions read.

**Alternatives.** Derive outcomes from allocation rows: count complete and
failed replicas per task and require all `count.num` to succeed.

**Trade-off.** In a `count = 3` batch task where one replica failed and the
last to exit succeeded, the job reads `complete`. Documented as a known
limitation.

**Why.** It keeps the job status and the dependency engine agreeing on what
a task did. Per-replica accounting has to handle rescheduled replacements,
retained terminal rows from earlier attempts, and `count.per` expansion,
which is a scheduler-sized change for a corner case (simplicity).

## 5. The stored flag is `active`, not `running`

**The call.** A submitted, unstopped job is stored as `active` (was `done`).

**Alternatives.** Store `running`, the word the request suggested.

**Trade-off.** None visible on the wire after entry 1; the word only shows in
state-store queries.

**Why.** The column changes only on submit, stop, resubmit, and KV
failure/recovery. `running` would be wrong for a finished batch job and for
one that never placed. `active` is true for every row it is written on.

## 6. The UI's Stop button stays on `failed` and `complete` jobs

**The call.** The job page hides **Stop job** only once the job is
`stopped`.

**Alternatives.** Keep hiding it on `failed`, as before.

**Trade-off.** Stop is offered on a job with nothing live, where it changes
only the lifecycle flag.

**Why.** `failed` now usually means "a task failed", and destroying the job
needs it stopped first. Hiding Stop would leave no path from the UI to
cleaning the job up.
