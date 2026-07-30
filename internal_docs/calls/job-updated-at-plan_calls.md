# Judgement Calls — Job mtime (`job.updated_at`)

Plan: [`../plans/job-updated-at-plan.md`](../plans/job-updated-at-plan.md).

Entries are ranked by project impact, highest first; an entry's number is its
rank.

## 1. The mtime describes the job record, not the workload under it

**The call.** `job.updated_at` moves for spec submissions and job-status
transitions, and for nothing else. Allocation placement, replacement, removal,
health writes, task-status writes, and node heartbeats do not touch it.

**The alternatives.**

- *Anything about the job moves it* — treat a rescheduled allocation as a change
  to the job. This is the reading a user might expect from the word "updated",
  and it is what you get for free if you stamp the mtime from the eval loop.
- *Derive it* from `MAX(job_config.created_at)` — no column, no migration, and
  it is exactly right for "when was the spec last changed".

**The trade-off.** With the narrow definition, a job whose allocations are
thrashing looks untouched, and an operator hunting a crash-looping job cannot
find it by sorting the Updated column. They have to look at the Allocations
column or the job detail page — which is where that information already lives.

**Why.** The broad definition destroys the column's usefulness rather than
extending it. Allocations churn continuously in a healthy cluster: rescheduling,
health checks, and node loss all rewrite allocation rows on their own clocks, so
every job would sit at "updated seconds ago" permanently and the sort would be
noise. Nomad draws exactly this line — `JobModifyIndex` moves on job changes,
allocations carry their own `ModifyIndex` — and K8s draws it harder still, with
`metadata.generation` incrementing only on spec writes. The derived-from-configs
alternative is genuinely attractive (it needs no migration) but it cannot see a
`job stop`, which is the single most common thing an operator does to a job and
the case that made the original column visibly wrong.

The user-facing consequence, stated in `docs/jobs-tasks-and-scheduling.md`: a
job's Updated time answers "when did someone last change this job", not "is this
job healthy right now".

## 2. A real column, not a relabelled "Created" header

**The call.** Add `job.updated_at` — a migration, a proto field, and six write
sites — rather than renaming the UI column to "Created", which is a two-line fix
that makes the displayed value honest.

**The alternatives.**

- *Rename the column to "Created" on the Jobs page.* The value shown becomes
  correct immediately, with no migration, no wire change, and no risk.
- *Rename it and make `SharedIndex`'s last column configurable*, so each index
  page labels its own recency signal (Jobs: Created, Nodes: Last heartbeat,
  Rollouts: Updated).

**The trade-off.** The column costs a schema migration on every existing
cluster, a wire field that can never be removed, and six write sites that a
future job-mutating code path has to remember to touch. The rename costs
nothing. And the rename would have been *sufficient* for the literal complaint —
the value shown would no longer be mislabelled.

**Why.** Simplicity is Legion's headline goal, and the rename optimizes the
wrong thing: it makes the UI honest about not having the information anyone
actually wants. "Which jobs changed recently" is a first-class operator
question — it is the first thing looked at during an incident — and Legion had
no way to answer it from any surface: not the CLI, not gRPC, not a state-store
query. Every comparable entity in the schema already carries an mtime (rollouts,
KV entries, templates, ACL policies, manager configs, instances, volumes); the
job, the most important object in the system, was the exception. Renaming the
column would have closed the bug report and left the gap.

The second alternative — per-page column labels — is still worth doing for the
Nodes page, where "Updated" really means "last heartbeat". It is not in this
change; noted in the plan's deferred list.

## 3. The status write and the mtime write are one statement

**The call.** Each of the five status transitions stamps `updated_at` inside the
same `UPDATE`, e.g. `SET status = 'stopped', updated_at = ?2 WHERE name = ?1`,
rather than calling a separate `touch_job(job_id)` helper afterwards.

**The alternatives.** A `Store::touch_job(conn, job_id)` helper called after each
status write. It reads better, keeps the intent named, and gives one place to
change the definition of "touched".

**The trade-off.** The SQL is repeated six times, and a future write site can
forget the clause. A helper would have been self-documenting at each call site.

**Why.** Two of the writes are conditional — `clear_job_failed_status` and
`clear_job_stopped_status` carry `AND status = 'failed'` / `AND status =
'stopped'` and return whether they matched a row. A separate `touch_job` runs
unconditionally, so a speculative clear that matched nothing would still tick
the mtime. These clears are called on every KV-triggered recovery and every
`RunJob`, so the bug would have shown up as *every job in the cluster* reading
as freshly updated after any recovery sweep — the exact failure mode entry 1
exists to prevent. Folding the stamp into the same statement makes "only rows
this write actually changed" a property of the SQL rather than a rule to
remember. `a_no_op_status_clear_leaves_updated_at_alone` pins it.

## 4. Pre-migration rows backfill to their creation time

**The call.** `UPDATE job SET updated_at = created_at` for every existing row,
so a job that predates the column reads as "not changed since submission".

**The alternatives.**

- *Leave it NULL* and render "—" in the UI for pre-migration jobs, which is
  literally true: we do not know when they last changed.
- *Backfill from `MAX(job_config.created_at)`*, which recovers the real
  spec-change time for jobs that were resubmitted before the migration.

**The trade-off.** The backfill asserts something we did not observe. A job
stopped last week, on a cluster upgraded today, will claim it was last updated
on the day it was submitted — understating its mtime until the next thing that
touches it.

**Why.** Understating is the safe direction: creation time is a true lower bound
on the last change, and "updated = created" is the same statement the UI makes
about a brand-new job, so it needs no special rendering and no user explanation.
NULL would have forced an `Option<i64>` through the proto (or a sentinel zero
rendering as "56 years ago", which is worse than any of these) and a "—" state
in a column that is otherwise always populated — API complexity every reader
pays forever, for a condition that exists once per cluster and ages out on the
next write. The `job_config` backfill was rejected on the same ground as the
derived alternative in entry 1: it recovers spec changes but not stops, so it
would produce a mix of accurate and understated values with no way to tell them
apart — worse than uniformly understated.

## 5. `ListJobs` now defaults to `updated_at:desc`

**The call.** Change the default sort from `created_at:desc` to
`updated_at:desc`, and make the Jobs page default match.

**The alternatives.** Keep `created_at:desc` as the default and offer
`updated_at` only as an explicit sort key. Changing a default is a
compatibility event that this bug fix did not have to trigger.

**The trade-off.** A caller relying on the previous ordering gets a different
page of results. `ListJobs` is paginated, so this is not purely cosmetic — an
agent paging through jobs sees a different sequence.

**Why.** The sort key should match the column the user is looking at, and the
visible column is "Updated". Every other index page in the UI already defaults
to `updated_at:desc` (Rollouts, Templates, Manager), so Jobs was the outlier in
both directions — wrong value *and* wrong sort. For any cluster that has never
changed a job the two orderings are identical, and `sort_by: "created_at"`
restores the old order exactly. The CLI never sent a sort key, so `job list`
ordering is the only unversioned surface affected.

## 6. The CLI table renders timestamps, `--json` keeps integers

**The call.** `JobPrint` formats `created_at` and `updated_at` as RFC3339 for
the human table. The `--json` envelope is untouched: it serializes the proto
message, which still carries unix seconds.

**The alternatives.** Render both as relative times ("3 hours ago"), matching
the UI. Or leave the table as raw epoch integers and change nothing beyond
adding the column.

**The trade-off.** RFC3339 is wider than an epoch integer and much wider than a
relative time, on a table that already carries a name and a status. And the
human table now disagrees in *format* with `--json` for the same field.

**Why.** `created_at: 1785456000` is not a timestamp a human can read, which
made this a latent second half of the same bug — the CLI's "when" column was as
unusable as the UI's. Agent-facing output is the JSON envelope and must stay
machine-parseable and stable, so the integer stays there; the table is for
people, so it gets the readable form. Relative times were rejected for the
table because CLI output gets pasted into incident notes and tickets, where "3
hours ago" loses its referent — the UI, which is live and re-renders, is the
right place for relative time.

## 7. `created_at` dropped from the upsert's `DO UPDATE SET`

**The call.** `JOB_PARAM_SET` — the `ON CONFLICT(id) DO UPDATE SET` clause of
`INSERT_JOB` — no longer includes `created_at = :created_at`.

**The alternatives.** Leave it. The insert is only reached after a `GET_JOB`
returned no rows, so the conflict branch is nearly unreachable in practice, and
touching it widens the diff of an already cross-cutting change.

**The trade-off.** None worth the name; this is a strictly narrower write.

**Why.** "Nearly unreachable" is doing real work in that sentence:
`submit_job_config` has a documented race between the existence check and the
insert (`src/storage/store.rs`, "There is a race condition here with the
version"), so two concurrent submits of a new job can land on the conflict
branch — and the losing one would silently reset the job's birth time. Since
this change is what makes `created_at` load-bearing (it is now the thing that
"immutable" is claimed about, and the backfill's floor), leaving a path that
rewrites it would undercut the guarantee the proto comment now makes.
