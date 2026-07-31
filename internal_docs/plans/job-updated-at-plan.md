# Job mtime (`job.updated_at`)

> **Judgement calls:** [`../calls/job-updated-at-plan_calls.md`](../calls/job-updated-at-plan_calls.md)

**Status: SHIPPED.** Phases 1–3 landed together (one column, one proto field,
one UI column). Deliberately deferred: a job mtime on the *detail* page header,
and mtime for nodes (the Nodes index shows `last_heartbeat` under the same
"Updated" header, which is a different and defensible thing).

## 1. Problem statement

The Jobs index in the UI has an "Updated" column. It has never shown an update
time — `ui/src/pages/Jobs.tsx` passed `job.createdAt` into it, because
`job.updated_at` did not exist anywhere in the system.

Concretely, today:

- A job submitted in January, redeployed six times, and stopped yesterday reads
  **"Updated 7 months ago"**. Sorting by it sorts by submission order.
- There is no way to answer "which jobs changed this week?" from any surface —
  CLI, gRPC, UI, or a state-store query. The `job` table stores `id`, `name`,
  `status`, `created_at`, and `restart_requested_at`, and nothing else.
- `legion job list` prints `created_at` as the raw epoch integer
  (`created_at: 1785456000`), which is not a timestamp anyone reads at 3am.

Two smaller defects fall out of the same recon:

- `INSERT_JOB`'s `ON CONFLICT(id) DO UPDATE SET` included
  `created_at = :created_at`, so a submit that lost the race to another submit
  would reset the job's birth time.
- Nothing in the codebase distinguished "the job record changed" from "an
  allocation of the job changed", so any naive fix risks the opposite failure:
  every job in the cluster reading as freshly touched because a pod restarted.

## 2. Prior art

- **Kubernetes** has no `updatedAt`. It has `metadata.creationTimestamp`
  (immutable) and `metadata.generation`, which increments on every spec change,
  plus per-condition `lastTransitionTime` on status. `kubectl get` shows `AGE`,
  computed from the creation timestamp — K8s answers "what changed" through the
  watch/event stream, not a column. *Taken:* the split between an immutable
  creation time and a spec-change signal. *Rejected:* exposing a generation
  counter as the primary surface — a monotonic integer is not something a
  human scans a table for.
- **Nomad** stores `CreateIndex` / `ModifyIndex` (raft indices) on every object,
  plus `JobModifyIndex` and `SubmitTime` on jobs. `nomad job status` shows
  `Submit Date`. The indices are the real mtime and are used for blocking
  queries. *Taken:* a job carries a modify signal distinct from its create
  signal, and it moves on job changes, not on allocation churn (Nomad
  allocations carry their own `ModifyIndex`). *Rejected:* surfacing a raft index
  as the user-facing value — Legion's raft mode is opt-in, so an index is not
  universally meaningful, and it is not a time.
- **Consul KV** carries `CreateIndex`/`ModifyIndex` with the same shape.

The consensus across all three: creation time is immutable, the modify signal is
separate, and child-object churn does not touch the parent. That is exactly the
design below, with a wall-clock timestamp rather than an index because the
consumer is a human reading a table.

## 3. Jobs to be done, by persona

**Operator (UI).** "I want to see which jobs somebody touched recently, because
the cluster is misbehaving and a recent change is my first suspect." Today they
sort by an Updated column that is actually creation order, which actively
misleads: the job someone stopped ten minutes ago sits at the bottom.

**Operator (CLI).** "I want `job list` to tell me when each job last changed."
Today they get a column of epoch integers describing submission.

**Agent (MCP / gRPC).** "I want to poll for jobs that changed since my last
sweep." Today the only way is to diff whole `ListJobs` responses, or to read the
event stream, which is a different subsystem with different retention.

### User scenarios

1. An operator opens `/jobs` after an incident. The list is sorted newest-change
   first; the three jobs deployed during the incident window are at the top.
2. An operator runs `legion job list` and reads `created_at` and `updated_at`
   side by side as RFC3339 timestamps. A job whose two columns match has not
   been touched since submission.
3. An agent calls `ListJobs` with `sort_by: "updated_at"` and pages until it
   sees a job older than its watermark.

## 4. Goals and non-goals

**Goals**

- `Job` carries an `updated_at` that means "the job record last changed".
- It moves for spec submissions and status transitions, and *only* for those.
- Existing clusters migrate without a NULL, an epoch zero, or an operator step.
- The UI's "Updated" column shows it; `ListJobs` can sort by it.

**Non-goals**

- A watch/blocking-query API keyed on the mtime. The event stream already
  covers "tell me when this changes"; this is a value on a row.
- Backfilling a *true* mtime for pre-migration rows. The information does not
  exist; see §7.
- An mtime for every other entity. Rollouts, KV entries, templates, ACL
  policies, and manager configs already have one; instances and volumes have one;
  nodes deliberately do not (their recency signal is `last_heartbeat`).
- Sub-second resolution. The column is whole seconds, like `created_at`.

## 5. Background — what exists today

- `src/storage/migrations.rs:37` — the `job` table: `id`, `name`, `status`,
  `created_at`, later `+ restart_requested_at`.
- `src/storage/store.rs:5434` — `submit_job_config` writes `created_at` as
  `OffsetDateTime::now_utc().unix_timestamp()` (integer seconds), and only when
  the row is missing.
- `src/storage/store.rs` — five sites write a job's status:
  `stop_job_apply`, `mark_job_failed_apply`, `mark_job_failed_for_kv_apply`,
  `clear_job_failed_status_apply`, `clear_job_stopped_status_apply`. A sixth,
  `request_job_restart`, stamps `restart_requested_at`.
- `src/manager/server.rs:3339` — `list_jobs` sorts by `name`, `status`, or
  `created_at`.
- `src/structs/job.rs:22` — `JobPrint` renders `created_at` via `.to_string()`
  on the raw `i64`.
- `ui/src/components/SharedIndex.tsx:212` — every index page renders a fixed
  "Updated" column from `IndexItem.updatedAt`.
- `ui/src/pages/Jobs.tsx:67` — Jobs fed `job.createdAt` into it. This is the
  bug.

Raft note: mutations run through `Store::replicated`, which captures the
resulting *row values* and replicates those (`src/storage/raft/capture.rs`).
Calling `now()` inside an apply closure is therefore safe and already done —
`request_job_restart` does it.

## 6. Proposed design

### 6.1 One column, unix seconds

`job.updated_at INTEGER`, the same representation as `job.created_at`, so the
two are directly comparable and the proto field is `int64` like its sibling.

### 6.2 What moves it

The mtime describes **the job record**, not the workload underneath it:

| Event | Moves mtime |
| --- | --- |
| First submission | yes (equal to `created_at`) |
| New spec version submitted | yes |
| Byte-identical resubmit (`Unchanged`) | no |
| `job stop` → `stopped` | yes |
| Marked `failed` (incl. KV dependency failure) | yes |
| Recovered to `done` from `failed` / `stopped` | yes |
| Restart request acknowledged | yes |
| Restart marker cleared by the eval | no (internal bookkeeping) |
| Allocation placed, replaced, removed, health-checked | no |
| Node heartbeat, read, list | no |

Every status write stamps the mtime **in the same `UPDATE`** as the status, so
the two can never disagree and the mtime only moves on rows the write actually
matched. The two `clear_job_*_status` calls are guarded on the current status
and return whether they matched — a speculative clear that matches nothing must
not tick the mtime, or every healthy job in the cluster would read as freshly
touched on every recovery sweep.

### 6.3 Surfaces

```
$ legion job list
 name        status    created_at             updated_at
 web         done      2026-01-04T09:12:00Z   2026-07-29T18:44:07Z
 batch       stopped   2026-07-30T11:02:31Z   2026-07-30T11:02:31Z
```

```
$ legion job list -j
{"ok":true,"data":[{"id":"web","name":"web","status":"done",
  "createdAt":"1767517920","updatedAt":"1785782647"}]}
```

The human table gains `updated_at` and renders both columns as RFC3339. `--json`
is unchanged in shape — it serializes the proto message, which still carries
integer seconds.

UI: the Jobs index reads `job.updatedAt` for its "Updated" column, gains an
"Updated (newest)" sort option, and defaults to it — matching Rollouts,
Templates, and Manager, all of which already default to `updated_at:desc`.

## 7. Proto, struct, and persistence changes

- `proto/legion.proto`: `Job.updated_at` (field 5, `int64`);
  `ListJobsRequest.sort_by` documents `updated_at` and the new default.
- Migration: `ALTER TABLE job ADD COLUMN updated_at INTEGER;` followed by
  `UPDATE job SET updated_at = created_at WHERE updated_at IS NULL AND
  typeof(created_at) = 'integer';`.

  Pre-migration rows have no mtime to recover — the information was never
  written. Creation time is the only honest lower bound, and it reads as "not
  changed since submission", which is the truthful statement. The
  `typeof` guard keeps a text `created_at` (only the pre-rename migration
  fixtures write one) from poisoning an INTEGER column; `job_from_row` falls
  back to `created_at` for a NULL, so those rows still read sensibly.
- `JOB_PARAM_SET` (the upsert's `DO UPDATE SET`) drops `created_at`: an upsert
  landing on an existing row must never move the birth time.
- No job-config fields, so no `configs/jobs/maximal.jsonnet` change.

## 8. Implementation phases

1. **Storage.** Column, migration, backfill, `job_from_row`, and the six write
   sites. Shippable alone: `state store` queries and the raft snapshot carry the
   value immediately.
2. **API + CLI.** Proto field, `list_jobs` sort key and default, `JobPrint`.
3. **UI.** Jobs index column source, sort option, default sort.

## 9. Testing plan

Unit (`src/storage/store.rs` tests): a new job reports equal create/update
times; a new spec version moves the mtime but not `created_at`; an unchanged
resubmit moves neither; each of the four status transitions moves it; a no-op
status clear does not; allocation churn does not.

Unit (`src/storage/migrations.rs` tests): a job row written before the column
existed comes out of `to_latest` with `updated_at == created_at`, located by
probing for the newest pre-column version so appending migrations cannot turn
the test into a no-op.

Regression: the full suite, since the `job` table is read by the ACL endpoint
matrix, the raft seam suites, and the scheduler tests.

## 10. Rollout and backward compatibility

Additive on every axis. An old client reading a new manager ignores field 5; a
new client reading an old manager sees `0`, which renders as an epoch-zero
relative time — but version skew inside a cluster is already bounded by
`docs/upgrades.md`, and the manager is upgraded before its clients. The
migration is a column add plus one `UPDATE`, and needs no operator action.

The one visible behavior change is the **default sort of `ListJobs`**, from
`created_at:desc` to `updated_at:desc`. For a cluster that has never changed a
job the two orders are identical. An explicit `sort_by: "created_at"` restores
the old order, and the CLI never sent a sort key, so `job list` output order is
the only thing affected.

## 11. Open questions

None blocking. One lean recorded: whether the job detail page should show
"created / updated" in its header. It should, but that is a header-layout change
with its own design surface, and the index is where the wrong value was actually
being read.

## 12. Acceptance criteria

- The Jobs index "Updated" column changes when a job is stopped or resubmitted,
  and does not change when its allocations churn.
- `legion job list` prints readable timestamps in both columns.
- A database created before this change migrates with no NULL mtimes.
- `cargo test --all --locked` passes.
