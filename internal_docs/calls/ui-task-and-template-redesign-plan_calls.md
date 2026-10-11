# Judgement calls: UI task page tabs, guided template launch, actions panel

Plan: [`../plans/ui-task-and-template-redesign-plan.md`](../plans/ui-task-and-template-redesign-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. The template Review step diffs rendered specs in the browser; no plan RPC

**Call.** Review renders the template (`RenderJobTemplate`), fetches the
target job's current submissions (`GetJobSpec`), and shows a unified line diff
of the job's recorded `job.rendered.json` against the new render, both printed
with keys sorted at every depth. The diff is a small LCS in
`ui/src/utils/diff.ts`, skipped above 20,000 lines or 2.5M comparison cells.

```
Updates the running job preview-feat-login — launched from preview-env v11.
Launching replaces its config and starts a rollout.

Jobspec changes                                  +3 −2 lines
@@ -14,7 +14,8 @@
-      "count": 1,
+      "count": 2,
```

**Alternatives.** A `PlanJobTemplate` RPC — the template analogue of
`legion job plan` — that runs the scheduler's dry run and reports placements,
replacements and failures, not just text changes.

**Trade-off.** The Review says *what the spec becomes*, not *what the
scheduler will do about it*: it cannot say "this replaces 2 allocations" or
"this will not fit". A user who needs that still runs `legion job plan`.

**Why (simplicity).** The plan's goal was no new RPCs for these phases, and
the spec diff alone answers the question that bites people — "am I replacing
someone's running job, and with what?" — using records every template run
already writes. The RPC is listed under Deferred, not rejected.

## 2. Task-page tabs live in query parameters, not nested routes

**Call.** `/jobs/web/tasks/api?tab=logs&alloc=4219`. `tab` is
`overview|logs|exec|spec`, default `overview`; an unknown value falls back to
Overview. Changes replace the history entry. Tab clicks keep `alloc`; the
header's Logs / Open shell buttons clear it.

**Alternatives.** Nested routes (`/jobs/web/tasks/api/logs`), which read
better and match Nomad's allocation URLs.

**Trade-off.** Uglier URLs, and the back button does not step through tabs.

**Why (simplicity).** One route and one component keep the page's single
polled resource shared by all four tabs (nested routes would either refetch
per tab or need a layout route holding the data), and every old task link
keeps working unchanged. Replacing rather than pushing history is deliberate:
flipping between Logs and Overview while debugging should not take five back
presses to leave.

## 3. The spec is found by name (`job.rendered.json`), not by position

**Call.** `extractJobSpecData` now prefers the `job.rendered.json` submission
and otherwise takes the first JSON submission that has a `tasks` array. JSON
without `tasks` is not a spec.

**Alternatives.** Add `ORDER BY` to `Store::get_submissions_for_config` so the
first JSON submission is reliably the spec.

**Trade-off.** None found in the UI; the server query still returns rows in
no defined order, so any other consumer that takes "the first JSON
submission" has the same latent bug.

**Why.** Found while building the Review step: a template run records
`template://…` (Nickel), then `template.variables.json`, then
`job.rendered.json`, and the query has no `ORDER BY`. The old code returned the
variables file as the spec — `{ tasks: [] }` — so a template-launched job's
page listed no tasks and its task pages said "Task not present in the current
job spec". Matching by name fixes it for any row order, without a server
change.

## 4. "Failures · 24h" counts `allocation.failed` events, matched by task config

**Call.** The Overview tile counts `allocation.failed` events in the last 24h
whose metadata names one of this task's `task_config` ids (read from the state
store, plus the ids of visible allocations). Recent events use the same
matching. The fetch is the job's last 200 events; when that window does not
reach back a day the tile shows `N+`. Without `operator:read` (which
`ListEvents` requires) the tile shows "—" and the events card says events are
unavailable.

**Alternatives.** (a) A restart counter per allocation, as Kubernetes shows —
there is none in the API. (b) Matching events by allocation id.

**Trade-off.** A floor, not an exact count, on a busy job; a state-store query
on every poll.

**Why.** (b) always counts zero: the manager deletes an allocation before
emitting its `allocation.failed` event, so a failed allocation's id is never in
any list the page has. The task config id survives in the event metadata. A
real restart counter is deferred in the plan.

## 5. The Exec tab keeps today's panel, with one target picker, until phase 3

**Call.** The Exec & actions tab shows a single target `<select>` (default:
`?alloc`, else the current selection, else the first unhealthy allocation,
else the first) and one `AllocationExecPanel`, opened, keyed on the allocation
id. The tab stays mounted after its first visit, so a shell survives a look at
the logs.

**Alternatives.** Build one of the five actions layouts now.

**Trade-off.** The tab carries the old panel's design for a while.

**Why.** The layout and the tab's name are still being chosen; phase 1 ships
the structural win (one panel instead of one per allocation) without
committing to either. Keying the panel on the id also fixes an old bug: the
previous page rebuilt every exec panel on each 5s poll, killing any open
terminal.

## 6. A launch's outcome is read from the reply message

**Call.** After `RunJobTemplate`, the Launch step decides between "follow the
rollout", "new job — no rollout", and "unchanged" by matching the reply's
first line against `/rollout/i` and `/unchanged/i`. The rollout to follow is
the one whose `toJobConfigId` equals the reply's `jobConfigId` (newest 5 by
`created_at`), not simply the newest.

**Alternatives.** Add a structured outcome field to `RunJobTemplateReply`.

**Trade-off.** Rewording the manager's message breaks the branch silently (it
degrades to a neutral note, not an error).

**Why.** No proto change in this phase. The proto field is the right fix and
is a small follow-up; matching on `jobConfigId` keeps the rollout choice
correct even when someone else's rollout is newer.

## 7. "was …" badges only when the job's current config came from this template

**Call.** The Review's values table compares with the job's recorded
`template.variables.json` only when `GetJobInfo.template.name` is this
template.

**Alternatives.** Compare whenever a variables file exists.

**Trade-off.** A job last launched from another template shows values without
history.

**Why.** Another template's variables can share a key and mean something
else; a "was 1" next to an unrelated `replicas` would mislead exactly the
cautious updater the step exists for.

## 8. Template-scoped tokens get "can't tell", not "creates"

**Call.** When `GetJobInfo` is denied, Review says the token cannot tell
whether the job exists, and shows the full rendered spec.

**Alternatives.** Treat a denial as "new job".

**Trade-off.** A token that may only launch through a template never sees
"Creates a new job" — `GetJobInfo` carries no template attribute, so it is
denied even for a job that does not exist. It also cannot follow the rollout.

**Why.** A denial is not evidence of absence; guessing "new" is the one
answer that could hide an overwrite.

## 9. Source moves to its own tab on the template page

**Call.** The template page has **Launch** and **Source** tabs; the source,
version list and publish editor live under Source, and the header's New
version button switches to it.

**Alternatives.** Keep the source beside the form, as before.

**Trade-off.** Authors switch tabs to read the source while filling the form.

**Why.** The launch flow is for consumers, who rarely read Nickel; the source
beside the form was the largest thing on the page and the least used.
