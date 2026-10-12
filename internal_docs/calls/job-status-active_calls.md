# Job status `active` — judgement calls

Calls made renaming the job's stored lifecycle status from `done`. No plan: it
is a relabel of an existing value, not a new surface. Entries are ranked by
project impact, highest first; the number is the rank.

## 1. The stored job status is `active`, not `running`

**The call.** A submitted, unstopped job is stored as `active`. Before and after
in `legion job list`:

```
NAME   STATUS          NAME   STATUS
web    done      →     web    active
```

**Alternatives.** `running`, which is the word the request suggested and what
the job page already derives when allocations are live. Or derive the status
on the server from allocations (`pending` / `running` / `complete`), the way
Nomad reports a job as `pending`, `running`, or `dead`.

**Trade-off.** `active` is less specific than `running`. An operator looking
at a healthy service sees `active` in the index, not `running`, and the job
page and the index still use different words for the same job (the open item
from [suspend-resume-ui calls 9](suspend-resume-ui-plan_calls.md#9-a-done-job-with-live-work-reads-running-derived-in-the-ui)).

**Why.** The field is a lifecycle flag. It changes only on submit, stop,
resubmit, and KV failure/recovery, never on allocation changes. Any word that
describes the workload's progress will be wrong for some jobs. `running` is
wrong for a batch-only job whose tasks all exited, and for a job whose tasks
all failed to place: the inverse of the `done`-on-a-service bug this fixes.
`active` is true for every job it is written on (simplicity: the label means
exactly what the code does). A derived status is the better end state, but it
puts an allocation aggregation on `ListJobs` and changes the status filter and
`status_counts` semantics, which is its own change and stays a TODO.
