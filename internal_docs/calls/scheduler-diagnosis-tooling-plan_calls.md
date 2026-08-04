# Judgement Calls: Scheduler Diagnosis Tooling

Decisions made while executing
[`../plans/scheduler-diagnosis-tooling-plan.md`](../plans/scheduler-diagnosis-tooling-plan.md)
(phases 1–3, status SHIPPED). These are all implementation calls; the plan's
own design calls are argued in the plan.

**Ordered most-impactful first. The entry number is its rank.**

---

## 1. The eval table stays authoritative for queue composition and terminal failures; the new metrics are the dashboard, not the detector

**Call.** Phase 2 added `legion_eval_queue_depth{kind}` and
`legion_evals_failed_total{kind,reason}`, and phase 3 was supposed to have the
`eval_starvation` and `invisible_eval_failures` detectors switch to them and
delete the SQL. They did not. `eval_starvation` reads the eval table and
reports the gauge beside it; `invisible_eval_failures` reads **both** the
counter and the rise in the table's failed-row count and takes the larger,
naming which one answered.

```console
$ legion scheduler diagnose -j | jq '.data.findings[] | select(.code=="invisible_eval_failures").evidence'
{
  "failedEvals": "264",
  "failedEvalsSource": "eval table",
  "failedEvalsCounter": "0",          # this manager failed none of them
  "failedEvalRowsAppeared": "264",
  "processedByStatus": "ok=8213, retried=41"
}
```

**Alternatives.** (a) What the plan said: read the labelled gauge, delete the
SQL fallback. Simpler, one source of truth, and one fewer query per snapshot.
(b) Read only the metrics and accept the gap.

**Trade-off.** Two reads instead of one, two numbers in the evidence where one
would do, and a detector that can disagree with the dashboard built on the same
family. Accepted, because that disagreement is *information*: it is exactly the
signature of work happening on another manager.

**Why (correctness beats simplicity here).** The metric registry is
**per-process**; the eval table is **replicated**. Under `lanes` topology the
manager answering the RPC may not be the one that claimed the eval or failed it,
so its counters read zero for work that unquestionably happened. Deleting the
SQL would have made `legion scheduler diagnose` report a healthy scheduler on a
cluster with 264 failed evals on a peer — the *precise* failure this feature was
built to eliminate, reintroduced by following the plan. The gaps still pay for
themselves: their value is the Prometheus dashboard, which the plan itself
argues for in §8 ("every gap improves Prometheus dashboards on its own").

## 2. `legion_evals_failed_total` is recorded inside the store write, and the reason arrives through a new sibling method rather than a widened signature

**Call.** The counter increments inside `Store::complete_eval_impl`, on the arm
that sets `status = 'failed'`, reading the eval's kind from the row in the same
transaction. The *reason* comes from a new `Store::fail_eval_with_event(id,
reason, event)`. The existing `complete_eval` / `complete_eval_with_event` keep
their `succeeded: bool` signatures and pass `EvalFailureReason::Other`.

**Alternatives.** (a) What the plan said: thread `kind` and `reason` through
`complete_eval` and `complete_eval_with_event`. (b) Record the counter at the
call site in `handle_claimed_eval`, where the typed error still exists.

**Trade-off.** A caller that fails an eval through the old method lands in
`reason="other"` rather than being forced to classify. Two methods do
overlapping things, which is a seam a future reader has to notice. Accepted:
the alternative is fifteen call sites — none of which can fail an eval —
passing an enum they must ignore, which is noise that makes the *real*
classification harder to see, not easier.

**Why (simplicity, in service of the guarantee).** The leak-proofing was the
whole point of G2, and it comes from the counter's *location*, not from the
signature. 264 evals once reached `status: failed` while
`evals_processed_total{status="error"}` never moved, precisely because the
counter was at one call site and the write was reachable from several. Counting
inside the write makes an invisible failure structurally impossible; the reason
label is a refinement on top of a guarantee that already holds. Kind is read
from the row for the same reason, and only on the failure path, so the success
path keeps exactly the statements it had.

## 3. `evidence` is a repeated key/value pair on the wire, not `map<string,string>`

**Call.** `SchedulerFinding.evidence` and `SchedulerRuledOut.evidence` are
`repeated SchedulerEvidence { string key; string value; }`. Values are
pre-formatted strings (`31.9%`, `3.07ms`, `0.98`), not numbers.

**Alternatives.** (a) The plan's sketch: `map<string,string> evidence`.
(b) A typed evidence message with a numeric field and a unit enum.

**Trade-off.** Clients that want a number must parse one, and the JSON the CLI
emits collapses the pairs back into an object anyway (so a duplicate key would
silently win — none of the detectors emits one). Accepted.

**Why (the whole feature's thesis).** Protobuf maps have no order. The order a
detector writes its evidence in *is* the order it reasoned in — headline number,
then the numbers that qualify it — and rendering "candidateRankP50" above
"rejectionShare" would defeat the point of pairing them. Formatted values are
the same argument at the value level: a bare `3.07` next to a bare `0.09` is the
misreading this page exists to prevent; `3.07ms` against `0.09ms` is a reading.

## 4. `heartbeat_ceiling` is printed first whatever its severity

**Call.** Findings sort worst-first, except `heartbeat_ceiling`, which is pinned
to the top of `findings` whenever it fires. Every other finding in that report
is flagged `secondary: true` and rendered "(may be secondary — fix heartbeat
cadence first)".

**Alternatives.** (a) Pure severity ordering, which is what the plan's §6.2
JSON implies. The `secondary` flag alone would carry the warning.

**Trade-off.** A `critical` rejection storm can appear below a `warning`
cadence problem, which looks wrong if you read severity as the only ordering
key.

**Why (simplicity / ease at 3am).** It looks wrong and it is right. A fleet
falling behind on heartbeats *generates* the rejection storm underneath it —
each false loss enqueues a reschedule eval — so a report that leads with the
storm sends the responder to tune `evalThreads` on a liveness problem. The
plan's own §6.3 says `heartbeat_ceiling` is "evaluated first"; making it
*printed* first is the same decision applied to the output, and the fixture
from the campaign (`freshness 0.22`) asserts it.

## 5. G5 (eval queue age) is deferred: the eval table has no creation timestamp

**Call.** Not built. `eval_starvation` fires on depth plus kind mix, with no age
input, and the limitation is stated in the public doc's "what these signals
still cannot tell you".

**Alternatives.** (a) Add a `created_at` column via a migration and implement
the gauge as planned. (b) Approximate age from `MIN(id)` — the ids are
autoincrement, so the *oldest* row is knowable even though its age is not.

**Trade-off.** `eval_starvation`'s severity stays coarser than it should be: a
764-deep queue draining in 2s and one taking 6s per item produce the same
finding, which the plan itself calls out as the reason G5 exists.

**Why (scope honesty).** The plan costed G5 as "cheap
(`SELECT MIN(created_at)` on the same refresh)" and marked it "if it comes
cheap". It does not: the `eval` table has columns `id, status, type, job_id,
job_config_id, node_id, priority, ready_at` and no creation time, so G5 is a
schema migration on a raft-replicated table, not a query. `ready_at` is not a
substitute — a delayed eval's `ready_at` is in the future. Rejected (b) because
a row id is not a duration, and a metric named `_seconds` that reports a
sequence number is worse than no metric. Filed as an open item in
[`../TODO.md`](../TODO.md) with the migration named.

## 6. A covering index for the by-kind queue-depth query was tried and reverted, on measurement

**Call.** `legion_eval_queue_depth{kind}` is refreshed with
`SELECT type, COUNT(*) FROM eval WHERE status = 'pending' GROUP BY type`, and
that query stays un-indexed on `type`. The 2.6% cost it adds at 8 eval workers
is accepted.

**Alternatives.** (a) `CREATE INDEX idx_eval_status_type ON eval(status, type)`,
which makes the aggregate index-only — the previous unlabelled `COUNT(*)` was
index-only via `idx_eval_status_priority_id`, and the label is exactly what took
that away. Written, migrated, and benchmarked.

**Trade-off.** Without the index the queue-depth refresh reads each pending row
for its `type` on every enqueue, claim, requeue, and prune.

**Why (measurement).** The index made it *worse*, not better —
`benchmarks/micro/eval_critical_path_bench.rs`, the bench that actually
exercises enqueue → claim → apply → complete:

| Workers | base | `{kind}` label | label + index |
| ---: | ---: | ---: | ---: |
| 1 | 156.68 ms | 156.58 ms | 160.02 ms |
| 4 | 410.29 ms | 405.50 ms | 481.11 ms |
| 8 | 714.35 ms | 733.20 ms | 774.30 ms |

The path is write-heavy against a small table: every eval insert and every
status update pays the index maintenance, and the read it accelerates is a
scan of a few hundred rows. +19% at four workers to save 2.6% at eight is the
wrong trade, and the reverted migration is recorded here rather than left as a
"we should index this" comment for someone to re-derive.

What would make us revisit: a cluster whose *pending* queue is routinely tens
of thousands of rows deep, where the scan stops being cheap. At that depth the
starvation the label exists to diagnose is already the incident.

## 7. `no_candidates` is a ninth rejection reason the plan did not list

**Call.** `legion_plan_rejected_total{reason}` has nine values, not the eight in
plan §6.5. The extra one is `no_candidates`, for a placement that carried no
candidate list at all.

**Trade-off.** One more series, and a value most clusters will never see.

**Why (correctness).** `plan_candidates: 0` is the documented mixed-version kill
switch, and with it a placement submits a single node with no fallback tail —
so the walk's `"no candidates listed"` branch is reachable in exactly that
configuration. Folding it into `node_epoch_moved` would have made the diagnosis
blame contention for a configuration choice, and the remedy ("reduce
`evalThreads`") would not have helped. The label is how the report can instead
say "candidate lists are off".

## 8. The command samples in-process rather than scraping `/metrics`

**Call.** `DiagnoseScheduler` reads the metric registry directly through
`telemetry::snapshot_manager_families()`, which force-creates the registry so an
existing-but-empty family stays distinguishable from a family this build does
not have.

**Alternatives.** (a) Scrape the manager's own `/metrics` endpoint over HTTP and
parse the exposition format — the shape most tools take, and it would reuse the
rendering path.

**Trade-off.** The diagnosis is only available *on* a manager, and a second
process cannot run the detectors against a scrape. Accepted; that is what
Prometheus is for.

**Why (simplicity).** `/metrics` is opt-in configuration, and the clusters least
likely to have it enabled are exactly the ones whose operator is now debugging a
slow scheduler at 3am. A diagnosis that requires the telemetry listener to have
been turned on in advance is a diagnosis that is missing when it is needed. The
in-process read also has no parse step, so a rendering change cannot silently
break the detectors.

## 9. `--sample` is capped at 300s and the call blocks for the window

**Call.** The RPC sleeps for `sample_seconds` between its two snapshots, so the
call blocks. The manager clamps to 300s and the CLI rejects anything longer
client-side with a typed error.

**Alternatives.** (a) A background sampler on the manager that keeps a rolling
window, so the RPC returns instantly. (b) Return immediately with a handle the
client polls.

**Trade-off.** A 30-second CLI command, and a client-supplied hold on a manager
task. Both are real; the cap bounds the second.

**Why (simplicity).** A rolling sampler is a background task, a retention
policy, and a memory budget, all to save a wait the operator is going to spend
anyway — they cannot act on a rate that has not elapsed yet. Blocking also makes
the semantics obvious: the window in the header is the window you waited for.
`--watch` is the loop, and it costs nothing extra.

## 10. `legion scheduler` is a new command group

**Call.** `legion scheduler diagnose`, not `legion operator diagnose` — the
plan's open question 4, resolved the way it leaned.

**Alternatives.** (a) `legion operator diagnose`, next to the other cluster-wide
operator surfaces, and matching the `operator:read` ACL the endpoint uses.

**Trade-off.** The command group and its ACL resource disagree by name, which
will read as an inconsistency to someone grepping for `operator`.

**Why (extensibility).** The natural neighbours are scheduler-shaped —
`scheduler status`, `scheduler phases` — and putting the first one under
`operator` would strand them. The ACL stays `operator:read` because the *data*
is cluster-wide operational state (queue composition, free capacity), which is
an access-control question, not a naming one.
