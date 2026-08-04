# Scheduler Diagnosis Tooling

**Status: SHIPPED** (phases 1, 2, and 3). `legion scheduler diagnose` exists,
all seven detectors run, and four of the five metric gaps are closed. The
public page this plan set out to automate remains the authority on what each
signal means and now documents the command:
[`docs/diagnosing-scheduler-performance.md`](../../docs/diagnosing-scheduler-performance.md).

> **Judgement calls:** [`../calls/scheduler-diagnosis-tooling-plan_calls.md`](../calls/scheduler-diagnosis-tooling-plan_calls.md)

**What shipped, against §8.** Phase 1 in full (the RPC, the CLI verb, `--json`,
`--sample`, the ACL classification and test entry, the MCP tool, the
capabilities entry, all seven detectors, and the campaign fixtures as unit
tests). Phase 2 for G1–G4. Phase 3 in full, including `--watch`.

**Deliberately not built, and why.**

- **G5, `legion_eval_queue_age_seconds`.** The plan costed it as "cheap
  (`SELECT MIN(created_at)` on the same refresh)"; the `eval` table has no
  creation timestamp, so it is a raft-replicated schema migration rather than a
  query. `eval_starvation` therefore fires on depth and kind mix with no age
  input, and its severity is coarser than §6.5 intended. Open item in
  [`../TODO.md`](../TODO.md); reasoning in calls entry 5.
- **§8 phase 3's "the SQL fallback is deleted".** It was not, and should not be:
  the metric registry is per-process while the eval table is replicated, so
  under `lanes` a manager can hold zero counters for failures that certainly
  happened. Deleting the SQL would have made the command report health on a
  cluster with failed evals on a peer. Calls entry 1.
- Everything §8 already named as deferred: baseline capture/compare, the UI
  page, an alerting rule pack, and per-pool attribution.

**Resolved open questions (§11).** 1 — `operator:read`. 2 — `30s`, and
`--watch` inherits it rather than defaulting lower. 3 — embed the verdict:
`heartbeat_ceiling` is a first-class finding, evaluated *and printed* first,
and it points at `legion node cadence` for the remedy. 4 — `legion scheduler`.
5 — no `preemptive` label. 6 — no exit-code contract yet; the verdict
vocabulary is stable but nothing branches on it in CI.

**Deliberately out of scope, and named as non-goals in §4:** anomaly detection,
any change to scheduler *behavior*, a Prometheus/alerting rule pack, a UI page,
and per-node or per-job attribution.

Origin: the AWS phase-1 campaign of 2026-08-02/03 and the scale campaign that
followed it. The post-mortem
([`../aws-phase1-findings.md`](../aws-phase1-findings.md)) records the
through-line — *every wrong turn came from reading one number and not the one
next to it* — and this plan exists to make the number next to it arrive
automatically.

Related plans, none of which this one supersedes:
[`late-binding-plan-apply-plan.md`](late-binding-plan-apply-plan.md) (owns the
rejection semantics), [`plan-path-scale-levers-plan.md`](plan-path-scale-levers-plan.md)
(owns the levers), [`heartbeat-cadence-at-scale-plan.md`](heartbeat-cadence-at-scale-plan.md)
and [`liveness-off-the-raft-path-plan.md`](liveness-off-the-raft-path-plan.md)
(own liveness, and shipped `legion node cadence`, the precedent this plan
generalizes).

---

## 1. Problem statement

Diagnosing a slow Legion scheduler today takes hours and a Prometheus scrape,
and the failure mode is not "the data is missing" — it is that the data is
**scattered across families that only mean something in pairs**, and several
signals **look like something they are not**.

Concretely, tonight, on a real cluster:

- `legion_plan_apply_total{outcome="rejected"}` sat at 73.6% and the correct
  remedy was to **reduce** `evalThreads`. Nothing in the product says that. The
  operator's instinct — throughput is low, add concurrency — makes it worse, and
  we measured it doing so (354.6 → 310.4 alloc/sec, CPU 29% → 85%).
- A box at 29% CPU with `legion_runtime_spawn_delay_seconds` entirely under 50µs
  was throughput-capped. The answer was `legion_raft_pipeline_wait_seconds` on
  one lane against another lane, a comparison no single metric expresses and no
  document told anyone to make.
- `legion_eval_queue_depth` said 764 and could not say **what** was in the
  queue, because it is one unlabelled gauge. The answer (5,760
  `node_lost_reschedule` against 2,503 `job_submit`) had to be reconstructed
  from `evals_processed_total{kind}` deltas.
- 264 evals ended `status: failed` in the eval table while
  `evals_processed_total{status="error"}` **never moved**, and every affected job
  reported `status: done` through `legion job info`. A dashboard built on the
  counter showed a healthy cluster.
- A previous campaign's headline number (0.9 placements/sec at p50 109s) was
  quoted as scheduler throughput and was actually the heartbeat ceiling. The
  discriminator, `legion_heartbeat_freshness_ratio`, was in the same scrape and
  went unread.

**What a user has to do today.** Scrape `/metrics` twice by hand, subtract
cumulative counters mentally, know which of ~40 scheduler-adjacent families pair
with which, know that `pipeline_wait` must be compared *across lanes* rather than
against a threshold, know that the eval table disagrees with the eval counter,
and know that the job status field is a lifecycle flag rather than a placement
verdict. An autonomous agent has to do all of that through text scraping,
because none of it is exposed as structured data.

`legion node cadence` already proved the alternative for the liveness half of
this problem: one command, one verdict, and the remedy computed rather than left
as a formula. There is no equivalent for the scheduler.

---

## 2. Prior art

**Nomad.** `nomad operator debug` bundles logs, metrics, pprof, and API state
into an archive for offline analysis, and `nomad eval status <id>` explains why a
single eval placed nothing (`blocked_eval`, `failed_tg_alloc` with per-node
filter counts: "class exhausted", "constraint filtered"). The per-eval
*explanation* is the strong idea and Legion's closest analogue is `legion job
plan`'s explain output. **Take:** per-cause attribution of a failed placement.
**Reject:** the debug-archive shape. An archive is an artifact for a support
engineer; the user in front of us is on-call at 3am and needs a verdict, not a
tarball.

**Kubernetes.** `kubectl describe pod` surfaces the scheduler's own `FailedScheduling`
event with the node-by-node reason ("0/5 nodes are available: 3 Insufficient
memory, 2 node(s) had taint …"). `kube-scheduler` exposes
`scheduler_pending_pods{queue}` — **labelled by queue**, which is precisely the
label Legion's `eval_queue_depth` lacks — and
`scheduler_schedule_attempts_total{result}` where `result` distinguishes
`scheduled|unschedulable|error`. **Take:** both label shapes, near-verbatim.
**Reject:** the descheduler/scheduler-framework plugin surface; out of scope.

**Consul.** `consul operator autopilot state` and `consul debug`. Autopilot's
output is the closest match in *tone* to what we want: a small set of named
health predicates with a boolean verdict each, rather than a metric dump.
**Take:** the named-predicate output shape.

**CockroachDB.** The `/_status/problemranges` endpoint and the "Advanced Debug"
statement-diagnostics bundle. Cockroach's genuinely transferable idea is that
the database itself names the *category* of problem it is in, and the UI links
each category to the doc section explaining it. **Take:** stable finding codes
that link to a doc anchor.

**What nobody does, and we should.** Every system above reports state. None of
them reports the *neighbouring* number that rules a cause out. Consul's autopilot
comes closest. Legion's whole failure mode this week was single-number reading,
so the design point that differentiates this work is: **the report always carries
the discriminator, including for the causes it ruled out.**

---

## 3. Jobs to be done, by persona

**On-call responder**

- When placements have slowed down and I have no context, I want one command to
  tell me whether the scheduler is actually the problem, so I do not spend an
  hour tuning the wrong subsystem.
- When I am told to make it faster, I want to be told which setting to change
  and in which direction, so I am not guessing at 3am.
- When a number looks bad, I want to see the number that proves what it means,
  so I do not chase the same wrong lead twice.

**Cluster operator**

- When I am sizing a cluster before a launch, I want to know which limit I am
  about to hit and whether a bigger box would move it, so I do not buy hardware
  that changes nothing.
- When work is not running, I want to find out from the system rather than from
  a user, so I am not relying on a status field that reports success by default.

**Autonomous agent**

- When I am asked to investigate a slow cluster, I want a structured verdict with
  stable machine-readable codes and evidence, so I can act without parsing
  prose or scraping a Prometheus text endpoint.
- When I propose a remedy, I want the report to name the exact config field and
  direction, so my change is checkable by a human before it lands.

**Benchmark author**

- When I publish a placement-throughput number, I want the run to record whether
  the control plane was contended, latency-bound, or heartbeat-limited, so I do
  not publish a heartbeat ceiling as a scheduler result. (This happened.)

*Not in scope:* the job author and the security/platform admin. Nothing here is
per-job or per-tenant; the report is cluster-wide operational state.

### User scenarios

1. **The 3am scale-up.** An operator doubled `evalThreads` to make a backlog
   drain faster and throughput fell. They run `legion scheduler diagnose`. It
   says: rejection share 73.6% over the last 30s, `applied` 310/sec against
   `rejected` 866/sec, candidate ranks deep, index candidates plentiful,
   `spawn_delay` healthy — *reduce `evalThreads`*, with the doc anchor.
2. **The idle box.** Throughput is capped, CPU is 29%. The report leads with
   "not CPU-bound", shows `pipeline_wait{scheduler}` 3.07ms against
   `{status}` 0.09ms with `propose_apply` flat at 1.5ms on both, and states
   explicitly that a larger instance will not help.
3. **The agent triage.** An agent is asked "is the cluster healthy?" It calls
   the `legion_scheduler_diagnose` MCP tool, gets `{ verdict: "degraded",
   findings: [{ code: "eval_starvation", … }] }`, and reports the finding code
   and remedy without inventing an explanation.

---

## 4. Goals and non-goals

**Goals**

- G1. One read-only command answers "is my scheduler healthy, and if not, what
  is wrong", with the remedy attached.
- G2. Every finding carries the **discriminating neighbour**, including for
  causes ruled out. A finding with no evidence is a bug in this feature.
- G3. Every number in the report is a **rate or delta over a stated window**,
  never a since-boot average, unless it is genuinely a gauge.
- G4. Fully agent-drivable: standard JSON envelope, stable finding codes, MCP
  tool, capabilities entry.
- G5. Close the four metric gaps that make correct diagnosis impossible today.
- G6. Phase 1 ships value using **only metrics that exist today**.

**Non-goals**

- N1. **No change to scheduler behavior.** Read-only, always. Nothing in this
  plan mutates the cluster or auto-tunes a knob.
- N2. **No anomaly detection or learned baselines.** Thresholds are constants
  with a documented derivation.
- N3. **No alerting rule pack, no dashboards.** Those consume the metrics this
  plan fixes; they are not this plan.
- N4. **No UI page.** Reconsider after the CLI surface has settled.
- N5. **No per-job or per-node attribution.** Cluster-wide only. Per-job "why
  didn't this place" is `legion job plan`'s job.
- N6. **No debug-archive/bundle command.** Named explicitly because Nomad and
  Consul both have one and it will be proposed. It serves a different persona.
- N7. **Does not subsume `legion node cadence`.** Diagnose *calls* the liveness
  verdict and points at it; the cadence command stays the authority.

---

## 5. Background — what exists today

**The precedent.** `legion node cadence` (`src/cmd/node.rs:117` clap config,
`:269` `get_heartbeat_cadence`) is backed by one RPC
(`GetHeartbeatCadence`, `proto/legion.proto:482`, handler
`src/manager/server.rs:5242`), classified `node:read` in
`tests/acl_endpoints.rs:96`, exposed as MCP tool `legion_node_cadence`
(`src/cmd/mcp.rs:257`, dispatch `:834`), and catalogued at
`src/cmd/capabilities.rs:359`. It prints a table, emits the envelope under
`--json`, and refuses to guess on a manager that does not own the subsystem.
This plan reproduces that shape exactly.

**The registry.** Hand-rolled on atomics in `src/manager/telemetry.rs`: cells at
`:76-167`, `CounterVec` `:346`, `GaugeVec` `:407`, `HistogramVec` `:479`, and a
per-family cap `MAX_SERIES_PER_FAMILY = 256` (`:51`) enforced in `Series::cell`
(`:193-222`) that folds overflow into an all-`"other"` series. A new family must
be constructed in `Metrics::new()`, pushed into the `families` vec (`:1229`),
and assigned into the struct (`:1333`). `CounterVec::inc_by` carries
`debug_assert_eq!(labels.len(), meta.labels.len())` (`:362`), so **adding a
label to an existing family requires updating every call site**.

**The four gaps, precisely located.**

1. `legion_eval_queue_depth` — unlabelled `Gauge` (`telemetry.rs:840`), set from
   `Store::pending_eval_count_tx` (`src/storage/store.rs:12723`), a bare
   `SELECT COUNT(*) FROM eval WHERE status = 'pending'`. Refreshed at four call
   sites (`store.rs:12344` claim, `:12406` startup requeue, `:12828` conflict
   requeue, `:13013` enqueue). Two paths are missing: `prune_stale_evals_apply`
   deletes pending rows without refreshing, so the gauge over-reads until the
   next enqueue or claim, and the non-event `release_eval_for_retry` sibling
   needs checking.
2. `legion_evals_processed_total{kind,status}` — recorded at exactly one site,
   `daemon.rs:2267`, **before** the match that decides the eval's fate
   (not-leader → requeue, plan-conflict → requeue, catch-all →
   `complete_eval_with_event(.., false, ..)`). So `status="error"` fires for two
   dispositions that do *not* fail the eval, while the write that actually sets
   `status = 'failed'` (`Store::complete_eval_impl`, `store.rs:12838`, the write
   at `:12851`) touches no counter and is reachable from any caller.
3. No `legion_nodes_recovered_total`. `nodes_marked_lost_total` is recorded at
   `daemon.rs:856`. The mirror-image transition is `BeatDisposition::Transition`
   in `Store::update_node_heartbeat` (`store.rs:5649`, arm at `:5652`) and the
   re-registration path that clears a `lost` row via `SEED_NODE_HEALTH`, neither
   of which counts anything. Already an open TODO item.
4. No rejection reason. `legion_plan_apply_total{outcome}` is incremented in
   `Store::apply_plan_submission_on`: `rejected` inside the `reject` closure
   (`store.rs:3314` signature, metric at `:3340`), `applied`/`fallback` at
   `:3786`. The closure already receives a `reason: String` and persists it to
   `plan_submission.rejection_reason` (`:3324`), but the string is
   `format!`-interpolated with node ids and epoch numbers, so it cannot become a
   label. Two `reject` call sites: the job-config epoch check (`:3364-3369`) and
   the end of the two-pass candidate walk (`:3719-3738`), the latter carrying a
   `first_skip_reason` / `first_stale_reason` free-text cause that collapses
   **eight** distinguishable classes. Note the walk gained a second
   (revalidating) pass and a per-plan capacity-recheck budget since the
   late-binding plan was written, and `legion_scheduler_plan_bind_pass{pass}`
   already reports `clean` vs `revalidated` — that family is not yet in
   `docs/metrics.md` and should be documented alongside this work.

**What already works and must be reused.** `legion_scheduler_plan_phase_seconds`
phases are timed in `Scheduler::plan_and_apply_internal`
(`src/manager/scheduler.rs:1198`) and emitted by `PlanTimings::observe`
(`:139`). Eval kinds are bare `&str` dispatched in `Eval::handle`
(`src/structs/eval.rs:31-42`); there is no enum, which is a small hazard for
label bounding. `legion state query` (`src/cmd/state.rs:56`) plus the curated
`AGENT_SAVED_QUERIES` (`:13-53`, already carrying `agent:pending-evals` and
`agent:recent-failed-evals`) are the workarounds the public doc points at today.

---

## 6. Proposed design

### 6.1 The surface

```console
$ legion scheduler diagnose
Scheduler                 DEGRADED   (sampled 30s)

  ✗ plan_rejection_storm            critical
    31.9% of plans rejected (1,247 rejected / 3,912 total) over 30s.
    Each rejection is a discarded plan plus a full eval retry.
    Ruled out: candidate lists are on (plan_candidates=8); the index
    handed placement 33-64 candidates/task; pool is not full
    (candidate rank p50 = 0).
    → Reduce `evalThreads` (currently 16). Start at 8.
      docs/diagnosing-scheduler-performance.md#symptom-throughput-fell-when-i-scaled-up

  ✗ scheduler_lane_queued           warning
    legion_raft_pipeline_wait_seconds{lane="scheduler"} 3.07ms/op
    against {lane="status"} 0.09ms/op, with propose_apply flat at
    1.51ms on both lanes.
    Ruled out: not CPU-bound (spawn delay p99 41us, in-flight 3/16
    worker threads). A larger instance will not help.
    → Reduce proposals per eval: lower `evalThreads`, and confirm
      topology = "lanes".
      docs/diagnosing-scheduler-performance.md#symptom-the-box-is-idle-and-throughput-is-still-capped

  ✓ heartbeat_ceiling               freshness 0.98 (false loss below 0.20)
  ✓ eval_starvation                 job_submit 61% of processed evals
  ✓ invisible_eval_failures         0 failed evals in the last 30s
  ✓ phase_growth                    apply 6.7ms of 9.3ms total (72%)

Nothing here mutates the cluster. See docs/diagnosing-scheduler-performance.md.
```

Four properties are deliberate:

- **Passing checks are printed.** "eval_starvation ✓ job_submit 61%" is the
  number that stops the next person re-investigating starvation. Silence on a
  passing check reproduces the exact failure this feature exists to fix.
- **`Ruled out:` is a required field on every failing finding.** A finding that
  cannot name what it excluded does not ship.
- **The remedy names the field and the direction**, and reads the *current*
  value out of the manager config so "reduce `evalThreads` (currently 16)" is
  actionable without a second lookup. Same principle as the cadence command's
  computed `heartbeatSeconds`.
- **The sample window is stated in the header.** Every rate is a delta.

Flags:

| Flag | Meaning |
| --- | --- |
| `--sample <dur>` | Sampling window, default `30s`. Two snapshots, delta between them. `0s` reports since-boot values and says so. |
| `--watch` | Re-sample and reprint until interrupted. |
| `-j` / `--json` | Standard envelope (`crate::cmd::output::print_success`). |

### 6.2 The report as data

`legion scheduler diagnose -j`:

```json
{
  "ok": true,
  "data": {
    "verdict": "degraded",
    "sampleSeconds": 30,
    "findings": [
      {
        "code": "plan_rejection_storm",
        "severity": "critical",
        "title": "31.9% of plans rejected",
        "evidence": {
          "legion_plan_apply_total{outcome=rejected}": 1247,
          "legion_plan_apply_total{outcome=applied}": 2503,
          "legion_plan_apply_total{outcome=fallback}": 162,
          "rejectionShare": 0.319
        },
        "ruledOut": [
          {"cause": "candidate_lists_disabled", "evidence": {"planCandidates": 8}},
          {"cause": "pool_full", "evidence": {"candidateRankP50": 0}}
        ],
        "action": {
          "kind": "config",
          "field": "evalThreads",
          "current": 16,
          "direction": "decrease",
          "suggested": 8,
          "text": "Reduce evalThreads to 8 and re-measure."
        },
        "doc": "docs/diagnosing-scheduler-performance.md#symptom-throughput-fell-when-i-scaled-up"
      }
    ],
    "passed": [
      {"code": "heartbeat_ceiling", "evidence": {"freshnessRatio": 0.98, "falseLossBelow": 0.20}}
    ]
  }
}
```

`verdict` ∈ `healthy | degraded | unhealthy`. **Finding codes are a stable
API** — additive only, never renamed, because agents and runbooks will branch on
them.

### 6.3 The detectors

One per observed signature, each with a named discriminator. Thresholds are
constants in `src/manager/diagnose.rs` with the derivation in a comment.

| Code | Fires when | Discriminator carried | Action |
| --- | --- | --- | --- |
| `heartbeat_ceiling` | `freshness_ratio` < 0.5, or < `served/lost_window` | fleet size, served interval, lost window | Point at `legion node cadence`. **Evaluated first**; when it fires, every scheduler finding is annotated "may be secondary". |
| `plan_rejection_storm` | rejection share > 5% (warn) / > 20% (critical) | `plan_candidates`, candidate-rank p50, index candidates/task, rejection reason mix (phase 3) | Decrease `evalThreads`. |
| `scheduler_lane_queued` | `pipeline_wait{scheduler}` p99 > 1ms **and** > 5x the quietest lane, with `propose_apply` within 2x across lanes | spawn delay, in-flight vs worker threads, per-lane `propose_apply`, `coalesce_batch_size{plan_apply}` `le=1` share | Decrease `evalThreads`; confirm `lanes`; explicitly state a bigger box will not help. |
| `phase_growth` | one phase holds > 50% of `total`, or `total` exceeds the sum of phases by > 20% | full phase mix as percentages, per-phase mean over the window | Per-phase remedy table from the public doc. |
| `eval_starvation` | one `kind` is > 60% of processed evals **and** queue depth > 100 | full kind mix, queue depth, queue age (phase 2) | Raise priority on the work that matters; expect it after node events. |
| `invisible_eval_failures` | terminal eval failures > 0 over the window | failed count by kind, `evals_processed_total{status}` for contrast | Query the eval table; do not trust the job status field. |
| `capacity_exhausted` | rejections dominated by capacity reasons, or terminal "no available nodes" failures > 0 | free capacity by pool | Add capacity. No scheduler knob applies. |

`capacity_exhausted` exists so the report can say **"this is not a tuning
problem"**, which is the most expensive wrong turn available: every knob in §6
makes a genuinely full cluster slower, not faster.

### 6.4 Where the data comes from

The handler assembles from three sources already on the manager:

1. **The metrics registry**, in process, via `telemetry::metrics()` — no HTTP
   scrape, no Prometheus dependency. The RPC takes two snapshots `sample`
   seconds apart and subtracts.
2. **The store**, for what metrics cannot express: queue composition by kind and
   terminal eval failures by kind (until phases 2 and 3 land), and free capacity
   by pool. All read-only, all shapes that already exist as saved queries.
3. **The resolved manager config** (`ManagerConfigFull`), for the *current*
   value of every knob a remedy names.

**Non-owning managers.** Scheduler metrics are per-process, and under `lanes` a
manager that does not lead the scheduler lane has near-zero counters. The reply
carries `is_scheduler_lane_leader`, and a non-owning manager reports what it can
and says so — exactly the cadence command's behavior, for the same reason.

### 6.5 The metric gaps

Each is independently valuable and independently shippable.

**G1 — `legion_eval_queue_depth` gains a `{kind}` label.**
`Gauge`→`GaugeVec` conversion at `telemetry.rs:840`; new
`Store::pending_eval_count_by_kind_tx` returning
`SELECT kind, COUNT(*) FROM eval WHERE status = 'pending' GROUP BY kind`,
replacing `pending_eval_count_tx` (`store.rs:12723`) at all four call sites,
plus the two missing paths (`prune_stale_evals_apply`, and the non-event
`release_eval_for_retry`). Kinds that empty out must be **set to 0, not
dropped**, or a drained kind's series goes stale. Ten known kinds is far inside
the 256 cap. `sum(legion_eval_queue_depth)` reproduces today's value exactly, so
existing dashboards keep working — the family name does not change. Directly
mirrors `kube-scheduler`'s `scheduler_pending_pods{queue}`.

**G2 — terminal eval failures reach the counters.** Two changes:

- Move the `evals_processed_total` recording at `daemon.rs:2267` **below** the
  disposition match, and widen `status` to `ok | retried | failed`. `retried`
  covers the not-leader and plan-conflict requeues, which today masquerade as
  errors while leaving the eval alive.
- Add `legion_evals_failed_total{kind, reason}` recorded **inside**
  `Store::complete_eval_impl` (`store.rs:12838`) on the `succeeded == false`
  arm, so no future caller can fail an eval without counting it. `reason` is a
  bounded classification (`no_capacity | unsupported_kind | conflict_exhausted |
  store_error | other`) derived at the call site, never a free-text error string.
  Requires threading `kind` and `reason` through `complete_eval` /
  `complete_eval_with_event`.

Breaking change to the meaning of `status="error"` — see §10.

**G3 — `legion_nodes_recovered_total{path}`.** `path` ∈ `heartbeat |
registration`. Recorded in the `BeatDisposition::Transition` arm
(`store.rs:5652`), **guarded on the previous status actually being
`lost`** (the arm also fires for `ready → active` and on a poisoned read model),
and on the re-registration path that clears a `lost` row. Makes `marked_lost -
recovered` computable, which is the only way flapping becomes visible. Already
an open TODO item; this plan is where the shape is specified.

**G4 — `legion_plan_rejected_total{reason}`.** A **new** family rather than a
second label on `plan_apply_total`, so the existing family's series shape and
every dashboard built on it survive untouched. Eight bounded values:

| `reason` | Site | Means |
| --- | --- | --- |
| `job_epoch_stale` | `store.rs:3364-3369` | Two plans raced on one job config. Serialization working as designed; reduce concurrency. |
| `node_epoch_moved` | `store.rs:~3556` (`first_stale_reason`) | A concurrent apply took the node. **The signature of too much concurrency.** |
| `node_gone` | `store.rs:~3599` | Node row vanished between plan and apply. |
| `node_unavailable` | `store.rs:~3606` | Cordoned, draining, or lost since the snapshot. Fleet churn, not contention — and the case where "reduce `evalThreads`" is the *wrong* advice. |
| `late_constraint_failure` | `store.rs:~3619` | Constraints/permits no longer satisfied. |
| `capacity_recheck_budget` | `store.rs:~3634` | The plan exhausted its per-plan capacity-recheck budget. |
| `capacity_shrank` | `store.rs:~3658` | Node totals changed under the plan. |
| `static_port_taken` | `store.rs:~3682` | Port collision at bind time. |

Implementation: a `first_skip_kind: Option<&'static str>` (and its
`first_stale_kind` twin) set alongside each existing `first_skip_reason` /
`first_stale_reason` assignment, threaded into the `reject` closure
(`:3314-3317`). Mechanical: eight sites plus the closure signature. Line numbers
are approximate — the walk is under active change (two-pass revalidation and the
recheck budget both post-date the late-binding plan), so re-locate by the format
string before editing.

This is the highest-value gap on the list: it converts "reduce concurrency" from
folklore into a reading. `node_epoch_moved` dominating means concurrency;
`node_unavailable`/`capacity_shrank` dominating means fleet churn and the
`evalThreads` advice is *wrong*.

**G5 (lower priority) — `legion_eval_queue_age_seconds`.** Gauge, age of the
oldest pending eval. A 764-deep queue draining in 2s and one taking 6s per item
are the same depth and completely different incidents. Cheap
(`SELECT MIN(created_at)` on the same refresh) and it is what makes the
`eval_starvation` detector's severity meaningful rather than a depth threshold
pulled out of the air.

### 6.6 Agent surface

Per AGENTS.md, non-optional:

- **MCP tool `legion_scheduler_diagnose`** in `src/cmd/mcp.rs`, `mutates: false`
  so it is always listed. Description states the verdict vocabulary and that the
  tool is read-only, in the same style as `legion_node_cadence`
  (`mcp.rs:257-258`).
- **Capabilities catalog** entry `scheduler.diagnose` in
  `src/cmd/capabilities.rs`, `mutates: false`, `read_only_safe: true`.
- **`docs/agent-readiness.md`** gains the tool in its listing.
- **Envelope discipline**: `output::resolve(format, action, result)` for the gRPC
  call, `output::print_success(&data)` for the payload. No ad-hoc JSON.
- **No idempotency key.** Read-only.

---

## 7. Proto, struct, config, and persistence changes

**Proto** (`proto/legion.proto`), one new RPC with full public-facing comments
on the method and **every** field:

```proto
// Reports whether the scheduler is healthy and, when it is not, which
// condition it is in and what to change.
//
// Read-only. Samples the in-process metric registry twice, `sample_seconds`
// apart, so every rate is a rate rather than a since-boot average, and reads
// eval-queue composition from the store. Backs `legion scheduler diagnose`.
//
// Answer with care on a manager that does not lead the scheduler lane: its
// counters are near zero. Check `is_scheduler_lane_leader` in the reply.
//
// ACL: operator:read, unscoped (the report is cluster-wide).
// See docs/diagnosing-scheduler-performance.md.
rpc DiagnoseScheduler (DiagnoseSchedulerRequest) returns (DiagnoseSchedulerReply);
```

`DiagnoseSchedulerRequest { uint32 sample_seconds = 1; }`.
`DiagnoseSchedulerReply { string verdict; uint32 sample_seconds;
bool is_scheduler_lane_leader; repeated Finding findings; repeated Finding
passed; }`, with `Finding { string code; string severity; string title;
map<string,string> evidence; repeated RuledOut ruled_out; Action action;
string doc_anchor; }`.

**ACL.** `self.check_acl(&request, AclResource::Operator, AclAction::Read,
AclScope::any())?` as the handler's first line, plus the entry in
`tests/acl_endpoints.rs` `endpoint_table` and a request builder — the
`every_manager_rpc_is_classified` test fails the build otherwise.

**Structs.** New `src/manager/diagnose.rs`: a `SchedulerSnapshot` (plain data,
no I/O), a `diagnose(before, after, config) -> Report` **pure function**, and the
threshold constants. Purity is the point — it is what makes tonight's numbers
usable as test fixtures.

**Config.** None. No new fields, no new defaults. Nothing goes in
`configs/jobs/maximal.ncl` because there is no job-config surface.

**Persistence.** None. No new tables, no migration, no format change.

---

## 8. Implementation phases

**Phase 1 — the command, on today's metrics.** `legion scheduler diagnose`
(+`--json`, `--sample`), the RPC, the ACL classification and test entry, the MCP
tool, the capabilities entry, and the detectors that today's data supports:
`heartbeat_ceiling`, `plan_rejection_storm`, `scheduler_lane_queued`,
`phase_growth`, plus `eval_starvation` and `invisible_eval_failures` served from
**store queries** rather than metrics. Ships the whole user-facing value with
zero metric changes. *Independently valuable: yes — it is the entire feature for
a user, just with two detectors reading SQL.*

**Phase 2 — the metric gaps.** G1 (`{kind}` on queue depth, plus the two missing
refresh sites), G2 (failed evals reach the counters), G3
(`nodes_recovered_total`), G4 (`plan_rejected_total{reason}`), and G5 (queue
age) if it comes cheap. No CLI change. *Independently valuable: yes — every gap
improves Prometheus dashboards on its own, whether or not phase 1 exists.*

**Phase 3 — detectors consume the new signals.** `plan_rejection_storm` reports
the reason mix and **changes its remedy accordingly** (concurrency vs fleet
churn); `eval_starvation` reads the labelled gauge instead of SQL and uses queue
age for severity; `invisible_eval_failures` reads counters. Add `--watch`.
*Independently valuable: yes — it makes the phase-1 advice correct in the cases
where it is currently a guess.*

**Deferred, named.** Baseline capture/compare (`--save-baseline` /
`--since-baseline`), a UI page, an alerting rule pack, and per-pool attribution.
None blocks the others; all are cheap once the pure `diagnose()` function
exists.

---

## 9. Testing plan

**The centerpiece: tonight's numbers as fixtures.** `diagnose()` is a pure
function over two snapshots plus config, so each real signature becomes one unit
test asserting the expected finding code, severity, and action direction:

| Fixture | Asserts |
| --- | --- |
| 4 threads / 1k nodes, 15.9% rejected | `plan_rejection_storm` at `warning`, action `evalThreads` decrease |
| 16 threads / 5k nodes, 73.6% rejected | same code at `critical` |
| 29% CPU, `pipeline_wait` 3.07 vs 0.09ms, `propose_apply` 1.5ms flat, spawn delay 41µs | `scheduler_lane_queued`, and **no** CPU-related finding |
| phases 0.29→9.64 / 1.72→7.01 / 6.74→19.8ms | `phase_growth` names `task_fetch` |
| 5,760 `node_lost_reschedule` vs 2,503 `job_submit`, depth 764 | `eval_starvation` |
| freshness 0.22, lost window 10s | `heartbeat_ceiling` **first**, scheduler findings annotated secondary |
| 264 failed evals, `status="error"` at 0 | `invisible_eval_failures` |
| healthy snapshot | `verdict: healthy`, and every check present in `passed` |

Regression value beyond correctness: if a future change makes any of these
signatures undiagnosable, a test fails.

**Unit** — threshold boundaries (just under / just over each), the non-owning
manager path, `sample_seconds = 0`, and a snapshot where a counter *decreased*
(manager restart mid-sample) which must not produce negative rates.

**Integration** — `tests/acl_endpoints.rs` (mandatory: the classification test
fails the build without it); an unauthenticated call returns `PermissionDenied`;
the MCP tool appears in `tools/list` **without** `--allow-write`; the
capabilities unit test.

**Metric changes (phase 2)** — `sum(eval_queue_depth{kind})` equals the old
unlabelled value across enqueue/claim/prune/requeue; a drained kind reports `0`
rather than disappearing; `complete_eval(.., false, ..)` always increments
`evals_failed_total` (the leak-proofing is the point); every one of the eight
rejection reasons is reachable and produces its label — six of them need a
crafted race, and the existing late-binding tests already construct most.

**Scale** — one rig cell reproducing the 16-thread/5k-node configuration and
asserting `legion scheduler diagnose -j` returns `plan_rejection_storm`. This is
the acceptance test that the feature works on the problem it was built for.

Per AGENTS.md, scheduler-touching changes ship with before/after benchmark
numbers; phase 2 adds a label to a hot-path gauge and a counter on the apply
path, so `cargo bench --bench scheduler_bench` on base and change goes in the PR.

---

## 10. Rollout and backward compatibility

- **Phase 1 is purely additive.** New RPC, new CLI verb, new MCP tool. Nothing
  existing changes shape.
- **Version skew.** A newer CLI against an older manager gets `Unimplemented`;
  the command must catch that and say "this manager predates `scheduler
  diagnose` (added in X); upgrade the manager or read
  docs/diagnosing-scheduler-performance.md" rather than surfacing a bare gRPC
  error. Standard upgrade order otherwise ([`docs/upgrades.md`](../../docs/upgrades.md)).
- **G1 is dashboard-compatible.** The family name is unchanged and
  `sum()` reproduces the old value. A query selecting the bare series without an
  aggregator now returns N series instead of 1 — noted in the metrics doc and in
  release notes.
- **G2 changes what `status="error"` means**, from "the handler returned Err"
  (including two retry paths) to "the eval terminally failed", with `retried`
  taking the rest. This is the only genuinely breaking change in the plan. It is
  worth it: the current value is misleading in both directions. Release-note it
  explicitly and update `docs/metrics.md` in the same change.
- **G3, G4, G5 are new families**, zero on a cluster where nothing has happened
  yet. Worth stating in the doc, because "the counter does not exist" and "the
  thing never happened" being indistinguishable is a trap this repo has already
  been bitten by (`aws-phase1-findings.md`: a health check treated absent
  failure counters as failures, and no step could ever report healthy).
- **No config migration, no data migration, no format change.**

---

## 11. Open questions

1. **`operator:read` or `node:read` for the ACL?** Lean `operator:read`: the
   report is cluster-wide operational state and exposes queue composition and
   capacity, which is closer to `operator` than to `node`. Cheap to be wrong in
   the strict direction and loosen later.
2. **Default sample window.** Lean `30s`: long enough that a low-rate cluster
   produces non-zero deltas, short enough for a 3am command. `10s` is more
   responsive under `--watch`; possibly `--watch` should default lower.
3. **Should `diagnose` embed the cadence verdict or only point at it?** Lean
   embed the *verdict* (one line, one number) and point at `legion node cadence`
   for the remedy. The mistake this feature exists to prevent is exactly a
   scheduler investigation that never looked at freshness, so a pointer alone is
   probably too weak.
4. **`legion scheduler` as a new command group, or `legion operator
   diagnose`?** Lean `legion scheduler`, leaving room for `legion scheduler
   status` / `legion scheduler phases` later. Weakly held.
5. **Should `plan_rejected_total{reason}` carry a second `preemptive` label?**
   The `reject` closure already knows (`reverted_evictions`). Lean no for now:
   14 series instead of 7 for a distinction nobody has needed yet.
6. **Does the report need a machine-readable severity floor for exit codes**
   (non-zero exit on `unhealthy`) so it can be used in CI? Lean yes but not in
   phase 1; it constrains the verdict vocabulary permanently.

---

## 12. Acceptance criteria

**Phase 1**

- `legion scheduler diagnose` and `-j` both work against a running manager; the
  JSON validates as the standard envelope and carries `verdict`,
  `sampleSeconds`, `findings`, `passed`.
- Every failing finding has a non-empty `ruledOut` and an `action` naming a
  field and a direction; a reviewer can check this by reading the fixtures.
- All seven detectors present; the healthy-cluster fixture puts every one in
  `passed` with evidence.
- `tests/acl_endpoints.rs` passes with the new endpoint classified; an
  unauthenticated call is denied.
- `legion_scheduler_diagnose` is listed by `mcp serve` without `--allow-write`,
  and absent from no listing.
- `capabilities()` includes `scheduler.diagnose` with `read_only_safe: true`.
- Each of the seven fixture snapshots produces its expected finding code.

**Phase 2**

- `legion_eval_queue_depth{kind}` sums to the previous unlabelled value across
  every mutation path, including prune; a drained kind reads `0`.
- `complete_eval(.., false, ..)` from *any* caller increments
  `legion_evals_failed_total`; a test calling it directly proves it.
- `legion_nodes_recovered_total{path}` increments on a lost node's next
  heartbeat and on re-registration, and does **not** increment on `ready →
  active`.
- All eight `legion_plan_rejected_total{reason}` values are reachable and
  covered by a test.
- `docs/metrics.md` documents every new family and the changed
  `evals_processed_total` status vocabulary.
- `cargo bench --bench scheduler_bench` before/after numbers in the PR.

**Phase 3**

- `plan_rejection_storm`'s action differs, verifiably, between a
  `node_epoch_moved`-dominated mix (reduce concurrency) and a
  `node_unavailable`-dominated mix (investigate fleet churn).
- `eval_starvation` reads the labelled gauge; the SQL fallback is deleted.
- `--watch` reprints on the sample interval and exits cleanly on SIGINT.
- The rig cell at 16 threads / 5k nodes returns `plan_rejection_storm` from
  `legion scheduler diagnose -j`.
