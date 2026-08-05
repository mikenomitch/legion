# Plan: The `evalThreads` Default

> **Judgement calls:** [`calls/eval-thread-default-plan_calls.md`](../calls/eval-thread-default-plan_calls.md) *(not yet written — created with Phase 1)*

**Status: PLANNED**

Nothing here is built, and **Phase 0 is a gate, not a formality**: the only
measurement we have compares 4 threads against 16 and says 16 wins. It does
not say whether 32 or 64 wins by more, or where the number turns back down.
Shipping a new default without that sweep would replace one unmeasured
constant with another.

Deliberately out of scope from the outset: any change to what `evalThreads`
*means*, any adaptive or self-tuning controller (that is
[adaptive-scheduler-concurrency-plan.md](adaptive-scheduler-concurrency-plan.md)),
and any change to the plan-apply commit path itself.

---

## 1. Problem Statement

`evalThreads` sets how many eval workers a manager runs — how many
evaluations can be planned and applied at once. It defaults to the host's
core count:

```rust
// src/structs/config.rs:1889
fn default_eval_threads() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}
```

**Core count is the wrong basis, and we now have the measurement that says
so.** An eval worker does not compute for its lifetime; it parks. It blocks
on a scheduler-lane quorum round to claim work, plans against an in-memory
snapshot, and then blocks again on the plan-apply commit — a raft round trip
through a coalescer that holds a single commit lock. A saturated 5,000-node
control plane measured **8% CPU across four cores** with its eval queue
backed up. The resource that decides how many workers are useful is
round-trip latency and how much the commit path can absorb, not how many
cores are available to burn.

**The measurement.** AWS campaign 2026-08-05
([aws-phase4-findings.md](../aws-phase4-findings.md)), cells G and H: same
binary, same rig, 5,000 nodes, 25,002 allocations, three 4-vCPU managers,
both cells verified `driver_bound: false`.

| cell | `evalThreads` | throughput | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| G | 4 (the default on that host) | 177.27 alloc/s | 63.4 s | 100.8 s |
| **H** | **16** | **219.25 alloc/s** | **54.1 s** | **67.3 s** |

**+23.7% throughput and p99 down 33%, from a value the default cannot
produce.** On the campaign's `m6id.xlarge` managers the default is 4, so
every cell that used it ran at the losing setting.

**Why the earlier answer was the opposite.** Phases 2 and 3 measured 203.19
alloc/s at 4 threads against 164.72 at 16 and concluded that concurrency
cost throughput. Both defects behind that number are fixed:

- The load generator submitted one `legion job run` CLI process per job from
  a 4-thread pool on a 2-vCPU box shared with Prometheus, capping submission
  at ~36 jobs/sec. Every "throughput" figure was the driver's rate. More
  eval threads dividing a fixed arrival rate looked like a regression.
- The eval notifier called `notify_waiters` on any eval-table change, so
  every worker woke and paid a scheduler-lane quorum round to discover an
  empty queue — 16,678 scheduler-lane ops at 4 threads against 47,096 at 16
  for identical work. Each added thread bought waste. Fixed by
  [eval-path-raft-cost-plan.md](eval-path-raft-cost-plan.md) entry 1; cell H
  measured 0.3 empty claims/sec.

With both fixed the sign of the relationship flipped, which is the result
the notifier fix predicted and a starved rig could never have shown.

**The shipped example config makes it worse.** `configs/manager.ncl` sets
`evalThreads = 4` explicitly, so anyone who starts from it gets the losing
value pinned regardless of their hardware, and never sees the default at
all. Whatever Phase 0 decides, that line moves with it.

**What an operator faces today.** Nothing tells them the default is wrong.
`legion` logs the worker count at startup and no more. The public guidance
was itself wrong until 2026-08-05 —
[docs/diagnosing-scheduler-performance.md](../../docs/diagnosing-scheduler-performance.md)
told operators to *lower* `evalThreads` on the strength of the retracted
measurement. It now says core count is a poor basis either way and that on
an uncontended cluster the default is more likely to be too low than too
high. The code has not caught up with the doc.

---

## 2. Prior Art

**Nomad — `num_schedulers`, defaults to the core count.**
Nomad's server runs N scheduler workers pulling evaluations from a broker,
and [`num_schedulers`](https://developer.hashicorp.com/nomad/docs/configuration/server#num_schedulers)
defaults to `runtime.NumCPU()`. This is where Legion's default comes from,
and it is worth being precise about why it works better for Nomad than for
us. Nomad's scheduler workers compute against an in-memory MemDB snapshot
and submit plans to a **plan applier** on the leader that serializes
application — structurally the same shape as Legion's coalescer. But
Nomad's plan applier commits through a single Raft group it already owns,
without a per-plan read-modify-write transaction against SQLite, and its
workers' non-planning work is genuinely CPU-bound MemDB traversal. Ours is
not: `build_plan` is 20 ms of a 381 ms eval at 100,000 allocations, and the
other 354 ms is the apply. **We took Nomad's default without Nomad's cost
distribution.** What we take from Nomad: the knob shape (a static integer,
set at startup, per server) and the practice of documenting it as an
operator-tunable rather than hiding it. What we reject: the core-count
default.

**Kubernetes — a fixed default that is deliberately not the core count.**
`kube-scheduler` runs **one** scheduling cycle at a time (a pod is selected,
filtered, and scored serially) and moves only the **binding** cycle to a
goroutine, so the decision is serialized and the commit is parallel. Within
one cycle, `parallelism` in
[KubeSchedulerConfiguration](https://kubernetes.io/docs/reference/config-api/kube-scheduler-config.v1/)
sets how many goroutines run the node feasibility and scoring passes, and
its **default is a flat 16 — not `NumCPU()`**. Kubernetes also bounds the
work per cycle rather than the workers:
[`percentageOfNodesToScore`](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduler-perf-tuning/)
scales the sampled node fraction down as the cluster grows. What we take:
(a) a fixed integer default is a legitimate, shipped choice for a scheduler
— it does not have to be derived from the host; (b) bounding work per
decision is a separate lever from bounding concurrent decisions, and Legion
already has its own version in `schedulerSettings.plan_candidates` and
`score_sample`. What we reject: serializing the decision. Legion's
optimistic plan-apply fence exists precisely so planners can run
concurrently, and
[late-binding-plan-apply-plan.md](late-binding-plan-apply-plan.md) drove the
resulting rejection rate to zero. Serializing now would give that up.

**Consul.** No analogue — Consul has no scheduler. Its relevant lesson is
negative and about a different subsystem: Consul sizes raft and its
connection pools from configuration and workload rather than from
`NumCPU()`, and Legion's own read-connection pool was moved off
`available_parallelism()` for the same reason
([eval-path-raft-cost-plan_calls.md](../calls/eval-path-raft-cost-plan_calls.md)
entry 3).

**The pattern across all three:** nobody derives *replicated-write*
concurrency from core count. Nomad derives *compute* concurrency from it, and
Nomad's scheduler workers really do compute.

---

## 3. Jobs to Be Done, by Persona

**Cluster operator**

- When I stand up a manager on whatever instance type my platform team gives
  me, I want the out-of-the-box settings to place work at close to the speed
  the hardware can, so I do not have to run a benchmark campaign before my
  first production deploy.
- When I move my managers to a bigger or smaller instance, I want to know
  whether that changed how fast work gets scheduled, so I can tell my
  capacity planning from my performance regressions.
- When I do decide to tune this, I want to be told what to watch and what
  "too high" looks like, so I can stop turning the dial when it stops
  helping.

**On-call responder**

- When deploys are landing slowly and I have no context, I want to see
  whether the schedulers are busy or just waiting, so I know whether to add
  capacity, change a setting, or look somewhere else entirely.
- When I read a runbook that tells me to change a setting, I want to trust
  that it was written against a measurement that still holds, so I am not
  making the incident worse. *(This job exists because we failed it once:
  the public guidance said "lower `evalThreads`" flatly, on the strength of
  a number that was measuring a load generator, until it was corrected on
  2026-08-05.)*

**Job author / app developer**

- When I ship a deploy to a large cluster, I want it to start rolling
  promptly rather than sitting in a queue, so my rollout time is about my
  service and not about the control plane.

*Not directly in scope:* the **autonomous agent** persona. `evalThreads` is
manager-side configuration set at process start; an agent driving the CLI or
MCP surface neither sets it nor sees it. It benefits from the same
throughput and latency as everyone else, and the observability in Phase 2 is
already structured, so there is no agent-specific surface to design here.

### User scenarios the design has to serve

1. **The default install.** An operator deploys three managers on 4-vCPU
   instances, runs 5,000 nodes, and never opens the scheduler docs. Their
   cluster should place work at roughly the rate the measurements say it can
   — not 23.7% below it.
2. **The deliberate tuner.** An operator with a 64-vCPU manager reads the
   docs, wants more concurrency, and sets `evalThreads` explicitly. Their
   value must keep winning over the default, and the docs must tell them
   what to measure to know whether it helped.
3. **The small cluster.** Someone running one manager on a laptop or a
   2-vCPU VM with 40 nodes must not get a default that spends memory and
   threads on concurrency they will never use.

---

## 4. Goals and Non-Goals

**Goals**

- G1. Establish, by measurement, where eval-worker concurrency stops buying
  throughput at a realistic fleet size — the *knee*, not just "16 > 4".
- G2. Set a default that is defensible from that measurement and does not
  derive from core count.
- G3. Make the chosen value legible: logged at startup, documented, and
  paired with the metrics that say whether it is right for a given cluster.
- G4. Keep the small-deployment case honest — no default that makes a
  single-node dev cluster pay for concurrency it cannot use.

**Non-goals**

- N1. **Any adaptive or self-tuning controller.** A closed loop over the
  plan-apply path is
  [adaptive-scheduler-concurrency-plan.md](adaptive-scheduler-concurrency-plan.md),
  which is separately planned and explicitly gated on its own Phase 0. This
  plan sets a *static* default and stops.
- N2. **Changing what `evalThreads` means.** Same knob, same semantics, same
  place in the config, same startup-only lifecycle.
- N3. **Fixing the plan-apply ceiling.** 347 ms of coalescer follower wait
  against a 138 ms commit is the real constraint at scale, and it belongs to
  [plan-apply-conflict-reduction-plan.md](plan-apply-conflict-reduction-plan.md)
  and the coalescer work. This plan chooses how many workers queue against
  that ceiling; it does not raise it. **If the ceiling moves, Phase 0 must be
  re-run** — the knee is a property of the commit path, not of the workers.
- N4. **Per-lane, per-pool, or per-namespace worker counts.** One number per
  manager.
- N5. **Runtime reconfiguration.** `evalThreads` stays startup-only.

---

## 5. Background — What Exists Today

**The knob.** `eval_threads` on `ManagerConfigFull`
(`src/structs/config.rs:1380-1381`), `#[serde(default = "default_eval_threads")]`,
so an absent `evalThreads` key takes `available_parallelism()`
(`src/structs/config.rs:1889`). Surfaced to operators as `evalThreads` in the
manager Nickel config.

**Where it is consumed.** `src/manager/daemon.rs:426` passes
`full_config.eval_threads` to `start_eval_loop`, which clamps it to at least
1 as `max_threads` (`:1457-1458`), logs it (`:1459`), emits a
`scheduler_started` cluster event carrying it (`:1486`,
`src/storage/events.rs:170`), and spawns exactly that many workers
(`:1582`):

```rust
for worker_id in 0..max_threads {
    spawn_eval_worker(&mut join_set, &store, worker_id);
}
```

Workers are supervised forever — any exit is replaced (`daemon.rs:1587-1592`) —
so the count is invariant for the process lifetime.

**What one worker does** (`eval_worker_loop`, `src/manager/daemon.rs:1674`):

1. Gate on scheduler-lane raft leadership; followers idle (`:1700`).
2. Claim an eval — `store.claim_eval()` (`:1719`), or take the one handed
   back by the previous iteration's fused write. A fresh claim is a
   **scheduler-lane quorum round**.
3. Handle it on the blocking pool — `spawn_blocking(resolve_claimed_eval)`
   (`:1759`). The handler is synchronous end to end: SQLite reads,
   the plan, the raft-proposal wait, and the conflict-retry backoff sleeps.
4. `store.complete_and_claim_next()` (`:1768`) — one scheduler-lane quorum
   round that both finishes this eval and claims the next.

So a worker is at most one eval, and its wall time is dominated by two
quorum round trips plus one plan-apply commit.

**What that costs, measured** (2026-08-05, `dense_fleet`, 16 threads, all
cells `driver_bound: false`):

| allocations | `apply` | `build_plan` | `capacity_snapshot` | plan total |
| ---: | ---: | ---: | ---: | ---: |
| 100,000 | 354 ms | 20 ms | 4 ms | 381 ms |
| 200,000 | 415 ms | 32 ms | 8 ms | 465 ms |

and inside that `apply` at 200,000 allocations: coalescer **follower wait
347 ms**, commit 138 ms, leader wait 66 ms, **batch size 1.32**. The apply
is 93% of an eval and it is overwhelmingly queueing. Workers are cheap to
add in CPU terms and expensive to add in queue-depth terms — which is
exactly why the knee is an empirical question rather than an arithmetic one.

**What the numbers do not settle.** Effective planner concurrency — evals
per second x plan latency, i.e. how many evals were actually inside the plan
path at once — is **3.96** at 100,000 allocations (456.58 alloc/s ÷ 44
allocations per eval = 10.4 evals/s, x 381 ms), with 16 workers configured.
At 200,000 it is ~2.9. Sixteen workers were configured and about four were
in the plan path. The coincidence with the
campaign's 4 vCPUs prompted a check of whether the read-connection pool
(then also 4) was gating the plan path; **it is not** — no read connection
is held across a quorum round, an await, or `build_plan`, and the apply runs
on the write connection (see
[eval-path-raft-cost-plan_calls.md](../calls/eval-path-raft-cost-plan_calls.md)
entry 3). The missing concurrency is unexplained and Phase 0 has to look at
it, because *if 16 workers only ever put 4 in the plan path, the knee may
already be below 16 for reasons that have nothing to do with the commit
lock.*

**One new coupling to state.** As of the same change, the read-connection
pool is sized from `evalThreads` rather than from the core count
(`read_only_conn_pool_size`, `src/storage/store.rs`). Raising `evalThreads`
therefore also widens the read pool, up to a cap of 16 connections, each
with a 64 MiB SQLite page-cache **cap** (`SQLITE_CACHE_SIZE_KIB`). This is
deliberate — the eval workers *are* the concurrent readers — but it means a
new default has a memory consequence as well as a concurrency one, and
Phase 0 must record manager RSS per cell rather than assuming it is free.

**Documentation as it stands.**
[docs/diagnosing-scheduler-performance.md](../../docs/diagnosing-scheduler-performance.md)
already carries the corrected guidance: a dated correction block on the
"throughput fell when I scaled up" section, the statement that core count is
a poor basis either way, and a knobs-table row saying to turn `evalThreads`
down only when the rejection share or the lane's `applied` rate justifies it
— "on an uncontended cluster the opposite applies: 16 beat 4 by 23.7% at
5,000 nodes." **Everything in this plan must stay consistent with that page,
and Phase 2 updates it rather than contradicting it.**

**Relevant metrics that already exist:** `legion_eval_claim_total{outcome}`,
`legion_plan_apply_total{outcome}`, `legion_scheduler_plan_phase_seconds{phase}`,
`legion_store_coalesce_*{path="plan_apply"}`, `legion_raft_ops_total{lane,op,outcome}`,
`legion_store_read_lock_wait_seconds`, `legion_store_write_lock_wait_seconds`,
`legion_eval_handle_seconds{kind}`. Phase 0 needs no new instrumentation,
which is most of why it is cheap.

---

## 6. Proposed Design

### 6.1 The surface, unchanged

```nickel
# configs/manager.ncl
{
  evalThreads = 16,   # still a plain integer, still startup-only
}
```

An explicit value always wins. Nothing about how the knob is written, read,
or validated changes.

### 6.2 The default, replaced

The default becomes a **named constant, not a host property**:

```rust
/// Eval workers a manager runs when `evalThreads` is unset.
///
/// NOT the core count. An eval worker's wall time is two scheduler-lane
/// quorum rounds plus a plan-apply commit; a saturated 5,000-node control
/// plane measured 8% CPU across four cores. <measured justification and
/// the campaign that produced it go here, once Phase 0 has run>
const DEFAULT_EVAL_THREADS: usize = /* Phase 0 decides: 16, 32, or 64 */;

fn default_eval_threads() -> usize {
    DEFAULT_EVAL_THREADS
}
```

**The value is deliberately left open.** It is Phase 0's output. What the
existing evidence supports is that it is **at least 16** and **not the core
count**; the phases below are ordered so the constant is written after the
sweep, not before.

### 6.3 What an operator sees

Startup already logs the worker count (`Beginning eval loop max_threads=16`)
and emits it as a `scheduler_started` cluster event. Phase 2 makes the
*provenance* explicit on both, so an operator can tell a default from their
own value without diffing config:

```
INFO Beginning eval loop max_threads=16 source=default
INFO Beginning eval loop max_threads=32 source=config
```

and the same `source` in the event's metadata alongside the existing
`evalThreads` field, so it is visible in `legion events` and the UI rather
than only in a manager's log file.

That single field is the whole UX change. There is no new flag, no new
config key, and no advisory that fires on a value we cannot actually judge —
we would be guessing at what is too high for a cluster we cannot see, and
the metrics in §5 already answer it for the operator who looks.

### 6.4 Small deployments

A flat default of 16 or 32 on a single-node dev cluster costs 16-32 parked
threads and up to 16 read connections. Threads that are parked on an empty
`Notify` are nearly free; the read pool is capped and its page cache is a
lazily-filled cap rather than a preallocation, so a dev cluster with a
2 MiB working set holds 2 MiB per connection. Phase 0 measures this rather
than asserting it (acceptance criterion A0.4) — if idle RSS on a small
cluster moves materially, the default gains a small-host clamp and the plan
records that as a judgement call.

---

## 7. Proto, Struct, Config, and Persistence Changes

**None.** No proto change, no wire or on-disk format change, no migration,
no new job-config field, and therefore no `configs/jobs/maximal.ncl` entry.
The struct field `ManagerConfigFull::eval_threads` keeps its type, name, and
serde attribute; only the body of `default_eval_threads()` changes, plus a
`source` field on the eval-loop startup log line and on the
`scheduler_started` event's metadata (additive, so an older reader of the
event stream is unaffected).

This is a **changed default** — which is exactly the category
[AGENTS.md](../../AGENTS.md) requires a plan for, and why this document
exists despite the diff being three lines.

---

## 8. Implementation Phases

### Phase 0 — Measure the knee *(prerequisite, ships alone, changes no code)*

**This phase is the reason the plan exists.** It produces a number; nothing
downstream is defensible without it.

Run the sweep on the AWS rig, one variable, everything else fixed. There is
no sweep flag for this knob — `--eval-threads` takes one value and needs the
control-plane reset to apply it — so it is one campaign invocation per cell,
which is also what keeps the cells honestly independent:

```bash
for threads in 4 8 16 32 64; do
  benchmarks/aws/scripts/run-campaign.sh \
    --system legion-a \
    --scenarios dense_fleet \
    --steps 5000 \
    --eval-threads "$threads"
done
```

4 is included even though cell G already measured it: comparing cells from
different campaigns is how phases 2 and 3 went wrong, so the baseline is
re-run inside this campaign.

- 5,000 nodes, three manager voters, `topology = "lanes"`, identical binary
  and identical allocations-per-node across cells.
- **Every cell must report `driver_bound: false`.** A cell that does not is
  discarded, not interpreted. This is the defect that invalidated three
  campaigns; the rig now reports the flag, so there is no excuse for
  repeating it.
- Record per cell: throughput, p50/p99 placement latency, drain time,
  `plan_phase_seconds{phase}`, `coalesce_*{path="plan_apply"}` (especially
  `batch_size` and follower wait), `raft_ops_total{lane,op,outcome}`,
  `eval_claim_total{outcome}`, `store_read_lock_wait_seconds`,
  `store_write_lock_wait_seconds`, manager CPU, **and manager RSS**.
- Compute **effective planner concurrency** (evals per second x plan
  latency) for every cell and put it next to the configured thread count.
  The 16-configured/3.96-effective gap at 100,000 allocations is the thing
  this sweep most needs to explain.
- Repeat the winning and neighbouring cells at a second fill (100,000
  allocations) to see whether the knee moves with live allocation count. It
  probably does — throughput already degrades ~1.6x per doubling of live
  allocations — and a default that is right only at one fill is not a
  default.

**Output:** a findings section in `internal_docs/` with the table, and a
recommended constant with its reasoning. **Exit criterion:** the sweep shows
a clear maximum or a clear plateau. If throughput is still climbing at 64,
extend the sweep before proceeding rather than shipping the edge of the
range.

**Independently valuable:** yes, regardless of what happens next. It is the
first honest measurement of Legion's scheduler concurrency response, and the
plan-apply and coalescer work both need it.

### Phase 1 — Set the default

Replace `default_eval_threads()`'s body with the constant Phase 0 chose;
write the doc comment with the campaign, the date, the cell table, and the
fill the number was measured at. Add the unit tests in §9. Write
`internal_docs/calls/eval-thread-default-plan_calls.md` with, at minimum:
the value chosen and the alternatives at their measured cost, why a flat
constant beat a host-derived expression, and the small-deployment trade-off.

**Independently valuable:** yes — the whole user-facing win is here.

### Phase 2 — Make it legible

- `source=default|config` on the eval-worker startup log line.
- Update [docs/diagnosing-scheduler-performance.md](../../docs/diagnosing-scheduler-performance.md):
  the knobs table row for `evalThreads` gains the new default and the sweep's
  numbers; the "throughput fell when I scaled up" section keeps its
  correction block and gains a pointer to the sweep. **Do not re-introduce
  the "lower `evalThreads`" advice** — it is correct only under a high
  rejection share, and the page already says so in exactly the right terms.
- Update `configs/manager.ncl` — it pins `evalThreads = 4` today, which is
  the value the sweep is expected to retire — and any docs stating the
  default is the core count.
- Update `internal_docs/TODO.md` in the same change.

**Independently valuable:** yes — it works against the old default too, and
an operator who cannot tell a default from their own setting cannot debug
either.

### Phase 3 — Hand off to the adaptive plan *(optional, no code here)*

Feed the sweep's response curve into
[adaptive-scheduler-concurrency-plan.md](adaptive-scheduler-concurrency-plan.md)
Phase 0. If the curve is flat over a wide range, a controller has little to
win and that plan should say so; if it is sharp and fill-dependent, that is
the strongest argument yet for closing the loop, and this sweep is its
baseline.

---

## 9. Testing Plan

**Unit (Phase 1)** — `src/structs/config.rs`:

- `eval_threads_default_is_a_measured_constant_not_the_core_count` — asserts
  the default equals `DEFAULT_EVAL_THREADS` and is **not** computed from
  `available_parallelism()`. This is the regression that matters: the failure
  mode is somebody "simplifying" it back to the host property.
- `eval_threads_default_applies_only_when_absent` — deserialize a manager
  config with `evalThreads` set and confirm the explicit value survives;
  deserialize one without it and confirm the constant appears.
- `eval_threads_default_is_within_the_swept_range` — asserts the constant is
  one of the values Phase 0 actually measured, so a later edit cannot drift
  it to an untested number without changing the test and noticing.

**Integration (Phase 1)** — there is no daemon-level test today that asserts
the spawned worker count follows the configured value (`daemon.rs`'s tests
cover the worker *loop*, not the pool size). Add one, covering both the
explicit-config and default paths, so the constant is exercised end to end
rather than only in serde.

**Regression (Phase 2)** — assert the `source` field on the
`scheduler_started` event, which is inspectable from the store without
capturing logs, rather than on the log line.

**Benchmark (required by [AGENTS.md](../../AGENTS.md))** — this is a
scheduler change, so it ships with before/after numbers. The relevant
numbers are the Phase 0 sweep itself, not
`benchmarks/micro/scheduler_bench.rs`: the micro-benchmarks run one thread
against a local store with raft off, where `Store::replicated`
short-circuits and eval-worker concurrency is structurally unmeasurable. Run
the micro-benchmarks as a no-regression check and say so explicitly in the
PR rather than quoting them as a result — the same reasoning as
[eval-path-raft-cost-plan_calls.md](../calls/eval-path-raft-cost-plan_calls.md)
entry 7.

**Not tested, deliberately:** that the chosen value is optimal on any
particular hardware. It is a default, measured on one representative shape;
the docs tell operators how to check it on theirs.

---

## 10. Rollout and Backward Compatibility

- **Existing clusters with `evalThreads` set explicitly: nothing changes.**
  The `serde` default only fires on an absent key.
- **Existing clusters relying on the default: the worker count changes on
  restart.** On the campaign's 4-vCPU managers that is 4 → the new constant.
  The behaviour is the measured one (more throughput, lower p99), and the
  costs are more parked threads, more read connections (capped at 16), and
  more submitters queueing at the plan-apply coalescer.
- **The one cluster shape that could regress** is one already running at a
  high plan-apply rejection share, where extra planners cost more than they
  place. Late-binding two-pass bind drove measured rejections to 0% at 5,000
  nodes, so this is not the expected case — but it is the case the docs must
  name, with the metric to check
  (`legion_plan_apply_total{outcome="rejected"}`) and the remedy (set
  `evalThreads` explicitly). The escape hatch is the knob itself, present
  since before this plan, needing only a restart.
- **No migration, no feature gate, no version skew concern.** The setting is
  manager-local; managers in a raft cluster may run different values during
  a rolling restart with no correctness consequence, only differing
  per-manager scheduling rates.
- **Upgrade note** in `docs/upgrades.md` for the release that carries it:
  one line saying the default changed, from what to what, and that an
  explicit setting is unaffected.

---

## 11. Open Questions

1. **Where is the knee — 16, 32, or 64?** *Unmeasured. This is Phase 0 and
   it gates everything.* Current lean: the response is not monotonic and
   flattens well before 64, because the plan-apply commit is serialized and
   the coalescer's batch size was 1.32 at 16 threads — more submitters
   waiting on a lock they are not batching against buys latency, not
   throughput. Lean toward the low end of what the sweep permits.
2. **Why did 16 configured workers produce ~4 effective ones?** Throughput ×
   plan latency gives 3.96 at 100,000 allocations and ~2.9 at 200,000. The
   read pool is ruled out (§5). Remaining candidates: the scheduler lane's
   single `pipeline_loop` thread serializing claims; `claim_eval` and
   `complete_and_claim_next` being called synchronously from the async loop
   (`daemon.rs:1719`, `:1768`) rather than from `spawn_blocking`, so each one
   parks a tokio runtime worker and there are `available_parallelism()` of
   those; or simply insufficient queue depth. **These have different
   answers**, and the third would mean the knee is a property of the rig.
   Lean: the runtime-thread one is real and worth fixing on its own merits
   regardless of what the sweep says.
3. **Does the knee move with live allocation count?** Almost certainly —
   throughput degrades ~1.6x per doubling of live allocations and plan time
   grew only 22% across the same step, so the cost is elsewhere and may
   shift the optimum. If the knee moves a lot, a single static default is
   the wrong answer and N1's non-goal (an adaptive controller) becomes the
   real plan. Lean: measure at two fills and see.
4. **What does the new default cost in memory?** Each eval worker is a
   parked thread, and the read pool now scales with `evalThreads` up to 16
   connections at a 64 MiB page-cache cap each. Lean: negligible on a small
   cluster because the cache is a lazily-filled cap, but it is measured in
   Phase 0, not asserted.
5. **Should the default be clamped on very small hosts?** A flat 32 on a
   1-vCPU dev VM is defensible but slightly silly. Lean: no clamp unless
   Phase 0's small-cluster RSS number says otherwise — a clamp reintroduces
   a host-derived term, which is the thing this plan is removing.

---

## 12. Acceptance Criteria

**Phase 0**

- A0.1 A sweep of at least 4/8/16/32/64 eval threads at 5,000 nodes, every
  cell reporting `driver_bound: false`, in one campaign on one binary.
- A0.2 A published table with throughput, p50/p99, drain, plan phases,
  coalescer batch size and follower wait, raft ops by outcome, read/write
  lock wait, CPU, and RSS per cell.
- A0.3 Effective planner concurrency computed per cell and stated next to
  the configured count, with the gap either explained or explicitly named as
  unexplained.
- A0.4 Idle and loaded manager RSS at the recommended value against RSS at
  4 threads.
- A0.5 A recommended constant with its reasoning, and the sweep repeated at
  a second fill for the winner and its neighbours.

**Phase 1**

- A1.1 `default_eval_threads()` contains no reference to
  `available_parallelism()`.
- A1.2 The three unit tests in §9 pass, including the one asserting the
  constant is a swept value.
- A1.3 A manager config with an explicit `evalThreads` produces exactly that
  many workers; one without produces the constant.
- A1.4 `internal_docs/calls/eval-thread-default-plan_calls.md` exists,
  ranked, with the value's alternatives at their measured cost.

**Phase 2**

- A2.1 Manager startup logs the worker count with `source=default|config`,
  and the `scheduler_started` event carries the same field, asserted by a
  test.
- A2.2 `docs/diagnosing-scheduler-performance.md` states the new default and
  the sweep's numbers, still tells operators to lower `evalThreads` **only**
  under a high rejection share, and retains the 2026-08-05 correction block.
- A2.3 No document in the repo claims the default is the core count.
- A2.4 `internal_docs/TODO.md` updated in the same change, with anything
  deferred added as a new open item.
