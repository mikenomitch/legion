# Plan: A Deadline on Eval Handlers

> **Judgement calls:** [`calls/eval-handler-deadline-plan_calls.md`](../calls/eval-handler-deadline-plan_calls.md)

**Status: SHIPPED** (2026-11-12). Public doc:
[docs/scheduler-and-eval-loop.md, "Handler deadline"](../../docs/scheduler-and-eval-loop.md#handler-deadline).
Deliberately not built: a configurable deadline, a delivery limit after which
a repeatedly stuck eval fails, fencing for the settle a plan carries itself,
and any way to stop a stuck handler's thread.

## 1. Problem statement

Eval claims are leader memory (`crates/manager/src/storage/store/eval_claims.rs`).
A worker claims a batch of up to 16 evals, runs their handlers one at a time
on tokio's blocking pool, and settles the batch in one write
(`eval_worker_loop`, `crates/manager/src/manager/daemon.rs`). It waited on
each handler with no bound.

A handler that never returns (a deadlock, a blocking call with no timeout)
therefore held:

- its own eval's claim, so no other worker would run it;
- every claim behind it in the batch, which never started;
- the worker itself, so `evalThreads` shrank by one for good;
- its admission permit, when adaptive concurrency enforces limits.

Claims are only forgotten when the manager loses or gains scheduler-lane
leadership. On a stable leader that never happens, so the work sat claimed
forever, shown as `in_flight` in `legion eval list`, with nothing in the
metrics to say why. The claim-epoch fence (2026-11-08) already made a late
settle from such a handler safe. What was missing was anything that stopped
waiting on it.

## 2. Prior art

- **Nomad's eval broker** hands an eval to a scheduler worker with a nack
  timer (`nack_timeout`, 60s by default). A worker that is still working
  resets the timer (`Eval.OutstandingReset`). A worker that neither acks nor
  resets is assumed dead, and the eval is redelivered. After
  `delivery_limit` (3) deliveries the eval goes to the failed queue, and a
  periodic reaper creates a follow-up eval for it later.
- **Kubernetes controllers** (`client-go` workqueue) have no per-item
  deadline. A stuck reconcile holds its worker forever. Controller authors
  are expected to put timeouts on their own calls.

We take Nomad's idea that the loop, not the handler, decides when to stop
waiting, and that the eval is delivered again rather than lost. We don't take
its timer resets: Legion's handlers have a bounded legitimate worst case (§5),
so a fixed deadline well above it needs no heartbeat. We also don't take the
delivery limit (judgement call 2).

## 3. Jobs to be done

**Cluster operator.**
- "When something wedges the scheduler, I want the rest of the queue to keep
  moving, not stall behind it."
- "I want to see that a handler got stuck, and which kind, so I can report it."

**Job author.** "A job I submitted should get placed even if some unrelated
eval got stuck on the same worker."

**Legion developer.** "A hang bug should show up as a counter and a log
line, not as a mysteriously smaller worker pool."

Scenarios:
1. A rollout tick deadlocks on a lock. Four job submits are queued behind it
   in the same batch. Before: they wait until the next leader election. After:
   they run five minutes later, and the tick is retried after another five.
2. A raft lane loses quorum for a minute. Handlers block on proposals and fail
   within `LEADER_WAIT` (15s). The deadline never fires; nothing changes.

## 4. Goals and non-goals

Goals:
- A stuck handler releases its batch within a fixed, documented bound.
- The stuck eval runs again; acknowledged work is never dropped.
- Every timeout is visible: a metric by kind, an event, a `warn` log.

Non-goals:
- Stopping the stuck thread. Blocking tasks can't be cancelled, and handlers
  hold SQLite connections and raft waits that have no cancellation points.
- Making slow handlers faster, or bounding conflict retries more tightly.
- A tunable. See judgement call 3.

## 5. Background: a handler's legitimate worst case

- Plan-conflict retries: at most `plan_conflict_max_retries` attempts, clamped
  to 20 (`Eval::max_plan_conflict_retries`, `crates/manager/src/manager/eval.rs`),
  with backoffs capped by `plan_conflict_backoff_max_ms`.
- A raft write that can't reach a leader fails within `LEADER_WAIT`, 15s
  (`crates/manager/src/storage/raft/driver.rs`).
- Planning itself is milliseconds to tens of milliseconds per job at 50k
  nodes (`docs/benchmarks.md`).

Five minutes is more than an order of magnitude above all of that combined.

## 6. Design

1. **The deadline.** The loop awaits each handler's `JoinHandle` under
   `tokio::time::timeout(EVAL_HANDLER_DEADLINE)`, 300s. On timeout it drops
   the handle, which detaches the blocking task.
2. **The outcome.** A timed-out handler gets
   `EvalCompletion::Released { delay: 300s, event }`, the same outcome
   plan-conflict exhaustion uses. It rides the batch's ordinary settle, so the
   claim-epoch fence applies, the claim is released, and the row goes back to
   `pending` with `ready_at` five minutes out.
3. **The rest of the batch** runs as normal after the timed-out member.
4. **Visibility.** `legion_eval_handler_timeouts_total{kind}`; an
   `eval.requeued` event with `meta.reason: "handler_timeout"` and
   `meta.deadlineMs`; a `warn` log, `Eval handler ran past its deadline;
   requeueing the eval and moving on`. Conflict requeues now carry
   `meta.reason: "plan_conflict"` so the two can be told apart.

What an operator sees:

```text
$ curl -s localhost:4747/metrics | grep handler_timeouts
legion_eval_handler_timeouts_total{kind="rollout_tick"} 1

$ legion events list --kind eval.requeued --level warn
... eval.requeued  Eval 4121 (rollout_tick) handler ran past the 300s handler
    deadline; requeued, retrying in 300000ms
```

## 7. Proto, struct, config, and persistence changes

None. No new RPC, field, config key, or table. One new metric family and one
new `meta` key on an existing event kind.

## 8. Phases

One phase, shipped: the deadline, the requeue, the metric and event, tests,
and docs.

## 9. Testing

- `eval_handler_past_its_deadline_is_not_waited_on_test`: a blocking handler
  past a 50ms deadline is reported as timed out promptly; one that returns
  is not; a panic is still a failure.
- `eval_handler_timeout_requeues_the_eval_with_a_delay_test`: the timed-out
  completion settles through a real store. The row is `pending`, unclaimed,
  not claimable before the delay, and has an `eval.requeued` event with
  `reason: handler_timeout`. The counter moves.

## 10. Rollout

On by default with no switch. No existing behavior changes unless a handler
runs for five minutes, which no healthy handler does.

## 11. Acceptance criteria

- A handler stuck past five minutes no longer blocks the rest of its batch.
- The stuck eval runs again after the delay.
- `legion_eval_handler_timeouts_total` and `eval.requeued` with
  `reason: handler_timeout` record each timeout.
