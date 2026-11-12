# Judgement Calls: Eval Handler Deadline

Decisions behind
[`plans/eval-handler-deadline-plan.md`](../plans/eval-handler-deadline-plan.md).
**Entries are ranked by project impact, highest first; the number is the
rank.**

## 1. A deadline on the handler's await, not a periodic re-sweep of old claims

**Call.** The eval loop awaits each handler under a five-minute
`tokio::time::timeout`. On timeout it requeues that one eval through the
batch's own fenced settle and carries on with the rest of the batch.

```text
WARN Eval handler ran past its deadline; requeueing the eval and moving on
     eval_id=4121 kind=rollout_tick deadline_secs=300 requeue_delay_secs=300
```

**Alternatives.** (a) What the TODO item named: a background sweep that
forgets claims older than a bound, bumping the claim epoch so a late settle
from the stuck worker is dropped. (b) Per-claim progress timestamps that the
worker touches as it starts each handler, with the sweep keyed on time since
last progress.

**Trade-off.** The deadline only covers the wait on a handler. A worker
stuck anywhere else in the loop (a claim or settle that never returns) is not
covered. Those are single store calls with their own raft and SQLite
timeouts, so that gap is narrow.

**Why (simplicity).** The sweep has to tell "stuck" from "queued behind a
slow sibling in the same batch". A batch of 16 waits for each member in
turn, so claim age says nothing about one handler, which is why (b) needs
per-claim progress tracking. The deadline measures exactly one handler, from
the one place that knows when it started, and it frees the rest of the batch
by letting the same worker go on to run them. That is the work the sweep
would have released for another worker to claim, without a second writer of
claim state and without re-running members that were never stuck.

## 2. A stuck eval is requeued every time, never failed

**Call.** The timed-out eval goes back to `pending` with a five-minute delay
(`EvalCompletion::Released`), however many times it has timed out before.

**Alternatives.** Nomad's `delivery_limit`: after N timeouts, mark the eval
`failed` so a poison eval stops consuming threads.

**Trade-off.** An eval that hangs every time it runs strands one
blocking-pool thread per attempt, roughly every ten minutes. Tokio's
blocking pool is 512 threads, so the pool would be exhausted after about
three and a half days. The metric makes the hang visible long before then.

**Why (simplicity, consistency).** Legion already decided this for plan
conflicts ([late-binding calls #3](late-binding-plan-apply-plan_calls.md)):
"placed" or "visibly waiting" are the honest states, and a terminal state
after M tries is a silent drop under another name. A stuck handler says even
less about whether the eval's work is wanted than a conflict does. Failing it
would also hide a hang bug behind a failed eval that looks like a user error.

## 3. The deadline and delay are constants, not configuration

**Call.** `EVAL_HANDLER_DEADLINE` and `EVAL_HANDLER_TIMEOUT_REQUEUE_DELAY` are
both five minutes, fixed in `crates/manager/src/manager/daemon.rs`.

**Alternatives.** A manager config field (`evalHandlerDeadline`) or a
`schedulerSettings` key.

**Trade-off.** An operator whose handlers really do run longer than five
minutes can't raise it without a rebuild.

**Why (simplicity).** No healthy handler comes near it (the plan's §5 puts
the worst legitimate case well under a minute), so a knob would only ever be
turned to hide a bug. A constant keeps the config surface free of a value
nobody can choose well. If a real long-running handler appears, that is the
time to add the field.

## 4. The requeue reuses `eval.requeued`, told apart by `meta.reason`

**Call.** A timeout emits the existing `eval.requeued` kind with
`meta.reason: "handler_timeout"`. Conflict requeues gained
`meta.reason: "plan_conflict"`.

**Alternatives.** A new event kind, `eval.timed_out`.

**Trade-off.** A consumer that counts `eval.requeued` as "conflict pressure"
now also counts timeouts. They should be rare enough that this doesn't
distort it, and `reason` separates them.

**Why (simplicity).** Both are the same state change: the eval went back to
pending with a delay instead of failing. Consumers already following
`eval.requeued` see stuck evals with no change on their side.
