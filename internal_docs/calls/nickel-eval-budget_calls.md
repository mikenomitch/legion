# Nickel Evaluation Budget: Judgement Calls

No plan: this closed the TODO.md item "Nickel evaluation has no memory or step
bound" as a bug fix in `crates/common/src/config_lang.rs`. Public doc:
[configuration-language.md, "Evaluation limits"](../../docs/configuration-language.md#evaluation-limits).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. The bound is a deterministic step budget, enforced through Nickel's cache

**Call.** Every evaluation runs under 1,000,000 steps, counted by wrapping
Nickel's evaluation cache (`BudgetedCache`): each thunk lookup
(`get_update_index`) and each new thunk (`add`) is a step. A spent lookup
returns Nickel's "blackholed" error, which ends the evaluation normally; a
spent allocation unwinds with a private payload that the evaluation thread
catches. The evaluation stops on its own thread, so nothing keeps running.

```text
evaluation did not finish within 1000000 steps; the program most likely never ends, such as a
field defined in terms of itself (`host = { oneOf = [host] }`) or a recursive function with no
base case
```

**Alternatives.** (a) A wall-clock deadline around a thread, as the manager's
template path already has (calls #14 of the 2026-10-09 audit). (b) A memory
limit read from the process RSS every few thousand steps, with a much larger
step budget for CPU. (c) Evaluate in a killable child process.

**Trade-off.** The budget bounds memory only through the steps that build it:
a runaway holds up to a few hundred megabytes (about 250 MB for the
self-referencing field, measured) before it stops. And a legitimately heavy
program can hit it: sorting 5,000 generated records takes about 46 million
steps. The number is fixed, with no flag or env var; raising it means a
release.

**Why (simplicity).** A step count is deterministic: a config that evaluates on
a laptop evaluates on the manager, in debug and release, under load or not. A
deadline (a) cannot stop the work it abandons, which was the original problem,
and fails the same config on a slow box. RSS (b) is process-wide (other
threads' allocations count), platform-specific (`/proc` on Linux, peak only on
macOS), and noisy. A child process (c) needs the re-exec plumbing the audit
already rejected. Real configs sit far inside the budget: `maximal.ncl` takes
about 3,500 steps and a 1,000-task generated jobspec about 140,000; a test
fails if `maximal.ncl` ever needs more than 1% of it.

## 2. Every evaluation gets its own thread with a 1 GiB stack reservation

**Call.** `on_eval_thread` runs each evaluation on a scoped thread asking for a
1 GiB stack, falling back to 64 MiB when the system refuses the reservation.

**Alternatives.** Evaluate on the caller's thread, as before.

**Trade-off.** One thread spawn per evaluation (microseconds), and on a host
that refuses the reservation a stopped runaway can still overflow the 64 MiB
fallback and abort the process. That is documented as a known limitation.

**Why (correctness).** Measured: a recursive function with no base case
(`let rec f = fun x => f (x + 1) in f 0`) builds a chain of pending `x + 1`s
that Nickel frees recursively when the budget stops it. At 1M steps the
release build needs between 64 and 256 MiB of stack, and the debug build more;
on a default 2 or 8 MiB stack, stopping the runaway would turn an
out-of-memory hang into an immediate stack-overflow abort. The reservation is
virtual, and only what the drop touches is ever committed.
