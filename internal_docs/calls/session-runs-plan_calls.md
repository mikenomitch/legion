# Judgement Calls: session-runs-plan

Decisions made while designing
[`plans/session-runs-plan.md`](../plans/session-runs-plan.md). **Entries
are ordered by project impact, highest first, and an entry's number is its
rank.** These are design-time calls; implementation inserts its own at
their rank.

## 1. Runs are a session primitive for every runtime, not an isolate API

**Call.** `RunSession` (`legion session run`) runs once in a fresh,
single-use session of any session task that opts in with a `run` block, and
replaces the isolate plan's `RunIsolate`. The isolate's tier choice is a
field the isolate task honours on the same call.

**Alternatives.** (a) Keep `RunIsolate`, and let sandbox users keep writing
start-exec-stop wrappers. (b) A separate run API per runtime.

**Trade-off.** The run API carries a few isolate-only fields (`tier`,
`tierReason`, `nextTier`) that other tasks ignore.

**Why (simplicity, composability).** The isolate plan's VM tier is "run
this on a sandbox task", which is the same call sandbox users need today;
building it once gives every runtime the same reply shape, limits,
idempotency, and cleanup, and keeps one verb in the CLI and one tool in MCP.

---

## 2. A run's session is single-use, always

**Call.** A run's session ends with the run and is never reused. A warm
start is a prepared, never-assigned VM, not a used one.

**Alternatives.** Reuse sessions across runs of the same task (Lambda's warm
environments), or of the same tenant key.

**Trade-off.** Every run pays a session start: a cold VM boot today, a
prepared-VM restore once scaling Phase 8 lands.

**Why (security).** The next run may be another tenant's code, and a reused
VM is a channel between them. Callers who want reuse within one tenant
start a session and `exec`, which makes the sharing explicit.

---

## 3. The jobspec names the command; anything else needs `exec:write`

**Call.** A run executes the task's `run.command` with the caller's files
and input under `job:write`. A caller-supplied `--command` needs
`run.allowCommand = true` on the task and `exec:write` on the job.

**Alternatives.** (a) Always accept a command, under `job:write`. (b) Always
require `exec:write`.

**Trade-off.** A platform that wants arbitrary commands sets two things.

**Why (security, consistency).** (a) would make `job:write` an exec grant
through a side door, contradicting the `exec` resource. (b) would make a
code interpreter with a fixed entry point need exec rights it does not use.
The split is the actions precedent: the spec names the program, the caller
supplies inputs.

---

## 4. A stopped run is data; error envelopes are for runs that never ran

**Call.** A run whose command exited, or that a limit stopped, answers
`ok: true` with `exit` or `stopped`. No room, no permission, an invalid
request, or a node lost mid-run is an error envelope.

**Alternatives.** Typed errors for limit stops, with extra fields added to
the standard error envelope.

**Trade-off.** An agent must read `data.stopped`, not only `ok`, to know the
code did not finish. The CLI exits non-zero for both, so scripts are
unaffected.

**Why (agent-readiness).** The standard error envelope carries only `code`,
`message`, `hint`, and `retryable`. A stop is the run's outcome, with stdout,
stderr, and timings an agent needs, which an error envelope would drop. It
is the rule the isolate plan's first draft introduced (isolate-runtime calls
entry 8 records the move).

---

## 5. Idempotency is per node

**Call.** An idempotency key derives a run id on the node that takes it, and
that node keeps the reply for 10 minutes. A retry that reaches another node
runs again.

**Alternatives.** A cluster-wide idempotency record, in raft or the content
store.

**Trade-off.** A caller that retries through a router to a different node
can run code twice; the doc says to keep retries on the node the first
attempt named.

**Why (performance).** A run writes nothing to raft; a cluster-wide record
would add a write per run to make one failure mode impossible. Edge session
starts already made this trade.
