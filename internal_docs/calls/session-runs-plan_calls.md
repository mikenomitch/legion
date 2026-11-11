# Judgement Calls: session-runs-plan

Decisions made while designing
[`plans/session-runs-plan.md`](../plans/session-runs-plan.md). **Entries
are ordered by project impact, highest first, and an entry's number is its
rank.** Entries 1 to 5 are design-time calls. Entries 6 onward were made
while building Phases 1 and 2 (2026-11-10); ranked against the design calls,
each landed below them.

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
entry 18 records the move).

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

---

## 6. Staging, the command, and collection are all execs into the session

*Added 2026-11-10, building Phase 1.*

**Call.** A run reaches its unit one way on every runtime: the worker's
existing exec path (`open_exec_session`). Files are staged with `sh -c
'mkdir -p ...; cat > "$1"; chmod ...'` and the file on stdin, and read back
with `head -c`.

```text
FAILED_PRECONDITION: could not stage main.py at /legion/run/main.py in the run's session (exit 127): sh: not found
Hint: staging needs /bin/sh, mkdir, cat and chmod in the task's image, and a writable /legion/run
```

**Alternatives.** (a) The plan's wording: write into the task directory on
the host for `exec` and `container`, and add a file-transfer call to the
guest agent for `sandbox`. (b) Bind-mount a host run directory into
containers and attach a disk to sandboxes.

**Trade-off.** `container` and `sandbox` runs need a shell and coreutils in
the image, so a distroless image cannot take runs; each file costs an exec.

**Why (simplicity, extensibility).** One code path is tested end to end on
`exec` in CI and on `container` with runc, and the sandbox path differs only
in the plugin's exec, which already works over the guest agent. (a) needs
three stagers and a guest protocol change; (b) changes how every session
unit is built for a feature most never use. A runtime that gains exec gains
runs.

---

## 7. On `exec`, the run directory is `<task dir>/run`, and `/legion/run` is rewritten in argv

*Added 2026-11-10, building Phase 1.*

**Call.** An `exec` session shares the host's root, so it has no
`/legion/run`. Its run directory is `<task dir>/run`; an argv element that
starts with `/legion/run` is rewritten to it; `LEGION_RUN_DIR` and
`LEGION_RUN_INPUT` carry the real paths on every runtime; and on `exec` the
command's working directory is the run directory.

```nickel
run = { command = ["python3", "/legion/run/main.py"], wall = "5m" }   # works on exec, container, sandbox
```

**Alternatives.** (a) Create `/legion/run` on the host. (b) Refuse `run` on
`exec`. (c) No rewriting: make authors write `$LEGION_RUN_DIR` and run a
shell.

**Trade-off.** A script that hard-codes `/legion/run/...` inside its body
works in a container and fails on `exec`; the public doc says to use
`$LEGION_RUN_DIR` there.

**Why (ease, security).** (a) is one directory shared by every session on
the node. (b) drops the runtime CI can test and the plan's Phase 1. (c)
makes the plan's own example fail on one of the three runtimes.

---

## 8. A run reads and writes only inside its run directory

*Added 2026-11-10, building Phase 1.*

**Call.** `--file` names and `--get` paths are relative to the run
directory, or absolute under `/legion/run/`; anything else, and any `..`, is
`INVALID_ARGUMENT`.

**Alternatives.** Let `--get` name any path in the unit, as the plan's
`--get <path>` could be read.

**Trade-off.** A command that writes its output elsewhere has to copy it
into the run directory.

**Why (security).** The worker reads `--get` files as itself. On `exec` an
unrestricted path is a read of any host file by anyone holding `job:write`,
and an unrestricted `--file` is a write.

---

## 9. A run's request and reply each fit one gRPC message

*Added 2026-11-10, building Phase 1.*

**Call.** `run.output` is at most `1MiB` (also the default); a request
stages at most 3 MiB; `--get` files share one `run.output` budget.

**Alternatives.** (a) Raise the message size limits on every client and
server that carries a run. (b) Stream files in and out.

**Trade-off.** Larger inputs and outputs go through artifacts, volumes, or
an object store the command reaches itself.

**Why (simplicity).** A run passes through the CLI, a manager, the worker
plane, and the API socket; one default-sized unary message works on all of
them unchanged. Streaming is the plan's own non-goal.

---

## 10. Output beyond `run.output` is truncated; it does not stop the run

*Added 2026-11-10. Deviates from plan §6.4, which lists `output` among the
limits that stop a run.*

**Call.** Bytes past `run.output` are dropped and the reply says
`truncated`; the command keeps running to its exit or its wall.
`stopped.reason` is `wall`, `memory`, or `session_ended`.

**Alternatives.** Kill the command when it exceeds the limit.

**Trade-off.** A command that prints without end runs until `run.wall`.

**Why (ease).** Plan §6.2 step 4 already says output is collected "up to
`run.output` (`truncated` set beyond it)". A chatty build that succeeds
should return its exit status and result, not be killed for its log.

---

## 11. A run outlives its caller; a run session without a driver is ended

*Added 2026-11-10, building Phase 1.*

**Call.** The worker drives a run in a detached task, so a caller that hangs
up does not cancel it; with a key, the reply is kept for the retry. A run's
session carries origin `run`, and the session maintenance loop ends any such
session no live driver owns, which is what a worker restart leaves.

**Alternatives.** (a) Cancel the run when the caller disconnects. (b) Record
runs in flight in `sessions.db` and resume them after a restart.

**Trade-off.** An abandoned run uses its envelope room until `run.wall`. A
restart fails runs in flight with `UNAVAILABLE`.

**Why (correctness).** Cancellation between start and end is exactly where
a session leaks, and a retry would then run the code twice. A run cannot be
resumed: its exec died with the worker.

---

## 12. A manager remembers which node took a key, and `node_id` pins a retry

*Added 2026-11-10, building Phase 1. Extends entry 5.*

**Call.** Idempotency stays per node, but a manager keeps `key -> node` in
memory for the reply window and sends a retry there, and
`RunSessionRequest.node_id` (`--on-node`) pins one sent through another
manager.

**Alternatives.** Leave it to the caller, as the plan's "keep retries on the
node the first attempt named" does.

**Trade-off.** The map is per manager and lost on restart; a retry through
another manager without `--on-node` can still run twice.

**Why (ease).** Through a manager the caller does not choose the node, so
without this a keyed retry would be routed afresh and usually run again.

---

## 13. Only a manager can authorize `--command`

*Added 2026-11-10, building Phase 1.*

**Call.** `command` is refused for a session ticket and on the workload
channel, on a task that allows it too.

**Alternatives.** Let `run.allowCommand = true` be enough at the edge.

**Trade-off.** An edge caller that needs its own command goes through a
manager.

**Why (security).** The plan requires `exec:write`, and neither a ticket nor
a socket carries ACLs. Accepting it there would make a session ticket an
exec credential.

---

## 14. The workload channel is its own gRPC service, and admits only what the jobspec declares

*Added 2026-11-10, for apps calls entry 130.*

**Call.** The worker serves a `Workload` service on the per-allocation API
socket, beside the proxied `Manager` service, with `RunSession` as its first
call. It acts for the socket's allocation and asks one function,
`workload_may_run(caller task, target task)`, which admits a task running
itself and nothing else.

**Alternatives.** (a) Admit any session task of the caller's job. (b) Add an
allowlist to the `run` block. (c) Leave the socket a pure proxy.

**Trade-off.** No pair is admitted in practice until a jobspec can declare
one (a session task has no socket), so `legion session run --local` is built
and tested but answers `PERMISSION_DENIED` for every sibling.

**Why (security, extensibility).** (a) would give every task with the
`legion` identity the right to run code in its siblings' sandboxes, which
its ACL does not. (b) would be a second declaration beside the child-sessions
plan's `children.tasks`, which is the declaration this needs. The service
and the hook are where that plan's node-local child starts attach.

---

## 15. Smaller calls

*Added 2026-11-10.*

- **No isolate fields in the proto.** `source`, `tier`, `tierReason`, and
  `nextTier` (plan §6.6) are not in `RunSessionRequest`/`Response`; they
  arrive with the runtime that reads them.
- **A failed run is not kept.** Only a reply is; an error is not replayed to
  a retry, which runs again.
- **`memory` stops come from the cgroup.** A rise in `memory.events`'
  `oom_kill` with exit 137 or a lost session; without a cgroup the same
  event is exit 137 or `session_ended`.
- **Exit status of a stopped run in the CLI:** 124 for `wall` (as
  `timeout(1)`), 125 otherwise.
- **A unit gets 20 seconds to accept its first exec** after it reports
  `running`: a container exists a moment after its runtime is spawned.
- **Phase 3 is not built.** No prepared VMs exist (sandbox-scaling Phase 8).

