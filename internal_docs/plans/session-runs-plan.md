# Plan: Session Runs: One-Shot Code Execution on Any Session Task

> **Judgement calls:** [session-runs-plan_calls.md](../calls/session-runs-plan_calls.md)

**Status: SHIPPED** (Phases 1 and 2, 2026-11-10; written 2026-11-07). Public
page: [docs/session-runs.md](../../docs/session-runs.md).

Built: the `run` block; `Manager.RunSession` and `Worker.RunSession` on
`exec`, `container`, and `sandbox` session tasks; the five steps; the reply
with stopped runs as data; per-node idempotency with the reply kept in
`sessions.db`; `--dry-run`; `legion session run`; the MCP tool
`legion_session_run`; and the `Workload` service on the per-allocation API
socket (apps calls entry 130).

Not built, and why:

- **Phase 3 (prepared VMs).** It needs sandbox-scaling Phase 8, which is not
  built. A sandbox run boots its VM.
- **The isolate fields** (`--source`, `tier`, `tierReason`, `nextTier`, and
  isolate tasks answering `RunSession` natively). They belong to the isolate
  runtime, which does not exist; the proto has none of them. *Update
  2026-11-11:* built with the [isolate runtime](isolate-runtime-plan.md),
  except `--source`: an isolate run executes the files it is sent
  (`main.<ext>`, else the first). It also added `tenant`, `session_id`, and
  `args` ([isolate calls entry 40](../calls/isolate-runtime-plan_calls.md#40-runs-gained-tenant-session_id-and-args)).
- **Sibling runs over the API socket.** The call is served, but only the
  child-sessions plan's `children.tasks` can declare that one task may run
  another (calls entry 14).

Where the build differs from the text below: output beyond `run.output` is
truncated rather than a stop (calls entry 10); files are staged and read
through an exec in the session on every runtime (entry 6); on `exec` the run
directory is `<task dir>/run` (entry 7); `--get` and `--file` stay inside
the run directory (entry 8); `run.output` is at most 1 MiB (entry 9); the
MCP tool is `legion_session_run`; and the sandbox path is covered by an
`#[ignore]`d test, since CI has no KVM (the container path was run for real
with runc).

A **run** is one call that executes something in a fresh, single-use
session and returns its outcome: start the session, put the caller's files
and input in place, execute the task's run command, collect stdout,
stderr, the exit status, an optional JSON result, and named output files,
end the session, and answer. It works on every session task: `exec`,
`container`, `sandbox`, and the [isolate runtime](isolate-runtime-plan.md),
whose `RunIsolate` it replaces.

```console
$ legion session run code-runner --task machine --file main.py -j
{"ok":true,"data":{"task":"machine","node":"w-17","sessionId":"machine-q4n7c2rd.01jc0...",
 "exit":0,"stdout":"42\n","stderr":"","result":{"rows":1204},"wallMs":412,
 "stopped":null,"deduplicated":false}}
```

Deliberately deferred, and named in §4: streaming output while a run is in
flight, runs that keep their session afterwards (start a session and
`exec` instead), and runs of keyed or service tasks.

---

## 1. Problem statement

Running a piece of code in a sandbox and getting its output back is the
most common thing an agent platform does with one, and Legion makes it
three calls and a cleanup:

1. `legion session start` (or `instance create`) and wait for `running`;
2. `legion exec` with the command, after copying files in somehow;
3. `legion session stop`, and on any failure in between, stop it anyway or
   leak it.

Each step can fail on its own, so every platform writes the same
start-exec-collect-stop wrapper, with its own idea of output limits,
timeouts, idempotency, and cleanup. The isolate runtime needed exactly this
call (`RunIsolate`) for isolates and for its VM tier, which showed the
wrapper belongs in Legion, once, for every runtime.

## 2. Prior art

- **`docker run --rm`, `kubectl run --rm -i`.** Create, attach, wait, and
  delete in one command.
  - **Take:** the lifecycle in one call, with the unit gone afterwards.
  - **Reject:** attaching a terminal; a run's output is captured and
    returned as data.
- **E2B and Modal sandboxes** (`sandbox.run_code`, `Sandbox.exec`) and
  **Cloudflare's Sandbox SDK** (`exec`). Code execution as an API call
  against a sandbox, returning stdout, stderr, and results.
  - **Take:** structured results (a JSON value next to the text streams).
  - **Reject:** keeping the sandbox around by default; a run is single-use.
- **AWS Lambda `Invoke`** (`RequestResponse`). One call, one fresh-or-warm
  environment, a result payload, and an error type that distinguishes the
  function failing from the invoke failing.
  - **Take:** the distinction between "the code ran and failed" and "the
    request failed" (§6.4).
  - **Reject:** reusing an environment across invocations; a run's session
    is never reused (§6.3).
- **Nomad `alloc exec` and dispatch jobs.** Exec needs a running
  allocation; a dispatched batch job is a raft object per run.
  - **Reject:** a control-plane object per run.

## 3. Jobs to be done, by persona

**Agent platform builder**
- When a model writes code, I want to run it in a fresh sandbox with one
  call and get back its output and result, so I do not maintain my own
  start-exec-stop wrapper and its leaks.
- When I retry a call that timed out, I want the retry not to run the code
  a second time, so non-idempotent snippets are safe to retry.

**Job author**
- When I ship a task that processes a file and returns an answer, I want
  callers to hand it the file and get the answer, so I do not stand up an
  HTTP server inside a sandbox for one request.

**Security / platform admin**
- When callers can run code in my task, I want them to run only the
  command the jobspec names unless I grant more, so "run my code" is not
  "run anything as root in the VM".

**Autonomous agent**
- When I run code, I want to tell "my code failed" from "Legion could not
  run it" in the structure of the reply, so I fix the right thing.

*Not in scope:* the on-call responder; a run leaves nothing behind to
inspect beyond its events and the reply.

## 4. Goals and non-goals

### Goals

- One call, any session task, the same reply shape.
- Single-use: a run's session serves that run and ends.
- No raft write per run.
- Idempotent per node, with the reply kept for retries.
- The task's own run command by default; any other command only with the
  `exec` permission.

### Non-goals

- **Streaming output** during a run. The reply comes at the end; long jobs
  start a session and `exec` with a stream.
- **Keeping the session.** A caller who wants the machine afterwards
  starts a session.
- **Keyed and service tasks.** A run is a session; keyed instances are
  durable by design.
- **Choosing between tasks.** One run, one task. The isolate runtime's
  `tier = "auto"` is a choice the isolate task makes before the run starts
  (isolate plan §6.12), on top of this.

## 5. Background: what exists today

- **Session mode** ([docs/session-mode.md](../../docs/session-mode.md)):
  starts with no raft write, routed by a manager's session router or at the
  edge with a session ticket (`Worker.StartSession`), waiting for `running`
  by default.
- **Exec** ([docs/exec-and-actions.md](../../docs/exec-and-actions.md)) into
  `exec`, `container`, and sandbox allocations, sandboxes over the guest
  agent's vsock; the `exec` ACL resource gates it.
- **Actions** run a task-declared command with typed inputs, which is the
  precedent for "the jobspec names the command, the caller supplies
  inputs".
- **Edge idempotency**: an idempotency key derives the id, so a retry to the
  same node finds the same unit (`deduplicated = true`).

## 6. Proposed design

### 6.1 Declaring that a task takes runs

A session task opts in with a top-level `run` block:

```nickel
{
  name = "machine",
  runtime = "sandbox",
  config = { image = "python:3.13-slim" },
  stateful = { mode = "session", envelope = { resources = { cpu = 8000, memory = 16384 } } },
  run = {
    command = ["python3", "/legion/run/main.py"],   # what a run executes
    wall = "5m",                                     # per run (required)
    output = "1MiB",                                 # stdout + stderr returned (default)
    allowCommand = false,                            # callers may not replace command (default)
  },
}
```

An isolate task takes runs without a `run` block: its image runs the
source, and its `config.limits` apply (isolate plan §6.11).

### 6.2 What a run does

1. **Admit** a fresh session of the task against its envelope, on a node the
   session router picks (or the edge node called), exactly as an unnamed
   session start. A prepared VM is used when the node has one (§6.3).
2. **Stage** the caller's files and input under `/legion/run/` (the task
   directory for `exec` and `container`, the guest over the agent for
   `sandbox`): `--file` entries by name, and `--input` as
   `/legion/run/input.json` with its path in `LEGION_RUN_INPUT`. `--source`
   is for isolate tasks; when an isolate task sends a run to its VM task,
   the source lands as `main.py` or `main.mjs`, named for the isolate's
   image.
3. **Execute** `run.command` (or the caller's `--command`, §6.5) once, as an
   exec in the running session, with `run.wall` as its deadline.
4. **Collect** exit status, stdout and stderr up to `run.output`
   (`truncated` set beyond it), `/legion/run/result.json` as `result` when
   the command wrote one, and each `--get <path>` the caller named, up to
   the same limit.
5. **End** the session, always, including after a failure in any step.

### 6.3 Single-use, always

A run's session is never reused for another run, because the next run may
be another tenant's code. A "warm" start means a prepared VM, restored from
a base snapshot and never assigned before (isolate plan §6.12, scaling plan
Phase 8), not a VM some earlier run used.

### 6.4 The reply: a stopped run is data

| Outcome | Envelope |
| --- | --- |
| The command ran and exited (any status) | `ok: true`, `exit` |
| The run was stopped by a limit (`wall`, `output`, memory), or an isolate stopped it as `unsupported` | `ok: true`, `exit: null`, `stopped { reason, limit, detail, hint }` |
| The run could not start or could not finish: no room, permission denied, invalid request, the node lost mid-run | `ok: false`, the standard error envelope (`RESOURCE_EXHAUSTED` and `UNAVAILABLE` retryable) |

The CLI exits non-zero for a non-zero `exit` and for a stopped run, so
scripts keep working. The isolate runtime adds `tier`, `tierReason`, and
`nextTier` to the same reply.

### 6.5 Who may run what

- **`job:write`** on the job runs the task's `run.command` with the caller's
  files and input. That is the action precedent: the jobspec names the
  program, the caller supplies its inputs.
- **`--command`** needs `run.allowCommand = true` on the task **and**
  `exec:write` on the job, the permission `legion exec` already needs,
  because an arbitrary command is an exec.

```text
PERMISSION_DENIED: --command needs exec:write on job code-runner (running the task's own run.command needs only job:write)
FAILED_PRECONDITION: task machine does not allow --command (run.allowCommand = false)
```

### 6.6 The surface

```console
$ legion session run <job> [--task T] [--file F]... [--source -] [--input JSON]
      [--get PATH]... [--command ARG...] [--idempotency-key K] [--dry-run]
      [--node URL --ticket T] [-j]
```

- **RPCs.** `Manager.RunSession` (routed by the session router, nothing
  written) and `Worker.RunSession` on the worker-plane port with a session
  ticket, for edge callers. `RunSessionRequest`: `job_id`, `task_name`,
  `files`, `source`, `input`, `get`, `command`, `idempotency_key`,
  `wait_timeout`, plus the isolate's `tier`. `RunSessionResponse`: `node_id`,
  `session_id`, `exit`, `stdout`, `stderr`, `truncated`, `result`, `files`,
  `wall_ms`, `cpu_ms`, `stopped`, `deduplicated`, plus the isolate's fields.
- **`--dry-run`** answers which node would take the run and whether the
  envelope has room, and for an isolate task what `tier = "auto"` would
  choose, without running anything.
- **Idempotency** is per node, as for edge starts: the key derives a run id,
  and the node keeps the reply for 10 minutes. A retry that lands on
  another node runs again; keep retries on the node the first attempt named.
- **Agents.** An MCP tool `session_run` (`mutates: true`, behind
  `--allow-write`), `catalog()` entries, and `docs/agent-readiness.md`.

## 7. Proto, struct, config, and persistence changes

- **Jobspec:** a top-level `run = { command, wall, output, allowCommand }`
  on session tasks; into `configs/jobs/maximal.ncl`.
- **Proto:** `RunSession` on Manager and Worker, the request and response
  above, `RunStop { reason, limit, detail, hint }`. Full proto comments.
- **Worker state:** recent replies by run id in `sessions.db`, kept 10
  minutes. Nothing in the store.
- **ACL:** `RunSession` checks `job:write`, then `exec:write` when `command`
  is set; both in `tests/integration/acl_endpoints.rs`.

## 8. Implementation phases

- **Phase 1: `exec` and `sandbox`.** The `run` block, the five steps, the
  reply, idempotency, `--dry-run`, the CLI, both RPCs, MCP and catalog
  entries, and the public page section.
- **Phase 2: `container`.**
- **Phase 3: prepared VMs.** Runs on sandbox tasks take a prepared,
  never-used VM when the node has one (needs scaling plan Phase 8).
- **With isolate Phase 1:** isolate tasks answer `RunSession` natively.

## 9. Testing plan

- **Unit:** staging paths per runtime; output truncation; reply
  classification (exit, stopped, error); the permission matrix for
  `--command`.
- **Integration (`tests/integration/session_runs.rs`):** a run on an `exec`
  task returns stdout and `result.json`; a wall-limit run is `ok: true` with
  `stopped`; a failure between start and collect still ends the session
  (no leaked session in `session nodes`); a retry with the same key on the
  same node answers `deduplicated`; `--command` refused without
  `exec:write`; ACL table entries.
- **KVM (ignored by default):** the same on a `sandbox` task, files in and
  `--get` out through the guest agent.

## 10. Rollout and backward compatibility

New, opt-in surface. A session task without `run` refuses `RunSession` with
`FAILED_PRECONDITION` naming the block to add. Nothing existing changes.

## 11. Open questions

1. **Should `wall` have a default?** Lean: no; a webhook and a build differ
   by three orders of magnitude, as for isolates.
2. **Should a run's output files have their own limit?** Lean: share
   `run.output` for now; a separate `run.files` limit if callers hit it.
3. **Streaming.** Lean: not in this plan; `exec` already streams.

## 12. Acceptance criteria

- **Phase 1:** `legion session run` on an `exec` task and a `sandbox` task
  returns stdout, `exit`, and `result` in one call; no failure path leaves a
  session running; a stopped run is `ok: true` with `stopped`; an
  idempotent retry is answered without running again.
- **Phase 2:** the same on `container`.
- **Phase 3:** a sandbox run's p50 start falls to the prepared-VM target
  measured by scaling Phase 8, and no prepared VM serves two runs.
