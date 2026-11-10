# Session Runs

A **run** is one call that executes something in a fresh, single-use
[session](session-mode.md) and returns the outcome. Legion starts the
session, puts the caller's files and input in place, executes the task's run
command once, collects stdout, stderr, the exit status, an optional JSON
result, and any files the caller named, ends the session, and answers.

```console
$ legion session run code-runner --task machine --file main.py --input '{"rows": 1204}' -j
{"ok":true,"data":{"task":"machine","node":"w-17","sessionId":"machine-q4n7c2rd.01jc0...",
 "runId":"run-q4n7c2rd.01jc0...","exit":0,"stdout":"42\n","stderr":"","truncated":false,
 "result":{"rows":1204},"files":[],"wallMs":412,"cpuMs":0,"stopped":null,"deduplicated":false}}
```

It exists so that a platform running model-written or user-written code does
not maintain its own start, exec, collect, stop wrapper, with its own leaks:
without it the same thing is `legion instance create`, a wait, a way to copy
files in, `legion exec`, and a stop that has to happen on every failure path.

Runs work on session tasks of the `exec`, `container`, and `sandbox`
runtimes. A run writes nothing to the replicated store.

## Declaring that a task takes runs

A session task opts in with a top-level `run` block:

```nickel
{
  name = "machine",
  runtime = "sandbox",
  config = { image = "python:3.13-slim" },
  resources = { cpu = 1000, memory = 1024 },
  stateful = {
    mode = "session",
    envelope = { resources = { cpu = 8000, memory = 16384 } },
  },
  run = {
    command = ["python3", "/legion/run/main.py"],  # what a run executes
    wall = "5m",                                    # deadline of one run (required)
    output = "256KiB",                              # stdout + stderr returned
    allowCommand = false,                           # callers may not replace command
  },
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `command` | required | The argv a run executes, with no shell. |
| `wall` | required | The command's deadline, at most `24h`. There is no default: a webhook and a build differ by three orders of magnitude. |
| `output` | `1MiB` | The most stdout plus stderr a run returns. `1MiB` is also the maximum. |
| `allowCommand` | `false` | Whether a caller may replace `command` for one run (see [Who may run what](#who-may-run-what)). |

Job validation refuses a `run` block on a task that is not a session task,
on a runtime other than `exec`, `container`, or `sandbox`, and one without a
`command` or a `wall`:

```text
task machine: run needs a session task (stateful.mode = "session"); a run is a fresh single-use session
task machine: run.wall is required: the deadline of one run, e.g. "30s" or "5m"
task machine: run.output 2MiB exceeds the 1MiB maximum: a run's reply is one message; write larger results to a volume or the object store
```

A session task without a `run` block refuses runs with `FAILED_PRECONDITION`,
naming the block to add. Declaring it changes nothing else about the task:
ordinary sessions of it start and behave as before.

## What a run does

1. **Admit.** A fresh session of the task is admitted against its envelope
   on a node the manager's session router picks, exactly as an unnamed
   session start is (or on the node called, for the edge form). A
   [disabled runtime](runtime-disable.md) refuses a run like any session
   start.
2. **Stage.** The caller's files and input are written into the **run
   directory**: each `--file` under its name, and `--input` as `input.json`,
   whose path the command finds in `LEGION_RUN_INPUT`.
3. **Execute.** The command runs once, as an exec in the running session,
   with `run.wall` as its deadline, `LEGION_RUN_DIR` set to the run
   directory, and stdin closed.
4. **Collect.** The exit status; stdout and stderr up to `run.output`;
   `result.json` in the run directory as `result`, when the command wrote
   one that parses as JSON; and each `--get` file.
5. **End.** The session is ended, always, including after a failure in any
   step.

### The run directory

| Runtime | Run directory | How files get there |
| --- | --- | --- |
| `container`, `sandbox` | `/legion/run` inside the unit | through the unit: an exec in the container's namespaces, or the sandbox's guest agent |
| `exec` | `<task directory>/run` on the host | the same way, as the session's own host process |

An `exec` session shares the host's root filesystem, so it has no
`/legion/run`. Legion makes one jobspec work on every runtime in two ways:

- **In `run.command`**, an argument that starts with `/legion/run` is
  rewritten to the session's run directory on `exec`.
- **Inside a script**, use `$LEGION_RUN_DIR` (and `$LEGION_RUN_INPUT`), which
  are right everywhere. On `exec` the command's working directory is also
  the run directory, so relative paths work.

Staging and collection need `/bin/sh`, `mkdir`, `cat`, `chmod`, and `head` in
the unit (any image with a shell and coreutils or BusyBox has them), and a
writable `/legion/run`. A `container` or `sandbox` unit is given up to 20
seconds after it reports `running` to accept its first exec.

File names, in `--file` and in `--get`, are relative to the run directory
(`main.py`, `data/in.csv`) or absolute under `/legion/run/`. Anything else,
and any `..`, is refused: a run cannot be used to read or overwrite the rest
of the unit, or of the host on `exec`.

### Limits

| Limit | Value |
| --- | --- |
| Files plus input staged by one run | 3 MiB |
| stdout plus stderr returned | `run.output` (at most 1 MiB); beyond it output is dropped and the reply says `truncated` |
| `--get` files returned, together | `run.output`; a file that does not fit is cut and marked `truncated` |
| `result.json` | `run.output`; a larger one, or one that is not JSON, is not returned |

A run's request and reply are each one gRPC message, which is where these
come from. Larger inputs and outputs belong in an
[artifact](artifacts.md), a [volume](volumes.md), or an object store the
command reaches itself.

### Single-use, always

A run's session is never reused for another run, because the next run may be
another tenant's code. Every run pays a session start.

## The reply: a stopped run is data

| Outcome | Envelope |
| --- | --- |
| The command ran and exited, with any status | `ok: true`, `exit` set |
| The run was stopped by a limit | `ok: true`, `exit: null`, `stopped { reason, limit, detail, hint }` |
| The run could not start or could not finish | `ok: false`, the standard error envelope |

`stopped.reason` is one of:

| Reason | When |
| --- | --- |
| `wall` | The command was still running at `run.wall` and was killed. `limit` is the wall as written. |
| `memory` | The kernel killed the command for exceeding the session's memory limit. Reported where the node governs the session with a cgroup ([resource enforcement](resource-enforcement.md) on, `exec` and `container`); elsewhere the same event shows as exit status 137 or as `session_ended`. |
| `session_ended` | The session ended under the run: an operator stopped it, a drain took it, or its runtime exited. |

So an agent can tell "my code failed" (a non-zero `exit`), "my code hit a
limit" (`stopped`), and "Legion could not run it" (an error) from the
structure of the reply alone. Errors:

| Code | When | Retryable |
| --- | --- | --- |
| `INVALID_ARGUMENT` | A file or `--get` path outside the run directory, input that is not JSON, more than 3 MiB staged | no |
| `PERMISSION_DENIED` | No `job:write`; `--command` without `exec:write` | no |
| `FAILED_PRECONDITION` | The task has no `run` block or does not allow `--command`; no node holds an envelope for the task; the command could not be executed or a file could not be staged | no |
| `RESOURCE_EXHAUSTED` | Every envelope is full, or the node's launch pool is busy | yes |
| `UNAVAILABLE` | The runtime is disabled, the node could not be reached, or the session was lost before the command ran | yes |
| `DEADLINE_EXCEEDED` | The session did not reach `running` within `--wait-timeout` (default 60 s, at most 600). It is ended; nothing ran | yes |

Without `-j`, `legion session run` prints the command's stdout and stderr on
its own stdout and stderr and exits with the command's status, so scripts
keep working. A run stopped by `run.wall` exits 124, and a run stopped for
any other reason exits 125.

## Who may run what

- **`job:write`** on the job runs the task's own `run.command` with the
  caller's files and input. The jobspec names the program; the caller
  supplies its inputs.
- **`--command`** replaces the command for one run. It needs
  `run.allowCommand = true` on the task **and** `exec:write` on the job, the
  permission `legion exec` needs, because an arbitrary command is an exec.
  Such a run is written to the `exec_audit` log with its caller.

```text
PERMISSION_DENIED: --command needs exec:write on job code-runner (running the task's own run.command needs only job:write)
FAILED_PRECONDITION: task machine does not allow --command (run.allowCommand = false)
```

`job:write` on a task whose `run.command` is an interpreter reading a staged
file (`python3 /legion/run/main.py`) is the right to run arbitrary code in
that task's sandbox, under that task's resources and network policy. That is
the point of the feature; grant it accordingly.

## Idempotency

`--idempotency-key` makes a retry safe for code that must not run twice.
Idempotency is **per node**:

- The node derives the run's id from the job, the task, and the key, and
  keeps the reply for 10 minutes (in memory and in its `sessions.db`, so it
  survives a worker restart). A retry that reaches the same node within that
  time is answered from the kept reply, with `deduplicated: true`, and
  nothing runs.
- A retry that arrives while the first attempt is still running waits for it
  and gets the same reply.
- Only a run that finished is kept. A failed attempt (an error envelope) is
  not, so its retry runs.
- A manager remembers which node it sent a key to for the same 10 minutes,
  so a retry through the same manager reaches that node. Through another
  manager, pass the node the first attempt named: `--on-node <node>`.
- A retry that lands on another node runs again.

Through a manager, a key is scoped to the caller and the request, like every
Legion idempotency key: the same key from another caller, or with different
files, is a different run.

## The surface

```console
$ legion session run <job> [--task T] [--file [NAME=]PATH]... [--input JSON|@FILE|-]
      [--get PATH]... [--out DIR] [--idempotency-key K] [--dry-run]
      [--wait-timeout SECONDS] [--on-node NODE]
      [--node ADDR --ticket T] [-j] [--command ARG...]
```

- `--file main.py` stages a local file under its own name; `--file
  data/in.csv=./sample.csv` under another. The executable bit is kept.
- `--get out.csv` returns a file; `--out DIR` writes the returned files into
  a directory, and `-j` carries them in the envelope (`content`, or
  `contentBase64` when the bytes are not UTF-8).
- `--dry-run` answers which node would take the run and whether its envelope
  has room, and runs nothing.
- `--command` takes the rest of the line; put it last.

**RPCs.** `Manager.RunSession` (routed by the session router, nothing
written) and `Worker.RunSession` on the worker-plane port for edge callers.
Both take `RunSessionRequest` and answer `RunSessionResponse`
(`proto/legion.proto`).

**At the edge.** With `--node` and `--ticket`, the CLI calls one worker
directly with a [session ticket](session-mode.md#starting-at-the-edge), with
no manager on the path. A ticket runs the task's own command; it cannot
replace it.

**Agents.** The MCP tool `legion_session_run` (mutating, behind
`--allow-write`) takes the same arguments, with file contents inline.

## Observability

- `legion_session_runs_total{outcome}` on managers (`ran`, `deduplicated`,
  `refused`, `failed`, `runtime_disabled`) and
  `legion_worker_session_runs_total{outcome}` on workers (`ran`, `stopped`,
  `failed`); see [metrics.md](metrics.md).
- A run's session appears in the node's session report with origin `run`
  while it lives, so `legion instance list` shows runs in flight.
- The worker logs `Run started` and `Run finished` with the run id, the
  session id, the exit status, and the stop reason.

## Known limitations

- **No streaming.** The reply comes when the run ends. For output as it is
  produced, start a session and use [`legion exec`](exec-and-actions.md).
- **The session is not kept.** A caller who wants the machine afterwards
  starts a session.
- **Session tasks only.** Keyed and service tasks do not take runs.
- **Every run starts a session.** Sandbox runs pay a VM boot each time;
  there is no pool of prepared VMs.
- **A worker restart ends runs in flight.** The restarted worker ends any
  run session its predecessor left, within a few seconds, and the caller
  sees `UNAVAILABLE`. With an idempotency key the retry runs again, because
  the first attempt never finished.
- **`memory` stops need a cgroup.** A sandbox guest that runs out of memory
  reports exit status 137 from its own kernel, not `stopped.reason =
  "memory"`.
- **`cpuMs` is 0** unless the node governs the session with a cgroup.
- **`container` and `sandbox` runs need a shell in the image** (see [The
  run directory](#the-run-directory)); a distroless image cannot take runs.
- **A workload runs a sibling task through its API socket only when its
  task declares it.** `legion session run --local` (`Workload.RunSession`)
  is admitted when the calling task names the target task in
  [`children.tasks`](child-sessions.md#declaring-children), or is the
  target task itself. A session task has no API socket, so the caller is a
  service, batch, slot, or keyed task; see
  [workload-identity.md](workload-identity.md#the-api-socket).
