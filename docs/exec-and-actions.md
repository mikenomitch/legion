# Exec & Actions

Legion can run a command inside a running allocation from the CLI or the web
UI. You can run a one-shot diagnostic, open an interactive shell, or run a
predefined, parameterized *action*. Two ACL resources (`exec` and `action`)
gate access, so you can give an on-call engineer a curated set of actions
without full shell access.

- [Exec](#exec)
- [Actions](#actions)
- [Which task runtimes support exec](#which-task-runtimes-support-exec)
- [ACLs](#acls)
- [Agents (MCP)](#agents-mcp)
- [Disabling exec](#disabling-exec)
- [How it works](#how-it-works)

---

## Exec

Run a command in a running allocation. A one-shot command prints its output and
exits with the remote exit code. Pass `-t` to allocate an interactive PTY.

```bash
# One-shot: prints output, exits with the remote exit code (scriptable).
legion exec 4217 -- cat /etc/resolv.conf

# Interactive shell: -i wires stdin, -t allocates a PTY (raw mode, resize,
# Ctrl-C passthrough, exit-code propagation).
legion exec 4217 -it -- /bin/sh

# Multi-task allocations need --task; single-task allocations infer it.
legion exec 4217 --task api -it -- /bin/bash

# Target the newest running allocation of a job instead of an id.
legion exec --job web -it -- /bin/sh

# For scripts and agents: collect the output into one JSON envelope.
legion exec 4217 --json -- cat /etc/resolv.conf
# {"ok":true,"data":{"allocationId":4217,"exitCode":0,"stdout":"nameserver …\n",
#  "stderr":"","message":""}}
```

Flags match docker and kubectl: `-i/--stdin` forwards local stdin, `-t/--tty`
allocates a pseudo-terminal, and everything after `--` is the remote argv.

`--json` is for one-shot commands: it buffers the output (decoded as UTF-8,
invalid bytes replaced) instead of streaming it, and cannot be combined with
`-i` or `-t`. The process still exits with the remote exit code, so a command
that fails gives `"ok": true` with a non-zero `exitCode` and a non-zero exit.
A request that never ran (an unknown allocation, an exec the ACLs refuse, an
unreachable node) is the standard `"ok": false` error envelope, with the
manager's or the worker's own message: for example `FailedPrecondition` when
the allocation is not running, or `Unavailable` naming the node and port the
manager could not reach.

In the UI, open a task (Jobs → a job → a task) and use the **Exec &
Actions** panel on any running allocation. Type a command and toggle
*Interactive (tty)* if you want a shell. An embedded terminal (xterm.js)
streams the session.

## Actions

An **action** is a named, parameterized command template declared on a task.
The server renders and validates it from the stored job spec. The caller sends
only an action name and parameters, never raw argv. This means an action-only
grant can't be turned into an arbitrary shell.

### Declaring actions

Add an `actions` array to a task in the job spec:

```nickel
{
  name = "web",
  task = {
    name = "api",
    runtime = "container",
    config = { image = "ghcr.io/acme/api:1.4", command = ["/srv/api"] },
    actions = [
      {
        name = "flush-cache",                       # [a-z0-9-]{1,128}, unique per task
        description = "Flush the in-memory cache",
        command = ["/srv/api", "admin", "flush-cache", "--scope", "${param.scope}"],
        params = [
          {
            name = "scope",
            type = "enum",                          # string | int | bool | enum
            values = ["all", "sessions", "assets"],
            "default" = "sessions",
          },
        ],
      },
      {
        name = "tail-slowlog",
        command = ["/usr/bin/tail", "-n", "${param.lines}", "/var/log/slow.log"],
        params = [
          { name = "lines", type = "int", "default" = "100", min = 1, max = 10000 },
        ],
        timeout = "60s",
      },
    ],
  },
}
```

Parameter fields:

| Field | Meaning |
| --- | --- |
| `type` | `string` (default), `int`, `bool`, or `enum` |
| `default` | value used when the param is omitted (makes it optional) |
| `required` | force required or optional (defaults to required unless a `default` is set) |
| `values` | allowed values for `enum` params |
| `pattern` | anchored regex constraint for `string` params |
| `min` / `max` | inclusive bounds for `int` params |
| `sensitive` | redact the value (`***`) in audit events, CLI echo, and UI history |
| `tty` | (action-level) allocate a PTY when the action runs |

`${param.<name>}` placeholders are substituted inside individual argv
elements. There is no shell evaluation and no word-splitting. Values are
checked against their type and constraints at submission and again at
invocation. Legion rejects unknown params and missing required params.

### Running actions

```bash
legion action list web                                   # discover actions
legion action run web flush-cache --param scope=assets   # newest running alloc
legion action run web tail-slowlog --alloc 4217 --param lines=500
legion action run web debug-shell --alloc 4217 -it       # a tty action
legion action list web --json                            # the actions as data
legion action run web flush-cache --json                 # one envelope, as for exec
```

In the UI, the same **Exec & Actions** panel shows each action with a form
built from its parameter schema (enum → dropdown, int/string → validated
input). *Run* streams output into the terminal and shows the exit code.

## Which task runtimes support exec

Each task runtime declares whether it supports exec. Against a runtime that
doesn't, `legion exec` fails fast with a clear error.

| Kind | Exec | Interactive (tty) | Notes |
| --- | --- | --- | --- |
| `exec` | ✅ | ✅ | Runs a sibling host process with the task's env/cwd. Host-level, so it requires the `host` ACL grant. The env is the task's own, built the same way: Legion's allocation env (`LEGION_ALLOC_ID`, `LEGION_JOB_ID`, `LEGION_TASK_NAME`, `LEGION_PORT_*` / `LEGION_ADDR_*`, `LEGION_UPSTREAM_*`), then worker-injected env (`LEGION_TASK_DIR`, artifacts, volumes, identities), then the task's `env`. `TERM` is set for tty sessions unless the task sets it. Raw exec and actions get the same env. The process stays in the host's network namespace, also for a `network.mode = "gated"` task. |
| `container` (Apple `container`) | ✅ | ✅ | `container exec` inside the container's namespaces (sandboxed). |
| `container` (youki / runc / crun) | ✅ | ✅ | `<runtime> exec` inside the container's namespaces (sandboxed). The process spec comes from the bundle Legion generated, so env, cwd, and user match the task. Only argv is replaced, and the exit status is the process's own. |
| `sandbox` (microVM) | ❌ | ❌ | Not yet supported, because exec into a guest needs a vsock agent. Reports unsupported up front instead of failing at session open. |
| `sandbox` (macOS dev mode) | ✅ | ✅ | `container exec` into the sandbox's VM-backed container. |
| `noop` / custom plugins | ❌ | ❌ | Opt in by implementing `exec_support()` + `exec()`. |

> **Windows:** interactive PTY uses ConPTY via `portable-pty`. It should work
> but is **experimental**, since there is no Windows CI coverage yet.

## ACLs

Exec adds two resources to the ACL vocabulary. Both are default-deny once ACLs
are enabled.

| Resource | `write` grants |
| --- | --- |
| `exec` | Open an interactive/one-shot exec session. |
| `action` | Run a declared action (the command is fixed and server-rendered). |

Rule selectors:

- `names`: job names (glob), as elsewhere.
- `action_names`: action names (glob), on `action` rules.
- `host = true`: on `exec` rules, also allows exec into host-level runtimes
  (the `exec` runtime). Without it, an `exec` rule matches only sandboxed
  runtimes (`container`, `sandbox`).

```nickel
[
  # Full shell into team-a's container jobs, but not host-level tasks:
  { resources = ["exec"],   actions = ["write"], names = ["team-a-*"] },
  # Host-level exec too (admins):
  { resources = ["exec"],   actions = ["write"], names = ["*"], host = true },
  # On-call: may run only the curated actions on the web job, no raw shell:
  { resources = ["action"], actions = ["write"], names = ["web"],
    action_names = ["restart-*", "flush-cache"] },
  { resources = ["job"],    actions = ["read"],  names = ["web"] },  # to discover them
]
```

`exec:write` implies `action:write` for the same job, since anyone who can open
a shell can type the command anyway. The reverse never holds. Keeping action
grants separate from shell access is why actions exist.

Every session and action run is audited under the tracing target `exec_audit`.
Each audit record has the principal, accessor id, allocation, task, command or
action plus parameters (sensitive values redacted), tty flag, and exit code.

## Agents (MCP)

The MCP server (`legion mcp serve`) exposes exec/actions to agents:

- `legion_action_list` (read-only): list a job's declared actions and params.
- `legion_action_run` (mutating, needs `--allow-write`): run an action
  one-shot and return `exitCode`, `stdout`, `stderr`, and `message`.
- `legion_exec_run` (mutating, needs `--allow-write`): run a one-shot command
  and return the same shape.

There is no interactive PTY over MCP. These tools are one-shot only.

## Disabling exec

Exec is on by default, though once ACLs are enabled each caller is still
denied unless granted. To turn it off entirely, set this in the manager config:

```nickel
# configs/manager.ncl
{
  exec = { enabled = false },   # ExecAllocation rejects; the WS bridge is not started
}
```

## How it works

```
CLI  ──gRPC ExecAllocation (bidi)──┐
                                   ├─► manager ──authz + action render──► worker.ExecTask ──► pty/pipe
UI   ──WebSocket /v1/exec/ws───────┘        (proxies frames)
```

- One protobuf frame protocol (stdin / stdout / stderr / resize / in-band exit)
  runs on every leg. The CLI and manager↔worker legs use native gRPC bidi
  streams. The browser uses a WebSocket because gRPC-Web can't do
  bidirectional streaming.
- The manager is the trust boundary. It authorizes the request, renders any
  action's argv from the stored spec, and proxies the stream to the worker.
  Clients never learn worker addresses.
- The WebSocket listener runs on the manager port + 1, because tonic's gRPC
  server doesn't perform HTTP upgrades. Browsers can't set WebSocket request
  headers, so they authenticate with the bearer token carried in the first
  frame. The handshake's `Origin` must be loopback or listed in
  `server.allowedOrigins` ([UI Architecture](ui-architecture.md#allowed-browser-origins)),
  so a web page cannot open a session.
- Interactive sessions use a PTY on the worker (`portable-pty`: openpty on
  Unix, ConPTY on Windows). Sessions end when the connection closes. There is
  no detach/reattach.
- Sessions re-check their credential every 30 seconds. A revoked or expired
  token, or a policy that no longer grants the session, ends it with an auth
  error and cancels the process on the worker.
