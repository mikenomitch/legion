# Exec & Actions

Legion can run a command **inside a running allocation** — a one-shot
diagnostic, an interactive shell, or a pre-baked, parameterized *action* — from
both the CLI and the web UI. Access is gated by two ACL resources (`exec` and
`action`) so an on-call engineer can be given a curated set of actions without
full shell access.

Design rationale and the alternatives considered live in
[internal_docs/plans/exec-and-actions-plan.md](../internal_docs/plans/exec-and-actions-plan.md).

- [Exec](#exec)
- [Actions](#actions)
- [Which task runtimes support exec](#which-task-runtimes-support-exec)
- [ACLs](#acls)
- [Agents (MCP)](#agents-mcp)
- [Disabling exec](#disabling-exec)
- [How it works](#how-it-works)

---

## Exec

Run a command in a running allocation. One-shot prints output and exits with
the remote exit code; `-t` allocates an interactive PTY.

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
```

Flags mirror docker/kubectl: `-i/--stdin` forwards local stdin, `-t/--tty`
allocates a pseudo-terminal, and everything after `--` is the remote argv.

In the **UI**, open a task (Jobs → a job → a task) and use the **Exec &
Actions** panel on any running allocation: type a command, toggle *Interactive
(tty)*, and an embedded terminal (xterm.js) streams the session.

## Actions

An **action** is a named, parameterized command template declared on a task.
It is rendered and validated **server-side** from the stored job spec — the
caller sends an action name and parameters, never raw argv — so an action-only
grant cannot be turned into an arbitrary shell.

### Declaring actions

Add an `actions` array to a task in the job spec:

```jsonnet
{
  name: 'web',
  task: {
    name: 'api',
    runtime: 'container',
    config: { image: 'ghcr.io/acme/api:1.4', command: ['/srv/api'] },
    actions: [
      {
        name: 'flush-cache',                       // [a-z0-9-]{1,128}, unique per task
        description: 'Flush the in-memory cache',
        command: ['/srv/api', 'admin', 'flush-cache', '--scope', '${param.scope}'],
        params: [
          {
            name: 'scope',
            type: 'enum',                          // string | int | bool | enum
            values: ['all', 'sessions', 'assets'],
            default: 'sessions',
          },
        ],
      },
      {
        name: 'tail-slowlog',
        command: ['/usr/bin/tail', '-n', '${param.lines}', '/var/log/slow.log'],
        params: [
          { name: 'lines', type: 'int', default: '100', min: 1, max: 10000 },
        ],
        timeout: '60s',
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
| `required` | force required/optional (defaults to required unless a `default` is set) |
| `values` | allowed values for `enum` params |
| `pattern` | anchored regex constraint for `string` params |
| `min` / `max` | inclusive bounds for `int` params |
| `sensitive` | redact the value (`***`) in audit events, CLI echo, and UI history |
| `tty` | (action-level) allocate a PTY when the action runs |

`${param.<name>}` placeholders are substituted **inside individual argv
elements** — there is no shell evaluation and no word-splitting. Values are
validated against their type/constraints at submission *and* again at
invocation. Unknown or missing-required params are rejected.

### Running actions

```bash
legion action list web                                   # discover actions
legion action run web flush-cache --param scope=assets   # newest running alloc
legion action run web tail-slowlog --alloc 4217 --param lines=500
legion action run web debug-shell --alloc 4217 -it       # a tty action
```

In the **UI**, the same **Exec & Actions** panel renders each action with a
form generated from its parameter schema (enum → dropdown, int/string →
validated input); *Run* streams output into the terminal with an exit code.

## Which task runtimes support exec

Each task runtime declares its exec support; `legion exec` fails fast with a clear
error against a runtime that does not support it.

| Kind | Exec | Interactive (tty) | Notes |
| --- | --- | --- | --- |
| `exec` | ✅ | ✅ | Runs a sibling host process with the task's env/cwd. **Host-level** — requires the `host` ACL grant. |
| `container` (Apple `container`) | ✅ | ✅ | `container exec` inside the container's namespaces (sandboxed). |
| `container` (youki) | ❌ | ❌ | Deferred; reports unsupported. |
| `sandbox` (microVM) | ❌ | ❌ | Deferred; exec into a guest needs a vsock agent. Reports unsupported rather than failing at session open. |
| `sandbox` (macOS dev mode) | ✅ | ✅ | `container exec` into the sandbox's VM-backed container. |
| `noop` / custom plugins | ❌ | ❌ | Opt in by implementing `exec_support()` + `exec()`. |

> **Windows:** interactive PTY uses ConPTY via `portable-pty`. It is expected
> to work but is **experimental** — there is no Windows CI coverage yet.

## ACLs

Two resources extend the existing ACL vocabulary (both default-deny once ACLs
are enabled):

| Resource | `write` grants |
| --- | --- |
| `exec` | Open an interactive/one-shot exec session. |
| `action` | Run a declared action (the command is fixed and server-rendered). |

Rule selectors:

- `names` — job names (glob), as elsewhere.
- `action_names` — action names (glob), on `action` rules.
- `host: true` — on `exec` rules, also permits exec into **host-level** runtimes
  (the `exec` runtime). Without it, an `exec` rule matches only sandboxed
  runtimes (`container`, `sandbox`).

```jsonnet
[
  // Full shell into team-a's container jobs, but not host-level tasks:
  { resources: ["exec"],   actions: ["write"], names: ["team-a-*"] },
  // Host-level exec too (admins):
  { resources: ["exec"],   actions: ["write"], names: ["*"], host: true },
  // On-call: may run only the curated actions on the web job, no raw shell:
  { resources: ["action"], actions: ["write"], names: ["web"],
    action_names: ["restart-*", "flush-cache"] },
  { resources: ["job"],    actions: ["read"],  names: ["web"] },  // to discover them
]
```

`exec:write` **implies** `action:write` for the same job (anyone who can open a
shell can type the command anyway). The reverse never holds — that separation
is the entire point of actions.

Every session and action run is audited (tracing target `exec_audit`) with the
principal, accessor id, allocation, task, command or action + parameters
(sensitive values redacted), tty flag, and exit code.

## Agents (MCP)

The MCP server (`legion mcp serve`) exposes exec/actions to agents:

- `legion_action_list` (read-only) — list a job's declared actions and params.
- `legion_action_run` (mutating, needs `--allow-write`) — run an action
  one-shot and return combined output + exit code.
- `legion_exec_run` (mutating, needs `--allow-write`) — run a one-shot command
  and return stdout, stderr, and exit code.

There is no interactive PTY over MCP — these tools are one-shot only.

## Disabling exec

Exec is on by default (it is still per-caller default-deny once ACLs are
enabled). A hardened deployment can turn it off entirely in the manager config:

```jsonnet
// configs/manager.jsonnet
{
  exec: { enabled: false },   // ExecAllocation rejects; the WS bridge is not started
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
  streams; the browser uses a WebSocket because gRPC-Web cannot do
  bidirectional streaming.
- The **manager is the trust boundary**: it authorizes the request, renders any
  action's argv from the stored spec, and proxies the stream to the worker.
  Clients never learn worker addresses.
- The WebSocket listener runs on the manager port **+ 1** (tonic's gRPC server
  does not perform HTTP upgrades). Browsers authenticate with the bearer token
  carried in the first frame, since they cannot set WebSocket request headers.
- Interactive sessions use a real PTY on the worker (`portable-pty`: openpty on
  Unix, ConPTY on Windows). Sessions die with the connection; there is no
  detach/reattach.
