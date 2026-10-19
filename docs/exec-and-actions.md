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
- [Direct exec](#direct-exec)
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

# Target an instance: the manager resolves it to whichever allocation
# fulfils it now, so the same id works across reschedules and resumes.
legion exec --instance user-42 -it -- /bin/bash

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

In the UI, open a task (Jobs → a job → a task), go to its **Actions** tab
and pick **Open a shell** at the bottom of the list, or press **Open shell** in
the header or **Shell** on an instance's row in the Overview. Pick the
instance, type a command, and toggle *Interactive (tty)* if you want a shell.
An embedded terminal (xterm.js) streams the session. A link can open it on one
allocation: `/jobs/default/web/tasks/api?tab=actions&action=shell&alloc=4217`.

The allocation page (`/allocations/<id>`) has the same **Actions** panel
aimed at that one allocation.

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

In the UI, the task page's **Actions** tab lists the task's actions. The
chosen one gets a form built from its parameter schema — a segmented control
or chips for `enum`, a switch for `bool`, inputs checked against `min`/`max`
and `pattern` as you type, a masked input for `sensitive` — plus a preview of
the argv it will run and the equivalent `legion action run` command. You can
run it on one instance, on all running instances, or only on the unhealthy
ones; each instance gets its own exit code and output. Running on several
instances opens one session per allocation from the browser, so each is
audited separately. A `tty` action opens a terminal in one instance.
`?tab=actions&action=<name>` links straight to an action.

## Which task runtimes support exec

Each task runtime declares whether it supports exec. Against a runtime that
doesn't, `legion exec` fails fast with a clear error.

| Kind | Exec | Interactive (tty) | Notes |
| --- | --- | --- | --- |
| `exec` | ✅ | ✅ | Runs a sibling host process with the task's env/cwd. Host-level, so it requires the `host` ACL grant. The env is the task's own, built the same way: Legion's allocation env (`LEGION_ALLOC_ID`, `LEGION_JOB_ID`, `LEGION_JOB_NAME`, `LEGION_NAMESPACE`, `LEGION_TASK_NAME`, `LEGION_PORT_*` / `LEGION_ADDR_*`, `LEGION_UPSTREAM_*`), then worker-injected env (`LEGION_TASK_DIR`, artifacts, volumes, identities), then the task's `env`. `TERM` is set for tty sessions unless the task sets it. Raw exec and actions get the same env. The process joins the cgroup the task runs in (the worker-created leaf when [resource enforcement](resource-enforcement.md) placed the task, or the task's explicit `config.cgroup`), so a session counts against the task's memory and CPU limits and is reaped with it on stop. A session that cannot join fails to start (exit 126 for a tty session) rather than running unconfined. It does not take the task's user, landlock, or hardening settings, and it stays in the host's network namespace, also for a `network.mode = "gated"` task. |
| `container` (Apple `container`) | ✅ | ✅ | `container exec` inside the container's namespaces (sandboxed). |
| `container` (runc / crun) | ✅ | ✅ | `<runtime> exec` inside the container's namespaces (sandboxed). The process spec comes from the bundle Legion generated, so env, cwd, and user match the task. Only argv is replaced, and the exit status is the process's own. |
| `sandbox` (microVM) | ✅ | ✅ | Through the guest agent's vsock listener (port 1100), as root in the guest with the task's environment ([task-runtime-sandbox.md § Exec](task-runtime-sandbox.md#exec)). A suspend or restore ends open sessions. |
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
  runtimes (`container`, `sandbox`). The same flag on `job` rules gates
  submitting a job with a host-level task
  ([Host-level jobs](auth-and-acls.md#host-level-jobs)), so a token that may
  deploy but not reach the host can't do it by submitting an `exec` task
  either.

```nickel
[
  # Full shell into team-a's container jobs, but not host-level tasks:
  { resources = ["exec"],   actions = ["write"], namespaces = ["default"], names = ["team-a-*"] },
  # Host-level exec too (admins):
  { resources = ["exec"],   actions = ["write"], namespaces = ["default"], names = ["*"], host = true },
  # On-call: may run only the curated actions on the web job, no raw shell:
  { resources = ["action"], actions = ["write"], namespaces = ["default"], names = ["web"],
    action_names = ["restart-*", "flush-cache"] },
  { resources = ["job"],    actions = ["read"],  namespaces = ["default"], names = ["web"] },  # to discover them
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

There is no interactive PTY over MCP. These tools are one-shot only. Both
run tools take `instance_id` in place of `allocation_id`.

- `legion_exec_ticket_mint` (mutating, needs `--allow-write`): mint an exec
  ticket for [direct exec](#direct-exec). It is authorized like
  `legion_exec_run` (or `legion_action_run` for an action ticket) and returns
  the ticket, the worker address, the node name, and the cluster CA.
- `legion_exec_key_list` (read-only), `legion_exec_key_create` and
  `legion_exec_key_delete` (mutating): [delegated minting
  keys](#delegated-minting-keys). Signing with a key's private half happens
  in the platform's own code, or with `legion exec-ticket sign`; there is no
  MCP tool for it, because the private key never leaves the platform.

## Direct exec

By default every exec session runs through a manager: two streams for the
session's life, and every byte twice. Direct exec takes the manager off the
data path. A manager mints a short-lived **exec ticket**, and the client opens
`Worker.ExecTask` on the worker running the target, presenting the ticket in
the first frame. The worker verifies the ticket offline. The manager does one
unary call per session and writes nothing.

It is opt-in twice: the worker must set `exec = { direct = true }`, and the
client must ask with `--direct` (or call `MintExecTicket` itself).

```nickel
# configs/workers/<type>.ncl
{
  workerType = "agents",
  exec = { direct = true },
}
```

```console
$ legion exec --instance user-42 --direct -it -- /bin/bash
root@user-42:/workspace#

$ legion exec-ticket mint --instance user-42 --tty -j -- /bin/bash
{"ok":true,"data":{"ticket":"eyJ…","worker":"https://10.0.3.17:4848",
 "node":"w-17","expiresAt":"2026-11-02T18:05:11Z","allocationId":88213,
 "jti":"ext_91ab…","maxSessionSeconds":3600,"caPem":"-----BEGIN CERTIFICATE-----…"}}
```

`legion exec --direct` mints and redeems in one step. `legion exec-ticket
mint` is for platforms that dial the worker themselves: present the ticket in
`ExecStart.exec_ticket` on `legion.Worker/ExecTask`, over TLS that trusts only
`caPem` and verifies the server name `<node>.node.legion`, with no client
certificate. `allocation_id` may be 0 and `command` empty; the worker runs
what the ticket names.

### What a ticket allows

A ticket is a JWT signed with the cluster's workload-identity key (audience
`legion:exec`), so the manager must run with `workloadIdentity.enabled: true`.

| Question | Answer |
| --- | --- |
| Who can mint | Anyone `ExecAllocation` would admit for the same target: `exec:write` on the job (with `host = true` for host-level runtimes), or `action:write` for an action ticket. The same coarse gate runs before any lookup, so a caller without exec learns nothing about what exists. |
| What it allows | One session, on one node, in one allocation, with the ticket's argv (rendered by the manager for an action; any argv for an `exec` ticket minted without a command) and its tty flag. |
| How long | Redeemable for `ttl` (default 60s, at most 10m). The session runs at most `max_session` (default and ceiling 1h), never past the minting token's own expiry. |
| Replay | Single use: the worker records each redeemed `jti` until the ticket expires. A ticket names its node, so it can't be redeemed elsewhere. A restarted worker refuses tickets issued before it started; mint again. |
| Revocation | Every heartbeat, the worker reports its open direct sessions and the manager re-checks each minting token, as the proxied path does every 30 seconds. A deleted or expired token, or a policy that no longer grants the session, ends it within one heartbeat interval. |
| A stolen ticket | Opens the one session it names, inside its redeem window, if the thief redeems it first. It reaches no other allocation, node, or command. |
| Transport | The mutual-TLS worker plane. The worker proves itself with its node certificate; the client presents no certificate, because the ticket is its credential. |
| A compromised worker | Sees tickets presented to it, which are already spent and name only itself. It can mint nothing. |

Minting is audited at the manager under `exec_audit` (`exec ticket minted`).
The worker reports each finished session on its next heartbeat, and the manager
logs it under `exec_audit` too (`direct exec session ended`, with the exit code
and what ended it). An action session's report carries the action name, never
its rendered argv, which can hold sensitive parameters.

### Delegated minting keys

A platform that starts sandboxes at the edge can also mint their exec tickets
itself, with no manager call per session. It registers an ES256 public key
scoped to a job glob, and workers accept tickets signed by the private half:

```console
$ openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out platform.pem
$ openssl pkey -in platform.pem -pubout -out platform.pub
$ legion exec-key create --name agents-platform --jobs 'agents-*' --max-ttl 5m \
    --public-key platform.pub -j
{"ok":true,"data":{"keyId":"xk_3d1cq7…","jobs":"agents-*","maxTtl":"5m","maxSession":"1h",…}}

# Signed offline, the way a platform's own code would:
$ legion exec-ticket sign --key platform.pem --key-id xk_3d1cq7… --node w-17 \
    --alloc 88213 --job agents-a --task workspace -j -- /bin/bash
```

A ticket signed by a key carries the key id as its JWT `kid` header and
`sub = "key:<id>"`, and otherwise has the claims of a manager-minted ticket
(audience `legion:exec`, ES256). Workers check it against the key:

- the job matches the key's `jobs` pattern. A pattern without a `/`
  (`agents-*`) matches jobs in the `default` [namespace](namespaces.md) only;
  `agents/*` or `agents-*/workspace*` is `namespace-glob/name-glob`. `*` alone
  never reaches into another namespace;
- the redeem window (`exp - iat`) is within the key's `maxTtl`, and
  `legion.io/max_session` within its `maxSession`;
- the target is not a host-level runtime. A key never opens a session in an
  `exec` task, whatever grant created it; mint through a manager with the
  `host` grant for those.

Everything else is checked as for any ticket: the node, the allocation and
task, the command and tty, single use, and the worker's restart.

Creating and deleting keys takes `acl:write` and listing takes `acl:read`,
because a key is a credential that mints shell access. A key's ceilings
default to, and cannot exceed, the manager's `exec.tickets.maxTtl` and
`maxSession`. Workers that admit tickets learn the key set on their heartbeat,
so a new key works on a node within one heartbeat interval. Deleting a key
reaches every worker the same way: its tickets stop verifying, and the
sessions it minted end with `PermissionDenied: session ended: the exec key
that minted this session was deleted`. Keys are audited under `exec_audit`
(`exec key created`, `exec key deleted`), and their sessions are reported
like any direct session, with `minted_by = key:<id>`.

A leaked private key is a shell capability for every matching job on a
non-host runtime until the key is deleted. Keep the glob narrow and the
ceilings short.

### Manager settings

```nickel
# configs/manager.ncl
{
  workloadIdentity = { enabled = true },
  exec = { tickets = { defaultTtl = "60s", maxTtl = "10m", maxSession = "1h" } },
}
```

An unparseable duration, or a `defaultTtl` above `maxTtl`, stops the manager at
startup.

### Failure modes

| Symptom | Cause |
| --- | --- |
| `FailedPrecondition: exec tickets are signed by the workload-identity issuer, which is not enabled` | Set `workloadIdentity.enabled: true` on the manager, or drop `--direct`. |
| `FailedPrecondition: this worker does not admit exec tickets` | The node's worker type has no `exec = { direct = true }`. |
| `could not reach node w-17 at https://10.0.3.17:4848 for direct exec` | The client has no route to the worker port. Direct exec needs one; drop `--direct` to go through the manager. |
| `PermissionDenied: exec ticket rejected: the ticket expired at …` | It was not redeemed within its `ttl`. Mint again. |
| `… has already been redeemed` | Tickets are single use. |
| `… was issued before this worker restarted` | The worker forgot its redeemed set when it restarted. Mint again. |
| `… is for node w-18, not this node` / `allocation 7 is not running on this node` | The ticket names another node, or the allocation moved. Mint again for the current target. |
| `… names the command ["/bin/bash"], not ["/bin/sh"]` | The session must run the ticket's argv and tty mode. |
| `InvalidArgument: ttl 1h is above this manager's ceiling of 10m` | Ask for less, or raise `exec.tickets`. |
| Session ends with `max_session elapsed` | The session ran its `max_session`. |
| Session ends with `PermissionDenied: session ended: the token that minted this session was deleted` | Revoked on a heartbeat. |
| `… is signed by exec key xk_…, which this node does not hold` | The key was deleted, or the worker has not had a heartbeat since it was created. |
| `… exec key xk_… mints tickets for jobs matching agents-*, not billing` | The ticket's job is outside the key's glob. |
| `… a delegated exec key never reaches a host-level runtime` | The target runs `exec`. Mint through a manager instead. |

### Limitations

- The client must be able to reach the worker port. There is no gateway tier
  for browsers or private workers yet; the UI terminal always goes through the
  manager.
- Revocation is bounded by one heartbeat interval (2s by default, longer on a
  large fleet with adaptive cadence), not immediate. A credential with no
  accessor id (a JWT auth method) can't be looked up again, so its sessions
  are bounded by `max_session`, which the mint clamps to that credential's
  expiry.
- A ticket is not bound to the client that minted it: whoever presents it
  first, inside its window, gets the session.
- Exec into a sandbox is root in the guest, the same promise as the proxied
  path.

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
  Clients never learn worker addresses, except through
  [direct exec](#direct-exec), where the mint reply names the worker and the
  ticket carries what the manager authorized.
- The WebSocket listener runs on the manager port + 1, because tonic's gRPC
  server doesn't perform HTTP upgrades. If that port is taken, the manager
  fails to start (`failed to bind the exec WebSocket listener on ...`);
  free the port or set `exec.enabled = false`. Browsers can't set WebSocket request
  headers, so they authenticate with the bearer token carried in the first
  frame. The handshake's `Origin` must be loopback or listed in
  `server.allowedOrigins` ([UI Architecture](ui-architecture.md#allowed-browser-origins)),
  so a web page cannot open a session.
- The WebSocket listener follows `server.tls`. When TLS is enabled, it serves
  `wss://` with the same certificate and key as the gRPC listener, and it
  refuses plaintext connections. When TLS is off, it serves plaintext `ws://`.
  The UI picks `wss://` whenever its manager endpoint is `https://`. A browser
  that does not trust the certificate on `port + 1` fails the terminal while
  the rest of the UI works
  ([UI Architecture](ui-architecture.md#exec--actions-interactive-terminal)).
- Interactive sessions use a PTY on the worker (`portable-pty`: openpty on
  Unix, ConPTY on Windows). Sessions end when the connection closes. There is
  no detach/reattach.
- Sessions re-check their credential every 30 seconds. A revoked or expired
  token, or a policy that no longer grants the session, ends it with an auth
  error and cancels the process on the worker.
