# Workload Exec & Actions Plan

Status: **In execution.** Open questions resolved (§6); the backbone (proto,
plugin trait, worker exec), manager proxy, CLI, WebSocket bridge, and UI are
being implemented on this branch.

Legion has no way to run a command inside a running workload. Operators can
read logs (`legion job logs`, the UI LogViewer) but cannot open a shell in a
container, run a one-off diagnostic, or hand a curated "restart the cache"
button to an on-call engineer who shouldn't have shell access. This plan adds:

1. **Workload exec** — run a command in a running allocation, one-shot or
   interactive (PTY), from the CLI and the UI. Each task kind declares
   whether (and how) it supports this.
2. **Actions** — pre-baked, parameterized command templates declared in the
   job spec, runnable against an allocation by users who hold a *narrower*
   ACL grant than full exec, from the CLI and a dedicated UI.

User-facing docs will land as `docs/exec-and-actions.md` plus updates to the
per-kind capability tables (`docs/task-kind-*.md`) when the phases ship.

---

## 1. Research: how comparable systems do this

### Nomad `alloc exec`

- **Transport:** WebSocket upgrade on
  `GET /v1/client/allocation/:alloc_id/exec`, JSON text frames with
  base64-encoded bytes: `{"stdin":{"data":...}}`, `{"stdout":{"data":...}}`,
  `{"tty_size":{"height":H,"width":W}}`, `{"exited":true,"result":
  {"exit_code":N}}`, plus `{}` heartbeats. Browsers can't set WS headers, so
  `?ws_handshake=true` moves token auth into the first frame.
- **Routing:** the HTTP layer bridges the WebSocket onto an internal
  yamux-multiplexed streaming RPC that is forwarded server→client-node.
- **Driver interface:** `ExecTaskStreaming(ctx, taskID, ExecOptions{Command,
  Tty, Stdin, Stdout, Stderr, ResizeCh}) (*ExitResult, error)`; drivers
  opt in via a capability flag.
- **ACLs:** namespace capability `alloc-exec`; drivers without filesystem
  isolation (`raw_exec`) additionally require `alloc-node-exec`, because exec
  there is host access. This isolation distinction is worth copying.

### Nomad Actions (1.7+)

- `action "name" { command = "..." args = [...] }` blocks nested in the task;
  listed via `GET /v1/job/:id/actions`; executed over the *same* WebSocket
  exec protocol with a server-side command lookup
  (`/v1/job/:id/action?action=name&allocID=...`). The UI streams output into
  a flyout panel.
- **Two confirmed gaps** (Nomad issues #18627, #19391): args are frozen in
  the jobspec — *no runtime parameters* — and there is *no per-action ACL*;
  actions require the same `alloc-exec` capability as a raw shell. So Nomad
  actions are a convenience, not privilege separation. Legion's design closes
  both gaps; that is most of the value of this plan.

### Kubernetes `kubectl exec`

- SPDY historically, WebSocket by default since 1.31. Subprotocol
  `v5.channel.k8s.io`: each binary WS message is prefixed with one channel
  byte (`0`=stdin, `1`=stdout, `2`=stderr, `3`=error/status, `4`=resize),
  v5 adds an explicit stdin CLOSE signal (half-close for piped input —
  earlier WS versions couldn't express EOF, a known protocol flaw, k8s
  issue #89899).
- Exit code travels as a typed `v1.Status` object on the error channel, not
  an in-band frame field.

### Docker exec API

- Three-step: `POST /containers/{id}/exec` (returns exec id) →
  `POST /exec/{id}/start` (connection hijacked into a raw byte stream) →
  `POST /exec/{id}/resize?h=&w=` *out of band*. Exit code is polled
  afterwards via `GET /exec/{id}/json` — no in-band exit frame, widely
  considered the weakest part of the design.
- Without TTY, output is multiplexed with the 8-byte `stdcopy` header
  (`[STREAM_TYPE,0,0,0,SIZE_u32be]`).

### Parameterized command templates with scoped permissions

- **AWS SSM Documents** (closest to what we want): documents declare typed
  `parameters` with `default`, `allowedValues`, `allowedPattern` (regex),
  validated **server-side at invocation**. IAM scopes per-document
  (`ssm:StartSession` on specific document ARNs) and can force
  document-only access — no raw shell. This is the model: *typed, validated
  params; the action itself is the ACL object*.
- **Rundeck:** jobs declare options (enum lists, regex validation); ACLs
  grant `run` per job, separate from node/shell access.
- **GitLab manual jobs:** prompt-time CI variables, but gated by branch
  protections rather than per-job ACLs — weaker, not a model to follow.

### Web terminal embedding: xterm.js vs libghostty

- **xterm.js** (`@xterm/xterm` + `@xterm/addon-fit` + `@xterm/addon-webgl`)
  is the production standard (VS Code, Nomad UI, k8s dashboards). Mature,
  canvas/WebGL rendered, actively maintained.
- **libghostty** is a C-ABI library for embedding Ghostty in *native* apps
  (it powers the macOS app). It is **not embeddable in a web page** as a
  full terminal widget. What is real in the browser as of early 2026:
  **libghostty-vt**, the zero-dependency VT parser, compiles to standalone
  WASM, and the community project `coder/ghostty-web` wraps it behind an
  xterm.js-compatible API with its own renderer. No official Ghostty web
  product exists yet (ghostty discussion #3599).

### Distilled requirements

1. One stream protocol end-to-end with typed frames (stdin/stdout/stderr/
   resize/exit), explicit stdin half-close (k8s v5 lesson), and an in-band
   exit frame carrying the code (avoid Docker's poll-after).
2. Task kinds opt in via a capability declaration, checked before a session
   is attempted; isolation-aware ACLs (Nomad's `alloc-node-exec` lesson).
3. Actions are rendered **server-side** from the stored job spec — the
   client sends an action name + params, never argv — so an action-only
   grant cannot be escalated into arbitrary exec.
4. Params are typed and validated server-side (SSM lesson); substitution is
   argv-element-level, never shell string interpolation (injection lesson).
5. Per-action ACL scoping, distinct from raw exec (the gap Nomad hasn't
   closed).
6. Browser support requires a non-gRPC-Web path: gRPC-Web cannot do client
   or bidirectional streaming from a browser, so interactive exec needs a
   WebSocket leg no matter what the native protocol is.

---

## 2. User scenarios

- **S1 — debug shell:** `legion exec <alloc-id> -it -- /bin/sh` drops an
  operator into an interactive shell in a container allocation; resize,
  Ctrl-C passthrough, and exit-code propagation all work; the same shell is
  reachable from a terminal on the allocation's UI page.
- **S2 — one-shot command:** `legion exec <alloc-id> -- cat /etc/resolv.conf`
  prints output and exits with the remote exit code; usable in scripts.
- **S3 — curated action:** a job author ships
  `actions: [{name: "flush-cache", params: [...]}]`; an on-call engineer
  whose token grants only `action:write` on that job runs it from the UI
  with a generated form (enum dropdown, validated string), watches the
  output stream, and **cannot** open a raw shell.
- **S4 — capability honesty:** running `legion exec` against a task whose
  kind doesn't support exec fails fast with a clear error naming the kind,
  both at the CLI and in the UI (button absent/disabled).
- **S5 — audit:** every exec session and action run appears in the event
  stream with principal, allocation, command/action, and exit code.

## 3. Goals / non-goals

**Goals**

- `legion exec` (one-shot and interactive PTY) routed client → manager →
  worker, for the built-in `exec` and `container` kinds.
- `TaskPlugin` capability declaration + per-kind docs table updates.
- Browser terminal (xterm.js) and action-run UI.
- `action` blocks in the job spec with typed, validated, ACL-scoped params.
- New ACL resources `exec` and `action`, default-deny like everything else.
- Audit events; MCP tools for agents.

**Non-goals (this plan)**

- A real SSH gateway (`ssh alloc-id.legion`) — see open questions.
- Session recording/replay (asciinema-style) — audit events only for now.
- Exec into the *node* (host shell without a task) — `legion node ssh` is a
  different feature with different trust math.
- File copy (`legion cp`) — natural follow-on once the stream exists.
- Fan-out actions ("run on all allocs of the group") — single-allocation
  target first; see open questions.

---

## 4. Design

### 4.1 Stream protocol (one protocol, three legs)

A single protobuf frame pair is used on every leg — CLI↔manager,
manager↔worker, and (encoded on binary WebSocket frames) browser↔manager:

```proto
// proto/legion.proto

message ExecClientFrame {
  oneof frame {
    ExecStart start    = 1;  // must be the first frame
    ExecInput stdin    = 2;
    ExecResize resize  = 3;
    ExecPing ping      = 4;  // keepalive, server echoes
  }
}

message ExecStart {
  int64 allocation_id = 1;
  string task_name    = 2;  // optional when the alloc runs a single task
  // Exactly one of `command` (raw exec, needs exec:write) or
  // `action` (needs action:write; manager renders argv server-side).
  repeated string command          = 3;
  string action                    = 4;
  map<string, string> action_params = 5;
  bool tty            = 6;
  ExecWinsize winsize = 7;
  // Browser path only: token via first frame because browsers cannot set
  // WebSocket headers (Nomad ws_handshake precedent). Ignored on gRPC,
  // where the normal authorization metadata is used.
  string bearer_token = 8;
}

message ExecInput   { bytes data = 1; bool eof = 2; }  // eof: stdin half-close
message ExecWinsize { uint32 cols = 1; uint32 rows = 2; }
message ExecResize  { ExecWinsize winsize = 1; }
message ExecPing    {}

message ExecServerFrame {
  oneof frame {
    ExecOutput stdout = 1;
    ExecOutput stderr = 2;   // tty sessions only emit stdout
    ExecExit exit     = 3;   // terminal frame; stream closes after
    ExecPing pong     = 4;
    ExecStarted started = 5; // ack: session is live (UI switches to terminal)
  }
}

message ExecOutput  { bytes data = 1; }
message ExecExit    { int32 code = 1; string message = 2; }
message ExecStarted { string session_id = 1; }
```

New RPCs:

```proto
service Manager {
  // ACL: exec:write (raw command) or action:write (action), enforced on the
  // ExecStart frame after alloc -> job resolution.
  rpc ExecAllocation (stream ExecClientFrame) returns (stream ExecServerFrame);
  // ACL: job:read, scoped by job name. Action discovery for CLI + UI.
  rpc ListJobActions (ListJobActionsRequest) returns (ListJobActionsReply);
}

service Worker {
  // Called only by the manager (worker plane).
  rpc ExecTask (stream ExecClientFrame) returns (stream ExecServerFrame);
}
```

**Decision — typed protobuf frames vs. alternatives considered:**

| Option | Pros | Cons |
| --- | --- | --- |
| **Protobuf oneof frames (chosen)** | one codec on every leg; UI already has generated protobuf classes (`ui/src/gen/`); in-band exit frame; explicit stdin EOF | binary frames are not eyeball-debuggable |
| Nomad-style JSON frames | curl/debug friendly | base64 inflation on bulk output; second codec to maintain alongside the gRPC one |
| k8s channel-byte prefix | minimal overhead | resize/exit become special-cased side channels; no room for fields like `session_id` without inventing v2 |
| Docker hijack + out-of-band resize/exit | trivially raw | resize and exit code as separate HTTP calls is exactly the wart everyone regrets |

**Judgement call:** auth on the ExecStart frame (not at connection time) for
the WebSocket leg. This mirrors Nomad's `ws_handshake` and keeps tokens out
of query strings (which get logged by proxies). On gRPC legs the existing
`authorization` metadata is used and the in-frame token is ignored.

**Judgement call:** the manager **proxies** rather than redirects. Clients
never learn worker addresses, TLS stays terminated at the manager listener,
and the manager is where ACL enforcement and audit events live. Cost: exec
bytes flow through the manager. Acceptable: interactive sessions are
low-bandwidth, and the same trade already holds for `GetAllocationLogs`.

### 4.2 Transports: gRPC bidi + a WebSocket bridge for browsers

The constraint: the UI talks ConnectRPC/gRPC-Web, and **browsers cannot do
client/bidi streaming over gRPC-Web**. Options compared:

| Option | How | Verdict |
| --- | --- | --- |
| **A (chosen): gRPC bidi everywhere + thin WS bridge for browsers** | CLI and manager↔worker use native tonic bidi streams; a tower layer on the existing manager listener upgrades `GET /v1/exec/ws` to WebSocket and pumps binary frames ↔ the same internal session handler | One protocol, two transports. The `OidcHttpLayer` (`src/manager/oidc_http.rs`) already proves the "intercept a plain-HTTP path on the gRPC listener, no axum dependency" pattern; WS adds hyper's upgrade mechanism + `tokio-tungstenite` |
| B: WebSocket only (Nomad's shape) | CLI also speaks WS | Throws away tonic's typed streams where they work fine; CLI gains a second client stack for no benefit |
| C: stay pure gRPC-Web — server-stream for output + unary `SendExecInput(session_id, bytes)` for keystrokes | no new transport at all | Every keystroke is an HTTP request; ordering needs sequence numbers; sessions need server-side registry + GC for orphaned input RPCs. Strictly more machinery than a WS bridge, with worse latency |

The WS bridge handles **both** raw exec and actions (the frame's `start`
oneof decides), so the UI terminal and the UI action-runner share one path.

New CLI plumbing is minimal: `legion exec` opens the bidi RPC with the
existing channel/auth helpers in `src/cmd/helpers.rs`.

### 4.3 Worker side: PTY and plugin contract

New trait surface on `TaskPlugin` (`src/task_plugins/mod.rs`), following the
existing `supports_*` capability-flag idiom:

```rust
/// What a kind can do for `legion exec` / actions.
#[derive(Clone, Copy, Default)]
pub struct ExecSupport {
    /// One-shot + streamed exec (pipes).
    pub exec: bool,
    /// Interactive PTY sessions (`--tty`).
    pub pty: bool,
    /// True when exec'ing means unsandboxed host access (drives the
    /// ACL `host` distinction, like Nomad's alloc-node-exec).
    pub host_level: bool,
}

pub trait TaskPlugin: Send + Sync {
    // ...existing methods...
    fn exec_support(&self) -> ExecSupport { ExecSupport::default() } // none
    fn exec(
        &self,
        allocation: &Allocation,
        task: &TaskConfig,
        runtime: &mut TaskRuntime,
        opts: ExecOptions,        // command, tty, initial winsize, extra env
    ) -> R<Box<dyn ExecSession>> { Err(/* kind does not support exec */) }
}

/// A live session the worker server pumps frames through.
pub trait ExecSession: Send {
    fn stdin(&mut self) -> &mut dyn Write;        // + close_stdin()
    fn read_output(&mut self, buf: &mut [u8]) -> R<ExecOutputChunk>; // stdout/stderr tagged
    fn resize(&mut self, cols: u16, rows: u16) -> R<()>;
    fn try_wait(&mut self) -> R<Option<i32>>;     // exit code
    fn kill(&mut self) -> R<()>;
}
```

Per-kind semantics:

- **`exec` kind** (`src/task_plugins/exec.rs`): spawn the command as a new
  process with the *same execution context the task got*: same `user`, same
  `cgroup`, same `landlock` ruleset (landlock is inherited at spawn — it
  cannot be attached to a foreign process after the fact, so spawning a
  sibling with the same rules is both the only option and the right one),
  cwd defaulting to the task directory, and the worker-injected env
  (`LEGION_TASK_DIR` etc.). `exec_support()` reports
  `{exec: true, pty: true, host_level: true}` — even with landlock this is
  a process on the host, so it carries the host-level ACL flag.
  **Judgement call:** we deliberately do *not* try to join the namespace of
  the running task process (it has none — it's a plain host process);
  "same config, sibling process" is the honest semantic.
- **`container` kind** (`src/task_plugins/container.rs`): delegate to the
  detected runtime binary — `docker exec [-it] <container> CMD` / Apple
  `container exec`. `{exec: true, pty: true, host_level: false}`. The
  `youki` runtime path defers exec support (it needs `youki exec`, which
  has different flags) — `exec_support()` is computed per detected runtime,
  not hardcoded per kind.
- **`noop` and dynamic plugins**: default `ExecSupport::default()` = no
  support; S4's fast failure comes free. The dylib ABI gains the two
  methods with default impls, so existing third-party plugins stay
  source-compatible but must rebuild (the registry already version-gates
  dylibs).

**PTY implementation — options compared:**

| Option | Verdict |
| --- | --- |
| **`portable-pty` (wezterm's crate) — chosen** | Unix `openpty` + Windows ConPTY behind one API; battle-tested in wezterm; Legion explicitly targets macOS/Linux/Windows |
| `nix::pty` + raw `forkpty` | Unix-only; we'd hand-roll ConPTY later |
| `pty-process` | nice tokio API but Unix-only |

For non-tty sessions, plain `std::process::Command` pipes suffice — no PTY
allocated (matches docker/k8s behavior: stderr stays separate only without
a TTY).

The worker enforces session hygiene (config below): max concurrent
sessions, idle timeout, absolute max duration. Sessions die with the
connection (manager drop kills the child) — **judgement call:** no
detach/reattach in v1; that's tmux territory and doubles the state model.

Exec'd processes intentionally do **not** use `spawn_task_process`'s
`setsid` + survive-worker-restart behavior; a session orphaned by a worker
restart should die, not leak.

### 4.4 ACL model

Two new resources extend the closed vocabulary (`src/structs/acl.rs`),
keeping the existing `read`/`write` action pair and rule shape:

| Resource | `read` | `write` |
| --- | --- | --- |
| `exec` | (reserved; no read surface in v1) | open an interactive/one-shot exec session |
| `action` | (covered by `job:read` for discovery) | run a declared action |

New rule scope selectors (alongside today's `names`/`paths`/`pools`):

- `action_names: [glob]` — action names, on `action` rules (final selector
  name pending, Q2 — `actions` would collide with the rule's existing
  read/write field).
- `host: true|false` on `exec` rules — when absent, the rule matches only
  sessions into kinds with `host_level: false`; `host: true` is required to
  exec into host-level kinds (the `exec` task kind). This is Nomad's
  `alloc-exec` vs `alloc-node-exec` split expressed in Legion's
  selector idiom rather than as a third resource.

```jsonnet
// Full shell into team-a's container jobs, but not host-level tasks:
{ resources: ["exec"],   actions: ["write"], names: ["team-a-*"] },
// Host-level too (admins):
{ resources: ["exec"],   actions: ["write"], names: ["*"], host: true },
// On-call: may run only the curated restart/flush actions on the web job
// (selector name pending — see open question Q2):
{ resources: ["action"], actions: ["write"], names: ["web"],
  action_names: ["restart-*", "flush-cache"] },
{ resources: ["job"],    actions: ["read"],  names: ["web"] },
```

**Decision — why not Nomad-style named capabilities** (`alloc-exec`,
`alloc-node-exec`, `action-exec`): Legion's whole ACL system is six
resources × two actions with scope selectors, and the auth plan
(`docs/plans/auth-acl-plan.md`) explicitly chose a small closed vocabulary.
Adding two resources + two selectors preserves the mental model; adding a
parallel capability vocabulary just for exec would fork it.

**Judgement call — does `exec:write` imply `action:write`?** Yes, by
union at enforcement: when a caller asks to run an action, the manager
allows it if *either* an `action:write` rule matches (job name + action
name) *or* an `exec:write` rule matches (job name + the host flag for that
task's kind). Rationale: anyone who can open a shell can trivially type the
action's command anyway; forcing them to also hold `action:write` would be
ceremony, not security. The reverse implication does **not** hold — that is
the entire point of actions.

**Security invariant (the load-bearing one):** for action runs the client
sends `{action, params}` only. The manager loads the job spec it has on
record, validates the params against the declared schemas, renders argv
itself, and forwards a fully-resolved raw command to the worker. A client
holding only `action:write` has no input path that reaches argv except
through validated parameters.

Enforcement placement follows AGENTS.md ("Auth & ACL Checks"): the
streaming handler authenticates the transport, then enforces on the parsed
`ExecStart` (two-step `authenticate_acl` → `acl::enforce` form, coarse
`AclScope::any()` gate before the alloc→job lookup to avoid existence
probing). `tests/acl_endpoints.rs` gains entries for `ExecAllocation` and
`ListJobActions` — the `every_manager_rpc_is_classified` test makes this
non-optional.

### 4.5 Actions in the job spec

`TaskConfig` (`src/structs/job.rs`) gains an `actions` field:

```jsonnet
// jobspec.jsonnet
{
  name: "web",
  groups: [{
    name: "app",
    tasks: [{
      name: "api",
      kind: "container",
      config: { image: "ghcr.io/acme/api:1.4", command: ["/srv/api"] },
      actions: [
        {
          name: "flush-cache",                       // [a-z0-9-]{1,128}, unique per task
          description: "Flush the in-memory cache",
          command: ["/srv/api", "admin", "flush-cache", "--scope", "${param.scope}"],
          params: [
            {
              name: "scope",
              description: "Which cache to flush",
              type: "enum",                          // string | int | bool | enum
              values: ["all", "sessions", "assets"],
              default: "sessions",
            },
          ],
        },
        {
          name: "tail-slowlog",
          command: ["/usr/bin/tail", "-n", "${param.lines}", "/var/log/slow.log"],
          params: [
            { name: "lines", type: "int", default: "100", min: 1, max: 10000 },
          ],
          timeout: "60s",                            // optional run cap
        },
      ],
    }],
  }],
}
```

Schema rules (validated at job submission *and* re-validated at invocation
against the stored spec):

- `name` required, `command` required non-empty argv array.
- `params[]`: `name`, `type` (`string` default), `required`
  (default false when `default` present, true otherwise), and per-type
  constraints — `pattern` (anchored regex) for strings, `min`/`max` for
  ints, `values` for enums. A string param without `pattern` is allowed but
  the docs will nudge toward constraints (SSM lesson: server-side
  validation is the defense).
- `tty: true|false` (default false) and optional `timeout`.
- `${param.x}` interpolates **inside argv elements** (and nowhere else).
  Interpolation never introduces word-splitting: the rendered value replaces
  the placeholder within that single argv element. There is deliberately
  no shell evaluation; an author who writes
  `command: ["/bin/sh", "-c", "echo ${param.x}"]` has opted into shell
  semantics for *their template*, and submission emits a warning when a
  param placeholder appears inside an argv element alongside `sh -c`-style
  commands. **Judgement call:** warn, don't reject — there are legitimate
  uses, and the param value still arrives pre-validated.
- Unknown params at invocation are rejected; missing required params are
  rejected; params not declared cannot be smuggled (strict allowlist).

**Decision — where do actions live?**

| Option | Verdict |
| --- | --- |
| **In the job spec, per task (chosen)** | Versioned/reviewed with the workload, like Nomad; the command is meaningless apart from the task image/binary it runs against; no new top-level object, no new sync problem |
| Cluster-level action registry (Rundeck-style standalone jobs) | Decouples ops playbooks from deploys, but invents a second spec format, a second store, and a binding problem ("which actions apply to which tasks?"); defer until demanded |
| KV-stored actions | Abuses KV as a config store and bypasses job plan/review flow |

**Decision — execution path reuses the exec stream** (Nomad got this part
right): an action run *is* an exec session whose argv was rendered
server-side. One worker code path, one audit shape, streaming output and
exit codes for free, `tty` actions work in the UI terminal unchanged.

### 4.6 CLI

```bash
# One-shot (S2): prints output, exits with the remote exit code.
legion exec 4217 -- cat /etc/resolv.conf

# Interactive shell (S1): -i wires stdin, -t allocates a PTY (raw mode,
# SIGWINCH -> resize frames, exit code propagated).
legion exec 4217 -it -- /bin/sh

# Multi-task allocations need --task; single-task allocs infer it.
legion exec 4217 --task api -it -- /bin/bash

# Job-level convenience: pick a running allocation of the job (newest).
legion exec --job web -it -- /bin/sh

# Actions.
legion action list web
legion action run web flush-cache --param scope=assets         # newest alloc
legion action run web tail-slowlog --alloc 4217 --param lines=500
legion action run web debug-shell --alloc 4217 -it             # tty action
```

Conventions: `--` separates the remote argv (clap `trailing_var_arg`);
`-i/--stdin` and `-t/--tty` mirror docker/kubectl muscle memory; non-tty
one-shot is the default so scripting stays safe. `legion action` is a new
top-level command group (`src/cmd/action.rs`), `legion exec` a new
top-level command (`src/cmd/exec.rs`) registered in `src/main.rs`.
**Judgement call:** top-level `legion exec` rather than `legion job exec`
— it targets an allocation, not a job, and it's the highest-frequency
debugging verb; burying it costs more than the namespace purity buys
(precedent: `kubectl exec`, `nomad alloc exec` both keep it shallow).

Terminal handling in the CLI: `crossterm` for raw mode + winsize (already
the ecosystem default; check `Cargo.toml` — if a terminal crate is already
present, reuse it). `~.` escape sequence (SSH convention) force-terminates
a wedged session.

### 4.7 UI

Two surfaces, sharing one `ExecSocket` client module (WS + protobuf frame
encode/decode) and one `<Terminal>` component:

1. **Exec terminal** — on the allocation row / `TaskDetail.tsx`: an "Exec"
   button (rendered only when the kind's `exec_support` says so and
   `WhoAmI`-derived permissions allow) opens a terminal panel:
   command input (default `/bin/sh`), tty toggle, then an xterm.js terminal
   bound to the WS bridge. `@xterm/addon-fit` drives `ExecResize` frames.
2. **Action runner** — on `JobDetail.tsx`/`TaskDetail.tsx`: an Actions
   panel listing declared actions (`ListJobActions`), each with a form
   generated from the param schema — `enum` → select, `bool` → toggle,
   `int`/`string` → validated input with the same constraints client-side
   (UX only; the server re-validates). Run → output streams into the panel
   (plain log pane for non-tty actions, full terminal for `tty: true`),
   ending with a green/red exit-code chip. Allocation picker defaults to
   the newest running alloc.

**Decision — terminal renderer: xterm.js, not libghostty (for now).**

| Option | Assessment |
| --- | --- |
| **`@xterm/xterm` + fit + webgl addons (chosen)** | Production-grade today; what Nomad's and k8s's UIs ship; trivial Solid integration (mount on a ref) |
| `coder/ghostty-web` (libghostty-vt WASM + community renderer) | Real and interesting, xterm.js-compatible API — but community glue around an unstable upstream; not something to bet the only interactive surface on yet |
| Full libghostty | Native C ABI for macOS/desktop apps; **cannot be embedded in a web SPA at all** — only applicable if Legion ever ships a desktop shell |

The brief asked for libghostty "if it makes sense": for a SolidJS web SPA
it does not yet. The hedge: keep the `<Terminal>` component's contract to
the xterm.js surface that ghostty-web already mimics (`open`, `write`,
`onData`, `resize`), so swapping renderers later is a one-file change.
Revisit when libghostty-vt stabilizes (tracked as Q5).

### 4.8 Configuration

```jsonnet
// configs/manager.jsonnet
{
  exec: {
    enabled: true,            // master switch for ExecAllocation + WS bridge
    sessionMaxDuration: "4h", // absolute cap, 0 = unlimited
    idleTimeout: "15m",       // no frames in either direction -> close
  },
}

// configs/worker.jsonnet
{
  exec: {
    enabled: true,            // worker-side refusal independent of manager
    maxSessions: 16,          // concurrent sessions per worker
  },
}
```

**Judgement call:** `exec.enabled` defaults to **true** — with ACLs
enabled, exec is already default-deny per-caller, and "UI out of the box"
is a stated differentiator; a security-hardened site can flip the worker
switch off and no manager setting can override it. Audit events fire
unconditionally.

### 4.9 Audit & observability

New scheduler events (existing event-stream machinery,
`docs/event-stream.md`): `exec_session_started` / `exec_session_ended`
(principal, accessor id, allocation, task, argv or action name + params,
tty flag, exit code, duration) and `action_run_*` variants. Param values
are included in events — see Q3 for the `sensitive` flag question. Spans
follow the existing OTEL conventions (`legion.allocation.id` etc.).

### 4.10 MCP

Per AGENTS.md, agent-relevant operations get MCP tools (`src/cmd/mcp.rs`):
`legion_action_list` (read-only), `legion_action_run` and `legion_exec_run`
(one-shot only, `mutates: true`, gated behind `--allow-write`). No
interactive PTY over MCP.

---

## 5. Delivery phases

1. **Worker exec core** — `ExecSupport`/`exec()` on `TaskPlugin`,
   `portable-pty` dependency, `exec` + `container`(docker) kind
   implementations, `Worker.ExecTask` bidi RPC, session limits/timeouts.
   Tests: in-process worker exec of `echo`/`cat` (pipe + pty), kill on
   disconnect.
2. **Manager proxy + CLI** — `Manager.ExecAllocation` (proxying,
   ACL `exec` resource + `host` selector, audit events,
   `tests/acl_endpoints.rs` entries), `legion exec` with raw mode, resize,
   exit-code propagation, `--job` convenience.
3. **Browser path** — WS bridge tower layer (hyper upgrade +
   `tokio-tungstenite`, first-frame auth), `ExecSocket` TS module,
   `<Terminal>` (xterm.js) on TaskDetail, capability-aware button.
4. **Actions** — spec schema + submission validation, `ListJobActions`,
   server-side render + param validation in `ExecAllocation`, `action`
   ACL resource + `actions` selector + the exec⊇action union rule,
   `legion action list|run`.
5. **Actions UI + agents + docs** — Actions panel with generated forms,
   MCP tools, `docs/exec-and-actions.md`, capability-table updates in
   `docs/task-kind-*.md` and `legion docs`/`legion capabilities` output.

Each phase is independently shippable; 1–2 alone deliver S2/S1 for the CLI.

## 6. Open questions — resolved

All eight were decided before implementation. Decisions and rationale:

- **Q1 — youki exec: ship without it.** Container exec lands for the docker
  and Apple `container` runtimes; the youki path reports
  `exec_support().exec = false` (honest capability reporting, S4). Youki
  exec needs its own flag handling and its image-loading story is itself
  still pending — blocking every other surface on it buys nothing.
- **Q2 — selector naming: `action_names`.** ACL rules already carry an
  `actions: ["read","write"]` field; reusing `actions` as a scope selector
  would make one JSON key mean two things in the same object. Verbose but
  unambiguous, and consistent with the plural selector style (`names`,
  `paths`, `pools`).
- **Q3 — sensitive params: yes, in the initial schema.**
  `params[].sensitive: true` redacts the value in audit events, CLI echo,
  and UI history. Added now because events are persisted — retrofitting
  redaction cannot fix already-written history.
- **Q4 — Windows workers: experimental.** `portable-pty` gives us ConPTY,
  but with zero Windows CI the honest label is experimental; documented as
  such in the capability tables. No code is gated — it just isn't promised.
- **Q5 — libghostty: xterm.js now, revisit on trigger.** Adopt a
  ghostty-based renderer only when (a) an official libghostty-vt WASM
  release exists and (b) a maintained renderer passes a terminal acceptance
  check (vim, resize, mouse, 256color). The `<Terminal>` component keeps to
  the xterm.js-compatible surface so the swap stays a one-file change.
- **Q6 — job-scoped target: newest running alloc, with a stateful guard.**
  `legion exec --job` / `legion action run <job>` without `--alloc` picks
  the newest *running* allocation and prints which one it picked. When the
  target task is stateful (allocations are not interchangeable), an explicit
  `--alloc` is required. Random selection (Nomad's choice) hides the
  decision from the operator; newest-with-echo keeps it debuggable.
- **Q7 — SSH gateway: nothing extra needed now.** The frame protocol already
  carries everything an SSH front-end would bridge (argv, tty flag,
  winsize/resize, stdin half-close, in-band exit code, `session_id`). A
  future gateway is additive; out of scope here.
- **Q8 — staging allocations: active-only in v1.** Exec targets `active`
  allocations; anything else fails with the allocation's current state in
  the error. Exec-into-staging is deferred until the artifact-cache plan
  settles staging observability.

## 7. References

- `docs/plans/auth-acl-plan.md` — ACL vocabulary rationale this plan extends.
- `src/task_plugins/mod.rs` — `TaskPlugin` trait + `supports_*` idiom.
- `src/manager/oidc_http.rs` — precedent for plain-HTTP routes on the gRPC listener.
- `src/manager/dispatcher.rs` — manager→worker dialing reused by the exec proxy.
- Nomad: alloc exec API & PR #6604 (protocol), action block docs, issues
  #18627 / #19391 (params + per-action ACL gaps).
- Kubernetes: WebSocket transition blog (1.31), `v5.channel.k8s.io`,
  issue #89899 (stdin-close flaw).
- AWS SSM documents (typed/validated params, per-document IAM) and Rundeck
  job options/ACLs.
- libghostty: mitchellh.com "Libghostty Is Coming", ghostty discussion
  #3599, `coder/ghostty-web`.
