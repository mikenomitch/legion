# Plan: Sandbox Exec

> **Judgement calls:** [`calls/sandbox-exec-plan_calls.md`](../calls/sandbox-exec-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-11). Phases 1 (exec into
microVMs over vsock), 2 (direct exec), and 3 (delegated minting keys) shipped
on 2026-10-12 with worker-plane-auth Phases 0 to 8, and Phase 0 is measured
on one host; deviations are calls 6 and 9 to 26. Open: Phase 0 at scale on
the rig, Phase 1 on KVM hardware (the steps are in §11, "Verification
record"), and Phases 4 and 5. The rest is below. This plan
is the brief for implementing exec into microVM sandboxes together with
[worker-plane-auth-plan.md](worker-plane-auth-plan.md) (PR
[#16](https://github.com/mikenomitch/legion/pull/16)). Deliberately
deferred and named: detach and reattach of exec sessions, file transfer and
port forwarding over exec, `exec` over vsock for `container` tasks, and
exec in macOS dev mode beyond what Apple `container exec` already gives
(§4).

**Relationship to existing plans.**

- [worker-plane-auth-plan.md](worker-plane-auth-plan.md) (PR #16) owns
  authentication of the worker listener: always mTLS with the node
  certificate, manager-only RPCs checked by a `legion://manager/<id>` URI
  SAN, and `StartClaim` authorized by its boot ticket. This plan designs no
  competing scheme. Its three amendments to that plan (§9) are folded into
  worker-plane-auth §6.1.
- [gated-sandbox-checkpoints-plan.md](gated-sandbox-checkpoints-plan.md)
  owns the guest agent, its static build (its Phase 2), the constant vsock
  CID, and the per-allocation `vsock_override` on every restore (its Phase
  1). Exec into microVMs reuses all of them and shares host-initiated vsock
  with its Phase 5 (ingress).
- [exec-and-actions-plan.md](exec-and-actions-plan.md) owns `legion exec`,
  actions, the `exec` and `action` ACL resources, and the manager proxy.
  Nothing in that surface changes.
- [sandbox-scaling-plan.md](sandbox-scaling-plan.md) owns session mode,
  where an unnamed session has no raft row. Phase 5 here resolves exec
  targets through its session index (scaling plan §6.4 and §6.12), and the
  gateway tier in Phase 4 is shared with its wake on request (scaling plan
  §6.14).

---

## The plan in one page

An agent sandbox that cannot be entered is not much of a sandbox. Today a
Linux microVM takes no exec sessions at all, and where exec does work,
every byte of every session flows through a manager. This plan fixes both,
in phases that each ship alone:

| Phase | What ships | Needs first |
| --- | --- | --- |
| 0 | Measure exec today: open latency and manager cost per session | nothing |
| 1 | **Exec into Linux microVMs over vsock.** Every microVM gets a vsock device and the guest agent. `legion exec`, actions, MCP, and the UI terminal work unchanged | gated plan Phases 0 and 1, worker-plane-auth Phase 1 |
| 2 | **Direct exec.** A manager mints a short-lived exec ticket with no raft write, and the client talks to the worker over the mTLS worker listener. A third caller class in worker-plane-auth's table | worker-plane-auth Phase 1 plus the §9 amendments |
| 3 | **Delegated minting keys.** A platform signs exec tickets for its own jobs with no manager call | Phase 2 |
| 4 | **The gateway tier.** A stateless `legion start gateway` that verifies tickets and forwards to workers, for browsers and private workers | Phase 2 |
| 5 | **Exec into unnamed sessions**, resolved through the scaling plan's session index | Phase 2 and scaling plan Phase 6 |

The manager-proxied path stays the default for people and the CLI. Direct
exec is opt-in.

---

## 1. Problem statement

**A microVM cannot be entered.** `legion exec` and actions work on `exec`
and `container` tasks and on macOS dev-mode sandboxes. On Linux, the
`sandbox` runtime reports no exec support: `exec_support()` returns false
because "exec into a microVM needs a vsock guest agent (deferred)"
(`src/task_plugins/sandbox.rs:1551`), and the public table shows
`sandbox (microVM)` without exec or tty (`docs/exec-and-actions.md`,
"Which task runtimes support exec"). The `exec` idle signal never fires on
a microVM, and `job validate` warns about it (`src/structs/suspend.rs:713`).

What a platform does instead: ships its own daemon inside the guest and its
own route to it, the way E2B ships `envd`, or runs commands through the
workload's own API. An on-call responder with a stuck agent has no shell at
all.

**Every exec byte goes through a manager.** CLI, UI, and MCP sessions
stream client → manager → worker, and the manager holds both legs for the
session's life (`docs/exec-and-actions.md`, "How it works"). An agent
platform with thousands of concurrent tool calls inside sandboxes puts all
of that I/O on the same three processes that run raft.

**The worker listener is unauthenticated, and only the bind protects it.**
The worker serves `PostWork`, `ExecTask`, `GetTaskLogs`, and
`RestartWorker` with no authentication and no TLS
(`src/worker/server.rs:55`, `:200`, built with no interceptor at
`src/worker/daemon.rs:318`). The proto says `ExecTask` is "called only by
the manager" (`proto/legion.proto:7224`), and the only thing that makes it
true is the bind: the worker listens on `127.0.0.1:4848`
(`src/worker/daemon.rs:155`). So the gap is local, not remote. Any process
on the worker's own host can call `ExecTask` against any allocation on that
node. That includes workloads Legion runs: an `exec` task and a
host-network container can open a shell in their neighbors. The
worker-plane-auth plan (PR #16) closes this with mTLS and manager-only
caller classes. This plan depends on it, for two reasons:

- Phase 1 makes every microVM sandbox exec-able. Shipped on today's
  listener, it would let any local host process shell into every sandbox
  on the node.
- Direct exec needs a worker listener that clients can reach, which is
  exactly what that plan makes safe.

## 2. Prior art

Nobody who runs sandboxes at scale keeps every shell byte on the control
plane, and the one system that tried redirects for exec abandoned them for
security reasons.

- **Modal** is the closest precedent for direct exec. The legacy path runs
  through the control plane (`ContainerExec`, `ContainerExecGetOutput`).
  The newer `TaskGetCommandRouterAccess` returns a JWT and a URL "for
  direct access to a task command router running on the modal-worker",
  and the V2 reply adds the worker's IP, so the client connects to it
  while using the URL's hostname for TLS SNI. Create and restore replies
  embed that access, so there is no extra round trip. The worker-side
  service makes exec idempotent with a client `exec_id` and stdio
  resumable by offset, and the client re-fetches its JWT on
  `UNAUTHENTICATED`
  ([api.proto](https://github.com/modal-labs/modal-client/blob/main/modal_proto/api.proto),
  [task_command_router.proto](https://github.com/modal-labs/modal-client/blob/main/modal_proto/task_command_router.proto),
  [sandbox exec](https://modal.com/docs/guide/sandbox-spawn)).
  - **Take:** a control-plane-signed, short-lived token for one worker,
    and an address the client dials directly. **Defer:** offset-resumable
    stdio, which is the reattach Legion has never had (non-goal).
- **E2B** runs `envd`, a Go daemon inside each Firecracker VM, on port
  49983 with connect-rpc `process` and `filesystem` services, including a
  pty mode and a reattach call
  ([envd](https://github.com/e2b-dev/infra/tree/main/packages/envd)).
  The SDK addresses `49983-<sandboxId>.e2b.app`, and a client proxy looks
  the sandbox up and forwards to its node
  ([client-proxy](https://github.com/e2b-dev/infra/tree/main/packages/client-proxy)).
  Every call carries a per-sandbox `X-Access-Token`. Without it, "anyone
  with a sandbox ID" can control the sandbox
  ([secured access](https://e2b.dev/docs/sandbox/secured-access)).
  - **Take:** an in-guest agent is the norm, and a stateless edge can route
    on the name alone. **Reject:** one long-lived bearer secret per
    sandbox. A Legion ticket is good for one session on one node.
- **Fly.io** splits the two cases. `fly ssh console` reaches `hallpass`, a
  small SSH server in each VM bound to the private 6PN address over
  WireGuard. It trusts a per-org SSH CA, and the API signs short-lived
  user certificates
  ([blog](https://fly.io/blog/ssh-and-user-mode-ip-wireguard/)).
  `fly machine exec` is synchronous request and response through the
  Machines API ([docs](https://docs.fly.io/flyctl/machine-exec)).
  - **Take:** a target that trusts a CA signature needs no per-user state
    and no call home per session.
- **Kubernetes** never took the API server out of the exec data path.
  `kubectl exec` goes client → API server → kubelet, now over WebSockets
  ([KEP-4006](https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/4006-transition-spdy-to-websockets/README.md)).
  `--redirect-container-streaming` let the API server follow a redirect
  to the CRI streaming server. It opened an SSRF hole (CVE-2018-1002102),
  was deprecated in 1.18, and its gate was removed in 1.24. The KEP called
  the gain "marginal", because the API server still proxied
  ([KEP-1558](https://www.kubernetes.dev/resources/keps/1558/)). KEP-4006
  also had to close a gap where a WebSocket upgrade, being a GET, let
  get-only roles exec.
  - **Take:** the credential the client presents to the worker must be
    what authorizes the session. A redirect the server follows is not.
    Authorize the exec verb, never the transport.
- **Cloudflare** routes the Sandbox SDK through a Worker and the sandbox's
  Durable Object, which authenticates and forwards to the container
  ([architecture](https://developers.cloudflare.com/sandbox/concepts/architecture/)).
  Preview URLs carry a random token in the hostname
  ([preview URLs](https://developers.cloudflare.com/sandbox/concepts/preview-urls/)).
  For SSH, Access issues short-lived certificates from a Cloudflare CA
  that servers trust in `TrustedUserCAKeys`
  ([infrastructure access](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/use-cases/ssh/ssh-infrastructure-access/)).
  - **Take:** the gateway tier (Phase 4) is Cloudflare's shape, a stateless
    router in front of a target that verifies on its own.
- **Kata Containers** runs `kata-agent` in the guest and drives it with
  ttRPC over vsock: `ExecProcess`, `WriteStdin`, `ReadStdout`,
  `TtyWinResize`, `SignalProcess`, `WaitProcess`
  ([agent.proto](https://github.com/kata-containers/kata-containers/blob/main/src/libs/protocols/protos/agent.proto)).
  Firecracker's host side is a Unix socket where the host writes
  `CONNECT <port>` and reads `OK <port>`
  ([vsock.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md)).
  - **Take:** vsock to a guest agent is the standard host-to-guest exec
    channel, and the agent is untrusted plumbing.
- **Teleport** issues certificates that live minutes to hours, and servers
  check only the CA signature and expiry, with session locks for urgent
  revocation ([architecture](https://goteleport.com/docs/reference/architecture/authentication/)).
  [RFC 9449 §11.1](https://www.rfc-editor.org/rfc/rfc9449.html#section-11.1)
  recommends a `jti` cache held for the proof's short window, and admits
  that strict single use across servers with no shared state is not
  feasible.
  - **Take:** bind each ticket to one worker, so a local `jti` cache is
    strict single use without shared state.

## 3. Jobs to be done, by persona

**Platform builder (the sandbox service)**
- When my agent needs to run a tool inside its sandbox, I want to run the
  command and stream its output from my own backend straight to the
  machine, so my control plane's capacity is not my agents' I/O budget.
- When I start a sandbox at the edge, I want to open a shell into it
  without another round trip to a control plane, so the shell is as fast as
  the start.

**On-call responder**
- When a user says "my agent is stuck", I want a shell inside their microVM
  with the same command I use for containers, so I can look around before I
  restart it.
- When I run a runbook step, I want the same actions to work on sandboxes
  as on containers, so I don't need a second runbook.

**Security / platform admin**
- When clients open shells on worker machines directly, I want each
  permission to be narrow, short-lived, and good for one shell, so a leaked
  one is worth almost nothing.
- When I disable a credential, I want its open shells to end soon, so
  revocation means something.
- When anything on a worker's host can reach the worker's API, I want that
  API to refuse everyone but my managers and holders of a valid ticket, so
  a compromised workload cannot shell into its neighbors.

**Autonomous agent**
- When I run a one-shot command in a sandbox, I want the same structured
  result I get from a container, with the process's real exit code, so my
  loop does not special-case runtimes.

**Cluster operator**
- When I turn on direct exec, I want it to reuse the worker port and
  certificates I already run, so it is a config flag, not a second PKI.

### User scenarios

1. **A shell in a stuck sandbox.** A responder runs
   `legion exec --instance user-42 -it -- /bin/bash`. The manager proxies
   the session as it does for containers. On the worker, the runtime dials
   the guest's vsock, and the agent starts bash under a pty. Resize works.
   `exit 3` returns 3.
2. **Ten thousand tool calls a minute.** A platform backend calls
   `MintExecTicket` for each tool call (or signs tickets with its delegated
   key), then opens `ExecTask` on the worker directly over mTLS. The
   managers see one small unary call per session, or none, and no session
   bytes.
3. **A leaked ticket.** A ticket shows up in a log. It names one node, one
   allocation, one command, and expired 60 seconds after minting. If it was
   already used, it is worth nothing. If not, the thief can run that one
   command once on that one target within the window.

## 4. Goals and non-goals

**Goals**
- `legion exec`, `legion action run`, the MCP exec tools, and the UI
  terminal work on Linux microVM sandboxes (Firecracker and
  cloud-hypervisor), with no change to those surfaces.
- The `exec` idle signal fires on Linux microVMs.
- Exec sessions can skip the manager on request, with no raft write per
  session and no manager in the byte path.
- Every exec session is authorized at the worker, by a manager certificate
  (per the worker-plane-auth plan) or by an exec ticket (this plan).
- Exec open latency and manager cost per session are measured and have
  targets (§7).

**Non-goals**
- **A competing worker authentication scheme.** The worker-plane-auth plan
  owns the listener, its TLS, and its caller classes. This plan adds one
  class to it (§9).
- **Detach and reattach**, output offsets, file transfer, and port
  forwarding. Sessions still end when the connection closes. Modal's
  offset-resumable stdio is the reference if this is ever wanted.
- **Exec into `container` tasks over vsock.** They keep `<runtime> exec`.
- **Public internet exec without a gateway.** Direct exec targets callers
  that can already route to workers. Browsers on the public internet wait
  for Phase 4.
- **New ACL resources.** `exec`, `action`, `names`, `action_names`, and
  `host` stay the vocabulary.

## 5. Background: what exists today

**The exec plumbing is complete above the runtime.**
- `TaskPlugin::exec_support()` returns `ExecSupport { exec, pty,
  host_level }`, and `TaskPlugin::exec()` returns a `Box<dyn ExecSession>`
  (`src/task_plugins/mod.rs:472`, `:834-848`).
- `ExecSession` is `take_output`, `write_stdin`, `close_stdin`, `resize`,
  `wait`, `try_wait`, and `kill` (`src/task_plugins/exec_session.rs:61`),
  with `ExecOptions { command, tty, winsize, cwd, env }`.
- `Worker.ExecTask` (`proto/legion.proto:7227`) drives a session on
  dedicated threads and counts open sessions per allocation in
  `OPEN_SESSIONS`, which feeds the idle policy's `exec` signal
  (`src/worker/exec.rs:37-70`).
- `Manager.ExecAllocation` (`proto/legion.proto:1108`) authorizes with a
  coarse `exec`/`action` gate before any lookup and the job scope after it,
  renders an action's argv from the stored spec, resolves the worker, and
  splices the stream (`resolve_exec_start`, `src/manager/exec.rs`). The
  session re-checks its credential every 30 seconds.
- The UI uses a WebSocket bridge on the manager port + 1 that runs the same
  core, authenticated by `ExecStart.bearer_token` in the first frame
  (`src/manager/exec_ws.rs`).
- `runtime_is_host_level("sandbox")` is false
  (`src/task_plugins/mod.rs:459`), so the `exec` ACL without `host` already
  covers sandboxes.

**The microVM runtime.** `exec_support()` reports exec only on a backend
that supports it, which today is macOS dev mode
(`src/task_plugins/sandbox.rs:1551`). Only gated sandboxes get a vsock
device (`src/task_plugins/sandbox.rs:1128`). The guest init starts
`/legion/legion guest-agent`, which relays guest-initiated traffic to host
CID 2 and holds no host-initiated listener (gated plan §5). A gated
sandbox's vsock path is baked into its memory checkpoint until the gated
plan's `vsock_override` lands (gated plan §6.2).

**Offline ticket verification exists.** Workers verify boot tickets against
a cached JWKS with a bounded clock skew and a local spent-`jti` set that
forgets entries when their ticket expires (`src/worker/boot_ticket.rs`).
The verifier is built only when `edgeBoot.acceptClaims` is on
(`src/worker/daemon.rs:297`).

**The worker listener** binds `127.0.0.1:4848` with no authentication
(§1). The worker-plane-auth plan makes it mTLS on all interfaces, with
manager-only RPCs and `StartClaim` authorized by its ticket.

## 6. Proposed design

### 6.1 Exec into microVMs over vsock (Phase 1)

- **Every microVM gets a vsock device and the guest agent**, whatever its
  network mode. The path is the gated plan's per-allocation
  `<alloc>/.sandbox/vsock.sock` with the constant CID 3 (gated plan §6.2).
- **The agent listens on one guest vsock port for exec** (proposed 1100)
  and nothing else new. For `tap` and `none` sandboxes the worker binds no
  guest-facing listeners, so the guest still cannot dial the host. The
  device carries host-initiated connections only, and `none` keeps its air
  gap for anything the guest starts. This amends the gated plan's
  definition of `none` ("no vsock device, no agent") and is recorded in
  calls entry 2.
- **The worker's `exec()` connects to the guest.** A new
  `VsockExecSession` dials `uds_path`, sends `CONNECT 1100`, reads
  `OK <port>` (Firecracker's convention, which cloud-hypervisor shares),
  and speaks length-delimited `ExecClientFrame` and `ExecServerFrame`
  protobufs, the frames every other exec leg uses. The first exchange is a
  version hello, because a restored guest can run an older agent than its
  worker, and the gated plan requires any agent protocol beyond raw bytes
  to negotiate (gated plan §6.3).
- **The agent spawns the command** under a pty (`openpty` in the guest) or
  with pipes, in the task's environment, working directory, and user from
  the identity disk. Stdin, output, and resize frames ride the connection.
  The exit status is the process's own. Closing the connection kills the
  process group, which is how both the worker's `kill()` and a client
  disconnect land.
- **The host treats the agent as untrusted plumbing**, as Kata does. The
  worker bounds frame sizes and concurrent sessions per allocation and
  parses nothing from the guest except frames. Exec into a sandbox is root
  in the guest, the same promise as `container exec`.
- **Suspend and resume.** A memory capture resets open vsock connections
  (gated plan §6.3), so an open session ends with a typed reason:

  ```text
  session ended: instance user-42 was suspended (exit status unavailable)
  ```

  After a resume, the VMM listens on the new allocation's path through
  `vsock_override`, and a new session reaches it. A resume is a new
  allocation, so callers holding an instance id use the new `--instance`
  flag on `legion exec` and `legion action run`, which resolves the current
  allocation.
- **The `exec` idle signal works** with no new code, because the worker
  already counts open sessions per allocation. Remove the warning at
  `src/structs/suspend.rs:713` and the matching rows in
  `docs/suspend-and-resume.md`.
- **Unchanged surfaces.** `legion exec`, `legion action run`,
  `legion_exec_run`, `legion_action_run`, and the UI terminal need no
  change. `exec_support()` reports `exec` and `pty` true on a microVM
  backend.

```console
$ legion exec --instance user-42 -it -- /bin/bash
root@user-42:/workspace# exit 3
$ echo $?
3
$ legion action run agents flush-cache --instance user-42 -j
{"ok":true,"data":{"exitCode":0,"stdout":"flushed 412 entries\n","stderr":""}}
```

An image the agent cannot run on fails by name, before gated plan Phase 2
ships the static agent:

```text
Error: sandbox exec needs the guest agent, which did not start in this guest
Hint: until the static legion-guest ships, the agent is the worker's glibc binary,
which musl images such as alpine cannot run
```

**Dependency order for Phase 1.**

1. Gated plan Phase 0: a gated sandbox booted on a real VMM.
2. Gated plan Phase 1: `vsock_override` on every restore. This is a hard
   prerequisite. Memory checkpoints are refused for gated sandboxes today
   because the vsock path is baked into the VMM's saved state. Giving every
   microVM a vsock device before the override exists would spread that
   refusal to TAP and `none` sandboxes, which checkpoint fine today.
3. Worker-plane-auth Phase 1 (PR #16). Phase 1 makes every sandbox
   exec-able, and on today's listener any process on the worker's host
   could then shell into every sandbox on the node (calls entry 4).
4. This plan's Phase 1. Host-initiated vsock and the agent's vsock listener
   are shared with gated plan Phase 5 (ingress). Whichever lands first
   builds them, and the other reuses them.
5. Gated plan Phase 2's static `legion-guest` widens the images exec works
   on.

Firecracker 1.17.0 fixed host-initiated RX stalling after a bare pause and
resume (gated plan §2), so Phase 1 sets that as the floor for exec.

### 6.2 Manager-proxied exec on the authenticated worker plane

After worker-plane-auth Phase 1, the manager dials `ExecTask` with its
`legion://manager/<id>` client certificate, and the worker admits it as a
manager-class call. This plan changes nothing about that path. Its
authorization stays where it is today, in `resolve_exec_start` and the
30-second credential re-check. It stays the default for the CLI, the UI,
and MCP, because it needs no route from the client to the worker.

### 6.3 Direct exec with tickets (Phase 2)

**The shape.** A manager mints a short-lived exec ticket. The client opens
`ExecTask` on the worker over the mTLS worker listener, presenting the
ticket in the first frame. The worker verifies it offline. This is Modal's
command-router shape, a signed token plus a worker address, and it avoids
what Kubernetes got wrong with streaming redirects: the worker follows
nothing, and the ticket the client presents is what authorizes the session
(§2).

An exec ticket is a JWT signed with the cluster's workload-identity key,
the same signer and JWKS boot tickets use:

```jsonc
{
  "aud": "legion:exec",
  "sub": "accessor:7c1f…",                 // who minted it, for audit
  "jti": "ext_91ab…", "iat": 1793635451, "exp": 1793635511,   // 60 s to redeem
  "legion.io/node": "w-17",                // redeemable on this node only
  "legion.io/job": "agents", "legion.io/task": "workspace",
  "legion.io/alloc": 88213,                // the allocation, or the envelope for a session
  "legion.io/instance": "user-42",         // optional, the instance or session id
  "legion.io/argv": ["/bin/bash"],         // rendered, absent = any command
  "legion.io/action": "flush-cache",       // set when minted for an action
  "legion.io/tty": true,
  "legion.io/max_session": 3600,           // seconds the session may run
  "legion.io/uses": 1
}
```

- **Minting is a signature, not a write.** `Manager.MintExecTicket` runs
  the authorization `ExecAllocation` runs (`resolve_exec_start`): the
  coarse `exec`/`action` gate before any lookup, the precise job scope
  after it, `host` for host-level runtimes, and action argv rendered from
  the stored spec. It returns the ticket, the worker's advertised address,
  and the node name to verify. No raft entry.
- **The client dials the worker over the worker-plane-auth listener**, with
  the cluster CA as its only root, verifying `<node>.node.legion`, exactly
  as that plan's `legion sandbox start` path does. It gets the CA from
  `legion net ca show` or the mint reply. It presents no client
  certificate.
- **The worker verifies offline**: signature against the cached JWKS,
  `aud = legion:exec`, expiry with the boot ticket's clock skew,
  `node` equals itself, the target is running here under that job and task,
  the requested argv and tty fit the ticket, and the `jti` is unspent in
  the local set.
- **Audit flows asynchronously.** The mint is audited at the manager under
  `exec_audit`, as today. The worker records redemption, argv, tty, and exit
  code locally and reports them on its next heartbeat
  (`HeartbeatRequest.exec_audit`), and the manager logs them under
  `exec_audit` too.

```console
$ legion exec --instance user-42 --direct -it -- /bin/bash
# one MintExecTicket call, then the session runs against w-17 directly

$ legion exec-ticket mint --instance user-42 --argv /bin/bash --tty -j
{"ok":true,"data":{"ticket":"eyJ…","worker":"https://10.0.3.17:4848",
 "node":"w-17","expiresAt":"2026-11-02T18:05:11Z"}}
```

### 6.4 The security boundary for direct exec

| Question | Answer |
| --- | --- |
| Who can mint | Anyone `ExecAllocation` would admit for the same target: `exec:write` on the job (plus `host` for host-level runtimes), or `action:write` for an action ticket. A delegated key (Phase 3) is scoped to a job glob, non-host runtimes, and a TTL ceiling. |
| What a ticket allows | One session, on one node, into one allocation or session, with the ticket's argv (or any argv for an `exec` rule) and tty flag, for at most `max_session`. |
| Replay | The `jti` is spent in the node's local set, bounded by the ticket's `exp`, the structure boot tickets already use. A ticket names its node, so it cannot be replayed elsewhere. A restart empties the set, so a restarted worker refuses tickets issued before it started (`iat` older than process start minus the clock skew). The client mints again. |
| Revocation | By TTL (60 s to redeem by default, 10 min ceiling). Revoking an ACL token also pushes its accessor id to workers in the next heartbeat reply (`revoked_accessors`), and workers end that accessor's open direct sessions. The bound is one heartbeat interval, against 30 s on the proxied path. |
| A stolen ticket | Opens one session on the named target, inside the redeem window, if the thief redeems before the owner. It cannot reach another instance, node, or command. |
| Transport | The worker-plane-auth mTLS listener. Server authentication is the node certificate. The client presents no certificate, because the ticket is its credential, as with `StartClaim`. |
| ACL mapping | Unchanged vocabulary. No new resource. |
| A compromised worker | Can read tickets presented to it, which are already spent and name only itself. Can mint nothing. |

### 6.5 Delegated minting keys (Phase 3)

A platform registers an exec minting key (`CreateExecKey`, ACL
`acl:write`), scoped to a job glob, non-host runtimes, and a TTL ceiling.
Workers learn keys on the heartbeat and trust tickets signed by them for
matching jobs. A platform can then mint tickets for sandboxes it started
at the edge with no manager call at all. One control-lane write per key,
never per session. Deleting the key reaches workers on their next
heartbeat.

```console
$ legion exec-key create --job 'agents-*' --max-ttl 5m --public-key key.pub -j
{"ok":true,"data":{"keyId":"xk_3d1c","jobs":"agents-*","maxTtl":"5m"}}
```

### 6.6 The gateway tier (Phase 4)

Some callers cannot route to workers: browsers on the public internet, and
deployments that keep the worker port private. `legion start gateway` runs
a stateless tier that terminates client TLS or WebSocket, verifies the same
ticket, and forwards to the worker over the mTLS worker plane. Gateways
scale horizontally and keep worker addresses private, which is Cloudflare's
shape. The UI can then mint a ticket and open its terminal through a
gateway, so browser sessions leave the manager too.

The same tier hosts the scaling plan's wake on request (scaling plan
§6.14). Whichever plan builds it first, the other adds its route. It comes
after Phase 2 because the common agent-platform caller is a backend
already inside the network, which Phase 2 serves with no public exposure.

### 6.7 Exec into unnamed sessions (Phase 5)

In the scaling plan's session mode, an unnamed session has no raft row
(scaling plan §6.4). `ExecAllocation` and `MintExecTicket` accept an
instance id. When it is a session id, the manager resolves it through the
leader's session index after the coarse ACL gate, and names the envelope
allocation and the session id. The worker checks that the session is
running locally under that envelope. A promoted session resolves like any
keyed instance.

## 7. Targets, with latency

Measured in Phase 0 on the AWS rig's control plane (3 × m6id.xlarge) with
simulated workers running real exec sessions against an echo process, plus
one KVM node for microVMs. The latency SLOs here are part of the scaling
plan's Phase 0 matrix (scaling plan §7.2).

| Phase | Manager work per session | Manager bytes per session | Concurrent sessions per cluster | Open latency, p50 / p99 |
| --- | --- | --- | --- | --- |
| Today, proxied into a container | two streams for the session's life | every byte, twice | measured in Phase 0 | measured in Phase 0 |
| 1: microVM, proxied | unchanged | unchanged | unchanged | ≤ 50 ms / ≤ 150 ms to first output on a warm guest, plus the proxied baseline |
| 2: direct | one unary mint RPC | about 1 KiB | ≥ 10,000 with managers carrying no session bytes | ≤ 20 ms / ≤ 80 ms from mint reply to first output, same AZ |
| 3: delegated keys | none | none | worker-bound | as Phase 2, minus the mint |
| 4: gateway | none | none | scales with gateways | Phase 2 plus one hop |

Mint throughput target: ≥ 5,000 tickets per second per manager (one ES256
signature and one read-model lookup each).

## 8. Proto, struct, config, and persistence changes

Every new RPC and field gets public-facing comments in `proto/legion.proto`.

- `ExecStart` gains `instance_id` (exec by instance or session id, resolved
  to the current allocation or envelope) and `exec_ticket`.
- `Manager.MintExecTicket { target, argv, action, action_params, tty, ttl }`
  returns `{ ticket, worker_address, node, expires_at }`. Same ACL as
  `ExecAllocation`. Added to `tests/acl_endpoints.rs`, to `catalog()` as
  `exec-ticket mint` (`mutates: false`, `read_only_safe: false`, since a
  ticket opens a shell), and to MCP as a tool gated like `legion_exec_run`.
- `Manager.CreateExecKey`, `ListExecKeys`, `DeleteExecKey` (Phase 3), ACL
  `acl:write`, with an `exec_key` control-lane table (public key, job glob,
  TTL ceiling, created by, created at).
- `HeartbeatRequest.exec_audit` and `HeartbeatReply.revoked_accessors` and
  `exec_keys`.
- Worker config: `exec = { direct = false }` turns on the ticket class on
  `ExecTask`. Manager config: `exec.tickets = { defaultTtl = "60s",
  maxTtl = "10m", maxSession = "1h" }`.
- Sandbox runtime: the vsock device and agent on every microVM, the exec
  vsock port, and the Firecracker 1.17.0 floor for exec. No job-config
  field, so nothing goes into `configs/jobs/maximal.ncl`.
  `config.network = { mode = "none" }` keeps its meaning for
  guest-initiated traffic.
- No migration beyond the `exec_key` table (Phase 3).

## 9. Required amendments to the worker-plane-auth plan

**Folded into [worker-plane-auth-plan §6.1](worker-plane-auth-plan.md#61-the-worker-listener-is-mutual-tls-always)**
(2026-10-11), as part of its Phase 1. That plan's caller-class table and
classification test are now the source of truth. The three amendments are
kept below as the record of what this plan asked for.

Folding them in added one dependency: a worker builds a ticket verifier only
when it fetches the cluster JWKS over TLS that pins the cluster CA, which is
worker-plane-auth Phase 4 (its §6.1 and §6.8, calls entry 13). So direct exec
(Phase 2 here) needs worker-plane-auth Phases 1 and 4.

1. **A third caller class in §6.1's table.**

   | RPC | Caller | Check |
   | --- | --- | --- |
   | `ExecTask` | a ticket holder | no manager certificate is required. The first frame carries an exec ticket (`aud = legion:exec`) verified offline against the cluster JWKS, as `StartClaim` verifies boot tickets. Admitted only when the worker sets `exec.direct = true`. |

   `ExecTask` then admits two classes, a manager certificate or a ticket.
   Every other manager-only RPC stays manager-only.
2. **The classification test allows an RPC with more than one class.**
   `every_worker_rpc_is_classified` must accept `ExecTask: manager |
   ticket` and still fail on an unclassified RPC.
3. **The JWKS verifier runs on every worker that admits tickets**, not only
   when `edgeBoot.acceptClaims` is on. Today it is built only for edge
   boot (`src/worker/daemon.rs:297`). The cache, refresh, and skew settings
   are shared, and the spent sets are per audience.

Nothing else in that plan changes. The listener still requests but does not
require a client certificate, which is what lets a ticket holder connect
without one.

## 10. Implementation phases

Each phase ships alone and is worth having alone.

- **Phase 0: measure.** Exec open latency (p50, p99) and manager CPU and
  bytes per concurrent session on the proxied path, into a container, at
  100, 1,000, and 10,000 concurrent sessions on simulated workers. Numbers
  go in `benchmarks/scheduler/README.md` beside the scaling plan's Phase 0
  matrix. No gate on the rest.
- **Phase 1: exec into microVMs over vsock (§6.1).** After gated plan
  Phases 0 and 1 and worker-plane-auth Phase 1. The vsock device and agent
  on every microVM, the agent's exec listener and pty spawn,
  `VsockExecSession`, the version hello, `--instance` on `legion exec` and
  `legion action run`, typed
  suspend endings, the idle-signal warning removed. Docs in the same change:
  the runtime table in `docs/exec-and-actions.md`, `docs/task-runtime-sandbox.md`,
  the `exec` signal rows in `docs/suspend-and-resume.md`, and the agent
  sandboxes guide.
- **Phase 2: direct exec (§6.3, §6.4).** After worker-plane-auth Phase 1
  and the §9 amendments. `MintExecTicket`, the ticket class on `ExecTask`,
  `legion exec --direct`, `legion exec-ticket mint`, the MCP tool, async
  audit, revoked accessors on the heartbeat. Docs: a "Direct exec" section
  in `docs/exec-and-actions.md` with the boundary table, `docs/auth-and-acls.md`,
  and `docs/agent-readiness.md`.
- **Phase 3: delegated minting keys (§6.5).**
- **Phase 4: the gateway tier (§6.6)**, shared with the scaling plan's wake
  on request. Built when browser or public access at scale needs it.
- **Phase 5: exec into unnamed sessions (§6.7)**, after scaling plan
  Phase 6.

## 11. Testing plan

- **Phase 1:** ignored KVM tests: a pty session into Firecracker and
  cloud-hypervisor guests (stdin, resize, exit status, kill on
  disconnect), an action run, exec after a memory resume reaches the new
  allocation, an open session ends with the suspend reason, exec on a
  `none` sandbox works while the guest still cannot dial the host, and an
  image without a runnable agent fails by name. Unit tests for the frame
  codec, the version hello against an older agent, and frame-size bounds
  against a hostile agent. The existing exec suites pass unchanged.
- **Phase 2:** a ticket for another node, allocation, argv, tty, or an
  expired one is refused. A replayed `jti` is refused. A ticket issued
  before a worker restart is refused. A ticket with `exec.direct = false`
  is refused with the config hint. An ACL matrix shows `MintExecTicket`
  admits exactly what `ExecAllocation` admits. Accessor revocation ends an
  open direct session within one heartbeat. The audit record arrives on the
  next heartbeat. The CLI refuses a worker whose certificate is not
  `<node>.node.legion`. `MintExecTicket` writes no raft entry
  (`legion_raft_ops_total` unchanged).
- **Phase 3:** a key outside its job glob, past its TTL ceiling, or for a
  host-level runtime is refused. A deleted key stops working within one
  heartbeat.
- **Phase 4:** a gateway forwards a ticketed session and refuses an
  unticketed one, and a browser terminal works through it.
- **Benchmarks:** the §7 matrix before and after Phases 1 and 2, in the PR.

### Verification record (2026-10-12)

What ran where, and what still needs hardware:

- **Ran in CI's environment (no KVM):** the guest agent's exec listener
  against the worker's `VsockExecSession` over Unix sockets, with a fake VMM
  answering `CONNECT 1100` (pipe and pty sessions, resize, exit status,
  process-group kill on disconnect, an old agent's hello, a hostile 4 GiB
  frame, a guest with no agent); the init script and root-disk payload for
  gated and ungated guests; direct exec end to end on a manager and an
  in-process worker (`tests/worker_plane_e2e.rs`: session, exit status,
  mint JSON, revocation within a heartbeat, delegated keys reaching the
  worker, refused for a host-level job and another job, and dropped on
  delete); the mint ACL matrix; Phase 0 at 1 to 500 sessions.
- **Needs a KVM host** (Firecracker 1.17.0+, cloud-hypervisor, a vsock guest
  kernel, the static `legion-guest`):

  ```text
  LEGION_SANDBOX_KERNEL=/var/lib/legion/kernels/vmlinux \
  LEGION_GUEST_BINARY=$PWD/target/x86_64-unknown-linux-musl/dist/legion-guest \
  cargo test --lib sandbox_exec_over_vsock -- --ignored --nocapture --test-threads 1
  ```

  then by hand: exec after a memory resume with `--instance`, a session
  across a suspend, exec into a `none` sandbox while the guest still cannot
  dial the host, the `exec` idle signal, and an action from the UI terminal.
- **Needs the AWS rig:** Phase 0 at 1,000 and 10,000 sessions on real
  workers, and the same-AZ open-latency SLO.
- **Needs runc:** a delegated key's accepted session on a `container` task
  (the e2e suite's tasks are `exec`, which keys never reach).

## 12. Rollout and backward compatibility

- **Phase 1 is default on** for microVM sandboxes on workers that ship it,
  and only on an authenticated worker plane. A guest captured before Phase
  1 has no exec listener, so exec into a sandbox resumed from an old
  checkpoint fails by name until its next cold boot. Older workers keep
  reporting no exec support, and the worker already refuses exec against a
  runtime that reports none.
- **Phase 2 is opt-in twice**: `exec.direct = true` on the worker, and
  `--direct` (or `MintExecTicket`) on the client. A cluster that changes
  nothing sees only manager-proxied exec.
- **Version skew.** An old worker refuses an `ExecTask` with a ticket and no
  manager certificate, as the worker-plane-auth plan specifies, and the
  CLI falls back to the proxied path with a note when `--direct` is
  refused that way.

## 13. Open questions

- **Should direct-exec tickets be bound to the client's key (lean: yes for
  the CLI, optional for platforms)?** A `cnf` claim naming the client's
  ephemeral key, proven by a signature over the `jti` in the first frame,
  makes a stolen ticket useless. It needs no TLS client certificate, so it
  needs no further amendment to the worker-plane-auth plan. It costs the
  client a key and a proof, which a platform backend may not want.
- **How much revocation lag is acceptable (lean: one heartbeat)?** The
  proxied path re-checks every 30 s. Adaptive cadence can stretch a beat to
  21 s on a large fleet, so the two are close, but an operator revoking a
  leaked token wants faster. A dedicated revocation push is the fallback.
- **Should `instance create` return an exec ticket (lean: on request, with
  `--exec-ticket`)?** Modal embeds command-router access in create and
  restore replies to save a round trip. Doing it by default would mint a
  shell capability nobody asked for.
- **Which vsock port does the agent listen on (lean: 1100)?** Any fixed port
  works. It must not collide with the gated plan's Phase 5 ingress ports,
  which that plan assigns per declared port.

## 14. Acceptance criteria

- **Phase 0:** exec open latency and manager cost per session are recorded
  for 100, 1,000, and 10,000 concurrent proxied sessions.
- **Phase 1:** on a KVM node, `legion exec --instance <id> -it -- /bin/sh`
  opens a shell in a Firecracker and a cloud-hypervisor sandbox, an action
  runs there from the UI, the exit status is the process's own, the `exec`
  idle signal keeps an instance with an open shell awake, and exec after a
  memory resume works. From another process on the worker's host, an
  `ExecTask` without a manager certificate or ticket is refused.
- **Phase 2:** 10,000 concurrent direct sessions on simulated workers move
  no session bytes through a manager, minting writes no raft entry, and the
  §6.4 refusals each have a test.
- **Phase 3:** a platform key mints a ticket that a worker accepts for a
  matching job and refuses for any other.
- **Phase 4:** a browser terminal session runs through a gateway with no
  route from the browser to the worker.
- **Phase 5:** exec into an unnamed session works proxied and direct, and a
  session id the index does not hold is refused without revealing whether
  it ever existed.
