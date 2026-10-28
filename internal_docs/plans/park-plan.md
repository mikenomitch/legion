# Plan: Park — the Workload Says When It Is Waiting

> **Judgement calls:** [`calls/park-plan_calls.md`](../calls/park-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-28). Phases 1 and 2 are built in
the same change: the `suspend.park` block, the park socket
(`$LEGION_PARK_SOCKET`) with `park`, `unpark`, `busy`, and `status`, a park
that suspends a keyed instance from its node (a local suspension with
`suspend_reason = park`) or a session on its node, a per-park checkpoint
choice, `busy` holding the idle policy off, and the same socket inside a
sandbox guest through the guest agent's vsock. Deliberately deferred and
named: a timed wake (`park until=`, Phase 3), Legion-run tools that wake the
agent that waits on them (Phase 4), park state that survives a worker
restart (Phase 5), and publishing a keyed park's checkpoint under
`durability = "store"` (§7, calls entry 6). Park on macOS covers `exec`
only: Apple `container`'s VM cannot reach a host unix socket. This plan executes the `hint` idle signal the suspend
plan deferred to its Phase 8
([suspend-resume-and-oversubscription-plan.md](suspend-resume-and-oversubscription-plan.md)
§6.7 and open question 2), in a different shape (calls entry 1).

---

## 1. Problem statement

An agent spends most of its life waiting: on a CI run, on a human's
approval, on a long build, on a model call. While it waits, its sandbox
holds its whole reservation. Legion can already suspend an instance and
free that reservation ([suspend and resume](../../docs/suspend-and-resume.md)),
but it has to guess *when*: the idle policy suspends after `idle.after`
(at least 30 seconds) of no network bytes, CPU, or exec sessions, and the
manager's sweep adds up to about 25 seconds more.

The guess is wrong in both directions for an agent:

- **Too late.** An agent that kicks off a 20-minute CI run and waits for
  its webhook is idle the moment the request returns. The idle policy
  keeps it awake for three more minutes by default, every time.
- **Too early, or never.** An agent waiting on a slow model call holds a
  quiet open connection. With the default signals that reads as idle and
  the policy suspends it mid-call, breaking the stream. With
  `connections` added it never suspends while the connection is open,
  and on a TAP sandbox `connections` cannot see the call at all (refused
  at submit, [suspend-and-resume.md](../../docs/suspend-and-resume.md#idle-policy)).

The agent knows exactly when it is waiting, and on what. Today it has no
way to say so. The public doc says as much: "Suspending *during* a model
call, and waking when it returns, needs the workload to say it is parked.
That is the `hint` signal, which is not available yet."

What a platform does instead today is drive `legion instance suspend` from
outside the sandbox, which means the harness has to call out to its own
platform with credentials to suspend itself, and the platform has to
track which of thousands of agents is waiting.

## 2. Prior art

- **Infinity** (AWS and UC Berkeley's Hydro project,
  [announcement](https://aws.amazon.com/blogs/opensource/introducing-infinity-a-runtime-for-extreme-agent-concurrency/),
  2026-10-23). Runs agents as stackless coroutines: each step loads the
  conversation history, runs one completion, persists the result, dispatches
  at most one tool call, and ends. A tool call is the `await` point, and a
  waiting agent costs only its stored history (about 100 KB in their
  benchmark). A tool result on the agent's queue starts the next step. **We
  take** the central idea: the right time to suspend is the await point,
  and only the harness knows where it is. **We reject** the programming
  model: Infinity needs the harness rewritten as slices over a store.
  Legion cannot ask that of every harness, and with a memory checkpoint it
  does not have to (§6.3).
- **The suspend plan's `hint` signal** (suspend plan §6.7, open question
  2). A local socket with `busy`, `idle`, and `idle-until <deadline>`, fed
  into the idle policy's clock. **We take** the transport (a unix socket
  for `exec` and `container`, the guest agent's vsock for a sandbox) and
  the `busy` half. **We reject** routing the park itself through the idle
  policy: it inherits the policy's 30-second floor and 25-second sweep
  lag, which is exactly the "too late" this plan exists to fix (calls
  entry 2).
- **Fly Machines** suspend on the platform's say-so (`fly machine suspend`
  or the proxy's autostop on no connections). The machine has no way to
  say "now". **AWS Lambda durable functions** and **Temporal** park a
  workflow at an `await` on an activity or timer and resume it on the
  result: the same await-point idea, again requiring the code to be
  written against their SDK.
- **Cloudflare Durable Objects** hibernate a WebSocket-holding object when
  it has no pending work and wake it on a message. The runtime knows the
  await points because it owns the event loop. Legion does not own the
  workload's event loop, so the workload has to say.
- **Kubernetes and Nomad** have nothing here: a pod or allocation is
  running or it is not.

## 3. Jobs to be done, by persona

**Agent platform builder** (writes the harness and the jobspec)
- When my agent starts something slow and has nothing to do until it
  finishes, I want its sandbox to stop costing me right away, so I can run
  many more agents on the same machines.
- When my agent is in the middle of a model call that looks quiet, I want
  nothing to put it to sleep, so the call is never cut off.
- When my agent has already saved its own state, I want the cheap kind of
  sleep, so a waiting agent costs almost nothing.
- When my agent has not saved its state (an off-the-shelf harness), I want
  it frozen exactly as it is, so it carries on as if no time passed.
- When the thing my agent waited on finishes quickly, I want it not to
  have slept at all, so a short wait never pays for a sleep and a wake.

**Autonomous agent** (the harness itself, inside the sandbox)
- When I am about to wait, I want one plain message to say so and a plain
  answer back, so I know it was understood or why it was not.

**Cluster operator**
- When instances go to sleep, I want to see which ones put themselves to
  sleep, so I can tell an agent that parks from an idle policy or memory
  pressure.

**Security / platform admin**
- When a workload can put itself to sleep, I want it to reach only its own
  instance, so one tenant can never suspend another.

*Not in scope:* the on-call responder. A park is an ordinary suspend with a
new reason, and everything they already use (`instance get`, events,
`instance start`) covers it.

### User scenarios

1. **The CI wait.** An agent in a keyed `container` sandbox with a volume
   writes its conversation to `/workspace/history.json`, starts a CI run
   with a callback to its own `http` port, and sends
   `park ci-4821 checkpoint=disk` on `$LEGION_PARK_SOCKET`. Ten seconds
   later the node stops it and the manager records it `suspended` with
   reason `park`. Twenty minutes later CI calls back through `legion start
   gateway`, which resumes the instance and holds the request; the task
   boots, reads its history, and serves the callback.
2. **The off-the-shelf harness.** An unmodified harness runs in a gated
   `sandbox`. A hook it already supports sends `park` before a long tool
   call and `unpark` after. The sandbox is memory-checkpointed while it
   waits. When the platform resumes it, the harness is exactly where it
   was, still waiting on the tool, and the tool's answer arrives.
3. **The quick tool.** An agent sends `park build after=30s`. The build
   finishes in 8 seconds; the agent sends `unpark`. Nothing was suspended.

## 4. Goals and non-goals

**Goals**
- The workload decides when it suspends, and the suspend starts within
  about a second of its deadline, not on the idle sweep's cadence.
- One protocol, one env var, for `exec`, `container`, and `sandbox`.
- A park can choose the cheap checkpoint (disk, "I saved myself") or the
  faithful one (memory, "freeze me").
- `busy` keeps the idle policy from suspending a workload that says it is
  working.
- A workload can affect only its own instance.
- Works for keyed instances and for sessions.

**Non-goals**
- **Delivering the result.** Waking a parked instance is what already
  exists: `legion instance start`, and for keyed instances `legion start
  gateway` with `suspend.wake`, which holds the tool's callback until the
  instance serves it. Legion does not grow a message queue here.
- **Serializing the workload's state for it.** Legion offers the two
  checkpoints it has. What a cold-booted harness reloads is the harness's
  business.
- **Inferring await points** from traffic. A quiet model call and a quiet
  wait on CI look the same from the datapath; only the harness knows where
  its tool boundaries are, which is Infinity's whole argument.
- **A timer wake** (`park until=`). Phase 3.
- **A new manager RPC or CLI verb.** The park is a worker-local surface;
  the suspend it causes is reported on the existing heartbeat.

## 5. Background: what exists today

- **The `suspend` block** (`src/structs/suspend.rs`): `checkpoint`,
  `idle`, `wake`, session-only fields. `IdleSignal::parse` refuses `hint`
  with a pointer at Phase 8 (`src/structs/suspend.rs:195`).
- **Idle detection.** The worker's usage sampler
  (`src/worker/usage.rs:431`) feeds `IdleTracker::observe`
  (`src/worker/idle.rs`) per allocation and reports `idle_since_unix` on
  the heartbeat, only when resource enforcement is on. The manager's sweep
  (`idle_sweep`, `src/manager/server/suspend_rpcs.rs:737`) starts a
  suspend through `begin_automatic_suspend`. Sessions are swept on their
  node (`idle_sessions_to_suspend`,
  `src/worker/task_manager/session_sleep.rs:200`).
- **Local suspensions.** The memory pressure response's `suspend` step
  checkpoints and stops a keyed instance from the node with no manager
  request (`run_local_suspension`, `src/worker/checkpoints.rs:555`). It
  holds the allocation out of relaunch, queues a `SnapshotReport` with
  `local_suspension = true`, and wakes the heartbeat. The manager applies
  it in `apply_local_suspension_tx`
  (`src/storage/store/suspend_ops.rs:558`), which hardcodes
  `suspend_reason = pressure`, discards a capture that lost a race with a
  SuspendInstance, and records it `deleting` when the instance moved on.
  This is the fast path a park needs.
- **Session suspend** (`suspend_session`,
  `src/worker/task_manager/session_sleep.rs:906`) takes a trigger string
  and the kind `local_suspend_kind` picks.
- **Checkpoint kind** (`local_suspend_kind`,
  `src/worker/task_manager.rs:1651`): the task's `suspend.checkpoint`, else
  memory when the runtime can take one of this allocation, else disk.
- **Task directories and env** (`prepare_instance`,
  `src/worker/task_manager.rs:2321`): `LEGION_TASK_DIR` and friends. A
  container sees the task dir at `/legion/task` and its env is translated
  (`translated_worker_env`, `src/task_plugins/container.rs:537`). A worker
  that restarts re-adopts running tasks in two other places that build the
  same env.
- **The guest agent** (`guest/src/agent.rs`) runs in every microVM. Its
  relays dial host vsock ports through `HostDial`; a guest connection to
  host port P arrives on the VMM's `<alloc>/.sandbox/vsock.sock_P`. With
  `--no-relay` (TAP and `none`), the node binds nothing a guest can dial.
  Upstream local ports are relayed to host vsock ports of the same number,
  so host ports the agent uses are refused as upstream ports at submit
  (`validate_gated_guest_ports`, `src/structs/network.rs:425`).
- **The gateway** (`src/gateway.rs`) resumes a suspended keyed instance on
  a request and holds it. Sessions are not routable through it yet
  (wake-on-request plan Phase 3).

## 6. Proposed design

### 6.1 The jobspec

```nickel
{
  name = "agent",
  runtime = "container",
  stateful = { mode = "keyed", storage = { ... } },
  network = { ports = [{ name = "http" }] },
  suspend = {
    checkpoint = "disk",
    wake = { port = "http" },
    park = { after = "10s" },   # turns on $LEGION_PARK_SOCKET
  },
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `suspend.park` | unset | Give the task a park socket. Unset: no socket, no env var. |
| `suspend.park.after` | `"10s"` | How long a park waits before it suspends, unless a `park` line names its own `after`. `"0s"` suspends at once; at most `"24h"`. |

Submit refuses `park` on a runtime Legion cannot hand a socket to:

```text
Error: task "agent" sets suspend.park, but runtime "noop" runs no process to park
Hint: suspend.park works on the exec, container, and sandbox runtimes
```

### 6.2 The socket and the protocol

`$LEGION_PARK_SOCKET` names a unix stream socket. A client writes one
request per line and reads one JSON line back, in the CLI's envelope. A
connection may carry several requests.

```text
park [LABEL] [after=DURATION] [checkpoint=memory|disk]
unpark
busy
status
```

```console
$ printf 'park ci-4821 checkpoint=disk\n' | nc -U "$LEGION_PARK_SOCKET"
{"ok":true,"data":{"state":"parked","label":"ci-4821","after":"10s","checkpoint":"disk","suspendsAt":"2026-10-28T17:03:10Z"}}
$ printf 'park after=soon\n' | nc -U "$LEGION_PARK_SOCKET"
{"ok":false,"error":{"code":"invalid_argument","message":"after \"soon\" is not a duration","hint":"use a duration like 10s or 2m","retryable":false}}
```

| Request | Effect |
| --- | --- |
| `park` | Suspend this instance once `after` passes, with the named checkpoint (else the task's default). Replaces any earlier park or `busy`. |
| `unpark` | Cancel a pending park or a `busy`. The idle policy, if any, decides again. |
| `busy` | Hold the idle policy off until `unpark` or `park`. Memory pressure and manual suspends still apply. |
| `status` | The current state: `running`, `busy`, `parked`, or `suspending`, with the last park failure if there was one. |

`LABEL` is the workload's name for what it waits on (`ci-4821`). It
appears in the node log and in `status`, and is never interpreted.

### 6.3 Two checkpoints, chosen per park

| `checkpoint=` | What the suspend keeps | What a resume does | Fits |
| --- | --- | --- | --- |
| `disk` | The instance volume | Boots the task fresh on it | A harness that saved itself (Infinity's model) |
| `memory` | The whole guest: memory, devices, root disk | Continues mid-instruction | An unmodified harness in a `sandbox` |

Omitted, the park uses the task's `suspend.checkpoint`, or the runtime's
default (memory for a sandbox launched for checkpoints, disk otherwise).
`checkpoint=memory` on a runtime that cannot take one is refused on the
socket, at park time, with the same sentence SuspendInstance uses.

### 6.4 What happens at the deadline

The node acts on its own, with no manager round trip before the capture:

- **Keyed instance:** the node runs the same local suspension the memory
  pressure response runs: checkpoint, stop, report on an early heartbeat.
  The manager applies it with `suspend_reason = park`.
- **Session:** the node suspends the session on itself, as its idle policy
  does, with trigger `park`.

A capture that fails leaves the task running, sets the park state back to
`running`, and puts the error in `status` (`lastError`). A park on an
instance with another suspend or resume in flight follows the local
suspension's existing rules (the local one wins over an outstanding
SuspendInstance; a capture on an instance that moved on is reclaimed).

### 6.5 `busy` and the idle policy

While a park is pending or `busy` holds, the idle policy treats the
allocation as active. So `busy` around a model call keeps the policy from
cutting it off, and a pending park is not overtaken by an idle suspend
with a checkpoint kind the workload did not choose. The `hint` signal name
stays refused, now pointing at `suspend.park` (calls entry 1).

### 6.6 Transports

| Runtime | `$LEGION_PARK_SOCKET` | Served by |
| --- | --- | --- |
| `exec` | `<task dir>/park.sock` | the worker, on that path |
| `container` | `/legion/task/park.sock` (the task dir, mounted) | the worker, on the host path |
| `sandbox` | `/legion/park.sock` in the guest | the guest agent (`--park`), relayed to host vsock port 1090, where the worker listens on `<alloc>/.sandbox/vsock.sock_1090` |

The socket lives in the allocation's owner-only directory (or behind its
own VMM), so only that allocation's processes can reach it (calls entry
3). Host vsock port 1090 joins 1080 and 53 as a port a gated sandbox's
upstream may not use, when the task parks (calls entry 11). A TAP or `none` sandbox, whose guest otherwise
cannot dial the host at all, gets exactly this one endpoint when it
declares `suspend.park`.

### 6.7 Waking up

Unchanged surfaces, documented on the park page:

- `legion instance start <id>` resumes a parked keyed instance or session.
- `legion start gateway` with `suspend.wake` resumes a parked keyed
  instance on a request to its port and holds the request until it
  serves, which is how a tool's callback reaches it.

## 7. Proto, struct, config, and persistence changes

- `SnapshotReport.local_suspension_reason` (string, field 12): why the
  node suspended on its own, `pressure` or `park`. The manager stores it
  as `suspend_reason`; anything else reads as `pressure`.
- `InstanceInfo.suspend_reason`'s comment gains `park`.
- `SuspendConfig.park: Option<ParkConfig>` with `after`;
  `ResolvedSuspend.park: Option<ResolvedPark>`.
- `SUSPEND_REASON_PARK = "park"`.
- `configs/jobs/maximal.ncl` gains `park = { after = "10s" }`.
- No store migration: `suspend_reason` is already a free-text column.
- A local suspension's snapshot row is inserted with the default
  durability, `local`, as it always was for pressure suspends, so a keyed
  park of a `durability = "store"` task is not published. Fixing it changes
  pressure suspends too and needs the publish precondition on this path;
  left as its own change (calls entry 6).

## 8. Implementation phases

1. **The park socket for `exec` and `container`, keyed and session.**
   `suspend.park`, the protocol, the node-side timer, local suspensions
   with a reason and a kind, `busy` into the idle tracker, the
   `legion_worker_parks_total{outcome}` counter, the public page.
2. **Sandboxes.** `guest-agent --park` (a guest unix socket relayed to
   host vsock port 1090), the init flag and guest env, the worker's
   listener on the VMM's socket, port 1090 reserved for upstreams.
3. **Timed wake.** `park until=<time>`: the instance resumes at the
   deadline if nothing woke it first (the manager's sweep for keyed
   instances, the node for sessions).
4. **Legion-run tools wake their caller.** A batch job or action started
   with `--notify <instance>` starts that instance when it finishes, so an
   agent whose tools are Legion jobs needs no gateway.
5. **Park state survives a worker restart.** Persist pending parks with
   the worker's state, so a restart does not leave a parked agent awake.

## 9. Testing plan

- **Unit (structs):** `suspend.park` resolve and validation, including the
  runtime refusal and the reworded `hint` refusal.
- **Unit (worker park module):** request parsing (labels, options,
  errors), state transitions, the deadline computation, and a served
  socket end to end with a fake suspender (park, unpark before the
  deadline, park that fires, a failed capture visible in `status`, a
  stale timer after a re-park doing nothing).
- **Unit (idle):** a held allocation is active for every policy.
- **Unit (store):** a local suspension report with reason `park` stores
  `suspend_reason = park`; an empty or unknown reason stores `pressure`.
- **Unit (guest):** the agent's park relay forwards a guest unix socket to
  the host's `vsock.sock_1090` (over unix sockets, as the other relays are
  tested); init renders `--park` and the env var.
- **Unit (network):** an upstream on port 1090 is refused on a gated
  sandbox.
- **Worker (real `exec` task, no manager):** the task gets its socket and
  env, `checkpoint=memory` is refused on the socket, and `park after=0s`
  produces a local suspension reported with reason `park`; the socket goes
  with the allocation. A session parks on its node through the worker's
  suspender (`src/worker/task_manager.rs` tests).
- **Integration (manager):** a heartbeat carrying a park local suspension
  leaves the instance `suspended` with reason `park`, emits it on
  `instance.suspended`, and resumes from the node's checkpoint
  (`tests/integration/suspend_integration.rs`).
- **KVM (not in CI):** a gated and a TAP sandbox park with memory and
  disk checkpoints and resume.

## 10. Rollout

No installed base: the feature is opt-in per task (`suspend.park`). A
worker that predates it ignores the block (no socket), and a manager that
predates it applies a park's report as a pressure suspension. Neither is
designed around.

## 11. Open questions

1. **Should `busy` expire?** A harness that crashes holding `busy` keeps
   its instance awake until it exits. *Lean:* no expiry in Phase 1; the
   state belongs to the allocation, and a crashed harness usually ends
   the task. Revisit with evidence.
2. **A convenience client in the guest** (`/legion/legion park ...`).
   *Lean:* not until the one-line `nc -U` proves too awkward; the socket is
   the contract.

## 12. Acceptance criteria

- Phase 1: an `exec` keyed instance with `suspend.park` that sends `park
  after=0s` is `suspended` with reason `park` within a few seconds, with
  no `suspend.idle` and no SuspendInstance; `unpark` before the deadline
  leaves it running; `checkpoint=memory` on `exec` is refused on the
  socket; `busy` keeps an idle policy from firing.
- Phase 2: a sandbox guest with `suspend.park` has `/legion/park.sock`
  and `LEGION_PARK_SOCKET`, and a `park` through it reaches the node.
