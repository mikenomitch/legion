# Park: Suspend When the Workload Says It Is Waiting

An agent spends most of its life waiting: on a CI run, on a person's
approval, on a long build. While it waits, its sandbox holds its whole
reservation. [Suspend and resume](suspend-and-resume.md) can give that
reservation back, but the idle policy has to guess when the agent is idle
from network bytes, CPU, and exec sessions. That guess is minutes late for
an agent that has just kicked off a long job. It is also wrong for an agent
in the middle of a quiet model call, which looks idle and is not.

The agent knows when it is waiting. **Park** lets it say so. A task whose
`suspend` block sets `park` gets a unix socket, `$LEGION_PARK_SOCKET`, that
only its own processes can reach. The workload writes `park` on it when it
starts waiting, and its node suspends it once the park's `after` passes.
It writes `busy` while it works, and the idle policy leaves it alone.

> Related: [suspend and resume](suspend-and-resume.md) (checkpoints,
> resuming, the idle policy), [gateway](gateway.md) (waking a suspended
> instance on a request), [session mode](session-mode.md#idle-sessions)
> (sessions that sleep on their node), [the sandbox
> runtime](task-runtime-sandbox.md#the-guest-agent) (the guest agent that
> carries the socket into a microVM), and the [agent sandboxes
> guide](guides/agent-sandboxes.md).

## The short version

```nickel
{
  name = "agents",
  tasks = [{
    name = "agent",
    runtime = "container",
    config = { image = "ghcr.io/example/agent:1.4" },
    stateful = { mode = "keyed", storage = { path = "/workspace" } },
    network = { ports = [{ name = "http" }] },
    suspend = {
      checkpoint = "disk",
      wake = { port = "http" },     # a tool's callback resumes it
      park = { after = "10s" },     # gives the task $LEGION_PARK_SOCKET
    },
  }],
}
```

Inside the task:

```console
$ printf 'park ci-4821\n' | nc -U "$LEGION_PARK_SOCKET"
{"ok":true,"data":{"state":"parked","label":"ci-4821","after":"10s","checkpoint":"disk","suspendsAt":"2026-10-28T17:03:10Z"}}
```

Ten seconds later, unless the workload sent `unpark` or `busy` first, the
node checkpoints and stops the instance. The manager records it `suspended`
with reason `park`:

```console
$ legion instance get agent-7
...
state: suspended
last suspend: 2026-10-28T17:03:10Z (park) in 140 ms
```

When the CI run calls back through [`legion start gateway`](gateway.md), the
instance resumes and the gateway holds the callback until it serves it.

## The jobspec

| Field | Default | Meaning |
| --- | --- | --- |
| `suspend.park` | unset | Give the task a park socket. Unset: no socket and no `LEGION_PARK_SOCKET`. |
| `suspend.park.after` | `"10s"` | How long a park waits before it suspends, unless the `park` request names its own `after`. `"0s"` suspends at once; at most `"24h"`. |

Park is a field of the [`suspend` block](suspend-and-resume.md#the-suspend-block),
so it applies to keyed instances and to [sessions](session-mode.md), and
everything else in the block still applies: `checkpoint` sets the default
kind, `wake` sets how a request wakes it, and `idle` still suspends a task
that never parks.

Submit refuses a runtime that runs no process Legion can hand a socket to:

```text
Error: task "agent" sets suspend.park, but runtime "noop" runs no process Legion can hand a park socket to
Hint: suspend.park works on the exec, container, sandbox runtimes
```

## The protocol

The socket is a unix stream socket. A client writes one request per line
and reads one line back for each. A connection may carry several requests,
answered in order, or one request and close.

```text
park [LABEL] [after=DURATION] [checkpoint=memory|disk]
unpark
busy
status
```

| Request | What it does |
| --- | --- |
| `park` | Suspend this instance once `after` passes, with the named checkpoint, or the task's default kind. Replaces an earlier park or `busy`. |
| `unpark` | Cancel a pending park, or a `busy`. The idle policy, if the task has one, decides again. |
| `busy` | Keep the idle policy from suspending this instance until `unpark` or `park`. Replaces a pending park. |
| `status` | Report the current state, changing nothing. |

`LABEL` is the workload's own name for what it waits on (`ci-4821`,
`approval/77`): up to 128 letters, digits, and `. _ - : / @`. It shows in
`status` and in the node's log, and Legion never interprets it. Don't put a
secret in it.

Every reply is the [CLI's JSON envelope](agent-readiness.md#2-structured-output-envelope).
A successful reply carries the state after the request:

| `data.state` | Meaning | Other fields |
| --- | --- | --- |
| `running` | Nothing pending. The idle policy decides. | `lastError` when the last park's suspend failed |
| `busy` | The idle policy is held off. | `since` |
| `parked` | A park is pending. | `label`, `after`, `checkpoint`, `suspendsAt` |
| `suspending` | The deadline passed and the suspend is running. The task stops when it finishes. | as `parked` |

A refusal names its reason and a fix:

```console
$ printf 'park checkpoint=memory\n' | nc -U "$LEGION_PARK_SOCKET"
{"ok":false,"error":{"code":"FailedPrecondition","message":"this allocation cannot take a memory checkpoint: runtime \"exec\" supports \"disk\" only","hint":"park with checkpoint=disk, or leave checkpoint out for the task's default","retryable":false}}
```

| `error.code` | When |
| --- | --- |
| `InvalidArgument` | The request does not parse: an unknown verb or option, a bad duration or label, an `after` over 24 hours, or a line over 1,024 bytes (the connection then closes). |
| `FailedPrecondition` | `checkpoint=memory` on an allocation that cannot take one, or any change while the state is `suspending`. |
| `ResourceExhausted` | The allocation already has 16 connections open. |
| `NotFound` | The allocation has stopped on this node. |
| `Unavailable` | A sandbox guest could not reach the node (`retryable: true`). |

From code, it is a few lines in any language:

```python
import json, os, socket

def park_request(line: str) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(os.environ["LEGION_PARK_SOCKET"])
        s.sendall(line.encode() + b"\n")
        reply = s.makefile().readline()
    return json.loads(reply)

park_request("park ci-4821 after=30s checkpoint=disk")
```

Open a connection per request. A connection held open across a memory
restore is closed by the restore.

## When it suspends

At the park's deadline the node suspends the instance itself. It doesn't
wait on the manager first:

- **A keyed instance** is checkpointed and stopped on its node. The node
  reports the suspend on its next heartbeat, which it sends at once, and
  the manager records it with `suspend_reason = park` and emits
  `instance.suspended` with `"reason": "park"`. This is the same path a
  node's [memory pressure response](suspend-and-resume.md#under-memory-pressure)
  takes.
- **A session** is suspended on its node, as its own idle policy would
  suspend it ([Idle sessions](session-mode.md#idle-sessions)).

So the suspend starts within about a second of `suspendsAt`, not on the
idle policy's cadence. From there it is an ordinary suspend: the
reservation is freed, `suspend.reserve` holds what it holds, and the
instance resumes like any other.

If the capture fails (the content store is disabled, the disk is full), the
task keeps running, the state goes back to `running`, and `status` carries
the reason in `lastError` until the next `park`. The node logs `Park suspend
failed; the task keeps running`.

## Choosing the checkpoint

A park picks between the two checkpoints Legion has, per park:

| `checkpoint=` | What the suspend keeps | What a resume does | Use it when |
| --- | --- | --- | --- |
| `disk` | The instance's volume (`stateful.storage`) | Boots the task fresh on it | The workload already saved what it needs, such as its conversation history, to its volume or an outside store. Nothing is captured, so a parked instance costs only its volume. |
| `memory` | The whole guest: memory, devices, root disk | Continues mid-instruction | The workload is an off-the-shelf harness that cannot reload itself. Sandboxes only. |

Without `checkpoint=`, a park takes the task's `suspend.checkpoint`, or the
runtime's default: memory for a sandbox, disk for every other runtime. The
reply's `checkpoint` field says which kind it resolved to.

A disk park of a [session](session-mode.md) keeps nothing but its spec,
because a session has no volume. Its resume is a fresh start under the same
id, so the workload must have saved its state somewhere else first.

## `busy` and the idle policy

While the workload is `busy`, or has a park pending, the task's
[idle policy](suspend-and-resume.md#idle-policy) counts the instance as
active, whatever its `signals` list. Two things follow:

- `busy` around a model call keeps the policy from suspending an agent that
  holds a quiet connection open. On a TAP sandbox, where the `connections`
  signal can't see that call, `busy` is the only way to keep it awake.
- A pending park is never overtaken by an idle suspend with a checkpoint
  kind the workload did not choose.

`busy` does not hold off a node's memory pressure response or a `legion
instance suspend`.

## Waking a parked instance

A parked instance is a suspended instance, so the usual ways to resume one
apply:

- `legion instance start <id>` (`StartInstance`), for a keyed instance or a
  session.
- [`legion start gateway`](gateway.md), for a keyed instance whose task sets
  `suspend.wake`. A request to the instance resumes it and is held until the
  instance serves it. Give the tool you are waiting on a callback URL
  through the gateway, and its callback is what wakes the agent. Sessions
  can't be woken through the gateway yet.

Legion delivers nothing itself. The callback, or whatever the workload
reads when it wakes, carries the result. A gateway retries nothing, so a
callback that times out is retried by its sender, and the workload should
treat a repeated callback for the same call as a duplicate.

## Where the socket is

| Runtime | `$LEGION_PARK_SOCKET` | The node listens on |
| --- | --- | --- |
| `exec` | `<task dir>/park.sock` | that path |
| `container` | `/legion/task/park.sock` (the task directory, mounted) | `<task dir>/park.sock` on the host |
| `sandbox` | `/legion/park.sock` in the guest | `<alloc dir>/.sandbox/vsock.sock_1090`: the guest agent relays each connection to host vsock port 1090 |

The socket sits in the allocation's owner-only directory, or behind its own
VMM, so only that allocation's processes can reach it, and a request on it
can only suspend the instance it belongs to. The file is mode `0666` so
whichever user the task runs as can connect; the directory around it is the
boundary.

In a sandbox the guest agent runs with `--park` and serves
`/legion/park.sock` in the guest. On a gated sandbox, an upstream may not
use port 1090, for the same reason it may not use 1080 or 53:

```text
Error: task "workspace" connect.upstreams entry "db" sets port 1090, but a gated sandbox's upstreams are relayed to the node on vsock ports of the same number, and this task sets suspend.park, whose socket the guest relays to vsock port 1090
Hint: pick another port, or drop it to take a dynamic one
```

A TAP or `none` sandbox otherwise has no way to dial its node at all. With
`suspend.park` it has exactly one: the park listener, which speaks only
this protocol and only about that sandbox.

## Observing parks

- `status` on the socket, from inside the task.
- `legion instance get <id>`: `last suspend: <time> (park)`, and
  `suspend_reason = "park"` with `-j`.
- The `instance.suspended` event, with `"reason": "park"`.
- The node log: `Parked` (with the label, `after`, and checkpoint), `Park
  deadline passed; suspending`, and `Park suspend failed; the task keeps
  running`.
- `legion_worker_parks_total{outcome}` on the worker's metrics:
  `parked`, `unparked`, `suspended`, `failed`
  ([metrics](metrics.md)).

## Limitations

- **A worker restart forgets pending parks and `busy`.** The sockets come
  back (the task keeps its env), but a park that was pending is gone and
  the task stays awake until it parks again. A restart never suspends a
  task that didn't ask.
- **`busy` does not expire.** A workload that crashes while busy, inside a
  task that keeps running, keeps its instance awake until the task stops or
  something sends `unpark`.
- **A keyed park's checkpoint stays on its node**, even with
  `suspend.durability = "store"`. The node takes it on its own, as it does
  for memory pressure, and it isn't published, so a resume is pinned to
  that node. To publish, suspend with `legion instance suspend` from
  outside. A session park with `durability = "store"` publishes as any
  session suspend does.
- **No timed wake.** A park suspends; it doesn't arrange a resume. Wake
  the instance from outside (`instance start`, the gateway) when the thing
  it waits on finishes.
- **On macOS, park works for `exec` tasks only.** A `container` task and
  a dev-mode sandbox there run inside Apple `container`'s VM, which a unix
  socket on the host does not reach: a dev-mode sandbox gets no
  `LEGION_PARK_SOCKET`, and a container sees the socket file in its task
  directory but cannot connect to it.
- **Each keyed park is a suspend and each wake a resume**, with the
  replicated writes those cost
  ([sandboxes at scale](sandboxes-at-scale.md#what-each-step-costs)). An
  agent fleet that parks often fits [session mode](session-mode.md) better,
  where neither writes to the replicated store.
