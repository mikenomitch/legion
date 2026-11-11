# Judgement calls: Park

Calls made while building [park-plan.md](../plans/park-plan.md). Entries are
ranked by project impact, highest first; the number is the rank.

## 1. Park is its own block and its own verbs, not the `hint` idle signal

The suspend plan reserved an idle signal, `hint`, for the workload's own
report: a local socket taking `busy`, `idle`, and `idle-until <deadline>`,
fed into the idle policy's clock. This work ships the socket, but as
`suspend.park` with `park`, `unpark`, `busy`, and `status`, and `hint` stays
refused, now with a pointer at `park`:

```nickel
# Planned:
suspend = { idle = { after = "3m", signals = ["network", "cpu", "hint"] } },
# Shipped:
suspend = { park = { after = "10s" } },   # + `park` on $LEGION_PARK_SOCKET
```

```text
Error: task "workspace": suspend.idle.signals "hint" is not a signal: the workload's own busy/idle report is the park socket
Hint: remove "hint" and add `park = {}` to the suspend block. ...
```

- **Alternatives.** (a) The planned `hint` signal: `idle` on the socket
  marks the allocation quiet, and the idle sweep suspends it `after` later.
  (b) Both: `hint` in `signals` for the clock, plus a separate park verb.
- **Trade-off.** Two concepts where the plan had one: idle signals, and a
  park socket that overrides them. A name the docs had promised now means
  "use something else".
- **Why (simplicity, performance).** Riding the idle clock inherits the
  policy's 30-second floor and its sweep's 25-second lag, which is the
  "too late" this feature exists to fix, and it cannot carry a per-wait
  checkpoint choice. A park says something different from "I'm quiet": it
  says "I'm blocked on X, suspend me now, this way". Giving it its own verb
  keeps the idle policy what it is (a guess from outside) and makes the
  workload's statement unambiguous. `busy` is the one piece of `hint` that
  survives, as a hold on the idle policy (entry 8).

## 2. A park suspends from the node, as a local suspension, not through the manager

At the deadline the worker checkpoints and stops a keyed instance itself
(`run_local_suspension_for` with `LocalSuspendCause::Park`), and reports it
on an early heartbeat with `local_suspension_reason = "park"`, the path the
memory pressure response already took. A session suspends on its node.

- **Alternatives.** (a) Report "park due" in the usage report and let the
  manager's idle sweep start the suspend (`begin_automatic_suspend`), as
  for idle. (b) A new worker-plane RPC, `ParkInstance`, that asks the
  manager to suspend.
- **Trade-off.** The manager learns of the suspend after the capture, not
  before: a park can race a `SuspendInstance` (the existing local-suspension
  rule applies: the node's capture wins, the outstanding row is
  discarded), and the manager's pre-suspend checks (`publish_precondition`
  for `durability = "store"`) do not run, which is entry 6. A new field on
  `SnapshotReport` carries the reason.
- **Why (performance, simplicity).** (a) costs up to 25 seconds more per
  park, and usage reports exist only on nodes with resource enforcement
  on, so a park would silently never fire on the rest. (b) is a new RPC
  with an ACL classification, a proto surface, and a round trip before
  every capture, to arrive at the same suspend. The local suspension
  already exists, is idempotent across lost heartbeats, and was built for
  exactly "the node decided".

## 3. The socket is the boundary: no token, one socket per allocation

`$LEGION_PARK_SOCKET` is a unix socket inside the allocation's owner-only
task directory (`exec`, `container`) or behind its own VMM (`sandbox`,
`<alloc>/.sandbox/vsock.sock_1090`). A request needs no credential, and it
can only suspend the instance the socket belongs to. The file is `0666`,
so whichever user the task runs as can connect; the directory is what
keeps other allocations out.

- **Alternatives.** (a) Give the workload a scoped workload-identity token
  and have it call `SuspendInstance` on itself through the manager. (b)
  One socket per node, identifying the caller by its peer credentials
  (`SO_PEERCRED`) and cgroup.
- **Trade-off.** A TAP or `none` sandbox, whose guest previously could not
  dial the host at all, can reach one host listener when its task sets
  `suspend.park` (documented in task-runtime-sandbox.md and park.md). That
  listener parses a line-oriented protocol of four verbs, capped at 1,024
  bytes a line and 16 connections.
- **Why (simplicity, security).** A token means a credential in every
  harness, an ACL rule that lets a workload suspend exactly itself, and a
  manager round trip for something the node can decide alone. Peer
  credentials do not cross a VMM or a user namespace cleanly. A path the
  allocation alone can reach is the narrowest grant there is, and the
  power it grants (stopping yourself) is one the workload already has by
  exiting.

## 4. Legion does not deliver the result; waking reuses `instance start` and the gateway

Infinity resumes an agent by putting the tool's result on its input queue.
Park only suspends. The result reaches a parked keyed instance as a request
through `legion start gateway` (`suspend.wake`), which resumes it and holds
the request, or the platform calls `legion instance start` and delivers it
itself.

- **Alternatives.** Per-call wake tokens minted by `park` (a one-shot URL
  the harness hands its tool) and an inbox: the manager stores the result
  and the resumed task reads it from a file.
- **Trade-off.** Sessions cannot be woken by a callback until the gateway
  can route to them (wake-on-request Phase 3). The harness owns
  deduplicating a callback its sender retried. There is no timed wake
  (plan Phase 3).
- **Why (simplicity).** The gateway already turns a request into a resume
  with one start per burst and a held request. An inbox is a new store with
  its own size limits, retention, and ACLs, and it would sit on the
  replicated log for keyed instances. The callback *is* the delivery.

## 5. The checkpoint kind is chosen per park, and resolved when the park is accepted

`park checkpoint=disk|memory` overrides `suspend.checkpoint` for that one
park, and the worker resolves the kind (requested, or the task's default)
when it accepts the request, answering `FailedPrecondition` then if the
allocation cannot take it. The reply names the resolved kind.

- **Alternatives.** (a) Per task only, through `suspend.checkpoint`. (b)
  Resolve at the deadline.
- **Trade-off.** One task-manager read per `park`, run on the blocking pool
  so the socket's async threads never wait on the task manager's lock. A
  change to the task between the park and its deadline does not change the
  kind.
- **Why (simplicity).** Whether a harness "saved itself" differs per wait:
  the same agent may save its history before a long CI wait (disk, nearly
  free) and not before a short approval (memory, faithful). Resolving early
  means the workload hears "memory is not possible here" when it asks, not
  as a `lastError` after the deadline.

## 6. A keyed park's checkpoint stays local, even with `durability = "store"`

A local suspension inserts its snapshot row with the default durability,
`local`, so the manager never asks the node to publish it. Pressure
suspends have always worked this way; a park inherits it.

- **Alternatives.** Insert the row with the task's `suspend.durability`, so
  the manager's publish work picks it up.
- **Trade-off.** A parked `store` instance resumes only on its node, and
  is lost with it. Users who need the published checkpoint suspend from
  outside with `legion instance suspend`. Stated in park.md's limitations;
  TODO item opened.
- **Why.** The fix changes what a memory pressure suspend does too, and the
  publish precondition (a content store on the node) is checked when a
  manager-started suspend begins, a check this path skips. That is its own
  change with its own tests, not a rider on this one. Sessions are not
  affected: their suspend publishes `store` checkpoints itself.

## 7. Park state lives in the worker's memory; a restart forgets it

A worker restart re-binds every task's socket (the task keeps its env), but
pending parks and `busy` are gone.

- **Alternatives.** Persist each allocation's park state with the worker's
  state file and re-arm the deadlines on restart.
- **Trade-off.** An agent parked when its worker restarts stays awake, at
  full reservation, until it parks again.
- **Why (simplicity).** The failure is awake, never asleep: a restart never
  suspends a task on a request the worker cannot vouch for. Persisting is
  plan Phase 5, worth doing once restarts of busy agent nodes are common
  enough to show up as cost.

## 8. `busy` and a pending park hold the idle policy off; `busy` never expires

While the state is `busy` or `parked`, the idle tracker counts the
allocation as active whatever its signals list (`IntervalActivity.park_held`).

- **Alternatives.** (a) `busy for=<duration>`, expiring on its own. (b) A
  pending park does not hold the policy.
- **Trade-off.** A harness that crashes while `busy`, inside a task that
  keeps running, keeps its instance awake until the task ends. With (b),
  an idle suspend could beat a park to the instance with a checkpoint kind
  the workload did not ask for.
- **Why (simplicity).** An expiry is a second clock to explain and tune,
  and the common crash ends the task. Revisit with evidence (plan open
  question 1).

## 9. Text requests, JSON-envelope replies

A request is a line of words (`park ci-4821 after=30s`); a reply is one line
of the CLI's `{ ok, data | error }` envelope.

- **Alternatives.** JSON both ways, or text both ways.
- **Trade-off.** Two formats on one socket.
- **Why (simplicity).** The request has to be typeable in a shell hook
  (`printf 'park\n' | nc -U "$LEGION_PARK_SOCKET"`), and the reply has to
  be parseable by an agent with the code, message, hint, and `retryable`
  it already handles from the CLI and MCP.

## 10. `after` defaults to 10 seconds, and 0 is allowed

- **Alternatives.** (a) 0, as Infinity parks at every await. (b) 30
  seconds, the idle policy's minimum.
- **Trade-off.** A tool call that lasts 11 seconds pays a suspend and a
  resume; a 3-second one pays nothing.
- **Why (performance).** A suspend is not free in Legion the way dropping
  in-memory state is in Infinity: a sandbox resume costs a few hundred
  milliseconds, and a cold boot more. Ten seconds keeps short tool calls
  from paying for it while a real wait stops costing almost at once. A
  harness that knows better says `after=0s`.

## 11. Host vsock port 1090 is refused for upstreams only when the task parks

- **Alternatives.** Reserve 1090 on every gated sandbox, as 1080 and 53
  are.
- **Trade-off.** Whether port 1090 is allowed depends on another block of
  the same task.
- **Why (simplicity).** An upstream on 1090 collides with nothing unless
  the guest also relays its park socket there; refusing specs that work
  would be a rule with no reason behind it.
