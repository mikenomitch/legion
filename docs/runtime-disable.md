# Runtime Disable: Stopping a Task Runtime Across the Cluster

When a sandbox boundary has a published escape (a runc CVE, a Firecracker or
cloud-hypervisor advisory, a bug in a runtime plugin), the response is
always the same: stop new untrusted code from starting on the affected
runtime now, decide what to do with what is running, upgrade, and turn it
back on. **Runtime disable** is the first and last step of that as one
command each:

```console
$ legion operator runtime disable container --reason "CVE-2024-21626, runc < 1.1.12"
Runtime container disabled for cluster since 2026-11-07T18:04:11Z (CVE-2024-21626, runc < 1.1.12).
Running on it in scope: 412 allocation(s), 0 session(s); they keep running (pass --drain to take them off).

$ legion operator runtime enable container
Runtime container enabled.
```

A disable is one replicated record. From the moment it is written:

- the scheduler places nothing on the runtime in scope, and jobs that need
  it wait, blocked, with the reason;
- every worker refuses to start it: placements that raced the disable,
  session starts, edge claims presented with a ticket, resumes, and
  restarts;
- running work is left alone, unless you ask for a drain.

Enabling the runtime deletes the record. Blocked jobs place on their own,
and callers that were refused succeed when they retry.

> Related: [namespace policies](namespaces.md#namespace-policies) (the
> standing rule for which runtimes a namespace may submit),
> [blocked evals](scheduler-and-eval-loop.md#blocked-evals),
> [suspend and resume](suspend-and-resume.md), [session
> mode](session-mode.md), [edge boot](edge-boot.md), and [graceful
> stop](graceful-stop.md).

## Runtime disable and namespace policy

Two things say which runtimes may run, and they do different jobs:

| | Namespace policy `runtimes` | Runtime disable |
| --- | --- | --- |
| What it is | A standing rule, part of a policy object | A temporary switch for an incident |
| Checked | When a job is submitted (`job run`, `job plan`) | When work starts: placement, session starts, claims, resumes, restarts |
| Scope | The namespaces the policy selects | The whole cluster, or namespace globs |
| Work already admitted | Never re-judged | Refused from then on; running work untouched unless drained |
| A refused submit | Rejected | Accepted with a warning; it places after the enable |
| Needs | `acl:write` | `operator:write` |

A policy cannot stop a job that was admitted before the advisory from
scaling out, restarting, or starting sessions. A disable can.

## The surface

```console
$ legion operator runtime disable <RUNTIME> [--namespace <GLOB>]... [--reason <TEXT>] [--drain]
$ legion operator runtime enable  <RUNTIME> [--namespace <GLOB>]...
$ legion operator runtime status
```

Every form takes `--json`/`-j`; `disable` and `enable` take
`--idempotency-key`.

- **`<RUNTIME>`** is a task's `runtime` value: `exec`, `container`,
  `sandbox`, `noop`, or the name of a [runtime
  plugin](task-runtime-plugins.md). Any name is accepted, so a plugin's
  runtime is disabled the same way. `envelope` is refused: it only reserves
  room for a session task, and disabling the session task's own runtime
  (for example `sandbox`) is what stops its sessions.
- **`--namespace`** (`-n`) limits the call to namespaces matching a glob,
  where `*` stands for any run of characters. Repeat it, or separate globs
  with commas. Without it the call covers the whole cluster. As in
  namespace policies, a glob that does not start with `legion-` never
  covers a reserved `legion-` namespace; a cluster-wide disable covers
  every namespace, reserved ones included.
- **`--reason`** (at most 512 bytes) is shown in every refusal, in
  `status`, and in the `runtime.disabled` event. Give the advisory id.
- **`--drain`** also takes running work off the runtime; see
  [Draining](#draining).

`operator:write` is needed to disable and enable, `operator:read` for
`status` ([auth and ACLs](auth-and-acls.md)).

### JSON

```console
$ legion operator runtime disable sandbox --namespace 'tenants-*' --reason "firecracker GHSA-xxxx" -j
{"ok":true,"data":{"runtime":"sandbox","enabled":false,"scope":"tenants-*",
 "namespaces":["tenants-*"],"since":"2026-11-07T18:04:11Z","by":"accessor:7f3c9a",
 "reason":"firecracker GHSA-xxxx","drain":false,"requested":"disable","changed":true,
 "deduplicated":false,"running":{"allocations":3,"sessions":9031}}}

$ legion operator runtime status -j
{"ok":true,"data":{"disabled":[{"runtime":"sandbox","scope":"tenants-*",
 "namespaces":["tenants-*"],"since":"2026-11-07T18:04:11Z","by":"accessor:7f3c9a",
 "reason":"firecracker GHSA-xxxx","drain":false,
 "running":{"allocations":3,"sessions":9031},
 "blocked":{"evals":12,"edgeRefusals":340},"unawareNodes":[]}]}}

$ legion operator runtime enable sandbox --namespace 'tenants-*' -j
{"ok":true,"data":{"runtime":"sandbox","enabled":true,"scope":"cluster",
 "requested":"enable","changed":true,"deduplicated":false}}
```

| Field | Meaning |
| --- | --- |
| `scope` | `cluster`, or the globs |
| `since` | When the runtime was first disabled. Widening the scope later does not move it |
| `by` | `accessor:<id>` of the token that wrote it, or the caller's principal when it has no token (`acl-disabled` on a cluster running with ACLs off) |
| `changed` | Whether the call changed the record. False for a repeat, and for an enable of a runtime that is not disabled |
| `running.allocations` | Live allocations of the runtime in scope |
| `running.sessions` | Running sessions of the runtime in scope that nodes report to the managers (sandbox sessions) |
| `drained` | On a `--drain`: `suspending`, `stopping`, and `failed` (allocations that could be neither) |
| `blocked.evals` | Evals blocked under `runtime_disabled:<runtime>`: jobs that place when the runtime is enabled |
| `blocked.edgeRefusals` | Starts nodes refused because of the disable, as the answering manager heard of them in heartbeats |
| `unawareNodes` | Ready nodes whose worker predates runtime disables; see [Version skew](#version-skew) |

### How scopes combine

There is one record per runtime.

| Call | A record exists? | Result |
| --- | --- | --- |
| `disable R` | no | disabled cluster-wide |
| `disable R -n A` | no | disabled for `A` |
| `disable R -n B` | scoped to `A` | disabled for `A, B` |
| `disable R` | scoped | disabled cluster-wide |
| `disable R -n A` | cluster-wide | stays cluster-wide (a disable never narrows) |
| `enable R` | any | enabled everywhere |
| `enable R -n A` | scoped to `A, B` | disabled for `B` |
| `enable R -n A` | cluster-wide | refused: `FAILED_PRECONDITION` |
| `enable R -n C` | scoped to `A, B` | refused: `FAILED_PRECONDITION` |
| `enable R` | no | succeeds, `changed: false` |

A repeated `disable` replaces `reason` and the `drain` flag with the newest
call's values and keeps `since`.

To keep a runtime off for some namespaces after a cluster-wide disable,
enable it and disable the namespaces:

```console
$ legion operator runtime enable sandbox
$ legion operator runtime disable sandbox --namespace 'tenants-*' --reason "still unpatched"
```

## What a disable does

| Path | Effect |
| --- | --- |
| Submit (`job run`, `job plan`) | Accepted, with a `[runtime-disabled]` warning naming the disable, so a deploy prepared during the incident is not lost |
| Placement | The scheduler places nothing of a task on the runtime in scope. The job's tasks in other groups still place. When nothing of the job could place, its eval is blocked under `runtime_disabled:<runtime>` |
| Scale-out, rescheduling after a failure | The same: no new allocation of the runtime starts |
| Updating a job that has a task on the runtime (a [rollout](rollouts.md)) | The rollout pauses before its next step places or retires anything, so the version that is running keeps running. Its tick looks again every 15 seconds, records `eval.blocked` with the disable's sentence, and the rollout continues after the enable. It is not failed |
| Restarting such a job (`job run --force-reschedule`, a batch schedule's next run) | Waits whole, blocked under `runtime_disabled:<runtime>`: nothing is stopped, and the restart happens after the enable |
| Session starts through a manager (`legion instance create` on a session task) | Refused by the manager, before any node is asked |
| Session starts and claims at the edge (a ticket presented to a node) | Refused by the node. The ticket is not spent |
| Resuming a suspended instance or session | Refused. It stays suspended with its checkpoint |
| Restarts (the task's `restart` policy, health-check restarts) | Refused by the node. The allocation is reported failed, and nothing replaces it while the runtime is disabled |
| Running work | Untouched, unless `--drain` |

A refusal is `UNAVAILABLE`, which is retryable, with error code
`RUNTIME_DISABLED` and the disable's own sentence:

```text
UNAVAILABLE: runtime "sandbox" is disabled in namespace tenants-acme since 2026-11-07T18:04:11Z: firecracker GHSA-xxxx (legion operator runtime status)
```

In the CLI's and the MCP server's error envelope that is:

```json
{"ok":false,"error":{"code":"RUNTIME_DISABLED",
 "message":"...: runtime \"sandbox\" is disabled in namespace tenants-acme since 2026-11-07T18:04:11Z: firecracker GHSA-xxxx (legion operator runtime status)",
 "hint":"retry once the runtime is enabled again; `legion operator runtime status` shows what is disabled and why",
 "retryable":true}}
```

The warning on a submit:

```text
[runtime-disabled] machine: runtime "sandbox" is disabled in namespace tenants-acme since 2026-11-07T18:04:11Z: firecracker GHSA-xxxx (legion operator runtime status); the job is accepted, and its work on this runtime starts when the runtime is enabled
```

And what `legion job status` shows for a job waiting on the runtime:

```text
Blocked on runtime_disabled:sandbox since 2026-11-07T18:05:02Z: task machine waits for its runtime to be enabled
  Failed to place task machine: runtime "sandbox" is disabled in namespace tenants-acme since 2026-11-07T18:04:11Z: firecracker GHSA-xxxx (legion operator runtime status)
```

Tasks of one group place together on one node, so a task that shares a
[group](jobs-tasks-and-scheduling.md) with a task on a disabled runtime
waits with it. Tasks of a job share one group unless they set `group`.

## How workers learn

Managers send the whole set of disables on every heartbeat reply, and push
it on the work stream (`StreamWork`) the moment the set changes, and when a
node's stream opens while anything is disabled. A worker replaces the set
it holds and refuses starts from then on. So:

- a node that streams its work learns of a disable as soon as its manager
  has applied it;
- every node learns within one heartbeat interval, whatever happened to the
  push;
- a start at the edge, with no manager on the path, is refused within that
  interval.

A worker writes the set beside its state file
(`<worker state dir>/runtime-states.json`) whenever it changes, and reads
it back when it starts. A worker that cannot reach any manager keeps the
last set it was given, across its own restarts.

Any manager answers a heartbeat from its own replica of the record, so a
follower that has not applied a just-written disable can serve the old set
for one beat. The next beat corrects it.

## Draining

`--drain` takes running work in scope off the runtime, keeping state where
it can:

| Unit | With a `suspend` block | Without |
| --- | --- | --- |
| A keyed instance | Suspended: its checkpoint is taken and kept, reason `runtime_disabled` | Its allocation is removed for a [graceful stop](graceful-stop.md) |
| A session | Suspended on its node, checkpoint kept | Stopped |
| A service, batch, or slot allocation | (cannot be suspended) | Removed for a graceful stop, cause `runtime_disabled` |

The managers drain allocations when the disable is written, and report what
they started in `drained`. Each node drains its own sessions for as long as
the record asks for a drain: on every set it receives, so a session that
was busy with another capture, or that started in a race with the disable,
is taken by a later pass.

- A suspend that cannot start (the node cannot take checkpoints, a
  `durability = "store"` checkpoint could not be published) falls back to a
  graceful stop.
- A stopped allocation is not replaced: its job's eval blocks under
  `runtime_disabled:<runtime>` and places again after the enable. The
  instance's `last_stop` says why.
- A suspended instance or session stays suspended after the enable until
  something resumes it (a wake on connect, `legion instance start`, or its
  caller), as any suspended unit does.
- A disable without `--drain`, then one with it, drains: the second call
  replaces the flag. Run the same `disable --drain` again to retry anything
  listed in `drained.failed`.
- `enable` takes no `--drain`.

A drain is not instant. A graceful stop takes up to the task's stop grace,
and a suspend takes as long as its capture. `status` shows what is still
running.

## Re-enabling

`enable` deletes the record (or removes the named globs from a scoped one).

- **Blocked jobs place.** The scheduler leader returns every eval blocked
  on the runtime to the queue, plus the evals of jobs that were waiting for
  room and also have a task on it, and queues one pass for jobs that placed
  part of their work. No resubmit is needed.
- **Refused callers succeed on retry.** A ticket a node refused was never
  spent.
- **Drained work.** Stopped services and instances place again with the
  rest. Suspended ones wait for a resume.

## Observability

- **Events** ([event stream](event-stream.md)): `runtime.disabled` (warn)
  and `runtime.enabled` (info), with `runtime`, `namespaces`, `scope`,
  `reason`, `since`, `by`, and `drain` in the metadata. A woken eval emits
  `eval.unblocked` with `cause = runtime_enabled`. Drained allocations
  emit `allocation.stopping` with `cause = runtime_disabled`, and a drained
  instance `instance.suspended` with `reason = runtime_disabled`.
- **Metrics** ([metrics](metrics.md)):
  `legion_runtime_disabled_refusals_total{runtime}` on each worker,
  `legion_evals_blocked{key_kind="runtime"}` and
  `legion_evals_unblocked_total{event="runtime_enabled"}` on the scheduler
  leader, and `legion_session_starts_total{outcome="runtime_disabled"}` on
  the managers' session router.
- **Worker logs** name the set whenever it changes
  (`Runtime disables changed`).

## Failure modes

| Symptom | Cause | What to do |
| --- | --- | --- |
| `INVALID_ARGUMENT: runtime "envelope" only reserves room for a session task` | `envelope` is not a runtime work runs on | Disable the session task's runtime (`sandbox`, `exec`, ...) |
| `INVALID_ARGUMENT: namespace glob ...` | A `--namespace` value is not a namespace name with optional `*` | Fix the glob |
| `FAILED_PRECONDITION: runtime "sandbox" is disabled cluster-wide since ...; a cluster-wide disable cannot be lifted one namespace at a time` | `enable -n` against a cluster-wide record | `enable` it, then `disable -n` what should stay off |
| `FAILED_PRECONDITION: runtime "sandbox" is not disabled for "payments"` | `enable -n` named a glob the record does not hold | Name the globs as `status` lists them |
| `FAILED_PRECONDITION` naming a raft leader | The call reached a manager that does not lead the control lane | Retry against the manager it names |
| `status` lists `unawareNodes` | Those workers predate runtime disables | See [Version skew](#version-skew) |
| Work still running after `--drain` | Stops and captures are in flight, or `drained.failed` listed it | Wait, check `status`, re-run the `disable --drain` |
| A job stays blocked after `enable` | It is blocked on something else now (room, a pool) | `legion job status <job>` names the key it waits under |
| A typo'd runtime name is accepted | Any name is a valid runtime name | `status` shows it with nothing running and nothing blocked; `enable` it |

## Version skew

A worker that predates runtime disables ignores the set. Placement still
refuses work for it, because the scheduler decides that, and a session
start through a manager is refused by the manager. What such a node still
does is start work a ticket is presented to it for (an edge claim or an
edge session start), and restart tasks it already runs. `status` names
those nodes in `unawareNodes`. Upgrade them, or drain them for the incident
(`legion node drain <node>`).

## Known limitations

- **No per-node or per-pool disable.** The scope is the cluster or
  namespaces. To take a runtime off some nodes, drain those nodes.
- **No automatic disable** from an advisory feed or a version check, and no
  expiry: a disable stands until someone enables the runtime.
- **Not an MCP tool.** The switch is in the CLI, the gRPC API, and the
  capability catalog. A cluster-wide stop of a runtime is an incident
  action for a person holding an `operator` token.
- **A placement that raced the disable is reported as a failed
  allocation.** The node refuses it, and the refusal spends one move of the
  task's `onUnhealthy` budget before its replacement blocks on the runtime.
  The same holds for a refused restart.
- **An update to any part of a job with a task on a disabled runtime
  waits** for the enable, including changes to its other tasks: its rollout
  pauses whole.
- **`running.sessions` counts what nodes report.** Sandbox sessions are
  reported to the managers; sessions of other runtimes are known only to
  their node and are not counted, though they are refused and drained like
  any other.
- **`blocked.edgeRefusals` is a count on the answering manager**, summed
  from the heartbeats it received. It restarts from zero when that manager
  restarts, and another manager has its own count.
- **A drained allocation is stopped, not moved.** Nothing else can run it
  while its runtime is disabled.
- **A disable does not change a job's spec.** A task cannot fall back to
  another runtime while its own is disabled.
