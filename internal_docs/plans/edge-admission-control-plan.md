# Edge admission control: shed edge starts before they boot

> **Judgement calls:** [edge-admission-control-plan_calls.md](../calls/edge-admission-control-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 (the per-node claim window, the
`OVERLOADED` refusal, its metrics and docs) is built. Phase 2 (managers
stop speculating when their own apply latency is high) and Phase 3 (a
ratification carries its deadline, so a manager never commits a claim its
node already killed) are planned. The public doc is
[edge-boot.md, "Overload"](../../docs/edge-boot.md#overload).

## 1. Problem

An edge start ([edge-boot.md](../../docs/edge-boot.md)) boots on a node while
its ratification is in flight, and the node kills it at its lease
(`speculativeStart.ratifyTimeout`, default 5s) if no verdict arrives. One
start at a time, that is the right answer. Under load it collapses:

- When the managers fall behind, ratify latency climbs past the lease. Every
  start a node admits boots, holds its memory for the whole lease, and is
  killed. The router retries it, so offered load grows exactly when the
  managers have no capacity to spare.
- Nothing stopped a node admitting new edge starts meanwhile. The one knob
  meant to bound unratified work, `edgeBoot.maxProvisionalPerNode`, was
  parsed by the manager and documented, but never left the manager's config:
  `LocalLedger::set_max_provisional` had no production caller, so every node
  ran with no cap.
- The `RatifyClaim` handler is synchronous, so a ratification the node has
  already given up on is still committed when the manager reaches it. The
  manager spends its scheduler lane on starts that are already dead.

The result is a goodput collapse. In the model in §9 (100 edge nodes,
managers that ratify 500 claims a second), offering twice that rate ratifies
164 starts a second and kills 673 after they booted. The nodes are
busy, the managers are busy, and almost nothing a user asked for comes up.

## 2. Prior art

- **TCP congestion control** (Jacobson 1988, RFC 5681): many senders share
  one bottleneck and see only delay and loss. Additive increase,
  multiplicative decrease converges to a fair share of the bottleneck without
  any sender knowing how many others there are (Chiu and Jain 1989). We take
  the control law as is, including "one cut per window of data", which here
  is "one cut per congestion episode".
- **Netflix concurrency-limits and Envoy adaptive concurrency**: a
  server-side or client-side concurrency limit that tracks latency and sheds
  with a retryable 503 instead of queueing. We take the shape (a concurrency
  limit, not a rate limit) and reject the gradient algorithms: they need a
  latency baseline that edge ratification, with its raft commit, does not
  have a stable one of.
- **Google SRE client-side throttling** (*Site Reliability Engineering*,
  ch. 21): clients reject locally once their own success ratio drops. We take
  "the client decides from its own view, no coordination". We reject the
  ratio form because here "too slow" is the failure, not an explicit error.
- **gRPC deadline propagation**: a server drops work whose caller's deadline
  passed. That is Phase 3.
- **Nomad and Kubernetes** have no analogue: neither starts work before the
  placement commits, so neither has a lease to blow.

## 3. Jobs to be done

**Platform builder** (runs a router in front of edge nodes)
- When the cluster is overloaded, I want my start request refused at once
  with a reason I can act on, so I can back off or tell my user, instead of
  watching a sandbox boot and die five seconds later.
- When a start is refused, I want to know whether another node would take
  it, so I retry in the right place.

**Cluster operator**
- When ratification slows down, I want the edge nodes to stop booting work
  that will be killed, so the managers get to recover instead of drowning.
- When I size an edge node, I want to bound how much started-but-unconfirmed
  work it can hold, so a manager outage cannot kill more than I planned for.

**On-call responder**
- When users report failed starts, I want one metric that says "the nodes
  are shedding because the managers are slow", so I look at the managers
  and not at the nodes.

**Autonomous agent**
- When `legion sandbox start` fails, I want a stable code and
  `retryable: true`, so I can retry without parsing prose.

*Not in scope:* the job author. Nothing in a jobspec changes.

## 4. Goals and non-goals

Goals:
- Refuse edge starts before admission when ratification is behind, with a
  typed, retryable refusal.
- Never kill or shorten a claim that was already admitted.
- Resume admission on its own when ratification recovers.
- Make `maxProvisionalPerNode` real.
- Change nothing for a healthy cluster: no refusals and no added latency
  below capacity.

Non-goals:
- Making ratification faster (the edge ratification throughput work).
- Island mode or longer leases.
- Shedding sessions. A session is never ratified, so it has no lease to
  blow, and the launch pool (`launchConcurrency`, `queueTimeout`) already
  bounds its runtime work.
- Cluster-wide coordination of admission. Each node decides alone.

## 5. Design

### 5.1 The claim window

Each node keeps a window over its unratified **edge** claims
(`src/worker/claim_window.rs`):

- It starts at, and never exceeds, `edgeBoot.maxProvisionalPerNode`.
- A verdict that arrives within `overloadThreshold` of the claim's lease is
  timely and grows the window by `1 / window`.
- A late verdict, a failed ratify call, or an expired lease halves it, never
  below one. A claim past its threshold without a verdict counts as late the
  next time the node decides on a start, so shedding begins within the
  threshold, not a lease later.
- Only a claim admitted after the last cut can cut again.

`Worker.StartClaim` reserves room before it takes a launch slot. It waits up
to `edgeBoot.queueTimeout` for room, then refuses. A reservation is bound to
the claim at admission and released at every exit an unratified claim has
(`ratify_claim`, `kill_claim`), so a slot cannot leak.

### 5.2 The surface

Worker type config:

```nickel
edgeBoot = {
  acceptClaims = true,
  queueTimeout = "1s",          # also bounds the wait for window room
  maxProvisionalPerNode = 32,   # moved here from the manager config
  overloadThreshold = 0.3,      # 0: static cap, no adaptation
}
```

The refusal:

```console
$ legion sandbox start agent-sandboxes --id user-42 --node https://10.0.1.7:4848 --ticket "$T" -j
{ "ok": false, "error": { "code": "OVERLOADED", "retryable": true,
  "message": "...node is overloaded: recent ratifications were slow, so it holds at most 2 unratified edge claims (edgeBoot.maxProvisionalPerNode is 32), and all 2 stayed unratified for 1s",
  "hint": "the managers are behind on ratification, so retry with backoff (other nodes share the same managers)" } }
```

On the wire it is `RESOURCE_EXHAUSTED` with `legion-error-code: OVERLOADED`.
The message tells a shedding window (managers behind, back off) from a full
one at its ceiling (this node is busy, another may take it).

Metrics: `legion_worker_claim_window` (gauge) and
`legion_worker_claim_overload_refusals_total` (counter).

### 5.3 What is deliberately left alone

- Manager-originated speculative dispatch is not counted (calls entry 3).
- The ratify path on the manager is unchanged (Phase 3).
- `LocalLedger` capacity is still never configured in production
  (`configure_ledger` has only test callers), so verification step 7 in
  edge-boot.md checks against infinite capacity. The window at least bounds
  how many edge claims a node admits. Tracked in TODO.md.

## 6. Proto, struct, and config changes

- `EdgeBootConfig.maxProvisionalPerNode` (manager) is removed.
- `WorkerEdgeBootConfig` gains `maxProvisionalPerNode` and
  `overloadThreshold`, validated when the manager loads worker configs.
- `LocalLedger.max_provisional` and `set_max_provisional` are removed.
- No wire change. `StartClaim`'s proto comment documents the refusal.
- No job-config field, so `maximal.ncl` is unchanged.

## 7. Phases

1. **Claim window** (built). Everything in §5.
2. **Managers stop speculating under load.** `dispatch_speculative` skips
   speculation when recent plan-apply latency passes a fraction of the
   task's lease. Today a speculative claim whose apply runs past its lease
   dies on the node, and its committed allocation boots again later.
3. **Deadline-aware ratification.** `RatifyClaimRequest` carries the claim's
   remaining lease, and the manager refuses `expired` without committing
   when it reaches a ratification too late. Today such a commit binds the
   instance to a claim the node already killed, so the router's retry gets
   `instance_claimed` until the allocation is reconciled away.

## 8. Testing

- Unit (`claim_window.rs`): cap, halving, once-per-episode cuts, overdue
  detection, the floor of one and recovery, failure kinds, the static mode,
  reservation release, waiters woken by a verdict.
- End to end on one node (`server.rs`, `edge_overload_test`): a real ticket
  verifier, task manager, and `exec` runtime with a ratifier whose latency
  the test sets. A start past the threshold is refused `OVERLOADED` and
  retryable, nothing is admitted, the in-flight claim still ratifies, the
  same ticket is accepted on retry, and the window grows back.
- Model (`claim_window.rs`, `overload_sim`): the real `WindowState` on 50 to
  200 simulated nodes against managers with fixed throughput. A test asserts
  goodput above 85% of capacity at twice the load, and `overload_table` prints
  §9.

## 9. Measurements

Model, not a cluster: 30 simulated seconds, Poisson arrivals, managers that
ratify 500 a second first come first served and process every request even
after its node gave up, 5s lease, 1s queue timeout. Per second of offered
load:

| Nodes | Load | Mode | Ratified | Killed after boot | Refused before boot |
| --- | --- | --- | --- | --- | --- |
| 100 | 0.9x | every mode | 451 | 0 | 0 |
| 100 | 1.2x | no window (before) | 491 | 10 | 0 |
| 100 | 1.2x | window (after) | 500 | 0 | 66 |
| 100 | 2.0x | no window (before) | 164 | 673 | 0 |
| 100 | 2.0x | static cap of 32 | 165 | 430 | 275 |
| 100 | 2.0x | window (after) | 500 | 0 | 453 |
| 100 | 3.0x | no window (before) | 124 | 1,130 | 0 |
| 100 | 3.0x | window (after) | 500 | 0 | 936 |

Below capacity the window never binds: every mode is identical up
to 0.9x at 50, 100, and 200 nodes. `overload_table` (ignored by default,
`cargo test --lib -- --ignored overload_table --nocapture`) prints the full
grid, with p50 and p99 ratify latency and the 0.5 threshold for
comparison.

Not measured: a real multi-node raft cluster, real microVM boots, and a
real router's retry behavior.

## 10. Rollout

Edge boot is off by default, so a cluster that does not use it sees no
change. An edge node now holds at most 32 unratified edge claims where it
used to hold any number. At healthy ratify latency that admits hundreds of
starts a second per node. A cluster that set `maxProvisionalPerNode` on the
manager must move it to the worker type's `edgeBoot` block. The manager's
`edgeBoot` block now refuses unknown fields, so a stale setting stops the
manager at startup instead of being silently ignored.

## 11. Open questions

- Should the window also wait less (or not at all) when it is shedding, so
  the refusal comes back at once? Lean: no. One wait keeps one knob, and the
  wait paces retries.
- Should a node learn from its pool's other nodes (gossip) to react faster
  at low traffic? Lean: not until a node with little traffic is shown to
  matter.

## 12. Acceptance criteria (Phase 1)

- An edge start past the threshold is refused `OVERLOADED`, retryable,
  before admission, and its ticket is still redeemable.
- An admitted claim is never killed by the window.
- The window returns to `maxProvisionalPerNode` once verdicts are timely.
- `maxProvisionalPerNode` is enforced on every edge node.
