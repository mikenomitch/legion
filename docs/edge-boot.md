# Edge boot: starting work before the placement is durable

Edge boot lets a node start work before its placement is committed. Use it
when start latency matters, such as a platform that boots a fresh sandbox for
each user request.

Legion normally commits a placement and then runs it. A submit becomes an
eval, the eval plans, and the plan applies. Under `persistence.type = "raft"`,
applying means proposing on the scheduler lane and waiting for the entry to
commit. Only then does a worker get the payload, build the root disk, and
boot.

For a service that runs one long-lived job, that order costs nothing. For a
platform that spawns a fresh sandbox per user request, it sits on the
critical path of every request. It is also serial: nothing on the node starts
until every control-plane step has finished.

**Edge boot lets a node reach a configured stage before its allocation is
durable. The durable commit still decides who owns an instance.** It also
lets a node authorize such a start on its own, from a short-lived capability,
without talking to a manager at all.

Edge boot is **off by default** in three places: `speculativeStart` is absent
from every jobspec, `edgeBoot.enabled` is false on the manager, and
`edgeBoot.acceptClaims` is false on every worker. A cluster that upgrades and
changes nothing behaves as it did before, including its wire payloads.

- [The two shapes](#the-two-shapes)
- [Gate levels](#gate-levels)
- [The jobspec surface](#the-jobspec-surface)
- [What is never speculative](#what-is-never-speculative)
- [What the gate does and does not stop](#what-the-gate-does-and-does-not-stop)
- [Boot tickets](#boot-tickets)
- [The kill path](#the-kill-path)
- [Enabling it](#enabling-it)
- [Observability](#observability)
- [Version skew](#version-skew)
- [Known limitations](#known-limitations)

## The two shapes

### Manager-side speculative dispatch

The manager sends the payload while the apply is still in flight instead of
after it commits. It then sends the apply outcome as the ratification. This
is an internal reordering only. It uses the same manager, the same worker
plane, and the same `PostWork` RPC, with a `provisional` block on the payload.

It saves the raft commit, the read-model refresh, and the dispatch
serialization behind them. That is roughly 5–25 ms on a three-node raft
cluster, and more when the scheduler lane is busy. It does **not** save the
client-to-manager round trip or the eval queue. It needs no new
authorization, and a job gets it just by declaring `speculativeStart`.

### Edge-initiated starts

```
platform's own API ──(user auth)──► platform control plane
                                        │  (once, ahead of time or on demand)
                                        ▼
                              manager: legion boot-ticket create
                                        │
                                  signed ticket (JWT, ~minutes)
                                        ▼
   user request ──► edge router ──► legion sandbox start --node https://<addr>
                                        │ verified offline, admitted locally,
                                        │ booted to the gate level
                                        ▼
                              manager: RatifyClaim ──► store apply ──► verdict
```

This saves the entire control-plane round trip, including the client's. For
a cross-region edge, that round trip is the largest cost. It also keeps
starts working while the control plane is unavailable, for as long as a
ticket is valid.

## Gate levels

A gate says how far a node may get on its own. Gates are ordered. Where two
limits meet (a task's mode, a ticket's `gate`, a caller's request), **the
lower one wins**. A jobspec cannot raise a ticket's limit, and a ticket
cannot raise a jobspec's.

| Mode | The node may, before ratification | It may not, until ratified |
| --- | --- | --- |
| `off` (default) | nothing | everything (the normal start path) |
| `stage` | fetch artifacts, pull the image, build the root disk, mount node-local storage | start the workload |
| `boot` | all of `stage`, plus boot the guest and run the workload with its network held shut | open the network, register in service discovery, attach shared storage, obtain any Legion-issued identity |
| `serve` | all of `boot`, plus open the network immediately | obtain any Legion-issued identity, attach shared storage |

- **`stage`**: pull the image, but don't boot until the claim is confirmed.
  Nothing outside the node can see it. Losing the race only wastes local
  work.
- **`boot`**: boot it, but allow no ingress or egress until the claim is
  confirmed. The workload runs and warms up. Nothing it does reaches the
  outside world, so a loser leaves nothing behind except its own disk.
- **`serve`**: boot all the way and allow networking. If the id turns out to
  be claimed already, kill it. This is the fastest mode, and **the only mode
  where a workload that loses its race may have caused externally visible
  side effects before it is killed.** Accept that trade-off before you use
  it. It only happens when two callers race for one instance id, which for
  an id-affine router points to a routing bug.

Every mode preserves Legion's own invariants. The modes differ only in what
an unratified workload can do to the outside world.

## The jobspec surface

```nickel
{
  name = "workspace",
  runtime = "sandbox",
  group = "sandboxes",
  stateful = { mode = "keyed", storage = { path = "/workspace", mode = "block" } },

  # How far this node may get before the placement is durable.
  speculativeStart = {
    mode = "boot",           # 'off' (default) | 'stage' | 'boot' | 'serve'
    ratifyTimeout = "5s",    # lease; unratified work is killed at expiry
    onRefused = "kill",      # 'kill' (default) | 'stop' (graceful, then remove)
  },

  config = { image = "alpine:latest", vcpus = 2 },
  resources = { memory = 1024 },
}
```

**Defaults.** `mode = "off"`, `ratifyTimeout = "5s"` (capped at `2m`),
`onRefused = "kill"`. Leaving out the block means `off` and costs nothing.

The block is validated at **submit**, not at dispatch. An unrecognized `mode`
or `onRefused`, or a `ratifyTimeout` of zero or over the cap, fails
`legion job run` with the reason. If a typo were silently read as `off`, you
would only notice it later as a latency regression.

```console
$ legion job run api.ncl
error: task a: invalid speculativeStart mode "boott" (off|stage|boot|serve)
```

`kill` is the default because the usual refusal is "someone else owns this
id", and a losing workload that keeps running breaks the at-most-one
guarantee. `onRefused = "stop"` runs the task's normal graceful stop
([`docs/graceful-stop.md`](graceful-stop.md)) instead, for workloads that
need to flush before they exit.

### Automatic downgrades

Two things force a task down to `stage` whatever it declares, because
starting earlier would need something the apply transaction has not produced
yet. `legion job validate` reports both before you deploy:

```console
$ legion job validate api.ncl
Jobspec 'api' is valid: 1 task(s) across 1 group(s)
warning: speculativeStart.mode `serve` is downgraded to `stage`: task `api`
declares network ports, which are assigned when the placement commits; gates
above `stage` would boot it without them
```

With `--json`, the same lines come back in a `warnings` array on the success
envelope.

- **Declared ports.** `network.ports` and `connect.upstreams` are assigned
  inside the apply, so a workload booted before the apply cannot know them.
  A `stage`-gated task is unaffected. It picks up the committed payload
  (actual allocation id, assigned ports, resolved mounts) before it starts.
- **Storage that cannot be fenced.** A volume that a second node could attach
  at the same time is mounted only after ratification. Node-local drivers
  (`local-dir`, `host`, `tmpfs`) are exempt. The only possible competitor is
  on the same node, where the worker's claim table already allows one live
  claim per instance.

## What is never speculative

These are fixed rules, not settings, and they are what make `serve` safe
enough to offer:

- **Legion-issued identity.** Workload identity tokens are scoped to an
  allocation id, which does not exist until ratification. So an unratified
  workload cannot authenticate to Legion, to a cloud provider, or to anything
  else that trusts the cluster issuer. A losing sandbox cannot act as the
  winner.
- **Shared writable storage.** Mounted only after ratification (see the
  downgrade rule above).
- **Service discovery.** A provisional allocation is not registered, so no
  other workload can route to something that may disappear.
- **Instance binding.** Nothing about `user-42` changes in the store until
  ratification. `legion instance get user-42` shows the live claim next to the
  instance: the node holding it, the gate, the stage it has reached, and when
  its lease expires. "Started but not yet confirmed" is a state you can see,
  instead of an instance that has no allocation for no clear reason.

A provisionally started task also gets **no `LEGION_ALLOC_ID`**, because it
does not have one yet and a placeholder would be wrong. It gets
`LEGION_CLAIM_ID` instead. `LEGION_INSTANCE_ID` is correct from the start,
and that is the identity a platform uses to address a sandbox.

## What the gate does and does not stop

**The `boot` gate is a strong network boundary for the `sandbox` runtime and
a weaker one for host runtimes:**

- **`sandbox` / microVM: enforced.** The guest reaches the outside world
  through one host TAP device (or, with `network.mode = "none"`, through
  nothing). The device is attached at boot, because Firecracker cannot
  hotplug a NIC, and held administratively **down**. Ratification brings it
  up. Both operations need `CAP_NET_ADMIN`, which `network.mode = "tap"`
  already requires.
- **`container` / `exec` with `network.mode = "gated"`: enforced.** The
  workload runs in an empty network namespace whose only way out is the node
  datapath ([Network Handlers](network-handlers.md#gated-mode)). Every
  listener denies with source `gate` until ratification opens it, whatever
  the allocation's policy allows. After ratification, the policy applies per
  destination.
- **`container` / `exec` in `host` mode: advisory.** Legion withholds what it
  controls: service registration and the node-local upstream forwarders,
  neither of which exists until ratification. But the workload shares the
  host's network stack and **can still reach the network through it.**
  Host-level egress filtering for these runtimes is up to the operator. The
  worker logs a warning each time it starts a `boot`-gated task on such a
  runtime.

Run untrusted code in a microVM, as
[`docs/task-runtime-sandbox.md`](task-runtime-sandbox.md) also advises.

## Boot tickets

A boot ticket is a short-lived JWT signed by the cluster's workload-identity
signer. The worker verifies it against the published JWKS. It uses the same
key material, rotation, and `kid` handling as workload identity.

```jsonc
{
  "iss": "https://manager.example.com:4747",
  "aud": "legion:node",                    // reserved; not a task audience
  "sub": "boot:agent-sandboxes/workspace",
  "exp": 1770000300, "iat": 1770000000, "jti": "tkt_5c1a…",
  "legion.io/scope": "boot",
  "legion.io/job": "agent-sandboxes",
  "legion.io/job_config": "jc-7f3a91",     // an exact version, never "latest"
  "legion.io/task": "workspace",
  "legion.io/instance": "user-42",         // or "legion.io/instance_prefix": "user-"
  "legion.io/pool": "edge-eu",             // where it may run
  "legion.io/gate": "boot",                // ceiling; the task's mode also caps it
  "legion.io/uses": 1                      // single-use by default
}
```

### Minting

```console
$ legion boot-ticket create agent-sandboxes \
    --task workspace --id user-42 --pool edge-eu --gate boot --ttl 5m
Boot ticket created (tkt_5c1a…).

This is the only time the ticket is shown:

  eyJhbGciOiJFUzI1NiIsImtpZCI6…

Pinned to job config: agent-sandboxes-7
Expires (unix): 1770000300
```

Minting requires the ACL resource **`boot`** (`boot:write`, scoped by job
name). It is separate from `job:write` so a platform's ticket minter can
authorize individual starts without the right to redeploy the fleet.
`boot:read` lists outstanding tickets.

A ticket must name an instance or a **non-empty** prefix. A ticket with
neither would authorize every sandbox of the task, so Legion does not mint
one. With a prefix ticket, you mint once per user cohort instead of once per
request. The cost is a wider capability if the node holding it is
compromised.

The manager stores only metadata: `jti`, scope, expiry, and the minting
accessor. The token itself is never written anywhere.

### Redeeming

```console
$ legion sandbox start agent-sandboxes \
    --id user-42 --node https://10.0.1.7:4848 --ca-cert ca.pem --ticket "$TICKET"
admitted
booted
ratified
```

`--node` is the node's `workerAddress` (`legion node info` shows it), always
`https://`: the worker plane is mutual TLS
([auth-and-acls.md](auth-and-acls.md#the-worker-plane)). `StartClaim` is the
one worker RPC that needs no client certificate, because the boot ticket is
the authorization. The caller still verifies the node: `legion sandbox start`
checks that the node presents a node certificate from the cluster CA before
it sends the ticket. It reads the CA from `--ca-cert <file>` (save it once
with `legion net ca show > ca.pem`), or fetches it from the manager with your
token when the flag is absent. A router with its own gRPC client does the
same: trust `ca.pem`, and expect the server name `<node>.node.legion`.
Routers must be able to reach 4848 on edge nodes.

The stream reports each transition. A router may start routing at `booted`,
but must handle `refused` at any point until `ratified`. `--dry-run` verifies
the ticket and local admission and reports what would happen, without
holding or booting anything.

### Verification on the worker is entirely offline

1. signature against a cached JWKS (fetched on first use, refreshed on a `kid`
   miss and on `edgeBoot.jwksRefresh`. **A fetch failure never invalidates
   cached keys**)
2. `iss`, `aud == legion:node`, `exp`/`nbf` with bounded clock skew
3. `legion.io/pool` against this node's own pool
4. `legion.io/gate` intersected with the task's `speculativeStart.mode` (the
   lower wins)
5. `jti` against a local single-use set
6. the local claim table (one live claim per instance id per node)
7. the local resource ledger (the node must have room)

### Replay and revocation

**Ratification, not verification, limits replay across nodes.** If two nodes
get the same ticket, both boot. The second is refused with
`instance_claimed` and kills its work. The manager enforces global single
use by spending the `jti` in one conditional update, so the second
ratification is refused even if the first workload has since exited. Nodes
are expected to behave correctly, but Legion does not trust any node to be
the only holder of a ticket.

**The ratification is one write.** The ticket spend, the instance bind, the
allocation, and the `provisional.ratified` event commit in a single
scheduler-lane entry, all or nothing. A ratification that is refused (the
node no longer fits, another node won the instance) therefore leaves the
ticket unspent: a single-use ticket is consumed only by the start that
actually ratifies.

**The edge start can be the create.** A ticket that names a keyed instance
id that has never existed creates the instance in that same entry, so a
platform does not have to call `CreateInstance` first. A destroyed id is a
tombstone, not "never existed", and is refused (`instance_claimed`): ids
are never recycled. An id that is not a valid keyed instance id is refused
with `unknown_target`.

**Revocation is by TTL only.** The fast path cannot check a revocation list
without adding the round trip the ticket exists to avoid. If you need fast
revocation, set short TTLs and mint new tickets. The manager can always
refuse ratification, which limits any damage to the gate level.

### The spec cache

A ticket names a `job_config_id`, and the node needs the matching task
config to run anything. A node caches every task config it is dispatched,
whether from an ordinary `PostWork`, a heartbeat's desired set, or a
provisional start. So the node can redeem a ticket for any config it has
already run an allocation of, without contacting a manager.

A `StartClaim` for a config the node has never seen is refused with
`FailedPrecondition` and names the config:

```
this node has no cached spec for job config agent-sandboxes-7; it caches a
config once it has been dispatched an allocation of it, and does not fetch
specs on the start path
```

**A node never fetches a spec on the start path**, because that would add
back the round trip the ticket exists to avoid. In practice, keep at least
one allocation of the job running in each edge pool, which a warm sandbox
fleet does anyway. There is no dedicated pre-warm push.

## The kill path

Every failure ends the same way: the node kills the work.

| Trigger | Detected by | Latency |
| --- | --- | --- |
| Ratification refused | the ratify reply (or a heartbeat catch-up verdict) | one RPC |
| Manager unreachable | the lease deadline | `ratifyTimeout` (default 5s) |
| Node loses contact after ratification | the existing self-fence | `stateful.onLost.selfTerminateAfter` |
| Worker restart with claims held | startup reconcile | immediate (provisional work is never resumed) |
| Manager forgot about it | the existing heartbeat reconcile | one heartbeat after the lease |

The last row matters because the worker already stops anything the manager
does not list. Provisional work is **exempt from that sweep for the length of
its lease**. The manager cannot list work it has not recorded yet, so a
missing entry is not a decision. After the lease, the existing reconcile code
stops it. So a bug in this feature makes the sandbox die. It never leaves a
duplicate running.

### Refusal reasons

| Reason | Meaning | Retried inside the lease? |
| --- | --- | --- |
| `instance_claimed` | another allocation already owns this instance id | no (a lost race is final) |
| `no_capacity` | the node no longer fits the placement, as re-validated on the fresh node row | no |
| `ineligible` | constraints/permit fail, or the node is cordoned, draining, or lost | no |
| `expired` | the lease ran out before a verdict arrived | no |
| `ticket_invalid` | bad signature, wrong audience, wrong pool, expired, or a spent `jti` | no |
| `unknown_target` | the job config, task, or instance does not exist | no |
| `disabled` | edge boot is off on this manager | no |
| `internal` | a store error or an election in progress | yes |

A worker that sees a reason it does not recognize kills the work, so version
skew fails closed.

## Enabling it

### Manager

```nickel
{
  edgeBoot = {
    enabled = true,            # default false
    defaultTicketTtl = "5m",
    maxTicketTtl = "1h",
    maxProvisionalPerNode = 32,
  },
  workloadIdentity = { enabled = true },  # boot tickets are signed by this issuer
}
```

`enabled = false` refuses `IssueBootTicket` and any `RatifyClaim` that
carries a ticket. **Manager-side speculative dispatch does not need this
switch.** It adds no new authorization, and a job's own `speculativeStart`
block is enough.

**Enabling edge boot is a security decision.** It lets nodes start work on a
capability. A compromised node can then run anything a live ticket names,
not just what it was given.

### Worker

```nickel
{
  workerType = "edge",
  pool = "edge-eu",
  edgeBoot = {
    acceptClaims = true,   # default false
    jwksRefresh = "5m",
    clockSkew = "30s",
  },
}
```

`acceptClaims` is set per node, separately from the manager's switch. An
operator who turns the feature on cluster-wide still chooses which nodes act
as edges. A node with it off refuses `StartClaim` with `FailedPrecondition`.

## Observability

**Events** (see [`docs/event-stream.md`](event-stream.md)):

| Kind | When |
| --- | --- |
| `alloc.provisional_started` | a node started work before the placement was durable. Carries claim id, gate, origin, and the ticket `jti` for edge starts |
| `alloc.ratified` | a provisional allocation became durable. Carries the allocation id and the latency |
| `alloc.refused` | a ratification was refused. Carries the reason code and detail |
| `boot_ticket.issued` | a ticket was minted. Carries `jti`, scope, and the minting accessor id |

Ticket material never appears in an event, a log line, or a store row. Only
the `jti` does.

**Metrics** (see [`docs/metrics.md`](metrics.md)):

- `legion_provisional_starts_total{origin,gate}`
- `legion_provisional_refused_total{reason}`
- `legion_provisional_killed_total{cause}`: `refused`, `expired`, `failed`
- `legion_provisional_ratify_seconds`: time from admission to verdict
- `legion_provisional_active`: unratified claims held right now, so you can
  see capacity that is started but not yet confirmed

**CLI:**

```console
$ legion node claims node-a -j
$ legion boot-ticket list --job agent-sandboxes
```

`legion node claims` reads live state the manager holds in memory from the
last heartbeat. After a manager failover, it shows nothing until each node's
next heartbeat. The list can be briefly incomplete, but it is never wrong.

## Version skew

`TaskAllocation.provisional` is a new optional field. An old worker would
ignore it and run the allocation as soon as it arrives. With pre-commit
dispatch, that means running it before the commit, with no gate and no
lease. This is the one dangerous skew case, and the dispatcher blocks it.
Speculative dispatch requires a minimum worker protocol version. Below it,
the dispatcher falls back to post-commit dispatch for that node.

`RatifyClaim` is a new RPC, so an old manager rejects it and workers fall
back to their lease, which means they kill the work. Skew fails closed.

Upgrade managers in the usual order ([`docs/upgrades.md`](upgrades.md)).

## Known limitations

- **The JWKS is fetched over the plaintext worker-to-manager channel** unless
  the manager serves `server.tls`. Someone on the path between an edge node
  and its manager could swap the keys that verify boot tickets. Keep that
  path on a trusted network until encrypted worker connections ship
  ([auth-and-acls.md](auth-and-acls.md#the-worker-plane)). The router-to-node
  hop (`StartClaim` on 4848) is TLS, verified against the cluster CA.
- **No island mode.** An unratified start that cannot reach a manager dies
  when its lease expires. Edge boot does not keep work running through a long
  control-plane outage.
- **The `boot` gate is advisory for `container` and `exec` in `host` mode**
  (see [What the gate does and does not stop](#what-the-gate-does-and-does-not-stop)).
  On Linux, `network.mode = "gated"` makes it enforced.
- **The `sandbox` network gate is link state only.** Legion holds the TAP
  device down. It does not install a per-interface default-drop filter. If
  you want a second layer of isolation, add one yourself.
- **No per-destination policy for the gated state.** The gate is all or
  nothing, for the TAP device and for `gated` mode. A task cannot reach a
  package registry, or anything else, before ratification.
- **Gates above `stage` are unavailable to tasks with declared ports, connect
  upstreams, or non-node-local storage** (see [Automatic
  downgrades](#automatic-downgrades)).
- **No edge-side capacity arbitration.** A node admits work against its own
  local free resources and nothing else. It never negotiates, bids, or
  preempts. The manager is still the only scheduler. The node's ledger is
  advisory, and the apply's re-validation has the final say.
- **Manager-side speculative dispatch does not name an instance.** The apply
  decides which keyed instance a placement binds, so a payload sent before
  the apply cannot know it. Keyed starts that need to name their sandbox up
  front should use the edge path.
- **A late-binding fallback loses the head start.** Under [late-binding plan
  apply](late-binding-plan-apply.md), a placement carries ranked candidate
  nodes, and the apply binds the first one that is still valid. Speculative
  dispatch goes to the planner's first choice. If the apply falls back to
  another candidate, the workload starts there through ordinary post-commit
  dispatch, on a node that staged nothing. The claim on the first choice
  expires at its `ratifyTimeout` without running. Nothing is placed twice,
  and only the head start is lost. The `fallbacks` field on the scheduler's
  `Completed scheduler plan_and_apply` log line counts how often this
  happens. If it is high for a job that uses `speculativeStart`, most of the
  speculation is wasted work.
- **No speculative attach for shared storage,** even for drivers that could
  fence it. Drivers cannot yet advertise a fencing capability.
- **Not a replacement for snapshots or fast start.** Edge boot removes
  control-plane latency. Snapshots remove boot latency. A platform that
  wants starts under 100 ms needs both.
