# Edge boot: starting work before the placement is durable

Legion's normal start path is strictly commit-then-run. A submit becomes an
eval, the eval plans, the plan applies — under `persistence.mode = "raft"` that
means proposing on the scheduler lane and waiting for the entry to commit — and
only then is the payload handed to a worker, which builds the root disk and
boots.

For a service that runs one long-lived job that ordering costs nothing. For a
platform spawning a fresh sandbox per user request it is on the critical path
of every request, and it is strictly serial: nothing on the node starts until
every control-plane step has finished.

**Edge boot lets a node reach a configured execution stage before its
allocation is durable, while keeping the durable commit as the single point
that decides who really owns an instance.** It also gives a node a way to
authorize such a start on its own, from a short-lived capability, without
talking to a manager at all.

Everything here is **off by default**, three times over: `speculativeStart` is
absent from every jobspec, `edgeBoot.enabled` is false on the manager, and
`edgeBoot.acceptClaims` is false on every worker. A cluster that upgrades and
changes nothing behaves exactly as it did, including its wire payloads.

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

The manager sends the payload *while* the apply is in flight rather than after
it commits, and delivers the apply outcome as the ratification. This is a
strictly internal reordering: the same manager, the same worker plane, the same
`PostWork` RPC with a `provisional` block on the payload.

It saves the raft commit, the read-model refresh, and the serialization of
dispatch behind them — roughly 5–25 ms on a three-node raft cluster, more when
the scheduler lane is busy. It does **not** save the client→manager round trip
or the eval queue. It needs no new authorization surface, and it is what a job
gets simply by declaring `speculativeStart`.

### Edge-initiated starts

```
platform's own API ──(user auth)──► platform control plane
                                        │  (once, ahead of time or on demand)
                                        ▼
                              manager: legion boot-ticket create
                                        │
                                  signed ticket (JWT, ~minutes)
                                        ▼
   user request ──► edge router ──► legion sandbox start --node <addr>
                                        │ verified offline, admitted locally,
                                        │ booted to the gate level
                                        ▼
                              manager: RatifyClaim ──► store apply ──► verdict
```

The saving is the entire control-plane round trip *including the client's* —
for a cross-region edge, the dominant term — plus independence from
control-plane availability for the length of a ticket's life.

## Gate levels

A gate says how far a node may get on its own. They are ordered, and wherever
two ceilings meet (a task's mode, a ticket's `gate`, a caller's request) **the
lower always wins**. Neither a jobspec nor a ticket can raise the other's limit.

| Mode | The node may, before ratification | It may not, until ratified |
| --- | --- | --- |
| `off` (default) | nothing | everything — today's behavior |
| `stage` | fetch artifacts, pull the image, build the root disk, mount node-local storage | start the workload |
| `boot` | all of `stage`, plus boot the guest and run the workload with its network held shut | open the network, register in service discovery, attach shared storage, obtain any Legion-issued identity |
| `serve` | all of `boot`, plus open the network immediately | obtain any Legion-issued identity; attach shared storage |

- **`stage`** — "pull the image but don't boot till it's the only one."
  Nothing outside the node can observe it. The only cost of losing the race is
  wasted local work.
- **`boot`** — "boot it, but no ingress or egress until it's definitely the
  only one." The workload runs and warms up; nothing it does reaches the
  outside world, so a loser leaves no trace beyond its own disk.
- **`serve`** — "boot all the way and allow networking; if the id turns out to
  be claimed, kill it." Fastest, and **the only mode where a workload that
  loses its race may already have produced externally visible side effects
  before it is killed.** That is the deal. It only happens when two callers
  genuinely race for one instance id, which for an id-affine router is a
  routing bug worth knowing about.

Every mode is safe with respect to Legion's own invariants. The modes differ
only in what an unratified workload can do to the outside world.

## The jobspec surface

```jsonnet
{
  name: 'workspace',
  runtime: 'sandbox',
  group: 'sandboxes',
  stateful: { mode: 'keyed', storage: { path: '/workspace', mode: 'block' } },

  // How far this node may get before the placement is durable.
  speculativeStart: {
    mode: 'boot',           // 'off' (default) | 'stage' | 'boot' | 'serve'
    ratifyTimeout: '5s',    // lease; unratified work is killed at expiry
    onRefused: 'kill',      // 'kill' (default) | 'stop' (graceful, then remove)
  },

  config: { image: 'alpine:latest', vcpus: 2, memory_mib: 1024 },
}
```

**Defaults.** `mode: 'off'`, `ratifyTimeout: '5s'` (capped at `2m`),
`onRefused: 'kill'`. An absent block means `off` and costs nothing.

The block is validated at **submit**, not at dispatch: an unrecognized `mode`
or `onRefused`, or a `ratifyTimeout` of zero or over the cap, fails
`legion job run` with the reason. A typo that was silently read as `off` would
be discovered as a latency regression weeks later, by someone who was not there
when it was written.

```console
$ legion job run api.jsonnet
error: task a: invalid speculativeStart mode "boott" (off|stage|boot|serve)
```

`kill` is the default because the common refusal is "someone else owns this
id", and a loser lingering is precisely what the at-most-one invariant forbids.
`onRefused: 'stop'` runs the task's normal graceful-stop ladder
([`docs/graceful-stop.md`](graceful-stop.md)) instead, for workloads that want
to flush before disappearing.

### Automatic downgrades

Two things force a task down to `stage` no matter what it declares, because
starting it earlier would need something the apply transaction has not produced
yet. Both are reported by `legion job validate` before the deploy:

```console
$ legion job validate api.jsonnet
Jobspec 'api' is valid: 1 task(s) across 1 group(s)
warning: speculativeStart.mode `serve` is downgraded to `stage`: task `api`
declares network ports, which are assigned when the placement commits; gates
above `stage` would boot it without them
```

With `--json` the same lines come back in a `warnings` array on the success
envelope.

- **Declared ports.** `network.ports` and `connect.upstreams` are assigned
  inside the apply, so a workload booted before it cannot know them. A
  `stage`-gated task is unaffected: it adopts the committed payload — real
  allocation id, assigned ports, resolved mounts — before it starts.
- **Storage that cannot be fenced.** A volume a second node could attach
  concurrently is mounted only after ratification. Node-local drivers
  (`local-dir`, `host`, `tmpfs`) are exempt: the only possible racer is on the
  same node, where the worker's claim table already allows one live claim per
  instance.

## What is never speculative

These are invariants, not settings, and they are the reason `serve` is
defensible at all:

- **Legion-issued identity.** Workload identity tokens are scoped to an
  allocation id, which does not exist until ratification. An unratified
  workload therefore cannot authenticate to Legion, to a cloud provider, or to
  anything else that trusts the cluster issuer. A losing sandbox cannot act as
  the winner.
- **Shared writable storage.** Mounted only after ratification (see the
  downgrade rule above).
- **Service discovery.** A provisional allocation is not registered, so no
  other workload can route to something that may vanish.
- **Instance binding.** Nothing about `user-42` changes in the store until
  ratification. `legion instance get user-42` reports the live claim alongside
  the instance — the node holding it, the gate, the stage it has reached, and
  when its lease expires — so "started but not yet confirmed" is a state you
  can see rather than an instance that inexplicably has no allocation.

A provisionally started task also sees **no `LEGION_ALLOC_ID`** — it does not
have one yet, and a placeholder would be a lie. It gets `LEGION_CLAIM_ID`
instead, and `LEGION_INSTANCE_ID` is correct from the first millisecond, which
is the identity a platform actually addresses a sandbox by.

## What the gate does and does not stop

**The `boot` gate is a real network boundary for the `sandbox` runtime, and a
weaker one for host runtimes.** This asymmetry is deliberate and worth stating
plainly:

- **`sandbox` / microVM — enforced.** The guest reaches the outside world
  through exactly one host TAP device (or, in `network.mode: 'none'`, through
  nothing at all). The device is attached at boot — Firecracker cannot hotplug
  a NIC — and held administratively **down**; ratification brings it up. Both
  operations need `CAP_NET_ADMIN`, which `network.mode: 'tap'` already requires
  of the operator.
- **`container` / `exec` — advisory.** Legion withholds what it controls:
  service registration and the node-local upstream forwarders, neither of which
  is created until ratification. But the workload shares the host's network
  stack and **can still reach the network through it.** Host-level egress
  filtering for these runtimes is the operator's job. The worker logs a warning
  every time it starts a `boot`-gated task on such a runtime.

Untrusted code belongs in a microVM, which is the same advice
[`docs/task-runtime-sandbox.md`](task-runtime-sandbox.md) already gives.

## Boot tickets

A boot ticket is a short-lived JWT signed by the cluster's workload-identity
signer and verified by the worker against the published JWKS — the same key
material, rotation, and `kid` discipline that already ships, with a new
consumer.

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

Gated by the ACL resource **`boot`** (`boot:write`, scoped by job name), which
is deliberately separate from `job:write`: a platform's ticket minter should be
able to authorize individual starts without holding the right to redeploy the
fleet. `boot:read` lists outstanding tickets.

A ticket must narrow to an instance or a **non-empty** prefix. A ticket naming
neither would authorize every sandbox of the task, and Legion does not mint
one. A prefix ticket is one mint per user cohort instead of one per request, at
the cost of a wider capability if the node holding it is compromised.

The manager stores only metadata — `jti`, scope, expiry, minting accessor. The
token bytes are never written anywhere.

### Redeeming

```console
$ legion sandbox start agent-sandboxes \
    --id user-42 --node http://10.0.1.7:4848 --ticket "$TICKET"
admitted
booted
ratified
```

The stream reports each transition. A router may begin routing at `booted` and
must be prepared for `refused` at any point until `ratified`. `--dry-run`
verifies the ticket and local admission and reports what *would* happen without
holding or booting anything.

### Verification on the worker is entirely offline

1. signature against a cached JWKS (fetched on first use, refreshed on a `kid`
   miss and on `edgeBoot.jwksRefresh`; **a fetch failure never invalidates
   cached keys**);
2. `iss`, `aud == legion:node`, `exp`/`nbf` with bounded clock skew;
3. `legion.io/pool` against this node's own pool;
4. `legion.io/gate` intersected with the task's `speculativeStart.mode` — the
   lower wins;
5. `jti` against a local single-use set;
6. the local claim table — one live claim per instance id per node;
7. the local resource ledger — the node must genuinely have room.

### Replay and revocation

**Replay across nodes is bounded by ratification, not by verification.** Two
nodes presented the same ticket both boot; the second is refused with
`instance_claimed` and kills its work. Globally-single-use is enforced *at the
manager*, which spends the `jti` in one conditional update — so the second
ratification is refused even if the first has since exited. Nodes are honest,
but they are not trusted to be the only holder.

**Revocation is by TTL only.** The fast path cannot consult a revocation list
without becoming the round trip the ticket exists to avoid. Operators who need
immediate revocation set short TTLs and re-mint; the manager can always refuse
ratification, which caps any damage at the gate level.

### The spec cache

A ticket names a `job_config_id`; the node needs the matching task config to
run anything. A node caches every task config it is *dispatched* — from an
ordinary `PostWork`, from a heartbeat's desired set, or from a provisional
start — so a config the node has already run an allocation of is redeemable
without touching a manager.

A `StartClaim` for a config the node has never seen is refused with
`FailedPrecondition` naming it:

```
this node has no cached spec for job config agent-sandboxes-7; it caches a
config once it has been dispatched an allocation of it, and does not fetch
specs on the start path
```

**A node never fetches a spec on the start path** — doing so would reintroduce
the round trip the ticket exists to avoid. The practical shape is therefore to
keep at least one allocation of the job running in each edge pool, which is
what a warm sandbox fleet looks like anyway. A dedicated pre-warm push is
tracked as future work.

## The kill path

Every failure mode ends in the same place: the node kills the work.

| Trigger | Detected by | Latency |
| --- | --- | --- |
| Ratification refused | the ratify reply (or a heartbeat catch-up verdict) | one RPC |
| Manager unreachable | the lease deadline | `ratifyTimeout` (default 5s) |
| Node loses contact after ratification | the existing self-fence | `stateful.onLost.selfTerminateAfter` |
| Worker restart with claims held | startup reconcile | immediate — provisional work is never resumed |
| Manager forgot about it | the existing heartbeat reconcile | one heartbeat after the lease |

The last row matters: the worker already stops anything the manager does not
list. Provisional work is **exempt from that sweep for the length of its
lease** — the manager cannot list work it has not recorded, so an absence there
is ignorance, not a decision — and after the lease it is stopped by the code
that was already there. The failure mode of a bug in this feature is therefore
"the sandbox dies", never "a duplicate survives".

### Refusal reasons

| Reason | Meaning | Retried inside the lease? |
| --- | --- | --- |
| `instance_claimed` | another allocation already owns this instance id | no — a lost race is final |
| `no_capacity` | the node no longer fits the placement, as re-validated on the fresh node row | no |
| `ineligible` | constraints/permit fail, or the node is cordoned, draining, or lost | no |
| `expired` | the lease ran out before a verdict arrived | no |
| `ticket_invalid` | bad signature, wrong audience, wrong pool, expired, or a spent `jti` | no |
| `unknown_target` | the job config, task, or instance does not exist | no |
| `disabled` | edge boot is off on this manager | no |
| `internal` | a store error or an election in progress | yes |

A worker that sees a reason it does not recognize kills the work — fail-closed
under skew.

## Enabling it

### Manager

```jsonnet
{
  edgeBoot: {
    enabled: true,            // default false
    defaultTicketTtl: '5m',
    maxTicketTtl: '1h',
    maxProvisionalPerNode: 32,
  },
  workloadIdentity: { enabled: true },  // boot tickets are signed by this issuer
}
```

`enabled: false` refuses `IssueBootTicket` and any ticket-bearing
`RatifyClaim`. **Manager-side speculative dispatch does not need this switch**
— it opens no new authorization surface, and a job's own `speculativeStart`
block is enough.

**Enabling edge boot is an explicit security decision.** It grants nodes the
ability to start work on a capability, and it grows a compromised node's blast
radius from "runs what it was given" to "runs anything a live ticket names".

### Worker

```jsonnet
{
  workerType: 'edge',
  pool: 'edge-eu',
  edgeBoot: {
    acceptClaims: true,   // default false
    jwksRefresh: '5m',
    clockSkew: '30s',
  },
}
```

`acceptClaims` is per node and independent of the manager's switch: an operator
who turns the feature on cluster-wide still chooses which nodes are edges. A
node with it off refuses `StartClaim` with `FailedPrecondition`.

## Observability

**Events** (see [`docs/event-stream.md`](event-stream.md)):

| Kind | When |
| --- | --- |
| `alloc.provisional_started` | a node started work before the placement was durable; carries claim id, gate, origin, and the ticket `jti` for edge starts |
| `alloc.ratified` | a provisional allocation became durable; carries the allocation id and the latency |
| `alloc.refused` | a ratification was refused; carries the reason code and detail |
| `boot_ticket.issued` | a ticket was minted; carries `jti`, scope, and the minting accessor id |

No ticket material ever appears in an event, a log line, or a store row — only
the `jti`.

**Metrics** (see [`docs/metrics.md`](metrics.md)):

- `legion_provisional_starts_total{origin,gate}`
- `legion_provisional_refused_total{reason}`
- `legion_provisional_killed_total{cause}` — `refused`, `expired`, `failed`
- `legion_provisional_ratify_seconds` — admission to verdict
- `legion_provisional_active` — unratified claims held right now, so
  started-but-unconfirmed capacity is visible rather than invisible

**CLI:**

```console
$ legion node claims node-a -j
$ legion boot-ticket list --job agent-sandboxes
```

`legion node claims` reads live state the manager holds in memory from the last
heartbeat. After a manager failover it shows nothing until each node's next
heartbeat — briefly incomplete, never wrong.

## Version skew

`TaskAllocation.provisional` is a new optional field. An old worker would
ignore it and run the allocation the moment it arrives — which, with pre-commit
dispatch, means running it *before* the commit with no gate and no lease. That
is the one genuinely dangerous skew case, and it is fenced at the dispatcher:
speculative dispatch requires a minimum worker protocol version and falls back
to post-commit dispatch, per node, below it.

`RatifyClaim` is a new RPC, so an old manager simply rejects it and workers
fall back to their lease — that is, they kill the work. Fail-closed under skew.

Manager upgrade order is the existing policy ([`docs/upgrades.md`](upgrades.md)).

## Known limitations

Stated here rather than left for a bug report:

- **No island mode.** An unratified start that cannot reach a manager dies when
  its lease expires. Surviving a long control-plane outage is a separate design
  with its own consistency argument.
- **The `boot` gate is advisory for `container` and `exec`** (see [What the
  gate does and does not stop](#what-the-gate-does-and-does-not-stop)).
- **The `sandbox` network gate is link state only.** Legion holds the TAP
  device down; it does not install a per-interface default-drop filter.
  Operators who want belt-and-braces isolation should add one themselves.
- **No per-destination policy for the gated state.** The gate is on/off at the
  interface. A sandbox that may reach a package registry but not the internet
  before ratification is a networking feature, not this one.
- **Gates above `stage` are unavailable to tasks with declared ports, connect
  upstreams, or non-node-local storage** (see [Automatic
  downgrades](#automatic-downgrades)).
- **No edge-side capacity arbitration.** A node admits against its own local
  free resources and nothing else. It never negotiates, bids, or preempts. The
  manager remains the only scheduler, and the node's ledger is advisory — the
  apply's re-validation is the authority.
- **Manager-side speculative dispatch does not name an instance.** Which keyed
  instance a placement binds is decided inside the apply, so a payload sent
  before it cannot know. Keyed starts that need to name their sandbox up front
  use the edge path.
- **No speculative attach for shared storage,** even for drivers that could
  fence it. A driver-advertised fencing capability is future work.
- **Not a snapshot/fast-start substitute.** This removes control-plane latency;
  snapshots remove boot latency. A platform wanting sub-100 ms starts needs
  both.
