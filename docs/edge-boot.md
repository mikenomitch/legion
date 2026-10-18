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
- [Ratification under load](#ratification-under-load)
- [Ratification on a raft cluster](#ratification-on-a-raft-cluster)
- [The kill path](#the-kill-path)
- [Overload](#overload)
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

**Defaults.** `mode = "off"`, `ratifyTimeout = "5s"` (at least `1s`, capped at `2m`),
`onRefused = "kill"`. Leaving out the block means `off` and costs nothing.

The block is validated at **submit**, not at dispatch. An unrecognized `mode`
or `onRefused`, or a `ratifyTimeout` under a second or over the cap, fails
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
    --id user-42 --node https://10.0.1.7:4848 --node-name w-17 --ticket "$TICKET"
admitted
booted
ratified
```

`--node` is the node's `workerAddress` (`legion node info` shows it), always
`https://`. The worker plane is always mutual TLS
([auth-and-acls.md](auth-and-acls.md#the-worker-plane)). The CLI verifies the
worker's node certificate against the cluster CA bundle before it sends the
ticket. It fetches the bundle from the manager with your token (`legion net
ca show`, `net:read`) or reads it from `--ca <file>` (save it once with
`legion net ca show > ca.pem`). With `--node-name`, the certificate must name
that node (`w-17.node.legion`); without it, any node certificate of the
cluster is accepted. The router presents no certificate: the ticket is its
credential, and the worker ignores a client certificate on `StartClaim`. A
router of your own must do the same: trust the bundle from `legion net ca
show`, and verify `<node>.node.legion`. Routers must be able to reach 4848
on edge nodes.

The stream reports each transition. A router may start routing at `booted`,
but must handle `refused` at any point until `ratified`. `--dry-run` verifies
the ticket and local admission and reports what would happen, without
holding or booting anything.

### Verification on the worker is entirely offline

1. signature against a cached JWKS (fetched on first use, refreshed on a `kid`
   miss and on `edgeBoot.jwksRefresh`. **A fetch failure never invalidates
   cached keys**). The JWKS is fetched over TLS that verifies the manager's
   cluster certificate (`manager.legion`) against the cluster CA, so an
   on-path attacker cannot swap the keys
   ([auth-and-acls.md](auth-and-acls.md#workers-reach-managers-over-tls)).
   Every manager serves the same keys, so the node asks the managers it
   knows in failover order and takes the first answer
2. `iss` against the cluster issuer, `aud == legion:node`, `exp`/`nbf` with
   bounded clock skew. The node learns the issuer from the managers
   (pre-registration and every heartbeat) rather than assuming it is the
   address in its join token, so a ticket minted on any manager verifies on
   any node
3. `legion.io/pool` against this node's own pool
4. `legion.io/gate` intersected with the task's `speculativeStart.mode` (the
   lower wins)
5. `jti` against a local single-use set
6. the local claim table (one live claim per instance id per node)
7. the local resource ledger (the node must have room)
8. room in the node's claim window, which bounds its unratified edge claims
   and sheds starts when ratification falls behind (see
   [Overload](#overload))

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

### Session tickets

`--scope session` mints a different capability: not one start of one
instance, but any number of [sessions](session-mode.md#starting-at-the-edge)
of one session task, on any node of one pool, until it expires.

```console
$ legion boot-ticket create agents --task workspace --pool agents \
    --scope session --ttl 1h
```

```jsonc
{ "aud": "legion:node", "legion.io/scope": "session",
  "legion.io/job": "agents", "legion.io/task": "workspace",
  "legion.io/pool": "agents", "legion.io/uses": 0 }   // 0: unlimited
```

- It names no instance, prefix, or job config. The node mints each
  session's id and uses the job config of the envelope it holds, whose
  payload carries the session task's spec.
- It names no gate: a session always serves at once. `--gate` and
  `--uses` other than `0` (the default for this scope) are refused, as are
  `--id`, `--id-prefix`, and `--job-config`. The task must be a session
  task (`stateful.mode = "session"`).
- It is presented to `Worker.StartSession`, `Worker.GetSession`, and
  `Worker.StopSession` (`legion session start|get|stop`), never to
  `StartClaim`, and a boot ticket is refused on those three.
- Nothing is spent per session and nothing is ratified: a session has no
  durable record to ratify into. A node verifies a ticket once (signature,
  expiry, pool) and caches the result by a hash of the token for its
  lifetime, so a burst of starts on one ticket costs one signature check
  per node.
- The blast radius of a leaked session ticket is "start sessions of this
  task in this pool until it expires, within the envelopes' room", which is
  what the platform's own picker can already do. Keep TTLs to what a
  picker needs between refreshes, and mint a ticket per picker.
- `legion boot-ticket list` shows `scope = session` for these tickets.
- For a session task with `suspend.durability = "store"`, the same ticket
  moves a session: `StartSession` with `resume_id` (`legion session start
  --resume-id <ID>`) resumes it on the node it is sent to from its
  manifest in the content store. When the manifest says the session runs
  on another node, the receiving node probes that node with the ticket
  (`GetSession`, 2 seconds), asks it to stop its copy (`StopSession`), and
  refuses `UNAVAILABLE` if it does not answer, unless `force_move` is set.
  The ticket must cover the session's job and task. See
  [Moving a session](session-mode.md#moving-a-session).
- An idempotent edge start of a `store` session creates its manifest
  before its gate opens; a second node that derived the same id answers
  `ALREADY_EXISTS` naming the first, instead of starting a duplicate.

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

The cached config includes the task's `speculativeStart` block, which rides
the dispatch payload, so the task's own mode caps the gate (step 4 above)
on every node.

The cached config does not carry the start's identity. Its env names the
instance of the allocation it was cached from, so an edge start removes
`LEGION_INSTANCE_ID`, `LEGION_INSTANCE_MODE`, and `LEGION_INSTANCE_SLOT`
from it and sets the claim's own: the instance id the start names, with
mode `keyed`.

## Ratification under load

Every edge start still costs the manager one ratification, so the rate at
which one manager confirms ratifications is the ceiling on sustained edge
starts.

**What a ratification checks.** Everything is read from committed rows
inside the ratification's own write, so nothing it checks can go stale
between the check and the commit:

- the ticket has a redemption left (the spend is in the same write)
- the named sandbox is not owned by another allocation of this job config,
  and is not stopped, suspended, or destroyed
- the node is still available and still satisfies the task's constraints
  and permit
- the node still has room, from its free capacity rebuilt from the
  allocations committed on it (`reservedResources` and `maxAllocations`
  included)
- static ports, and `distinct` constraints

A ratification does **not** compare the version numbers ("epochs") that
the scheduler's own plans use to detect that they planned against stale
state. A ratification never planned against a snapshot, so there is
nothing for them to detect. Starts of one sandbox job from many nodes,
and many starts from one node, therefore do not refuse each other. They
share writes instead: ratifications that arrive together commit as one
scheduler-lane entry, each in its own savepoint, up to 64 per entry
([sandboxes at scale](sandboxes-at-scale.md)).

**Throughput.** Measured with the real `RatifyClaim` handler in-process
(ticket re-verification, spend, create, bind, event, committed payload):
one job config, 2,000 claims per cell, every claim a new sandbox, a
4-thread manager runtime, and a fixed number of concurrent callers. The
`raft` store is a single-voter cluster. These numbers come from an Apple
M5 laptop (10 cores, 32 GB) shared with other build jobs, so read them as
relative, not as a sizing figure.

| Store | Claims from | Callers | Confirmed/s | p50 ms | p99 ms | Claims per entry |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| local | 256 nodes | 1 | 320 | 2.1 | 21 | 1.0 |
| local | 256 nodes | 64 | 442 | 140 | 285 | 18 |
| local | 256 nodes | 256 | 461 | 536 | 1,013 | 56 |
| local | one node | 256 | 415 | 542 | 986 | 54 |
| raft | 256 nodes | 1 | 174 | 4.8 | 30 | 1.0 |
| raft | 256 nodes | 64 | 357 | 187 | 361 | 29 |
| raft | 256 nodes | 256 | 381 | 587 | 1,161 | 61 |
| raft | one node | 256 | 372 | 686 | 1,117 | 61 |

Every claim confirmed in every cell. Two things follow from the table:

- Entries fill as callers grow, but the confirmed rate barely moves past
  64 callers. The bound is the work each ratification does inside the
  entry, which runs one ratification after another, not the number of
  entries. More callers past that point only add queueing to the verdict
  latency.
- Claims from one node confirm at about the same rate as claims spread
  over 256 nodes, so a dense node does not need its own batching.

These are ceilings for the manager's commit path. They do not include the
network, HTTP/2, or a follower round trip in a multi-voter raft cluster,
and one manager's whole scheduler lane is shared with placements, creates,
and every other instance write. To reproduce them, run the ignored test
`tests/integration/ratify_throughput_measure.rs`:

```console
$ cargo test --release --test integration ratify_throughput_measure:: -- --ignored --nocapture
```

`LEGION_RATIFY_CLAIMS`, `LEGION_RATIFY_CONCURRENCY` (a comma list),
`LEGION_RATIFY_BACKENDS` (`local`, `raft`), and `LEGION_RATIFY_WORKERS`
change the cells.

**What happens past the ceiling.** Ratifications queue for the scheduler
lane, and verdict latency (`legion_provisional_ratify_seconds`) climbs.
A claim with no verdict by its `ratifyTimeout` is killed and its start is
refused as `expired`. Edge nodes shrink their claim window when their
verdicts come back slowly and refuse new starts `OVERLOADED` before boot
(see [Overload](#overload)), so only claims admitted before the slowdown
run into the lease. Watch the p99 of `legion_provisional_ratify_seconds`
against `ratifyTimeout`.

## Ratification on a raft cluster

Under `persistence.type = "raft"`, only the raft leader commits a
ratification. A node sends its ratifications to one manager at a time, and
that manager may be a follower.

- **A follower answers "not the leader".** It does not forward the call
  and does not judge the claim. It answers `FailedPrecondition` with `this
  manager is not the raft leader…`, writes nothing, and leaves the ticket
  unspent.
- **The node asks the leader.** On that answer, or when the call never
  reached the manager (`Unavailable`), the node tries the other managers it
  knows, in order, and keeps sending ratifications to the first one that
  ratifies. It drops that one when it answers not-leader or stops
  answering. A call that timed out is not retried elsewhere, because the
  leader may have ratified it already and a second ratification would be
  refused. The retries count against the claim's `ratifyTimeout`. If every
  manager answers not-leader, as during an election, the node kills the
  claim, and the start is refused `internal` with `ratification call
  failed`.
- **A just-ratified claim survives a stale desired set.** A node stops work
  that its heartbeat's desired set does not list (see [The kill
  path](#the-kill-path)). A follower that has not applied a ratification
  yet, or a leader that built the set just before it, leaves out a claim it
  just confirmed. So a ratified claim stays exempt from that sweep until a
  desired set lists it, or for 30 seconds, whichever comes first. After
  that, an absence means stop again. The cost is that a claim the managers
  want gone right after its ratification can run up to 30 seconds longer.

**Tested on three managers.** On AWS, three raft managers and three workers
raced edge starts of the same keyed instance, a host process on the `exec`
runtime. For each instance id, every worker started it at the same moment
with `legion sandbox start`. In runs of 60, 200, and 300 ids, every id had
exactly one ratified start, and the other two were refused
`instance_claimed`. In the 300-id run the raft leader was stopped 30
seconds in, and a new leader was elected. The run ended with exactly 300
processes running, one per instance id, each with its own
`LEGION_INSTANCE_ID`, and all were still running 80 seconds later. No
losing start's process survived in any run: all 1,120 losers across 560
ids were killed. The setup and the script are in
[RESULTS.md](../benchmarks/aws/sandbox-boot/RESULTS.md#ratification-on-three-managers-2026-10-14).

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
stops it. A claim that was just ratified stays exempt until a desired set
lists it, or for 30 seconds (see [Ratification on a raft
cluster](#ratification-on-a-raft-cluster)). So a bug in this feature makes
the sandbox die. It never leaves a duplicate running.

### Refusal reasons

The node kills the work on every refusal. No reason is retried inside the
lease, so a start refused as `internal` (a store error, or an election that
left no manager to answer as leader) is lost, and the caller starts it
again. A follower's not-leader answer is not a refusal: the node asks the
other managers instead (see [Ratification on a raft
cluster](#ratification-on-a-raft-cluster)).

| Reason | Meaning |
| --- | --- |
| `instance_claimed` | another allocation already owns this instance id (a lost race is final) |
| `no_capacity` | the node no longer fits the placement, as re-validated on the fresh node row |
| `ineligible` | constraints/permit fail, or the node is cordoned, draining, or lost |
| `expired` | the lease ran out before a verdict arrived |
| `ticket_invalid` | bad signature, wrong audience, wrong pool, expired, or a spent `jti` |
| `unknown_target` | the job config, task, or instance does not exist |
| `disabled` | edge boot is off on this manager |
| `internal` | a store error, a failed ratify call, or no manager answered as raft leader |

A worker that sees a reason it does not recognize kills the work, so version
skew fails closed.

## Overload

An edge start boots while its ratification is in flight. If no verdict
arrives within `ratifyTimeout`, the node kills the work. That is the right
answer for one start, but under load it compounds. When the managers fall
behind, ratify latency climbs past the lease, and every start the node admits
boots, waits out its lease, and dies. The router retries it, which adds load
exactly when the managers have none to spare. Throughput of starts that
actually ratify drops toward zero while the nodes stay busy booting work that
is already doomed.

So a node sheds edge starts early instead: before it admits them, boots them,
or spends their tickets.

### The claim window

Each node holds at most a **window** of unratified edge claims. A start that
finds the window full waits up to `edgeBoot.queueTimeout` for a claim to
settle. If none does, the node refuses it with a retryable `OVERLOADED`.

The window adapts to how fast the node's own ratifications come back, the
way TCP adapts to a congested link:

- It starts at, and never exceeds, `edgeBoot.maxProvisionalPerNode`
  (default 32).
- A verdict that arrives within `edgeBoot.overloadThreshold` of its claim's
  lease (default `0.3`, so 1.5s of a 5s lease) is timely. Each timely
  verdict grows the window by `1 / window`, about one slot per window of
  verdicts.
- A verdict that arrives later, a ratify call that fails, or a lease that
  runs out halves the window, never below one. A claim still waiting past
  its threshold counts as late as soon as the node next decides on a start,
  so shedding begins within the threshold rather than a lease later.
- Claims that were already in flight when the window was cut do not cut it
  again. They belong to the slow patch the node already reacted to.

A window of one still admits a start whenever nothing is in flight, so every
node keeps probing. When verdicts come back fast again, the window grows back
on its own. Nothing else has to reset it.

**Claims already admitted are never touched.** The window only decides what
is admitted next. A slow claim runs until its verdict or its lease, exactly
as before.

At a healthy ratify latency of tens of milliseconds, a window of 32 admits
hundreds of starts a second on one node, more than a node can boot. A burst
larger than the window waits for ratifications to drain, typically for a
few milliseconds. The window binds only when ratification slows.

### What the caller sees

The refusal comes back before the stream opens, as `RESOURCE_EXHAUSTED`
tagged with the stable code `OVERLOADED`:

```console
$ legion sandbox start agent-sandboxes --id user-42 --node https://10.0.1.7:4848 \
    --ticket "$TICKET" -j
{
  "ok": false,
  "error": {
    "code": "OVERLOADED",
    "message": "Failed to start sandbox on the edge node: node is overloaded: recent ratifications were slow, so it holds at most 2 unratified edge claims (edgeBoot.maxProvisionalPerNode is 32), and all 2 stayed unratified for 1s",
    "hint": "the managers are behind on ratification, so retry with backoff (other nodes share the same managers)",
    "retryable": true
  }
}
```

- **Nothing was spent.** The ticket is still redeemable, so the retry can
  present the same one.
- **Back off before you retry.** A window below `maxProvisionalPerNode`
  means the managers are behind, and every node in the pool shares them.
  Retrying at once on another node mostly moves the refusal there.
- **A full window at its ceiling is different.** The message then names
  `maxProvisionalPerNode` instead. Ratification is keeping up, but this
  node already holds as many unratified claims as it allows, so another
  node may take the start at once.
- `--dry-run` reports when the window is full, without waiting or holding
  anything.

### What it does not cover

- **Only edge claims count.** A manager-originated speculative dispatch was
  placed by the scheduler, which already checked the node's capacity, and
  it is neither counted nor refused. A session is never ratified, so the
  window never sees it. Sessions and claims both still wait for a launch
  slot (`launchConcurrency`, `queueTimeout`), which bounds the runtime work
  in flight.
- **Each node learns alone.** A node judges ratification by its own
  verdicts. A node that takes few starts learns slowly, and a cluster of N
  edge nodes keeps at least N claims in flight, one probe each.
- **A start can wait twice.** It waits up to `queueTimeout` for room in the
  window and then up to `queueTimeout` for a launch slot.
- **The managers do not shed.** A ratification that reaches a busy manager
  is still processed in order, even after the node gave up on it. The
  window keeps that rare by keeping ratify latency under the threshold, not
  by dropping work at the manager.

## Enabling it

### Manager

```nickel
{
  edgeBoot = {
    enabled = true,            # default false
    defaultTicketTtl = "5m",
    maxTicketTtl = "1h",
  },
  workloadIdentity = { enabled = true },  # boot tickets are signed by this issuer
}
```

Tickets carry the cluster's workload-identity issuer as `iss`. It is one
value for the whole cluster: set `workloadIdentity.issuer` to the same URL on
every manager, or leave it unset for the default, which every manager of a
`raft` or `streaming` cluster shares (`https://manager.legion`) and which is
the advertised address of a single `localDisk` manager
([workload-identity.md](workload-identity.md#the-issuer)). A node that
refuses a ticket with `InvalidIssuer` names the issuer it expected in the
error. That error means managers disagree on an explicit
`workloadIdentity.issuer`.

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
    launchConcurrency = 0,  # 0: twice the node's cores
    queueTimeout = "1s",
    stopCheckpointTimeout = "60s",
    maxProvisionalPerNode = 32,  # unratified edge claims held at once
    overloadThreshold = 0.3,     # fraction of ratifyTimeout, 0 is a static cap
  },
}
```

`acceptClaims` is set per node, separately from the manager's switch. An
operator who turns the feature on cluster-wide still chooses which nodes act
as edges. A node with it off refuses `StartClaim`, `StartSession`, and a
session ticket on `GetSession`/`StopSession` with `FailedPrecondition`, and
does not advertise the `edge-sessions` capability.

`launchConcurrency` bounds how many provisional starts (claims and
[sessions](session-mode.md), whoever sent them) run through the runtime at
once; a start that waits `queueTimeout` for a slot is refused with
`RESOURCE_EXHAUSTED` (retryable) and counted in
`legion_worker_launch_refusals_total`. These two apply whether or not
`acceptClaims` is on. `stopCheckpointTimeout` bounds how long a stopping
worker spends checkpointing sessions whose task sets
`suspend.onWorkerStop = "checkpoint"`.

`maxProvisionalPerNode` and `overloadThreshold` shape the node's claim
window (see [Overload](#overload)). `maxProvisionalPerNode` must be at least
1. `overloadThreshold` is a fraction of each claim's `ratifyTimeout`, from 0
to 1, and `0` turns adaptation off, leaving `maxProvisionalPerNode` as a
static cap. The manager rejects a worker config with values outside those
ranges at startup. Size `maxProvisionalPerNode` to the unratified work you
are willing to see killed at once if the managers become unreachable: every
unratified claim is a booted workload that dies at its lease.

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
- `legion_worker_claim_window`: the node's claim window right now. Below
  `maxProvisionalPerNode` means the node has seen slow ratifications
- `legion_worker_claim_overload_refusals_total`: edge starts refused
  `OVERLOADED`

A node logs `Ratification is slow, shrinking the edge claim window` at
`info` when it cuts the window, at most once per slow patch, and `Edge claim
window is back at its ceiling` when it has fully recovered. Individual
refusals log at `debug` only, because under overload there are many.

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

`TaskAllocation.task_speculative_start` carries a task's `speculativeStart`
block to the node's spec cache. A manager that does not send it, or a worker
that does not read it, leaves every cached task at `off`. The node then
refuses each ticket for it, which also fails closed:

```text
task box does not permit speculative starts (speculativeStart.mode is `off`,
or was downgraded by an invariant)
```

Upgrade managers in the usual order ([`docs/upgrades.md`](upgrades.md)).

## Known limitations

- **Managers must agree on an explicit issuer.** Legion cannot check that
  every manager was given the same `workloadIdentity.issuer`. A manager with
  a different value mints tickets that nodes refuse with `InvalidIssuer`,
  and nodes follow whichever manager answered their latest heartbeat.
- **A ratification that timed out is not retried on another manager.** The
  leader may have ratified it, so the node kills the claim instead
  (`ratification call failed`), and the caller starts it again.
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
