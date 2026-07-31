# Plan: Edge Boot — Provisional Allocations, Gated Starts, and Boot Tickets

> **Judgement calls:** [`calls/edge-boot-plan_calls.md`](../calls/edge-boot-plan_calls.md)

**Status: PLANNED.** Nothing here is built. Today every allocation reaches a
node *after* it is durable in the store (§5), and there is no option — one-shot,
single-topology, or otherwise — that starts work before the commit.

Deliberately out of v1 and named as such: island mode (unratified work
outliving a manager outage), per-destination network policy for the gated
state, speculative attach of *shared* writable volumes, and edge-side capacity
arbitration between tenants (§4 Non-Goals).

## Problem Statement

Legion's start path is strictly commit-then-run. A submit becomes an eval, the
eval plans, `plan_and_apply_*` submits the plan, the store applies it — which
under `persistence.mode = "raft"` means proposing on the scheduler lane and
waiting for the entry to commit — and only then does
`dispatch_allocations` (`src/structs/eval.rs:309`,
`src/manager/dispatcher.rs:11`) open a connection to the worker and hand it the
payload. The worker builds the root disk and boots.

For a service that runs one long-lived job this ordering costs nothing. For the
workload Legion is courting — a sandbox platform spawning a fresh microVM per
user request — it is on the critical path of every request:

| Step | Where | Typical cost |
| --- | --- | --- |
| client → manager RPC | user's edge → manager region | 1–150 ms (cross-region: the whole budget) |
| `RunJob`/`CreateInstance` write + eval enqueue | control lane | ~1 ms local, 5–20 ms raft |
| eval claim + plan | eval worker pool | 1–20 ms, more under contention |
| plan apply | scheduler lane | ~1 ms local, 5–20 ms raft |
| dispatch to worker | manager → node | 0.5–5 ms, serialized per allocation |
| **boot** | node | 50–3000 ms |

Two things make this worse than the table suggests. First, the sequence is
strictly serial: nothing on the node — not the image pull, not the root-disk
build, not the kernel boot — starts until every control-plane step has
finished. On a cold image the node spends hundreds of milliseconds doing work
it could have started at millisecond zero. Second, the control plane is a hard
dependency for a start: a saturated eval pool, a raft election, or a brief
partition between the edge and the manager region turns "boot a sandbox" into
"fail to boot a sandbox", even though the node itself is healthy and has room.

What a platform builder has to do today: keep a warm pool of pre-created
instances large enough to absorb both demand spikes *and* control-plane
latency, and eat the cost of idle VMs. That is a workaround for the scheduler,
not a product decision.

The other half of the problem is authorization. A platform's end users do not
hold Legion credentials — their requests arrive at an edge node with the
platform's own auth. Today the only way for a node to start work is for the
manager to tell it to (`PostWork` is worker-plane, called by the manager,
`proto/legion.proto:4136`), so every start requires a client with `job:write`
or `instance:write` talking to a manager. There is no way to hand an edge node
a narrow, verifiable "you may start exactly this one sandbox" capability.

**This plan makes the node able to start work before the placement is durable,
while keeping the durable commit as the single point that decides who really
owns an instance — and gives the node a way to authorize such a start on its
own.**

## Prior Art

- **Kubernetes scheduler "assume" cache.** kube-scheduler writes the chosen
  node into its in-memory cache and moves on to the next pod *before* the
  binding round-trips through the apiserver; if the bind fails the assumption
  is rolled back ("forget"). The kubelet, however, is still told only after
  the write lands.
  - **Take:** the framing that the assumption and the durable record are two
    events, and the rollback path is a first-class part of the design.
  - **Reject:** keeping the optimism entirely inside the scheduler. That saves
    scheduler throughput, not start latency, which is the thing that hurts here.
- **Kubelet static pods.** A kubelet runs pods from local manifests with no
  apiserver involvement at all; the apiserver later gets a read-only mirror
  pod. This is the only mainstream precedent for "the node decides".
  - **Take:** the node can be trusted to start work, and the control plane can
    learn about it afterwards.
  - **Reject:** the "never reconciled, never fenced" part. A static pod is
    outside the scheduler's world forever; edge boot must converge to exactly
    one owner and kill the loser.
- **Sparrow** (Ousterhout et al., SOSP 2013) — late binding: work binds to
  whichever probed worker frees first, not to a scheduling-time choice.
  - **Take:** the binding point can move later than the decision point.
  - **Note:** Legion's [late-binding plan](late-binding-plan-apply-plan.md)
    already moves *node choice* to commit time. This plan moves *execution*
    earlier than commit. They compose: a ratification is a plan submission
    whose candidate list has length one.
- **Omega** (Schwarzkopf et al., EuroSys 2013) — optimistic concurrency over
  shared state, where a rejected transaction is replanned.
  - **Take:** optimism is the right default and the conflict path must be
    cheap and explicit. Legion already implements this at the apply
    (`src/storage/store.rs:2552`).
- **Nomad** — allocations are written to raft by the plan applier and pulled by
  the client on its next `Node.GetClientAllocs`. No pre-commit execution.
- **Firecracker/microVM platforms** generally solve the same latency problem
  with warm pools and snapshot restore rather than with pre-commit execution.
  Legion's own
  [snapshot and fast-start plan](sandbox-snapshots-and-fast-start-plan.md)
  attacks the boot half; this plan attacks the control-plane half, and they
  are additive — a restore that takes 50 ms is dominated by a 100 ms
  control-plane round trip until this lands.
- **Lease-based fencing** (Chubby, ZooKeeper, and Legion's own node-loss
  fencing, `docs/stateful-workloads.md` § "Node loss and fencing") — a
  participant that loses contact kills itself before anyone can replace it.
  - **Take:** this is exactly the mechanism an unratified start needs, and
    Legion already ships the worker half of it
    (`self_terminate_after_seconds`, `src/manager/dispatcher.rs:150`; the
    watchdog in `src/worker/daemon.rs:496`).

Nobody in this list lets a node start a workload optimistically *and* converge
it to a single global owner. That combination is what this plan adds, and the
gate levels (§6.2) are what make it safe enough to default anything on.

## Jobs To Be Done

**Platform builder (the sandbox service)**
- When a user's request hits my edge, I want their sandbox to start in the
  time it takes to boot a VM, not in the time it takes to boot a VM plus a
  round trip to another region, so my p99 is a machine number and not a
  network number.
- When two of my regions race to start the same user's sandbox, I want exactly
  one of them to survive and the other to disappear on its own, so I never
  have to write reconciliation logic for my own duplicates.
- When I know a sandbox is about to be needed, I want to hand the edge a
  ticket ahead of time, so the boot decision costs zero control-plane calls.
- When my control plane is briefly unreachable, I want already-authorized
  starts to keep working, so a manager hiccup is not a user-visible outage.

**Security / platform admin**
- When I let an edge node start workloads, I want the thing it holds to be
  narrow — one job, one sandbox id, one pool, expiring in minutes — so a
  compromised edge node cannot deploy anything else.
- When untrusted user code runs in a sandbox that might lose its race, I want
  it to have no cluster credentials and no network until it is confirmed, so a
  loser cannot act as the winner.
- When I audit who started what, I want the ticket, the node, and the
  ratification verdict in the event log, so an edge start is at least as
  traceable as a `job run`.

**Cluster operator**
- When nodes start work before the manager has recorded it, I want the node's
  view to converge to the manager's within a bounded, configurable window, so
  "what is running" is still a question with one answer.
- When I look at capacity, I want work that has been started but not yet
  confirmed to be visible, so I am not surprised by usage I cannot see.

**On-call responder**
- When sandboxes are dying seconds after starting, I want to see *why* the
  ratification was refused — id taken, no capacity, expired ticket — so I know
  whether to look at my routing or my cluster.

**Autonomous agent**
- When I start a sandbox and get an id back, I want to know whether it is
  confirmed yet or still provisional, so I do not build on state that may be
  killed.

*Not in scope:* the end user of the sandbox platform. They never see Legion;
their experience of this feature is that starts are faster.

### User scenarios

1. **Warm edge start.** A request arrives at a node in `eu-central` holding a
   pre-minted ticket for job `agent-sandboxes`, instance `user-42`. The node
   verifies the ticket offline, boots the microVM with its network gated, and
   in parallel asks the manager to ratify. Ratification lands 40 ms later; the
   link comes up; the user's first byte is served in ~120 ms instead of ~260 ms.
2. **Lost race.** Two edge regions both receive traffic for `user-42` and both
   boot. The first ratification binds the instance; the second is refused with
   `InstanceClaimed`, and that node kills its VM before it ever had a network
   (gate level `boot`). The platform's router sees one healthy sandbox and no
   duplicate-state bug.
3. **Prepared, not booted.** A cautious operator runs a job with
   `speculativeStart.mode: 'stage'`. Nodes pull the image and build the root
   disk the moment they are told to, but nothing executes until the allocation
   is committed. Time-to-boot drops by the image-pull time with no change to
   what can be observed by anyone outside the node.
4. **Nothing configured.** An operator upgrades and touches nothing. Every job
   behaves exactly as today: dispatch happens after the commit, and no node
   accepts an edge start (the worker listener rejects claims unless the feature
   is enabled).

## Goals

- A node can reach a configured execution stage — staged, booted, or serving —
  **before** its allocation is durable, and the furthest stage is declared per
  task, not globally.
- **The durable commit remains the only authority on ownership.** An
  unratified start binds nothing: no instance id, no volume it can corrupt, no
  service registration, no workload identity.
- **A loser always dies, without operator involvement**, bounded by a
  configurable lease, on every failure mode — refusal, timeout, partition, or
  worker restart. Fail-closed is the default and the only default.
- A node can authorize a start **without contacting the manager**, using a
  capability that is narrow (one job config, one instance, one pool), short
  lived, offline-verifiable, and auditable.
- Nothing changes for jobs that do not opt in, byte for byte, including the
  wire payloads (`speculativeStart` absent ⇒ no new fields on the dispatch).
- The at-most-one-live-allocation invariant for keyed instances
  (`assign_stateful_instance_tx`, `src/storage/store.rs:7488`) holds unmodified,
  with its existing tests unchanged and green.

## Non-Goals

- **No island mode in v1.** An unratified start that cannot reach a manager
  dies when its lease expires. Surviving a long control-plane outage is a
  separate plan with its own consistency argument (§11).
- **No edge-side capacity arbitration.** A node admits against its own local
  free resources and nothing else. It never negotiates with other nodes, never
  bids, and never preempts. The manager remains the only scheduler.
- **No speculative attach of shared writable storage.** Volumes that a second
  node could attach concurrently mount only after ratification (§6.5). Node-
  local volumes — the `local-dir` default, already pinned by the volume plan —
  are the exception, because the only possible racer is on the same node and
  the worker fences it locally.
- **No fine-grained network policy for the gated state.** The gate is on/off
  at the interface. Per-destination allowlists (a sandbox that may reach the
  package registry but not the internet before ratification) are a networking
  feature, not this one.
- **Not a snapshot/fast-start substitute.** This removes control-plane latency;
  [snapshots](sandbox-snapshots-and-fast-start-plan.md) remove boot latency.
  A platform wanting sub-100 ms starts needs both.
- **No new placement intelligence at the edge.** A ticket says *what* may run
  and *where it may run*; picking among eligible nodes is the caller's job
  (the platform's router already knows which POP the user hit).

## Background — What Exists Today

- **Dispatch is post-commit and synchronous.** `Eval::schedule_job_allocations`
  plans, applies, then dispatches (`src/structs/eval.rs:299-311`).
  `dispatch_allocations` (`src/manager/dispatcher.rs:11`) resolves task config,
  node address, instance, and volume mounts from the store, builds a
  `TaskAllocation` (`:124`) and calls `PostWork` per allocation over a lazy
  channel. There is no pre-commit path and no batching.
- **Apply is the fencing point.** `apply_plan_submission_apply`
  (`src/storage/store.rs:2552`) compares the version vector, re-validates
  availability/constraints/permit on the fresh node row, assigns networks,
  inserts allocations, and bumps node and job-config epochs. Stateful instance
  binding — the at-most-one-live-allocation guarantee — happens inside that
  transaction (`assign_stateful_instance_tx`, `:7488`). Under raft this is a
  replicated scheduler-lane op, so "applied" means "committed".
- **The worker reconciles against the manager's desired set.**
  `TaskManager::reconcile_allocations` (`src/worker/task_manager.rs:762`) stops
  every local allocation whose id is not in the heartbeat reply's
  `desired_allocations` and starts every desired id it does not have. **An
  allocation the manager does not know about is killed on the next heartbeat.**
  That is exactly the kill-the-loser behavior this plan needs — the work is to
  *protect* a provisional start for the length of its lease, not to build a
  reaper.
- **The worker already self-fences.** Stateful tasks carry
  `self_terminate_after_seconds` (`src/manager/dispatcher.rs:150`) and a
  watchdog kills them when contact with the manager lapses
  (`src/worker/daemon.rs:293,496`). A provisional start is the same mechanism
  with a much shorter clock.
- **Task runtimes already split prepare from start.** `TaskPlugin::prepare`
  (`src/task_plugins/mod.rs:389`) and `TaskPlugin::start` (`:398`) are separate
  calls, and the worker already runs prepare before start
  (`src/worker/daemon.rs:703-729`). The `stage` gate is a pause between two
  calls that already exist. The `sandbox` runtime does its expensive work —
  OCI layout, root-disk build, `mkfs.ext4 -d` — in that prepare half
  (`docs/task-runtime-sandbox.md` § "How a microVM Boots").
- **Workers hold a per-node credential, not a user credential.**
  Registration verifies a join secret (`verify_join_for_registration`,
  `src/manager/server.rs:1410`) and mints a node token used on
  `Heartbeat`/`HealthCheck`; node tokens grant nothing on the user-facing API
  (`docs/auth-and-acls.md` § "Worker nodes"). Workers have no way to verify a
  *caller's* credential.
- **The manager already runs an OIDC signer and publishes a JWKS.**
  `src/manager/workload_identity.rs` signs task and node identity tokens with
  rotation, and `src/manager/oidc_http.rs:93` serves
  `/.well-known/jwks.json`. Workers fetch identity tokens from the manager
  (`src/worker/identity.rs`) but **do not** verify anything themselves — there
  is no JWKS client on the worker today.
- **Idempotency records exist** (`Store::idempotency_lookup` /
  `idempotency_record`, `src/storage/store.rs:11932`) and are the mechanism
  RunJob/StopJob/DestroyJob use for retry safety. Single-use tickets ride them.
- **Sandbox networking is minimal.** `mode: 'none'` (loopback only) is the
  default; `mode: 'tap'` attaches an operator-precreated device
  (`docs/task-runtime-sandbox.md` § Networking). There is no port mapping yet.
  The network gate (§6.7) therefore has very little existing surface to gate —
  which makes it cheap to build now and important to design before the datapath
  grows.

## Proposed Design

### 6.1 One new concept: the provisional allocation

A **provisional allocation** is work a node has started that the store has not
yet recorded. It has:

- a **claim id** — `clm_<uuid>`, minted by whoever asked for the start;
- the **job config id** and task name it runs (an exact version, never "latest");
- an **instance id** when the task is stateful (the sandbox case) — this is the
  identity the race is actually about;
- the **gate level** it is allowed to reach before ratification (§6.2);
- a **lease deadline** — after which the node kills it, unconditionally;
- local **resource reservations**, held in a new worker-local ledger so a node
  cannot double-admit itself into oversubscription.

Its lifecycle has exactly one linearization point:

```
   claimed ──► staged ──► booted ──► serving        (node-local progress)
      │           │          │
      └───────────┴──────────┴──► RATIFY (store apply, raft-committed)
                                    │
                        ┌───────────┴────────────┐
                        ▼                        ▼
                    confirmed                 refused
              (becomes a normal            (node kills it,
               allocation, id bound)        emits the reason)
```

**Ratification is not a new consistency mechanism.** It is the existing plan
apply with the node already chosen: a plan submission with one placement and a
one-element candidate list, applied through `apply_plan_submission_apply`
(`src/storage/store.rs:2552`) with its epoch checks, its constraint
re-validation, and — critically — its `assign_stateful_instance_tx` call. If
another allocation already owns `user-42`, this apply fails exactly the way it
fails today, and that failure *is* the refusal. No new invariant, no second
code path for correctness, and no weakening of the guarantee the stateful plan
exists to protect.

Two starts racing for the same instance therefore resolve the same way two
concurrent plans resolve today: the store serializes them, the first binds, the
second is told no.

### 6.2 Gate levels: how far the node may get on its own

One task-level block, absent by default:

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

| Mode | The node may, before ratification | It may not, until ratified |
| --- | --- | --- |
| `off` | nothing | everything (today's behavior) |
| `stage` | fetch artifacts, pull the image, build the root disk, provision (not attach) node-local volumes | start the workload |
| `boot` | all of `stage`, plus boot the guest and run the workload with its network gated shut | open the network, register in service discovery, attach shared storage, obtain any Legion-issued identity |
| `serve` | all of `boot`, plus open the network immediately | obtain any Legion-issued identity; attach shared storage |

These are the three postures asked for, in order of increasing risk:

- **`stage`** — "pull the image but don't boot till it's the only one." Nothing
  outside the node can observe it. The only cost of losing is wasted local work.
- **`boot`** — "boot it, but no ingress or egress until it's definitely the
  only one." The workload runs and warms up; nothing it does can reach the
  outside world, so a loser leaves no trace beyond its own disk.
- **`serve`** — "boot all the way and allow networking; if the id turns out to
  be claimed, kill it." Fastest, and the only mode where a loser can have
  emitted externally visible side effects. That is the deal, stated in the
  public doc in exactly those words.

`serve` is the only mode that can produce a user-visible anomaly, and it does
so only when two callers genuinely race for one instance id — which, for a
platform whose router is id-affine, is a routing bug it wanted to know about
anyway. Every mode is safe with respect to *Legion's* invariants; the modes
differ only in what an unratified workload can do to the outside world.

### 6.3 Two ways a provisional allocation starts

**(a) Manager-side speculative dispatch** — no new authorization surface.

The manager dispatches the payload *while* the apply is in flight rather than
after it commits: `dispatch_allocations` is called with the provisional payload
before the plan submission is proposed, and the apply outcome is delivered as
the ratification. This is a strictly internal reordering — the same manager,
the same worker plane, the same `PostWork` RPC with a `provisional` block on
the payload.

It saves the raft commit, the read-model refresh, and the serialization of
dispatch behind them: roughly 5–25 ms on a three-node raft cluster, more when
the scheduler lane is busy. It does **not** save the client→manager round trip
or the eval queue. It is the right default for a job that wants faster starts
without changing anything about how it is submitted, and it is where the whole
state machine, the gates, the lease, and the kill path get built and tested.

**(b) Edge-initiated start** — the node decides, using a ticket.

```
platform's own API ──(user auth)──► platform control plane
                                        │  (once, ahead of time or on demand)
                                        ▼
                              manager: IssueBootTicket   ── ACL: boot:write
                                        │
                                  signed ticket (JWT, ~minutes)
                                        ▼
   user request ──► edge router ──► worker: StartClaim(ticket, instance id)
                                        │ verifies offline, admits locally,
                                        │ boots to the gate level
                                        ▼
                              manager: RatifyClaim  ──► store apply ──► verdict
```

The saving is the entire control-plane round trip *including the client's* —
which for a cross-region edge is the dominant term — plus independence from
control-plane availability for the length of a ticket's life. This is what
makes edge-level boot decisions real rather than a latency micro-optimization.

`StartClaim` is server-streaming so the caller learns each transition without
polling:

```
→ StartClaim { ticket, instance_id: "user-42", claim_id: "clm_9f2c", gate: BOOT }
← { stage: "staged",   claim_id: "clm_9f2c" }
← { stage: "booted",   claim_id: "clm_9f2c" }          # caller may route here
← { stage: "ratified", allocation_id: 8123 }           # network opens
```

or, on the losing side:

```
← { stage: "refused", reason: "INSTANCE_CLAIMED", detail: "bound to node-a" }
```

The CLI wrapper is `legion sandbox start <job> --id user-42 --node <addr>
--ticket <t>` with `--json`, an entry in `catalog()`
(`src/cmd/capabilities.rs`), and an MCP tool marked `mutates: true` — the
agent-readiness rules apply to this surface like any other.

### 6.4 Boot tickets: how a node authorizes a start without the manager

A **boot ticket** is a short-lived JWT signed by the manager's existing
workload-identity signer (`src/manager/workload_identity.rs`) and verified by
the worker against the cluster's JWKS — the same key material, rotation, and
`kid` discipline that already ships, with a new consumer.

```jsonc
{
  "iss": "https://manager.example.com:4747",
  "aud": "legion:node",                    // reserved audience; not a task audience
  "sub": "boot:agent-sandboxes/workspace",
  "exp": 1770000300, "iat": 1770000000, "jti": "tkt_5c1a…",
  "legion.io/scope": "boot",
  "legion.io/job": "agent-sandboxes",
  "legion.io/job_config": "jc-7f3a91",     // an exact version, never "latest"
  "legion.io/task": "workspace",
  "legion.io/instance": "user-42",         // or "legion.io/instance_prefix": "user-"
  "legion.io/pool": "edge-eu",             // where it may run
  "legion.io/region": "eu-central",
  "legion.io/gate": "boot",                // ceiling; the task's own mode also caps it
  "legion.io/uses": 1                      // single-use by default
}
```

**Minting** is `IssueBootTicket`, gated by a new ACL resource `boot`
(`boot:write`, scoped by job name), following the `scale` precedent: a platform's
ticket minter should be able to authorize starts without holding `job:write`,
because `job:write` means "redeploy the fleet". `boot:read` lists outstanding
tickets. Both go in `tests/acl_endpoints.rs` (`endpoint_table` + a request
builder) — the classification test fails the build otherwise.

**Verification on the worker** is entirely offline:

1. signature against a cached JWKS (fetched at registration, refreshed on a
   `kid` miss and on a TTL; a fetch failure does not invalidate cached keys);
2. `iss`, `aud == legion:node`, `exp`/`nbf` with bounded clock skew;
3. `legion.io/pool` and `legion.io/region` against this node's own attributes;
4. `legion.io/gate` intersected with the task's `speculativeStart.mode` — the
   **lower** of the two wins, always;
5. `jti` against a local single-use set (bounded, expires with the ticket);
6. the local claim table — one live claim per instance id per node;
7. the local resource ledger — the node must genuinely have room.

**Replay across nodes** is bounded by ratification, not by verification: two
nodes presented the same ticket both boot, and the second is refused
(`InstanceClaimed`) and killed. Globally-single-use is enforced *at the
manager* by recording `jti` through the existing idempotency table
(`Store::idempotency_record`, `src/storage/store.rs:11954`) when
`legion.io/uses = 1` — the second ratification for that `jti` is refused even
if the first has since exited. Nodes are honest but not trusted to be the only
holder.

**Revocation is by TTL only**, and the public doc says so. A ticket is a
capability with a minutes-long life; the fast path cannot consult a revocation
list without becoming the round trip it exists to avoid. Operators who need
immediate revocation set short TTLs and re-mint; the manager can always refuse
ratification, which caps any damage at the gate level.

**The job config must be on the node.** A ticket names a `job_config_id`; the
worker needs the matching task config to run anything. Workers gain a small
**spec cache**, populated by (a) any allocation they have run for that config,
(b) an explicit `legion job prewarm --pool edge-eu` push, and (c) a fallback
fetch from the manager on a miss. A cache miss costs one manager round trip and
is still faster than the full path; a cache hit is what makes the edge start
control-plane-free. Pre-warming the spec alongside the image is the intended
production shape, and it composes with the
[artifact cache and pre-warm plan](artifact-cache-and-prewarm-plan.md).

### 6.5 What is never speculative, at any gate level

These are invariants, not settings, and they are the reason `serve` is
defensible at all:

- **Legion-issued identity.** Workload identity tokens
  (`IssueWorkloadIdentityToken`) are scoped to an allocation id, which does not
  exist until ratification. An unratified workload therefore cannot
  authenticate to Legion, to AWS, or to anything else that trusts the cluster
  issuer — it falls out of the existing design rather than being bolted on. A
  losing sandbox cannot act as the winner.
- **Shared writable storage.** A volume that a second node could attach
  concurrently is mounted only after ratification. Node-local volumes are
  exempt because the only possible racer is on the same node, where the
  worker's claim table already fences it; a driver may opt in by advertising a
  fencing capability in the volume contract (`capabilities`, see the
  [volume plugin plan](volume-plugin-system-plan.md)). A task whose storage
  cannot be fenced and whose mode is `boot`/`serve` is **downgraded to `stage`
  at admission, with a warning on `job validate`** — visible before the deploy,
  never silently.
- **Service discovery.** A provisional allocation is not registered, so no
  other workload can route to something that may vanish. Registration is part
  of opening the network (§6.7).
- **Instance binding.** Nothing about `user-42` changes in the store until
  ratification. `legion instance get user-42` reports `provisional` with the
  node and the claim id — visible, but not bound.

### 6.6 The kill path

Every failure mode ends in the same place: the node kills the work.

| Trigger | Detected by | Latency |
| --- | --- | --- |
| Ratification refused | the ratify reply | one RPC |
| Manager unreachable | the lease deadline | `ratifyTimeout` (default 5s) |
| Node loses contact after ratification | existing self-fence | `selfTerminateAfter` |
| Worker restart with claims on disk | startup reconcile | immediate — provisional work is never resumed |
| Manager forgot about it | existing heartbeat reconcile | one heartbeat |

The last row is the important one: `reconcile_allocations`
(`src/worker/task_manager.rs:762`) already stops anything the manager does not
list. The change is to give provisional work a **protection window** — it is
exempt from reconcile-driven stops until its lease expires, and after that it
is stopped by the existing code with no new reaper. The failure mode of a bug
in this plan is therefore "the sandbox dies", never "a duplicate survives".

`onRefused: 'stop'` runs the task's normal graceful-stop ladder
(`docs/graceful-stop.md`) instead of a hard kill, for workloads that want to
flush before disappearing. `kill` is the default because the common refusal is
"someone else owns this id", and a loser lingering is precisely what the
invariant forbids.

### 6.7 Opening the network

At gate level `boot` the guest boots with its interface present but shut, and
ratification opens it. Mechanically, per runtime:

- **`sandbox` / microVM.** The TAP device is attached at boot (Firecracker
  cannot hotplug a NIC, so attaching later is not an option) and held **down**
  on the host, plus a default-drop filter on the interface for platforms where
  link state alone is not enough. Ratification brings it up and drops the
  filter. Both operations need `CAP_NET_ADMIN`, which `mode: 'tap'` already
  requires of the operator.
- **`container` / `exec`.** The gate is the loopback upstream listeners
  (`src/worker/forwarder.rs`) plus service registration: neither is created
  until ratification. Host-level egress filtering for these runtimes is left to
  the operator and documented as a gap — a `boot`-gated `exec` task can still
  reach the network through the host's own stack.

That asymmetry is honest and worth stating loudly in the public doc: **the
`boot` gate is a real network boundary for `sandbox`, and a weaker one for host
runtimes.** Untrusted code belongs in a microVM anyway, which is the same
advice `docs/task-runtime-sandbox.md` already gives.

### 6.8 Observability

- **Events:** `alloc.provisional_started` (claim id, gate, origin
  `manager|edge`, ticket `jti` for edge starts), `alloc.ratified` (allocation
  id, latency), `alloc.refused` (reason code), `alloc.gate_opened`.
- **Metrics:** `legion_provisional_starts_total{origin,gate}`,
  `legion_provisional_ratify_seconds` (histogram),
  `legion_provisional_refused_total{reason}`,
  `legion_provisional_killed_total{cause}`, and
  `legion_provisional_active` (gauge, so unratified capacity is visible).
- **CLI:** `legion instance get` reports `provisional` state with node and
  claim; `legion node claims <node> -j` lists a node's live claims;
  `legion job validate` reports gate downgrades (§6.5).
- **Spans:** `claim_id`, `instance_id`, `job_config_id`, `node_id`, `gate` on
  the claim, ratify, and gate-open boundaries — never the ticket itself, and
  never a ticket secret in a log line (`jti` only).

## Proto, Struct, Config, and Persistence Changes

**Proto** (all documented with public-facing comments, per the docs rule):
- `Manager.IssueBootTicket` / `ListBootTickets` — mint and inspect tickets.
- `Manager.RatifyClaim` — worker-plane; carries the claim, the ticket's `jti`,
  and the node's own credential; returns `confirmed { allocation_id }` or
  `refused { reason }`.
- `Worker.StartClaim` (server-streaming) — the edge entry point.
- `TaskAllocation` gains a `provisional` sub-message (claim id, gate, lease
  deadline). Absent for non-speculative jobs, so today's payloads are unchanged
  on the wire.
- `HeartbeatRequest` gains `provisional_claims` so the manager can see
  unratified work; `HeartbeatReply` gains `claim_verdicts` as a catch-up
  channel when a `RatifyClaim` reply was lost.

**Job config:** `speculativeStart { mode, ratifyTimeout, onRefused }` on the
task, with `mode: 'off'` the default and the whole block optional. Added to
`configs/jobs/maximal.jsonnet` and a new
`configs/jobs/sandbox-edge-boot.jsonnet` demo.

**Manager config:** `edgeBoot { enabled: false, defaultTicketTtl: '5m',
maxTicketTtl: '1h', maxProvisionalPerNode: 32 }`. Off by default — this opens a
new authorization surface and must be an explicit operator decision.

**Worker config:** `edgeBoot { acceptClaims: false, jwksRefresh: '5m',
clockSkew: '30s' }`, plus the listener the `StartClaim` RPC is served on
(reusing the existing worker server, `src/worker/server.rs`).

**ACL:** new resource `boot` in `AclResource` (`src/structs/acl.rs:19`) with
`read`/`write`, scoped by job name; `ListBootTickets`/`IssueBootTicket` entries
in `tests/acl_endpoints.rs`; `RatifyClaim` classified worker-plane via
`acl::enforce_worker_plane`. Documented in `docs/auth-and-acls.md`.

**Store:** a `boot_ticket` table (`jti` PK, job, task, job_config_id, instance
or prefix, pool, gate, uses, expires_at, accessor id, created_at) for audit and
single-use enforcement — rows expire with the ticket. No change to the
`allocation` table: a ratified claim is an ordinary allocation, minted by the
existing insert path. **Allocation ids stay manager-minted**; the edge
addresses a sandbox by its instance id (`user-42`), which is stable from the
first millisecond and is the identity the platform already uses.

**Worker (no persistence):** a `ClaimTable` (claim id → gate, lease, resources,
ticket `jti`) and a `LocalLedger` seeded from the node's declared resources and
its live allocations. Both are in-memory: a worker restart kills provisional
work by design (§6.6).

## Implementation Phases

Each phase ships and stands alone. The ordering follows mechanism cost, not
risk appetite — `stage` needs the least new machinery, `boot` the most.

- **Phase 0 — Measure the critical path.** Instrument and record the real
  breakdown of a sandbox start (submit → eval claim → plan → apply → dispatch →
  prepare → boot → first byte), in both `local` and `raft` persistence modes,
  cold and warm image, single-region and simulated cross-region. Recorded in
  `benchmarks/scheduler/README.md` per the AGENTS.md before/after rule. **This
  phase can kill or reshape the plan**: if the commit is 2 ms and the image
  pull is 900 ms, then `stage` is the whole feature and the ticket machinery
  waits.
- **Phase 1 — Provisional allocations, manager-originated, gate `stage`.**
  The claim table, the local ledger, the lease and protection window in
  `reconcile_allocations`, the ratification path (plan submission with a
  one-element candidate list), pre-commit dispatch, `speculativeStart.mode:
  'stage'`, the kill path, events and metrics. Delivers image-pull overlap on
  every job that opts in, with no new authorization surface and nothing
  externally observable before commit.
- **Phase 2 — Gate `serve`.** Boot immediately, kill on refusal. Needs no new
  gating mechanism (it is Phase 1 with the pause removed), and is the honest
  fast path for stateless sandbox workloads whose side effects are idempotent.
  Ships with the documented "a loser may have already done things" warning.
- **Phase 3 — Gate `boot` and the network gate.** TAP link-down + default-drop
  filter for `sandbox`, gated service registration and upstream listeners for
  the other runtimes, the admission-time downgrade rule for unfenceable
  storage (§6.5), `gate_opened` events, and the volume-fencing capability check.
  This is the mode the plan is really for: fast *and* invisible until confirmed.
- **Phase 4 — Boot tickets and edge-initiated starts.** `IssueBootTicket`, the
  `boot` ACL resource, the worker's JWKS client and offline verification, the
  spec cache with prewarm push, `Worker.StartClaim`, `RatifyClaim`, single-use
  `jti` enforcement, `legion sandbox start` + MCP tool, audit events.
- **Phase 5 — Backlog, each separately justified:** island mode (bounded
  survival of unratified work across a control-plane outage, with an explicit
  consistency argument); ticket pre-distribution and bulk minting for warm
  fleets; per-destination policy for the gated network state; speculative
  attach for drivers that implement real attach fencing; ratification batching
  when a node starts many claims per second.

## Testing Plan

- **Unit (Phase 1):** claim state machine (every transition, including
  double-ratify and ratify-after-lease); local ledger admission and release;
  reconcile protection window (a claim survives a heartbeat that omits it,
  and dies one tick after its lease); gate parsing and the "lower of task mode
  and ticket gate wins" rule.
- **Unit (Phase 4):** ticket verification — bad signature, unknown `kid`,
  expired, wrong `aud`, wrong pool, skew at the boundary, replayed `jti`,
  instance-prefix matching; JWKS cache behavior when the fetch fails.
- **Integration (Phase 1–3):** two nodes race for one keyed instance — exactly
  one ratifies, the loser is dead within the lease, `instance get` shows one
  live allocation throughout; refusal for `NoCapacity` and `Ineligible`;
  manager killed mid-claim — the node kills its work at the deadline and the
  cluster shows nothing; worker restart with live claims leaves nothing behind;
  a `boot`-gated sandbox cannot reach the network before ratification and can
  immediately after (the network assertion is the core of Phase 3).
- **Invariant regression:** the existing stateful suites — at-most-one-live
  allocation, node-loss fencing, `concurrent_plans_cannot_oversubscribe_a_node`
  — run **unmodified** with `speculativeStart` on and off. Any change to those
  files is a design failure, not a test update.
- **ACL:** `cargo test --test acl_endpoints` covers the new manager RPCs;
  `RatifyClaim` is classified worker-plane; a node token cannot mint a ticket
  and a boot ticket cannot call anything else.
- **Raft:** ratification is a scheduler-lane op — replay determinism across
  followers, and a ratify issued against a non-leader forwards or refuses
  cleanly (no silent local bind).
- **Benchmarks:** Phase 0's matrix re-run after Phases 1, 3, and 4, with
  time-to-boot and time-to-serve percentiles in the PR; a contention benchmark
  measuring refusal rate and wasted-boot rate at N-way races per instance;
  a no-op guard proving `mode: 'off'` costs nothing measurable.
- **Ignored end-to-end** (KVM + VMM, like `sandbox_microvm_boots`): a real
  microVM booted to `boot` gate, network verified closed, ratified, verified
  open.

## Rollout and Backward Compatibility

- **Everything is off by default**, three times over: `speculativeStart` is
  absent from every jobspec, `edgeBoot.enabled` is false on the manager, and
  `edgeBoot.acceptClaims` is false on the worker. A cluster that upgrades and
  changes nothing behaves byte-for-byte as today, including wire payloads.
- **Version skew.** `TaskAllocation.provisional` is a new optional field: an
  old worker ignores it and runs the allocation the moment it arrives — which,
  with pre-commit dispatch, would run it *before* the commit with no gate and
  no lease. Pre-commit dispatch therefore **requires a minimum worker
  protocol version** (the existing worker protocol version already carried on
  registration) and falls back to post-commit dispatch for older workers, per
  node. This is the one genuinely dangerous skew case and it is fenced at the
  dispatcher.
- **Manager upgrade order** is the existing policy (`docs/upgrades.md`);
  `RatifyClaim` is a new RPC, so an old manager simply rejects it and workers
  fall back to their lease (i.e. they kill the work) — fail-closed under skew.
- **Enabling edge boot is an explicit security decision** and the public doc
  frames it that way: it grants nodes the ability to start work on a
  capability, and the blast radius of a compromised node grows from "runs what
  it was given" to "runs anything a live ticket names".

## Open Questions

- **Is `boot` or `stage` the right recommended default for sandbox platforms
  (lean: `boot`)?** `stage` is free of externally visible risk but only saves
  the pull; `boot` saves the boot too and is invisible until confirmed. If
  Phase 0 shows boot time dominated by the pull, `stage` becomes the honest
  recommendation and Phase 3 loses urgency.
- **Default `ratifyTimeout` (lean: 5s).** Long enough to survive a raft
  election, short enough that a partitioned node's wasted VM is bounded. Wants
  Phase 0 data on ratification latency percentiles.
- **Should a ticket be allowed to name an instance *prefix* (lean: yes, with
  a required non-empty prefix)?** Prefixes let a platform mint one ticket per
  user cohort rather than per sandbox, which is the difference between a mint
  per request and a mint per hour. The cost is a wider capability if a node is
  compromised.
- **Does the local ledger need to be authoritative for non-speculative
  allocations too (lean: no)?** Keeping it advisory avoids two sources of truth
  for capacity; the manager's apply-time re-validation already fences the real
  thing. Revisit if provisional starts are observed pushing nodes past their
  declared resources.
- **How does an edge start interact with `job plan` / dry-run (lean: a
  `--dry-run` on `StartClaim` that verifies the ticket and admission without
  booting)?** Agents want to self-correct before mutating; this is cheap to add
  in Phase 4 and easy to forget.
- **Should ratification bypass the eval queue entirely (lean: yes, as
  designed)?** It is a direct store op precisely to avoid queue latency. If
  that turns out to starve or reorder against normal evals under load, the
  fallback is a dedicated high-priority lane for ratifications.

## Acceptance Criteria

- **Phase 0:** the start-path breakdown is recorded in
  `benchmarks/scheduler/README.md` for both persistence modes, cold and warm
  image, with the control-plane share of time-to-first-byte stated as a number.
- **Phase 1:** a `stage`-gated job measurably starts sooner (image pull
  overlaps the commit) with no change to any externally observable state before
  the commit; killing the manager mid-claim leaves no running work after the
  lease; the stateful and oversubscription suites pass unmodified.
- **Phase 2:** a `serve`-gated race between two nodes for one instance id ends
  with exactly one live allocation and one killed VM, and the refusal reason is
  visible in events and `legion instance get`.
- **Phase 3:** an integration test proves a `boot`-gated sandbox has no network
  reachability before ratification and full reachability after, and that a task
  with unfenceable shared storage is downgraded to `stage` with a warning at
  `job validate` time.
- **Phase 4:** a node with **no** connectivity to the manager at request time
  boots a sandbox from a pre-minted ticket, ratifies when the manager is
  reachable again within the lease, and refuses a replayed ticket; every mint,
  claim, and verdict appears in the audit event stream with the accessor id and
  `jti`; no ticket material appears in any log.
