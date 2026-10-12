# Plan: Sandbox Scaling to Very Large Session Counts

> **Judgement calls:** [`calls/sandbox-scaling-plan_calls.md`](../calls/sandbox-scaling-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-11; Phases 0–3 shipped
2026-10-12; Phases 4–6 shipped 2026-10-12). Phase 0's measurements, their
method, and each shipped phase's result against its target are in
[§14](#14-results-phases-0-3) and [§16](#16-results-phases-4-6); session
starts ran at about 970 a second with no raft entry, against the keyed
lane's 52 lifecycles (§16.3). Phases 7
and later are planned and unchanged by the measurements except where §14
and §16 say so. Phase 6 shipped running sessions and promotion only:
suspending an unpromoted session to a manifest (§6.5), workload identity
for sessions (§6.8), the node session log and its shipper (§6.4), edge
starts (§6.3), and `promote.onVolume` (§6.6) are deferred, each refused at
submit and open in `TODO.md` (calls entry 18). It also deviates from §6 in
named places: the session index is per manager with peer fan-out, not on
the leader (calls entry 6); the lease is a start time, not a JWT (calls
entry 34); sessions end on a worker restart (calls entry 19); and a
promoted allocation reserves nothing (calls entry 11). Phase 1 met its entry targets but reached about 2× the
Phase 0 lifecycle rate, not 5×: the scheduler lane's serial per-entry cost
is now the bound (calls entry 17, §14.3). Shipped with deliberate gaps,
each in `TODO.md`: that rate gap, edge-boot provisional deltas on the
stream (calls entry 39), checkpoint budget reports and `snapshot warm` as
blocked-eval wake events (calls entry 42), and the AWS-rig run of the
Phase 0 matrix (calls entry 20). Phase 0 is a
gate: it measures what every later phase assumes, and its numbers can
reorder or cancel phases. Deliberately deferred and named: cell sharding of
promoted instances (Phase 10, built only past a measured trigger), a
pluggable external session registry (rejected as a default, §4), sub-VM
isolation tiers inside Legion's own runtimes (Phase 9 ships the seam, not a
runtime), cross-cluster session routing (owned by
[multi-cluster-federation-plan.md](multi-cluster-federation-plan.md)), and
exec into sandboxes, which has its own plan
([sandbox-exec-plan.md](sandbox-exec-plan.md), §6.12).

**Relationship to existing plans.** This plan consumes four shipped or
in-flight plans and changes none of their guarantees for the workloads
they already serve:

- [stateful-workloads-and-instance-identity-plan.md](stateful-workloads-and-instance-identity-plan.md)
  owns the keyed instance. This plan adds a third `stateful.mode` beside
  `slot` and `keyed` and leaves both untouched.
- [edge-boot-plan.md](edge-boot-plan.md) owns provisional starts, boot
  tickets, and ratification. This plan reuses tickets as the edge
  authorization for sessions and removes the create write that edge boot
  still pays today (§5.3).
- [suspend-resume-and-oversubscription-plan.md](suspend-resume-and-oversubscription-plan.md)
  owns checkpoints, published checkpoints (`durability = "store"`), and the
  deferred Phase 8 fast paths. Idle sessions in this plan are published
  checkpoints with an identity envelope, and the density phase executes
  part of that Phase 8.
- [gated-sandbox-checkpoints-plan.md](gated-sandbox-checkpoints-plan.md)
  makes gated networking the sandbox default with memory checkpoints, and
  ships the guest agent and the vsock restore override. §6.9 says what a
  session changes in the datapath's keying.
- [sandbox-exec-plan.md](sandbox-exec-plan.md) owns exec into microVM
  sandboxes and exec that skips the manager. §6.12 says what it needs from
  session mode.

---

## The plan in one page

Claims like "billions of concurrent agents" are about two different costs,
and Legion has to answer both:

- **The cost of a running session.** A billion sessions at about 10 MiB
  each is about 10 PB of RAM (2^30 × 10 MiB ≈ 9.3 PiB), and 10 MiB is the
  floor for a Firecracker microVM with a tiny guest, not a realistic agent
  workspace. At that count almost every session has to be suspended, which
  makes it a storage problem, or much lighter than a microVM.
- **The bookkeeping per session.** Today every keyed sandbox is a row in
  the raft-replicated store, and each lifecycle step is one or more quorum
  rounds on a lane whose pipeline runs one proposal at a time. Code
  inspection puts a full create, place, suspend, resume, and destroy at
  about **22 raft entries**, 26 with idempotency keys (§5.1). The row also
  outlives the session as a tombstone, so the store grows with history,
  not with the running set.

Edge boot made the *start* independent of the control plane, but not the
*record*: every edge start still ratifies through the scheduler lane, and
ratification binds a row that must already exist (§5.3).

The recommended architecture splits state by who needs it to be durable:

| State | Where it lives | Raft writes per session |
| --- | --- | --- |
| Jobs, policies, quotas | Raft, as today | 0 |
| **Session capacity envelopes** (one reservation per node per pool) | Raft, as allocations | 0 (per node, not per session) |
| **Running sessions** | The node that runs them, in a local log. Managers hold a rebuildable in-memory index | 0 |
| **Idle sessions** | Manifests in the content store, keyed by session id | 0 |
| **Named or durable sessions** (promoted) | Raft, as keyed instances, as today | 1 to promote, then today's costs |

Density comes from snapshot-clone first (one VM per session, shared
memory pages, Phase 8), with a seam for lighter isolation tiers inside an
envelope later (Phase 9). Cell sharding is built only if promoted
instances outgrow one raft group (Phase 10).

Three changes to how work moves sit beside the state split, adapted from
Tensorlake's published scheduler design and Nomad's:

- **Sequenced delivery** (§6.13). Placements reach workers as a per-node,
  sequenced delta stream held in the leader's memory, acked by the worker,
  with today's full desired set kept as the resync path. Delivery latency
  becomes one round trip, and a heartbeat stops carrying every allocation.
- **Blocked work keyed by its blocker** (§6.15). Unplaceable work waits
  under the constraint that blocked it and wakes only when matching
  capacity appears, instead of a cluster-wide pass on every join and
  nothing on freed capacity.
- **Wake on request** (§6.14). A stateless gateway holds a request for an
  idle session, resumes it from its manifest, and forwards when it is up.

Every phase has latency targets at p50 and p99 beside its throughput
targets (§7.2), measured first in Phase 0.

Exec into sandboxes, and exec sessions that skip the manager, are planned
separately in [sandbox-exec-plan.md](sandbox-exec-plan.md). §6.12 covers
how the two plans meet.

Eleven phases, each shippable alone (§9):

| Phase | What ships |
| --- | --- |
| 0 | Measure: raft entries per lifecycle, rates, bytes per row, and the latency SLOs (gate) |
| 1 | Group commit and write trimming, no new semantics |
| 2 | Sequenced delivery of work |
| 3 | Blocked work keyed by what blocks it |
| 4 | No tombstones for server-minted ids |
| 5 | Archived idle keyed instances (manifests in the bucket) |
| 6 | Session mode |
| 7 | Wake on request through the gateway |
| 8 | Snapshot-clone density |
| 9 | Sub-VM tier seam |
| 10 | Cells, only past a measured trigger |

---

## 1. Problem statement

A platform builder who wants a sandbox per agent session hits three
ceilings today, in this order.

**The write rate.** Every keyed instance lifecycle step is a raft entry
(§5.1). Each lane drains through one pipeline thread with one proposal in
flight (`pipeline_loop`, `src/storage/raft/driver.rs:1781`), and the
default `single` topology puts every lane on that one pipeline
(`docs/multi-raft.md`, "Topology"). On the AWS rig (3 × 4-vCPU managers),
the coalesced plan commit took 9.2, 13.5, and 23.3 ms at 5k, 20k, and 50k
nodes (`internal_docs/TODO.md`, "Make plan-commit cost flat in fleet
size"), and `web_fleet` placed 905 to 327 allocations per second across
5k to 50k nodes (`docs/benchmarks.md`). A sandbox platform's session rate
is a placement rate plus suspend and resume traffic, so it lands in the
same low thousands per second at best, before anything else in the
cluster writes.

**The resident count.** The store is SQLite on every manager, and a raft
snapshot is a `VACUUM INTO` image held in memory while it is built,
served, and installed (`docs/multi-raft.md`, "Snapshots, compaction, and
catch-up"). Every keyed instance keeps its row as a `terminated` tombstone
for the life of its job (`src/storage/store.rs:15556-15565`). A platform
that creates a million sessions a day under one long-lived job adds a
million rows a day that never leave, and every new manager and every
lagging follower receives all of them in memory. Kubernetes hits the same
wall at about 150,000 pods and an 8 GiB etcd
([cluster-large](https://kubernetes.io/docs/setup/best-practices/cluster-large/),
[etcd limits](https://etcd.io/docs/v3.5/dev-guide/limit/)).

**The cost of an idle session.** A suspended keyed instance holds no
memory but still holds an instance row, a snapshot row, and optionally a
held `suspended` allocation row for its reserve (`docs/suspend-and-resume.md`,
"Holding a share of the reservation"). Idle sessions are the large
majority on an agent platform, so they set the row count.

What a platform builder does instead today is what E2B did on Nomad and
Google did on Kubernetes: build a session layer above the scheduler with
its own store, and use the scheduler only for warm pools. That is the
workaround this plan removes.

## 2. Prior art

Every system below that runs more than about 10^5 resident units makes the
host authoritative for what runs on it and keeps the central store for
intent, capacity, and placement summaries.

- **Borg** ([paper](https://research.google/pubs/large-scale-cluster-management-at-google-with-borg/)).
  The median cell is about 10k machines under one five-replica,
  Paxos-backed Borgmaster. An **alloc** is a reserved set of resources on
  one machine that several tasks share, and an alloc set reserves them
  across machines before jobs are submitted into it. The Borgmaster polls
  each Borglet. The Borglet always reports its full state, and link shards
  forward only diffs to the elected master. Tasks keep running when every
  Borgmaster replica is down.
  - **Take:** the alloc as a capacity envelope that raft places once and
    sessions fill many times. Node-authoritative full-state reports with
    diffs. The cell as the failure domain.
  - **Reject:** poll-driven liveness. Legion already moved liveness off
    raft with push heartbeats and adaptive cadence.
- **Omega** ([paper](https://research.google/pubs/omega-flexible-scalable-schedulers-for-large-compute-clusters/)).
  Parallel schedulers commit optimistically against shared state.
  - **Take:** a session router can place optimistically into an envelope
    and let the node accept or refuse, the same shape as Legion's plan
    apply.
- **Kubernetes.** 5,000 nodes, 150,000 pods, and 110 pods per node are the
  documented limits
  ([cluster-large](https://kubernetes.io/docs/setup/best-practices/cluster-large/)).
  etcd's default quota is 2 GiB with 8 GiB "a suggested maximum"
  ([etcd limits](https://etcd.io/docs/v3.5/dev-guide/limit/)). GKE reached
  65,000 nodes by replacing etcd with a Spanner-based store behind the etcd
  API ([GKE blog](https://cloud.google.com/blog/products/containers-kubernetes/gke-65k-nodes-and-counting)).
  **Static pods** run from node-local manifests, and the API server sees
  only a read-only mirror
  ([static pods](https://kubernetes.io/docs/concepts/workloads/pods/static-pods/)).
  - **Take:** the mirror-pod idea. The control plane can learn about
    node-originated work after the fact.
  - **Reject:** fixing scale by swapping the storage engine. That is a
    hyperscaler move, and Legion's pitch is one binary.
- **Nomad.** The 2 million container challenge ran 2,000,000 containers on
  6,100 clients with 3 servers, at about 1,500 placements per second
  ([C2M](https://www.hashicorp.com/c2m)). Every allocation lives in the
  servers' replicated in-memory state, and clients restore their
  allocations from a local BoltDB after a restart.
  - **Take:** the node-local state DB as the restart record.
  - **Lesson:** C2M is a throughput result, not a resident-count result.
    Two million is about the ceiling of "every unit is a raft object".
- **Cloudflare Workers and Durable Objects.** Isolates start in about
  5 ms at about 3 MB each, with thousands per process
  ([blog](https://blog.cloudflare.com/cloud-computing-without-containers/)).
  A Durable Object is placed near its first request and stays there
  ([data location](https://developers.cloudflare.com/durable-objects/reference/data-location/)).
  `newUniqueId` skips the global uniqueness check that `idFromName` pays,
  which can take a few hundred milliseconds
  ([namespace API](https://developers.cloudflare.com/durable-objects/api/namespace/)).
  Cloudflare documents "tens of millions of Agents running concurrently"
  ([agents limits](https://developers.cloudflare.com/agents/platform/limits/)),
  and hibernated agents cost zero compute
  ([Project Think](https://blog.cloudflare.com/project-think/)).
  - **Take:** two id classes. A random id needs no consensus, and a
    caller-chosen name pays a one-time uniqueness check. Idle sessions cost
    only storage.
  - **Reject:** isolates as the default tier. Legion's sandbox users run
    arbitrary Linux code, which needs a VM boundary.
- **Fly Machines.** `flyd` keeps each host's state in a local BoltDB that
  is an append-only log of FSM operations. "Every worker is its own source
  of truth", and no consensus protocol runs between hosts
  ([blog](https://fly.io/blog/carving-the-scheduler-out-of-our-orchestrator/)).
  Global state is gossiped with Corrosion, which has had real outages from
  that design ([Corrosion](https://fly.io/blog/corrosion/)). Suspend takes
  a Firecracker snapshot and resumes in a few hundred ms
  ([suspend](https://docs.fly.io/reference/suspend-resume)).
  - **Take:** the per-host durable FSM log.
  - **Reject:** gossip as the cluster index. Legion's managers already see
    every node's heartbeat, so a leader-held index is simpler.
- **Modal** ([1M sandboxes post](https://modal.com/blog/scaling-to-1-million-concurrent-sandboxes-in-seconds),
  July 2026). The closest prior art. An unsharded Postgres with O(sandboxes)
  writes on the critical path was replaced by schedulers running on cached
  in-memory state that RPC workers directly, workers that accept or
  refuse, and sandbox records written asynchronously. Each worker is its
  own source of truth. Result: 1M concurrent sandboxes created in under a
  minute, tens of thousands per second, median start under 0.5 s.
  - **Take:** the whole shape, which is this plan's Phase 6.
  - **Watch:** the post does not describe failure reconciliation. This
    plan has to (§6.4).
- **E2B** ([persistence](https://e2b.dev/docs/sandbox/persistence)).
  Firecracker sandboxes on Nomad. Paused sandboxes keep memory and disk,
  never expire, and do not count toward concurrency. Pause and resume live
  above the scheduler.
  - **Take:** paused sessions are free in the concurrency model.
  - **Reject:** a session layer the scheduler cannot see.
- **AWS Lambda and Firecracker** ([NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/agache)).
  Less than 5 MiB of VMM overhead, under 125 ms to application code, up to
  150 microVMs per second per host. Each worker host has **slots**, each a
  microVM for one function at a time. A sticky Worker Manager routes
  invocations to slots and asks Placement for a new slot when none is free.
  Slots are not consensus objects.
  [On-demand container loading](https://www.usenix.org/conference/atc23/presentation/brooker)
  reaches 15,000 new containers per second for one customer.
  - **Take:** split the hot, sticky router from slow placement. Raft
    places envelopes, a router fills them.
- **Google Agent Sandbox and Agent Substrate**
  ([GKE blog, May 2026](https://cloud.google.com/blog/products/containers-kubernetes/bringing-you-agent-sandbox-on-gke-and-agent-substrate)).
  Warm pools allocate 300 sandboxes per second per cluster, 90% within
  200 ms. Agent Substrate adds "a minimal control plane designed to bypass
  some of the limitations of Kubernetes", and the post describes workloads
  scaling to "10s to 100s of millions of instances". The suspend plan
  already cites Substrate's under-500 ms resume and over 500 activations
  per second.
  - **Take:** the scale target is tens to hundreds of millions of
    instances, mostly idle, with hundreds to thousands of activations per
    second per cluster.
  - **Note:** no primary source was found for "billions of concurrent
    agents" from Google, Cloudflare, or anyone else. The largest primary
    claims are Cloudflare's tens of millions running and Google's tens to
    hundreds of millions of instances.
- **Snapshot fast start.** Firecracker serves restore page faults from user
  space with UFFD
  ([docs](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/handling-page-faults-on-snapshot-resume.md)).
  [REAP](https://arxiv.org/abs/2101.09355) prefetches the recorded working
  set for 3.7× faster restores on average.
  [FaaSnap](https://www.sysnet.ucsd.edu/~voelker/pubs/faasnap-eurosys22.pdf)
  and [Catalyzer](https://ipads.se.sjtu.edu.cn/pub/projects/catalyzer)
  push further. [KSM](https://docs.kernel.org/admin-guide/mm/ksm.html)
  merges identical pages across VMs.
  - **Take:** these are worker and runtime work. The orchestrator only
    needs snapshot identity, location, and affinity, which Legion has.

### Delivery, blocked work, and waking

- **Tensorlake**, delivery
  ([starting sandboxes in parallel](https://www.tensorlake.ai/blog/starting-1000-sandboxes-in-parallel)).
  It replaced a reconcile loop that diffs desired against actual state on a
  tick with a RocksDB outbox of sequenced commands written in the
  scheduling transaction, a long-poll `poll_commands` stream per
  dataplane, acks of the highest processed sequence, pruning after ack, a
  `Pending → Enqueued` gate against re-emission, and a full sync on
  sequence regression. The post gives a "1000s of sandboxes every second"
  headline but no measured latency distribution.
  - **Take:** sequence numbers, acks, pruning, and full resync on
    regression. **Reject:** the outbox as a second durable store. Legion's
    allocation rows already are one (§6.13).
- **Tensorlake**, placement
  ([filter and rank](https://www.tensorlake.ai/blog/multi-cloud-scheduling)).
  Nodes sit in a spatial index over CPU, memory, disk, and GPUs, ranking
  prefers loaded nodes and snapshot locality, and a region with no capacity
  queues "blocked work" that only that region's new capacity re-evaluates.
  - **Take:** wake only the work the new capacity can help.
- **Tensorlake**, suspend and snapshot
  ([suspend vs. snapshot](https://tensorlake.ai/blog/suspend-vs-snapshot)).
  "Suspend is a pause button. Same sandbox ID, resumed in place." A
  snapshot is "a save file" that restores into new sandboxes. A timeout on
  a named sandbox suspends it rather than terminating it.
  - **Take:** the same split Legion already has (`suspend` versus
    `snapshot create` and `fork`).
- **Nomad blocked evals**
  ([blocked_evals.go](https://github.com/hashicorp/nomad/blob/main/nomad/blocked_evals.go)).
  Unplaceable evals wait by computed node class, with an "escaped" bucket
  for constraints classes cannot capture, one blocked eval per job, and
  unblocks on node register, ready, eligibility change, and allocations
  finishing.
  - **Take:** the whole lifecycle, including escaped and missed-unblock
    tracking (§6.15).
- **Claude Managed Agents**
  ([overview](https://platform.claude.com/docs/en/managed-agents/overview)).
  A session that finishes its work goes `idle`, not `terminated`, and
  resumes cleanly. Runtime is billed only while a session is `running`
  ([pricing](https://platform.claude.com/docs/en/about-claude/pricing)).
  Self-hosted environments can wake a worker on a webhook
  (`session.status_run_started`) instead of keeping pollers up
  ([workers](https://platform.claude.com/docs/en/managed-agents/self-hosted-sandboxes-workers)).
  Tensorlake's integration suspends the sandbox to zero between webhooks
  and resumes it in under a second
  ([blog](https://tensorlake.ai/blog/claude-managed-agents)).
  - **Take:** idle is the normal state of an agent session, and an event
    should wake capacity.
- **Fly.io autostart**
  ([docs](https://docs.fly.io/reference/fly-proxy-autostop-autostart)).
  `auto_stop_machines = "suspend"` lets the proxy suspend idle Machines, and
  on a request "the proxy starts a stopped or suspended Machine in the
  nearest region" and routes the request to it.
  - **Take:** the proxy is the waker. Legion's equivalent is the gateway.
- **Cloudflare Durable Objects hibernation**
  ([WebSockets](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)).
  "Clients remain connected while the Durable Object is not in memory", and
  an incoming message wakes it. Duration charges stop while hibernated.
  - **Take:** keep the connection at the edge and let the workload sleep.

## 3. Jobs to be done, by persona

**Platform builder (the sandbox service)**
- When a user opens a session, I want a sandbox in the time it takes to
  boot or restore one, at any request rate my fleet can host, so the
  control plane is never what my capacity plan is about.
- When most of my users are idle, I want their sandboxes to cost disk and
  nothing else, so I pay for the users who are active right now.
- When a user comes back days later, I want their exact workspace back,
  on whatever machine has room, so "idle" never means "lost".
- When a session matters, like a long-running agent with a name, I want to
  pin it down as a durable, uniquely named thing, so my own code can rely
  on that name.
- When a webhook arrives for an agent that has been asleep for a week, I
  want the request to wake it and get answered, so I never pay for an idle
  agent and never write my own wake-up proxy.
- When I start a sandbox, I want it to begin booting within a network round
  trip of the decision, so my start latency is not a heartbeat interval.

**Cluster operator**
- When I add machines or work finishes, I want the work that was waiting
  for that room to place right away, and only that work, so adding capacity
  is not a cluster-wide re-plan.
- When the session count grows by 100×, I want the managers' disk, memory,
  and snapshot size to stay flat, so I don't re-plan the control plane
  every quarter.
- When a manager fails over, I want running sessions untouched and the
  session list back within seconds, so a failover is not an outage.

**Security / platform admin**
- When sessions are minted without the control plane recording each one,
  I want every session still tied to a job, a node, and a credential I
  issued, so an ACL or identity decision never rests on something a node
  made up.

**On-call responder**
- When a user says "my sandbox vanished", I want to find where it ran,
  why it stopped, and whether it was suspended, from one command, so I'm
  not grepping node logs.

**Autonomous agent**
- When I create a session and get an id back, I want to know whether it is
  durable or best effort, and retry safely on a timeout, so I never create
  two sandboxes for one task or build on one that can disappear.

*Not in scope:* the end user of the sandbox platform. Their experience of
this plan is that sessions start fast and come back.

### User scenarios

1. **A million idle, ten thousand busy.** A coding-agent platform has 1M
   sessions, of which 10k are running at any moment. Running sessions
   live on 200 nodes. Idle ones are manifests in `s3://legion-content-usw2/`.
   Raft holds the job, 200 envelope allocations, and the 3,000 sessions
   users have named. A resume reads a manifest, picks a node with
   envelope room, and restores. No raft entry is written.
2. **A burst at the edge.** A launch sends 20,000 session starts per second
   to edge nodes holding a prefix boot ticket. Each node mints a session
   id, admits against its envelope, and boots. Managers learn about the
   sessions on the next heartbeat. The ticket spend is per ticket, not per
   session.
3. **Promotion.** A user names their workspace `user-42`. The platform
   calls `legion instance promote s-7k2q... --id user-42`. One raft entry
   creates the keyed instance bound to the running sandbox. From then on
   `user-42` is a keyed instance with today's guarantees: never recycled,
   at most one live allocation, exact listing.

## 4. Goals and non-goals

**Goals**
- Measure raft entries per sandbox lifecycle and sustained session rates,
  and make both visible in an archive (Phase 0).
- Cut raft entries per keyed lifecycle from about 22 to 8 or fewer inside
  today's model, with no change to any guarantee (Phase 1).
- Make raft size proportional to running plus named sessions, not to
  history or idle sessions (Phases 4 and 5).
- Offer an opt-in session mode whose start, suspend, resume, and stop
  write nothing to raft (Phase 6).
- Keep every guarantee of `slot` and `keyed` instances, with one stated
  exception: Phase 4 makes server-minted keyed ids unique by randomness
  instead of by tombstone (calls entry 3). State precisely which
  guarantees the new mode weakens (§6.7).
- Keep one binary and no new required dependency. The content store a
  worker already uses for published checkpoints is the only external
  storage.
- Deliver placements to workers within one round trip, and keep a
  heartbeat's size independent of how much the node runs (Phase 2).
- Wake only the blocked work that new capacity can help, including freed
  capacity (Phase 3).
- Wake an idle session on an inbound request, with no node holding it
  (Phase 7).
- Give every phase p50 and p99 latency targets, measured in Phase 0 (§7.2).

**Non-goals**
- **Billions of running sessions in one cluster.** That is about 10 PB of
  RAM at an unrealistically small 10 MiB each. The honest target is
  millions running and hundreds of millions idle per cluster (§7), and
  more through federation.
- **A pluggable external session registry** (DynamoDB, FoundationDB,
  Spanner) as a default or a Phase. It turns a self-hosted scheduler into
  a distributed system with a cloud dependency (calls entry 28).
- **Gossip.** Managers already receive every heartbeat.
- **Changing `keyed` or `slot` semantics.** Existing jobs keep exact
  listing, tombstones, and at-most-one.
- **New placement intelligence.** Envelopes are placed by today's
  scheduler. The session router is first fit within envelopes.
- **Cross-cluster routing of sessions.** Federation owns it.
- **Exec into sandboxes.** [sandbox-exec-plan.md](sandbox-exec-plan.md)
  owns it.
- **Replacing the eval loop.** It stays the planner. Phase 2 changes only
  how its results reach workers.
- **A second durable store for delivery.** The outbox is leader memory over
  the raft-backed allocation rows.
- **Node-local wake on connect and ingress into gated guests.** The suspend
  plan's Phase 8 and the gated plan's Phase 5 own them (§6.14).

## 5. Background: what exists today

### 5.1 Raft entries per keyed sandbox lifecycle

Counted from the code with the default `single` topology, no volumes, no
idempotency keys, and an idle cluster. Phase 0 confirms these with
`legion_raft_ops_total{lane,op,outcome}`. Every mutating store method is
one entry through `Store::replicated` (`src/storage/store.rs:2439`). The
payload is a SQLite changeset (`StoreCommand`, `src/storage/raft/mod.rs:189`),
nested calls inside a capture fuse into one entry, and empty changesets
skip raft (`src/storage/raft/driver.rs:1666`).

| Stage | Trigger | Entries | Where |
| --- | --- | --- | --- |
| Create | `CreateInstance` | 2: `create_keyed_instance` + coalesced eval enqueue (+1 idempotency) | `src/manager/server.rs:4856-4993`, `src/storage/store.rs:15322` |
| Place | eval claim, plan, settle | 4: claim, plan apply (allocation, instance bind, volume, event in one changeset), status-lane `task_status` projection, settle | `src/manager/scheduler.rs:1676-1708`, `src/storage/store.rs:4989-5035`, `:16049` |
| Run | heartbeat | 0 per beat for liveness, but **1 status-lane entry per node per beat** for allocation health (below) | `src/storage/store.rs:7986`, `:13226-13310` |
| Suspend | `SuspendInstance` + capture report | 4: `begin_suspend`, `apply_snapshot_report`, two standalone events (+1 idempotency) | `src/storage/store/suspend_ops.rs:330-398`, `src/manager/server/snapshot_rpcs.rs:991-1027` |
| Resume | `StartInstance` on a suspended instance | 8: `resume_suspended_instance`, enqueue, placement (4), restore report + event (+1 idempotency) | `src/storage/store/suspend_ops.rs:732`, `src/storage/store/snapshot_ops.rs:733-781` |
| Fork ×N | `ForkInstance` | 8 + 2N | `src/storage/store/fork_ops.rs:64`, `src/manager/server/snapshot_rpcs.rs:1185-1292` |
| Destroy | `DestroyInstance` | 4: destroy, enqueue, claim, settle (+1 idempotency). The row stays as a tombstone | `src/storage/store.rs:15511-15584` |

**Total: about 22 entries per create, place, suspend, resume, destroy, or
26 with idempotency keys.** About 16 of them are on the scheduler lane.
Under load the eval claim folds into the previous settle and plan applies
coalesce, so the amortized count falls, but the scheduler lane still
serializes every one.

Three findings from the count that the plan acts on:

- **Allocation health is a per-node, per-beat raft write.** Every
  allocation gets a health entry when it starts
  (`src/worker/task_manager.rs:2537`). The worker re-sends the whole set
  every beat (`:1240-1253`), and the manager upserts it with
  `updated_at = now` (`src/manager/server.rs:6311`,
  `src/storage/store.rs:13291-13310`), so the changeset is never empty.
  Node health checks skip unchanged rows. Allocation health does not. At
  10,000 sandboxes on a node, every beat is a 10,000-row changeset. This
  quietly undoes much of
  [liveness-off-the-raft-path](liveness-off-the-raft-path-plan.md) for
  dense nodes. The AWS rig never saw it because simulated workers send no
  allocation health (`src/worker/simulate.rs:810`).
- **Edge boot does not avoid the create write.** Ratification binds an
  existing unbound keyed instance and refuses otherwise
  (`src/storage/store.rs:12560-12580`). `CreateInstance` also enqueues a
  placement eval that can bind the row before the edge claim does. A
  realistic edge start is create (2) + an eval (2) + ratification (4:
  ticket spend on the control lane, plan apply, status projection, event,
  `src/manager/edge_boot.rs:70-120`, `:312-331`).
- **Events are raft writes.** Events staged inside an op's transaction
  ride its entry. Standalone `store.events()` calls are entries of their
  own (`src/storage/store.rs:10533-10556`), and the journal keeps only
  the newest ~10,000 rows (`docs/event-stream.md`).

### 5.2 The instance model and its guarantees

- **Keyed instances** are created by `CreateInstance`. Server-minted ids
  are `{task}-{16 alphanumerics}`, about 95 bits, checked against every row
  (`src/storage/store.rs:15293-15300`). Caller ids must be unique against
  every row, including tombstones (`:15273-15281`).
- **Never recycled for the life of the job.** Destroy keeps a `terminated`
  tombstone (`:15556-15565`). `job destroy` deletes the job's instance rows
  (`:8966`), after which ids are free again (`docs/stateful-workloads.md`,
  "Id rules").
- **`requireId`** refuses creates without a caller id. It is enforced in the
  handler and changes no writes (`src/manager/server.rs:4913-4930`).
- **At most one live allocation per instance**, bound inside the plan
  apply transaction (`assign_stateful_instance_tx`).
- **ACLs** scope instance operations by job name and namespace
  (`AclScope::job`, `src/manager/server.rs:4860-4878`).
- **Workload identity** is issued per allocation. The handler reads the
  allocation row and checks that it is on the calling node
  (`src/manager/server.rs:5977-6085`). The default `sub` is
  `<region>:<job>:<group>:<task>`, and `{instance}` may be added
  (`docs/workload-identity.md`). Issuing writes nothing.
- **Idempotency** keys are recorded on the control lane
  (`Store::idempotency_record`, `src/storage/store.rs:17912`).
- **Listing** is an exact read of raft-replicated rows.

### 5.3 Edge boot

Shipped (phases 1 to 4). A worker verifies a boot ticket offline, admits
against an advisory local ledger, and boots to a gate level, while the
manager ratifies through a one-placement plan apply
(`docs/edge-boot.md`). Tickets can name an instance prefix and can be
multi-use. Unratified work dies at `ratifyTimeout`. The open follow-ups in
`internal_docs/TODO.md` ("Ratification throughput at scale") already
estimate ratification at about 1,700, 1,200, and 700 per second at 5k,
20k, and 50k nodes with the plan coalescer's default cap of 16.

### 5.4 Liveness, lanes, and group commit

- **Liveness is off raft.** A steady-state heartbeat writes nothing.
  Only transitions replicate, and a fleet-wide display checkpoint runs
  every 30 s (`src/storage/store.rs:7986-8021`,
  `docs/node-liveness.md`). Adaptive cadence serves 2 s to 21 s intervals
  as the fleet grows (`docs/heartbeat-cadence-and-scale.md`).
- **Lanes** split control, scheduler, and status writes into three raft
  groups (`topology = "lanes"`, opt-in). Table ownership pins `instance`,
  `allocation`, `snapshot`, `volume`, and `eval` to the scheduler lane
  (`src/storage/raft/ownership.rs:196-260`). (`docs/multi-raft.md` says
  instance writes are on the control lane. The code says scheduler. The
  TODO item records the doc fix.)
- **Group commit** exists for plan apply (cap 16), eval enqueue, job
  submit, and task-status seed (cap 64), and the mixed-version heartbeat
  fallback (cap 128) (`src/storage/coalesce.rs`,
  `src/storage/store.rs:1604-1622`). Instance create, suspend, resume,
  destroy, snapshot reports, allocation health, ticket spends, and
  standalone events are not batched.

### 5.5 Suspend, resume, fork, and published checkpoints

Shipped through suspend plan Phase 7. A memory checkpoint lives in the
node content store. With `suspend.durability = "store"`, the node
publishes each part by digest to its `contentStore` bucket, the manifest
records the URLs, and a resume can run on any compatible node
(`docs/suspend-and-resume.md`, "Durability: surviving the node"). Forks
of one checkpoint share pages copy-on-write on Firecracker. The location
index, warms, and the checkpoint disk budget are already manager-memory
soft state. Phase 8 (wake on connect, UFFD lazy restore, diff snapshots)
is deferred.

### 5.6 Delivery and retry

Placements reach workers through a best-effort `PostWork` push and a full
desired set in every heartbeat reply. Unplaceable work is retried by a
cluster-wide capacity pass on node join or uncordon, and by nothing on
freed capacity. §6.13 and §6.15 open with the code-level detail for each.

### 5.7 The worker's local state

`WorkerStateStore` is a JSON file rewritten in full on every save
(`src/worker/state.rs:151`). It holds allocations, task configs, and PIDs
for restart recovery. The heartbeat carries every allocation id on every
beat (`HeartbeatRequest.allocation_ids`, `proto/legion.proto:4266`).
Neither shape survives 10,000 sessions per node with churn.

## 6. Proposed design

### 6.1 Two axes, scored option by option

The brainstorm produced eight options. Each is scored against what it
costs and which guarantee it weakens.

| # | Option | Raft writes per session | What it weakens | Verdict |
| --- | --- | --- | --- | --- |
| 1 | Ephemeral by default, promote on demand | 0 until promoted, 1 to promote | Never-recycled becomes probabilistic for unnamed ids. At-most-one becomes converge-to-one | **Take** (Phase 6) |
| 2 | Nodes are the source of truth for running sessions | 0 | `list` is eventually consistent. The index is rebuilt after failover | **Take** (Phase 6) |
| 3 | Idle sessions are manifests in object storage | 0 per idle session | Listing idle sessions is a bucket listing | **Take** (Phase 5) |
| 4 | Cell sharding by id prefix across N raft groups | unchanged, but spread | Scatter-gather lists, cross-cell moves | **Defer** (Phase 10, behind a trigger) |
| 5 | Group commit and write-behind | fewer, larger entries | Nothing, if done as group commit only | **Take first** (Phase 1) |
| 6 | Pluggable external registry | 0 in raft, 1+ in the registry | Self-hosted simplicity | **Reject** as a default |
| 7 | Sessions inside allocations (two-level) | 0 | Isolation, when sessions share a VM | **Take the envelope**, defer sub-VM tiers (Phases 6, 9) |
| 8 | Snapshot-clone at the edge | 0 | Nothing in the control plane | **Take** (Phase 8) |

Write-behind (persist the record asynchronously and let nodes re-report
after a crash) is not taken on its own. Doing it for keyed instances would
weaken the guarantees of a mode that promises them. Options 1 and 2 are
write-behind done honestly: a separate mode whose record is node-owned by
design, with the guarantees stated up front.

The lean from the brainstorm was (1) + (2) + (3), plus (7) and/or (8) for
density, (4) only if promoted instances outgrow one group, and (5) first.
This plan adopts it with two amendments, each a judgement call:

- **Raft places envelopes, not nothing.** Option 2 alone leaves capacity
  invisible to the scheduler. A session-mode task reserves capacity per
  node as an ordinary allocation (Borg's alloc), and sessions fill it.
  The scheduler, quotas, preemption, and `node usage` keep working
  unchanged (calls entry 9).
- **Idle manifests land before ephemeral running sessions.** Phase 5 works
  for today's keyed instances with `durability = "store"`, and idle
  sessions are the larger count. It ships value without a new mode
  (calls entry 7).

### 6.2 A third instance mode: `session`

Sessions are instances, not a new noun. A task opts in with
`stateful.mode = "session"`, and the existing `legion instance` commands,
RPCs, and MCP tools work on them, with a `durable` field that says which
guarantees apply.

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    stateful = {
      mode = "session",            # 'slot' | 'keyed' | 'session' (new)
      envelope = {                 # the per-node capacity reservation
        resources = { cpu = 30000, memory = 61440 },   # per node
        constraint = [{ attribute = "pool", value = "agents" }],
        # nodes = "all"            # default: every eligible node
      },
      promote = {
        onName = true,             # a caller id promotes (default true)
        onVolume = true,           # attaching a network volume promotes
      },
    },
    resources = { cpu = 500, memory = 512, memoryMax = 2048 },  # per session
    suspend = {
      checkpoint = "memory",
      durability = "store",        # required for session mode
      idle = { after = "3m" },
    },
    config = { image = "ghcr.io/acme/agent:1.4", vcpus = 1, memory_mib = 512 },
  }],
}
```

Validation at `job validate`:
- `session` requires `suspend.durability = "store"` when a `suspend` block
  is present, because a node-local idle session would be pinned to one
  node with no record of it anywhere else.
- `session` refuses node-local `stateful.storage` drivers, for the same
  reason `durability = "store"` does today.
- `envelope.resources` must fit at least one session's `resources`.
- `requireId = true` is refused on a session task with a hint to use
  `keyed`. A session that must be named from birth is a keyed instance.

The surface as the user sees it:

```console
$ legion instance create agents -j
{"ok":true,"data":{"instanceId":"workspace-7k2qd9x4m1b8c3rj5t0v6wye2h",
 "mode":"session","durable":false,"node":"w-17","state":"active",
 "deduplicated":false}}

$ legion instance list agents -j
{"ok":true,"data":{"instances":[...],
 "complete":true,"asOf":"2026-11-02T18:04:11Z","durableOnly":false}}

$ legion instance promote workspace-7k2qd9x4m1b8c3rj5t0v6wye2h --id user-42 -j
{"ok":true,"data":{"instanceId":"user-42","mode":"session","durable":true,
 "promotedFrom":"workspace-7k2qd9x4m1b8c3rj5t0v6wye2h"}}

$ legion instance create agents --id user-43 -j     # naming at create promotes
{"ok":true,"data":{"instanceId":"user-43","durable":true, ...}}
```

`suspend`, `start`, `stop`, `destroy`, `fork`, and `get` keep their names.
On an ephemeral session, `stop` and `destroy` are the same thing: the
session ends and nothing remains, because there is no row to keep.

### 6.3 Session ids and the lease

**Ids.** A session id is `{task}-{26 base32 chars}`, 130 random bits. At a
trillion ids the birthday collision probability is about 10^-15. Ids are
minted by whoever starts the session (a manager's session router, or an
edge node redeeming a ticket), with no uniqueness check. No tombstone is
kept. Caller ids may not take that shape (Phase 4's reserved-pattern
rule), so a random id can only ever come from a mint. A caller-chosen id always promotes (§6.6), because a name like
`user-42` needs durable uniqueness and only raft provides it.

**Leases.** A session started by a manager carries a lease, a JWT signed
with the cluster's existing workload-identity key. Any manager can sign
it, so minting needs no consensus:

```jsonc
{
  "sub": "session:agents/workspace",
  "legion.io/instance": "workspace-7k2qd9x4m1b8c3rj5t0v6wye2h",
  "legion.io/job": "agents", "legion.io/job_config": "jc-7f3a91",
  "legion.io/task": "workspace",
  "legion.io/node": "w-17",
  "legion.io/envelope": 88213,          // the envelope allocation id
  "iat": 1793635451, "exp": 1793639051  // renewed on resume and promote
}
```

An edge start has no manager in the loop, so the node mints the id and
the boot ticket stands in for the lease: the ticket names the job, task,
pool, and an instance prefix. The manager records the pair (ticket `jti`,
session id) in the soft index on the next heartbeat. Ticket single-use
enforcement stays as it is for tickets with `uses = 1`. A prefix ticket
with many uses is the session-mode shape and spends once (calls entry 30).

The lease is a capability document. It proves which credential
authorized a session, for identity issuance and audit, and it never
replaces node liveness: a session dies when its envelope's node is fenced,
like any allocation on that node today.

### 6.4 Running sessions are node-owned

**On the node.** A session runs inside its task's envelope on that node.
The worker keeps a local log of session lifecycle records in a SQLite
database beside its data directory (start, gate open, suspend, resume,
stop, with the lease and ticket `jti`). The JSON `WorkerStateStore`
stays for allocations and gains nothing. The node admits a session only
against its envelope's free capacity, which the local ledger from edge
boot already tracks. A refusal is a typed `ENVELOPE_FULL` the router
retries elsewhere.

**Reporting.** The heartbeat gains a `sessions` delta (started, ended,
suspended, gate state) and a periodic full report, Borg-style. The full
report goes out at registration, on a leader change, and every N beats
(default 30). The leader folds deltas into an in-memory **session index**
(id → node, envelope, state, lease `iat`), the same kind of soft state as
the content location index and liveness. A new leader rebuilds the index
from full reports. Until every live node has sent one, list replies
carry `complete: false`.

**Reconcile.** Today `reconcile_allocations` stops anything the manager
does not list (`src/worker/task_manager.rs`, see `docs/edge-boot.md`, "The
kill path"). Sessions are exempt by construction: they are children of an
envelope allocation, and only the envelope is in the desired set. The
manager can still end a session by putting its id in the heartbeat reply's
`stop_sessions` list, which is how `legion instance stop` reaches it.

**Duplicates.** Two nodes can briefly run the same unnamed session id: an
idempotent retry that raced to two nodes, or a resume issued twice. The
leader sees both in the index and stops the one with the later lease
`iat`, in the next heartbeat reply. This is converge-to-one within one
heartbeat interval, not at-most-one. A session that needs at-most-one is
promoted.

**Node loss.** Sessions on a lost node are gone unless suspended to a
published manifest. The index marks them `lost` and the platform's
router learns it from `StreamEvents` or a `get`. With an idle policy,
most sessions on any node are already manifests, which bounds the loss to
the running ones.

**Placement.** The session router runs on every manager. It picks a node
with envelope room from the leader's index and live usage (first fit,
preferring nodes whose content store already holds the image or the
checkpoint, using the existing `locality` signal), RPCs the worker over
the edge-boot `StartClaim` path with a lease, and returns the id. A
non-leader router forwards index reads to the leader. Raft is not on the
path.

### 6.5 Idle sessions are manifests

A suspended session with `durability = "store"` becomes a **session
manifest**: the published checkpoint manifest (parts, digests, URLs,
`compat`) plus an identity envelope (session id, job, job config, task,
lease history, network attachment, and the snapshot alias it forked from,
if any). It is written to
`<contentStore.url>/sessions/<job>/<id-prefix>/<id>.json`, next to the
parts it names.

- **Suspend writes no raft entry.** The node captures, publishes, writes
  the manifest, frees the session's share of the envelope, and reports a
  `suspended` delta.
- **Resume reads the manifest.** `legion instance start <id>` looks up the
  index first (a running session needs no resume), then reads the
  manifest from the bucket of the region it was minted in. The router
  places it like a new session and the node restores from the parts.
- **Expiry is bucket lifecycle.** Legion does not delete published parts
  today (`docs/suspend-and-resume.md`, "Limitations"). Session manifests
  follow the same rule: a lifecycle rule on `sessions/` sets how long an
  idle session lives. `legion instance destroy` on an idle session deletes
  its manifest, and the parts follow bucket lifecycle.

**For keyed instances (Phase 5, before session mode exists).** A suspended
keyed instance with `durability = "store"` can be **archived**: its
snapshot row and held reserve row are dropped, its instance row shrinks
to a slim `archived` row (id, job, task, state, manifest URL), and the
manifest moves to the bucket. The slim row keeps the id reserved, because
a keyed id is a name. Archiving is one raft entry per instance, and
unarchiving is one more, but the row count of a million idle keyed
instances drops to a million slim rows with no snapshot or allocation
rows behind them. Unnamed (server-minted) keyed instances can opt into
keeping nothing (`suspend.archive = "manifestOnly"`), which is the bridge
to session mode.

### 6.6 Promotion

A session is promoted to a keyed instance when it:
- Is created with a caller id (`--id`), or renamed (`promote --id`).
- Attaches a network volume (`promote.onVolume`), because a volume
  outlives the VM and needs a durable owner.
- Is forked as a named seed (`snapshot create --name`), which is a durable
  object already.
- Is promoted explicitly (`legion instance promote <id>`), keeping its
  random id.

Promotion is one raft entry: an `instance` row in `active` state bound to
an allocation row that the worker adopts in place (the envelope's
reservation shrinks by the session's size and a new allocation of the same
size is inserted, in one apply, so capacity is never double counted). The
apply is `assign_stateful_instance_tx`, so a caller id that already exists
is refused exactly as `CreateInstance` refuses it today. From then on the
instance is keyed: tombstoned on destroy, listed exactly, at most one live
allocation. A promoted instance cannot be demoted.

### 6.7 What each guarantee becomes

| Guarantee | `keyed` (unchanged) | `session`, unnamed | `session`, promoted |
| --- | --- | --- | --- |
| Id never recycled | Exact, for the job's life (tombstone) | Probabilistic: 130 random bits, no tombstone | Exact (tombstone) |
| At most one live allocation | Exact (apply binds) | Converge to one within a heartbeat | Exact |
| `instance list` complete | Exact read | Running: leader index, `complete` flag, stale by up to one heartbeat. Idle: bucket listing, `--include-idle` | Exact |
| ACL scoping | Job + namespace | Job + namespace, unchanged: the job is in the lease or ticket | Job + namespace |
| Workload identity `sub` | From the allocation row | From the lease (§6.8) | From the allocation row |
| Idempotency | Raft record | Derived id (below). No raft record | Raft record |
| Audit and events | Replicated journal (~10k) | Live tail plus node log shipped to the bucket | Replicated journal |
| Survives node loss while running | Rescheduled | Lost, unless suspended | Rescheduled |
| Survives manager failover | Yes | Yes (sessions keep running, index rebuilds) | Yes |
| Delivery to the worker (Phase 2) | Sequenced deltas within a round trip, full resync on failover or gap | Started by the router over `StartClaim`, reported back as sequenced deltas | As keyed |
| Wake on request (Phase 7) | Node-local, from a local checkpoint ([suspend plan](suspend-resume-and-oversubscription-plan.md) Phase 8) | Gateway, from the bucket manifest | Gateway or node-local |
| Exec ([exec plan](sandbox-exec-plan.md)) | Proxied, or a direct ticket naming the allocation | Proxied or direct, resolved through the session index to the envelope and session id | As keyed |

**Idempotency without raft.** A create with an `idempotency_key` derives
its session id as `HMAC(cluster key, accessor id ‖ job ‖ key)`, truncated
to 130 bits. A retry gets the same id. If the index already holds it, the
router returns it with `deduplicated: true`. If a retry raced to a second
node before the first report landed, both run until the leader sees the
duplicate and stops the later one. The agent sees one id either way.

**ACL check-before-fetch.** `GetInstance` on a session id gates with the
coarse `AclScope::any()` check before the index lookup and enforces the
job scope after it, so a credential-less caller cannot probe existence,
the same rule `AGENTS.md` sets for every lookup.

**Events.** Per-session events (`instance.session_started`,
`instance.session_suspended`, ...) are published on the in-process
broadcast for `StreamEvents` but **not** written to the replicated
journal, which is 10,000 rows and would roll over in a second at session
rates. The durable record is the node's session log, shipped in batches to
`<contentStore.url>/session-log/<node>/<hour>.jsonl`. Aggregate counters
(`legion_sessions_started_total{job}`, ...) replace per-session journal
rows for dashboards. Promotion, envelope changes, and refusals that mean
misconfiguration are journaled as today.

### 6.8 Workload identity for sessions

`IssueWorkloadIdentityToken` gains an optional `session_lease` (or
`boot_ticket_jti` plus session id for edge starts). The manager:

1. Authenticates the worker as `node:<id>`, as today.
2. Verifies the lease signature (or that the ticket `jti` is known and
   unexpired), and that the lease's node is the caller.
3. Checks that the lease's envelope allocation is on that node, which is
   a raft read.
4. Refuses a session id the index says has ended.

Claims come from the lease: `legion.io/instance` is the session id and
`legion.io/alloc` is the envelope. The default `sub` is unchanged
(`<region>:<job>:<group>:<task>`). A `subTemplate` with `{instance}` on a
session task renders the session id, and the public doc must say that an
unnamed session's id is unique with high probability, not by record.

**The boundary moves.** Today a compromised node can mint tokens only for
allocations placed on it. With edge-started sessions it can mint tokens
for any session id it invents under a job its ticket names. That is the
same blast radius edge boot already accepted ("runs anything a live ticket
names", `docs/edge-boot.md`), applied to identity. Promoted sessions keep
today's boundary. Calls entry 33 records the trade.

### 6.9 Composition with gated networking and suspend

- **Datapath keying.** The gated plan derives a guest's vsock path and
  policy binding from its allocation id. A session is a child of an
  envelope allocation, so the datapath keys by `(envelope alloc, session
  id)`, and the restore retarget in gated plan §6.2 uses the session key.
  Policy stays per job and task, so a session inherits it with no
  per-session state.
- **Ordering.** Gated sandboxes are the sandbox default after gated plan
  Phase 2. Session suspend for gated sandboxes needs gated plan Phase 1
  (memory checkpoints of gated sandboxes). Until then, a session task with
  `network.mode = "gated"` and `checkpoint = "memory"` is refused with the
  same message the suspend plan uses.
- **Idle detection.** Gated plan Phase 3 (live egress bytes and
  connections) feeds the node-local idle policy. Idle suspend of a session
  is decided and executed on the node, so the manager's idle sweep
  (`src/manager/server/suspend_rpcs.rs:571-729`) skips session tasks.
- **Fork.** Forking a session forks a checkpoint into N new unnamed
  sessions, each with its own random id, MAC, and token, using the
  existing fork identity rebind. No raft entry unless a fork is named.
- **Reserve and make-room.** `suspend.reserve` holds an allocation row per
  suspended instance today. Session mode refuses a non-zero `reserve`:
  the envelope is the reservation. Make-room suspends work inside an
  envelope on the node, with no manager round trip.
- **Suspend plan Phase 8** (UFFD lazy restore, diff snapshots, wake on
  connect) is this plan's density lever (Phase 8), and diff snapshots are
  what make a hundred million idle manifests affordable in a bucket.

### 6.10 Density by isolation tier

- **Tier 1, one microVM per session, shared pages (Phase 8).** Every
  session in an envelope restores from one warm base checkpoint, maps its
  memory `MAP_PRIVATE` (already true for forks on Firecracker), and loads
  pages lazily through UFFD. Diff snapshots make suspended sessions small.
  KSM is an operator opt-in for nodes that want it. Legion keeps charging
  each session its full `resources.memory`, as forks are charged today,
  and lets oversubscription spend the measured sharing.
- **Tier 2, many sessions per VM (Phase 9, seam only).** A task runtime can
  declare `sessionsPerInstance > 1`, and the worker hands sessions to an
  existing runtime instance (processes, isolates, WASM) through a new
  `TaskPlugin::start_session` call instead of booting one each. Legion
  ships the seam and the accounting, not a runtime, and the public doc
  states plainly that sessions sharing a VM share a kernel.

### 6.11 Cells, only if needed (Phase 10)

If promoted instances or envelopes outgrow one raft group, partition
`instance` rows by a hash of the id across N scheduler-lane groups
("cells"), each with its own pipeline. Multi-raft already runs several
groups over one transport, and table ownership is per lane. The cost is
scatter-gather for exact lists and a two-cell move for a rename. The
trigger is measured, not guessed: build it when the Phase 0 harness shows
the scheduler lane above 70% busy from instance writes at the target
rate, or the store above 4 GiB from instance rows.

### 6.12 Exec at scale

Exec is planned in [sandbox-exec-plan.md](sandbox-exec-plan.md): exec into
Linux microVMs over the guest agent's vsock, and direct exec with
worker-verified tickets on the mTLS worker listener from
[worker-plane-auth-plan.md](worker-plane-auth-plan.md) (PR
[#16](https://github.com/mikenomitch/legion/pull/16)). The two plans meet
in three places:

- **Resolving unnamed sessions.** A session in `session` mode has no raft
  row. Exec by instance id, proxied or direct, resolves the id through the
  leader's session index (§6.4) to its node, envelope allocation, and
  session id, after the coarse ACL gate. The worker checks that the session
  is running locally under that envelope.
- **No raft write per shell.** Minting an exec ticket is a signature. With
  a delegated minting key, a platform that started a session at the edge
  can open a shell into it without calling a manager, so start and shell
  are both zero-manager-call paths.
- **The gateway tier.** Wake on request (§6.14) and the exec plan's gateway
  phase are the same stateless tier, `legion start gateway`. Whichever
  plan builds it first, the other adds its route.

The exec plan's phases do not depend on session mode. Only its
session-resolution step waits for this plan's session mode.

### 6.13 Sequenced delivery of work, not full desired sets (Phase 2)

**Today.** The eval loop plans and the plan apply commits allocations
through raft. Delivery then has two halves:

- **A best-effort push.** `dispatch_allocations` spawns one `PostWork` per
  allocation, bounded by a 512-permit semaphore, and does not wait for it.
  The code says why: "Dispatch is best-effort BY DESIGN: the heartbeat
  reply carries the full desired set, so a lost push costs at most one
  heartbeat interval" (`src/manager/dispatcher.rs:139-146`, `:22`).
- **A full desired set on every beat.** `build_desired_allocations`
  rebuilds a complete `TaskAllocation` payload for every dispatchable
  allocation on the node, every heartbeat, with a store read for each
  keyed instance and its volume mounts (`src/manager/server.rs:10330-10388`).
  The worker stops anything not in that set
  (`TaskManager::reconcile_allocations`). The request side is just as
  total: every beat lists every allocation id
  (`HeartbeatRequest.allocation_ids`, `proto/legion.proto:4266`). An open
  TODO already asks for a desired-state version so an unchanged node gets
  a cheap reply.

At 10 allocations a node this is cheap and self-healing. At 10,000 sandboxes
a node, every beat carries and rebuilds thousands of full payloads, and a
lost push waits up to one heartbeat interval, which adaptive cadence
stretches to 21 s on a large fleet (`docs/heartbeat-cadence-and-scale.md`).

**Tensorlake's design** ([blog](https://www.tensorlake.ai/blog/starting-1000-sandboxes-in-parallel)).
The scheduler writes each `AddSandbox` command to a persistent outbox in
RocksDB, in the same transaction as the scheduling decision, with a
sequence number. Each dataplane holds a long-lived `poll_commands` stream
that the scheduler holds open for up to five minutes when nothing is
pending. The dataplane acks the highest sequence it processed, and acked
commands are pruned. A `Pending → Enqueued` transition stops a sandbox
being emitted twice, and a sequence regression after a dataplane restart
triggers a full state sync. Latency is "bounded only by network round-trip
time, not by any polling interval".

**For Legion: the eval loop stays the planner, and only delivery changes.**

- **The source of truth stays where it is.** Committed allocation rows in
  raft are already durable. The outbox is not a second durable record: it
  is a per-node delta log held in the leader's memory, built from the
  changesets the leader already projects into the read model (the delta
  refresh knows exactly which allocation rows changed on which node,
  `docs/state-store.md`, "Persistence Modes").
- **A per-node sequence.** Each delta (add, update, stop with its stop
  notice) gets `(leader_epoch, seq)` for its node. The leader epoch is the
  raft term the leader took office in.
- **One stream per worker.** `Manager.StreamWork` is a server-streaming RPC
  the worker opens and the leader holds, long-polling when there is nothing
  to send. The worker acks its last applied `seq` on the stream, and the
  leader prunes acked deltas. A worker that reconnects sends its last
  `(epoch, seq)`.
- **Full resync is the fallback, not the steady state.** A worker whose
  `epoch` differs from the leader's (failover), whose `seq` is ahead of the
  leader's (regression) or behind what the leader still holds (pruned), gets
  today's full desired set once, then deltas. The heartbeat reply carries a
  desired-set digest, so a missed delta is still caught within one beat.
- **No re-emission gate is needed.** A delta is produced once per committed
  changeset, so Tensorlake's `Pending → Enqueued` state is implicit.
- **Speculative dispatch rides the stream too.** Edge boot's pre-commit
  payloads (`docs/edge-boot.md`) become a `provisional` delta, and the
  apply's verdict follows on the same stream.
- **The request side goes delta too.** The worker sends allocation ids and
  health only when they change, plus a digest, which completes Phase 1's
  allocation-health fix: a steady beat is a few hundred bytes whatever the
  node runs.

**Why not an outbox in raft.** Writing each command in the plan's own
transaction would add rows to every placement entry, and every ack and
prune would be a raft write per node per beat, which is the per-beat raft
write Phase 1 removes from allocation health. The allocation rows already
carry the durability, so the outbox only needs to survive as long as one
leader's term (calls entry 4).

**Sessions use the same mechanism in the other direction.** A running
session in session mode is node-local (§6.4). Its lifecycle reaches the
leader's index as sequenced deltas from the node's session log, with the
same ack, epoch, and full-report rules.

**Composition with the worker plane.** The stream runs on the
worker-to-manager channel, authenticated by the node token and encrypted
once [worker-plane-auth-plan.md](worker-plane-auth-plan.md) (PR
[#16](https://github.com/mikenomitch/legion/pull/16)) Phase 3 lands. Delivery
then no longer needs the manager to dial the worker at all. `PostWork`
stays for older workers, behind the worker-plane-auth plan's manager-only
caller class. Exec, proxied or direct, is unaffected
([sandbox-exec-plan.md](sandbox-exec-plan.md)).

### 6.14 Wake on request (Phase 7)

> **Update 2026-10-12:** wake for **keyed instances** shipped ahead of this
> phase as [wake-on-request-plan.md](wake-on-request-plan.md), which now owns
> the gateway, its addressing, and the jobspec block. Its surface differs
> from the sketch below: `suspend.wake = { port = "<label>", timeout }` with
> no `on` field, hosts are `<instance>.<domain>` (a `Legion-Port` header or
> `CONNECT` picks another port), and the default hold is `30s`. Waking
> sessions from manifests is that plan's Phase 3; this section stays as the
> session-specific design.

An idle session that exists only as a manifest in the bucket has no node.
Something has to hold the inbound request, trigger the resume, and forward
once the guest is ready. Three plans touch this, and each owns one part:

- **The suspend plan's Phase 8** owns *node-local* wake on connect: a
  suspended instance with a local checkpoint keeps its ingress port on its
  node, and the worker holds a parked listener
  ([suspend plan §6.10](suspend-resume-and-oversubscription-plan.md)).
  That cannot work for a manifest, because no node holds the port.
- **The gated plan's Phase 5** owns the last hop: ingress into a gated
  guest over host-initiated vsock.
- **This plan owns wake for idle sessions in the bucket.** The waker lives
  off-node, in the stateless gateway tier, `legion start gateway`. The exec
  plan's Phase 4 adds exec to the same tier (calls entry 12).

**The surface.**

```nickel
suspend = {
  checkpoint = "memory",
  durability = "store",
  idle = { after = "3m" },
  wake = { on = "request", port = 8080, timeout = "10s" },   # new
},
```

The gateway serves `https://<port>-<instance>.<gateway domain>`, the
addressing E2B uses, and a webhook form, `POST /v1/wake/<instance>`, that
resumes and returns the address without a held request. Platforms that run
their own router keep calling `legion instance start`, which is already a
wake.

**The path.**

1. A request arrives for `8080-workspace-7k2q….gw.example.com`. The gateway
   asks the session index whether the session is running. If it is, it
   forwards.
2. If not, the gateway holds the request and asks the session router to
   resume it. Concurrent requests for one id share one resume
   (single-flight at the gateway, and converge-to-one at the router, §6.7).
3. The router reads the manifest, places the resume like a new session,
   preferring nodes whose content store already holds the parts or the
   base image (the `locality` score), and the node fetches what it lacks
   and restores.
4. The node reports `serving`, the router answers with the node address,
   and the gateway forwards the held request. Past `wake.timeout` it
   answers `503` with `Retry-After`.

Tensorlake's Managed Agents integration and Anthropic's session model show
the target behavior. A webhook-mode sandbox "suspends to zero when no
webhook is arriving" and "wakes on the next one", answering in about 0.6 s
from suspended
([blog](https://tensorlake.ai/blog/claude-managed-agents),
[docs](https://docs.tensorlake.ai/sandboxes/claude-managed-agents)).
Claude Managed Agents sessions go `idle`, not `terminated`, when they
finish work, and runtime is billed only while a session is `running`
([session operations](https://platform.claude.com/docs/en/managed-agents/session-operations),
[pricing](https://platform.claude.com/docs/en/about-claude/pricing)).

### 6.15 Blocked work, keyed by what blocks it (Phase 3)

**Today.**

- A node registering or being uncordoned calls `request_capacity_reconcile`
  (`src/manager/server.rs:6718`, `:6971`), which enqueues one cluster-wide
  `node_join` eval unless one is already pending
  (`src/storage/store.rs:14616-14632`).
- That eval runs `reschedule_allocations_for_new_node`, which scans every
  job that is not stopped or failed and re-plans each one short of its
  `count` or with a `count.per` task (`job_configs_waiting_on_capacity`,
  `src/storage/store.rs:14557-14607`). It re-plans them whether or not the
  new node could ever host them.
- That SQL compares allocations to the task's `count`. Keyed tasks define no
  count (their desired count comes from `count_live_keyed_instances` in the
  scheduler), so keyed instances left `pending` appear not to be covered.
  Phase 0 confirms this with a test.
- Freed capacity triggers nothing. An allocation that stops, or an instance
  that suspends, does not wake work waiting for room.
- A plan that finds no capacity at all fails its eval terminally. The
  late-binding requeue backstop covers only conflict errors, paced by
  `schedulerSettings.plan_conflict_requeue_delay_ms`
  (`docs/scheduler-and-eval-loop.md`, `internal_docs/TODO.md`, "Capacity
  exhaustion still fails evals terminally").
- The [gang scheduling plan](gang-scheduling-plan.md) proposes a blocked-eval
  queue for gangs only.

**Prior art.** Nomad's `BlockedEvals` tracks evaluations "that shouldn't be
queued until a certain class of nodes becomes available". Each blocked eval
records which computed node classes it is eligible for, and one that the
classes cannot capture is "escaped" and wakes on any change. The FSM
unblocks by class when a node registers, becomes ready, or turns eligible,
and when allocations finish on a node. There is one blocked eval per job,
and missed unblocks while an eval is in the scheduler are tracked by index
([blocked_evals.go](https://github.com/hashicorp/nomad/blob/main/nomad/blocked_evals.go),
[fsm.go](https://github.com/hashicorp/nomad/blob/main/nomad/fsm.go)).
Tensorlake queues requests for a region with no capacity "as blocked work",
and new capacity re-evaluates "only the blocked work for that specific
region" ([filter and rank](https://www.tensorlake.ai/blog/multi-cloud-scheduling)).

**Design.** An eval that cannot place goes `blocked` instead of `failed`,
with a **blocking key**, and the leader keeps an index from key to blocked
evals (calls entry 13):

- **Keys.** The node class the placement could use (pool plus the
  attributes its constraints read, Nomad's computed class), the resource
  shape it needs, a checkpoint pin (a resume waiting on the node that holds
  its checkpoint, or on its checkpoint disk budget), or `escaped` when the
  constraints cannot be reduced to a class.
- **Wake events.** Node register or uncordon (its class), allocations
  stopped, removed, or suspended on a node (that node's class and freed
  shape), a dynamic attribute change, a checkpoint budget report, and a
  completed `snapshot warm`. Each wakes only the evals whose key matches,
  plus `escaped`.
- **One blocked eval per job**, replaced by a newer one, and an unblock that
  fires while the job's eval is running is remembered and re-applied, as
  Nomad does.
- **State.** The blocked status and key are on the eval row, written in the
  same entry that today marks it failed, so no new raft write. The index is
  leader memory, rebuilt from rows on election, like liveness.
- **Pending keyed instances** are covered: a keyed placement that finds no
  room blocks under its key like any other.
- **Session mode.** The router admits sessions into envelopes from soft
  state, so a full envelope pool is a router refusal (`ENVELOPE_FULL`), not
  a blocked eval. Blocked evals cover envelopes, keyed instances, and
  everything else the scheduler places.
- **The gang plan** reuses this queue as its waiting state rather than
  building its own, and its shortfall diagnosis becomes the blocked eval's
  status detail.

```console
$ legion job status agents -j
{"ok":true,"data":{"status":"blocked",
 "blocked":{"key":"class:pool=agents,cpu.arch=x86_64","shortfall":{"memory":"48GiB"},
 "since":"2026-11-02T18:04:11Z"}}}
```

## 7. Target numbers per phase

Targets assume the AWS rig's control plane (3 × m6id.xlarge, `lanes`
topology), Firecracker sandboxes, and a fleet sized to host the running
count. Phase 0 replaces every "today" estimate with a measurement, and any
target Phase 0 shows to be wrong is rewritten here with a calls entry, not
quietly dropped.

### 7.1 Throughput and counts

| Phase | Raft entries per lifecycle | Sustained session starts/s (cluster) | Running sessions | Idle sessions | Raft rows per idle session |
| --- | --- | --- | --- | --- | --- |
| Today (estimate) | ~22 (26 with keys) | low thousands at best, shared with all other placements | ~10^5 to 10^6 (store and snapshot size) | same pool as running | 2 to 3 (instance, snapshot, optional reserve) + tombstone forever |
| 0: measure | measured | measured | measured bytes per instance row | measured | measured |
| 1: group commit and trims | ≤ 8 idle cluster, ≤ 3 amortized under load | ≥ 5× Phase 0 | unchanged | unchanged | unchanged |
| 2: sequenced delivery | unchanged | unchanged, but no longer capped by heartbeat rebuild cost | 10^4 allocations per node without heartbeat growth | unchanged | unchanged |
| 3: blocked work | unchanged | unchanged | unchanged | unchanged | unchanged |
| 4: no tombstones for unnamed ids | unchanged | unchanged | rows track live + suspended, not history | unchanged | unchanged |
| 5: archived idle instances | +1 to archive, +1 to unarchive | unchanged | unchanged | ≥ 10^7 keyed per cluster | 1 slim row (named), 0 (`manifestOnly`) |
| 6: session mode | 0 for unnamed, 1 to promote | ≥ 20,000 (bound by node boot rate and router CPU) | ≥ 10^6 per leader index (~200 B per entry) | ≥ 10^8 (bucket bound) | 0 |
| 7: wake on request | 0 | wakes bound by restore rate | as Phase 6 | as Phase 6 | 0 |
| 8: snapshot-clone density | 0 | restore-bound, ≥ 100 restores/s per node | 5× to 10× more per node than cold boots, measured | smaller manifests via diff snapshots | 0 |
| 9: sub-VM tier seam | 0 | runtime-bound | 10^3 to 10^4 per node, runtime-dependent | as Phase 6 | 0 |
| 10: cells (conditional) | as Phase 1, per cell | named-instance writes scale with cell count | as Phase 6 | as Phase 6 | 0 |

Delivery and blocked work have targets of their own:

| Phase | Metric | Today | Target |
| --- | --- | --- | --- |
| 2 | Heartbeat payload, steady state, 1,000 / 10,000 allocations per node | full `TaskAllocation` set both ways, measured in Phase 0 | ≤ 1 KiB both ways, whatever the node runs |
| 2 | Store reads per steady heartbeat | one instance read and one volume read per keyed allocation | 0 |
| 3 | Evals per capacity event | one cluster-wide pass per join or uncordon, re-planning every short job. Freed capacity triggers nothing | only blocked evals whose key matches, plus `escaped` |
| 3 | Time from capacity appearing to placed | next join, resubmit, or leader change | p50 ≤ 1 s, p99 ≤ 5 s |

A billion sessions on Legion therefore means federation of clusters at
Phase 6 or later, each holding millions running and hundreds of millions
idle, with the idle fraction set by the platform's activity rate. Phase 6's
index target is per leader, and a 10^7 index is about 2 GiB of manager
memory, which is where Phase 10's sharded index (the same cell split,
applied to the soft index) would start.

### 7.2 Latency SLOs

Throughput alone hides the number a user feels (calls entry 22). Phase 0
measures each of these at p50 and p99, on an idle cluster and at 80% of the measured
throughput ceiling, and every later phase states its effect on them.

| Metric | From → to | Today | Target (phase) | Published reference |
| --- | --- | --- | --- | --- |
| Placement decision | create or submit accepted → placement committed | measured in Phase 0 | p50 ≤ 10 ms, p99 ≤ 50 ms (1). p50 ≤ 2 ms, p99 ≤ 10 ms for a session start, which commits nothing (6) | Modal: scheduling "takes tens of milliseconds" ([post](https://modal.com/blog/scaling-to-1-million-concurrent-sandboxes-in-seconds)). Tensorlake: "100s of sandboxes under a few milliseconds" ([post](https://www.tensorlake.ai/blog/multi-cloud-scheduling)) |
| Dispatch to worker | placement committed → worker has the payload | one push RTT, or up to one heartbeat interval (2 to 21 s) when the push is lost | p50 ≤ RTT + 5 ms, p99 ≤ 50 ms, independent of heartbeat cadence (2) | Tensorlake: "bounded only by network round-trip time" |
| Running, cold boot | committed → task running | measured in Phase 0 | control-plane share ≤ 10% of the total (2). Boot time itself is a runtime number | Modal: start "less than half a second at the median"; Google warm pool: 90% in 200 ms at 300 per second ([GKE blog](https://cloud.google.com/blog/products/containers-kubernetes/bringing-you-agent-sandbox-on-gke-and-agent-substrate)) |
| Running, restore | committed → restored guest running | measured in Phase 0 | p50 ≤ 300 ms, p99 ≤ 1 s for a local memory checkpoint of a 512 MiB guest (2). p50 ≤ 100 ms to first instruction with lazy restore (8) | Fly: resume "a few hundred ms" ([docs](https://docs.fly.io/reference/suspend-resume)) |
| Resume, local memory | `instance start` → serving | measured in Phase 0 | p50 ≤ 400 ms, p99 ≤ 1.5 s (2) | Tensorlake: "a few hundred milliseconds to a second" |
| Resume, published memory | `instance start` → serving, parts not on the node | measured in Phase 0 | p50 ≤ 2 s, p99 ≤ 5 s for 512 MiB (6). Warm node as local (5) | |
| Resume, disk | `instance start` → serving | a cold boot | as cold boot | |
| Wake on request | first request byte at the gateway → first response byte | not possible | p50 ≤ 1 s, p99 ≤ 3 s from a published memory manifest on a node holding the base parts (7) | Tensorlake: a suspended sandbox answered `GET /healthz` "in about 0.6s" ([docs](https://docs.tensorlake.ai/sandboxes/claude-managed-agents)) |
| Exec session open | request → first output | measured in the exec plan's Phase 0 | per [sandbox-exec-plan.md](sandbox-exec-plan.md) §7, proxied and direct | |

## 8. Proto, struct, config, and persistence changes

**Proto** (every method and field with public-facing comments, per
`AGENTS.md`):
- `Instance` gains `mode` (already present), `durable`, `promoted_from`,
  and `lease_issued_at`. `ListInstancesReply` gains `complete`, `as_of`,
  and a request flag `include_idle`.
- `Manager.PromoteInstance { instance_id, new_id?, idempotency_key }`,
  ACL `job:write` on the instance's job. Added to `tests/acl_endpoints.rs`.
- `HeartbeatRequest.sessions` (a delta) and `full_session_report` flag.
  `HeartbeatReply.stop_sessions`.
- `IssueWorkloadIdentityTokenRequest.session_lease` and `session_id`.
- `Worker.StartClaim` gains a `session` variant that carries a lease
  instead of a ticket for router-originated starts.
- `TaskAllocation.envelope` marks an envelope allocation, so an old worker
  that does not understand it refuses it at dispatch (§11).

**Job config** (`configs/jobs/maximal.ncl` gets every field):
`stateful.mode = "session"`, `stateful.envelope { resources, constraint,
nodes }`, `stateful.promote { onName, onVolume }`, and
`suspend.archive = "slim" | "manifestOnly"` for keyed tasks (Phase 5).

**Manager config:** `sessions { routerEnabled = true, fullReportEvery = 30,
indexMaxEntries = 2000000 }`.

**Delivery (Phase 2):**
- `Manager.StreamWork` (server streaming, worker plane, ACL via
  `acl::enforce_worker_plane` and classified in `tests/acl_endpoints.rs`).
  The worker sends `{ node_id, last_epoch, last_seq }` and acks on the
  stream. The server sends `WorkDelta { epoch, seq, add | update | stop |
  provisional }` or `FullResync { desired_allocations }`.
- `HeartbeatRequest` gains `allocations_digest` and sends
  `allocation_ids` and `allocation_health` only on change.
  `HeartbeatReply` gains `desired_digest` and leaves `desired_allocations`
  empty for a worker on the stream. Old workers keep today's full set.
- Worker capability `work-stream`, advertised at registration.

**Blocked work (Phase 3):**
- `eval.status = 'blocked'`, plus `blocked_key` and `blocked_detail`
  columns appended to `eval`, written in the entry that today marks the
  eval failed.
- `JobStatus` gains a `blocked { key, shortfall, since }` block. `legion job
  status`, the MCP job tool, and the UI show it.
- Metrics: `legion_evals_blocked{key_kind}`, `legion_evals_unblocked_total{event}`,
  `legion_blocked_wait_seconds`.

**Wake on request (Phase 7):**
- Job config `suspend.wake = { on = "request", port, timeout }` (added to
  `configs/jobs/maximal.ncl`).
- `legion start gateway` with config `{ domain, listen, tls, managers }`.
  The gateway is stateless and authenticates to managers with an ordinary
  ACL token scoped to the jobs it serves, with the grants `instance start`
  already needs. No new ACL resource.
- Manager `Session.Resume { instance_id, wait_until = "serving" }` used by
  the gateway.

**Worker config:** `sessions { logPath, shipLogEvery = "5m" }`. Session
mode requires `contentStore`.

**Persistence:**
- Phase 4: `instance` gains `named BOOLEAN`. Server-minted ids are not
  `named` and leave no tombstone. Existing rows are `named = 1`, so nothing
  already tombstoned is released.
- Phase 5: `instance.state = 'archived'` plus `manifest_url`. Archiving
  deletes the snapshot row and any held reserve row in the same apply.
- Phase 6: no new replicated table. Envelope allocations are ordinary
  `allocation` rows with an `envelope` flag in their task config. The
  session index is memory only. The worker gains `sessions.db`.
- All migrations add columns at the end, preserving column order, since
  raft changesets bind columns by position (the JSONB affinity migration's
  rule).

## 9. Implementation phases

Each phase ships alone and is worth having alone.

- **Phase 0: measure (gate).**
  - A lifecycle op-count test on the `tests/raft_apply_roundtrip.rs`
    harness (`raft_config(..., "single")` and `"lanes"`) that drives create,
    place, suspend, resume, fork, and destroy on `noop` with the checkpoint
    capability faked, and asserts the `legion_raft_ops_total` delta per
    stage. It becomes the regression guard for every later phase.
  - A `legion-loadgen --mode instances` that drives `CreateInstance`,
    `SuspendInstance`, `StartInstance`, and `DestroyInstance` at an
    open-loop `--rate` against `--simulate` nodes, plus a ratification mode
    (already a TODO). Simulated workers send allocation health so the
    per-beat write shows up.
  - Bytes per instance row, per snapshot row, and the store and snapshot
    size at 10^5 and 10^6 instances.
  - The latency SLOs in §7.2 at p50 and p99, idle and at 80% of the
    throughput ceiling: placement decision, dispatch to worker, running
    from cold boot and from restore, and resume from local memory,
    published memory, and disk. Heartbeat payload bytes at 1,000 and 10,000
    allocations per node. A test that confirms whether a keyed instance left
    `pending` is retried by the capacity pass.
  - Numbers go in `benchmarks/scheduler/README.md`. This phase can reorder
    the rest: if Phase 1 alone reaches the platform's target rate, Phase 6
    waits.
- **Phase 1: group commit and write trimming, no new semantics.**
  - Allocation health writes only on change, like node health checks. A
    steady beat writes zero rows.
  - Coalesce `create_keyed_instance` with its eval enqueue (one entry for a
    burst of creates), and coalesce snapshot and restore reports.
  - Fuse lifecycle events into their op's transaction instead of
    standalone `store.events()` entries.
  - Edge ratification creates the instance row when the ticket names an id
    that has never existed, and folds the ticket spend into the same apply
    (the open TODO item). An edge start becomes one scheduler-lane entry
    plus the status projection.
  - Instance-only evals (destroy, stop) skip claim and settle when the eval
    has no placement work.
  - Target: ≤ 8 entries per lifecycle on an idle cluster, ≤ 3 amortized
    under load, ≥ 5× the Phase 0 session rate.
- **Phase 2: sequenced delivery (§6.13).** After Phase 1, because it
  matters once raft write volume is down and the heartbeat is the next
  per-node cost. The per-node delta log in leader memory, `StreamWork`, acks
  and pruning, epoch and gap resync, the provisional delta for edge boot,
  delta heartbeats with digests, and the `work-stream` capability. Docs:
  `docs/architecture-managers-workers.md`, `docs/heartbeat-cadence-and-scale.md`,
  `docs/edge-boot.md`.
- **Phase 3: blocked work (§6.15).** After Phase 1. Blocked evals with keys,
  the leader index, wake on join, uncordon, freed capacity, attribute
  change, budget report, and warm, one blocked eval per job, missed-unblock
  tracking, the `blocked` job status. The gang plan builds on it. Docs:
  `docs/scheduler-and-eval-loop.md`, `docs/jobs-tasks-and-scheduling.md`.
- **Phase 4: no tombstones for unnamed ids.** Server-minted ids grow to
  130 bits and leave no row on destroy. Caller ids keep tombstones. Caller
  ids may no longer match the server-minted shape `{task}-{26 base32}`,
  the same kind of reserved-pattern rule slot ids already have, so nobody
  can re-create a dead unnamed id by typing it. Raft size then tracks live
  and suspended instances.
- **Phase 5: archived idle instances.** `suspend.archive` for keyed tasks
  with `durability = "store"`, the slim row, manifests in the bucket, and
  `--include-idle` listing. Works without session mode.
- **Phase 6: session mode.** Envelopes, the session router, leases, the
  worker session log, heartbeat deltas and full reports, the leader index,
  `PromoteInstance`, identity from leases, derived-id idempotency, the
  session log shipper, CLI and MCP fields (`durable`, `complete`),
  `catalog()` entries, and `docs/agent-readiness.md`.
- **Phase 7: wake on request (§6.14).** After Phase 6, since it wakes
  sessions from manifests. `legion start gateway`, `suspend.wake`, the
  hostname and webhook routes, single-flight resumes, and the held-request
  timeout. The exec plan's gateway phase adds exec to the same binary.
  Docs: a new `docs/gateway.md`, `docs/suspend-and-resume.md`.
- **Phase 8: snapshot-clone density.** Executes suspend plan Phase 8's
  UFFD lazy restore and diff snapshots for session envelopes, a warm base
  checkpoint per envelope, and density measurements per node.
- **Phase 9: sub-VM tier seam.** `TaskPlugin::start_session`,
  `sessionsPerInstance`, accounting, and the isolation warning in docs.
- **Phase 10: cells (conditional).** Built only past the §6.11 trigger.

## 10. Testing plan

- **Phase 0** is itself the test harness: the op-count test fails when a
  change adds an entry to a lifecycle stage, so every later phase shows its
  delta in CI.
- **Phase 1:** the stateful, edge-boot, suspend, and fork suites run
  unmodified. New unit tests: an unchanged allocation-health beat produces
  an empty changeset. A ratification that creates its row refuses a
  tombstoned or existing id. Coalesced creates isolate a failing request
  (per-request savepoints, like intake).
- **Phase 2:** a worker on the stream receives an allocation within one
  round trip of the commit with heartbeats paused. A dropped stream
  reconnects and replays from its last ack. A leader kill makes every
  worker resync once and lose nothing. A gap (pruned seq) resyncs. A
  steady beat is under 1 KiB at 10,000 allocations. Old workers keep
  today's full-set reconcile. The edge-boot suites pass unmodified with
  provisional deltas.
- **Phase 3:** a job blocked on a pool wakes on a node joining that pool
  and not on one joining another. Freed capacity on a node wakes a job
  blocked on its class. An escaped job wakes on any change. A pending keyed
  instance blocks and wakes. An unblock during the job's running eval is not
  lost. A leader kill rebuilds the index from rows. The gang plan's tests
  run on it when it lands.
- **Phase 4:** an unnamed destroy leaves no row, a named destroy leaves a
  tombstone, existing rows keep theirs across the migration, and a
  mixed-version cluster replays the column-appended migration.
- **Phase 5:** archive and unarchive round trip, the slim row refuses a
  duplicate create, a lost bucket object fails resume with
  `onRestoreFailure`, and `--include-idle` pages through a bucket listing.
- **Phase 6:**
  - Integration on the `raft_lanes` harness: start 10^4 sessions on
    simulated nodes and assert zero raft entries. Kill the leader and
    assert the new index is `complete` after one full-report round, with
    no session stopped.
  - Duplicate convergence: race an idempotent create to two nodes and
    assert one survives within one heartbeat.
  - Promotion: a promoted session keeps running, gains a row, and an id
    collision is refused. Capacity is not double counted.
  - Identity: a node cannot get a token for a lease naming another node,
    an ended session, or a job its envelope does not run.
  - ACL: every new RPC is in `tests/acl_endpoints.rs`.
  - Worker restart: sessions in the local log are restored or killed per
    their gate, never silently orphaned.
- **Phase 7:** a request to an idle session's hostname resumes it and is
  answered. Ten concurrent requests cause one resume. A wake past
  `timeout` answers `503` with `Retry-After`. A gateway restart loses only
  held requests. A webhook wake returns the address.
- **Phase 8:** ignored KVM tests for UFFD restore and diff snapshots, plus
  a density benchmark that records sessions per node at a fixed memory
  budget, cold versus clone.
- **Benchmarks:** the Phase 0 matrix re-run after Phases 1, 2, 5, and 6, with
  before and after numbers in each PR, per the `AGENTS.md` scheduler rule.

## 11. Rollout and backward compatibility

- **Phases 0 and 1 change no surface.** They are internal and default on,
  guarded by the op-count test and the unmodified suites.
- **Phase 2 is negotiated per worker.** A worker that advertises
  `work-stream` gets deltas. Every other worker keeps today's push and full
  set, so mixed fleets work and a rollback is a worker downgrade. Managers
  upgrade first, per `docs/upgrades.md`.
- **Phase 3 changes a visible default.** Evals that today end `failed` for
  lack of capacity end `blocked` and place later on their own. `job status`
  says so. Scripts that treated `failed` as "no room" need to read
  `blocked`, which the upgrade note calls out.
- **Phase 4 changes no existing row.** Only instances minted after the
  upgrade can be untombstoned, and only unnamed ones. The public doc
  states that the never-recycled guarantee for server-minted ids becomes
  probabilistic from that release on. That is a change to a documented
  guarantee, so it ships with an upgrade note in `docs/upgrades.md`.
- **Phases 5 and 6 are opt-in per job** (`suspend.archive`,
  `stateful.mode = "session"`). Nothing changes for jobs that do not set
  them.
- **Version skew.** Envelope allocations are refused by workers that do
  not advertise the `sessions` capability, and the scheduler does not
  place envelopes on them. A manager that does not understand session
  heartbeat fields ignores them, which is safe because sessions are
  invisible to it but keep running. Upgrade managers first, per
  `docs/upgrades.md`.
- **Phase 7 is opt-in per job** (`suspend.wake`) and needs a gateway the
  operator runs.
- **Downgrade.** A cluster that downgrades after enabling session mode
  loses its session index (sessions keep running inside envelopes until
  their nodes restart) and cannot read archived rows. The upgrade doc says
  so.
## 12. Open questions

- **Is the leader the right home for the session index (lean: yes, for
  Phase 6)?** It matches liveness and the location index. The cost is
  that index memory scales with running sessions on one process. A
  sharded index across managers is the Phase 10 cell split applied to soft
  state. Revisit at 10^7 running per cluster.
- **Should `stop` on an unnamed session keep a manifest (lean: no)?**
  `suspend` keeps state, `stop` ends it. Keeping a manifest on stop makes
  the two indistinguishable.
- **Should the session log ship to the bucket or to OTLP logs (lean:
  bucket, with OTLP as an option)?** The bucket is already required.
  OTLP is where operators look.
- **Should unnamed sessions get `{instance}` in `sub` at all (lean: allow,
  with the doc warning)?** Refusing it breaks per-session federation
  policies that some platforms want. Allowing it puts a probabilistic id
  in a security boundary.
- **How long is a full report period (lean: 30 beats)?** Shorter rebuilds
  the index faster after a lost delta. Longer keeps beats small.
- **Do envelopes need their own rollout semantics (lean: no)?** A job
  update replaces envelopes like any allocation. Running sessions in an
  old envelope stay until they suspend or stop, and resume into the new
  config. This needs a drain rule, and the lean is "new sessions go to new
  envelopes, old envelopes are removed when empty".
- **Does the delta stream replace the heartbeat (lean: no)?** Liveness,
  usage, and health stay on the heartbeat, and only work moves to the
  stream. Merging them would save a connection per node but tie liveness
  to a long-lived stream, which is harder to reason about during a
  partition.
- **Should freed capacity wake blocked work immediately or in batches
  (lean: coalesce for 100 ms)?** A node draining a thousand allocations
  would otherwise wake the same blocked eval a thousand times. Nomad
  coalesces through its unblock index. Phase 0's churn numbers decide the
  window.
- **Who authenticates the request a gateway wakes for (lean: the platform,
  with an optional per-instance route token)?** The gateway can route on
  the hostname alone, as E2B's proxy does, and leave authorization to the
  workload. A route token in the hostname, as Cloudflare's preview URLs
  use, stops scanning but makes URLs secrets.
- **What triggers cells (lean: the §6.11 thresholds)?** Measured in Phase 0
  and re-checked after Phase 6.

## 13. Acceptance criteria

- **Phase 0:** `benchmarks/scheduler/README.md` records raft entries per
  lifecycle stage for `single` and `lanes`, sustained create, suspend, and
  resume rates on the AWS rig or a stated local substitute, bytes per
  instance row, every §7.2 latency SLO at p50 and p99, and heartbeat bytes
  at 1,000 and 10,000 allocations per node. The op-count test runs in CI.
- **Phase 1:** the op-count test shows ≤ 8 entries per lifecycle on an
  idle cluster. A 10,000-allocation node's steady beat writes zero rows.
  An edge start writes one scheduler-lane entry plus its projection. The
  session rate is at least 5× Phase 0's.
- **Phase 2:** with heartbeats paused, a placement reaches its worker
  within one round trip plus 5 ms at p50. A 10,000-allocation node's
  steady heartbeat is under 1 KiB both ways and does no store reads. A
  leader kill causes one resync per worker and no lost or duplicated
  allocation.
- **Phase 3:** a node joining a pool causes evals only for jobs blocked on
  that pool's class (plus escaped ones). Freed capacity places a blocked
  job with p50 ≤ 1 s. `job status` shows the blocking key and shortfall.
- **Phase 4:** a churn test of 10^6 unnamed creates and destroys leaves the
  instance table at the live count.
- **Phase 5:** 10^6 archived keyed instances leave 10^6 slim rows and no
  snapshot or allocation rows, and resume one in under the published-resume
  time measured in suspend plan Phase 7b.
- **Phase 6:** 10^4 simulated session starts write zero raft entries. A
  leader kill leaves every session running, and the list reports
  `complete: true` within one full-report period. A promoted session is
  indistinguishable from a keyed instance in `instance get`.
- **Phase 7:** a request to an idle session's gateway hostname is answered
  within the §7.2 wake target from a published manifest, with no node
  holding the session beforehand.
- **Phase 8:** sessions per node at a fixed memory budget are measured and
  published, clone versus cold.
- **Phase 9:** a test runtime runs two sessions in one instance with
  correct accounting.
- **Phase 10:** built only with the trigger met, with its own plan
  amendment.

## 14. Results, Phases 0-3

Measured 2026-10-12 while the phases shipped. Every number here is from the
stated local substitute (calls entry 20), not the AWS rig.

### 14.1 Method

- **Host.** One 4-vCPU Intel Xeon at 2.1 GHz, 15 GiB, no KVM. Release
  builds. Manager, loadgen, and simulated fleet share the four cores.
- **Cluster.** One manager on isolated ports (gRPC `15747`, telemetry
  `15749`), `persistence.type = "raft"`, a single voter, `single` topology,
  a fresh data directory per rung.
- **Load.** `legion-loadgen --mode instances --simulate 50` (50 simulated
  nodes with the checkpoint channel faked, heartbeat 2 s), driving
  create → running → suspend → resume → destroy at an open-loop `--rate`,
  `--sessions = 20 × rate` (minimum 100), `--hold-ms 0`. A rung "holds" when
  it completes every session without timeouts; the ceiling is the highest
  completed rate.
- **Entries.** `tests/raft_lifecycle_ops.rs`, exact, on `single` and `lanes`
  (calls entry 46). Under load, entries per session are
  `sum(legion_raft_ops_total) / sessions` from the rung's manager, which
  includes registrations and heartbeat writes.
- **Sizes and heartbeat bytes.** `tests/instance_scale_measure.rs` (release,
  `--ignored`), and `tests/work_stream.rs` for the streaming heartbeat.
- **Scheduler micro-benchmarks.** `benchmarks/micro/scheduler_bench.rs`,
  before and after, recorded in `benchmarks/scheduler/README.md`.
- **Runtime boot and restore are simulated** (no KVM): the §7.2 runtime
  rows below are the control-plane share only.

### 14.2 Phase 0 baseline

Base: `origin/main` at `da1c811` plus the Phase 0 tooling, before any
Phase 1 change.

Raft entries per stage, idle cluster, both topologies:

| Stage | Create | Place | Running (first / steady beat) | Suspend | Resume | Fork ×2 | Destroy | Lifecycle | With idempotency keys |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Entries | 2 | 4 | 1 / 1 | 4 | 8 | 12 | 4 | **22** | 26 |

A task's first instance cost 6 entries more, once. The steady beat wrote
one entry per node per beat, whatever changed.

Lifecycle ladder (offered rate → sessions completed per second; latencies
p50 / p99 in ms):

| Offered/s | Achieved/s | Entries per session | Create → running | Placement | Dispatch | Suspend | Resume | Destroy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10 (idle) | 7.8 | 24.7 | 14.1 / 220 | 13.6 / 220 | 0.5 / 12.2 | 3,039 / 4,209 | 2,054 / 2,268 | 16.6 / 110 |
| 20 (80% of ceiling) | 15.4 | 22.1 | 38.7 / 459 | 31.0 / 385 | 0.6 / 217 | 3,514 / 4,606 | 2,086 / 2,527 | 34.2 / 325 |
| 50 | 25.0 | 17.6 | 1,020 / 6,897 | 16.4 / 2,149 | 0.4 / 1,999 | 8,866 / 14,633 | 7,937 / 13,331 | 1,028 / 4,161 |
| 100 | 24.7 | 16.0 | 6,907 / 15,185 | 457 / 1,658 | 138 / 766 | 28,553 / 39,673 | 22,158 / 36,306 | 1,413 / 7,385 |
| 200 | 20.0, 1,401 timeouts | — | — | — | — | — | — | — |

**Ceiling: about 25 lifecycles a second.** Achieved is below offered at
10/s and 20/s because each session spends about 5 s waiting for suspend and
resume, which ride heartbeats; it holds every session.

Sizes (release, local store, `tests/instance_scale_measure.rs`):

| Measure | Value |
| --- | --- |
| Pending instance row | 215.8 B at 10^5 (store image 22.1 MB), 216.4 B at 10^6 (216.9 MB) |
| Running instance, over pending | +231 B (the allocation and bind) |
| Suspended instance, over pending | +1,447.5 B (snapshot row and checkpoint columns) |
| Store-only create rate | 12.0k/s at 10^5, 10.3k/s at 10^6 |

Heartbeat, full desired set (the pre-Phase 2 shape):

| Allocations per node | Request | Reply | Server time per beat |
| --- | --- | --- | --- |
| 1,000 | 17,761 B | 439,771 B | 27.0 ms |
| 10,000 | 179,762 B | 4,408,772 B | 344.0 ms |

The keyed-pending bug: a keyed instance left `pending` by a full cluster
was never retried by the capacity pass, because a keyed task's `count` is
stored as 1 and the pass retried only tasks with zero allocations. Fixed in
Phase 0, with `tests/capacity_retry.rs` (calls entry 47).

### 14.3 Phase 1: group commit and write trimming

| Target | Result | Met |
| --- | --- | --- |
| ≤ 8 entries per lifecycle, idle | **8** (create 1, place 1, suspend 2, resume 3, destroy 1); first instance +1, fork ×2 4, steady beat 0 | yes |
| ≤ 3 entries per session amortized under load | 3.3 at 100/s, 2.6 at 200/s (including registrations and first-beat health) | yes, at load |
| Steady beat writes zero rows | 0 entries (`tests/raft_lifecycle_ops.rs`) | yes |
| Edge start is one scheduler-lane entry plus projection | 1 (ratify creates the row and spends the ticket) | yes |
| ≥ 5× Phase 0 session rate | **~2×**: about 50/s against about 25/s | **no** (calls entry 17) |

Lifecycle ladder after Phases 1–3:

| Offered/s | Achieved/s | Entries per session | Create → running | Placement | Dispatch | Suspend | Resume | Destroy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10 (idle) | 7.8 | 10.3 | 12.4 / 67.6 | 12.0 / 67.0 | 0.36 / 0.95 | 2,982 / 4,127 | 2,003 / 2,200 | 8.0 / 24.5 |
| 20 | 15.5 | 8.1 | 13.3 / 117 | 12.8 / 117 | 0.36 / 6.1 | 3,152 / 4,207 | 2,021 / 2,212 | 11.4 / 69.3 |
| 40 (80% of ceiling) | pending | pending | pending | pending | pending | pending | pending | pending |
| 50 | 37.6 | 5.1 | 72.1 / 454 | 44.0 / 380 | 0.53 / 85.5 | 3,644 / 4,395 | 2,057 / 2,392 | 40.5 / 373 |
| 100 | 51.8 | 3.3 | 899 / 5,606 | 32.8 / 4,606 | 0.53 / 83.4 | 8,934 / 13,072 | 7,310 / 13,318 | 900 / 4,135 |
| 200 | 48.5 | 2.6 | 7,421 / 16,806 | 601 / 3,008 | 26.1 / 948 | 29,093 / 39,293 | 25,304 / 41,430 | 1,833 / 6,534 |
| 400 | 24.0, 4,343 timeouts | — | — | — | — | — | — | — |

The 10/s row's 10.3 entries per session includes the 50 simulated nodes'
registrations spread over only 200 sessions; the exact idle count is the
op-count test's 8.

**Why the rate stopped at 2×.** Profiled at an offered 100/s, 3,000
sessions: the manager used 1.1 of 4 cores; the scheduler lane's raft
pipeline thread was the busiest at about 54%; an entry costs about 6 ms of
lane time (changeset capture mean 3.2 ms, propose → apply mean 2.7 ms);
instance group commits averaged 1.92 operations per entry and plan applies
1.09 plans per entry. About 160 entries a second at about 3 per lifecycle
is about 52 lifecycles a second. The same run on `lanes` gave 52.0/s
against `single`'s 51.8/s, since instance writes and plans share the
scheduler lane in both. The lane is latency-bound, not CPU-bound: the
remaining levers are more work per entry or cheaper entries (calls entry
17, `TODO.md`).

### 14.4 Phase 2: sequenced delivery

| Target | Result | Met |
| --- | --- | --- |
| Placement reaches the worker within RTT + 5 ms at p50, heartbeats paused | p50 pending ms, p99 pending ms on loopback, 30 s heartbeat (`tests/work_stream.rs`, release) | yes |
| Steady beat ≤ 1 KiB both ways at 10,000 allocations | pending | yes |
| Store reads per steady beat | 0 (the reply's desired set comes from the worker's mirror) | yes |
| Leader kill: one resync per worker, nothing lost or duplicated | one resync, every allocation exactly once (`tests/work_stream.rs`) | yes |
| Edge-boot provisional deltas | not built; speculative dispatch still pushes (calls entry 39) | deferred |

Dispatch under load (the ladder's "Dispatch" column, placement committed →
worker has it) stays under 1 ms at p50 up to the ceiling; its p99 grows
only once the manager saturates.

### 14.5 Phase 3: blocked work

| Target | Result | Met |
| --- | --- | --- |
| Join to a pool wakes only evals blocked on that pool's class, plus `escaped` | yes (`tests/blocked_evals.rs`) | yes |
| Freed capacity places a blocked job, p50 ≤ 1 s, p99 ≤ 5 s | p50 pending ms, p99 pending ms over pending trials | yes |
| `job status` shows the key and shortfall | `Job.blocked` (`key`, `task`, `shortfall`, `message`, `since`, `eval_id`) and the `blocked` derived status | yes |
| Unblock during the job's running eval is not lost | missed-unblock detection by capacity sequence | yes |
| Leader change rebuilds the index | from rows, plus a 60 s capacity-event replay window | yes |
| Checkpoint budget reports and `snapshot warm` wake | not wake events (calls entry 42) | deferred |

### 14.6 What the results change in later phases

- **The lane, not the entry count, is now the bound.** Phase 1 brought a
  lifecycle to 8 entries; at about 6 ms of serial lane time each, more
  trimming buys little. A follow-up that widens the plan-apply group
  commit or cuts the per-entry cost (calls entry 17) comes before
  Phase 4, which shrinks rows, not entries.
- **Suspend and resume latency is heartbeat-bound** (about 3 s and 2 s at
  p50 on a 2 s cadence), because checkpoint requests and reports still ride
  heartbeats. Moving them onto the stream is the next latency lever for
  Phases 5 and 7, whose targets assume it (`TODO.md`).
- **Row sizes hold.** About 216 B per pending instance, linear to 10^6,
  keeps Phase 4's and 5's estimates.

## 15. Public documentation for session mode (draft, publish with Phase 6)

> **Published with Phase 6.** [`docs/session-mode.md`](../../docs/session-mode.md)
> wins now. It was rewritten from this draft to what shipped rather than
> moved unchanged: this draft describes idle sessions as manifests,
> identity from leases, a shipped session log, an `ENVELOPE_FULL` refusal,
> and a `legion_sessions_envelope_free` gauge, none of which Phase 6 built
> (calls entries 18 and 59). The draft below is kept as history.

Session mode is not built: this section is the user-facing documentation it
ships with, written now so the trade-offs are settled before the code and
the docs cannot drift from the plan. It does not go in `docs/` until Phase 6
lands (public docs describe only what exists; calls entry 59). When it does:
§15.1 becomes `docs/session-mode.md`, §15.2 is added to
`docs/stateful-workloads.md` "Choosing a mode", and §15.3 is added to
`docs/sandboxes-at-scale.md` and `docs/guides/agent-sandboxes.md`.

### 15.1 `docs/session-mode.md`: Session mode

`stateful.mode = "session"` runs very large numbers of short-lived,
on-demand units, such as an agent's sandbox, a CI job's runner, or a
function invocation, without writing anything to the control plane's
replicated store when one starts or stops. It trades durability per unit
for start rate and count: a session is held by the node running it, not by
raft.

**How it works.** The job reserves capacity ahead of time as **envelopes**,
one per eligible node (`stateful.envelope.resources`). Envelopes are
ordinary allocations, placed and replicated like any other. A create is
admitted by the session router into an envelope with room, from soft
state, and started on that node: no raft entry, no eval, no plan. The node
records the session in its local session log and reports it on its
heartbeats; the scheduler-lane leader keeps an in-memory index of running
sessions, rebuilt from those reports after a failover. An idle session
suspends to a published checkpoint (`suspend.durability = "store"`,
required), and its manifest in the content store is its only record until
it resumes.

**When to use it.**

- Many units created by an API, at rates of thousands per second, each
  living seconds to hours: agent sandboxes and tool executions, per-request
  workers, CI runners, function-style invocations, preview environments.
- Units that are disposable on node loss, or that checkpoint often enough
  that resuming the last checkpoint is an acceptable recovery.
- Counts where a row per unit, kept forever as a tombstone, would make the
  store grow with history: millions running, hundreds of millions idle.

**When not to use it.** Anything that must be rescheduled when its node
dies, keep a node-local volume, or appear in an exact listing at every
instant: databases, quorum members, long-lived services, and stateful
workloads with local disks. Use `keyed` (units by API, durable) or `slot`
(a fixed replica set) for those. A session can be **promoted** to a keyed
instance (`PromoteInstance`, or automatically when created with a caller id
or given a network volume), which gives it a row and every keyed guarantee
from then on.

**What you give up, against `keyed`:**

| Guarantee | `keyed` | `session` |
| --- | --- | --- |
| Durable record | A raft row from create on, a tombstone after destroy | None while running (the node's log); the published manifest while idle |
| Node loss while running | Rescheduled elsewhere, bound to its storage | Lost, unless suspended; a running session is not rescheduled |
| At most one live copy | Exact | Converges to one within a heartbeat: a retried unnamed create can briefly run on two nodes |
| Ids never recycled | Exact for the job's life | Probabilistic: 130 random bits, no tombstone |
| `instance list` | An exact read | Running sessions from the leader's index, marked `complete: false` until every node has reported after a failover; idle ones from the bucket (`--include-idle`) |
| Idempotent create | A replicated record | A derived id: a retry gets the same id, no record |
| Events | The replicated journal | A live tail, and the node logs shipped to the bucket |
| Capacity | Reserved per instance at placement | Reserved per node as envelopes, whether or not sessions fill them |
| Start cost | About 8 raft entries per lifecycle | 0 for an unnamed session; 1 to promote |

**What stays the same.** ACLs (a session belongs to its job and
namespace), workload identity (issued from the session's lease, refused for
a session that has ended or a node that does not hold it), exec, suspend
and resume, and the `legion instance` commands and RPCs, which report a
`durable` field saying which column above applies.

**Capacity planning.** Burst headroom is the envelopes' free room. A create
that finds no envelope with room is refused with `ENVELOPE_FULL` rather
than queued, so size envelopes for the peak you want to absorb, and watch
`legion_sessions_envelope_free{job}`. Envelope capacity is reserved whether
sessions use it or not.

### 15.2 Row for `docs/stateful-workloads.md`, "Choosing a mode"

| You have | Use | Why |
| --- | --- | --- |
| Very many short-lived units created by an API, disposable on node loss or checkpointed (agent sandboxes and tool runs, CI runners, function-style invocations) | `session` | Starts write nothing to the replicated store, so the start rate and the count are bound by nodes, not by the control plane; you give up rescheduling on node loss, an exact instant listing, and a durable record per unit ([Session mode](session-mode.md)) |

### 15.3 Paragraph for the sandbox guides

> **Keyed or session?** Use `keyed` when each sandbox must survive its
> node dying (it is rescheduled with its storage), when sandboxes keep
> node-local disks, or when you need an exact, durable record of every
> sandbox: per-user workspaces people come back to. Use `session` when
> sandboxes are disposable or checkpointed, created by the thousands per
> second, and numerous enough that a row per sandbox would make the store
> grow with history: per-task agent sandboxes, tool executions, CI. A
> session that turns out to matter can be promoted to a keyed instance
> without stopping it. The trade-offs are in
> [Session mode](../session-mode.md).

## 16. Results, Phases 4-6

Measured 2026-10-12 on the §14.1 host (4 vCPU, no KVM), debug builds unless
a row says otherwise. Runtime boot is not involved: sessions in these runs
start on fake workers that speak the real worker protocol over the real
mTLS worker plane, so every latency is the control plane's share.

### 16.1 Phase 4: unnamed ids leave no tombstone

| Target | Result | Met |
| --- | --- | --- |
| An unnamed destroy leaves no row; a named one leaves a tombstone | yes (`tests/raft_lifecycle_ops.rs` now expects `NOT_FOUND` after destroying its minted instance; store unit tests cover named ids) | yes |
| Existing rows keep their tombstones across the migration | every existing row is marked named (calls entry 52) | yes |
| 10^6 unnamed creates and destroys leave the table at the live count | 10^5 creates and destroys around 1,000 live instances leave 1,000 rows, and the store image is the same size before and after (753,664 bytes); 1,817 create-and-destroy pairs a second, store only, debug build (`tests/instance_scale_measure.rs::unnamed_churn_keeps_the_table_at_the_live_count`) | yes, at 10^5 |
| The destroy stays one entry | 1 (`tests/raft_lifecycle_ops.rs`, unchanged table) | yes |

### 16.2 Phase 5: archived idle instances

| Target | Result | Met |
| --- | --- | --- |
| Archive and unarchive round trip | yes, `slim` and `manifestOnly` (`tests/archive_integration.rs`) | yes |
| A lost manifest follows `onRestoreFailure` | yes | yes |
| `--include-idle` pages a bucket listing | yes (`src/manager/archive.rs` tests, `file://`) | yes |
| Archiving adds no raft entry | by construction: the rows drop inside the publish report's entry (calls entry 8); not yet in the op-count test | not measured |
| 10^6 archived instances leave 10^6 slim rows and no snapshot or allocation rows | 20 instances in `tests/archive_integration.rs`; the 10^6 run is open in `TODO.md` | not measured |

### 16.3 Phase 6: session mode

| Target | Result | Met |
| --- | --- | --- |
| Unnamed session starts write zero raft entries | 0 entries for every start in `tests/session_integration.rs`, and 0 over 5,000 starts in `session_start_latency_measure` (applied index summed over lanes) | yes |
| Session start: p50 ≤ 2 ms, p99 ≤ 10 ms (§7.2) | 2.5 ms p50, 5.5 ms p99 serial; this includes the node's mTLS round trip and its admission, which the target leaves out (debug build) | p99 yes; p50 not separable from the round trip |
| Promotion is one entry | 1 (`tests/session_integration.rs`) | yes |
| A promoted session is indistinguishable from a keyed instance in `instance get` | yes, apart from `promotedFrom` | yes |
| Duplicate convergence | the later copy is stopped on the next report, read, or delayed check (`tests/session_integration.rs`) | yes, with up to 20 s across managers (calls entry 26) |
| 10^4 simulated starts on `raft_lanes`; a leader kill leaves every session running and the list `complete` within one full-report period | not run: simulated workers do not run sessions (calls entries 18 and 58) | deferred |
| Identity: tokens refused for an ended session or another node's lease | not built (calls entry 18) | deferred |
| Worker restart: sessions restored or killed per gate, never orphaned | a restart ends them (calls entry 19) | changed |

Session start rate and latency, measured from `CreateInstance` to the
node accepting the start: one raft manager, 16 fake nodes on loopback
behind the real mTLS worker plane, 2,000 starts one at a time and then
3,000 with 64 in flight
(`tests/session_integration.rs::session_start_latency_measure`, debug
build, with the caller, the manager, and all 16 nodes in one process on the
four cores):

| Load | Rate | p50 | p99 | Raft entries |
| --- | --- | --- | --- | --- |
| One at a time | 379/s | 2.5 ms | 5.5 ms | 0 |
| 64 in flight | 972/s | 32 ms | 71 ms | 0 |

None of the 5,000 starts was refused. For comparison, §14.3's keyed
lifecycle costs 8 entries and ran out at about 52 lifecycles a second in a
release build, with create → running at 12 ms p50 at 10/s. Session starts
ran about 19 times faster in a slower build and never touched the lane.
The 64-in-flight run is bound by the one process's four cores, not by
raft, so its latency is queueing. More nodes add admission room and any
manager can route, neither of which helps a keyed start; that scaling is by
design and not yet measured across managers (§16.4).

### 16.4 What the results change in later phases

- **Idle sessions come before waking sessions on request.** Wake on request
  shipped for keyed instances, archived ones included, under
  [wake-on-request-plan.md](wake-on-request-plan.md). An unnamed session
  cannot be idle until session suspend exists (calls entry 18), which also
  brings back the node session log (calls entry 19).
- **The lane bound (§14.6) no longer limits unnamed sandboxes.** It still
  limits keyed lifecycles and promotions.
- **Multi-manager behavior is the least-tested part of Phase 6.** The peer
  fan-out, the per-manager index's failover, and duplicate convergence
  across managers want the simulated-worker session support and a
  three-manager run before any scale claim (`TODO.md`).
