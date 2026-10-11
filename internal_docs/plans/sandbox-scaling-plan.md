# Plan: Sandbox Scaling to Very Large Session Counts

> **Judgement calls:** [`calls/sandbox-scaling-plan_calls.md`](../calls/sandbox-scaling-plan_calls.md)

**Status: PLANNED** (written 2026-10-11). Nothing here is built. Phase 0 is a
gate: it measures what every later phase assumes, and its numbers can
reorder or cancel phases. Deliberately deferred and named: cell sharding of
promoted instances (Phase 7, built only past a measured trigger), a
pluggable external session registry (rejected as a default, §4), sub-VM
isolation tiers inside Legion's own runtimes (Phase 6 ships the seam, not a
runtime), and cross-cluster session routing (owned by
[multi-cluster-federation-plan.md](multi-cluster-federation-plan.md)).

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
  makes gated networking the sandbox default with memory checkpoints. §6.9
  says what a session changes in the datapath's keying.

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
memory pages, Phase 5), with a seam for lighter isolation tiers inside an
envelope later (Phase 6). Cell sharding is built only if promoted
instances outgrow one raft group (Phase 7).

Eight phases, each shippable alone (§9). Phase 0 measures. Phase 1 is
group commit and write trimming inside today's model. Phases 2 to 4 move
history, idle state, and then running state off raft. Phases 5 to 7 are
density and the conditional sharding step.

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
  - **Take:** the whole shape, which is this plan's Phase 4.
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

**Cluster operator**
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
  history or idle sessions (Phases 2 and 3).
- Offer an opt-in session mode whose start, suspend, resume, and stop
  write nothing to raft (Phase 4).
- Keep every guarantee of `slot` and `keyed` instances, with one stated
  exception: Phase 2 makes server-minted keyed ids unique by randomness
  instead of by tombstone (calls entry 3). State precisely which
  guarantees the new mode weakens (§6.7).
- Keep one binary and no new required dependency. The content store a
  worker already uses for published checkpoints is the only external
  storage.

**Non-goals**
- **Billions of running sessions in one cluster.** That is about 10 PB of
  RAM at an unrealistically small 10 MiB each. The honest target is
  millions running and hundreds of millions idle per cluster (§7), and
  more through federation.
- **A pluggable external session registry** (DynamoDB, FoundationDB,
  Spanner) as a default or a Phase. It turns a self-hosted scheduler into
  a distributed system with a cloud dependency (calls entry 8).
- **Gossip.** Managers already receive every heartbeat.
- **Changing `keyed` or `slot` semantics.** Existing jobs keep exact
  listing, tombstones, and at-most-one.
- **New placement intelligence.** Envelopes are placed by today's
  scheduler. The session router is first fit within envelopes.
- **Cross-cluster routing of sessions.** Federation owns it.

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

### 5.6 The worker's local state

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
| 1 | Ephemeral by default, promote on demand | 0 until promoted, 1 to promote | Never-recycled becomes probabilistic for unnamed ids. At-most-one becomes converge-to-one | **Take** (Phase 4) |
| 2 | Nodes are the source of truth for running sessions | 0 | `list` is eventually consistent. The index is rebuilt after failover | **Take** (Phase 4) |
| 3 | Idle sessions are manifests in object storage | 0 per idle session | Listing idle sessions is a bucket listing | **Take** (Phase 3) |
| 4 | Cell sharding by id prefix across N raft groups | unchanged, but spread | Scatter-gather lists, cross-cell moves | **Defer** (Phase 7, behind a trigger) |
| 5 | Group commit and write-behind | fewer, larger entries | Nothing, if done as group commit only | **Take first** (Phase 1) |
| 6 | Pluggable external registry | 0 in raft, 1+ in the registry | Self-hosted simplicity | **Reject** as a default |
| 7 | Sessions inside allocations (two-level) | 0 | Isolation, when sessions share a VM | **Take the envelope**, defer sub-VM tiers (Phases 4, 6) |
| 8 | Snapshot-clone at the edge | 0 | Nothing in the control plane | **Take** (Phase 5) |

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
  unchanged (calls entry 5).
- **Idle manifests land before ephemeral running sessions.** Phase 3 works
  for today's keyed instances with `durability = "store"`, and idle
  sessions are the larger count. It ships value without a new mode
  (calls entry 4).

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
kept. Caller ids may not take that shape (Phase 2's reserved-pattern
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
with many uses is the session-mode shape and spends once (calls entry 10).

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

**For keyed instances (Phase 3, before session mode exists).** A suspended
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
today's boundary. Calls entry 12 records the trade.

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
  connect) is this plan's density lever (Phase 5), and diff snapshots are
  what make a hundred million idle manifests affordable in a bucket.

### 6.10 Density by isolation tier

- **Tier 1, one microVM per session, shared pages (Phase 5).** Every
  session in an envelope restores from one warm base checkpoint, maps its
  memory `MAP_PRIVATE` (already true for forks on Firecracker), and loads
  pages lazily through UFFD. Diff snapshots make suspended sessions small.
  KSM is an operator opt-in for nodes that want it. Legion keeps charging
  each session its full `resources.memory`, as forks are charged today,
  and lets oversubscription spend the measured sharing.
- **Tier 2, many sessions per VM (Phase 6, seam only).** A task runtime can
  declare `sessionsPerInstance > 1`, and the worker hands sessions to an
  existing runtime instance (processes, isolates, WASM) through a new
  `TaskPlugin::start_session` call instead of booting one each. Legion
  ships the seam and the accounting, not a runtime, and the public doc
  states plainly that sessions sharing a VM share a kernel.

### 6.11 Cells, only if needed (Phase 7)

If promoted instances or envelopes outgrow one raft group, partition
`instance` rows by a hash of the id across N scheduler-lane groups
("cells"), each with its own pipeline. Multi-raft already runs several
groups over one transport, and table ownership is per lane. The cost is
scatter-gather for exact lists and a two-cell move for a rename. The
trigger is measured, not guessed: build it when the Phase 0 harness shows
the scheduler lane above 70% busy from instance writes at the target
rate, or the store above 4 GiB from instance rows.

## 7. Target numbers per phase

Targets assume the AWS rig's control plane (3 × m6id.xlarge, `lanes`
topology), Firecracker sandboxes, and a fleet sized to host the running
count. Phase 0 replaces every "today" estimate with a measurement, and any
target Phase 0 shows to be wrong is rewritten here with a calls entry, not
quietly dropped.

| Phase | Raft entries per lifecycle | Sustained session starts/s (cluster) | Running sessions | Idle sessions | Raft rows per idle session |
| --- | --- | --- | --- | --- | --- |
| Today (estimate) | ~22 (26 with keys) | low thousands at best, shared with all other placements | ~10^5 to 10^6 (store and snapshot size) | same pool as running | 2 to 3 (instance, snapshot, optional reserve) + tombstone forever |
| 0: measure | measured | measured | measured bytes per instance row | measured | measured |
| 1: group commit and trims | ≤ 8 idle cluster, ≤ 3 amortized under load | ≥ 5× Phase 0 | unchanged | unchanged | unchanged |
| 2: no tombstones for unnamed ids | unchanged | unchanged | rows track live + suspended, not history | unchanged | unchanged |
| 3: archived idle instances | +1 to archive, +1 to unarchive | unchanged | unchanged | ≥ 10^7 keyed per cluster | 1 slim row (named), 0 (`manifestOnly`) |
| 4: session mode | 0 for unnamed, 1 to promote | ≥ 20,000 (bound by node boot rate and router CPU) | ≥ 10^6 per leader index (~200 B per entry) | ≥ 10^8 (bucket bound) | 0 |
| 5: snapshot-clone density | 0 | restore-bound, ≥ 100 restores/s per node | 5× to 10× more per node than cold boots, measured | smaller manifests via diff snapshots | 0 |
| 6: sub-VM tier seam | 0 | runtime-bound | 10^3 to 10^4 per node, runtime-dependent | as Phase 4 | 0 |
| 7: cells (conditional) | as Phase 1, per cell | named-instance writes scale with cell count | as Phase 4 | as Phase 4 | 0 |

A billion sessions on Legion therefore means federation of clusters at
Phase 4 or later, each holding millions running and hundreds of millions
idle, with the idle fraction set by the platform's activity rate. Phase 4's
index target is per leader, and a 10^7 index is about 2 GiB of manager
memory, which is where Phase 7's sharded index (the same cell split,
applied to the soft index) would start.

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
`suspend.archive = "slim" | "manifestOnly"` for keyed tasks (Phase 3).

**Manager config:** `sessions { routerEnabled = true, fullReportEvery = 30,
indexMaxEntries = 2000000 }`.

**Worker config:** `sessions { logPath, shipLogEvery = "5m" }`. Session
mode requires `contentStore`.

**Persistence:**
- Phase 2: `instance` gains `named BOOLEAN`. Server-minted ids are not
  `named` and leave no tombstone. Existing rows are `named = 1`, so nothing
  already tombstoned is released.
- Phase 3: `instance.state = 'archived'` plus `manifest_url`. Archiving
  deletes the snapshot row and any held reserve row in the same apply.
- Phase 4: no new replicated table. Envelope allocations are ordinary
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
  - Numbers go in `benchmarks/scheduler/README.md`. This phase can reorder
    the rest: if Phase 1 alone reaches the platform's target rate, Phase 4
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
- **Phase 2: no tombstones for unnamed ids.** Server-minted ids grow to
  130 bits and leave no row on destroy. Caller ids keep tombstones. Caller
  ids may no longer match the server-minted shape `{task}-{26 base32}`,
  the same kind of reserved-pattern rule slot ids already have, so nobody
  can re-create a dead unnamed id by typing it. Raft size then tracks live
  and suspended instances.
- **Phase 3: archived idle instances.** `suspend.archive` for keyed tasks
  with `durability = "store"`, the slim row, manifests in the bucket, and
  `--include-idle` listing. Works without session mode.
- **Phase 4: session mode.** Envelopes, the session router, leases, the
  worker session log, heartbeat deltas and full reports, the leader index,
  `PromoteInstance`, identity from leases, derived-id idempotency, the
  session log shipper, CLI and MCP fields (`durable`, `complete`),
  `catalog()` entries, and `docs/agent-readiness.md`.
- **Phase 5: snapshot-clone density.** Executes suspend plan Phase 8's
  UFFD lazy restore and diff snapshots for session envelopes, a warm base
  checkpoint per envelope, and density measurements per node.
- **Phase 6: sub-VM tier seam.** `TaskPlugin::start_session`,
  `sessionsPerInstance`, accounting, and the isolation warning in docs.
- **Phase 7: cells (conditional).** Built only past the §6.11 trigger.

## 10. Testing plan

- **Phase 0** is itself the test harness: the op-count test fails when a
  change adds an entry to a lifecycle stage, so every later phase shows its
  delta in CI.
- **Phase 1:** the stateful, edge-boot, suspend, and fork suites run
  unmodified. New unit tests: an unchanged allocation-health beat produces
  an empty changeset. A ratification that creates its row refuses a
  tombstoned or existing id. Coalesced creates isolate a failing request
  (per-request savepoints, like intake).
- **Phase 2:** an unnamed destroy leaves no row, a named destroy leaves a
  tombstone, existing rows keep theirs across the migration, and a
  mixed-version cluster replays the column-appended migration.
- **Phase 3:** archive and unarchive round trip, the slim row refuses a
  duplicate create, a lost bucket object fails resume with
  `onRestoreFailure`, and `--include-idle` pages through a bucket listing.
- **Phase 4:**
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
- **Phase 5:** ignored KVM tests for UFFD restore and diff snapshots, plus
  a density benchmark that records sessions per node at a fixed memory
  budget, cold versus clone.
- **Benchmarks:** the Phase 0 matrix re-run after Phases 1, 3, and 4, with
  before and after numbers in each PR, per the `AGENTS.md` scheduler rule.

## 11. Rollout and backward compatibility

- **Phases 0 and 1 change no surface.** They are internal and default on,
  guarded by the op-count test and the unmodified suites.
- **Phase 2 changes no existing row.** Only instances minted after the
  upgrade can be untombstoned, and only unnamed ones. The public doc
  states that the never-recycled guarantee for server-minted ids becomes
  probabilistic from that release on. That is a change to a documented
  guarantee, so it ships with an upgrade note in `docs/upgrades.md`.
- **Phases 3 and 4 are opt-in per job** (`suspend.archive`,
  `stateful.mode = "session"`). Nothing changes for jobs that do not set
  them.
- **Version skew.** Envelope allocations are refused by workers that do
  not advertise the `sessions` capability, and the scheduler does not
  place envelopes on them. A manager that does not understand session
  heartbeat fields ignores them, which is safe because sessions are
  invisible to it but keep running. Upgrade managers first, per
  `docs/upgrades.md`.
- **Downgrade.** A cluster that downgrades after enabling session mode
  loses its session index (sessions keep running inside envelopes until
  their nodes restart) and cannot read archived rows. The upgrade doc says
  so.

## 12. Open questions

- **Is the leader the right home for the session index (lean: yes, for
  Phase 4)?** It matches liveness and the location index. The cost is
  that index memory scales with running sessions on one process. A
  sharded index across managers is the Phase 7 cell split applied to soft
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
- **What triggers cells (lean: the §6.11 thresholds)?** Measured in Phase 0
  and re-checked after Phase 4.

## 13. Acceptance criteria

- **Phase 0:** `benchmarks/scheduler/README.md` records raft entries per
  lifecycle stage for `single` and `lanes`, sustained create, suspend, and
  resume rates on the AWS rig or a stated local substitute, and bytes per
  instance row. The op-count test runs in CI.
- **Phase 1:** the op-count test shows ≤ 8 entries per lifecycle on an
  idle cluster. A 10,000-allocation node's steady beat writes zero rows.
  An edge start writes one scheduler-lane entry plus its projection. The
  session rate is at least 5× Phase 0's.
- **Phase 2:** a churn test of 10^6 unnamed creates and destroys leaves the
  instance table at the live count.
- **Phase 3:** 10^6 archived keyed instances leave 10^6 slim rows and no
  snapshot or allocation rows, and resume one in under the published-resume
  time measured in suspend plan Phase 7b.
- **Phase 4:** 10^4 simulated session starts write zero raft entries. A
  leader kill leaves every session running, and the list reports
  `complete: true` within one full-report period. A promoted session is
  indistinguishable from a keyed instance in `instance get`.
- **Phase 5:** sessions per node at a fixed memory budget are measured and
  published, clone versus cold.
- **Phase 6:** a test runtime runs two sessions in one instance with
  correct accounting.
- **Phase 7:** built only with the trigger met, with its own plan
  amendment.
