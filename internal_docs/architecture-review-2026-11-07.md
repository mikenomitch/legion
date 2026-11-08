# Architecture review against best in class (2026-11-07)

A design-level review asking where Legion's choices lag the best current
practice, not whether the code is correct. Four questions:

1. Is networking or the mesh doing anything that will slow it down?
2. Is the scheduler doing anything slow?
3. Which trade-offs make Legion less AI-native or less future-proof, for
   agents as operators and for AI workloads?
4. Where is it not pluggable enough for some workloads?

Five reviewers each took one area: the networking datapath and mesh, the
scheduler and eval path, the state store and raft, plugin seams, and AI
readiness. Each compared Legion with named systems (Cilium, Istio ambient,
Envoy xDS, Nomad, Kubernetes and its scheduler framework, Kueue, Borg,
etcd, CockroachDB, CRI/CNI/CSI, proxy-wasm, E2B, Modal). The first pass ran
on `0f5672a` and `8a3645f` (2026-10-19). Every finding was then re-checked
on `9ded26d` (2026-11-07), after the apps, app object kinds, autoscaler v2,
and park merges. File and line references are from that commit and may
drift.

Unlike the [2026-10-09 audit](architecture-audit-2026-10-09.md), which looked
for bugs, most of these findings are about performance ceilings or missing
seams. Each one says whether `TODO.md` already tracked it. Every untracked
finding was added to [TODO.md](TODO.md) with this document, and each new
entry cites its finding ID.

## The short version

- **The core design holds up, and parts of it lead the field.**
  - Late-binding plan/apply measured 0% conflicts, where a conflict rejects a
    whole Nomad plan.
  - The datapath uses real `splice(2)` and an eBPF classifier at `connect()`.
  - Mesh mTLS uses node identity, as ztunnel does.
  - Edge boot starts sandboxes without a raft write.
  - Fork, suspend, and park are first-class operations.
  - The exec-plugin contract is a good model.
- **The ceilings are in the write path, not the algorithms.** Placement costs
  milliseconds. The commit costs the rest:
  - one stop-and-wait pipeline per lane;
  - one capture mutex shared by every lane;
  - every write executed twice;
  - two fsyncs per entry.
- **The mesh's cost is in the control path.** Each node's endpoint feed and
  policy assignments are rebuilt from scratch on every heartbeat. The packet
  path is sound.
- **The plugin story does not yet match the "highly extensible" claim.** Five
  plugin kinds are Rust dylibs: in process, with no stable ABI and no panic
  containment. There are no seams for GPUs, CNI, admission policy on jobs, or
  OCI runtime classes.
- **Agents as operators have good mechanics but little governance.** Plan,
  idempotency, typed errors, and delegation for apps exist. A durable audit
  journal, token expiry by default, approval gates, rate limits, and
  compare-and-set on jobs do not.
- **AI workloads:** sandboxes are strong, GPUs are effectively unsupported,
  and the artifact cache is too small for model weights. Autoscaling was a
  gap and is now largely closed.

### What to do, in order

1. **Fix the replicated write path.**
   - Pipeline the lanes (tracked).
   - Stop executing each write twice.
   - Split the capture mutex per lane.
   - Prune `plan_submission` (SCH2).
   - Fix the claim query (SCH3).
2. **Push mesh endpoints as versioned per-service deltas**, separate from the
   heartbeat, and cache policy assignments (NET1, NET2).
3. **Score placements by requested resources**, not allocation counts (SCH4).
4. **Move dylib plugins out of process**, or onto the WASM layer network
   plugins already use. Then split the scheduling seam into
   filter/score/reserve/permit stages, chosen per pool or job (PLG1, SCH6).
5. **A first-class device model:** fingerprinting, CDI injection, health,
   topology, and quota (PLG2).
6. **Agent governance:**
   - an audit journal;
   - token expiry by default and attenuation;
   - `expected_version` on `RunJob`;
   - a current MCP revision with annotations and elicitation (AI2 to AI5).

### What changed between the two passes

| Area | Change since `8a3645f` | Effect on findings |
| --- | --- | --- |
| Autoscaling | Autoscaler v2: metric sources, strategies, cron, scale to zero, NodeGroups with an `aws-asg` target plugin | The "manual only" finding is largely fixed. Remaining gaps are in [docs/autoscaling.md](../docs/autoscaling.md) |
| Control-plane seams | App object kinds (Legion's CRDs) with JSON Schema and CEL `rules`, app controllers via `WatchObjects`, jobspec extensions | User-defined objects and controllers now exist. Admission policy on *jobs* still does not (PLG5) |
| Agent credentials | Delegation (`<viewer> via app <app>`), acting for an object, job grants up to the submitter's | Attenuation exists for workloads and apps, not for ACL tokens (AI3) |
| Raft write volume | Park's keyed suspends and wakes are scheduler-lane entries. App objects and the autoscaler add control-lane and status-lane writes on decisions | Raises the stakes of SCH1 |
| Networking | Only a rename in `kernel_guard.rs` | Every networking finding still holds |

## Networking and the mesh

The packet path is good for a userspace design:
- `splice(2)` is real, at about 0.1 CPU-s/GiB.
- The eBPF classifier passes allowed host-mode flows with no proxy hop.
- One certificate per node, not per workload, keeps churn low.
- Policy bodies are content-addressed, endpoint state fails static, and
  balancer picks cost nanoseconds.

The costs are on the manager side, in what every heartbeat recomputes.

### NET1. The endpoint feed is rebuilt from scratch on every heartbeat (high)

- **Today:** every heartbeat with connect allocations calls
  `build_upstream_endpoints` (`src/manager/server.rs:8963` → `:15281`).
  - It re-parses each allocation's `task_connect` JSON.
  - It runs one `service_endpoints` query per (service, consumer job)
    (`:15325`), and parses three JSON values per catalog row
    (`src/storage/store.rs:15929`, `:15935`, `worker_address_host`).
  - It memoizes only within the call, then clones the result into each
    allocation's list (`:15371`).
  - One node with 100 consumers of a 1,000-endpoint service ships about 100k
    endpoints per beat. The worker replaces every list wholesale
    (`src/worker/netd/datapath.rs:448`).
- **Convergence follows heartbeat cadence.** The cadence is `ceil(N/1250)` s
  (`src/constants.rs:150`): about 8 s at 10k nodes and 16 s at 20k. So
  backend up/down convergence slows as the fleet grows.
- **Best in class:** Envoy delta xDS, Kubernetes EndpointSlices, and Consul
  blocking queries push only changed, per-service, versioned sets, in under
  a second.
- **The mesh plan says this already shipped.**
  [service-mesh-plan.md](plans/service-mesh-plan.md) §2 marked "push
  endpoint deltas per service" as "kept, already shipped". It did not ship.
  This change corrects that row.
- **Direction:**
  - Keep a versioned endpoint set per service, updated on catalog writes.
  - Each node tracks the (service, version) pairs it needs and receives only
    changed sets on the work stream.
  - Deduplicate per node, not per allocation.
- **Tracked:** no.

### NET2. Policy assignments are recomputed on every heartbeat (high once any policy exists)

- **Today:** past its gate, `build_net_policies` (`server.rs:14405`) does
  this on every heartbeat:
  - lists every stored policy (`:14434`);
  - parses each allocation's `task_network`;
  - runs `merge_net_policies` per allocation (`:14481`).
- **Cost:** O(policies × allocations) per node per beat, whether or not
  anything changed. Only the merged bodies are cached, by hash.
- **Direction:** memoize the effective hash on (policy generation, selector
  facts, task network hash).
- **Tracked:** no.

### NET3. Every L4 mesh connection pays a full mTLS handshake and userspace copies on both nodes (medium-high)

- **Today:** a mesh connection takes three TCP connections and two userspace
  proxies: the client goes to the loopback forwarder, then over a new mTLS
  connection to the remote node's ingress, which dials the task.
  - TLS streams take the copy path, not splice (`src/worker/netd/splice.rs:12`).
  - The eBPF classifier passes upstream VIP traffic up to netd
    (`bpf/legion_net.bpf.c:299`), so the kernel fast path never covers mesh
    traffic.
  - `forwarder.rs:1283` rebuilds the rustls `ClientConfig` on every identity
    change, which throws away the session-resumption cache. So every 8-hour
    certificate refresh and every CA rotation causes a burst of full
    handshakes.
- **Best in class:** ztunnel pools HTTP/2 CONNECT (HBONE) tunnels between
  nodes. Cilium does L4 in eBPF with WireGuard or IPsec in the kernel.
- **Direction:** pooled tunnels, then kTLS. Keep the resumption store across
  config rebuilds, which is cheap and separate.
- **Tracked:** kTLS and multiplexed tunnels, yes (Networking). The
  resumption-cache loss, no.

### NET4. HTTP mode buffers request bodies with no cap (medium)

- **Today:** when no body-reading hop applies, `src/worker/netd/http.rs:962`
  runs `body.collect()` with no `Limited`. Only the body-hop branch is capped
  (`:981`).
- **Risk:** a large upload from any caller can exhaust the worker's memory,
  and every upload is delayed until it is fully read.
- **Best in class:** Envoy has a per-request buffer limit and streams by
  default.
- **Direction:** stream when no retry or body hop needs replay. Otherwise
  cap the buffer and return `413`.
- **Tracked:** no. The open plugin-streaming item covers plugins, not this
  case.

### NET5. IPv6 addresses are mishandled (medium)

- **Today:**
  - Endpoints are formatted `"{}:{}"` with no brackets
    (`src/worker/netd/specs.rs:234`).
  - `worker_address_host` splits on the last `:` (`store.rs:339`).
  - Ingress binds `0.0.0.0` by default (`src/worker/netd/ingress.rs:306`).
- **Effect:** an IPv6-only node most likely cannot take part in the mesh.
- **Direction:** use `SocketAddr` end to end, and bind dual-stack.
- **Tracked:** no.

### NET6. The nftables guard fallback walks a linear chain and forks `nft` every beat (low-medium)

- **Today:** where the eBPF classifier is unavailable, the nftables table
  has no `ct state established` short-circuit. So every egress packet walks
  one rule per allocation.
  - Counters are read by forking `nft -j list table` every heartbeat
    (`src/worker/netd/kernel_guard.rs:413`, called from `forwarder.rs:561`).
- **Direction:**
  - Accept established flows first.
  - Read counters over netlink, less often.
  - Or drop the nftables path, which is already an open decision.
- **Tracked:** the "drop nftables" decision is tracked. The fork and the
  missing short-circuit are not.

### NET7. CA rotation re-issues every node's certificate at once (low)

- **Today:** `refresh_node_identity` (`src/worker/daemon.rs:3320`) re-issues
  as soon as the bundle changes. The only spread is the heartbeat's ±10%
  jitter.
- **Effect:** about 10k CSRs land within one beat interval, which is a real
  load on an `exec` or Vault signer. The handshake spike from NET3 lands at
  the same moment.
- **Direction:** a splay proportional to fleet size.
- **Tracked:** no.

### NET8. Per-connection locks and allocations (low now; matters above ~50k connections/s per node)

- **Today:**
  - `EdgeLedger` is one node-wide `Mutex<HashMap>`
    (`src/worker/netd/graph.rs:42`).
  - The forwarder's in-flight map allocates a `String` key per connection
    (`forwarder.rs:165`, `:2745`).
  - Each copy direction gets a 16 KiB heap buffer (`splice.rs:253`).
- **Best in class:** Cilium keeps identities in O(1) maps.
- **Direction:** interned ids, plus sharded or atomic counters.
- **Tracked:** no.

### NET9. A node caps at 255 allocations with upstreams (low today, matters for dense sandbox nodes)

- **Today:** VIP blocks come from `127.42.b` with `VIP_BLOCK_MAX = 255`
  (`src/structs/network.rs:32`).
  - Upstream loopback ports and ingress twin ports share the 20000–32000
    range with task ports.
  - A dense sandbox node (the 96-vCPU numbers in
    [sandboxes-at-scale.md](../docs/sandboxes-at-scale.md)) can hit both.
- **Tracked:** no.

### Noted, not filed

- **NET10.** External upstreams resolve DNS on every connection with no
  cache (`src/worker/netd/handlers.rs:972`, `guard.rs:39`). This is
  deliberate, against DNS rebinding, and relies on the host's caching
  resolver.
- **NET11.** Mesh mTLS is off by default (`service.mesh = true` turns it on),
  where Linkerd and Istio ambient are on by default. This is a documented
  call in [mesh-interfaces-plan_calls.md](calls/mesh-interfaces-plan_calls.md)
  entry 8. Revisit it once NET3 makes the mesh hop cheap.
- **NET12.** There is no overlay and no per-allocation IP, and `bridge` and
  `cni/*` are rejected. This is a reasonable single-binary call, and it is
  tracked (Service discovery & the mesh, Plugin surface). It caps parity with
  Kubernetes and Nomad's CNI support: multi-NIC, SR-IOV, RDMA, and VPC-native
  IPs.

## Scheduler and eval path

**Current numbers.** [docs/benchmarks.md](../docs/benchmarks.md) (2026-10-10)
measured 905 placements/s at 5k nodes, falling to 327/s at 50k, and `apply` is
about 82% of plan time at 50k (TODO, Scheduling & placement). Planning is
cheap. Committing is the ceiling.

What is good:
- Late-binding candidate lists, with a two-pass bind and 0% measured
  conflicts.
- Epochs per job config and per node.
- O(1) snapshots from persistent maps.
- Group commit, with the eval settle fused into the apply.
- Eval claims held in leader memory.
- Declarable scheduling indexes.
- Blocked evals keyed by class, much like Kubernetes queueing hints.
- Preemption that commits atomically with the plan, and gang quorum.

### SCH1. The commit pipeline is stop-and-wait, under one cross-lane mutex, and runs each write twice (critical at scale)

- **Today:** each lane has one `pipeline_loop` thread
  (`src/storage/raft/driver.rs:2179`). For each entry it runs these steps
  strictly in sequence:
  1. capture the mutation;
  2. `block_on(client_write)` for the quorum commit and local apply (`:2094`);
  3. reply.
- **Shared across lanes:**
  - Every lane's capture and apply share one process-wide `CAPTURE_MUTEX`
    (`src/storage/raft/capture.rs:163`) and one SQLite write connection.
  - Snapshot install holds that mutex (`state_machine.rs:79`).
- **The leader runs every write twice:** it executes, extracts the changeset,
  rolls back, then applies the changeset on commit.
- **Fsyncs:** the raft log and the state DB both run `synchronous = FULL`
  (`log_store.rs:16`, `store.rs:2826`), so every entry costs two fsyncs on
  every node.
- **Best in class:** Nomad's plan applier overlaps the commit of plan N with
  checking plan N+1. etcd/raft batches and pipelines proposals. CockroachDB
  keeps many writes in flight under per-key latches.
- **Effect:** the `lanes` topology overlaps only the quorum waits. Local CPU
  and IO stay serialized across every lane.
- **Park adds pressure.** Every keyed park is a suspend and every wake a
  resume, and both are scheduler-lane entries (`src/storage/store/suspend_ops.rs`).
  They now compete with plans on the bottleneck lane.
- **Tracked:**
  - Pipelining, one fsync per entry, and fewer round trips per eval are
    tracked (Scheduling & placement).
  - Executing twice is named in an existing item but has no item of its own.
  - The cross-lane mutex is new.
- **Direction:** keep the leader's captured writes instead of re-applying
  them, and give each lane its own mutex and write connection. Both
  compound with pipelining.

### SCH2. `plan_submission` rows are never deleted (high, grows without bound)

- **Today:** nothing in `src/` deletes or prunes `plan_submission`.
  - Each row carries every placement's 8 ranked candidates as JSON
    (`DEFAULT_COUNT = 8`, `src/manager/scheduler.rs:316`).
  - Every row goes through the raft log and into every snapshot.
- **Effect:** at 1k plans/s, that is about 86M rows a day. It inflates
  snapshot size, install time, and the memory that T4 holds.
- **Direction:**
  - Delete the row on apply.
  - Keep only rejected or sampled plans, for diagnosis.
  - Or keep it out of the replicated state entirely.
- **Tracked:** no.

### SCH3. Claiming evals gets slower as the queue grows (high under bursts)

- **Today:** both costs below run while holding the claims mutex
  (`eval_claims.rs:48`), so every eval thread waits on them.
  - Each claim runs `COUNT(*)` over pending evals (`eval_claims.rs:84` →
    `store.rs:19310`).
  - Each claimed eval runs a subsume query whose `COALESCE(job_id,'')`
    (`eval_claims.rs:136`) stops SQLite using the `job_id` index.
- **Measured** on a copy of the schema (Python `sqlite3`, not the Rust
  binary), with 50k pending evals:
  - 8.7 ms per subsume query;
  - 1.9 ms per count;
  - about 140 ms per batch of 16.
- **Likely cause** of the open "burst submission costs throughput" item,
  which does not name it.
- **Direction:**
  - Store `''` rather than NULL and index the column.
  - Keep a pending counter in memory.
  - Or move to an in-memory eval broker, as Nomad has.
- **Tracked:** no.

### SCH4. `bin_pack` and `spread` score allocation counts, not resources (medium-high)

- **Today:** the score is the node's `assigned_count`, min-max normalized
  against the other candidates (`src/manager/scheduler.rs:6195`;
  `policy.rs:121`, `:256`). That candidate list may be truncated.
  - A 32-CPU allocation weighs the same as a 0.1-CPU one.
  - The docs call this "fullness", which overstates it.
- **Best in class:** Nomad BestFit v3, and the Kubernetes `MostAllocated`,
  `LeastAllocated`, and `BalancedAllocation` scorers, all score requested
  resources against capacity.
- **Effect:** fragmentation, and stranded memory or GPUs, on mixed
  workloads.
- **Direction:** score the dominant-resource fraction.
  `NodeScoringState.common_resources` already holds the data.
- **Tracked:** no.

### SCH5. The apply transaction writes JSON and reads it back (medium)

- **Today:**
  - The plan apply serializes placements to JSON and INSERTs them
    (`store.rs:5098`).
  - It then SELECTs them back from `plan_submission` (`:5842`) and re-parses
    them (`:5891`).
  - Each candidate the bind tries runs a node-row SELECT and parses its
    attribute JSON (`:5949`). That one is memoized per plan.
- **Why it matters:** all of this sits inside the serialized commit (SCH1).
- **Direction:** pass the struct through, and read nodes from the read
  model's `Arc<Node>`, guarded by the node epoch.
- **Tracked:** partly. "Make plan-commit cost flat in fleet size" lists
  suspects, and the SELECT-back is not one of them.

### SCH6. The scheduling plugin seam can only pick a node (medium; blocks whole workload classes)

- **Today:**
  - The trait is one method, `select_node` (`src/manager/scheduler/policy.rs:214`),
    run over already-eligible nodes.
  - One policy applies cluster-wide (`scheduler.rs:1590`).
  - The plugin's context has no free resources.
  - The candidate fast path and ranking are hard-coded to
    `"bin_pack" | "spread"` (`scheduler.rs:4477`, `:5347`, `:5688`, `:5735`),
    so a plugin pays O(eligible) per placement.
  - The dylib is re-opened on every eval: `src/structs/eval.rs:446` →
    `load_policy` → `policy.rs:411`.
- **Best in class:** the Kubernetes scheduler framework (PreFilter, Filter,
  Score, Reserve, Permit, Bind) with per-workload profiles (`schedulerName`).
- **Blocks:**
  - topology-aware GPU and NCCL placement;
  - Kueue- or Volcano-style admission;
  - cost- or carbon-aware placement.
- **Tracked:** no. "A framework for scheduler plugins" is checked off as
  done.

### SCH7. Placement capabilities behind Kubernetes, Nomad, and Kueue (medium)

- **Missing, and not tracked:**
  - **Soft spread with a skew target** across zones or racks. Kubernetes has
    `maxSkew`; Nomad has `spread`. Here `distinct` and `count.per` are hard
    rules only.
  - **A feasibility cache by computed node class.** Class keys exist only for
    blocked evals (`src/manager/blocked.rs:16`).
  - **Usage-aware placement and a descheduler.** Both appear only as
    limitations in the public docs
    ([resource-enforcement.md](../docs/resource-enforcement.md),
    [preemption-and-eviction.md](../docs/preemption-and-eviction.md)).
- **Missing, and already tracked:**
  - GPU topology, MIG, and DRA (see PLG2).
  - Fair share and hierarchical queues: a deliberate non-goal, revisited only
    on real multi-tenant demand.

### SCH8. Info-level logs on every plan and claim (low)

- **Today:** these log at `info` on every plan or claim:
  - "Starting scheduler plan_and_apply" (`scheduler.rs:2006`);
  - the applied line (`:2391`);
  - "Claimed evals" (`eval_claims.rs:169`);
  - one more per submit (`src/structs/eval.rs:186`).
- **Why it matters:** AGENTS.md says high-frequency events log at `debug`.
- **Tracked:** no.

### SCH9. `NEXT_UP.md` shows August throughput (doc)

`NEXT_UP.md` still leads with 80.35/s at 5k nodes from 2026-08-04, but the
current numbers are 905/s at 5k and 327/s at 50k. That file belongs to
whoever is driving the current work, so this review leaves it alone and files
a TODO.

## State store and raft

What is good:
- Replicating changesets rather than statements avoids rqlite's
  nondeterminism.
- The applied index commits in the same transaction as each apply.
- A mixed-version guard refuses writes that older members can't apply.
- Liveness is off the raft path.
- StreamWork pushes deltas, with an order-independent digest.
- The read model is projected from committed changesets, with drift metrics.

### ST1. The log format is the SQLite schema, and data migrations run outside the log (medium-high)

- **Today:** changesets bind columns by position (`capture.rs:688`). So every
  migration is a log-format change, and table rebuilds must keep column order
  exactly.
- **Divergence risk (inferred, not reproduced):** data migrations run locally
  on each node at startup (for example `migrations.rs:1041`,
  `UPDATE job SET updated_at = created_at`). Replicas that migrate at
  different applied indexes could backfill different values.
- **Best in class:** Nomad and Kubernetes keep the schema in code, through
  versioned commands and objects.
- **Direction:** apply data migrations as replicated barrier entries at one
  log index, and tag entries with a schema version. Longer term, consider
  logical commands, or rows encoded by column name.
- **Tracked:** no. The positional rule and the R2 guard are recorded as Done.

### ST2. No revision, watch, or blocking-query model for core objects (high for controllers and agents)

- **Today:**
  - Reads carry no index and no consistency option.
  - Event IDs are wall clock plus a per-process sequence, so they can go
    backwards after a failover ([event-stream.md](../docs/event-stream.md)).
  - The event journal keeps 10,000 rows (`store.rs:1966`).
  - A write forwarded by a follower returns without waiting for the
    follower's own apply (`src/manager/leader_forward.rs`), so
    read-after-write through a follower can be stale.
- **New watches exist only for apps and the autoscaler:** `WatchObjects`,
  `WatchScalingPolicies`, and `WatchAppExtensions`. `WatchObjects` polls a
  fingerprint about once a second and resends a full snapshot on reconnect,
  with no resume version.
- **There is no watch on jobs, allocations, or KV.**
- **Best in class:** Kubernetes list-and-watch at a `resourceVersion`, and
  Nomad's `?index=` blocking queries. Both let controllers and agents wait
  for change instead of polling.
- **Effect:** at 10× churn, a 10k-event window covers seconds. Every
  controller built on app objects inherits the polling.
- **Direction:**
  - Return a revision on every read and write reply, either per lane or as a
    hybrid logical clock. The applied index is already stored in the same
    transaction as the apply.
  - Add `min_index`/wait to reads.
  - Derive event IDs from the log position.
  - Offer opt-in linearizable reads; `ensure_linearizable` already exists.
- **Tracked:** no.

### ST3. A manager restart or failover forces a full resync of every worker's desired set (medium at scale)

- **Today:** StreamWork positions are tied to a random epoch per manager
  process (`src/storage/work_log.rs:25`, `:179`). A worker on any other
  epoch gets its whole desired set rebuilt from store reads.
- **Effect (inferred):** losing the leader at 10k–50k nodes means 10k+
  full-set builds at once, on top of the liveness grace rebuild.
- **Direction:** base positions on replicated row versions, so a worker can
  resume "changes since X" from any manager.
- **Tracked:** no.

### ST4. Snapshots are held in memory, and the log tail is capped at 1,000 entries (medium)

- **Today:** snapshots are `Cursor<Vec<u8>>`
  (`src/storage/raft/state_machine.rs:788`), so a manager needs about twice
  the DB's size in memory. The retained log tail is `.clamp(1, 1000)`
  (`driver.rs:3717`).
- **Effect:** at 10× the write rate the tail lasts seconds, so any follower
  restart turns into a full snapshot install, which takes the capture mutex
  (SCH1).
- **Direction:** retain the log tail by bytes or by time, not entry count.
- **Tracked:** streaming snapshots as files is tracked (audit R5). The tail
  clamp is not.

### ST5. The heartbeat handler still runs blocking store work on the async runtime (medium)

- **Today:** `handle_failed_allocations`, `handle_terminated_allocations`,
  and `update_allocation_health_snapshots` run inline
  (`server.rs:8885`–`8894`), and can make blocking raft writes. Only
  `update_node_heartbeat` is offloaded.
- **Direction:** offload these three as `update_node_heartbeat` is, and
  eventually make `Store::replicated` async.
- **Tracked:** only as context inside other items.

### Noted, not filed

**ST6.** Legion runs one raft domain on openraft 0.9. Pre-vote (tracked) and
federation (planned, nothing built) are open. Leader transfer already exists
as an emulation (`driver.rs:961`). Future multi-region work should use
independent cells with asynchronous replication between them, never a WAN
raft group: the stop-and-wait lane would multiply WAN latency per entry.

## Pluggability

Legion has two plugin models, and they differ sharply in quality.

| Model | Kinds | Properties |
| --- | --- | --- |
| Rust `cdylib`, `Box<dyn Trait>` over `libloading` | tasks, scheduling, rollouts, node checks, artifacts | In process. Rust only. Tied to one exact build by an FNV fingerprint. No panic containment |
| exec or HTTP with JSON (`legion-scaling/1` for the autoscaler) | volumes, keyring, cluster CA, autoscaler sources, strategies, and targets | Isolated. Any language. Idempotent operations, a versioned manifest, timeouts, conformance harnesses |
| WASM (proxy-wasm ABI, `src/wasm/`) | network plugins | Sandboxed, with a fuel limit, any language that targets WASM. Only network plugins use it |

The exec and WASM rows are the right models. The dylib row carries the
high-rate seams and the risk.

### PLG1. Dylib plugins: no stable ABI and no panic containment (high)

- **Today:**
  - Five load sites: `src/worker/node_check.rs:112`,
    `src/manager/rollout_strategy.rs:97`, `src/manager/scheduler/policy.rs:422`,
    `src/task_plugins/mod.rs:2257`, `src/artifacts/fetcher.rs:410`.
  - The guard is an FNV hash (`src/plugin_runtime.rs:30`), which the code
    itself calls "a guard, not a stable ABI".
  - No plugin call is wrapped in `catch_unwind`.
  - Scheduling and rollout plugins run inside the manager, so a plugin bug
    can take down the control plane.
- **Effect:** every plugin has to live in-tree and be rebuilt for each
  release.
- **Best in class:**
  - go-plugin, CRI, CSI, and device plugins use gRPC over a unix socket.
  - proxy-wasm and the wasmtime component model (WIT) give sandboxed
    in-process plugins with a stable ABI.
- **Direction:**
  - Task runtimes and artifact fetchers: out of process, over exec or gRPC.
  - Scheduling, rollout, and node-check hooks: onto the existing WASM layer,
    which is deterministic and fuel-bounded, so safe inside the manager.
  - Until then, wrap every plugin call in `catch_unwind`.
- **Tracked:** the stable ABI is tracked (audit M6). Panic containment and
  the WASM direction are not.

### PLG2. No device model: GPUs effectively do not work (critical for AI workloads)

- **Today:**
  - Custom resources come only from static node config
    (`src/resources.rs:982`). A node-check plugin cannot advertise devices
    it discovers.
  - Device-ID sets are scheduled, but nothing hands the assigned IDs to the
    task. There is no `CUDA_VISIBLE_DEVICES`, CDI, or `nvidia` anywhere in
    `src/`.
  - Containers deny every device (`src/task_plugins/oci_bundle.rs:1688`).
  - The microVM has no VFIO path.
  - There is no device health, NUMA, NVLink, or PCIe topology, and no
    MIG/MPS sharing.
  - Namespace quota covers cpu, memory, and cores only
    (`src/structs/namespace_policy.rs:79`).
  - So a GPU job can run only under raw `exec`.
- **Best in class:** Kubernetes device plugins (`Allocate` returns env,
  mounts, and device nodes) and DRA, the NVIDIA GPU Operator, Nomad's nvidia
  device plugin, and Slurm GRES.
- **Blocks:** GPU training and inference, FPGA, RDMA/InfiniBand, and
  multi-node NCCL.
- **Direction:** a device-plugin seam (exec or gRPC) that:
  1. fingerprints devices;
  2. returns a CDI spec on allocate, which the container and exec runtimes
     apply;
  3. reports health and topology attributes;
  4. adds quota dimensions.
  MIG and fractional sharing come after.
- **Tracked:** no. Only "Resource plugins" and NUMA core placement exist.
  Needs a plan.

### PLG3. Task runtimes are closed in practice (medium)

- **Today:**
  - `exec`, `container`, `sandbox`, and `envelope` are hard-coded
    (`src/task_plugins/mod.rs:2244`). Only `noop` is a dylib.
  - The `TaskPlugin` trait has about 42 methods, all of which a plugin would
    have to carry across the fragile ABI.
  - The OCI runtime allowlist is `["runc", "crun"]`
    (`src/task_plugins/container.rs:263`), so gVisor (`runsc`) and Kata are
    refused.
  - There is no Windows support (no `cfg(windows)`).
- **Best in class:** CRI with `RuntimeClass` (shim v2), and Nomad's task
  driver plugins.
- **Blocks:** gVisor or Kata isolation for untrusted agent code (an
  alternative to microVMs), unikernels, and Windows.
- **Direction:** a per-task OCI runtime class, checked at preflight, before
  any out-of-process runtime API.
- **Tracked:** no. The WASM task runtime is tracked; gVisor, Kata, and a
  runtime class are not.

### PLG4. No admission or mutation policy on jobs, and no secrets provider (medium)

- **Today:** app object kinds have CEL `rules`
  (`src/structs/object_rules.rs`), but a jobspec passes through no
  operator-defined policy. There is no OPA, CEL, or webhook admission on
  `RunJob`. Namespace policies cover quota, pools, runtimes, and counts, not
  arbitrary rules ("no `latest` tags", "every service has an owner label").
- **No secrets provider seam:** secrets come from KV plus workload identity,
  and autoscaler plugin config has no secret references
  ([autoscaling.md](../docs/autoscaling.md)).
- **Best in class:** Kubernetes `ValidatingAdmissionPolicy` (CEL), Gatekeeper
  and Kyverno, and Vault or External Secrets providers.
- **Direction:** reuse the `cel` evaluator object kinds already use, as
  namespace- or cluster-scoped job validation rules, run in `job validate`,
  `job plan`, and `job run`.
- **Tracked:** no. Eval hooks and log sinks are tracked (Plugin surface).

### PLG5. Shallow seams: rollouts, node checks, volumes (low-medium)

- **Rollouts:** the trait is `name` and `resolve` only
  (`src/manager/rollout_strategy.rs:8`). There are no step, analysis, or
  abort hooks, which Argo Rollouts and Flagger have.
- **Node checks:**
  - They return an untyped `Map` (`src/worker/node_check.rs:57`).
  - They dlopen on every pass (`:105`).
  - They have no pressure or taint semantics; pluggable node statuses are
    tracked.
- **Volumes:** node side only. `create`, `attach`, `detach`, and `expand`
  are reserved (`src/volumes/mod.rs:82`), so EBS-style block volumes that
  follow an allocation cannot work. The CSI bridge is tracked.
- **Tracked:** rollout hooks and the per-pass dlopen are not.

## AI readiness

What is ahead of the field:
- Edge boot and session tickets start work with no raft write.
- Suspend, resume, fork, snapshot, warm, promote, and park are first-class.
- Sandboxes are gated by default, with `net policy test` and `net doctor`.
- Credential injection keeps API keys out of the guest and its checkpoints.
- Workload identity is OIDC.
- Errors are typed and retryable, idempotency keys are broad, and
  `job plan` reports preemption and gangs.
- Delegation for apps, job grants, and app objects with
  `expected_version` and `--wait` are a good base for agent operators.

### AI1. GPUs

See PLG2. This is the largest gap for AI workloads.

### AI2. No durable audit trail of who did what (high for agent operators)

- **Today:**
  - Only denials are logged, to the `acl_audit` tracing target
    (`src/manager/acl.rs`, `src/manager/server/delegation.rs:460`).
  - `SchedulerEvent` has no actor field.
  - A few events now carry `by` (`scale.pinned`, app install;
    `src/storage/events.rs:1217`, `:1659`), and app objects record their
    writer's `authority`.
  - Sessions have no audit log ([session-mode.md](../docs/session-mode.md)).
- **Effect:** "which agent, acting for whom, did this?" has no answer.
- **Best in class:** Kubernetes audit policy, Nomad Enterprise audit
  logging, CloudTrail.
- **Direction:** a durable, queryable write journal that records:
  - the accessor;
  - the principal;
  - the on-behalf-of chain;
  - the operation;
  - a request digest;
  - the idempotency key;
  - the outcome.
  Expose it through `state query` and MCP.
- **Tracked:** no. The only related item is object stamping.

### AI3. Credentials and guardrails for agent operators (high)

- **Exists now:**
  - On-behalf-of delegation for apps (`src/manager/delegation.rs`).
  - Acting for an object.
  - Job grants under "grant what you hold" (`src/structs/grant.rs`).
  - App install and upgrade show a permission diff and need approval.
- **Still missing:**
  - **Token expiry.** ACL tokens never expire by default; `acl.tokenMaxTtl`
    is unbounded unless set (`src/structs/config.rs:1602`).
  - **Token attenuation.** Minting any token needs `acl:write`
    (`server.rs:12132`), so an agent cannot hand a sub-agent a narrower
    token. Biscuit or macaroon-style attenuation, or a "create a child token
    that is a subset of mine" RPC, would close this.
  - **Approval gates.** There is no human-approval gate for destructive
    operations outside app install. MCP's `--allow-write` is all-or-nothing.
  - **Rate limits.** There is no per-token rate limit or budget, so nothing
    stops a runaway agent short of revoking its token.
  - **Caller-aware catalog.** The capability catalog is not filtered by the
    caller's ACL (`src/cmd/capabilities.rs:40`), so an agent learns about
    operations it cannot perform only by failing.
- **Tracked:** no.

### AI4. The MCP server is behind the protocol and the API (medium-high)

- **Protocol:** still `2024-11-05` (`src/cmd/mcp.rs:52`), stdio only, and only
  `initialize`, `tools/list`, `tools/call`, and `ping` (`:1649`). There are no:
  - tool annotations (`readOnlyHint`, `destructiveHint`);
  - `structuredContent` or `outputSchema`;
  - Streamable HTTP transport;
  - elicitation, which is the natural hook for the AI3 approval gate.
- **Coverage:** tools grew to 107. About 60 user-facing Manager RPCs still
  have no tool, including:
  - KV;
  - ACL token, policy, and user management;
  - rollout get, advance, and stop;
  - `ListJobSpecHistory`;
  - `StreamEvents`;
  - app install and upgrade.
- **No wait on jobs:** `legion_job_run` has no wait option (`mcp.rs:1155`);
  only object apply has one.
- **Tracked:** no.

### AI5. Jobs are not as declarative as objects (medium)

- **No diff:** `PlanJobReply` gives placements plus `matches_existing` and
  `same_content`, with no field-level diff. Nomad `job plan`, `kubectl diff`,
  and Terraform all give one.
- **No compare-and-set:** `RunJobRequest` has no `expected_version`, so two
  agents silently overwrite each other's jobs. CAS already exists on
  `ApplyObjectRequest.expected_version`, ScaleTask, and app records.
- **No intent layer:** there is nothing above jobs, such as "serve model X at
  p99 < 200 ms", that KServe or Knative offer. App object kinds are now the
  natural place to build one.
- **Tracked:** no.

### AI6. Model weights do not fit the artifact cache (medium-high for inference)

- **Today:**
  - The node cache class is clamped to 1–20 GiB by default
    (`src/structs/config.rs:3360`), smaller than one 70B checkpoint.
  - ORAS layers are not cached.
  - There is no peer-to-peer distribution and no lazy loading.
- **Best in class:** Dragonfly, Spegel, SOCI/Nydus, Modal's lazy filesystem.
- **Tracked:** tree and ORAS caching are tracked. The model-scale sizes,
  P2P, and lazy loading are not.

### AI7. Gang scheduling has no topology or interruption awareness (medium for training)

- **Today:** gangs have quorum and `LEGION_GANG_*` rendezvous variables, but:
  - placement ignores racks, NVLink domains, and network locality;
  - nothing reacts to spot interruption notices, in core or in the
    `aws-asg` plugin.
- **Best in class:** Kueue topology-aware scheduling, Volcano, Slurm,
  SkyPilot.
- **Tracked:** reservation, backfill, readiness timeout, and in-place restart
  are tracked. Topology and interruption are not.

### AI8. Sandbox claims still outrun verification

- **Tracked, and still open:**
  - Gated sandboxes have not booted on a real VMM (gated-sandbox Phase 0).
  - Lazy memory restore is not measured on KVM.
  - The warm pool is unmerged.
  - Sessions cannot carry workload identity.
  - Injected credentials cross HTTP proxy hand-offs in plaintext.
- **One stale TODO entry:** it said the `sandbox` runtime "cannot exec,
  signal, or deliver identity files". Exec works through the guest agent, and
  identity files are projected through the identity disk
  (`src/task_plugins/sandbox.rs:181`). Only on-demand signals are still
  missing. This change corrects that entry.

### AI9. No usage or cost accounting over time per namespace (medium for multi-tenant and agent budgets)

- **Today:** namespaces show only current charged usage. There is no
  time-integrated usage (CPU-seconds, GiB-hours, GPU-hours) per namespace,
  job, or token.
- **Effect:** "how much did this agent spend this week?" has no answer, and
  there is no basis for budgets (AI3).
- **Tracked:** no.

### Noted, not filed

- **AI10.** CPU is counted in MHz, and `cpus` is an alias for MHz (a
  footgun, tracked). There is no federation (planned, nothing built).
- **Fixed since the first pass.** Autoscaling was "manual only" and is now
  largely closed by autoscaler v2. User-defined object kinds and controllers
  were missing and now exist as app object kinds.

## Finding index

The status column records what the fix change of 2026-11-08 did with each
finding: one plan per area, each with its judgement calls, and every open
remainder is an item in [TODO.md](TODO.md). AI8 (sandbox verification) needs
a real VMM and was not attempted.

| ID | Finding | Impact | Tracked before | Status (2026-11-08) |
| --- | --- | --- | --- | --- |
| NET1 | Endpoint feed rebuilt every heartbeat; plan claimed delta push shipped | High | No | Fixed: versioned per-service sets, deltas on heartbeat and pushed on StreamWork ([plan](plans/mesh-control-path-plan.md)) |
| NET2 | Policy assignments recomputed every heartbeat | High | No | Fixed: memoized per policy generation and input ([plan](plans/mesh-control-path-plan.md)) |
| NET3 | Full mTLS handshake per mesh connection; resumption cache dropped | Med-high | Partly | Partly: resumption survives refreshes; pooled tunnels and kTLS open ([plan](plans/mesh-control-path-plan.md)) |
| NET4 | Uncapped HTTP request body buffering | Medium | No | Fixed: streamed; buffered only for retries, capped by `maxBodyBytes` ([plan](plans/mesh-control-path-plan.md)) |
| NET5 | IPv6 address formatting and binding | Medium | No | Fixed: bracketed host:port end to end, dual-stack ingress ([plan](plans/mesh-control-path-plan.md)) |
| NET6 | nftables fallback: no established short-circuit, `nft` fork per beat | Low-med | Partly | Mostly: established short-circuit, vmap dispatch, counters every 10s; netlink open ([plan](plans/mesh-control-path-plan.md)) |
| NET7 | No splay on CA-rotation reissue | Low | No | Fixed: per-node splay up to 10 min ([plan](plans/mesh-control-path-plan.md)) |
| NET8 | Per-connection global mutex and allocations | Low | No | Mostly: sharded ledger, atomic in-flight, pooled buffers; endpoint interning open ([plan](plans/mesh-control-path-plan.md)) |
| NET9 | 255 allocations with upstreams per node | Low | No | Fixed: 16,384 VIP blocks from 127.64.0.0/10; datapath ports 61000–65535 ([plan](plans/mesh-control-path-plan.md)) |
| SCH1 | Stop-and-wait lane, cross-lane capture mutex, double execution | Critical | Partly | Partly: group commit, one fsync per entry, leader executes once under `single`; overlap and a per-lane mutex (needs a DB file per lane) open ([plan](plans/raft-write-path-plan.md)) |
| SCH2 | `plan_submission` never pruned | High | No | Fixed: applied plans write no row; ~1,000 rejected kept ([plan](plans/placement-and-commit-cost-plan.md)) |
| SCH3 | Claim cost grows with queue depth | High | No | Fixed: index-usable claim queries, bounded count ([plan](plans/placement-and-commit-cost-plan.md)) |
| SCH4 | Scoring by allocation count, not resources | Med-high | No | Fixed: dominant-resource scoring, count-scaled anti-affinity ([plan](plans/placement-and-commit-cost-plan.md)) |
| SCH5 | Apply writes JSON and reads it back | Medium | Partly | Partly: JSON round trip gone; node-row read open ([plan](plans/placement-and-commit-cost-plan.md)) |
| SCH6 | Scheduling plugin seam is one `select_node` call, cluster-wide | Medium | No | Not in this change |
| SCH7 | No soft spread, node-class cache, usage-aware placement, descheduler | Medium | No | Not in this change |
| SCH8 | Info logs per plan and claim | Low | No | Fixed ([plan](plans/placement-and-commit-cost-plan.md)) |
| SCH9 | `NEXT_UP.md` numbers out of date | Doc | No | Not in this change (owner's file) |
| ST1 | Schema-coupled log; data migrations outside the log | Med-high | No | Mostly: data steps run once through the log; logical commands open ([plan](plans/raft-write-path-plan.md)) |
| ST2 | No revision, watch, or blocking query for core objects | High | No | Fixed: revisions, blocking reads, `Watch`, monotonic event ids, read-after-write, linearizable reads; `WatchObjects` open ([plan](plans/revisions-and-watches-plan.md)) |
| ST3 | Full worker resync on manager restart | Medium | No | Mostly: workers resume on any manager whose journal covers them; a restarted manager still resyncs ([plan](plans/revisions-and-watches-plan.md)) |
| ST4 | In-memory snapshots; 1,000-entry log tail | Medium | Partly | Fixed: `raft.logRetainBytes` (256 MiB); in-memory snapshots still open ([plan](plans/raft-write-path-plan.md)) |
| ST5 | Blocking store work in the heartbeat handler | Medium | No | Fixed: heartbeat store work on the blocking pool ([plan](plans/revisions-and-watches-plan.md)) |
| PLG1 | Dylib ABI, no panic containment | High | Partly | Partly: panic containment and quarantine for scheduling, rollout, node-check, fetcher plugins; out-of-process/WASM open ([plan](plans/device-model-and-plugin-seams-plan.md)) |
| PLG2 | No device model; GPUs unsupported | Critical (AI) | No | Phase 1: device plugins, assignment, CDI, quota; not run on real GPUs; MIG/VFIO open ([plan](plans/device-model-and-plugin-seams-plan.md)) |
| PLG3 | Closed task runtimes; runc/crun only | Medium | No | Fixed: OCI runtime classes ([plan](plans/device-model-and-plugin-seams-plan.md)) |
| PLG4 | No admission policy on jobs; no secrets provider | Medium | No | Partly: CEL job rules; mutation and secrets provider open ([plan](plans/device-model-and-plugin-seams-plan.md)) |
| PLG5 | Shallow rollout and node-check seams | Low-med | Partly | Partly: node checks typed and cached; rollout hooks open ([plan](plans/device-model-and-plugin-seams-plan.md)) |
| AI2 | No audit journal | High | No | Fixed: replicated audit journal ([plan](plans/agent-governance-plan.md)) |
| AI3 | Token expiry, attenuation, approvals, rate limits, caller-aware catalog | High | No | Fixed: expiry by default, child tokens, approvals, rate limits, caller-aware catalog ([plan](plans/agent-governance-plan.md)) |
| AI4 | MCP protocol revision and coverage | Med-high | No | Mostly: MCP 2025-06-18, annotations, structured results, elicitation, loopback HTTP, full RPC coverage; SSE and remote auth open ([plan](plans/agent-governance-plan.md)) |
| AI5 | No CAS or field diff on jobs | Medium | No | Fixed: `expected_version` and a field diff; intent layer a non-goal ([plan](plans/revisions-and-watches-plan.md)) |
| AI6 | Artifact cache too small for model weights; no P2P or lazy loading | Med-high | Partly | Partly: 50%-of-disk cache, 1 TiB artifacts, ORAS layers cached; P2P and lazy loading open ([plan](plans/device-model-and-plugin-seams-plan.md)) |
| AI7 | Gang topology and interruption awareness | Medium | No | Partly: `gang.topology`; interruption awareness open ([plan](plans/placement-and-commit-cost-plan.md)) |
| AI9 | No usage accounting over time | Medium | No | Fixed: hourly usage rollups incl. devices ([plan](plans/agent-governance-plan.md)) |
