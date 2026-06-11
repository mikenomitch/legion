# Plan: Multi-Raft Persistence for the Manager Store

> **Related:**
> - [`docs/persistence-plan.md`](../persistence-plan.md) — the parent pluggable-persistence roadmap. This plan executes (and supersedes) its *“Remaining GA work (store replication path)”* section.
> - [`docs/raft-operations.md`](../raft-operations.md) — current Raft control-plane operator workflows (election/failover/membership/join-token).
> - [`docs/state-store.md`](../state-store.md) — the SQLite store, read-model cache, and allocation-epoch CAS this plan replicates.
> - [`docs/persistence-streaming.md`](../persistence-streaming.md) — the rustyriver streaming tier this plan sits *above* on the durability gradient.
>
> **Deliverable of this document:** a plan, not the implementation. The implementation is decomposed below into a Claude Dynamic Workflow so it can be executed (and re-executed) phase-by-phase with verification at every step.

---

## TL;DR

Turn Legion's Raft backend from a leader-election *scaffold* into a real, durable, **multi-raft** replicated state machine, modeled on the lane-partitioned multi-raft design in the *Source Inspiration* tweet. Replicated manager state is split across **three independent Raft groups by write class** — **control**, **scheduler**, and **status** — so a burst of user requests or a flood of worker status updates can never stall the scheduler's critical write path. Multi-raft is **on by default** whenever `persistence.type = raft`; single-group is an explicit escape hatch. Ergonomics are unchanged for operators: one config block, one join token, one aggregate status line. The plan also fills in the **missing real Raft functionality** (consensus transport, log/state-machine storage, commit→apply path, snapshots, real membership) and the **missing Raft test coverage** (apply round-trips, multi-node, fault injection, lane-isolation, and a performance SLO that mirrors the tweet's 12× result).

---

## Source Inspiration

This plan is modeled on Diptanu Gon Choudhury's multi-raft scheduling write-up
([tweet](https://x.com/diptanu/status/2062912406955946443)). Key points, and how they map to Legion:

| Source observation | Legion analogue |
| --- | --- |
| One replication pipeline became the bottleneck: a burst of **100k sandbox creates** competed with **scheduler updates** for the same Raft log. | A burst of `RunJob`/`CreateInstance` (or KV writes) competes with allocation writes on a single Raft log. |
| Split into **3 independent replication lanes**: (1) user create/delete/update, (2) scheduler placement decisions, (3) dataplane status updates. | **control** lane (user mutations), **scheduler** lane (placements/evals), **status** lane (task/health updates). |
| Result: **~12× sandbox scheduling throughput**; “the scheduler is never blocked waiting for more messages.” | Target: scheduler-lane apply latency/throughput stays flat under heavy control-lane and status-lane load. |
| Pattern borrowed from **TiKV / CockroachDB** multi-raft. | Same engine model (many independent Raft groups in one process over a shared transport). |

> ★ Insight ─────────────────────────────────────
> The source design's multi-raft is **functional/lane partitioning** (split by *write class*), not TiKV's
> **key-range sharding** (split by *data range*). For a scheduler the lane split is the higher-leverage
> move: the scheduler critical path is a *different kind* of write than user requests and status
> heartbeats, and isolating it is what keeps placement latency flat under load. Key-range sharding is a
> further axis we can add *inside* the scheduler lane later (see Phase 8), but it is not the headline win.
> ─────────────────────────────────────────────────

---

## Problem Statement

Legion's Raft backend today is a **control-plane simulation**, not a replicated store:

1. **No real consensus.** [`RaftPersistenceDriver`](../../src/storage/persistence.rs) tracks `role`/`term`/`commit_index`/`applied_index` in an in-process `Mutex<RaftState>` and mutates them when an operator issues a command. `openraft` is a dependency but only `OpenRaftConfig::default()` is constructed and stored in an unused field (`_raft_config`). No log, no network, no quorum, no apply loop.
2. **Writes are not replicated.** Every mutation goes through `Store::write_connection()` → local SQLite ([`src/storage/store.rs`](../../src/storage/store.rs)). The persistence driver is consulted only for `status()` / `raft_status()` / operator commands — it is **not on the write path**. So `persistence.type = raft` gives leader-election *theater* with single-node durability underneath (as [`docs/state-store.md`](../state-store.md) and [`docs/raft-operations.md`](../raft-operations.md) both admit: “manager data is still local SQLite per node”).
3. **Even once real, a single Raft group is a throughput ceiling.** This is the exact bottleneck the source tweet calls out: one log serializes *all* writes, so user-request bursts and status floods contend with — and stall — the scheduler.
4. **Test coverage is near zero.** [`src/storage/persistence.rs`](../../src/storage/persistence.rs) has 3 unit tests (local-disk status, streaming standby, raft *bootstrap-status*). There are **no** apply/replay tests, **no** multi-node tests, **no** snapshot/restore tests, **no** partition/fault-injection tests, and **no** Raft entries in [`tests/`](../../tests). The user explicitly asked that missing Raft tests and functionality be added.

We want Legion to be **faster and more scalable than Nomad/K8s** (per [`README.md`](../../README.md)) — and Nomad's single-Raft control plane is a known scaling limit. Multi-raft is how we beat it while keeping the single-binary, token-join ergonomics Legion is built around.

---

## Goals

- **Real replicated writes.** Mutating store operations become deterministic commands proposed to Raft, committed by quorum, then applied to local SQLite (rqlite-style command replication, *not* WAL shipping — decision inherited from [`docs/persistence-plan.md`](../persistence-plan.md)).
- **Multi-raft by default.** When `persistence.type = raft`, state is partitioned across the **control / scheduler / status** lanes automatically. No extra config required to get lane isolation.
- **Scheduler critical path is isolated and fast.** Allocation/eval/plan-submission writes ride their own Raft group; their apply latency and throughput must stay within SLO regardless of control-lane or status-lane load. This is the success metric.
- **Ergonomics stay reasonable.** One `persistence: { type: "raft" }` block, one cluster-join token that enrolls a node into *all* lanes, and a single aggregate `raft status` summary (with `--per-lane` for detail). Operators should not have to reason about three groups for day-to-day use.
- **Fill the functionality gaps.** Implement the openraft transport, log + state-machine storage, commit→apply path, snapshot/compaction, and wire the *existing* operator commands (election/transfer/membership/join) to real consensus instead of the simulation.
- **Fill the test gaps.** Add unit, integration, multi-node, fault-injection, lane-isolation, and performance-SLO coverage for Raft.
- **Safe gradient preserved.** `local_disk` stays the default backend; `streaming` stays the middle tier; `raft` (now multi-raft) is the top tier. Mixed-mode and rolling upgrades are addressed.

## Non-Goals

- **Replacing SQLite** as the per-node state-machine store. We replicate *commands into* SQLite.
- **Cross-region active/active.** Lanes are region-scoped; geo-replication is out of scope.
- **Key-range sharding as the v1 default.** Horizontal sharding of the scheduler lane is specified as an *optional, later* axis (Phase 8), behind a flag, not the default.
- **Replacing the rustyriver/`streaming` tier.** It remains the low-complexity HA option for users who don't want consensus.
- **Multi-writer SQLite** or distributed transactions *across* lanes. Cross-lane consistency is handled by an eventually-consistent read model + the existing allocation-epoch CAS (see Design §5), not 2PC.

---

## Background: What Exists Today

| Concern | Where | Notes |
| --- | --- | --- |
| Persistence trait + drivers | [`src/storage/persistence.rs`](../../src/storage/persistence.rs) | `PersistenceDriver` trait; `LocalDisk`, `Streaming`, `Raft` impls. `RaftPersistenceDriver` is a `Mutex<RaftState>` simulation (lines ~362–690). |
| Unused real engine | [`src/storage/persistence.rs:421`](../../src/storage/persistence.rs) | `_raft_config: OpenRaftConfig::default()` — openraft is imported but never drives anything. |
| Driver build/select | [`src/storage/persistence.rs:692`](../../src/storage/persistence.rs) | `build_persistence_driver()` matches `PersistenceType`. |
| Store write path | [`src/storage/store.rs:361`](../../src/storage/store.rs) | `write_connection()` → single `Mutex<Connection>`; multi-statement writes use `conn.transaction()`. **This is the single integration seam for propose→apply.** |
| Persistence wiring | [`src/storage/store.rs:215`](../../src/storage/store.rs) | `build_persistence_driver(...)` + `persistence_driver.start()` at `start_storage`; `persistence_status()`, `raft_status()`, `raft_history()`, `issue_raft_command()`, `issue_raft_join_token()` delegate to the driver. |
| Raft gRPC surface | [`src/manager/server.rs:1797`](../../src/manager/server.rs) | `get_raft_status`, `list_raft_history`, `issue_raft_command`, `get_raft_join_token`; `map_raft_status`/`map_raft_event` adapters. |
| Raft CLI | [`src/cmd/raft.rs`](../../src/cmd/raft.rs) | `raft status|history|command|join-token`. |
| Config | [`src/structs/config.rs:146`](../../src/structs/config.rs) | `PersistenceType::{LocalDisk,Streaming,Raft}`; `RaftPersistenceConfig{node_id,bootstrap,peers,join_token_ttl_seconds}` (+ `*Full` variants). Default backend = `LocalDisk`. |
| Tables (state machine) | [`src/storage/migrations.rs`](../../src/storage/migrations.rs) | 23 tables incl. `node`, `worker_type`, `job`, `job_config`, `task_config`, `task_status`, `allocation`, `allocation_epoch`, `allocation_epoch_partition`, `plan_submission`, `eval`, `kv_*`, `rollout*`, `idempotency_record`. |
| Epoch CAS | [`docs/state-store.md`](../state-store.md) | `allocation_epoch` compare-and-swap already guards placement against stale cluster state — the basis for tolerating cross-lane read lag. |
| Idempotency | AGENTS.md / `Store::idempotency_*` | Mutating RPCs carry `idempotency_key`; commands must dedupe on apply (replay-safety). |
| Read model cache | [`src/storage/store.rs:105`](../../src/storage/store.rs) | In-memory `SchedulingReadModel` already decouples scheduler reads from the write DB — the natural projection point for a multi-lane read view. |
| Raft tests | [`src/storage/persistence.rs:714`](../../src/storage/persistence.rs) | 3 unit tests; **no** integration/multi-node/fault/perf tests anywhere. |
| Perf harness | [`tests/scheduler_benchmark_slo.rs`](../../tests/scheduler_benchmark_slo.rs), [`benches/`](../../benches) | SLO test reads `min_evals_per_sec`, `max_p99_latency_ms`, `max_conflict_rejection_rate`, `min_correctness_ratio`; `scheduler_bench` + `eval_critical_path_bench` exist. Reuse these as the multi-raft perf gate. |

**Bottom line:** the operator-facing Raft surface (RPC/CLI/UI/history/config/join-token) already exists and is good. What's missing is the *engine* underneath it and the *partitioning* on top of it.

---

## Proposed Architecture

### 1) Three lanes, by write class

Each lane is an independent openraft group with its own log, leader, quorum, and apply loop. Tables are partitioned across lanes by *who writes them and how often*:

| Lane | Raft group id | Tables it owns (writes) | Write character |
| --- | --- | --- | --- |
| **control** | `legion/control` | `job`, `job_config`, `submission`, `job_config_submission`, `task_config`, `job_update`, `node`, `worker_type`, `manager_config`, `kv_entry`, `kv_lock`, `job_kv_reference`, `rollout` (desired state), `saved_state_query`, `idempotency_record`, `service` + `service_intention` *(planned — see §1a)* | User/operator-driven; bursty (job submit storms, KV writes). |
| **scheduler** | `legion/scheduler` | `allocation`, `allocation_epoch`, `allocation_epoch_partition`, `plan_submission`, `eval` | The **critical path**; must stay low-latency. |
| **status** | `legion/status` | `task_status`, `allocation_health`, `rollout_event`, `node_health` *(new — see below)*, `service_health` *(planned — see §1a)* | High-frequency, high-volume worker→manager updates. |

> ★ Insight ─────────────────────────────────────
> The `node` table mixes **identity** (registration: node id, attributes — control) with **liveness**
> (last-heartbeat, status — extremely high frequency). Leaving heartbeat updates in the control lane would
> reintroduce exactly the contention we're eliminating. So heartbeat-driven liveness moves to a new
> `node_health` table on the **status** lane, while `node` registration stays on **control**. The scheduler
> reads a *join* of the two via the read model. This is the smallest change that keeps each lane's writes
> homogeneous.
> ─────────────────────────────────────────────────

**Lane assignment is centralized, not scattered.** Introduce a single `Lane` enum and a `lane_for(command)` classifier so the mapping lives in one place and is unit-testable. Every replicated command declares its lane; a `#[test]` asserts the table↔lane mapping is total and disjoint (no table written by two lanes).

### 1a) Where service discovery fits — and why it does **not** get its own lane (recommendation)

Service discovery is not built yet — it is an open item on [`docs/TASK-LIST.md`](../TASK-LIST.md) (named services on a task, health checks, querying address/port from other services, per-service access ACLs) and a stated [`README.md`](../../README.md) goal, so this is forward-looking guidance for when it lands. It is worth deciding now because SD's write profile is exactly the kind of thing the lane model exists to place correctly.

**The key realization: "service discovery" is not one write class — it is four concerns, each of which already maps to an existing lane (or to no lane at all).**

| SD concern | What it is | Frequency / character | Where it belongs |
| --- | --- | --- | --- |
| **Definition** ("declare a named service on a task") | Part of the job/task spec. | Rare, user-driven; changes only on job submit/update. | **control** lane, beside `task_config` where it is declared (`service`). |
| **Intentions / ACLs** ("which jobs may access my service") | Policy declared in spec. | Rare, user-driven. | **control** lane (`service_intention`). |
| **Instance registration** (a running alloc → name/address/port) | Which concrete endpoints back a service *right now*. | Tied to allocation churn, bounded by placement rate. | **Derived — no separate replicated write** (see below); falls back to **scheduler** lane if materialized. |
| **Health-check status** (passing ↔ critical) | Liveness of each instance. | Very high volume: O(instances × 1/interval). Same write class as `allocation_health`. | **status** lane (`service_health`). |
| **Resolution / query** ("get address+port of service X") | The latency-critical application-data-path *read*. | Very high read rate; **not a Raft write**. | **Read model** projection / a resolution index — lane-independent. |

> ★ Insight ─────────────────────────────────────
> **Lanes partition by write *class*, not by *feature*.** The multi-raft win comes from isolating
> *kinds* of writes (control vs. placement vs. status), achieved with the *minimum* number of groups. A
> lane-per-subsystem ("SD gets one, then secrets, then autoscaling…") slides straight back into the
> operational complexity multi-raft was meant to remove — every new lane is another leader, election
> timeout, snapshot pipeline, and failure mode to reason about. A new lane is justified only when a
> feature introduces a genuinely *new* write class that fits none of the three. Service discovery does
> not: it fragments cleanly into all three, and its hottest concern is a read.
> ─────────────────────────────────────────────────

**Registration is best treated as a derived projection, not a replicated write.** A live endpoint is just `allocation ⋈ node (address) ⋈ service definition (name/port)`. The scheduler lane already replicates the allocation; the control lane already replicates the definition; the node carries the address. So the read model can *project* the current endpoint set with **no new SD write and no cross-lane atomicity problem** — registration is then exactly as consistent as the allocation it derives from. Only if we later need registration to carry state the allocation doesn't (e.g. app-reported readiness distinct from health) does it become a materialized write, in which case it co-commits on the **scheduler** lane so an allocation and its endpoint land atomically (never an orphan endpoint pointing at a dead alloc).

**Why a dedicated SD lane is tempting — and why it is the wrong default:**

- *Tempting:* health-check volume can rival `task_status`, and SD sits on the application data path, so it "feels" critical enough to isolate.
- *Wrong as a default because:*
  1. The critical SD path is **resolution, which is a read** already isolated from every write log by the read model — so the strongest intuition for a dedicated lane doesn't actually need one.
  2. Health writes **are** the status write class; splitting them off duplicates the lane (`service_health` and `allocation_health` are the same shape) for no isolation gain over the status lane itself.
  3. It forfeits the **atomic allocation+endpoint** property that derived/scheduler-lane registration gives.
  4. It erodes the **3-lane ergonomic** that keeps the operator mental model (and the one-token, one-status-line UX) simple.

**The legitimate pressure, and the right release valve.** At extreme scale a health-check firehose *can* dominate the status lane and starve `task_status`. The correct responses are general status-lane levers, **not** an SD-specific lane: (a) keep the *raw* health signal off Raft — run checks on the worker / via gossip and replicate only the **debounced state transition** (the Consul model), and/or (b) shard the status lane (same mechanism as the optional scheduler-lane sharding in Phase 8). For operators who explicitly want to fault-isolate the application data plane from cluster scheduling/status, expose a dedicated SD lane behind the same escape hatch as `topology`/sharding: `raft.lanes.serviceDiscovery = true` promotes registration+health into their own group. **Default: off.**

**Recommendation (default):** do **not** give service discovery its own Raft lane. Map it across the existing three — definitions/intentions on **control**, health on **status**, registration **derived** (scheduler lane only if materialized) — and serve resolution from the read model. Revisit a dedicated lane only as an opt-in for service-discovery-heavy fleets. This keeps the headline goal intact: the scheduler lane stays minimal and fast, and SD adds essentially **one** new replicated write type (health), which slots into a lane that already exists.

### 2) Command-replicated writes (one seam)

Mutations are funneled through a single typed command envelope rather than touching SQLite directly:

```rust
/// Deterministic, replayable description of one store mutation.
struct StoreCommand {
    lane: Lane,                      // control | scheduler | status
    op: StoreOp,                     // typed enum: UpsertAllocation, RecordTaskStatus, PutKv, ...
    idempotency_key: Option<String>, // dedupe on apply (replay-safety)
}
```

Flow per mutation:

1. RPC handler builds a `StoreCommand`.
2. `MultiRaftDriver` routes it to `lane`'s `Raft` instance and `client_write`s it.
3. On quorum commit, that lane's **state machine** applies the op inside a local SQLite transaction (`write_connection()` + `transaction()`), recording the `idempotency_key` so a replayed entry is a no-op.
4. Handler returns after apply (linearizable read-your-write within a lane).

The existing `Store` write methods are refactored so their SQL bodies become the *apply* implementation, and the public methods become *propose* wrappers. Net: SQL ownership stays in `Store`; Raft only decides *when/where* the SQL runs.

> ★ Insight ─────────────────────────────────────
> Determinism is the whole ballgame for command replication. Any `now()`, `rand()`, or auto-increment
> rowid that leaks into a command makes replicas diverge. The plan mandates: timestamps/ids are stamped
> by the **proposer** and travel *inside* the command, never re-generated at apply. A property test
> (apply the same log on two fresh state machines → byte-identical SQLite content hash) is a required exit
> gate for Phase 0.
> ─────────────────────────────────────────────────

### 3) One openraft engine, N groups, shared transport

- **Type config:** one `RaftTypeConfig` (`StoreCommand` as the app data, a `StoreResponse` as the reply).
- **Groups:** `MultiRaftDriver` owns `HashMap<Lane, Raft<C>>` — three `Raft` instances, each with its own `RaftLogStorage` + `RaftStateMachine`.
- **Transport:** a **single** gRPC service (`RaftService`) multiplexes append-entries/vote/snapshot RPCs for all groups, keyed by `group_id`. One TCP/HTTP2 connection pool between managers carries all lanes (so multi-raft does *not* triple the connection count). Implements openraft `RaftNetwork`/`RaftNetworkFactory`.
- **Storage:** see decision below.

### 4) State-machine storage layout (key decision)

Two viable layouts; the plan **recommends Option A** and documents B as the fallback (tracked in Open Questions):

- **Option A — single SQLite DB, lane-scoped apply (recommended).** Keep today's one `store.sqlite`. Each lane applies only its own tables. Per-lane Raft log + snapshot metadata live in separate small stores (e.g. `raft/<lane>/`). Snapshots capture only that lane's table subset (via `sqlite_dbpage`/`VACUUM INTO` of the relevant tables, or a logical dump). *Pros:* cross-lane reads are ordinary SQL joins; minimal disruption to `Store`; the read-model cache already exists. *Cons:* snapshot logic must scope to a table subset; one file is a shared fsync surface (mitigated by WAL + per-lane batching).
- **Option B — one SQLite file per lane.** Physical isolation; trivially independent snapshots/compaction. *Cons:* cross-lane reads need `ATTACH DATABASE` or in-memory joins; bigger surgery to `Store`; three fsync surfaces.

> ★ Insight ─────────────────────────────────────
> Option A wins for *ergonomics and correctness-of-reads* (joins stay joins), and the lane benefit we
> actually care about — **write-path isolation** — comes from independent *logs and apply loops*, not from
> separate data files. Physical file separation (B) is a performance refinement we can adopt later if a
> single WAL fsync becomes the bottleneck, without changing the lane model.
> ─────────────────────────────────────────────────

### 5) Cross-lane consistency & the read model

Lanes commit independently, so a placement (scheduler lane) may briefly run against a job_config (control lane) that's a few entries behind. This is safe because:

- The **in-memory `SchedulingReadModel`** projects committed state from *all three* lanes into one view the scheduler reads from. Each lane's apply loop updates the cache; the scheduler never reads mid-commit DB state.
- The **`allocation_epoch` CAS** already rejects placements computed against stale cluster state (`PlanApplyOutcome::Rejected` → caller retries). Cross-lane lag manifests as an occasional epoch conflict + retry, not a correctness bug.
- **Read-your-writes** is guaranteed *within* a lane; *across* lanes we provide read-after-commit with bounded staleness, surfaced as a metric (`lane_apply_lag`).

### 6) Write routing, leadership, and gating

- Each lane elects its own leader; a node can lead some lanes and follow others (load spreads naturally).
- A mutating RPC routes its command to the **current leader of that command's lane**. On a follower, openraft returns `ForwardToLeader`; the driver either forwards or returns `FAILED_PRECONDITION` + leader hint (matching the redirect model already described in [`docs/persistence-plan.md`](../persistence-plan.md)).
- Reads are served locally from the read model (followers can serve reads; linearizable reads use openraft `ensure_linearizable` when explicitly requested).

### 7) Default-on multi-raft + escape hatch

- `persistence.type = "raft"` ⇒ **multi-raft with the three default lanes**, no extra config.
- Escape hatch: `raft.topology = "single"` collapses all tables into one group (for tiny clusters or debugging). Default is `raft.topology = "lanes"`.
- A node joins the cluster once; the join enrolls it into **all** lanes the cluster runs. Operators never hand-assign lanes.

### 8) Ergonomics

- **Config:** unchanged shape plus optional knobs (all defaulted):
  ```jsonnet
  persistence: {
    type: "raft",
    raft: {
      nodeId: "mgr-a",
      bootstrap: true,
      peers: ["mgr-a", "mgr-b", "mgr-c"],
      joinTokenTtlSeconds: 900,
      dataDir: ".legion/raft",        // new: per-lane log/snapshot root
      topology: "lanes",              // new: "lanes" (default) | "single"
      snapshotEntries: 10000,         // new: per-lane compaction threshold
      // advanced/optional:
      // lanes: { scheduler: { electionTimeoutMs: 300 }, ... }
    },
  }
  ```
- **CLI:** `raft status` shows an aggregate health line by default; `raft status --per-lane` (and `raft history --lane scheduler`) expose detail. `raft command ... --lane <lane>` targets a specific group; omitting `--lane` applies cluster-wide where meaningful (e.g. add-voter enrolls into all lanes).
- **UI:** the existing Cluster → Raft page gains a per-lane breakdown (3 status cards) but keeps the single roll-up at the top.

### 9) What stays the same

The RPC/CLI/UI/history/join-token surfaces, the `PersistenceDriver` trait, and the `local_disk`/`streaming` tiers are unchanged in shape. `RaftStatus` gains optional per-lane detail (additive, backward-compatible).

---

## Operability at Scale: Tuning, Diagnostics & Autopilot

Multi-raft only pays off if operators can *see* which lane is the bottleneck and *know how to fix it*. This section is the plan for the tuning surface, the docs that explain it, the bottleneck telemetry, the UI, and a speculative autopilot — all built so that **a smart developer with little distributed-systems background can understand what is happening and act**, while a Raft expert still has the raw knobs.

### T0) Design principle — learnable without being a Raft expert *(cross-cutting)*

Every operability surface below follows **progressive disclosure**: three layers, each a strict superset of context for the one above.

| Layer | Audience | What they see | Example |
| --- | --- | --- | --- |
| **L1 — Verdict** | "is it healthy / what do I do?" | One plain-language status line + at most one recommended action. No Raft vocabulary. | *"Scheduling is keeping up. ✅"* / *"Job submissions are piling up faster than they're being saved. Try profile `high-throughput` on the control lane."* |
| **L2 — Symptom → fix** | "why, and what exactly changes?" | Named symptom, plain cause, the specific knob, current→suggested value, expected effect, tradeoff. | *"The control lane's leader is **busy** (writes wait ~280ms). Cause: big submit burst. Fix: `control.tuning.maxPayloadEntries 300 → 1024` (more writes per round-trip; uses slightly more memory)."* |
| **L3 — Raw** | Raft experts | Live openraft metrics + every effective knob with its source. | `apply_lag=312ms last_log_index=98142 last_applied=97003 millis_since_quorum_ack=41` |

Supporting mechanisms (used by config, docs, and UI alike):

- **A jargon-translation table** is the single source of truth for plain-language names, reused by the UI tooltips, the CLI `raft explain`, and the docs glossary, so the vocabulary is identical everywhere.
- **`legion raft explain <knob|metric|symptom>`** prints the L2 explanation for any term (`--json` for agents). The UI renders the same text in a tooltip; the docs embed it by anchor. One source, three surfaces.
- **A built-in "how this works" explainer** (a short, analogy-driven mental model — *"each lane is a separate to-do list with its own scribe; the scheduler's list must never wait behind the others"*) ships in the docs and is linked from the UI's *Learn* affordance.

> ★ Insight ─────────────────────────────────────
> The accessibility win and the agent-readiness win are the **same** mechanism. An LLM agent and a
> junior developer both need: a structured verdict, a named cause, and a concrete next action — not a
> wall of `last_applied` numbers. So the L1/L2 layers are emitted as the **typed diagnosis object**
> (T3) that the UI renders and the MCP tool returns. We build the explanation model once and it serves
> humans and agents simultaneously.
> ─────────────────────────────────────────────────

### T1) Tuning in config — the knobs (best ways to tweak)

Config has **three tiers**, each merging over the one before, so the common case is zero knobs and the hard case is fully controllable:

```jsonnet
persistence: {
  type: "raft",
  raft: {
    nodeId: "mgr-a", bootstrap: true, peers: ["mgr-a","mgr-b","mgr-c"],
    dataDir: ".legion/raft", topology: "lanes",

    // TIER 1 — global default for every lane. Most operators set only `profile`.
    tuning: { profile: "balanced" },   // responsive | balanced | high-throughput | wan

    // TIER 2 — per-lane overrides (merge over the global tuning)
    lanes: {
      scheduler: { tuning: { profile: "responsive" }, leaderPreference: ["mgr-a","mgr-b"] },
      status:    { tuning: { profile: "high-throughput" }, replicateRawHealth: false },
      // control: inherits global `balanced`
    },

    // TIER 3 — cluster shape (Legion-level, not raw openraft)
    voters: 3,                 // voters per group = quorum size = "how many leaders' worth of durability"
    leaderBalancing: "auto",   // auto | off | pinned — spread lane leadership across managers
    autopilot: { mode: "advisory" },   // off | advisory | auto  (see T5)
  },
}
```

**Profiles are the ergonomic core.** Each profile is a *named bundle* of the raw openraft knobs, chosen for a scheduling goal, so nobody has to learn 13 knobs to get a good result:

| Profile | Goal (plain language) | Sets (openraft) | Use when |
| --- | --- | --- | --- |
| `responsive` | "commit each write as fast as possible" | small `max_payload_entries`, short `heartbeat_interval`/election timeouts | the **scheduler** lane; latency-sensitive placement. |
| `balanced` *(default)* | "good latency and throughput" | openraft defaults | general use / the control lane. |
| `high-throughput` | "absorb big bursts, fewer round-trips" | large `max_payload_entries`, bigger snapshot cadence | the **status** lane, submit storms. |
| `wan` | "survive high-latency links" | longer heartbeats/election timeouts, bigger `replication_lag_threshold` | managers across datacenters. |

Any raw knob can still be set explicitly to override the profile. The **raw openraft `Config` knobs** Legion exposes (grounded in openraft 0.9), with their real defaults:

| Knob (config name) | openraft field | Default | Hot-reload? |
| --- | --- | --- | --- |
| `heartbeatIntervalMs` | `heartbeat_interval` | 50 | restart |
| `electionTimeoutMinMs` / `MaxMs` | `election_timeout_min`/`max` | 150 / 300 | restart |
| `maxPayloadEntries` | `max_payload_entries` | 300 | restart |
| `replicationLagThreshold` | `replication_lag_threshold` | 5000 | restart |
| `snapshotPolicy` | `snapshot_policy` | `since_last:5000` | restart |
| `maxInSnapshotLogToKeep` | `max_in_snapshot_log_to_keep` | 1000 | restart |
| `installSnapshotTimeoutMs` / `sendSnapshotTimeoutMs` | `install_snapshot_timeout` / `send_snapshot_timeout` | 200 / 0 | restart |
| `purgeBatchSize` | `purge_batch_size` | 1 (Legion default 256) | restart |
| *runtime toggles* | `enable_tick`/`enable_heartbeat`/`enable_elect`, `trigger_*` | on | **hot** |
| *leadership* | `change_membership`, leader transfer | — | **hot** |

> ★ Insight ─────────────────────────────────────
> Honesty about hot vs. restart matters for trust. In openraft 0.9 the timing/payload `Config` is fixed
> at `Raft::new`, so changing `maxPayloadEntries` needs a **rolling restart** (safe: restart one voter at
> a time, quorum holds). What *is* live: enabling/disabling tick/heartbeat/elect, triggering
> snapshots/elections, **leader transfer**, and **membership** (add/remove voter). The plan therefore
> makes the autopilot's *automatic* actions prefer the hot levers (rebalance leaders, change membership,
> trigger snapshot) and treat restart-only knobs as **recommendations applied on the next rolling
> restart** — never a surprise bounce.
> ─────────────────────────────────────────────────

**Cluster-shape knobs answer the "too many workers / wrong number of leaders" questions directly:**

- **"Too many workers per leader"** → the status lane absorbs all worker heartbeats/health. Levers, in order: `status.tuning = high-throughput` (batch more), `status.replicateRawHealth = false` (replicate only debounced transitions, not every check), add managers, or (Phase 8) shard the status lane. The diagnosis (T3) computes the **workers-per-status-leader ratio** and recommends the specific lever.
- **"More/fewer leaders"** → two distinct meanings, both surfaced: (a) *durability/quorum width* = `voters` per group (3 → 5 tolerates one more failure, costs a round-trip); (b) *leader placement* = `leaderBalancing`/`leaderPreference`, spreading the three lanes' leadership across different managers so one node isn't the leader for all three (CPU hotspot). The UI leadership map (T4) makes imbalance visible at a glance.

`legion raft tuning [--lane <l>] [--json]` prints the **effective** config per lane (value + source: default / profile / override) and validates on load (reject out-of-range, warn on risky combos like `heartbeat ≥ electionTimeoutMin`).

### T2) Documenting what each change does to scheduling

- **Single source of truth, no drift.** Each knob is a typed field carrying doc metadata (`#[doc]` + a `TuningKnob { plain_name, effect_on_scheduling, symptom, tradeoff, safe_range, hot_reload }`). The reference table in `docs/multi-raft-tuning.md`, the `raft explain` output, and the UI tooltips are **generated from that one struct** — exactly the pattern Legion already uses for `job schema` / `GetStateStoreSchema`. Change the knob, the docs change.
- **Every knob documented as scheduling cause-and-effect**, not Raft mechanics. Example rows:

  | Knob | Plain "what it does" | Effect on scheduling | Symptom it fixes | Tradeoff |
  | --- | --- | --- | --- | --- |
  | `maxPayloadEntries` | how many writes ship per network round-trip | ↑ throughput under bursts; ↓ tail latency if too large | submissions/placements lagging during a burst | more memory per round-trip |
  | `electionTimeout*` | how long before a leaderless lane re-elects | too low → spurious elections stall that lane; too high → slow failover | "leader keeps changing" / "slow recovery after a manager dies" | failover speed vs. stability |
  | `voters` | how many managers must agree per write | ↑ durability; each extra voter adds a round-trip | tolerate more manager failures | write latency |
  | `replicateRawHealth` | save every health check vs. only state changes | off → far fewer status writes, scheduler less contended | status lane saturated by health checks | brief delay seeing flaps |

- **A symptom→fix decision matrix** ships in the docs and *is the same table the diagnosis engine uses* (T3), so what the docs teach and what the system recommends are identical:

  | You observe… | Likely bottleneck | Change | Direction |
  | --- | --- | --- | --- |
  | Placements slow only during submit storms | control lane crowding shared resources | `control.profile` | → `high-throughput` |
  | Scheduler apply-lag rising, others fine | scheduler lane saturated | `scheduler.profile` / Phase 8 shards | → `responsive` / shard |
  | A lane re-elects repeatedly | election timeout too tight for load/latency | `electionTimeout*` or `profile` | → `wan` / raise |
  | One manager pinned at high CPU | all lane leaders on one node | `leaderBalancing` | → `auto` |
  | Status writes dominate; many workers | worker fan-in too high | `status.replicateRawHealth` / add managers | → `false` / scale out |

### T3) Tracking bottlenecks & exposing them (via tracing → users *and* agents)

A per-lane **sampler task** reads each group's openraft `RaftMetrics` watch stream plus Legion-internal counters, on a fixed tick, and turns raw numbers into a typed **diagnosis**.

**The signals (grounded in openraft `RaftMetrics` + Legion instrumentation):**

| Signal | Derived from | Means (plain) | Bottleneck it reveals |
| --- | --- | --- | --- |
| **Apply lag** | `last_log_index − last_applied` | "writes are committed but not yet saved to SQLite" | the **apply path / SQLite** is the bottleneck for that lane. |
| **Commit/replication lag** | leader `last_log_index` − min over `replication` map | "followers are behind the leader" | **network / a slow follower**. |
| **Quorum-ack age** | `millis_since_quorum_ack` | "time since a majority last confirmed the leader" | **leader isolation / overload**. |
| **Propose→commit p99** | Legion `#[instrument]` span per `client_write` | "how long a write waits" | that **lane is saturated**. |
| **Election churn** | rate of `current_term` increase / leader changes | "leadership keeps flipping" | **election timeout too tight**. |
| **Forwarded/rejected writes** | Legion counter on `ForwardToLeader` | "clients hitting non-leaders" | **leadership placement**. |
| **Snapshot/install rate** | `snapshot`/`purged` movement | "compaction churn / laggards needing full copies" | `snapshot_policy` / `replication_lag_threshold`. |
| **Workers-per-status-leader** | node count ÷ status-lane leaders | "fan-in per scribe" | **worker overload** of the status lane. |

**Emission (tracing-first, OTEL-ready).** Each tick emits a structured `tracing` event per lane with stable fields (`lane`, `apply_lag_ms`, `commit_lag`, `quorum_ack_ms`, `propose_commit_p99_ms`, `verdict`) at `debug`, and a single `info` `raft.diagnosis` event when the verdict changes (per AGENTS.md tracing guidance: lifecycle, not per-iteration noise; never secrets). These fields are named so the *future* OTEL metrics pipeline (an `observability.md` gap) can export them as gauges with no re-instrumentation.

**Exposure — one diagnosis object, every surface:**

- **gRPC:** new `GetRaftDiagnostics` returns `RaftDiagnostics { repeated RaftLaneDiagnosis lanes }`, each = `{ lane, health (ok|watch|degraded), verdict_plain, top_symptom, recommendation { knob, current, suggested, expected_effect, hot_reload }, raw_metrics }`. `RaftStatus` keeps the L3 numbers; diagnostics adds L1/L2.
- **CLI:** `legion raft diagnose [--lane] [--json]` (L1 line by default, `-v` for L2, `-vv` for L3) and `legion raft explain <term>`.
- **Agents:** a **read-only** MCP tool `legion_raft_diagnose` (`mutates: false`) + a `catalog()` entry + a seeded saved query `agent:raft-bottlenecks` over a `raft_lane_metrics` read-model view. *This deliberately extends the agent surface*: today `raft` is operator-only, but the user wants agents to see bottlenecks — so **read-only** raft diagnostics join the agent surface while mutating raft ops stay operator-only.

### T4) UI — Raft health, bottlenecks & current config (learnable)

Extend the existing [`ui/src/pages/Raft.tsx`](../../ui/src/pages/Raft.tsx) (and surface a summary on [`ui/src/pages/Observability.tsx`](../../ui/src/pages/Observability.tsx)), reusing the established `createResource` + 2s polling pattern. The page is laid out by disclosure layer:

- **Top: one-line verdict (L1).** A single banner — *"Scheduling is healthy"* or *"⚠ Control lane is the bottleneck — job saves are queuing."* — with the recommended action as a button. No jargon.
- **Leadership map.** A tiny matrix (managers × lanes) showing who leads each lane, so "all three leaders on mgr-a" is obvious at a glance; imbalance is flagged with the `leaderBalancing` fix.
- **Per-lane cards (L2).** For each lane: a plain health pill, a one-line cause, and developer-friendly metrics ("writes wait ~40ms", "followers in sync") with a **"what's this?" tooltip** on every metric drawn from the `raft explain` text. Sparklines from the polled history.
- **Effective-config panel (L2/L3).** Per lane: each knob with **current value, source badge** (`default` / `profile:responsive` / `override`), a plain "what it affects" line, and an *Explain* link. Read-only by default; edits produce a copy-pasteable config block (and, if autopilot is on, show what it would/did change).
- **Diagnosis & Recommendations (L2).** The typed `recommendation` rendered as: *cause → suggested change (current→suggested) → expected effect → tradeoff*, with **Copy to config** and (if `autopilot=auto`) an *"applied automatically · view in history"* badge.
- **Learn affordance.** A collapsible "How multi-raft works here" explainer (the T0 analogy + a 4-line mental model) so a developer can self-onboard in the UI without leaving for the docs.

### T5) Speculative — Raft Autopilot (auto-tweaking)

An optional control loop that closes the observe→diagnose→act loop, modeled on Legion's existing eval loop. **Default `advisory`** (recommends only); `auto` is opt-in.

- **Loop:** sample metrics (T3) → diagnose → generate a candidate change → *advisory:* surface it (T4); *auto:* apply it within guardrails → watch the SLO window → **keep if better, auto-revert if worse**. The verification anchor is the existing [`tests/scheduler_benchmark_slo.rs`](../../tests/scheduler_benchmark_slo.rs) metric set (`max_p99_latency_ms`, `min_evals_per_sec`, `max_conflict_rejection_rate`).
- **Safe action set (eligible for `auto`):** rebalance lane leadership, adjust membership width, trigger snapshots, toggle `replicateRawHealth`, and *recommend* restart-only knob changes for the next rolling restart. **Never auto:** election-timeout tightening, voter removal below safety, or anything membership-affecting during instability.
- **Guardrails:** hysteresis + a cool-down (`maxChangesPerHour`), require SLO headroom before acting, **freeze during election churn / partition / ongoing membership change**, one change at a time, and a full audit trail in `raft history` with `actor=autopilot` and the before/after metrics that justified it. Everything it does is visible and explainable in the same L2 format a human would get.

> ★ Insight ─────────────────────────────────────
> Autopilot is only trustworthy if it is **reversible and accountable**. Each action carries the metric
> snapshot that triggered it and an automatic rollback if the SLO window regresses — so "auto" is a
> bounded experiment with a safety net, not a black box. Because it speaks the same L2 diagnosis language
> as the advisor, an operator reviewing history reads *why* it acted in plain English, which is what makes
> graduating from `advisory` → `auto` a comfortable, informed step rather than a leap of faith.
> ─────────────────────────────────────────────────

---

## Proto, Struct, Persistence, and Config Changes

1. **`src/structs/config.rs`** — extend `RaftPersistenceConfig`/`*Full` with `data_dir`, `topology` (`lanes|single`, default `lanes`), `snapshot_entries`, **the tiered `tuning` (with `profile`) + per-lane `lanes` overrides + cluster-shape knobs (`voters`, `leaderBalancing`, `autopilot`)**, and a `TuningKnob` doc-metadata layer (single source for docs/explain/UI). Defaults keep `type=local_disk` as the global default and `profile=balanced`.
2. **New `src/storage/raft/` module** —
   - `mod.rs` (types: `Lane`, `StoreCommand`, `StoreOp`, `StoreResponse`, `RaftTypeConfig`),
   - `lane.rs` (`lane_for()` classifier + exhaustiveness test),
   - `network.rs` (`RaftNetwork`/`RaftNetworkFactory` over gRPC),
   - `log_store.rs` + `state_machine.rs` (openraft storage over SQLite/`raft/<lane>/`),
   - `driver.rs` (`MultiRaftDriver: PersistenceDriver` owning `HashMap<Lane, Raft<C>>`),
   - **`diagnostics.rs` (per-lane metrics sampler → typed `RaftLaneDiagnosis`; the L1/L2 verdict engine), `tuning.rs` (profiles → openraft `Config`; `TuningKnob` registry + `explain`), `autopilot.rs` (advisory/auto control loop).**
3. **`src/storage/store.rs`** — split each mutating method into `propose_*` (build+route command) and `apply_*` (existing SQL body), invoked by the lane state machine. Read paths unchanged except the read model now fans in from all lanes **and exposes a `raft_lane_metrics` view for the saved query.**
4. **`proto/legion.proto`** — add the internal `RaftService` (append/vote/snapshot, group-keyed) used *between managers*; extend `RaftStatus` with `repeated RaftLaneStatus lanes`; **add `GetRaftDiagnostics` → `RaftDiagnostics`, `GetRaftTuning` → effective per-lane config, and the `RaftLaneDiagnosis`/`TuningKnob`/`Recommendation` messages.** Document every field with public-facing comments (per AGENTS.md, proto is source of truth). Regenerate via `build.rs`.
5. **`src/storage/migrations.rs`** — add `node_health` table (status lane); no destructive changes to existing tables.
6. **`src/manager/server.rs` / `src/cmd/raft.rs`** — surface per-lane status/history; add `--lane` flags; **add `raft diagnose`, `raft tuning`, `raft explain`**; keep aggregate defaults. **Add the read-only `legion_raft_diagnose` MCP tool and seed `agent:raft-bottlenecks`.** Update `catalog()` in `src/cmd/capabilities.rs` and MCP tools in `src/cmd/mcp.rs` for any new/renamed surfaces (AGENTS.md requirement).
7. **`ui/src/pages/Raft.tsx` / `Observability.tsx`** — disclosure-layered Raft health UI (verdict banner, leadership map, per-lane cards with explain-tooltips, effective-config panel, recommendations, Learn affordance).
8. **Docs** — update `docs/raft-operations.md`, `docs/state-store.md`, `docs/persistence-plan.md` (mark store-replication items done); add `docs/multi-raft.md` (operator guide) **and `docs/multi-raft-tuning.md` (generated knob reference + symptom→fix matrix + plain-language "how it works" explainer + glossary)**; cross-link from `README.md`.

---

## How to Execute This Plan as a Claude Dynamic Workflow

This plan is written to be run by a **Claude Dynamic Workflow** (the `ultracode`/`/workflows` orchestration model: a JS script spawns and coordinates subagents across phases, keeping intermediate results in script variables and returning only verified results). The structure below maps directly onto workflow primitives so the orchestration is *codified and rerunnable*, and **every change is verified as it goes**.

### Workflow metadata

```js
workflow({
  name: "multi-raft-persistence",
  description: "Implement lane-partitioned multi-raft for the Legion manager store, verified per phase.",
  // args lets you re-run a slice: { phases: ["P0","P1"], lanes: ["scheduler"] }
  phases: [
    { id: "P0", title: "Real single-group Raft apply path",        detail: "Engine + commit→apply + determinism" },
    { id: "P1", title: "Multi-group runtime + shared transport",    detail: "HashMap<Lane,Raft> over one gRPC service" },
    { id: "P2", title: "Lane routing for all mutations",            detail: "propose_*/apply_* split; lane_for()" },
    { id: "P3", title: "Cross-lane read model + consistency",       detail: "fan-in projection; epoch-CAS interplay" },
    { id: "P4", title: "Per-lane snapshots, compaction, recovery",  detail: "install-snapshot + crash restart" },
    { id: "P5", title: "Real membership/election/join",             detail: "replace the simulation" },
    { id: "P6", title: "Default-on + ergonomics",                   detail: "topology=lanes default; CLI/UI/docs" },
    { id: "P7", title: "Performance hardening + 12× benchmark",     detail: "lane-isolation SLO gate" },
    { id: "P8", title: "(Optional) scheduler-lane key-range shards", detail: "behind a flag" },
    { id: "P9",  title: "Tuning surface + profiles + docs",          detail: "profiles→Config; TuningKnob registry; raft explain" },
    { id: "P10", title: "Bottleneck diagnostics + tracing",          detail: "per-lane verdict; GetRaftDiagnostics; MCP tool" },
    { id: "P11", title: "Learnable Raft UI",                          detail: "L1 verdict, leadership map, explain-tooltips, recs" },
    { id: "P12", title: "(Speculative) Raft autopilot",              detail: "advisory/auto loop, guardrails, auto-revert" },
  ],
});
```

### Decomposition primitives (how each phase fans out)

- **`pipeline(workItems, implement → selfTest → adversarialVerify)`** — within a phase, independent work items (a struct, a storage impl, a test file) flow through *implement → test → verify* without a whole-phase barrier. A work item's verification starts the moment its implementation lands.
- **`parallel([...])`** — used **only** where a step needs *all* prior results at once: the **phase gate** (build + full test suite must see every item) and **synthesis** of the per-lane status surface.
- **`synthesize(results)`** — barrier that merges per-item outputs (e.g. the three lane state machines, or the per-file review findings) into the phase result and the updated checklist in this doc.
- **Adversarial verification** — for every work item that produces a *claim of correctness* (“this command is deterministic”, “this lane is isolated”), spawn a **separate skeptic agent** whose only job is to refute it against the item's `exit_rubric`. Producer and skeptic never share context. A work item is **done only if the skeptic cannot break it.** This is the core “verified as it goes” mechanism.

### Work-item schema

Each work item below is expressed so a workflow can consume it directly:

```jsonc
{
  "id": "P2.upsert_allocation",
  "phase": "P2",
  "depends_on": ["P0.apply_seam", "P1.driver"],
  "files": ["src/storage/store.rs", "src/storage/raft/lane.rs"],
  "intent": "Split upsert_allocation into propose_* (route to scheduler lane) + apply_* (existing SQL).",
  "exit_rubric": [                     // machine-checkable; the skeptic agent tests these
    "upsert_allocation routes to Lane::Scheduler (unit test asserts lane_for)",
    "apply is idempotent on replay (same idempotency_key → no duplicate row)",
    "no behavior change in local_disk mode (existing tests green)"
  ],
  "verify": [                          // run by the implementer AND the phase gate
    "cargo fmt --all -- --check",
    "cargo clippy --all -- -D warnings",
    "cargo test --all --locked storage::"
  ]
}
```

### Per-phase verification gate (“verified as it goes”)

Every phase ends with a **`parallel` barrier** that must pass before the next phase starts. The gate is identical and non-negotiable:

```bash
cargo fmt --all -- --check
cargo clippy --all -- -D warnings
cargo build --all
cargo test --all --locked
```

Plus the phase's **targeted** suite (named per phase below) and, for P7, the **performance SLO** (`tests/scheduler_benchmark_slo.rs` must still pass, and the lane-isolation benchmark must hit target). If any gate command fails, the workflow re-enters the phase (loop-until-green) rather than advancing — and surfaces a diagnosis if it stalls after several rounds.

### Rerun / resume contract

- The workflow is **idempotent per phase**: re-running a completed phase re-verifies (cheap, cached where possible) rather than rewriting.
- `args.phases` restricts a run to a slice (e.g. just `P7` to re-benchmark). `args.lanes` restricts lane-scoped items.
- The **checklist in “Implementation Phases” below is the source of truth for progress** — agents tick boxes as exit criteria are met, so a resumed run knows where to pick up.

---

## Implementation Phases

> Each phase: **work items** (fan out in parallel), an **exit criteria** checklist (the rerun source-of-truth), and a **targeted verify** suite layered on top of the universal gate.

### Phase 0 — Real single-group Raft apply path *(the missing engine)*

Make `raft` actually replicate, with a single group, before partitioning. This de-risks consensus correctness independently of multi-raft.

- [ ] `StoreCommand`/`StoreOp`/`StoreResponse` + `RaftTypeConfig` (`src/storage/raft/mod.rs`).
- [ ] openraft `RaftLogStorage` + `RaftStateMachine` over SQLite (`raft/` dir) (`log_store.rs`, `state_machine.rs`).
- [ ] gRPC `RaftService` + `RaftNetwork`/`RaftNetworkFactory` (`network.rs`, `proto/legion.proto`).
- [ ] Refactor a *first slice* of `Store` mutations into `propose_*`/`apply_*`; route through one `Raft`.
- [ ] Determinism guard: ids/timestamps stamped at propose time, never at apply.

**Exit criteria:** propose→commit→apply round-trip works on a 1-node and 3-node cluster; **property test** (replay the same log on two fresh state machines → identical SQLite content hash) passes; idempotency-key replay is a no-op.
**Targeted verify:** `cargo test --all --locked raft::` + the new determinism/replay tests.

### Phase 1 — Multi-group runtime + shared transport

- [ ] `MultiRaftDriver: PersistenceDriver` owning `HashMap<Lane, Raft<C>>` (`driver.rs`).
- [ ] One `RaftService` multiplexes all groups by `group_id`; one connection pool across lanes.
- [ ] Bootstrap/start all configured lanes from `data_dir`; per-lane log/snapshot dirs.

**Exit criteria:** three groups come up over one transport; each elects independently; `raft_status()` returns per-lane detail; killing one lane's leader does not disturb the others' leadership.
**Targeted verify:** multi-group bring-up integration test (`tests/raft_multigroup.rs`).

### Phase 2 — Lane routing for all mutations

- [ ] `Lane` enum + `lane_for(StoreOp)` classifier (`lane.rs`) with **exhaustiveness + disjointness test** (every table mapped to exactly one lane).
- [ ] Convert **all** remaining `Store` mutations to `propose_*`/`apply_*` and route to the correct lane.
- [ ] `ForwardToLeader` handling: forward or return `FAILED_PRECONDITION` + leader hint.
- [ ] Add `node_health` table + migration; move heartbeat liveness writes to the status lane.

**Exit criteria:** no mutation bypasses Raft when `type=raft`; the lane-mapping test is total/disjoint; `local_disk`/`streaming` behavior is byte-for-byte unchanged (existing suite green).
**Targeted verify:** full `cargo test --all --locked`; a grep-based test/CI check that no `write_connection()` call sites remain *outside* `apply_*`.

### Phase 3 — Cross-lane read model + consistency

- [ ] Read model fans in committed state from all three lanes; scheduler reads only the cache.
- [ ] `lane_apply_lag` metric; bounded-staleness documented.
- [ ] Verify epoch-CAS interplay: cross-lane lag → epoch conflict + retry, never a bad placement.

**Exit criteria:** an integration test schedules against a control-lane config update and confirms either read-after-commit or a clean epoch-rejection+retry (never divergence).
**Targeted verify:** `tests/raft_cross_lane.rs` + existing `tests/scheduler_integration.rs` green.

### Phase 4 — Per-lane snapshots, compaction, recovery

- [ ] Snapshot build/install per lane (Option A: lane-scoped table subset).
- [ ] Compaction at `snapshot_entries`; log truncation.
- [ ] Crash/restart recovery: install snapshot then replay tail.

**Exit criteria:** kill -9 a node mid-write, restart → it catches up via snapshot+log and converges (content hash matches peers); a lagging follower is caught up by install-snapshot.
**Targeted verify:** `tests/raft_snapshot_recovery.rs`.

### Phase 5 — Real membership / election / join *(replace the simulation)*

- [ ] Wire `add-voter`/`add-learner`/`remove-member`/`transfer-leader`/`trigger-election`/`step-down` to openraft `change_membership`/`add_learner`/leadership APIs — per lane, with the cluster-wide convenience default (enroll into all lanes).
- [ ] Join-token flow performs real `add_learner` → promote to voter across lanes; one token, all lanes.
- [ ] Keep `RaftHistoryEvent` audit trail wired to real state transitions.

**Exit criteria:** a new node joins via token and becomes a voter in all lanes; leader transfer/step-down/election reflect *real* openraft state (not simulated counters); membership changes survive restart.
**Targeted verify:** `tests/raft_membership.rs` (3→5 node growth, removal, transfer).

### Phase 6 — Default-on multi-raft + ergonomics

- [ ] `topology` defaults to `lanes`; `single` escape hatch implemented.
- [ ] CLI: aggregate `raft status` + `--per-lane`/`--lane`; UI per-lane cards.
- [ ] Update `catalog()` + MCP tools; `--json` envelopes intact (AGENTS.md).
- [ ] Docs: new `docs/multi-raft.md`; update `raft-operations.md`, `state-store.md`, `persistence-plan.md`, `README.md`; add a `configs/jobs`/`configs/manager` multi-raft example and include new config in `configs/jobs/maximal.jsonnet` if applicable.

**Exit criteria:** `persistence.type=raft` with no other knobs yields a 3-lane cluster; capability-catalog test passes; docs build/links check; `raft status` reads cleanly for a human in one line.
**Targeted verify:** `cargo test --all --locked cmd::` (capabilities/MCP) + a docs link check.

### Phase 7 — Performance hardening + the 12× benchmark

- [ ] Lane-isolation benchmark: saturate control lane (100k job/instance creates) **and** status lane (heartbeat flood) while measuring **scheduler-lane** apply latency/throughput.
- [ ] Compare `topology=single` vs `topology=lanes`; the lane mode must show the isolation win (target: scheduler-lane p99 stays within SLO under load; document the speedup multiple actually achieved).
- [ ] Batching/pipelining tuning per lane (openraft `Config` per lane); keep `tests/scheduler_benchmark_slo.rs` green.

**Exit criteria:** `scheduler_benchmark_slo` passes; lane-isolation bench shows scheduler-lane latency under cross-lane load within target (and materially better than single-group); results recorded in `docs/multi-raft.md` and `benchmarks/`.
**Targeted verify:** `cargo test --test scheduler_benchmark_slo --locked`; `cargo bench --bench scheduler_bench`; new `benches/multi_raft_lane_isolation.rs`.

### Phase 8 — *(Optional, flagged)* scheduler-lane key-range sharding

- [ ] Behind `raft.scheduler.shards = N` (default 1): split the scheduler lane by node-pool/namespace using the existing `allocation_epoch_partition` seam.
- [ ] Hash/route placement commands to the owning shard; rebalance policy stub.

**Exit criteria:** with `shards=1` behavior is identical to Phase 7; with `shards>1`, placement throughput scales with shard count in the bench; off by default.
**Targeted verify:** `tests/raft_scheduler_shards.rs`.

> **Phase ordering note:** P9–P11 below are **core** (the operability requirement), not optional like P8 — the numbering is just sequence. They depend on the multi-group runtime (P1–P2) and are most useful after default-on (P6) and perf (P7); P9 and P10 can fan out in parallel once P2 lands. P12 (autopilot) is **speculative**, default-off, and depends on P10.

### Phase 9 — Tuning surface + profiles + docs *(answers "how do I change it")*

- [ ] `tuning.rs`: `profile` presets (`responsive|balanced|high-throughput|wan`) → openraft `Config`; per-lane merge; cluster-shape knobs (`voters`, `leaderBalancing`/`leaderPreference`).
- [ ] `TuningKnob` doc-metadata registry as the single source for the generated knob reference, `raft explain`, and UI tooltips.
- [ ] Config validation (range/risky-combo warnings) + `raft tuning [--lane] [--json]` (effective value + source) + `raft explain <term>`.
- [ ] Generate `docs/multi-raft-tuning.md` (knob reference + symptom→fix matrix + plain-language explainer + glossary).

**Exit criteria:** `type=raft` with only `profile` set yields a sensible per-lane `Config`; `raft tuning` shows value+source; `raft explain maxPayloadEntries` returns the L2 explanation; the generated doc matches the typed registry (a test asserts no drift).
**Targeted verify:** `cargo test --all --locked tuning::` + a doc-generation/no-drift test.

### Phase 10 — Bottleneck diagnostics + tracing + agent/CLI exposure *(answers "where's the bottleneck")*

- [ ] `diagnostics.rs`: per-lane sampler over openraft `RaftMetrics` + Legion counters → typed `RaftLaneDiagnosis` (health, verdict, top symptom, recommendation).
- [ ] Structured `tracing` per lane (stable fields, OTEL-ready) + `raft.diagnosis` event on verdict change.
- [ ] `GetRaftDiagnostics`/`GetRaftTuning` RPCs; `raft diagnose [-v|-vv]`; read-only `legion_raft_diagnose` MCP tool + `catalog()` entry; seed `agent:raft-bottlenecks` + `raft_lane_metrics` view.

**Exit criteria:** under a forced single-lane overload, `raft diagnose` names the right lane + symptom + a concrete recommendation; the MCP tool returns the same typed object; the verdict matches the symptom→fix matrix (shared table).
**Targeted verify:** `tests/raft_diagnostics.rs` (inject overload → assert verdict/recommendation) + capabilities/MCP tests.

### Phase 11 — Learnable Raft UI

- [ ] Extend `ui/src/pages/Raft.tsx`: L1 verdict banner + action, leadership map, per-lane cards with explain-tooltips, effective-config panel (value+source), recommendations (copy-to-config), Learn affordance; summary tile on `Observability.tsx`.
- [ ] Wire to `GetRaftDiagnostics`/`GetRaftTuning` via the existing 2s polling pattern; regenerate `ui/src/gen/` clients.

**Exit criteria:** a developer with no Raft background can open the page, read the one-line verdict, hover any metric for a plain explanation, see which knob to change (current→suggested), and find the leadership balance — without external docs.
**Targeted verify:** UI build (`vite build`) green; component test for the verdict/recommendation rendering; manual usability pass against the L1/L2/L3 layering.

### Phase 12 — *(Speculative, default-off)* Raft Autopilot

- [ ] `autopilot.rs` control loop: observe→diagnose→(advisory surface | auto apply)→watch SLO→keep/revert; `autopilot.mode = off|advisory|auto`.
- [ ] Safe action set (leader rebalance, membership width, snapshot trigger, `replicateRawHealth`, restart-knob *recommendations*); guardrails (hysteresis, `maxChangesPerHour`, freeze during instability, SLO headroom); audit in `raft history` (`actor=autopilot` + before/after metrics) + auto-rollback on regression.

**Exit criteria:** `advisory` surfaces the same recommendation a human would pick; `auto` applies a safe change under a synthetic bottleneck, **and auto-reverts when an injected regression breaches the SLO**; never acts during simulated election churn/partition; every action is explainable in L2 form in history.
**Targeted verify:** `tests/raft_autopilot.rs` (apply-then-keep and regress-then-revert paths) + `tests/scheduler_benchmark_slo.rs` green with autopilot enabled.

---

## Testing Plan *(the missing coverage, mapped to phases)*

### Unit (beside code, `#[cfg(test)]`)
- **Lane mapping** total & disjoint; `lane_for()` for every `StoreOp` (P2).
- **Command determinism**: serialization is stable; no `now()`/`rand()` at apply (P0).
- **Idempotency**: replaying a committed command with the same key is a no-op (P0/P2).
- **Config defaults**: `type=raft` ⇒ `topology=lanes`, three lanes; `single` collapses (P6).
- **Status mapping**: `RaftStatus` aggregate + per-lane adapters (P1/P6).
- **Profile → Config**: each profile maps to the expected openraft knobs; per-lane override merges over global; invalid combos rejected (P9).
- **Doc no-drift**: the generated `multi-raft-tuning.md` knob table equals the `TuningKnob` registry; `raft explain` covers every knob/metric/symptom (P9).
- **Diagnosis mapping**: synthetic metric inputs → expected verdict/recommendation; the diagnosis table == the docs symptom→fix matrix (P10).

### Integration (`tests/`)
- `raft_apply_roundtrip.rs` — propose→commit→apply, 1- and 3-node (P0).
- `raft_multigroup.rs` — three groups up on one transport; independent leadership (P1).
- `raft_cross_lane.rs` — placement vs control-lane update: read-after-commit or clean epoch retry (P3).
- `raft_snapshot_recovery.rs` — kill-9 + restart catch-up; install-snapshot for laggards (P4).
- `raft_membership.rs` — token join across all lanes; grow/shrink; transfer; survives restart (P5).
- `raft_diagnostics.rs` — inject a single-lane overload → `raft diagnose` + `legion_raft_diagnose` name the right lane/symptom/recommendation (P10).
- `raft_autopilot.rs` — advisory matches human pick; auto applies a safe change then **auto-reverts on injected SLO regression**; freezes during instability (P12).
- `raft_scheduler_shards.rs` — optional shard scaling (P8).

### Multi-node / fault injection
- **Partition**: minority partition rejects writes; majority continues; rejoin catches up (P4/P5).
- **Leader loss per lane**: each lane re-elects without cross-lane impact (P1).
- **Determinism under churn**: N nodes, randomized op stream → identical SQLite content hash on all (P0/P2). This is the headline correctness invariant; run it in CI.

### Performance / regression
- `tests/scheduler_benchmark_slo.rs` must stay green throughout (existing SLOs: `min_evals_per_sec`, `max_p99_latency_ms`, `max_conflict_rejection_rate`, `min_correctness_ratio`).
- `benches/multi_raft_lane_isolation.rs` — scheduler-lane latency under control+status saturation, `single` vs `lanes` (P7).
- Regression: `local_disk`/`streaming` paths unchanged (entire existing suite green every phase).

### Adversarial verification (workflow-level)
For each correctness claim (determinism, lane isolation, idempotency, no-bypass), a **skeptic agent** attempts to construct a counterexample against the item's `exit_rubric` before the item is marked done.

---

## Performance Targets / SLOs

Mirroring the source tweet's framing — *the scheduler is never blocked*:

- **Scheduler-lane apply p99** stays within `max_p99_latency_ms` (from `tests/scheduler_benchmark_slo.rs`) **while** the control lane absorbs a 100k-create burst and the status lane absorbs a heartbeat flood.
- **Lane-isolation speedup**: scheduler-lane throughput under cross-lane load in `topology=lanes` must be **materially higher** than `topology=single` on the same workload; record the measured multiple (the tweet reports ~12× for sandbox creates — our number will differ; the *requirement* is a large, documented win, not a specific constant).
- **No correctness regression**: `min_correctness_ratio` and `max_conflict_rejection_rate` SLOs hold.
- **Connection efficiency**: multi-raft uses a single inter-manager connection pool (lane count does not multiply connections).

---

## Risks & Mitigations

| Risk | Mitigation |
| --- | --- |
| Non-deterministic apply → replica divergence | Stamp ids/timestamps at propose; CI content-hash equality test under randomized churn (P0/P2). |
| Hidden write that bypasses Raft | `propose_*`/`apply_*` split + CI check that no `write_connection()` exists outside `apply_*` (P2). |
| Cross-lane staleness causes bad placement | Read-model fan-in + allocation-epoch CAS retry; bounded-staleness metric (P3). |
| Snapshot scoping bug (Option A) corrupts a lane | Per-lane snapshot round-trip test + content hash vs peers (P4); Option B fallback documented. |
| Operator confusion from 3 groups | Default-on, single join token, aggregate status line; per-lane is opt-in (P6). |
| Mixed-mode / rolling upgrade from scaffold-raft or local_disk | One-time import: read existing `store.sqlite`, seed each lane's log via a bootstrap snapshot; documented upgrade path (P6). |
| Perf tuning regresses scheduler | SLO test is a hard gate every phase; per-lane openraft `Config` tuning isolated to P7. |
| Scope creep into key-range sharding | Phase 8 is optional, flagged, default-off. |
| Misconfiguration via raw knobs | Profiles cover the common case; load-time validation rejects out-of-range and warns on risky combos (e.g. `heartbeat ≥ electionTimeoutMin`); `raft tuning` shows effective value+source (P9). |
| Diagnosis is wrong / noisy → bad advice | Verdict engine and docs share one symptom→fix table (tested for parity); thresholds default-sane and tunable; verdict-change events are debounced (P10). |
| Autopilot makes things worse | Default `advisory`; conservative `auto` action set; freeze during instability; SLO-anchored auto-revert; full audit with justifying metrics (P12). |
| Tuning docs drift from real knobs | Knob reference, `raft explain`, and UI tooltips are generated from one `TuningKnob` registry; a no-drift test fails the build on mismatch (P9). |

---

## Open Questions

1. **State-machine layout** — Option A (single SQLite, lane-scoped apply) vs Option B (file-per-lane). Plan recommends **A**; revisit if a single WAL fsync becomes the write bottleneck under P7.
2. **`node` table split** — confirm `node_health` (status lane) vs keeping liveness on control at debug frequency. Plan recommends the split.
3. **Forward vs reject on follower writes** — auto-forward to the lane leader (better UX) vs `FAILED_PRECONDITION` + hint (simpler, explicit). Plan leans **auto-forward** with a hint fallback.
4. **Linearizable reads** — default to read-model (bounded staleness) and offer opt-in linearizable reads, or make some RPCs linearizable by default? Plan: bounded-staleness default, opt-in linearizable.
5. **Lane count/identity as config** — fixed three lanes vs operator-definable lane→table map. Plan: fixed three by default; advanced override deferred.
6. **Upgrade from today's simulated raft** — is an in-place migration needed, or is `raft` treated as not-yet-GA so we can require a fresh cluster for the real engine? Affects P6 upgrade work.
7. **eval lane placement** — evals live on the scheduler lane here; confirm the eval-loop wake/notify path tolerates eval being replicated (vs a local-only queue with replicated *results*).
8. **Service discovery lane (decided, §1a)** — plan recommends **no dedicated lane by default**: definitions/intentions → control, health → status, registration → derived (scheduler lane only if materialized), resolution → read model. Opt-in `raft.lanes.serviceDiscovery` promotes registration+health to their own group for SD-heavy fleets. Open sub-question: do health checks ride Raft at all, or only their **debounced** state transitions (Consul-style off-Raft raw checks)? Plan leans toward replicating only debounced transitions to bound status-lane volume.
9. **Hot-reload depth** — openraft fixes timing/payload `Config` at construction, so those need a rolling restart. Is a rolling-restart "apply tuning" helper worth building (drain → restart one voter → repeat), or do we document the manual procedure? Plan: document first, helper later.
10. **Autopilot default & blast radius** — keep `advisory` the default (recommend-only) and require explicit opt-in to `auto`? Which actions are *ever* eligible for `auto` (plan: leadership/membership/snapshot/debounce only)? Plan: advisory default, conservative auto set, full audit + auto-revert.
11. **Diagnosis thresholds** — fixed defaults (e.g. apply-lag "watch" at 100ms, "degraded" at 250ms) vs. SLO-derived vs. learned baselines. Plan: ship sane fixed defaults, make them tunable, consider learned baselines later.
12. **Profile set** — are four profiles (`responsive|balanced|high-throughput|wan`) the right vocabulary for non-experts, or do we need fewer/clearer names? Validate with the L1 usability pass (P11).

---

## Acceptance Criteria

- With `persistence.type = "raft"` and no other configuration, a 3-node cluster runs **three Raft lanes** (control/scheduler/status), replicates **all** manager mutations, and survives node loss with quorum.
- The **same command log replayed on fresh nodes yields byte-identical SQLite state** (determinism invariant), verified in CI under randomized churn.
- **Scheduler-lane apply latency stays within SLO** while control- and status-lane bursts run concurrently, and `topology=lanes` measurably beats `topology=single` on that workload (documented multiple).
- The **previously-simulated operator commands** (election, transfer, step-down, membership, join-token) drive **real openraft state**, per lane, with the audit history intact.
- **Snapshots/compaction/recovery** work per lane: a crashed node rejoins and converges; a lagging follower is caught up by install-snapshot.
- **Ergonomics preserved**: one config block, one join token enrolls all lanes, one aggregate `raft status` line (with `--per-lane` detail), `--json` envelopes and capability catalog updated.
- **No regression** for `local_disk`/`streaming`: the full existing test suite stays green; `tests/scheduler_benchmark_slo.rs` passes at every phase.
- **Missing tests added**: apply round-trip, multi-group, cross-lane, snapshot/recovery, membership, partition/fault-injection, determinism-under-churn, and the lane-isolation benchmark all exist and pass.
- **Docs updated**: `docs/multi-raft.md` added; `raft-operations.md`, `state-store.md`, `persistence-plan.md`, `README.md` reflect the real, multi-raft, default-on backend.
- **Tunable at scale**: per-lane + global `tuning` profiles and cluster-shape knobs (`voters`, `leaderBalancing`) let an operator change a bottlenecked lane, the leader balance, and the worker-fan-in response; `raft tuning` shows the effective value and its source; restart-vs-hot is documented per knob.
- **Bottlenecks are observable to humans and agents**: a per-lane diagnosis (health/verdict/top-symptom/recommendation) is emitted as structured tracing and exposed via `GetRaftDiagnostics`, `raft diagnose`, and the read-only `legion_raft_diagnose` MCP tool; a forced single-lane overload is correctly named with a concrete fix.
- **Learnable without distributed-systems expertise**: the UI leads with a one-line plain verdict, every metric has a plain-language "what's this?" explanation, the effective config shows value+source+effect, and recommendations give current→suggested with expected impact — a smart developer can diagnose and fix a lane without reading Raft theory. `raft explain <term>` and the generated tuning guide back this up.
- **Speculative autopilot is safe**: `advisory` by default; `auto` only touches the safe action set, freezes during instability, audits every change with the metrics that justified it, and auto-reverts on SLO regression.
