# Execution Plan: Real Durability + Operational Table Stakes

> **Judgement calls:** [`calls/durability-and-ops-execution-plan_calls.md`](../calls/durability-and-ops-execution-plan_calls.md)

> **Status:** EXECUTED (2026-07-23) — all phase-table rows (A-0..A-4,
> B-0..B-3) are complete; execution-time judgment calls are logged in
> §JC-16+. Remaining work is the §6 follow-ups (tracked in
> `internal_docs/NEXT_UP.md`). **Amended 2026-07-28:** §JC-40's co-located lane
> leadership is corrected from an invariant to a best-effort property; see
> the "CORRECTION (2026-07-28)" block in that section.
>
> This document was the working plan for two workstreams executed together on
> branch `claude/todos-xv56bj`:
>
> - **Workstream A — Real durability:** make `persistence.type = raft` actually
>   replicate manager state, executing the core of
>   [multi-raft-persistence-plan.md](multi-raft-persistence-plan.md) (its P0–P6),
>   with one major, documented architecture deviation (§A2).
> - **Workstream B — Operational table stakes:** the missing boring-but-critical
>   operational features found by a code audit: a metrics endpoint, manager health
>   probes, node cordon/drain, a worker↔manager version handshake, and state
>   backup/restore.
>
> Every phase ends with a verification gate (fmt + clippy `-D warnings` + full
> test suite) and targeted tests. Judgment calls made along the way are logged in
> §JC at the bottom — read that section to understand every place this plan
> deviates from prior plans or picks one defensible option over another.

---

## 1. Research: what the codebase actually is (audit findings, 2026-07-23)

Three deep code audits (storage/raft, observability, scheduler/node seams) were
run before writing this plan. Load-bearing facts:

### Storage / Raft

- `RaftPersistenceDriver` (`src/storage/persistence.rs:518`) is a **simulation**:
  an in-process `Mutex<RaftState>` mutated by operator commands. openraft 0.9.21
  is a dependency but only `Config::default()` is ever constructed, into a dead
  field. No log, no transport, no quorum, no apply loop. Join "tokens" are
  unvalidated formatted strings.
- **Every mutation flows through one seam**: `Store::write_connection()`
  (`src/storage/store.rs:457`) → a single `std::sync::Mutex<Connection>`.
  ~70 public mutating methods across ~10 domains; 32 use `conn.transaction()`.
- **The entire `Store` API is synchronous** — sync rusqlite called inline from
  async tonic handlers, no `spawn_blocking`. Any replication design that needs
  an `.await` inside a store mutation must bridge sync↔async explicitly.
- The `SchedulingReadModel` cache (nodes / allocations-by-node / task-configs)
  is updated *inline* by mutating methods; the event stream stages event rows
  *inside* the same write transaction and publishes on a broadcast channel.
  Replication must not let either escape before quorum commit.
- The allocation-epoch CAS (`apply_plan_submission`, `store.rs:837`) already
  rejects stale plans — the designed safety valve for replication-induced lag.
- Every one of the 30 tables has a **declared PRIMARY KEY** (verified against
  `src/storage/migrations.rs`) — a precondition for the design in §A2.
- Tests are **in-process** (no subprocess, no network): storage tests call
  `Store::start_storage`, service tests call `Manager` trait methods directly.
  The perf gate is `tests/integration/scheduler_benchmark_slo.rs` + `benchmarks/scheduler/slo.json`.

### Observability / operations (what's real vs. what docs claim)

| Area | Verdict |
| --- | --- |
| Manager/worker app logging (console + JSON file + rotation/retention) | **Implemented** (`src/tracing_util.rs`) |
| Task log capture, rotation, size caps, disk sweep | **Implemented** (`src/worker/logs.rs`, 10 MiB × 5 files × 7 days, 10s sweep) |
| OTLP trace export | **Implemented** (`src/otel.rs`; `docs/observability.md` wrongly says it isn't) |
| Event stream (journal + broadcast + List/Stream RPCs + CLI/MCP/UI) | **Implemented** (`src/storage/events.rs`) |
| **Metrics** (any endpoint, any counters) | **Missing entirely** |
| **Manager self-health probes** (`/healthz`, `/readyz`) | **Missing** (the `HealthCheck` RPC is worker→manager reporting, not a probe) |
| **Node drain/cordon** | **Missing** — a `{"drain": false}` attr is written at registration and never read |
| **Worker↔manager version handshake** | **Missing** — no version field anywhere in the wire protocol |
| **State backup/restore** | **Missing** as an operator command |

Also: node `availability` is the schedulability axis and
`Scheduler::available_nodes()` (`src/manager/scheduler.rs:812`) is the single
placement choke point that filters on `availability == "available"` — cordon
and drain get placement-exclusion for free by setting any other value.

### Best practices from comparable systems (applied throughout)

- **Command/effect replication over WAL shipping** (rqlite, and Legion's own
  `docs/persistence-plan.md` decision): replicate at a logical level; never ship
  raw WAL pages through consensus.
- **Row-effect replication beats statement replication for determinism**
  (MySQL row-based vs statement-based replication): replicating *effects*
  eliminates the entire non-determinism class (`now()`, `random()`,
  auto-increment) that statement/command replication must audit for, forever.
  SQLite's session extension exists precisely to capture row effects.
- **Leader-only writes with explicit redirect** (Nomad, Consul, etcd): follower
  receiving a write returns a typed error + leader hint; clients retry against
  the leader. Auto-forwarding is a UX refinement, not a correctness need.
- **Fresh-cluster requirement for a new consensus engine** (etcd v2→v3,
  Consul raft protocol bumps): when the log format is new, require a clean
  bootstrap seeded from a backup rather than in-place mixed-mode migration.
- **Drain as a first-class, two-verb operation** (K8s cordon/drain, Nomad node
  eligibility + drain): separate "stop placing" (cordon) from "also evict"
  (drain); reuse the existing rescheduling machinery for eviction.
- **Managers upgrade before workers** (Nomad/K8s version-skew policy): encode
  a protocol version, reject incompatible registration loudly, warn on skew.
- **Metrics as a pull-model Prometheus endpoint** (everything since 2015):
  the universally scrapeable baseline; OTLP metric push can layer on later.

---

## 2. Workstream A — Real Raft persistence

### A1. Goal and non-goals

**Goal:** with `persistence.type = "raft"`, every manager mutation is committed
by quorum before it is acknowledged or visible; a 3-manager cluster survives
the loss of any one node with zero committed-write loss; a crashed node
catches up via snapshot + log replay; membership/join/transfer are real
openraft operations; the operator surface (RPC/CLI/UI/history) that exists
today keeps working, now backed by truth.

**Non-goals for this execution** (deferred, tracked in §Follow-ups):
lane-partitioned topology as the *default* (see JC-2), the tuning/diagnostics/
autopilot phases (P9–P12) of the multi-raft plan, key-range sharding (P8),
auto-forwarding writes from followers, and cross-region topologies.

### A2. Architecture: leader-executes, changeset-replicated (the key decision)

The multi-raft plan (§2) specified typed `StoreOp` command replication with a
per-command determinism audit. The audit findings above change the calculus
(**JC-1**, the biggest judgment call in this plan — full rationale in §JC):

**Mechanism.** A store mutation in raft mode runs as:

1. The public store method (unchanged signature) packages its body as a
   closure and hands it to the **replication pipeline** for its lane.
2. On the pipeline's dedicated OS thread (per lane), with the write connection
   locked and a **SQLite session** attached: open `SAVEPOINT`, execute the
   closure (the *existing* SQL body, mechanically refactored to take
   `&mut Connection` instead of self-locking), **capture the changeset**
   (the exact row-level effects: inserts/updates/deletes with values,
   including leader-assigned rowids), then **`ROLLBACK`** the savepoint. The
   local DB is untouched; the closure's return value is retained.
3. The changeset bytes (plus lane + a monotonically-assigned op id) are
   proposed to that lane's `openraft::Raft` via `client_write` (the pipeline
   thread bridges sync→async with `Handle::block_on`, legal from a non-runtime
   thread).
4. On quorum commit, **every node's state machine** (leader included) applies
   the changeset inside a transaction, records the applied log id in the same
   transaction, updates the read-model cache from the changeset's contents,
   and publishes any staged events/notifies.
5. The original caller unblocks with the closure's return value only after
   local apply — read-your-writes holds.

Nothing is visible anywhere before quorum commit; every replica applies
byte-identical effects, so **replica divergence is impossible by
construction** rather than by per-method audit. The determinism property test
from the multi-raft plan (same log → identical DB content hash) is kept as an
end-to-end invariant check.

**Failure semantics:** closure error → nothing proposed, error returned.
Proposal fails (lost leadership, no quorum) → the savepoint was already rolled
back, nothing to undo; typed error + leader hint returned. This is strictly
better than any execute-then-undo scheme.

**Preconditions this rests on** (each enforced by a test):
- every replicated table declares a PRIMARY KEY (sessions silently skip
  PK-less tables) — `migrations` test walks `sqlite_master` and asserts it;
- `conn.transaction()` cannot be used inside the capture savepoint, so the 32
  transaction call-sites convert to savepoints (behavior-identical at top
  level; verified by the full existing suite);
- read-model/event/notify side effects are suppressed during capture and
  re-emitted at apply (capture-time suppression flag + changeset-driven
  apply-time projection).

**Store refactor shape** (mechanical, per mutating method):

```rust
// Public method: signature unchanged — callers never know.
pub fn stop_job(&self, name: &str) -> R<()> {
    let name = name.to_string();
    self.replicated(Lane::Control, move |store, conn| store.stop_job_apply(conn, &name))
}
// The old body, taking the connection instead of locking it.
pub(crate) fn stop_job_apply(&self, conn: &mut Connection, name: &str) -> R<()> { ... }
```

`replicated()` in `local_disk`/`streaming` mode locks the connection and runs
the closure directly — those backends are **byte-for-byte unchanged** in
behavior, which the entire existing test suite verifies at every gate.

### A3. Topology: engine is multi-group; default is `single` (JC-2)

The engine is built as the multi-raft plan specifies: a `MultiRaftDriver`
owning `HashMap<Lane, Raft>`, one gRPC `RaftService` multiplexing all groups
by `group_id`, per-lane log stores under `<data_dir>/<lane>/`. But the
**default topology for this release is `single`** (one group, all tables),
with `topology: "lanes"` implemented and tested as **experimental**:

- Per-lane snapshot/restore is only sound when table ownership is disjoint
  across lanes, and today the `node` table is written by both control-class
  writes (registration) and status-class writes (heartbeats). The multi-raft
  plan's own fix — the `node_health` table split — is real schema surgery that
  this execution defers rather than rushes.
- `single` topology gives the actual durability win (the point of this
  workstream) with whole-DB snapshots (`VACUUM INTO`) that are trivially
  sound.
- The lane machinery (groups, transport multiplexing, lane classification of
  every method, independent elections) ships and is integration-tested, so
  flipping the default later is a config-default change plus the node split —
  not an engine rewrite.

### A4. Cluster lifecycle

- **Bootstrap:** first manager starts with `raft.bootstrap = true`;
  `Raft::initialize` with itself as sole voter; if a pre-existing
  `store.sqlite` has data (upgrade from local_disk or from the old simulated
  raft), its contents become the seed state — the log starts above a synthetic
  snapshot of the existing DB. Real upgrade path, no data loss (JC-6).
- **Join:** `legion raft join-token` (existing surface) now returns a signed
  token carrying cluster id, leader advertise address, and a shared cluster
  secret. A new manager starts with `raft.joinToken`; it calls the leader's
  `RaftService.Join`, is added as **learner** to every configured group,
  catches up via snapshot install, and is promoted to voter. One token, all
  lanes — the ergonomics the multi-raft plan requires.
- **Peer addressing:** `raft.peers` entries become `"id=host:port"`
  (the manager gRPC advertise address; the `RaftService` shares the existing
  tonic listener). Bare `"id"` entries remain accepted where an address can be
  resolved from a join, so old configs parse.
- **Transport auth:** `RaftService` RPCs require the cluster secret (metadata
  header), minted at bootstrap and carried by join tokens. This is explicitly
  a stopgap consistent with the cluster's existing worker-plane posture and
  the tracked `TODO(worker-authn)`/mTLS gap (JC-7).
- **Operator commands:** `trigger-election`, `step-down`, `transfer-leader`,
  `add-voter`, `add-learner`, `remove-member` re-wire from the simulation to
  the real openraft APIs; `RaftHistoryEvent` audit entries now record real
  transitions. `raft status` reports real role/term/leader/indexes, plus
  per-lane detail when topology=lanes.
- **Non-leader writes:** mutating RPCs on a non-leader return
  `FAILED_PRECONDITION` with the leader id/address in the message and in
  error metadata (JC-4). The CLI surfaces the hint.

### A5. Snapshots, compaction, recovery, backup

- Snapshot build (single topology): `VACUUM INTO` a temp file at the applied
  index (taken on the pipeline thread so it can't interleave a capture);
  snapshot metadata = last applied log id + membership. Compaction: log purge
  up to the snapshot per `raft.snapshotEntries` (default 10_000).
- Install: replace the state DB wholesale, reopen connections, rebuild the
  read model, resume applies. Crash mid-install is safe (install writes to a
  temp path, atomic rename).
- Recovery: on restart, openraft replays vote/log from the per-lane log store;
  state machine resumes from its recorded applied index (stored in the DB it
  applied to, so state and index can never diverge).
- **`legion state backup <path>`** (works in every persistence mode; leader-
  only in raft mode) and **`--restore-from <path>`** on manager start: the
  operator-facing "loading state back in from a file" gap from TASK-LIST.

### A6. Phases and gates (Workstream A)

| Phase | Content | Targeted verification |
| --- | --- | --- |
| **A-0 capture layer** ✅ | rusqlite `session` feature; savepoint conversion of all tx sites; `replicated()` seam + `Lane` classification of every mutating method (exhaustiveness test); suppression hooks; `local_disk` behavior unchanged | `tests/integration/raft_changeset_capture.rs` (capture/rollback/apply, nested savepoints, rowid fidelity, PK-required edge), `tests/integration/raft_seam.rs`; full existing suite green |
| **A-1 engine** ✅ | `src/storage/raft/`: type config, sqlite log store, state machine (changeset apply + read-model/event projection), `RaftTransport` proto + network, `MultiRaftDriver`, single topology (lanes scaffolded, refused at start — JC-17) | `tests/integration/raft_apply_roundtrip.rs` (1- and 3-node in-process), determinism content-hash in `tests/integration/raft_durability.rs` |
| **A-2 lifecycle** ✅ | bootstrap-from-existing-DB, join-token → learner → voter, real operator commands, non-leader rejection + hint, config extensions, daemon wiring, leader-only scheduling loops | `tests/integration/raft_membership.rs`, `tests/integration/raft_upgrade_bootstrap.rs` (multigroup bring-up is deferred with lanes — JC-17) |
| **A-3 durability** ✅ | snapshots + compaction + install, crash/restart recovery, `state backup`/`--restore-from`, adversarial-review fixes (JC-18..22) | `tests/integration/raft_snapshot_recovery.rs` (compaction, restart, laggard snapshot catch-up, backup/restore), `tests/integration/raft_durability.rs` (leader loss, minority partition, any-single-node loss), `tests/integration/raft_cascade_delete.rs`, `tests/integration/raft_perf_smoke.rs` |
| **A-4 surfaces & docs** ✅ | CLI/UI real status (+ per-lane), capabilities/MCP catalog, `docs/multi-raft.md`, updates to `raft-operations.md`, `state-store.md`, `persistence-plan.md`, `upgrades.md`, `README.md`, TASK-LIST | capability-catalog test; docs review pass |

Universal gate after every phase: `cargo fmt --all -- --check`,
`cargo clippy --all -- -D warnings`, `cargo build --all`,
`cargo test --all --locked`, plus `cargo test --test integration scheduler_benchmark_slo::
--locked -- --ignored` at A-3/A-4 (SLO must not regress; it runs on
`local_disk`, which A-0 keeps byte-identical).

---

## 3. Workstream B — Operational table stakes

### B1. Metrics + manager health probes

- New optional config block `manager.telemetry { metricsEnabled, port }`
  (default enabled on `server.port + 2`), following the `ExecConfig`
  optional-section pattern; a dedicated hyper listener (the `exec_ws` pattern —
  no new web framework) serving:
  - `GET /metrics` — Prometheus text exposition;
  - `GET /healthz` — 200 once serving (liveness);
  - `GET /readyz` — 200 only when the store answers a probe query and, in
    raft mode, the local node knows a leader (readiness).
- Metrics registry: a small internal module (counters/gauges/histograms on
  atomics, fixed buckets) — zero new dependencies (JC-8). Initial series:
  - `legion_grpc_requests_total{method,code}`, `legion_grpc_request_duration_seconds{method}` (tower layer on the tonic server);
  - `legion_evals_processed_total{kind,status}`, `legion_eval_queue_depth`;
  - `legion_plan_apply_total{outcome}` (applied / epoch-rejected);
  - `legion_store_write_lock_wait_seconds` (the single-writer health signal);
  - `legion_heartbeats_total`, `legion_nodes{status}`, `legion_allocations`;
  - `legion_events_emitted_total{kind}`;
  - raft mode: `legion_raft_is_leader{lane}`, `legion_raft_term{lane}`,
    `legion_raft_last_log_index{lane}`, `legion_raft_applied_index{lane}`,
    `legion_raft_apply_lag{lane}` — the fields named so the multi-raft plan's
    later diagnostics phase (P10) can build on them unchanged.

### B2. Node cordon / drain

- Two verbs, Nomad/K8s-style: **cordon** = stop placing (set
  `availability = "drained"`; `available_nodes()` already excludes it),
  **drain** = cordon + evict-and-reschedule via a new `node_drain` eval that
  reuses the lost-node reschedule body without the loss-confirmation gate.
  **uncordon** restores `availability = "available"` and enqueues the
  existing new-node reschedule pass.
- Stateful allocations keep their existing `onLost`-style protection: drain
  evicts what is replaceable and reports what it is waiting on (JC-9).
- Surfaces: `Manager.SetNodeAvailability` RPC (ACL `Node/Write`,
  idempotency key, event emission `node.cordoned` / `node.drained` /
  `node.uncordoned`), CLI `legion node cordon|drain|uncordon <id>` (+`--json`),
  capabilities catalog + MCP tool entries, proto docs.

### B3. Worker↔manager version handshake

- `WorkerRegisterRequest` gains `build_version` + `protocol_version`;
  the register reply returns the manager's. Protocol version starts at `1`
  (const). Manager **rejects** registration from an incompatible protocol
  version with an actionable error; **warns** (log + node attr) on
  build-version skew. Worker logs the manager version at join.
- Version surfaces in the node ledger (`dynamic_attrs.buildVersion`) so
  `legion node list`/UI show fleet skew at a glance.
- `docs/upgrades.md`: upgrade order (managers first, then workers), skew
  policy, and the raft fresh-bootstrap note from A4.

### B4. Documentation corrections

- Rewrite `docs/observability.md` to match reality (OTLP is shipped; metrics
  now exist; link logging.md as the task-log authority).
- Update `docs/TASK-LIST.md`: check off what this work completes; correct the
  stale event-stream/logging entries found by the audit.
- Create `docs/NEXT_UP.md` (referenced by AGENTS.md but missing) pointing at
  this plan's follow-ups.

### B5. Phases and gates (Workstream B)

| Phase | Content | Targeted verification |
| --- | --- | --- |
| **B-0 drain** ✅ | store method + eval + RPC + CLI + capabilities + events | `tests/integration/node_drain.rs` (cordon excludes from placement; drain evicts + reschedules elsewhere; uncordon restores; idempotency; ACL entry) |
| **B-1 version handshake** ✅ | proto fields, register/reply, reject/warn logic, ledger surfacing, `docs/upgrades.md` | unit tests for compat matrix; register round-trip test |
| **B-2 metrics + probes** ✅ | telemetry config, listener, registry, instrumentation points, `/healthz` `/readyz` (`legion_raft_*` series deferred — JC-25) | `tests/integration/metrics_endpoint.rs` (scrape parses; counters move; readyz flips with store health) |
| **B-3 docs** ✅ | observability.md rewrite, TASK-LIST refresh, NEXT_UP.md | link/consistency pass |

Same universal gate after each phase. B runs **before** A's store surgery to
keep `server.rs`/proto merge conflicts one-directional (B's edits land first;
A's larger refactor rebases on top mechanically).

---

## 4. Testing plan (net-new coverage)

- **Mechanism tests** (landed with this plan): changeset capture/rollback/
  apply, nested savepoints, leader-rowid fidelity, PK-required edge.
- **Schema guard:** every table in `sqlite_master` declares a PK.
- **Lane classification:** every public mutating store method is classified
  (exhaustive match) and, in lanes topology, table ownership is disjoint.
- **Raft integration** (in-process, multi-node in one process — the harness
  style the repo already uses): apply round-trip (1/3 nodes), determinism
  content-hash across replicas under randomized op interleaving, membership
  grow/shrink/transfer, non-leader write rejection with hint, snapshot
  round-trip + crash recovery + laggard catch-up, cross-lane multigroup
  bring-up with independent elections.
- **Drain/version/metrics** integration tests as listed per phase.
- **Regression:** the full existing suite (`local_disk` byte-identical) at
  every gate; `scheduler_benchmark_slo` unchanged and green.
- **Adversarial pass:** before final merge, a skeptic review agent attempts to
  break each headline claim (no divergence, no pre-commit visibility, no
  mutation bypassing replication, drain never strands a replaceable alloc)
  against the code, and its findings must be resolved.

---

## 5. Execution model

Executed as Claude dynamic workflows, one per phase group, matching this
plan's phase tables: implementation agents fan out per work item (files kept
disjoint per agent), each phase ends in the universal gate run by a barrier
step, and failed gates loop the phase instead of advancing. The store-surgery
phase (A-0) is deliberately sequential (single 6.5k-line file) with
compile-checked slices. Progress is journaled by ticking the phase tables
here and in TASK-LIST.

---

## 6. Follow-ups (explicitly deferred, in priority order)

1. `node` liveness split (`node_health` table) → flip `topology=lanes` to
   default → per-lane snapshots (multi-raft plan P2's table split + P4).
2. Lane-isolation benchmark + tuning profiles + diagnostics + UI verdict
   layer (multi-raft plan P7, P9–P11).
3. Worker mTLS / per-node credentials (existing `TODO(worker-authn)`), which
   also upgrades the raft transport auth beyond the shared secret.
4. Auto-forward writes to the lane leader — **shipped for the worker plane
   and internal lane halves** (plan §8 D2, 2026-07-28); client-plane RPCs
   keep reject + hint deliberately (§8 D3).
5. OTLP metrics push + worker-side `/metrics`.
6. Raft autopilot (multi-raft plan P12) — speculative, default-off.

---

## 7. Workstream C — Unblocking `topology: lanes` + voter-count checks (2026-07-24)

JC-17 refused `lanes` at engine start because it was structurally unsound.
This workstream makes it sound and ships it, plus ergonomic raft voter-count
(quorum) checks. Executed on the same branch/PR.

### C1. Why lanes was actually blocked (sharper than the JC-2 statement)

With row-changeset replication, lane independence fails in two distinct ways:

1. **Same-row cross-lane writes.** Control writes `node.availability`
   (cordon) while status writes `node.last_heartbeat` — the same row from
   two independent Raft logs. Lanes commit and apply in an order that can
   differ per node; a changeset `UPDATE` carries old-value preconditions, so
   out-of-order apply is either a loud conflict (ABORT policy: availability
   loss) or, with REPLACE, silent divergence. **Correctness requires every
   row to have exactly one writing lane.**
2. **Per-lane snapshots require per-lane reconstructibility.** A lane's
   snapshot + its own log must reproduce its tables. Any table written by
   two lanes breaks that (and the earlier joint-image design it forces has
   an unfixable regress-on-install hazard between independently-progressing
   lanes).

### C2. The design that unblocks it

**Single-writer-lane-per-table, enforced at capture time.**

- **Ownership map** (`src/storage/raft/ownership.rs`): every replicated
  table → exactly one lane, plus a small **commutative class**: insert-only
  tables with globally-unique PKs (`scheduler_event`, `idempotency_record`)
  that any lane may INSERT into, applied with per-table `OMIT`
  conflict policy (insert-if-absent is order-independent), pruned locally
  outside raft, and excluded from the content hash.
- **Capture-time guard:** after capture, the changeset is iterated and every
  touched table must be owned by the op's lane (or be a commutative INSERT).
  A violation fails that op before proposal — misclassification is
  impossible to ship, in every topology (single runs the same check so bugs
  surface before anyone flips the flag).
- **`node` / `node_health` split** (the multi-raft plan's own fix, now
  load-bearing): liveness (`status`, `last_heartbeat`, `lost_at`,
  `health_checks`) moves to `node_health` (status lane); `node` keeps
  identity/pool/attrs/`availability` (control lane). Readers join via the
  read model / hydration.
- **Eval queue goes leader-local (reverses JC-15).** Evals are regenerable
  work items (the leadership-acquisition hook + reconcile loops already
  regenerate them after failover — built in the hardening phase). Taking
  them out of raft removes the biggest class of cross-lane writes: any op on
  any lane may enqueue work without contaminating its changeset. A scoped
  `local_write` seam (like capture/apply guards) admits exempt tables
  through the bypass guard, and a debug assertion pins that local writes
  touch only exempt tables.
- **Composite ops split at lane boundaries via evals** (the Nomad shape:
  deregister → eval → scheduler acts): control ops stop cascading into
  scheduler tables inline (e.g. destroy/stop enqueue the existing
  `job_stop`-style evals; the scheduler-lane eval handler does the
  allocation writes). Status ops (node lost) mark liveness then enqueue the
  reschedule eval. Rollout mutations consolidate onto a single lane.
  Cross-lane atomicity was never promised (§A2/plan §5); the epoch CAS +
  read model absorb the lag, now genuinely exercised by tests.
- **Per-lane snapshots become trivially sound:** snapshot = logical dump of
  the lane's owned tables (+ the commutative tables ride the control lane's
  snapshot; other lanes' commutative inserts replay with OMIT); install =
  replace owned tables only, inside one transaction with the applied
  marker. No joint images, no cross-lane regress hazard.
- **Default:** if and only if the full lanes test battery and a fresh
  adversarial pass come back clean, `topology` defaults to `lanes` per the
  original multi-raft plan, with `single` kept as the documented escape
  hatch. Otherwise `single` stays default and this section says why.

### C3. Voter-count (quorum) checks — ergonomic shape

Plain-language, surfaced where operators already look, hard-blocking only
where safety demands:

- `raft.expectedVoters` (optional) in config; validation errors on `< 1`,
  warns on even values with the "2 voters = quorum 2 = survives nothing;
  use 1, 3, or 5" explanation.
- `legion raft status` gains a quorum line per lane: voters, quorum size,
  failures survivable, and a ⚠ with the exact suggestion when the count is
  even or below `expectedVoters`.
- Membership changes (`add-voter`, `remove-member`, join promotion) warn
  loudly when the resulting voter count is even; only genuinely unsafe
  operations are refused (removing the last voter / breaking quorum
  feasibility). Promotion is never silently withheld — ergonomics keep the
  one-token join flow intact.
- `/metrics` gains `legion_raft_voters{lane}`; `readyz` mentions quorum
  degradation reasons; events record membership-parity warnings.

### C4. Phases

| Phase | Content | Targeted verification |
| --- | --- | --- |
| **C-0 foundations** ✅ | ownership map + capture-time guard + commutative class (per-table OMIT) + local pruning; eval-queue local-write seam + failover regeneration test | ownership guard unit tests; eval failover test |
| **C-1 node split** ✅ | `node_health` migration + store/read-model/hydration/reader updates (JC-30/31) | full suite (readers behave identically); node lifecycle tests green; `node_health_row_lifecycle_across_the_split` |
| **C-2 composite split** ✅ | control/status ops stop writing foreign lanes inline (wrapper composites + eval-mediated reschedule tails, JC-32..35); cross-lane FK removal migration; rollout lane consolidation (rollout/rollout_event → scheduler); `OWNERSHIP_GUARD_ENFORCES` flipped to true (guard errors in every topology) | `tests/integration/raft_ownership_guard.rs` (representative ops from every store domain through a raft-mode store, enforcement on); full suite green |
| **C-3 lanes engine** ✅ | per-lane table-scoped snapshots (JC-37); start-time refusal removed — lanes brings up three groups with per-lane snapshot policy/compaction; apply-side index-skip guard added (JC-38); lane-named leader hints + per-lane status summary (JC-39); daemon leader-gating per lane (JC-40); lanes config example | `tests/integration/raft_lanes.rs` battery (bring-up/per-lane writes, independent elections, lane-scoped snapshot catch-up, cross-lane race convergence); lane snapshot unit tests in `state_machine.rs`/`ownership.rs` |
| **C-4 voter checks** | config knob, status quorum lines, membership warnings/guards, metrics, docs | voter-parity unit + CLI tests |
| **C-5 verify + ship** | skeptic pass on lanes claims → harden → docs (multi-raft.md lanes section, default decision) → full gate | gate + skeptic verdicts resolved |

---

## 8. Workstream D — Standing-split resilience (2026-07-28) — EXECUTED

> Status: EXECUTED the same day, alongside the fixes from the four-sweep
> adversarial verification pass (governor catch-up cap and flap ladder,
> readyz divergence timer, joint-intersection status voters, remove-member
> hardening, seed-snapshot purge gate, orphan-sweep grace, plan-fence epoch
> mirror + apply-time availability check). Judgement calls for all of it:
> [calls doc](../calls/durability-and-ops-execution-plan_calls.md).
> Deferred: apply-time resource-total re-validation, idempotency-record
> atomicity, destroy_job split ordering, eval retry backoff — all in
> `TODO.md` with context.

JC-40's correction made lane co-location **best effort**: a standing split
(different managers leading different lanes for minutes — convergence
stand-downs reach 480 election windows) is a supported steady state. The
documented cost was "cross-lane composites have no home until convergence".
The verification pass behind this workstream priced the bill precisely, and
it is worse than documented, in order:

1. **Worker liveness dies, then takes workloads with it.** Heartbeats are
   status-lane writes (`update_node_heartbeat`); workers dial ONE manager
   forever (`worker/daemon.rs::start_heartbeats` — retry loop, no redial,
   no leader-hint handling). Under a split that puts the status lane away
   from the manager the workers dial, every heartbeat is rejected: the
   status-lane leader (elsewhere) watches replicated `last_heartbeat` go
   stale and marks every affected node lost → eviction/reschedule of
   healthy workloads; meanwhile each worker's self-fence watchdog kills its
   stateful allocations locally. A control-plane leadership shape becomes a
   data-plane outage.
2. **Job submit has no home** (the documented gap): the control leader
   rejects the status/scheduler halves, the scheduler leader rejects the
   control half; `/readyz` 503s and the operator remedy is "wait or
   restart a manager".
3. **An `Updated` submit whose rollout creation was rejected is
   unrecoverable by retry.** The retry's `handle_job_submit` returns
   `Unchanged`, and no path ever creates the missing rollout;
   `regenerate_scheduling_evals` then enqueues its `job_submit` fallback,
   which places the new config at full count — **bypassing canary/staged
   gating**. (Also reachable in `single` topology by crashing between the
   submit and the rollout insert, with no manager restart afterwards.)

### D1. Derived-write reconcilers (tolerate + reconcile)

For composite halves that are **pure, idempotent projections of
already-replicated control state**, rejection stops being an error. The op's
one non-derivable half (the control write carrying user/operator intent) is
the operation; the cross-lane halves are projections that any correct node
can re-derive later — exactly the `regenerate_scheduling_evals` shape JC-41
already established.

- `handle_job_submit` tolerates a typed `NotRaftLeader` on its status-seed
  and volume-claim-sync halves (warn + succeed). Everything else still
  fails loudly.
- `run_job`'s scheduler-lane tail (`seed_keyed_instances`,
  `create_rollout_for_update`, `enqueue_eval`) tolerates the typed
  rejection the same way; the reply message says scheduling is deferred
  until lane leadership converges.
- Reconcilers complete the halves on the owning lane's leader, each a
  cheap missing-rows query, run from the node-status-checker loop:
  - `seed_missing_task_statuses` (status lane): `task_config ⟕ task_status`
    → seed `pending` rows.
  - `sync_missing_task_volume_claims` (scheduler lane): latest config per
    active job with volume-declaring tasks but no claim rows → run the
    existing idempotent sync.
  - `reconcile_missing_rollouts` (scheduler lane, manager layer — it needs
    `build_rollout_launch_config`): active job whose latest config has a
    predecessor, no rollout targeting it, no allocations on it, and live
    allocations on an older config → the acknowledged-update signature →
    `create_rollout_for_update` + let eval regeneration arm the tick.
    Runs **immediately before every `regenerate_scheduling_evals` call**
    (leadership acquisition + divergent-leadership branch) so the
    `job_submit` fallback can never again bypass a rollout the job
    deserved, and periodically on the checker cadence so the
    crash-without-restart case heals without a client retry.

### D2. Lane-write forwarding for worker-plane liveness

A heartbeat is **new information** — nothing can re-derive it — and its
loss is finding #1. So the worker plane gets the plan's follow-up #4
(auto-forwarding), scoped to where it is load-bearing:

- New `RaftTransport` RPC `ForwardLaneWrite`: a serde op enum
  (`ForwardableLaneWrite`) + lane wire name, authenticated by the cluster
  secret like every transport RPC. The receiving manager executes the named
  store op locally; if it does not lead the lane either, the typed
  `NotRaftLeader` (with ITS hint) rides back in the reply payload.
- The driver resolves the lane leader's address from its own metrics
  (leader hint) and reuses the shared `ChannelPool`.
- `Store::update_node_heartbeat` / `update_node_health_checks` /
  worker-report `task_status`+`allocation_health` upserts fall back to
  forwarding on a local `NotRaftLeader`. Worker RPC surface unchanged;
  workers keep dialing one manager.
- Forwarded ops carry their **full arguments** — the executor never
  derives payload from its possibly-lagging replica — and stay
  EXISTS-guarded no-ops when the target's control replica has not seen the
  node yet (the next heartbeat repairs it, same as the pre-split contract).
- Registration: the control half must land on the manager the worker
  dials (reject + hint, unchanged); its status half (`mark_node_ready`)
  forwards like a heartbeat.

### D3. Non-goals

- No general client-plane auto-forwarding (follow-up #4 for client RPCs
  stands: reject + leader hint remains the contract).
- No leader transfer (openraft 0.9 has none); convergence stays JC-40's
  governor.
- No relaxation of single-writer table ownership; forwarding proposes on
  the OWNING lane's leader, it never writes cross-lane.
- `/readyz` keeps failing on a standing split (it is an abnormal shape an
  operator should investigate), but its body stops claiming composites
  "cannot complete on any node" — after D1/D2 the write path degrades
  (reconcile-latency scheduling, forwarded liveness) instead of dying.

### D4. Verification

- `tests/integration/raft_lanes.rs::lanes_that_settle_on_different_leaders_stay_correct`
  updated to the new contract: submit **succeeds** on the control leader
  under a standing scheduler split, reconcilers complete the halves on the
  lane leaders, and the healed cluster converges byte-identically.
- New: standing status-lane split with a live heartbeat path — the node is
  NOT marked lost while heartbeats forward, and the rollout reconciler
  creates a gated rollout for an update submitted into the split.
- Reconciler unit tests beside the store methods; forwarding round-trip
  unit test through two in-process transports.

---

## JC. Judgment-call log

**JC-1 — Changeset (row-effect) replication instead of typed-command
replication.** The multi-raft plan specified a typed `StoreOp` enum with
proposer-stamped ids/timestamps and a determinism audit of every method. The
audit found ~70 sync mutating methods whose bodies freely use timestamps,
autoincrement rowids, and internal composition — auditing and freezing all of
that forever is the highest-risk part of the original plan, and it must be
re-done for every future store method. SQLite's session extension (available:
rusqlite `bundled` + `session`) captures the *effects* instead, making
determinism structural. Every replicated payload is exactly what the leader's
SQL did, byte-for-byte, including rowids. Costs accepted: changeset payloads
are somewhat larger than typed commands; replicated "commands" are opaque row
effects rather than semantic ops (mitigated: the op name travels in metadata
for observability); tables must declare PKs (true today, enforced by test).
The persistence-plan's core decision — "replicate deterministic store
operations, not WAL bytes" — is honored in spirit: this is logical row-change
replication, not WAL shipping.

**JC-2 — `topology = single` is the v1 default; lanes ship experimental.**
The multi-raft plan wants lanes default-on. Per-lane snapshot soundness
requires disjoint table ownership, which requires the `node`/`node_health`
split — real schema surgery this execution defers. Durability (the actual
goal of item 1) is delivered by a correct single group; the multi-group
engine, transport, and lane classification all ship and are tested, so the
later default flip is small. Correctness beat headline.

**JC-3 — Replication pipeline threads are dedicated OS threads, not tokio
tasks.** Store mutations are sync and called from async handlers; a dedicated
thread per lane owns capture (sync SQLite) and bridges to async
`client_write` via `Handle::block_on` — legal off-runtime, and it avoids
every `block_in_place`/current-thread-runtime hazard. Callers block on a
channel, which is exactly as blocking as today's inline SQLite writes.

**JC-4 — Non-leader writes are rejected with a leader hint, not forwarded.**
Matches `persistence-plan.md`'s redirect model, avoids double-serialization
of closures (which cannot cross the wire), and is what Nomad/etcd shipped
first. Auto-forward is follow-up #4.

**JC-5 — The 32 `conn.transaction()` sites become savepoints.** `BEGIN`
fails inside the capture savepoint; SQLite savepoints at top level behave as
deferred transactions, so `local_disk` semantics are unchanged (whole suite
verifies). One mechanical pattern everywhere beats a per-site special case.

**JC-6 — Upgrading to real raft seeds from the existing DB at bootstrap;
mixed-mode clusters (simulated raft + real raft) are not supported.** The old
"raft" was leader-election theater with local SQLite — there is no replicated
state to migrate, only the local DB, which bootstrap ingests. This follows
the etcd/Consul precedent for consensus-engine changes and is documented in
`docs/upgrades.md`.

**JC-7 — Raft transport auth is a shared cluster secret for now.** The
cluster's worker plane already has a tracked authn gap (`TODO(worker-authn)`)
and no mTLS; inventing a bespoke manager-plane PKI ahead of the planned
worker-mTLS work would be throwaway. The secret is minted at bootstrap,
travels only in join tokens, and the upgrade path to mTLS is follow-up #3.

**JC-8 — Metrics registry is hand-rolled (no new dependency).** The repo
deliberately serves OIDC and WebSockets with bare hyper/tower instead of a
framework; a ~200-line atomic counter/gauge/histogram module in that spirit
avoids a dependency tree for what v1 needs. If cardinality/features grow,
swapping the storage for the `prometheus` crate behind the same module API
is contained.

**JC-9 — Drain respects existing stateful-workload protection windows.**
Drain reuses the loss-reschedule machinery minus the confirmation delay for
stateless allocations, but keeps the `onLost`-style gates for stateful
instances — a drain must not force-move state the operator configured to be
move-protected. Drain status reports what it is still waiting on instead of
silently hanging.

**JC-10 — B lands before A.** Both touch `proto/legion.proto` and
`server.rs`; B is small and A's store surgery is large, so B first makes the
conflict surface one-directional and gets item 3 verified early.

**JC-12 — Capture uses a thread-local context, not connection threading.**
Store method bodies internally call `self.write_connection()` and
`self.events()`; threading a `&mut Connection` parameter through ~70 bodies
(and their helpers) would be invasive and would still deadlock on the
recorder's own locking. Instead: a global **capture mutex** serializes
captures; the pipeline thread sets a thread-local capture context; the
session is created against the connection for the capture's duration
(one documented `unsafe` lifetime extension, sound because the capture mutex
plus the raft-mode write funnel guarantee exclusive write access and the
connection outlives the session); method bodies run unmodified, locking and
unlocking the connection as they always did, with their savepoints nesting
inside the capture savepoint. Per-method conversion is therefore mechanical:
rename the body to `*_apply`, add a wrapper that routes through
`replicated(lane, ...)`.

**JC-13 — Nested public-method calls compose into one changeset.** Several
store methods call other public mutating methods (e.g. `mark_nodes_as_lost`
→ reschedule). `replicated()` detects an active capture on the current
thread and runs the closure inline, so a composite operation produces one
atomic raft entry — no double-proposal, no partial replication.

**JC-14 — Bypass protection is a runtime guard, not a grep.** In raft mode,
`write_connection()` outside an active capture (or the apply path) returns a
loud error. Any code path that tries to write without replicating fails
immediately and visibly — including future code that hasn't been classified.
Startup-time writes (migrations, requeue of in-flight evals, manager-config
upsert) are sequenced around engine start and documented; tests'
`reset_all_tables` runs pre-engine and is exempt.

**JC-15 — Evals replicate like everything else.** Eval queue operations are
writes; exempting them (Nomad-broker-style leader-local queues) would
complicate the "no mutation bypasses" invariant for a performance
optimization Legion does not yet need. In raft mode the eval *processing*
loop runs only on the leader (followers' loops idle until leadership),
while eval *state* replicates so a failover resumes the queue.

### JC-40..43 — Lanes v2: what the adversarial pass forced

Three skeptic agents broke all three lanes claims. The findings were not
scattered bugs; they traced to three root causes, each with one fix.

**JC-40 — Lane leadership is CO-LOCATED, not spread.** The multi-raft
literature spreads leadership across nodes for load balance. That is
unsound here: Legion has composite operations that write two lanes
(registration = control identity + status liveness; job submit = control
tables + scheduler eval), and under divergent per-lane leadership each
half-fails on the node that does not lead the other lane. Joins, `/readyz`,
remove-member feasibility, and startup writes break the same way.
**Decision:** the control lane is the anchor; whichever node leads it
campaigns to acquire the other lanes, so one node leads all three in the
common case. *(See the 2026-07-28 correction below: this is BEST EFFORT, not
an invariant — a campaign is a race raft may refuse forever, and convergence
now stands aside entirely rather than disrupt an election.)*
*This keeps the entire multi-raft win* — the isolation benefit comes
from **independent logs, apply loops, and replication pipelines per write
class** (a control-write burst cannot stall the scheduler's log), not from
where leaders sit. Leader spreading was always the secondary benefit; it is
the one that is incompatible with a shared SQLite state machine, so it goes.
Divergent leadership remains *safe* — it is converged away where it can be,
and composite ops retry against the anchor. Divergence that does NOT converge
is a supported steady state whose cost is documented in
`docs/multi-raft.md`.

*As built:* `driver::ANCHOR_LANE` is `Lane::Control`, and
`driver::lane_convergence_loop` is an ENGINE lifecycle task — one per node,
spawned by `start_engine` only when the topology runs more than one group,
so every store that runs lanes converges (embedded uses and the raft test
batteries included, not just processes that start the manager daemon).

**CORRECTION (2026-07-28): co-location is BEST EFFORT, not an invariant.**
The paragraph this replaces said the anchor leader "campaigns … until one
node leads all three", which reads as a state the cluster always reaches. It
is not one, and it cannot be: openraft 0.9 has no leader-transfer API, so the
only lever is `Raft::trigger().elect()`, and a campaign is a *race* raft's
election safety may refuse forever. Worse, the original loop's campaigns were
actively harmful in the one case that matters. Evidence, from a 2-core runner
reproduction with openraft tracing on
(`killing_the_all_lane_leader_reconverges_onto_one_survivor`): the status lane
sat leaderless for the full 210s timeout at term 132, applied index frozen at
5, with the two survivors' logs IDENTICAL — the campaigns were not losing on
log currency, they were restarting the node's own candidacy faster than the
election could finish and dragging the other survivor's term along.

Three facts from the openraft 0.9 source drove the redesign:

1. `RaftCore::handle_tick_election` already campaigns on **every** non-leader
   voter, candidates included, once its randomized timer expires. A
   leaderless lane therefore needs no help from convergence — this node is
   already campaigning for it.
2. That tick applies raft's real policy: it defers behind a committed leader
   lease, and adds `smaller_log_timeout` (2x `election_timeout_max`) whenever
   a vote response revealed a longer log elsewhere (`Engine::set_greater_log`).
3. `ExternalCommand::Elect` — what `trigger().elect()` sends — calls
   `Engine::elect()` **directly**, bypassing all of it. No log check, no lease
   check, no deference to an election in flight.

So the loop was strictly more aggressive than any natural election, on the
exact lane that could least afford it. What ships instead is a per-lane
governor (`LaneConvergeGovernor`, a pure state machine over sampled metrics
so the properties are unit-testable against a fake clock):

- **Never campaign for a leaderless lane.** This is the invariant that makes
  convergence unable to break a lane: an unreachable lane is a dead write
  path, which always outranks co-location. Only a lane with a settled leader
  ELSEWHERE is campaigned for, where a campaign is an optimization and losing
  it costs nothing the cluster did not already have.
- **Bounded disruption.** Per-lane exponential backoff (2 → 16 election
  windows), a verdict window before the next attempt so a campaign is never
  re-issued before the previous one is decided, a stand-down after three
  consecutive losses for 60 election windows, and that stand-down doubling up
  to 480 windows for a lane this node keeps failing to take. Any win resets
  everything. Every duration is a multiple of `raft.electionTimeoutMaxMs`, so
  raising the documented remedy for loaded/WAN/CI clusters stretches the
  whole schedule with it.
- **Skip a campaign that cannot win.** A lane whose log is still streaming
  into this node means this node is behind whoever it replicates from; hold.
  That is the only log-currency signal openraft 0.9 gives a non-leader
  (`RaftMetrics` carries this node's own `last_log_index`; the replication
  match view exists only on that group's leader). The authoritative test is
  the election itself, which the backoff scores.
- **Campaigns are counted** per lane and exposed as `Store::raft_lane_campaigns`
  so the battery asserts the bound directly instead of inferring it from raft
  terms.

*Root cause of the reproduction, which was NOT the convergence loop:* the
harness's `start_cluster` declared bring-up complete off
`raft_status().voters`, which read the **anchor lane's** voter list, and then
killed the leader — while the third node's join was still promoting it on the
status lane. openraft applies a membership change as joint-then-uniform and
`voter_ids()` reports the incoming member from the joint step onwards, so both
the readiness gate and the join's own `wait_until_voter_on_every_lane` check
passed on a lane still in `[{a,b},{a,b,c}]`. That joint config needs a majority
of **each** config, i.e. both `a` and `b`; killing `a` cost the status lane its
write path permanently, and the 132 terms were two survivors and a convergence
loop failing at an election that was arithmetically impossible. Three fixes:
`lane_membership_settled` requires a UNIFORM membership, `wait_until_voter_on_every_lane`
uses it, `raft_status().voters` now means "settled voter on EVERY lane", and a
lane in a joint config reports the strictest config's `quorum`/`survives` plus
a plain-language advisory instead of the union's flattering numbers.

The loop still deliberately does NOT force a remote lane leader to step down:
openraft 0.9 has no leader-transfer API, so "forcing" would mean a side channel
telling the remote leader to abdicate — two nodes acting on one lane, which
under a stale anchor view degenerates into ping-pong, for zero gain (the
campaign that follows the abdication is the same campaign we already issue).

Composite resilience while lanes diverge — unchanged, and now the load-bearing
half of the design rather than a transient nicety: every wrapper-sequential
composite's second half is rejected BEFORE its capture with the typed
`NotRaftLeader` leader hint (`execute_replicated`'s pre-capture leadership
check), so a composite never half-applies silently, and every first half is
idempotent on retry — `insert_node` (upsert + `INSERT OR REPLACE`),
mark-ready/lost (upserts with `COALESCE`d `lost_at`) + `sync_node_availability`
(UPDATE by id), `destroy_job` (three DELETE passes), `remove_node`
(`remove_allocs_for_node` then DELETE). `handle_job_submit` runs its status
seed and its scheduler-lane volume-claim sync for EVERY outcome, `Unchanged`
included, so a client retry after a rejected half can still complete the
composite. The honest cost, now documented publicly: a composite that spans a
split has no home on ANY node until leadership converges — a submit fails on
the control leader (scheduler half) and on the scheduler leader (control half)
— and the RPC-level submit tail's eval enqueue is recovered by
`regenerate_scheduling_evals` on the scheduler lane's leader (JC-41's safety
net, exercised by `tests/integration/raft_lanes.rs::lanes_that_settle_on_different_leaders_stay_correct`).
`/readyz` (`telemetry::raft_readiness_failure`) fails while per-lane leaders
DISAGREE for longer than a 10s grace measured from `election_epoch_ms` — brief
post-election convergence stays 200, a standing split does not, and the body
carries a `lane <name>: leader <node> (this node: <role>)` line per lane on
both paths. The Join handler requires anchor-lane leadership up front and
rejects otherwise with the control leader's hint, then retries each lane's
learner-add/promotion inside a 20s convergence window and verifies the joiner
is a SETTLED voter on every lane before reporting success. `remove-member`'s
quorum-feasibility guard evaluates EVERY lane and refuses outright if the
removing node does not lead every lane.

Proofs: `tests/integration/raft_lanes.rs` (e1) convergence after a deliberate split, where
the anchor demonstrably CAN win, plus a bound on the campaigns it took; (e2)
after the all-lane leader is killed, every lane elects a surviving leader
(required) and co-location is accepted either way (best effort) with the
cross-lane composites asserted to complete without half-applying and the
campaign count asserted bounded; (e3) composites driven through a leadership
change with client-style retry with both halves asserted present; (e4) a
fourth node joining mid-convergence becoming a voter on all three lanes;
(e5) `lanes_that_settle_on_different_leaders_stay_correct` holds a permanent
split and pins the typed rejections from both sides, the intact halves, the
eval safety net, and the healed retry. The leadership-INDEPENDENCE test
suspends convergence through the `Store::raft_set_lane_convergence` test hook,
since divergence is exactly what it needs to observe. The governor's own
properties (never campaign into an election; bounded, escalating stand-downs;
the catching-up hold) are unit tests in `driver.rs` against a fake clock.

**JC-41 — Evals go back into Raft (reverses JC-27).** JC-27 made the eval
queue leader-local, reasoning that evals are regenerable. The skeptics
proved they are not: an acknowledged `job stop`, a mid-flight rollout tick,
a config-update's previous-version cleanup, and a KV-triggered restart each
ride an eval whose loss is permanent and user-visible — and the
regeneration hook cannot reconstruct operator intent it never recorded (it
even re-evicts nodes that were only cordoned). This is a durability
regression in *single* topology too, not just lanes. **Decision:** evals are
replicated again, owned by the **scheduler** lane, written as their own
sequential op so no cross-lane changeset contamination returns. Combined
with JC-40 the enqueue always happens on the node that leads both lanes.
`regenerate_scheduling_evals` stays as a belt-and-braces reconcile, no
longer load-bearing for correctness.

*As built:* `eval` is `Owner::Lane(Lane::Scheduler)` in the ownership map,
so it rides the scheduler lane's snapshot, applies from changesets like any
other table, and is back in `Store::state_content_hash` — replicas that
disagree about pending work now fail the convergence fingerprint. Every
eval method (`insert_eval`, `set_eval_status`, `claim_eval`,
`requeue_in_flight_evals`, `prune_stale_evals`, `complete_eval`,
`complete_eval_with_event`, `release_eval_for_retry`,
`enqueue_eval_after_delay`) is a `replicated(Lane::Scheduler, …)` op over
its existing `_apply` body; `enqueue_eval` remains a zero-delay convenience
wrapper. The `eval.enqueued` / `eval.completed` events are staged INSIDE
those transactions again (`scheduler_event` is commutative, so a
scheduler-lane INSERT is legal), so a row and its event can no longer
disagree. The `Owner::LocalOnly` class and the `local_write` seam are KEPT
but are now empty of tables — pinned by `ownership::LOCAL_ONLY_AUDIT` and
its coverage test — with `prune_scheduler_events` (JC-28 journal
maintenance) as the seam's only live caller. Two JC-27-era special cases
were reverted: the whole-DB snapshot install no longer strips `eval` rows,
and nothing filters the table out of apply. Cross-lane FKs stay removed for
the JC-32 reason (a control-lane `DELETE FROM job` cascading into a
scheduler-lane table is an ownership violation, so `job destroy` would
fail); dangling references terminate rather than spin — `prune_stale_evals`
drops superseded/missing configs, and anything else fails in its handler
into the terminal `failed` status, which the claim query never selects
(`tests/integration/eval_regen_failover.rs::evals_for_vanished_scopes_terminate_
instead_of_spinning`). The reconciler's over-execution finding is fixed by
the replicated `node.drain_evict` intent column: `set_node_availability`
persists whether the operator asked for eviction and clears it whenever
availability leaves `drained`, so a cordon-only node never gets a
`node_drain` eval (`tests/integration/node_drain.rs::cordon_only_nodes_are_never_
evicted_by_reconciliation`); the CLI/RPC surface is unchanged. Proofs:
`tests/integration/raft_eval_failover.rs` (3-node cluster — acknowledged `job stop`
completes and a mid-flight rollout continues after the leader dies, with
followers holding the eval rows and the eval-inclusive hash converging) and
`tests/integration/eval_regen_failover.rs` (the demoted reconciler).

**JC-42 — Event IDs must be globally unique, not `MAX(id)+1`.** The JC-28
commutative class assumed insert-only tables have globally unique PKs.
`scheduler_event` mints `MAX(id)+1` at capture time, so two lanes capturing
concurrently pick the *same* id; `OMIT` then resolves order-dependently and
replicas silently diverge. **Decision:** event ids carry a per-lane stride
(id ≡ lane index mod N), so concurrent lanes can never collide, ordering
stays monotonic per lane, and the cursor/backfill contract is preserved.
The same audit applies to any future commutative table.

*As built:* `Lane::ID_STRIDE = 8` (a power of two — the residue is the low
3 bits — with headroom above the three lanes) and `Lane::id_residue()`
(control 0 / scheduler 1 / status 2). A minted id is
`unix_ms << 21 | sequence << 3 | residue`; the capture publishes its lane on
the thread (`capture::current_capture_lane`) and the minter reads it, falling
back to the control residue outside any capture, so `local_disk` ids are
unchanged in shape. Cross-lane collision is now structural, not
probabilistic; strict per-lane monotonicity is enforced by a per-residue
high-water mark that bumps a full stride when the clock does not advance.
Two consequences the adversarial pass forced explicitly: the opportunistic
prune boundary had to become `(id / STRIDE) % INTERVAL == 0` (the old
`id % INTERVAL == 0` is satisfiable only by residue 0, so only the control
lane would ever have pruned), and the commutative-PK audit is now a named
constant with a coverage test (`ownership::COMMUTATIVE_PK_AUDIT`):
`idempotency_record`'s PK is the caller-supplied idempotency key, so it is
genuinely globally unique and needs no stride — and every current writer
proposes it on the control lane, one total order. The event *stream* keeps
its increasing-id guarantee per subscriber; under `lanes` its live tail is
ordered per lane, so an event whose lane commits late can be skipped live
and read back from the journal by cursor (documented in
`docs/event-stream.md`). A per-lane stream cursor would close that window
and is left as a deliberate follow-up — it changes what a lag backfill
re-reads, which is its own decision.

**JC-43 — Topology is stamped and immutable per data dir.** Switching
`single` ↔ `lanes` on an existing data dir silently brings up blank groups
that existing members never join. The topology is recorded at bootstrap and
a mismatch refuses to start with an actionable message (backup → fresh
cluster → restore), matching the JC-6 posture for engine changes.

**JC-44 — The lanes-v2 sweep: runtime sizing, seam honesty, boot-path
patience (2026-07-25).** The findings JC-40..43 did not cover, and the call
made on each:

- *Runtime starvation.* The engine ran on a hardcoded 2-worker runtime while
  three lanes did blocking SQLite work inline, serialized on one capture
  mutex — two concurrent blocking operations could pin the whole runtime and
  stall heartbeats/elections. Fixed at both ends: the multi-second
  operations (snapshot **build** and **install**, which hold the capture
  mutex through a `VACUUM`/dump/bulk-replace) moved to tokio's blocking pool
  (`spawn_blocking`, still awaited — ordering and backpressure unchanged),
  and the runtime is sized `max(4, lanes * 2)`. `apply` stays inline
  deliberately: it is bounded work, it is the pipeline's backpressure point,
  and openraft runs exactly ONE state-machine worker per lane, so at most
  `lanes` worker threads can be inside an apply — with `lanes * 2` threads
  at least half the pool is always free.
- *The `local_write` seam.* Its scope assertion was `debug_assertions`-only,
  so release builds admitted a write to ANY replicated table. The check is
  now active in every build via the session's **table filter** (fires once
  per touched table, records no changeset in release — so the cost is a
  table name, not the row images). The finer per-op rule (Commutative =
  DELETE-only) still needs the changeset and stays debug-only. Declaring
  intended tables at the call site was rejected as the fix: it would police
  the declaration, not the write.
- *Boot-path aborts.* Startup writes (`load_initial_data`, the UI seed,
  `upsert_manager_config`) are cross-lane composites that `?`-aborted the
  manager when their second half hit a lane this node did not yet lead.
  `startup_write` retries while the error is a typed `NotRaftLeader` (20 ×
  500ms — the JC-40 convergence window), then WARNS and continues; every
  other error still aborts, because a malformed config job must stop
  startup loudly.
- *`state_backup` semantics.* Kept as a whole-DB `VACUUM INTO` (that is what
  `--restore-from` consumes: a fresh cluster's adopted baseline), but the
  leadership gate moved from control-only to **every lane** — free under
  JC-40 co-location, and it refuses the one case a control-only check
  lied about (a node leading control while lagging another lane). The
  reply's `applied_index` is documented as the max across lanes, not a
  cluster-wide cut.
- *DDL through `query_state_store`.* Sessions do not capture schema changes,
  so in raft mode a `--allow-write` DDL statement produced an empty
  changeset, was skipped before proposal (JC-26), and was reported as a
  success that replicated nothing. Raft mode now refuses anything that is
  not row DML with an error naming migrations as the path. `local_disk` mode
  is unchanged.
- *Lane reclassification.* The manifest check that makes lane snapshots safe
  also makes moving a table between lanes a rolling-upgrade breaker. Left as
  is (it is the guard that prevents clobbering a sibling lane) and made
  explicit instead: the mismatch error spells out the JC-43 fresh-cluster
  path, and `docs/multi-raft.md` documents ownership as a cluster-wide
  agreement.
- *Not changed:* the eval failure path already retries transient rejections
  (`daemon::handle_claimed_eval` releases a `NotRaftLeader`-failed eval with
  `release_eval_for_retry` instead of failing it terminally); verified, not
  touched.

### JC-26..29 — Workstream C calls (lanes unblock)

**JC-26 — Single-writer-lane-per-table, enforced at capture time, replaces
"disjointness by audit".** The guard iterates every captured changeset and
rejects ops whose rows leave their lane's ownership; it runs in every
topology, so violations surface long before anyone enables lanes. This is
the same philosophy as the JC-14 bypass guard: invariants as runtime walls,
not review checklists.

**JC-27 — The eval queue leaves raft (reverses JC-15).** JC-15 chose
"replicate everything" for invariant simplicity; lanes give the concrete
reason to revisit: evals are the natural decoupling point for cross-lane
composites, and replicating them would put every lane's fingerprints in one
table. Evals are derived, regenerable work (failover regeneration already
exists and is tested); they move to a scoped local-write seam with their
own guard. Followers carry no eval rows and never schedule.

**JC-28 — A commutative class for insert-only tables.**
`scheduler_event` and `idempotency_record` are inserted by ops on any lane
with globally-unique PKs; insert-if-absent applied with per-table OMIT is
order-independent, so they are exempt from single-writer ownership. Their
pruning becomes local per-node maintenance outside raft, and they are
excluded from the replica content hash (events were already advisory).

**JC-29 — Voter checks warn loudly, block rarely.** Even voter counts get
plain-language warnings at every surface (config load, status, membership
changes, metrics) with the exact fix suggested; only operations that break
quorum feasibility outright are refused. Join promotion is never silently
withheld — quietly leaving a node a learner to preserve parity would trade
a visible warning for an invisible availability surprise.

**JC-30 — Registration is a two-op composite, not a "first heartbeat
seeds" deferral (C-1).** §7-C2 sketched registration touching only `node`
with the first heartbeat creating `node_health`. As built, the public
`insert_node` issues TWO single-lane replicated ops: control seeds `node`
(identity/pool/attrs/availability), then status seeds `node_health` via
`INSERT OR REPLACE` (which also resets a stale `lost_at` — re-registration
"is by definition not lost", a behavior the existing suite pins). Deferring
the seed to the first heartbeat would have dropped registration-supplied
liveness (status/heartbeat/health checks) observable through every existing
node test and would leave re-registration unable to clear `lost_at` without
a cross-lane write. Each changeset stays single-lane (the point of §7-C2);
cross-op atomicity was never promised, and a crash between the ops leaves a
node whose missing health row hydrates exactly the registration defaults
(`'active'`/empty heartbeat/`'{}'`, via COALESCE in the shared
`node ⟕ node_health` hydration join) until the first heartbeat upserts it.
Liveness upserts are EXISTS-guarded on `node` so writes for unregistered
nodes stay no-ops, exactly like the pre-split UPDATEs.

**JC-31 — Orphaned `node_health` rows are swept by a status-lane op;
status ops keep their inline `availability` flips until C-2.** `remove_node`
(control) deletes only the `node` row — `node_health` deliberately has no
FK to `node`, so a control-lane delete can never cascade status-lane rows
into its changeset. The orphan is harmless (every reader hydrates FROM
`node`, so ghosts cannot resurrect) and `prune_node_health_orphans`
(status lane, leader-only, skips proposing when there is nothing to sweep)
collects it on the node-status-checker tick. Separately, mark-ready/lost
still write `node.availability` inline — a foreign-lane write covered by
the C-0 single-topology grace window and explicitly left for C-2's
composite split, which C-1 does not preempt.

**JC-32 — Cross-lane `ON DELETE CASCADE` removal (C-2).** The biggest
foreign-lane writer turned out to be the schema itself: a control-lane
`DELETE FROM job`/`node` cascaded scheduler/status rows (allocation,
instance, rollout, rollout_event, task_status, allocation_health) into the
control changeset, and scheduler-lane allocation deletes cascaded
status-lane `allocation_health` rows. Six tables were rebuilt WITHOUT
cross-lane FKs (the C-0 eval / C-1 node_health precedent), replacing
cascades with explicit owner-lane deletes (destroy_job's per-lane cleanup
ops, prune_rollouts' rollout_event delete, remove_node's
remove_allocs_for_node pass) plus a new status-lane orphan sweep
(`prune_allocation_health_orphans`, on the node-status-checker tick).
Insert-side referential integrity was already enforced by reads (every
insert path resolves its parent rows first), so no FK loss is observable
there; `rollout_event` keeps no FK at all — its deletes are explicit and
same-lane.

**JC-33 — Two split shapes: wrapper-sequential for synchronous composites,
eval-mediated for the reschedule tails.** Ops whose callers observe the
composite's full effect on return use the JC-30 shape — the public wrapper
issues one single-lane replicated op per lane, in order: mark-ready/lost →
`sync_node_availability` (control), `handle_job_submit` →
`seed_task_statuses` (status), `apply_plan_submission` →
`update_task_statuses_for_allocations` (status), `mark_job_failed_for_kv`
→ `stamp_allocations_failed_for_kv` (scheduler), `remove_node` →
`remove_allocs_for_node` first, and `destroy_job` → status cleanup →
scheduler cleanup → control delete. The lost-node reschedule tail instead
moved behind a new `node_lost_reschedule` eval (the plan's Nomad shape):
`mark_nodes_as_lost` marks liveness + flips availability + enqueues;
`recheck_lost_nodes` is now enqueue-only (deduped per node while one eval
is pending, re-enqueued each tick — same convergence behavior);
`regenerate_scheduling_evals` regenerates the eval after failover.
Store-level tests pump the local eval queue where the daemon's eval worker
would.

**JC-34 — Rollout mutations consolidated on the scheduler lane by
reclassification, not eval mediation.** All five rollout writers
(`create_rollout`, `update_rollout_status`, `update_rollout_config_json`,
`prune_rollouts`, `append_rollout_event`) are standalone store ops — an
op's lane is independent of its caller — so consolidating meant changing
their `replicated()` lane, no new evals. `rollout_event` was reclassified
to the scheduler lane rather than made Commutative: it has exactly one
writing op (`append_rollout_event`), so single-writer ownership is the
simpler sound option (JC-28's commutative class stays reserved for tables
genuinely written from multiple lanes).

**JC-35 — Reads inside a capture see committed state; the
remove-then-reconcile composites are now sequential ops.** A captured
body's reads go through the read-only pool, which sees the last COMMITTED
state — not the capture savepoint's own uncommitted writes. The
`reschedule_allocations_for_{lost,drained,new}_node` composites therefore
had a latent raft-mode bug (masked in local_disk, exposed by the new guard
test): the reconcile composed into the same capture as the eviction could
not see the eviction and placed zero replacements. They are now
wrapper-level sequences of single-purpose scheduler-lane ops
(`remove_allocations`, then `reconcile_job_config_allocations` per config)
so each op reads its predecessor's committed effects; crashes mid-sequence
are healed by the idempotent eval re-runs. Same-lane atomicity was traded
away deliberately — every pass is an idempotent convergence step.

**JC-36 — `query_state_store` write statements stay a control-lane op.**
The state-store escape hatch replicates arbitrary operator SQL on the
control lane; with enforcement on, a write touching a non-control table is
rejected by the guard in raft mode with the standard ownership error
(local_disk is unaffected — no capture there). This is the intended
consequence of single-writer ownership, documented rather than
special-cased: operators mutate scheduler/status state through the real
APIs, not raw SQL.

**JC-37 — Lane snapshots are standalone SQLite dump databases, not
JSON-lines (C-3).** A lane's snapshot is a small SQLite db holding one
table per owned table (control also carries the JC-28 commutative tables)
plus a manifest (`legion_lane_snapshot` format/lane markers and a table
list). SQLite storage classes round-trip row values exactly (no
blob/float text-encoding ambiguity, which JSON-lines would reintroduce),
and both build and install use a SEPARATE connection to the dump file —
no ATTACH/DETACH state on the shared write connection. Build runs under
the same `exclusive_state_access` guard as the whole-DB `VACUUM INTO`
(which `single` topology keeps unchanged); install validates the
manifest against the local ownership map (a dump listing a foreign
lane's table, a foreign lane's dump, or a whole-DB image is refused
loudly), then in ONE transaction — capture-excluded, apply-guarded,
`foreign_keys` OFF per JC-18 — deletes the dumped tables' rows, inserts
the dump's rows, and writes the lane's `raft_applied` marker from the
snapshot meta. The applied marker travels in the snapshot *metadata*
(unlike whole-DB images, where it travels inside the image), because the
install target is the live shared DB. Retention/tmp-cleanup/policy reuse
the single-topology code per lane; the bootstrap seed snapshot (JC-21)
now runs per lane, so the three seed dumps together cover an adopted DB.

**JC-38 — Apply-side index-skip guard added (was missing from A-3).**
`StoreStateMachine::apply` now reads the lane's recorded applied index
once per batch and skips entries at or below it (still acking them).
openraft normally resumes above `applied_state()`, but an entry
re-delivered around a snapshot install or restart replay would
conflict-abort on its own already-applied effects — under lanes there are
three independent install/replay pipelines, so the guard is load-bearing
rather than theoretical. Unit-tested (`apply_skips_entries_at_or_below_
the_applied_index`).

**JC-39 — Leader hints and status name the lane.** `NotRaftLeader` gained
a `lane` field: under `lanes` topology every rejection says which lane's
leader to contact ("not the raft leader for the status lane; retry
against its leader mgr-b at …"); `single` topology keeps the unqualified
message (one leader — naming a lane would only confuse). `raft status`'s
aggregate leader column shows the plain leader while all lanes agree and
switches to a `control=…,scheduler=…,status=…` summary when they differ;
per-lane lines (A-2's `RaftLaneStatus`) carry the full detail. A
`Store::raft_trigger_lane_election` test hook (mirroring the JC-partition
hook) moves a single lane's leadership so the battery can prove election
independence — the operator command deliberately still elects all lanes.

**JC-40 — Daemon loops gate per lane; cross-lane composites under
divergent leadership are reject-and-retry, not forwarded.** Startup
writes and their leadership-acquisition rerun hook track control
(config/seed/manager-config) and scheduler (rollout prune) leadership
independently; eval requeue/regeneration and the eval workers gate on the
scheduler lane; the node-status checker gates on the status lane, with
rollout reconciliation moved onto its own scheduler-gated cadence timer
(the status and scheduler leaders can be different nodes). A wrapper
composite (e.g. `mark_nodes_as_lost` = status mark + control availability
sync) still proposes each half from the calling node, so when lanes have
different leaders the foreign-lane half is cleanly rejected with that
lane's leader hint; callers warn-and-retry (the status checker no longer
`?`-exits on it) and each half is idempotent, but full composite
convergence under long-lived divergent leadership needs auto-forwarding —
already tracked as follow-up #4, now with this concrete consumer. The
skeptic pass (C-5) should weigh this when the default-topology decision
is made.

*Superseded in part by the lanes-v2 JC-40 above:* long-lived divergent
leadership is no longer a state the system sits in — the engine's
anchor-lane convergence loop removes it, so the per-lane daemon gating
below stays exactly as described but its divergent-leadership branches
(the eval-regeneration safety net, the status checker's warn-and-retry)
are now transient-window code, not the steady state. Auto-forwarding
(follow-up #4) stays a follow-up, and is no longer load-bearing.

### JC-16+ — execution-time calls

Made during workflow execution (phases B-0, A-1..A-3, and the adversarial
review); each is documented at its code site.

**JC-16 — Drain holds *every* stateful allocation.** JC-9 said drain keeps
the `onLost` protection gates. As implemented
(`Store::reschedule_allocations_for_drained_node`), a drained-but-alive
node has no `lost_at`, so the loss duration passed to
`lost_allocation_replaceable` is `None` and **all** stateful allocations
are held, not just those inside a protection window. They keep running on
the drained node until it is actually lost (the lost-node path takes over)
or the operator replaces them (`ReplaceInstance`). The `node.drained`
event reports evicted/held counts, so a drain that is waiting says so.

**JC-17 — `topology: 'lanes'` is refused at engine start, not shipped
runnable-experimental.** Stronger than JC-2: because the `node` table is
written by two lanes and event rows are staged on every lane, independent
per-lane groups could apply shared-row writes in different orders on
different replicas — silent divergence. The engine rejects any multi-group
topology at start (`ensure_engine_topology_supported`), the state machine
**refuses snapshot build/receive/install loudly** for lanes (per-lane
whole-DB images would clobber sibling lanes' tables), and lanes runs under
`SnapshotPolicy::Never` so the paths cannot trigger. The lane machinery
still ships classification-complete and unit-tested as scaffolding; the
planned `tests/raft_multigroup.rs` bring-up test is deferred to the
node/node_health split (Follow-ups 1).

**JC-18 — `foreign_keys = OFF` during changeset apply.** Changesets
already record every cascaded child row exactly as the leader's SQL
produced it; re-firing `ON DELETE CASCADE` at apply would delete children
early and abort the quorum-committed entry forever
(`SQLITE_CHANGESET_NOTFOUND`), permanently bricking the store on a routine
`job destroy`. Adversarial finding; regression `tests/integration/raft_cascade_delete.rs`.

**JC-19 — Captures and applies are mutually exclusive under the global
capture mutex.** An apply interleaving with an open capture savepoint
would nest inside it and be silently erased by the capture's rollback —
destroying a committed entry on this replica. Applies (and the
blank/membership marker writes) take the capture mutex and additionally
fail loudly if the connection is not in autocommit. Adversarial finding;
regressions in `src/storage/raft/capture.rs` tests.

**JC-20 — Panic containment in the capture path.** A panic inside a
captured body previously orphaned the open capture savepoint (every later
"commit" a mere nested RELEASE — a write-dead zombie) and poisoned the
capture mutex. Capture teardown is now unwind-safe, the pipeline thread
converts body panics into error replies and keeps serving, and mutex
poisoning is tolerated.

**JC-21 — Bootstrap builds a seed snapshot of the adopted DB.** JC-6
refinement: pre-raft state exists in no log entry, so a joiner caught up
by log replay alone would silently miss the entire adopted baseline. A
fresh bootstrap forces a whole-DB snapshot as the log's base and purges
the log to it, making snapshot install (which carries the full DB) the
necessary catch-up path for blank joiners. Regression
`tests/integration/raft_upgrade_bootstrap.rs`.

**JC-22 — Cluster identity is stamped and double-starts are locked out.**
The state DB is stamped with the secret-derived cluster id at first engine
start; a later start with a different derived id (wiped `dataDir` +
re-bootstrap) is refused unless `forceNewCluster: true` — closing a
split-brain path. Separately, `<dataDir>/lock.sqlite` holds an exclusive
OS lock so a second manager process over the same raft data dir fails
fast instead of running a competing raft core.

**JC-23 — Join tokens are unsigned payloads embedding the secret.** §A4
said "signed token"; as built, the token is base64url JSON carrying
`{cluster_id, secret, leader_advertise_addr, expires_unix}` — the embedded
secret *is* the credential (constant-time compare + expiry on the leader),
so a signature would add nothing: token confidentiality equals secret
confidentiality, the JC-7 posture.

**JC-24 — `step-down`/`transfer-leader` return honest unsupported
errors.** openraft 0.9 has no leader-transfer API (it lands in 0.10);
these commands fail with guidance (`trigger-election` on the desired
leader) instead of the simulation-era fake success. Relatedly, the leader
hint on non-leader rejection travels in the `FAILED_PRECONDITION`
*message* (name + address), not structured error metadata as §A4 sketched.

**JC-25 — The `legion_raft_*` metric series remain reserved.** B1 listed
raft gauge series; the execution shipped raft observability through
`raft status`/`history` (live engine metrics) and the `/readyz` leader
check, and kept the Prometheus prefix reserved rather than emitting
half-designed series. Tracked with the diagnostics follow-up (§6.2).

**JC-26 — No-op mutations skip raft entirely.** A capture that touches
zero rows yields an empty changeset; there is no effect to replicate, so
no entry is proposed and the caller gets its return value directly. Also
in the pipeline: the not-a-leader check runs *before* capture, so
followers never open a capture savepoint that could collide with their
concurrent committed-entry applies.

**JC-27 — Restore clears the cluster stamp (JC-22 × A-3 interaction,
caught by the docs-pass verification).** A `VACUUM INTO` backup taken on a
raft cluster carries the source cluster's `raft_cluster` stamp; restoring
it onto a fresh bootstrap node (the documented flow) then tripped JC-22's
re-bootstrap refusal, since the fresh node derives a different cluster id.
Fix: `restore_from_backup` clears `raft_cluster` alongside `raft_applied`
— an explicit `--restore-from` *means* "seed a fresh cluster from these
contents", so the accidental-wipe protection does not apply to it (the
guard still fires on every non-restore path; `tests/integration/raft_upgrade_bootstrap.rs`
unchanged and green). `state_content_hash` correspondingly excludes
`raft_cluster` (node-local, never replicated) so replica-convergence
fingerprints compare only replicated content.

**JC-44 — The topology stamp lives in the raft DATA DIR (and the join
handshake), not only in the state DB (JC-43 as built).** The data dir is
where the per-lane logs and snapshots are, so it is the artifact that
genuinely cannot be reinterpreted under another topology;
`<data_dir>/topology.json` records `{topology, lanes}` and is written at
first engine start, mirroring how the cluster secret is persisted. It is
checked BEFORE any log/vote write and has deliberately **no
`forceNewCluster` override** — the supported migration is backup → empty
data dir → restore, and an empty data dir has no stamp to conflict with.
The state-DB stamp is kept as the mirror-image guard (data dir wiped, state
DB preserved), where `forceNewCluster` still applies. `Join` carries the
joiner's topology so a mixed-topology member is refused at enrollment,
before any membership change, with the same message (as `InvalidArgument`,
so the joiner's retry loop stops instead of hammering a config mismatch).

**JC-45 — The raft transport's real size ceiling was a TIMEOUT, not a byte
limit; timing knobs move to 250 ms / 1-2 s.** Raising tonic's message limit
and base64-ing the binary payloads (changeset, snapshot chunk — ~3x smaller
than serde_json's number-array rendering) was necessary but not sufficient:
openraft uses `install_snapshot_timeout` (default **200 ms**) as the
per-chunk deadline and `heartbeat_interval` as the append-entries deadline,
and a payload that misses its deadline is retried verbatim forever. Measured
at 50 ms heartbeats: a follower behind a compacted log received the same
1.4 MiB batch indefinitely, the leader never saw the conflict reply, so it
never fell back to install-snapshot — the follower could NEVER catch up.
Fix: `install_snapshot_timeout` 30s, `max_payload_entries` 32,
`heartbeat_interval` 250 ms with 600-1200 ms election timeouts — the
election window is kept as tight as the heartbeat allows, not scaled up
proportionally, because openraft derives the follower leader lease from
`election_timeout_max` and a live leader refreshes it every heartbeat, so a
larger max makes every deliberate leadership move take proportionally
longer to win. Failover stays ~1s, in the range of Consul's and etcd's
defaults. Consequence for tests: `trigger_lane_election` makes a
node *campaign*, it does not make it win — the lanes battery now retries the
trigger like the daemon's convergence loop and the durability harness
already did.

---

**JC-11 — Event journal and read model are projected from changesets at
apply time.** Events staged during capture would otherwise broadcast before
quorum (or be lost on rollback); the read model would cache uncommitted
state. Both side-effect channels are suppressed during capture and driven by
the committed changeset at apply, on every replica — followers therefore get
read models and event streams for free, which the simulation-era code never
had to think about.
