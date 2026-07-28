# Persistence Plan: Pluggable Backends for the Manager Store

> **Judgement calls:** [`calls/persistence-plan_calls.md`](../calls/persistence-plan_calls.md)

## Status and Goal

This document proposes a **pluggable persistence layer** under the current SQLite-backed `Store` so operators can pick a reliability/complexity profile:

1. **Local disk only** (today): simplest, zero clustering.
2. **SQLite + asynchronous replication** (Litestream-like): low complexity, low-cost disaster recovery, optional warm standby.
3. **SQLite + consensus replication** (Raft): highest durability/availability, strongest write guarantees.

Design intent:

- Keep existing store APIs and data model mostly unchanged.
- Add a persistence backend abstraction under `Store::start_storage` and write paths.
- Maintain a clear gradient from simple to complex so users can stop at the level they actually need.

---

## Requirements and Non-Goals

### Requirements

- Same logical store semantics at API level (same tables/migrations/state model).
- Minimal behavioral differences for scheduler/eval loops.
- Pluggable backend chosen by manager config.
- Clear operational commands (init, join, status, promote/failover).
- Safe defaults that avoid split brain.

### Non-Goals (initially)

- Multi-writer SQLite.
- Cross-region active/active writes.
- Exact zero-RPO failover for non-consensus modes.

---

## Proposed Architecture

## 1) Introduce a Persistence Driver Boundary

Add a trait that owns write durability behavior while letting `Store` keep SQL and read-model logic.

```rust
pub trait PersistenceDriver: Send + Sync {
    /// Called once at manager startup. Can restore DB, acquire leases, start replication, etc.
    async fn start(&self, ctx: PersistenceContext) -> Result<PersistenceHandle>;

    /// Called before/after mutating store transactions as needed by backend.
    async fn before_write(&self, op: &StoreOpEnvelope) -> Result<()>;
    async fn commit_write(&self, op: &StoreOpEnvelope, tx_result: &StoreWriteResult) -> Result<()>;

    /// Health and role for admin/status APIs.
    async fn status(&self) -> PersistenceStatus;
}
```

Notes:

- `StoreOpEnvelope` should be deterministic and small (operation name + normalized args + request id).
- The default driver (`local_disk`) is effectively no-op hooks.
- Litestream/Raft drivers extend behavior without rewriting store SQL.

## 2) Manager Role Model

Expose manager role as part of persistence status:

- `primary` (can accept writes)
- `standby` (read-only or idle)
- `candidate` (transition)

Guardrails:

- Only `primary` handles mutating gRPC methods.
- Non-primary nodes return `FAILED_PRECONDITION` with redirect metadata.

---

## Option A: Local Disk (Baseline)

### Description

Current mode: manager writes to local SQLite only.

### Guarantees

- Single-node durability only.
- RPO depends on disk safety and backups (potentially high).
- RTO tied to host recovery time.

### Complexity

- Lowest.
- No network dependencies.

### When to use

- Dev/test.
- Small single-node deployments where occasional restore is acceptable.

---

## Option B: Litestream-like Asynchronous Replication

### Description

Primary manager writes local SQLite/WAL, and a replicator continuously ships WAL pages to object storage (S3/GCS/Azure/etc.).

Standby manager keeps a restorable local DB and can be promoted.

### Guarantees

- **RPO: small but non-zero** (replication lag window).
- **RTO: low to medium** (promote standby + leader switch).
- No linearizable multi-node writes by itself.

### Key Question: Simple failover without split brain

The simplest reliable model is:

1. **Single writer lease** in a tiny coordination system (e.g., etcd/Consul lease or Kubernetes Lease API).
2. Primary must hold lease to accept writes.
3. Standby is read-only and continuously restores/refreshes from replica stream.
4. On primary loss, standby acquires lease, runs final catch-up restore, becomes primary.
5. Old primary, when it returns, starts as standby and cannot write unless it reacquires lease.

This gives automatic recovery and split-brain resistance with minimal moving pieces.

#### Why this is the simplest safe path

- Litestream alone is backup/replication, not consensus/leader election.
- The lease provides fencing for writes.
- Operationally still far simpler than full Raft.

### Should we add leader election in this mode?

Yes, **but keep it lease-only and single-primary**:

- Good: simple failover UX, no consensus log implementation work.
- Tradeoff: still async replication, so failover can lose the last few seconds of writes.

This is a good middle tier for users who want automatic failover but can tolerate tiny data loss.

### Recommended implementation details

- New backend: `persistence.type = "streaming"`.
- Sub-config:
  - `replication` (bucket/prefix, sync interval)
  - `lease` (provider, key, ttl, renew interval)
  - `failover` (promote timeout, catch-up timeout)
- Write gate: every mutating RPC checks `role == primary && lease_held`.
- Fencing token included in manager metadata for observability.

### CLI/ops commands

- `legion manager persistence status`
- `legion manager persistence promote --node <id>` (admin override)
- `legion manager persistence demote --node <id>`
- `legion manager persistence lease inspect`

---

## Option C: Raft-backed Persistence (Consensus)

### Description

All mutating operations become replicated log entries. A write is acknowledged only after quorum replication + commit.

### Guarantees

- Strong consistency for writes (quorum-based).
- No split brain under correct quorum operation.
- Better failover correctness than streaming replication.

### Best way to do Raft with SQLite

**Recommended approach:** replicate **deterministic store operations**, not raw SQLite WAL bytes.

Flow:

1. Manager receives mutating RPC.
2. Build deterministic `StoreOpEnvelope` command.
3. Propose to Raft.
4. On commit, apply command to local SQLite transaction.
5. Return success after apply (or after commit+apply depending API contract).

Why:

- WAL shipping via Raft is brittle and SQLite-internals-heavy.
- Command replication follows known designs (rqlite-like) and keeps schema ownership in Legion.
- Easier compatibility across SQLite versions and pragmas.

### Raft vs Paxos

Choose **Raft** for Legion:

- Easier operational/debug model (leader, term, log index).
- Better ecosystem and implementation guidance.
- Paxos is viable but usually harder to implement/operate correctly for product teams.

### Rust library guidance

Prefer an established Rust Raft engine with membership/change support and metrics hooks.

Pragmatic path:

- Start with `openraft` (or `raft-rs` if deeper control is needed).
- Build a thin `RaftNode` service around:
  - log storage
  - snapshot storage
  - network transport
  - FSM apply adapter to existing `Store` write APIs

### Cluster UX: easy setup with join tokens

Use a manager-cluster join token flow similar to worker joins.

- `legion manager cluster init`
  - boots first voter, emits manager join token.
- `legion manager cluster join <TOKEN>`
  - auto-registers node, receives peer list, requests membership change.
- `legion manager cluster status`
  - shows leader, term, commit/applied indices, quorum health.
- `legion manager cluster members`
  - list voters/non-voters.
- `legion manager cluster remove <node-id>`
- `legion manager cluster transfer-leader <node-id>`

Token payload should include:

- cluster id
- bootstrap endpoint(s)
- expiry
- optional CA pin / TLS material reference
- one-time nonce

No manual IP choreography by default:

- require advertised stable node IDs + addresses
- support DNS seed names and re-resolution
- store cluster membership in Raft config state

### Snapshots and recovery

- Periodic SQLite snapshot + Raft snapshot metadata.
- Fast catch-up: install snapshot then replay remaining log.
- Keep snapshot/compaction knobs simple with sane defaults.

---

## Additional Option Worth Including: Managed External Database Backend

If the project later needs higher durability without owning consensus, an optional external backend (e.g., Postgres) could be added.

### Why it might make sense

- Offloads replication/failover/backup complexity to managed DB.
- Good for users who do not want to run Raft themselves.

### Why not first

- Larger rewrite cost vs current SQLite assumptions.
- Moves away from single-binary simplicity.

Recommendation: keep this as a future fourth tier, not in initial rollout.

---

## Decision Matrix

| Mode | Write Consistency | Failover | RPO | Operational Complexity | Best For |
|---|---|---|---|---|---|
| Local disk | Single node | Manual/host-level | High to medium | Very low | Dev/small installs |
| Streaming + lease | Single primary, lease fenced | Automatic (single standby or N standbys) | Low but non-zero | Low-medium | Small/medium prod |
| Raft | Quorum/linearizable writes | Automatic via quorum | Near-zero for committed writes | High | High-criticality control planes |

---

## Recommended Rollout Plan

## Phase 0: Internal abstractions (no behavior change) ✅ Done (2026-03-19)

- [x] Add `PersistenceDriver` and `local_disk` implementation.
- [x] Add `PersistenceStatus` plumbing and manager role exposure.
- [x] Keep default config exactly as today.

## Phase 1: Streaming backend (Litestream-like) with optional lease failover ✅ Completed (2026-03-20)

Delivered scope:

- Implement replication integration + restore-on-start.
- Add lease provider abstraction with:
  - `none` for manual failover.
  - `object_storage` for automatic failover with write fencing.
- Enable automatic standby promotion when lease acquired (`object_storage` lease mode).
- Add admin/monitoring CLI commands.

Explicitly deferred (not implemented in Phase 1):

- `k8s_lease` provider
- `etcd` provider
- `consul` provider

Exit criteria:

- [x] failover drills succeed automatically (with `object_storage` lease mode)
- [x] split brain prevented by lease fencing tests
- [x] documented RPO envelope under load

### Phase 1 lease model details

#### `none` lease provider (manual failover)

- Intended for simple environments that can tolerate operator-driven failover.
- Only one manager should be configured for writes at a time.
- Promotion/demotion is an explicit admin action.
- No automatic fencing guarantees are provided beyond operational discipline.

#### `object_storage` lease provider (R2-compatible)

The object-storage lease is designed against S3-compatible semantics so it works with Cloudflare R2 and standard S3 APIs.

Lease record shape (single key per cluster, e.g. `leases/<cluster-id>/primary.json`):

```json
{
  "holder_id": "mgr-a",
  "lease_id": "01HR...ULID",
  "term": 42,
  "acquired_at_unix_ms": 1760000000000,
  "expires_at_unix_ms": 1760000015000,
  "renew_deadline_unix_ms": 1760000010000
}
```

Fencing and concurrency rules:

1. Acquire with conditional write (`If-None-Match: *`) when key does not exist.
2. Renew only with a compare-and-swap style conditional write (`If-Match: <etag>`) so only current holder can extend.
3. Release by conditional delete/write using the latest observed ETag.
4. Writers must check `role == primary && lease_held && now < expires_at` before every mutating RPC.
5. Standby promotion requires successful acquire, then final catch-up restore, then role transition.

R2 compatibility notes:

- Uses only S3-compatible object operations (HEAD/GET/PUT/DELETE + conditional headers).
- Avoids provider-specific lock primitives.
- Clock-skew tolerance is handled by conservative TTL/renew intervals and a safety margin in `renew_deadline_unix_ms`.

Suggested defaults:

- `ttl = 15s`
- `renew_interval = 5s`
- `renew_jitter = 250ms`
- `takeover_grace = 2 * max(sync_interval, renew_interval)`

### Streaming replication implementation choice (Litestream vs custom)

After evaluating Rust ecosystem options for SQLite WAL shipping, the recommended Phase 1 implementation is to use **Litestream itself** as the replication engine rather than building a custom Rust WAL streamer.

Reasoning:

- There is no mature Rust crate that provides Litestream-equivalent SQLite WAL shipping, restore, retention, and operational hardening as a drop-in library.
- Building a custom WAL shipper would add high correctness risk around SQLite/WAL edge cases, crash recovery, and object-store consistency behavior.
- Litestream already solves the critical replication mechanics and has known operational behavior.
- Legion should focus custom Rust logic on lease fencing, role transitions, and control-plane orchestration.

Implementation shape:

- Run/coordinate Litestream process from manager runtime (or sidecar) with generated config.
- Keep a narrow adapter boundary in Legion:
  - start/stop replication
  - trigger restore/catch-up
  - expose health/lag into `PersistenceStatus`
- Revisit native Rust implementation only if operational constraints later require deeper embedding.

## Phase 2: Raft tech preview ✅ Superseded/Done (2026-07-23)

- [x] Replicated writes — for **all** store APIs, not a subset (changeset
      replication; see "Remaining GA work" below).
- [x] Cluster bootstrap/join/member commands.
- [x] Observability: leader, term, indexes, history (Prometheus
      `legion_raft_*` series still reserved).

Exit criteria:

- [x] deterministic apply tests pass under network partitions
      (`tests/raft_durability.rs`)
- [x] crash/restart + snapshot restore validated
      (`tests/raft_snapshot_recovery.rs`)

## Phase 3: Raft GA (control-plane operations) ✅ Done (2026-03-20)

- [x] Leader election and failover controls are available in manager RPC, CLI, and UI.
- [x] Operator commands exist for election trigger, step-down, leader transfer, and membership changes.
- [x] Raft join-token issuance and audit history are exposed for cluster operations.
- [x] Observability includes role/term/leader/indexes/history and actor-attributed command traces.

### Remaining GA work (store replication path)

> Status 2026-07-23: the store replication path is **implemented** by the
> real raft engine (`src/storage/raft/`), executed per
> [docs/plans/durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md).
> One deliberate deviation from this plan's §"Best way to do Raft with
> SQLite": instead of typed `StoreOpEnvelope` command replication, the
> engine replicates **SQLite session changesets** (the exact row-level
> effects of each mutation), which makes determinism structural rather
> than audited per method (plan JC-1). Operator docs: `docs/multi-raft.md`.

- [x] Cover all mutating store operations with committed Raft apply
      (every mutating method routes through `Store::replicated()`; a
      runtime guard rejects bypassing writes in raft mode).
- [x] Snapshot/compaction pipeline for SQLite state-machine replication
      (`VACUUM INTO` whole-DB snapshots every `raft.snapshotEntries`,
      log purge behind them, snapshot-install catch-up).
- [x] Rolling-upgrade and migration hardening — for the supported paths:
      `local_disk`/simulated-raft → raft adopts the existing DB at
      bootstrap (with a seed snapshot for joiners); backup/restore seeds a
      fresh cluster; rollback to `local_disk` is documented. Mixed-mode
      clusters (old simulated raft + real raft) are deliberately
      unsupported (fresh-bootstrap rule, `docs/upgrades.md`).
- [x] Durability/partition/fault-injection test matrix for replicated
      writes (`tests/raft_durability.rs`: divergence-under-interleaving,
      leader loss mid-burst, minority partition, any-single-node loss;
      `tests/raft_snapshot_recovery.rs`: crash/restart + laggard
      snapshot catch-up; `tests/raft_upgrade_bootstrap.rs`,
      `tests/raft_cascade_delete.rs`).
- [ ] Parallelize scheduler planning across manager nodes with Raft-fenced plan ownership so multiple managers can compute placement decisions concurrently without double-apply.

---

## Concrete Config Shape (Draft)

```jsonnet
{
  persistence: {
    type: "local_disk", // local_disk | streaming | raft

    streaming: {
      replica_url: "s3://bucket/legion/prod",
      sync_interval: "1s",
      lease: {
        provider: "object_storage", // none | object_storage
        key: "leases/prod/primary.json",
        ttl: "15s",
        renew_interval: "5s",
        object_storage: {
          endpoint: "https://<account-id>.r2.cloudflarestorage.com", // optional for AWS S3
          region: "auto", // R2 commonly uses "auto"
          bucket: "legion-prod-ha",
        },
      },
      standby: {
        auto_promote: true,
        catchup_timeout: "20s",
      },
    },

    raft: {
      node_id: "mgr-a",
      data_dir: ".legion/raft",
      bootstrap: false,
      join_token: null,
      snapshot_interval: "60s",
      max_log_entries: 10000,
    },
  },
}
```

---

## Risks and Mitigations

- Risk: hidden divergence between store write paths and replicated commands.
  - Mitigation: centralize all mutations behind command envelopes + property tests.
- Risk: perceived simplicity loss.
  - Mitigation: keep local mode default; make streaming mode one extra config block.
- Risk: operator error during failover.
  - Mitigation: fenced write gate + explicit role/status endpoints + tokenized cluster join.

---

## Final Recommendations

1. Build the persistence driver interface first, preserving current SQLite behavior.
2. Ship **streaming + lease-fenced failover** as the first HA tier (best simplicity/safety tradeoff).
3. Implement **Raft command replication** (not WAL replication) for the strongest durability tier.
4. Keep UX token-based and command-driven to avoid manual IP-heavy operations.
5. Document explicit guarantees (RPO/RTO/consistency) so users pick the simplest mode that meets needs.
