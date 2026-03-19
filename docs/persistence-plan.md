# Persistence Plan: Pluggable Backends for the Manager Store

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

## Phase 0: Internal abstractions (no behavior change)

- Add `PersistenceDriver` and `local_disk` implementation.
- Add `PersistenceStatus` plumbing and manager role exposure.
- Keep default config exactly as today.

## Phase 1: Streaming backend (Litestream-like) with optional lease failover

- Implement replication integration + restore-on-start.
- Add lease provider abstraction (`none`, `k8s_lease`, `etcd`, `consul`).
- Enable automatic standby promotion when lease acquired.
- Add admin/monitoring CLI commands.

Exit criteria:

- failover drills succeed automatically
- split brain prevented by lease fencing tests
- documented RPO envelope under load

## Phase 2: Raft tech preview

- Implement command-replicated writes for a subset of APIs.
- Add cluster bootstrap/join/member commands.
- Add observability: leader, term, lag, apply latency.

Exit criteria:

- deterministic apply tests pass under network partitions
- crash/restart + snapshot restore validated

## Phase 3: Raft GA

- Cover all mutating store operations.
- Production hardening (membership changes, backup, compaction, rolling upgrades).
- Clear migration path from non-Raft modes.

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
        provider: "k8s_lease", // none | k8s_lease | etcd | consul
        name: "legion-manager-primary",
        ttl: "15s",
        renew_interval: "5s",
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
