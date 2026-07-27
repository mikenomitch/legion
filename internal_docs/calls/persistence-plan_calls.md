# Judgement Calls — Pluggable Persistence

Companion to [`plans/persistence-plan.md`](../plans/persistence-plan.md).
The decisions behind the persistence tiers, pulled out of the option analysis.

Entries are ranked by project impact, highest first.

## 1. Three tiers, not one HA story

Local disk, streaming replication with lease failover, and Raft consensus are all
supported so operators pick a reliability/complexity profile rather than paying
for consensus they don't need. The cost is a persistence driver boundary and
three sets of documented guarantees; the benefit is that the simplest deployment
stays a single binary with a SQLite file.

## 2. Raft replicates *commands*, not the WAL

The strongest tier replicates deterministic commands through the log and applies
them to each node's SQLite, instead of shipping WAL frames. WAL-level replication
would couple consensus to SQLite's file format and make deterministic apply tests
impossible.

## 3. Streaming tier: lease-only single-primary, not consensus

Automatic failover comes from a single-writer lease with write-gating
(`role == primary && lease_held`) and a fencing token, not from an election
protocol. Litestream-style replication is backup, not consensus — the lease is
what makes it split-brain-safe. The accepted trade-off is async replication: a
failover can lose the last few seconds of writes, which is stated in the tier's
guarantees rather than hidden.

## 4. Litestream as the streaming engine — later superseded by rustyriver

The plan's Phase 1 recommendation was to run Litestream itself rather than write a
Rust WAL shipper: no mature Rust crate was equivalent, and hand-rolling WAL
edge cases, crash recovery, and object-store consistency is high-risk work that is
not Legion's differentiator. Legion's custom code stayed in lease fencing, role
transitions, and orchestration, behind a narrow adapter (start/stop, restore/
catch-up, health/lag).

That call has since been revisited exactly on the terms it set out: with
[rustyriver](../rustyriver-migration.md) — an embeddable Rust reimplementation of
Litestream v0.5 — the same adapter boundary is satisfied in-process, so the
sidecar binary went away without re-taking on the WAL correctness risk.

## 5. Driver boundary first, behavior unchanged

Phase 0 introduced the abstraction under the existing SQLite `Store` with no
behavior change. Every later tier is an implementation of that seam, so no tier
gets to reshape the store's API on its way in.

## 6. Guarantees are documented per tier (RPO/RTO/consistency)

An explicit decision matrix exists so users can pick the simplest mode that meets
their needs, rather than defaulting to the most durable one out of uncertainty.

## 7. Managed external database: considered, deferred

An external managed DB backend is documented as a real option but not built first;
it trades Legion's single-binary story for someone else's operational surface, and
the streaming tier covers most of the same need.

## 8. Cluster UX is token- and command-driven

Join tokens and `legion manager persistence …` verbs instead of IP-heavy manual
operations, so the HA tiers keep the same "one command" ergonomics as the rest of
the product.
