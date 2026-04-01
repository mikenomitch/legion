# Running Raft on Legion (Leader Election + Failover)

This guide explains how to run Legion managers in **Raft control-plane mode** for leader election, failover operations, and cluster operator workflows.

> Current scope: Raft currently governs manager leadership/election workflows and operational controls. SQLite state-machine replication via Raft apply is not yet enabled, so manager data is still local SQLite per node.

## What you get today

- Leader election visibility (`role`, `leader`, `term`, commit/applied indexes).
- Operator controls for failover workflows:
  - trigger election
  - step down leader
  - transfer leadership
  - add/remove members (voter/learner)
- Join-token issuance for manager cluster enrollment workflows.
- Auditable raft history in RPC, CLI, and UI.

## Configure manager Raft mode

In your manager Jsonnet config, set persistence to `raft`:

```jsonnet
{
  persistence: {
    type: 'raft',
    raft: {
      nodeId: 'mgr-a',
      bootstrap: true,
      peers: ['mgr-a', 'mgr-b', 'mgr-c'],
      joinTokenTtlSeconds: 900,
    },
  },
}
```

- `nodeId`: local manager ID for raft control-plane identity.
- `bootstrap`: set true for first manager in a cluster.
- `peers`: initial known raft members (IDs/endpoints by your operational convention).
- `joinTokenTtlSeconds`: default expiration for issued join tokens.

## Start manager

```bash
cargo run -- start manager -c configs/manager.jsonnet
```

## CLI operations

### Status

```bash
cargo run -- raft status
```

### History

```bash
cargo run -- raft history --limit 100
```

### Commands

```bash
cargo run -- raft command trigger-election
cargo run -- raft command step-down
cargo run -- raft command transfer-leader --target mgr-b
cargo run -- raft command add-voter --target mgr-d
cargo run -- raft command add-learner --target mgr-e
cargo run -- raft command remove-member --target mgr-e
```

### Join token

```bash
cargo run -- raft join-token
```

## UI operations

Open the Legion UI and navigate to **Cluster → Raft**.

The page provides:

- status card (role/leader/term/indexes/library)
- operator command buttons with optional target node
- one-click join-token issuance
- recent event history with actor, target, and term

## Observability expectations

For production-ready operator workflows, raft events are recorded with:

- event type (`election`, `failover`, `membership`, `join_token`, etc.)
- actor (`cli`, `ui`, `api`, `system`)
- target node (when relevant)
- term and leader metadata
- RFC3339 timestamp

## Not yet done

- Raft-backed commit/apply path for all store mutations.
- Raft snapshots/compaction tied to SQLite state-machine durability.
- Cross-node deterministic replay validation for mutating APIs.

Track progress in `docs/persistence-plan.md`.
