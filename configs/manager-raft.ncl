# 3-manager raft cluster example (docs/raft-operations.md).
#
# With `persistence.type = "raft"` every store mutation is committed by
# quorum before it is acknowledged or visible: the leader captures the
# mutation's row-level effects (a SQLite session changeset), proposes them
# through openraft, and every manager applies the committed changeset to its
# local state DB. Losing any one of three managers loses no committed write.
#
# Bring-up order:
#   1. Start the FIRST manager with `bootstrap: true` (this file as-is).
#      It elects itself leader of a single-voter cluster and mints the
#      cluster secret under `raft.dataDir`.
#   2. Mint a join token on it:  legion raft join-token
#   3. Start each additional manager with `bootstrap: false` and
#      `joinToken = "<token>"` (see the commented block below). It enrolls
#      itself = added as a learner, log caught up, promoted to voter.
#
# Upgrading an existing single-manager cluster: just flip persistence to
# this raft block on the existing manager — a first raft start with an
# existing store.sqlite adopts that data as the initial state (see
# docs/upgrades.md "Raft persistence upgrades").
{
  server = {
    port = 4747,
    host = "0.0.0.0",
  },
  # State store dir (store.sqlite, keys/, backups/). Defaults to
  # $LEGION_DATA_DIR, else ./.legion (docs/data-directory.md).
  # storePath = "/var/lib/legion",
  workerConfigPath = "./configs/workers/",
  evalThreads = 4,
  schedulingPolicy = "bin_pack",
  defaultPriority = 50,
  artifacts = {},

  persistence = {
    type = "raft",
    raft = {
      # Stable manager identity inside the raft cluster. Must be unique per
      # manager and must never change once the node has joined (the raft
      # member id is derived from it).
      nodeId = "mgr-a",

      # FIRST manager only: bootstrap a brand-new cluster with itself as
      # sole voter. Every other manager joins with a token instead.
      bootstrap = true,

      # 'host:port' peers dial for this manager's raft transport (it shares
      # the manager gRPC listener). Required whenever `server.host` is a
      # wildcard bind — peers cannot dial 0.0.0.0.
      advertiseAddr = "10.0.0.1:4747",

      # Known peers, 'name=host:port'. Informational for `raft status` and
      # used to resolve bare names in operator commands (`raft command
      # add-voter --target mgr-b`); actual membership is driven by
      # join tokens / operator commands, not this list.
      peers = [
        "mgr-a=10.0.0.1:4747",
        "mgr-b=10.0.0.2:4747",
        "mgr-c=10.0.0.3:4747",
      ],

      # How many voting managers this cluster is SUPPOSED to have (1, 3,
      # or 5). Optional and purely ergonomic: `legion raft status` warns
      # when fewer voters are present, config load warns when it is even
      # ("2 voters need both alive to commit — this cluster survives zero
      # failures"). Membership is still driven by joins/commands.
      expectedVoters = 3,

      # Raft log + cluster secret live here (state DB stays in storePath).
      # Defaults to <data dir>/raft (docs/data-directory.md).
      # dataDir = "/var/lib/legion/raft",

      # 'single' (default): one raft group carries all state — the
      # recommended topology. 'lanes': three independent raft groups
      # (control / scheduler / status), each with its own log, elections,
      # and per-lane table-scoped snapshots — heartbeat storms cannot
      # stall control-plane commits and vice versa. Every table is written
      # by exactly one lane (enforced at capture time), which is what
      # makes per-lane snapshots sound. NOTE: the topology is fixed at the
      # cluster's first start — there is no in-place migration between
      # 'single' and 'lanes'; pick one per cluster (see the commented
      # example below and docs/multi-raft.md).
      topology = "single",

      # ── lanes topology example ──
      # Same config otherwise; every manager in the cluster must use the
      # same topology. One join token still enrolls a joining manager on
      # ALL three lanes, `legion raft status` reports each lane's
      # role/leader/indexes, and `snapshotEntries` applies per lane.
      # topology = "lanes",

      # Lifetime of tokens minted by `legion raft join-token`.
      joinTokenTtlSeconds = 900,
    },
  },

  # ── Joining managers (mgr-b / mgr-c) use this persistence block instead:
  # persistence = {
  #   type = "raft",
  #   raft = {
  #     nodeId = "mgr-b",
  #     bootstrap = false,
  #     advertiseAddr = "10.0.0.2:4747",
  #     # From `legion raft join-token` on any running member. Carries the
  #     # cluster secret and the leader's address; single-use is not
  #     # enforced, so treat the token itself as a secret. Only consulted
  #     # on FIRST start — restarts rejoin from their raft log.
  #     joinToken = "lgnraft_...",
  #     peers = [
  #       "mgr-a=10.0.0.1:4747",
  #       "mgr-b=10.0.0.2:4747",
  #       "mgr-c=10.0.0.3:4747",
  #     ],
  #     dataDir = ".legion/raft",
  #   },
  # },
}
