# Judgement calls: the Managers page lists raft members (spot-check P2-12)

A bug fix with no plan. The finding is P2-12 in
[launch-spot-check-findings.md](../launch-spot-check-findings.md): on a
three-voter raft cluster the UI's Managers page showed one manager, because it
listed the manager config table and only the control-lane leader writes that
table. Public docs: [raft-operations.md](../../docs/raft-operations.md#members)
and [ui-architecture.md](../../docs/ui-architecture.md).

Entries are ranked by project impact, highest first. The number is the rank.

## 1. A new `ListManagers` RPC, not a `members` field on `GetRaftStatus`

The roster comes from a new Manager RPC, `ListManagers`, backed by
`legion raft members` and the MCP tool `legion_manager_list`.

```
$ legion raft members
  NODE                     ROLE       ADDRESS                  REACHABLE     TERM    APPLIED
  mgr-1                    leader     10.0.1.11:4747           yes 2ms          4       1802
* mgr-2                    follower   10.0.1.12:4747           self             4       1802
```

**Alternative.** Add `repeated RaftMemberInfo members` to `RaftStatus` and
have the page call `GetRaftStatus`. No new RPC, ACL row, catalog entry, or
CLI verb.

**Trade-off.** One more RPC on the Manager surface, with its ACL row, catalog
entry, CLI verb, and MCP tool to keep in sync.

**Why.** `GetRaftStatus` is documented as *this manager's* status, cheap and
local, and `legion raft status` and the Raft page poll it. Folding a live
fan-out to every peer into it would make the cheap call slow whenever a peer
is down, and it would still need a non-raft answer for the single-manager
case, where `RaftStatus` is mostly empty. A separate RPC keeps each call
explainable (simplicity) and lets the roster work identically in every
persistence mode.

## 2. Reachability is a live probe from the answering manager, forwarding the caller's credential

For every other member, the answering manager calls `GetRaftStatus` on it
concurrently with a 2-second timeout, copying the caller's `authorization` or
`x-legion-token` header. A member that does not answer is listed with
`reachable = false` and the error.

**Alternatives.**

- Use only the leader's openraft replication metrics (matched index per
  follower). Free, but only the leader has them, and the page has to work
  when it points at a follower.
- Add a manager-plane `RaftTransport` ping authenticated by the cluster
  secret, like `QuerySessions`. No user credential crosses managers, but it
  is a new manager-plane RPC with a mixed-version story.
- Report no reachability at all, only membership.

**Trade-off.** Each page poll costs one RPC per other manager, and a dead
member adds up to 2 seconds to the reply. The view is the answering
manager's: a partitioned manager reports everyone else unreachable. The
caller's token is sent to peers. That is the same thing the leader-forward
layer already does for writes, and peers apply the same `operator:read`
check, but it means a caller whose token a peer cannot validate sees that
peer as unreachable (`PermissionDenied`).

**Why.** `GetRaftStatus` exists on every manager release, so the probe needs
no protocol change, and its reply carries each member's own term and applied
index, which is the useful health signal (lag, a missed election). Raft
clusters have 1, 3, or 5 managers, so the fan-out is bounded and small
(performance is not on a hot path here). Simplicity won over the cleaner
cluster-secret path.

## 3. Config rows are keyed by the raft node id, and followers still do not write one

`run_manager` now names the manager by `store.raft_node_id()` under raft
(falling back to `server.host:server.port` otherwise) for its config row and
its liveness digests. Before, every manager used `host:port`, which is
`0.0.0.0:4747` on every member of a typical cluster, so all managers shared
one config row, and the page could not join a row to a raft member.

**Alternative.** Leave the key alone and join config rows to members by
advertise address. Or also make every manager publish its config by
forwarding the write to the leader (`ForwardableLaneWrite`).

**Trade-off.** A follower that has never led still has no config snapshot, so
the Config column shows `—` and its name does not link to a config page.
Rows written before this change under the old key linger in existing stores
(no migration, per AGENTS.md "Upgrades and Breaking Changes"). The rename also
changes the observer id in liveness digests, which fixes a latent collision:
followers sharing one id overwrote each other's digest claims on the status
leader.

**Why.** The raft node id is the only identity guaranteed unique across the
cluster, and it is what `ListManagers` reports, so the join is exact.
Forwarding config writes from followers would add a new forwardable op for a
display-only feature. Membership is what the bug was about, and the page no
longer depends on config rows for it. Follower config publishing is an open
item in `internal_docs/TODO.md`.

## 4. Role names are `leader`, `follower`, `learner`, `standalone`, not `primary`/`standalone`

`ManagerMember.role` uses raft vocabulary. `RaftStatus.role` uses
`primary`/`standby`/`candidate`, which come from the persistence driver and
predate raft.

**Alternative.** Reuse the `primary`/`standby` values for consistency with
`GetRaftStatus`.

**Trade-off.** Two role vocabularies in the API. A reader comparing
`legion raft status` with `legion raft members` sees `primary` in one and
`leader` in the other.

**Why.** The request and the finding ask for "leader/follower/learner", and
those are the words an operator uses for a raft member. `standby` cannot say
"voter" versus "learner", which is the distinction the roster exists to
show. `leader` means control-lane leader, since that lane owns membership,
joins, and config, and `leader_lanes` carries per-lane detail under
`topology = "lanes"`. Voter-ness uses the same settled-on-every-lane rule as
`RaftStatus.voters`, so the two commands never disagree on who votes.

## 5. The Managers page drops search, sort, and pagination

The page was a `SharedIndex` (search, sort by update time, pages of 20). It is
now a single table of every member.

**Alternative.** Keep `SharedIndex` and feed it raft members.

**Trade-off.** No search box or keyboard index navigation on this page.

**Why.** A raft cluster has 1, 3, or 5 managers. Search and paging over five
rows is noise, and a table can show role, address, reachability, term, and
lag side by side, which `SharedIndex`'s title and subtitle rows cannot.
