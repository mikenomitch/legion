# Judgement calls: ha track (spot-check P0-5, P0-6, P0-8, P1-5, P2-2 transfer-leader, P2-5, P2-10)

Ordered by project impact, most impactful first; the entry number is the rank.

## 1. A follower replays a rejected write to the leader itself; it does not pre-route writes, and the CLI does not chase leaders

Any unary Manager RPC that a follower rejects with the typed not-the-leader
error is replayed, verbatim (headers, bearer token, idempotency key, deadline,
body), to the leader named in the rejection, and the leader's reply is
returned. A tower layer (`src/manager/leader_forward.rs`) buffers the request,
runs it locally, and forwards only when the reply is `FAILED_PRECONDITION`
carrying the `x-legion-raft-leader` metadata that `map_store_error` now
attaches. One hop (`x-legion-forwarded-by`), never `ExecAllocation` (the one
client-streaming RPC; a unit test checks the list against the proto), never
the RaftTransport service.

```console
$ LEGION_ADDR=http://follower:4747 legion job run web.ncl     # now just works
# only when the leader is unreachable from the follower:
Error setting node availability: this manager is not the raft leader; its leader is mgr-a
(raft advertise address 172.31.90.175:4747) (this manager tried to forward the request to
the leader at 172.31.90.175:4747, its raft advertise address, and failed: ...)
```

- **Alternatives.** (a) Pre-route by method: keep a list of write RPCs and proxy
  them before running anything locally. (b) Make the CLI retry against the hint
  (P2-5's fallback suggestion), and reword the hint. (c) Forward per store op
  (extend `ForwardLaneWrite` to every client write).
- **Trade-off.** Every write on a follower runs its handler up to the first
  rejected proposal, then runs again on the leader: one wasted capture-free
  pass plus a manager-to-manager hop. A handler that performed an external
  side effect (a worker-plane call) before its first store write would repeat
  it; none of the write paths reviewed do, and a client retry against the
  leader has the same property. Under `lanes`, a composite whose first half
  committed on a lane this manager leads is replayed whole, which is the
  documented converging client-retry shape. The forward is plaintext h2c
  between managers, like the raft transport.
- **Why.** Simplicity/ease: one manager address works for every caller —
  CLI, UI, agents (MCP), and workers (`WorkerRegister` on a follower is fixed
  by the same layer, closing the TODO item). (a) needs a write list that drifts
  as RPCs are added and would forward reads that happen to call a writer;
  rejection-driven forwarding needs no list because the store already knows
  exactly when a write cannot run here. (b) still fails from a laptop that
  cannot reach the leader's private advertise address, which was the actual
  complaint. (c) is a closed enum per op — fine for the worker plane's dozen
  writes, unmaintainable for the whole API.

## 2. Workers learn every manager from raft membership on each reply and fail over one manager at a time

`WorkerRegisterReply.manager_addresses` and `HeartbeatReply.manager_addresses`
(new proto fields) carry every raft member's `advertiseAddr` with the
answering manager's API scheme. The worker persists the set
(`managers.json` beside its state), always keeps its token's address, talks
to one active manager, and after two consecutive unreachable heartbeats moves
to the next, wrapping around. The switch happens under one tonic balance
`Channel` whose endpoint set always holds exactly the active manager, so every
`ManagerClient` already handed out follows it.

- **Alternatives.** (a) Load-balance across all managers (tonic
  `balance_list`). (b) An operator-supplied manager list (worker flag, or a
  multi-address join token). (c) DNS / a load balancer in front of managers.
- **Trade-off.** The addresses are raft advertise addresses; a deployment
  whose managers peer on a network workers cannot reach gets no failover
  benefit (the worker tries each, then keeps its token's address). A failover
  costs two failed beats plus up to a 5s connect timeout (or 30s request
  timeout for a hung manager), inside the leader's 30s peer-digest grace for
  stopped managers. `HeartbeatReply` grows by a few addresses per beat.
- **Why.** Simplicity: zero new config, and it self-updates as managers join
  or leave. (a) spreads every worker's beats across a dead manager too: tonic
  keeps a failed lazy endpoint in the set and its calls keep failing, so a
  third of heartbeats would fail while one manager is down. (b) is a new knob
  to keep in sync with membership. (c) is a dependency Legion's "one binary"
  story avoids.

## 3. A node-token re-registration keeps the token instead of rotating it

`WorkerRegister` authenticated by the node's own node token (no join secret)
returns no `node_token`, meaning "keep the one you presented". A join-token or
management registration still mints and rotates.

- **Alternative.** Keep rotating on every registration and have the worker
  persist each new token.
- **Trade-off.** A leaked node token is not invalidated by the node's next
  restart; revocation is by removing the node (or resetting ACLs). Behavior
  change in `tests/acl_endpoints.rs` (`re-registration rotates the node token`
  for node-token registrations is gone).
- **Why.** Simplicity and robustness: with rotation, a crash between the
  manager's commit and the worker's fsync leaves the worker holding a revoked
  token and (with the join token expired) unable to ever rejoin — exactly the
  P0-8 failure, rarer. Rotation on self-presented credentials gains nothing:
  the caller already proved possession, and an attacker holding the token can
  rotate it too.

## 4. The availability reconcile runs every tick on the liveness owner, on evidence

The status-lane leader's tick returns `active` + `unavailable` nodes to
`available` only when it has heard from them since taking responsibility (a
beat or a peer digest, not the acquisition seed), and withdraws `lost` +
`available` nodes it has not heard from. It reads the scheduler's read model
and never touches `drained`.

- **Alternatives.** (a) Reconcile once, on leadership acquisition. (b) Only
  fix the root causes (re-registration keeping `unavailable`; follower
  forwarding) and add nothing.
- **Trade-off.** One read-model scan per tick (O(nodes) in memory) on the
  leader, forever, for a state that should be rare once the root causes are
  fixed.
- **Why.** Liveness is in memory and only transitions replicate, so any lost
  half-write (a leadership change between the two lanes' writes) is otherwise
  permanent and invisible ("No nodes available" with every node `active`).
  (a) misses disagreements created after acquisition; (b) leaves the next
  half-write bug as silent as this one. Evidence-gating keeps the pass from
  returning a dead node to the pool on a stale row.

## 5. Liveness never overwrites `drained`

`sync_node_availability` (the liveness half of loss and recovery) now updates
only rows that are not `drained`. A cordoned node that goes lost stays
`drained`, so it is still cordoned when it returns.

- **Alternative.** Keep loss writing `unavailable` over anything, and teach
  recovery to remember the prior value.
- **Trade-off.** A drained-and-lost node reads `lost` / `drained` rather than
  `lost` / `unavailable` in `legion node list`. Placement is identical (only
  `available` places).
- **Why.** Before, loss rewrote `drained` to `unavailable` and recovery then
  rewrote that to `available`: an outage silently undid an operator's cordon.
  The owner-of-the-value rule (liveness owns `available`/`unavailable`,
  operators own `drained`) is one sentence and needs no extra column.

## 6. Re-registration turns `unavailable` into `available`

`WorkerRegister` keeps an existing `drained`, but maps `unavailable` (set by
node loss) to `available`.

- **Alternative.** Keep the stored value and rely on the next heartbeat's
  transition (which was the design).
- **Trade-off.** None found: a registering worker is alive by definition.
- **Why.** The heartbeat repair only works where the transition write can
  run; on a follower in `single` topology it could not (P0-5's lane-mismatch
  bug), so the node stayed `active` and unschedulable, which is what the
  spot check observed. The registration log line ("it is schedulable again")
  was also false before.

## 7. A restarted raft member starts quiet instead of implementing pre-vote

A restarted voter in a multi-voter group starts with openraft's `elect`
switch off, and turns it on when an AppendEntries reaches its transport or
after a quiet window (5 x `electionTimeoutMaxMs`, at least 3s). It also skips
the up-to-15s leader wait at engine start.

- **Alternatives.** (a) Implement pre-vote in Legion's network layer. (b)
  Upgrade openraft. (c) Raise the election timings.
- **Trade-off.** A whole-cluster restart elects up to one quiet window later.
  A member cut off by a partition (not restarted) still campaigns and still
  deposes the leader when the partition heals; documented.
- **Why.** Code reading plus a control run showed the restart case: a
  restarted member waits one leader lease plus its election timeout for a
  leader to reach it, but the manager binds its listener only after raft
  starts, so a slow start used up that window; the member then campaigned
  repeatedly while unreachable, and its higher term deposed the healthy
  leader (`a_restarted_follower_does_not_depose_the_leader` fails without the
  fix and passes with it). That matches "term 10 after two leader stops".
  (a) is a
  protocol change inside openraft's vote handling; (b) is a large upgrade;
  (c) does not address the restart case at all. Performance: also removes up
  to 15s per lane from a restarted member's startup.

## 8. `transfer-leader` and `step-down` are built on remote elections

`transfer-leader --target X` asks X (new `RaftTransport.TriggerElection`,
cluster-secret auth, no Manager ACL row) to campaign, re-asking once per
election timeout + 250ms until X leads the control group or 10 election
timeouts (min 10s) pass, while the asking manager turns its own `elect` off.
`step-down` picks the voter with the furthest replication progress and runs
on the leader (a follower forwards it there via entry 1).

- **Alternatives.** (a) Return `FailedPrecondition`/`Unimplemented` with an
  actionable message (the minimum the finding allowed). (b) Have the leader
  stop heartbeating to let leases expire.
- **Trade-off.** A transfer costs a few terms and is best-effort under load:
  the command reports a warning (not an error) when leadership has not moved
  by the deadline. Disabling `elect` on the asking manager is a no-op unless
  it is the incumbent.
- **Why.** Simplicity/ease: decommissioning the leader and planned
  maintenance are common operator tasks, and "run trigger-election on the
  other host" was the workaround the spot check needed. (b) stalls writes for
  a full lease and does not choose the successor.

## 9. Kill an orphaned process group at restore; document `KillMode` rather than ship a unit

When a restored allocation's tracked pid is dead but its process group still
has members, the worker SIGKILLs the group (`kill(-pgid)`; pid 0/1 refused).
`docs/architecture-managers-workers.md` documents `KillMode=process`, a fixed
`LEGION_DATA_DIR`, and `--name`.

- **Alternatives.** (a) Ship `legion-worker.service` with
  `KillMode=control-group` (tasks die with the worker). (b) Adopt orphaned
  children as the task.
- **Trade-off.** P1-5's root cause is not confirmed from the rig; the fix
  covers the two causes the code allows (dead group leader with live children;
  a different data dir / identity). A task restarted under a new identity
  still cannot see its old processes.
- **Why.** Tasks outliving the worker is a design property (re-adoption);
  (a) would quietly turn every worker restart into a full task restart. (b)
  cannot know which child is "the task". Linux never reuses a pid while it is
  a live process-group id, so killing the group touches only the task's own
  descendants.

## 10. The receiver checks the forwarded lane in group space

`ForwardLaneWrite`'s receiver now compares the envelope's `group_id` against
the group that commits the op's lane on this manager (`control` under
`single`), via the same `physical_group` helper the sender uses.

- **Alternative.** Have the sender put the logical lane in `group_id`.
- **Trade-off.** None on the wire: senders already send the physical group,
  so new receivers accept what every existing sender sends, and no
  `PROTOCOL_VERSION` bump is needed.
- **Why.** `group_id` means "raft group" on every other RaftTransport RPC;
  changing the sender would have made one RPC's field mean something else.

## 11. Failover thresholds: two failures, 5s connect, 30s request, 10s keepalive

- **Alternative.** Fail over on the first failure; no request timeout.
- **Trade-off.** Up to ~two retry intervals before moving; a 30s cap on any
  worker->manager unary call.
- **Why.** One dropped request (a manager restarting its listener) should not
  move a fleet; without the timeouts a manager whose host vanished hangs a
  heartbeat for the OS TCP timeout (minutes), far past the lost window.

## 12. `--run-worker` advertises the manager's routable host, not a new knob

The in-process worker reaches its manager over loopback, so the worker
plane's route-based detection advertised `127.0.0.1`, unreachable from other
managers. The manager now sets the worker's advertise flag to the host of
`server.advertiseAddr`, else the raft `advertiseAddr`, when that host is
routable; `LEGION_WORKER_ADVERTISE_ADDR` still wins.

- **Alternative.** A manager-config field for the co-located worker's
  advertise address, or detecting toward a peer manager.
- **Trade-off.** A manager whose only advertised addresses are loopback keeps
  the old behavior (correct for a single box).
- **Why.** Simplicity: the manager already states where it can be reached;
  reusing it needs no new setting.
