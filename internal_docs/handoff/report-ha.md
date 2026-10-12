# HA track report (saved by coordinator from the agent's hand-back)

| Finding | Status |
|---|---|
| P0-5a ForwardLaneWrite lane mismatch | Fixed + test |
| P0-5 WorkerRegister on follower (TODO note correct) | Fixed via P2-5 layer + test |
| P0-5b Workers know one manager | Fixed + tests |
| P0-6 Recovered nodes stay unavailable | Fixed + tests |
| P0-8 Restarted worker can't rejoin after join-token TTL | Fixed both sides + tests |
| P1-5 Orphans after re-join | Partly fixed; root cause not reproducible here |
| P2-5 Writes to follower fail | Fixed + test |
| P2-2 transfer-leader | Implemented (+ step-down) + test |
| P2-10 Raft terms climb | Restart case fixed; test fails w/o fix |
| --run-worker advertises 127.0.0.1 | Fixed + unit test |
| clippy heartbeat_tick | Fixed |

P0-5a: sender put physical group in group_id; receiver compared to logical lane -> every forwarded status-lane write refused. Both now use raft::physical_group / RaftTransportRegistry::group_for. Wire-compatible.
P2-5 + WorkerRegister: src/manager/leader_forward.rs layer innermost on manager listener; unary Manager RPC on follower returning FAILED_PRECONDITION with x-legion-raft-leader metadata (attached by map_store_error) replayed whole to leader; one hop (x-legion-forwarded-by); ExecAllocation excluded (test keeps list synced with proto); conn headers stripped; unreachable leader -> original rejection + what was tried; message says "raft advertise address".
P0-5b: HeartbeatReply.manager_addresses (11), WorkerRegisterReply.manager_addresses (5) from raft membership (empty outside raft). src/worker/managers.rs: set incl token address, persisted <state dir>/managers.json; one active manager; move after 2 consecutive unreachable heartbeats, wrap; all clients follow; restart starts from persisted set; artifact/node-identity credential source dials active manager.
P0-6: re-registration kept `unavailable` -> now maps to available (keeps drained); follower repair heartbeat hit P0-5a; loss/recovery two replicated writes -> half-applied on leadership change. Store::reconcile_node_availability every status-leader tick: active+unavailable -> available only if heard since takeover; lost+available withdrawn; legion_nodes_recovered_total{path="reconcile"}. Also: loss no longer overwrites operator's drained.
P0-8: worker persists node token <state dir>/node_token (0600, never logged); restart pre-registers + registers with it, no join secret; refused -> drop and fall back to join secret; manager doesn't mint/rotate on self-token re-registration.
P2-2: transfer-leader --target X and step-down on remote elections: RaftTransport.TriggerElection (cluster-secret auth, no ACL row), re-asked per election timeout, asking manager stops campaigning; deadline -> warning not error; unknown/learner target INVALID_ARGUMENT; step-down on follower forwarded; handler on blocking pool.
P2-10: restarted member campaigns if no leader reaches it within lease + election timeout; listener bound after raft start -> higher term deposes healthy leader. Fix: restarted voters start elections off (also skips 15s/lane leader wait), on when AppendEntries arrives or after 5x electionTimeoutMaxMs (>=3s). Partitioned member still disrupts (needs pre-vote; openraft 0.9 lacks). Documented.
P1-5: restore forgot a task whose tracked pid died while children held ports -> now SIGKILLs the rest of the process group (pid 0/1 refused). Other path: state dir ./.legion relative to cwd -> re-join from elsewhere = new node; worker logs state dir at startup; docs give KillMode=process, fixed LEGION_DATA_DIR, --name. No systemd unit ships.
--run-worker: in-process worker advertises host of manager's routable server.advertiseAddr else raft advertiseAddr; LEGION_WORKER_ADVERTISE_ADDR wins.

Tests: tests/raft_follower_writes.rs (new, 4): follower forwards lost node's recovery; registration + cordon via follower, reply lists 3 managers; transfer-leader moves leadership, unknown target INVALID_ARGUMENT; restarted follower with delayed listener doesn't change term (control w/o quiet start: restarted node took leadership). tests/node_liveness.rs 18/18 (4 new P0-6). tests/acl_endpoints.rs 27/27 (join flow extended; re-registering with node token no longer rotates). Lib: 126 in modules + new (managers failover, client built before failover reaches new manager, restarted worker rejoins real ACL manager incl 0600/fallback/both-dead, state token/managers file/orphan group, leader_forward, run-worker advertise helper, re-registration after loss, liveness observation). NOT RUN: raft_lanes and rest of suite.

Docs: raft-operations.md (Writes on any manager, Moving leadership, quiet restart, worker failover), multi-raft.md, architecture-managers-workers.md (managers a worker can reach, systemd, reconcile, --run-worker advertising), node-liveness.md (+ corrected single-topology digest claim), auth-and-acls.md (short join-token TTLs safe), metrics.md, upgrades.md, state-store.md, autoscaling.md.

TODO candidates:
- Static-port collision: worker stops unwanted alloc and starts replacement in same tick.
- Pre-vote for partitioned members.
- Workers fail over using raft advertise addresses (must be reachable from workers); consider serving server.advertiseAddr instead.
- legion_leader_forwards_total metric.
- ui/src/gen not regenerated.

Files: src/storage/raft/{mod,server,driver}.rs, src/storage/{store,liveness}.rs, src/manager/{server,daemon,mod,telemetry}.rs, src/manager/leader_forward.rs (new), src/worker/{daemon,state,mod,artifact_credentials}.rs, src/worker/managers.rs (new), src/worker/task_manager.rs (one call), src/cmd/raft.rs (help), proto, tests/raft_follower_writes.rs (new), tests/node_liveness.rs, tests/acl_endpoints.rs, docs.
