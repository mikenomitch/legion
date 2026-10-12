## Summary

Fixes every finding from the 2026-10-11 pre-launch spot check: three raft managers with ACLs on and two spot workers on AWS, plus a local macOS cluster. Single-node demos mostly worked there; the multi-node story did not. This PR fixes the multi-node story.

- **The report:** `internal_docs/spot-check-2026-10-11-findings.md`.
- **The plan:** `internal_docs/plans/launch-spot-check-fixes-plan.md`. §8 gives the root cause each fix found, which in several cases differs from the report's reading, and the outcome for each finding. §8.7 lists what was deferred.
- **Judgement calls:** `internal_docs/calls/launch-spot-check-fixes-plan_calls.md` (52 entries, ranked). The worker-plane calls are in `worker-plane-auth-plan_calls.md`, entries 10, 11, 15 and 20–25.
- **TODO.md:** shipped work is checked off under "Pre-launch spot check (2026-10-11)". Deferred work is open under "Follow-ups from the 2026-10-11 spot-check fixes".

| Finding | Outcome |
| --- | --- |
| **P0-1** workers on other hosts lose exec, logs, discovery, mesh | Phase 1 of `worker-plane-auth-plan.md`:<br>• The worker binds `0.0.0.0:4848` and advertises a reachable `https://` address: auto-detected, or set with `--advertise-addr` / `LEGION_WORKER_ADVERTISE_ADDR`.<br>• The listener is mutual TLS with per-RPC caller classes. Only a manager certificate can call `PostWork`, `ExecTask`, `GetTaskLogs` or `RestartWorker`.<br>• One manager-side client factory verifies `<node>.node.legion`.<br>• Node keys are issued by CSR and never leave the node. |
| **P0-2** batch never finishes, restart ignored | `TaskAllocation` now carries `task_lifecycle` and `task_restart`. A batch task that exits 0 completes, and restart policies are honored. Includes an end-to-end test. |
| **P0-3** health checks never start with artifacts | Staging threads enter the tokio runtime. Edge boot's provisional launch had the same bug, also fixed. |
| **P0-4** DAG deps not enforced | Only a status the worker reported satisfies `complete` or `success`. Dependents are held until then, and a terminal report queues the job's next eval. Finished steps release their capacity. |
| **P0-5** one manager down takes workers offline | • Forwarded heartbeats are checked against the physical lane, fixing `single`.<br>• Workers learn every manager address from registration and heartbeat replies, persist it, and fail over.<br>• Followers forward rejected writes to the leader, which also fixes `WorkerRegister` on a follower. |
| **P0-6** recovered nodes stay unavailable | Three causes, all fixed. Plus an availability reconcile on the status leader, and loss no longer overwrites an operator's `drained`. |
| **P0-8** worker can't rejoin after token TTL | The worker persists its node token (`0600`) and re-registers with it. The manager does not rotate a token a node presents for itself. |
| **P0-7** stock images fail on Linux | • Docker's default capabilities minus `NET_RAW`, adjustable through `security.capabilities`.<br>• Adds are bounded by the worker type's `container.allowCaps`, which defaults to that same set.<br>• Layer ownership, setuid bits and file capabilities are kept.<br>• The stdio logs are chowned to the container user.<br>• `nofile` defaults to 1048576, with a `ulimits` override.<br>• Verified with stock nginx and the Postgres entrypoint under real runc. |
| **P1-1** resource enforcement never on | The governor gets its config explicitly, before any allocation starts. |
| **P1-2** static ports deadlock | • The planner tracks ports per node, so in-plan port claims are counted.<br>• A plan places what fits and reports the rest as unplaced, and freed capacity retries them.<br>• A static-port rollout that can't surge stops the old allocation first.<br>• No more hot requeue loops. |
| **P1-3** reschedule excludes the only node | A two-pass plan tries other nodes first, then the failed node. The stateful message no longer tells users to delete data on a healthy node. |
| **P1-4** gated netns race | Allocations pushed by `PostWork` now create their own namespace. Before, only the heartbeat did. |
| **P1-5** orphans on re-join | Partly fixed; the root cause was not reproducible here. Restore now kills a task's leftover process group, and the systemd guidance (`KillMode`, `LEGION_DATA_DIR`, `--name`) is documented. |
| **P1-6** macOS sandbox `--` | Already fixed on main; a regression test is added. |
| **P1-7** allocation ids reused | The `lanes` table-scoped snapshot now carries `sqlite_sequence`. The worker moves aside a log dir that belongs to a different allocation. |
| **P1-8** job status misleading | Status is derived from live state (`running` / `pending` / `complete` / `failed` / `stopped`). Re-running an unchanged spec restarts a stopped, failed or idle job. |
| **P1-9** real failure hard to find | Failure reasons and events carry the task's last stderr lines. Logs of removed allocations stay readable for 24h (`--include-removed`). |
| **P2-1 … P2-11** | • The quickstart works outside a checkout.<br>• Precondition and worker errors keep their cause and code.<br>• `kv get` on a missing key exits 1 with `NotFound`.<br>• Every command accepts `--json` (enforced by a test), and output is camelCase everywhere.<br>• Followers forward writes; `transfer-leader` and `step-down` work.<br>• macOS 200 MiB floor check; the "node not found" and log-sink noise are gone.<br>• Plugin dir defaults next to the binary; exec sessions get the task env.<br>• Per-blob image cache; a restarted raft member starts quiet.<br>• New `docs/installation.md`. |

Also fixed along the way:
- **Speculative dispatch never fired for real workers:** it read `protocolVersion` from the wrong attribute map.
- **`tls-edge.ncl`** bound the wrong port on Linux.
- **`redis.ncl`** couldn't run.
- **`action run -p`** in the docs; the flag is `--param`.
- **The dev guide** pointed workers at `:4748`.
- **The UI** still special-cased the old `done` status.

## Breaking changes

Legion has no installed base, so these are deliberate (AGENTS.md):

- **Worker plane, `PROTOCOL_VERSION = 3`:**
  - `workerAddress` is now `https://`, and port 4848 binds `0.0.0.0` with mTLS.
  - `IssueNodeCertificateRequest.csr_pem` is required, and `IssueNodeCertificateReply.key_pem` is removed (field 2 reserved).
  - The simulator's `--advertise-host` is renamed `--advertise-addr`.
  - Managers must be able to reach workers on TCP 4848.
- **Restart default:** a batch task whose `restart` block omits `policy` now gets `onFailure`, not `always`.
- **`condition = "complete"`** now means "finished either way", as `task-supervision.md` already documented. `success` means exit 0.
- **Partial placement:** plans for tasks outside multi-task groups place what fits instead of failing all-or-nothing. Previews (`job plan`) and groups stay all-or-nothing.
- **Job status vocabulary:** the API returns `running`, `pending`, `complete`, `failed` or `stopped`, never `done`.
- **Unchanged re-run:** `job run` with an unchanged spec restarts a stopped, failed or idle job.
- **Container defaults:**
  - 13 capabilities instead of 3.
  - New worker-type `container.allowCaps`. A job that adds a capability fails at start unless the node allows it.
  - `nofile` defaults to 1048576.
- **JSON output:** `-j` CLI output and MCP results are camelCase throughout. MCP tool arguments are unchanged.
- **CLI exit codes and commands:**
  - `kv get` on a missing key exits 1.
  - `job run` requires a spec path.
  - `start manager` without `-c` uses built-in defaults.
  - The `boot-ticket` and `sandbox` command groups require a subcommand.
- **Node token re-registration:** re-registering with a node's own token no longer rotates it.
- **Capacity accounting:**
  - Completing an allocation bumps its node's epoch, so a racing plan conflicts once.
  - Finished batch allocations no longer count toward node usage or `maxAllocations`.

## Testing

- `cargo fmt --all -- --check` is clean.
- `cargo clippy --all --all-features --locked -- -D warnings -A dead_code -A unused_variables` is clean (CI's invocation).
- @@TESTS@@
- UI: `npm run gen` regenerated `ui/src/gen`, and `npm run build` passes.
- About 150 new regression tests across all six areas. Highlights:
  - `tests/worker_plane.rs`, including `every_worker_rpc_is_classified`.
  - `tests/raft_follower_writes.rs`: three managers in `single` mode, with follower forwarding, `transfer-leader`, and quiet restart.
  - DAG and static-port scenarios in `tests/eval_loop.rs`.
  - A batch task exiting 0 end to end, in `tests/artifact_staging_integration.rs`.
  - Four new availability tests in `tests/node_liveness.rs`.
- Container end-to-end tests (stock nginx, the Postgres entrypoint, non-root stdio) ran under real runc on the dev host. They are `#[ignore]` because they need a runtime and pre-fetched images.
- **Not run on the AWS rig.** Re-running `tests/spot-check/smoke.sh` without the shims is the first follow-up in TODO.md.

## Scheduler benchmarks (AGENTS.md)

@@BENCH@@

## Deferred (all open in TODO.md)

- **Worker plane:**
  - Two-step CA rotation.
  - The CA pin in join tokens, moved to Phase 4.
  - `Worker/Probe`.
  - The exec `ticket` class.
  - The encrypted worker-to-manager channel (Phase 4).
- **Raft:**
  - Pre-vote for partitioned members.
  - Failover uses raft advertise addresses.
- **Containers:**
  - Capability-aware placement.
  - The per-author ACL `host` grant.
  - User namespaces and seccomp.
  - `command` vs `ENTRYPOINT` differs between macOS and Linux.
- **Smaller items:**
  - Batch exit reports are not persisted across a worker restart.
  - `legion exec` into a gated exec task runs in the host netns.
  - Log retention differs between manager (24h) and worker (7 days).
- **Docs:** the release versions in `docs/installation.md` are placeholders. They need checking against the upstream release pages before launch.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01VoxqZdy3ztMQcq8nbV43Ue
