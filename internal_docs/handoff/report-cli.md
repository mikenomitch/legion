# CLI track report (saved by coordinator from the agent's hand-back)

All eight findings fixed with regression tests (17 new). Targeted: 138 lib tests (cmd::, logs, raft snapshot, store, server) + --test acl_endpoints (27) pass.

P1-7: on main the AUTOINCREMENT rebuild already covered deletes/restarts/whole-DB snapshots/changesets (spot-check base af0b57a predates commit 7d84e8c). Remaining path: `lanes` table-scoped snapshot copied rows only -> installing replica re-seeds id counter to max surviving id -> after election could reissue a deleted id. Fix: state_machine.rs carries each dumped table's sqlite_sequence; install only raises. Worker: logs.rs claim_allocation_log_dir writes logs/<id>/owner.json; dir owned by another allocation (or output w/o owner file) renamed to <id>.replaced-<unix>. Called from both launch sites in task_manager.rs. Tests: snapshot_install_never_reissues_a_deleted_allocation_id, a_new_leader_never_reissues_a_deleted_allocation_id, allocation_ids_are_never_reused_after_removal_or_restart, 3 worker::logs tests. Limitation: rebuilt cluster with identical job/version/task/alloc id appends.

P1-9b: new table allocation_history (scheduler lane, migration appended, ownership.rs line); written in the shared stop-notice fn in the same tx as DELETE; 24h retention (ALLOCATION_HISTORY_RETENTION_SECS). GetAllocationLogs finds removed allocs by id; job w/o live allocs -> newest 10 removed; include_removed (CLI --include-removed, MCP arg); destroyed jobs too. Coarse job:read check before lookup. Proto fields include_removed, removed, removal_cause, removed_at_unix, status. Worker deletes inactive alloc log dirs idle > LEGION_LOG_RETENTION_DAYS (default 7) from maintain_logs. Verified live.

P2-1: start manager w/o -c uses built-in defaults; workerConfigPath default unset -> built-in `default` worker type; configured nonexistent dir = startup error. `job run` requires a spec path (was silently ./configs/jobs/demo.ncl). README job status -> job info. Walked every quickstart command from an empty dir.

P2-2: StorageError::FailedPrecondition; job destroy on running job -> FailedPrecondition w/ reason + `legion job stop`; keyring refusals; fork/snapshot sites that faked it with ForeignKey. Untyped errors still INTERNAL but carry cause. CLI per-code hint table for JSON+human; Unavailable distinguishes manager unreachable vs worker/leader behind it; human errors end in [code=...]. Worker relay covered by workerplane's WorkerTarget::explain.

P2-3: kv get missing -> NotFound envelope, exit 1 in both formats; RPC unchanged (UI contract). No MCP KV tool.

P2-4: cli() moved into lib; test every_command_accepts_json walks the command tree (daemons, mcp serve, guest helpers exempt). Added --json to raft status|history|command|join-token, exec, action list|run, docs task-schemas; boot-ticket and sandbox groups require a subcommand. exec --json returns {allocationId, exitCode, stdout, stderr, message}. build.rs adds rename_all="camelCase" to every proto message; renamed snake_case hand-built keys, MCP tool descriptions, tests/apple_container_idle.rs reads. Catalog: exec.run, action.list|run, raft.*; job.logs, kv.get updated. BREAKING: every snake_case key in CLI -j output and MCP results is now camelCase; MCP tool args unchanged.

P2-6b: not a race: registration's get_node lookup misses for a new node and #[instrument(err)] logged ERROR. get_node/get_job/destroy_job record errors at debug; map_store_error logs by severity (internal=error, unavailable=warn, rest=debug). Tracing-capture test.

P2-7b: root cause: alloc registered from start of launch (pull/staging/claim) but log dir created only at task start -> 10s sweep read_dir ENOENT. Claim creates dir before launch; missing dir = nothing to do.

Docs: logging.md (Logs of removed allocations, owner.json, retention), agent-readiness.md (§2 rewritten, §6a, MCP table), kv-store.md, state-store.md, configuration-language.md, exec-and-actions.md, JSON examples in suspend-and-resume.md, node-liveness.md, worker-simulation.md.

Shared-file edits: manager/server.rs (map_store_error, get_allocation_logs, tests), storage/store.rs, storage/store/{fork,snapshot,keyring}_ops.rs, storage/errors.rs, migrations.rs, constants.rs, proto; ha: raft/{state_machine,ownership}.rs, manager/daemon.rs, cmd/raft.rs; workerplane: cmd/start.rs; lifecycle: worker/task_manager.rs.

TODO candidates:
- Manager forgets removed allocs after 24h, workers keep files 7 days: add --node override or configurable retention.
- UI Logs tab never sets include_removed.
- CLI #[instrument(err)] on command handlers prints each human-mode error twice as ERROR trace lines before `Error:`.
- Event metadata keys still mixed case (camelCase rule covers API messages + CLI envelopes only).
- https CA default configs/tls/rootca.pem still relative to cwd.
- job info prints only the job row.
