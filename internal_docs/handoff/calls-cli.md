# CLI track: judgement calls (spot-check fixes P1-7, P1-9 logs, P2-1, P2-2, P2-3, P2-4, P2-6 log, P2-7 log sink)

Entries are ranked by project impact, most impactful first; the number is the
rank. Format: internal_docs/calls/README.md.

## 1. Every API message serializes camelCase, set once in build.rs

The finding was "instance JSON is snake_case (`current_alloc_id`) while
hand-built envelopes are camelCase (`instanceIds`)". The fix is one line in
`build.rs`: `message_attribute(".", "#[serde(rename_all = \"camelCase\")]")`,
so every prost message (all `--json` output that prints an API reply, and
every MCP tool result) emits camelCase. The few hand-built envelopes that were
snake_case were renamed to match (`instance fork|start|suspend -j`:
`pauseMs`, `checkpointState`, `resumedFrom`, `restoreMs`, `coldBoot`,
`durationMs`; `net handler test -j`: `latencyMs`; MCP exec tools: `exitCode`).

```console
$ legion instance get user-42 -j | jq .data.instance.currentAllocId   # was .current_alloc_id
$ legion job logs web -j | jq '.data[0].removalCause'
```

- **Alternatives.** (a) Make the hand-built envelopes snake_case to match the
  proto (fewer lines touched, but the jobspec and every documented envelope —
  `allocationId`, `latestId`, `readOnlySafe` — are camelCase, so that would
  break more of the published surface); (b) per-struct `rename_all` only on
  the types the finding named (leaves the inconsistency everywhere else);
  (c) convert keys at print time in `output::print_success` (mangles data
  maps: node attributes, env var names, status-count maps).
- **Trade-off.** A large, silent JSON change: every script that reads a
  snake_case key from `-j` or MCP output now gets `null`. MCP tool *arguments*
  stay snake_case (`allocation_id`, `idempotency_key`), so an MCP call's input
  and output use different conventions. gRPC (and the UI over gRPC-Web) is
  unaffected. Any JSON written by serde from a proto type before this change
  (idempotency replies, 24h) no longer parses; no installed base.
- **Why.** One rule ("every key Legion emits is camelCase") is the simplest
  thing an agent can rely on, and matches the jobspec. AGENTS.md says break
  surfaces for a cleaner one. Enums, oneof variants, and map keys are left
  alone, so data is never renamed.

## 2. Removed allocations: a 24h manager-side history plus worker-side retention

`legion job logs` used to resolve a job to its live allocation rows only.
Now every removal path records the allocation (job, job config, task name,
node, status, stop cause, time) in a new scheduler-lane table
`allocation_history`, written in the same transaction as the DELETE (from the
`record_stop_notices_with_preemptor_tx` choke point every removal already
calls) and expired there after `ALLOCATION_HISTORY_RETENTION_SECS` (24h).
`GetAllocationLogs` serves them: by id, or for a job with no live allocation
(its newest 10 removed ones), or with `include_removed`. The worker keeps a
removed allocation's log directory until it has been idle for
`LEGION_LOG_RETENTION_DAYS` (default 7), then deletes it (checked every 5
minutes; live allocations' dirs never).

```console
$ legion job logs web
== allocation 31 (task web) on w-1 [removed, stop at 2026-10-12T01:49:13Z] ==
$ legion job logs web --include-removed
```

- **Alternatives.** (a) Keep terminal allocation rows in `allocation` with a
  status (Nomad's model, GC later): every scheduler/capacity query would have
  to filter them, a large change to the scheduler track's code; (b) ask every
  worker "which log dirs do you have for job X" (new worker RPC, fan-out to
  the whole fleet per request); (c) `--node` + `--allocation` passthrough with
  no manager memory (the user has to know the node, which is exactly what
  they lost); (d) reuse `allocation_stop_notice` with a longer retention (it
  has no job or task, and its semantics are "deliver to the worker").
- **Trade-off.** One more replicated table and one INSERT per removed
  allocation on the scheduler lane (plus one indexed DELETE for expiry); a
  churny cluster holds a day of rows. Two retentions (manager 24h, worker 7d)
  that a user has to understand: past 24h the files exist but `job logs` can
  no longer find them. Implicit fallback (no live allocations → show removed)
  is a small piece of magic, documented and marked in the header.
- **Why.** Simplicity for the user: the obvious command works for the case
  that matters most (the thing failed and is gone), with no new concept in
  the scheduler. Performance: one insert per removal, no fleet fan-out.

## 3. Allocation ids: carry `sqlite_sequence` through lane snapshots; key nothing differently

`allocation` already had AUTOINCREMENT (it landed after the spot-check base),
so deletes and restarts no longer reuse ids. The remaining path was the
`lanes` topology's table-scoped snapshot: it copies rows, so an installing
replica re-seeds its sequence to the largest *surviving* id, and after it
wins an election it can reissue an id the old leader used and deleted. The
dump now carries each dumped table's `sqlite_sequence` mark and install
raises (never lowers) the local mark. Whole-DB (`single`) snapshots
(`VACUUM INTO`) and changeset apply (explicit-id inserts) already preserve
it; both are now tested.

- **Alternatives.** A replicated id-allocator table (`id_sequence`) bumped in
  the placement transaction (independent of SQLite internals, but one more
  write on the hottest scheduler path and another table to own); keying ids
  by a UUID/ULID (unbounded ripple through every `i64` allocation id in the
  proto, worker state, and paths).
- **Trade-off.** Relies on SQLite's AUTOINCREMENT bookkeeping, and edits a
  file owned by the ha track (`src/storage/raft/state_machine.rs`, two small
  hunks plus tests).
- **Why.** Smallest correct change on the path that was actually wrong;
  zero cost on the placement path (performance).

## 4. A stale worker log dir is moved aside on an `owner.json` mismatch

Before a fresh launch the worker compares `logs/<id>/owner.json` (allocation
id, job, job config id, task config id, task name) with the allocation it is
starting; a mismatch, or output with no owner file, renames the directory to
`<id>.replaced-<unix>` and starts empty. A match (the same allocation
relaunched) keeps appending.

- **Alternatives.** Key the directory by job + allocation (`logs/<job>/<id>`;
  breaks the shipping glob and the manager's read path, and still collides
  for the same job name on a rebuilt cluster); always rotate the dir on any
  fresh launch (splits the logs of one allocation across a worker restart).
- **Trade-off.** An identical rebuilt cluster (same job name, version, task,
  task config id, and allocation id) is indistinguishable and appends; the
  limitation is documented. Two call sites in `task_manager.rs` (lifecycle
  track's file) changed to call the claim before the stderr mark is taken.
- **Why.** Within a cluster ids are no longer reused, so this only guards
  against a worker outliving its cluster's state; the owner-file check is
  cheap, never fails a launch, and keeps the data rather than deleting it.

## 5. `legion start manager` without `-c` means built-in defaults, including a built-in worker type

No `-c` no longer reads `./configs/manager.ncl`; the manager runs on
`ManagerConfig` defaults. `workerConfigPath` now defaults to unset, which
registers one built-in worker type `default` (pool `default`). A configured
directory that does not exist is a startup error naming it. `legion job run`
with no path (which read `./configs/jobs/demo.ncl`) now requires the path.

- **Alternatives.** Fall back to defaults only when the default file is
  missing (two behaviors depending on the cwd — the bug's shape); keep
  `./configs/workers/` as the worker-type default and skip it when missing
  (same cwd magic).
- **Trade-off.** Inside a repo checkout, `legion start manager` with no `-c`
  no longer picks up `configs/manager.ncl` or `configs/workers/*`; dev flows
  must pass `-c configs/manager.ncl` (AGENTS.md already does).
- **Why.** Simplicity: the README quickstart has to work from any directory,
  and "no file means defaults" is the explainable rule.

## 6. Precondition failures are a typed `StorageError::FailedPrecondition`; untyped errors keep their cause

Added `StorageError::FailedPrecondition { message }` (Display = the message),
mapped to `FAILED_PRECONDITION`. Converted: job destroy on a running job,
keyring rotate/rewrap refusals, and the fork/snapshot sites that used
`StorageError::ForeignKey` to get the right code (their messages read
"foreign key constraint failed: …"). Two untyped "not found" bails became
`StorageError::NotFound`. Any remaining untyped error is still `INTERNAL` but
now carries `"{action}: {err:#}"` instead of the bare action.

```console
$ legion job destroy web
Error: failed to destroy job: Error deleting job: Job web must be stopped before it can be destroyed (it is running); run `legion job stop web` first (the request is valid, but the cluster's current state does not allow it; the message says what has to change first) [code=FailedPrecondition]
```

- **Alternatives.** Map by message text in `map_store_error` (fragile); fix
  the one destroy call site (the finding asked for the class).
- **Trade-off.** Internal messages can now reach clients verbatim (the same
  text the server already logged; logs must not hold secrets either).
- **Why.** The caller sees the reason without reading server logs (ease).

## 7. `map_store_error` logs by severity; expected lookups are debug

Every mapped store error used to be logged at ERROR, and `#[instrument(err)]`
on `get_node`/`get_job`/`destroy_job` logged every NotFound/refusal at ERROR.
That is where the fresh-start `ERROR … node not found` came from: registration
looks the joining node up first, and a new node always misses. Now
Internal/DataLoss/Unknown log at error, Unavailable at warn, everything else
(NotFound, FailedPrecondition, non-leader redirects) at debug; those three
store functions record errors at debug.

- **Alternatives.** Add a `find_node -> Option<Node>` for registration only
  (fixes the one line, leaves `legion job info typo` logging ERROR).
- **Trade-off.** Refused requests disappear from default-level server logs.
- **Why.** ERROR has to mean "the server is broken" for operators to trust it.

## 8. `kv get` on a missing key fails in the CLI; the RPC is unchanged

The CLI turns `GetKvReply { entry: None }` into a `NotFound` envelope and
exit 1. The RPC keeps returning an empty entry.

- **Alternative.** Make the RPC return NOT_FOUND (consistent for gRPC
  clients, but the UI and other callers rely on the empty reply).
- **Why.** Fixes the scripting idiom without touching the UI contract.

## 9. `--json` on every command, enforced by a tree-walking test; `exec --json` is a one-shot capture

`cli()` moved from `main.rs` to `crate::cmd::cli()` so a unit test walks the
real clap tree and fails when a runnable command lacks `--json` (exempt:
`start`, `start manager|worker`, `join`, `mcp serve`, `guest-agent`,
`guest-resume`). Added `--json` to `raft status|history|command|join-token`,
`exec`, `action list|run`, `docs task-schemas`. `exec --json` buffers output
into `{allocationId, exitCode, stdout, stderr, message}` and still exits with
the remote code; it refuses `-t`/`-i`.

- **Alternatives.** Exempt `exec`/`action run` as interactive (agents would
  keep scraping stdout); NDJSON frames for exec (streamable, but not one
  envelope).
- **Trade-off.** `ok: true` with a non-zero process exit when the remote
  command fails; output decoded as lossy UTF-8.
- **Why.** Agents get one envelope per call (ease), and the rule is enforced
  rather than remembered.

## 10. Human error lines print the code name; per-code hints shared with JSON

`friendly_status` printed tonic's prose for the code
(`[code=The system is not in a state required for the operation's execution]`);
it now prints `[code=FailedPrecondition]`, and one `hint_for(code, message)`
serves both formats. `Unavailable` distinguishes a transport failure (the
manager is unreachable) from a manager answer (a worker or leader behind it).
