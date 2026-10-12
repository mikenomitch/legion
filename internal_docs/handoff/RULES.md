# Shared rules for every fix agent

Repo: /home/user/legion (Rust, ~220k lines). Read /home/user/legion/AGENTS.md and
CLAUDE.md first: they are binding (ACL checks on every Manager RPC, `--json` on
every CLI command, proto comments on every RPC/field, every new job-config field in
configs/jobs/maximal.ncl, public docs in docs/ for new surfaces, tracing rules).

Findings brief: /tmp/claude-0/-home-user-legion/32614872-b679-5d36-a79c-c30e02e5819c/scratchpad/findings.md
Scratch dir (write your report + calls draft here):
/tmp/claude-0/-home-user-legion/32614872-b679-5d36-a79c-c30e02e5819c/scratchpad/

## You are one of six agents editing the SAME working tree at the same time

Tracks (who owns what):
- **lifecycle**: P0-2, P0-3, P1-1, P1-4, P1-9 (misattributed pull failures), P2-7 (plugin-dir default), P2-8.
  Owns src/worker/task_manager.rs, src/worker/task_health.rs, src/worker/exec.rs, src/task_plugins/exec.rs (gated-netns part),
  the TaskAllocation proto message, and the manager code that builds TaskAllocation.
- **scheduler**: P0-4, P1-2, P1-3, P1-8. Owns src/manager/scheduler.rs + src/manager/scheduler/*, src/manager/rollout.rs,
  job-status code in src/storage/store.rs.
- **ha**: P0-5, P0-6, P0-8, P1-5, P2-5, P2-10, and the `raft command transfer-leader` half of P2-2. Owns src/storage/raft/*,
  src/storage/liveness.rs, src/manager/daemon.rs, manager-address/failover + credential persistence in src/worker/daemon.rs,
  src/cmd/raft.rs, src/cmd/join.rs.
- **containers**: P0-7, P1-6, P2-6 (memory floor check), P2-9, P2-11. Owns src/task_plugins/oci_bundle.rs, oci_image.rs,
  container.rs, sandbox.rs, docs/task-runtime-container.md, docs/task-runtime-sandbox.md, install docs.
- **workerplane**: P0-1. Owns src/worker/server.rs, worker bind/advertise in src/worker/daemon.rs, src/manager/exec.rs,
  the manager->worker client factory, src/manager/mesh_ca.rs, src/cmd/start.rs.
- **cli**: P1-7, P1-9 (logs vanish), P2-1, P2-2 (error mapping, except transfer-leader), P2-3, P2-4, P2-6 ("node not found"
  log), P2-7 (log sink maintenance noise). Owns src/cmd/* (except raft.rs/join.rs/start.rs), src/worker/logs.rs, README.md.

Shared hot files (src/manager/server.rs, src/storage/store.rs, src/worker/daemon.rs, proto/legion.proto,
src/structs/*): several agents edit these. Rules:
1. Make small, surgical Edit calls. Never rewrite or reformat a whole shared file. Never revert or "clean up"
   code you did not write in this session — another agent is mid-edit.
2. If an Edit fails because the file changed since you read it, re-Read the region and retry.
3. Format ONLY your own files with `rustfmt --edition 2021 <file>`; for a shared file, do not run rustfmt on it at all
   (just write well-formatted code by hand). Never run `cargo fmt --all`.
4. Do NOT commit, stash, reset, checkout, or otherwise touch git state. The coordinator commits at the end.
5. Do not edit internal_docs/TODO.md or internal_docs/README.md (the coordinator does). You MAY add/edit public docs in
   docs/ for your area, and proto comments.

## Building and testing

- One shared target dir; cargo serializes builds on a lock ("Blocking waiting for file lock") — that wait is normal.
- Iterate with `cargo check --lib --locked` (or `cargo check --tests --locked`). Verify with targeted tests:
  `cargo test --locked --lib <module_or_test_filter>` and `cargo test --locked --test <file>`. Do NOT run the whole
  suite (`cargo test --all`) — the coordinator does that once at the end. Do not run benches.
- If a build fails ONLY in files owned by another track, that agent is mid-edit: wait (poll with a short until-loop via
  the Monitor tool or a background command, NOT a foreground `sleep`) and retry. Don't fix their files. If it is still
  broken after ~10 minutes, note it in your report and continue with what you can verify.
- Write a regression test for every bug you fix wherever practical (unit test near the code, or an integration test
  in tests/). Name tests descriptively.
- Clippy must stay clean on your code: `cargo clippy --locked --lib -- -D warnings` (shared lock; run near the end).

## What "done" means for you

1. Code fixed, with tests.
2. Public docs updated for any behavior/default/surface change (docs/*.md in your area), proto comments complete,
   new job-config fields in configs/jobs/maximal.ncl.
3. Write `<scratch>/report-<track>.md`: per finding — status (fixed / partly fixed / deferred), root cause in one or
   two sentences, the fix, files changed, tests added and their results, and anything deferred or newly discovered
   (these become TODO items).
4. Write `<scratch>/calls-<track>.md`: judgement calls in the format of internal_docs/calls/README.md (the call, the
   alternatives, the trade-off, why it won, user-facing surface), most impactful first.
5. Final message: a short summary pointing at the report.

Repo conventions reminder: no compatibility shims or aliases for old forms (AGENTS.md "Upgrades and Breaking Changes");
pick the right default for a new cluster. Never log secrets/tokens. Don't add model names to any file.
