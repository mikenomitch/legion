# Judgement calls: test build footprint and CI speed

No plan: this is developer tooling (Cargo profiles, test layout, CI), not a
user-facing feature. The measurements behind these calls are in
[`development.md`](../development.md#keeping-builds-and-test-runs-small).

Entries are ranked by project impact, highest first; the number is the rank.

## 1. Integration tests are modules of one binary; four files stay separate

Sixty-three of the 67 `tests/*.rs` files moved into `tests/integration/` as
modules of `main.rs`. Each file had been its own ~200 MB binary linking all of
Legion, so the merge took integration binaries from 12.9 GB to 1.1 GB and
replaced 67 links per library change with one.

```sh
# before
cargo test --test acl_endpoints
# after
cargo test --test integration acl_endpoints::
```

Four files keep their own binaries because they need their own process:
`raft_lifecycle_ops` and `raft_job_eval_ops` assert exact deltas of the
process-wide raft op counter, `adaptive_scheduler_concurrency` drives the
scheduler's global planner gate, and `session_index_scale_measure` installs a
`#[global_allocator]`.

**Alternatives.** Keep one binary per file and only cut debuginfo (the CI fix
already in place). Or adopt cargo-nextest, which runs each test in its own
process, so merged tests could not share globals at all.

**Trade-off.** Tests from different files now share a process and the thread
pool, so a test that leans on process-global state can be disturbed by a
neighbour it never met before. Every `--test <file>` command in the docs
changed shape. A filter with a trailing argument (`-- --nocapture name`) now
has to be folded into the module path, because libtest ORs multiple filters.
Editing one test file recompiles the whole integration crate (incrementally).

**Why.** Disk and link time were the problem the user hit, and the merge
addresses both directly (**performance**). Nextest would keep isolation, but
local `cargo test` would still run the merged binary in one process, and the
per-file serial gates (`static Mutex`) would stop serializing under it. Two
test runners with different semantics is worse than one rule
(**simplicity**). The globals were audited file by file: each file that
needed isolation stayed out; the rest already tolerated sharing a process
with the other tests in their own file.

The extra concurrency was not free: in the first reruns of the merged binary
two `work_stream` tests failed about one run in four. One was a test that
assumed a push and the re-send before it arrive as one message; the test now
accepts either. The other was a real race in `WorkLog::take_pending`: a
superseded stream's sender could take the resync a reconnect queued, so the
new stream never got its opening message. That is fixed in the product, with
a unit test. The race also reproduced with the neighbouring modules running
in a separate process, so it was load-sensitive, not caused by shared state.

## 2. Kept test directories go under a per-process root removed at exit

Fifty-eight test helpers called `TempDir::keep()` (or `mem::forget`) so a
returned `Store` outlived its guard, and nothing ever deleted the directories:
a full run left over 1.1 GB of SQLite stores in `$TMPDIR`. Kept directories
now go under `legion-tests-*`, one per process, which an `atexit` hook
removes.

**Alternatives.** Return the `TempDir` alongside the store (a wrapper that
derefs to `Store`), so drop order cleans up per test. Or create directories
under Cargo's `CARGO_TARGET_TMPDIR`, which `cargo clean` removes.

**Trade-off.** Cleanup happens at process exit, not per test, so a run's
stores accumulate until it ends. A run killed by a signal (Ctrl-C) skips
`atexit` and leaves its one root behind. It relies on `libc::atexit`, which
is unusual in a test helper.

**Why.** The change is one argument per call site (`tempdir()` →
`tempdir_in(root)`), where the wrapper type would have changed 58 helper
signatures and every caller that moves a `Store` by value (**simplicity**).
`CARGO_TARGET_TMPDIR` is only set for integration tests and still grows
without bound.

## 3. Only the bundled SQLite is optimized in dev builds

`[profile.dev.package.libsqlite3-sys] opt-level = 3`. Every store-backed test
runs the full migration chain through SQLite, and at `-O0` that setup was the
largest single cost in the unit-test suite.

**Alternatives.** `opt-level = 2` for every dependency. Or a migrated
template database copied per test.

**Trade-off.** One C file compiles slower. Dependencies other than SQLite
stay slow in tests; the crypto-heavy `checkpoint_seal` tests would have run
3x faster with all dependencies optimized.

**Why.** Measured: SQLite alone cut the unit suite's test-seconds by about a
quarter. All dependencies added ~10% more but took the clean dependency build
from ~4.5 to ~9.5 minutes on 4 cores, which every fresh worktree pays
(**performance** of the edit loop over the test run). A template database
would mean a test-only path through `Store::start_storage`.

## 4. The raft cluster tests share one gate across the binary

`raft_lanes`, `raft_follower_writes`, `raft_durability`, and
`raft_lanes_durability` each had a file-local `static Mutex` so only one
3-node cluster ran at a time. They now take `support::cluster_gate()`.

**Alternatives.** Keep the four per-file gates.

**Trade-off.** Gated tests from different files can no longer overlap, so the
gated stretch of the run is fully serial (~140 s of the integration binary's
~225 s on 4 cores).

**Why.** Before the merge each file ran in its own process after the
previous one finished, so two clusters from different files never overlapped.
Separate gates in one binary would have allowed exactly the overlap the gates'
comments describe as flaky. One gate keeps the old guarantee.

## 5. Line tables for Legion's crates, no debuginfo for dependencies

`[profile.dev] debug = "line-tables-only"` and
`[profile.dev.package."*"] debug = false`. CI used `debug = 0` through env
vars, which are now removed so CI and local builds match.

**Alternatives.** `debug = 0` everywhere, as CI had. Or leave full debuginfo
locally and only strip in CI.

**Trade-off.** Stepping through code with variables needs
`CARGO_PROFILE_DEV_DEBUG=2` for that build. Backtraces through dependencies
lose file and line.

**Why.** Panic backtraces with `file:line` in Legion's own code are the
debugging most people do, and line tables cost little next to full DWARF
(**simplicity** of one profile for CI and laptops).

## 6. CI saves the dependency cache from `main` only, and now runs on `main`

`Swatinem/rust-cache` with `save-if: github.ref == 'refs/heads/main'`, plus a
`push: branches: [main]` trigger for the Rust jobs. The PR-only jobs (proto
breaking check, UI build, benchmark gate, AWS rig) skip on push.

**Alternatives.** Save on every run, PRs included. Or save only on PRs and
keep CI PR-only.

**Trade-off.** Each merge to `main` now runs the test and clippy jobs (free
minutes on a public repository). The first PR after a `Cargo.lock` change
builds dependencies cold until that change reaches `main`.

**Why.** Actions caches written on a PR ref are visible only to that PR, so
without `main` runs no PR could ever reuse another's dependency build. Saving
from PRs as well would let many PR entries crowd `main`'s out of the 10 GB
budget.

## 7. CI installs a pinned `protoc` release instead of the apt package

A composite action (`.github/actions/setup-protoc`) downloads protoc 21.12
(= 3.21.12, the version Ubuntu 24.04's `protobuf-compiler` ships) and checks
its SHA-256, replacing `apt-get update && apt-get install` in four jobs.

**Alternatives.** Keep apt. Or `arduino/setup-protoc`, which needs a token to
avoid GitHub API rate limits.

**Trade-off.** The checksum was recorded from a download on 2026-10-18, not
from a published checksum file (protobuf releases do not ship one), so it
pins what was fetched then rather than proving provenance. Bumping protoc is
now a manual edit.

**Why.** `apt-get update` spent 20–40 s per job refreshing package lists for
one binary, and the pin closes the UI client drift risk noted in
[ui-holistic-polish calls entry 9](ui-holistic-polish-plan_calls.md) (a
runner image bump silently changing protoc) (**simplicity**: same
version everywhere, same pattern as the existing `buf` install).

## 8. No tmpfs `TMPDIR` in CI

Pointing `TMPDIR` at `/dev/shm` to skip `fsync` changed the unit suite from
229 s to 230 s, so it was dropped.

**Trade-off.** None taken; recorded so the next person does not re-measure
from scratch. On a runner with slower disks the result could differ.
