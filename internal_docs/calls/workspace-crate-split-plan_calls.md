# Judgement calls: workspace crate split

Calls made planning and executing
[workspace-crate-split-plan.md](../plans/workspace-crate-split-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. The crates are a chain with the manager above the worker, not the nine-crate graph first sketched

Legion is `legion-proto` → `legion-common` → `legion-worker` →
`legion-manager` → `legion-rs` (the binary). The plan's first sketch had
nine crates with the worker and the manager as siblings over a shared store.

```text
legion-rs        src/             cmd, apps
legion-manager   crates/manager   manager, storage
legion-worker    crates/worker    worker, task_plugins, artifacts, objects, plugin loader
legion-common    crates/common    structs, autoscaler, config_lang, telemetry, cluster TLS
legion-proto     proto            generated wire types
```

**Alternatives.** (a) Worker and manager as siblings, which would let cargo
build them in parallel. (b) The full sketch: separate `legion-store`,
`legion-scheduler`, `legion-datapath`, `legion-apps`.

**Trade-off.** A change in the worker crate rechecks the manager (15 s
against 11 s for a manager-only change). The manager crate still holds the
store, so it is the largest at 189k lines and 3.8 GB to build.
`legion-worker` is an imprecise name: it also holds things both sides use
(the object-store client, artifacts, the plugin loader, the rollout-strategy
loader), because in a chain anything the manager shares with the worker can
live in the worker's crate.

**Why (simplicity, measured).** Counting the references that cross each
proposed boundary ([plan § 5.2](../plans/workspace-crate-split-plan.md#52-what-crossed-the-boundaries))
settled it. The manager uses the worker crate in 40 places that are real
dependencies, not accidents: it validates every job against the task
runtimes, and a single-process cluster runs a worker in the manager's
process. Making them siblings meant inventing an interface for all of that.
With the manager above, those 40 need nothing, and the references going the
other way were 55 that all moved down cleanly. The chain needed about 35
cuts in total, none of which changed behavior. A chain is also the better
shape for the problem being fixed: sibling crates compile at the same time,
and it is the sum of what is compiling that runs a machine out of memory.

## 2. Stop at four library crates; the store stays in the manager's crate

The split ends with the store and the scheduler still inside
`legion-manager`.

**Alternatives.** Carry on to the plan's `legion-store` /
`legion-scheduler` split, which would take the largest crate from 189k lines
to about 105k.

**Trade-off.** The largest single compile is 3.8 GB (4.5 GB with its unit
tests) where a further split might reach 2.5 GB. A scheduler change still
recompiles the store's 85k lines of code that did not change.

**Why (simplicity).** The 28 remaining references from `storage` into
`manager` are not misplaced items. `Store` owns the autoscaler's rollups and
status board and the cluster CA as fields, and calls `Scheduler::new` to
replan inside a write. Cutting them means deciding who owns that state and
whether the store may plan, which is a design change to the write path, with
`scheduler_bench` numbers owed before and after. Everything up to here was
moving code; that is not. The build already fits a 16 GB machine with room,
and the numbers say where the next GB is if it is wanted
([plan § 8](../plans/workspace-crate-split-plan.md#8-implementation-phases)).

## 3. The shipping binary is 5.8 MB larger, and that was accepted

`target/dist/legion` is 97.3 MB after the split and was 91.5 MB before it
(linux/amd64, stripped, fat LTO; `.text` 71.3 → 76.3 MB).

```text
main (one crate)      91,483,040 bytes
this layout           97,314,784 bytes   (+6.4%)
```

**Alternatives.** (a) Keep one crate and its 9 GB build. (b) Hold the split
until the growth is found and removed. (c) Give the shipping profile its own
single-crate build, so only development builds are split.

**Trade-off.** Every node downloads and holds 5.8 MB more. `Cargo.toml` says
of the shipping profile that "binary size is a feature", and this moves it
the wrong way.

**Why (ease of working on Legion over 6% of the binary).** The build was
failing people outright: a test build that cannot finish on a 16 GB machine
costs more than 6% of a 91 MB file. (c) is not something cargo can express.
(b) is open-ended: the likely cause is generic code instantiated once per
crate where it was instantiated once (optimized builds do not share
monomorphizations across crates, and LTO did not fold them back), and the fix
for that is non-generic wrappers at the crate boundaries for the heavy cases
(a `JobConfig` parsed from JSON in four crates), which wants a symbol-level
size comparison first. That is in TODO.md with the existing question of why
the binary is 91 MB at all. Revisit sooner if the binary's size starts
blocking an edge deployment.

## 4. Every crate re-exports the modules below it, so no path changed

`crate::structs::Job` works in all four crates, `crate::worker::netd` works
in the manager, and `legion::manager::…` works in tests and plugins, because
each crate's root has `pub use legion_common::{structs, prelude, …};`.

```rust
// crates/manager/src/lib.rs
pub use legion_common::{autoscaler, constants, prelude, structs, telemetry, util, /* … */};
pub use legion_worker::{artifacts, plugin_runtime, task_plugins, worker, /* … */};
pub mod manager;
pub mod storage;
```

**Alternatives.** Name the owning crate at every use:
`legion_common::structs::Job`.

**Trade-off.** A path no longer says which crate a module is in; a reader
has to look at the crate roots (or `docs/codebase-conventions.md`). A
`crate::` path in the manager can now mean code in another crate.

**Why (simplicity).** About 11,000 `crate::` paths, every integration test,
every benchmark, and every plugin would have changed for no build benefit,
and every branch in flight would have conflicted on every import. Plugins
are the sharp case: they are outside the workspace, name `legion::…`, and
their `export_*!` macros expand to `$crate::…` paths that resolve through
these re-exports.

## 5. Tests that need a crate above their code moved to `tests/integration/`

Sixty-odd worker unit tests ran against the real manager: the ticket
verifiers against the manager's token issuer, the heartbeat loop against a
manager on a loopback port. A unit test cannot see a crate above its own, so
six test modules moved to the integration binary as `worker_boot_ticket`,
`worker_direct_exec`, `worker_edge_overload`, `worker_content_report`,
`worker_daemon`, and `worker_managers`.

```sh
# before
cargo test --lib worker::boot_ticket
# after
cargo test --test integration worker_boot_ticket::
```

**Alternatives.** (a) Move the issuer's signing core into `legion-common` and
leave the tests where they were. (b) A dev-dependency from the worker crate
on the manager, which cargo allows. (c) A small fake issuer in the worker's
tests.

**Trade-off.** These tests are no longer beside the code they exercise, and
`cargo test -p legion-worker` does not run them. Eight worker items became
`pub` so the tests could reach them (`register_self`, `heartbeat_tick`,
`RegistrationPayload`, and others), and a `spent_count()` accessor replaced
a test's read of a private field.

**Why (simplicity, and not weakening a security test).** (a) splits a
security-sensitive type so its store-backed half becomes an extension trait
over newly exposed key state, to save moving tests. (b) makes
`cargo test -p legion-worker` build the manager, which is the cost the split
exists to remove. (c) would stop checking the worker's verifier against
what the manager actually signs. They are contract tests between two crates,
and the integration binary is where those live.

## 6. The files moved into `crates/<name>/src/`

The modules were moved with `git mv`, not left in `src/` behind
`[lib] path = "../../src/…"` crate roots.

**Alternatives.** Keep every file where it was and point three crate
manifests into the shared `src/` directory. No path in any doc, CI filter, or
open branch would have changed.

**Trade-off.** About 1,500 `src/…` references across the docs, plans, CI
path filters, tests, and UI comments were rewritten (mechanically, by one
script), and every open branch has to rebase across the rename.
`CARGO_MANIFEST_DIR` is no longer the repository root in three crates, so
the 22 tests that read `configs/` go up two directories.

**Why (simplicity).** A directory that is one crate is what every Rust
contributor and tool expects; three crates sharing one `src/` would need a
paragraph of explanation at the top of every one of them, and "which crate
is this file in" would have no answer on disk.

## 7. The shared crate is `legion-common`, because `legion-core` is someone else's

The bottom library crate is named `legion-common`. It was `legion-core` for
most of this work.

**Alternatives.** `legion-core`; or an `legion-rs-` prefix on every crate
(`legion-rs-core`, `legion-rs-worker`, …), following the binary crate.

**Trade-off.** "Common" says less than "core" about what is in it. The other
three names (`legion-proto`, `legion-worker`, `legion-manager`) are free on
crates.io today but sit in a namespace another project also publishes into
(`legion-skills`), and nothing reserves them.

**Why.** `legion-rs` and `legion-guest` are published on crates.io, and
`cargo install legion-rs` is the README's quickstart, so the crates under
it have to be publishable. `legion-core` on crates.io is the ECS library's.
A crate rename is cheap here (the package name and three `pub use` lines,
because of entry 4), so the remaining names can change at publish time if
one has been taken.

## 8. A bare `cargo test` still runs every crate's tests

`default-members` lists the binary and the four library crates.

```toml
default-members = [".", "proto", "crates/common", "crates/worker", "crates/manager"]
```

**Alternatives.** Cargo's default: without it, `cargo test`, `cargo build`,
and `cargo clippy` with no `-p` act on the root package alone.

**Trade-off.** `cargo test --lib <filter>` builds four unit-test binaries to
run a test that is in one of them. `-p` is the fast path and people have to
know it.

**Why (ease).** The alternative fails silently. `cargo test` would have
passed while skipping about 85% of the unit tests, and the docs contain
dozens of `cargo test --lib <filter>` commands that would have started
finding nothing.

## 9. Where a lower crate needed a list from an upper one, each side keeps its own and a test compares them

Jobspec validation (common) must know the load-balancing strategies the
datapath (worker) can build; the manager must know the bundled apps'
manifests, whose bodies are in the binary crate.

```rust
// crates/common/src/structs/network.rs
pub const BALANCE_STRATEGIES: &[&str] = &["least_latency", "round_robin", "least_conn", "local_first"];
// crates/worker/src/worker/balance.rs, in a test
assert_eq!(balancer_names(), crate::structs::BALANCE_STRATEGIES);
```

**Alternatives.** A registry the upper crate fills at startup, or a trait
the lower crate calls through.

**Trade-off.** Adding a strategy is three edits where it was two, and adding
a bundled app is a manifest row and a body row. `BALANCERS` was documented
as "the only list of strategy names in the tree"; it no longer is.

**Why (simplicity).** A startup registry is state that tests, the CLI's
offline `job validate`, and every embedded use must remember to fill, and
when they forget validation quietly accepts less. Two constants and a
failing test cannot be forgotten at runtime.

## 10. Jobspec validation stays with the task runtimes; the rollout-strategy loader moved down to it

`validate_job_config` is in `legion-worker` (`task_plugins`). It also checks
that a rollout strategy plugin loads, so `rollout_strategy.rs` moved from
the manager to sit beside the other dylib loaders, with
`manager::rollout_strategy` kept as a re-export.

**Alternatives.** Move validation up into the manager and split it: runtime
checks in the worker crate, rollout and autoscale checks in the manager.

**Trade-off.** A manager-side plugin kind now lives in a crate named for the
worker.

**Why (simplicity).** The loader is 198 lines with no manager state. Splitting
the validator would have changed which error a jobspec with two problems
reports first, and left two functions with nearly the same name, one of
which silently checks less.

## 11. Fix build memory by shrinking the crate, not by loosening the `dist` profile

The shipping profile is unchanged: fat LTO, one codegen unit.

**Alternatives.** `lto = "thin"` with `codegen-units = 16` for `dist` (the
existing `distfast` profile), which builds in roughly a third of the time.

**Trade-off.** The shipping build still takes over half an hour.

**Why (performance, measured).** A per-pass profile of the compiler
([plan § 5.3](../plans/workspace-crate-split-plan.md#53-where-the-memory-goes))
shows memory growing through every pass in proportion to the crate: 1.4 GB
of expanded syntax, 2.8 GB more for type and borrow checking, 2 GB more for
MIR and monomorphization. Nothing in it is the link. The LTO settings buy
back time, not memory, at the cost of binary size; they are worth revisiting
if release wall-clock becomes the complaint.

## 12. A `contentStore` block's behavior is an extension trait; its data is a config type

`ContentStoreTarget` is a field of the worker config, which `legion-common`
parses, but everything it does (open the store, resolve credentials) needs
the object-store client in `legion-worker`. The struct moved to
`structs/config.rs`; its nine methods became `ContentStoreTargetExt` in
`content_publish.rs`.

**Alternatives.** Move the object-store client, the artifact credential
types, and the chunk store into `legion-common` so the methods could stay
inherent.

**Trade-off.** Callers import a trait to call `target.validate()`.

**Why (simplicity).** The alternative drags four modules and the artifact
fetchers' plugin loader down with it. The plugin ABI fingerprint hashes two
task-runtime types, so the loader cannot move below the task runtimes
without splitting the fingerprint.

## 13. Test helpers used across crates are always compiled

`mesh_ca::issue_node_cert` and `test_scratch` were `#[cfg(test)]`. The
worker's and manager's tests use them, and a dependency is never built with
`cfg(test)`, so they are ordinary `pub` items now, documented as test
support.

**Alternatives.** A `test-support` cargo feature on `legion-common`, enabled
from the upper crates' dev-dependencies.

**Trade-off.** A function that mints a node certificate with a locally
generated key is present in non-test builds. Nothing calls it, it is not
reachable from any RPC, and LTO drops it from the shipping binary.

**Why (performance of the build).** Features are part of a crate's build
hash. A feature that is on for `cargo test` and off for `cargo build` makes
cargo keep two builds of `legion-common` and of everything above it, which is
the doubling this work set out to remove (the `loadgen` feature already has
a TODO for the same reason).

## 14. The first crate out was the generated proto code

Before the layered split, `legion-proto` was extracted alone: the code
`tonic-build` generates from `legion.proto`.

**Alternatives.** Start with the store or the scheduler.

**Trade-off.** It moved 10% of the peak by itself, which looked like the fix
and was not.

**Why.** It has no references to the rest of Legion, so it could move
without cutting anything, and it is the crate an external client needs. It
stays as the bottom of the chain.

## 15. `proto/` is the crate, rather than `crates/proto/` reaching into it

`proto/Cargo.toml` sits beside `legion.proto`. The three later crates are
under `crates/`.

**Alternatives.** `crates/proto/build.rs` compiling `../../proto/legion.proto`.

**Trade-off.** The crate directories are not all in one place: `guest/` and
`proto/` at the root, the rest under `crates/`.

**Why (simplicity).** `proto/legion.proto` is a path the UI's generator, the
`proto-breaking` CI job, `buf.yaml`, and the RPC-classification tests all
know. A build script that reaches outside its own package also cannot be
packaged.

## 16. One dependency table for the workspace, and three unused dependencies dropped

Versions and features are declared once in `[workspace.dependencies]`; each
crate lists `name.workspace = true` for what it uses. Sorting dependencies
by which crate uses them found three that nothing does:
`serde_rusqlite`, `tracing-futures`, and
`opentelemetry-semantic-conventions`.

**Alternatives.** Repeat versions in each manifest; keep the unused three.

**Trade-off.** A crate's manifest no longer shows a dependency's version or
features; they are in the root `Cargo.toml`.

**Why (simplicity).** Four manifests with their own copies of sixty versions
drift, and cargo then builds two versions of a dependency without saying so.

## 17. "Before and after" is `main` and this branch, on linux/amd64

The request was to build `dist` before and after so the stale numbers in
`Cargo.toml` could be corrected. Those numbers (`43.7 MB` / `26.0 MB`) were
macOS arm64.

**Alternatives.** Keep a two-platform table.

**Trade-off.** The macOS numbers are gone rather than kept beside the Linux
ones.

**Why.** linux/amd64 is what people deploy, nobody re-measures both, and a
two-platform table goes stale twice as fast.
