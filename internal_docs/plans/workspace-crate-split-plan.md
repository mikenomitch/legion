# Workspace Crate Split

> **Judgement calls:** [workspace-crate-split-plan_calls.md](../calls/workspace-crate-split-plan_calls.md)

**Status: SHIPPED** (2026-11-10) as five crates in a chain: `legion-proto` →
`legion-common` → `legion-worker` → `legion-manager` → the `legion` binary.
Building Legion went from 9.0 GB to 5.0 GB at the peak, and the test build,
which a 16 GB machine could not finish, now fits (§ 5.4). It cost 5.8 MB of
shipping binary (91.5 → 97.3 MB). **Deliberately deferred**, each with what it would take in § 8: splitting the store and the
scheduler out of `legion-manager`, a datapath crate, a plugin API crate so
plugins stop depending on all of Legion, and publishing the crates.

The public description of the layout is
[docs/codebase-conventions.md, "Crates"](../../docs/codebase-conventions.md#crates).

## 1. Problem

Legion was one library crate of 470,000 lines, plus the code `tonic-build`
generates from a 19,000-line `legion.proto`. rustc compiles a crate in one
process, and what that process holds grows with the crate:

- **Memory.** Building the library took 9.0 GB. `cargo test` compiles the
  library and the library-with-its-tests at the same time, and on a 16 GB
  machine that was killed at 13.5 GB: the unit tests alone need 10.9 GB.
  Contributors were passing `-j 1` to get a test binary at all.
- **Time.** Any edit rechecked all of it: 30 seconds and 3.9 GB for
  `cargo check` after touching one file, whichever file. The ~490
  dependencies build in parallel; Legion itself was one serial tail at the end
  of every build.
- **Every test binary links all of it**, and every plugin under `plugins/`
  compiles all of it for a handful of types.

The crate was also one namespace in which anything could call anything, and
it did: the module graph had cycles through `structs`, `storage`, `manager`,
and `worker`. A cycle cannot cross a crate boundary, so most of this work was
cutting those references in an order where each step still compiled.

## 2. Prior art

- **Nomad** is one Go module, but Go compiles per package (directory), so its
  ~200 packages already build in parallel with a per-package memory peak. Its
  `api/` package is a separate module so clients do not pull the server: the
  same shape as `legion-proto`.
- **Kubernetes** splits the wire types out as `k8s.io/api` and
  `k8s.io/apimachinery`, with `client-go` on top and the components in their
  own packages. The scheduler depends on the API types and a
  `framework.Handle` interface, never on the apiserver's storage.
- **Rust projects at this size** (rustc itself, `wasmtime`, `deno`,
  `materialize`, `nushell`) all split into workspace crates for the reasons in
  § 1: a types leaf, domain crates in the middle, a thin binary on top.

**Take:** the leaf wire-types crate and the thin binary on top. **Reject:**
separate repositories or separately versioned modules; Legion ships one
binary at one version. **Did not take, yet:** k8s's scheduler-behind-an-
interface, which is what separating the store from the scheduler needs
(§ 8).

## 3. Jobs to be done

**Contributor**
- When I change one file in the scheduler, I want the rebuild to touch the
  manager and the binary, so I stay in an edit–test loop measured in seconds.
- When I build Legion on a 16 GB laptop or a CI runner, I want the build and
  the tests to fit in memory, so nothing swaps, is OOM-killed, or needs `-j1`.
- When I run one part's unit tests, I want to build only that part and what
  it depends on, so testing the worker does not wait on the store.

**Release engineer**
- When I cut a release, I want the shipping build to run on a stock runner.
- When I quote the binary's size, I want the number to be what we ship on the
  platform people run.

**Plugin author**
- When I write a plugin, I want to depend on the types and the plugin
  interface, not on all of Legion. *Not served yet:* § 8, "A plugin API
  crate".

**Operator / job author / agent:** *not in scope.* Nothing they can observe
changed: one binary, same CLI, same wire format, same on-disk formats.

### Scenarios

1. A contributor edits `crates/worker/src/worker/daemon.rs` and runs
   `cargo test -p legion-worker daemon`. It builds the worker crate's tests
   against a `legion-common` that is already built; the manager, the store, and
   the CLI are not compiled.
2. A contributor runs `cargo test --all` on a 16 GB machine with no flags,
   and it builds.
3. An agent session edits the scheduler and runs `cargo check`: 11 seconds
   and 1.4 GB, where it was 29 seconds and 3.9 GB.

## 4. Goals and non-goals

**Goals**
- Lower the peak memory of building and testing Legion, far enough that a
  16 GB machine does both without flags.
- Shorten the rebuild after an edit by not recompiling what did not change.
- Make the layers real: a crate's dependencies are its allowed calls.
- Correct the shipped-size numbers, measured on linux/amd64.

**Non-goals**
- **More binaries.** Still one `legion` binary; `legion-guest` stays the only
  second artifact.
- **A stable Rust API.** The crates are an internal build structure,
  versioned in lockstep, with no compatibility promise between them.
- **Publishing** the crates to crates.io (§ 10).
- **Behavior changes.** Nothing on the wire, on disk, or at the CLI.
- **Cargo features to trim the binary** (a worker-only build). Possible now
  that the crates exist, but a product decision, not a build one.

## 5. Background

### 5.1 The shape before

`src/` by top-level module, as it stood on `main` (lines, tests included):

| Module | Lines | What it is |
| --- | ---: | --- |
| `manager/` | 105,000 | server (44k), scheduler (14.5k), daemon, rollout, telemetry, CA, ACLs |
| `storage/` | 88,000 | `store.rs` (33.5k) + `store/` (14k), raft (15k), migrations, events |
| `worker/` | 80,000 | `netd/` datapath (25.7k), task manager (15k), daemon, forwarder |
| `structs/` | 51,000 | hand-written domain types around the generated proto types |
| `cmd/` | 38,500 | the CLI and MCP server |
| `task_plugins/` | 32,500 | exec, container, sandbox, microVM runtimes, jobspec validation |
| `apps/` | 28,500 | autoscaler, certs, ingress, gitops apps |
| `autoscaler/` | 12,400 | |
| everything else | 34,000 | artifacts, volumes, devices, config language, TLS, … |

About 27% of it is unit tests.

### 5.2 What crossed the boundaries

References from one top-level module to another were counted from `use`
trees and inline `crate::`/`super::` paths, separating code from tests. For
the layering common → worker → manager → binary, the references that pointed
the wrong way, before any change:

| From → to | Code | Tests | What they were |
| --- | ---: | ---: | --- |
| common modules → manager/storage | 42 | 15 | the eval handler living in `structs` (it runs evals against the `Store`), the object-store client under `storage/`, liveness settings, device quota |
| common modules → worker/task_plugins | 11 | 6 | config types defined beside their runtime, balancer names, template syntax, the jobspec schema |
| worker/task_plugins → manager/storage | 55 | 76 | the metrics registry (33), mesh CA helpers, payload building in the dispatcher, constants |
| manager/storage → cmd/apps | 8 | 4 | the bundled apps' manifests, the CLI's token path |
| worker → cmd/apps | 0 | 3 | |
| **manager/storage → worker/task_plugins** | **40** | **4** | **kept: the manager depends on the worker crate** |

The last row is why the manager is above the worker rather than beside it
([calls 1](../calls/workspace-crate-split-plan_calls.md)): the manager
validates jobs against the task runtimes and can run a worker in its own
process. Those are real dependencies.

No inherent `impl` was in a different module tree from its type, so the
orphan rule cost nothing beyond the four impls on generated types.

Not cut, and what is left for a later split: `storage` → `manager`, 28
references (§ 8).

### 5.3 Where the memory goes

`-Z time-passes` on the library as one crate (dev profile, tests excluded),
resident memory at the end of each compiler pass:

| Pass | RSS after | Added |
| --- | ---: | ---: |
| macro expansion | 1.5 GB | 1.4 GB |
| name resolution, AST lowering | 2.2 GB | 0.7 GB |
| type checking (coherence included) | 3.5 GB | 1.3 GB |
| borrow checking | 5.0 GB | 1.5 GB |
| MIR for metadata, monomorphization collection | 6.9 GB | 1.9 GB |
| code generation | 7.3 GB | 0.3 GB |

There is no single hot pass and nothing pathological: every stage adds
memory roughly in proportion to how much code it sees, and nothing is freed
until the crate is done. (Sampled from outside, with LLVM's threads counted,
the same build peaks at 9.0 GB.) So `cargo check` alone needed 5–6 GB, the
LTO link was never the peak, and no profile setting would have helped. The
only lever is how much code one rustc process holds.

### 5.4 Measured, before and after

linux/amd64, 4 vCPU, 16 GB, no swap, cargo 1.97.0. "Before" is `main` at
`d1077f2`. Peak is resident memory sampled twice a second, summed over every
compiler and linker running at that moment.

| | Before | After |
| --- | ---: | ---: |
| `cargo build`, clean: wall | 5m 16s | 4m 19s |
| `cargo build`, clean: peak | 9.0 GB | **5.0 GB** |
| Largest single compile | 9.0 GB (`legion`) | **3.8 GB** (`legion-manager`) |
| Legion's own crates only: wall | 3m 44s | 2m 52s |
| `cargo check`, clean: wall / peak | 3m 00s / 6.2 GB | 2m 19s / 3.6 GB |
| `cargo check` after editing the scheduler | 29 s / 3.9 GB | 11 s / 1.4 GB |
| … after editing the worker daemon | 31 s / 3.9 GB | 15 s / 1.4 GB |
| … after editing `structs/job.rs` | 30 s / 4.0 GB | 19 s / 1.4 GB |
| … after editing a CLI command | 30 s / 3.9 GB | 3 s / 0.7 GB |
| `cargo build` after editing the scheduler | 50 s / 6.3 GB | 27 s / 3.7 GB |
| Release build (`cargo bench --no-run`): wall | 12m 18s | 11m 12s |
| Release build: peak / largest single compile | 10.0 GB / 10.0 GB | 8.6 GB / 4.5 GB |
| `cargo build --profile dist`: wall | 35m 05s | 23m 46s |
| `--profile dist`: peak / largest single process | 11.1 GB / 11.1 GB | 9.8 GB / 7.8 GB (the LTO link) |
| `target/dist/legion`, stripped | 91.5 MB | **97.3 MB** |
| `cargo test --all --no-run`: result | OOM-killed at 13.5 GB | builds in 3m 47s |
| `cargo test --all --no-run`: peak | (13.5 GB when killed) | **9.6 GB** |
| Largest unit-test compile | 10.9 GB (`legion`) | **4.5 GB** (`legion-manager`) |

Each crate's own compile, dev build: `legion-manager` 3.8 GB,
`legion-worker` 2.2 GB, the binary crate 2.2 GB, `legion-common` 1.7 GB,
`legion-proto` 1.6 GB.

**The shipping binary grew by 5.8 MB (6.4%).** `.text` went from 71.3 MB
to 76.3 MB. The likeliest cause, not yet confirmed symbol by symbol: an
optimized build does not share monomorphized generics between crates, so a
generic instantiated in several of Legion's crates (a `serde` deserializer
for `JobConfig`, say, used by the store, the worker, and the CLI) is
compiled once per crate where it used to be compiled once, and fat LTO does
not fold the copies back together. It is the price of the split, taken
knowingly ([calls 3](../calls/workspace-crate-split-plan_calls.md)), and
what to try is in TODO.md ("Find what makes the linux/amd64 binary 97 MB").

The shipping build's peak moved less than the dev build's because its
crates overlap: with one codegen unit each, four of them are in LLVM at
once (9.8 GB at the worst moment), and the fat-LTO link is 7.8 GB whatever
the crate layout. The dist wall time fell by a third because those crates
now compile in parallel. (The "before" wall time is from `9a97d54`, two days
earlier; memory and size are from `d1077f2`.)

### 5.5 Scheduler benchmarks

`resources`, `constraints`, `scheduling_index`, and the domain types are in
`legion-common` now, a crate away from the scheduler that calls them per
candidate, and the release profile has no cross-crate LTO. `scheduler_bench`
on `main` and on this branch, same machine, release profile, median:

| Benchmark | `main` | Branch |
| --- | ---: | ---: |
| `candidate_construction/indexes_off/nodes_1000` | 2.75 ms | 3.17 ms |
| `candidate_construction/indexes_on/nodes_1000` | 2.68 ms | 2.35 ms |
| `candidate_construction/indexes_off/nodes_5000` | 13.90 ms | 14.86 ms |
| `candidate_construction/indexes_on/nodes_5000` | 6.81 ms | 6.57 ms |
| `candidate_construction/indexes_off/nodes_10000` | 30.06 ms | 27.32 ms |
| `candidate_construction/indexes_on/nodes_10000` | 9.31 ms | 9.57 ms |
| `plan_under_fill/plan_only/fill_0` | 224 µs | 208 µs |
| `plan_under_fill/plan_and_apply/fill_0` | 2.21 ms | 2.14 ms |
| `plan_under_fill/plan_only/fill_40` | 270 µs | 257 µs |
| `plan_under_fill/plan_and_apply/fill_40` | 2.60 ms | 2.55 ms |
| `scheduler_speed/bin_pack/nodes_100_allocs_1000` | 77.1 ms | 76.4 ms |
| `scheduler_speed/bin_pack/nodes_100_allocs_10000` | 437.6 ms | 439.2 ms |

No difference outside the run-to-run spread (the `nodes_1000` rows'
confidence intervals overlap, and the two directions cancel). The 4-vCPU
machine is noisy; nothing here says the boundary costs anything.

## 6. Design, as built

### 6.1 The crates

```text
legion-rs        src/             the binary: cmd/ (CLI, MCP), apps/, gateway
   │
legion-manager   crates/manager/  manager/ (server, eval loop, scheduler, rollouts)
   │                              storage/ (store, raft, migrations)
legion-worker    crates/worker/   worker/ (agent, netd datapath), task_plugins/,
   │                              artifacts/, objects/, content_publish, wasm/,
   │                              plugin_runtime, rollout_strategy
legion-common    crates/common/   structs/, autoscaler/, bundled_apps, config_lang,
   │                              volumes/, devices/, cluster_tls, mesh_ca, telemetry
legion-proto     proto/           generated from legion.proto
```

| Crate | Lines | Depends on |
| --- | ---: | --- |
| `legion-rs` | 68,000 | all below |
| `legion-manager` | 189,000 | worker, common |
| `legion-worker` | 126,000 | common |
| `legion-common` | 85,000 | proto, `legion-guest` |
| `legion-proto` | generated | prost, tonic, serde |

It differs from the first sketch of this plan, which had nine crates
(`-core`, `-types`, `-store`, `-scheduler`, `-datapath`, `-worker`,
`-manager`, `-apps`, binary) with the worker and manager as siblings. What
changed and why is [calls 1 and 2](../calls/workspace-crate-split-plan_calls.md).

### 6.2 Paths did not change

Each crate re-exports the modules of the crates below it at its root:

```rust
// crates/worker/src/lib.rs
pub use legion_common::{autoscaler, constants, prelude, structs, telemetry, util, /* … */};
pub mod worker;
pub mod task_plugins;
```

So `crate::structs::Job` resolves in every crate, and `legion::manager::…`
in the integration tests, benchmarks, and plugins. The `#[macro_export]`
plugin macros live with their traits (`export_scheduling_policy!` in the
manager, the rest in the worker) and are re-exported by the binary crate, so
a plugin still writes `legion::export_rollout_strategy!(…)`.

### 6.3 How each wrong-way reference was cut

All of them are moves; none changed behavior.

**Moved down**, to the lowest crate that needs it:

| What | From | To |
| --- | --- | --- |
| The metrics registry (cells, `Metrics`, `WorkerMetrics`, rendering, the worker's listener) | `manager/telemetry.rs` | `common/telemetry.rs`; the manager keeps its `/readyz` endpoint and re-exports the rest |
| Mesh CA helpers | `manager/mesh_ca.rs` | `common/mesh_ca.rs` |
| Payload building (`build_task_allocation`, `effective_gate`, …) | `manager/dispatcher.rs` | `common/structs/task_payload.rs` |
| What both sides must compute alike (`WorkDigest`, `StableHash`, heartbeat clamp, error-code constants) | `storage/*`, `manager/*` | `common/structs/worker_protocol.rs` |
| Jobspec-only autoscale validation | `manager/autoscale.rs` | `common/autoscaler/jobspec.rs` |
| Template syntax checks | `worker/templates.rs` | `common/structs/template_syntax.rs` |
| `RuntimeClassConfig`, `ContentStoreTarget`, `LivenessSettings` | beside their runtime | `common/structs/config.rs` |
| Bundled apps' manifests and `seed_record` | `apps/`, `manager/apps.rs` | `common/bundled_apps.rs` |
| The CLI token path | `cmd/helpers.rs` | `common/cli_paths.rs` |
| The object-store client | `storage/objects/` | `worker/objects/` |
| The rollout-strategy plugin loader | `manager/rollout_strategy.rs` | `worker/rollout_strategy.rs` |

**Moved up**, to where its dependencies are:

| What | From | To |
| --- | --- | --- |
| The eval handler (`Eval::handle`, as `EvalExt`) | `structs/eval.rs` | `manager/eval.rs` |
| Device quota enforcement | `devices/quota.rs` | `storage/device_quota.rs` |
| The jobspec JSON Schema builder | `structs/job_schema.rs` | `worker/task_plugins/job_schema.rs` |
| `export_scheduling_policy!` | `plugin_runtime.rs` | `manager/scheduler/policy.rs` |

**A list on each side, and a test that they match**
([calls 9](../calls/workspace-crate-split-plan_calls.md)): load-balancing
strategy names; bundled apps (manifests in common, bodies in the binary).

**Extension traits**, where a type is defined below the code its methods
need: `EvalExt`, `SubmissionExt`, `LedgerNodeExt`, `LedgerAllocationExt` on
generated types; `ContentStoreTargetExt` on the `contentStore` config block.

**Tests.** Sixty-odd worker unit tests that ran against a real manager moved
to `tests/integration/` as six `worker_*` modules
([calls 5](../calls/workspace-crate-split-plan_calls.md)). Four tests in
`structs` that asserted something about an upper layer moved up beside it.

### 6.4 What the contributor sees

```bash
cargo build                          # unchanged
cargo test --all --locked            # unchanged; now fits in 16 GB
cargo test --lib <filter>            # unchanged: covers every crate (default-members)
cargo test -p legion-worker <filter> # new: only the worker crate and legion-common
cargo test -p legion-manager scheduler
cargo check                          # 3–19 s after an edit, was 30 s
```

## 7. Proto, struct, config, and persistence changes

None on the wire, on disk, or in job config. `proto/legion.proto` is at the
same path. The plugin ABI fingerprint's build-info string now names
`legion-worker` (the crate the loader is in) where it named `legion-rs`;
plugins are rebuilt against their host in any case.

## 8. Implementation phases

**Phase 1: `legion-proto` (SHIPPED 2026-11-08).** The generated code as its
own crate; `legion::structs` re-exports it.

**Phase 2: the layered split (SHIPPED 2026-11-10).** `legion-common`,
`legion-worker`, `legion-manager`, and the binary crate, as in § 6.

**Not started.** Each is independent of the others.

- **The store out of the manager's crate.** `legion-manager` is 189k lines
  and the largest compile. `storage` has 28 references into `manager`, and
  they are not misplaced items: `Store` holds `ScalingRollups`,
  `ScalingStatusBoard`, and `ClusterCa` as fields; `store.rs` calls
  `Scheduler::new` to replan inside a write; the store's transactions call
  `autoscale::{is_autoscaled, AutoscaleLayers::load_tx}`,
  `blocked::{PlacementBlocked, node_could_help}`, and
  `concurrency::conflict_window`. Separating them means deciding who owns
  the autoscaler's in-memory state and the CA, and putting plan application
  behind an interface the store is handed (k8s's `framework.Handle` shape).
  That is a design change to the write path and needs its own plan, with
  `scheduler_bench` before and after. Expected gain: the largest compile at
  about 2.5 GB, and a scheduler edit that no longer rebuilds the store.
- **A datapath crate.** `worker/netd/` is 25.7k lines with few references
  out. It would take the worker crate from 126k to about 100k: worth doing
  only if the worker's 2.2 GB becomes the constraint.
- **A plugin API crate.** Every plugin depends on `legion-rs` and so
  compiles all five crates. The plugin traits and the types that cross the
  dylib boundary could be one small crate; the ABI fingerprint's layout hash
  has to move with them. Also fixes "Share one target dir across plugin
  builds" (TODO.md) from the other side.
- **`legion-loadgen` as a workspace member**, off the `loadgen` feature
  (TODO.md, "Test hygiene").

## 9. Testing plan

- Each step: `cargo test --all --all-features --locked`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
  `cargo fmt --all --check`. A refactor with no behavior change has the
  existing suite as its test.
- The cuts were made first, inside the single crate, each one compiling, so
  that the split itself was only moving directories and widening visibility.
  A script recounted wrong-way references after every cut until there were
  none.
- New tests: the balancer registry and the jobspec's strategy list agree;
  every bundled manifest has a body and the reverse; key-carrying messages
  never print or serialize their keys (`legion-proto`).
- The footprint numbers in § 5.4 are from a sampler that records, per
  compiler process, the crate it is building; rerun it when the layout
  changes.

## 10. Rollout and backward compatibility

Nothing to roll out at runtime: same binary, same formats. A checkout
rebuilds once. Open branches rebase across the directory moves (git follows
the renames).

**Publishing changes.** `legion-rs` and `legion-guest` are on crates.io at
0.1.0, published as one crate plus the guest. `legion-rs` now has
path-and-version dependencies on `legion-proto`, `legion-common`,
`legion-worker`, and `legion-manager`, so the next release publishes those
first, in that order. None is published yet, and `legion-worker` cannot pass
`cargo publish`'s verification until `bpf/legion_net.bpf.o` is inside its
package (TODO.md, "Publish"). The shared crate is `legion-common` rather
than `legion-core` because that name is taken
([calls 7](../calls/workspace-crate-split-plan_calls.md)).

## 11. Open questions

- **Cross-crate inlining on the hot path.** Generic and `#[inline]` code
  inlines across crates; plain functions do not without LTO. The shipping
  profile is fat LTO, so the shipped binary is unaffected. Benchmarks run
  the release profile, where the scheduler now calls `resources`,
  `constraints`, and `scheduling_index` across a crate boundary; § 5.5
  measured no change. The store and the scheduler are still one crate.
  Measure again if they are separated.
- **Does the binary crate need `apps/` and `cmd/` apart?** At 68k lines and
  2.2 GB it is not the constraint. Lean: no.

## 12. Acceptance criteria

- `cargo test --all --all-features --locked` builds and passes on a 16 GB,
  4-vCPU machine with no `-j` flag. *Met: 3,902 passed, 0 failed.*
- No single compile over 4 GB in a dev build, or 5 GB with unit tests. *Met:
  3.8 GB and 4.5 GB.*
- `cargo test -p legion-worker` builds without compiling `legion-manager`.
  *Met.*
- No scheduler benchmark regression. *Met (§ 5.5).*
- `cargo build --profile dist` completes on the same machine, and the README,
  AGENTS.md, `Cargo.toml`, and `internal_docs/development.md` carry the
  linux/amd64 sizes. *Met.*
- All five dylib plugin kinds build against the new layout with no source
  change. *Met.*
- The shipping binary does not grow. *Not met: +5.8 MB (§ 5.4).*
