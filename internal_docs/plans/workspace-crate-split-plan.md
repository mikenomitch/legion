# Workspace Crate Split

> **Judgement calls:** [workspace-crate-split-plan_calls.md](../calls/workspace-crate-split-plan_calls.md)

**Status: IN EXECUTION** — phase 1 (`legion-proto`) shipped 2026-11-08: the
main crate's peak build memory fell from 11.1 GB to 10.0 GB (§ 5.3). Phases
2–7 are planned. Deliberately deferred: publishing the new crates to
crates.io (§ 10), and moving the example plugins off the `legion` crate
(§ 11).

## 1. Problem

Legion is one library crate of 465,000 lines (`src/`, excluding generated
code) plus the protobuf code `tonic-build` generates from a 19,000-line
`legion.proto`: 640 messages and enums, each deriving `serde`, `prost`, and
`Clone`/`PartialEq`, and the `Manager` client and server stubs. rustc compiles
a crate in one process, and most of the front end — macro expansion, type
checking, borrow checking — is single-threaded within it. So:

- **Memory.** The whole crate's HIR, type tables, and MIR are live in one
  rustc at once. The measured peak is in § 5. On a laptop or a 16 GB CI
  runner that is the build's ceiling, and on the `dist` profile the fat-LTO
  step stacks the whole dependency tree's IR on top of it.
- **Time.** Every edit to any file re-runs the front end over the whole crate
  (incremental helps the back end far more than the front). The ~490
  dependencies build in parallel; Legion itself is one long serial tail at the
  end of every build.
- **Every test binary links all of it.** Already mitigated by the single
  `tests/integration/` binary ([test-build-footprint
  calls](../calls/test-build-footprint_calls.md)), but a unit test in the
  scheduler still waits for the worker's datapath to type-check.
- **Plugins link all of it.** Every plugin under `plugins/` depends on the
  whole `legion` crate for a handful of types, so a `LEGION_BUILD_PLUGINS=1`
  build compiles Legion once per plugin (TODO.md, "Share one target dir across
  plugin builds").

The crate is also a single namespace in which anything can call anything, and
it does: the module graph in § 5.2 has cycles through `structs`, `storage`,
`manager`, and `worker`. That is the actual obstacle — a cycle cannot cross a
crate boundary — and most of this plan is about breaking those edges in an
order where each step is shippable.

## 2. Prior art

- **Nomad** is one Go module, but Go compiles per package (directory), so its
  ~200 packages already build in parallel with a per-package memory peak. Its
  `api/` package is a separate module precisely so clients don't pull the
  server: the same shape as `legion-proto` below.
- **Kubernetes** splits the wire types out as `k8s.io/api` and
  `k8s.io/apimachinery` (staged repos), with `client-go` on top, and the
  components (`kube-scheduler`, `kubelet`, `kube-apiserver`) in their own
  packages and binaries. The scheduler depends on the API types and a
  `framework.Handle` interface, never on the apiserver's storage.
- **Consul** keeps `api/` and `sdk/` as separate Go modules for the same reason
  as Nomad.
- **Rust projects at this size** — rustc itself (~50 `rustc_*` crates),
  `wasmtime` (`cranelift-*`, `wasmtime-*`), `deno` (`deno_core`, `deno_runtime`,
  ext crates), `databend`, `materialize`, `nushell` — all split into workspace
  crates for exactly the compile-time and memory reasons in § 1. The common
  shape: a `*-types` / `*-proto` leaf, domain crates in the middle, a thin
  binary crate on top, with traits at the seams where a lower crate needs to
  call up.

**Take:** the leaf wire-types crate (k8s `api`, Nomad `api`), traits at the
seams instead of upward calls (k8s scheduler `framework.Handle`), and the thin
binary on top. **Reject:** splitting into separate repositories or separately
versioned modules (k8s staging) — Legion ships one binary at one version, and a
single workspace with path dependencies is all the build needs.

## 3. Jobs to be done

**Contributor**
- When I change one file in the scheduler, I want the rebuild to touch the
  scheduler and the binary, so I can stay in an edit–test loop measured in
  seconds, not minutes.
- When I build Legion on a 16 GB laptop or a CI runner, I want the build to
  fit in memory with headroom, so it does not swap, get OOM-killed, or force
  me onto `-j1`.
- When I run one crate's unit tests, I want to build only that crate and what
  it depends on, so testing the store does not wait on the datapath.

**Release engineer**
- When I cut a release, I want the shipping build to run on a stock runner,
  so the release does not depend on one oversized machine.
- When I quote the binary's size, I want the number to be what we actually
  ship on the platform people run, so the README is not off by a platform.

**Plugin author**
- When I write a plugin, I want to depend on the types and the plugin
  interface, not on all of Legion, so my plugin builds in seconds and is not
  rebuilt every time Legion's internals change.

**Operator / job author / agent:** *not in scope.* Nothing they can observe
changes: one binary, same CLI, same wire format, same on-disk formats.

### Scenarios

1. A contributor edits `src/manager/scheduler.rs` and runs
   `cargo test -p legion-scheduler`. Today that recompiles all of Legion and
   links a 200 MB test binary; at the end of phase 4 it compiles the
   scheduler crate and its tests, against `legion-types` that is already
   built.
2. CI builds `--profile dist` on a 16 GB runner, and it finishes.
3. A plugin author depends on `legion-proto` and `legion-plugin-api`, and
   their `cargo build` never compiles `rusqlite`, `openraft`, or the
   datapath.

## 4. Goals and non-goals

**Goals**
- Lower the peak memory of building Legion (every profile), and keep the
  `dist` build inside a 16 GB machine with headroom.
- Shorten the serial tail of a clean build and the front-end time of an
  incremental one, by letting independent crates compile in parallel.
- Make the subsystem boundaries real: a crate's dependencies are its allowed
  calls.
- Correct the shipped-size numbers, measured on linux/amd64.

**Non-goals**
- **More binaries.** Still one `legion` binary: manager, worker, CLI, MCP
  server. `legion-guest` stays the only second artifact.
- **A stable Rust API.** The crates are an internal build structure, versioned
  in lockstep at `0.1.0`, with no compatibility promise between them.
- **Publishing** the crates to crates.io (§ 10).
- **Behavior changes.** Every phase is a refactor with no change on the wire,
  on disk, or at the CLI.
- **Cargo features to trim the binary** (a worker-only build). Possible once the
  crates exist, but a separate decision about the product, not the build.

## 5. Background — what exists today

### 5.1 The shape of the build

The workspace today (`Cargo.toml`, `[workspace]`) is the root `legion-rs`
package plus `guest/` (`legion-guest`, the static in-guest agent) and the
scaling-plugin SDK. `src/` is one library crate (`[lib] name = "legion"`) and
`src/main.rs` the binary.

Sizes of `src/` by top-level module (lines, including tests):

| Module | Lines | What it is |
| --- | ---: | --- |
| `manager/` | 104,078 | server (44k across `server.rs` + `server/`), scheduler (14.5k), daemon, rollout, telemetry, CA, ACLs |
| `storage/` | 87,154 | `store.rs` (33.5k) + `store/` (14k), raft (15k), migrations, events |
| `worker/` | 78,368 | `netd/` datapath (25.7k), task manager (15k), daemon, forwarder |
| `structs/` | 50,477 | hand-written domain types around the generated proto types |
| `cmd/` | 38,475 | the CLI and MCP server |
| `task_plugins/` | 32,034 | exec, container, sandbox, microVM runtimes |
| `apps/` | 28,436 | autoscaler, certs, ingress, gitops apps |
| `autoscaler/` | 12,346 | |
| `artifacts/`, `volumes/`, `devices/`, … | 34,000 | |

Plus the generated `legion.rs`: 40,909 lines / 2.0 MB before the derives
expand.

### 5.2 The module graph has cycles

Counting `crate::<module>` references between top-level modules (a grep
over `src/`), the edges that block a split are the back-edges into the low
layers:

| From → to | Refs | What crosses |
| --- | ---: | --- |
| `storage` → `manager` | 96 | `manager::telemetry::metrics` (44 — the metrics registry), `scheduler::{Scheduler, PlannedPreemption, PlanMode, GangReport}`, `cluster_ca`, `mesh_ca`, `autoscale`, `scaling_inputs`, `blocked` |
| `structs` → `storage` | 20 | `Store` (eval handling in `structs/eval.rs`), `NotRaftLeader`, `arm_eval_settle`, move-policy types |
| `structs` → `manager`, `worker`, `task_plugins`, `autoscaler` | 45 | node-attribute name constants, validation hooks (`validate_job_config`, `validate_source`, balancer names), rollout handlers |
| `worker` → `manager` | 105 | `telemetry` (35), `mesh_ca` (31), `dispatcher` (17), `workload_identity` |
| `manager` → `worker` | 14 | `netd`, `daemon`, `pressure` |
| `apps` → `cmd` | 24 | `cmd::helpers`, `cmd::scaling` |
| `storage` → `cluster_tls` → `manager` | 11 + 7 | raft transport TLS |

Every one of these is a call from a lower layer up into a higher one. None is
deep: most are a constant, a metrics handle, or a type that belongs lower down
than where it was first written.

### 5.3 Where the memory goes

Measured 2026-11-08 on linux/amd64 (4 vCPU, 16 GB, no swap), clean build,
`cargo 1.97.0`, peak RSS sampled once a second:

| | Before (`9a97d54`) | After phase 1 |
| --- | ---: | ---: |
| Peak RSS, whole build | 11.4 GB | 10.1 GB |
| Peak RSS, the `legion` lib rustc | **11.1 GB** | **10.0 GB** |
| Peak RSS, fat-LTO link of the binary | 7.0 GB | 6.7 GB |
| `legion` lib compile (front end + codegen, one process) | ~15 min | ~15 min |
| Fat-LTO link | ~16 min | ~16 min |
| Clean `cargo build --profile dist`, wall | 35m 06s | 34m 44s |
| `target/dist/legion`, stripped | 91,206,208 B | 91,207,040 B |

What that says:

- **The peak is the `legion` crate's front end, not the link.** RSS climbs
  to 11 GB in the first four minutes of compiling the library (parsing,
  expansion, type and borrow checking, MIR for 465k lines), then falls to
  5–7 GB for LLVM. The fat-LTO link of the binary tops out at 7 GB. So
  changing the `dist` LTO settings would not lower the peak (calls 1); making
  the crate smaller would, and that is this plan.
- **`cargo test` doubles it.** A test build compiles the library and the
  library-with-unit-tests as two separate rustc processes, in parallel. A
  clean `cargo test --all --all-features` on this 16 GB box was OOM-killed
  (13.6 GB summed, the dev-profile `legion` lib at 9.0 GB) — after phase 1.
  Building `--lib` tests first and the rest second works; it is a workaround
  in `internal_docs/development.md`, not a fix.
- **The rest of the build is not the problem.** The ~490 dependency crates
  build in parallel and never sum past ~5 GB on four cores.
- **Phase 1 moved ~10% of the peak.** The generated code is 40,909 lines
  before its derives expand; out of the main crate, it compiles in its own
  rustc (under 4.7 GB for everything running alongside it) and pipelines
  with the start of `legion`. Size is unchanged, as a pure move should be.
- **Time is dominated by single-threaded work** — the front end of one crate,
  LLVM with `codegen-units = 1`, the fat-LTO link — with three of four cores
  idle for ~30 of the 35 minutes. Parallel crates shorten the front-end
  share; the fat-LTO link stays serial unless the shipping profile changes.

The binary is **91 MB on linux/amd64**, not the 26 MB the docs quoted. That
figure was macOS arm64, from an older and smaller tree. The ELF breakdown: `.text` 67.8
MB of machine code, `.rodata` 5.8 MB, unwind tables (`.eh_frame` +
`.gcc_except_table`) 7.6 MB, relocations 2.8 MB. Mach-O's compact unwind
info and an arm64 encoding account for some of the gap, not 3.5×; what grew
is filed in TODO.md (Cross-cutting, "Find what makes the linux/amd64 binary
91 MB").

## 6. Proposed design

### 6.1 Target crate graph

```text
                         legion (bin: src/main.rs)            ← cmd, MCP, wiring
                ┌──────────────┼──────────────────┐
         legion-manager   legion-worker      legion-apps
           │      │          │      │             │
           │  legion-scheduler  legion-datapath   │
           │      │          │                    │
           └── legion-store ─┘                    │
                  │                               │
             legion-types  ←──────────────────────┘   (domain structs, config_lang,
                  │                                    resources, constraints, devices,
             legion-core                               metrics registry, util, constants)
                  │
             legion-proto        legion-guest (exists)
```

| Crate | Takes from `src/` | Depends on |
| --- | --- | --- |
| `legion-proto` (`proto/`) | the generated code | prost, tonic, serde |
| `legion-core` | `constants`, `util`, `prelude`, `signals`, `tracing_util`, `otel`, `manager::telemetry::metrics` (the registry, not the endpoint) | proto |
| `legion-types` | `structs/` (hand-written), `config_lang`, `constraints`, `resources`, `devices`, `scheduling_index`, `cluster_tls` | core, proto, nickel |
| `legion-store` | `storage/` incl. `raft/`, `migrations`, `events`, `keyring` | types, rusqlite, openraft |
| `legion-scheduler` | `manager/scheduler*`, `rollout*`, `dispatcher`, `blocked`, `concurrency`, `gang` | types; reads the cluster through a trait the store implements |
| `legion-datapath` | `worker/netd/`, `worker/forwarder`, `gateway` | types, core |
| `legion-worker` | `worker/` (rest), `task_plugins/`, `volumes/`, `artifacts/`, content store, checkpoints | types, datapath |
| `legion-manager` | `manager/` (rest): server, daemon, ACLs, CAs, identity, telemetry endpoint | store, scheduler, types |
| `legion-apps` | `apps/`, `autoscaler/` | types, a client trait |
| `legion` | `cmd/`, `main.rs` | everything |

Store and scheduler: the scheduler is a planner over a read model
(`scheduling_index`, capacity snapshots) and returns a plan; applying it is
the store's job. Today `storage` constructs the `Scheduler` and names its
result types. After phase 4 the plan types (`PlannedPreemption`, `PlanMode`,
`GangReport`) live in `legion-types`, the store applies a plan it is handed,
and the code that runs the scheduler against the store lives in
`legion-manager`. The scheduler crate never sees SQLite.

### 6.2 How a back-edge is broken

Three moves cover every edge in § 5.2:

1. **Move it down.** A constant, a metrics handle, or a plain type that lives
   too high moves to the lowest crate that uses it (`WORKER_FLEET_ATTR`,
   `netd::kernel::NODE_ATTR`, `PlannedPreemption`, the metrics registry).
2. **Turn the upward call into a trait** the lower crate declares and the
   higher one implements, registered at startup in `legion` (`main.rs`) —
   `structs` validating a job's task config against the task plugins,
   `storage` asking the CA for a certificate.
3. **Move behavior off a type onto the subsystem that owns it.** `Eval::handle`
   (`src/structs/eval.rs`) runs evals against the `Store`; it is eval-loop
   code that sits in `structs` because `Eval` is a proto type. It moves to the
   manager as `fn handle_eval(eval: &Eval, store: &Store)`.

### 6.3 What the user sees

Nothing at runtime. Contributors see:

```bash
cargo build                          # unchanged: builds the binary
cargo test --all --locked            # unchanged: tests every crate
cargo test -p legion-scheduler       # new: just the scheduler and its deps
cargo bench --bench scheduler_bench  # unchanged (bench targets move with the crate)
```

`tests/integration/` stays one binary on the `legion` package.

## 7. Proto, struct, config, and persistence changes

None on the wire, on disk, or in job config. `proto/legion.proto` stays at the
same path (the `proto/` directory becomes the `legion-proto` package around
it), so the UI's generator, the `proto-breaking` CI job, and the RPC
classification tests that parse it are unaffected.

## 8. Implementation phases

Each phase leaves `cargo test --all --locked` green and is one PR.

**Phase 1 — `legion-proto` (SHIPPED 2026-11-08).**
The generated code moves to `proto/` as its own crate; `legion::structs`
re-exports it, so no call site changes. The handful of inherent impls on
generated types become extension traits in `legion` (the orphan rule forbids
inherent impls on another crate's types) and the redacting `Debug` impls move
with the types. `include_str!("../../proto/legion.proto")` becomes
`legion_proto::PROTO_SOURCE`. Measured result in § 5.3: −1.1 GB peak, no
size change. The `dist` profile is unchanged (calls 1).

**Phase 2 — `legion-core`.** Move the metrics registry out of
`manager::telemetry`, plus `constants`, `util`, `signals`, tracing. Removes the
largest single back-edge (44 refs `storage` → `manager::telemetry::metrics`, 35
from `worker`). Small and mechanical.

**Phase 3 — `legion-types`.** Hand-written `structs/` plus the pure domain
modules. Break `structs` → {storage, manager, worker, task_plugins,
autoscaler}: move the node-attribute constants down, move `Eval::handle` to
the manager, put job-config validation behind a `TaskConfigValidator` trait.
This is the phase that makes every later one possible.

**Phase 4 — `legion-store` and `legion-scheduler`.** Move plan types to
`legion-types`; the store stops constructing the scheduler. The two crates
build in parallel. Scheduler PRs carry `scheduler_bench` numbers before and
after (AGENTS.md); this phase must show no regression, since moving code
across a crate boundary can cost inlining (§ 11).

**Phase 5 — `legion-datapath`.** `worker/netd`, forwarder, gateway. Its
upward edges are `mesh_ca` and `telemetry`, both lower by now.

**Phase 6 — `legion-worker` and `legion-manager`.** What remains of each;
`manager` ↔ `worker` edges (`worker_plane`, `dispatcher`) settle into
`legion-types`.

**Phase 7 — `legion-apps`, and the `legion` binary crate.** `apps` stops
reaching into `cmd::helpers` (the shared client helpers move to a small
module in `legion-types` or a `legion-client`); `cmd/` and `main.rs` become
the top crate. The root `src/` is then just the binary.

## 9. Testing plan

- Every phase: `cargo test --all --locked`, `cargo clippy --all -- -D
  warnings`, `cargo fmt --all --check` — a refactor with no behavior change has
  the existing suite as its test.
- Every phase records the build footprint against the previous phase on the
  same machine: clean `cargo build` peak RSS and wall time, an incremental
  rebuild after touching one file in a leaf crate and one in a top crate, and
  the `dist` binary size. The script used for § 5.3 samples the summed RSS of
  the build's rustc processes once a second.
- Phase 4 adds `scheduler_bench` before/after.
- A test that guards the graph: once a crate exists, a `cargo metadata` check
  in CI fails if a lower crate gains a dependency on a higher one (cargo
  already rejects cycles; this catches a *new* upward edge that happens to be
  acyclic but violates the layering).

## 10. Rollout and backward compatibility

Nothing to roll out: same binary, same formats. Contributors with a checkout
rebuild once. The crates are not published: `legion-rs` declares path +
version dependencies on them (as it does on `legion-guest`), so publishing
`legion-rs` would require publishing each first, in dependency order. That is
deferred until there is a reason to publish at all.

## 11. Open questions

- **Cross-crate inlining on the hot path.** Generic and `#[inline]` code
  inlines across crates; plain functions do not without LTO. `release` uses
  thin-local LTO only. Lean: measure in phase 4 with `scheduler_bench`, and
  if the scheduler regresses, set `lto = "thin"` on `release` rather than
  sprinkle `#[inline]`.
- **Where plugins attach.** Plugins should depend on `legion-proto` plus a
  small `legion-plugin-api`, not on `legion`. Lean: do it after phase 3, when
  the plugin traits can move to `legion-types` without dragging the store.
- **`cluster_tls`** sits between `storage` (raft transport) and `manager`
  (CA). Lean: it goes to `legion-types` with its CA dependency behind a trait.

## 12. Acceptance criteria

- **Phase 1:** `cargo test --all --locked` green; `legion-proto` builds
  alone (`cargo build -p legion-proto`); the main crate's peak rustc RSS is
  lower than before, measured as in § 5.3; `cargo build --profile dist`
  completes on a 16 GB / 4 vCPU machine; README, AGENTS.md, Cargo.toml and
  `internal_docs/development.md` carry the linux/amd64 sizes.
- **Phases 2–7:** each one green, each with its footprint numbers in the PR,
  and at the end `src/` contains only the binary crate.
