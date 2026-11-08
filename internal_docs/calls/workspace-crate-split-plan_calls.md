# Judgement calls: workspace crate split

Calls made planning and executing
[workspace-crate-split-plan.md](../plans/workspace-crate-split-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Fix build memory by shrinking the crate, not by loosening the `dist` profile

The memory problem was reported against building the binary, and the
shipping profile is the expensive one: fat LTO, one codegen unit, 35 minutes
on four cores. The `dist` profile is unchanged; the fix is the crate split,
starting with phase 1.

```toml
[profile.dist]
inherits = "release"
lto = "fat"          # unchanged
codegen-units = 1    # unchanged
strip = "symbols"
```

**Alternatives.** (a) `lto = "thin"` with `codegen-units = 16` for `dist`
— the existing `distfast` profile — which builds in roughly a third of the
time. (b) Keep fat LTO but raise `codegen-units`, trading some size for a
parallel back end.

**Trade-off.** The shipping build stays at ~35 minutes and still needs ~10 GB
free; a release still cannot be cut on an 8 GB machine.

**Why (performance, measured).** Sampling the build once a second
([plan § 5.3](../plans/workspace-crate-split-plan.md#53-where-the-memory-goes))
put the peak in the `legion` crate's front end — 11.1 GB, four minutes into
compiling the library, before LLVM starts — while the fat-LTO link of the
whole binary topped out at 7.0 GB. No LTO or codegen-unit setting touches the
front end, so (a) and (b) would give up binary size and buy back time but not
memory. The front end scales with the size of the one crate, and the split is
the only change that shrinks it. Revisit (a) if release wall-clock becomes the
complaint: it is a time lever, and a real one.

## 2. The first crate out is the generated proto code, not storage

The request listed storage/raft, scheduler, datapath, apps, and cmd. Phase 1
extracts none of those: it extracts `legion-proto`, the code `tonic-build`
generates from `legion.proto`.

**Alternatives.** (a) Start with `legion-store`, the first crate named and the
largest after the manager (87k lines). (b) Start with the scheduler, the crate
contributors edit most.

**Trade-off.** Phase 1 does not yet give anyone `cargo test -p
legion-scheduler`; the subsystem crates the request asked for are phases 4–7.

**Why (simplicity, performance).** A crate boundary cannot cross a cycle, and
every subsystem named is inside one: `storage` calls into `manager` 96 times
(§ 5.2 of the plan), `structs` calls into `storage`, `manager`, `worker`,
`task_plugins`, and `autoscaler`. Extracting `storage` first means breaking
those edges and moving the store in one PR — the kind of change that sits in
review for a week and conflicts with everything. The generated code has no
edges up at all, so it moves cleanly, and it is the single biggest block of
front-end work in the crate: 640 types, each with expanded `serde`, `prost`,
`Clone`, `PartialEq`, and `Debug` impls, plus the gRPC stubs. It is also the
crate plugins and external clients actually need, which is the k8s `api` /
Nomad `api` shape.

## 3. `proto/` becomes the crate, rather than `crates/legion-proto/` reaching into it

The package lives in the directory that already holds `legion.proto`:
`proto/Cargo.toml`, `proto/build.rs`, `proto/src/lib.rs`.

**Alternatives.** A `crates/` directory for all new crates, with
`crates/legion-proto/build.rs` compiling `../../proto/legion.proto`.

**Trade-off.** New crates will not all sit under one `crates/` directory:
`guest/` and `proto/` are at the root, later ones are planned under `crates/`.

**Why (simplicity).** `proto/legion.proto` is a path many things know: the
UI's ConnectRPC generator, the `proto-breaking` CI job (`git show
"$BASE_SHA:proto/legion.proto"`), `buf.yaml`, the RPC-classification tests
that read the file, and every doc that cites it. Keeping it where it is costs
nothing. And a crate whose build script reaches outside its own directory
cannot be packaged (`cargo package` only takes files under the manifest), so
the `crates/` layout would have made the proto crate unpublishable for no
benefit.

## 4. Inherent impls on generated types become extension traits, not moves

The orphan rule forbids an inherent `impl Eval { … }` once `Eval` is defined in
another crate. Four types had one: `Eval` (the whole eval handler, 800 lines
calling the `Store`), `Submission::hash`, `LedgerNode::into_entry`, and
`LedgerAllocation::into_allocation`. Each became a trait in the same file
(`EvalExt`, `SubmissionExt`, `LedgerNodeExt`, `LedgerAllocationExt`), so call
sites read the same:

```rust
use crate::structs::EvalExt;
eval.handle(&store)?;                       // unchanged
Eval::error_is_plan_conflict(&err)          // unchanged
```

**Alternatives.** Move the behavior to where the plan says it belongs —
`Eval::handle` to the manager as `handle_eval(&eval, &store)` — in this PR.

**Trade-off.** A trait whose only implementor is one type is a seam with no
second side; `EvalExt` in particular is eval-loop code still sitting in
`structs`. Phase 3 has to move it anyway.

**Why (simplicity).** Phase 1 stays a mechanical diff a reviewer can check by
eye: the method bodies did not change. The redacting `Debug` impls for the
three key-carrying messages are the exception — a foreign trait on a foreign
type has no extension-trait form, so they moved into `legion-proto`
(`proto/src/redacted.rs`), with a test that the key bytes never print or
serialize.

## 5. `legion::structs` re-exports the proto crate wholesale

`src/structs/mod.rs` now starts `pub use legion_proto::*;` where it used to
`include_proto!`. No call site names `legion_proto` except the three that read
the `.proto` text (`legion_proto::PROTO_SOURCE`).

**Alternatives.** Rewrite every `crate::structs::Job` that is a wire type to
`legion_proto::Job`, so a reader can tell generated types from hand-written
ones by path.

**Trade-off.** The two still share a namespace, so the boundary is visible in
`Cargo.toml` but not at the call site.

**Why (simplicity).** Thousands of import lines would change for no build
benefit, and phase 3 moves the hand-written half of `structs` into
`legion-types` anyway — the right time to decide what that crate re-exports.

## 6. "Before and after" is this branch's base and this branch, on linux/amd64

The request was to build `dist` before and after so the stale numbers in
`Cargo.toml` could be corrected. The numbers there (`43.7 MB` / `26.0 MB`) were
measured on macOS arm64 (`__LINKEDIT`, `__TEXT` are Mach-O segments).

**Alternatives.** Build `--release` and `dist` once and call those before and
after, which is what the `Cargo.toml` table compares.

**Trade-off.** Two `dist` builds and a `release` build on a 4-vCPU box. The
macOS numbers are dropped rather than kept beside the Linux ones, because
nobody re-measures both and a two-platform table goes stale twice as fast.

**Why.** Both questions needed answering: what the shipping binary is on the
platform people deploy (linux/amd64, the TODO item "Re-measure binary size on
linux/amd64"), and what this change does to it and to the build's memory.
