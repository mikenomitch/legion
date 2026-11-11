# Codebase Conventions

This page lists the conventions used across the Legion codebase.

## Crates

Legion builds one binary from a Cargo workspace of layered library crates.
A crate may depend only on the crates below it:

| Crate | Directory | What is in it |
| --- | --- | --- |
| `legion-rs` (the `legion` binary and library) | `src/` | The CLI and MCP server (`cmd/`), the bundled apps' code (`apps/`), the gateway |
| `legion-manager` | `crates/manager/` | The manager: gRPC server, eval loop, scheduler, rollouts (`manager/`), and the state store with its raft layer (`storage/`) |
| `legion-worker` | `crates/worker/` | The worker and its datapath (`worker/`), task runtimes and jobspec validation (`task_plugins/`), artifacts, content publishing, the object-store client, the WASM embedding layer, the dylib plugin loader |
| `legion-common` | `crates/common/` | Domain types and the jobspec (`structs/`), the config language, autoscaling policy (`autoscaler/`), bundled app manifests, volumes, devices, cluster TLS, the metrics registry |
| `legion-proto` | `proto/` | The messages and gRPC stubs generated from `legion.proto` |

`legion-guest` (`guest/`) is the in-guest agent, which also ships as its own
static binary. It depends on nothing else of Legion's; `legion-common`, the
worker, and the binary use it.

`legion-isolate` (`crates/isolate/`) is the [isolate](task-runtime-isolate.md)
runtime's host process and module compiler, built as its own binary,
`legion-isolate`. It is the only crate that links the WebAssembly engine
(wasmtime, pinned to an exact version), and it depends on `legion-proto`
alone, for the node-local protocol the worker speaks to it
(`proto/isolate_host.proto`). Nothing of Legion's depends on it: the worker
starts the binary and talks to it over inherited sockets. Keeping it apart
keeps the engine out of the worker's address space and build, lets the host
confine itself without affecting the worker, and lets an engine upgrade ship
as a new binary. It is a workspace member but not a default member, so a
bare `cargo build` does not build it: use `cargo build -p legion-isolate`.

The order is `proto` → `common` → `worker` → `manager` → `legion`. The manager
sits above the worker because it validates jobs against the task runtimes
and can run a worker in its own process; the worker never calls into the
manager or the store.

The split exists for the build. rustc compiles a crate in one process whose
memory grows with the crate, so as one 470,000-line crate Legion needed
9 GB to build, and its test build did not fit in 16 GB. As separate crates
the largest compile is under 4 GB (4.5 GB with its tests), a change to the
scheduler does not recompile the worker, and `cargo test -p legion-worker`
builds and runs the worker's tests without building the manager.

### Paths stay the same

Each crate re-exports the modules of the crates below it at its root, so
code names things the way it did when Legion was one crate:
`crate::structs::Job` and `crate::prelude::*` work in every crate,
`crate::worker::netd` works in the manager, and `legion::manager`,
`legion::worker`, and `legion::structs` work in the integration tests,
benchmarks, and plugins. A module lives in exactly one crate; the re-export
is only a path.

What does not carry across a crate boundary:

- **`pub(crate)`.** An item another crate uses is `pub`. Keep `pub(crate)`
  for what really is internal to one crate.
- **Inherent impls.** `impl Type { .. }` must be in the crate that defines
  `Type`. Behavior on a lower crate's type that needs an upper crate is an
  extension trait beside the code that needs it: `EvalExt`
  (`crates/manager/src/manager/eval.rs`) for the generated `Eval`,
  `ContentStoreTargetExt` (`crates/worker/src/content_publish.rs`) for the
  `contentStore` config block. The same rule puts a foreign trait impl on a
  generated type (the redacting `Debug` for key-carrying messages) in
  `proto/src/`.
- **`#[cfg(test)]` helpers.** A test helper used by another crate's tests is
  compiled always and says so in its doc comment
  (`legion_common::test_scratch`, `mesh_ca::issue_node_cert`).

### When a lower crate needs to know something from an upper one

It cannot call up. The three ways this is handled, in order of preference:

1. **Move it down.** A constant, a plain type, or a pure function that both
   layers need belongs in the lower one: what a manager and a worker must
   compute identically is in `structs/worker_protocol.rs`, the payload a
   worker runs is built in `structs/task_payload.rs`, the metrics registry
   is `telemetry.rs` in `legion-common`.
2. **Keep a list on each side, with a test that they match.** Jobspec
   validation accepts the load-balancing strategies in
   `structs::BALANCE_STRATEGIES`; the datapath builds them from
   `worker::balance::BALANCERS`; a test beside the registry fails when the
   two differ. Bundled apps are the same: manifests in
   `crates/common/src/bundled_apps/`, bodies in `src/apps/`, one test.
3. **A trait the upper crate implements**, when the lower crate has to call
   behavior it cannot hold. Use it last: it hides a dependency.

### Where things live

- Manager control-plane code is under `crates/manager/src/manager/`;
  persistence under `crates/manager/src/storage/`.
- Worker runtime code is under `crates/worker/src/worker/`; task runtime
  implementations under `crates/worker/src/task_plugins/`.
- Shared structs and schema-facing types live under
  `crates/common/src/structs/`. The generated types are re-exported there, so
  Legion code names them as `crate::structs::Job` like any other struct.
- CLI surface is under `src/cmd/`.
- Embedding WASM is under `crates/worker/src/wasm/`: the engine, limits,
  failure descriptions, guest memory, the WASI preview1 shim, and module
  fetching. An embedder (the datapath's proxy-wasm host in
  `crates/worker/src/worker/netd/plugins/`) brings its own ABI on top.
- Apps ([apps.md](apps.md)) are split by side: the manifest, record,
  render, and build-marker logic are pure functions in
  `crates/common/src/structs/app.rs`; the bundled apps' manifests and the
  record a cluster seeds each with in `crates/common/src/bundled_apps.rs`;
  the records in `crates/manager/src/storage/app_store.rs`; the reconciler
  in `crates/manager/src/manager/apps.rs` and the RPCs in
  `crates/manager/src/manager/server/app_rpcs.rs`; and the in-task framework
  behind `legion app run`, with each bundled app's body, in `src/apps/`.
- Plugin crates live under `plugins/<kind>/<name>/`. They are outside the
  workspace, depend on `legion-rs`, and use the `legion::` paths.

### Tests across crates

Unit tests sit beside the code and can use anything in their own crate and
the crates below. A test that needs a crate *above* the code it exercises
(a worker's ticket verifier against the manager's issuer, a worker's
heartbeat loop against a real manager) is a module of the integration
binary, `tests/integration/` (see
[testing-and-benchmarks.md](testing-and-benchmarks.md#test-layout)).

## Naming and Style

- Rust 2021.
- 4-space indentation.
- `snake_case` for files/modules/functions.
- `UpperCamelCase` for types.
- `SCREAMING_SNAKE_CASE` for constants.

## Error Handling

- Use `anyhow::Result` (`R<T>` alias) for fallible flows.
- Attach context on fallible boundaries with `Context`.
- In the manager gRPC server layer, map storage and domain errors to typed gRPC statuses (`map_store_error`).

## Tracing and Diagnostics

- Prefer `tracing` macros (`info!`, `warn!`, etc.) over `println!` in runtime code.
- Use `#[instrument]` on significant async or orchestration boundaries.
- CLI command handlers (`src/cmd/`) use `#[instrument(skip_all)]` without
  `err`. The binary prints a returned error once, on stderr. An `err` event
  would print it a second time as a colored tracing line on the console.

## Serialization and API Surface

- Protobuf schema in `proto/legion.proto` is the source of gRPC API contracts.
- Most Rust structs use `camelCase` serde naming for API/config consistency.
- Nickel is first-class for manager/job/worker config inputs.

## Scheduler and Eval Conventions

- Use the eval queue (`enqueue_eval*`) to drive reconciliation instead of ad-hoc orchestration loops where possible.
- Keep scheduling side effects conflict-safe through the allocation epoch and plan submission apply.
- Constraints and permits are hard filters. Affinities are soft scores.

## Plugin Conventions

- Plugin crates must export a `get` symbol that returns a trait object.
- Dynamic plugin paths follow `plugins/<kind>/<name>/target/release/lib<name>.<ext>`.
- Built-in implementations can coexist with dynamic plugins of the same category.

## Datapath Conventions

The node datapath (connect upstreams, VIPs and per-allocation resolvers, the
gated egress proxy, ingress listeners) lives in `crates/worker/src/worker/netd/` and
`crates/worker/src/worker/forwarder.rs`.

- The worker talks to the datapath only through `netd::Netd`: it sends
  `NetdInput`s (the desired set, endpoint feed, and policy assignments from an
  acknowledged heartbeat, plus the answers to fetches), asks `wants()` what to
  fetch from the manager, and drains a `NetdReport` (denials, service-graph
  samples, per-allocation status) for the next heartbeat. Nothing outside
  `netd` holds the forwarder or its context.
- How an allocation is attached — host loopback, a gated namespace, a gated
  sandbox guest over vsock, or a TAP-wired guest — is one
  `netd::attach::Attachment`, decided once per allocation and carried by every
  listener spec. It says where a listener binds and whether it fails closed.
  A new capture mode is a variant plus its arms in `Attachment::listen` and
  `Attachment::task_dial`.
- Listeners bind a `netd::listen::ListenAddr` (TCP, optionally inside a
  namespace, or a VMM's vsock host socket) and run one accept loop,
  `spawn_accept_loop`. Outbound TCP from the datapath goes through
  `netd::listen::dial_tcp`.
- Protocols come from libraries where one fits: hyper for HTTP, hickory-proto
  for DNS messages, rustls for TLS. PROXY protocol v2 and the VMM's
  vsock `CONNECT` handshake are small and written by hand.

## Object Storage Conventions

- Every object store is opened through `crates/worker/src/objects/`: `StoreUrl`
  parses `s3://` and `file://` URLs and owns the key layout under them,
  `resolve_principal_with` is the one AWS credential chain, and `open` /
  `open_s3` return `Arc<dyn ObjectStore>`. No other module builds an S3
  client or matches a store URL's scheme.
- What differs between consumers is an `OpenOptions` field (conditional
  puts, the region default, whether the process's other `AWS_*` settings
  apply, whether an ambiguous ambient environment falls through), never a
  second code path.
- A new backend (`gs://`, `az://`) is an `object_store` Cargo feature, a
  `Scheme` arm, a `Backend` variant built in `open`, and an
  `ObjectCredential` variant.

## State and Persistence

- Cluster manager state is SQLite-backed and migration-managed.
- Worker runtime state is local JSON file state with PID recovery checks.
- Update the in-memory read model cache when you mutate scheduler-relevant DB rows.

## Testing Conventions

- Co-locate unit tests with implementation files.
- Keep integration scenarios under `tests/`.
- Prefer temp directories/databases per test for isolation.
- Run the full Rust test suite for Rust changes.

## Documentation Conventions

- Keep architecture docs in `docs/`.
- When behavior changes, update docs and examples in the same change.
- Keep planned-but-not-implemented behavior clearly labeled.
