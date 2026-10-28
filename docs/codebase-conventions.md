# Codebase Conventions

This page lists the conventions used across the Legion codebase.

## Repository and Module Boundaries

- Manager control-plane code is under `src/manager/`.
- Worker runtime code is under `src/worker/`.
- Persistence code is under `src/storage/`.
- Shared structs and schema-facing types live under `src/structs/`.
- CLI surface is under `src/cmd/`.
- Task runtime implementations are under `src/task_plugins/`.
- Embedding WASM is under `src/wasm/`: the engine, limits, failure
  descriptions, guest memory, the WASI preview1 shim, and module fetching.
  An embedder (the datapath's proxy-wasm host in `src/worker/netd/plugins/`)
  brings its own ABI on top.
- Apps ([apps.md](apps.md)) are split by side: the manifest, record,
  render, and build-marker logic are pure functions in
  `src/structs/app.rs`; the records in `src/storage/app_store.rs`; the
  reconciler in `src/manager/apps.rs` and the RPCs in
  `src/manager/server/app_rpcs.rs`; the bundled manifests and the in-task
  framework behind `legion app run` in `src/apps/` (a bundled app is one
  entry in `apps::BUNDLED`).
- Plugin crates live under `plugins/<kind>/<name>/`.

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
gated egress proxy, ingress listeners) lives in `src/worker/netd/` and
`src/worker/forwarder.rs`.

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

- Every object store is opened through `src/storage/objects/`: `StoreUrl`
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
