# Codebase Conventions

This file captures current conventions used across Legion.

## Repository and Module Boundaries

- Manager control-plane code is under `src/manager/`.
- Worker runtime code is under `src/worker/`.
- Persistence code is under `src/storage/`.
- Shared structs and schema-facing types live under `src/structs/`.
- CLI surface is under `src/cmd/`.
- Task runtime implementations are under `src/task_plugins/`.
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
- In manager gRPC server layer, map storage and domain errors to typed gRPC statuses (`map_store_error`).

## Tracing and Diagnostics

- Prefer `tracing` macros (`info!`, `warn!`, etc.) over `println!` in runtime code.
- Use `#[instrument]` on significant async or orchestration boundaries.

## Serialization and API Surface

- Protobuf schema in `proto/legion.proto` is the source of gRPC API contracts.
- Most Rust structs use `camelCase` serde naming for API/config consistency.
- Nickel is first-class for manager/job/worker config inputs.

## Scheduler and Eval Conventions

- Use eval queue as reconciliation driver (`enqueue_eval*`) instead of ad-hoc orchestration loops where possible.
- Keep scheduling side effects conflict-safe via allocation epoch + plan submission apply.
- Keep constraint/permit behavior hard-filtered and affinity behavior soft-scored.

## Plugin Conventions

- Plugin crates must export `get` symbol returning trait object.
- Dynamic plugin paths follow `plugins/<kind>/<name>/target/release/lib<name>.<ext>`.
- Built-in implementations may coexist with dynamic plugins of same category.

## State and Persistence

- Cluster manager state is SQLite-backed and migration-managed.
- Worker runtime state is local JSON file state with PID recovery checks.
- Update in-memory read model cache when mutating scheduler-relevant DB rows.

## Testing Conventions

- Co-locate unit tests with implementation files.
- Keep integration scenarios under `tests/`.
- Prefer temp directories/databases per test for isolation.
- Run full Rust test suite for Rust changes.

## Documentation Conventions

- Keep architecture docs in `docs/`.
- When behavior changes, update docs and examples in the same change.
- Keep planned-but-not-implemented behavior clearly labeled.

## (SUGGESTIONS - DELETE ME)

1. Standardize node status vocabulary (`active` vs `ready`) to reduce state-machine ambiguity.
2. Move remaining direct reconciliation paths to explicit eval kinds for one control-loop model.
3. Add a dedicated docs index page in `docs/` to avoid scattered entry points.
4. Define a strict schema for `config_attrs` keys (like `workerAddress`, `resources`) and validate centrally.
5. Add explicit policy for when to add scheduler benchmarks alongside scheduler logic changes.

