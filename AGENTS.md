# Repository Guidelines

## Project Structure & Module Organization
- Source lives in `src/`: CLI in `cmd/`, orchestration in `manager/` and `worker/`, storage helpers in `storage/`, TLS helpers in `certs.rs`, tracing utilities in `tracing_util.rs`, generated proto types in `structs.rs`, constants in `constants.rs`.
- Protobuf schema: `proto/legion.proto` compiled by `build.rs` via `tonic-build`.
- Configs and examples: `configs/` (manager, worker, TLS dev certs, job demos).
- When adding anything to job config, include it in `configs/jobs/maximal.jsonnet`.
- Plugins: `plugins/<kind>/<name>/` built as `dylib` during Cargo build.
- Tests sit beside modules; integration assets under `tests/`.

## Build, Test, and Development Commands
- Build crate and plugins: `cargo build` (requires `protoc`).
- Release build: `cargo build --release`.
- Run tests: `cargo test --all --locked`.
- Run manager: `cargo run -- start manager -c configs/manager.jsonnet`.
- Run worker: `cargo run -- join <TOKEN>` or `cargo run -- start worker <TOKEN>`.
- Jobs: `cargo run -- job run configs/jobs/demo.jsonnet`; list with `cargo run -- job list -j`.

## Coding Style & Naming Conventions
- Rust 2021, 4-space indent, snake_case modules/files, UpperCamelCase types, SCREAMING_SNAKE_CASE constants.
- Prefer `tracing` macros; add `#[instrument]` on async boundaries.
- Format with `cargo fmt --all`; lint with `cargo clippy --all -- -D warnings`.

## Testing Guidelines
- Use Rust `#[cfg(test)]` unit tests near code; integration data in `tests/`.
- Name tests descriptively with `_test` modules/functions.
- Run full suite via `cargo test --all --locked`; avoid mutating shared fixtures.

## Commit & Pull Request Guidelines
- Commits: short, imperative summaries (e.g., "Refactor store helpers").
- PRs: include summary, rationale, scope; link issues; note UX or CLI changes with before/after where relevant; ensure CI (fmt, clippy, tests) passes.

## Security & Configuration Tips
- `protoc` must be available locally; CI installs `protobuf-compiler`.
- Default manager address `http://127.0.0.1:4747`; insecure listener `http://127.0.0.1:4748`.
- Dev TLS certs live in `configs/tls/`; do not use in production.

## Docs, Plan, and To Dos
- Docs, Plan, and To Dos live in `docs/`
- `docs/NEXT_UP.md` contains the immediate todos for the current task that the human developer is working on
- `docs/TASK-LIST.md` contains a higher-level To Do list
- `docs/differentiators.md` contains a list of reasons that legion is different from K8s and Nomad
