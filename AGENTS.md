# Repository Guidelines

## Project Structure & Module Organization
- Source: `src/` with key modules: `cmd/` (CLI), `manager/`, `worker/`, `storage/`, `certs.rs`, `tracing_util.rs`, `structs.rs` (generated types), `constants.rs`.
- Protocol buffers: `proto/legion.proto` (compiled by `build.rs` via tonic-build).
- Configs & examples: `configs/` (manager, workers, TLS, jobs).
- Plugins: `plugins/<kind>/<name>/` (built as `dylib` during build).
- Tests: unit tests co-located in modules; integration assets under `tests/`.

## Build, Test, and Development Commands
- Build: `cargo build` (requires `protoc`). Builds crate `legion` and plugins.
- Release: `cargo build --release`
- Test: `cargo test --all --locked`
- Run manager: `cargo run -- start manager -c configs/manager.jsonnet`
- Run worker (join): `cargo run -- join <TOKEN>` or `cargo run -- start worker <TOKEN>`
- Jobs: `cargo run -- job run configs/jobs/demo.jsonnet`; list: `cargo run -- job list -j`
- Nodes: `cargo run -- node list` | info/update via subcommands
- Join token: `LEGION_ADDR=http://127.0.0.1:4748 cargo run -- join-token --ttl 1h --type default`

## Coding Style & Naming Conventions
- Language: Rust 2021; 4-space indent; snake_case for modules/files; UpperCamelCase for types; SCREAMING_SNAKE_CASE for constants.
- Formatting: `cargo fmt --all`; Linting: `cargo clippy --all -- -D warnings`.
- Tracing: prefer `tracing` macros and `#[instrument]` for async boundaries.
- Keep modules cohesive (e.g., worker checks in `src/worker/node_check/…`).

## Testing Guidelines
- Framework: Rust `#[cfg(test)]` unit tests; run with `cargo test --all --locked`.
- Place tests near code; name with `_test` modules/functions.
- If a test needs data, use `tests/` assets (e.g., SQLite fixtures) and avoid mutating repo state.

## Commit & Pull Request Guidelines
- Commits: imperative, concise (e.g., "Refactor store helpers", "Install protoc for CI tests").
- PRs: include summary, rationale, and scope; link issues; add CLI examples and before/after notes when UX changes; ensure CI passes.

## Security & Configuration Tips
- Protobuf compiler (`protoc`) is required locally (CI installs `protobuf-compiler`).
- Defaults: manager `http://127.0.0.1:4747`, insecure `http://127.0.0.1:4748`; config path defaults to `configs/manager.jsonnet`.
- TLS dev certs live under `configs/tls/`; do not use in production.
