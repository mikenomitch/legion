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
- Always run Rust tests unless the change only touches the front end or documentation.

## Commit & Pull Request Guidelines
- Commits: short, imperative summaries (e.g., "Refactor store helpers").
- PRs: include summary, rationale, scope; link issues; note UX or CLI changes with before/after where relevant; ensure CI (fmt, clippy, tests) passes.

## Security & Configuration Tips
- `protoc` must be available locally; CI installs `protobuf-compiler`.
- Default manager address `http://127.0.0.1:4747`; insecure listener `http://127.0.0.1:4748`.
- Dev TLS certs live in `configs/tls/`; do not use in production.

## Tracing Guidance
- Use `#[instrument]` on async or queue/loop boundaries (RPC handlers, scheduler entrypoints, eval worker loops).
- Include stable IDs in span fields (`job_id`, `job_config_id`, `node_id`, `eval_id`, `worker_id`) and avoid logging giant JSON blobs.
- Keep high-frequency heartbeats and polling logs at `debug` unless the event changes state, errors, or indicates data loss.
- Never trace secrets/tokens (join tokens, raw cert material, KV lock IDs, env var values that may contain credentials).
- Prefer concise lifecycle logs (`started`, `planned`, `applied`, `failed`, `recovered`) over per-iteration noise.

## Agent-Friendly Contributions
Legion is operated by humans **and** by autonomous agents (via the CLI, gRPC, and
the MCP server). Keep these surfaces agent-friendly when contributing. See
`docs/agent-readiness.md` for the full picture.

- **Structured output is mandatory.** Every CLI command must accept `--json`/`-j`
  (`crate::cmd::helpers::json_flag`) and emit the standard envelope via
  `crate::cmd::output`: `output::print_success(&data)` on success, and route gRPC
  calls through `output::resolve(format, action, result)` so failures become the
  typed `{ ok: false, error: { code, message, hint, retryable } }` envelope. Do
  not hand-print ad-hoc JSON.
- **Keep the capability catalog in sync.** When you add, rename, or remove a CLI
  command or a user-facing gRPC method, update `catalog()` in
  `src/cmd/capabilities.rs` (name, summary, `mutates`, `read_only_safe`). Mutating
  operations must have `read_only_safe: false` (enforced by a unit test).
- **Expose agent-relevant operations over MCP.** If an operation is useful to an
  agent, add a tool in `src/cmd/mcp.rs`. Read-only tools are always listed;
  mutating tools must set `mutates: true` so they are gated behind `--allow-write`.
  Never write logs to stdout in MCP mode — stdout is the JSON-RPC stream (use
  `init_tracing_with_options(.., console_to_stderr = true)`, already wired for
  `mcp serve`).
- **Mutating RPCs should be retry-safe.** New mutating manager RPCs should accept
  an optional `idempotency_key` and report `deduplicated`, using
  `Store::idempotency_lookup` / `Store::idempotency_record` (see RunJob/StopJob/
  DestroyJob for the pattern).
- **Prefer read-only-by-default and dry-runs.** Anything that can offer a
  validate/plan/preview path (like `job validate` / `job plan`) should, so agents
  can self-correct before mutating the cluster.
- **Document new agent surfaces** in `docs/agent-readiness.md`.

## Docs, Plan, and To Dos
- Docs, Plan, and To Dos live in `docs/`
- `docs/agent-readiness.md` describes the agent-facing surfaces (MCP, structured
  output, capabilities, validate/schema, idempotency, wait, saved queries)
- `docs/NEXT_UP.md` contains the immediate todos for the current task that the human developer is working on
- `docs/TASK-LIST.md` contains a higher-level To Do list
- `docs/differentiators.md` contains a list of reasons that legion is different from K8s and Nomad
- Every gRPC method and every request/response attribute must be documented with in-depth, public-facing proto comments in `proto/legion.proto` and treated as the source of truth for API behavior.
- Architecture and behavior docs:
  - `docs/architecture-managers-workers.md`
  - `docs/plugin-system.md`
  - `docs/scheduler-and-eval-loop.md`
  - `docs/jobs-tasks-and-scheduling.md`
  - `docs/task-kind-plugins.md`
  - `docs/task-kind-exec.md`
  - `docs/task-kind-container.md`
  - `docs/task-kind-noop.md`
  - `docs/state-store.md`
  - `docs/observability.md`
  - `docs/ui-architecture.md`
  - `docs/kv-store.md`
  - `docs/testing-and-benchmarks.md`
  - `docs/codebase-conventions.md`
  - `docs/agent-readiness.md`
