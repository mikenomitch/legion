# Plugin System

This document describes Legion's plugin architecture, loading behavior, and trade-offs.

## Why Legion Uses In-Process `cdylib` Plugins

Legion currently uses Rust `cdylib` plugins loaded in-process via `libloading`.

This approach was chosen because it is:

- **Fast**: no RPC boundary between core scheduler/worker and plugin code.
- **Simple to deploy**: plugins are shared libraries on disk under a predictable directory.
- **Ergonomic for Rust extension authors**: plugin authors implement traits and export a `get` constructor.
- **Flexible**: task kinds, scheduling policies, node checks, and rollout strategies can each be extended.

## Downsides and Risks to Watch

The primary trade-off of in-process plugins is reliability/isolation:

- **Plugin panics can terminate the host process** (manager/worker) if not contained.
- **Memory safety/ABI mistakes are dangerous** because dynamic loading requires unsafe FFI boundaries.
- **Version drift can break loading** if plugin and host disagree on API expectations.

To reduce this risk, Legion validates plugin manifests (kind/name/API version) before loading.

## How Plugin Loading Works

Legion resolves plugin roots from `LEGION_PLUGIN_DIR` (default: `plugins/` under repo root).

Dynamic plugin loading pattern:

- Plugins are Rust `cdylib` libraries.
- Exported symbol must be `get`.
- Legion loads plugin `.so/.dylib/.dll` with `libloading`.

Common directory pattern:

- `plugins/<kind>/<name>/target/release/lib<name>.<ext>`

### Plugin Manifest (Required for Dynamic Plugins)

Each dynamic plugin root now includes a `plugin.json` manifest.

Example:

```json
{
  "kind": "tasks",
  "name": "noop",
  "legionApiVersion": 1
}
```

Fields:

- `kind`: plugin category directory (`tasks`, `scheduling`, `node_checks`, `rollouts`, `artifacts`).
- `name`: plugin name directory and library basename.
- `legionApiVersion`: plugin API version expected by the host.

Before loading the dynamic library, Legion validates:

1. `plugin.json` exists.
2. `kind` matches the expected plugin category.
3. `name` matches the requested plugin name.
4. `legionApiVersion` equals the host-supported version.

If any check fails, plugin loading fails with contextual errors.

## Plugin Types Implemented Today

## 1) Task Kind Plugins

Purpose: run workload tasks on workers.

- Built-in implementations in-process:
  - `exec` (`src/task_plugins/exec.rs`)
  - `container` (`src/task_plugins/container.rs`)
- Dynamically loaded examples/shipped plugins:
  - `noop` (`plugins/tasks/noop/`)

Entry trait: `TaskPlugin` in `src/task_plugins/mod.rs`.

## 2) Scheduling Policy Plugins

Purpose: select node placement strategy.

- Built-in policies:
  - `bin_pack`
  - `spread`
- Dynamic example plugin:
  - `random` (`plugins/scheduling/random/`)

Entry trait: `SchedulingPolicy` in `src/manager/scheduler/policy.rs`.

## 3) Node Check Plugins

Purpose: augment worker fingerprint and node health checks.

- Built-in checkers/fingerprinters:
  - arch
  - os
  - kitchen-sink fingerprint
  - disk health checks
- Dynamic example plugin:
  - `example` (`plugins/node_checks/example/`)

Entry trait: `NodeChecker` in `src/worker/node_check.rs`.

## 4) Rollout Strategy Plugins

Purpose: transform/resolve rollout config before rollout execution.

- Built-in strategy:
  - `default`
- Dynamic example plugin:
  - `slow_start` (`plugins/rollouts/slow_start/`)

Entry trait: `RolloutStrategyPlugin` in `src/manager/rollout_strategy.rs`.

## 5) Artifact Fetcher Plugins

Purpose: serve artifact source schemes beyond the built-ins
(`http`/`https`, `file`, `git`, `s3`, `oci`). See
[docs/artifacts.md](artifacts.md) for the artifact system itself.

- Built-in fetchers register in `FetcherRegistry::with_builtins()`
  (`src/artifacts/fetcher.rs`, implementations in `src/artifacts/fetchers/`).
- Dynamic example plugin:
  - `demo` (`plugins/artifacts/demo/`) — serves `demo://`, materializing the
    URL's `content` query parameter as a staged file.

Entry trait: `ArtifactFetcher` in `src/artifacts/fetcher.rs`.

Contract specifics (the rest follows the common pattern above):

- Directory layout: `plugins/artifacts/<scheme>/` with a `plugin.json` of
  `{"kind": "artifacts", "name": "<scheme>", "legionApiVersion": 1}` and the
  dylib at `plugins/artifacts/<scheme>/target/release/lib<scheme>.<ext>`.
- Exported symbol: `get() -> Box<dyn ArtifactFetcher>` (the usual `get`
  constructor, returning the fetcher trait object).
- The trait is deliberately **blocking** (`fetch` streams bytes into an
  `ArtifactSink`, which hashes, counts, and enforces size limits): fetchers
  run on worker staging threads, never the async runtime, and a blocking
  trait keeps the dylib ABI free of async-trait hazards. `resolve()` is an
  optional second method that pins mutable references to digests for
  `resolve: 'pin'`.
- **Registry resolution order: built-ins first, then dynamic plugins.** A
  plugin is only consulted (lazily loaded, manifest validated) when no
  built-in serves the requested scheme, and loading fails if the plugin's
  `schemes()` does not include the scheme it was loaded for.
- Auth resolution, checksum verification, unpacking, retries, and status
  reporting are all shared pipeline code in the worker core — a fetcher
  plugin only moves bytes. Custom schemes must also be added to the
  cluster's `artifacts.allowedSchemes` (and the worker-side allowlist if
  customized).

## Build Behavior

`build.rs` behavior:

- Protobuf always compiles.
- Plugins build only when `LEGION_BUILD_PLUGINS=1`.
- Plugin builds are skipped if `LEGION_SKIP_PLUGIN_BUILD` is set.
- When `build.rs` builds plugins, it uses an isolated `CARGO_TARGET_DIR`
  under `target/plugin-build/...` for compile isolation.
- Runtime dynamic loading still resolves from plugin crate-local paths such as
  `plugins/<kind>/<name>/target/release/lib<name>.<ext>`.

This allows normal core builds without paying plugin build cost.

## Planned Plugin Categories (Not Implemented Yet)

Based on `docs/TASK-LIST.md`, `README.md`, and `docs/differentiators.md`, these are intended but not present as first-class plugin frameworks today:

- Networking/CNI plugins.
- Volume plugins.
- Artifact acquisition plugins (image/file/etc lifecycle).
- Command/API endpoint extension plugins.
- Logging/observability pipeline plugins.
- Eval hooks / scheduler reaction hooks.
- UI extension plugins.
- Job submit hook plugins.

Some similarly named functionality exists in non-plugin form today (for example, resource defaults/validation in `ResourcePolicy`).
