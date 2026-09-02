# Plugin System

This document describes Legion's plugin architecture, loading behavior, and trade-offs.

## Why Legion Uses In-Process `cdylib` Plugins

Legion currently uses Rust `cdylib` plugins loaded in-process via `libloading`.

This approach was chosen because it is:

- **Fast**: no RPC boundary between core scheduler/worker and plugin code.
- **Simple to deploy**: plugins are shared libraries on disk under a predictable directory.
- **Ergonomic for Rust extension authors**: plugin authors implement traits and export a `get` constructor.
- **Flexible**: task runtimes, scheduling policies, node checks, rollout strategies, and artifact fetchers can each be extended.

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
  "legionApiVersion": 2
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

### Version history

The version is shared by every plugin category, because all plugins link the
`legion` crate and pass Rust types across the boundary — a layout change in one
shared struct invalidates any prebuilt library, whatever the category.

| Version | Change |
| --- | --- |
| 2 | `TaskRuntime` renamed to `TaskInstance` and `RuntimeVolume` to `MountedVolume`; `TaskInstance` gained `volumes`. |
| 1 | Initial. |

**Rebuild your plugins when this number changes.** A library built against an
older version whose manifest still claims the current one is not caught by the
manifest check, and a struct-layout mismatch across the `dylib` boundary is
undefined behavior — in practice a `SIGBUS` at first use, not a clean error.
This is why the version is bumped for layout changes and not only for signature
changes.

## Plugin Types Implemented Today

## 1) Task Runtime Plugins

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
  `{"kind": "artifacts", "name": "<scheme>", "legionApiVersion": 2}` and the
  dylib at `plugins/artifacts/<scheme>/target/release/lib<scheme>.<ext>`.
- Exported symbol: `get() -> Box<dyn ArtifactFetcher>` (the usual `get`
  constructor, returning the fetcher trait object).
- The trait is deliberately **blocking** (`fetch` streams bytes into an
  `ArtifactSink`, which hashes, counts, and enforces size limits): fetchers
  run on worker staging threads, never the async runtime, and a blocking
  trait keeps the dylib ABI free of async-trait hazards. `resolve()` is an
  optional second method that pins mutable references to digests for
  `resolve = "pin"`.
- **Registry resolution order: built-ins first, then dynamic plugins.** A
  plugin is only consulted (lazily loaded, manifest validated) when no
  built-in serves the requested scheme, and loading fails if the plugin's
  `schemes()` does not include the scheme it was loaded for.
- Auth resolution, checksum verification, unpacking, retries, and status
  reporting are all shared pipeline code in the worker core — a fetcher
  plugin only moves bytes. Custom schemes must also be added to the
  cluster's `artifacts.allowedSchemes` (and the worker-side allowlist if
  customized).

## 6) Volume Plugins

Purpose: materialize persistent storage for stateful tasks (and volume claims
on any task). See [docs/volumes.md](volumes.md) for the user-facing doc and
[plans/volume-plugin-system-plan.md](../internal_docs/plans/volume-plugin-system-plan.md) for the
design.

- Built-in drivers (in-process, ship in the binary):
  `local-dir` (the default), `host`, `nfs`, `tmpfs` — `src/volumes/builtins.rs`.
- Reference exec plugin: `plugins/volumes/example/` (a ~60-line shell script).

Entry trait: `VolumeDriver` in `src/volumes/mod.rs`. In the jobspec and API the
selectable thing is called a **driver** (`storage = { driver = "nfs" }`); "plugin"
refers to the packaging that delivers one (`plugins/volumes/<name>/` with a
`plugin.json`).

Contract specifics — this category deliberately differs from the others:

- **The ABI is exec, not `cdylib`** (CNI-style): a plugin is any executable at
  `plugins/volumes/<name>/<name>` that reads a JSON request on stdin and writes
  a JSON response on stdout, with the operation as `argv[1]`. Volume operations
  are low-frequency (task start/stop), usually shell out to system tools
  (`mount`, `zfs`, a cloud CLI), and must not be able to take a worker down when
  they crash. Process isolation buys all three and the per-call fork cost is
  irrelevant at this rate.
- Manifest: `{"kind": "volumes", "name": "<name>", "legionVolumeApiVersion": 1}`
  — note the distinct version key, since the volume contract versions
  independently of the dylib plugin API.
- Four operations, all **required to be idempotent**: `capabilities`, `mount`,
  `unmount`, `destroy`. `create`, `attach`, `detach`, `snapshot`, `restore`, and
  `expand` are reserved names a plugin may advertise for forward compatibility.
- **Drivers run on workers only.** The manager owns the volume state machine but
  loads no drivers: it learns what exists from the capabilities workers publish
  in their fingerprints. The control plane therefore needs no storage
  credentials and no mount privileges.
- Built-ins win over exec plugins on a name collision, so an installed plugin
  cannot silently redefine what `local-dir` means on a running cluster.
- `legion volume driver test <name>` runs the contract conformance suite against
  a driver — the same suite Legion's own tests use, no manager required.

### Coexistence with CNI (the networking kind)

"CNI-style" above describes the *architecture* (exec binaries, JSON over
stdio, versioned manifest), not wire compatibility — and the two systems
cannot collide on a host that runs both:

- **Separate trees.** Legion discovers volume plugins only under its own
  `plugins/volumes/<name>/` directories (each gated by a `plugin.json`).
  CNI binaries live in `/opt/cni/bin` with configs in `/etc/cni/net.d`;
  Legion never reads either, and CNI runtimes never read Legion's tree.
- **Different ABI on purpose.** CNI passes the verb via the `CNI_COMMAND`
  environment variable and network config on stdin; Legion passes the op as
  `argv[1]` and a self-describing request on stdin. A binary cannot be
  mistaken for the other kind by accident, and a CNI plugin dropped into
  `plugins/volumes/` fails loudly at `capabilities` rather than half-working.
- **Different version keys.** `legionVolumeApiVersion` in the manifest vs
  CNI's `cniVersion` in network config — each contract evolves independently.
- **Networking stays CNI's job.** When Legion grows a first-class `datapath`
  plugin kind (planned; see the note under
  [Planned Plugin Categories](#planned-plugin-categories-not-implemented-yet)),
  the design is a *shim that drives real CNI plugins* with their own protocol —
  reusing the ecosystem, not re-inventing it under the volume ABI.

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

## Plugin-Shaped (Trait Exists, No Dynamic Loading)

### Auth Methods

Identity sources for the ACL system (username/password, JWT, OIDC) implement
the `AuthMethod` trait in `src/manager/auth_method.rs` — the same
trait-plus-built-ins shape as the categories above. Custom methods can be
registered programmatically when embedding Legion as a library
(`Authenticator::register_method`).

Dynamic `cdylib` loading is deliberately **not** wired for this category.
Auth methods run inside the manager and decide cluster admission, so a buggy
or malicious plugin is a full cluster compromise — a much worse failure
domain than a task or scheduling plugin. If external extensibility is needed,
the plan is:

1. First, an in-tree `webhook` method type that calls an operator-supplied
   endpoint (`{credential} -> {principal, policies}`), keeping custom auth
   code behind a process/network trust boundary.
2. Only if real demand emerges, a `plugins/auth/<name>/` category exporting
   `get() -> Box<dyn AuthMethod>` like the other kinds. Token minting,
   secret hashing, and policy evaluation stay in core regardless; methods
   can only ever yield policy names, never management access.

See internal_docs/plans/auth-acl-plan.md for the full reasoning.

## Planned Plugin Categories (Not Implemented Yet)

These categories are intended but are not first-class plugin frameworks today:

- Networking/CNI plugins (a declarative `datapath` plugin kind + CNI compatibility shim is designed in [plans/service-discovery-plan.md](../internal_docs/plans/service-discovery-plan.md)).
- Command/API endpoint extension plugins.
- Logging/observability pipeline plugins.
- Eval hooks / scheduler reaction hooks.
- UI extension plugins.
- Job submit hook plugins.
- Auth method plugins (webhook-first; see above).

Some similarly named functionality exists in non-plugin form today (for example, resource defaults/validation in `ResourcePolicy`).
