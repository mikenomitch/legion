# Plugin System

This page describes Legion's plugin architecture, how plugins load, and the trade-offs.

## Why Legion Uses In-Process `cdylib` Plugins

Legion uses Rust `cdylib` plugins loaded in-process with `libloading`.

We chose this approach because it is:

- **Fast**: there is no RPC boundary between the core scheduler or worker and plugin code.
- **Simple to deploy**: plugins are shared libraries on disk in a predictable directory.
- **Easy for Rust authors**: you implement traits and export a `get` constructor.
- **Flexible**: you can extend task runtimes, scheduling policies, node checks, rollout strategies, and artifact fetchers.

## Downsides and Risks to Watch

The main trade-off of in-process plugins is weaker reliability and isolation:

- **Plugin panics can terminate the host process** (manager/worker) if not contained.
- **Memory safety and ABI mistakes are dangerous** because dynamic loading requires unsafe FFI boundaries.
- **Version drift can break loading** if plugin and host disagree on API expectations.

To reduce this risk, Legion validates plugin manifests (kind, name, API version)
and checks each library's [build fingerprint](#build-fingerprint-required-for-dynamic-plugins)
before it calls any plugin code.

## How Plugin Loading Works

Legion finds the plugin root in `LEGION_PLUGIN_DIR` when it is set. Otherwise
it uses the first of these directories that exists:

1. `<dir of the legion binary>/plugins` — a bundle unpacked next to the
   binary.
2. `<dir of the legion binary>/../lib/legion/plugins` — an installed binary,
   for example `/usr/local/bin/legion` with `/usr/local/lib/legion/plugins`.
3. `plugins/` in the source checkout the binary was built from — what
   `cargo run` and `cargo test` use.

When none exists, the root is `<dir of the legion binary>/plugins`, and only
the built-in runtimes are available. That is a normal setup, not an error:
the built-in task runtimes (`exec`, `container`, `sandbox`) are compiled into
the binary and need no manifest on disk, and a worker that finds no optional
plugin (it probes for `noop` at start) says so at `debug` level. A plugin a
job or config actually names still fails with the missing path in the error.

Dynamic plugins follow this pattern:

- Plugins are Rust `cdylib` libraries.
- The exported symbol must be `get`.
- Legion loads the plugin's `.so/.dylib/.dll` with `libloading`.

The usual directory layout is:

- `plugins/<kind>/<name>/target/release/lib<name>.<ext>`

### Plugin Manifest (Required for Dynamic Plugins)

Each dynamic plugin root includes a `plugin.json` manifest.

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

If any check fails, loading fails with an error that explains which check failed.

### Version history

Every plugin category shares one version. All plugins link the `legion` crate
and pass Rust types across the boundary, so a layout change in one shared
struct breaks every prebuilt library, whatever the category.

| Version | Change |
| --- | --- |
| 2 | `TaskRuntime` renamed to `TaskInstance` and `RuntimeVolume` to `MountedVolume`. `TaskInstance` gained `volumes`. |
| 1 | Initial. |

**Rebuild your plugins when this number changes.** The manifest check does not
catch a library built against an older version whose manifest claims the
current one. A struct-layout mismatch across the `dylib` boundary is undefined
behavior. In practice you get a `SIGBUS` at first use instead of a clean error.
For this reason the version changes for layout changes as well as signature
changes. The build fingerprint below catches what this hand-bumped number
can't.

### Build fingerprint (required for dynamic plugins)

Plugins return `Box<dyn Trait>` and pass Rust types such as `serde_json::Value`
across the boundary. Rust has no stable ABI, so a plugin is only sound when it
was built by the same `rustc`, against the same `legion` crate version, with
the same layout for every shared type. A plugin built with a different
toolchain, or whose own `Cargo.lock` resolves `serde_json` to a different
version or turns on `serde_json/preserve_order`, would otherwise load fine and
then corrupt memory.

Every plugin exports a fingerprint of the build it came from, next to its `get`
constructor:

```rust
legion::declare_plugin_abi!();

#[no_mangle]
pub extern "C" fn get() -> Box<dyn SchedulingPolicy> { Box::new(MyPolicy) }
```

Task plugins call `legion::declare_task_plugin_abi!()` instead, which includes
it. Before it calls `get`, the host compares the plugin's fingerprint with its
own and refuses the plugin when they differ:

```text
scheduling plugin random was built differently from this legion binary and cannot be
loaded safely: Rust plugins have no stable ABI. Plugin: rustc 1.96.0 (...) [...];
legion-rs 0.1.0; target aarch64-apple-darwin; panic=unwind (fingerprint ...). Host: ...
```

A plugin that exports no fingerprint is refused too, with an error naming the
macro to add. The fingerprint hashes:

- the `rustc` version and commit hash,
- the `legion` crate name and version,
- the target triple and panic strategy,
- the size and alignment of the types that cross the boundary (`Value`,
  `Map`, `anyhow::Error`, `Allocation`, `TaskConfig`, `JobConfig`,
  `TaskInstance`, `StopContext`, and a few std types), which catches
  dependency versions or features that change their layout.

The build profile is not part of it, so a debug `legion` loads release
plugins. To fix a mismatch, rebuild the plugin with the host's `rustc` against
the same `legion` source, and keep its `Cargo.lock` in step with the host's.
The `LEGION_BUILD_PLUGINS=1` build does the first two for you.

Known limitations:

- This is a guard, not a stable ABI. Two builds with equal fingerprints are
  trusted to agree on layout. That holds for the same compiler, crate
  versions, and features, but a dependency change that keeps every probed
  type's size and alignment while reordering fields would get past it.
- Plugins still run in-process. Rollout strategy plugins run inside the
  manager, so a buggy one can crash it.

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

Purpose: choose the node placement strategy.

- Built-in policies:
  - `bin_pack`
  - `spread`
- Dynamic example plugin:
  - `random` (`plugins/scheduling/random/`)

Entry trait: `SchedulingPolicy` in `src/manager/scheduler/policy.rs`.

## 3) Node Check Plugins

Purpose: extend worker fingerprinting and node health checks.

- Built-in checkers/fingerprinters:
  - arch
  - os
  - kitchen-sink fingerprint
  - disk health checks
- Dynamic example plugin:
  - `example` (`plugins/node_checks/example/`)

Entry trait: `NodeChecker` in `src/worker/node_check.rs`.

## 4) Rollout Strategy Plugins

Purpose: transform or resolve rollout config before the rollout runs.

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
  - `demo` (`plugins/artifacts/demo/`) serves `demo://` and writes the
    URL's `content` query parameter out as a staged file.

Entry trait: `ArtifactFetcher` in `src/artifacts/fetcher.rs`.

Contract details (everything else follows the common pattern above):

- Directory layout: `plugins/artifacts/<scheme>/` with a `plugin.json` of
  `{"kind": "artifacts", "name": "<scheme>", "legionApiVersion": 2}` and the
  dylib at `plugins/artifacts/<scheme>/target/release/lib<scheme>.<ext>`.
- Exported symbol: `get() -> Box<dyn ArtifactFetcher>` (the usual `get`
  constructor, returning the fetcher trait object).
- The trait is **blocking**. `fetch` streams bytes into an `ArtifactSink`,
  which hashes, counts, and enforces size limits. Fetchers run on worker
  staging threads, never the async runtime, and a blocking trait keeps the
  dylib ABI free of async-trait hazards. `resolve()` is an optional second
  method that pins mutable references to digests for `resolve = "pin"`.
- **The registry checks built-ins first, then dynamic plugins.** Legion only
  loads a plugin (lazily, after validating its manifest) when no built-in
  serves the requested scheme. Loading fails if the plugin's `schemes()`
  does not include the scheme it was loaded for.
- Auth resolution, checksum verification, unpacking, retries, and status
  reporting are shared pipeline code in the worker core. A fetcher plugin
  only moves bytes. Add custom schemes to the cluster's
  `artifacts.allowedSchemes` too (and to the worker-side allowlist if you
  customized it).

## 6) Volume Plugins

Purpose: materialize persistent storage for stateful tasks (and volume claims
on any task). See [docs/volumes.md](volumes.md) for the user-facing doc.

- Built-in drivers (in-process, ship in the binary):
  `local-dir` (the default), `host`, `nfs`, `tmpfs` (`src/volumes/builtins.rs`).
- Reference exec plugin: `plugins/volumes/example/` (a ~60-line shell script).

Entry trait: `VolumeDriver` in `src/volumes/mod.rs`. In the jobspec and API you
select a **driver** (`storage = { driver = "nfs" }`). "Plugin" refers to the
packaging that delivers one (`plugins/volumes/<name>/` with a `plugin.json`).

This category's contract differs from the others:

- **The ABI is exec, CNI-style, instead of `cdylib`.** A plugin is any
  executable at `plugins/volumes/<name>/<name>` that reads a JSON request on
  stdin and writes a JSON response on stdout, with the operation as
  `argv[1]`. Volume operations are infrequent (task start and stop), usually
  shell out to system tools (`mount`, `zfs`, a cloud CLI), and must not take a
  worker down when they crash. Process isolation covers all three, and the
  per-call fork cost does not matter at this rate.
- Manifest: `{"kind": "volumes", "name": "<name>", "legionVolumeApiVersion": 1}`.
  The version key is different because the volume contract is versioned
  separately from the dylib plugin API.
- There are four operations, and all **must be idempotent**: `capabilities`,
  `mount`, `unmount`, `destroy`. `create`, `attach`, `detach`, `snapshot`,
  `restore`, and `expand` are reserved names a plugin may advertise for
  forward compatibility.
- **Drivers run on workers only.** The manager owns the volume state machine
  but loads no drivers. It learns what exists from the capabilities workers
  publish in their fingerprints, so the control plane needs no storage
  credentials and no mount privileges.
- Built-ins win over exec plugins when names collide, so an installed plugin
  cannot silently redefine what `local-dir` means on a running cluster.
- `legion volume driver test <name>` runs the contract conformance suite
  against a driver. It is the same suite Legion's own tests use, and it needs
  no manager.

### Coexistence with CNI (the networking kind)

"CNI-style" above describes the architecture (exec binaries, JSON over
stdio, versioned manifest). The two are not wire-compatible, and they cannot
collide on a host that runs both:

- **Separate trees.** Legion discovers volume plugins only under its own
  `plugins/volumes/<name>/` directories (each gated by a `plugin.json`).
  CNI binaries live in `/opt/cni/bin` with configs in `/etc/cni/net.d`.
  Legion never reads either, and CNI runtimes never read Legion's tree.
- **Different ABI.** CNI passes the verb in the `CNI_COMMAND` environment
  variable and network config on stdin. Legion passes the operation as
  `argv[1]` and a self-describing request on stdin. One kind of binary cannot
  be mistaken for the other by accident. A CNI plugin dropped into
  `plugins/volumes/` fails with a clear error at `capabilities` instead of
  half-working.
- **Different version keys.** The manifest uses `legionVolumeApiVersion` and
  CNI network config uses `cniVersion`, so each contract evolves on its own.
- **Networking stays CNI's job.** Legion has no `datapath` plugin kind and
  does not load CNI plugins. The `network.mode` values `bridge` and
  `cni/<name>` are reserved and rejected at submit.

## Build Behavior

`build.rs` behavior:

- Protobuf always compiles.
- Plugins build only when `LEGION_BUILD_PLUGINS=1`.
- Plugin builds are skipped if `LEGION_SKIP_PLUGIN_BUILD` is set.
- When `build.rs` builds plugins, it uses a separate `CARGO_TARGET_DIR`
  under `target/plugin-build/...`.
- At runtime, dynamic loading still looks in plugin crate-local paths such as
  `plugins/<kind>/<name>/target/release/lib<name>.<ext>`.
- `build.rs` also records the `rustc` version, crate version, target, and
  panic strategy for the [build fingerprint](#build-fingerprint-required-for-dynamic-plugins).
  It runs in both the host and plugin builds, since plugins depend on the
  `legion` crate.

This lets normal core builds skip the cost of building plugins.

## Plugin-Shaped (Trait Exists, No Dynamic Loading)

### Auth Methods

Identity sources for the ACL system (username/password, JWT, OIDC, webhook) implement
the `AuthMethod` trait in `src/manager/auth_method.rs`, the same
trait-plus-built-ins shape as the categories above. You can register custom
methods in code when you embed Legion as a library
(`Authenticator::register_method`).

Dynamic `cdylib` loading is **not** supported for this category. Auth methods
run inside the manager and decide who is admitted to the cluster, so a buggy
or malicious plugin would compromise the whole cluster. That is a much worse
failure than a bad task or scheduling plugin.

To plug in a custom identity system (LDAP, a SAML bridge, in-house SSO) in
any language, use the built-in `webhook` auth method. It sends the
credential to an HTTP endpoint you run, which returns the principal and
policy names. This keeps custom auth code behind a process or network trust
boundary. Webhooks are only called at login. See
[Auth & ACLs](auth-and-acls.md).

## Planned Plugin Categories (Not Implemented Yet)

Legion has no plugin kind for networking (CNI), API extensions, logging
pipelines, eval or submit hooks, or the UI. Some of this exists in
non-plugin form, such as resource defaults and validation in
`ResourcePolicy`.
