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

- **Plugin panics can terminate the host process** (manager/worker) if not
  contained. Scheduling, rollout, node-check, and artifact-fetcher plugins
  are contained and quarantined (see [Panics and
  quarantine](#panics-and-quarantine)); task runtime plugins are not yet.
- **Memory safety and ABI mistakes are dangerous** because dynamic loading requires unsafe FFI boundaries.
- **Version drift can break loading** if plugin and host disagree on API expectations.

To reduce this risk, Legion validates plugin manifests (kind, name, API version)
and checks each library's [build fingerprint](#build-fingerprint-required-for-dynamic-plugins)
before it calls any plugin code.

Plugins that talk to hardware or external systems are **exec plugins**
instead: a separate program per call, JSON on stdin and stdout. Device
plugins ([docs/devices.md](devices.md)), volume drivers
([docs/volumes.md](volumes.md)), the keyring and cluster CA `exec`
providers, and the autoscaler's plugins work this way. A crash or a hang
costs one call.

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
  "legionApiVersion": 3
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
| 3 | Scheduling, rollout, node-check, and artifact-fetcher plugins export `legion_contained_get` through `legion::export_*!` instead of `get` ([Panics and quarantine](#panics-and-quarantine)). `NodeChecker` lost `has_fingerprinter`/`has_health_check`, and `health_check` returns `Option<NodeCheckReport>`. `TaskInstance` gained `devices`. |
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

Every plugin exports a fingerprint of the build it came from, next to its
constructor. Scheduling, rollout, node-check, and artifact-fetcher plugins
get both from one macro per kind:

```rust
legion::export_scheduling_policy!(MyPolicy::new());   // or export_rollout_strategy!,
                                                       // export_node_checker!,
                                                       // export_artifact_fetcher!
```

Task plugins call `legion::declare_task_plugin_abi!()` and export `get`
themselves. Before it calls the constructor, the host compares the plugin's
fingerprint with its own and refuses the plugin when they differ:

```text
scheduling plugin random was built differently from this legion binary and cannot be
loaded safely: Rust plugins have no stable ABI. Plugin: rustc 1.96.0 (...) [...];
legion-worker 0.1.0; target aarch64-apple-darwin; panic=unwind (fingerprint ...). Host: ...
```

A plugin that exports no fingerprint is refused too, with an error naming the
macro to add. The fingerprint hashes:

- the `rustc` version and commit hash,
- the name and version of the crate the loader lives in (`legion-worker`),
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
- Plugins still run in-process. A panic is contained (below), but a plugin
  that corrupts memory, loops forever, or calls `abort` still takes its host
  with it.

### Panics and quarantine

A plugin is a `cdylib` with its own copy of the Rust standard library, so a
panic that unwinds out of it into Legion is a foreign exception and aborts
the process. Legion therefore catches it on the plugin's side:

- `legion::export_*!` wraps the plugin's value so that every trait method
  runs under the plugin's own `catch_unwind`. A panic is recorded and the
  method returns a neutral answer: an error where it returns a `Result`.
- After every call the host asks the plugin, through a C-ABI probe
  (`legion_plugin_take_panic`), whether it panicked. If it did, the call
  fails with `<kind> plugin <name> panicked in \`<method>\` (<message>); it
  is quarantined until this process restarts`, and the plugin is
  **quarantined**: Legion does not call it again until the process
  restarts. Later calls fail fast with `<kind> plugin <name> is quarantined:
  it panicked earlier (...)`.

What a quarantine means per kind:

| Kind | Runs in | While quarantined |
| --- | --- | --- |
| Scheduling policy | Manager | Placements use `bin_pack` |
| Rollout strategy | Manager | Resolving a rollout that names it fails |
| Node check | Worker | It reports a failing check, so the node reads as unhealthy |
| Artifact fetcher | Worker | Fetches of its scheme fail, and the allocation with them |

Every quarantine is logged at `error` (`Plugin panicked; quarantining it
until this process restarts`) and counted in
`legion_plugin_panics_total{kind,name}` ([docs/metrics.md](metrics.md)). A
worker also lists its quarantined plugins under `plugins` in its health
checks (`legion node info -j`, `healthChecks.plugins`), which marks the node
unhealthy.

Known limitations:

- **Task runtime plugins are not contained yet.** Their trait has about 40
  methods; a panicking `noop`-style plugin still takes the worker down.
- A panic **inside the exported constructor** itself cannot be caught (it
  is an `extern "C"` function) and aborts. The macros keep it to one
  allocation.
- Both sides need `panic = "unwind"`; the fingerprint already refuses a
  plugin built with a different panic strategy.
- A quarantine ends only with a restart. There is no command to lift it.

## Plugin Types Implemented Today

## 1) Task Runtime Plugins

Purpose: run workload tasks on workers.

- Built-in implementations in-process:
  - `exec` (`crates/worker/src/task_plugins/exec.rs`)
  - `container` (`crates/worker/src/task_plugins/container.rs`)
- Dynamically loaded examples/shipped plugins:
  - `noop` (`plugins/tasks/noop/`)

Entry trait: `TaskPlugin` in `crates/worker/src/task_plugins/mod.rs`.

## 2) Scheduling Policy Plugins

Purpose: choose the node placement strategy.

- Built-in policies:
  - `bin_pack`
  - `spread`
- Dynamic example plugin:
  - `random` (`plugins/scheduling/random/`)

Entry trait: `SchedulingPolicy` in `crates/manager/src/manager/scheduler/policy.rs`.

## 3) Node Check Plugins

Purpose: extend worker fingerprinting and node health checks.

- Built-in checkers/fingerprinters:
  - arch
  - os
  - kitchen-sink fingerprint
  - disk health checks
- Dynamic example plugin:
  - `example` (`plugins/node_checks/example/`), named in a worker type's
    `additionalNodeChecks`

Entry trait: `NodeChecker` in `crates/worker/src/worker/node_check.rs`:

```rust
pub trait NodeChecker: Send + Sync {
    fn name(&self) -> &'static str;
    /// Static facts, merged into the node's fingerprint attributes at registration.
    fn fingerprint(&self) -> Map<String, Value> { Map::new() }
    /// Run every node-check pass (30 seconds).
    fn health_check(&self) -> Option<NodeCheckReport> { None }
}

pub struct NodeCheckReport {
    pub status: CheckStatus,              // Pass | Warn | Fail
    pub message: String,                  // "/var has 5% free"
    pub attributes: Map<String, Value>,   // {"lowestFreePercent": 5}
}
```

A report is stored as `healthChecks.<check name> = { "status", "message",
"attributes" }`. `warn` keeps the node healthy; `fail` marks it unhealthy.
The built-in `disk` check warns when the fullest disk has under 50% free and
fails under 20%. A node-check plugin is loaded **once** per worker process
and reused on every pass. A plugin that cannot load is reported as a failing
check (`plugin failed to load: ...`) and not retried until the worker
restarts.

## Device Plugins

Purpose: discover GPUs and other devices, report their health and topology,
and hand an assigned device to a task. Device plugins are exec plugins, not
dylibs: any program that speaks `legion-device/1` (`fingerprint`,
`allocate`) on stdin and stdout, configured in a worker type's
`devices.plugins`. See [docs/devices.md](devices.md), which also covers the
shipped `nvidia` and `fake` plugins and `legion device plugin test`.

## 4) Rollout Strategy Plugins

Purpose: transform or resolve rollout config before the rollout runs.

- Built-in strategy:
  - `default`
- Dynamic example plugin:
  - `slow_start` (`plugins/rollouts/slow_start/`)

Entry trait: `RolloutStrategyPlugin` in `crates/worker/src/rollout_strategy.rs`.

## 5) Artifact Fetcher Plugins

Purpose: serve artifact source schemes beyond the built-ins
(`http`/`https`, `file`, `git`, `s3`, `oci`). See
[docs/artifacts.md](artifacts.md) for the artifact system itself.

- Built-in fetchers register in `FetcherRegistry::with_builtins()`
  (`crates/worker/src/artifacts/fetcher.rs`, implementations in `crates/worker/src/artifacts/fetchers/`).
- Dynamic example plugin:
  - `demo` (`plugins/artifacts/demo/`) serves `demo://` and writes the
    URL's `content` query parameter out as a staged file.

Entry trait: `ArtifactFetcher` in `crates/worker/src/artifacts/fetcher.rs`.

Contract details (everything else follows the common pattern above):

- Directory layout: `plugins/artifacts/<scheme>/` with a `plugin.json` of
  `{"kind": "artifacts", "name": "<scheme>", "legionApiVersion": 3}` and the
  dylib at `plugins/artifacts/<scheme>/target/release/lib<scheme>.<ext>`.
- Exported constructor: `legion::export_artifact_fetcher!(MyFetcher)`, which
  also contains its panics (see [Panics and quarantine](#panics-and-quarantine)).
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
  `local-dir` (the default), `host`, `nfs`, `tmpfs` (`crates/common/src/volumes/builtins.rs`).
- Reference exec plugin: `plugins/volumes/example/` (a ~60-line shell script).

Entry trait: `VolumeDriver` in `crates/common/src/volumes/mod.rs`. In the jobspec and API you
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

Three build scripts, one per job:

- `proto/build.rs` compiles the protobuf schema, always.
- The root `build.rs` builds the plugins, and only when
  `LEGION_BUILD_PLUGINS=1`.
- Plugin builds are skipped if `LEGION_SKIP_PLUGIN_BUILD` is set.
- When `build.rs` builds plugins, it uses a separate `CARGO_TARGET_DIR`
  under `target/plugin-build/...`.
- At runtime, dynamic loading still looks in plugin crate-local paths such as
  `plugins/<kind>/<name>/target/release/lib<name>.<ext>`.
- `crates/worker/build.rs` records the `rustc` version, crate version,
  target, and panic strategy for the
  [build fingerprint](#build-fingerprint-required-for-dynamic-plugins). It
  runs in both the host and plugin builds, since a plugin depends on the
  `legion` crate and so on `legion-worker`, where the loader is.

This lets normal core builds skip the cost of building plugins.

## Plugin-Shaped (Trait Exists, No Dynamic Loading)

### Auth Methods

Identity sources for the ACL system (username/password, JWT, OIDC, webhook) implement
the `AuthMethod` trait in `crates/manager/src/manager/auth_method.rs`, the same
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

## Out-of-Process Plugins: Autoscaling

The autoscaler's metric sources, strategies, and targets are a different
kind of plugin: separate programs (or URLs) speaking a JSON-RPC protocol,
`legion-scaling/1`, installed in the autoscaler app's config rather than
loaded into a Legion process. A crash or a hang costs one call, not the
host. See [Writing Autoscaling Plugins](autoscaling-plugins.md) and
[Autoscaling, "Plugins"](autoscaling.md#plugins).

## Planned Plugin Categories (Not Implemented Yet)

Legion has no plugin kind for networking (CNI), API extensions, logging
pipelines, eval or submit hooks, or the UI. Some of this exists in
non-plugin form, such as resource defaults and validation in
`ResourcePolicy`.
