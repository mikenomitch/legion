# Modular Builds and Installable Plugins

> **Judgement calls:** [modular-build-plan_calls.md](../calls/modular-build-plan_calls.md)

**Status: PLANNED** (2026-11-10). Nothing here is built. The default build
stays what it is today, everything in one binary, and gains the first-party
plugins that today sit in the repo as loose scripts. Cargo features leave
parts out: the manager role, the `container` and `sandbox` runtime engines,
each bundled app, and the heavy built-ins behind the plugin seams. A plugin
or app a build lacks can come back as a directory in the plugin root, put
there by hand or by `legion plugin install <url>`. Measured before planning (§ 5.4): the
manager role is about a fifth of the code and both runtime engines together
are under 1 MB, so this is a plan about what a binary can do more than about
how big it is. **Deliberately deferred**, each with its
reason in § 8: the first-party `container` and `sandbox` engines as
installable packages (Phase 9, gated on three prerequisites), cluster-wide
distribution of plugins, package signing, and an app SDK crate.

## 1. Problem

Legion ships as one binary in one shape. Every `legion` is a manager, a
worker, the CLI, the MCP server, three task runtimes, and four apps: 97 MB on
linux/amd64 with `--profile dist`. `Cargo.toml` has one feature, `loadgen`,
and it does not touch the binary. Four things follow.

- **Nothing can be left out.** A worker on a small edge box that only runs
  `exec` tasks carries the raft log, the SQLite store, the scheduler, the
  microVM runtime, the OCI client, an ACME client, and a reverse proxy. The
  only way to drop any of it is to delete code from a fork.
- **Every worker can become a manager.** `legion start manager` works on any
  host that has the binary. An operator who wants "this fleet can never run a
  control plane" has a convention, not a guarantee.
- **The first-party plugins are not in the build at all.** The NVIDIA device
  plugin, the KMS and Vault key providers, and the Vault CA provider are
  shell scripts under `plugins/` that an operator copies to each host and
  names by absolute path
  (`command = ["/opt/legion/plugins/devices/nvidia/legion-device-nvidia"]`,
  [devices.md](../../docs/devices.md)). Rust plugins build only with
  `LEGION_BUILD_PLUGINS=1` and are found at
  `plugins/<kind>/<name>/target/release/`, a Cargo layout rather than an
  install layout. No command installs a plugin.
- **Nothing says what a node can run.** A `container` task placed on a node
  with no runc fails at start, because placement never asks. That is
  tolerable while every node is the same build. It stops being tolerable the
  moment builds differ.

The crate split made the first two fixable and said so: its non-goals list
"Cargo features to trim the binary (a worker-only build). Possible now that
the crates exist, but a product decision, not a build one"
([workspace-crate-split-plan.md § 4](workspace-crate-split-plan.md#4-goals-and-non-goals)).
This plan is that decision.

## 2. Prior art

| System | What it does | Take | Reject |
| --- | --- | --- | --- |
| [Vector](https://vector.dev/docs/setup/installation/manual/from-source/) (Rust) | Every source, transform, and sink is a Cargo feature; the default is all of them; custom builds pass `--no-default-features --features …`. | Features named after the component, default everything, a few named presets. | A feature per component regardless of size: hundreds of features nobody can test. |
| [Caddy](https://caddyserver.com/docs/build) | `xcaddy build --with <module>` compiles modules in. [`caddy add-package`](https://caddyserver.com/docs/command-line#caddy-add-package) "installs" one by downloading a new binary. | The honesty: for statically linked code, installing means another binary. | A build server. |
| [k3s](https://docs.k3s.io/installation/packaged-components) | One binary with batteries (Traefik, ServiceLB, local-path); `--disable traefik`; a manifest dropped in a directory is deployed; `k3s server` and `k3s agent` from the same binary. | Packaged components, and "put a file in a directory" as an install path. | Only a runtime switch. Legion has that already (`apps.<name>.enabled = false`); this plan is about the code not being there. |
| [Nomad](https://developer.hashicorp.com/nomad/docs/configuration#plugin_dir) | One binary is client and server. Some task drivers are built in; others are executables in `plugin_dir`. Clients fingerprint drivers as `driver.<name>` attributes, and the scheduler only places on a node that has the driver. | `plugin_dir` drop-in. The fingerprint attribute and the feasibility check, which Legion lacks today. | An out-of-process driver protocol, for now (§ 8, Phase 9). |
| [Kubernetes](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/) | A binary per role (kubelet, apiserver); CRI, CNI, CSI, and device plugins are all separate processes. | Role separation as a security property: a kubelet cannot be an apiserver. | A binary and a name per role. Legion stays one `legion`. |
| [Telegraf custom builder](https://github.com/influxdata/telegraf/blob/master/tools/custom_builder/README.md), [OpenTelemetry Collector Builder](https://opentelemetry.io/docs/collector/custom-collector/) | A tool works out the build flags for the subset you want. | A helper that writes the feature list, so nobody types `--no-default-features --features a,b,c,…`. | Deriving the subset from a config file. |
| [Vault plugin catalog](https://developer.hashicorp.com/vault/docs/commands/plugin/register), [Terraform lock file](https://developer.hashicorp.com/terraform/language/files/dependency-lock), [krew](https://krew.sigs.k8s.io/docs/developer-guide/plugin-manifest/) | A plugin runs only if its SHA-256 matches a recorded one; indexes carry a checksum per platform. | A checksum is required at install and recorded beside the plugin. A static index file per release. | A registry service. |

Legion already has the checksum half in one place: the autoscaler's plugins
install from `source = "https://…"` with `checksum = "sha256:…"`
([autoscaling-plugins.md, "Packaging"](../../docs/autoscaling-plugins.md#packaging)).
This plan gives every other plugin kind the same thing.

## 3. Jobs to be done

**Cluster operator**
- When I run workers on small or locked-down machines, I want a binary with
  only what those machines use, so there is less on the box to patch, audit,
  and misuse.
- When I build a worker fleet, I want it to be impossible to start a control
  plane from that binary, so a mistake or an intruder on a worker cannot
  stand up a second manager.
- When a node needs something its build does not have (a GPU plugin, a
  runtime, an app), I want to add it with one command or by copying files,
  so I do not rebuild and redeploy the binary for one node pool.
- When my machines cannot reach the internet, I want to install the same
  things from files I carried in, so the air gap is not a different product.
- When I look at a node, I want to see what it was built with and what was
  added later, so I can tell why a job will not place there.

**Job author**
- When I submit a job that needs a runtime no node has, I want to be told
  that, by name, at submit and in the job's status, so I am not reading a
  start failure on one node an hour later.

**Security / platform admin**
- When something is installed on a node after the fact, I want it to be
  checked against a checksum I chose and recorded, so I can answer "what code
  is this node running, and who put it there?".
- When I harden a fleet, I want code I do not use to be absent, not merely
  switched off.

**Plugin or app author**
- When I publish a plugin, I want users to install it from my URL with one
  command, so my README is one line and not a page of `mkdir` and `chmod`.

**Packager (distro, internal platform team)**
- When I build Legion for my own platform, I want to say what to leave out in
  the words I think in ("without the sandbox, without the ingress"), so I do
  not have to learn the feature graph.

**Autonomous agent**
- When I am about to use a command or a runtime, I want to ask the binary
  what it has, in JSON, so I do not find out by failing.

**Contributor**
- When I add an optional part to Legion, I want one rule for whether it gets
  a feature and one place to register it, so the list stays honest.

### Scenarios

1. **The edge fleet.** An operator builds `scripts/legion-build --preset
   worker --without runtime-sandbox`, bakes it into an image, and joins 200
   nodes. `legion start manager` on any of them is refused. A sandbox job
   submitted to the cluster lands only on the datacenter pool, and the job's
   status says the edge nodes have no `sandbox` runtime.
2. **The accelerator pool, later.** Six nodes get a vendor's accelerator.
   On each, the operator runs `legion plugin install
   https://vendor.example/legion-device-tpu.tar.gz --checksum sha256:…`,
   adds `devices.plugins.tpu = {}` to the worker config, and restarts the
   worker. Nothing else in the cluster changes. (For NVIDIA there is nothing
   to install: that plugin is built in.)
3. **The air gap.** The operator copies `apps-gitops-…tar.gz` to the three
   managers and unpacks it into the plugin root. Within a minute `legion app
   list` shows `gitops`, disabled; `legion app enable gitops` starts it.

## 4. Goals and non-goals

**Goals**
- The default build is the batteries-included one, and stays one binary
  named `legion`.
- One `cargo` command (or one script flag) builds Legion without: the manager
  role; the `container` or `sandbox` runtime engine; any bundled app; any of
  the heavy built-ins behind a plugin seam.
- A build that lacks something says so, places around it, and names the fix.
- A plugin or app installs from a URL with one command, or by putting a
  directory in the plugin root. Both end in the same files on disk.
- Clusters of mixed builds work without configuration.

**Non-goals**
- **More binaries or names.** The worker-only build is still `legion`.
- **A stable plugin ABI.** Rust plugins stay tied to the exact build
  ([plugin-system.md](../../docs/plugin-system.md#build-fingerprint-required-for-dynamic-plugins));
  that is its own TODO item (audit M6).
- **A registry service.** A release carries a static `index.json`.
- **The manager pushing plugins to nodes.** `legion plugin install` runs on
  the host it installs on. Fleet-wide installs are config management's job
  for now (§ 11, Q5).
- **Leaving out `exec`**, the CLI, or the config language. A build with none
  of those is not Legion.
- **A feature per small thing.** § 6.2 sets the bar.
- **Runtime toggles.** `apps.<name>.enabled`, namespace policies, and
  `runtimes` restrictions already exist and are untouched.

## 5. Background

### 5.1 What is compiled in, and where

| Part | Where it is wired in | Optional today? |
| --- | --- | --- |
| Manager role | `legion-manager`, an unconditional dependency of the binary crate; `src/cmd/start.rs:3` | No |
| Task runtimes `exec`, `container`, `sandbox`, `envelope` | A `match` on the name in `load_task_plugin` (`crates/worker/src/task_plugins/mod.rs:2255`); anything else is looked up on disk. The worker loads a fixed list at start (`crates/worker/src/worker/task_manager.rs:6445`) | No |
| Bundled apps | Two tables under one name: manifests in `MANIFESTS` (`crates/common/src/bundled_apps.rs:38`), bodies in `BUNDLED` (`src/apps/mod.rs:72`). An app's job runs `legion:self app run <name>`, which the exec runtime resolves to the worker's own executable (`crates/worker/src/task_plugins/exec.rs:1146`) | No |
| Artifact fetchers, volume drivers, scheduling policies, node checks | Built-ins registered ahead of the on-disk lookup; built-ins win a name collision ([plugin-system.md](../../docs/plugin-system.md)) | No |
| Rust (`dylib`) plugins | `build.rs:9-16` builds `plugins/*/*` only when `LEGION_BUILD_PLUGINS=1`; `plugin_paths` (`crates/worker/src/plugin_runtime.rs:276`) expects `…/target/release/lib<name>.so` | Off by default |
| Exec plugins (devices, keyring, cluster CA, autoscaler) | Named by `command` in config; volumes alone are found by name under the plugin root | Not in the build |

The plugin root is `LEGION_PLUGIN_DIR`, or the first that exists of
`<exe dir>/plugins`, `<exe dir>/../lib/legion/plugins`, and the source
checkout (`crates/common/src/util.rs:382`).

### 5.2 What already points the right way

- **The manager needs little from the binary crate.** `src/` refers to
  `crate::manager` or `crate::storage` in about 40 places across 17 files.
  Most are constants (`MIN_REVISION_HEADER`, `APPROVAL_HEADER`,
  `ERROR_CODE_METADATA`) and small pure helpers (`AutoscaleLayers`,
  `effective_gate`, `checkpoint_room`) that belong in `legion-common`. The
  rest are manager code the CLI runs in its own process: `start manager` and
  its `--restore-from`, the keyring provider test, the CA provider
  conformance run.
- **The runtimes are contained.** Outside their own files, the `sandbox`
  engine's modules are named only in `task_plugins/mod.rs` and
  `schema_docs.rs`, and `container`'s in five files under `task_plugins/`
  plus `worker/usage.rs`. `sandbox` uses `container` (24 references: the
  image-to-rootfs path and dev mode), so the dependency runs one way. The
  manager reaches both only through validation helpers
  (`sandbox_checkpoint_support`, `CapabilityAllowlist`,
  `runtime_class::validate_classes`, `sandbox_addr::validate_bridges`),
  which is the spec half of § 6.4.
- **Implicit placement constraints exist.** A container task with user
  namespaces carries a generated constraint on `container.idmappedMounts`
  (`crates/worker/src/task_plugins/container.rs:377`, `:400`), and volume
  drivers are filtered on the `volumeDrivers` fingerprint
  (`crates/worker/src/worker/daemon.rs:769`). Runtimes have no equivalent:
  `readiness()` only feeds health checks.
- **Apps already cope with mixed builds.** Workers report `buildVersion` and
  `buildId` (`crates/common/src/util.rs:15`); the app reconciler rolls an app
  on a build marker over the nodes its placement allows, and renders from the
  manifest stored on the record when the leader's build does not have one
  ([apps.md, "Upgrades of bundled apps"](../../docs/apps.md#upgrades-of-bundled-apps)).
- **`legion app install <file or URL>` exists**
  (`src/cmd/app.rs:907`, `AppSource` at
  `crates/common/src/structs/app.rs:895`), with a grant prompt, a dry run,
  and admission checks.
- **Safe unpacking and checksum checks exist** in the artifact pipeline
  (`unpack_archive` and `verify_checksum`,
  `crates/worker/src/artifacts/stage.rs:324`, `:1231`).

### 5.3 What does not

- **The manager validates jobs with the worker's runtime objects.**
  `legion-manager` depends on `legion-worker` and loads a runtime to read its
  schema, implicit artifacts, and volume modes
  (`crates/manager/src/manager/server.rs:4026`, `:4270`); the store calls
  `normalize_runtime_task` inside the submit transaction
  (`crates/worker/src/task_plugins/mod.rs:1694`). One trait, `TaskPlugin`
  (`mod.rs:541`), holds both what a runtime *accepts* and how it *runs*. If
  a feature removed a runtime whole, a manager built without it could not
  admit its jobs.
- **A Rust plugin links all of Legion.** `plugins/tasks/noop/Cargo.toml`
  depends on `legion-rs`. A plugin has its own copy of every static in the
  worker crate, and task runtime plugins have no panic containment
  ([plugin-system.md](../../docs/plugin-system.md#panics-and-quarantine)).
  This is why § 8 does not promise the `container` engine as a package yet.
- **There are no releases.** `docs/installation.md` says `cargo install
  legion-rs`; CI has no release workflow. "Install by URL" needs something at
  the URL.

### 5.4 Where the bytes are

Measured for this plan on an unstripped `--release` build of `a028a51`
(linux/amd64, rustc 1.97.0): a 171.2 MB file with 95.4 MB of `.text`. Each
row sums the code symbols whose demangled name mentions the part (`nm -C
-S`), so generic code instantiated for a part's types counts toward it.

| Part | Code that names it | Share of code |
| --- | --- | --- |
| Manager role (`legion_manager`, `openraft`, the manager's gRPC server, its other own dependencies) | 18.8 MB | 19.4% |
| The four apps and their framework (autoscaler 1.2, ingress 0.75, certs 0.42, gitops 0.42) | 3.1 MB | 3.2% |
| MCP server | 2.1 MB | 2.2% |
| WASM network plugins (`wasmi`, `wast`, `wasmparser`, the embedding) | 1.8 MB | 1.9% |
| S3 (`object_store`, `quick_xml`) | 1.1 MB | 1.1% |
| Kernel classifier (`aya`, `netd::kernel`) | 0.7 MB | 0.7% |
| `sandbox` engine | 0.47 MB | 0.5% |
| OTLP export | 0.28 MB | 0.3% |
| `oci-client` | 0.27 MB | 0.3% |
| `container` engine | 0.26 MB | 0.3% |
| **Everything this plan makes optional** | **about 29 MB** | **about 30%** |

For scale, three things that are not optional: the Nickel config language
is 9.5 MB, the CLI's commands other than MCP are 8.7 MB, and code that names
the generated gRPC types is 18.2 MB.

These are estimates for ranking, not predictions. The build is not LTO
(`dist` has 76.3 MB of `.text`, a fifth less), a symbol that names two parts
counts for both, and removing a part also removes code that only it reaches
but that does not name it. Phase 0 replaces the table with real `dist`
builds. Three conclusions are already safe:

1. **The manager role is the only large lever.** A worker-only binary is
   about a fifth smaller.
2. **The floor is shared.** With everything optional left out, about 70%
   remains: gRPC types, serde, the config language, the worker. A `minimal`
   build is smaller, not small.
3. **The runtime engines are under 1 MB together.** Leaving out `sandbox` or
   `container` changes what the binary can do as root on a host. It does not
   change its size in any way an operator would notice. The same holds for
   three of the four apps.

So this plan is mostly about what a binary *can do*: its role, its
privileges, its attack surface. Size is a 20 to 30% side effect.

## 6. Proposed design

### 6.1 One rule: built in, installed, or absent

Every optional part of Legion has a kind and a name, written
`<kind>/<name>`: `tasks/container`, `apps/ingress`, `devices/nvidia`,
`artifacts/oci`. The kinds are the plugin root's existing directory names,
plus `apps`. On a given host, each one is in exactly one state:

- **built in**: compiled into this binary (its Cargo feature was on);
- **installed**: a directory `<plugin root>/<kind>/<name>/`;
- **absent**.

Built in wins over installed, which is already the rule for volume drivers
and artifact fetchers, so an installed directory can never redefine what a
compiled-in name means. Absent is a state Legion reports and plans around,
not an error it discovers.

A static **catalog** in `legion-common` has one row per first-party optional
part, whether or not this build has it: kind, name, the feature that
compiles it in, a one-line summary, and whether a package of it is published.
It is what lets a build name the thing it lacks. A unit test fails when the
catalog and the features in `Cargo.toml` disagree.

```console
$ legion plugin list
KIND       NAME        SOURCE     VERSION  DETAIL
role       manager     absent     -        not in this build (worker-only)
tasks      exec        built in   0.2.0
tasks      container   built in   0.2.0
tasks      sandbox     absent     -        not in this build
apps       autoscaler  built in   0.2.0
apps       ingress     installed  0.2.0    /usr/local/lib/legion/plugins/apps/ingress
artifacts  oci         built in   0.2.0
devices    nvidia      built in   0.2.0
devices    tpu         installed  0.3.0    sha256:9f2c…  from https://example.com/…
scaling    aws-asg     absent     -        legion plugin install scaling/aws-asg
```

`-j` gives the same rows in the standard envelope. `legion capabilities -j`
gains a `build` object (the features this binary has) and marks commands
this build cannot run.

### 6.2 The build surface

Features are declared on the binary crate and forwarded down the chain, so
each crate gates its own code and the catalog in `legion-common` sees all of
them.

| Feature | Off means | Dependencies it drops | Code (§ 5.4) | Implies |
| --- | --- | --- | --- | --- |
| `manager` | No manager role (§ 6.3) | `legion-manager`, and with it `openraft`, `rusqlite_migration`, `argon2`, `imbl`, `json-patch`, `tonic-web`, `tower-http`, `hyper-tungstenite`, `tokio-tungstenite` | 18.8 MB | |
| `runtime-container` | No `container` engine | | 0.26 MB | `fetcher-oci` |
| `runtime-sandbox` | No `sandbox` engine: microVMs and memory checkpoints | | 0.47 MB | `runtime-container` |
| `app-autoscaler` | The autoscaler is not bundled | | 1.2 MB | |
| `app-certs`, `app-gitops` | That app is not bundled | | 0.42 MB each | |
| `app-ingress` | The ingress is not bundled | | 0.75 MB | `app-certs` (it answers HTTP-01 with the cert manager's code) |
| `fetcher-oci` | No `oci://` artifacts or image pulls | `oci-client` | 0.27 MB | |
| `object-store-s3` | No `s3://` artifacts, S3 content store, or S3 backups | `object_store`'s `aws` feature | 1.1 MB | |
| `net-plugins` | No WASM network plugins | `wasmi`, `wasmi_core` | 1.8 MB | |
| `net-kernel` | No kernel classifier (Linux) | `aya` | 0.7 MB | |
| `otel` | No OTLP trace export | the four `opentelemetry*` crates, `tracing-opentelemetry` | 0.28 MB | |
| `mcp` | No `legion mcp` | | 2.1 MB | |
| `plugin-<kind>-<name>` | That first-party exec plugin is not embedded (§ 6.6) | | a few KB | |

Group features `runtimes`, `apps`, and `plugins` turn on every member.

**The bar for a feature.** A part gets one only when leaving it out removes
a privileged capability (starting a control plane, running a VM, loading a
BPF program, binding port 443, holding a cluster-wide grant), removes a
dependency, or saves at least 1 MB of the `dist` binary, measured.
Everything else is just code. The built-in volume drivers, the scheduling
policies, and the `git`, `http`, and `file` fetchers stay unconditional.

By § 5.4, `manager`, `mcp`, `net-plugins`, `object-store-s3`, and
`app-autoscaler` clear the bar on size. The two runtime engines, the other
three apps, and `net-kernel` clear it on privilege alone. `otel` and
`fetcher-oci` clear it only as dependencies, at about a quarter of a
megabyte each; Phase 0 decides whether they stay.

**Three presets**, because Cargo features can add but not subtract:

| Preset | Is | For |
| --- | --- | --- |
| `full` (the default) | Everything | Managers, laptops, anyone who has not thought about it |
| `worker` | `full` without `manager` | Worker fleets |
| `minimal` | No optional feature: the CLI, a worker, the `exec` runtime, the `http`/`file`/`git` fetchers | Edge boxes, and the base for an app package (§ 6.9) |

```bash
cargo build --profile dist                                            # full
```

```bash
cargo build --profile dist --no-default-features --features worker    # worker-only
```

```bash
cargo install legion-rs --no-default-features --features worker
```

For "everything except", a script computes the list from `cargo metadata`
and prints the `cargo` command it runs:

```console
$ scripts/legion-build --without runtime-sandbox,app-ingress
$ scripts/legion-build --preset worker --without app-gitops
$ scripts/legion-build --preset minimal --with runtime-container
$ scripts/legion-build --without app-certs
error: app-ingress needs app-certs. Leave both out: --without app-certs,app-ingress
$ scripts/legion-build --list          # every feature, what it leaves out, what it implies
```

### 6.3 A worker-only build

Without `manager`, the binary does not link `legion-manager`: no raft, no
store, no scheduler, no CA issuance, no manager gRPC server. The client CLI
is unchanged, since it only speaks gRPC.

The commands that need manager code stay in the command tree and refuse by
name. `unrecognized subcommand 'manager'` would send someone to the docs for
a typo they did not make.

```console
$ legion start manager -c manager.ncl
Error: this legion build is worker-only: it has no manager role, so it can join a cluster but not run one.
Hint: install the full build on this host, or build with `--features manager` (`legion plugin list` shows what this build has)
```

```json
{ "ok": false, "error": { "code": "not_in_this_build", "message": "…", "hint": "…", "retryable": false } }
```

The same answer comes from the few commands that run manager code in the
CLI's own process: `legion keyring root-key generate`, `legion keyring
provider test`, and `legion net ca provider test`. The constants and pure helpers the CLI and the apps borrow from the
manager move down to `legion-common` (the convention already says a lower
crate's item used above moves down).

A released worker build is `legion-worker-<version>-<target>.tar.gz`. The
file inside is `bin/legion`, so `legion:self`, unit files, and every doc
example work unchanged.

### 6.4 Runtimes: the spec is always there, the engine is optional

A runtime has two halves that today share one trait:

- the **spec**: its config schema, validation, normalization, resource
  checks, implicit artifacts and constraints, and its static answers (is
  exec host-level, can it take a block volume);
- the **engine**: prepare, start, stop, exec, signal, checkpoint, usage.

The spec is small and is what a manager, the store, and `legion job
validate` need. The engine is where the code and the privilege are. So
`runtime-container` and `runtime-sandbox` gate the engines only. Every build
can *describe* every first-party runtime; a build can *run* the ones it has.

In the code, each runtime's files split into `spec` (unconditional) and
`engine` (behind the feature). With the feature off, `engine` is a stub whose
functions return the `not_in_this_build` error, `readiness()` reports
nothing, and the worker does not load the runtime. `exec` has no feature.
The internal `envelope` runtime (a reservation with nothing to start) stays
unconditional.

This also settles the mixed-build question for managers: a manager built
without the `sandbox` engine still admits sandbox jobs, exactly as a full
one does, and places them on nodes that can run them.

### 6.5 Apps: the whole app is optional

An app's feature gates its manifest row, its body row, and its CLI verbs
(`legion cert`, `legion gitops`) together. Unlike a runtime's spec, an app's
manifest is what *creates* the app in a cluster: a manager whose build has no
`gitops` seeds no `gitops` record, so there is nothing for it to
misrepresent. The verbs stay in the command tree and refuse with
`not_in_this_build`, like `start manager`.

Jobspec keys follow the app. `service.ingress` exists in a cluster because
the ingress app's record declares it; in a cluster without the app it is an
unknown field, with the usual did-you-mean.

In a cluster of mixed builds (full managers, slim workers, the ingress placed
on workers), the app's rendered job carries one more generated constraint,
"this node has `app.<name>`" (§ 6.7), and the build marker counts only nodes
that do. When no eligible node has it:

```console
$ legion app list
NAME     VERSION  SOURCE   STATUS   PLACEMENT  REPLICAS  UI  DETAIL
ingress  0.2.0    bundled  pending  workers    0/3       -   no eligible node has app ingress (3 nodes in pool edge are built without it); `legion plugin install apps/ingress` on them, or `legion app disable ingress`
```

### 6.6 Seam built-ins, and the first-party plugins move in

Two changes on the plugin side.

**The heavy built-ins get features** (`fetcher-oci`, `object-store-s3`,
`net-plugins`, `net-kernel`): the table in § 6.2. A job that needs one names
it in the error:

```text
Error: artifact "weights" uses s3://, and no node in pool "edge" has the s3 fetcher (artifacts/s3)
```

**The first-party exec plugins are embedded.** `legion-device-nvidia`,
`legion-kms-aws`, `legion-kms-vault`, and `legion-ca-vault` are POSIX shell
scripts of a few kilobytes. Each is compiled in behind a
`plugin-<kind>-<name>` feature and found by name. This is what makes "the
default build has a bunch of stuff in it" true for plugins, which today it
is not.

A plugin named in config is resolved by name, built in first, then
installed. `command` becomes the escape hatch for a program somewhere else,
not the normal spelling:

```nickel
# Before: a script the operator copied to the host
devices = { plugins = { nvidia = {
  command = ["/opt/legion/plugins/devices/nvidia/legion-device-nvidia", "--cdi=uuid"] } } }

# After: the plugin named nvidia, built in or installed
devices = { plugins = { nvidia = { args = ["--cdi=uuid"] } } }
```

| Seam | Names a plugin today | After |
| --- | --- | --- |
| Devices | `devices.plugins.<name>.command` (a path) | `<name>` resolves; `command` optional |
| Keyring | `rootKey = { provider = "exec", command = […] }` | `provider = "aws-kms"` resolves `keyring/aws-kms`; `exec` + `command` still works |
| Cluster CA | `exec` + `command` | Same as keyring |
| Autoscaler | `plugins.<name>.source` + `checksum`, or `command` | Also: neither, and `<name>` resolves `scaling/<name>` on the host |
| Volumes | By name under the plugin root | Unchanged |
| Rust plugins | By name under the plugin root | Unchanged, plus the flat layout (§ 6.8) |

A built-in exec plugin is written once to
`<data dir>/builtin-plugins/<kind>/<name>/<sha256>/` (root-owned, not
writable by others), checked against its hash at each start, and run from
there. `aws-asg` is a Rust program with its own crate and lockfile; it is
not embedded, and ships as a package (§ 6.10).

The example Rust plugins (`noop`, `random`, `slow_start`, `demo`, `example`,
`panicking`) stay examples and test fixtures. They are packaged with `legion
plugin pack`, which is also what a third-party author uses.

### 6.7 What a node says it has, and what placement does with it

A worker publishes one fingerprint attribute per runtime and app it can run:

```text
runtime.exec       = "built in"
runtime.container  = "built in"
runtime.tpu-vm     = "installed"
app.ingress        = "installed"
```

`runtime.<name>` is set when the engine is present **and** its readiness
check passes, and is re-published when that changes. So a node whose build
has the `container` engine but whose host has no runc does not claim it,
which fixes the start failure Legion has today. The reason is still in the
node's health checks, where it is now.

Normalization adds a generated constraint to every task, and one more to
every bundled app's job, with an operator that already exists:

```nickel
{ key = "runtime.sandbox", operator = "is_set" }   # every task, for its runtime
{ key = "app.ingress", operator = "is_set" }       # a bundled app's job
```

This is the path `container.idmappedMounts` already uses
(`constrain_to_idmapped_mounts`, idempotent on resubmit). No new scheduler
code path; the benchmark obligation is in § 9.

A job no node can run is **accepted with a warning**, not refused: nodes
join later, and node groups scale up.

```console
$ legion job run agent.ncl
submitted agent (version 1)
Warning: task "vm" uses runtime "sandbox", and no ready node in pool "edge" can run it: 4 nodes are built without the sandbox engine. It will place when one can.
$ legion job status agent
vm   0/2   blocked: no node has runtime.sandbox (4 of 4 nodes in pool edge filtered)
```

### 6.8 Plugin packages and `legion plugin`

A **package** is a `.tar.gz` of one plugin directory. Installed, it is flat:

```text
<plugin root>/
  tasks/noop/          plugin.json  libnoop.so
  devices/tpu/         plugin.json  legion-device-tpu    .install.json
  apps/ingress/        plugin.json  app.ncl  legion      .install.json
  scaling/aws-asg/     plugin.json  aws-asg              .install.json
```

Rust plugins and volume plugins have a `plugin.json` today. Every kind gets
one: the existing keys, the kind's own protocol version, and three new keys.

```json
{
  "kind": "devices",
  "name": "tpu",
  "legionDeviceApiVersion": 1,
  "version": "0.3.0",
  "target": "x86_64-unknown-linux-gnu",
  "entrypoint": "legion-device-tpu"
}
```

`target` is omitted for a script. The loader looks for
`<root>/<kind>/<name>/<entrypoint>` first, then the two Cargo layouts it
knows today, which stay for development. `.install.json` is written by the
installer: the source, the checksum, when, and which user.

```console
$ sudo legion plugin install https://example.com/legion-device-tpu-0.3.0-linux-amd64.tar.gz \
    --checksum sha256:9f2c1d…
devices/tpu 0.3.0 (exec, legion-device/1) for x86_64-unknown-linux-gnu
  from    https://example.com/legion-device-tpu-0.3.0-linux-amd64.tar.gz
  sha256  9f2c1d… (matches)
  into    /usr/local/lib/legion/plugins/devices/tpu
A worker whose config names it runs it as the worker's user (root). Install? [y/N] y
installed devices/tpu 0.3.0
Next: add `devices.plugins.tpu = {}` to the worker config and restart the worker.
```

| Command | Does |
| --- | --- |
| `legion plugin install <source>` | `<source>` is an `https` URL, a local `.tar.gz` or directory, or `<kind>/<name>` (looked up in the release index, § 6.10). Flags: `--checksum`, `--yes`, `--dry-run`, `--force` (replace an installed one), `--dir`, `-j` |
| `legion plugin list` | § 6.1 |
| `legion plugin remove <kind>/<name>` | Deletes an installed directory. Refuses a built-in |
| `legion plugin verify` | Runs every load-time check on everything installed, without loading it into a running process. Non-zero exit if anything would be refused. Run it after upgrading `legion` |
| `legion plugin pack <dir>` | For authors: checks the manifest, writes `<kind>-<name>-<version>-<target>.tar.gz`, prints its SHA-256 |

What `install` checks, in order, before anything reaches the plugin root:

1. The download is under the size cap and its SHA-256 matches. A URL
   **requires** a checksum (`--checksum`, or the index's). A local file does
   not: you already have it.
2. It unpacks with the artifact pipeline's safe unpack: no absolute paths,
   no `..`, no links out of the directory, a size budget.
3. The manifest's kind is known, its name is `[a-z0-9][a-z0-9_-]*`, its API
   version is this binary's, its target is this host's.
4. Nothing built in has that kind and name. (It would never be loaded.)
5. For a Rust plugin: the library's build fingerprint equals this binary's.
   The library is opened and the fingerprint symbol read; its constructor is
   not called.
6. The directory is moved into place in one rename. An interrupted install
   leaves nothing.

**By hand** is the same result with fewer checks:

```bash
sudo tar -xzf legion-device-tpu-0.3.0-linux-amd64.tar.gz -C /usr/local/lib/legion/plugins/devices/
```

Checks 3 to 5 run again whenever Legion loads a plugin, however it got
there, together with one new one: **the directory and its files must be
owned by root or by the user Legion runs as, and not writable by group or
others.** Otherwise the plugin is refused, with the `chmod` in the hint.

| Kind | Takes effect |
| --- | --- |
| `tasks`, `node_checks`, `devices`, `apps` | When the worker restarts (it publishes what it has at registration) |
| `artifacts`, `volumes` | On next use |
| `scheduling`, `rollouts`, `keyring`, `cluster_ca` | When the manager restarts or the config naming it is loaded |
| `scaling` | When the autoscaler's config names it |

`legion plugin install` is local. It is not an RPC and not an MCP tool: a
remote call that puts code on a host and runs it as root is a different
security decision from the one this plan makes (§ 6.11).

**After upgrading Legion**, installed Rust plugins are refused at load,
because the build fingerprint changed, with the existing "built differently"
error. `legion plugin verify` lists them; `legion plugin install --force
<kind>/<name>` fetches the build for the new version. Exec plugins and apps
speak versioned protocols and keep working.

### 6.9 Apps: two ways back

**Across the cluster, one command.** This is today's `legion app install`,
unchanged, pointed at a first-party manifest. The release publishes
`apps/<name>/app.ncl` for each bundled app; its job fetches the app's
executable as a checksummed artifact. A name is looked up in the index.

```console
$ legion app install gitops
gitops 0.2.0 runs its own code (1 artifact) on your managers, as its own unprivileged user, …
Your token holds all of these. Grant them and install? [y/N]
```

The result is an *installed* app, with everything that means in
[apps.md](../../docs/apps.md): the installer's grant, `legion app upgrade`,
`uninstall`. Nothing on any node's disk changes.

**On a host, from files.** A package `apps/<name>/` in the plugin root makes
the app part of that host's build:

- `legion app run <name>` runs the compiled-in body, or else executes
  `<plugin root>/apps/<name>/<entrypoint>` with the same environment. The
  job's command stays `legion:self app run <name>` either way, so one
  rendered job runs across mixed nodes.
- The node publishes `app.<name> = "installed"`.
- A manager that has the package seeds the app as it seeds a compiled-in
  one, granted by `legion:bootstrap`, **disabled** until `legion app enable
  <name>` or `apps.<name>.enabled = true` in the manager config. `legion app
  list` shows `SOURCE bundled (disk)`.

The second route is for air-gapped clusters and for images that bake
everything in. It has the bundled trust model, which is a real statement:
whoever can write a manager's plugin root can grant an app its permissions.
That person is root on a manager and could have replaced the binary.

An app package's executable is `legion` itself, built `--preset minimal
--with app-<name>`: the features already produce it, and no app SDK crate is
needed first. § 5.4 says what that costs: about 70% of a full binary, to
carry an app whose own code is under 1.5 MB. Leaving an app out for size and
then installing it back is a net loss; this route is for clusters that left
an app out on purpose and changed their mind, and for the air gap (§ 11,
Q7).

### 6.10 Releases

One release, per target (linux amd64 and arm64, macOS arm64):

```text
legion-0.2.0-x86_64-unknown-linux-gnu.tar.gz           bin/legion             full
legion-worker-0.2.0-x86_64-unknown-linux-gnu.tar.gz    bin/legion             worker preset
plugins/apps-gitops-0.2.0-x86_64-unknown-linux-gnu.tar.gz    (and each other app)
plugins/scaling-aws-asg-0.2.0-x86_64-unknown-linux-gnu.tar.gz
plugins/devices-nvidia-0.2.0.tar.gz                    (each embedded plugin, for builds without it)
apps/gitops/app.ncl                                    (each app's manifest, for `legion app install`)
index.json                                             kind, name, version, target, url, sha256
SHA256SUMS
```

`legion plugin install <kind>/<name>` and `legion app install <name>` read
`index.json` for this binary's own version, from the release URL or from
`LEGION_PLUGIN_INDEX` (a mirror, or a `file://` directory in an air gap). A
0.2.0 binary installs 0.2.0 first-party packages and nothing else.

### 6.11 Trust

- A plugin runs with its host process's authority, usually root. Installing
  one is the trust decision, and it is made on the host, by someone who can
  already write there.
- URLs need a checksum. The index supplies them for first-party packages,
  over HTTPS from the release origin. That is integrity, not provenance:
  someone who controls the release origin controls both. Signing is § 11, Q2.
- Nothing installs itself: not at start, not because a job asked, not
  because a manager said so.
- The permission check at load closes the "any local user can drop a
  library in `plugins/`" hole, which exists today.
- A worker-only build removes the manager's code from the host. It does not
  stop someone with root from installing a full build; it stops the binary
  that is there from being one.

### 6.12 Failure modes

| What happened | What the user sees |
| --- | --- |
| A command this build lacks | `not_in_this_build`, naming the feature and how to see what the build has |
| A job needs a runtime, fetcher, or app no node has | A submit warning, and `blocked: no node has runtime.<name>` in the job's status |
| The checksum does not match | `install` stops before unpacking; both hashes are printed |
| A package for another OS or architecture | `devices/tpu is built for aarch64-unknown-linux-gnu; this host is x86_64-unknown-linux-gnu` |
| A Rust plugin from another build | The existing "built differently from this legion binary" error, at install and again at load |
| A name that is built in | `tasks/container is built into this binary; there is nothing to install` |
| A plugin directory others can write | Refused at load: `…/devices/tpu is writable by group; chmod -R go-w it` |
| No index reachable | `could not read the plugin index at <url>; pass a URL and --checksum, or set LEGION_PLUGIN_INDEX` |
| An installed plugin after a Legion upgrade | `legion plugin verify` lists it; the node stops publishing what it provided, and placement moves on |

## 7. Proto, struct, config, and persistence changes

- **Cargo:** the features of § 6.2 on `legion-rs`, forwarded to
  `legion-manager`, `legion-worker`, and `legion-common`; `legion-manager`
  becomes an optional dependency of the binary crate. `tests/integration`
  gets `required-features = ["full"]`.
- **Proto:** none. The node attributes ride the existing fingerprint map.
- **Jobspec:** none, so nothing for `configs/jobs/maximal.ncl`. Generated
  constraints are not user fields.
- **Worker config:** `devices.plugins.<name>.command` becomes optional;
  `args` is new.
- **Manager config:** `encryption.rootKey.provider` and the cluster CA
  provider accept a plugin name.
- **App config (autoscaler):** a plugin with neither `source` nor `command`
  resolves by name.
- **`plugin.json`:** `version`, `target`, `entrypoint`; `.install.json`
  beside it.
- **Env:** `LEGION_PLUGIN_INDEX`. `LEGION_PLUGIN_DIR` is unchanged.
- **Errors:** the `not_in_this_build` code in the CLI envelope.
- **App records:** an on-disk app is `AppSource::Bundled` with `source_ref =
  "disk"`. No migration.
- **CLI:** `legion plugin list|install|remove|verify|pack`; `legion app
  install <name>`; each in `capabilities.rs`. `legion plugin list` is an MCP
  tool (read-only); the others are not.

## 8. Implementation phases

Each ships alone and is worth having alone.

**Phase 0: measure, and write the catalog.** Run `cargo bloat --profile dist
--crates` (this also closes the TODO item "Find what makes the linux/amd64
binary 97 MB") and hold § 6.2's list to the bar. Add the catalog and `legion
plugin list` for what exists today. *Value: operators can see what a node
has; the feature list is evidence, not a guess.*

**Phase 1: nodes say what they can run.** `runtime.<name>` attributes, the
generated constraint, the submit warning, the blocked reason. *Value: fixes
"a container task lands on a node with no runc" for every cluster, with no
build changes at all.* Ships with scheduler benchmark numbers.

**Phase 2: the worker-only build.** The `manager` feature, the three
presets, `scripts/legion-build`, `not_in_this_build`, the moves into
`legion-common`, the CI matrix.

**Phase 3: runtime engines and seam built-ins.** The spec/engine split,
`runtime-container`, `runtime-sandbox`, `fetcher-oci`, `object-store-s3`,
`net-plugins`, `net-kernel`, `otel`, `mcp`.

**Phase 4: apps.** `app-*` features, `app.<name>` attributes, the app job's
constraint, build-marker eligibility.

**Phase 5: packages.** The flat layout, the manifest fields, `legion plugin
install|remove|verify|pack` from a URL or a file, the permission check, and
plugins named in config without `command`. Independent of Phases 2 to 4: it
is useful for third-party plugins on today's build.

**Phase 6: first-party plugins in the binary.** Embed the four scripts
behind `plugin-*` features.

**Phase 7: releases.** The release workflow, both variants, the packages,
`index.json`, install by name, `legion app install <name>`, and
`docs/installation.md` rewritten around downloads.

**Phase 8: apps from disk.** `apps/<name>/` packages, the `legion app run`
fall-through, seeding from disk.

**Phase 9 (gated): first-party engines as packages.** `tasks/container` and
`tasks/sandbox` installable into a build without them. Not started until
all three exist, each already an open item:

1. a plugin API crate, so a plugin does not link and duplicate all of Legion
   ([TODO, "A plugin API crate"](../TODO.md));
2. panic containment for task runtime plugins (review PLG1);
3. the engines reaching worker state through a handle they are given, not
   through statics a plugin would get its own copy of.

Until then, a node gets those engines by running a build that has them, and
`legion plugin list` says so instead of offering an install.

**Deferred, on purpose.** Pushing plugins from the manager to nodes;
signing; an app SDK crate; an out-of-process runtime protocol; a feature for
`exec`; a `plugins = [{ source, checksum }]` block in the worker config.

## 9. Testing plan

- **Phase 0:** a unit test that the catalog matches `Cargo.toml`'s features
  (parse the manifest); `legion plugin list -j` snapshot for the full build.
- **Phase 1:** unit tests for the generated constraint; an integration
  module (`tests/integration/modular_build.rs`) with a worker that publishes
  no `runtime.sandbox`: the job is accepted with the warning, stays blocked
  with the named reason, and places when a node that has it joins.
  `cargo bench --bench scheduler_bench -- candidate_construction` before and
  after, both numbers in the PR, since every task gains a constraint.
- **Phases 2 to 4:** a CI job that runs `cargo check --locked` for `full`,
  `worker`, `minimal`, and `full` minus each feature in turn. Check, not
  test: one full test build is already most of a runner's memory.
  `cargo test -p legion-worker -p legion-common --no-default-features` for
  the unit tests that must pass without anything optional. A smoke job
  builds the `worker` preset, asserts `legion start manager -j` returns
  `not_in_this_build`, joins a full manager, and runs an `exec` job. A
  cluster test with one full and one sandbox-less worker.
- **Phase 4:** the existing test that `MANIFESTS` and `BUNDLED` name the same
  apps, under each `app-*` combination the CI matrix covers; a reconciler
  test that an app waits, with the detail line, when no eligible node has it.
- **Phase 5:** fixture tarballs for each refusal in § 6.12 (bad checksum,
  wrong target, traversal, link escape, built-in name, group-writable
  directory, fingerprint mismatch using the `noop` fixture); a killed
  install leaves no directory; `verify` exits non-zero on a stale library;
  `pack` then `install` round-trips.
- **Phase 7:** the release workflow runs `legion plugin install` for every
  index entry against the binary it just built.
- **Phase 8:** a manager with an `apps/` package seeds it disabled; a node
  with the package runs `legion app run` through the fall-through; a node
  without it is never chosen.

## 10. Rollout and backward compatibility

The default build keeps every feature on, so `cargo build`, `cargo test`,
and `cargo install legion-rs` produce what they produce today. What changes
for an existing cluster:

- **Phase 1 breaks mixed versions during the upgrade.** A new manager places
  a task only on a node that publishes `runtime.<name>`, and an old worker
  publishes none. Upgrade workers with or before the managers, or expect
  nothing new to place on old workers until they are. No shim: Legion has no
  installed base to keep one for.
- **Nodes without a working container runtime stop receiving container
  tasks.** That is the fix, and it changes what `legion job status` shows
  for a job that used to fail at start.
- **A plugin directory writable by group or others stops loading** (Phase
  5). The error names the `chmod`.
- **`devices.plugins.<name>.command` is optional.** Existing configs with a
  path keep working: `command` is the escape hatch, not a deprecated field.
- **A first-party app installed by URL keeps its record** if the managers
  later move to a build that bundles it; the build does not seed over an
  existing record. To switch to the bundled one, uninstall it, and it is
  seeded within a minute.

Docs when each phase ships: a new `docs/builds-and-plugins.md` (features,
presets, the script, `legion plugin`, the package format, the trust
statements of § 6.11, and what is not installable yet), and updates to
`plugin-system.md`, `apps.md`, `installation.md`, each task-runtime page,
`devices.md`, `agent-readiness.md`, `codebase-conventions.md` (the bar for a
feature), and `internal_docs/development.md`.

## 11. Open questions

1. **Do `otel` and `fetcher-oci` stay features?** Each is about 0.3 MB of
   code. Lean: keep `otel` only if dropping its five crates shortens the
   build measurably; fold `fetcher-oci` into `runtime-container` unless
   someone wants `oci://` artifacts without the runtime.
2. **Signing.** Lean: checksums now. Sign (minisign or cosign) when there is
   a release key and a rotation story to go with it; a signature nobody
   verifies the key of is decoration.
3. **Per-architecture artifacts in an app manifest.** `legion app install
   gitops` needs the right executable on amd64 and arm64 nodes. Lean: check
   whether the jobspec can already express it (an artifact per constraint);
   if not, the index resolves the manifest per target and mixed-architecture
   placement waits for it.
4. **Is Phase 9 worth doing?** Lean: not unless someone asks. Published
   build variants cover "this node needs the sandbox", and the effort may be
   better spent on an out-of-process runtime protocol, which would also give
   crash isolation.
5. **Fleet-wide installs.** Lean: later, as a `plugins` block in the worker
   config that the worker fetches at start, the shape the autoscaler already
   has. Not the manager pushing code.
6. **Does an installed first-party app pass installed-app admission?** The
   ingress adds `net_bind_service`, and installs refuse host-level
   constructs. Lean: verify each manifest in Phase 7; the ingress may be
   disk-only until that is settled.
7. **An app package is mostly a second copy of Legion** (§ 6.9). Lean:
   accept it for Phase 8, and say so on the docs page. The fix is an app SDK
   crate small enough that an app's executable is a few megabytes; build it
   when a third-party Rust app needs it, not for this.

## 12. Acceptance criteria

- **Phase 0:** `legion plugin list -j` on a full build lists every catalog
  row as `built in`. The plan's § 5.4 has a `dist` per-crate table, and
  § 6.2's list is edited to match it.
- **Phase 1:** a `container` job on a cluster whose only node has no runc is
  accepted with a warning and reads `blocked: no node has runtime.container`.
  Scheduler benchmarks in the PR are within noise.
- **Phase 2:** `scripts/legion-build --preset worker` produces a `legion`
  with no `legion_manager` symbol in it (`nm | grep` on an unstripped build
  is empty). `legion
  start manager -j` prints `not_in_this_build`. It joins a full manager and
  runs a job. The size difference is recorded in `Cargo.toml`'s `dist`
  comment beside the full number.
- **Phase 3:** a manager built `--without runtime-sandbox` admits a sandbox
  job and places it on a worker that has the engine. `legion job validate`
  in a `minimal` build validates a sandbox task.
- **Phase 4:** a cluster of managers built `--without app-gitops` has no
  `gitops` record and no `legion gitops` command beyond the refusal.
- **Phase 5:** the transcript in § 6.8 is real. Every row of § 6.12 that
  concerns packages has a test. Unpacking the same tarball by hand loads the
  same plugin.
- **Phase 6:** on a full build, `devices.plugins.nvidia = {}` works with no
  file copied to the host.
- **Phase 7:** on a fresh machine, downloading the worker tarball, `legion
  join`, and `legion plugin install scaling/aws-asg` all work with no Rust
  toolchain.
- **Phase 8:** scenario 3 in § 3, on three managers with no network.
