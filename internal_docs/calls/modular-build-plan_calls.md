# Judgement Calls: Modular Builds and Installable Plugins

Plan: [modular-build-plan.md](../plans/modular-build-plan.md)

Entries are ranked by project impact, highest first; an entry's number is its
rank. These are the calls made while writing the plan. Nothing is built yet,
so each one is still cheap to overturn, and the ones implementation settles
differently get rewritten here, not quietly dropped.

## 1. The request's "drivers" are left out at build time; installing the first-party ones later is deferred

**Call.** The request asked to leave out drivers, apps, and plugins, and to
"later install the plugins/apps via a URL or adding them to the filesystem".
The plan reads that literally. Every plugin kind and every app is installable
(Phases 5 to 8). The `container` and `sandbox` engines can be *left out* in
Phase 3, but putting one back into a build without it is Phase 9, gated on
three things that do not exist. Until then:

```console
$ legion plugin list
tasks  sandbox  absent  -  not in this build
```

with no install offered, and the way to get the engine on a node is a build
that has it. Third-party runtimes written as Rust plugins install like any
other plugin from Phase 5 on.

**Alternatives.** (a) Package the first-party engines as Rust plugins now,
built from the same source by the release. (b) Design an out-of-process
runtime protocol (Nomad's model) and move the engines behind it.

**Trade-off.** The one part of the request a reader could fairly expect
("I left out the sandbox, now I want it on this pool") is answered with
"install the other build", not one command. Revisit if operators ask for it
more than once.

**Why (simplicity, and not shipping something unsound).** A Rust plugin
links all of Legion and gets its own copy of every static in the worker
crate; the engines reach worker state through those statics. A packaged
`container` engine would load, pass its fingerprint check, and talk to a
second, uninitialized copy of the worker's state. Task runtime plugins also
have no panic containment. And because the plugin links everything, the
package would be most of a binary (about 70% is shared floor, plan § 5.4) to
deliver an engine that measures under half a megabyte. (b) is the
better long-term shape and is a plan of its own.

## 2. Optional parts are Cargo features on the one binary, default all on

**Call.** `legion-rs` gains features; `default = ["full"]`; the worker-only
build is the same crate and the same file name.

```bash
cargo build --profile dist --no-default-features --features worker
```

**Alternatives.** (a) A second binary crate (`legion-workerd`) that depends
only on `legion-worker`: the dependency graph, not `cfg`, guarantees there is
no manager in it. (b) One binary, everything compiled, roles switched off in
config.

**Trade-off.** `#[cfg(feature = …)]` in roughly 40 places in the binary
crate, and a CI matrix to keep the combinations compiling. A feature
combination nobody builds can rot between releases.

**Why (simplicity).** One name. `legion:self`, every unit file, every doc
example, and every script keeps working on a worker-only host. (a) is the
stronger guarantee but forks the CLI, which lives in the binary crate; the
crate-split plan's non-goal "More binaries" still holds. (b) is what exists
(`apps.<name>.enabled`) and does not remove code, which is the request.

## 3. A runtime's spec is always compiled; only its engine is optional

**Call.** `runtime-container` and `runtime-sandbox` gate prepare, start,
stop, exec, and checkpoint. Schema, validation, normalization, and the static
answers stay in every build. A manager built without the `sandbox` engine
admits sandbox jobs.

**Alternatives.** Gate the whole runtime. A manager then needs a runtime
present to admit its jobs, which is today's rule for third-party runtime
plugins and needs no refactor.

**Trade-off.** A real refactor: `sandbox.rs` (4.5k lines) and `container.rs`
(4k) mix both halves and have to be split, and a stub engine has to exist
for the off case. A few hundred kilobytes of schema stay in a `minimal`
build that will never run a VM.

**Why (simplicity for the operator).** With the whole runtime gated, what a
cluster accepts would depend on how each *manager* was built, and a leader
change between two differently built managers could change the answer to
`legion job run`. With the spec always present, a build difference only ever
changes where a job can run, which placement already explains.

## 4. A missing runtime is a placement fact, not a submit error

**Call.** Nodes publish `runtime.<name>` and `app.<name>`; every task gets a
generated constraint; a job nothing can run is accepted with a warning and
reads `blocked: no node has runtime.sandbox`.

**Alternatives.** Refuse at submit when no node in the pool can run it.

**Trade-off.** A typo in a pool name plus a slim fleet gives a job that sits
blocked, where a refusal would have been immediate. The warning at submit is
the mitigation. Every task now carries one more constraint through the
scheduler: Phase 1 owes benchmark numbers.

**Why (simplicity).** Nodes join later and node groups scale from zero, so
"no node has it right now" is not "this job is wrong". It is also the
mechanism Legion already has: volume drivers and idmapped mounts are
constraints on fingerprint attributes. One more rule of the same kind,
instead of a new admission stage.

## 5. `legion plugin install` is local to the host, and not an RPC

**Call.** The command writes to the plugin root of the machine it runs on.
There is no `InstallPlugin` RPC, no MCP tool, and no manager-driven
distribution.

**Alternatives.** A cluster-level object ("this pool runs these plugins")
that workers reconcile, the way WASM network plugins are already stored in
the manager and delivered to workers.

**Trade-off.** Installing on 200 nodes is 200 runs of the command, by
cloud-init or config management. The single command the request asked for is
single per host, not per cluster.

**Why (security boundary).** A network plugin is sandboxed WASM. These are
native code running as root. A remote call that places native code on every
node is a control-plane-compromise-equals-fleet-root path, and it deserves
its own plan with signing in it, not a side effect of a build-size feature.
Apps keep their cluster-wide command because their code already arrives
through the artifact system, as an unprivileged task.

## 6. Apps are optional whole, unlike runtimes

**Call.** An `app-*` feature gates the manifest, the body, and the app's CLI
verbs together. A cluster whose managers lack `gitops` has no `gitops`
record.

**Alternatives.** Keep every manifest compiled (as with runtime specs) and
gate only bodies: the app always exists in the cluster and waits for a node
that has its code.

**Trade-off.** Two rules where one would be tidier: "spec always" for
runtimes, "all or nothing" for apps. Leaving all four apps out saves about
3 MB, and an app package to put one back is most of a second binary (plan
§ 6.9), so this is a capability switch, not a size one.

**Why (simplicity).** A runtime's spec only describes what jobs may say. An
app's manifest *creates* something: a record, a grant, a job, jobspec keys,
UI pages. Seeding an app that no node can run would give every such cluster
a permanently pending app to explain.

## 7. An app directory on a manager's disk is trusted like a bundled app

**Call.** A manager that finds `<plugin root>/apps/<name>/` seeds it with
its manifest's permissions granted by `legion:bootstrap`, disabled until
enabled.

```console
$ legion app list
NAME    VERSION  SOURCE          STATUS
gitops  0.2.0    bundled (disk)  disabled
```

**Alternatives.** (a) Treat an on-disk app as a manifest source only, and
still require `legion app install` with the installer's own grant. (b) Seed
it enabled, as compiled-in apps mostly are.

**Trade-off.** Whoever can write a manager's plugin root can grant an app
anything, with no "you can grant only what you hold" check. `legion app
enable` then needs only `app:write`.

**Why (ease, for the air gap).** That person is root on a manager and could
replace the binary, which grants the same thing. Making them run an install
command would add a step and no protection. Seeding disabled is the one
concession: a file copied by mistake does nothing until someone asks.

## 8. Subtraction is a script, because Cargo features only add

**Call.** Three presets (`full`, `worker`, `minimal`) as features, and
`scripts/legion-build --without a,b` for everything else.

**Alternatives.** (a) Only raw `cargo` with a documented feature list. (b)
Negative features (`no-sandbox`). (c) A `build.rs` that reads an env var.

**Trade-off.** A script to maintain, and a second way to build that `cargo
install` users do not get.

**Why (ease).** People think "without the sandbox", and the request was
phrased that way. (b) breaks Cargo's rule that features are additive and
goes wrong the first time two crates disagree. (c) cannot drop a dependency,
which is most of the size.

## 9. The first-party exec plugins are embedded as the scripts they are

**Call.** `legion-device-nvidia`, both key providers, and the Vault CA
provider are compiled in as bytes, written to a root-owned directory on
first use, and run from there. A plugin in config is found by name.

**Alternatives.** (a) Port them to Rust subcommands of the binary
(`legion:self device-plugin nvidia`). (b) Ship them beside the binary in the
release tarball and keep naming them by path.

**Trade-off.** They still need `/bin/sh` and their tools (`nvidia-smi`, the
`aws` CLI) on the host, and Legion writes an executable to disk at runtime.

**Why (ease, extensibility).** They double as the reference implementations
of each exec protocol; a 60-line script an author can read is worth more
than the same logic inside the binary. (b) leaves `cargo install` users with
no plugins and keeps absolute paths in every config.

## 10. A feature needs a dependency, a privilege, or a megabyte

**Call.** A part gets a feature only if leaving it out drops a privileged
capability, drops a dependency, or saves 1 MB of the `dist` binary,
measured. Under twenty features, not a hundred. Measuring changed what the
bar is for: the two runtime engines are 0.26 and 0.47 MB of code, and three
of the four apps are under 1 MB (plan § 5.4), so they pass on privilege, not
size. Only the manager role (about 19% of the code) is a real size lever.

**Alternatives.** Vector's model: a feature per component.

**Trade-off.** Someone who wants Legion without, say, the NFS volume driver
cannot have it. And the plan cannot be sold as "a small Legion": a build
with everything optional left out still keeps about 70% of the code.

**Why (simplicity).** Every feature doubles the combinations that have to
compile, and CI checks each one. The NFS driver costs nothing and risks
nothing.

## 11. `legion plugin` covers apps on disk; no new noun

**Call.** The command is `legion plugin`, the directory stays the plugin
root, and an app package is the kind `apps` inside it. "Component" appears
only as the name of the internal catalog.

**Alternatives.** `legion component …` or `legion package …` as a neutral
noun over plugins and apps.

**Trade-off.** `legion plugin install apps/gitops` (files on this host) and
`legion app install gitops` (a record in the cluster) read alike and do
different things. The docs page has to say which is which in its first
screen.

**Why (simplicity).** Legion's users already have two nouns. A third to
unify them is one more thing to learn before installing anything.

## 12. Commands a build lacks stay in the command tree

**Call.** `legion start manager` on a worker-only build fails with
`not_in_this_build` and a hint, instead of clap's `unrecognized subcommand`.

**Alternatives.** Remove the subcommands under `cfg`, so `--help` shows only
what works.

**Trade-off.** `legion --help` on a slim build lists commands that will
refuse.

**Why (ease).** The person who types it most is following a doc written for
the full build. "This build is worker-only" answers them; "unrecognized
subcommand" sends them looking for a typo.

## 13. `exec` has no feature

**Call.** The `exec` runtime is in every build.

**Alternatives.** `runtime-exec`, for a container-only worker that can run
no host process.

**Trade-off.** A hardened fleet cannot remove host-level execution at build
time; it uses a namespace policy's `runtimes` instead.

**Why (simplicity).** Bundled apps run as `legion:self app run`, on `exec`.
A build without it cannot run any app, and the system worker in every
manager depends on it.
