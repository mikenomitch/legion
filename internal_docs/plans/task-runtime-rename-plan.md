# Task Kind → Task Runtime Rename

> **Judgement calls:** [`../calls/task-runtime-rename-plan_calls.md`](../calls/task-runtime-rename-plan_calls.md)

**Status: IN EXECUTION.** Single-pass rename, no compatibility shim. The old
`kind` spelling is rejected at submit rather than deprecated — see §10.
Deliberately deferred: nothing. The rename is all-or-nothing by design; a
half-renamed vocabulary is worse than either end state.

## 1. Problem statement

Legion calls the pluggable component that runs a task its **kind**:

```jsonnet
tasks: { web: { kind: 'container', config: { image: 'nginx' } } }
```

Three concrete problems with that word.

**It collides with Kubernetes.** In K8s, `kind` is the *object type* —
`kind: Deployment`, `kind: Service`. Every operator arriving from K8s reads
`kind: container` as "this object is a container", not "a container runs this
task". The word is spent.

**It does not distinguish the axis.** `kind` is Legion's generic discriminator
and appears on at least five unrelated things: health checks
(`TaskCheckKind`, `src/structs/job.rs:480`), scheduler events
(`src/storage/events.rs:29`), evals (`src/structs/eval.rs:31`), auth methods
(`src/manager/auth_method.rs`), and the plugin manifest's category
(`src/plugin_runtime.rs:10`, the `plugins/<kind>/<name>/` directory). Grepping
`kind` to find the task-execution axis returns mostly noise, and a reader of
`task.kind` next to `check.kind` has no signal that these are different concepts.

**It is vaguer than the thing it names.** Every value the field takes — `exec`,
`container`, `noop` — is a *runtime*: the thing responsible for starting,
supervising, signalling, and reaping the workload. `kind` describes a taxonomy;
`runtime` describes a responsibility, which is what plugin authors actually
implement.

Today a user who wants to know what can run their task has to read
`docs/task-kind-plugins.md` and infer that "kind" means "runtime". That is a
word we can just spell correctly.

## 2. Prior art

| System | Word | What it names |
| --- | --- | --- |
| [Nomad](https://developer.hashicorp.com/nomad/docs/job-specification/task#driver) | `driver` | `exec`, `docker`, `java`, `qemu` — pluggable task executors |
| [Kubernetes](https://kubernetes.io/docs/concepts/containers/runtime-class/) | `runtimeClassName` / CRI "container runtime" | runc, gVisor, Kata — the thing that runs the container |
| [Nomad](https://developer.hashicorp.com/nomad/docs/job-specification/job#type) | `type` | `service`, `batch`, `system` — *scheduler* type, not executor |
| GitHub Actions | `uses` / `runs-on` | action reference / runner machine |
| Airflow | `executor` | Celery, Kubernetes, Local — where tasks run |

**What we take.** K8s's *runtime* vocabulary. "Container runtime" is the single
most widely understood name in the industry for "the component that runs the
workload", and it generalizes cleanly past containers: `exec` is a process
runtime, `noop` is a null runtime. It also reads correctly in the plugin trait —
a `TaskPlugin` implements a runtime.

**What we reject.** Nomad's `driver`. It is the closest prior art and was
seriously considered (see calls §2); rejected because "driver" carries a
device/kernel connotation that fits `exec`/`container` but not the broader
plugin surface, and because Legion is not trying to be a Nomad clone at the
vocabulary level. `type` is rejected as contentless and because Nomad already
uses it for a different axis, which would import a fresh collision.

**Naming collision this creates.** `runtime` is already load-bearing inside
Legion — see §5. Resolving that is the substance of this plan, not an
afterthought.

## 3. User scenarios

**Writing a jobspec.** An operator writes a task and picks what runs it:

```jsonnet
tasks: {
  web: {
    runtime: 'container',
    config: { image: 'nginx:1.27' },
  },
  migrate: {
    runtime: 'exec',
    lifecycle: 'prestart',
    config: { command: './migrate.sh' },
  },
}
```

**Hitting the hard break.** An operator submits a pre-rename jobspec and gets a
diagnostic that names the fix, not a generic parse error:

```
$ legion job run configs/jobs/demo.jsonnet
error: task `web`: unknown field `kind`, expected `runtime`
  hint: `kind` was renamed to `runtime`; replace `kind: "container"` with `runtime: "container"`
```

**Writing a task plugin.** A plugin author implements a runtime and receives
per-task live state that is no longer called "runtime":

```rust
impl TaskPlugin for ContainerRuntime {
    fn name(&self) -> &str { "container" }

    fn start(&self, alloc: &Allocation, task: &TaskConfig,
             instance: &mut TaskInstance) -> R<()> { … }
}
```

## 4. Goals and non-goals

**Goals.**

- One word for the task-execution axis, everywhere: jobspec, proto, CLI, DB
  column, Rust structs, plugin API, docs, UI.
- Free the word `runtime` from its current internal meaning so the rename does
  not create a worse ambiguity than the one it fixes.
- Fail loudly and helpfully on the old spelling.

**Non-goals.**

- Renaming any *other* `kind` axis. Health-check kind, event kind, eval kind,
  auth-method kind, and the plugin-manifest category stay as they are. They do
  not collide with anything and are correctly named for what they are.
- Renaming the `plugins/tasks/` directory or the plugin manifest's
  `kind: "tasks"` category field — that is the plugin *category* axis, not the
  task-execution axis.
- Any behavior change. This is a vocabulary change with a schema migration; no
  scheduling, staging, or supervision semantics move.
- A compatibility alias. Explicitly rejected — see §10 and calls §1.

## 5. Background — what exists today

**The jobspec field.** `TaskConfig.kind: String` (`src/structs/job.rs:1120`) and
its deserialization twin `TaskConfigInput.kind` (`src/structs/job.rs:1557`).
Constructed by `TaskConfig::new(name, kind)` (`src/structs/job.rs:1856`), used
across the codebase as `task.kind`.

**The wire field.** `string task_kind = 7` (`proto/legion.proto:3144`).

**The DB column.** `task_config.kind STRING NOT NULL`
(`src/storage/migrations.rs:82`), read positionally in three query sites
(`src/storage/store.rs:521`, `:1212`, `:1500`) and bound as `":kind"` at
`src/storage/store.rs:2527`.

**The plugin registry.** `TaskPluginRegistry::load_plugin(&mut self, kind: &str, …)`
and `plugin(&self, kind: &str)` (`src/task_plugins/mod.rs:317`, `:330`), plus
`kind_is_host_level(kind: &str)` (`:144`) which the manager calls to pick the
`host` ACL flag before proxying an exec.

**The collision.** Two existing uses of `runtime` stand in the way.

1. **`TaskRuntime`** (`src/task_plugins/mod.rs:112`) is *per-task live execution
   state* — `child`, `pid`, `alloc_dir`, `task_dir`, `extra_env`,
   `staged_artifacts`, `log_dir`. It is public plugin API
   (`legion::task_plugins::TaskRuntime`), passed `&mut` to every `TaskPlugin`
   method, and held in `task_manager.rs`'s `runtimes` map (~66 uses in
   `src/worker/task_manager.rs`, ~15 in `src/task_plugins/container.rs`).
   Post-rename it would read as `runtime: &mut TaskRuntime` for a task whose
   `runtime` is `"container"` — two meanings in one signature.
2. **`ContainerRuntime`** (`src/task_plugins/container.rs:59`) is the *OCI
   runtime* — Apple `container` vs a `docker`/`podman` binary. Its local
   bindings are already spelled `runtime_kind` (`:285`, `:373`, `:452`), which a
   mechanical `kind` → `runtime` pass would turn into `runtime_runtime`.

## 6. Proposed design

**6.1 The axis is named `runtime`.** In the jobspec, the field is `runtime`. In
the proto, `task_runtime`. In Rust, `TaskConfig.runtime`. In the DB,
`task_config.runtime`. In the docs, "task runtime".

**6.2 `TaskRuntime` becomes `TaskInstance`.** The struct holds the live state of
one running task instance, which is what it has always been; `TaskInstance` is
what it should have been called. Local bindings move `runtime` → `instance`, and
`task_manager.rs`'s `runtimes` map becomes `instances`.

**6.3 `ContainerRuntime` keeps its name; its locals are disambiguated.** The
type is correctly named — it *is* a container runtime, in the CRI sense, one
level below the task runtime. Its `runtime_kind` locals become `oci_runtime`,
matching the existing `oci_runtime` binding at `container.rs:205`.

**6.4 The plugin registry speaks `runtime`.** `load_plugin(runtime: &str)`,
`plugin(runtime: &str)`, and `kind_is_host_level` → `runtime_is_host_level`.

**6.5 What keeps `kind`.** `TaskCheckKind`, `SchedulerEvent.kind`, `Eval.kind`,
`AuthMethod.kind`, `PluginManifest.kind`, and the state-inspector object kind.
None are touched.

## 7. Proto, struct, config, and persistence changes

**Proto.** `TaskConfigProto.task_kind` (field 7) → `task_runtime`. Field number
and wire type unchanged, so this is a source break for generated clients, not a
wire break for encoded bytes. Doc comment rewritten to name the runtime
plugin contract.

**Struct.** `TaskConfig.kind` → `runtime`; `TaskConfigInput.kind` → `runtime`
with `#[serde(deny_unknown_fields)]`-driven rejection of `kind` plus an explicit
error hint. `TaskConfig::new(name, runtime)`.

**Config.** Every `configs/jobs/*.jsonnet` moves `kind:` → `runtime:`, including
`maximal.jsonnet`, which is the field-coverage fixture.

**Persistence.** One new migration, appended:

```sql
ALTER TABLE task_config RENAME COLUMN kind TO runtime;
```

SQLite has supported `RENAME COLUMN` since 3.25 (2018); the bundled `rusqlite`
is well past that. The migration is irreversible in the `.down()` direction only
insofar as the reverse rename is trivial, so `.down()` carries the inverse
statement.

## 8. Implementation phases

This ships as **one phase**. The phasing rule in
[`plans/README.md`](README.md) asks that each phase be independently shippable
and valuable; a rename split across phases leaves the tree in a state where two
names for one concept are both live, which is strictly worse than either
endpoint and cannot be released. The unit of work is the whole rename.

Ordering within the single phase, chosen so the compiler drives correctness:

1. Proto + generated types.
2. `TaskConfig` / `TaskConfigInput` / `TaskConfig::new`.
3. Migration + storage SQL.
4. `TaskRuntime` → `TaskInstance` and the plugin registry.
5. Remaining `src/` call sites (manager, worker, cmd, artifacts).
6. Configs, tests, UI.
7. Docs, including the `docs/task-kind-*.md` → `docs/task-runtime-*.md` renames.

## 9. Testing plan

- **Compiler as the primary check.** Renaming a struct field and a public type
  makes every stale reference a hard error; `cargo build --all` passing is most
  of the coverage.
- **`tests/integration/config_jobs.rs`** parses every `configs/jobs/*.jsonnet`, so it is the
  regression net for the config surface.
- **New unit test in `src/structs/job.rs`**: a jobspec using `kind:` is rejected,
  and the error message names `runtime`.
- **New migration test**: an existing DB with a populated `task_config.kind`
  migrates and the value is readable as `runtime`.
- **`tests/integration/acl_endpoints.rs`** re-run per AGENTS.md, since the proto is touched.
- **Full suite**: `cargo fmt --all`, `cargo clippy --all -- -D warnings`,
  `cargo test --all --locked`.

## 10. Rollout and backward compatibility

**Hard break, by decision.** `kind` stops parsing. There is no alias and no
deprecation window.

- **Existing jobspecs** must be edited. The submit-time error names the exact
  fix (§3), so the migration is mechanical: `sed -i 's/\bkind:/runtime:/'` over
  a jobspec directory, reviewed.
- **Existing clusters** migrate their DB automatically on manager start via the
  appended migration. No operator action.
- **Already-running allocations** are unaffected; the column rename is
  transparent to the worker, which receives the runtime name over gRPC.
- **Out-of-tree task plugins** break at compile time on `TaskRuntime` →
  `TaskInstance`. This is the cost of resolving the collision and is called out
  in the public plugin doc.

## 11. Open questions

None outstanding. The three that were open at the start — the name itself, how
to resolve the `TaskRuntime` collision, and whether to carry a `kind` alias —
were all decided before implementation and are recorded in the calls doc.

## 12. Acceptance criteria

- `rg '\bkind\b' src/ proto/ configs/` returns only the non-task axes listed in
  §6.5.
- `rg 'TaskRuntime'` returns nothing.
- A jobspec with `kind:` is rejected with an error naming `runtime`.
- A pre-rename DB opens, migrates, and serves `job list` / `job status`.
- `docs/task-runtime-{plugins,exec,container,noop}.md` exist, are linked from
  `docs/README.md`, and no doc refers to a "task kind".
- `cargo fmt --all --check`, `cargo clippy --all -- -D warnings`, and
  `cargo test --all --locked` are green.
