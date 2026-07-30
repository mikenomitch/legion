# Container Runtime Core Gaps: Exec on Linux, Signals, Cgroup Limits, and File Delivery

> **Judgement calls:** [../calls/container-runtime-exec-and-signals-plan_calls.md](../calls/container-runtime-exec-and-signals-plan_calls.md)

**Status: PLANNED.**

The `container` task runtime's four remaining core gaps, one plan, one goal:
Linux production parity with the macOS dev experience, and a capability table
without ❌s in its core rows. Sequenced so the first three phases share one
mechanism (shelling out to a runtime subcommand addressed by container name)
and the last two extend the bundle/launch surface the runtime already owns:

1. **Exec does not work on Linux.** `legion exec`, the UI terminal, MCP
   one-shot exec, *and declared actions* all fail against container tasks on
   Linux — the OS the workloads actually ship on. This was deferred as Q1 of
   the exec plan ([exec-and-actions-plan.md](exec-and-actions-plan.md)).
2. **The runtime cannot deliver signals.** No platform can signal a container
   task: `changeMode: signal` identities are rejected at admission, and Linux
   teardown is `delete --force` (SIGKILL) with no graceful-shutdown window at
   all.
3. **Linux containers run unconstrained.** Declared `resources.memory` /
   `resources.cores` size the guest VM on macOS but set no cgroup limits on
   Linux — the scheduler reserves capacity the kernel never enforces.
4. **The task directory is invisible inside the container**, which is the one
   limitation behind three capability-table ❌s at once: KV *file* injection,
   identity *file* delivery, and file/tree artifacts.

Deliberately deferred (see Non-goals): jobspec `stopSignal`/`stopGracePeriod`
knobs, Linux port mapping / network namespaces, cpuset pinning, alloc-dir
sharing between tasks.

---

## 1. Problem statement

**Exec.** On macOS, `legion exec --job web -it -- /bin/sh` into a container
task drops you into a shell. On Linux — with youki/runc/crun — the same command
fails with `this container runtime does not support exec yet`
(`src/task_plugins/container.rs:351`). Because *actions* ride the same
worker-side exec path (the manager renders argv and forwards it to
`worker.ExecTask`), a curated `legion action run web flush-cache` fails on
Linux container tasks too, as does the UI's Exec & Actions panel and the MCP
`legion_exec_run` / `legion_action_run` tools. Today the operator's workaround
is to find the node, SSH to it, and run `runc exec` by hand — outside Legion's
ACLs and audit trail. The dev-on-macOS story and the prod-on-Linux story have
inverted capability sets.

**Signals.** `supports_signal()` is `false` for the container runtime, so:

- A job that declares an identity with `changeMode: signal` is rejected at
  admission for container tasks
  (`src/task_plugins/mod.rs:506`, test `container_task_rejects_signal_change_mode`
  at `src/task_plugins/mod.rs:1365`). The only refresh option is
  `changeMode: restart` — a full container restart to pick up a token.
- Stopping a container task on Linux is `<runtime> delete --force` — SIGKILL,
  immediately (`src/task_plugins/container.rs:540`). A service never sees
  SIGTERM, never drains connections, never flushes. For the stateful workloads
  Legion explicitly targets, every reschedule is a crash. (macOS is better only
  by accident: Apple `container stop` signals init and waits.)

The root cause is the same for both: the worker's signal path is a raw
`kill(2)` on the tracked PID (`src/worker/task_manager.rs:340`), and for
container tasks the tracked PID is the **runtime CLI supervisor** (`container
run` / `<runtime> run`), not the workload. Signaling it kills the supervisor
and leaves the container running — so the runtime honestly reports
"unsupported" rather than doing the wrong thing.

**Resource limits.** A job declaring `resources: { memory: 256, cores: 2 }`
gets exactly that guest VM on macOS — and *nothing* on Linux: the generated
bundle's `linux.resources` carries only the device cgroup rule
(`src/task_plugins/oci_bundle.rs:481`). The scheduler subtracts the
declaration from the node's capacity, but the kernel enforces nothing, so one
leaking container can consume the whole node and take every co-scheduled
allocation down with it. The scheduler's bookkeeping and the node's reality
disagree — on the platform where multiple containers actually share a kernel.

**File delivery.** The worker stages artifacts, renders KV files, and writes
identity tokens into the allocation's *host* task directory — and the
container cannot see any of it: the runtime mounts only declared `volumes[]`,
and worker-injected env (`LEGION_TASK_DIR`, `LEGION_ARTIFACT_*`) names host
paths that are meaningless inside, so it is deliberately not applied. The
result is two admission-time rejections for container tasks
(`kv_injections.files`, identity `file` delivery — `src/task_plugins/mod.rs:537`,
`:498`) plus file/tree artifacts that are *accepted* and then stage to a
directory the workload can never read. Today's workaround is baking config into the image or abusing a
volume — both of which bypass the refresh machinery (KV re-render, identity
rotation) that file delivery exists to provide.

## 2. Prior art

- **Docker** ([docs](https://docs.docker.com/reference/cli/docker/container/stop/)):
  `docker exec -it`, and `docker stop` = SIGTERM, wait 10 s (`-t`), then
  SIGKILL; `docker kill --signal` for arbitrary signals. The 10-second
  TERM→KILL default is the industry's shared muscle memory. **Take:** the
  default; the exec UX. **Reject:** per-command `-t` tuning for now (no knob
  until someone needs it).
- **runc / crun / youki** (OCI runtime CLIs): `exec [--tty] <id> <cmd…>` runs a
  process inside the container, building its process spec from the bundle's
  `config.json` (env/cwd/user inherited, argv replaced) unless `--process` is
  given; `kill <id> <signal>` signals the container init and accepts signal
  names or numbers. Exit status of `exec` is the process's own. **Take:** all
  of it — this is exactly the seam Legion already shells out to. **Reject:**
  detached exec + `--console-socket` (see §6.1; containerd needs it because it
  has no foreground terminal — we have one).
- **Nomad** ([alloc exec](https://developer.hashicorp.com/nomad/docs/commands/alloc/exec),
  [`kill_signal`/`kill_timeout`](https://developer.hashicorp.com/nomad/docs/job-specification/task)):
  per-driver exec capability, `nomad alloc signal`, and per-task
  `kill_signal`/`kill_timeout` jobspec knobs. **Take:** the per-runtime
  capability report (Legion already has `ExecSupport`) and graceful-kill
  semantics. **Reject for now:** the jobspec knobs and a user-facing signal
  verb — surface we can add later without breaking anything (non-goals).
- **Kubernetes** (CRI `Exec`/`ExecSync`, `terminationGracePeriodSeconds`):
  kubelet asks the CRI for a streaming exec URL; termination is
  TERM → grace period → KILL, with preStop hooks. **Take:** the termination
  shape. **Reject:** the streaming-URL indirection — Legion's frame protocol
  and worker-side PTY already exist and work
  ([docs/exec-and-actions.md](../../docs/exec-and-actions.md)).
- **Resource limits — docker, Nomad, K8s, and the OCI spec.** Docker's
  `--cpus=N` is implemented as CFS bandwidth (`cpu.quota = N × cpu.period`),
  not pinning; `--memory` is a hard cgroup limit. Nomad's docker driver does
  the same for `cpu`/`memory`, and its `resources.cores` *reserves whole
  cores* via cpuset — which requires the client to account core assignments
  across every allocation on the node. K8s adds requests-vs-limits and QoS
  classes on top. The OCI runtime spec carries all of it declaratively in
  `linux.resources` — the runtime creates the cgroup and applies limits;
  Legion only has to write JSON it already generates.
  **Take:** docker's quota semantics for `cores` (matches what macOS `--cpus`
  already means for the same field) and hard memory limits. **Reject:** cpuset
  pinning (a cluster-wide core-accounting subsystem for a benefit Legion's
  scheduler doesn't model yet) and the K8s requests/limits split (Legion has
  one `resources` block; keeping it one concept is the simplicity bet).
- **Task filesystem into containers — Nomad, docker, K8s.** Nomad mounts the
  alloc/task/secrets dirs into every docker container at canonical in-container
  paths (`/alloc`, `/local`, `/secrets`) and points `NOMAD_TASK_DIR` at the
  *in-container* path — jobs never see host paths. Docker's single-file bind
  mounts are a famous pitfall: an atomic-rename update replaces the inode and
  the container keeps the stale file. K8s solves refresh atomicity with
  projected volumes and symlink swapping — correct, and heavy. **Take:**
  Nomad's canonical mount point + env translation, and *directory-level*
  mounting so host-side atomic renames stay visible (which is also why we
  reject per-file mounts). **Reject:** K8s-style projection machinery, and
  Nomad's three-directory split — Legion has one task dir today, so one mount.

## 3. Jobs to be done

**On-call responder**
- When a container task is failing on a Linux node and the logs don't say why,
  I want to get a shell inside that exact container, so I can look around
  before restarting it destroys the evidence.
- When I run a runbook action against the service, I want it to work in
  production (Linux), not just on the author's laptop (macOS), so 3am me isn't
  the first person to discover the gap.

**Job author / app developer**
- When my service is rescheduled or stopped, I want it to receive SIGTERM and
  a few seconds to drain, so in-flight requests and buffered writes aren't
  lost.
- When my identity token refreshes, I want my process told to re-read it (a
  signal), so I don't take a full restart every few hours.
- When I declare memory for my container, I want a leak in *my* service to
  kill *my* container, so it doesn't become the neighboring team's incident.
- When my app expects a config file or a data file at startup, I want to
  declare it in the jobspec and read it from a well-known path inside the
  container, so I don't rebuild the image for every config change.

**Cluster operator**
- When I promise "what works on the mac dev cluster works in prod," I want the
  container runtime's capability table to be the same on both, so I stop
  explaining the asymmetry.
- When I pack multiple teams' containers onto a node, I want the limits the
  scheduler placed against to be the limits the kernel enforces, so one bad
  deploy can't take out the node.

**Security / platform admin**
- When a service needs a rotating credential, I want it delivered as a file
  the process re-reads on a signal, so it never sits in the environment where
  any `inspect` or `/proc` reader can lift it.

**Autonomous agent**
- When I diagnose a job over MCP, I want one-shot exec to work regardless of
  which OS the allocation landed on, so my remediation playbooks are portable.

**User scenarios**

1. An engineer runs `legion exec --job api -it -- /bin/sh` against a job on a
   Linux worker running youki. A shell opens inside the container, `env` shows
   the task's env, `exit 3` propagates exit code 3 to their terminal.
2. A rollout replaces an allocation. The old container gets SIGTERM, the app
   closes its listeners and exits within the grace window, and the worker
   reaps it — `delete --force` never fires. A hung container is force-deleted
   after 10 s.
3. A job declares `identity { changeMode: 'signal', changeSignal: 'SIGHUP' }`
   on a container task. Submission is accepted; on token refresh the app gets
   SIGHUP inside the container.
4. A container task declares `resources: { memory: 256 }` and leaks. On Linux
   the kernel OOM-kills it at 256 MiB; the allocation fails with a message
   naming the memory limit, the restart policy takes over, and the node's
   other allocations never notice.
5. A job declares a KV-rendered config file at `/legion/task/config/app.toml`
   and an identity file at `/legion/task/identity/token`, with
   `changeMode: signal`. The container reads both at the declared paths; when
   the KV key changes or the token rotates, the files are atomically replaced
   and the app gets its SIGHUP — no restart, no image rebuild.

## 4. Goals and non-goals

**Goals**

- `legion exec` (one-shot and `-it`), actions, the UI terminal, and MCP exec
  work against container tasks on Linux with youki, runc, and crun.
- The container runtime reports `supports_signal() == true` and delivers
  signals to the container init on both platforms; `changeMode: signal` is
  accepted for container tasks.
- Linux container teardown becomes graceful: SIGTERM → bounded wait →
  `delete --force`.
- Declared `resources.memory` / `resources.cores` are kernel-enforced on
  Linux via the bundle's `linux.resources` — the same fields that already
  size the macOS guest, with the same declared-only semantics.
- The task directory is mounted into the container at a canonical path, and
  `supports_kv_file_injection()` / `supports_identity_file()` flip to true:
  KV file injection, identity file delivery, and file/tree artifacts work in
  containers on both platforms, refresh included.
- The capability tables in
  [docs/task-runtime-container.md](../../docs/task-runtime-container.md),
  [docs/exec-and-actions.md](../../docs/exec-and-actions.md), and
  [docs/workload-identity.md](../../docs/workload-identity.md) lose six ❌s.

**Non-goals** (each stays on
[TODO.md](../TODO.md) / the public docs as an honest limitation)

- **Jobspec `stopSignal` / `stopGracePeriod` knobs and a `legion alloc signal`
  CLI verb.** New user-facing surface; deferred until a job actually needs a
  non-default value (§11 Q3). The fixed docker-style default ships first.
- **Linux port mapping / private network namespaces.** Containers keep host
  networking; that's the networking plan's territory, not a bundle tweak.
- **Cpuset pinning for `cores`.** Kernel-level core *reservation* needs
  node-wide core accounting the scheduler doesn't model; CFS bandwidth gives
  the enforcement without the subsystem (§6.4).
- **Mapping `resources.cpu` (MHz) to `cpu.weight`.** Stays scheduling-only on
  every platform, as today (§11 Q4).
- **File-delivery destinations outside the task-dir mount.** One mount, one
  rule (§6.5); per-file bind mounts and K8s-style projection are rejected, not
  postponed.
- **Alloc-dir sharing between tasks** — tracked by
  [shared-alloc-dir-proposal.md](shared-alloc-dir-proposal.md), blocked on
  multi-task allocations.
- **A read-only secrets split of the mount** (Nomad's `/secrets`): revisit
  when Legion grows a distinct secrets dir; today there is one task dir.
- **Detach/reattach exec sessions.** Same call as the exec plan: sessions die
  with the connection.
- The `sandbox` microVM guest-agent exec path — tracked by the sandbox plan.

## 5. Background — what exists today

- **The exec seam.** `TaskPlugin::exec_support()` / `exec()`
  (`src/task_plugins/mod.rs:337`) return an `ExecSession` pumped by the worker
  server; `spawn_pty` / `spawn_pipe` (`src/task_plugins/exec_session.rs:88`,
  `:133`) wrap portable-pty and plain pipes. The container plugin already
  builds a runtime-specific argv in `build_exec_command`
  (`src/task_plugins/container.rs:261`) — including an `Oci` arm — but
  `ContainerRuntime::supports_exec()` (`container.rs:94`) hard-codes
  `AppleContainer` only, so the Linux arm is dead code. Note the dead arm
  emits `exec --tty --interactive` — `--interactive` is a Docker/Apple flag
  that runc/crun/youki do not have; enabling it as-is would fail.
- **ACLs need nothing.** Container exec is sandboxed (`host_level: false`,
  `runtime_is_host_level` at `src/task_plugins/mod.rs:224`); an `exec:write`
  rule without `host` already covers it on both platforms.
- **The signal path bypasses plugins.** `WorkerTaskManager::signal_allocation`
  (`src/worker/task_manager.rs:340`) resolves the tracked PID and calls
  `kill(2)` directly (`send_signal`, `task_manager.rs:1107`). There is no
  `TaskPlugin` method for signals — only the boolean `supports_signal()` gate
  (`src/task_plugins/mod.rs:291`), consumed at admission for
  `changeMode: signal` identities and true only for the `exec` runtime
  (`src/task_plugins/exec.rs:250`). The one caller is the identity refresher
  (`src/worker/identity.rs:645`).
- **Stop today.** `container.rs:523` tears down by name — Apple
  `container stop` (signals + waits) on macOS, `<runtime> delete --force`
  (SIGKILL) on Linux — then reaps the supervisor child via
  `reap_task_process` (`mod.rs:117`), itself a `child.kill()`.
- **Container identity.** Every allocation's container is named
  `legion-<alloc-id>` (`container.rs:141`) on both platforms, so any runtime
  subcommand can address it without new state.
- **The bundle spec is Legion's to extend.** `build_spec`
  (`src/task_plugins/oci_bundle.rs:428`) generates the whole OCI runtime
  `config.json`; its `linux.resources` today holds only the device-cgroup
  deny rule (`oci_bundle.rs:481`). The runtime (runc/crun/youki) creates the
  cgroup and applies whatever resources the spec declares — no new process
  management needed. `resource_amount` (`container.rs:160`) already parses
  `memory`/`cores` for the macOS flags; the exec runtime already stakes out
  the cgroup-v2-only position (`exec.rs:475`).
- **Volumes are the template for the task-dir mount.** The worker resolves
  volume env to *in-task destinations* for isolating runtimes — the
  `in_task_view` match in `mount_allocation_volumes`
  (`src/worker/task_manager.rs:911`) and `TaskInstance::volume_env`
  (`mod.rs:184`) — and the plugin turns `instance.volumes` into
  `--volume host:dest` args (macOS, `container.rs:230`) or `rbind` mount
  entries (`volume_mounts`, `oci_bundle.rs:409`). The task-dir mount is one
  more entry through the same two code paths.
- **Worker-injected env exists but is withheld.** `instance.extra_env` carries
  `LEGION_ALLOC_DIR`, `LEGION_TASK_DIR`, `LEGION_ARTIFACT_<NAME>`
  (`task_manager.rs:865`) with *host* paths; the container plugin deliberately
  merges only `volume_env()` (`container.rs:447`). `${artifact.<name>}`
  interpolation into the runtime config is also host-path substitution
  (`substitute_artifact_tokens`, `task_manager.rs:874`).
- **File destinations are free-form paths today.** `kv_injections.files[]`
  declares a `path` (`src/structs/job.rs:1703`); identity `file` delivery
  declares one too, absolute by convention (e.g.
  `/var/run/legion/identity/<name>/token`,
  `src/structs/workload_identity.rs:829`) — sensible for the exec runtime's
  host filesystem, unreachable inside a container. Identity naming already
  enforces a traversal-safe charset at admission *and* re-checks on the worker
  (`workload_identity.rs:266`) — the precedent §6.5's path rule follows.

## 6. Proposed design

### 6.1 Exec on the Linux OCI path

Flip `ContainerRuntime::supports_exec()` to `true` for `Oci` and make
`build_exec_command` emit the OCI CLI dialect:

```
# Apple (unchanged):   container exec [--tty --interactive] legion-<alloc> CMD…
# OCI (youki/runc/crun): <runtime> exec [--tty] legion-<alloc> -- CMD…
```

Dialect differences, each deliberate:

- **`--tty` only** — the OCI runtimes have no `--interactive`; stdin wiring is
  implicit in foreground mode.
- **`--` before the command** on the OCI path, so a remote argv starting with
  `-` cannot be eaten by the runtime's flag parser. (Asymmetric on purpose:
  Apple's parser treats `--` as the target executable — the same quirk the
  launch path already documents.)
- **Extra env** (`ExecOptions.env`, e.g. the caller's `TERM`) maps to repeated
  `--env K=V` flags on the OCI path. Today the Apple path ignores it; that
  stays as-is.

**Semantics we inherit for free** (and verify in tests rather than re-build):
`<runtime> exec` constructs the process from the bundle's `config.json` —
env, cwd, and user match the task exactly, argv is replaced, and the CLI exits
with the process's own exit status. Since Legion authored that `config.json`
(`oci_bundle.rs`), exec'd processes see precisely the merged task env.

**TTY sessions need no console socket.** containerd needs `--console-socket`
because it runs the runtime detached. Legion runs `exec` in the foreground
*under the PTY that `spawn_pty` already allocates* — the CLI has a real
controlling terminal, does its own raw-mode/resize handling (SIGWINCH
propagates through the PTY), and bridges to the container console. One-shot
sessions use `spawn_pipe`; stdout/stderr stay separate, exactly like the Apple
path.

Failure surface: exec against a stopped/unknown container returns the
runtime's own stderr through the session, tagged with the container name. The
`exec_support()` report stays computed per detected runtime — an OS with no
runtime keeps reporting unsupported, so `legion exec` still fails fast with
the clear error.

### 6.2 Signal delivery becomes a plugin seam

Add one defaulted method to `TaskPlugin` (dylib-ABI additive; default keeps
every existing plugin source-compatible):

```rust
/// Deliver a Unix signal (name, e.g. "SIGHUP") to the running task.
/// The default signals the tracked PID — correct wherever the tracked
/// process *is* the workload (the exec runtime). Isolating runtimes
/// override to reach inside.
fn signal(&self, allocation: &Allocation, task: &TaskConfig,
          instance: &mut TaskInstance, signal: &str) -> R<()>
```

- `WorkerTaskManager::signal_allocation` routes through the plugin registry
  instead of calling `send_signal` directly; the default impl *is* today's
  behavior, so the exec runtime is untouched.
- The container plugin overrides it:
  - **macOS:** `container kill --signal <SIG> legion-<alloc>`.
  - **Linux:** `<runtime> kill legion-<alloc> <SIG>` (names accepted by all
    three runtimes; delivered to the container init).
- `ContainerTaskPlugin::supports_signal()` returns `true`, which flips the
  admission gate: `changeMode: signal` identities on container tasks are now
  accepted (the test at `mod.rs:1365` inverts to assert acceptance). The
  refresh loop in `worker/identity.rs` needs no change — it already calls
  `signal_allocation`.

The signal-name vocabulary stays the existing `parse_signal` set; the plugin
passes the *name* through to the CLI rather than a number, so the runtime does
its own resolution.

### 6.3 Graceful teardown on Linux

`stop()`'s Linux arm becomes the docker shape, with a fixed default:

1. `<runtime> kill legion-<alloc> SIGTERM` — ignore "not running" errors (a
   finished batch task is already gone).
2. Poll `<runtime> state legion-<alloc>` until it reports stopped/absent, up
   to `CONTAINER_STOP_GRACE` = **10 s** (docker's default; a constant, not a
   knob — §11 Q3).
3. `<runtime> delete --force legion-<alloc>` — unconditionally, as the reaper
   for the stopped container *and* the killer for a hung one.
4. Reap the foreground supervisor child, as today.

macOS keeps delegating to `container stop` (already TERM-then-wait). `destroy`
keeps delegating to `stop`.

### 6.4 Linux cgroup limits from the resources the job already declares

No new jobspec surface. The same two fields that size the macOS guest gain
their Linux meaning, written into the generated `config.json` by `build_spec`
(which gains the task's `resources` as a parameter):

```jsonnet
resources: { memory: 256, cores: 2 }   // MiB, core count — as today
```

becomes

```json
"linux": {
  "cgroupsPath": "legion/legion-<alloc-id>",
  "resources": {
    "devices": [ ... ],                          // unchanged
    "memory": { "limit": 268435456, "swap": 268435456 },
    "cpu":    { "quota": 200000, "period": 100000 }
  }
}
```

The mappings, each deliberate:

- **`memory` (MiB) → `memory.limit` bytes, hard.** Exceeding it is a kernel
  OOM kill of the container's cgroup — the failure lands on the task that
  leaked, not the node. `memory.swap` is set *equal* to the limit, so swap
  cannot be used to escape it (the K8s semantic; docker's default of
  2×limit-in-swap makes limits soft in practice, which defeats the point of
  scheduling against them).
- **`cores` → CFS bandwidth, not pinning.** `quota = cores × period` with the
  kernel-default 100 ms period — exactly how docker implements `--cpus=N`,
  and the same *meaning* the field already has on macOS ("N CPUs' worth"),
  just enforced by throttling instead of by VM sizing. Fractional values work
  naturally (`cores: 0.5` → quota 50000); the macOS arm keeps its rounding.
  Cpuset reservation is the rejected alternative (see prior art): it needs
  the worker to own a node-wide core-assignment map, and the scheduler
  doesn't model individual cores anywhere else.
- **`cpu` (MHz) stays scheduling-only** on Linux exactly as it is on macOS —
  one asymmetric field would be worse than one unenforced one (§11 Q4).
- **Declared-only, like macOS:** a job that declares nothing gets no
  `memory`/`cpu` entries and runs unconstrained — scheduler defaults are not
  folded in, mirroring the existing macOS call. The asymmetry that *remains*
  is the runtime's own: an unsized macOS guest still gets Apple's 1 GiB/4-CPU
  VM default, while an unsized Linux container is genuinely unlimited.
- **`cgroupsPath` is set explicitly** (`legion/legion-<alloc-id>`, cgroupfs
  form) so operators can find a task's cgroup by allocation id with no
  runtime-specific default knowledge. The systemd cgroup driver
  (`:slice:prefix:name` form) is out of scope until someone runs Legion under
  systemd-managed cgroup delegation and needs it.

**Requirements and failure modes.** Limits require the cgroup v2 unified
hierarchy — the position the exec runtime already takes for cgroup joining —
and the memory/cpu controllers delegated to whatever user the runtime runs
as. Both failure shapes surface as the runtime's own error at container
create, which `start()` already propagates with context; the public doc gets
a failure-modes row for each (missing controllers → actionable
"enable cgroup v2 / delegation" guidance). **OOM diagnostics:** on a nonzero
exit, the plugin best-effort reads the cgroup's `memory.events` `oom_kill`
counter before teardown and, when it fired, annotates the failure record
("container exceeded its 256 MiB memory limit and was OOM-killed") — exit
code 137 alone sends the author hunting through dmesg.

### 6.5 Mount the task directory; file delivery follows

**The mount.** Every container task gets its host task directory bind-mounted
read-write at one canonical in-container path:

```
/legion/task
```

- **Linux:** one more `rbind` entry from `volume_mounts`'s pattern, appended
  *before* declared volumes (so an explicit volume can still shadow it, same
  rule the mount list already follows for its defaults).
- **macOS:** one more `--volume <host-task-dir>:/legion/task` on the `run`
  argv, through the code path volumes already use.

The *directory* is mounted, never individual files: the worker's refresh
machinery (KV re-render, identity rotation) replaces files atomically by
rename, which stays visible through a directory bind mount and silently
breaks through a per-file one (the docker pitfall from prior art). One
verification item rides the e2e tests: Apple `container`'s virtiofs must
propagate host-side renames promptly; if it doesn't, that's a documented
macOS limitation, not a design change.

**Env translation.** The container plugin stops discarding worker-injected
env and instead merges a *translated* view, extending the exact convention
volumes established (in-task destinations for isolating runtimes):

- `LEGION_TASK_DIR=/legion/task`;
- `LEGION_ARTIFACT_<NAME>` values have the host task-dir prefix swapped for
  `/legion/task` (an artifact staged outside the task dir would be omitted,
  not leaked as a dead host path — cannot happen today, defense anyway);
- `${artifact.<name>}` config interpolation uses the same translated paths
  for container tasks, keyed off the existing `in_task_view` match in the
  worker (`task_manager.rs:911`);
- `LEGION_ALLOC_DIR` stays withheld — the alloc dir is not mounted, and
  pointing env at an invisible path is the bug this section exists to fix.
  It returns if/when the shared-alloc-dir proposal lands.

**File delivery.** With the mount in place, the two admission gates flip
(`supports_kv_file_injection`, `supports_identity_file` → true) and file/tree
artifacts become readable where they already stage. One new rule makes the
declared paths honest — **for container tasks, a file-delivery destination
must resolve under `/legion/task`** (relative paths resolve against it):

```jsonnet
kvInjections: { files: [
  { path: 'config/app.toml', file: { template: '...' } },  // → /legion/task/config/app.toml
] },
identities: [{
  name: 'api',
  file: { path: '/legion/task/identity/token' },           // absolute form, in range
  changeMode: 'signal', changeSignal: 'SIGHUP',
}]
```

An out-of-range path is rejected at submit with the fix in the message:

```
task api identity "api": file.path "/var/run/legion/token" is outside the
container task directory; use a path under /legion/task (or a relative path),
or switch to env delivery
```

The worker re-checks the resolved path before any join (defense in depth —
the same two-layer stance identity naming already takes for traversal).
On the host side nothing moves: the worker keeps writing to
`<task_dir>/<relative>`, and the mount does the rest. The exec runtime is
untouched — its host-absolute paths remain valid because its filesystem *is*
the host's. This is the one place a jobspec is not verbatim-portable between
the two runtimes; the error message and the docs both say so plainly.

**Refresh, end to end.** Phase 2's signal seam plus this mount completes the
loop scenario 5 describes: host-side atomic rewrite inside the mounted dir →
visible in-container → `changeMode: signal` delivers `changeSignal` via
`<runtime> kill`. No container restart for a token rotation or a KV flip.

**Security posture.** The mount exposes the allocation's own private task
directory and nothing else; the log dir is not mounted (captured stdout/stderr
can carry secrets and stays owner-restricted on the host). `noNewPrivileges`,
masked paths, and the capability set are unchanged. A read-only secrets split
is a named non-goal until Legion has a distinct secrets dir to split out.

## 7. Proto, struct, config, and persistence changes

- **None to the proto, wire format, or persistence.** The exec frame protocol,
  `ExecAllocation`, and worker state are untouched.
- **No new jobspec fields** — `resources.memory`/`cores`, `kv_injections.files`,
  identity `file`, and `artifacts` all exist; this plan changes what they *do*
  for container tasks, so `configs/jobs/maximal.jsonnet` is unchanged (its
  comments on the container-task example get the new semantics).
- `TaskPlugin` gains the defaulted `signal()` method (dylib plugins stay
  source-compatible; the registry's existing version gating covers the rebuild
  requirement, same as the exec plan's ABI addition).
- `build_bundle`/`build_spec` gain the task's `resources` (internal signature
  change only); capability flips in the container plugin:
  `supports_kv_file_injection`, `supports_identity_file`, `supports_signal`
  → true.
- **One new validation rule** (a *tightening* only for specs that were
  rejected outright before): container-task file-delivery paths must resolve
  under `/legion/task` (§6.5) — enforced at admission, re-checked on the
  worker.
- Docs: capability tables in `docs/task-runtime-container.md` (signal, KV
  file, identity file, Linux limits, Linux exec rows; the "Resources and
  Ports" and "Current Scope and Limitations" sections), `docs/exec-and-actions.md`
  (the `container (youki)` row), `docs/workload-identity.md` (runtime matrix +
  container file paths), `docs/kv-store.md` (container file injection),
  `docs/artifacts.md` (file/tree artifacts in containers);
  `internal_docs/TODO.md` per phase.

## 8. Implementation phases

Each ships alone. 1 goes first (it revives the dead OCI argv path the others
lean on for confidence); 2–5 are mutually independent, listed in the order
that front-loads user value.

- **Phase 1 — Linux OCI exec.** `supports_exec()` flip, OCI argv dialect in
  `build_exec_command` (+ env mapping, `--` separator), unit tests, e2e
  `#[ignore]` tests, doc-table updates. Worker-only; no manager change.
  *Value: exec + actions + UI + MCP work on Linux.*
- **Phase 2 — the signal seam.** Trait method + default, task-manager
  routing, container override for both platforms, admission-gate flip and test
  inversion, docs. *Value: `changeMode: signal` on containers; the seam any
  future runtime (sandbox guest agent) implements.*
- **Phase 3 — graceful Linux stop.** TERM → bounded wait → `delete --force`,
  e2e test that a TERM-trapping container exits gracefully and a TERM-ignoring
  one is force-deleted at the deadline, docs. Builds on Phase 2's `kill`
  helper but ships independently (it can shell out directly if 2 hasn't
  landed). *Value: no more data-loss stops on Linux.*
- **Phase 4 — Linux cgroup limits.** `resources` threaded into
  `build_spec`, the `memory`/`cpu` mappings + `cgroupsPath`, OOM annotation
  from `memory.events`, unit tests over the generated spec, e2e OOM/throttle
  tests, docs (including the failure-modes rows). Entirely inside
  `oci_bundle.rs`/`container.rs`. *Value: the scheduler's arithmetic becomes
  the kernel's; noisy neighbors die alone.*
- **Phase 5 — task-dir mount + file delivery.** The `/legion/task` mount on
  both platforms, translated env + `${artifact.*}` interpolation, the
  path-range validation with its error message, capability flips
  (`supports_kv_file_injection`, `supports_identity_file`), e2e file-visibility
  and refresh tests, docs across the four affected pages. Touches worker core
  only at the existing `in_task_view` seam. *Value: three capability-table
  rows at once — config files, tokens, and data artifacts inside containers,
  with rotation.*

## 9. Testing plan

- **Unit (every phase, CI-safe):**
  - argv construction — OCI exec includes `--tty`/`--`/`--env` and never
    `--interactive`; Apple arm unchanged (no `--`); signal argv per platform;
    stop sequence ordering (kill before delete). Mirrors the existing
    `build_apple_launch` test style.
  - generated `config.json` — memory limit/swap byte math, quota/period from
    fractional and whole `cores`, no `memory`/`cpu` entries when nothing is
    declared, `cgroupsPath` shape, devices rule untouched.
  - mounts and env — `/legion/task` mount present on both arms and ordered
    before declared volumes; `LEGION_TASK_DIR` and `LEGION_ARTIFACT_*`
    translation (including the omit-when-outside case); path-range validation
    accepts relative + in-range absolute, rejects out-of-range with the
    documented message.
- **E2E, `#[ignore]`d behind a real runtime** (extending the existing
  `oci_runtime_*` suite; run with
  `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime -- --ignored`):
  - exec one-shot: output captured, exit code propagated, task env visible;
  - exec tty: shell round-trip through a PTY, resize, Ctrl-C;
  - signal: a SIGHUP-trapping script observably reacts;
  - graceful stop: TERM-trapping container exits inside the window (and
    `delete --force` finds it already gone); TERM-ignoring container is gone
    by ~grace+ε;
  - limits: a memory-hog container dies at its declared limit with the OOM
    annotation in the failure record; `nproc`-style spin under `cores: 1`
    shows throttling (cgroup `cpu.stat` `nr_throttled > 0` — cheaper and less
    flaky than wall-clock assertions);
  - files: a staged file artifact and a KV-rendered file are readable at
    their `/legion/task` paths from inside the container; a host-side atomic
    rewrite is visible inside without restart (the rename-propagation check —
    on macOS this is also the virtiofs verification from §6.5).
  - Matrix: runc (primary), crun, youki — same tests, `LEGION_OCI_RUNTIME`
    selects; divergences become documented limitations, not silent skips.
- **Regression:** full `cargo test --all --locked`; `cargo test --test
  acl_endpoints` (no new endpoints — must stay green untouched); the identity
  admission test inversion; existing volume-mount tests unchanged (the
  task-dir mount must not reorder or shadow declared volumes).
- Not a scheduler change — no benchmark numbers required.

## 10. Rollout and backward compatibility

- No feature gate. Every phase changes worker-local behavior only (plus, for
  Phase 5, an admission rule on the manager); no manager/worker version
  handshake concerns (an old manager talking to a new worker simply finds
  exec working where it used to error).
- Four **visible behavior changes** to release-note:
  1. Linux container stop delivers SIGTERM and waits up to 10 s before the
     force-delete. Rescheduling a hung container is up to 10 s slower than
     today's instant SIGKILL — the correct trade, and the docker default
     everyone already operates under.
  2. `changeMode: signal` container jobs that used to be rejected at submit
     now deploy; likewise KV-file and identity-file container jobs (Phase 5).
  3. **Declared limits become real on Linux.** A container job declaring
     `memory: 256` while actually using more ran fine yesterday and is
     OOM-killed today. This is the declared contract finally being enforced —
     the release note says so plainly, and the OOM annotation tells the
     author which limit fired and where it came from. Jobs declaring nothing
     see no change.
  4. Container tasks start receiving `LEGION_TASK_DIR` / `LEGION_ARTIFACT_*`
     (in-container values) and a `/legion/task` mount. An image that already
     has a `/legion` path would be shadowed — improbable, called out in the
     release note regardless.
- Existing jobs, specs, and clusters otherwise need no action.

## 11. Open questions

- **Q1 — probe `exec` availability per node, or fail at session open?** An
  exotic OCI runtime without `exec` would report `ExecSupport{exec: true}` and
  then fail when the session opens. *Lean:* don't probe — all three supported
  runtimes have `exec`, the failure is clearly attributed, and a per-node
  `exec --help` probe is a startup cost for a hypothetical.
- **Q2 — does Apple `container kill` cover the full signal-name set?** The
  subcommand exists; the accepted name spellings need verifying on real
  hardware during Phase 2. *Lean:* pass `SIG`-stripped names and normalize in
  the plugin if needed; worst case macOS maps unsupported names to a clear
  error.
- **Q3 — jobspec `stopSignal` / `stopGracePeriod`?** *Lean:* not until a
  workload demonstrably needs a non-default. When it does, the fields slot
  into `stop()`/`signal()` without reshaping anything here — and that request,
  not this plan, should decide names and defaults (they belong at task level,
  next to `resources`, and would go through their own mini-plan since they are
  new jobspec surface for *every* runtime).
- **Q4 — map `resources.cpu` (MHz) to `cpu.weight`?** Nomad does (MHz →
  shares), and it only matters under contention. *Lean:* no — the field is
  scheduling-only on macOS, and one field meaning different things per
  platform costs more explanation than proportional weight buys. Revisit with
  evidence of contention complaints on packed nodes.
- **Q5 — is `/legion/task` the right canonical path?** Nomad's `/local` is
  shorter; `/legion/task` is unmistakably Legion's and leaves room for
  siblings (`/legion/alloc`, `/legion/secrets`) if the deferred splits ever
  land. *Lean:* `/legion/task` — provenance beats brevity for a path users
  will grep for, and it is a constant we can never comfortably change once
  jobs hardcode it, so pick the self-describing one.
- **Q6 — should the env-delivery identity backing file (`<ENV>_FILE`) be
  exposed in containers?** It lives under the worker's own root today, not
  the task dir. *Lean:* out of scope — env delivery works by value at launch;
  containers that want a refreshable on-disk token should use file delivery,
  which this plan makes work. Reconsider only if a real SDK needs the
  `_FILE` convention specifically.

## 12. Acceptance criteria

- **Phase 1:** on a Linux worker with runc, scenario 1 works end to end
  (interactive shell, env, exit code); `legion action run` against a Linux
  container allocation succeeds; the capability tables say so; unit + ignored
  e2e tests exist for all three runtimes.
- **Phase 2:** a container job with a `changeMode: signal` identity is
  accepted and its process observably receives the signal on refresh, on both
  platforms; third-party dylib task plugins build unchanged.
- **Phase 3:** the two e2e stop tests (graceful exit, forced deadline) pass;
  `stop` on an already-exited container remains error-free; docs no longer
  call Linux stop "delete --force" only.
- **Phase 4:** scenario 4 works end to end on a Linux worker — the memory hog
  is OOM-killed at its declared limit with the annotated failure record, the
  spin test shows CFS throttling, an undeclared job's spec carries no
  `memory`/`cpu` entries, and the capability table's "Memory / CPU limits"
  row reads ✅ for Linux with the cgroup-v2 requirement documented.
- **Phase 5:** scenario 5 works end to end on both platforms — declared KV
  file, identity file, and a file artifact all readable at `/legion/task`
  paths inside the container, a rotation visible without restart; the
  out-of-range path is rejected at submit with the documented error; the
  capability table's KV-file / identity-file / artifact rows read ✅ with the
  path rule stated.
