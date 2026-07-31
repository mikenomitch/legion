# Container Runtime Core Gaps: Exec on Linux, Signals, Resource Enforcement, and File Delivery

> **Judgement calls:** [../calls/container-runtime-exec-and-signals-plan_calls.md](../calls/container-runtime-exec-and-signals-plan_calls.md)

**Status: SHIPPED** (2026-07-31). All seven phases landed together.

Deviations from the plan as written, each recorded in the
[judgement calls](../calls/container-runtime-exec-and-signals-plan_calls.md)
(entries 8–11): the wire *does* gain two `TaskAllocation` fields (§7 claimed
none were needed — the worker never receives the jobspec, only the dispatched
allocation); the enforcement subsystem reuses the existing opt-in
`resourceEnforcement` worker config as its gate rather than shipping
ungated; the `cores`-xor-`cpu` rule turned out to already exist in the
scheduler's resource normalization; and `resources.cpus` is the scheduler's
alias for `cpu` (MHz), **not** for `cores`, so it does not request a cpuset.

Public documentation: [docs/task-runtime-container.md](../../docs/task-runtime-container.md),
[docs/resource-enforcement.md](../../docs/resource-enforcement.md) (new),
[docs/task-supervision.md](../../docs/task-supervision.md),
[docs/task-runtime-plugins.md](../../docs/task-runtime-plugins.md).

The `container` task runtime's four remaining core gaps, one plan, one goal:
Linux production parity with the macOS dev experience, and a capability table
without ❌s in its core rows. Sequenced so the early phases share one
mechanism (shelling out to a runtime subcommand addressed by container name)
and the later ones extend the bundle/cgroup and launch surfaces the runtime
already owns.

Gap 3 outgrew the container runtime during design and is built as a
**worker-core subsystem with a task-runtime interface**: the `exec` runtime
adopts it in this plan (phase 6), and any runtime — in-tree or dylib — opts
in with one trait method. That is the plan's one piece of new extensibility
surface, and the phase list reflects it.

1. **Exec does not work on Linux.** `legion exec`, the UI terminal, MCP
   one-shot exec, *and declared actions* all fail against container tasks on
   Linux — the OS the workloads actually ship on. This was deferred as Q1 of
   the exec plan ([exec-and-actions-plan.md](exec-and-actions-plan.md)).
2. **The runtime cannot deliver signals.** No platform can signal a container
   task: `changeMode: signal` identities are rejected at admission, and Linux
   teardown is `delete --force` (SIGKILL) with no graceful-shutdown window at
   all.
3. **Linux workloads run unconstrained.** Declared `resources.memory` /
   `resources.cores` size the guest VM on macOS but set no cgroup limits on
   Linux, and `resources.cpu` (MHz) is enforced nowhere — the scheduler
   reserves capacity the kernel never sees. The fix is Nomad's model: hard
   memory limits, `cores` as *exclusive pinned cores* (cpuset), and MHz as
   proportional `cpu.weight` — built as a **worker-owned subsystem behind a
   runtime-agnostic interface** (`resource_enforcement()`), consumed by the
   container runtime, adopted by the exec runtime in the same plan, and open
   to any task-runtime plugin.
4. **The task directory is invisible inside the container**, which is the one
   limitation behind three capability-table ❌s at once: KV *file* injection,
   identity *file* delivery, and file/tree artifacts.

Graceful teardown ships with task-level `stopSignal` / `stopGracePeriod`
jobspec knobs (defaults `SIGTERM` / `10s`), honored by the container runtime
on both platforms and by the exec runtime.

Deliberately deferred (see Non-goals): Linux port mapping / network
namespaces, NUMA/SMT-aware core placement, alloc-dir sharing between tasks.

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
allocation down with it. `resources.cpu` (MHz) is worse still — it is
scheduling arithmetic on *every* platform, so two containers that declared
100 and 4000 MHz contend as equals the moment the node is busy. The
scheduler's bookkeeping and the node's reality disagree — on the platform
where multiple containers actually share a kernel.

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
  SIGKILL; `docker kill --signal` for arbitrary signals; images can declare
  `STOPSIGNAL`, which `stop` honors when no override is given. The 10-second
  TERM→KILL default is the industry's shared muscle memory. **Take:** the
  defaults, the exec UX, and honoring the image's `StopSignal` as the middle
  layer of the default chain. **Reject:** per-*invocation* `-t` tuning — the
  knob belongs on the task in the jobspec, where it is declared once and
  audited, not retyped per stop.
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
  `kill_signal`/`kill_timeout` jobspec knobs (defaults SIGINT—sic—and 5 s,
  client-side `max_kill_timeout` cap). **Take:** the per-runtime capability
  report (Legion already has `ExecSupport`), graceful-kill semantics, and the
  two task-level knobs — as `stopSignal`/`stopGracePeriod`, with docker's
  friendlier SIGTERM/10 s defaults and an admission-time cap instead of a
  client config knob. **Reject for now:** a user-facing `alloc signal` verb —
  additive later, and `changeSignal` already covers the declared-refresh case.
- **Kubernetes** (CRI `Exec`/`ExecSync`, `terminationGracePeriodSeconds`):
  kubelet asks the CRI for a streaming exec URL; termination is
  TERM → grace period → KILL, with preStop hooks. **Take:** the termination
  shape. **Reject:** the streaming-URL indirection — Legion's frame protocol
  and worker-side PTY already exist and work
  ([docs/exec-and-actions.md](../../docs/exec-and-actions.md)).
- **Resource limits — Nomad, docker, K8s, and the OCI spec.** Nomad's model
  ([cpuset management](https://developer.hashicorp.com/nomad/docs/concepts/cpu)):
  the client partitions usable CPUs into a **share slice** and a **reserve
  slice**; a task declaring `cores` gets whole CPUs moved from share to
  reserve, exclusively; every other task lives in the share slice with
  `cpu.weight` derived from its declared MHz, so contention degrades
  *proportionally to what was declared*. The scheduler counts cores
  numerically — the *client* picks which physical cores. Docker's `--cpus=N`
  is CFS bandwidth (quota/period) with no exclusivity; `--memory` is a hard
  cgroup limit everywhere. K8s adds requests-vs-limits and QoS classes. The
  OCI runtime spec carries all of it declaratively in `linux.resources` — the
  runtime creates the cgroup and applies whatever the spec says.
  **Take:** Nomad's model wholesale — share/reserve cpuset partition,
  worker-side core allocator, MHz → `cpu.weight` — plus hard memory limits.
  **Reject:** docker-style CFS quota for `cores` (throttling admits tail
  latency that pinning exists to remove, and it leaves MHz meaningless), the
  K8s requests/limits split (Legion has one `resources` block; keeping it one
  concept is the simplicity bet), and Nomad's NUMA-aware placement (deferred,
  see non-goals).
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
- When my service is latency-sensitive, I want the cores I declared to be
  *mine* — not shared with whatever else landed on the node — so my p99
  doesn't depend on my neighbors' load.
- When my database needs more than ten seconds to checkpoint on shutdown, I
  want to say so in the jobspec, so a routine reschedule doesn't force-kill it
  mid-flush.
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
- When the node is saturated, I want CPU to degrade in proportion to what
  each job declared, so the 4000-MHz service doesn't lose a coin flip to the
  100-MHz cron job.
- When I run container and exec tasks on the same node, I want one set of
  rules governing them both, so a core reserved by one workload is actually
  off-limits to the other.

**Runtime plugin author**
- When I build a custom task runtime, I want to opt into the node's CPU and
  memory accounting with one declaration, so my tasks are governed like the
  built-ins without me reimplementing an allocator.
- When my runtime manages its own isolation (a VM, a different cgroup
  driver), I want to receive the worker's decision and apply it my own way,
  so I get correct cluster-wide accounting without fighting the worker for
  the same cgroup files.

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
2. A rollout replaces an allocation. The old container gets its `stopSignal`
   (default SIGTERM), the app closes its listeners and exits within
   `stopGracePeriod`, and the worker reaps it — `delete --force` never fires.
   A hung container is force-deleted at the deadline. A database task that
   declared `stopGracePeriod: '90s'` gets its full checkpoint window.
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
6. A latency-sensitive service declares `resources: { cores: 2 }` on a Linux
   node. The worker assigns it two specific cores, removes them from every
   other governed task's CPU set, and `grep Cpus_allowed_list
   /proc/self/status` inside the container shows exactly those two. A batch
   job on the same node declaring `cpu: 500` shares the remaining cores at a
   weight proportional to 500 MHz.
7. The same node also runs an `exec`-runtime log shipper. It lives in the
   share slice automatically — it cannot touch the pinned service's two
   cores, and its own declared `memory` is now a real limit. Nothing in its
   jobspec changed.

## 4. Goals and non-goals

**Goals**

- `legion exec` (one-shot and `-it`), actions, the UI terminal, and MCP exec
  work against container tasks on Linux with youki, runc, and crun.
- The container runtime reports `supports_signal() == true` and delivers
  signals to the container init on both platforms; `changeMode: signal` is
  accepted for container tasks.
- Container teardown becomes graceful and *declarable*: task-level
  `stopSignal` / `stopGracePeriod` jobspec fields (defaults `SIGTERM` /
  `10s`), honored by the container runtime on both platforms and by the exec
  runtime; Linux stop becomes signal → bounded wait → `delete --force`.
- Declared resources are kernel-enforced on Linux with Nomad's semantics:
  `memory` a hard limit, `cores` exclusive pinned CPUs (worker-assigned
  cpuset, share/reserve partition), `cpu` (MHz) proportional `cpu.weight` —
  so no `resources` field is scheduling-only on Linux anymore.
- Enforcement is a **worker-core subsystem with a plugin contract**, not
  container-runtime plumbing: `resource_enforcement()` on the trait
  (`Unsupported` / `WorkerCgroup` / `RuntimeApplied`), a worker-computed
  `ResourceAssignment` on the instance, and a one-call spawn helper — the
  exec runtime adopts it in this plan, so reserved cores are exclusive
  across *all* governed workloads on the node, and a custom runtime opts in
  with one method plus one helper call.
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

- **A `legion alloc signal` CLI verb.** Additive later; `changeSignal` covers
  the declared-refresh case and `stopSignal` the teardown case.
- **Linux port mapping / private network namespaces.** Containers keep host
  networking; that's the networking plan's territory, not a bundle tweak.
- **NUMA/SMT-aware core placement.** The core allocator hands out whole
  logical CPUs with no topology awareness — no NUMA-node affinity, no
  SMT-sibling avoidance. Nomad gates NUMA behind Enterprise for a reason;
  revisit with a workload that measurably needs it.
- **Fencing processes outside Legion.** The share/reserve partition governs
  every runtime that opts into `resource_enforcement()` — node daemons, SSH
  sessions, and tasks on `Unsupported` runtimes remain beyond it. Inherent
  to a shared host, not closable by the interface.
- **`sandbox` adopting the enforcement interface.** The microVM runtime is
  the designed-for next adopter (`RuntimeApplied`, pinning vCPU threads) but
  ships nothing here (§11 Q4).
- **A non-root / delegated-subtree hierarchy root.** v1 assumes the standard
  root worker creating `legion/` at the cgroup root; a configurable root for
  delegated setups is future work.
- **CPU enforcement on macOS beyond guest sizing.** Apple `container` has no
  weight or pinning analogue; `cpu` (MHz) remains scheduling-only there, and
  `cores` keeps meaning VM size, not exclusivity.
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
  `memory`/`cores` for the macOS flags. The exec runtime already has the
  join mechanism the `WorkerCgroup` mode generalizes: its optional
  `config.cgroup` field writes the child into a named cgroup via a
  `pre_exec` hook, cgroup-v2-only (`exec.rs:472`) — today the *job* names
  the cgroup and nobody creates or sizes it; §6.4 has the worker do both.
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

### 6.3 Graceful teardown, declared in the jobspec

Two new **task-level** fields — the only new jobspec surface in this plan:

```jsonnet
task: {
  name: 'db',
  runtime: 'container',
  config: { image: 'postgres:16', command: ['postgres'] },
  stopSignal: 'SIGINT',          // default 'SIGTERM'
  stopGracePeriod: '90s',        // default '10s' (duration string, as actions' timeout)
}
```

- **`stopSignal`** — the signal delivered when Legion stops the task.
  Validated at admission against the existing `parse_signal` vocabulary
  (`task_manager.rs:1126`). The default chain on the Linux container path is
  docker's: jobspec `stopSignal` → the image config's `StopSignal` (Legion
  parses the image config already) → `SIGTERM`. Apple `container stop` and
  the exec runtime use jobspec-or-`SIGTERM` (Apple's runtime applies the
  image's stop signal itself).
- **`stopGracePeriod`** — how long the task gets between the signal and the
  hard kill. Duration string, same format as action `timeout`. Validated
  positive and capped at admission — **5 m** — so a wedged task cannot stall
  drains and reschedules indefinitely (the cap plays the role of Nomad's
  client-side `max_kill_timeout`, but as one cluster-wide admission rule
  instead of a per-client knob; §11 Q3 holds the cap value).

Both fields are task-level (they sit next to `resources`, not inside the
runtime `config`) because the semantics are runtime-independent; each runtime
honors them with its own mechanism:

- **container, Linux:** `<runtime> kill legion-<alloc> <stopSignal>` — ignore
  "not running" errors (a finished batch task is already gone) — then poll
  `<runtime> state legion-<alloc>` until stopped/absent, up to
  `stopGracePeriod`, then `<runtime> delete --force legion-<alloc>`
  unconditionally (the reaper for the stopped container *and* the killer for
  a hung one), then reap the foreground supervisor child as today.
- **container, macOS:** `container stop --signal <stopSignal>
  --time <secs> legion-<alloc>` (flag spellings verified on hardware in this
  phase — same caveat as Q2).
- **exec:** `reap_task_process` grows the same shape — deliver `stopSignal`
  to the tracked PID, wait up to `stopGracePeriod`, then SIGKILL. Today's
  behavior is the degenerate case (`stopGracePeriod: '0s'` semantics), so
  the exec runtime gets graceful stop as a side effect of the same fields.
  Once §6.4's enforcement lands (phase 6), the *hard-kill* step upgrades to
  the task cgroup's `cgroup.kill`, which reaps double-forked descendants the
  tracked PID misses. The graceful step stays a PID signal either way:
  cgroup v2 exposes `cgroup.kill` only (SIGKILL), with no way to broadcast
  an arbitrary signal to a cgroup's members.
- **noop / runtimes with nothing to signal:** the fields are accepted and
  inert, documented as such.

`destroy` keeps delegating to `stop`. The preemption plan's "graceful stop"
phase (its eviction path) consumes these same fields rather than growing its
own — coordinate there, don't duplicate.

### 6.4 Linux cgroup enforcement, Nomad's model: hard memory, pinned cores, weighted MHz — behind a runtime-agnostic interface

No new jobspec surface — the three `resources` fields the scheduler already
reads gain their kernel meaning on Linux, for **every runtime that opts in**,
not just `container`. Like Nomad — whose client-side cpuset manager governs
all task drivers through one mechanism — the enforcement lives in **worker
core**, and task runtimes consume it through a small contract.

**The subsystem.** The worker partitions the node's usable CPUs Nomad-style
into two parent cgroups it creates and owns:

```
legion/
├── share/      cpuset.cpus = usable − reserved     ← unpinned tasks live here
│   └── legion-<alloc>   cpu.weight from declared MHz
└── reserve/    cpuset.cpus = union of reservations
    └── legion-<alloc>   cpuset.cpus = its assigned cores, exclusively
```

A task declaring `cores` gets whole CPUs moved out of the share slice; every
other opted-in task lives under `share/` and inherits its shrinking cpuset
automatically (cgroup v2 cpusets are hierarchical — one write to
`legion/share/cpuset.cpus` re-fences every tenant at once, no per-task
updates; the allocator maintains the `reserve/` union mask alongside it).
Before `prepare()` — the same lifecycle point where volumes are mounted and
attached — the worker computes each allocation's **assignment** and hangs it
on the instance:

```rust
/// Computed by worker core on Linux for every runtime whose
/// `resource_enforcement()` is not Unsupported; None elsewhere.
pub struct ResourceAssignment {
    pub slice: Slice,                    // Share | Reserve
    /// Cgroup for this task, relative to the cgroup root
    /// (`legion/share/legion-<alloc>` / `legion/reserve/legion-<alloc>`).
    pub cgroup_path: PathBuf,
    pub cores: Option<Vec<u32>>,         // Reserve: worker-assigned logical CPUs
    pub cpu_weight: Option<u64>,         // Share: from declared MHz
    pub memory_limit_bytes: Option<u64>, // from declared memory
}
```

**The contract runtimes adopt.** One defaulted trait method, in the style of
`supports_*` and `exec_support()`:

```rust
/// How this runtime participates in worker-enforced resource placement.
pub enum ResourceEnforcement {
    /// Not governed (the default): no assignment is computed. Honest for
    /// runtimes with nothing to confine (noop) and for unported plugins.
    Unsupported,
    /// Worker core creates the assignment's cgroup, writes its limits
    /// (cpuset.cpus / cpu.weight / memory.max+swap.max), and the runtime
    /// spawns its process into it — for runtimes whose task IS a host
    /// process. Adopting is two lines: return this, and spawn through
    /// `join_assignment_cgroup(&mut cmd, &instance)` (a pre_exec join of
    /// the target cgroup, the same race-free mechanism the exec runtime's
    /// explicit `cgroup` config already uses).
    WorkerCgroup,
    /// The runtime applies the assignment itself, by its own means — the
    /// container runtime writes it into the OCI spec; a microVM runtime
    /// would pin vCPU threads. Worker core computes and accounts, but
    /// touches no cgroup for this task.
    RuntimeApplied,
}

fn resource_enforcement(&self) -> ResourceEnforcement {
    ResourceEnforcement::Unsupported
}
```

Worker core owns everything hard — fingerprinting, the allocator, both slice
masks, persistence and restart recovery, release on destroy — so an adopting
runtime never touches accounting. `WorkerCgroup` runtimes additionally get
**whole-tree teardown for free**: because the task's processes all live in a
Legion-owned cgroup, the hard-kill step of `stop()` writes `cgroup.kill`
instead of `kill(pid)`, catching double-forked descendants that the current
tracked-PID reaping can orphan.

**Consumption, per built-in:**

- **`container` → `RuntimeApplied`.** The assignment lands in the generated
  OCI spec — `cgroupsPath` from `assignment.cgroup_path`, `cpu.cpus` /
  `cpu.shares` / `memory` from the assignment fields — and the runtime binary
  creates the cgroup at that path, which places it inside the worker's
  hierarchy so the share mask governs it. This is the design already shown
  below; only the values' *source* moves into worker core.
- **`exec` → `WorkerCgroup` — folded in.** Exec tasks join the same
  hierarchy, so a core reserved by any task is exclusive against *all*
  governed workloads on the node, and exec tasks gain enforcement they never
  had: `cores` pins them, `cpu` (MHz) weights them, and `memory` becomes a
  real `memory.max` (previously scheduling-only for exec, too). Two
  interactions with existing exec config, both resolved for back-compat:
  an explicit `config.cgroup` **wins** and opts the task out of the Legion
  hierarchy (it keeps meaning what it means today — join the cgroup the job
  named); declaring `cores` *together with* an explicit `cgroup` is rejected
  at admission, since Legion cannot promise exclusivity inside a cgroup it
  does not manage. `user`, landlock, and `oom_score_adj` compose unchanged
  (the cgroup join is one more pre_exec step, ordered before setuid, exactly
  as the existing join works).
- **`noop`, `sandbox`, dylib plugins → `Unsupported`** until they opt in;
  admission keeps accepting their `resources` as scheduling-only, as today.
  The sandbox runtime is the obvious next `RuntimeApplied` adopter (pin the
  VMM's vCPU threads to `assignment.cores` — §11 Q4).

The generated container spec:

```jsonnet
resources: { memory: 256, cores: 2 }    // MiB, whole cores — pinned task
resources: { memory: 128, cpu: 500 }    // MiB, MHz — shared task
```

becomes, respectively:

```json
"linux": {
  "cgroupsPath": "legion/reserve/legion-<alloc>",
  "resources": {
    "devices": [ ... ],                                   // unchanged
    "memory": { "limit": 268435456, "swap": 268435456 },
    "cpu":    { "cpus": "3,7" }                           // worker-assigned
  }
}
```

```json
"linux": {
  "cgroupsPath": "legion/share/legion-<alloc>",
  "resources": {
    "memory": { "limit": 134217728, "swap": 134217728 },
    "cpu":    { "shares": 500 }                           // runtime → cpu.weight
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
- **`cores` → exclusive pinned CPUs.** The scheduler keeps counting `cores`
  numerically; at start the worker's core allocator picks specific free
  logical CPUs (lowest-numbered free) and the assignment flows to the runtime
  as described above. Exhaustion — the scheduler's count says yes but no
  whole cores are free (fingerprint drift, offlined CPUs) — fails the
  allocation with an error naming the assigned/free sets, feeding the normal
  `on_unhealthy` move machinery rather than wedging.
  **Validation, on every governed runtime (`container` and `exec` alike):**
  `cores` must be a whole number (a cpuset cannot hold half a core; macOS
  rounds today, which was quietly lossy — now `cores: 1.5` is rejected at
  submit), and a task declares `cores` *or* `cpu`, not both (Nomad's rule;
  two CPU contracts on one task have no coherent meaning).
- **`cpu` (MHz) → proportional weight.** Written as OCI `cpu.shares` = MHz
  (Nomad's convention), which cgroup-v2 runtimes convert to `cpu.weight`
  via the standard mapping (`weight = 1 + ((shares − 2) × 9999) / 262142` —
  the same formula systemd and Nomad use). Weight only bites under
  contention: an idle node lets everyone burst, a saturated one degrades each
  task in proportion to its declaration. MHz stops being scheduling-only on
  Linux; on macOS it stays unenforced (no Apple analogue — see non-goals).
- **Declared-only, like macOS:** a job that declares nothing gets no
  `memory`/`cpu` entries, lands in the share slice at the default weight, and
  is otherwise unconstrained — scheduler defaults are not folded in,
  mirroring the existing macOS call.
- **`cgroupsPath` is explicit** (`legion/share/…` / `legion/reserve/…`,
  cgroupfs form) so operators can find a task's cgroup by allocation id and
  the share/reserve split is visible in the hierarchy. The systemd cgroup
  driver (`:slice:prefix:name` form) stays out of scope until someone runs
  Legion under systemd-managed delegation and needs it.

**Requirements and failure modes.** Everything here requires the cgroup v2
unified hierarchy — the position the exec runtime already takes — and a
worker able to create and write the `legion/` subtree at the cgroup root
(the standard root-worker deployment; the worker enables the
cpuset/cpu/memory controllers in the parents' `subtree_control`, and a
configurable hierarchy root for delegated non-root setups is deliberately
future work). Failures surface as the creating side's error — the runtime's
own at container create for `RuntimeApplied`, the worker's cgroup write for
`WorkerCgroup` — propagated with context by `start()`, and the public doc
gets an actionable failure-modes row for each. The worker fingerprints
usable CPUs from its own `cpuset.cpus.effective` at startup (not `nproc` —
the worker itself may be confined). **Exclusivity scope, stated honestly:**
the share/reserve fence governs every runtime that opts in — `container` and
`exec` after this plan — but processes outside Legion entirely (node
daemons, SSH sessions) and tasks on `Unsupported` runtimes are beyond it and
may still touch reserved cores. That residue is inherent to running on a
shared host, not a gap the interface can close. **OOM diagnostics:** on a
nonzero exit, the worker best-effort reads the cgroup's `memory.events`
`oom_kill` counter before teardown and, when it fired, annotates the failure
record ("task exceeded its 256 MiB memory limit and was OOM-killed") — exit
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

- **None to the proto or wire format.** The exec frame protocol,
  `ExecAllocation`, and the manager↔worker RPC surface are untouched (the
  jobspec rides as JSON inside the existing job-config payload).
- **Two new jobspec fields** — task-level `stopSignal` and `stopGracePeriod`
  (§6.3). Both go into `configs/jobs/maximal.jsonnet` with their defaults and
  the cap in comments. `resources.memory`/`cores`/`cpu`,
  `kv_injections.files`, identity `file`, and `artifacts` all exist already —
  this plan changes what they *do* for container tasks, and the
  maximal-jsonnet comments on the container example get the new semantics.
- **Worker-local state gains one field:** the persisted allocation record
  (`PersistedAllocation`) carries the allocation's `ResourceAssignment`,
  `serde`-defaulted so records written before the change load unchanged.
  Worker restart rebuilds the core allocator and both slice masks from these
  records — the assignment is the unit of persistence, so a future adopter
  needs no new state.
- `TaskPlugin` gains two defaulted methods — `signal()` and
  `resource_enforcement()` — and `TaskInstance` gains
  `resource_assignment: Option<ResourceAssignment>`, filled by worker core
  before `prepare()` (dylib plugins stay source-compatible via the defaults;
  the registry's existing version gating covers the rebuild requirement, same
  as the exec plan's ABI addition). A shared `join_assignment_cgroup` helper
  joins the `spawn_pty`/`spawn_pipe`/`configure_task_log_capture` family.
- `build_bundle`/`build_spec` gain the task's assignment (internal signature
  change only); capability flips in the container plugin:
  `supports_kv_file_injection`, `supports_identity_file`, `supports_signal`
  → true; `resource_enforcement()` returns `RuntimeApplied` for `container`
  and `WorkerCgroup` for `exec`.
- **New validation rules:** `stopSignal` from the `parse_signal` vocabulary;
  `stopGracePeriod` a positive duration ≤ the cap; on governed runtimes
  (`container`, `exec`) `cores` must be a whole number and `cores` xor
  `cpu`; an exec task combining `cores` with an explicit `config.cgroup` is
  rejected (no exclusivity inside a cgroup Legion doesn't manage);
  container-task file-delivery paths must resolve under `/legion/task`
  (§6.5) — all enforced at admission, the path rule re-checked on the worker.
- Docs: capability tables in `docs/task-runtime-container.md` (signal, KV
  file, identity file, Linux limits, Linux exec rows; the "Resources and
  Ports" and "Current Scope and Limitations" sections), `docs/exec-and-actions.md`
  (the `container (youki)` row), `docs/workload-identity.md` (runtime matrix +
  container file paths), `docs/kv-store.md` (container file injection),
  `docs/artifacts.md` (file/tree artifacts in containers),
  `docs/task-runtime-exec.md` (the runtime is now governed; `cgroup`
  precedence), and **a new "Resource enforcement" section in
  `docs/task-runtime-plugins.md`** — the three modes, the assignment struct,
  the two-line `WorkerCgroup` adoption recipe, and what worker core promises
  to do on the plugin's behalf; `internal_docs/TODO.md` per phase.

## 8. Implementation phases

Each ships alone. 1 goes first (it revives the dead OCI argv path the others
lean on for confidence); the rest are mutually independent except that 5
wants 4's assignment plumbing and 6 wants 5's allocator, listed in the order
that front-loads user value.

- **Phase 1 — Linux OCI exec.** `supports_exec()` flip, OCI argv dialect in
  `build_exec_command` (+ env mapping, `--` separator), unit tests, e2e
  `#[ignore]` tests, doc-table updates. Worker-only; no manager change.
  *Value: exec + actions + UI + MCP work on Linux.*
- **Phase 2 — the signal seam.** Trait method + default, task-manager
  routing, container override for both platforms, admission-gate flip and test
  inversion, docs. *Value: `changeMode: signal` on containers; the seam any
  future runtime (sandbox guest agent) implements.*
- **Phase 3 — graceful stop + the `stopSignal`/`stopGracePeriod` fields.**
  The two task fields with validation, defaults, and the admission cap; the
  Linux signal → bounded-wait → `delete --force` sequence; the Apple
  `stop --signal --time` arm; the exec runtime's signal → wait → SIGKILL in
  `reap_task_process`; `maximal.jsonnet`; e2e tests (trap-and-exit inside the
  window, ignore-and-force-kill at the deadline, a long `stopGracePeriod`
  honored); docs. Builds on Phase 2's `kill` helper but ships independently
  (it can shell out directly if 2 hasn't landed). *Value: no more data-loss
  stops, and drains the job author can actually size.*
- **Phase 4 — the enforcement interface + memory limits + CPU weight.**
  `resource_enforcement()` on the trait, `ResourceAssignment` on the
  instance, worker-core computation before `prepare()`; the `legion/share`
  parent cgroup; the `memory` hard-limit/swap mapping and `cpu` MHz →
  `cpu.shares` consumed by the container runtime (`RuntimeApplied`, via
  `build_spec` + `cgroupsPath`); OOM annotation from `memory.events`; the
  `cores`-xor-`cpu` and whole-`cores` validation rules (landing here so
  later phases change behavior, not the accepted spec surface); unit tests
  over the generated spec; e2e OOM test; docs. *Value: the contract exists,
  memory becomes real, and MHz stops being scheduling-only — before pinning
  even lands.*
- **Phase 5 — exclusive cores (cpuset pinning).** The worker core allocator
  (fingerprint from `cpuset.cpus.effective`, lowest-free assignment, release
  on stop/destroy, restart recovery via the persisted-record field); the
  `legion/reserve` slice and both-mask maintenance; `cpu.cpus` in the spec;
  the exhaustion failure path; e2e pinning tests. *Value: `cores` means
  "these CPUs are yours" on Linux, like Nomad.*
- **Phase 6 — the exec runtime folds in (`WorkerCgroup`).** Worker-created
  task cgroups sized from the assignment; the `join_assignment_cgroup`
  spawn helper and the exec runtime calling it; the explicit-`config.cgroup`
  precedence rule and the `cores`+`cgroup` rejection; `cgroup.kill`-based
  hard kill for governed process trees; e2e mixed-node tests (scenario 7);
  `docs/task-runtime-exec.md` and the plugin-authoring page document the
  contract. *Value: reserved cores are exclusive across all governed
  workloads; exec tasks gain real memory limits; the interface is proven by
  a second, differently-shaped consumer.*
- **Phase 7 — task-dir mount + file delivery.** The `/legion/task` mount on
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
    stop sequence ordering (kill before delete) and the `stopSignal` /
    `stopGracePeriod` values flowing into it. Mirrors the existing
    `build_apple_launch` test style.
  - jobspec validation — `stopSignal` vocabulary, `stopGracePeriod`
    positive-and-capped, whole-number `cores`, `cores`-xor-`cpu`,
    `cores`-with-explicit-`cgroup` on exec, each with its documented error
    message.
  - the enforcement interface — an `Unsupported` runtime gets no assignment
    computed and no cgroup created; a `RuntimeApplied` one gets an
    assignment and worker core touches no cgroup files for it; a
    `WorkerCgroup` one gets its cgroup created and populated before
    `prepare()`. A **contract test generic over the trait** asserts these
    three shapes, so the next adopter (in-tree or dylib) inherits the suite
    rather than writing one.
  - generated `config.json` — memory limit/swap byte math; `cpu.shares` from
    MHz; `cpu.cpus` from assigned cores; `cgroupsPath` share vs. reserve; no
    `memory`/`cpu` entries when nothing is declared; devices rule untouched.
  - the core allocator — assignment (lowest free), release, double-release
    idempotence, exhaustion error naming free/assigned sets, both-mask
    arithmetic, and restart recovery from persisted records (including
    records without the field).
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
  - graceful stop: a container trapping its declared `stopSignal` exits
    inside the window (and `delete --force` finds it already gone); a
    signal-ignoring container is gone by ~grace+ε; a non-default
    `stopGracePeriod: '30s'` is honored; the exec runtime passes the same
    trio;
  - limits: a memory-hog container dies at its declared limit with the OOM
    annotation in the failure record; a `cpu: 500` container's cgroup shows
    the expected `cpu.weight` (file-content assertion — weight ratios under
    load are too flaky for CI);
  - pinning: `Cpus_allowed_list` inside a `cores: 2` container equals its
    two assigned CPUs; a concurrently running share-slice container's
    effective cpuset excludes them; stopping the pinned task returns the
    cores to the share slice; a worker restart preserves the assignment;
  - **mixed-runtime enforcement (scenario 7):** a pinned container task and
    an `exec` task on one worker — the exec task's `Cpus_allowed_list`
    excludes the container's reserved cores and vice versa, its declared
    `memory` shows up as `memory.max` on its worker-created cgroup, and an
    exec task with an explicit `config.cgroup` lands there instead
    (back-compat). A double-forked child of an exec task is reaped by
    `cgroup.kill` where the old tracked-PID path would have orphaned it;
  - files: a staged file artifact and a KV-rendered file are readable at
    their `/legion/task` paths from inside the container; a host-side atomic
    rewrite is visible inside without restart (the rename-propagation check —
    on macOS this is also the virtiofs verification from §6.5).
  - Matrix: runc (primary), crun, youki — same tests, `LEGION_OCI_RUNTIME`
    selects; divergences become documented limitations, not silent skips.
- **Regression:** full `cargo test --all --locked`; `cargo test --test
  acl_endpoints` (no new endpoints — must stay green untouched); the identity
  admission test inversion; existing volume-mount tests unchanged (the
  task-dir mount must not reorder or shadow declared volumes); the exec
  runtime's existing cgroup/landlock/user tests unchanged (the assignment
  join must compose with them, not replace them); the reference dylib task
  plugin in `plugins/tasks/` builds and loads without implementing either new
  trait method.
- Not a scheduler change — no benchmark numbers required.

## 10. Rollout and backward compatibility

- No feature gate. Every phase changes worker-local behavior only, plus
  admission rules on the manager (Phases 3–7); no manager/worker version
  handshake concerns (an old manager talking to a new worker simply finds
  exec working where it used to error; the new jobspec fields are inert JSON
  to an old worker). Third-party dylib task plugins keep working untouched —
  both new trait methods are defaulted, and `Unsupported` means an unported
  plugin behaves exactly as it does today.
- **Visible behavior changes** to release-note:
  1. Container (and exec-runtime) stop delivers `stopSignal` and waits up to
     `stopGracePeriod` (SIGTERM / 10 s undeclared) before the hard kill.
     Rescheduling a hung container is up to the grace period slower than
     today's instant SIGKILL — the correct trade, and the docker default
     everyone already operates under.
  2. `changeMode: signal` container jobs that used to be rejected at submit
     now deploy; likewise KV-file and identity-file container jobs (Phase 7).
  3. **Declared limits become real on Linux, for container *and* exec
     tasks.** A job declaring `memory: 256` while actually using more ran
     fine yesterday and is OOM-killed today; a `cores` job is pinned to
     specific CPUs instead of floating; a `cpu` (MHz) job's share of a
     saturated node now tracks its declaration. This is the declared
     contract finally being enforced — the release note says so plainly, and
     the OOM annotation tells the author which limit fired. Jobs declaring
     nothing see no change. Exec tasks additionally move into a
     Legion-managed cgroup unless they name one themselves, which changes
     where an operator looks for them in the hierarchy (and makes their
     descendants reliably reaped).
  4. **Validation tightenings can reject previously-accepted specs at
     re-submit:** fractional `cores` (silently rounded on macOS before,
     ignored on Linux), `cores` together with `cpu`, and — on exec tasks —
     `cores` together with an explicit `config.cgroup`. Every error names
     the fix; running allocations are untouched until their next submit.
  5. Container tasks start receiving `LEGION_TASK_DIR` / `LEGION_ARTIFACT_*`
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
- **Q2 — do Apple `container kill --signal` and `container stop --signal
  --time` cover the needed flag/name set?** The subcommands exist; exact
  spellings need verifying on real hardware during Phases 2–3. *Lean:* pass
  `SIG`-stripped names and normalize in the plugin if needed; worst case
  macOS maps unsupported names to a clear error.
- **Q3 — is 5 minutes the right admission cap for `stopGracePeriod`?** The
  cap exists so a wedged task cannot stall drains and reschedules
  indefinitely; the value is a taste call (Nomad's client default caps at
  30 s, K8s commonly runs minutes). *Lean:* 5 m — long enough for a database
  checkpoint, short enough that a node drain still converges; revisit against
  the preemption plan's eviction deadlines when that lands.
- **Q4 — which runtime adopts `resource_enforcement()` next, and does the
  interface survive it?** `sandbox` is the designed-for case
  (`RuntimeApplied`, pinning the VMM's vCPU threads to `assignment.cores`
  and sizing the guest from `memory_limit_bytes`), and it is the honest test
  of whether the three modes are the right cut — a microVM's "memory limit"
  is guest size, not a cgroup ceiling, so the assignment may need to say
  which it means. *Lean:* ship the interface with two consumers of different
  shapes (`RuntimeApplied` container + `WorkerCgroup` exec), and let the
  sandbox adoption — its own change — be what forces any refinement, rather
  than speculating a third mode now.
- **Q4b — should `WorkerCgroup` tasks also get the OOM annotation and
  `cgroup.kill` teardown when they declared no resources?** Worker core
  could create a cgroup for every governed task regardless, purely for
  process-tree containment. *Lean:* yes, and it is nearly free — but it
  makes "declared nothing" no longer mean "worker touches nothing," so
  confirm during Phase 6 that the empty-cgroup case is genuinely
  side-effect-free before committing.
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
- **Phase 3:** scenario 2 works end to end — declared `stopSignal` and
  `stopGracePeriod` honored on both container platforms and the exec runtime,
  the signal-ignoring container force-killed at the deadline; the two fields
  validate (vocabulary, cap) with the documented errors and appear in
  `maximal.jsonnet`; `stop` on an already-exited container remains
  error-free; docs no longer call Linux stop "delete --force" only.
- **Phase 4:** scenario 4 works end to end on a Linux worker — the memory hog
  is OOM-killed at its declared limit with the annotated failure record, a
  `cpu: 500` container's cgroup carries the mapped weight, an undeclared
  job's spec carries no `memory`/`cpu` entries, fractional-`cores` and
  `cores`+`cpu` specs are rejected at submit, and the capability table's
  "Memory / CPU limits" row reads ✅ for Linux with the cgroup-v2 requirement
  documented. The interface contract test passes for all three modes, and
  `docs/task-runtime-plugins.md` documents the adoption recipe.
- **Phase 5:** scenario 6 works end to end on a Linux worker — the pinned
  container sees exactly its assigned CPUs, concurrent share-slice containers
  exclude them, the cores return on stop, and the assignment survives a
  worker restart; core exhaustion fails the allocation with the documented
  error instead of wedging.
- **Phase 6:** scenario 7 works end to end — an exec task and a pinned
  container task on one worker mutually exclude each other's cores, the exec
  task's declared `memory` is enforced as `memory.max`, an exec task naming
  its own `config.cgroup` still lands there, `cores`+`cgroup` is rejected at
  submit, and a double-forked exec descendant is reaped on stop. The exec
  runtime's existing user/landlock/cgroup tests still pass unchanged.
- **Phase 7:** scenario 5 works end to end on both platforms — declared KV
  file, identity file, and a file artifact all readable at `/legion/task`
  paths inside the container, a rotation visible without restart; the
  out-of-range path is rejected at submit with the documented error; the
  capability table's KV-file / identity-file / artifact rows read ✅ with the
  path rule stated.
