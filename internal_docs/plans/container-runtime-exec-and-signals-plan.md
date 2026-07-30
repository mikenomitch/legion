# Container Runtime: Exec on Linux, Signal Delivery, and Graceful Stop

> **Judgement calls:** [../calls/container-runtime-exec-and-signals-plan_calls.md](../calls/container-runtime-exec-and-signals-plan_calls.md)

**Status: PLANNED.**

Two gaps in the `container` task runtime, one plan, because they share the same
mechanism (shelling out to a runtime subcommand addressed by container name)
and the same goal (Linux production parity with the macOS dev experience):

1. **Exec does not work on Linux.** `legion exec`, the UI terminal, MCP
   one-shot exec, *and declared actions* all fail against container tasks on
   Linux — the OS the workloads actually ship on. This was deferred as Q1 of
   the exec plan ([exec-and-actions-plan.md](exec-and-actions-plan.md)).
2. **The runtime cannot deliver signals — the missing core feature.** No
   platform can signal a container task: `changeMode: signal` identities are
   rejected at admission, and Linux teardown is `delete --force` (SIGKILL) with
   no graceful-shutdown window at all.

Deliberately deferred (see Non-goals): Linux cgroup limits, mounting the task
directory into the container (KV/identity *file* delivery, file/tree
artifacts), jobspec `stopSignal`/`stopGracePeriod` knobs.

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

**Cluster operator**
- When I promise "what works on the mac dev cluster works in prod," I want the
  container runtime's capability table to be the same on both, so I stop
  explaining the asymmetry.

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

## 4. Goals and non-goals

**Goals**

- `legion exec` (one-shot and `-it`), actions, the UI terminal, and MCP exec
  work against container tasks on Linux with youki, runc, and crun.
- The container runtime reports `supports_signal() == true` and delivers
  signals to the container init on both platforms; `changeMode: signal` is
  accepted for container tasks.
- Linux container teardown becomes graceful: SIGTERM → bounded wait →
  `delete --force`.
- The capability tables in
  [docs/task-runtime-container.md](../../docs/task-runtime-container.md),
  [docs/exec-and-actions.md](../../docs/exec-and-actions.md), and
  [docs/workload-identity.md](../../docs/workload-identity.md) lose three ❌s.

**Non-goals** (each stays on
[TODO.md](../TODO.md) / the capability table as an honest ❌)

- **Linux cgroup resource limits and port mapping.** Real, adjacent, and
  *independent*: they live in the bundle's `config.json` generation
  (`oci_bundle.rs`), not in the runtime-subcommand seam this plan builds on.
  Bundling them here would couple two unrelated review surfaces.
- **Mounting the task directory into the container** (unblocks KV/identity
  *file* delivery and file/tree artifacts). Needs a volume-plumbing design of
  its own; explicitly deferred by the artifact and identity plans already.
- **Jobspec `stopSignal` / `stopGracePeriod` knobs and a `legion alloc signal`
  CLI verb.** New user-facing surface; deferred until a job actually needs a
  non-default value (§11 Q3). The fixed docker-style default ships first.
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

## 7. Proto, struct, config, and persistence changes

- **None to the proto, wire format, or persistence.** The exec frame protocol,
  `ExecAllocation`, and worker state are untouched.
- **No new jobspec fields** — nothing for `configs/jobs/maximal.jsonnet`.
- `TaskPlugin` gains the defaulted `signal()` method (dylib plugins stay
  source-compatible; the registry's existing version gating covers the rebuild
  requirement, same as the exec plan's ABI addition).
- Docs: capability tables in `docs/task-runtime-container.md` (`Signal
  delivery` → ✅, exec row → ✅ Linux), `docs/exec-and-actions.md` (the
  `container (youki)` row), `docs/workload-identity.md` (§ runtime matrix:
  container signal → ✅); `internal_docs/TODO.md` per phase.

## 8. Implementation phases

Each ships alone, in any order after Phase 1; 2 and 3 are independent of each
other.

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

## 9. Testing plan

- **Unit (every phase, CI-safe):** argv construction — OCI exec includes
  `--tty`/`--`/`--env` and never `--interactive`; Apple arm unchanged (no
  `--`); signal argv per platform; stop sequence ordering (kill before
  delete). Mirrors the existing `build_apple_launch` test style.
- **E2E, `#[ignore]`d behind a real runtime** (extending the existing
  `oci_runtime_*` suite; run with
  `LEGION_OCI_RUNTIME=runc cargo test --lib oci_runtime -- --ignored`):
  - exec one-shot: output captured, exit code propagated, task env visible;
  - exec tty: shell round-trip through a PTY, resize, Ctrl-C;
  - signal: a SIGHUP-trapping script observably reacts;
  - graceful stop: TERM-trapping container exits inside the window (and
    `delete --force` finds it already gone); TERM-ignoring container is gone
    by ~grace+ε.
  - Matrix: runc (primary), crun, youki — same tests, `LEGION_OCI_RUNTIME`
    selects; divergences become documented limitations, not silent skips.
- **Regression:** full `cargo test --all --locked`; `cargo test --test
  acl_endpoints` (no new endpoints — must stay green untouched); the identity
  admission test inversion.
- Not a scheduler change — no benchmark numbers required.

## 10. Rollout and backward compatibility

- No feature gate. All three phases change worker-local behavior only; no
  manager/worker version handshake concerns (an old manager talking to a new
  worker simply finds exec working where it used to error).
- Two **visible behavior changes** to release-note:
  1. Linux container stop delivers SIGTERM and waits up to 10 s before the
     force-delete. Rescheduling a hung container is up to 10 s slower than
     today's instant SIGKILL — the correct trade, and the docker default
     everyone already operates under.
  2. `changeMode: signal` container jobs that used to be rejected at submit
     now deploy.
- Existing jobs, specs, and clusters need no action.

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
