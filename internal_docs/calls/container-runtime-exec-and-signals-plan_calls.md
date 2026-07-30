# Judgement Calls: Container Runtime Exec & Signals Plan

Decisions made while writing
[the plan](../plans/container-runtime-exec-and-signals-plan.md). Ordered by
project impact, highest first; the entry number is its rank. (Planning-stage
calls — implementation will append its own as phases execute.)

## 1. One plan carries all four gaps, with exec + signals sequenced ahead of limits + file delivery

**The call.** The original request was "add exec — it's also missing some
other core feature," and the plan's first cut scoped exactly that pair
(signal delivery as the identified second feature), naming cgroup limits and
the task-dir mount as non-goals. On review the request was widened to include
both alternatives, so the plan now covers all four as phases 1–5 of a single
document — sequenced exec → signals → graceful stop → limits → files.

**The alternatives.** (a) The original two-gap scope, with limits and file
delivery as separate future plans — smaller document, each plan one review
surface; (b) leading with cgroup limits or the task-dir mount instead of
signals as "the other core feature": limits are arguably *the* point of a
container orchestrator, and the mount clears three capability rows at once.

**The trade-off.** One plan now spans two unrelated mechanical seams (runtime
subcommands vs. bundle/launch generation), so its phases will land as several
PRs against one design doc, and a stall in one workstream makes the whole
plan read as stalled. The phase independence requirement does the containment.

**Why.** The four gaps share one user-facing story — the container runtime's
capability table, and macOS-dev/Linux-prod parity — and splitting that story
across three documents would scatter the argument for how the pieces compose
(file delivery is only fully valuable *with* the signal seam: rotation
without restart). Signals still lead the sequencing because they are the only
gap that is ❌ on every platform and the only one that is actively
data-losing today (Linux stop is an unconditional SIGKILL), and because they
ride the same mechanism exec revives. Simplicity of the story won over
smallness of the document.

## 2. The task dir mounts at one canonical path, and container file destinations must resolve under it

**The call.** Every container task gets its host task directory bind-mounted
read-write at `/legion/task` (a fixed constant), worker env is translated to
in-container values (`LEGION_TASK_DIR=/legion/task`, `LEGION_ARTIFACT_*`
prefix-swapped), and file-delivery destinations (`kv_injections.files[].path`,
identity `file.path`) for container tasks are admission-rejected unless they
resolve under `/legion/task`.

**The alternatives.** (a) Mount the task dir at its *host* path inside the
container, so existing absolute paths and untranslated env stay valid;
(b) honor arbitrary destinations (like the identity examples'
`/var/run/legion/...`) via per-file or per-parent-directory bind mounts;
(c) Nomad's three-mount split (`/alloc`, `/local`, `/secrets`) from day one.

**The trade-off.** A jobspec using file delivery is no longer verbatim-portable
between the `exec` and `container` runtimes (exec keeps host-absolute paths),
and jobs that want a token at an FHS-conventional path like `/var/run/...`
must symlink from their entrypoint or use env delivery. `/legion/task` is
also effectively permanent once jobs hardcode it. Evidence to revisit: real
jobs repeatedly fighting the path rule.

**Why.** Simplicity and honesty of the contract. Mirroring host paths (a)
leaks worker filesystem layout into every container and turns an operational
detail (where a worker keeps its allocs) into user-visible API. Per-file
mounts (b) silently break refresh — atomic-rename replacement changes the
inode and the container keeps the stale file (the known docker pitfall);
directory-level mounting is what makes KV re-render and identity rotation
work with zero projection machinery (the K8s-style symlink apparatus is the
cost we're refusing). The three-way split (c) triples the mount surface for a
system that today has one task dir — `/legion/task` leaves the namespace room
(`/legion/secrets`) if that ever changes. The env translation follows the
exact precedent volumes already set (in-task destinations for isolating
runtimes), so this extends an existing convention rather than inventing one.

## 3. Signals become a defaulted `TaskPlugin::signal()` trait method, not a container special case

**The call.** `WorkerTaskManager::signal_allocation` routes through a new
trait method whose default is today's `kill(2)`-the-tracked-PID behavior;
the container plugin overrides it with `container kill` / `<runtime> kill`.

**The alternative.** Branch inside the task manager: `if runtime ==
"container" { shell out } else { kill(pid) }`. Smaller diff, no ABI change, no
dylib-rebuild consequence.

**The trade-off.** A trait addition touches the dylib ABI (source-compatible
via the default, but third-party plugins rebuild under the registry's version
gate), and every future plugin author now sees a method they may not need.

**Why.** Extensibility — the exact shape `supports_signal()` was already
promising: the boolean is a trait capability, so the delivery mechanism
belongs on the trait too, not in worker core. The sandbox runtime's future
guest-agent signal path implements the same method instead of growing a second
special case, and worker core stays runtime-agnostic (the same argument that
put `exec()` on the trait in the exec plan). Simplicity of the *system* beat
smallness of the *diff*.

## 4. `cores` maps to CFS bandwidth (docker's `--cpus` semantics), not cpuset pinning; `cpu` (MHz) stays scheduling-only

**The call.** On Linux, `resources.cores: N` becomes
`cpu.quota = N × 100000 / period 100000` in the bundle spec — throttling to
"N CPUs' worth," fractional values included — and `memory` becomes a hard
limit with `swap` pinned equal to it. `resources.cpu` (MHz) maps to nothing,
exactly as on macOS.

**The alternatives.** (a) Cpuset reservation for `cores` (Nomad's model):
exclusive cores, no throttling-induced tail latency; (b) mapping MHz to
`cpu.weight`/shares (also Nomad) so contention degrades proportionally;
(c) docker's swap default (2×limit) instead of swap = limit.

**The trade-off.** Bandwidth throttling admits scheduling jitter that pinned
cores would prevent (a quota-exhausted task waits for the next period), and
an MHz-declaring job still gets no kernel enforcement at all. Swap = limit is
stricter than docker; a workload relying on swap headroom OOMs where docker
would page.

**Why.** Consistency and simplicity over a subsystem. `cores` already *means*
"N CPUs' worth" on macOS (`--cpus` sizes the guest); CFS quota is the same
meaning enforced the same way docker enforces it, while cpuset would require
the worker to own a node-wide core-assignment map the scheduler doesn't model
anywhere. Per-platform divergence in what `cpu` means (weight on Linux,
nothing on macOS) would cost more explanation than proportional weight buys —
kept symmetric, revisit with contention evidence (plan Q4). Swap = limit is
the K8s semantic: a limit the kernel can page around is not a limit the
scheduler can trust.

## 5. Graceful stop ships with a fixed SIGTERM/10 s default — no jobspec knobs

**The call.** Linux teardown becomes TERM → wait ≤ 10 s (a constant) →
`delete --force`. No `stopSignal`/`stopGracePeriod` jobspec fields, no env
override.

**The alternative.** Ship Nomad-style `kill_signal`/`kill_timeout` task fields
now, while the teardown code is open — it is where they would land anyway.

**The trade-off.** A workload needing a longer drain (a database checkpoint,
a long-poll server) cannot express it and gets force-killed at 10 s. Evidence
that would revisit this: the first real job that documents needing >10 s.

**Why.** Simplicity/ease — Legion's headline goal is fewest knobs that could
possibly work, and docker's 10-second default is the behavior every operator
already assumes. New jobspec surface is a per-runtime contract (`exec` and
`sandbox` would need to honor it too), deserves its own small plan, and is
purely additive later; shipping the safe default first fixes the data-loss bug
without committing to field names under time pressure.

## 6. OCI exec support is reported true without probing the runtime binary

**The call.** `ExecSupport{exec: true, pty: true}` for the Linux OCI path is
asserted from runtime detection alone; a hypothetical OCI runtime lacking
`exec` fails at session open with the runtime's own error, not at
`exec_support()` time.

**The alternative.** Probe once per process (e.g. `<runtime> exec --help`)
and cache, keeping the "fail fast and honestly" property of the exec plan's S4
principle for exotic runtimes.

**The trade-off.** Against a runtime without `exec`, the CLI's clean
"runtime does not support exec" error degrades to a session that opens and
immediately surfaces the binary's usage error.

**Why.** Performance and simplicity over a hypothetical: youki, runc, and
crun — the three runtimes Legion documents — all implement `exec`, the
failure remains clearly attributed when it happens, and a startup probe adds a
subprocess launch on every worker for a case no supported configuration hits.
Kept as open question Q1 in the plan so implementation can flip if the e2e
matrix says otherwise.
