# Judgement Calls: Container Runtime Exec & Signals Plan

Decisions made while writing
[the plan](../plans/container-runtime-exec-and-signals-plan.md). Ordered by
project impact, highest first; the entry number is its rank. (Planning-stage
calls — implementation will append its own as phases execute. Entries 3 and 4
record reversals of earlier planning calls, made at the product owner's
direction; the superseded designs appear as the losing alternatives.)

## 1. One plan carries all the container runtime's core gaps, with exec + signals sequenced first

**The call.** The original request was "add exec — it's also missing some
other core feature," and the plan's first cut scoped exactly that pair
(signal delivery as the identified second feature), naming cgroup limits and
the task-dir mount as non-goals. The scope was then widened twice on review:
first to include both alternatives, then to Nomad-parity CPU enforcement and
the graceful-kill jobspec knobs. The plan now covers it all as phases 1–6 of
a single document — sequenced exec → signals → stop knobs → memory/weight →
pinning → files.

**The alternatives.** (a) The original two-gap scope, with the rest as
separate future plans — smaller document, each plan one review surface;
(b) leading with cgroup limits or the task-dir mount instead of signals as
"the other core feature."

**The trade-off.** One plan now spans three mechanical seams (runtime
subcommands, bundle/cgroup generation plus a worker-side allocator, and mount
plumbing), so its phases will land as several PRs against one design doc, and
a stall in one workstream makes the whole plan read as stalled. The phase
independence requirement does the containment.

**Why.** The gaps share one user-facing story — the container runtime's
capability table, and macOS-dev/Linux-prod parity — and splitting that story
across documents would scatter the argument for how the pieces compose (file
delivery is only fully valuable *with* the signal seam; pinning is only
honest *with* the share-slice weighting). Signals still lead the sequencing
because they are the only gap that was ❌ on every platform and the only one
actively data-losing (Linux stop was an unconditional SIGKILL), and because
they ride the same mechanism exec revives. Simplicity of the story won over
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

## 3. CPU enforcement is Nomad's model: `cores` → exclusive cpuset via share/reserve slices, `cpu` (MHz) → `cpu.weight`

**The call.** On Linux the worker partitions usable CPUs into a `legion/share`
and a `legion/reserve` parent cgroup. `resources.cores` means whole CPUs
assigned by a worker-side core allocator and removed from the share slice's
cpuset (exclusive among Legion's container workloads); `resources.cpu` (MHz)
becomes proportional `cpu.weight` (via OCI `cpu.shares` = MHz, the standard
shares→weight conversion) for share-slice tasks. Two validation rules ride
along: container `cores` must be whole numbers, and a container task declares
`cores` *or* `cpu`, never both. This **reverses the plan's earlier call**
(CFS bandwidth for `cores`, MHz staying scheduling-only), at the product
owner's direction.

**The alternatives.** (a) The superseded design: `cores` as docker-style CFS
quota (`quota = cores × period`), no pinning, no weight — one spec-generation
change, no worker subsystem, fractional cores supported, MHz left as
scheduling arithmetic; (b) pinning without exclusion — write `cpu.cpus` on
the pinned container only, skip the share-slice maintenance — cheaper, but
"exclusive" would be a polite fiction; (c) weight *and* quota together
(K8s-ish requests/limits) — rejected with the same one-`resources`-block
argument as before.

**The trade-off.** The worker grows a real subsystem: CPU fingerprinting,
an assignment map persisted in worker state and rebuilt on restart, a shared
mask rewritten on every pinned start/stop, and an exhaustion failure path.
Fractional `cores` and `cores`+`cpu` specs that were previously accepted now
fail validation. Exclusivity is scoped to Legion-managed container cgroups —
exec-runtime tasks and host processes can still touch reserved cores (plan
Q4). And `cores` now means something different on Linux (exclusivity) than
on macOS (VM sizing) — the asymmetry the CFS design avoided.

**Why.** Performance, and making the jobspec mean what it says. Bandwidth
throttling admits the exact tail-latency jitter that people declare `cores`
to escape, and leaving MHz unenforced meant a saturated node degraded the
4000-MHz service and the 100-MHz cron job equally — the scheduler's
arithmetic and the kernel's behavior told different stories. Nomad's model is
the proven shape of "declared CPU is real CPU," and the hierarchical-cpuset
trick (one write to the share slice re-fences every tenant) keeps the
implementation a fraction of what it looks like. The simplicity cost is real
and was consciously paid — recorded here because the earlier entry argued the
other side.

## 4. `stopSignal` / `stopGracePeriod` ship now as task-level jobspec fields, defaults SIGTERM / 10 s, capped at admission

**The call.** Graceful teardown is declarable per task: `stopSignal`
(validated against the existing signal vocabulary; on the Linux container
path the default chain is jobspec → image `StopSignal` → SIGTERM) and
`stopGracePeriod` (duration string, default `10s`, admission-capped — lean
5 m). Honored by the container runtime on both platforms and by the exec
runtime; inert on runtimes with nothing to signal. This **reverses the
plan's earlier call** (a fixed SIGTERM/10 s constant, knobs deferred), at the
product owner's direction.

**The alternatives.** (a) The superseded design: constants only, fields
deferred until demanded — no new surface, no cap to pick, but a database
needing a 90-second checkpoint simply couldn't have it; (b) Nomad's exact
names and shape (`kill_signal`/`kill_timeout` + a per-client
`max_kill_timeout` config) — familiar, but "kill signal" describes the
mechanism, "stop" describes the intent, and a per-client cap is a knob per
node where one admission rule does; (c) runtime-`config`-level fields —
would make the semantics container-specific when they are not.

**The trade-off.** Two more fields in the universal task schema that every
current and future runtime must define semantics for (even if "inert"), a
cap value that is now API surface, and `maximal.jsonnet`/docs/validation
carrying them forever. Defaults preserve existing behavior, so the cost is
surface, not migration.

**Why.** Real workloads genuinely need non-default drains (the checkpoint
case), and the fields land at task level next to `resources` because the
*intent* — "give my process this signal and this long" — is
runtime-independent, extensibility's cheapest win. Keeping docker's
SIGTERM/10 s defaults means a job that declares nothing behaves exactly as
the fixed-constant design would have; the knobs only appear when reached
for. The admission cap keeps the operator promise the constant used to make:
no task can hold a drain hostage.

## 5. Signals become a defaulted `TaskPlugin::signal()` trait method, not a container special case

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
