# Judgement Calls: Container Runtime Exec & Signals Plan

Decisions made while writing
[the plan](../plans/container-runtime-exec-and-signals-plan.md). Ordered by
project impact, highest first; the entry number is its rank. (Planning-stage
calls — implementation will append its own as phases execute.)

## 1. The "other missing core feature" is signal delivery + graceful stop, not cgroup limits or the task-dir mount

**The call.** The request was "add exec to the container runtime — it's also
missing some other core feature," leaving the second feature to be identified.
The plan scopes it as signal delivery (and the graceful Linux teardown it
enables), with Linux cgroup limits and the task-dir mount named as explicit
non-goals.

**The alternatives.** Both are real candidates a reasonable engineer would
pick: (a) **Linux cgroup resource limits** — the bundle currently sets none,
so Linux containers run unconstrained, which is arguably *the* point of a
container orchestrator; (b) **mounting the task directory into the
container** — the one limitation that blocks three capability-table rows at
once (KV file injection, identity file delivery, file/tree artifacts).

**The trade-off.** Linux containers keep running with no resource enforcement
and no file-based delivery until their own plans land. If a user hits
memory-QoS problems before graceful-stop problems, we sequenced wrong.

**Why.** Three reasons, in order. Signal delivery is the only capability-table
row that is ❌ on *every* platform — cgroups and file delivery are
Linux-or-partial gaps, signals are a total gap. Second, it is the only
candidate that is actively *data-losing* today: Linux stop is an unconditional
SIGKILL, which turns every reschedule of a stateful workload (a headline
Legion use case) into a crash. Third, mechanism: signals ride the exact seam
this plan already builds — a runtime subcommand addressed by container name —
so exec and signals ship as one coherent, small diff (simplicity), while
cgroups live in bundle `config.json` generation and the task-dir mount needs a
volume-plumbing design; bolting either on would double the review surface for
an unrelated feature. The user-visible surface of the call: after this plan,
`stop` delivers SIGTERM and `changeMode: signal` is accepted for container
tasks, while `resources.memory` on Linux remains scheduling-only and the
capability table keeps its honest ❌s for file delivery.

## 2. Signals become a defaulted `TaskPlugin::signal()` trait method, not a container special case

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

## 3. Graceful stop ships with a fixed SIGTERM/10 s default — no jobspec knobs

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

## 4. OCI exec support is reported true without probing the runtime binary

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
