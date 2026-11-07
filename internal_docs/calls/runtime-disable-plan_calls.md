# Judgement Calls: runtime-disable-plan

Decisions made while designing
[`plans/runtime-disable-plan.md`](../plans/runtime-disable-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** These are design-time calls; implementation inserts
its own at their rank.

## 1. One switch for every runtime, enforced at start, separate from namespace policy

**Call.** `legion operator runtime disable <runtime> [--namespace ...]`
writes a replicated record that refuses every start of the runtime in scope
(placement, router and edge session starts, claims, restarts), on the
workers as well as the managers. Namespace policy keeps gating submit.

**Alternatives.** (a) A kill switch per runtime (the isolate plan's first
draft had `legion operator isolate disable`). (b) Extend namespace policy
with a cluster-level policy and enforce `runtimes` at placement too.

**Trade-off.** Two mechanisms touch "which runtimes may run here": a
standing rule (policy) and an incident switch (this).

**Why (simplicity in an incident).** Escapes happen in every isolating
runtime (runc, the VMMs, wasmtime), and the response is the same, so one
command fits all. (b) would make a temporary incident action an edit to a
permanent rule, across every policy, and would still miss edge starts unless
workers enforced policy too.

---

## 2. Running work is untouched unless asked; `--drain` suspends before it stops

**Call.** A disable refuses new starts only. `--drain` suspends units whose
task has a `suspend` block and gracefully stops the rest.

**Alternatives.** Stop everything on the runtime at once.

**Trade-off.** Work already running on a vulnerable boundary keeps running
until the operator drains it.

**Why (ease, correctness).** Whether running work must come off depends on
the advisory (some need a malicious workload already inside, some do not),
and that judgement is the operator's. When it must come off, suspending
keeps state, so enabling again loses nothing.

---

## 3. Submit warns; placement refuses

**Call.** A job submitted for a disabled runtime is accepted with a warning,
and its evaluation is blocked with `runtime_disabled` until enable.

**Alternatives.** Refuse at submit.

**Trade-off.** A job can be admitted that cannot place until the runtime is
enabled.

**Why (ease).** An author deploying during the incident does not lose their
deploy, and it places on its own when the runtime comes back.

---

## 4. Not an MCP tool

**Call.** The switch is in the CLI, the API, and the catalog, but not in the
MCP server's tools.

**Alternatives.** A write-gated MCP tool like every other mutating
operation.

**Trade-off.** An agent operating the cluster cannot flip it through MCP.

**Why (security).** A cluster-wide stop of a runtime is an incident action
with a blast radius over every tenant; it should take a human with an
`operator` token, not an agent that was given `--allow-write` for routine
work.
