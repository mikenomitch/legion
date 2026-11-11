# Plan: The Isolate Runtime: Dense Multi-Tenant Isolates That Burst to a VM

> **Judgement calls:** [isolate-runtime-plan_calls.md](../calls/isolate-runtime-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-28; burst redesigned the same
day after review: each run happens in one place, chosen before it starts; a
session's address is always its isolate; the VM is a declared sibling task
reached as a child session; isolates use `memory`/`memoryMax` like every
runtime. See calls entries 6, 12, 33 and 24. On 2026-11-07, everything that
was not isolate-specific moved into shared plans, calls entry 25. Phases 1
to 5 built 2026-11-11, then hardened the same day after a security review,
an end-to-end test round, and control-plane fixes.) Public page:
[docs/task-runtime-isolate.md](../../docs/task-runtime-isolate.md). Phase 0
findings: [isolate-runtime-phase0-findings.md](../isolate-runtime-phase0-findings.md).

What is built, by phase (each difference from the text below is a calls
entry, and the affected section carries a dated **As built** note):

- **Phase 0: container measurements only.** A spike on a 4-vCPU Firecracker
  guest measured start, idle memory, slots per layout, epoch and fuel
  overhead, fiber against stackless waits, binary size, and compile times.
  The AWS run (nested and metal, 16 vCPU / 64 GiB, 5-level paging) has not
  happened; §4 and §10 keep their targets, with the container numbers beside
  them.
- **Phases 1 to 5: built, with deviations.** `legion-isolate` (`host`,
  `compile`, `engine-info`); one host per cordon replica per node with
  shards, the pooling allocator, epoch preemption, limits, admission, and
  self-confinement (eight layers after the hardening round, below); request, tenant, and session
  lifetimes; the `js` and `python` images; session runs on isolate tasks
  with `tier`, the pre-flight check, the memo, `nextTier`, and
  `--fallback vm`; HTTP ingress and egress through the envelope's
  datapath; child VMs through `legion:isolate/vm`; hibernation and
  page-out; guest output in the task's log stream; `legion isolate stats` and `get`; the runtime-disable
  reaction; session-isolate resize. The deviations: fibers for every guest
  and no WASI 0.3 world (calls 7); hibernation keeps the `state` map, not
  memory (calls 9); paging is kernel reclaim on the host's cgroup (calls
  31); Legion's own in-memory `wasi:filesystem` (calls 32); a gRPC host
  protocol with an `Origin` per request instead of PROXY v2 (calls 10);
  a supervisor instead of per-session claims (calls 5); the envelope
  allocation carries the datapath (calls 26); `limits.output` truncates
  (calls 60); reuse needs the `runner` export (calls 40); new `args`,
  `tenant`, `session_id` fields (calls 39); `entry` is not pre-initialized
  (calls 41); a JS entry is a script (calls 49).
- **The hardening round (2026-11-11).** A security review of the first
  build, then fixes: the host and the compiler apply eight confinement
  layers (a PID-namespace fork with a confined supervisor, an empty
  read-only mount root, Landlock scopes where the kernel has them, and
  seccomp argument filters added; calls 15, 16, 19, 21), and the compiler
  honours `--confine`, `enforce` by default (calls 20); `config.entry` is
  read without following symlinks (calls 17); a host may act only as
  isolates the worker issued it (calls 28); bounds on what a guest can make
  its host hold and a host can make its worker hold (calls 44, 45);
  VM-tier `args` only after an entry-ending `run.command` (calls 29);
  tenant keys documented as routing keys, and a session's tenant wins
  (calls 27); prefixed cordon names (calls 36); NOT_FOUND for every
  session id the caller cannot read (calls 35); guest-log limits,
  targeted hibernation, and the host cgroup following its envelopes.
  **Verified end to end** in a 4-vCPU container, through a manager, a
  worker, and real confined hosts (debug builds, `best-effort`, all eight
  layers applied): runs, cordons, limits, sessions and hibernation, a host
  killed and restarted, guest logs, HTTP in through the supervisor's
  ingress socket and out through the real egress datapath with an injected
  credential, and both language images. The VM tier and child VMs ran with
  an `exec` session task standing in for the sandbox (calls 65): no real
  VM has run.
- **Phase 6: mostly not built.** The `mpk` and `small` slot layouts exist as
  worker config (`isolate.layout`) and were measured for slot counts only;
  nothing else in Phase 6 is built (`cordonBoundary = "vm"` is refused at
  worker start).
- **Phases 7 and 8: untouched.**

Deferred, deliberately not built in this round:

- the AWS Phase 0 run and the density campaign (§12);
- memory-image hibernation (no engine API; calls 9) and the stackless
  WASI 0.3 world (the engine keeps a stack per store; calls 7);
- `suspend.durability = "store"` for session isolates (calls 58), so no
  durable named isolate (calls 34); host-held connections and WebSockets
  across hibernation (calls 30); gateway wake for session isolates;
- connection handoff from an isolate (calls 33); the VM task sharing a
  `connect` block, which needs a gated datapath for sandbox sessions
  (calls 64); prepared VMs (sandbox-scaling Phase 8);
- stage-time pre-initialization of `entry` (calls 41); dynamic Wasm
  modules sent as runs; ES modules in `js` (calls 49); Python packages
  beyond the standard library and curated images (calls 47); `js-small`
  (QuickJS, measured 5× smaller idle, calls 46); streaming HTTP bodies
  (calls 52);
- `cordonBoundary = "vm"`, MPK as a soaked default, SELinux `execmem`,
  perf-counter quarantine, Spectre-mode knobs, 5-level paging (Phase 6);
- replica rebalancing and shrinking, shard rebalancing (calls 38, 53);
  a `cpu.weight` on the host cgroup (calls 31; its `memory.max` now
  follows envelope resizes);
- per-job metric labels (calls 70); invocation-level idempotency in the
  host (calls 68);
- fuzz targets (§12); KVM runs of the VM-tier tests; aarch64; shipping
  `legion-isolate` and the images in a release, and reproducible image
  builds (calls 74);
- dropping the host's kernel uid (calls 18); denying anonymous executable
  memory, which needs an LSM; tracking in-flight invocations per replica
  rather than per cordon (calls 28); a cgroup per compile (calls 20);
- a release-build end-to-end measurement against the Phase 1 target of
  under 2 ms (the debug-build measurement missed it, §15).

This plan adds a second, much lighter tier to Legion's sandboxing. Code runs
first as an **isolate**: a WebAssembly instance inside a shared,
multi-tenant host process, started in microseconds, holding no thread while
it waits on I/O, and costing tens to hundreds of kilobytes while it idles.
When code needs a real Linux machine (a subprocess, a native package, a
writable filesystem, more memory or CPU than an isolate may have), it
**bursts** to a microVM from the existing [`sandbox`](../../docs/task-runtime-sandbox.md)
runtime, and only then: either the run is sent to a VM before it starts,
or an isolate starts a VM it owns and drives.

The plan spends most of its length on the isolate tier: the engine, the
process and thread model, memory, the start path, how an isolate gives up
its resources while it awaits, the host API, and the security model, each
with the options considered and scored (§6). The VM tier reuses what
exists, and the plan says only what burst needs from it (§6.12). The
orchestrator is covered in one section (§8), because isolates ride
[session mode](../../docs/session-mode.md) rather than adding a placement
model.

Deliberately deferred, and named in §4: a V8 engine (Phase 7, behind a
measured trigger), moving a running isolate's continuation into a VM
(Phase 8, research), connection handoff from an isolate to its VM
(child-sessions plan, Phase 3), envelopes shared by many small jobs,
isolates in the managers' session index, wasm threads, and GPU access from
either tier. Dropped on review: Legion re-running a stopped run in a VM on
its own (calls entry 6).

Companion plans: what this runtime needs that is not isolate-specific is
planned as shared primitives, and §6.14 shows how they compose:

- [child-sessions-plan.md](child-sessions-plan.md): a unit that owns
  sessions or instances of a sibling task, reached through it;
- [resize-plan.md](resize-plan.md): in-place vertical scaling for any unit;
- [session-runs-plan.md](session-runs-plan.md): one-shot runs on any session
  task, which replace this plan's first `RunIsolate`;
- [derived-content-plan.md](derived-content-plan.md): one node cache for
  everything Legion builds (compiled modules, pre-initialized images);
- [runtime-disable-plan.md](runtime-disable-plan.md): the cluster-wide kill
  switch, for any runtime;
- [legion-apps-plan.md](legion-apps-plan.md) §6.4: the workload channel the
  isolate host uses to reach the worker.

Relationship to other plans: this executes, and replaces the shape of,
[sandbox-scaling-plan.md](sandbox-scaling-plan.md) §6.10's Phase 9 ("Tier
2, many sessions per VM", a seam with no runtime behind it), and it is the
plan the open TODO item "A **WASM** runtime" asks for. It keeps
[mesh-interfaces-plan_calls.md entry 22](../calls/mesh-interfaces-plan_calls.md#22-the-plugin-engine-is-wasmi-an-interpreter-not-wasmtime)
(network plugins stay on wasmi) and explains in its own calls entry 3 why
the isolate host is the case that entry said would overturn it, and why the
worker binary still does not grow.

---

## The plan in one page

- **What runs.** A WebAssembly component, compiled ahead of time. JS and
  Python run as *images*: their interpreters compiled to Wasm and
  pre-initialized, so a JS isolate starts from a snapshot of a booted
  engine with the user's script already loaded (§6.2, §6.5).
- **Where it runs.** In `legion-isolate`, a separate binary the worker
  starts as one host process per **cordon** (a trust partition, by default
  one per namespace). One host holds thousands to a hundred thousand
  isolates from many tenants, spread over one shard per core, each shard a
  single-threaded event loop (§6.3).
- **How it starts.** A slot from wasmtime's pooling allocator, the
  module's memory image mapped copy-on-write, imports resolved ahead of
  time, no compile and no file I/O: tens of microseconds (§6.5).
- **How it waits.** The host drives each guest's event loop. A guest runs
  a turn to completion, returns to the host when it awaits, and holds no
  stack and no thread until its I/O completes. Long waits page its memory
  out; longer ones hibernate it to disk and free its slot (§6.6, §6.7).
- **What it can touch.** A small host API: an HTTP handler, outbound HTTP
  through the node's egress datapath (policy, upstreams, and credential
  injection unchanged), a scratch filesystem in memory, coarse clocks,
  logs, tenant-scoped state, and the VM handle. The host process itself
  has no network and no filesystem beyond its compiled-module cache
  (§6.9, §6.10).
- **How it bursts.** Each run happens in exactly one place, chosen before
  it starts: the caller, or `tier = "auto"`'s pre-flight check and memory
  of past runs, picks the isolate or a single-use VM of a sibling `sandbox`
  task. An isolate that wants a machine starts one as a **child session**
  and drives it (exec, files, HTTP), while requests keep addressing the
  isolate. Legion never moves a running invocation or runs it twice on its
  own (§6.12).
- **How accounting follows.** Nothing changes size at a burst: a second
  unit appears in its own task's envelope. Growing a unit is a resize
  (§6.11, §6.14).
- **What the control plane sees.** Envelopes, as for sessions, and
  per-task aggregate counters on the heartbeat. No raft entry, no
  evaluation, and no index row per isolate or per invocation (§8).

---

## 1. Problem statement

Legion's lightest sandbox today is a microVM. It is a good one: about
0.18 to 0.23 s from create to a guest that takes exec, 150 to 200 boots a
second on a 96-vCPU node, and nothing written to raft for a session
([sandboxes at scale](../../docs/sandboxes-at-scale.md#measured-on-aws)).
For a whole class of workloads it is still two to four orders of magnitude
too heavy.

1. **Running a snippet costs a machine.** An agent platform that executes
   model-written code ("parse this CSV", "call this API and reshape the
   JSON", "evaluate this expression") runs a few milliseconds of
   interpreter per call. Today each call boots a guest kernel and an init,
   reserves at least 64 MiB of guest memory (Apple `container`'s floor is
   200 MiB), and pays a 140 ms `ready` stage before the first line runs.
   The platform either pays that per call or keeps warm VMs per user and
   pays for the idle memory.
2. **Density stops at the guest kernel.** A 128 MiB sandbox is 8 per GiB
   before any sharing. The projections top out around 120,000 running
   sandboxes on 100 bare-metal 192 GiB nodes. A platform with millions of
   mostly idle users, each wanting a live, addressable execution context,
   cannot hold them resident as VMs, and suspending them to disk costs a
   restore of hundreds of milliseconds on wake.
3. **Waiting is not free.** A VM blocked on an LLM API call for 20
   seconds holds its whole memory and its vCPU thread. Agent workloads are
   dominated by exactly that wait.
4. **There is no in-between.** The TODO list's "A **WASM** runtime" item
   names the embedding layer (`src/wasm/`) and stops at the open
   questions: an interpreter or a compiler, the CPU budget, filesystem,
   networking. Sandbox-scaling Phase 9 reserves a seam for "many sessions
   per VM" and ships no runtime behind it. A job author who wants
   something lighter than a VM today has `exec`, which is not a sandbox.

What users want is narrow to say: *run untrusted code in under a
millisecond, keep hundreds of thousands of them alive per host, let them
wait for free, and give them a real machine the moment they actually need
one, without changing how they call it.*

## 2. Prior art

Researched 2026-10-28. Vendor numbers are each vendor's own, under its own
hardware and definition of "started"; read them as orders of magnitude.

### Isolate platforms in production

- **Cloudflare Workers** ([2018](https://blog.cloudflare.com/cloud-computing-without-containers/),
  [security model](https://developers.cloudflare.com/workers/reference/security-model/),
  [Dynamic Workers, 2026-03-24](https://blog.cloudflare.com/dynamic-workers/)).
  V8 isolates, "hundreds or thousands" per process (Cloudflare now says
  "tens of thousands of tenants" share one process), about 5 ms and a few
  MB each, 128 MB memory per isolate, CPU enforced by a CPU-time signal and
  `TerminateExecution`, LRU eviction under a global memory cap. One isolate
  serves many concurrent requests; a thread runs one isolate at a time and
  releases it between turns. The runtime process sits inside namespaces
  and seccomp with no filesystem and Unix sockets only, egress through a
  proxy, and code fetched by a supervisor; tenants are split by trust into
  **cordons** ("a free-plan customer will not be scheduled in the same
  process as an enterprise customer"); suspicious Workers are moved into
  their own process by performance-counter detection; and since September
  2025 every isolate's heap has its own Intel MPK key
  ([2025-09-25](https://blog.cloudflare.com/safe-in-the-sandbox-security-hardening-for-cloudflare-workers/)).
  Dynamic Workers (`load()`) create an isolate per call for AI-generated
  code, with no network unless granted. In August 2026 researchers leaked a
  JWT across tenants in production at up to 12 bit/s with a remote timer
  over a WebSocket, defeating frozen local clocks; the process-isolation
  detector missed it because it judged only finished invocations
  ([2026-08-19](https://blog.cloudflare.com/revisiting-spectre-attacks-on-workers)).
  - **Take:** cordons by trust; the confined runtime process with no
    filesystem, no network, and egress through a proxy; an LRU cap with
    eviction; capabilities granted, not ambient; detection on *running*
    work, not finished work.
  - **Reject:** V8 as the engine (§6.1), and one long-lived isolate per
    script as the only lifetime.
- **Fastly Compute** ([Wasmtime 1.0](https://bytecodealliance.org/articles/wasmtime-1-0-fast-safe-and-production-ready),
  [JS on Compute](https://www.fastly.com/blog/compute-edge-now-supports-javascript)).
  Wasmtime with the pooling allocator and copy-on-write memory images: a
  fresh instance per request in about 5 µs (SpiderMonkey.wasm went from 2
  ms to 5 µs). JS runs as SpiderMonkey compiled to Wasm (StarlingMonkey),
  which Fastly calls an outer boundary around the JS engine; reuse is
  opt-in, because it "results in a larger blast radius". weval AOT gives a
  2.77× geomean over the interpreter
  ([2024-11-11](https://www.fastly.com/blog/speeding-up-javascript-on-compute)).
  - **Take:** the whole engine stack: wasmtime, pooling, CoW, instance per
    request by default, JS inside Wasm.
- **Shopify Functions** ([Javy](https://shopify.engineering/javascript-in-webassembly-for-shopify-functions)).
  QuickJS compiled to Wasm, about 3× slower than equivalent Rust Wasm,
  with hard limits (256 kB binary, 10 MB linear memory, 11M instructions
  for a cart of up to 200 lines). Chosen because a JIT is impossible inside
  Wasm and QuickJS is plain C.
  - **Take:** QuickJS as the small JS image candidate; instruction and
    memory limits as first-class, published numbers.
- **Fermyon Spin / Akamai** ([SpinKube](https://www.akamai.com/blog/developers/2025/jul/spinkube-k3s)).
  wasmtime components, "0.5 ms cold start" and "thousands of apps per
  node" (vendor claims, no method).
  - **Take:** WASI components as the packaging format.
- **Deno Deploy** ([2023](https://deno.com/blog/subhosting-security-run-untrusted-code),
  [2024](https://deno.com/blog/build-secure-performant-cloud-platform)).
  Each isolate in its own process and namespace, with seccomp and cgroups,
  and runners are "pools of V8 isolates in a virtual machine".
  - **Take:** the VM around a pool of isolates, as Legion's optional
    `cordonBoundary = "vm"`.
- **Azure Front Door edge actions** (public preview 2026-07-14) run
  JavaScript in Hyperlight micro-VMs with 16 KB of code and 10 ms of
  execution ([docs](https://learn.microsoft.com/azure/frontdoor/edge-actions)).
  - **Note:** the first production use of a hardware-isolated middle tier.

### Kubernetes and Nomad

- **Kubernetes** runs Wasm through containerd shims
  ([runwasi](https://github.com/containerd/runwasi), SpinKube) selected by
  a `RuntimeClass`. Each app is still a pod with its own shim process,
  sandbox, and API object, so density is the pod's density and a cold
  start includes the kubelet.
  - **Reject:** a unit per app in the control plane. Legion's isolates
    are not API objects at all (§8).
- **Nomad** has no first-class Wasm driver; community task drivers run a
  Wasm runtime per task, a process per allocation, each a raft object.
  - **Reject:** the same, for the same reason.

### Research systems

- **Faasm** ([ATC '20](https://arxiv.org/abs/2002.09344)). Wasm
  "Faaslets" with snapshot-restored "Proto-Faaslets": 0.5 ms and 90 KB
  each, more than 100,000 per host, against Docker's 2.8 s and ~8,000.
  A snapshotted CPython restored in 0.9 ms.
  - **Take:** pre-initialized snapshots as the start path, and the
    density they buy.
- **Sledge** ([Middleware '20](https://faculty.cs.gwu.edu/gparmer/publications/middleware20sledge.pdf))
  and **Lucet** ([Fastly 2019](https://www.fastly.com/blog/lucet-performance-and-lifecycle)):
  30 µs starts; Lucet's 23 µs teardown and 0.5 µs context switch.
  - **Take:** the switch-cost target.
- **Dandelion** ([SOSP '25](https://arxiv.org/html/2505.01603v2)). Pure
  compute functions that make no syscalls, with I/O only through trusted
  communication functions; sandbox creation from 89 µs (CHERI) to 889 µs
  (KVM); 96% less committed memory than Firecracker with Knative on the
  Azure Functions trace. It also measured Hyperlight Wasm at 9.1 ms cold.
  - **Take:** compute isolated from I/O, with I/O done by a trusted party
    (Legion's worker).
- **ColorGuard** ([ASPLOS '25](https://zyedidia.github.io/papers/seguecg.pdf)).
  MPK striping in wasmtime's pooling allocator: 14,582 → 218,716 slots of
  408 MB per process, switch cost 30 → 52 ns, up to 29% more throughput
  than 15 processes. Upstream, experimental.
  - **Take:** in Phase 6 (§6.4).
- **weval** ([PLDI '25](https://cfallin.org/pubs/pldi2025_weval.pdf)).
  Partial evaluation of an interpreter into compiled code: 2.17× on Octane
  over SpiderMonkey's interpreter with inline caches.
  - **Take:** for the `js` image.
- **SEUSS, Catalyzer, SOCK, Junction, TrEnv.** Unikernel snapshots
  (> 50,000 cached instances), checkpoint-restore with `sfork` (< 1 ms),
  zygotes with import caches (45× over Docker), kernel bypass (19 to 62×
  denser), and memory templates (< 10 ms containers).
  - **Take:** the consensus that starts come from restoring prepared
    state, never from booting faster.

### VM fast start, and how others escalate

- **Nobody moves a running isolate's memory into a VM.** Every production
  escalation is explicit, and state crosses tiers through a filesystem or
  object store. Cloudflare's Project Think climbs Workspace → Dynamic
  Worker → npm → browser → Sandbox as the agent "escalates as needed",
  with the Sandbox's files "synced bidirectionally with the Workspace"
  ([2026-04-15](https://blog.cloudflare.com/project-think/)). Containers
  start when a Durable Object calls `ctx.container.start()`. Modal picks
  gVisor or a VM per sandbox (`runtime = "vm"`); Vercel and Deno start a
  sandbox from an SDK call. Cloudflare's Sandbox SDK and Deno Sandbox
  both keep credentials outside the sandbox and add them on egress.
  - **Take:** an explicit, application-visible escalation (`vm.acquire`),
    files as the handoff, credentials added on egress, and the owner as
    the address (Cloudflare's Durable Object in front of its container).
    Legion adds choosing the tier before a run from a pre-flight check
    (§6.12).
  - **Reject (defer):** continuation migration, which nobody ships.
- **Prepared VMs, not faster boots.** Cloudflare cut median container
  start from 4.0 s to 648 ms by restoring "a prepared virtual machine that
  isn't yet assigned" ([2026-09-30](https://blog.cloudflare.com/faster-agent-sandboxes/)).
  Unikraft holds a million scaled-to-zero VMs on one 48-core box and
  resumes in under 10 ms from compressed differential snapshots on NVMe
  ([2026](https://unikraft.com/blog/1m-vms-single-box)). Firecracker
  restores in as little as 4 ms ([Brooker](https://brooker.co.za/blog/2022/11/29/snapstart/)),
  and the Zeroboot prototype forks a Firecracker snapshot into a new KVM
  VM in 0.79 ms p50 at about 265 KB private each (one vCPU, no network,
  [repo](https://github.com/zerobootdev/zeroboot)). Lambda SnapStart
  records and preloads each function's working set.
  - **Take:** a per-node pool of prepared, unassigned, single-use VMs restored
    from a base snapshot, with the guest agent's existing resume handler
    refreshing identity, RNG, and clock on assignment (§6.12).
- **Hyperlight** ([2024](https://opensource.microsoft.com/blog/2024/11/07/introducing-hyperlight-virtual-machine-based-security-for-functions-at-scale/),
  [repo](https://github.com/hyperlight-dev/hyperlight)). Micro-VMs of one
  vCPU and a memory slice with no devices: 1 to 2 ms create (vendor),
  9.1 ms measured for Hyperlight Wasm by Dandelion, 45 to 93 µs snapshot
  loads, embeddable from Rust, pre-1.0, Hyperlight Wasm "experimental".
  - **Watch:** a candidate cordon boundary (§14 Q6), not the isolate tier.

### What the prior art changes in this plan

1. Isolates are not the only wall anywhere they run in production, so the
   host process is confined and holds no secrets (§6.10).
2. The engine Legion should embed is the one Fastly runs, not the one
   Cloudflare runs: Fastly's stack fits a Rust codebase, starts three
   orders of magnitude faster, and keeps JS engine bugs inside Wasm (§6.1).
3. Escalation is explicit and file-based everywhere, and the light tier
   stays the address. Legion keeps both, and adds only a choice of tier
   made before a run starts (§6.12).
4. A warm VM tier comes from prepared VMs, which Legion's snapshots and
   guest agent already nearly provide (§6.12).

## 3. Jobs to be done, by persona

**Agent platform builder** (the primary persona: runs a product that
executes code on behalf of its own users or of a model)
- When a model writes a snippet to answer a question, I want it to run
  somewhere safe in about the time the model took to emit the last token,
  so tool calls feel instant and I do not pay for a machine per call.
- When one of my millions of users comes back after an hour, I want their
  execution context (variables, open connections, the half-finished task)
  to still be there without my having paid to keep it in memory, so I can
  offer "always on" without "always billed".
- When a snippet turns out to need `pip install`, a compiler, or a binary,
  I want Legion to send code like it to a real Linux machine, with the
  network rules and secrets I chose, and to tell me plainly when it guessed
  wrong, so I do not have to predict the tier for every snippet and my code
  never runs twice without my asking.
- When my user's session moves part of its work onto a machine, I want my
  clients to keep using the one address they already have, so nothing
  upstream of me changes.
- When model-written code is hostile, I want it unable to read another
  user's data or my API keys, so one prompt injection is not a breach.

**Job author / app developer**
- When I write a small HTTP handler or webhook, I want to deploy it and
  have it answer from every node within a millisecond of a cold start, so
  I can scale it to zero without a cold-start tax.
- When my handler calls a paid API, I want the key injected on the way
  out, as it is for my containers today, so the key is never in my code
  or my isolate's memory.
- When my code fails, I want to know whether it hit a limit, called
  something isolates cannot do, or crashed, in one line, so I know whether
  to fix code, raise a limit, or send it to a VM.

**Cluster operator**
- When I give the cluster to isolates, I want to know how many fit on a
  node and see the host processes as ordinary, bounded things I can drain,
  so isolates do not become a second, invisible fleet.
- When an engine CVE is announced, I want to turn the isolate tier off
  (runs go to VMs or fail cleanly) in one command and turn it back on
  after upgrading, so the response is minutes, not a migration.

**Security / platform admin**
- When tenants of different trust share a node, I want to choose which of
  them may share a process, so a free-tier tenant's code never runs beside
  a tenant whose isolates handle credentials.
- When I approve the isolate tier, I want the host process confined so
  that escaping the engine still leaves an attacker with no network, no
  filesystem, and no other cordon, so the engine is not the only wall.

**Autonomous agent** (driving Legion directly)
- When I need to run code as a step of a task, I want one call that
  returns stdout, stderr, the result, which tier ran it and why, and which
  tier to use next time, as structured data, so I can act on the outcome without parsing text.
- When I retry after a timeout, I want the retry not to run my code twice,
  so non-idempotent snippets are safe to retry.

*Not in scope:* the **on-call responder** gets no new surface beyond
existing allocation, log, and metrics views (the cordon host is an
allocation-like process with logs and metrics; isolates are aggregated,
not individually inspectable in the UI, in this plan).

### User scenarios

1. **The code-interpreter tool.** An agent platform submits one job,
   `code-runner`, with an isolate task on the `python` image and a sibling
   `machine` task on `python:3.13-slim`. Its backend calls `RunSession`
   with each snippet and `tier = "auto"`. 99% of calls finish in the
   isolate in 2 to 20 ms. A snippet that imports `torch` is sent to a fresh
   `machine` VM by the pre-flight check before it starts. One that shells
   out to `ffmpeg` is stopped in the isolate (`stopped.reason =
   "unsupported"`) with `nextTier = "vm"`; the backend reruns it with
   `tier = "vm"` and keeps the hint for that code hash, so its next run on
   any node goes straight to a VM. Every reply says where the code ran,
   why, and whether it ran twice.
2. **A million resident agents.** Each of the platform's users has an
   isolate session (`lifetime = "session"`) holding their agent loop's
   state. While the loop awaits the model, the isolate holds no stack and
   no thread. After 10 seconds idle it is paged out; after 2 minutes it is
   hibernated to the node's disk and its slot is freed. A message for the
   user rehydrates it in about a millisecond on the node its id names.
   When the agent needs to build something, its isolate calls
   `vm.acquire()` and gets a child VM on the same node; requests still go
   to the user's isolate id, which forwards what needs the machine.
3. **Scale-to-zero webhooks.** A job author ships a Rust component as an
   HTTP handler on every node of a pool. Each request gets a fresh
   instance (`lifetime = "request"`), so no state leaks between requests,
   and the first request after an idle week costs the same as the
   millionth.

## 4. Goals and non-goals

### Goals

- **G1: Start in microseconds.** A precompiled component instantiates in
  ≤ 20 µs p50 and ≤ 100 µs p99 on the host it runs on. A JS image with
  the author's script pre-initialized is running in ≤ 300 µs p50. A
  dynamic JS snippet of 10 KB is running in ≤ 1 ms p50. A Python image
  with a small snippet is running in ≤ 5 ms p50.
- **G2: Maximum density.** At least 4,000 small (Rust/Go/C) isolates and
  500 JS isolates resident per GiB of host memory while idle, and at least
  10^5 resident isolates per 64 GiB host, with ≥ 10^6 hibernated isolates
  per node bounded by disk.
- **G3: Waiting is free.** An isolate awaiting I/O holds no OS thread and,
  for guests on the stackless async ABI, no stack. Switching between
  isolates on a shard costs ≤ 1 µs.
- **G4: Many tenants per process,** partitioned by an operator-chosen
  trust boundary, with per-tenant CPU fairness and hard per-isolate limits.
- **G5: Burst only when needed, decided up front.** Each run happens in
  one place, chosen before it starts. A VM run or a child VM reaches a
  guest in the sandbox's boot time (≤ 250 ms p50), in ≤ 50 ms from a warm
  snapshot, and in ≤ 10 ms p50 on metal from a prepared VM. Code that never
  needs a VM never touches one.
- **G6: Defense in depth.** An engine escape lands in a process with no
  network, no writable filesystem, no other cordon's memory, and no way to
  generate new code.
- **G7: Nothing per isolate on raft.** Isolate starts, invocations, and
  hibernations write nothing to the replicated store and nothing to the
  managers' session index.
- **G8: Agent-ready.** One structured call to run code, with idempotency,
  a dry-run (`validate`), an MCP tool behind `--allow-write`, and
  catalog entries.
- **G9: Built from shared primitives.** Burst adds no burst-only
  machinery: it is child sessions, envelopes, and the front-door rule, and
  growing a unit is resize, all usable by any runtime.

All numbers above are targets that Phase 0 replaces with measurements. A
target Phase 0 shows to be wrong is rewritten here with a calls entry.

**As built (2026-11-11): container measurements beside the targets.** The
AWS run that would replace these targets has not happened, so they stand.
What a 4-vCPU Firecracker guest container measured (noisy, shared;
[findings](../isolate-runtime-phase0-findings.md) and
`crates/isolate/images/README.md`), labelled as such:

| Target | Container measurement | Verdict here |
| --- | --- | --- |
| G1: component instantiate ≤ 20 µs p50, ≤ 100 µs p99 | 5.0 / 9.8 µs from `InstancePre`; full store cycle 8.1 / 22.7 µs | met |
| G1: JS pre-initialized script ≤ 300 µs | not measured: `entry` is not pre-initialized (calls 41); a fresh `js` hello world is 1.14 ms p50 (2.00 ms p99), warm 107 µs | open |
| G1: JS dynamic 10 KB ≤ 1 ms p50 | 2.86 ms for a ~12 KB snippet in the shipped image (1.75 ms for 10 KB in the spike) | missed |
| G1: Python small snippet ≤ 5 ms p50 | hello world 2.55 ms (5.99 ms p99), warm 171 µs; a ~12 KB snippet 12.0 ms | met for small snippets only |
| G2: ≥ 4,000 small / ≥ 500 JS isolates per GiB idle | Rust component 33.8 KiB idle (~31,000 per GiB); `js` 767 KiB (~1,300 per GiB); `python` 1,519 KiB (~690 per GiB) | met for RAM; slots and stacks bind first |
| G2: ≥ 10^5 resident per 64 GiB host, ≥ 10^6 hibernated | 21,870 guard-page slots per process (so ≥ 5 hosts per node), 328,842 with MPK and a raised `vm.max_map_count`; no density campaign | not measured |
| G3: waiting holds no stack; switch ≤ 1 µs | fiber suspend/resume 480 ns; a store keeps a stack after any async call on either ABI (calls 7) | switch met; "no stack" not achievable on wasmtime 50 |
| G5: VM run ≤ 250 ms cold | not measured (no `/dev/kvm`) | open |
| G6: no network, no writable filesystem, no new code | network namespace with a down loopback; in-memory scratch only; anonymous `PROT_EXEC` not denied without an LSM (calls 14) | met except "no new code" |
| G7, G8, G9 | as designed | met |

**As built, hardening round (2026-11-11): end to end and host
benchmarks.** `RunSession` through a manager to a real worker and a real
confined host, in the same 4-vCPU container with **debug builds** of the
worker and the host, p50 of 30 runs after a warm-up: a Rust `wasi:cli/run`
component 4.8 ms (min 4.4 ms), a Python hello world 17.6 ms (min 16.6 ms);
the first Python run, which includes the one-time compile by a debug
compiler, 60.5 s. No release-build end-to-end number was taken, so the
Phase 1 criterion (under 2 ms for a component, §15) is not shown met.
In process (`isolate_bench`, release, one `Host::invoke` through a shard):
a fresh command instance per call 107 µs, a call on a warm runner 38 µs, a
call whose guest awaits the worker 47.5 µs.

### Non-goals

- **Running arbitrary Linux binaries in an isolate.** That is what the
  VM tier is for. Isolates run Wasm.
- **Legion re-running a run, or moving one between tiers, on its own.** A
  caller may rerun; Legion reports what happened and which tier to use next
  (calls entry 6).
- **POSIX compatibility in the isolate tier.** No `fork`, no processes, no
  raw sockets, no persistent filesystem. WASIX-style POSIX emulation is
  rejected (§6.9).
- **A JIT JavaScript engine in Phase 1 to 6.** JS runs on an interpreter
  with ahead-of-time partial evaluation. V8 is Phase 7, behind a trigger.
- **Live migration of a running isolate into a VM.** Phase 8, research.
- **Envelopes shared by thousands of small jobs** (one per FaaS customer).
  This plan's density comes from one job hosting many tenants. A shared
  pool envelope is an open question (§14).
- **Isolate sessions in the managers' index, and per-isolate UI.** Nodes
  answer for their own isolates; the UI shows aggregates.
- **Promoting an isolate session to a keyed instance.** A keyed instance is
  an allocation each; durable named isolates use derived ids and
  `durability = "store"` instead (§8, calls entry 34).
- **Wasm threads and shared memory.** They are a high-resolution timer and
  a scheduling hazard (§6.10).
- **GPU access** from either tier.
- **Determinism or replayable execution.**

## 5. Background: what exists today

### 5.1 The WASM embedding layer

`src/wasm/` (`src/wasm/mod.rs:1-37`) is the shared layer the network
plugins use: one engine with fuel metering (`src/wasm/engine.rs:9-16`), a
memory cap that remembers a refusal (`src/wasm/engine.rs:21-71`), failure
descriptions (`describe_failure`, `src/wasm/engine.rs:78-97`), a WASI
preview1 shim with arguments, environment, clocks, randomness, and
stdout/stderr (`src/wasm/wasi.rs`), and module fetching by URL and
`sha256:` through the artifact stager into the node content store
(`src/wasm/module.rs`). It embeds **wasmi 2.0**, an interpreter
(`Cargo.toml:128`), chosen for binary size: wasmi adds 1.8 MB, wasmtime
with Cranelift about 10 MB, to a ~26 MB worker
([mesh-interfaces calls entry 22](../calls/mesh-interfaces-plan_calls.md#22-the-plugin-engine-is-wasmi-an-interpreter-not-wasmtime)).

The proxy-wasm host keeps a small pool of instances per (plugin revision,
scope, config), each behind a mutex for the length of a callback, picked
"first idle, else round-robin" (`src/worker/netd/plugins/mod.rs:618-699`).
That is the right shape for a few plugins per node and the wrong one for
10^5 tenants: one instance per slot, synchronous callbacks, fuel per call,
and no notion of an instance waiting.

### 5.2 Session mode, envelopes, and edge starts

Session mode ([docs/session-mode.md](../../docs/session-mode.md)) already
runs "very large numbers of short-lived, on-demand units, such as an
agent's sandbox, a tool execution, a CI runner, or a function invocation"
with no write to raft. The pieces this plan rides:

- **Envelopes.** A session task generates `<task>-envelope` on the
  built-in `envelope` runtime (`src/task_plugins/envelope.rs:1-7`), one
  allocation per eligible node, which reserves the room. "The allocation is
  the reservation, so there is nothing to start." Elastic envelopes
  (`stateful.envelope.max`, `shrinkAfter`) grow and shrink per node.
- **The session router and edge starts.** A manager routes a start to a
  node with room over `PostWork`; at the edge a caller holding a session
  ticket calls `Worker.StartSession` with no manager on the path, and the
  node verifies the ticket offline.
- **Ids that name their node.** An unnamed session's id carries an
  8-character tag of its node (`workspace-q4n7c2rd.7k2qd9x4…`), so routing
  a later request needs no lookup.
- **The launch pool.** `edgeBoot.launchConcurrency` slots bound concurrent
  starts per node, refusing with retryable `RESOURCE_EXHAUSTED` after
  `edgeBoot.queueTimeout` (`src/worker/launch_pool.rs:1-12`). A slot is
  sized for a VM boot; an isolate start is four orders of magnitude
  cheaper and must not take one (§8).
- **Sleeping and moving sessions.** `suspend.durability = "local" |
  "store"`, `idle.after`, `keepFor`, manifests with generations in the
  content store (`src/worker/task_manager/session_sleep.rs:1-15`,
  `session_store.rs:1-15`). Isolate hibernation reuses this vocabulary and,
  for `store`, the manifest format.
- **What it costs to report.** The session index is about 200 bytes per
  running session per manager ([scaling plan §7.1](sandbox-scaling-plan.md#71-throughput-and-counts)).
  At 10^5 isolates per node across 100 nodes that is 2 GB per manager,
  which is why isolates are not reported per unit (§8).

### 5.3 The sandbox runtime (the burst target)

The `sandbox` runtime (`src/task_plugins/sandbox.rs`, 4,511 lines;
`src/task_plugins/microvm.rs`, 2,660 lines) boots an OCI image in a
cloud-hypervisor or Firecracker microVM. What burst reuses unchanged:

- **The root disk cache:** one base disk per image per node, reflinked per
  sandbox in about 10 ms on XFS ([sandbox doc](../../docs/task-runtime-sandbox.md#the-root-disk-cache)).
- **The guest agent's exec listener** on vsock port 1100, so the isolate
  can run commands in its VM ([sandbox-exec-plan.md](sandbox-exec-plan.md)).
- **Gated networking with credential injection**, so a burst VM reaches
  exactly the upstreams its job declares with the same injected keys
  ([network handlers](../../docs/network-handlers.md#credential-injection)).
- **Session mode for sandboxes**, idle suspend, and sleeping on the node.
- **Snapshots, forks, and create-from-snapshot** for keyed instances.
  Sessions cannot start from a snapshot yet
  ([session doc](../../docs/session-mode.md): "Not available for sessions:
  `--from-snapshot`"). A warm burst therefore needs
  [scaling plan Phase 8](sandbox-scaling-plan.md) ("a warm base checkpoint
  per envelope"); until then a burst is a cold boot (§6.12).

### 5.4 The datapath the isolate's I/O goes through

- **Gated egress:** an empty network namespace per allocation with the
  egress proxy, resolver, upstream listeners, and ingress listeners bound
  inside it ([gated mode](../../docs/network-handlers.md#gated-mode)).
  Per-isolate namespaces are impossible at 10^5 per node, so the host
  process carries each isolate's identity on its requests instead (§6.9).
- **PROXY protocol v2 with Legion TLVs** (`src/worker/netd/proxyproto.rs:1-19`)
  already names a flow's workload, allocation, and labels to something that
  owns bytes. The isolate host's requests to the worker carry the same
  TLVs, so policy, upstreams, and credential injection resolve per isolate.
- **Ingress** listeners and the gateway's wake-on-request
  ([gateway](../../docs/gateway.md)) are the HTTP entry for request and
  session isolates.

### 5.5 Confinement and accounting the host process reuses

- A seccomp denylist for containers (`src/task_plugins/seccomp.rs:1-25`),
  user namespaces (`src/task_plugins/userns.rs`), and cgroup v2 resource
  enforcement with oversubscription and a named memory-pressure response
  (`src/worker/pressure.rs:1-12`,
  [resource enforcement](../../docs/resource-enforcement.md)).
- Idle detection (`src/worker/idle.rs:1-12`) and the per-node memory
  pressure steps, which learn one new step: hibernate isolates (§6.7).
- Namespace policies already restrict which runtimes a namespace may use
  (`runtimes = ["container", "sandbox"]`,
  [namespaces](../../docs/namespaces.md)), which is how an admin keeps a
  namespace off the isolate tier, or on it only.
- A shipped helper binary has a precedent: `legion-guest`, found beside
  the worker or through `LEGION_GUEST_BINARY`
  ([sandbox doc](../../docs/task-runtime-sandbox.md#the-guest-agent)).
  `legion-isolate` is found the same way.

### 5.6 Sizing and scaling today

- **Reservation and ceiling.** `resources.memory` is what the scheduler
  packs on and the cgroup's `memory.low`; `resources.memoryMax` is the hard
  ceiling a task may burst to and, for a sandbox, its guest size
  ([resource enforcement](../../docs/resource-enforcement.md#burstable-memory-memorymax)).
  Isolates adopt both fields unchanged (§6.11).
- **Only envelopes resize.** Elastic envelopes grow and shrink with one
  scheduler-lane compare-and-set per resize (`EnvelopeResize`,
  `proto/legion.proto:6135-6150`; `src/storage/store/envelope_ops.rs:80`).
  No running allocation, session, or isolate can change its reservation;
  [resize-plan.md](resize-plan.md) generalizes the envelope's mechanism.
- **Count overrides** (`legion job scale`) change a task's count without a
  deploy, under the `scale` ACL resource; resize follows the same shape.
- **Sessions are leaves.** No session can start or own another one;
  [child-sessions-plan.md](child-sessions-plan.md) adds that, and the
  front-door rule for addressing.

## 6. The decision space

Each subsection names the options that were genuinely on the table, scores
them against the three things the isolate tier exists for (start time,
density, and waiting for free) plus security and fit with Legion, and
picks one. The judgement calls doc records the reasoning behind the
biggest picks.

### 6.1 The engine

| Option | Start | Idle cost | Tenants per process | Waiting | Languages | Escape record | Binary | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **wasmtime, pooling allocator, CoW images** | 5 to 30 µs | its private pages: KBs for a small module, ~1 MB-class for a JS heap | 16k to 32k slots; ~218k with MPK | stackless callback ABI holds no stack; fibers otherwise | anything that compiles to Wasm; JS and Python as interpreters inside | 5 critical advisories in 2026, mostly compiler backends (aarch64, Winch), one in the default config on every architecture; none in 2024 and 2025 | runtime-only +1.2 to 2.1 MB; with Cranelift ~19 MB | **Take** |
| V8 isolates (`v8` crate, `deno_core`, or workerd) | 0.7 to 5 ms from a snapshot | a few MB | hundreds to thousands; one 4 GB shared cage by default, multi-cage ("IsolateGroups") unsupported upstream and unreleased in the Rust crate; a 2022 report of a 256 to 512 isolate cap with the shared cage, not confirmed fixed | native: promises hold no stack | JS, TS, Wasm | V8 bugs start about half of Chrome's 7 to 10 in-the-wild 0-days a year; workerd's README says it "is not a hardened sandbox" | +22 to 42 MB, roughly doubling the binary | **Defer** to Phase 7 behind a trigger |
| wasmi (today's plugin engine) | no compile | as wasmtime | as wasmtime | `call_resumable` suspends at host calls | as wasmtime | tiny interpreter | 1.8 MB | Keep for plugins; **reject** here: ~10× slower than Cranelift, in Pulley's band |
| WAMR, WasmEdge, Wasmer | fast AOT (1.6 to 2.1× native on libsodium vs wasmtime's 2.4×) | small | fine | weaker async and component story | as wasmtime | WAMR and WasmEdge are C/C++ runtimes inside the multi-tenant process | small | **Reject** |
| Hyperlight micro-VMs | 1 to 2 ms (vendor), 9.1 ms measured; 45 to 93 µs snapshot loads | MBs (hundreds per GiB) | one VM per tenant, many per process | a host call is a VM exit (two on nested virtualization) | Wasm or JS inside the VM | hardware boundary; pre-1.0 | needs KVM/MSHV | **Reject** as the isolate tier; watch as a cordon boundary |
| A process per tenant from a zygote (seccomp, Landlock, user namespaces) | 0.5 to 2 ms (Dandelion 486 µs; 2.3 ms fork+exec nested, namespaces +~1.5 ms) | MBs of kernel and page-table state | one | the kernel's | any native code | kernel attack surface | none | **Reject**: fails "many tenants per process"; the VM tier does it better |
| gVisor | ~190 ms | ~24 MB | one sandbox | the Sentry's | any | strong | separate | Reject for this tier (a VM-tier alternative) |
| Native QuickJS in the host | < 300 µs | hundreds of KB | many | engine-level | JS only | a C engine in the shared process: any bug owns every tenant | small | **Reject** |

**Why wasmtime and not V8.** V8 is the only engine with a production record
of thousands of tenants per process, and Cloudflare's design is the
reference for operating one. But on the axes this plan optimizes, Wasm
wins each:

- **Start.** 5 µs to a fresh instance against 0.7 to 5 ms: an
  instance per request is affordable, so the default lifetime can be the
  safe one.
- **Density.** An idle instance costs only what it wrote; a V8 isolate's
  floor is megabytes before user code. For Rust, Go, and C guests the gap
  is one to two orders of magnitude.
- **Containment.** JS and Python interpreters run *inside* Wasm, so an
  engine bug, the class behind most V8 exploits, corrupts one isolate's
  linear memory instead of the process. The trusted computing base in the
  shared process is wasmtime's runtime and Cranelift's output, both in
  Rust or verified-in-progress, not a JIT for a dynamic language.
- **Fit.** wasmtime is a Rust crate with a runtime-only build of about
  2 MB; V8 would roughly double Legion's binary and bring a C++ toolchain.
  Upstream does not support lifting V8's 4 GB shared cage.

**What it costs: JS throughput.** StarlingMonkey in Wasm scores about 1/75
of V8's JIT on Octane before weval, which narrows that by 2 to 3×; QuickJS
natively is about 1/30. For agent snippets, glue code, and request
handlers that spend their time waiting on I/O, that is the right trade.
For CPU-bound JS it is not, and the answer is the burst tier, not a second
engine: a burst VM runs Node with V8's JIT behind a hardware boundary
(`tier = "vm"`, which the reply's `nextTier` suggests after an isolate run
hits its CPU limit). Phase 7 adds V8 as a second engine only if
Phase 3's telemetry shows the trigger in calls entry 1.

### 6.2 Languages and images

An **image** is a pre-initialized component that Legion ships or the
author builds; a tenant's code is either compiled into a component or
handed to an image as source.

| Language | How | Start | Notes | Verdict |
| --- | --- | --- | --- | --- |
| **Rust** | components via `wit-bindgen`, including async on the callback ABI | 5 to 30 µs | the best-supported guest; smallest footprint | First class |
| **Go** | `componentize-go` (Go 1.25.5+); TinyGo `wasip2` | µs | mainline `GOOS=wasip2` not shipped | Supported |
| **C, C++, .NET, MoonBit** | wasi-sdk; componentize-dotnet (NativeAOT-LLVM) | µs | | Supported |
| **JS: StarlingMonkey** (SpiderMonkey in Wasm, weval AOT) | the `js` image; ComponentizeJS for an author's own component | 5 µs instantiate, 0.36 ms to hello world | full modern JS and Web APIs (`fetch`, streams, crypto); runs Fastly's and Spin's JS; ~8 MB image shared copy-on-write | **Default `js`** |
| JS: QuickJS / quickjs-ng (Javy) | a `js-small` image | < 300 µs lifecycle | ~1 MB, reference counting (smaller private heaps), ~3× slower than Rust Wasm; Shopify's choice | **Candidate**, if Phase 0 shows a several-times smaller idle footprint |
| JS: Boa, Hermes, MicroQuickJS | | | Boa: 95.6% test262 but the slowest on Octane; Hermes: no server embedding story; MicroQuickJS: ES5 subset | Reject |
| **Python: CPython for WASI** (componentize-py, pre-initialized) | the `python` image | 1 to 5 ms per run after pre-initialization (one embedder's report); < 1 ms instantiate from `InstancePre` (maintainer's estimate) | pure-Python packages; native extensions only where a Wasm build exists | **Default `python`** |
| Python: Pyodide | | ~1 s cold even with Cloudflare's memory snapshot | built for Emscripten and a JS host, not WASI | Reject |
| Python: MicroPython, RustPython | | ms | MicroPython is 303 KB but not CPython; RustPython is not production-ready | Reject (MicroPython a possible `python-small`) |

Three rules follow:

1. **Images are code Legion ships and pins.** Each release pins an image
   digest per name; a job can pin an older one.
2. **What an image cannot do, it says so, by name.** Python's
   `subprocess`, `ctypes`, and socket modules, and JS's `child_process` and
   `node:` builtins outside the supported set, are stubs that raise
   `IsolateUnsupported("<what>")` and report it to the host, so a snippet
   that needs a machine is stopped with a reason the caller and the memo
   can act on (§6.12).
3. **No package installer in an isolate.** Packages are in an image or
   bundled into the author's component. `pip install` is a VM's job.

**Python is the honest weak spot.** Cloudflare's Python Workers, the most
engineered Python-on-isolates in production, start in about a second with
real packages. Legion's target (≤ 5 ms) holds only for the standard
library plus small snippets; anything heavy is routed to a VM by the
pre-flight check, and the public doc says so.

**As built (2026-11-11).** `js` is StarlingMonkey via componentize-js
0.23.0 without weval (calls entry 46); `python` is CPython 3.14 via
componentize-py 0.25.1 with the whole standard library in a compressed
blob and no packages (calls entries 47, 48). Rule 2 is built for both
images (calls entries 49, 66). Rule 1 is half built: the image manifest
pins each built artifact's sha256 and a job can pin `name@sha256:<hex>`,
but builds are not reproducible and no release ships images yet (calls
entry 74).

### 6.3 Process and thread model

Three questions: whether isolates run in the worker process, how tenants
map to processes, and how a process maps to cores.

| Option | Start and switch cost | Blast radius of an engine escape | Density | Verdict |
| --- | --- | --- | --- | --- |
| **A. In the worker process** | Lowest: no IPC for I/O | The worker: every allocation's control, the node's credentials, the datapath | One address space for everything | **Reject** |
| **B. One host process per job per node** | IPC per I/O | One job | Bounded by job count: a FaaS shape with 10^4 jobs is 10^4 processes | Phase 1 only (§11), as `cordon = "job"` after |
| **C. One host process per cordon** (a trust partition, default per namespace) | IPC per I/O | One cordon, which holds no secrets (§6.10) | Many tenants per process, the point of the exercise | **Take** |
| **D. One process per tenant from a zygote** | ~1 ms fork, MiBs of kernel state | One tenant | Kernel-bound, ~10^4 per node | Reject for the isolate tier (it is what the VM tier does better) |

**Option C, with the cordon as the unit of trust.** The worker starts one
`legion-isolate` host process per cordon on its node when the first
envelope that needs it lands, and stops it when the last leaves. Cordon
kinds (`config.cordon`):

- `namespace` (default): every isolate task in the namespace shares a
  host. A platform job hosting a million end-user tenants is one cordon.
- `job`: a host for this job alone, for a job whose isolates handle
  material it will not share a process with anyone over (the kernel then
  enforces its envelope too; §14 Q7).
- `shared`: one host for every namespace whose policy allows it, for
  operators running anonymous, low-trust code at maximum density. An
  operator opts in per namespace (`isolateCordons`).

A cordon may need more than one process. Without MPK, a process holds
about 16,000 to 21,000 guard-page slots (§6.4), so a cordon that needs more
runs several host replicas and the worker spreads new isolates across
them. That is the fallback the ColorGuard paper measured MPK against
(about 29% more throughput with one MPK process than with 15 processes).

**Inside a host: one shard per core, shared nothing.** Each shard is a
thread pinned to a CPU of the cordon's cpuset, running a single-threaded
async executor that owns a set of isolates (their wasmtime `Store`s). An
isolate runs only on its shard, so its memory stays in that core's caches
and nothing in a turn takes a lock. New request isolates go to the less
loaded of two random shards; session isolates stay on their shard and
move only between turns, when a rebalancer sees a shard persistently
over its share. The shard count follows the cordon's CPU share and is
resized between turns.

| Thread model | For | Against | Verdict |
| --- | --- | --- | --- |
| **Thread per core, shared nothing** (Seastar, Glommio, Redpanda's wasm) | No locks in a turn, cache-warm isolates, a predictable switch | Imbalance needs an explicit rebalancer | **Take** |
| Work-stealing (tokio multi-thread) | Balances itself | Isolates migrate between cores mid-life, `Store` access needs synchronization, and the slice accounting has to follow the task | Reject for guest execution; the host's I/O to the worker may still use it |
| A thread per isolate | Simple, preemptive | 10^5 threads; defeats "waiting is free" | Reject |

Redpanda's engineers ran wasmtime on Seastar's reactor threads with 128
KiB fiber stacks and reported three times the throughput of their first
design, which is the closest public evidence for this shape
([rockwotj](https://rockwotj.com/blog/async-wasm-in-seastar)).

**The hard rule this implies:** nothing on a shard thread blocks. Host
functions are async or complete in microseconds. Compilation never runs on
a shard (it is a different process, §6.5), and the first touch of a newly
mapped compiled module is prefetched off the shard before the module is
marked ready.

**As built (2026-11-11).** Option C with pinned shards, but without the
rebalancers: holders stay on the shard they started on and the shard count
is fixed at host start (calls entry 38), and a cordon's replicas only grow,
with each task's budget split evenly between them (calls entry 53). The
worker clamps a host's slots to what `vm.max_map_count` allows.

### 6.4 Memory layout

Each isolate's linear memory is a slot in wasmtime's pooling allocator.
The decision is how slots are laid out in the host's virtual address
space, because on x86-64 that, not RAM, is what caps isolates per process.

| Layout | Slots per process (x86-64, 47-bit) | Run-time cost | Spectre on heap accesses | Maturity | Verdict |
| --- | --- | --- | --- | --- | --- |
| **Guard pages** (4 GiB reservation + guard per slot; wasmtime's default) | ~16,000 to 32,000 ([docs](https://docs.wasmtime.dev/api/wasmtime/struct.PoolingAllocationConfig.html), ColorGuard) | None: bounds checks elided | Safe by construction: a speculative index stays inside the slot's own reservation | Default, most tested | **Take as the default** |
| **MPK striping / ColorGuard** (up to 15 slots share a reservation, each a different protection key) | ~15× more: 14,582 → 218,716 slots of 408 MB measured ([ColorGuard, ASPLOS '25](https://zyedidia.github.io/papers/seguecg.pdf)) | A switch costs 30 → 52 ns | MPK also blocks speculative access to other colours (per the paper) | Upstream but "experimental", off by default, x86 Linux only | **Take in Phase 6**, behind its own soak, default-on per cordon only after it |
| **Small reservations + explicit bounds checks** | ~10^6 | ~1.5× slower; 1.6 to 1.8× with Spectre guards | Needs the Spectre `cmov` guards | Tested, but the zero-guard corner has had a leak ([CVE-2026-34988](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-6wgr-89rj-399p)) | **Reject as a default**; an operator density mode for `shared` cordons only |
| **5-level paging** (57-bit addresses, 128 PiB) | ~10^6 with guard pages | None | As guard pages | Kernel opt-in per mapping; whether wasmtime's pool asks for high addresses is unverified | **Investigate in Phase 6** |
| Several host processes per cordon | Slots × processes | One more process per ~16k slots | As guard pages | Nothing new | **Take as the fallback** without MPK (ARM, older x86) |

The density-tuned layouts are the least tested paths in the engine (two of
2026's advisories live there), so the layout is a property of the cordon,
set by the operator, never a per-job knob.

**The per-isolate memory ceiling, `resources.memoryMax`, defaults to 128 MiB** (Cloudflare Workers'
limit), with a worker-wide ceiling of 4 GiB. 128 MiB is far below the
~550 MB at which MPK striping reaches its full 15× (8 GiB / 15), so the
default never fights the density layout.

**Slot reuse is where instantiation time goes at high rates.** The
pooling allocator's measured bottleneck at hundreds of thousands of
instantiations a second is `madvise` and the TLB-shootdown IPIs it causes
([wasmtime #8034](https://bytecodealliance.github.io/zulip-archive/stream/225357-git-wasmtime/topic/wasmtime.20.2F.20issue.20.238034.20Wasmtime.20throughput.20low.20on.20linux.20s.2E.2E.2E.html)).
The host uses the knobs the docs recommend for that: slot affinity to a
module (`max_unused_warm_slots` raised from 100), `linear_memory_keep_resident`
for the first pages of small modules (reset with `memset`, not
`madvise`), `pagemap_scan` on Linux 6.7+ to reset only resident pages,
and batched decommits. Pool sizes (`total_memories`, `total_stacks`,
`total_core_instances`, default 1,000 each) are sized from the host's
slot budget.

**Copy-on-write images** (`memory_init_cow`, on by default on Linux)
mean a module's initial memory, including a pre-initialized JS heap, is
mapped from the compiled artifact and shared by every instance until
written. An idle isolate's cost is its *private* pages: what it wrote.

**As built (2026-11-11).** Guard pages are the default; `mpk` and `small`
exist as the worker-wide `isolate.layout`, not per cordon, and are not
soaked (calls entry 22). Measured in a container: 21,870 guard-page slots
per process, 328,842 with MPK (`vm.max_map_count` raised), 702,500 small
reservations; live instances cost about 8 mappings each, so the default
`vm.max_map_count` allows about 8,000 per process, and the worker clamps
`slotsPerHost` (default 10,000) to it (calls entry 53).

### 6.5 The start path and compilation

What happens between "run this" and the guest's first instruction,
fastest first:

| Start | What it costs | When |
| --- | --- | --- |
| **Warm instance reuse** | A turn on an existing instance: < 1 µs | `lifetime = "tenant"` or `"session"`, the instance is resident |
| **Fresh instance from a cached artifact** | A pool slot, a CoW map of the memory image, `InstancePre` (imports resolved ahead): 5 to 30 µs (SpiderMonkey.wasm in 5 µs, [Fallin 2022](https://bytecodealliance.org/articles/wasmtime-10-performance); Lucet 30 µs) | `lifetime = "request"`, every call |
| **Fresh instance plus dynamic source** | The above from a pre-initialized language image, then the image parses the snippet: hundreds of µs to a few ms | a session run with `source` |
| **Wake from page-out / hibernation** | Page faults, or a slot plus a diff read: §6.7 | A session isolate after idling |
| **Compile, then instantiate** | Cranelift: tens to hundreds of ms per MB of Wasm | First use of a module on a node, or dynamic Wasm |

**Pre-initialization makes language images fast.** Wizer, now `wasmtime
wizer` inside Wasmtime (merged October 2025), runs a module's
initialization once and snapshots the resulting memory into the module's
data segments. SpiderMonkey plus Wizer started in 0.36 ms against a JS
isolate's ~5 ms ([Clark 2021](https://bytecodealliance.org/articles/making-javascript-run-fast-on-webassembly)),
and a snapshotted CPython restored in 0.9 ms in Faasm. Legion pre-initializes
twice:

1. **Images** (once per Legion release): the `js` and `python` images ship
   pre-initialized with the engine booted and the standard library loaded.
2. **Entries** (once per job config per node, at stage time): when a task
   names `entry`, the stager runs the image with the author's script under
   Wizer and caches the result as derived content
   ([derived-content plan](derived-content-plan.md)). Initialization
   runs with no I/O imports linked, so a script that fetches at top level
   fails at stage with a named error rather than baking a response into the
   snapshot. Every instance then reseeds randomness and re-reads the clock
   at its first turn, the isolate version of a VM fork's identity refresh,
   and the shared restore-hygiene test (§12) holds it to the same rule as
   VM forks and prepared VMs.

**As built (2026-11-11):** only step 1 exists. `entry` arrives through the
task's `artifacts` and is staged into the run directory and executed on
every fresh instance; nothing is pre-initialized at stage time, because
re-snapshotting an image needs its componentize toolchain on the node
(calls entry 41). Dynamic source is sent as a run's files (`main.<ext>`,
else the first file), not a `source` field. The images reseed what their
snapshot froze on each instance's first run (calls entry 62).

**Where compilation runs.** wasmtime splits cleanly: a runtime-only build
that loads precompiled `.cwasm` artifacts adds 1.2 to 2.1 MB; Cranelift is
the rest of the default 19 MB
([minimal builds](https://docs.wasmtime.dev/examples-minimal.html)).
Loading an artifact is `unsafe` by design: a tampered `.cwasm` is
arbitrary code execution, so the artifact is a trust boundary.

| Option | For | Against | Verdict |
| --- | --- | --- | --- |
| **A short-lived compiler process on the node** (`legion-isolate compile`), artifacts kept as node-local derived content ([derived-content plan](derived-content-plan.md)) | The artifact never crosses a trust boundary; the host process never compiles; a compile that blows up dies alone with its own seccomp and memory limit; every component is validated by the latest `wasmparser` first, which alone would have stopped CVE-2026-115384 | Every node compiles each module once | **Take** |
| Compile on the manager (or a `legion-system` app), signed artifacts distributed through the content store | One compile per cluster per module; workers could be runtime-only | Signature verification becomes the only thing between a bucket writer and code execution on every node; a dynamic module pays a network round trip | Defer, as an optimization for large fleets with few modules |
| Compile inside the host process | No IPC | A compiler in the multi-tenant process, and a compile on a shard thread stalls its isolates | Reject |
| Accept user-supplied `.cwasm` | Zero compile | Arbitrary code execution by design | Reject outright |

**Which compiler.** Cranelift only, on x86-64 first.

| Tier | Compile | Code | Notes | Verdict |
| --- | --- | --- | --- | --- |
| **Cranelift** | baseline | 1.5 to 2.5× native on crypto-heavy code | Most fuzzed, formally verified lowering rules in progress (Crocus, Arrival) | **Take** |
| Winch (baseline) | ~7 to 20× faster | ~1.1 to 1.9× slower than Cranelift | **Incompatible with epoch interruption** (needs fuel), and a 2026 sandbox escape ([CVE-2026-34987](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-xx5w-cvp6-jv83)) | Reject for untrusted code until both change |
| Pulley (interpreter) | about as slow as Cranelift (Cranelift emits its bytecode) | ~10× slower | Portable; no fast-load benefit | Reject |
| wasmi beside wasmtime for first runs | none | ~10× slower | Two engines' bugs in one trust boundary for a rare path (dynamic *Wasm*; dynamic code is mostly JS and Python source, which needs no compile) | Reject |

The two critical aarch64 escapes of April 2026 and the maintainers' note
that OSS-Fuzz has no aarch64 runners make x86-64 the first supported host
architecture; aarch64 follows when continuous fuzzing covers it (§6.10).

**As built (2026-11-11):** the compiler process is built as the first row
of the table, with Cranelift only, x86-64 Linux only, and artifacts as
derived content of kind `cwasm` keyed by the module digest and the
binary's `compatKey`; it refuses core modules and unprovided imports by
name (calls entry 13). Measured compiles: StarlingMonkey 12 MB in 3.3 s
and CPython 18 MB in 2.5 s on 4 threads; Cranelift adds 13.7 MiB to the
binary, not ~19 MB.

### 6.6 Waiting without holding anything

This is the property the user asked for in so many words: isolates cede
resources during async calls. What an awaiting isolate can hold is a
thread, a stack, and memory. §6.3 already takes the thread: a shard runs
another isolate while one waits. This section is about the stack; §6.7 is
about memory.

| Option | What a waiting isolate holds | Guests it works for | Maturity | Verdict |
| --- | --- | --- | --- | --- |
| **Stackless: the Component Model async callback ABI** (WASI 0.3) | Nothing but its linear memory and a task record: the guest returns to the host's event loop between turns, clearing its native stack | Guests built for it: Rust (wit-bindgen async), JS (jco), and any guest whose language already has an event loop | WASI 0.3.0 shipped 2026-06-11; `component-model-async` Tier 1 and on by default since Wasmtime 46; a critical guest-controlled write to the host's native stack in this path was fixed 2026-10-02 ([CVE-2026-115384](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-32h6-97mm-8q3c)) | **Take as the target** |
| **Fibers: `call_async` on a stack per in-flight call** | A fiber stack (default 2 MiB of address space, tunable to 128 KiB; resident only as touched) | Every guest, including WASI 0.2 guests that block in a sync import | Mature; how wasmtime has done async since 2021 | **Take as the fallback** for 0.2 guests |
| Wasm stack switching | A continuation | Future toolchains | Phase 3 proposal; not tiered in wasmtime | Wait |
| Asyncify-style transforms | Nothing native, but code size and speed costs | Any | Mature, slow | Reject |

**The host drives each guest's event loop.** For a stackless guest, every
event (the request arrived, a fetch completed, a timer fired) is a turn: the
host calls the guest's callback, the guest runs until it awaits, returns
`wait` or `yield` with what it waits on, and the host parks it. The
guest's own continuation state is in its linear memory, where a JS engine
or a Rust future keeps it anyway. Between turns the isolate is data, not
an execution, which is what makes hibernation (§6.7) possible at any
await, not only when idle.

**Turns are short by construction, and preempted when not.** Each turn
runs under an epoch deadline. A ticker thread advances the engine epoch
every millisecond; a turn that exceeds its slice (default 5 ms) yields to
the shard (`epoch_deadline_async_yield_and_update`) rather than trapping,
so a CPU-bound guest is time-sliced fairly, and the invocation's CPU
budget is checked at each yield. Epochs cost about 10% on compute-heavy
code and are 2 to 3× cheaper than fuel; an MMU-based variant under review
cuts SpiderMonkey's overhead from 14.4% to 2.8%. Fuel is used only where
an operator asks for exact instruction counts. Bulk operations
(`memory.copy`) check once, so the memory cap also bounds the longest
uninterruptible stretch.

**Fairness across tenants** is deficit round robin per shard, keyed by
(job, tenant key), weighted by the job's envelope CPU, so one tenant with
10,000 runnable isolates gets the same share as one with ten.

**As built (2026-11-11): fibers for every guest, no stackless world.**
Phase 0 measured that on wasmtime 50.0.1 every `Store` that has run an
async call keeps a fiber stack until it is dropped, on the callback ABI
too, and that an await costs 1.8 µs on the callback ABI against 0.48 µs on
a fiber. So the "take as the target" row above buys nothing on this engine:
the host runs every guest with `call_async` on 128 KiB fibers, built
without `concurrency_support`, and the guest world is WASI 0.2 only
(calls entry 7, reversing entry 8). A waiting isolate holds a stack and its
slot, and hibernates only between calls. Epochs measured +14 to +25% on
call-heavy code, not ~10% (calls entry 37).

### 6.7 Ceding memory: four resident tiers

A waiting isolate holds no thread and (stackless) no stack, but it still
holds its private pages. At 10^5 isolates of 1 MiB that is 100 GiB.
Density at rest therefore needs memory to leave, too, in tiers keyed off
how long the isolate has been idle (or waiting):

| Tier | What it holds | Wake cost | Entered |
| --- | --- | --- | --- |
| **Hot** | A slot, its private pages resident | none | running, or idle < `pageOutAfter` (default 2 s) |
| **Paged out** | A slot; its private pages pushed to the kernel's compressed swap (zswap/zram) or swap with `MADV_PAGEOUT` | Page faults: ~µs per page, typically ≤ 200 µs for a small isolate | idle ≥ `pageOutAfter`, or node memory pressure |
| **Hibernated** | No slot. A session checkpoint in the node content store: its private pages (diffed against the artifact's image), globals, table state, and the host-side resource table, compressed | A slot, the CoW image, the diff read back: ≤ 2 ms | idle ≥ `suspend.idle.after`, slot pressure, or memory pressure; only between turns |
| **Evicted / published** | Nothing on the node (`local`), or a manifest and parts in the content store (`store`) | As for a session resume | `suspend.keepFor` passes; `durability = "store"` publishes at hibernation |

- **Hibernation needs a turn boundary.** A stackless isolate is at one
  whenever it waits. A fiber-based isolate is at one only when it has no
  call in flight. That is the practical reason stackless is the target:
  with fibers, "waiting on a 20 s LLM call" pins a slot and a stack; with
  callbacks it can hibernate while it waits.
- **Host-held resources outlive the guest.** Open sockets, WebSockets, and
  timers belong to the host (the guest holds handles). A hibernated session
  isolate's WebSocket stays open, and a message wakes it, which is
  Cloudflare's WebSocket hibernation for Durable Objects.
- **Nothing here is a new store or a new pressure step.** A hibernated
  isolate is a suspended session: its checkpoint lives in the node content
  store like any session's, pinned, charged to the `snapshots` budget,
  evicted by `suspend.keepFor`, and published with a session manifest under
  `durability = "store"`. The worker's pressure response
  (`src/worker/pressure.rs`) already has the two steps this needs: its
  `reclaim` step calls a runtime's reclaim hook, which for isolates pages
  out idle isolates (as `memory.reclaim` and the balloon do for other
  runtimes), and its `suspend` step hibernates them, least recently used
  first. The isolate host also calls the same reclaim hook on its own when
  an isolate passes `pageOutAfter`; other runtimes can adopt idle-time
  reclaim through that hook later (TODO), with no isolate code involved.
- **Park works for isolates too.** [Park](park-plan.md) lets a workload
  say it is waiting, so its node suspends it at once instead of after
  `idle.after`. A session isolate gets the same contract as a host call
  instead of a socket: a guest that parks is hibernated at its next turn
  boundary. Park's other problem, a quiet open connection read as idle,
  does not arise here, because the host holds the connection and its
  response wakes the isolate.
- **Paging needs the node to have somewhere to page.** Without zswap,
  zram, or swap, `MADV_PAGEOUT` does nothing for anonymous pages and the
  paged-out tier collapses into hot. The public doc says so and the worker
  reports it.

**As built (2026-11-11).** The tiers exist with different mechanisms.
*Hibernated* is a state checkpoint: the isolate's `legion:isolate/state`
map and session metadata, not its pages; it wakes into a fresh instance
with `control.info().resumed = true`, because the engine's only way to read
a component's memories (`guest_debug`) costs 320 µs per instantiation and
2× run time (calls entry 9). *Paged out* is the worker asking the host which
idle instances are cold and then asking the kernel to reclaim that many
bytes from the host's cgroup (`memory.reclaim`, cgroup v2 with swap or
zswap); the host does no `madvise`, and the worker logs once where the node
cannot page (calls entry 31). Hibernation happens only with no call in
flight (calls entry 7). Not built: host-held connections across
hibernation, `durability = "store"` (calls entry 58), and gateway wake.
Park is the guest's `control.park()`, not the `suspend.park` block.

### 6.8 Instance lifetime

| Lifetime | What a call gets | State between calls | Isolation between calls | Verdict |
| --- | --- | --- | --- | --- |
| **`request`** (Fastly's model) | A fresh instance, destroyed after | None | Total: nothing survives a request | **Default** |
| **`tenant`** (Workers' model) | A resident instance per (job, tenant key), reused while warm | Globals persist, best effort | Within one tenant only | Opt-in, for images whose per-call setup is costly (Python) |
| **`session`** (Durable Objects' model) | An addressable instance with a session id | Durable across hibernation; `store` survives the node | One session | Opt-in, for agents and actors |

`request` is the default because it is also a security property: an
instance that lives for one request cannot accumulate a side channel's
bits across requests, and a bug that corrupts it dies with it. Fastly
argues exactly this, and the 2026 Spectre result on Workers needed a
connection held open for hours to work.

### 6.9 The host API

The world in §7.4. What is in it, and what is deliberately not:

| Capability | Provided as | Not provided |
| --- | --- | --- |
| Inbound HTTP | `wasi:http/incoming-handler`, dispatched from node ingress with no TCP hop | Raw listeners |
| Outbound HTTP | `wasi:http/outgoing-handler`, carried to the worker's egress datapath | Raw sockets, DNS |
| Files | `wasi:filesystem` over an in-memory scratch tree, counted in `memoryMax`, gone at the end of the instance | A persistent or host filesystem |
| State | `legion:isolate/state`: bytes scoped to the tenant or session, kept in the isolate's checkpoint and the session manifest | Cluster KV writes (KV is raft-replicated; the hot path must not touch it) |
| Config | Env from the jobspec; KV-rendered env at stage, as for other runtimes | Secrets: credentials are injected on egress, never handed in |
| Clocks | Wall clock at 1 ms; both clocks frozen within a turn | High-resolution timers, `SharedArrayBuffer`, threads |
| Logs | stdout/stderr to the task's log stream with an `isolate` field, and into a run's reply | A log file per isolate |
| A VM | `legion:isolate/vm` (§6.12) | Processes in the isolate |

**I/O goes through the worker, attributed per isolate.** The host process
has no network interface. Every outbound request goes over the host's
socketpair to the worker as HTTP/2 with a PROXY v2 header naming the job,
allocation (the envelope), and isolate, the same TLVs the datapath writes
today (`src/worker/netd/proxyproto.rs`). The worker applies the job's
`connect` block, egress policy, handlers, and credential injection as it
does for a gated allocation. The isolate never sees a credential and
cannot reach a destination its job's policy does not allow.

| Option | For | Against | Verdict |
| --- | --- | --- | --- |
| **The worker does egress** | The host holds no secrets and no network; one policy engine | One extra local hop (~10 to 30 µs) and worker CPU per request | **Take** |
| The host does egress under pushed policy | One hop fewer | The multi-tenant process holds credentials and network access, the two things Google says sensitive data must be kept away from | Reject |
| A network namespace per isolate | Kernel-enforced | Impossible at 10^5 per node | Reject |

**Rejected: POSIX emulation (WASIX).** It would make more code run in
isolates, and every syscall it emulates is host attack surface and a
reason an isolate never bursts when it should. Code that needs POSIX gets a
VM, which already is POSIX.

**As built (2026-11-11).** The worker does egress as taken, but the hop is
not PROXY v2 over HTTP/2: the host speaks gRPC (`proto/isolate_host.proto`)
over two inherited socketpairs, and every request carries an `Origin` the
worker checks against the host's cordon; per-isolate attribution is in
debug log fields, and denials name the task (calls entry 10). The worker
dials the envelope allocation's own datapath sockets for egress and
originates TLS (calls entries 26, 51). The in-memory filesystem is Legion's
own `wasi:filesystem` (wasmtime-wasi 50 has none; calls entry 32).
Guest stdout/stderr from handler and session isolates go to the envelope
allocation's task log, one batch per invocation when it ends, each line
prefixed with the isolate id and tenant (the log format has no fields;
calls entry 69). Inbound HTTP
goes through a node-local Unix socket per port, bodies buffered (calls
entry 52).

**As built, hardening round (2026-11-11).** Attribution is by the
worker, not the caller: a host may act only as isolates the worker issued
to its cordon (calls entry 28), a call in a session is attributed to the
session's tenant, and a tenant key is documented as a routing key the
caller chooses, not an identity (calls entry 27). Egress is bounded per
invocation: at most 8 requests in flight (the rest wait), none outlives
the invocation, a resource table of 4,096 entries and header sets of
64 KiB per instance; request and response bodies are not charged to
`memoryMax` (calls entry 44). The worker caps what a host can make it
hold: checkpoints checked before buffering, 256 concurrent calls, stderr
lines of 4 KiB, and guest logs of 4,096 lines per batch at 1 MiB/s per
host (calls entry 45). An injected credential stays unseen by the guest
only while the upstream does not echo request headers back.

### 6.10 Security model and defense in depth

The 2026 evidence sets the bar. Researchers leaked a JWT across tenants
from Cloudflare's production Workers at up to 12 bits a second, using a
remote timer over a WebSocket, which defeats frozen local clocks
([Cloudflare, 2026-08-19](https://blog.cloudflare.com/revisiting-spectre-attacks-on-workers),
[paper](https://arxiv.org/abs/2608.17043)). Wasmtime published five
critical sandbox escapes in 2026, most found with LLM tooling, after none
in 2024 and 2025 ([BA, 2026-04-09](https://bytecodealliance.org/articles/wasmtime-security-advisories)).
Google's position is that Spectre cannot be mitigated in software inside
one process: "the only effective mitigation is to move sensitive data out
of the process's address space" ([v8.dev](https://v8.dev/blog/spectre)).
So the engine is one wall among several, and the design keeps secrets out
of the shared process altogether.

**Layers, inside out:**

1. **Wasm software fault isolation**, Cranelift on x86-64, guard-page
   layout, Spectre mitigations on (index masking for tables and
   `br_table`, explicit-check `cmov` where checks are explicit).
2. **Language engines inside the sandbox.** JS and Python interpreters run
   in Wasm, so an engine bug (the class behind most V8 exploits) corrupts
   only that isolate's linear memory. JS also runs without a JIT, which is
   what Google advises high-risk users do with V8.
3. **Instance lifetime.** `request` by default (§6.8).
4. **Clocks and concurrency.** Coarse wall clock, both clocks frozen in a
   turn, no wasm threads or shared memory. Known to be insufficient alone
   (the remote-timer result), kept because it raises the cost.
5. **No secrets in the process.** Credentials are injected by the worker
   on egress. The host has no TLS keys, no tokens, no KV access. What a
   cordon holds is its tenants' data, which is why the cordon is the trust
   unit.
6. **Cordons.** Tenants of different trust never share a process: by
   default one per namespace; `job` for sensitive work; `shared` only
   where an operator opts in.
7. **The host process is confined.** Its own user namespace and
   unprivileged uid; an empty network namespace (no interface, not even
   loopback); a seccomp **allowlist** (unlike the containers' denylist) of
   roughly what a reactor needs: `futex`, `mmap`/`munmap`/`madvise`/`mprotect`,
   `read`/`write`/`recvmsg`/`sendmsg` on inherited descriptors,
   `epoll`/`io_uring`, `clock_gettime`, thread `clone` only, `exit`;
   Landlock read-only on its artifact directory and nothing else; no
   `execve`; `no_new_privs`; and a cgroup with the cordon's `memory.max`,
   `cpu.weight`, and `pids.max`. Cloudflare's layer-2 sandbox is the same
   shape: namespaces and seccomp, no filesystem, Unix sockets only, egress
   through a proxy, code fetched by a supervisor.
8. **No compiler in the host, and validation before compile.** Artifacts
   come from the node-local compiler process (§6.5), which validates each
   component with the newest `wasmparser` before compiling: the published
   mitigation for 2026's async-callback stack overflow, applied to every
   module. The host role never calls Cranelift. Denying
   anonymous executable memory outright needs an LSM (SELinux `execmem`);
   seccomp cannot tell a file mapping from an anonymous one in `mprotect`,
   so this is a goal under seccomp alone and a guarantee only with the LSM
   (Phase 6).
9. **Optional VM boundary per cordon** (`cordonBoundary = "vm"`): the
   cordon's host runs inside a microVM from the sandbox runtime, so an
   escape plus a kernel exploit still lands in a guest. workerd's README
   says to run it inside a VM, and Deno runs its isolate pools in VMs.
   Costs a guest kernel per cordon and a vsock hop for I/O.
10. **Detection and quarantine** (Phase 6). Tenants that trap repeatedly,
    hit CPU limits, or show anomalous branch-miss ratios on performance
    counters are moved to a quarantine cordon. Cloudflare's dynamic process
    isolation missed the 2026 attack because it judged only finished
    invocations; Legion's judges running ones too, and treats long-lived
    connections as a co-location risk.
11. **Patch speed.** `legion-isolate` is its own binary, upgradeable
    without the worker, and `legion operator runtime disable isolate`
    ([runtime-disable plan](runtime-disable-plan.md), §13) turns the tier off
    cluster-wide while an advisory is open. Cloudflare's V8
    patch gap is under 24 hours; that is the target.

**What this design does not claim:** that tenants in one cordon are safe
from a determined Spectre attacker. It claims that such an attacker
reaches only data in that cordon, never a credential, and that an
operator decides who shares a cordon. The public doc must say this in
those words.

**As built (2026-11-11).** Layers 1 to 7 are built, layer 7 as the host
confining itself before any thread (`isolate.confine = "enforce"` by
default; calls entry 14); its network namespace holds an unconfigured
loopback that is down, not nothing. Layer 8's "no compiler in the host" is
built; denying anonymous executable memory (SELinux `execmem`) is not.
Layers 9 (`cordonBoundary = "vm"`, refused at worker start) and 10
(quarantine) are not built. Layer 11's engine is pinned (`=50.0.1`) in its
own binary.

**As built, hardening round (2026-11-11): layer 7 as eight layers.** A
security review of the first build found layer 7 leaky: a compromised
host could signal any process on the node (`F_SETOWN` with `O_ASYNC`),
`stat` any path, and, on a kernel without Landlock, read and write files
its root kernel uid owns; the compiler was confined only best effort. The
host now applies, before any thread or module, and reports in `legion
isolate stats`: a user namespace (`userns`), a network namespace
(`netns`), an empty read-only mount root holding only the artifact
directories (`mount-root`, calls entry 16), a PID namespace whose
spawned process stays behind as a confined supervisor (`pid-ns`, calls
entry 15), `no_new_privs`, Landlock (`landlock`), Landlock's signal and
abstract-socket scopes where the kernel offers ABI 6 (`landlock-scopes`,
required only there, calls entry 21), and seccomp with argument filters
that kill (`seccomp`, calls entry 19). The compiler applies the same with
its own allowlist and takes `--confine`, `enforce` by default (calls
entry 20). Still short of the text above: the kernel uid is the worker's
(the "unprivileged uid" is 65534 only inside the namespace; calls entry
18); the artifact directories are mounted executable, because wasmtime
maps compiled code from them; the cgroup has `memory.max` (now following
its envelopes) and `pids.max` but no `cpu.weight`; anonymous executable
memory is not denied (no LSM).

### 6.11 Limits and accounting

**Memory is the same two numbers as every other runtime.** An isolate does
not get a parallel pair of fields (calls entry 24):

| Field | For an isolate | For a sandbox, for comparison |
| --- | --- | --- |
| `resources.memory` | the **reservation**: what the node admits against the envelope, the isolate's expected private memory | the reservation: `memory.low` and what the scheduler packs on |
| `resources.memoryMax` | the **ceiling**: the cap on linear memory, enforced by the pooling allocator's limiter | the guest size |

- **Defaults.** `memoryMax` defaults to 128 MiB, the runtime's own default
  (as the sandbox runtime defaults its guest to 512 MiB), with a worker-wide
  maximum of 4 GiB. `memory` defaults to the image's measured idle private
  size, which each image publishes (Phase 0 measures them; the order of
  magnitude is a quarter of a MiB for a small Rust component and a few MiB
  for `js`). Submit refuses a `memoryMax` below the image's initial linear
  memory, naming both numbers.
- **Why the reservation is not the ceiling.** Charging the ceiling would
  make a JS isolate that writes 2 MiB cost 128 MiB of envelope: 60× less
  density for a ceiling nearly no isolate reaches. The host keeps a
  measured private-pages estimate per image and reports it beside the
  declared reservation, so an operator sees when the reservation is wrong.
- **Per invocation, time and output:** `config.limits.cpu` (turn CPU summed,
  checked at every yield), `limits.wall`, and `limits.output`. These are
  budgets, not reservations, so they live in `config`. `cpu` and `wall`
  are required (calls entry 59). A run stopped by one is reported in its
  reply (`stopped.reason = "limits"`, naming the limit), with
  `nextTier = "vm"` when the task has a VM task.
- **Per job on a node:** the envelope. `resources.cpu` is the isolate's
  weight in the shard scheduler; the envelope's CPU is the job's share.
- **Per cordon:** the kernel. The cgroup's `memory.max` is the sum of the
  envelopes it serves; per-job memory inside a shared host is the host's
  accounting, not the kernel's (§14 Q7).
- **Concurrency:** active invocations per job per node are bounded by the
  envelope CPU and a queue (`RESOURCE_EXHAUSTED`, retryable, after
  `edgeBoot.queueTimeout`), so overload sheds before it queues forever.
- **Resizing a running isolate** is [resize](resize-plan.md)'s Phase 1: a
  session isolate's reservation is re-admitted against its envelope and its
  ceiling is a limiter change, up to the cordon's slot size. No raft.

**As built (2026-11-11).** `limits.output` truncates (the reply says
`truncated`) rather than stopping the run, as `run.output` does for session
runs; `cpu`, `wall`, and memory stop it (calls entry 60). `memoryMax` caps
linear memory plus scratch file bytes (calls entry 32). `max_active` is the
envelope's CPU over the task's CPU weight and `max_resident` its memory over
the reservation, enforced by the host per invocation (calls entry 54). A
session resize changes `memory` and `memoryMax` live; `cpu` answers
`not_supported` (it is the task's weight). The host cgroup's `memory.max`
is set once and does not follow envelope resizes, and no `cpu.weight` is
set (calls entry 31). *Hardening round:* `memory.max` now follows envelope
grows and shrinks and tasks joining or leaving the cordon, and the host is
placed in its cgroup before `exec`; `cpu.weight` is still not set, and
none of it ran in the cgroup v1 build container.

**Accounting at burst: nothing changes size, a second unit appears.**
Because a VM, whether a VM run's or an isolate's child, is a session of its
own task (§6.12), a burst never resizes the isolate:

| | Before | After a burst |
| --- | --- | --- |
| Isolate `user-42` | 2 MiB, 10 MHz from the isolate task's envelope | unchanged: still the address |
| Its VM | none | 1 GiB, 1000 MHz from the VM task's envelope |
| Raft writes | 0 | 0 (the VM is admitted on the node) |

An elastic VM envelope that fills grows by its existing rule: one raft
entry per envelope resize, not per burst. Bursts draw only from the VM
task's envelope, so a storm of them cannot starve isolates; a full one
refuses with `RESOURCE_EXHAUSTED` (retryable) while it grows or the router
tries another node. The VM's session report carries its owner reference and the
isolate's tenant label, so a platform bills one user for both.

### 6.12 Burst: one run, one place

**The rule.** Every run happens in exactly one place, chosen before it
starts, and a session's address is always its isolate. Legion never moves
a running invocation between tiers and never runs one twice on its own.

The first draft of this plan had Legion replay a failed run in a VM when
the run had not yet made an outbound request. Review found it confusing,
and it was dropped (calls entry 6): whether a snippet burst or failed
depended on the order of two lines, one call could run in two different
Pythons, and the invocation's size changed mid-flight.

| Model | What happens | Verdict |
| --- | --- | --- |
| **A. Choose the tier before the run** | The caller, or `tier = "auto"`'s checks, picks the isolate or a VM; the run happens there and only there | **Take** |
| **B. A child VM the isolate owns** | The isolate starts a microVM as a [child session](child-sessions-plan.md) and drives it; requests keep addressing the isolate | **Take** |
| C. Legion replays a failed run in a VM | Automatic, but code runs twice and the outcome depends on what ran first | **Reject**; a caller may rerun (`--fallback vm`), knowingly |
| D. Continuation migration | Resume the same instance inside a VM-hosted host | **Defer** to Phase 8; nobody ships it |
| E. Re-point the session's address at its VM | Requests go straight to the VM after a burst | **Reject** (child-sessions calls entry 4) |

#### Path 1: the tier, picked before the run

A run is a [session run](session-runs-plan.md) on the isolate task, with
one more field: `tier` is `"isolate"`, `"vm"`, or `"auto"` (the default). A
caller that kept a previous reply's `nextTier` for this code simply asks
for that tier. `auto` decides before anything runs, from:

1. **the pre-flight check:** a scan of the source's imports against the
   image's module list (Python's `import`, JS's `import`/`require`), in
   microseconds to a millisecond; and
2. **the node's memo:** code hashes of this job that needed a VM, kept for
   `config.vm.memoFor` (default 1 h).

A `vm` run is a session run on the VM task (`config.vm.task`): the
isolate task hands over the source as `/legion/run/main.py` or
`main.mjs`, named for its image, with the input and any files, and the VM
task's own `run.command` executes it. The reply keeps the same shape, with
`tier = "vm"` and `tierReason`. Like every session run, it is
**single-use**: the VM serves one run and ends, because the next run may be
another tenant's code.

An isolate run stopped by an unsupported call or a limit stays stopped.
Its reply says why (`stopped.reason` is `unsupported` or `limits`) and
carries `nextTier = "vm"`, and the node records the code hash in its memo.
A caller who wants the rerun asks for it: `--fallback vm` on the CLI, and
`fallback` on the MCP tool and the client library, each documented as
"runs your code a second time, in a VM". The server never does it.

#### Path 2: a child VM the isolate owns

Any isolate whose task has a VM task can call `vm.acquire()`. The node
starts a child session of the VM task on the same node, charged to that
task's envelope ([child-sessions plan](child-sessions-plan.md) Phase 1), and
returns a handle: `exec`, `put` and `get` files, `fetch` a port, `resize`,
`release`, and `id`.

- **The handle is state.** A session isolate keeps the child's id in its
  state, so it survives hibernation and, with `durability = "store"`, a
  move. After either, the isolate re-attaches by id.
- **Children end with their owner:** with the request for a `request`
  isolate, with the session for a session isolate. In between, the child
  follows its own task's `suspend.idle`.
- **When the child is gone** (stopped, evicted, its node lost), the next
  call through the handle answers `child_gone` with why, and the isolate
  decides: start a fresh one from files it kept, or fail.
- **Growing the child** is a [resize](resize-plan.md) (`machine.resize`),
  admitted against the VM task's envelope; past the VM's boot size it is
  `needs_restart`.

#### Where later requests go

- **Stateless runs:** there is nothing to route. The tier is picked per run,
  and `nextTier` makes the caller the memory that works on every node,
  with Legion storing nothing. The per-node memo covers callers that ignore
  the hint.
- **Session isolates:** the session's id names the isolate's node, and the
  router, gateway, and edge pickers route there as for any session. The
  isolate handles each request itself or forwards it to its child:

```text
client ──► gateway / router ──► isolate session "user-42"   (the address never changes)
                                   │  handles it, or
                                   └─► machine.fetch / exec ──► its child VM (same node when there is room)
```

- **Bulk bytes** (a large upload, a long-lived stream to the VM) should not
  pass through the isolate. Connection handoff
  ([child-sessions plan](child-sessions-plan.md) §6.8, its Phase 4) lets the
  isolate decide and the worker splice the client's connection to the
  child's port. Until then such traffic goes through the isolate.

#### What the VM is

An ordinary session task the job declares next to the isolate task, on the
`sandbox` runtime, with its own image, resources, envelope, `run` block,
`connect`, `network`, and `suspend`. Nothing is generated. Listing it in
the isolate task's `children` is what grants both uses, single-use VM runs
and child VMs, so one line of the spec names every machine the task's code
can reach. A child runs under its own task's policy, not the isolate's
(child-sessions calls entry 3); to give both tasks the same upstreams and
credentials, bind them once in Nickel (§7.1).

#### How fast it gets there

In three steps that each ship alone:

1. **Cold boot** (works today): the sandbox's 0.18 to 0.23 s from create to
   a guest taking exec, from the cached root disk.
2. **Restore from a warm base snapshot** (needs
   [scaling plan Phase 8](sandbox-scaling-plan.md): sessions started from a
   snapshot): each node keeps a memory snapshot of the VM task's booted
   image; a start restores it, Firecracker sharing clean pages
   `MAP_PRIVATE`. Restores in the 4 to 10 ms class are published for
   Firecracker and Unikraft.
3. **Prepared, unassigned VMs** (Cloudflare's 2026 design): the node keeps a
   few restored, paused VMs per VM task and assigns one per start. They are
   never reused: "warm" means restored and not yet assigned, never "used
   before". Assignment runs the guest agent's existing resume handler
   (identity, RNG reseed, clock, `onResume`), as a fork does today. Target
   ≤ 10 ms p50 on metal, sized by the VM envelope's floor, refilled off the
   critical path.

#### What crosses, and what stays hard

- **Crosses:** the source, the input, and files the caller sent (path 1);
  files the isolate `put`s (path 2); outputs back. Environment and network
  rules are the VM task's own. Memory never crosses (that is model D).
- **Two Pythons.** The isolate's Python (CPython for WASI, the standard
  library, curated packages) and the VM's (full CPython, `pip`) differ. The
  public doc lists the isolate's subset, and the pre-flight check catches
  the import differences.
- **The pre-flight check is a heuristic.** Dynamic imports and `eval` slip
  past it, so some runs fail in the isolate first; the memo and `nextTier`
  bound that to once per code hash.
- **VM runs are slow until warm starts exist:** a cold boot per run until
  scaling Phase 8 lands.

**As built (2026-11-11).** Both paths are built as described, decided
node-side (calls entry 6). Differences: the VM tier exists only when the
task has a `config.vm` block (`vm = {}` takes the default task; calls
entry 12); a run's `args` are appended to the VM task's `run.command`
(calls entry 39); `--fallback vm` is refused with `--session` and
`--dry-run`; and a request or tenant isolate owns its children under a new
owner kind, `isolate` (calls entry 50). The shared `connect` binding does
not validate yet, because sandbox session tasks still refuse `connect`
(calls entry 64), and a suspended child is not resumed through its handle
(calls entry 67). Nothing here has run against a real VM: the container it
was built in has no `/dev/kvm`, and connection handoff from an isolate and
prepared VMs are not built.

**As built, hardening round (2026-11-11).** A run's `args` reach the VM
task's argv only when its `run.command` ends with the staged entry
(`/legion/run/main.py` or `/legion/run/main.mjs`); otherwise a run with
args that resolves to the VM tier, `auto` included, is refused
`FAILED_PRECONDITION` before anything runs. Every VM-tier run gets
`LEGION_RUN_ARGS` as a JSON array (calls entry 29). With `config.entry`,
that file is the entry of a VM run too. Child-VM owners are bound to their
job and task (calls entry 28). Verified end to end with an `exec` session
task standing in for the VM task, because the build container has no
`/dev/kvm` and the sandbox's Linux development backend takes no exec
(calls entry 65): `tier = "vm"`, `requested`, `nextTier`, `vm_ms`,
single-use sessions, the `args` rule, the disabled-runtime divert, stopped
runs not rerun, the memo, child ownership, `children.max`, a child
surviving its owner's hibernation, and children ending with their owner.
Not verified: a real sandbox VM run or child, and re-attaching a child by
id after hibernation. The two sandbox variants are written and ignored
("needs /dev/kvm").

### 6.13 Observability

Per task, never per isolate, on the existing metric families' pattern:
`legion_isolate_instantiate_seconds{image}`, `legion_isolate_turns_total`,
`legion_isolate_cpu_seconds_total`, `legion_isolate_resident{tier}`,
`legion_isolate_wakes_total{from}`, `legion_isolate_limit_total{limit}`,
`legion_isolate_vm_runs_total{reason}`, `legion_isolate_child_vms`,
`legion_isolate_stopped_total{reason}`, `legion_isolate_compile_seconds`,
and per cordon `legion_isolate_host_rss_bytes` and slot occupancy. One
log line per VM run and per stopped run; none per invocation by
default.

**As built (2026-11-11):** the families exist on the worker without `job`
or `task` labels (per-task numbers are on the heartbeat and in `legion
isolate stats`), the CPU counter is `legion_isolate_cpu_microseconds_total`,
and slot occupancy is `legion_isolate_slots` (calls entry 70).

### 6.14 What composes

Burst, growth, and density are built from a few runtime-agnostic
primitives, so each feature is a combination and none of the primitives is
isolate-specific:

| Primitive | What it is | Status |
| --- | --- | --- |
| Envelope | a per-node capacity pool that node-owned units are admitted against with no raft write; elastic | exists |
| Reservation and ceiling | `resources.memory` and `resources.memoryMax` | exists for every runtime; isolates adopt it (§6.11) |
| Resize | change a unit's reservation, ceiling, or CPU in place | exists only for envelopes; [resize-plan.md](resize-plan.md) generalizes it |
| Move | suspend and resume elsewhere | exists for `store` sessions and keyed instances |
| Ownership (child sessions) | a unit that owns sessions or instances of a sibling task, which end with it and are attributed to it | [child-sessions-plan.md](child-sessions-plan.md) |
| Front door | one address per logical unit, bound to its root | the rule in child-sessions §6.7 |
| Session run | one call: a fresh single-use session, files and input in, output and result out | [session-runs-plan.md](session-runs-plan.md) |
| Checkpoints | a suspended unit's state in the node content store, pinned, budgeted, published with a manifest | exists for sessions and instances; isolates use it (§6.7) |
| Reclaim hook | a runtime gives memory back without stopping (`memory.reclaim`, a balloon, isolate page-out) | exists in the pressure response; isolates add an idle trigger (§6.7) |
| Derived content | node-local artifacts built from verified inputs (root disks, compiled modules, pre-initialized images) | [derived-content-plan.md](derived-content-plan.md) generalizes the root disk cache |
| Runtime disable | a cluster-wide switch that refuses new starts of a runtime | [runtime-disable-plan.md](runtime-disable-plan.md) |
| Park | the workload says it is waiting, and its node suspends it | exists ([park-plan.md](park-plan.md)); isolates take it as a host call (§6.7) |
| Workload channel | the one way a workload reaches its worker, with one attribution rule | legion-apps-plan §6.4, amended |

| Feature | Composed from |
| --- | --- |
| A VM run (`tier = "vm"`) | a session run on the VM task |
| A hibernated isolate | a checkpoint, written by the pressure response's `suspend` step or an idle policy |
| A CVE response | runtime disable, plus the isolate's divert to its VM task |
| An isolate driving a VM | child session + front door + the VM task's envelope |
| A session that needs more memory | resize, else move |
| The autoscaler's vertical scaling | a policy that calls resize |
| An agent spawning tool sandboxes, from any runtime | child sessions over the workload channel |
| Continuation migration (Phase 8) | move, across runtimes |

**What stays isolate-specific:** the Wasm engine and the guest's world,
the shard executor and its per-tenant fairness, the slot layout, cordons,
the pre-flight check, and the compiler itself (its outputs are derived
content; the compiler is not shared). Each waits for a second user before
it is generalized.

**What this plan deliberately does not build:** a tier change as a resize,
an isolate that "grows into" a VM past a size threshold. Burst is mostly
about capability (a subprocess, a native package), not size, and an isolate
that only needs memory gets a higher ceiling in microseconds. Converting a
unit's runtime mid-life would weld size and capability into one operation
nothing else builds on (resize calls entry 9).

## 7. The surface

### 7.1 The jobspec

An isolate task is a session task on the `isolate` runtime. Its VM, when it
has one, is a sibling session task on the `sandbox` runtime that takes
[session runs](session-runs-plan.md). Everything under the isolate's
`config` is new; `stateful`, `resources`, `connect`, `network`, and
`suspend` mean what they mean today; `children` comes from the
[child-sessions plan](child-sessions-plan.md) and `run` from the
session-runs plan.

```nickel
let net = {
  upstreams = [{
    name = "openai",
    to = "https://api.openai.com",
    credentials = [{ header = "Authorization", prefix = "Bearer ",
                     from = { kv = "secrets/openai" } }],
  }],
} in
{
  name = "code-runner",
  tasks = [
    {
      name = "run",
      runtime = "isolate",
      config = {
        # What runs: a built-in image ("js", "python") or a component.
        image = "python",
        # entry = "main.py",            # pre-initialized at stage time; omit
        #                                # for dynamic code (session runs)
        # module = { source = "oci://ghcr.io/acme/handler:1.4", sha256 = "..." },
        lifetime = "request",            # "request" (default) | "tenant" | "session"
        limits = {
          cpu = "100ms",                 # CPU per invocation (turns summed); required
          wall = "30s",                  # wall clock per invocation; required
          output = "1MiB",               # stdout + stderr returned per run
        },
        cordon = "namespace",            # "namespace" (default) | "job" | "shared"
        vm = {
          task = "machine",              # default: the only entry in children.tasks
          memoFor = "1h",                # route a code hash that needed a VM straight to one
        },
      },
      # Reservation (what the envelope admits on) and ceiling (the linear
      # memory cap), as for every runtime. Defaults: the image's measured idle
      # size, and 128 MiB.
      resources = { cpu = 10, memory = 2, memoryMax = 128 },
      stateful = {
        mode = "session",
        envelope = { resources = { cpu = 16000, memory = 32768 } },
      },
      children = { tasks = ["machine"], max = 1 },
      connect = net,
    },
    {
      name = "machine",
      runtime = "sandbox",
      config = { image = "python:3.13-slim", vcpus = 1 },
      resources = { cpu = 1000, memory = 1024 },
      stateful = {
        mode = "session",
        envelope = { resources = { cpu = 8000, memory = 16384 },
                     max = { cpu = 32000, memory = 65536 } },
      },
      # What a VM run executes (session-runs plan). The isolate task hands
      # its source over as /legion/run/main.py, named for its image.
      run = { command = ["python3", "/legion/run/main.py"], wall = "5m" },
      suspend = { idle = { after = "2m" } },
      connect = net,
    },
  ],
}
```

What submit checks, with the errors it gives:

```text
[isolate] code-runner/run: runtime "isolate" needs stateful.mode = "session" (isolates are node-owned and never written to the store; see docs/task-runtime-isolate.md)
[isolate] code-runner/run: config.limits.cpu and config.limits.wall are required (budgets differ by orders of magnitude between a webhook and an agent loop; there is no default)
[isolate] code-runner/run: resources.memoryMax 8192 MiB is above this cluster's isolate maximum 4096 MiB (worker isolate.maxMemory); run it with tier = "vm" for more
[isolate] code-runner/run: resources.memoryMax 16 MiB is below image "js"'s initial linear memory (24 MiB)
[isolate] code-runner/run: config.vm.task "machine" is not in children.tasks; add it there, which is what lets this task's isolates start it
[isolate] code-runner/machine: config.vm.task of code-runner/run names machine, which has no run block; add run.command so tier = "vm" runs know what to execute
[isolate] code-runner/run: config.cordon = "shared" is refused by namespace policy 'tenants' (isolateCordons = ["namespace", "job"])
```

An HTTP handler declares a port, and ingress dispatches into the host
without a TCP hop:

```nickel
network = { ports = [{ name = "http" }] },
```

**As built (2026-11-11).** The jobspec is as above with these differences
(the working example is `configs/jobs/isolate.ncl`, every field is in
`configs/jobs/maximal.ncl`):

- `entry` names a file delivered by the task's `artifacts`
  (`${artifact.<name>}` or a relative path) and is not pre-initialized
  (calls entry 41); a JS entry is a script, not an ES module (calls entry
  49).
- The VM tier needs an explicit `vm` block; `vm.task` defaults to the only
  entry of `children.tasks` inside it (calls entry 12).
- `config.env` exists; unknown fields are refused (calls entry 73).
- `connect = net` on the `machine` task does not validate: sandbox session
  tasks still refuse `connect`, so the demo VM tasks use `network = { mode =
  "none" }` (calls entry 64). The isolate task's own `connect` and
  `network.ports` ride its envelope; `service` is refused (calls entry 26).
- `suspend` applies only with `lifetime = "session"`, and
  `durability = "store"` is refused at submit (calls entry 58), as is
  `onWorkerStop` since the hardening round: a stopping worker always
  hibernates its session isolates.
- Cordon names carry a kind prefix (`ns:<namespace>`, `job:<job id>`,
  `shared`; calls entry 36). The
  worker's `isolate.cordonBoundary = "vm"` is refused by worker-config
  validation.
- `memory` defaults to 1 MiB for `js` and modules and 2 MiB for `python`;
  `cpu` to 10 (calls entry 24).

### 7.2 The CLI

Runs use the [session-runs plan](session-runs-plan.md)'s verb; the isolate
task adds `--tier`, and the reply adds `tier`, `tierReason`, and `nextTier`.

```console
$ legion session run code-runner --task run --file snippet.py -j
{"ok":true,"data":{"task":"run","tier":"isolate","tierReason":"auto: no imports outside the image",
 "node":"w-17","exit":0,"stdout":"42\n","stderr":"","cpuMs":3.1,"wallMs":4.0,
 "nextTier":null,"deduplicated":false}}

$ legion session run code-runner --task run --file train.py -j
{"ok":true,"data":{"task":"machine","tier":"vm","tierReason":"auto: import torch is not in image python",
 "node":"w-17","exit":0,"stdout":"done\n","wallMs":1890,"vmMs":231,
 "nextTier":"vm","deduplicated":false}}

$ legion session run code-runner --task run --file transcode.py -j
{"ok":true,"data":{"task":"run","tier":"isolate","node":"w-17","exit":null,
 "stopped":{"reason":"unsupported","detail":"subprocess.run: processes are not available in an isolate",
            "hint":"rerun with --tier vm (this code's next run on w-17 goes to a VM on its own for the next hour), or pass --fallback vm to rerun it once automatically"},
 "nextTier":"vm","stdout":"","stderr":"","cpuMs":1.2,"wallMs":3.0,"deduplicated":false}}

$ legion session run code-runner --task run --file transcode.py --fallback vm -j
{"ok":true,"data":{"tier":"vm","tierReason":"fallback: stopped in the isolate, unsupported (subprocess.run)",
 "ranTwice":true, ...}}

$ legion session run code-runner --task run --file loop.py -j
{"ok":true,"data":{"task":"run","tier":"isolate","node":"w-17","exit":null,
 "stopped":{"reason":"limits","limit":"cpu","detail":"exceeded its 100ms CPU limit (100.4ms)",
            "hint":"raise config.limits.cpu, or rerun with --tier vm"},
 "nextTier":"vm","cpuMs":100.4,"wallMs":101.0,"deduplicated":false}}

$ legion session run code-runner --task run --file snippet.py --dry-run -j   # the pre-flight check only
{"ok":true,"data":{"tier":"isolate","imports":["json","csv"],"node":"w-17","room":true}}

$ legion isolate stats code-runner -j
{"ok":true,"data":{"task":"run","nodes":[
  {"node":"w-17","resident":48213,"active":312,"pagedOut":20114,"hibernated":391022,
   "invocationsPerSec":8410,"cpuSecondsPerSec":11.2,"vmRunsPerMin":14,"childVms":37,
   "cordon":"default","hostRssBytes":21474836480}]}}
```

A stopped run is `ok: true`: the code ran and was stopped, which is the
run's outcome, as a non-zero `exit` is (session-runs calls entry 4). Error
envelopes are for requests that did not run at all. `--fallback vm` is the
client's second request, never the server's. Child VMs are sessions of the
VM task, so `legion session get <isolate session>` lists them and `legion
session exec <child>` reaches one.

**As built (2026-11-11).** `legion session run` gained `--tier
auto|isolate|vm`, `--tenant KEY`, `--session ID`, `--arg ARG` (repeatable),
and `--fallback vm` (calls entries 6, 39); its JSON adds `tier`,
`tierReason`, `nextTier`, `vmMs`, `isolateStart`, and `ranTwice` only on
isolate replies, with fractional `cpuMs`/`wallMs`, and it exits 124 for a
wall stop and 125 for any other stop (calls entry 72). `legion isolate
stats <JOB> [--task T]` and `legion isolate get <SESSION_ID> [--on-node N]`
exist (calls entries 42, 71); child VMs of a session isolate are listed by
`isolate get`.

### 7.3 The RPCs

- **`Manager.RunSession` and `Worker.RunSession`** from the
  [session-runs plan](session-runs-plan.md), which an isolate task answers
  natively. The isolate adds `tier` (`auto` by default) to the request and
  `tier`, `tier_reason`, `next_tier`, and `vm_ms` to the reply. There is no
  server-side fallback field: a rerun is a second request.
- **`Worker.GetIsolateStats`** and the heartbeat's per-task aggregates
  behind `legion isolate stats`.
- The kill switch is the [runtime-disable plan](runtime-disable-plan.md)'s
  `legion operator runtime disable isolate`; an isolate task reacts to it as
  §13 describes.

**As built (2026-11-11).** `RunSessionRequest` also gained `tenant`,
`session_id`, and `args` (calls entry 39), and the reply `cpu_micros`,
`wall_micros`, and `isolate_start`; `RunStop` reasons `limits`,
`unsupported`, `trap`, and `ended`, with `limit` `cpu|wall|memory` (the
output budget truncates and never stops a run, calls entry 60);
`RunDryRun` gained `tier`, `tier_reason`, `imports`, and `missing_imports`.
The stats are `Manager.GetIsolateStats` and `Worker.GetIsolateStats`, and
`Manager.GetIsolate` / `Worker.GetIsolate` answer `legion isolate get`;
the heartbeat carries `isolate_tasks` and `isolate_hosts`. The node-local
worker↔host protocol is `proto/isolate_host.proto` (calls entry 10).
Since the hardening round, every session lookup (`GetInstance`, stop,
suspend, start, resize, promote, `GetIsolate`) answers a caller who
cannot read the session's job with the verb's ordinary `NOT_FOUND`, for
every runtime's sessions (calls entry 35); `HelloReply.pid`, shown as
`pid` in the stats, is the spawned supervisor's pid (calls entry 15).

### 7.4 The guest's world

The interface a component is written against, in WIT. Imports a world
does not use are not linked, so a component that imports something the
host does not provide fails at deploy, not at the call.

```wit
package legion:isolate@0.1.0;

interface vm {
  variant vm-error {
    not-configured,                      // the task has no VM task
    resource-exhausted(string),          // retryable: the VM task's envelope is full
    child-gone(string),                  // stopped, evicted, or its node was lost
    needs-restart(string),               // a resize past what the VM can do live
    failed(string),
  }
  record size { memory-mib: u32, memory-max-mib: option<u32>, cpu-mhz: option<u32> }

  resource machine {
    id: func() -> string;                // keep it in state to re-attach later
    exec: func(argv: list<string>, stdin: list<u8>, timeout-ms: u32) -> result<exec-result, vm-error>;
    put: func(path: string, bytes: list<u8>) -> result<_, vm-error>;
    get: func(path: string) -> result<list<u8>, vm-error>;
    fetch: func(port: u16, request: outgoing-request) -> result<incoming-response, vm-error>;
    resize: func(to: size) -> result<_, vm-error>;            // resize-plan
    release: func();
  }
  acquire: func() -> result<machine, vm-error>;               // a child session of config.vm.task
  attach: func(id: string) -> result<machine, vm-error>;      // after hibernation or a move
}

world handler {
  import wasi:clocks/monotonic-clock@0.2.0;   // frozen within a turn
  import wasi:clocks/wall-clock@0.2.0;        // coarsened to 1 ms
  import wasi:random/random@0.2.0;
  import wasi:cli/environment@0.2.0;          // the task's env
  import wasi:cli/stdout@0.2.0;               // to the task's log, and the run's reply
  import wasi:cli/stderr@0.2.0;
  import wasi:filesystem/types@0.2.0;         // in-memory scratch, counted in memoryMax
  import wasi:http/outgoing-handler@0.2.0;    // through the node egress datapath
  import legion:isolate/state;                // tenant- or session-scoped bytes
  import legion:isolate/vm;                   // child VMs
  export wasi:http/incoming-handler@0.2.0;    // or wasi:cli/run for commands
}
```

The same world exists in its WASI 0.3 form (async functions on the
stackless ABI) once the toolchains it needs support it (§6.6).

**As built (2026-11-11).** The contract is
`crates/isolate/wit/legion-isolate.wit` (`legion:isolate@0.1.0`): imports
`control` (`info()` with `resumed`, `park()`, `unsupported()`), `state`,
and `vm` as above (with `fetch` taking Legion's own request and response
records); an export `runner`, which a component
needs to be reused under `tenant` or `session` lifetimes and to report
exact exit codes (`wasi:cli/run` is single-shot and reports 0 or 1; calls
entry 40); worlds `image` and `imports`. The host links WASI 0.2 up to
0.2.12 (sockets linked but denying everything) and refuses an import it
does not provide at compile. There is no WASI 0.3 form (calls entry 7) and
no wasm threads.

### 7.5 Agents

- Runs are the session-runs plan's MCP tool `session_run`, with the
  isolate's `tier` and a `fallback` parameter (the tool's own second
  request, reported as `ranTwice`).
- `catalog()` gains `isolate stats`; `session run` and `operator runtime`
  come from their plans.
- `docs/agent-readiness.md` gains the isolate fields, including `nextTier`
  as the hint an agent should carry forward.

**As built (2026-11-11):** `legion_session_run` takes `tier`, `tenant`,
`session_id`, `args`, and `fallback` (refused with `session_id` or a dry
run); read-only tools `legion_isolate_stats` and `legion_isolate_get`;
catalog entries `isolate.stats` and `isolate.get`.

## 8. The control plane (kept small on purpose)

Isolates add no placement model. An `isolate` task is a session task:

1. **Envelopes reserve the room.** Submit generates `<task>-envelope` as
   for any session task. The envelope stays a reservation and runs
   nothing; the cordon host is node infrastructure, started by the worker
   when the first envelope that needs it lands, and its cgroup's
   `memory.max` and `cpu.weight` are the sum of the envelopes it serves.
   When a cordon runs several host processes (§6.3), the worker splits each
   job's envelope between them and moves room between them with the same
   grow-and-shrink rule elastic envelopes use; that split stays inside the
   worker. The VM task is an ordinary sibling session task with its own
   envelope; nothing is generated for burst.
2. **Routing is the session router's.** Runs and HTTP ingress go to any node
   with envelope room, picked by `stateful.placement`. A session isolate's
   id carries its node tag, so a later call goes straight to its node,
   through the router, the gateway, or an edge caller. The isolate stays the
   address after it starts a child VM.
3. **Starting an isolate takes no launch slot.** The launch pool bounds VM
   boots. An isolate start is admitted against the envelope by the host
   itself, and the host's own concurrency limits apply (§6.11). A VM, for a
   VM run or as a child, takes a launch slot like any sandbox session.
4. **Nothing per isolate leaves the node.** The heartbeat carries one
   aggregate per isolate task (`resident`, `active`, `paged_out`,
   `hibernated`, invocation and CPU counters, VM runs, child VMs, refusals),
   folded by the manager like usage. This is not a special case in the
   session code: a runtime declares the reporting level its sessions get
   (`indexed`, as every runtime has today, or `aggregate`), and the isolate
   runtime declares `aggregate`. A jobspec knob for it waits for a second
   runtime that wants one. `legion isolate get <id>` asks the node the id's
   tag names. Child VMs are sandbox sessions and are reported like any,
   with their owner reference.
5. **Hibernated session isolates are suspended sessions.** Their
   checkpoints live in the node content store like any session's, pinned,
   charged to the `snapshots` budget, and evicted by `suspend.keepFor`;
   `durability = "store"` publishes them with a session manifest, so the
   session can wake on another node and re-attach its child by id.
6. **Isolate sessions are never promoted to keyed instances.** A keyed
   instance is an allocation each, which would undo the density the tier
   exists for. A durable, named isolate uses what session durability
   already provides: an id derived from an idempotency key, and
   `durability = "store"`. `legion instance promote` on an isolate session
   answers `FAILED_PRECONDITION` saying so.

What the managers never see: an isolate's start, end, invocation,
page-out, or hibernation.

**As built (2026-11-11).** Items 2, 3, 4, and 6 are built as written
(the router reserves nothing for an isolate run, calls entry 55; session
isolates are found by their id's node tag, calls entry 42). Differences:
item 1's replica split only grows (no moving room between replicas), and
the host cgroup's `memory.max` is the sum of the envelopes with no
`cpu.weight` (calls entries 31, 53; since the hardening round it follows
envelope resizes); item 4's reporting
level is not a declared runtime property, the supervisor simply never
reports isolates to the index (calls entry 5); item 5 holds for `local`
only, because `durability = "store"` is not built (calls entry 58).

## 9. Proto, struct, config, and persistence changes

- **Proto.** The isolate's fields on `RunSessionRequest` (`tier`) and
  `RunSessionResponse` (`tier`, `tier_reason`, `next_tier`, `vm_ms`),
  `Worker.GetIsolateStats`, and `IsolateTaskStats` on the heartbeat's usage
  report, each with full public proto comments. Runs, owner references,
  resize, and the runtime disable come from their own plans.
- **Job config.** `config` for the `isolate` runtime (§7.1): `image`,
  `entry`, `module`, `lifetime`, `limits.{cpu, wall, output}`, `cordon`,
  and `vm.{task, memoFor}`, validated by the runtime's schema;
  `resources.memory` and `memoryMax` with the isolate meanings of §6.11.
  `children` and `run` come from their plans. New fields go into
  `configs/jobs/maximal.ncl` with the runtime.
- **Namespace policy.** `isolateCordons` (allowed cordon kinds) next to
  `runtimes`.
- **Worker config.** An `isolate` block: `binary` (default: beside the
  worker, then `LEGION_ISOLATE_BINARY`), `maxMemory` (per isolate, default
  4 GiB), `slotsPerHost`, `shards` (default: cores in the host's cpuset),
  `epoch` (default `1ms`), `slice` (default `5ms`), `pageOutAfter` (default
  `2s`), and `cordonBoundary = "process" | "vm"`.
- **Persistence.** None in the store. On the node: compiled modules and
  pre-initialized images and entries as derived content
  ([derived-content plan](derived-content-plan.md)), and hibernated
  isolates as session checkpoints in the node content store.
- **Struct.** `IsolateTaskConfig`, `IsolateLimits`, `IsolateVmConfig`,
  `IsolateStats` in `src/structs/`.

**As built (2026-11-11).** Proto as §7.3's note: more fields than listed
here, plus `GetIsolate` on both services, and a second, node-local proto,
`proto/isolate_host.proto`. Job config adds `config.env`. The structs live
in `crates/common/src/structs/isolate.rs`. The worker block is `isolate =
{ binary, maxMemory, slotsPerHost, shards, epoch, slice, pageOutAfter,
cordonBoundary, layout, confine, images, maxBodyBytes }`
(`configs/workers/default.ncl`); `cordonBoundary = "vm"` is refused, the
binary is looked up as `isolate.binary`, then beside the worker, then
`LEGION_ISOLATE_BINARY`, and images as `isolate.images`, then
`isolate-images` beside the binary, then `LEGION_ISOLATE_IMAGES` (calls
entry 63). On the node: compiled modules as derived content `cwasm`
(pre-initialized kinds have no builder), checkpoints in the content
store's `snapshots` class, and the session book in
`<state dir>/isolates/sessions.json`.

## 10. Target numbers per phase

Targets assume a 16-vCPU, 64 GiB x86-64 node with 4-level paging and
MPK, and are replaced by Phase 0's measurements.

| Phase | Start p50 | Resident per node | Waiting cost | Burst |
| --- | --- | --- | --- | --- |
| 0: measure | measured per image | measured bytes per idle isolate per image | measured fiber vs stackless | measured cold boot |
| 1: isolate runtime, one job per host | ≤ 20 µs (component) | ≥ 1.6×10^4 per host process (guard-page slots) | one fiber per in-flight call | none |
| 2: shared hosts, cordons | unchanged | unchanged per host, any mix of jobs | unchanged | none |
| 3: images and dynamic code | ≤ 300 µs JS pre-init, ≤ 1 ms JS dynamic, ≤ 5 ms Python | ≥ 500 JS per GiB | unchanged | none |
| 4: sessions and memory tiers | wake from page-out ≤ 200 µs, from hibernation ≤ 2 ms | ≥ 10^5 resident, ≥ 10^6 hibernated | no stack for stackless guests | none |
| 5: burst | unchanged | unchanged | unchanged | a VM run or child VM ≤ 250 ms cold; ≤ 50 ms from a warm snapshot (scaling Phase 8); ≤ 10 ms p50 on metal from a prepared VM |
| 6: density and hardening | unchanged | ≥ 2×10^5 resident (MPK striping) | unchanged | unchanged |

**As built (2026-11-11): container measurements, not the AWS rig.** The
targets above stand until the AWS run; beside them, a 4-vCPU Firecracker
guest container measured:

| Phase | Start p50 (container) | Resident (container) | Waiting cost (container) | Burst |
| --- | --- | --- | --- | --- |
| 0 | component 5.0 µs; `js` hello 1.14 ms fresh, 107 µs warm; `python` hello 2.55 ms fresh, 171 µs warm | idle per instance: component 33.8 KiB, `js` 767 KiB, `python` 1,519 KiB | fiber 480 ns per await; callback ABI 1.8 µs; a stack held per store either way | not measured (no KVM) |
| 1 | as Phase 0 | 21,870 guard-page slots per process (the worker defaults to 10,000) | one fiber per store that has made an async call | none |
| 3 | `js` 12 KB snippet 2.86 ms; `python` 12 KB snippet 12.0 ms | `js` ~1,300 per GiB | | |
| 4 | not measured (no swap for page-out; hibernation wakes a fresh instance) | no density campaign | stack held while waiting (calls entry 7) | |
| 6 | | MPK 328,842 slots per process with `vm.max_map_count` raised; small reservations 702,500 | | |

The JS dynamic target is missed and the Python target holds only for small
snippets; neither target has been rewritten, because the numbers come from
the wrong machine. End to end through a manager (debug builds, same
container; §4's last note): a Rust component 4.8 ms p50 and a Python hello
world 17.6 ms p50, so the Phase 1 start target is not shown met end to end.

## 11. Implementation phases

Each phase ships on its own and is useful on its own.

- **Phase 0: measure before building.** A spike binary, not merged into
  the worker, on the AWS rig's instance types (nested and metal): wasmtime
  pooling instantiation p50/p99 for a Rust component, the StarlingMonkey
  and QuickJS images, and CPython; private memory per idle instance per
  image; slots per process with guard pages, with MPK striping, and with
  small reservations; epoch-check overhead on a CPU benchmark; fiber
  versus stackless suspend and resume; V8 isolate creation and floor
  through the `v8` crate for the same JS, so the Phase 7 call rests on our
  own numbers; `legion-isolate` binary size with and without Cranelift;
  whether an idle `Store` keeps its worker fiber (and so a stack slot) on
  the callback ABI; whether the pool can place slots above 47 bits on a
  5-level-paging kernel; and Python's start with the standard library
  pre-initialized. Output:
  a findings doc and §4/§10 rewritten to measured numbers.
- **Phase 1: the `isolate` runtime, one job per host.** `legion-isolate`
  (host and `compile` subcommand), one host per (job, node) envelope, the
  shard model, pooling allocator with copy-on-write images, epoch
  preemption, limits, `wasi:cli/run` commands and `wasi:http` handlers
  from precompiled components, request lifetime, logs, egress through the
  worker datapath with PROXY v2 attribution, ingress dispatch, session
  runs on isolate tasks (with [session runs](session-runs-plan.md) Phase
  1), compiled modules as derived content (with
  [derived content](derived-content-plan.md) Phase 2), `isolate stats` and
  `GetIsolateStats`, catalog entries, and the public page
  `docs/task-runtime-isolate.md`. Closes the TODO "WASM runtime" item.
- **Phase 2: shared hosts and cordons.** Many jobs per host by cordon
  policy, per-job admission and per-tenant fair scheduling inside a host,
  the host's seccomp allowlist, Landlock, empty network namespace, and
  user namespace, namespace policy `isolateCordons`, and the isolate's
  reaction to `operator runtime disable` (§13; the switch itself is the
  [runtime-disable plan](runtime-disable-plan.md)'s Phase 1).
- **Phase 3: images and dynamic code.** The `js` and `python` images,
  stage-time pre-initialization of `entry` as derived content, dynamic
  snippets through session runs, compiled dynamic Wasm, and the pre-flight
  import check.
- **Phase 4: session isolates and memory tiers.** `lifetime = "tenant" |
  "session"`, the stackless async world, page-out and hibernation,
  host-held connections, gateway wake, `durability = "store"` through
  session manifests, hibernation as content-store checkpoints, the
  reclaim hook for page-out, and resizing a session
  isolate's `memory` and `memoryMax` once [resize](resize-plan.md) Phase 1
  lands.
- **Phase 5: burst.** Needs [child sessions](child-sessions-plan.md)
  Phase 1 and [session runs](session-runs-plan.md) Phase 1 (sandbox). `config.vm`
  and its validation; `tier = "vm"` runs as session runs on the VM task; `tier = "auto"` with the pre-flight check, the
  per-node memo, and `nextTier`; `legion:isolate/vm` (acquire, attach,
  exec, files, fetch, release) for child VMs; the client-side `--fallback
  vm`; `machine.resize` once [resize](resize-plan.md) Phase 1 lands; and
  prepared, single-use VMs once scaling Phase 8 lands.
- **Phase 6: density and hardening.** MPK striping, small-reservation
  slots, 5-level paging where the kernel offers it, `cordonBoundary =
  "vm"`, SELinux `execmem` denial for hosts, perf-counter quarantine, and
  the Spectre-mode knobs.
- **Phase 7 (conditional): a V8 engine** for JS tenants, built only when
  Phase 3's telemetry shows the trigger in calls entry 1.
- **Phase 8 (research): continuation migration**, resuming a hibernated
  isolate inside a VM-hosted `legion-isolate` with the full filesystem and
  process capabilities (§6.12, model D).

**As built (2026-11-11).** Phase 0 ran in a container only (the status
block at the top lists what). Phases 1 to 5 were built together rather
than shipped one by one; the cordon model of Phase 2 replaced Phase 1's
"one host per job" from the start (`cordon = "job"` is that shape). Not
built from these phases: stage-time pre-initialization of `entry` and
compiled dynamic Wasm (Phase 3); the stackless world, host-held
connections, gateway wake, and `durability = "store"` (Phase 4);
`machine.resize` is built, prepared VMs are not (Phase 5); a compile
prefetch before a host upgrade (§13). Phase 6 has only the layouts as
config.

## 12. Testing plan

- **Unit (Phase 1+).** Limits arithmetic and trap classification
  (`describe_failure`'s successor for wasmtime traps, epoch deadlines,
  memory refusals); admission against an envelope with the reservation
  and the ceiling distinct; the shard scheduler's fairness (deficit round robin
  across tenants under a synthetic load, asserting CPU shares within 5%);
  idempotent invocation ids; the egress attribution header round trip.
- **Integration (`tests/integration/isolate.rs`, one module of the shared
  binary).** A component answers HTTP through ingress; a component's
  outbound request reaches an upstream with an injected credential the
  guest never sees; CPU, wall, memory, and output limits each produce a
  `stopped` reply; a host process killed mid-invocation fails its
  in-flight runs with a retryable error and the worker restarts it; a job
  in one cordon cannot be scheduled into another's host; a session run on
  an isolate task through the manager and at the edge with a ticket;
  `instance promote` on an isolate session refused; the ACL table entry for
  every new Manager RPC in `tests/integration/acl_endpoints.rs`.
- **Restore hygiene (shared).** One conformance test,
  `tests/integration/restore_hygiene.rs`, for every way a unit is made from
  a snapshot: a VM fork, a prepared VM, and an isolate from a
  pre-initialized image. Two units from one snapshot never share RNG
  output, identity tokens, or instance ids. Isolates add their case to it;
  they do not get a test of their own.
- **Confinement (Phase 2).** A test build of the host with a hook that
  attempts each forbidden syscall (`socket`, `openat` outside the cache,
  `execve`, `ptrace`, anonymous `PROT_EXEC`) and asserts the kill; the
  host's network namespace has no interface.
- **Guest-escape regression corpus.** Known-bad modules (out-of-bounds
  loads at every width, table index masking cases, the CVE-2023-26489
  reproduction) run against every engine upgrade in CI.
- **Fuzzing.** `cargo fuzz` targets for each host function's argument
  decoding and for the WIT boundary, run nightly.
- **Benchmarks.** `benchmarks/micro/isolate_bench.rs`: instantiate per
  image, turn switch, page-out and wake, hibernate and wake, egress round
  trip through the worker. Scheduler-style before/after numbers in every
  PR that touches the host's hot path.
- **Density campaign (Phase 4, 6).** An AWS run that fills a node with
  session isolates to its memory budget and reports resident, paged, and
  hibernated counts, wake latencies, and host RSS per isolate.
- **Burst (Phase 5).** Ignored-by-default KVM tests: a `tier = "vm"` run
  returns the same reply shape as an isolate run; `auto` sends a run with
  an unsupported import to a VM before it starts; a stopped isolate run
  carries `nextTier` and is never re-run by the server, and `--fallback vm`
  reruns it once with `ranTwice`; a VM run's VM is never reused for another
  run; `vm.acquire` and exec; a session isolate keeps its child across
  hibernation and re-attaches by id; stopping the isolate ends its child;
  `child_gone` after the child is stopped; promoting an owner with a
  child is refused until child sessions Phase 2.

**As built (2026-11-11).** What exists, and where it differs:

- **Unit and host tests** are in `crates/isolate/tests/` (`run`,
  `admission`, `lifetimes`, `http`, `scratch`, `binary`, `escape_corpus`)
  against the real host library and checked-in guest fixtures, and in
  `crates/worker/src/worker/isolates/` (supervisor with a fake host,
  egress, ingress, child VMs). Fair scheduling is a shard unit test.
  Invocation-id idempotency in the host is not built (calls entry 68).
- **Integration** is `tests/integration/isolate.rs` (the supervisor
  against the real `legion-isolate` binary; its real-sandbox VM-tier tests
  are ignored for `/dev/kvm`, and an `exec` task stands in for the VM task
  in the rest), `tests/integration/isolate_images.rs` (the built images
  through manager, worker, and host; ignored unless the images are built),
  and
  `tests/integration/isolate_control.rs` (manager, routing, ACLs,
  promote refusal, runtime disable, child owners, credentials), plus the
  ACL table entries.
- **Restore hygiene** is tested in `crates/isolate` (`tests/run.rs` and
  the image tests), not in a shared `tests/integration/restore_hygiene.rs`,
  which does not exist (calls entry 62).
- **Confinement** tests need `--features confinement-test`; the
  anonymous `PROT_EXEC` case only reports that it is not denied.
- **The escape corpus** exists; **fuzz targets do not**.
- **Benchmarks** are `crates/isolate/benches/isolate_bench.rs`, not
  `benchmarks/micro/`.
- **Language images** are tested by `crates/isolate/tests/images.rs`,
  ignored unless the images are built.
- **The density campaign** and **the KVM burst tests** have not run (no
  AWS run, no `/dev/kvm`).

**As built, hardening round (2026-11-11): what ran.** In the 4-vCPU
build container, as reported by the agents that ran them:

- **`crates/isolate`** with `--features confinement-test`: lib 49,
  `admission` 8, `binary` 19 (each confinement layer shown to stop its
  attacks alone through the test-only `--test-skip`; a stray inherited
  descriptor closed; killing the spawned pid kills the host; the compiler
  honours `--confine`), `escape_corpus` 2, `http` 10, `lifetimes` 23,
  `run` 20, `scratch` 1. Ignored unless the images are built, and passing
  when run: `images` 20 and `binary`'s
  `both_language_images_run_in_the_confined_binary_test` (both images
  compiled and served under `enforce`).
- **Worker unit tests** (`isolates::`, session runs, children, netd,
  pressure, edge, the API socket): 281 passed.
- **Integration** (`isolate::`, `isolate_control::`, `session_runs::`,
  `child_sessions::`, `worker_plane_classes::`, `runtime_disable::`,
  `config_jobs::`): 66 passed, 4 ignored, two of them the KVM isolate
  tests. `isolate::` now drives a manager on raft, a real worker, and real
  hosts through `RunSession` (its `cluster` harness), and covers HTTP in
  through the supervisor's ingress socket and out through the real
  `NetdEgress` with an injected credential the guest never sees.
- **`tests/integration/isolate_images.rs`** (ignored unless the images
  are built; passing when run): Python and JS runs, exit codes, the CPU
  limit, `unsupported`, dry-run imports, session globals lost and
  `legion.state` kept across hibernation, and the end-to-end timing.
- **The VM tier** ran with an `exec` session task standing in for the
  sandbox (calls entry 65).

Not covered end to end: netd's own TCP ingress listener in front of the
supervisor's socket; the heartbeat path that installs an upstream
credential (the test installs it directly); re-attaching a child by id
after hibernation; a real sandbox VM; the host cgroup and page-out (the
container has cgroup v1 and no swap).

## 13. Rollout and backward compatibility

- **New surface only.** No existing job, runtime, or default changes.
  Network plugins keep wasmi.
- **Default on where it can run.** The runtime loads on a worker that finds
  `legion-isolate` beside it (or through `LEGION_ISOLATE_BINARY`), as the
  sandbox runtime loads where it finds `legion-guest`. A worker without the
  binary does not advertise the `isolates` capability, so envelopes are
  not placed there. A namespace policy keeps namespaces off the tier
  (`runtimes`) or limits cordon kinds (`isolateCordons`).
- **The kill switch** is `legion operator runtime disable isolate
  [--namespace N]` ([runtime-disable plan](runtime-disable-plan.md)): new
  isolate starts are refused with `runtime_disabled` (retryable), and
  `--drain` hibernates session isolates and lets request isolates finish
  within `limits.wall`. The isolate runtime adds one reaction of its own: a
  `tier = "auto"` run of a task with a VM task runs in the VM instead,
  decided before it starts like any tier choice. This is the CVE response.
- **Engine upgrades.** The compiled-module cache is keyed by engine
  version, so a new `legion-isolate` recompiles on first use per node;
  Phase 1 adds a prefetch that compiles a job's module for the new version
  before the old host drains. Isolate checkpoints record the engine version
  and image digest; a mismatch wakes the isolate fresh with an `onResume`
  notice rather than restoring incompatible memory.
- **Breaking changes.** None; there is no installed base of isolates.

**As built (2026-11-11).** The capability is named `isolates`, and the
binary is looked up as the worker's `isolate.binary`, then
`legion-isolate` beside the worker, then `LEGION_ISOLATE_BINARY`; images
as `isolate.images`, then `isolate-images` beside the binary (with its
`manifest.json`), then `LEGION_ISOLATE_IMAGES` (calls entry 63). Neither
ships in a release yet. The kill switch's `auto` reaction is built, and
`--drain` hibernates only the covered jobs' session isolates (calls entry
43). Engine upgrades: the `compatKey` keys the cache as planned, but the
prefetch is not built, and a checkpoint from another engine or module
version is restored at the state level, not refused (calls entry 9). One
small break outside isolates: the idempotency fingerprint of every session
run now includes `tier`, `tenant`, `session_id`, and `args` (calls entry
39).

## 14. Open questions

1. **StarlingMonkey or QuickJS for the default `js` image?** Lean:
   StarlingMonkey for Web API compatibility and weval's speed, with QuickJS
   as `js-small` if Phase 0 shows its idle footprint is several times
   smaller. Phase 0 decides.
2. **Shared pool envelopes for many small jobs.** A FaaS platform with one
   Legion job per customer cannot afford an envelope per job per node.
   Lean: a namespace-level isolate envelope that every isolate task in the
   namespace draws from, with per-job quota from namespace policy; its own
   plan after Phase 2.
3. **UDS framing or a shared-memory ring between host and worker?** Lean:
   HTTP/2 over a socketpair first; a ring only if the worker's CPU per
   egress request exceeds 20 µs at the target rate.
4. **Python packages with native code.** Lean: curated images
   (`python-data` with numpy and pandas built for WASI) when Phase 3 finds
   working builds, and VM runs for everything else; never a package
   installer in the isolate.
5. **Should session isolates ever appear in the managers' index?** Lean:
   no; revisit only if a platform needs cluster-wide listing and can name
   the count.
6. **Hyperlight micro-VMs as the cordon boundary instead of a microVM per
   cordon.** Lean: revisit at Phase 6 if Hyperlight's Linux/KVM support
   and host-call model are stable.
7. **Per-tenant memory enforcement inside a shared host.** The kernel's
   memory controller is per process, so per-job memory inside a shared
   host is enforced by the host's own accounting, with the cordon's cgroup
   as the backstop. Lean: accept, state it in the public doc, and keep
   `cordon = "job"` for jobs that need the kernel to enforce their
   envelope.
8. **When does aarch64 join?** Both April 2026 critical escapes were
   aarch64-only or Winch-only (October's hit every architecture), and
   OSS-Fuzz has no aarch64 runners. Lean:
   x86-64 only until continuous fuzzing covers aarch64 Cranelift and
   Phase 0's numbers hold on Graviton; aarch64 hosts meanwhile run the
   isolate tier only with `cordon = "job"`, or not at all.
9. **Should `tier = "vm"` runs ever reuse a VM?** A single-use VM per run
   is what keeps one tenant's code from running in a VM another tenant's
   code just used. Lean: never across tenant keys; reuse within one tenant
   is what a session isolate with a child VM already gives, so no separate
   reuse mode.
10. **Should a VM task inherit the isolate task's `connect`?** Lean: no.
    Children run under their own task's policy (child-sessions calls entry
    3), and a Nickel binding shares the block in one line (§7.1); the
    public doc shows the binding.
11. **Should the pre-flight check run on the router instead of the node?**
    It would let `auto` pick a node that has VM room for code that needs a
    VM. Lean: node first (the image module lists are there); revisit if
    VM-tier runs are often refused for room on the node the router picked.

**As built (2026-11-11): what the work answered.**

- **Q1:** StarlingMonkey stays the `js` image (without weval AOT, calls
  entry 46). QuickJS measured 5.1× smaller idle (127.6 against 651.2 KiB)
  and faster on dynamic snippets, which meets the stated condition, but
  `js-small` was not shipped; it is an open item.
- **Q3:** gRPC over two inherited socketpairs (calls entry 10). No
  measurement of the worker's CPU per egress request yet, so no ring.
- **Q4:** no curated images; the `python` image is the standard library
  only (calls entry 47).
- **Q5:** no; session isolates are not in the index (calls entries 4, 42).
- **Q7:** accepted as leaned: per-job memory in a shared host is the
  host's accounting, and the cordon cgroup is the backstop (calls entry
  31).
- **Q8:** x86-64 Linux only.
- **Q9:** a VM run is never reused (single-use session runs).
- **Q10:** the lean holds, but the Nickel binding it relies on does not
  validate yet (calls entry 64).
- **Q11:** node first, as leaned.
- Q2 and Q6 are untouched.

## 15. Acceptance criteria

- **Phase 0:** a findings doc with the measurements listed in §11, and §4
  and §10 rewritten to them, with a calls entry for any target dropped.
- **Phase 1:** `legion session run` on a Rust component task returns in
  under 2 ms end to end on one node; `isolate_bench` shows instantiation at
  or under the Phase 0 target; an outbound request carries an injected
  credential the guest never read; every limit produces its `stopped`
  reply;
  `docs/task-runtime-isolate.md` exists and stands alone.
- **Phase 2:** two jobs in the same namespace share one host process and
  two namespaces do not; the confinement test kills the host on each
  forbidden syscall; `legion operator runtime disable isolate` refuses new
  isolate runs within one heartbeat, and a `tier = "auto"` run of a task
  with a VM task runs in the VM instead.
- **Phase 3:** `session run --file x.js` and `--file x.py` meet the §10
  start targets; a dynamic Wasm module runs on its first call after one
  Cranelift compile, and later calls reuse the cached artifact.
- **Phase 4:** a node holds the §10 resident and hibernated counts in the
  density campaign; a hibernated session isolate wakes within target and
  keeps its state; an isolate awaiting a 20 s upstream holds no thread
  (shard thread count constant under 10^4 concurrent awaits).
- **Phase 5:** a snippet importing a package the image lacks runs in a VM
  on its first call with `tierReason` naming the import; a snippet calling a
  subprocess is stopped in the isolate with `nextTier = "vm"` and is not
  re-run unless the caller asks; `vm.acquire` returns a machine that runs
  `exec`, attributed to the isolate by its owner reference; requests for a session
  isolate keep reaching it after it starts a child; a VM given the same
  `connect` binding reaches the same upstream with the same injected
  credential.
- **Phase 6:** slots per host with MPK striping at least 5× the guard-page
  layout's, measured; `cordonBoundary = "vm"` runs a cordon host inside a
  microVM end to end.

**As built (2026-11-11): where each criterion stands.**

- **Phase 0:** a findings doc exists, from a container, not the AWS rig;
  §4 and §10 carry the container numbers beside the targets instead of
  being rewritten.
- **Phase 1:** `docs/task-runtime-isolate.md` exists; every limit produces
  its reply (`output` truncates instead, calls entry 60); `isolate_bench`
  exists. The 2 ms end-to-end run through the manager and the injected
  credential through the real datapath with the real host had not been
  measured or run end to end when this was written.

**As built, hardening round (2026-11-11): where each criterion stands
now.** Per phase, met, met in part, or not verified:

- **Phase 0: met in part.** Container findings only; the AWS run and the
  rewrite of §4 and §10 are open.
- **Phase 1: met in part.** *Not met as measured:* `legion session run`
  on a Rust component through a manager took 4.8 ms p50 (min 4.4 ms)
  with debug builds of the worker and the host in a 4-vCPU container; no
  release-build end-to-end number was taken, so under 2 ms is not shown.
  *Met:* `isolate_bench` instantiation (5.0 µs p50 from `InstancePre`,
  against the ≤ 20 µs target); an outbound request through the real
  egress datapath carried an injected credential the guest never read
  (installed directly rather than by the heartbeat); every limit produces
  its stop (`output` truncates); the public doc stands alone.
- **Phase 2: met.** Cordons share a host per namespace and not across
  namespaces, against real hosts; the confinement tests kill the host on each
  forbidden syscall and show every layer stopping its attacks alone; the
  disable refuses isolate runs and sends `auto` runs of a task with a VM
  task to the VM (the "within one heartbeat" timing was not measured).
- **Phase 3: met in part.** Both images run end to end; the JS start
  target is missed and the Python one met only for small snippets
  (container numbers; a Python hello world is 17.6 ms p50 end to end
  with debug builds); dynamic Wasm modules as runs are not built.
- **Phase 4: met in part.** A hibernated session isolate wakes keeping
  its `state` (and losing its globals) end to end; no density campaign;
  wake latency against the 2 ms target not measured; an awaiting isolate
  holds a stack (calls entry 7).
- **Phase 5: not verified on a VM.** The tier logic and child-VM
  ownership pass end to end with an `exec` stand-in (calls entry 65),
  including "a stopped run is not re-run" and `nextTier`; no sandbox VM
  ran; the shared-`connect` criterion fails by construction (calls
  entry 64).
- **Phase 6: met in part.** MPK slots measured at 15× guard pages;
  `cordonBoundary = "vm"` is not built and is refused by worker-config
  validation.
- **Phase 2:** cordons share a host per namespace and confinement kills
  on forbidden syscalls in the binary tests (`--features
  confinement-test`); the disable refuses isolate runs and sends `auto`
  runs of a task with a VM task to the VM.
- **Phase 3:** the JS start target is missed and the Python one met only
  for small snippets (container numbers); dynamic Wasm modules as runs are
  not built.
- **Phase 4:** not met as written: no density campaign, and an isolate
  awaiting an upstream holds a stack (calls entry 7).
- **Phase 5:** built but not run against a real VM (no `/dev/kvm`); the
  shared-`connect` criterion fails by construction today (calls entry 64).
- **Phase 6:** MPK slots measured at 15× guard pages in the container
  (with `vm.max_map_count` raised); `cordonBoundary = "vm"` is not built.
