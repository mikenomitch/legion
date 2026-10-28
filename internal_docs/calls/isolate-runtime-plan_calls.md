# Judgement Calls: isolate-runtime-plan

Decisions made while designing
[`plans/isolate-runtime-plan.md`](../plans/isolate-runtime-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** These are design-time calls; implementation inserts
its own at their rank. The research behind them (2026-10-28) is cited in
the plan's §2 and §6.

## 1. The engine is wasmtime; JS and Python run as interpreters inside Wasm, not on V8

**Call.** Isolates are WebAssembly components on wasmtime (pooling
allocator, copy-on-write images, Cranelift ahead of time). JavaScript runs
as SpiderMonkey compiled to Wasm (StarlingMonkey, with weval), Python as
CPython for WASI, both pre-initialized. V8 is not embedded.

```nickel
config = { image = "js", entry = "handler.js" }   # SpiderMonkey-in-Wasm, not V8
```

**Alternatives.**
- (a) V8 isolates, Cloudflare's design: the only engine with a production
  record of thousands of tenants per process, native async through
  promises, and a JIT. Embeddable through the `v8` crate or `deno_core`.
- (b) Both engines from the start, V8 for JS and wasmtime for everything
  else.
- (c) Hyperlight micro-VMs, for a hardware boundary per tenant.

**Trade-off.** JS throughput. StarlingMonkey in Wasm scores about 1/75 of
V8's JIT on Octane before weval, which buys back 2 to 3×. CPU-bound JS is
much slower in an isolate than it would be on V8. Accepted, with two
answers: CPU-bound JS bursts to a VM running Node with V8's JIT behind a
hardware boundary (`burst.on = ["limits"]`), and Phase 7 adds V8 as a
second engine if the trigger below is met.

**Why (performance at the density end, simplicity, security).**
- **Start:** 5 µs to a fresh Wasm instance against 0.7 to 5 ms for a V8
  isolate from a snapshot. That is what makes an instance per request
  affordable, and so what makes the safe lifetime the default (entry 10).
- **Density:** an idle instance costs only the pages it wrote; a V8
  isolate's floor is megabytes before user code, and V8's 4 GB shared cage
  caps the process. Lifting the cage (IsolateGroups) is unsupported
  upstream and unreleased in the Rust crate.
- **Containment:** a JS or Python engine bug, the class behind about half
  of Chrome's in-the-wild 0-days, corrupts one isolate's linear memory
  instead of the shared process. Google's own advice to high-risk users is
  to turn V8's optimizer off; this design never has one in the process.
- **Fit:** wasmtime's runtime-only build is about 2 MB of Rust; V8 roughly
  doubles the binary and adds a C++ build.
- Against (c): Hyperlight measured 9.1 ms cold for its Wasm guest, costs
  megabytes per VM, takes a VM exit per host call (two on nested
  virtualization), and is pre-1.0. It is watched as a cordon boundary
  (plan §14 Q6), not as the isolate tier.

**Revisit (the Phase 7 trigger).** Build V8 when, over a month of Phase 3
telemetry, more than 20% of JS CPU-seconds come from invocations that
spend over 10 ms of CPU and those tenants have not moved to burst, *and*
upstream supports more than one pointer-compression cage per process.

---

## 2. Tenants share a process within a cordon; the cordon is the trust unit, and no secret enters it

**Call.** One `legion-isolate` host process serves every isolate task in
a **cordon**: by default one per namespace. `cordon = "job"` gives a job
its own host; `cordon = "shared"` mixes namespaces, only where namespace
policy allows. Credentials never reach the host: the worker injects them
on egress.

```nickel
config = { cordon = "namespace" }   # default | "job" | "shared"
```

```text
[isolate] code-runner/run: config.cordon = "shared" is refused by namespace policy 'tenants' (isolateCordons = ["namespace", "job"])
```

**Alternatives.**
- (a) One host per job, the Phase 1 shape: simple, and the kernel
  enforces each job's envelope.
- (b) One host per node for everyone, the densest.
- (c) A cordon per tenant key inside a job.

**Trade-off.** Tenants in one cordon are not safe from a determined
Spectre attacker: researchers leaked a JWT across tenants in Cloudflare's
production Workers in 2026 at up to 12 bit/s. Per-job memory inside a
shared host is enforced by the host's accounting, not the kernel. And a
platform job with 10^4 small Legion jobs (one per customer) gets no density
from this, because each job still needs an envelope (plan §14 Q2).

**Why (performance, with security setting the boundary).** "Many tenants
per process" is the requirement, and (a) defeats it for any platform with
many jobs. (b) puts a free-tier tenant beside one handling sensitive data,
which Cloudflare refuses to do and which Google says only process
separation fixes. The namespace is the boundary Legion already uses for
ACLs, quotas, and policy, so an admin reasons about one unit. Keeping
secrets out of the process means the worst cross-tenant outcome is reading
another tenant's in-cordon data, never a credential; the public doc states
exactly that.

---

## 3. Isolates ride session mode and are invisible to raft and to the managers' index

**Call.** An `isolate` task must be `stateful.mode = "session"`. It gets
an envelope per node like any session task; the session router or an edge
caller picks the node; session isolates carry the node tag in their id.
Nothing per isolate or per invocation is written to raft or kept in the
managers' session index. Managers see one aggregate per task per node on
the heartbeat.

```console
$ legion isolate stats code-runner -j
{"ok":true,"data":{"task":"run","nodes":[{"node":"w-17","resident":48213,"active":312,
 "pagedOut":20114,"hibernated":391022,"invocationsPerSec":8410, ...}]}}
```

**Alternatives.**
- (a) A new placement model for isolates (a "function" object, a
  dedicated router).
- (b) Report session isolates in the managers' index like sandbox
  sessions, so `instance list` shows them.

**Trade-off.** No cluster-wide list of isolates; `legion isolate get <id>`
asks the node the id names. A node that dies loses its `local` sessions'
hibernation files, as for any `local` session.

**Why (simplicity, performance).** Session mode was built for "a tool
execution ... or a function invocation" and already has envelopes, routing
by id, edge starts with offline tickets, idle suspend, and manifests. A
new model would duplicate all of it. Against (b): the index costs about
200 bytes per running session per manager; 10^5 isolates on each of 100
nodes is 2 GB per manager, for a list nobody reads at that size.

---

## 4. The host is a separate process and binary with no network; the worker does all I/O

**Call.** Isolates run in `legion-isolate`, a binary shipped beside the
worker (like `legion-guest`), in host processes the worker starts and
confines: an empty network namespace, a seccomp allowlist, Landlock
read-only on the artifact directory, no `execve`. Every outbound request
goes over a socketpair to the worker, tagged with PROXY v2 TLVs naming the
isolate, and the worker applies the job's `connect` policy and credential
injection. The worker binary does not gain wasmtime; network plugins stay
on wasmi.

**Alternatives.**
- (a) Isolates inside the worker process: no IPC, and runtime-only
  wasmtime is only about 2 MB.
- (b) The host does its own egress under policy pushed from the worker:
  one hop fewer.
- (c) The host role inside the main `legion` binary, compiler separate.

**Trade-off.** A local hop of roughly 10 to 30 µs and worker CPU per
outbound request; another binary to ship; the worker supervises more
processes.

**Why (security first, then simplicity).** (a) makes an engine escape a
worker compromise: every allocation's control and the node's credentials.
(b) puts credentials and network access into the multi-tenant process,
the two things the design keeps out of it (entry 2). A separate binary
keeps [mesh-interfaces entry 22](mesh-interfaces-plan_calls.md#22-the-plugin-engine-is-wasmi-an-interpreter-not-wasmtime)'s
reasoning intact for the worker, which still grows by nothing, while the
isolate host is exactly the case that entry said would justify wasmtime:
compute-heavy guests where an interpreter's 10× matters. Against (c): it
maps the whole worker into every multi-tenant process and couples the
host's upgrade to the worker's, and a fast engine patch (entry 2's
mitigation for an engine CVE) wants the host upgradeable on its own.

---

## 5. Burst is an attached VM plus replay before egress; a running isolate never migrates

**Call.** Three mechanisms. The guest can call `vm.acquire()` and drive a
microVM of the task's burst image (exec, files, HTTP). An invocation that
fails `unsupported` or `limits` is replayed in a VM only if it made no
egress (`replay = "before-egress"`). A pre-flight import check and a
per-node memo of code hashes route known VM work to a VM first. Moving a
live isolate's state into a VM is deferred to Phase 8 research.

```console
$ legion isolate run code-runner --file transcode.py -j
{"ok":true,"data":{"tier":"vm","burst":{"reason":"unsupported",
 "detail":"subprocess.run: processes are not available in an isolate","replayed":true}, ...}}
```

**Alternatives.**
- (a) Continuation migration: hibernate the isolate and resume the same
  instance inside a VM with more capabilities.
- (b) Replay always, or never.
- (c) Burst only by explicit API.

**Trade-off.** Replay runs code twice, so it is narrow: a snippet that
called an API and then shelled out fails `isolate_unsupported` instead of
bursting, unless the author sets `replay = "always"` and owns the
idempotency. The memo is node-local, so another node relearns with one
wasted run.

**Why (ease, grounded in prior art).** No production system migrates
isolate memory into a VM; Cloudflare's Project Think, Containers, Modal,
Vercel, and Deno all escalate explicitly and hand over files. (a) needs
host-held sockets re-bound across a machine boundary and identical engine
versions on both sides. (c) makes every author predict the tier, which is
the job the agent-platform persona asked not to do. Replay before egress
is the one automatic rule that is always safe.

---

## 6. Waiting holds nothing: a host-driven event loop on the stackless callback ABI, fibers as the fallback

**Call.** The host drives each guest's event loop. Guests on the WASI 0.3
Component Model async callback ABI run a turn, return to the host when
they await, and hold no native stack between turns. WASI 0.2 guests that
block in sync imports run on a fiber per in-flight call (128 KiB stacks,
pooled).

**Alternatives.**
- (a) Fibers for everyone, wasmtime's mature async model.
- (b) Wait for Wasm stack switching.
- (c) Asyncify-style transforms.

**Trade-off.** The newest code path in the engine: WASI 0.3 shipped
2026-06-11, and a critical guest-controlled write to the host's native
stack in `component-model-async` was fixed on 2026-10-02
([CVE-2026-115384](https://github.com/bytecodealliance/wasmtime/security/advisories/GHSA-32h6-97mm-8q3c)).
The compiler process validating every component with the newest
`wasmparser` (entry 9) is the published mitigation and is applied to all
modules. Two async paths to test and support.

**Why (performance at density).** With fibers, an isolate awaiting a 20 s
LLM call pins a slot and a stack, and 10^5 suspended fibers are about 12.5
GiB of stack address space. With callbacks the isolate is data between
turns, which is also what lets it hibernate *while it waits* (entry 11).
(b) is a Phase 3 proposal with no tiered wasmtime support; (c) costs code
size and speed on every call.

---

## 7. A new runtime, `isolate`, that bursts to `sandbox`, not a mode of the `sandbox` runtime

**Call.** The request was "a sandbox runtime with two modes". The plan
ships it as two runtimes that act as one product: `runtime = "isolate"`
with a `burst.sandbox` block holding an unchanged `sandbox` config.

```nickel
runtime = "isolate",
config = { image = "python", burst = { sandbox = { image = "python:3.13-slim" } } },
```

**Alternatives.** `runtime = "sandbox"` with `config.mode = "isolate" |
"vm"`, which matches the request's words.

**Trade-off.** Users meet two runtime names for one idea, and the docs
must present them as one tiered system.

**Why (simplicity of the surface and the code).** The two modes share no
config fields beyond `env`: one has `image`, `kernel`, `vcpus`, and block
volumes; the other has `module`, `limits`, `lifetime`, and `cordon`. A mode
switch would make every field conditional on another. `TaskPlugin`
declares capabilities per runtime, not per config (`supports_signal`,
`file_delivery_root`, live restart), and the answers differ. Namespace
policy's `runtimes` allowlist can then admit isolates without VMs or the
reverse.

---

## 8. Slots use guard pages by default; MPK striping is Phase 6; the layout belongs to the cordon, not the job

**Call.** The pooling allocator uses wasmtime's default guard-page layout
(about 16,000 to 32,000 slots per process); a cordon that needs more runs
more host processes. MPK striping (ColorGuard, about 15× more slots) is
built in Phase 6 behind its own soak. Small reservations with explicit
bounds checks are an operator density mode for `shared` cordons only. No
job can pick its layout.

**Alternatives.**
- (a) MPK striping from Phase 1, the obvious density win, and per the
  paper also a speculative-access barrier between neighbours.
- (b) Small reservations with explicit bounds checks as the default:
  millions of slots.

**Trade-off.** Fewer isolates per process until Phase 6, and more
processes per cordon meanwhile (ColorGuard measured one MPK process about
29% faster than 15 processes).

**Why (security over density, for now).** The density-tuned layouts are
the least-tested paths: wasmtime marks MPK experimental, and a 2026 leak
between pooled slots appears only with zero guard and a reservation under
4 GiB. Guard pages make heap accesses Spectre-safe by construction with no
run-time cost; explicit checks cost 1.6 to 1.8× with Spectre guards. MPK's
protection also assumes no control-flow hijack (a guest that can jump to a
`wrpkru` gadget disables it), the same caveat Cloudflare's threat model
states. Making the layout a cordon property means one operator decision,
not a per-job dial nobody can audit.

---

## 9. Code is compiled by a node-local compiler process with Cranelift only, x86-64 first; user `.cwasm` is never loaded

**Call.** `legion-isolate compile` runs as a short-lived, confined process
on the node and writes artifacts into the local content store, keyed by
module digest, engine version, and CPU features. The host role never
compiles. Cranelift is the only compiler; Winch and Pulley are off. x86-64
is the first supported host architecture.

**Alternatives.**
- (a) Compile once per cluster (a manager or a `legion-system` app) and
  distribute signed artifacts.
- (b) Winch for fast first compiles of dynamic modules.
- (c) wasmi beside wasmtime for first runs, no compile at all.

**Trade-off.** Every node compiles each module once, and a dynamic Wasm
module pays a Cranelift compile on its first call. aarch64 nodes wait.

**Why (security).** Deserializing a `.cwasm` is arbitrary code execution
by design, so the artifact must never cross a trust boundary; with (a) a
signature check is all that stands between a bucket writer and every node.
Winch is incompatible with epoch interruption and had a 2026 sandbox
escape. (c) puts two engines' bugs in one trust boundary for a rare path:
dynamic code is mostly JS and Python source, which needs no compile.
aarch64 had both April 2026 critical escapes and no continuous fuzzing.

---

## 10. The default lifetime is one instance per request

**Call.** `lifetime = "request"` by default: a fresh instance per
invocation, destroyed after. `"tenant"` (reused per tenant key while warm)
and `"session"` (addressable, hibernating) are opt-in.

**Alternatives.** Workers' model as the default: a long-lived isolate per
script, globals persisting across requests.

**Trade-off.** No in-memory caches across requests by default, and images
with costly per-call setup (Python) pay it every time unless the author
opts into `tenant`.

**Why (security, made affordable by entry 1).** An instance that lives
for one request cannot accumulate a side channel's bits across requests,
and a corrupted one dies with its request. Fastly's argument, and the 2026
Workers attack needed a connection held open for hours. At 5 to 30 µs per
instance the safe default costs almost nothing.

---

## 11. Idle memory leaves in four tiers, and hibernation happens at turn boundaries

**Call.** Hot, then paged out (`MADV_PAGEOUT` to zswap/zram/swap) after
`pageOutAfter`, then hibernated (private pages diffed against the image,
plus host-side state, compressed to a file; slot freed) after
`suspend.idle.after` or under slot or memory pressure, then evicted or
published per `suspend.keepFor` and `durability`. Host-held sockets stay
open across hibernation.

**Alternatives.** Cloudflare's model: LRU-evict whole isolates and let the
next request start fresh.

**Trade-off.** The paged-out tier depends on the node having compressed
swap or swap; without it, `MADV_PAGEOUT` does nothing for anonymous memory
and the tier collapses into hot. Hibernation files cost disk.

**Why (performance at density).** Eviction is fine for stateless
handlers, which already get `request` lifetime. For a million resident
agents it throws away exactly the state the platform persona wants kept.
Reusing session sleep's vocabulary (`idle.after`, `keepFor`, `durability`)
keeps one model for both tiers.

---

## 12. Preemption by epoch, yielding at the end of a slice; fuel only on request

**Call.** A ticker advances the engine epoch every millisecond; a turn
that exceeds its 5 ms slice yields to its shard instead of trapping, and
the invocation's CPU budget is checked at each yield.

**Alternatives.** Fuel metering, as the network plugins use: exact and
deterministic.

**Trade-off.** About 10% on compute-heavy code (an MMU-based variant under
review cuts SpiderMonkey's overhead to 2.8%), and CPU limits are
millisecond-granular, not instruction-exact.

**Why (performance).** Epochs are 2 to 3× cheaper than fuel, and
time-slicing a CPU-bound guest instead of killing it is what keeps one
tenant from starving a shard.

---

## 13. The memory cap and the admission charge are two numbers

**Call.** `config.limits.memory` is the hard cap a tenant cannot exceed;
`resources.memory` is what the node assumes one isolate costs when it
admits against the envelope. The host keeps a measured per-image estimate
of private pages so an operator can see when the charge is wrong.

```nickel
config = { limits = { memory = "128MiB" } },   # the cap
resources = { cpu = 5, memory = 2 },            # the charge
```

**Alternatives.** One number, charged at the cap, as every other runtime
does.

**Trade-off.** Two numbers to explain, and an envelope can be
over-committed when isolates outgrow their charge, with the cordon's
cgroup and the hibernation step as the backstop.

**Why (performance).** Charging the cap makes a JS isolate that writes
2 MiB cost 128 MiB of envelope: 60× less density, for a ceiling nearly no
isolate reaches.

---

## 14. Each host runs one shard per core, shared nothing

**Call.** A host's isolates are partitioned across pinned shard threads,
each a single-threaded executor; an isolate runs only on its shard and
moves only between turns.

**Alternatives.** A work-stealing runtime (tokio multi-thread).

**Trade-off.** Imbalance needs an explicit rebalancer.

**Why (performance).** No lock in a turn, cache-warm isolates, and slice
accounting that never follows a task across cores. Redpanda reported 3×
throughput running wasmtime on Seastar's reactor threads.

---

## 15. The default `js` image is StarlingMonkey; QuickJS is a candidate `js-small`

**Call.** `image = "js"` is SpiderMonkey compiled to Wasm with weval AOT.
QuickJS ships as `js-small` only if Phase 0 shows a several-times smaller
idle footprint.

**Alternatives.** QuickJS as the default (Shopify's choice): about 1 MB,
reference counting, smaller private heaps.

**Trade-off.** A larger image (~8 MB, shared copy-on-write) and possibly
larger private heaps per isolate.

**Why (ease).** Modern JS and Web APIs (`fetch`, streams, crypto) work
unchanged, which is what model-written code assumes; it is what Fastly and
Spin run in production; and weval closes much of the interpreter gap.

---

## 16. Python is CPython for WASI; heavy packages burst

**Call.** `image = "python"` is CPython built for WASI (componentize-py),
pre-initialized with the standard library. Native-extension packages are
available only in curated images with working Wasm builds; anything else
is routed to a VM by the pre-flight check.

**Alternatives.** Pyodide, Cloudflare's choice for Python Workers.

**Trade-off.** Fewer packages than Pyodide's ecosystem; data-science code
mostly bursts.

**Why (fit).** Pyodide targets Emscripten and a JS host, not WASI, and
starts in about a second with real packages even with Cloudflare's
snapshot. CPython for WASI pre-initializes into a component wasmtime
instantiates like any other.

---

## 17. Default limits: 128 MiB memory, 4 GiB ceiling, 1 MiB output

**Call.** `limits.memory` defaults to 128 MiB with a worker-wide ceiling of
4 GiB; `limits.output` to 1 MiB. `limits.cpu` and `limits.wall` have no
defaults and are required at submit.

**Alternatives.** Defaults for CPU and wall too (Workers: 30 s CPU on paid
plans).

**Trade-off.** One more thing to write in every isolate task.

**Why (ease of debugging).** 128 MiB is Workers' limit and sits under the
~550 MB at which MPK striping gets its full 15×. CPU and wall budgets vary
by three orders of magnitude between a webhook and an agent loop; a wrong
default would either kill legitimate work or let runaway snippets burn a
shard, and a required field puts the decision where the author sees it.
