# Isolate runtime: Phase 0 findings (container run)

> **Plan:** [isolate-runtime-plan.md](plans/isolate-runtime-plan.md), §11 "Phase 0".
> **Status:** partial. These are measurements from a 4-vCPU Firecracker guest,
> not from the AWS rig the plan names. The AWS run (nested and metal instance
> types, 16 vCPU / 64 GiB, 4-level and 5-level paging) is still open, and §4 and
> §10 of the plan have not been rewritten from these numbers. *2026-11-11:* the
> runtime was built on these findings (items 7, 8, 12 and 13 changed the design:
> [calls entries 7, 9, 21, 22](calls/isolate-runtime-plan_calls.md)); §4 and §10
> now carry these and the shipped images' container numbers beside the targets.

## Environment

| | |
| --- | --- |
| Machine | Firecracker microVM guest (a development container), **not** the AWS rig |
| CPU | Intel Xeon @ 2.30 GHz, 4 vCPU, x86-64; `pku` and `ospke` present (15 protection keys allocatable); `la57` absent |
| Memory | 15 GiB, no swap, no zswap or zram |
| Kernel | Linux 6.18.44, cgroup v1, THP `madvise`, `vm.overcommit_memory = 0`, `vm.max_map_count = 65530` |
| Virtualization | no `/dev/kvm` |
| Toolchain | rustc 1.97.0, wasmtime / wasmtime-wasi / wasmtime-wasi-http 50.0.1, wit-bindgen 0.63, componentize-js 0.23.0 (StarlingMonkey), componentize-py 0.25.1, rquickjs 0.10.0, `v8` crate 152.2.0 (V8 15.2.124.1) |

The box was shared: a Legion workspace build ran during the first part of the
session, and the timing runs are single runs on a VM with noisy neighbors.
Treat percentiles as indicative; the epoch results in particular varied run to
run (noted below). Spike code, built guests and exact commands live outside the
repo (spike `README.md`); nothing here was merged.

## Method

One spike binary (`spike`, wasmtime 50.0.1 with Cranelift, pooling allocator,
component model, async, component-model-async) with a subcommand per
measurement. Guests:

- **Rust component**: `wasm32-wasip2` cdylib via wit-bindgen 0.63, 45,797
  bytes, exports `run`, `fib`, `spin`, `ping` (calls a host import `tick` n
  times), `touch` (allocates and keeps memory). A `wasi:cli/run` command
  (67,520 bytes) for comparison.
- **Rust async component**: same toolchain, `export ping: async func(n: u32) -> u32`
  importing `tick: async func() -> u32`; its core module uses the Component
  Model callback ABI (`[callback][async-lift]ping`).
- **JS (StarlingMonkey)**: componentize-js, world
  `run: func(source: string, input: string) -> result<string, string>`; `run`
  evaluates the snippet with `new Function("input", source)`. Pre-initialized
  by componentize-js (engine booted, runner script loaded); the snippet is
  dynamic. Also built with `enableAot` (weval).
- **JS (QuickJS)**: rquickjs 0.10 in a Rust component, same world, `run` evals
  the snippet. **Not pre-initialized**: the QuickJS runtime is created on the
  first call, so its first-call time includes engine start.
- **Python**: componentize-py, same world; `run` does `exec(source)` with
  stdout redirected into a `StringIO`. Pre-initialized by componentize-py.
- **V8**: the `v8` crate with the prebuilt static library (default build: no
  pointer compression, no sandbox), one isolate and one context per run, snippet
  run as `(function(input){...})('world')`.

The "10 KB snippet" is generated JS / Python: ~10,300 bytes of small function
definitions and calls, returning one number. All language images return
the same value (`n=168430`), so the work is the same.

Per measurement:

1. **Instantiation**: a precompiled artifact loaded with
   `Component::deserialize_file`, `memory_init_cow(true)`, pooling allocator,
   `InstancePre` built once. Each iteration: build a `WasiCtx` (stdout/stderr to
   64 KiB in-memory pipes), `Store::new`, attach a `StoreLimits` limiter, set an
   epoch deadline, `instantiate`, drop the store (returning the slot).
   20,000 iterations, timing each phase. Async: `instantiate_async` on a
   current-thread tokio runtime, with `concurrency_support` on (default) and off.
2. **Private memory per idle instance**: N instances, each instantiated in its
   own store and called once with a "hello" snippet, all kept alive;
   `/proc/self/smaps_rollup` before and after; reported as the `Private_Dirty`
   delta / N (equal to the `Anonymous` delta in every run). N = 1000 for every
   image (200 for a first JS/Python run, same results). Also N instances only
   instantiated, never called. Shared copy-on-write image pages show up in
   `Rss` but not in `Private_Dirty`, which is the number reported.
3. **Slots per process**: binary search on `total_memories` with
   `max_memory_size = 128 MiB` for the largest pool `Engine::new` can reserve,
   for the default guard layout, MPK (`memory_protection_keys(Enabled::Yes)`),
   and small reservations (`memory_reservation(128 MiB)`,
   `memory_guard_size(0)`). Then `fill`: actually hold that many live
   instances of the Rust component (each called once).
4. **Epochs and fuel**: the Rust component compiled per variant; `spin(3×10^8)`
   (an integer loop, one back-edge check per iteration) and recursive `fib(36)`
   (call-heavy); 5 runs each, median and minimum.
5. **Async**: `ping(10^6)` where `tick` is a sync host import, an async host
   import that returns ready, and an async host import that returns `Pending`
   once (forcing the fiber to suspend and be resumed). Default (2 MiB) and
   128 KiB `async_stack_size`; `concurrency_support` on and off. The same with
   the callback-ABI guest through `run_concurrent` / `call_concurrent` /
   `func_wrap_concurrent`. Stack holding was tested with a pool of
   `total_stacks = 1, 2 or 8` and many stores or calls parked in a host import.
6. **Binary size**: a minimal host (load a `.cwasm`, `InstancePre`,
   `call_async`), wasmtime features `runtime, pooling-allocator,
   component-model, async, component-model-async, std`, with and without
   `cranelift`; `opt-level = 3`, `strip = true`; also with fat LTO,
   `codegen-units = 1`, `panic = "abort"`. A stripped `println!` binary
   (342,520 bytes) is the baseline.
7. **Compile time**: `Engine::precompile_component`, with
   `parallel_compilation` (4 threads) and without.

## Results

Plan targets are from §4 (G1–G3), §6.1, §6.4, §6.5 and §10, which assume a
16-vCPU 64 GiB node. Times are µs unless marked.

### Start

| What | Plan target | Measured (p50 / p99) |
| --- | --- | --- |
| Rust component, `instantiate` from `InstancePre` (pooling, CoW) | ≤ 20 / ≤ 100 µs | **5.0 / 9.8** |
| … whole cycle: `WasiCtx` + `Store::new` + instantiate + drop | (same) | **8.1 / 22.7** (store drop alone 2.4 / 4.4; `WasiCtx` build 0.4 / 1.0) |
| … `instantiate_async`, `concurrency_support` on (default) | | 5.8 / 21.1 (whole cycle 9.3 / 32.4) |
| … `instantiate_async`, `concurrency_support` off | | 4.4 / 11.6 (whole cycle 7.9 / 25.6) |
| `wasi:cli/run` command component, instantiate | | 5.1 / 12.3 |
| StarlingMonkey image, `Store::new` + instantiate | 5 µs (§6.2) | 28.6 / 92.1 |
| StarlingMonkey, dynamic "hello" snippet, time to first result | 300 µs is for a *pre-initialized author script* (not measured) | **445 / 746** |
| StarlingMonkey, dynamic 10 KB snippet, time to first result | ≤ 1 ms | **1,752 / 1,853** |
| StarlingMonkey, warm call on a reused instance | < 1 µs turn (§6.5) | 21.5 / 65 |
| StarlingMonkey + weval AOT: hello / 10 KB | | 657 / 1,098; 2,095 / 4,129 |
| QuickJS (not pre-initialized): instantiate / hello / 10 KB | < 300 µs lifecycle | 8.4 / 18.9; **218 / 291**; **1,206 / 1,281** |
| QuickJS warm call | | 6.6 / 22.5 |
| CPython image, `Store::new` + instantiate | < 1 ms (maintainer estimate) | **582 / 983** |
| CPython, "hello" (`print`) time to first result | ≤ 5 ms | **1,081 / 1,683** |
| CPython, 10 KB snippet time to first result | | 9,864 / 15,112 |
| CPython warm call | | 32.9 / 84.0 |
| V8: `Isolate::new` / `Context::new` / hello compile+run | 0.7 to 5 ms from a snapshot (§6.1) | 632 / 277 / 34 (p50) |
| V8 hello, time to first result (isolate + context + run) | | **923 / 1,680** (n=200); 1,078 / 1,687 (n=1000) |
| V8 10 KB snippet, time to first result | | 1,525 / 2,289 |

### Memory

| What | Plan target | Measured |
| --- | --- | --- |
| Rust component, private per idle instance (called once with two strings) | ≥ 4,000 per GiB (≤ 262 KiB each) | **33.8 KiB** (≈ 31,000 per GiB) |
| Rust component, called once with `fib(10)` (fill runs, 21k to 300k instances) | | 21.9 KiB (≈ 47,900 per GiB) |
| Rust component, instantiated, never called | | 4.0 KiB |
| StarlingMonkey, private per idle instance | ≥ 500 per GiB (≤ 2 MiB each); "~1 MB-class" (§6.1) | **651.2 KiB** (≈ 1,610 per GiB) |
| StarlingMonkey + weval AOT | | 649.5 KiB |
| StarlingMonkey, never called | | 28.8 KiB |
| QuickJS (runtime created at first call, all private) | candidate if "several times smaller" than StarlingMonkey | **127.6 KiB** (≈ 8,200 per GiB; 5.1× smaller) |
| QuickJS, never called | | 4.0 KiB |
| CPython, private per idle instance | | **1,016.0 KiB** (≈ 1,030 per GiB) |
| CPython, never called | | 291.0 KiB (written during instantiation) |
| V8 isolate + context, idle after one run | "a few MB" (§6.1) | **1,216 KiB** (≈ 860 per GiB), n = 200 and n = 1000 alike |

Idle numbers above are for stores that only ever made synchronous calls. A
store that has made an async call also keeps a fiber stack (next table), whose
touched pages stay resident; that was not separately measured per instance.

### Slots per process and live instances

| Layout | Plan | Max slots (`Engine::new` succeeds) | Live instances actually held |
| --- | --- | --- | --- |
| Guard pages (default 4 GiB reservation) | ~16,000 to 32,000 | **21,870** (a second run: 21,730); bound by the 47-bit address space | 21,000 (0.62 s to create, 459,996 KiB private) |
| MPK striping, default `vm.max_map_count` (65,530) | ~15× guard | **65,491**: capped by the kernel's map count, not by address space | not filled |
| MPK striping, `vm.max_map_count` raised to 10^6 | 218,716 (ColorGuard paper) | **328,842** (15.0× guard; 15 keys) | 133,306, then `ENOMEM` (map count again: ~7.5 mappings per live instance) |
| MPK striping, `vm.max_map_count` 4×10^6 | | | **200,000** (5.85 s, 4.38 GB private, 21.9 KiB each); larger counts not tried |
| Small reservations (128 MiB, no guard, explicit bounds checks) | ~10^6 | **702,500** (a second run: 699,267) | **300,000** (8.92 s, 6.57 GB private); larger counts not tried for lack of RAM |
| 5-level paging | ~10^6 with guard pages | not measured: `la57` absent | |

`vm.max_map_count` was raised only for these runs and restored to 65,530
afterwards. MPK is reported usable by
`PoolingAllocationConfig::are_memory_protection_keys_available()` in this
Firecracker guest, and `pkey_alloc` returns keys 1 to 15.

### CPU, waiting and async

| What | Plan | Measured |
| --- | --- | --- |
| Epoch checks, call-heavy `fib(36)` | ~10% on compute-heavy code | plain 47.1 to 47.9 ms (best runs); epoch, no ticks 58.9 / 77.6 ms; epoch + 1 ms ticker + `Continue` 54.4 / 55.4 ms; epoch + async yield each tick 54.8 / 87.4 ms. Best-run overhead **+14% to +25%**; medians were noisier (up to about +80% in one run) |
| Epoch checks, loop-heavy `spin(3×10^8)` | | plain 491 to 499 ms; epoch variants 495 to 509 ms (**+1% to 3%**) |
| Fuel, `fib(36)` / `spin` | epochs 2 to 3× cheaper than fuel | 68.6 / 70.3 ms (**+46% to +47%**; `fib(32)`: +47%) / ≈ 0% |
| Sync host import round trip | | 54 to 97 ns |
| Async host import that is ready at once (fiber, no suspend) | | 58 to 87 ns |
| **Async host import that suspends once: fiber suspend + resume round trip** | switch ≤ 1 µs (G3) | **480 ns** (2 MiB stack) / **484 ns** (128 KiB), `concurrency_support` off; 515 / 567 ns with it on |
| **Same through the callback ABI** (`func_wrap_concurrent`, async-lowered import) | | **1,804 ns** per round trip |
| Whole `call_async` of a trivial export | | **0.18 µs** (`concurrency_support` off) / **0.91 µs** (on) |
| Whole `run_concurrent` + `call_concurrent` of a trivial async export | | **2.94 µs** p50 |
| Stack held by a waiting call, callback ABI | none (G3) | none per call: 500 calls parked in one store with `total_stacks = 1` |
| Stack held per store, callback ABI | none | **one per store**: 200 stores each with one parked call needed 200 stacks (8 succeeded with `total_stacks = 8`, 192 failed "maximum concurrent limit of 8 for fibers reached") |
| Stack held by an *idle* store after its async call returned | | **one per store, fiber and callback ABI alike**: with `total_stacks = 2`, the third store fails although the first two are idle |

### Size and compile

| What | Plan | Measured |
| --- | --- | --- |
| Minimal runtime-only host, release + strip | runtime-only +1.2 to 2.1 MB | 2,286,768 bytes (**+1.85 MiB** over the empty baseline) |
| … with fat LTO, `codegen-units = 1`, `panic = "abort"` | | 1,612,384 bytes (**+1.21 MiB**) |
| Same host with Cranelift, release + strip | ~19 MB | 14,691,584 bytes (**+13.7 MiB**) |
| … with fat LTO | | 11,228,904 bytes (+10.4 MiB) |
| V8 probe binary (`v8` crate, release + strip) | +22 to 42 MB | 48,166,744 bytes (not a like-for-like host; for scale only) |
| Rust component: wasm → cwasm, compile time | | 45,797 → 164,496 bytes; 22 ms (4 threads) / 51 ms (1 thread) |
| StarlingMonkey image | "~8 MB image" | 12,000,067 → 37,255,464 bytes; **3.3 s** / 11.3 s; load (`deserialize_file`) 1.7 ms |
| StarlingMonkey + weval AOT | | 14,922,396 → 43,497,280 bytes; 4.9 s / 12.9 s |
| CPython image | | 18,453,247 → 35,392,080 bytes; **2.5 s** / 9.0 s; load 2.9 ms |
| QuickJS component | ~1 MB | 1,021,869 → 3,051,816 bytes; 0.46 s / 1.06 s; load 0.4 ms |
| Cranelift compile rate | tens to hundreds of ms per MB | 133 (CPython) to 480 (Rust) ms per MB of Wasm with 4 threads; 490 to 1,110 ms per MB single-threaded |

### Imports of the language images

These decide what the host must link for each image (from `jco wit`):

- **StarlingMonkey** (`@0.2.12`; http `@0.2.10`): `wasi:io/{poll,error,streams}`,
  `wasi:clocks/{monotonic-clock,wall-clock}`, `wasi:random/random`,
  `wasi:cli/{stdin,stdout,stderr,terminal-*}`,
  **`wasi:filesystem/{types,preopens}`**, **`wasi:http/{types,outgoing-handler}`**
  (for `fetch`, present even though the world does not mention HTTP). No
  sockets, no `wasi:cli/environment` or `exit`.
- **CPython** (`@0.2.9`): everything above except `wasi:http`, plus
  `wasi:cli/{environment,exit}` and **all of `wasi:sockets`**
  (`network`, `instance-network`, `tcp`, `tcp-create-socket`, `udp`,
  `udp-create-socket`, `ip-name-lookup`). It also exports an extra
  `exports` interface beside `run`.
- **Rust and QuickJS components**: `wasi:io`, `wasi:clocks/monotonic-clock`,
  `wasi:cli/{environment,exit,stdin,stdout,stderr,terminal-*}`; no
  filesystem, no sockets, no http.

### Guest-state access for hibernation (asked during the run)

With `Config::guest_debug(true)`, `Store::debug_all_instances()` and
`Instance::debug_memory` / `debug_global` / `debug_table` give the host every
core memory, global and table of a live component instance, readable and
writable. A spike snapshot of the Rust component (3 core instances, one
2.6 MB memory, one mutable global) restored into a fresh instance from the
same `InstancePre` produced byte-identical memory and a working allocator.
The cost of turning `guest_debug` on: instantiation 8.5 µs → 320 µs p50,
`fib(32)` 10.7 → 21.2 ms. Without it there is no public API for core memories
or globals of a component instance. Details, and what such a snapshot would
lose (component handle tables, host resources, task state), are in the API
notes that accompany this run.

## Not measured, and why

- **The AWS rig** (nested and metal instance types, 16 vCPU / 64 GiB). Not
  available; every number above is from this container and stays open there.
- **5-level paging slots**: the CPU has no `la57`.
- **Wake from page-out**: no swap, zswap or zram in this guest, so
  `MADV_PAGEOUT` cannot move anonymous pages anywhere.
- **Run-time cost of small reservations** (explicit bounds checks; the plan's
  ~1.5×): the CPU benchmarks barely touch linear memory, so they would not show
  it; a memory-heavy benchmark was not written.
- **A JS image with the author's script pre-initialized** (the plan's 300 µs
  target): only the dynamic-snippet path was built.
- **QuickJS pre-initialized**: the QuickJS component creates its runtime on
  first call, so its 218 µs and 127.6 KiB include engine start and are not
  directly comparable to the pre-initialized StarlingMonkey image. Javy is not
  published on npm or PyPI; rquickjs was used instead.
- **wasmtime-wizer 50 on a component**: its library API was read, not run.
- **An HTTP handler end to end**: the wasi-http host code compiles, but no
  `wasi:http/incoming-handler` guest was built or served.
- **Private memory of an idle store that holds a fiber stack**, per instance.
- **V8 with pointer compression and the sandbox** (workerd's configuration,
  and the one with the shared 4 GB cage): the prebuilt library has neither, so
  the 1,000-isolate run says nothing about the cage limit.
- **Why the CPython image takes 0.58 ms and writes 291 KiB to instantiate**:
  not diagnosed (no profiler run).
- Raw GitHub downloads were not blocked after all (the V8 prebuilt came from
  GitHub releases).

## What this changes in the plan

1. **Component start (G1) holds with room to spare.** 5 µs to instantiate and
   8 µs for the full store cycle at p50, 23 to 32 µs at p99, on 4 vCPU. The
   ≤ 20 / ≤ 100 µs targets can stay or tighten after the AWS run.
2. **The JS dynamic-snippet target (≤ 1 ms for 10 KB) is not met by either
   interpreter here**: StarlingMonkey 1.75 ms, QuickJS 1.21 ms (V8 1.5 ms with
   isolate creation). The cost is parsing and running the snippet, not
   instantiation (StarlingMonkey instantiates in 29 µs). weval AOT does not help
   dynamic code (it specializes for code known at build time) and was slower
   here. Either the target moves to roughly 2 ms or the snippet path needs a
   faster parser/compiler; rewriting §4 needs a calls entry.
3. **QuickJS meets the plan's own condition for a `js-small` image**: 5×
   smaller idle footprint (127.6 vs 651.2 KiB) and faster dynamic snippets,
   before any pre-initialization. That makes the `js-small` candidate worth a
   Phase 3 decision rather than an aside, at the price of a second JS engine's
   compatibility surface (no Web APIs by default).
4. **Python's ≤ 5 ms holds for a small snippet** (1.1 ms) but not for 10 KB
   (9.9 ms), and the image costs 0.58 ms and 291 KiB just to instantiate, about
   20× the JS image's instantiation time. The plan's "< 1 ms instantiate"
   estimate is right; "µs" is not.
5. **Density per GiB beats the targets for every image**: Rust ~31,000 to
   48,000, StarlingMonkey ~1,600, QuickJS ~8,200, CPython ~1,030, V8 ~860. The
   binding limit is slots and stacks, not RAM, until hibernation.
6. **MPK density depends on a node sysctl the plan does not mention.** MPK
   striping gives 15× guard-page slots (328,842 here), but with the default
   `vm.max_map_count` of 65,530 a process cannot even reserve more than 65,491
   slots, and holding live instances costs ~7.5 mappings each (200,000 live
   instances needed the limit raised to 4×10^6). The worker would have to raise
   or check `vm.max_map_count` for any MPK or high-density cordon. Guard-page
   slots (21,870 per process) are address-space bound as the plan says, so
   10^5 resident isolates per node needs ≥ 5 host processes without MPK.
7. **Waiting is not free per store in wasmtime 50.0.1 (G3, §6.6, §6.7).** The
   callback ABI frees the stack *per call* (500 parked calls shared one stack),
   but every `Store` that has run an async call keeps one fiber stack until it
   is dropped, on the callback path (a cached worker fiber) and the fiber path
   alike, and there is no public API to release it. So an idle or waiting
   session isolate holds a stack slot either way, `total_stacks` caps resident
   async isolates, and "no stack for stackless guests" (§10 Phase 4) is only
   reachable by dropping the store (hibernation) or by an upstream change. The
   plan's argument that stackless is what makes hibernation possible at any
   await still holds; the "holds nothing but memory" row of §6.6 does not.
8. **The callback ABI is slower per await than a fiber** in this version:
   1.8 µs per host-import round trip against 0.48 µs for a fiber
   suspend/resume, and a trivial call costs 2.9 µs through `run_concurrent`
   against 0.18 µs through `call_async`. `concurrency_support`, on by default,
   adds ~0.7 µs to every `call_async` and is baked into compiled artifacts, so
   a cordon must decide it at compile time. The G3 switch target (≤ 1 µs) is
   met by fibers and missed by the callback path.
9. **Epoch overhead is higher than the plan's ~10% on call-heavy code**
   (+14% to +25% best runs on `fib`, ~1 to 3% on a tight loop); fuel was +46%
   to +47%. The relative ranking stands; the number in §6.6 should say
   "10 to 25%, workload dependent" until the AWS run.
10. **Binary size**: runtime-only adds 1.2 to 1.9 MiB, as planned; Cranelift
    adds 10 to 14 MiB, not ~19 MB. A separate compiler process costs less disk
    than the plan assumed, but the case for keeping Cranelift out of the host
    process is about attack surface, not size, and is unchanged.
11. **Compile times put language images firmly at stage time**: 3.3 s
    (4 threads) to 11.3 s (1 thread) for the StarlingMonkey image, 2.5 to 9.0 s
    for CPython, and the artifacts are 35 to 43 MB each. Artifacts also encode
    the memory layout (`memory_reservation`, guard size) and
    `concurrency_support`, so a node needs one artifact per (image, layout,
    concurrency setting); the derived-content cache key must include
    `Engine::precompile_compatibility_hash()`.
12. **No writable or virtual filesystem comes for free.** Both language images
    import `wasi:filesystem`, and wasmtime-wasi 50's filesystem is backed only
    by real host directories. To meet G6 ("no writable filesystem") the host
    needs either its own `wasi:filesystem` implementation (generated bindings
    plus ~35 trait methods; the generation and the rest of the linking compile
    in the spike) or an empty read-only preopen. The CPython image also imports
    all of `wasi:sockets`, which must be linked and denied (wasmtime-wasi's
    default denies every address). The StarlingMonkey image always imports
    `wasi:http/outgoing-handler`, which is where the host's egress hook
    (`WasiHttpHooks::send_request`) sits.
13. **Hibernation cannot use wasmtime's public API as is.** Reading and
    writing a component instance's memories and globals is possible only with
    `guest_debug`, which costs ~40× on instantiation and 2× on execution.
    wasmtime-wizer 50 can pre-initialize a component (good for images and
    entries) and its instrumentation exports read-only state accessors, but has
    no setters and rewrites the component per snapshot. Phase 4 needs either an
    upstream API or Legion's own instrumentation pass.
14. **V8's numbers are close enough that the Phase 7 trigger should rest on
    these, not on the table in §6.1**: 0.92 ms to a first result and 1.2 MiB
    idle per isolate (no pointer compression), against 0.45 ms and 0.65 MiB for
    StarlingMonkey. V8 is 2× slower to start and 2× bigger at rest here, not the
    order of magnitude §6.1 suggests; its advantage is JIT throughput, which
    this run did not measure.
