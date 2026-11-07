# Plan: Derived Content: One Node Cache for Everything Legion Builds

> **Judgement calls:** [derived-content-plan_calls.md](../calls/derived-content-plan_calls.md)

**Status: PLANNED** (written 2026-11-07). Nothing here is built.

Workers increasingly *build* things from content they already hold: a
sandbox's base root disk from an image's layers, and soon a compiled Wasm
module from a component, a pre-initialized JS or Python image, a task's
pre-initialized entry, and a base memory snapshot of a booted VM for
prepared starts. Each is expensive to build, cheap to keep, the same for
every unit of its inputs on the node, and safe to throw away. This plan
makes them one thing: **derived content**, a new class of the node content
store, keyed by what it was built from, with one budget, one eviction
policy, one single-flight build path, and one trust rule.

The root disk cache ([rootfs-base-cache-plan.md](rootfs-base-cache-plan.md))
is the first implementation of the pattern and moves onto it unchanged in
behavior. The [isolate runtime](isolate-runtime-plan.md) (compiled modules,
pre-initialized images and entries) and the prepared VMs of
[sandbox-scaling-plan.md](sandbox-scaling-plan.md) Phase 8 are the next
consumers.

Deliberately deferred, and named in §4: fetching derived content built on
another node or a manager (it would need signed artifacts), publishing it,
and per-kind budgets.

---

## 1. Problem statement

`src/task_plugins/rootfs_cache.rs` solves "build once per node, keep within
a budget, share across units" for root disks: inputs hashed into a key,
single-flight builds so a burst builds once, LRU eviction by blocks used, a
free-disk floor borrowed from the content store, a temp directory cleared at
start, and a prefetch seam (`ensure_image_base`). It is single-purpose, with
its own directory, its own budget (`LEGION_SANDBOX_ROOTFS_CACHE`), and its
own eviction loop beside the content store's.

The next consumers need the same thing:

| Consumer | Built from | Size | Plan |
| --- | --- | --- | --- |
| Base root disks | an image's layers, the guest binary, `rootfs_headroom_mib` | 50 MB to GBs | built |
| Compiled Wasm modules (`.cwasm`) | a module digest, the engine version, the CPU feature set | KBs to tens of MB | isolate Phase 1 |
| Pre-initialized images | an image digest and the engine version | ~10 MB | isolate Phase 3 |
| Pre-initialized entries | an image, the author's script digest, the engine version | ~10 MB | isolate Phase 3 |
| Base memory snapshots for prepared VMs | an image, the kernel, the VMM version, the guest binary, the guest size | the guest's memory | scaling Phase 8 |

Built one at a time, that is four more caches with four budgets an operator
has to size, four eviction loops competing for one disk, and four places to
get the trust rule wrong. The `.cwasm` case makes the last one sharp: loading
a compiled module is arbitrary code execution by design, so where it came
from is a security boundary.

## 2. Prior art

- **Bazel's action cache and content-addressable store**
  ([remote caching](https://bazel.build/remote/caching)). Outputs are keyed
  by a digest of the action (its inputs, command, and environment), stored
  in a content store, and evicted by the store.
  - **Take:** key by inputs and the builder, not by the output; let the
    store own eviction.
  - **Reject (for now):** a remote, shared cache. That needs signed outputs
    here (§4).
- **Nix store paths.** A derivation's output path is a hash of its inputs.
  - **Take:** "what it was built from" is the identity.
- **wasmtime's own compilation cache** (keyed by module, engine version, and
  configuration).
  - **Take:** the key components for `.cwasm`; **reject** its separate
    directory and budget.
- **Legion's node content store** (`docs/artifacts.md`, "The node content
  store"): per-class budgets, pinning, TTL, `minFreeDisk`, single-flight
  fetches.
  - **Take:** all of it, as the home of the new class.

## 3. Jobs to be done, by persona

**Cluster operator**
- When I size a node's disk, I want one number for "things Legion builds
  and can rebuild", so I do not tune a separate cache per feature.
- When disk runs low, I want rebuildable things to go first and in one
  order, so the node never refuses a start to keep a cache.

**Security / platform admin**
- When a worker loads compiled code, I want to know it was compiled on that
  node from verified inputs, so nobody who can write to a bucket can run
  code on my nodes.

**Job author**
- When I start many sandboxes or isolates of one image, I want only the
  first to pay the build, so starts after it are fast.

*Not in scope:* agents and on-call responders get no new surface beyond the
content store's existing status views.

## 4. Goals and non-goals

### Goals

- One class, `derived`, in the node content store, with its own budget and
  LRU, sharing `minFreeDisk`, TTL, pinning, and garbage collection.
- One build path: a recipe (kind, inputs, builder), a key, single-flight,
  build into a temp directory, commit atomically.
- One trust rule: derived content is built on its node, from inputs the node
  verified, and is never fetched, published, or moved.
- Root disk bases on it with no change in behavior.

### Non-goals

- **Fetching derived content** built elsewhere (a manager-side compile, a
  peer node). It would need signatures checked before every load, and the
  first consumer that wants it writes that plan.
- **Publishing it.** It is rebuildable by definition.
- **Per-kind budgets.** One budget; per-kind floors only if measurement
  shows one kind starving another (§11).
- **New user surface.** No jobspec field; one worker config key.

## 5. Background: what exists today

- **The root disk cache** (`src/task_plugins/rootfs_cache.rs`): key from
  `BaseInputs` (layer digests, guest binary, headroom), layout
  `<worker state dir>/rootfs-cache/<key>/{rootfs.img, base.json}` plus
  `tmp/`, single-flight per key, a reflink keep (or a background second
  format on filesystems without reflink), LRU by blocks, nothing kept below
  the content store's `minFreeDisk`, `ensure_image_base` and
  `ensure_layout_base` for prefetchers. Budget: `LEGION_SANDBOX_ROOTFS_CACHE`,
  default 10% of the state filesystem clamped to 1 to 20 GiB, `off` to
  disable.
- **The node content store** (`<worker state dir>/content/`): blobs named by
  their sha256 with a sqlite index; classes `artifacts` and `snapshots`, each
  with a budget, LRU eviction within the class, pinning by owners, a TTL
  (default 72 h), `minFreeDisk`, and GC every 10 minutes; configured under
  the worker's `content` block.

## 6. Proposed design

### 6.1 The `derived` class

The content store gains a third class. A derived entry differs from the
other two in one way: it is named by its **recipe key**, not by the digest
of its bytes. It lives at `<worker state dir>/content/derived/<kind>/<key>/`
(a file or a tree), beside the allocation directories, so a root disk can
still be a reflink of its base. The index row records the kind, the key,
the inputs it was built from, the builder version, its size in blocks
actually used, its last use, and its pins.

Everything else is the store's: the class budget and LRU, pinning (a
prepared VM pool pins its base snapshot while it holds VMs restored from
it), the TTL, `minFreeDisk` (derived content is evicted before artifacts
and snapshots are refused), and GC.

```nickel
content = {
  budgets = {
    artifacts = "10%",
    snapshots = "50GiB",
    derived = "15%",        # default 10%, clamped 1 GiB to 20 GiB; "0" disables
  },
},
```

### 6.2 Recipes

A consumer describes what it builds as a recipe:

| Part | Meaning |
| --- | --- |
| `kind` | `rootfs`, `cwasm`, `preinit-image`, `preinit-entry`, `vm-base` |
| inputs | digests of everything that decides the output's bytes, never a secret or anything per unit |
| builder version | the code that builds it (Legion's version for in-tree builders, the engine version for wasmtime) |
| build | a function that fills a temp directory |
| confined | whether the build runs in a confined builder process (§6.4) |

The key is a hash of kind, inputs, and builder version. `ensure(recipe)`
returns the committed entry, waits for a build in flight, or builds: into
`derived/tmp/`, then a rename into place, so a crash never leaves a
half-built entry under a real key. Concurrent `ensure` calls for one key
build once.

### 6.3 Nothing per unit, ever

A derived entry holds only what every unit of its inputs shares. Per-unit
material (env, identity, hostnames, the generated init) is written into
each unit's own copy afterwards, as the root disk cache does today. A
recipe whose inputs would include a secret is refused at construction, in
code, by a type that cannot carry one.

### 6.4 The trust rule, and confined builders

- **Built here, from verified inputs, never fetched.** A derived entry is
  produced on its node by Legion's own builder from content the node has
  verified against its digest. There is no API to put one, and none to read
  one off the node.
- **Builders that execute or parse untrusted input run confined.**
  Compiling a component, and pre-initializing an author's script (which
  runs its top-level code), happen in a short-lived builder process with
  its own seccomp profile, no network, a memory limit, and a deadline: the
  isolate plan's `legion-isolate compile` role. Building a root disk with
  `mkfs.ext4 -d` stays in the worker, as today.

### 6.5 Observability

`legion_derived_builds_total{kind,outcome}`, `legion_derived_build_seconds{kind}`,
`legion_derived_hits_total{kind}`, and the class's bytes in the existing
content-store gauges. `GetNodeContent` and the Nodes page list the derived
class like the other two, with each entry's kind and inputs.

## 7. Proto, struct, config, and persistence changes

- **Worker config:** `content.budgets.derived`. **Breaking:**
  `LEGION_SANDBOX_ROOTFS_CACHE` is removed rather than aliased, and
  `docs/task-runtime-sandbox.md` documents the new key.
- **On disk:** `<worker state dir>/rootfs-cache/` is deleted at first start
  of the new worker; bases rebuild on first use (the cache fills on demand
  by design, so this costs one build per image per node).
- **Content store index:** a `derived` table (kind, key, inputs, builder
  version, size, last use).
- **Proto:** derived entries in `GetNodeContent` replies.

## 8. Implementation phases

- **Phase 1: the class, recipes, and root disks.** The `derived` class and
  budget, `ensure`, the temp-and-rename commit, and `rootfs_cache.rs` moved
  onto it with its tests unchanged.
- **Phase 2: isolate artifacts** (with isolate Phases 1 and 3): `cwasm`,
  `preinit-image`, `preinit-entry`, and the confined builder.
- **Phase 3: `vm-base`** (with scaling Phase 8): base memory snapshots
  pinned by prepared-VM pools.

## 9. Testing plan

- **Unit:** key stability and sensitivity (every input changes the key, no
  per-unit input can be supplied); single-flight under concurrent `ensure`;
  a crash mid-build leaves no entry; LRU across kinds; pinned entries survive
  pressure; `minFreeDisk` evicts derived content before refusing anything.
- **Regression:** the root disk cache's existing tests pass unchanged on the
  new class.
- **Integration:** a burst of sandbox starts of one image builds one base;
  removing the old `rootfs-cache/` directory at upgrade.

## 10. Rollout and backward compatibility

Phase 1 changes where root disk bases live and the name of their budget,
and nothing else a user sees. The PR that ships it says so: the env var is
gone, the first start of each image per node rebuilds its base once.

## 11. Open questions

1. **Per-kind floors.** A node that restores many large VM base snapshots
   could evict compiled modules. Lean: one LRU until a measurement shows
   churn; then a floor per kind.
2. **Should `vm-base` live in `snapshots` instead?** Lean: no; it is
   rebuildable from an image, unlike a user's snapshot, and should go first
   under pressure.

## 12. Acceptance criteria

- **Phase 1:** the root disk cache's behavior and tests are unchanged on the
  new class; `GetNodeContent` lists derived entries; one budget key sizes
  them.
- **Phase 2:** a component compiles once per node per engine version, in a
  confined process; a pre-initialized entry is built once per job config per
  node.
- **Phase 3:** a prepared-VM pool's base snapshot is pinned while the pool
  holds VMs and evictable after.
