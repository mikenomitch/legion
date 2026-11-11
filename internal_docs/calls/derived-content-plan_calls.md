# Judgement Calls: derived-content-plan

Decisions made while designing
[`plans/derived-content-plan.md`](../plans/derived-content-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** These are design-time calls; implementation inserts
its own at their rank.

## 1. Derived content is never fetched, published, or moved

**Call.** A derived entry is built on its node, by Legion's builder, from
inputs the node verified. Nothing can put one into the store, read one off
the node, or fetch one built elsewhere.

**Alternatives.** Build once per cluster (a manager or a `legion-system`
app compiles) and distribute signed artifacts through the content store.

**Trade-off.** Every node builds each artifact once: a Cranelift compile per
module per node, a root disk per image per node.

**Why (security).** Loading a compiled Wasm module is arbitrary code
execution by design, and a VM base snapshot is a running machine's memory.
With distribution, a signature check would be the only thing between
anyone who can write to the bucket and code execution on every node. The
first consumer that needs cluster-wide builds writes that plan, with the
signature check in it.

---

## 2. A class of the node content store, not a second cache

**Call.** Derived content is a third class (`derived`) of the node content
store, beside `artifacts` and `snapshots`, sharing its budgets, LRU,
pinning, TTL, `minFreeDisk`, and GC. The root disk cache moves onto it.

**Alternatives.** (a) Generalize `rootfs_cache.rs` into a standalone derived
cache with its own budget and eviction. (b) A cache per consumer, as root
disks have today.

**Trade-off.** The content store learns entries that are not named by the
digest of their bytes, which it has never had.

**Why (simplicity).** One disk, one set of budgets, one eviction order, one
`minFreeDisk`. (a) keeps two eviction loops competing for the same free
space; (b) multiplies that by every consumer and hands operators a budget
per feature.

---

## 3. Keyed by inputs and builder, not by output digest

**Call.** An entry's key is a hash of its kind, the digests of its inputs,
and its builder's version. Its bytes are never hashed.

**Alternatives.** Name derived outputs by the sha256 of their bytes, like
every other content-store blob.

**Trade-off.** Two recipes that happen to produce identical bytes are
stored twice.

**Why (performance).** Hashing a multi-gigabyte root disk or a VM's memory
costs seconds on the start path, to verify something that is never fetched
(entry 1) and so never needs verifying. The inputs already identify the
output, as Bazel's action keys and Nix store paths do.

---

## 4. One budget for every derived kind

**Call.** `content.budgets.derived` sizes all derived content together, with
one LRU across kinds.

**Alternatives.** A budget per kind (root disks, compiled modules,
pre-initialized images, VM bases).

**Trade-off.** A node restoring many large VM base snapshots can evict small
compiled modules, which then recompile.

**Why (simplicity).** An operator sizes "what Legion can rebuild" once. Per-
kind floors come back if a measurement shows churn (plan §11 Q1).

---

## 5. Builders that run untrusted input are confined

**Call.** A build that executes or parses untrusted input (compiling a
component, pre-initializing an author's script) runs in a short-lived
builder process with its own seccomp profile, no network, a memory limit,
and a deadline. Root disk builds stay in the worker.

**Alternatives.** Build everything in the worker process.

**Trade-off.** A process spawn per build, and a second seccomp profile to
maintain.

**Why (security).** Pre-initialization runs the author's top-level code, and
a compiler fed a hostile module is the bug class the 2026 wasmtime
advisories came from; neither belongs in the process that holds every
allocation's control.

---

## 6. `LEGION_SANDBOX_ROOTFS_CACHE` is removed, not aliased

**Call.** The root disk budget becomes `content.budgets.derived`, and the
env var is gone.

**Alternatives.** Keep the env var as an alias for the derived budget.

**Trade-off.** An operator who set it loses the setting on upgrade, and each
node rebuilds its bases once.

**Why (simplicity).** Legion has no installed base to protect, and an alias
named for root disks that sizes compiled modules too would mislead. The PR
says what broke.

---

*Entries 7 to 14 were made while building Phase 1 (2026-11-10). Each ranks
below the design calls above, so none of those was renumbered.*

## 7. No content store, no root disk cache

**Call.** The root disk cache now exists only on a worker with a node content
store. `content.enabled = false`, or a store that cannot open, turns it off,
and the worker says so at start:

```text
Sandbox root disk cache off (this node has no content store)
```

**Alternatives.** (a) Open a private derived-only store when the content
store is disabled, so root disks stay cached as they were. (b) Keep the old
standalone cache as a fallback.

**Trade-off.** An operator who disabled the content store to stop artifact
caching now also pays a root disk build on every sandbox start. Before this,
the two were independent.

**Why (simplicity).** The plan's point is one place and one budget for
rebuildable content. (a) and (b) both keep a second cache with its own
directory and eviction for a configuration that asks for no caching. The
public page states the dependency under Limitations.

## 8. The builder version is the Legion version plus the builder's format

**Call.** A root disk base's recipe is built by
`legion-<crate version>+rootfs.<format>`. The plan says "Legion's version for
in-tree builders"; the format number the old cache keyed on is kept beside
it.

**Alternatives.** (a) The format number alone, as before: a release that does
not change the base layout reuses every base. (b) The crate version alone.

**Trade-off.** Every release rebuilds each image's base once per node, even
when nothing about bases changed.

**Why (simplicity, safety).** A release changes the guest binary anyway, and
the guest binary is already an input, so (a) saves little in practice. The
version makes "a new builder never reuses an old builder's output" true
without anyone remembering to bump a constant. (b) loses the bump for
development builds, which all share one crate version.

## 9. A confined recipe is refused in-process, and Phase 1 ships no confined builder

**Call.** `Recipe::confined()` marks a build that parses or executes
untrusted input. `ContentStore::ensure_derived`, which builds in the worker
process, refuses such a recipe. A confined builder commits its output with
the staged form (`derived_stage` and `derived_commit`), and the entry records
`confined = true`.

```text
derived content of kind cwasm is built from untrusted input and needs a confined builder; it cannot be built in the worker process
```

**Alternatives.** (a) Build the confined builder process now, with nothing to
run in it. (b) Leave `confined` out until Phase 2.

**Trade-off.** Until the isolate runtime brings its compiler, a `cwasm` or
`preinit-*` recipe cannot be built at all.

**Why (security).** The builder is the isolate plan's `legion-isolate
compile` role, and its seccomp profile and limits depend on what it runs; (a)
would be a sandbox designed around no workload. (b) would let the first
consumer build in-process by default. Failing closed means the unsafe path
does not exist to be taken by accident.

## 10. The derived listing rides the content report, whole, only when it changed

**Call.** `GetNodeContent` lists derived entries from what the node sent on
its heartbeat: `ContentLocationReport.derived`, the whole listing (at most
256 entries, most recently used first), present on a full report and on a
delta only when the derived set, a size, or a pin changed.

**Alternatives.** (a) The manager asks the worker over the worker plane when
`GetNodeContent` is called. (b) Send derived entries as ordinary
`added`/`removed` deltas in the location index. (c) Send the listing on
every heartbeat.

**Trade-off.** The listing is as stale as the last change the manager
acknowledged, last-used times are those of the last send (a use is not a
change), and a node with more than 256 entries shows only the newest.

**Why (performance, security).** (b) would put derived entries in the index
the scheduler scores and peers fetch by, which is exactly what entry 1
forbids. (a) adds a manager-to-worker call to a read path that has none. (c)
costs every heartbeat of every node for a listing that changes when an image
is first used.

## 11. A recipe's inputs are digests, numbers, and fingerprints, under static names

**Call.** "A type that cannot carry a secret" is `Recipe`: an input is a
`ContentDigest`, a `u64`, or bytes the recipe hashes itself, and every name
is a `&'static str`. The guest binary's identity (its path, size, and mtime)
is recorded as its SHA-256.

**Alternatives.** (a) A free-form string map with a deny list of suspicious
names. (b) Digests only, which cannot express a headroom or a binary's
identity.

**Trade-off.** `legion node content -j` shows `guestBinary` as a hash, not a
path, so an operator cannot read which binary a base was built with.

**Why (security).** With (a) the guarantee is a convention. Here the recorded
value is never caller text, so nothing reaches the index, the heartbeat, or
the API that a build could have been handed by mistake.

## 12. Root disks keep their own fill path over the store's primitives

**Call.** `rootfs_cache.rs` does not call `ensure_derived`. It uses the
store's claim, stage, and commit directly, so a miss still builds the start's
own disk first and offers a reflink of it (or a background second format) to
the cache afterwards.

**Alternatives.** Build into the store with `ensure_derived` and clone from
the entry, which is simpler and makes every miss wait for the commit.

**Trade-off.** Two ways to fill the class exist, and the root disk tests
needed five mechanical line edits (the directory names, the in-flight check,
and marking a base used through the store instead of a file's mtime). The
plan asked for them unchanged.

**Why (performance).** "A miss is never slower than a start without the
cache" is the root disk cache's stated contract, and the plan requires its
behavior not to change.

## 13. Only a derived share the operator sets counts toward the 100% rule

**Call.** `artifacts`, `snapshots`, and an explicit `derived` share must add
up to at most 100%. The default derived share (10%) is not counted.

**Alternatives.** Count the default, as the other two classes' defaults are.

**Trade-off.** `artifacts = "75%"` with defaults passes validation though
the three shares read as 105%.

**Why (ease of use).** The default is capped at 20 GiB, so on any disk where
the sum matters it is a rounding error, and counting it would reject configs
that worked before this change for no real risk. `minFreeDisk` remains the
floor.

## 14. Derived pins share the blob pin table, and the class gets its own gauge

**Call.** A derived pin is a row in the existing `pin` table under
`derived:<kind>:<key>`, so `release_owner` drops an owner's derived pins with
its blob pins. The class's bytes are exported as `legion_derived_bytes`.

**Alternatives.** A separate pin table; no gauge (the plan says "in the
existing content-store gauges", and there are none).

**Trade-off.** A pin's `digest` column no longer always holds a digest.

**Why (simplicity).** One release path cannot forget a kind of pin. The
gauge is the smallest thing that makes the budget observable.
