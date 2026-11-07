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
