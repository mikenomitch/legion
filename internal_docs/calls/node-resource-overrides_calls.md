# Judgement calls: node resource overrides

Calls made on a worker type's `resources` block, which has no plan of its
own. The open item that would let a node override its builtin capacity is
"Let a node override its builtin resource capacity" in
[TODO.md](../TODO.md) (Scheduling & placement).

**Entries are ranked by project impact, highest first; an entry's number is
its rank.** Anything here a user needs to know is also stated in
[resource-enforcement.md](../../docs/resource-enforcement.md).

## 1. A builtin name in a worker type's `resources` fails manager startup

*2026-10-19.*

**The call.** A worker config whose `resources` names `cpu`, `memory`,
`cores`, or `allocations` is refused when the manager loads its worker
configs, naming the keys and pointing at `reservedResources` and
`maxAllocations`. Custom resources are unchanged. Before, the scheduler
skipped builtin keys without a word.

```text
invalid resources in worker config configs/workers/big.ncl: resources may only declare custom resources, but it names cpu, memory. A node's memory, cpu, and cores come from its fingerprint and cannot be overridden here; use reservedResources to hold capacity back, and maxAllocations to cap allocations
```

**Alternatives.** Warn and keep ignoring the keys; or implement the
override (replace the fingerprint, or act as a second ceiling).

**Trade-off.** A config that carried such a key, harmlessly ignored, now
stops the manager until it is removed.

**Why.** Simplicity. A key that silently does nothing is the bug; what an
override should mean (replace or cap, and how it composes with
`reservedResources`) is a product question the open TODO item owns, and
refusing now leaves that choice free, where a warning would invite configs
that a later meaning could change under them.
