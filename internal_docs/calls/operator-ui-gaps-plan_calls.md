# Judgement calls: operator UI gaps

Calls made for [operator-ui-gaps-plan.md](../plans/operator-ui-gaps-plan.md).
**Ordered by project impact, highest first; the number is the rank.**

## 1. `ListKv` stops returning values by default

**Call.** `ListKvRequest.include_values` defaults to `false`; a listing
returns every entry with `value = ""` and the manager never decrypts it.
`legion kv list --values` opts in. `GetKv` keeps returning the value, with a
`metadata_only` flag for the reveal-on-click UI.

```
$ legion kv list -j            # values are ""
$ legion kv list --values -j   # decrypts every listed entry
```

**Alternatives.** (a) Mask in the UI only, keep `ListKv` as is — the smallest
diff. (b) A separate `ListKvKeys` RPC, leaving `ListKv`'s behaviour alone.

**Trade-off.** A breaking default: any client that read values out of a
listing now gets empty strings until it passes `include_values`. In-repo the
CLI's `--json` was the only such reader.

**Why (simplicity, security).** (a) still decrypts every secret under the
prefix and ships it to the browser on every refresh — the user's complaint was
"you are decrypting and showing off KV values", and a masked textarea over a
network response full of plaintext does not answer it. (b) keeps two ways to
list and leaves the unsafe one as the default. The repo rule is to pick the
default that is right for a new cluster and break freely; browsing key names
should not be a bulk secret read.

## 2. Stale rollouts are cancelled at tick time, not only at the KV path

**Call.** `handle_rollout_tick` checks that the rollout's target is the job's
latest config and cancels it (then reschedules the latest config) when it is
not. `reconcile_kv_changes` is unchanged.

**Alternatives.** Cancel active rollouts in `reconcile_kv_changes` whenever a
re-render produces a new config, as `create_rollout_for_update` does for
RunJob.

**Trade-off.** The stale rollout lives until its next tick (seconds), so the
UI can briefly show "Rollout #1 in progress" after a KV restart.

**Why (simplicity).** The bug is a rollout acting on a target that is no
longer the job's; the tick is the one place every path converges. A
KV-path-only fix leaves any other config-creating path (and every job already
stuck in the field, like `kv-demo`) exposed, and needs each new path to
remember the rule.

## 3. The instance event filter reads metadata, not a new column

**Call.** `instance_id` on ListEvents/StreamEvents matches `instanceId` /
`instance_id` in `metadata_json`, membership in `instances` / `suspending`,
or an `allocation_id` whose allocation row fulfilled the instance — with
`json_extract`/`json_each` in SQL.

**Alternatives.** A first-class `scheduler_event.instance_id` column set at
emission, indexed, and a `SchedulerEvent.instance_id` field.

**Trade-off.** A full scan of the bounded journal (~10k rows) per query, and
an allocation event only matches while the allocation row exists (pruned
allocations match only through metadata). A column would be indexed and
permanent, but needs a migration, a raft-replicated schema change, and every
emission site to set it — and still could not tag allocation events, whose
emitters do not know the instance.

**Why (simplicity).** The journal is small and bounded, the metadata already
names instances consistently, and the allocation half covers exactly the
events an operator asks about ("why did my sandbox restart"). Revisit if the
journal bound grows by an order of magnitude.

## 4. The allocation page reads the ledger instead of a new `GetAllocation` RPC

**Call.** `/allocations/:id` finds its row in `GetLedger` (now carrying
`task_name`), plus events, node usage, logs, and health from the existing
calls.

**Alternatives.** A job-scoped `GetAllocation` RPC returning the allocation,
its health snapshot, and timestamps.

**Trade-off.** The page needs `node:read` to show the allocation row; a
job-scoped token sees only events and logs. Fetching the whole ledger for one
row is wasteful on a big cluster.

**Why (simplicity).** Every other page already reads the ledger the same way;
a new RPC needs ACL wiring, an endpoint-table entry, a catalog entry, and an
MCP tool, and was not what was asked. Deferred, listed in the plan.

## 5. The public address is a per-node env var, not worker-type config

**Call.** `LEGION_WORKER_PUBLIC_ADDRESS` on the worker process, registered as
the `publicAddress` config attribute; display only.

**Alternatives.** (a) A `publicAddress` field in `configs/workers/<type>.ncl`.
(b) Auto-detect from cloud metadata by default.

**Trade-off.** One more env var next to `LEGION_WORKER_PORT`; on EC2 the
operator wires the metadata lookup into their start script.

**Why (simplicity).** Worker config is per *type* and rendered by the manager
— a public IP is per *node*, so (a) cannot express it. (b) means probing
three cloud metadata services at boot and guessing which interface is public;
an explicit value is explainable and testable. `auto` is deferred.

## 6. Reversed: exec uses the task page's Actions panel, not a second console

**Call.** On rebase, drop this branch's `TaskExecConsole` (one console per
task with an allocation picker, keyed by id so polls never remount it) in
favour of main's `TaskActions` panel, which shipped in parallel. The
allocation page embeds `TaskActions` with its one allocation as the only
target.

**Alternatives.** Keep `TaskExecConsole` on the allocation page and
`TaskActions` on the task page.

**Trade-off.** The console's extras are gone: quick-pick commands, the
draggable terminal height, and the "allocation gone" marker on a session
whose allocation disappears. `ExecTerminal` keeps the console's
improvements: refit on container resize, per-stream decoders, and a status
callback.

**Why (simplicity).** Both fixed the same bug (the per-allocation panels
remounted on every poll and closed the terminal). Two exec UIs that look
different and behave differently is worse than either alone, and the Actions
panel also runs declared actions on several instances at once.

## 7. Private addresses stay clickable, marked "private"

**Call.** Without a public address, a private endpoint address is still a
link, dimmed, with a `private` badge and a tooltip naming
`LEGION_WORKER_PUBLIC_ADDRESS`.

**Alternatives.** Render private addresses as plain text.

**Trade-off.** A link that often fails to open.

**Why (ease).** Operators on a VPN, a bastion tunnel, or the same VPC can
open it; hiding the link punishes them to warn everyone else. The badge
explains the failure before it happens.

## 8. "+ New" on the KV page only creates

**Call.** The KV page's "+ New" sends `expectedVersion: 0`, so naming a key
that already exists is refused ("X already exists. Select it in the list…")
instead of overwriting it. The key of an existing entry is no longer editable
in the panel.

**Alternatives.** Keep the old behaviour: "+ New" upserts, and editing the
key field of an existing entry writes a copy under the new name.

**Trade-off.** Renaming a key is now delete-and-create from the CLI or two
UI steps.

**Why (ease).** With values hidden, an upsert from "+ New" overwrites a value
the operator never saw. The old key-field edit silently forked entries rather
than renaming them.

## 9. `kv get --json` puts `references` beside the entry's fields

*Follow-up, 2026-10-19.*

**The call.** `legion kv get -j` prints `data` as the entry's own fields with
a `references` array added (`[]` when nothing reads the key), the same list
the text output prints as `used by:`.

**Alternatives.** Nest the entry (`data.entry`, `data.references`), mirroring
`GetKvReply`.

**Trade-off.** `data` is no longer exactly a `KvEntry`, the shape `kv put -j`
prints.

**Why.** Ease. Every script reading `data.value` or `data.version` keeps
working; a nested shape would have broken all of them for one new field.
