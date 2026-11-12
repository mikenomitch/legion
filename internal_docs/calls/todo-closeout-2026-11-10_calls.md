# Judgement calls: the TODO close-out (2026-11-10)

This work had no plan: it closed open items in [`TODO.md`](../TODO.md), by
doing the small ones and removing the ones that were done, duplicated, or
decided. What it shipped is in TODO.md's Done section under "TODO close-out
(2026-11-10)". Entries are ranked by project impact, so an entry's number is
its rank. Limitations a user needs to know are also stated in the public docs
each entry links.

## 1. The default heartbeat ceiling drops from 120s to 55s

**The call.** `heartbeat.maxInterval` defaults to `55s`, and a configured
ceiling at or past 90s starts with a warning instead of being refused:

```text
WARN heartbeat.maxInterval (120s) reaches the default stateful self-fence
(stateful.onLost.selfTerminateAfter, 90s): at that interval a healthy worker
can fence its stateful tasks between beats. Keep maxInterval below it or raise
selfTerminateAfter on stateful jobs
```

**Alternatives.** (a) Clamp the served interval at runtime below the smallest
`selfTerminateAfter` among running stateful allocations, the fix
[cadence calls #14](heartbeat-cadence-at-scale-plan_calls.md) calls right.
(b) Refuse a `maxInterval` at or past 90s. (c) Leave it documented.

**Trade-off.** Past about 68,750 nodes at the default rate the fleet beats
faster than `targetRatePerSecond` (the manager warns it is at the ceiling)
where it used to back off to 89s and 120s. A job with a `selfTerminateAfter`
shorter than 55s is still not protected.

**Why (simplicity).** A default that lets a healthy worker kill its own
stateful tasks is wrong for a new cluster, and the fix is one constant. (a)
couples the cadence controller to the allocation set, which gives up its
being a pure function of fleet size. (b) refuses a setting a cluster with no
stateful jobs can legitimately want. No fleet Legion has run reaches the
rungs this removes. Public doc:
[heartbeat-cadence-and-scale.md](../../docs/heartbeat-cadence-and-scale.md#known-limitations).

## 2. `resources.cpus` is refused, not deprecated

**The call.** The `cpus` alias for `cpu` is removed. A job or a manager's
`taskResourceDefaults` that sets it is refused by name:

```text
task resources set `cpus`, which is not a resource
Hint: use `cpu` for a CPU share in MHz (cpu = 500 is about half a 1 GHz core),
or `cores` for whole exclusive CPUs
```

The thirteen example jobs that wrote `cpus = 0.1` now write `cpu = 100`.

**Alternatives.** (a) Keep the alias and add a deprecation warning, which is
what the TODO item asked for. (b) Remove the special case and let `cpus` fall
through as a custom resource.

**Trade-off.** Any existing jobspec that uses `cpus` stops submitting. The
example jobs now reserve the share they looked like they reserved (100 MHz
where `cpus = 0.1` reserved a tenth of a megahertz), so they take real room
on a node.

**Why (simplicity).** `cpus = 1` reads as one CPU and reserved 1 MHz; every
bundled example had the mistake in it. AGENTS.md says not to keep
compatibility aliases. (b) would turn the mistake into "no node has the
resource `cpus`", a placement failure with no pointer to the cause. Public
doc: [resource-enforcement.md](../../docs/resource-enforcement.md).

**Follow-up (decided in review).** The refusal is temporary. `cpus` comes
back meaning a number of cores, converted to MHz per node from its
fingerprinted per-core frequency, rather than at a fixed rate; tracked in
`TODO.md` under Placement features. Refusing it now keeps the name free for
that meaning instead of shipping a third one in between.

## 3. A net plugin a running job uses cannot be deleted

**The call.** `DeleteNetPlugin` is refused while a job that is not stopped
names the plugin in its latest config, as it already was for policies.
`NetPlugin.referenced_by_jobs` lists those jobs for `get` and `status`:

```text
net plugin 'llm-token-meter' is used by job(s) 'metered', 1 job(s) you cannot read; update or stop them first
```

**Alternatives.** (a) Keep allowing the delete and only list the jobs. (b)
Add the jobs to the existing `referenced_by` field as `job:<id>` strings.

**Trade-off.** A delete now reads every running job's config, like a plugin
write does. An operator who cannot read a job that blocks them sees a count,
not a name, and has to ask someone who can.

**Why (simplicity).** Before, the delete succeeded and the job's hops failed
with `not on this node` from the next heartbeat: a breakage discovered on the
datapath that the write could have refused. (b) would put two kinds of name
in one field every reader would have to split. Public doc:
[network-plugins.md](../../docs/network-plugins.md).

## 4. `liveness.batchWindow` is deleted, not shipped

**The call.** The parsed-but-inert `liveness.batchWindow` field is removed
from the manager config, `configs/manager.ncl`, and the docs.

**Alternatives.** Keep it until the AWS attribution run
([liveness calls #4](liveness-off-the-raft-path-plan_calls.md)) says whether
a window helps.

**Trade-off.** If the measurement later favours a window, the field comes
back as a new change. A config that still sets it has no effect, as
before.

**Why (simplicity).** A documented knob that does nothing is worse than no
knob: an operator tuning it learns nothing. A steady-state heartbeat no
longer commits at all, so the window would only shape the checkpoint.

## 5. `legion kv list --values` marks a locked value instead of failing

**The call.** A value this manager cannot decrypt comes back with an empty
`value` and `locked: true` (`(locked)` in the table), and the ACL filter runs
before anything is decrypted. `kv get` of that key still fails.

**Alternatives.** (a) Leave the list failing whole. (b) Leave locked entries
out of the list.

**Trade-off.** A script that reads `value` without checking `locked` sees an
empty string for a secret that has one.

**Why (ease).** One data key that has not unwrapped on this manager used to
hide every other value in the prefix, including from a caller who could not
read the locked key at all. (b) would make a key look deleted. Public doc:
[kv-encryption.md](../../docs/kv-encryption.md).

## 6. A follower forwards `RatifyClaim` like any other write

**The call.** `edge_boot::ratify` returns the typed not-leader error, so the
handler answers with the status the leader-forward layer proxies. A follower
that knows the leader returns the leader's verdict; one that does not still
answers `FailedPrecondition`.

**Alternatives.** Keep the worker's own retry across managers as the only
path ([edge-ratify calls #2](edge-ratify-throughput-plan_calls.md) deferred
this).

**Trade-off.** A ratification through a follower costs one extra hop inside
the claim's `ratifyTimeout`, where the node used to redial.

**Why (performance).** The node's retry spent a round trip per wrong manager
and dropped the manager it was talking to; the forward is one hop and keeps
the connection. The worker's retry stays for the election case. Public doc:
[edge-boot.md](../../docs/edge-boot.md#ratification-on-a-raft-cluster).

## 7. Workers publish their pool as an attribute; `nodeId` needs nothing

**The call.** Every worker registers `pool` in its attributes, unless its
worker type already declares one, so `count = { num = 1, per = "pool" }`
works. The `nodeId` half of the item was already served by `per = "id"`.

**Alternatives.** Special-case `pool` in the scheduler's
`group_nodes_by_attr`, as `id` is.

**Trade-off.** A node's pool now appears twice in what it registers (the
`pool` field and the attribute), and an operator attribute named `pool`
shadows the built-in one.

**Why (extensibility).** As an attribute it is visible to everything that
reads attributes (constraints, `node info`), not only to `count.per`, and it
needs no change on the plan path.

## 8. Dropped log lines are counted at scrape time, per registry

**The call.** `legion_log_dropped_lines_total{sink}` (manager) and
`legion_worker_log_dropped_lines_total{sink}` (worker) are advanced to the
log writers' own drop counts when the endpoint is scraped.

**Alternatives.** Count in the writer and push into the registry as lines
drop.

**Trade-off.** The counter moves only when something scrapes it, so an OTLP
push between scrapes can lag.

**Why (simplicity).** The writers already count their drops, and the logging
path is the one place that must not call into anything that can block.

## 9. Content digests are hashed with `ring`

**The call.** `ContentDigest` and the content store's streaming writer use
`ring`'s SHA-256, behind a three-line local wrapper.

**Alternatives.** Turn on `sha2`'s `asm` feature, which accelerates every
`sha2` call site on aarch64.

**Trade-off.** Two SHA-256 implementations stay linked (`sha2` is still used
elsewhere and by dependencies).

**Why (performance).** `ring` uses the CPU's SHA extensions on both
architectures with no feature flag, and the checkpoint chunker already uses
it. `asm` pulls in `sha2-asm`, a new build dependency.

## 10. Items removed as decided, with where the decision lives

**The call.** These open items were removed without code, because each is a
limitation already decided and written down, or a question answered here:

| Item | Where it stands |
| --- | --- |
| `authz` webhooks and the worker's own calls are not judged by the host guard | [worker-plane-auth calls 23](worker-plane-auth-plan_calls.md) |
| An agent-less guest loses unflushed writes on stop | [sandbox-clean-stop calls 1](sandbox-clean-stop_calls.md) |
| `SIGINT`/`SIGQUIT` reach a sandbox command ignored | [sandbox-clean-stop calls 3](sandbox-clean-stop_calls.md), [task-runtime-sandbox.md](../../docs/task-runtime-sandbox.md) |
| Victim metadata is a store query inside planning | [preemption calls 20](preemption-and-eviction-plan_calls.md) |
| Drain rate limiting, fair share beyond bands | [preemption calls 10](preemption-and-eviction-plan_calls.md) |
| A typed error for `edge_boot::classify` | [edge-boot calls 17](edge-boot-plan_calls.md) |
| `sd_notify`, watchdog pings, socket activation for `exec` | [host-process-supervision plan §8](../plans/host-process-supervision-plan.md) |
| A checkpoint from before every microVM had vsock | [sandbox-exec calls 22](sandbox-exec-plan_calls.md); no installed base |
| Pre-namespace manifest paths | [namespaces calls 11](namespaces-plan_calls.md); no installed base |
| `reclaim` needs Linux 5.19 | [resource-enforcement.md](../../docs/resource-enforcement.md) |
| Running processes keep their old env token after a restore | [workload-identity.md](../../docs/workload-identity.md) |
| A Firecracker checkpoint cannot move between different data dirs | [suspend-and-resume.md](../../docs/suspend-and-resume.md) |
| Mesh plugin work "on demand only" | [mesh-interfaces plan §6.F](../plans/mesh-interfaces-plan.md) |
| Drop the `nftables` host guard for eBPF only | Kept, as the plan leans, until the kernel floor is settled ([mesh-interfaces plan §11 Q4](../plans/mesh-interfaces-plan.md)) |
| `legion fmt` on topiary + tree-sitter | No: `nickel format` already formats a jobspec, and the dependency is the one `Cargo.toml` turns off to keep the build small |
| A pluggable configuration language | No: Nickel imports JSON, YAML and TOML, and the API takes JSON |
| A virtual filesystem for the alloc directory | No: an idea with no problem attached |

**Alternatives.** Leave them open as reminders.

**Trade-off.** A reader of TODO.md no longer sees these limitations there;
they have to be in the docs and calls files, which each row links.

**Why (simplicity).** TODO.md's own rule is "what is wrong, where, and a
link"; none of these has work anyone intends to do. Reopening one means
adding it back with the reason things changed.

## 11. Two fixes were looked at and left open

**The call.** The stale `runc` stop tests and the bundled-roots image pull
stay open.

**Alternatives.** Rewrite the two tests against `stop_with`; switch
`oci-client` to native roots.

**Trade-off.** Both items are still on the list.

**Why.** Neither could be verified here: an image pull through this
environment's TLS-intercepting proxy fails on the bundled roots (the second
item is the cause of the first being untestable), and a trust-store change is
a security default that deserves its own review.
