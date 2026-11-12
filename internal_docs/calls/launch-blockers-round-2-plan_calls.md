# Judgement calls: launch blockers, round 2 (2026-11-12)

Plan: [launch-blockers-round-2-plan.md](../plans/launch-blockers-round-2-plan.md).
Entries are ranked by project impact, highest first; an entry's number is its
rank. Entries 1 and 2 supersede calls #8 and #7 of
[launch-blockers-and-security-gaps-plan_calls.md](launch-blockers-and-security-gaps-plan_calls.md).

## 1. OCI and S3 are pinned through a loopback forward proxy, not a resolver hook

**Call.** On a guarded worker (and the manager's pin lookups) the OCI and S3
clients send through a `GuardProxy`: a `127.0.0.1` listener in the process
that speaks `CONNECT` and absolute-form HTTP, resolves each destination,
judges every address with the host guard, and dials only a judged one.
Refusals are `403` + `Legion-Denied-By: host-guard`, and the fetch error
carries the reason:

```console
staging failed: oci://registry.example/team/tool:v1: ...: the host guard refused 169.254.169.254:80: registry.example resolves to 169.254.169.254 (link-local); allocations may not reach it; it is opened only by the worker's hostGuard.allow
```

**Alternatives.** (a) A reqwest `dns_resolver` that judges: neither
oci-client 0.15 nor object_store 0.11 accepts a reqwest client or resolver.
(b) object_store 0.12's `HttpConnector` hook: tied to rustyriver's
object_store version, and it still leaves OCI. (c) Fork oci-client.

**Trade-off.** A second hop and a listener per guard; a plain-`http`
cross-origin redirect loses its label (reqwest drops `Proxy-Authorization`),
so it is refused without the guard's reason in the error. An operator's own
`HTTPS_PROXY`/`AWS_PROXY_URL` wins and the fetch is then judged by name only.
The S3 credential endpoints (`169.254.169.254`, `169.254.170.2`,
`fd00:ec2::254`) bypass the proxy so ambient principals keep working.

**Why (security, simplicity).** One mechanism pins both clients, redirects
included, with no fork and no version coupling, and the same code serves the
manager. The listener takes no secret: it only narrows what a host process
could dial directly, and gated allocations cannot reach loopback.

## 2. A worker stops an unlocked identity's tasks; a locked one is left alone

**Call.** Each worker holds an `flock` on `workers/<name>/worker.lock` for
its life. A second process on the same identity refuses to start. At startup
a worker stops the live tasks of any other identity in the data dir whose
lock is free, and sets that state aside (`worker_state.json.abandoned-<ts>`).

```text
Stopped 3 task(s) left running by worker identity wkr-old, which no worker in this data dir runs; the manager places their work again
```

**Alternatives.** Keep warning (calls #7 of the previous round); stop only
after a grace period since the old identity last ran (a heartbeat-touched
lock file); make it opt-in.

**Trade-off.** Two workers sharing one data dir under different names stop
each other's tasks if one starts while the other is down (a restart window).
The docs say to give each its own `LEGION_DATA_DIR`.

**Why (simplicity).** P1-5's case (an installer re-run under a new name)
starts the new worker right after the old one stops, so any grace period
would miss exactly the case the fix is for. A free lock is a fact the kernel
maintains, and the manager has already given the old node up.

## 3. Capability-aware placement is an implicit `contains` constraint per extra capability

**Call.** Workers advertise their expanded `container.allowCaps` (every name
for `ALL`, and on macOS); a container task gets
`container.allowCaps contains "CAP_X"` for each capability outside the
default set.

**Alternatives.** A scheduler-side capability feasibility check reading
worker types; advertising only the non-default extras.

**Trade-off.** One constraint per extra capability (usually one or two), and
the attribute lists up to ~41 names on an `ALL` node.

**Why (simplicity, extensibility).** It reuses the constraint engine, shows
up in `job plan`'s unplaced reasons for free, and matches how runtime classes
and idmapped mounts already steer placement.

## 4. `net policy test` judges advertised node rules: all refuse → deny, some refuse → a note

**Call.** Workers advertise `hostGuardRules`. `TestNetPolicy` denies with
`host-guard` only when every advertising node in the pool refuses, and
otherwise appends `(the host guard refuses it on 1 of 2 node(s) (edge-a): …)`
to the explanation.

**Alternatives.** Ask a live node (a worker-plane RPC per question); deny when
any node refuses.

**Trade-off.** Node addresses are as of registration; names resolve on the
manager. A one-node refusal does not change the verdict field.

**Why (simplicity).** The dry-run stays a manager read agents can call in a
loop. "Deny if any" would call a node's own address unreachable from the
nodes that can reach it.

## 5. Unratified claims are recorded in a side file, unsynced, only when their set changes

**Call.** `provisional_claims.json` holds pid, start time, and cgroup leaf
per unratified non-session claim; it is rewritten without fsync when the set
of keys changes, and the next worker process stops each recorded process.

**Alternatives.** Put them in `worker_state.json` with a flag; fsync each
write.

**Trade-off.** A claim killed but still in its graceful stop at the crash is
not recorded.

**Why (performance).** A provisional start is the edge-boot hot path; a full
state write with fsync per start was the cost the earlier design avoided. A
process crash keeps the page cache, and a host crash takes the processes
with it, so the fsync buys nothing.

## 6. Workers get each manager's `server.advertiseAddr` from its published config row

**Call.** `manager_addresses` maps each raft member to its config row's
`server.advertiseAddr`, falling back to the raft address; cached 30s.

**Alternatives.** Carry the API address in raft membership metadata (a
format change); a worker flag listing managers.

**Trade-off.** A follower whose config row has not been published yet is
listed by its raft address until it is.

**Why (simplicity).** The row already exists and is replicated; no new
config and no membership format change.

## 7. Removed-allocation history follows the worker's log retention, capped per job

**Call.** 7 days (`DEF_TASK_LOG_RETENTION_DAYS`), newest 1000 per job.

**Alternatives.** A `--node` override on `job logs` for unknown allocations
(needs a job-free ACL scope); a configurable retention (followers would prune
differently).

**Trade-off.** More rows in `allocation_history`; a worker whose
`LEGION_LOG_RETENTION_DAYS` is raised past 7 keeps files the manager forgets.

**Why (simplicity).** One window for both sides and no new flag; the cap keeps
a churning job from growing the table.

## 8. A static-port start waits for the port on the worker, not in the rollout

**Call.** `begin_task_allocation_gated` returns `InFlight` while another
allocation on the node holds one of the payload's static ports; the next
reconcile starts it.

**Alternatives.** Have the rollout wait for the old allocation's terminal
report before dispatching the replacement.

**Trade-off.** The replacement's start waits up to the old one's drain plus
`killAfter`, invisible except at debug level.

**Why (simplicity).** The worker is the only party that knows when the port is
actually free, and the same check covers the same-tick stop-then-start case.
