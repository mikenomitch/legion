# Plan: Runtime Disable: A Cluster-Wide Switch for a Task Runtime

> **Judgement calls:** [runtime-disable-plan_calls.md](../calls/runtime-disable-plan_calls.md)

**Status: PLANNED** (written 2026-11-07). Nothing here is built.

When a sandbox boundary has a published escape (a runc CVE, a VMM CVE, a
wasmtime advisory), an operator needs to stop new work from starting on
that runtime across the cluster in one command, optionally take running
work off it, and turn it back on after upgrading. Today the closest tool is
a namespace policy's `runtimes` allowlist, which only gates submitting a
job, one namespace at a time, and never touches work that is placed,
starting at the edge, or running.

```console
$ legion operator runtime disable container --reason "CVE-2024-21626, runc < 1.1.12" -j
{"ok":true,"data":{"runtime":"container","scope":"cluster","since":"2026-11-07T18:04:11Z",
 "reason":"CVE-2024-21626, runc < 1.1.12","running":{"allocations":412,"sessions":9031},
 "drain":false}}
```

The [isolate runtime](isolate-runtime-plan.md) was going to ship its own
`legion operator isolate disable`; it uses this instead.

Deliberately deferred, and named in §4: per-node or per-pool disables,
automatic disables from an advisory feed, and disabling a runtime only for
untrusted workloads.

---

## 1. Problem statement

The isolation runtimes are where a published escape turns into an
incident: [CVE-2024-21626](https://github.com/opencontainers/runc/security/advisories/GHSA-xr7r-f8xq-vfvv)
("Leaky Vessels", runc), the five critical wasmtime advisories of 2026
cited in the isolate plan, and any Firecracker or cloud-hypervisor escape.
The response is always the same: stop new untrusted code from starting on
the affected runtime now, decide what to do with what is running, upgrade,
and turn it back on. Legion has no way to do the first step:

1. **Namespace policy gates submit only.** `runtimes = [...]` refuses a new
   job; it never refuses a placement, a session start, an edge claim, or a
   scale-out of a job already admitted (docs/namespaces.md, "When the checks
   run").
2. **It is per namespace.** A cluster with fifty namespaces needs fifty
   policy edits, each a change to a standing rule meant to be permanent.
3. **Nothing takes running work off a runtime** except stopping each job by
   hand.

## 2. Prior art

- **Kubernetes `RuntimeClass`** selects a runtime per pod; removing a
  `RuntimeClass` or its node handler makes new pods unschedulable on it,
  and running pods are untouched.
  - **Take:** new starts stop, running work stays unless asked.
  - **Reject:** an object per runtime that has to be deleted and recreated.
- **Nomad** disables a driver per client with `plugin "<driver>" { config {
  enabled = false } }` and a client restart.
  - **Reject:** a config change and restart on every node during an
    incident.
- **Feature kill switches** (LaunchDarkly-style flags, Cloudflare's global
  config pushes): one replicated flag, evaluated at the point of use, fast
  to propagate.
  - **Take:** the shape: a replicated flag workers read on every start.

## 3. Jobs to be done, by persona

**Cluster operator / on-call responder**
- When an escape in a runtime is announced, I want to stop every new start
  on it, cluster-wide, in one command, so nothing new runs on a broken
  boundary while I upgrade.
- When the affected work holds state, I want it suspended rather than
  killed, so turning the runtime back on loses nothing.
- When I turn it back on, I want blocked work to place again on its own, so
  recovery is one command too.

**Security / platform admin**
- When only some tenants run untrusted code, I want to disable a runtime for
  their namespaces only, so trusted internal work keeps running.

**Job author**
- When my job cannot start because a runtime is disabled, I want the error
  to say so and why, so I do not debug my spec.

## 4. Goals and non-goals

### Goals

- One command disables a runtime cluster-wide or for named namespaces.
- It acts on every start path: placement, session starts (router and edge),
  edge claims, restarts, and scale-outs.
- Running work is untouched by default; `--drain` suspends what can be
  suspended and stops the rest.
- Workers learn of a disable within one heartbeat, and refuse starts
  themselves, so edge starts with no manager on the path are covered.
- Re-enabling wakes blocked work.

### Non-goals

- **Per-node or per-pool disables.** Drain the node instead.
- **Automatic disables** from an advisory feed or a version check.
- **"Disable for untrusted code only."** The scope is namespaces, which is
  where trust already lives.
- **Changing namespace policy.** Policy stays the standing rule at submit;
  this is the incident switch at start.

## 5. Background: what exists today

- Namespace policy `runtimes` (docs/namespaces.md): checked at `job run`,
  `job plan`, and `RunJobTemplate`, never at placement.
- The `operator` ACL resource covers raft and cluster operations.
- Blocked evaluations wake on capacity events, keyed by what blocks them
  ([sandbox-scaling-plan.md](sandbox-scaling-plan.md) Phase 3).
- Graceful stop and suspend exist per instance; the memory pressure
  response already suspends instances worker-side and reports them.

## 6. Proposed design

### 6.1 The surface

```console
$ legion operator runtime disable sandbox --namespace tenants-* --reason "firecracker GHSA-..." [--drain]
$ legion operator runtime enable sandbox --namespace tenants-*
$ legion operator runtime status -j
{"ok":true,"data":{"disabled":[{"runtime":"sandbox","namespaces":["tenants-*"],
 "since":"2026-11-07T18:04:11Z","by":"accessor:7f3c...","reason":"firecracker GHSA-...",
 "blocked":{"evals":12,"edgeRefusals":340}}]}}
```

RPCs: `Manager.SetRuntimeEnabled` and `Manager.ListRuntimeStates`, both
`operator:write` / `operator:read`, with an idempotency key on the set.
Catalog entries; deliberately not an MCP tool.

### 6.2 What a disable does

| Path | Effect |
| --- | --- |
| Submit (`job run`, `job plan`) | accepted with a warning naming the disable, so a deploy prepared during the incident is not lost |
| Placement | the scheduler treats the runtime as unavailable in scope; the eval is blocked with reason `runtime_disabled:<runtime>` and wakes on enable |
| Session starts (router and edge), edge claims, restarts | the worker refuses with `UNAVAILABLE` `runtime_disabled`, retryable, naming the reason |
| Running work | untouched, unless `--drain` |
| `--drain` | instances and sessions whose task has a `suspend` block are suspended (their checkpoints kept); everything else gets a graceful stop |

```text
UNAVAILABLE: runtime "sandbox" is disabled in namespace tenants-acme since 2026-11-07T18:04:11Z: firecracker GHSA-... (legion operator runtime status)
```

### 6.3 How workers learn

The disable is a replicated record. Managers send the current set on every
heartbeat reply and push changes on `StreamWork`; a worker refuses starts
from the moment it holds the record, so an edge start with no manager on
the path is refused within one heartbeat interval of the disable. A worker
that cannot reach any manager keeps the last set it held.

### 6.4 Re-enabling

`enable` deletes the record. Evals blocked on it wake, refused callers'
retries succeed, and drained instances stay suspended until something
resumes them (a wake, a start, or the caller), as any suspended instance
does.

### 6.5 Runtimes that react

A runtime may react to a disable before refusing. The isolate runtime does:
a `tier = "auto"` run of an isolate task with a VM task runs in the VM when
the `isolate` runtime is disabled, decided before it starts like any tier
choice. That is the runtime's own behavior, documented on its page; this
plan only delivers the state.

## 7. Proto, struct, config, and persistence changes

- **Store:** a `runtime_state` table (runtime, namespace patterns, reason,
  since, accessor) on the Control lane, one row per disable.
- **Proto:** the two RPCs; `runtime_states` on `HeartbeatReply` and a
  `StreamWork` item; `runtime_disabled` as a blocked-eval reason. Full proto
  comments.
- **Events:** `runtime.disabled` and `runtime.enabled` in the event journal.
- **ACL:** entries in `tests/integration/acl_endpoints.rs`.

## 8. Implementation phases

- **Phase 1:** the record, the RPCs and CLI, refusal on every start path,
  blocked evals that wake on enable, events, and the public section in
  `docs/task-supervision.md` (or a new `docs/runtime-disable.md`).
- **Phase 2:** `--drain`.

## 9. Testing plan

- **Unit:** scope matching against namespace globs; the refusal on each
  start path; a worker holding the last set without managers.
- **Integration (`tests/integration/runtime_disable.rs`):** disable `exec`
  and see a placement block with the reason, an edge session start refused,
  and running allocations untouched; enable and see the blocked eval place;
  scope to one namespace and see another unaffected; ACL entries.
- **Phase 2:** `--drain` suspends a keyed sandbox with a `suspend` block and
  gracefully stops a service.

## 10. Rollout and backward compatibility

New surface; nothing is disabled unless an operator says so. A worker too
old to understand the record ignores it, so a mixed-version cluster names
those workers in `runtime status` until they are upgraded.

## 11. Open questions

1. **Should submit be refused rather than warned?** Lean: warn. A refusal
   loses the deploy an author prepared during the incident, and placement
   is refused anyway.
2. **Should a disable expire on its own?** Lean: no; an incident switch that
   turns itself back on before the upgrade is worse than one someone forgets.

## 12. Acceptance criteria

- **Phase 1:** within one heartbeat of `disable`, no new allocation or
  session of the runtime starts anywhere in scope, edge starts included;
  running ones are untouched; after `enable`, blocked work places without
  resubmission.
- **Phase 2:** `--drain` leaves no running unit of the runtime in scope, and
  suspended ones resume after `enable` with their state.
