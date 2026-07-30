# Legion as a systemd replacement, and as a Chef replacement

Gap analysis, not a plan. No code is proposed as committed here — where a
direction looks worth taking, it says which plan would have to be written
first. Written against `main` as of 2026-07-30.

The question this answers: *could Legion be used instead of systemd, and
instead of Chef, and if not, what would have to change?*

Three separate questions hide in there, and they have three different answers:

| Question | Answer |
| --- | --- |
| Can Legion replace systemd **as PID 1 / init**? | No, and it shouldn't try. |
| Can Legion replace **the systemd units you write for your own services**? | Partly today; four concrete gaps stand between "partly" and "yes". |
| Can Legion replace **Chef**? | Not today. It has the delivery half and none of the convergence half. It is a plausible extension (a `converge` task runtime + node-configuration jobs), and it would be a real differentiator, but it is a new subsystem and needs a plan. |

---

## 1. Decomposing "systemd"

systemd is at least eight products in one binary tree. Legion overlaps some of
them almost exactly and none of the others.

| systemd role | Legion today |
| --- | --- |
| PID 1: initrd handoff, early mounts, reaping orphans, ordered shutdown | **Nothing.** Not attempted anywhere in the tree. |
| Service supervision (`Type=simple/exec`, `Restart=`, `ExecStart`) | **Partial.** `exec` runtime (`src/task_plugins/exec.rs`) spawns and tracks a host process; restarts come from health checks, not exit codes. |
| Dependency ordering (`After=`, `Requires=`, `BindsTo=`) | **Yes, richer.** `dependsOn` with `start`/`complete`/`success` conditions, topologically sorted, cycles rejected (`docs/jobs-tasks-and-scheduling.md`). |
| Timers (`OnCalendar=`, `OnUnitActiveSec=`) | **Yes.** `lifecycle: "batch"` + `schedule: "every 15m"` or a cron expression. |
| Resource control (`MemoryMax=`, `CPUQuota=`, delegation) | **No, for `exec`.** `resources` is scheduling arithmetic only; `config.cgroup` *joins* an existing cgroup v2 path (`exec.rs:472-481`), it never creates one or writes a limit. Real enforcement exists only in the `container` and `sandbox` runtimes. |
| journald | **Close enough.** Per-allocation `stdout.log`/`stderr.log`, copy-truncate rotation, retention, `legion job logs --follow`, MCP + UI tailing, OTLP shipping (`docs/logging.md`). |
| Socket activation, `sd_notify` readiness, `WatchdogSec` | **No.** No fd passing, no notify socket. Readiness is external probing (`boot`/`health` checks). |
| Sandboxing (`ProtectSystem=`, `PrivateTmp=`, `NoNewPrivileges=`, seccomp, caps) | **Thin.** `user`, `oom_score_adj`, and Landlock read/write path sets. No namespaces, seccomp, or capability control for `exec`. |
| udev, logind, networkd, resolved, hostnamed, tmpfiles, sysusers | **Out of scope**, deliberately. |
| Template/instance units (`foo@1.service`) | **Yes, better.** `count`, `count: {num, per}`, and stateful slot/keyed instances with stable identity and per-instance storage. |
| `systemctl`, `journalctl`, `systemd-run` | **Yes, and cluster-wide.** `legion job run/stop/logs`, `legion exec`, named parameterized actions with their own ACL resource (`docs/exec-and-actions.md`). |

The pattern: Legion is already a *better* distributed service manager than
systemd on the axes systemd is weakest at (multi-host placement, rollouts,
identity, service discovery, cluster-wide observability), and a *worse* local
process supervisor than systemd on the axes systemd is strongest at (exit-code
semantics, resource enforcement, sandboxing, activation, and being there at
boot).

## 2. What genuinely works today

A "systemd-shaped" workload — a long-lived host binary, not a container — is a
first-class citizen:

- `runtime: "exec"` spawns with `setsid` so tasks survive worker restarts and
  are signal-isolated from the agent (`src/task_plugins/mod.rs:33`).
- `user`, `cwd`, `env`, `oom_adj`, `cgroup`, `landlock` cover the common
  hardening fields.
- `config.files[]` writes host files with modes before start (`exec.rs:349`) —
  and the KV `files`/`env` injections render through templates server-side with
  `watch`/`onChange`/`onDelete` policies, so a KV write can restart the service
  or drive a rollout (`docs/kv-store.md`). That is `systemd` + `EnvironmentFile`
  + a config-management push, in one field.
- `artifacts` fetch binaries/tarballs/git/S3/OCI into the task directory, with
  checksums and digest pinning (`docs/artifacts.md`).
- `checks` (`http`, `tcp`, `exec` — the `exec` kind runs **on the worker host**,
  `task_health.rs:393`) plus `on_unhealthy` restart/move budgets give a
  two-tier restart-then-reschedule policy.
- Worker restart recovers: persisted allocations whose pid is still alive are
  re-adopted (`task_manager.rs:983`); anything missing is re-sent by the
  manager on the next heartbeat's desired set, so it gets relaunched.
- Manager loss is fail-static: running tasks keep running. Only *stateful*
  allocations self-fence, and only when the job asked for it
  (`self_terminate_after_seconds > 0`).

So the honest positioning today is: **Legion can already run the services you
would otherwise write unit files for, as long as the process is well-behaved
enough that a health check is a good proxy for "is it alive".**

## 3. The four gaps that block "yes"

### 3.1 Nothing observes process exit (the big one)

There is no reaper. Nothing in the worker calls `try_wait`/`waitpid` on a task
process while it runs; the only `try_wait` in the tree is for interactive exec
sessions (`src/worker/exec.rs:211`). Consequences, all verifiable in the code:

- **A task with no checks is reported healthy forever.** `spawn_health_monitor`
  short-circuits: `set_allocation_health(.., true, "healthy (no checks
  configured)")` (`task_health.rs:54`). A crashed `exec` service with no checks
  is a silently dead allocation.
- **Reconciliation cannot notice.** `reconcile_allocations` compares id sets
  only; a dead process still has an `instances` entry, so the worker considers
  it running (`task_manager.rs:523`), and `is_allocation_active` is literally
  `instances.contains_key` (`:612`).
- **Batch success is optimistic.** A batch task's status is set to `Success`
  from its *lifecycle*, at planning time — `task_status_for_lifecycle`:
  `TaskLifecycle::Batch => TaskStatusState::Success` (`storage/store.rs:6455`).
  Exit codes never enter the picture, so `dependsOn: {condition: "success"}`
  means "was placed", not "exited 0".
- **No exit-status vocabulary at all**: no `Restart=on-failure` vs `always`,
  no `RestartSec` backoff for a crash (the delay knobs live in `on_unhealthy`
  and only fire from check failures), no `SuccessExitStatus`.

Everything else in this document is a smaller problem than this one, and fixing
it pays for itself independent of any systemd ambition: batch pipelines,
`job wait`, and DAG conditions are all currently trusting a value that means
something weaker than it says.

### 3.2 No resource enforcement for host processes

`resources: {cpus, memory, disk}` is scheduler bookkeeping. For `exec` the only
runtime knob is `cgroup: "path"`, which writes the child's pid into an
**already existing** `/sys/fs/cgroup/<path>/cgroup.procs` and fails if cgroup v2
isn't mounted. Legion never creates the cgroup, never sets `memory.max` or
`cpu.max`, and doesn't ask for delegation. So a Legion-managed host service can
be starved or can starve its neighbours in ways systemd would have prevented,
and the cluster's capacity math is an honour system on `exec` nodes.

### 3.3 Legion has no place at boot, and no local-only mode

Two related problems:

- **Something has to start the agent.** On a Linux box today that something is
  systemd. There is no `legion install --systemd`/`--launchd` helper, no
  packaged unit, nothing in `src/cmd/` for it. So Legion is a *layer above*
  systemd, not a replacement for it, on any machine that boots.
- **Everything routes through a manager.** There is no local unit store and no
  standalone mode. You can absolutely run a manager and a worker on the same
  box (`legion start manager -c …` then `legion join <TOKEN>`), which is the
  single-machine story — but the manager then also needs supervising, and if the
  manager's state is gone the box has no record of what it should be running.
  systemd's core value on a laptop or an appliance is that it works with no
  network and no control plane.

### 3.4 No activation or readiness protocol

No socket activation (no `LISTEN_FDS` handoff), no `sd_notify` (readiness is
inferred by probing, so slow-starting services need a `boot` check and a grace
window instead of just saying "ready"), no watchdog ping. `network.ports` gets
you allocated ports, not pre-bound listeners, so ordering-free startup between
dependent services isn't expressible the way `.socket` units make it.

## 4. Doing it today, honestly

If you want to run host services on a fleet with Legion right now, this works —
note the liveness check, which is what buys you crash-restart in the absence of
a reaper:

```jsonnet
{
  name: "host-services",
  tasks: [{
    name: "node-exporter",
    runtime: "exec",
    lifecycle: "service",
    // "One per node" is approximated by count.per over a node attribute.
    // A true sysbatch/system job type is still a todo (internal_docs/TODO.md).
    count: { num: 1, per: "id" },
    artifacts: ["https://example.invalid/node_exporter-1.8.2.linux-amd64.tar.gz"],
    config: {
      command: ["${artifact.node_exporter}/node_exporter"],
      user: "nobody",
      // Must already exist with its limits set — Legion joins, it does not create.
      cgroup: "legion/node-exporter",
      landlock: { read_only_paths: ["/usr", "/proc", "/sys"], read_write_paths: ["/tmp"] },
    },
    network: { ports: [{ name: "metrics", static: 9100 }] },
    checks: [{
      // The liveness check IS the reaper. Without a check, a crash is invisible.
      type: "health", kind: "tcp", port: "metrics",
      every: "10s", timeout: "2s", unhealthy_after_count: 2,
    }],
    on_unhealthy: { restart_count: 5, restart_period: "10m", restart_delay: "5s",
                    move_count: 0, move_period: "10m", move_delay: "1m" },
  }],
}
```

Timers become `lifecycle: "batch"` + `schedule`. Ordering becomes `dependsOn`.
`EnvironmentFile` and drop-ins become KV `env`/`files` with
`onChange: "restart"`. `systemctl status` becomes `legion job info`;
`journalctl -fu` becomes `legion job logs --follow`; `systemd-run` becomes
`legion exec` or a declared action.

What you cannot express: memory/CPU caps on the process, "restart only on
non-zero exit", socket activation, `PrivateTmp`, capability bounding sets, or
"this must come up before the network is configured".

## 5. What it would take (systemd path)

Ordered by value-per-unit-of-work. The first two are worth doing on their own
merits, whatever we decide about systemd.

1. **Process reaping and exit-status semantics.** A worker-side reaper that
   waits on task children (and pid-polls re-adopted ones), records exit status,
   and feeds it into: allocation failure reporting, real batch
   `Complete`/`Success`, and a new task-level `restart` policy
   (`always | onFailure | never` + backoff/`successExitStatus`) that is
   independent of `checks`. Touches the worker, the heartbeat proto, the store's
   task-status derivation, and the DAG condition semantics — a format/behavior
   change with a migration story, so it needs a plan.
2. **Legion owns the cgroup.** Create a per-allocation cgroup v2 subtree, apply
   `resources` as `memory.max` / `cpu.max` / `io.weight`, require delegation,
   and keep `cgroup:` as an escape hatch for "join this instead". This makes
   `resources` mean the same thing across `exec`, `container`, and `sandbox`,
   and is the difference between capacity accounting and capacity enforcement.
3. **`exec` hardening block.** `namespaces` (mount/pid/ipc/uts), `privateTmp`,
   `readOnlyPaths`/`protectSystem` (on top of Landlock), `capabilities`,
   `seccomp`, `noNewPrivileges`, `nice`, `umask`. A security boundary → plan
   required, and worth aligning with the existing root-privilege-split todo.
4. **Readiness and activation.** An `sd_notify`-compatible readiness socket
   (`Type=notify` equivalent) so slow starters report ready instead of being
   probed, then a watchdog ping, then socket activation via pre-bound fds tied
   to `network.ports`. Each is independently shippable; activation is the
   largest and the least obviously worth it for a scheduler.
5. **Boot integration and a standalone mode.** `legion install` emitting a
   systemd/launchd unit for the agent, plus node-configuration jobs that run on
   join (already a todo), plus a local desired-state cache so a worker can
   restore its set with the manager unreachable. This is what makes Legion
   usable as *the* service manager on an appliance or edge box.
6. **PID 1: explicit non-goal.** Early boot ordering, initrd handoff, mount
   units, udev, dbus activation, logind seats, and ordered shutdown are a
   distinct product with a decade of hardware-specific edge cases, and they buy
   Legion nothing on its actual axes (ease, performance, flexibility). The
   supportable position is: **systemd starts Legion; Legion runs your
   services.** Say so in the docs rather than leaving it ambiguous.

With 1–3 done, "use Legion instead of unit files for your own services" is an
honest claim on Linux and macOS. With 5, it extends to single-box and edge
installs. 4 and PID 1 are optional and probably never.

---

## 6. Chef

### 6.1 What Chef actually is

- A **resource/provider model** with idempotent convergence: `package`, `file`,
  `template`, `directory`, `user`, `group`, `service`, `mount`, `cron`,
  `execute` — each declaring desired state and converging to it, reporting
  *changed* vs *up-to-date*.
- **Cookbooks, recipes, attributes, roles, environments** — a layered
  configuration language with precedence.
- **Ohai** facts about the node.
- **chef-client runs** on an interval (or one-shot), producing a run report of
  what changed; `--why-run` for a dry run.
- **Search and data bags** — query the server for nodes/attributes; store
  shared (optionally encrypted) data.
- **Notifies/subscribes** — cross-resource ordering inside a run.
- **knife bootstrap** — turn a bare host into a managed node over SSH.
- Increasingly, **compliance** (InSpec profiles, drift reporting).

The essential difference from a scheduler: Chef *mutates the host and leaves*.
Legion's whole model is that a mutation lives exactly as long as the allocation
that owns it, and is torn down when the allocation stops
(`stop_allocation_inner` unmounts volumes and deletes the alloc dir). Those are
opposite lifecycles, and that — not any missing feature — is the crux.

### 6.2 What Legion already has

More than you would guess, because config *delivery* is well built out:

| Chef | Legion today |
| --- | --- |
| `remote_file`, `git`, artifact fetching | `artifacts` — http/file/git/S3/OCI-ORAS, checksums, digest pinning, auth from KV (`docs/artifacts.md`) |
| `file` / `template` resources | `exec` `config.files[]` (host path + mode + content) and KV `files[]` rendered server-side from templates |
| attributes / data bags | KV store with typed values, versions, optimistic concurrency, locks, JSON patch; `$`-references in jobspecs |
| cookbooks + attribute precedence | Jsonnet (imports, libsonnet, vars) — genuinely a decent recipe language — plus **job templates**: cluster-stored specs with typed variables and delegated submission (`docs/job-templates.md`) |
| ohai | node fingerprint attrs + `config_attrs` + dynamic attrs, with fingerprinter plugins (`src/worker/node_check.rs`) |
| roles / environments | node pools, `constraints`, `permit`, `affinities` |
| chef-client interval | `lifecycle: "batch"` + `schedule: "every 30m"` / cron |
| `--why-run` | `legion job plan` / `job validate` (read-only, agent-friendly) |
| search | `legion state query` over the SQLite state store + saved queries |
| run reports | event stream + per-allocation logs + OTEL traces/metrics |
| knife bootstrap | `legion join <TOKEN>` — one command, though it presumes the binary is on the host |
| notifies/subscribes | KV `onChange: restart | rollout | fail`, and `dependsOn` between tasks |

### 6.3 What's missing

- **No resource model and no idempotence.** There is no `package`, `user`,
  `service`, `mount`, or `cron` resource, and no notion of "already in desired
  state, did nothing". Everything host-shaped goes through `command`, so
  idempotence is the author's problem and "what changed" is unknowable.
- **No drift detection or reporting.** Legion knows the state of *allocations*,
  not the state of *hosts*. Nothing periodically compares declared host state to
  actual host state, and nothing surfaces per-resource diffs.
- **Lifecycle mismatch.** Host mutations made by a task are not owned by
  Legion's teardown path (the alloc dir goes away; the RPM you installed does
  not), and conversely a converge-and-exit task looks to Legion like a batch
  allocation that "succeeded" the moment it was placed (§3.1).
- **No run-on-join / run-once-per-node.** Both `sysbatch` and a system job type
  are open todos (`internal_docs/TODO.md`, "Job types"), as are node
  configuration jobs. `count: {num: 1, per: "id"}` approximates it and does
  not converge new nodes the way sysbatch would.
- **No bootstrap of an unmanaged host.** Chef reaches a bare machine over SSH.
  Legion needs its agent already installed and a join token issued — fine for a
  fleet you provision, wrong for "adopt this box".
- **No compliance/audit story**, no encrypted data bags (KV values are stored in
  the manager's state; ACLs gate access, and `GetArtifactCredential` is a known
  open hole).
- **No privilege model for host mutation.** Legion's ACLs are resource-shaped
  (`job|node|kv|state|operator|acl`); "this job may install packages on nodes in
  pool X" has no vocabulary. A converge runtime is a security boundary and
  cannot ship without one.

### 6.4 What it would take (Chef path)

The shape that fits Legion rather than fighting it — a plugin, not a rewrite:

1. **A `converge` task runtime plugin.** `runtime: "converge"` whose config is a
   list of typed, idempotent resources, each reporting
   `unchanged | changed | failed`:
   ```jsonnet
   {
     name: "base",
     runtime: "converge",
     lifecycle: "batch",
     schedule: "every 30m",
     config: {
       resources: [
         { type: "package",   name: "nginx", version: "1.26.*", state: "present" },
         { type: "user",      name: "app", shell: "/bin/false", system: true },
         { type: "directory", path: "/etc/app", owner: "app", mode: "0750" },
         { type: "template",  path: "/etc/app/app.conf", mode: "0640",
           template: 'tenant={{ kv "tenant/id" }}\n', notifies: ["service:app"] },
         { type: "service",   name: "app", state: "running", enabled: true },
       ],
     },
   }
   ```
   Providers behind it are per-platform (apt/dnf/brew/pkg), which argues for the
   *runtime* being the plugin boundary and providers being internal to it —
   consistent with how `container`/`sandbox` are structured.
2. **Node configuration jobs** (already a todo): a sysbatch that also runs on
   join, can set dynamic node attributes, and gates a node's readiness on
   converging. This is what makes "a node joins and configures itself" work, and
   it's the piece that makes 1 useful rather than decorative.
3. **Per-resource reporting into the store and the event stream**, so
   `legion node converge --dry-run` is Chef's why-run, and drift is queryable via
   `legion state query` and visible in the UI. Without this it's a shell script
   with extra steps.
4. **A host-mutation ACL scope** — new resource (`host`?) with pool scoping, plus
   an opt-in on the worker config (`allowHostConverge: true`), so a cluster can
   refuse host mutation outright.
5. **Exit-status semantics from §3.1**, reused: a converge run's real exit code
   is what makes `dependsOn: {condition: "success"}` mean "the base config
   converged" — the ordering primitive the whole idea rests on.

Non-goals worth naming up front: compliance/InSpec profiles, an
attribute-precedence system beyond Jsonnet + KV, SSH bootstrap of unmanaged
hosts, and anything resembling a full DSL. The 80/20 is packages, files,
directories, users, services, and mounts, converged on a schedule and on join,
with honest change reporting.

### 6.5 Is it worth it?

Strategically, yes — and `internal_docs/differentiators.md` already gestures at
it twice ("Node Configuration Jobs … Ansible type stuff", listed under both
plugin types and "Maybe"). *One* control plane, *one* config language, *one*
ACL model, and *one* event stream for both host configuration and workloads is
a real differentiator against Nomad and K8s, both of which push you to a second
tool. It also lands squarely on Legion's "omakase, easy day 1" thesis: the
node-config job is how a fresh box gets CNI, volumes, and prerequisites without
a second product.

Tactically, it must not be first. It depends on the reaper (§3.1), it wants the
cgroup work (§3.2) to bound converge runs, and it needs a security boundary
designed rather than discovered. Ordering: reaper → sysbatch/node-config jobs →
converge runtime.

---

## 7. Summary

- **systemd as init:** no, and it should be a documented non-goal.
- **systemd as your services' supervisor:** already usable if you attach a
  liveness check; genuinely competitive after (1) a process reaper with
  exit-status semantics, (2) Legion-owned cgroups with real limits, (3) an
  `exec` hardening block. Add a standalone/boot-integration mode and it works on
  single boxes and edge devices too.
- **Chef:** not today — Legion has delivery (artifacts, templates, KV,
  fingerprints, scheduling) and none of convergence (resources, idempotence,
  drift, reporting). A `converge` runtime plus node-configuration jobs is the
  natural shape, is differentiating, and needs its own plan.
- The single highest-leverage item across both answers is the same one, and it
  is a bug-shaped gap rather than a feature: **nothing in Legion observes a task
  process exiting.**
