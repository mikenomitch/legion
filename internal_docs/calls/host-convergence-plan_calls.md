# Judgement Calls: Host Convergence (the Chef replacement)

Plan: [../plans/host-convergence-plan.md](../plans/host-convergence-plan.md).
Entries are ranked by project impact, highest first; an entry's number is its
rank. **The plan is `PLANNED` — these are design calls made while writing it, not
implementation calls.** Add implementation calls to this file as phases land.

## 1. Convergence is a task runtime, not a new top-level object

Host configuration is expressed as a task whose `runtime` is `converge` and
whose `config.resources` is a list of idempotent host resources — not as a new
`policy`/`profile`/`manifest` object with its own API, storage, and lifecycle.

**Alternatives.** (a) A first-class `HostConfig` object with its own RPCs
(`ApplyHostConfig`, `GetHostState`), the way Chef has cookbooks and Puppet has
catalogs. (b) A node-config *field* on the worker config, applied at join, with
no scheduling involvement at all.

**Trade-off.** Host mutation inherits scheduling semantics that do not obviously
fit it: allocations, counts, rollouts, and a lifecycle that normally means "stop
the task and undo its effects". A converge run is a task that deliberately
outlives its allocation, which is a wrinkle in Legion's otherwise clean "the
allocation owns its mutations" story.

**Why.** Everything convergence needs already exists on the task path and is
already documented, ACL'd, observable, and agent-readable: Jsonnet specs,
artifact fetching with checksums and pinning, KV templates with change policies,
constraints and node pools for targeting, batch schedules for intervals, the
event stream, `job plan`/`validate`, and per-allocation logs. (a) rebuilds all of
that for one feature. (b) cannot express "converge these nodes, on this
schedule, and tell me what changed" without inventing a scheduler.

## 2. Providers live inside the runtime; the plugin seam is the runtime itself

`package` resolves to apt/dnf/apk/pacman/brew *inside* the `converge` runtime,
selected from the node's fingerprint. There is no per-provider plugin boundary.

**Alternative.** A provider plugin ABI — `plugins/converge/<provider>/` — so a
site can add `zypper` or a company-internal package manager without touching
Legion.

**Trade-off.** Adding a package manager means a PR against Legion, and the
runtime's binary carries every provider whether or not a node can use one.

**Why.** Extensibility is a core goal, but the seam has to be where the variation
actually is. Providers share the inspect/decide/act/report machinery almost
entirely — the variation is a handful of shell invocations and their output
parsing. A plugin ABI there multiplies the surface (versioning, capability
negotiation, error mapping) for a change that is usually thirty lines. The
runtime *is* the plugin: a site with genuinely unusual needs ships its own
runtime, exactly as a site can ship its own task kind today.

## 3. `notifies` is a flat edge list, not a dependency graph

Resources run in declaration order. `notifies: ["service:app"]` means "if I
changed, act on that resource once, at the end of the run".

**Alternative.** Puppet-style autorequire with a real resource graph and
topological ordering (a file's parent directory is created before it,
automatically).

**Trade-off.** The author orders resources by hand, and a spec that puts the
`service` before the `template` it depends on simply behaves wrongly rather than
being reordered for them.

**Why.** Simplicity, and Puppet's own history: the graph is where config
management becomes a language, and the debugging story ("why did this run in that
order?") is the most-cited reason people leave. Declaration order is what an
author already reads top-to-bottom. If a real ordering need appears, `dependsOn`
between *tasks* already exists one level up.

## 4. Convergence needs its own ACL resource (`host`) and a per-worker opt-in

Submitting a job with a `converge` task requires `host:write` scoped to every
node pool it can land on, and a worker refuses converge allocations unless its
config sets `hostConverge.enabled`.

**Alternative.** Reuse `job:write` — a converge task is a task, and whoever can
run `exec` on a node can already install a package with a shell command.

**Trade-off.** A new ACL resource is new vocabulary in the policy language, a new
column in every "what can this token do" answer, and a migration for existing
policies. The "you could already do it with `exec`" argument is genuinely true.

**Why.** It is true and it is not the point: the difference between "can run a
workload" and "can permanently reconfigure the machines the fleet runs on" is
exactly the distinction a platform admin needs to express, and today they cannot.
Making it a *first-class* boundary is also what makes the feature adoptable —
without it, enabling convergence quietly widens what every existing job-submitter
can do. The worker opt-in exists for the same reason enforcement is opt-in
elsewhere: an upgrade should never hand a cluster a new power it did not ask for.

## 5. Why-run is a dispatched dry run, not a local simulation

`legion job plan` on a converge job sends a why-run to the targeted nodes and
returns per-node, per-resource verdicts.

**Alternative.** Evaluate the spec centrally against the node attributes the
manager already holds, with no worker round-trip — instant, and it works for
nodes that are down.

**Trade-off.** A dry run needs the nodes to be reachable and costs a dispatch per
node; a 500-node fleet answers in seconds, not milliseconds.

**Why.** The question "would this change anything?" is unanswerable from
fingerprints. Whether `/etc/app/app.conf` matches the template, whether nginx is
already 1.26.4, whether `/data` is mounted — only the node knows. A central
simulation would produce confident, wrong answers, which is worse than no dry run
at all for the exact audience (agents, and humans about to change 400 machines).

## 6. Drift is a scheduled run that reports, not a separate watcher

Drift detection is the existing `lifecycle: "batch"` + `schedule` path: run,
report what changed. A `driftOnly: true` flag reports without acting.

**Alternative.** A file-watching daemon on each node (inotify on managed paths)
that reports drift the moment it happens.

**Trade-off.** Drift is noticed on the interval, not immediately, and only for
resources the spec actually declares.

**Why.** This is how Chef, Puppet, and Ansible all work, and it reuses machinery
Legion already ships. A watcher is a new per-node daemon, a new event volume
problem, and a new class of "what happened to this file at 3am" false positives —
for a latency improvement that only matters if someone is *acting* on drift
alerts within minutes, which is not the job to be done here.

## 7. No SSH bootstrap of unmanaged hosts

Adoption stays `legion join <TOKEN>` on a host that already has the binary.

**Alternative.** `knife bootstrap`-style: point Legion at an IP with SSH
credentials and let it install the agent.

**Trade-off.** Legion cannot adopt a machine that has never heard of it, which is
precisely the first step of a Chef migration.

**Why.** SSH bootstrap means Legion holds SSH credentials for machines it does
not run on — a new, large credential-handling surface and a new failure mode
(half-bootstrapped hosts), for a step every provisioning tool the user already
has (cloud-init, Packer, an image, one `curl | sh` line) does better. Named as a
non-goal in the plan so it is a decision, not an oversight.

## 8. The 80/20 resource set, and no compliance product

v1 is `package`, `file`, `template`, `directory`, `user`, `group`, `service`,
`mount`, `line_in_file`, and a guarded `command`. No InSpec-style profiles, no
`reboot` resource, no Windows providers.

**Alternative.** Match Chef's resource catalogue, or at least add compliance
reporting, since "did the fleet stay compliant" is what a lot of Chef spend is
actually for.

**Trade-off.** Some real cookbooks will not port cleanly, and the compliance
audience is not served at all.

**Why.** The named ten cover what a node needs to run workloads, which is the job
this feature exists for. Compliance is a different product with a different buyer
and its own reporting model; bolting a weak version of it onto v1 would make the
feature look finished when it is not. `reboot` deserves its own plan because it
interacts with drains, stateful fencing, and allocation lifecycle at once.
