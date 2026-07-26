# Judgement Calls — Container Task Kind on macOS

No plan document: the work started as "make the container kind run on a Mac"
and turned out to be a chain of bug fixes, two of which needed a new surface.
The public page is [`docs/task-kind-container.md`](../../docs/task-kind-container.md).

Entries are ranked by project impact, highest first.

## 1. Declared `resources` are enforced on the container runtime, not just reserved

**Call.** A container task's declared `resources` size the workload:
`memory` (MiB) becomes `--memory` and `cores` becomes `--cpus`. That required a
new `TaskAllocation.task_resources` field, because the resources a job declares
were never sent to the worker at all — the dispatcher shipped config, checks,
network, identities, and artifacts, but not resources.

```jsonnet
resources: { memory: 512, cores: 1 },   // 512 MiB / 1-core guest, not 1 GiB / 4
```

**Alternatives.** Leave resources scheduling-only, matching what the `exec` kind
does today. Legion would keep one meaning for `resources` ("what the scheduler
reserves") instead of two.

**Trade-off.** `resources` now means different things per kind: enforced for
`container`, advisory for `exec`. A container that fit comfortably before can
now OOM, because it previously got Apple's 1 GiB default no matter what the job
asked for. Accepted, and called out on the public page.

**Why (simplicity).** The alternative is worse the moment two containers land on
one node: the scheduler believes it reserved 256 MiB while each guest took a
gigabyte, so bin-packing is fiction and the node overcommits silently. Both
comparators enforce here (Nomad's docker driver, K8s limits), so an operator's
existing intuition is the one that now holds.

## 2. Only *declared* resources cross the wire — scheduler defaults are not folded in

**Call.** `task_resources` carries the jobspec's own `resources` block. The
scheduler's defaults (256 MiB / 100 MHz / 1 allocation) are applied for
placement as before but deliberately not sent, so a kind can tell "unset" from
"explicitly asked for". A job declaring nothing keeps the runtime's own defaults.

**Alternatives.** Send the normalized request, so what the guest gets and what
the scheduler reserved always agree — the internally consistent answer.

**Trade-off.** Reservation and enforcement can disagree: a task declaring
nothing reserves 256 MiB and runs in a 1 GiB guest. That inconsistency is real
and is documented rather than hidden.

**Why (simplicity).** Sending normalized values would silently shrink every
existing container that never declared memory down to 256 MiB — an invisible
behavior change that surfaces as someone's workload OOMing after an upgrade.
The default `cpu` of 100 MHz has no runtime analogue anyway, so consistency was
only ever available on one axis.

## 3. Assigned ports are published into the container

**Call.** Each assigned port becomes `--publish <host>:<container>`, using the
port's `to` when the job pinned one and the host port on both sides otherwise —
matching the `LEGION_PORT_<NAME>` value the task is handed.

**Alternatives.** Leave networking to the runtime and document that macOS
containers are reachable only at the VM's own IP, which `legion service resolve`
would then have to learn to report.

**Trade-off.** Legion now owns a port-mapping concept for one runtime, and a
host port occupied by something outside Legion fails the container at launch
(the scheduler's dynamic assignment does not probe the host for occupancy).

**Why (ease).** Unpublished, the container is unreachable by its own health
check, so every service job on a Mac flaps between "started" and "unhealthy"
with nothing in the logs explaining why. Service discovery already promises an
address a consumer can dial; publishing is what makes that promise true.

## 4. A staged image launches by its pinned digest, not the jobspec's tag

**Call.** With `containerImageDesugar` on, `consume_artifact` records the
artifact's pinned reference and the launch uses it
(`docker.io/library/alpine@sha256:…`) instead of `config.image`.

**Alternatives.** Keep launching the tag. `container image load` registers the
layout under that tag, so it usually resolves to the digest that was staged.

**Trade-off.** The launch reference no longer matches what the jobspec says,
which is one more indirection when reading a worker log. Mitigated by logging
the resolved image on every start.

**Why (correctness).** "Usually" is the whole problem: a tag can move between
the pin and the run, and an allocation restored after a worker restart has no
staged record and falls back to the tag anyway. Pinning that a concurrent push
can defeat is decoration.

## 5. `oci://` image pulls resolve `linux/<arch>`; ORAS artifacts keep the host platform

**Call.** The image-layout path installs a platform resolver that selects
`linux/<arch>` from a multi-platform index. The ORAS path (`mediaType`) keeps
oci-client's host-platform default.

**Alternatives.** Force `linux/<arch>` for every `oci://` fetch — one rule, no
branch.

**Trade-off.** Two behaviors in one fetcher, keyed off whether `media_type` is
set, which a reader has to notice.

**Why (correctness).** Container images hold Linux guests on every host — Apple
`container` boots a Linux VM — so the host platform is simply the wrong
question; on macOS it asks for `darwin/arm64` and matches nothing. But an ORAS
artifact indexed by platform is meant for the machine fetching it, and forcing
Linux there would hand a macOS host the wrong binary. The two cases genuinely
want opposite answers.

## 6. An unusable runtime omits its readiness key rather than reporting `fail`

**Call.** A node publishes `task_plugins.container: "pass"` when the runtime
preflights cleanly, and omits the key entirely when it does not. The reason is
logged at `debug`.

**Alternatives.** Report `container: "fail: <reason>"`, which is what a
readiness check is nominally for and keeps the diagnosis in the node's own
health payload.

**Trade-off.** An operator reading `legion node list` sees an absence rather
than an explanation, and has to check the worker log for why.

**Why.** `derive_node_health` ANDs every check a node publishes, so any string
that is not `"pass"` marks the whole node unhealthy. A Linux box without youki
would be reported unhealthy for running `exec` tasks it handles perfectly well.
The honest-fail version needs per-kind health, which is a bigger change than
this branch should make; the preflight already puts the actionable message on
the allocation that actually tried to start.

## 7. `LEGION_WORKER_PORT` was added to make a second local cluster testable

**Call.** The worker's gRPC listen port — and the `workerAddress` it advertises
— became overridable by env var, defaulting to 4848 as before.

```bash
LEGION_WORKER_PORT=4858 legion start worker "$TOKEN" --name test-worker
```

**Alternatives.** Stop the cluster already running on the machine and test on
the default ports, adding no surface at all.

**Trade-off.** A new env var that exists mostly for development, and a second
way to get the worker address wrong (listen and advertise must agree, so both
read the same helper).

**Why (ease).** Without it a second worker silently registers against the *other*
manager and advertises an address another process owns, so allocations get
dispatched to the wrong worker — which looks like a job that runs nowhere, and
cost real time to diagnose. Multi-worker-per-host is also how anyone tests
placement locally, and `configs/workers/alt.jsonnet` already implies it was
meant to work.

## 8. Linux gets no cgroup resource limits in this change

**Call.** The resource mapping is macOS-only. The Linux bundle path still writes
no `linux.resources` into `config.json`, and the gap is stated on the public
page rather than left for a reader to discover.

**Alternatives.** Write the limits into the OCI spec too — the same few lines,
and the field is standard.

**Trade-off.** Resource enforcement now differs by platform, which is exactly
the kind of split that ages badly.

**Why.** A rootless runtime without cgroup delegation rejects the spec outright,
turning a working Linux container into a failing one, and there was no Linux
host available on this branch to find out. Shipping an untested change to the
one path that currently works is a bad trade against a documented gap. What
would change the call: a Linux worker in CI running the `oci_runtime` e2e tests.
