# Judgement calls: containers track (spot-check fixes P0-7, P1-6, P2-6, P2-9, P2-11)

Ordered most impactful first; the number is the rank.

## 1. The default capability set is Docker's, less `CAP_NET_RAW`

**The call.** A Linux container gets Docker's default capability set (moby
`oci/caps/defaults.go`) without `CAP_NET_RAW`: 13 capabilities instead of the
previous 3. The set goes into bounding, effective, and permitted; inheritable
and ambient stay empty, as Docker does since 20.10.14.

**Alternatives.**
- Docker's 14 exactly, including `NET_RAW` (what the request listed).
- Keep the three and require every jobspec that runs a stock image to add
  `CHOWN`, `SETUID`, `SETGID`, `DAC_OVERRIDE`, `FOWNER`.

**Trade-off.** Images that ping or traceroute at start need
`security.capabilities.add = ["NET_RAW"]`, on a node whose
`container.allowCaps` admits it (entry 2). Container root (still host uid 0,
no user namespace yet) gains `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `FSETID`,
`MKNOD`, `SETFCAP`, `SETPCAP`, `SETUID`, `SETGID`, `SYS_CHROOT` over what it
had. `MKNOD` is harmless behind the device-cgroup deny rule; the rest act only
on what the mount namespace shows it (its rootfs, `/legion/task`, volumes).

**Why.** Stock images are written against Docker's set. nginx's master
`chown`s its cache and `setuid`s its workers; the Postgres entrypoint `chown`s
PGDATA and `su-exec`s. Keeping three capabilities made every official image a
jobspec puzzle, which defeats "simplest scheduler to run". `NET_RAW` is the one
place Legion differs from Docker on purpose: a Docker container has its own
network namespace, and a Legion container shares the node's unless `gated`, so
raw sockets there sniff and spoof the node's interfaces. Nomad's Docker driver
drops `NET_RAW` from its `allow_caps` default for the same reason. Simplicity
won over Docker parity for that one capability; security won over parity
because the network model differs.

Surface:

```nickel
config = {
  image = "alpine:3.20",
  command = ["ping", "-c", "3", "1.1.1.1"],
  security = { capabilities = { add = ["NET_RAW"] } },
},
```

The worker-plane plan (§6.14, "the three capabilities ... stay as they are")
was annotated with a dated update rather than rewritten.

## 2. Capability adds are gated by a per-worker-type allowlist (`container.allowCaps`, Nomad's `allow_caps`) that defaults to the default set; jobs adjust with `security.capabilities`

**The call.** A job adjusts its set with `security = { capabilities = { add,
drop } }` (Docker `--cap-add`/`--cap-drop` semantics, validated at submit).
Each worker type carries `container = { allowCaps = [...] }`, which the
manager validates and normalizes at config load and hands to the worker at
registration (`WorkerConfig.container`, a JSON column on `worker_type`, then
`LEGION_CONTAINER_ALLOW_CAPS` for the in-process runtime, like
`resourceEnforcement`). A Linux container whose effective set (default, less
drop, plus add) holds anything outside its node's allowlist fails at start.
The allowlist defaults to exactly the 13-capability default set, so out of the
box drops always work and adds never do. `job run` / `job plan` also reject a
task whose set *no* worker type allows.

```nickel
# configs/workers/privileged.ncl
{ workerType = "privileged", pool = "privileged",
  container = { allowCaps = ["ALL"] } }
```

```text
task tool would run with capabilities this node does not allow container tasks to have: SYS_ADMIN. This node allows: AUDIT_WRITE, CHOWN, ... (the worker type's container.allowCaps). An operator can allow them by adding "SYS_ADMIN" to container.allowCaps in this worker type's config; otherwise take them out of the task (remove them from security.capabilities.add, or list them in security.capabilities.drop)
```

**Alternatives.**
- No node allowlist; gate adds on the ACL `host` grant
  (worker-plane-auth-plan §6.16) later. This was the first version of this
  call.
- Nomad's exec-driver behavior: silently trim the default set to a narrowed
  allowlist instead of failing.
- Make placement capability-aware, so a task that adds a capability only
  places on nodes that allow it.
- A worker-local setting (agent flag or env) instead of the worker-type
  config the manager serves.

**Trade-off.** A task that adds a capability places like any other and fails
at start on a node that refuses it; only the "no worker type allows it" case
is caught at submit, and authors pin such tasks to the right pool by hand. The
list is the whole allowlist, so an operator who wants "defaults plus NET_RAW"
writes all 14 names. An operator who narrows it below the default set makes
every container job there drop capabilities explicitly. Any `job:write` holder
can still use whatever a node allows: the allowlist bounds the node, not the
author (the ACL `host` grant remains the per-author gate, still a TODO).

**Why.** Before this change a job could not add capabilities at all, so
shipping `add` without a gate would have opened a new path to host root for
every `job:write` holder (`add = ["SYS_ADMIN"]` on an image that runs as
root). The `host` grant does not exist yet, and the allowlist closes the hole
now with the boundary in the right place: the node operator decides what a
node can be made to do. Failing instead of trimming (Nomad's Docker driver,
not its exec driver) means a job never runs silently without a capability it
asked for, which is the 3am-debuggable behavior. Following the worker-type
pattern keeps one place an operator configures a class of nodes, and lets the
manager check submissions against every type without asking the nodes.
Capability-aware placement is the better long-run UX but is a scheduler
change (a node attribute per capability or a new filter), so it is a TODO.
Security won over the extra config surface; simplicity kept it to one list
with Nomad's semantics, not a policy language.

## 3. Non-root stdio: chown the log files to the image user, not pipes plus a drain

**The call.** At every start the worker chowns the allocation's `stdout.log`
and `stderr.log` to the uid/gid the container runs as (the image `USER`,
resolved against its rootfs). The log directory stays `0700` root. Reopening
`/dev/stdout` (`/proc/self/fd/1`) checks only the file's inode, verified on
this host: a root-owned file in a 0700 dir refuses uid 101 with `EACCES`; after
chown, uid 101 reopens it.

**Alternatives.**
- Docker's model: hand the container pipes `fchown`ed to its user and drain
  them into the files from a worker thread.
- The same pipes drained by a per-task shim process (conmon-style) that
  outlives the worker.
- Make the files `0666` (any uid in the container can reopen).

**Trade-off.** A process that switches to *another* user inside the container
(`su`, `gosu`) and reopens `/dev/stdout` still gets `EACCES` (exactly as under
Docker, whose pipes belong to the spec user). A reopen with `O_TRUNC`
(`echo x > /dev/stdout`) truncates the captured log, because it is a file and
not a pipe; that was already true for root containers and `exec` tasks. Both
are in the public doc.

**Why.** Tasks deliberately survive worker restarts (own session, re-adopted by
pid). A worker-thread drain would close the read end on every worker restart
and SIGPIPE every container's next write. A shim process is the robust form of
pipes, but it is a new long-lived process per task, a hidden subcommand, and
an upgrade story, for the narrow su-then-reopen case. Copy-truncate rotation
keeps the inode, so the chown survives rotation. `0666` would also cover
su-then-reopen but makes "world-writable log file" a thing to explain.
Simplicity and restart-robustness won over full pipe semantics.

## 4. The macOS 200 MiB floor: reject at submit only when the task is pinned to macOS; otherwise fail early on the macOS worker

**The call.** `check_resources` (submit) rejects a `container` or `sandbox`
task whose constraints pin `os = "macos"` and whose guest is under 200 MiB.
Every other task is checked at start on a macOS worker, which fails before
invoking Apple `container`, with the same message naming the floor and the
field to raise. Linux is unaffected.

**Alternatives.**
- Warn at submit for every container task under 200 MiB.
- A placement-time constraint that keeps small container tasks off macOS
  nodes.
- Round the VM up to 200 MiB on macOS and let it run.

**Trade-off.** An unpinned task on a mixed or macOS-only cluster learns about
the floor at start, not submit (but from the allocation's failure reason, not
buried in stderr).

**Why.** At submit the target OS is unknown unless the task pins it. A blanket
warning would fire on every small Linux container job (128 MiB containers are
common), which trains people to ignore warnings. A placement filter would turn
a fixable spec error into `No nodes available` on a macOS-only dev cluster,
hiding the reason, and lives in the scheduler track's code. Rounding up makes
the declared memory a lie and overcommits the Mac by the difference per task.
Simplicity: one rule ("macOS VMs are at least 200 MiB"), enforced where the
fact is known, with one message.

```text
task web sizes its macOS VM at 128 MiB (resources.memoryMax, else resources.memory), below the 200 MiB Apple `container` requires; raise it to at least 200, or constrain the task to Linux nodes
```

## 5. The image cache is the node content store, per blob, with a HEAD freshness check and no TTL

**The call.** Image manifests, configs, and layers go into the existing node
content store (`artifacts` class) by digest, on both pull paths. A digest
reference the node holds costs no request. A tag costs one `HEAD` per start;
if the registry fails (429, down), the tag's last digest on this node (a new
`image_ref` table in the store's index) is used with a warning. Layers are
hard-linked into each allocation's layout. A process-wide claim makes
concurrent pulls of one reference share the download.

**Alternatives.**
- A short TTL during which a tag skips even the `HEAD` (the request offered
  this).
- Docker/Nomad's "present locally means use it" (never re-check a tag).
- A separate image store (own directory, own budget) from the content store.

**Trade-off.** Every start of a tag makes one request, so an image whose
registry is unreachable *and* has never been resolved on the node still fails.
Images share the `artifacts` budget (default 10% of disk, 1 to 20 GiB) with
checksummed artifacts, so a node with many large images evicts more. The
extracted rootfs is still rebuilt per allocation.

**Why.** `HEAD` is free under Docker Hub's anonymous limit and picks up a moved
tag on the next start, which is what someone who just pushed `:dev` expects; a
TTL would serve the old image for its duration, and "never re-check" makes
`:latest` mean "whatever this node first saw". The registry-failure fallback
covers the spot-check's 429 case without either. Reusing the content store
gets LRU, TTL, budgets, `minFreeDisk`, integrity checks, locality
advertisement, and `legion node content` for free, where a second store would
duplicate all of it (simplicity, and extensibility: the cache plan's per-blob
OCI storage is exactly this). The built-in container and sandbox runtimes
reach the store through a process-wide registration, because threading a
handle through `TaskInstance` would bump the task-plugin ABI for two built-in
callers.

## 6. Layer ownership is kept only when the worker is root; xattrs are best effort and skip `security.selinux` and `trusted.*`

**The call.** Extraction keeps each entry's owner exactly when the worker's
euid is 0. Legion applies it itself after the tar crate unpacks (`lchown`,
then re-applying a setuid/setgid mode that `chown` clears), because the tar
crate's own `preserve_ownerships` fails the whole layer on an empty uid/gid
field, which Go's `archive/tar` (Docker) reads as 0; an existing test layer hit
exactly that. PAX `SCHILY.xattr.*` records are re-applied as root, skipping
SELinux labels and overlay bookkeeping and logging (not failing) any attribute
the filesystem refuses.

**Alternatives.** Always try to chown and fail the start when it fails; or
apply every xattr and fail on error (the tar crate's `unpack_xattrs`).

**Trade-off.** A rootless worker produces a rootfs owned by its user, so an
image with a non-root `USER` may not be able to write its own directories
there; a host that refuses `user.*` xattrs loses them silently (debug log).

**Why.** A non-root worker cannot chown at all, so "try and fail" would break
every image for rootless development. Failing a start over an SELinux label
the host's policy would replace anyway helps nobody. Simplicity: the root path
is Docker's behavior, the non-root path degrades instead of failing.

## 7. `RLIMIT_NOFILE` defaults to 1048576 (soft = hard), capped at the worker's own hard limit, with a Compose-style `ulimits` override

**The call.** The bundle sets `nofile` soft = hard = 1048576, clamped to
`fs.nr_open` and to the worker's own hard limit (root or not). `ulimits` is a
map of lowercase names to a number, `"unlimited"`, or `{ soft, hard }`; each
declared entry replaces the default of that name.

**Alternatives.** containerd's own default (hard 1024, the bug) or systemd's
1024 soft / 524288 hard, which leaves programs to raise their soft limit.

**Trade-off.** Old software that loops over every possible descriptor (some
`close_fds` implementations) is slower with a million-descriptor limit, which
is why systemd and containerd moved to low soft limits.

**Why.** It is the value Docker containers ran with for years and what CRI-O
sets, so images are tuned for it; Redis's warning and a proxy's exhaustion go
away without a jobspec change. The override covers the rest in one familiar
shape (Compose's `ulimits`). Simplicity over the corner case.

The cap at the worker's hard limit came from the end-to-end run: on the test
host even root (with `CAP_SYS_RESOURCE`) could not raise its hard limit above
20000, so `runc` failed every start with `error setting rlimits for ready
process: operation not permitted`. Capping first means a start never fails on
the default; the node-wide knob is the worker unit's `LimitNOFILE=` (systemd's
default 524288 is still far above Redis's 10032). The alternative, probing
whether raising works, is a fork per start to answer a question the operator
already answered in the unit file.

## 8. `postgres.ncl` drops `to = 5432` and listens on the assigned port

**The call.** The example runs `docker-entrypoint.sh postgres -p
"$LEGION_PORT_PG"` and declares the port without `to`.

**Alternative.** Keep `to = 5432` so consumers' VIPs listen on the well-known
port, and listen on 5432 inside.

**Trade-off.** A VIP for this service gets the dynamic port number rather than
5432.

**Why.** On Linux the container shares the host network, so listening on 5432
collides with a second Postgres on the node and is not the port the service
and health check use; on macOS, `to` would publish the host port to 5432 while
Postgres listens on the assigned one. Without `to`, one command works on both.

## 9. Linux `command` still replaces `ENTRYPOINT`; examples name the entrypoint instead

**The call.** The divergence (Linux ignores the image's `ENTRYPOINT` and
`CMD`; Apple `container` keeps `ENTRYPOINT`) is documented and the examples
call `docker-entrypoint.sh` explicitly, which works on both. Changing the
semantics is left as a TODO.

**Alternative.** Make Linux honor `ENTRYPOINT` (and `CMD` when `command` is
omitted), with an `entrypoint` override, as Docker does.

**Trade-off.** `command = ["postgres"]` (the natural Docker habit) still fails
on Linux, with Postgres refusing to run as root.

**Why.** It changes what every existing Linux container jobspec runs and makes
`command` optional: a user-facing surface change that needs its own plan, not
a fix folded into a capability change.
